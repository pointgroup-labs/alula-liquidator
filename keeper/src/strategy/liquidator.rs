//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits liquidations.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use engine::{
    lending_model::{
        DepositPosition, MarketData, Obligation, ObligationKey, PoolData, Underlying, liquidation,
        profitability,
    },
    ports::{BatchSimulator, EventCodec, LedgerReader, OperationBuilder, OperationEvent},
    reactor::{BoxFuture, Strategy},
};
use stellar_rpc_client::Event as SorobanEvent;
use tracing::{debug, error, info, warn};

use crate::{
    collect::{Event, stellar_ledger::NewLedger},
    constants::BPS_FACTOR,
    execute::{
        Action,
        stellar_tx::{LiquidationOutcomeMetric, SubmitStellarTx, TransactionSettleHook},
    },
    liquidator_capital::{LiquidatorCapital, random_id},
    metrics::{self, LiquidationKind, ScanOutcome, SkipReason},
    stellar::client::Gateway,
    storage::{cursor::CursorRepo, obligations::ObligationsRepo},
};

pub struct LiquidatorConfig {
    pub max_retries: u32,
    pub xlm_address: String,
    pub markets: Vec<String>,
    pub xlm_safety_margin: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub min_profit_margin_cents: i128,
    pub max_market_data_age: Duration,
    pub max_allowed_swap_slippage_bps: i128,
}

/// A cached market view plus the moment it was read. A failed refresh keeps the
/// previous view, so without the timestamp nothing downstream can tell a
/// current snapshot from prices frozen an hour ago.
struct MarketSnapshot {
    data: MarketData,
    /// When the prices in `data` were read. Only a snapshot that priced at
    /// least one pool is ever installed (see [`Liquidator::install_snapshot`]),
    /// so this attests held prices, not merely a reachable RPC. Pools missing
    /// from `data.pools_data` are not covered — obligations touching them are
    /// skipped rather than judged.
    ///
    /// Monotonic, so a wall-clock step cannot age a fresh snapshot.
    refreshed_at: Instant,
}

impl MarketSnapshot {
    fn new(data: MarketData) -> Self {
        Self { data, refreshed_at: Instant::now() }
    }

    fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.refreshed_at)
    }
}

#[derive(Debug, Clone)]
enum LiquidationType {
    /// Direct liquidation using the available liquidator's liquidity
    Direct,
    /// PreSwap liquidation via swapping the available liquidator's liquidity
    /// for the repaid asset during liquidation
    PreSwap {
        /// asset, swapped for the 'repaid asset' during liquidation
        source_asset: String,
        /// amount of 'source_asset' swapped for the 'repaid asset'
        source_amount_in: i128,
        /// 'repay_amount', passed to the 'liquidate' endpoint
        min_amount_out: i128,
        /// address of the swap provider to use
        swap_provider: String,
    },
    /// Liquidation utilizing the flash-borrow of `repay_amount` of the borrow asset, seizing collateral,
    /// swapping seized collateral back to the borrow asset, auto-repay.
    Flash {
        flash_repay_amount: i128,
        swap_provider: String,
        /// Underlying collateral that the seizure draws from jTokens (i.e. the
        /// portion beyond the borrower's plain collateral). The contract credits
        /// it as jTokens to the keeper's OWN obligation, so it must be withdrawn
        /// to the wallet before the seized collateral can be swapped to repay the
        /// flash loan. Zero when plain collateral covers the whole seizure.
        immediate_withdraw_amount: i128,
    },
}

#[derive(Debug)]
struct LiquidationPlan {
    repay_amount: i128,
    net_profit_value: i128,
    borrower_key: ObligationKey,
    borrow_pool_address: String,
    collateral_pool_address: String,
    expected_seized_collateral: i128,
    liquidation_type: LiquidationType,
}

pub struct Liquidator {
    pkey: String,
    skey: SigningKey,
    gateway: Arc<Gateway>,
    cursor_repo: CursorRepo,
    last_refresh_ledger: u32,
    config: LiquidatorConfig,
    obligations_repo: ObligationsRepo,
    ledger_reader: Arc<dyn LedgerReader>,
    market_data: HashMap<String, MarketSnapshot>,
    liquidator_capital: Arc<LiquidatorCapital>,
    obligations: HashMap<String, HashMap<ObligationKey, Obligation>>,
}

impl Liquidator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pkey: String,
        skey: SigningKey,
        gateway: Arc<Gateway>,
        cursor_repo: CursorRepo,
        config: LiquidatorConfig,
        obligations_repo: ObligationsRepo,
        ledger_reader: Arc<dyn LedgerReader>,
        liquidator_capital: Arc<LiquidatorCapital>,
    ) -> Self {
        Self {
            skey,
            pkey,
            config,
            gateway,
            cursor_repo,
            ledger_reader,
            obligations_repo,
            liquidator_capital,
            last_refresh_ledger: 0,
            obligations: HashMap::new(),
            market_data: HashMap::new(),
        }
    }
}

impl Strategy<Event, Action> for Liquidator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewLedger(b) => self.handle_new_ledger(b).await,
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(?self.config.markets, "sync_state: loading market(s)...");
            let markets = self.config.markets.clone();
            for market in markets {
                match self.ledger_reader.read_market_data(&market).await {
                    Ok(md) => {
                        info!(market, ?md);
                        self.install_snapshot(&market, md);
                    }
                    Err(e) => error!(?e, ?market, "get_market_data failed"),
                }

                let cached_obligations =
                    self.obligations_repo.load_all(&market).unwrap_or_else(|e| {
                        warn!(?e, ?market, "load_all obligations failed");

                        HashMap::new()
                    });
                if cached_obligations.is_empty() {
                    info!(?market, "no cached obligations");
                }

                self.obligations.insert(market, cached_obligations);
            }
            info!("sync_state: done");

            Ok(())
        })
    }
}

impl Liquidator {
    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();
        if !self.config.markets.contains(&market) {
            warn!(%market, "event from non-configured market");

            return vec![];
        }

        let Ok(operation_event) = self
            .gateway
            .decode_operation(&event)
            .inspect_err(|e| warn!(?e, "failed to decode operation event"))
        else {
            return vec![];
        };

        let event_name: &str = operation_event.as_ref();
        match operation_event {
            OperationEvent::Deposit
            | OperationEvent::Borrow
            | OperationEvent::AddCollateral
            | OperationEvent::Repay
            | OperationEvent::Withdraw
            | OperationEvent::RemoveCollateral => {
                let pool = self.gateway.decode_topic(&event, 1);
                let obl_display = self.gateway.decode_topic(&event, 2);
                info!(%event_name, ledger = event.ledger, %market, %pool, %obl_display, "position update event");

                let Ok(key) = self.gateway.parse_obligation_key_from_topic(&event, 2) else {
                    error!(%event_name, "cannot parse obligation key");

                    return vec![];
                };

                self.apply_obligation_snapshot(
                    &market,
                    "obligation",
                    event_name,
                    &key,
                    &event.value,
                );
            }
            OperationEvent::Liquidate => {
                let liquidator = self.gateway.decode_topic(&event, 1);
                let borrower = self.gateway.decode_topic(&event, 2);
                let borrow_pool = self.gateway.decode_topic(&event, 3);
                let collateral_pool = self.gateway.decode_topic(&event, 4);

                info!(ledger = event.ledger, %market, %liquidator, %borrower, %borrow_pool, %collateral_pool, "liquidation event");

                let Ok(borrower_key) =
                    self.gateway.parse_obligation_key_from_topic(&event, 2).inspect_err(|e| {
                        warn!(?e, "cannot parse borrower obligation key");
                    })
                else {
                    return vec![];
                };
                self.apply_obligation_snapshot(
                    &market,
                    "borrower_obligation",
                    event_name,
                    &borrower_key,
                    &event.value,
                );

                let liquidator_key = ObligationKey { user: liquidator, seed: None };
                self.apply_obligation_snapshot(
                    &market,
                    "liquidator_obligation",
                    event_name,
                    &liquidator_key,
                    &event.value,
                );
            }
        }

        if let Err(e) = self.cursor_repo.set(&event.id, event.ledger) {
            warn!(?e, id = %event.id, "failed to save cursor");
            metrics::CursorSource::LiquidatorEventCursor.record();
        }

        vec![]
    }

    fn apply_obligation_snapshot(
        &mut self,
        market: &str,
        field_name: &str,
        event_name: &str,
        key: &ObligationKey,
        value_xdr_base64: &str,
    ) {
        match self.gateway.parse_obligation_from_event_value(value_xdr_base64, field_name, key) {
            Ok(Some(obligation)) => {
                debug!(?key, ?obligation, %event_name, %market, "obligation snapshot");
                if let Err(e) = self.obligations_repo.put(market, key, &obligation) {
                    warn!(?e, %event_name, %market, "failed to save obligation");
                }
                if key.user == self.pkey
                    && let Some(snapshot) = self.market_data.get(market)
                {
                    emit_keeper_position_metrics(market, &obligation, &snapshot.data);
                }

                self.obligations
                    .entry(market.to_string())
                    .or_default()
                    .insert(key.clone(), obligation);
            }
            Ok(None) => {
                // TODO: This is liquidator's obligation, not a borrower's
                info!(?key, %event_name, %market, "obligation deleted on the market");
                if let Err(e) = self.obligations_repo.delete(market, key) {
                    warn!(?e, %event_name, %market, "failed to delete obligation");
                }
                if let Some(map) = self.obligations.get_mut(market) {
                    map.remove(key);
                }
            }
            Err(e) => warn!(%event_name, %field_name, %market, ?e, "parse error"),
        }
    }

    async fn handle_new_ledger(&mut self, new_ledger: NewLedger) -> Vec<Action> {
        let ledger = new_ledger.seq_num;
        if ledger.saturating_sub(self.last_refresh_ledger) < self.config.refresh_interval_blocks {
            return vec![];
        }
        self.last_refresh_ledger = ledger;

        info!(ledger, "refreshing market data + evaluating obligations...");

        self.refresh_market_data().await;

        let mut actions = vec![];
        for market in &self.config.markets {
            actions.extend(self.evaulate_market(market).await);
        }

        if !actions.is_empty() {
            info!(ledger, count = actions.len(), "submitting liquidations...");
        }

        actions
    }

    async fn refresh_market_data(&mut self) {
        let markets = self.config.markets.clone();
        for market in markets {
            match self.ledger_reader.read_market_data(&market).await {
                Ok(market_data) => {
                    if !self.install_snapshot(&market, market_data) {
                        continue;
                    }

                    let own_key = ObligationKey::new(self.pkey.clone());
                    if let Some(obl) = self.obligations.get(&market).and_then(|m| m.get(&own_key))
                        && let Some(snapshot) = self.market_data.get(&market)
                    {
                        emit_keeper_position_metrics(&market, obl, &snapshot.data);
                    }
                }
                // The previous snapshot is kept but not re-stamped, so the age
                // guard in `evaulate_market` decides when it stops being usable.
                Err(e) => warn!(?e, %market, "refresh failed"),
            }
        }
    }

    /// Installs a refreshed view, re-stamping its freshness, and reports whether
    /// it was accepted. A rebuild that priced no pool is refused: the aggregate
    /// call and every per-pool read failing is the oracle outage the age guard
    /// exists for, and stamping an empty view fresh is what would hide it.
    fn install_snapshot(&mut self, market: &str, market_data: MarketData) -> bool {
        if market_data.prices_nothing() {
            warn!(
                %market,
                unavailable_pools = market_data.unavailable_pools.len(),
                "rebuilt snapshot priced no pool; keeping the previous one"
            );

            return false;
        }

        metrics::set_snapshot_unavailable_pools(market, market_data.unavailable_pools.len() as u64);
        self.market_data.insert(market.to_string(), MarketSnapshot::new(market_data));

        true
    }

    async fn evaulate_market(&self, market: &str) -> Vec<Action> {
        let started = Instant::now();
        let Some(snapshot) = self.market_data.get(market) else {
            metrics::record_scan(
                market,
                ScanOutcome::NoMarketData,
                started.elapsed().as_secs_f64(),
            );

            return vec![];
        };

        let age = snapshot.age(started);
        if age > self.config.max_market_data_age {
            warn!(
                %market,
                age_secs = age.as_secs(),
                max_age_secs = self.config.max_market_data_age.as_secs(),
                "market snapshot too old; refusing to judge obligations on stale prices"
            );
            metrics::record_scan(
                market,
                ScanOutcome::StaleMarketData,
                started.elapsed().as_secs_f64(),
            );

            return vec![];
        }
        let market_data = &snapshot.data;

        let Some(obligations) = self.obligations.get(market).filter(|m| !m.is_empty()) else {
            metrics::record_scan(
                market,
                ScanOutcome::NoObligations,
                started.elapsed().as_secs_f64(),
            );

            return vec![];
        };

        let mut actions = Vec::new();
        let (mut checked, mut liquidatable) = (0_u64, 0_u64);
        for (obligation_key, obligation) in obligations {
            if obligation_key.user == self.pkey {
                // NB: self liquidations are not supported on the market currently
                continue;
            }

            let unpriced_pools = liquidation::unpriced_pools_touched(obligation, market_data);
            if !unpriced_pools.is_empty() {
                warn!(
                    ?obligation_key,
                    %market,
                    ?unpriced_pools,
                    "obligation touches a pool this snapshot could not price; skipping"
                );
                SkipReason::UnpricedPool.record();
                for pool_address in &unpriced_pools {
                    metrics::record_unpriced_pool_skip(market, pool_address);
                }

                continue;
            }

            // Counted here, past the skips: `checked` is the denominator of
            // `liquidatable`, so an obligation the snapshot could not price must
            // not read as one that was judged and found healthy.
            checked += 1;
            match liquidation::compute_is_liquidatable(obligation, market_data) {
                Ok(is_liquidatable) => {
                    if !is_liquidatable {
                        continue;
                    }
                }
                Err(e) => {
                    error!(%e);

                    continue;
                }
            }

            liquidatable += 1;
            // Bailing out of the whole scan here dropped the plans already built
            // and skipped the counters below, so the liveness gauge froze and
            // KeeperScanStalled fired with nothing naming the obligation at fault.
            let Ok(is_insolvent) = liquidation::compute_is_insolvent(obligation, market_data)
                .inspect_err(|e| error!(?e, ?obligation_key, %market, "insolvency check failed"))
            else {
                SkipReason::InsolvencyCheckFailed.record();

                continue;
            };

            info!(?obligation_key, ?obligation, is_insolvent, "LIQUIDATABLE");

            if let Some(action) = self
                .try_liquidate(market, is_insolvent, market_data, obligation, obligation_key)
                .await
            {
                info!(?action, "LIQUIDATION ACTION");
                actions.push(action);
            }
        }

        metrics::set_scan_counts(market, checked, liquidatable);
        // On the ok path `record_scan` also advances the last-successful-scan
        // liveness gauge and the readiness tick.
        metrics::record_scan(market, ScanOutcome::Ok, started.elapsed().as_secs_f64());

        info!(%market, checked, liquidatable, "market evaluation complete");

        actions
    }

    async fn try_liquidate(
        &self,
        market: &str,
        is_insolvent: bool,
        market_data: &MarketData,
        borrower_obligation: &Obligation,
        borrower_obligation_key: &ObligationKey,
    ) -> Option<Action> {
        let liquidator_obl_key = ObligationKey::new(self.pkey.clone());

        let mut best_liquidation_plan: Option<LiquidationPlan> = None;
        for borrow_position in &borrower_obligation.borrows {
            let Some(borrow_pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == borrow_position.pool_address)
            else {
                error!(?borrow_position, "borrow pool missing from market data");

                continue;
            };

            for deposit_position in &borrower_obligation.deposits {
                if deposit_position.pool_address == borrow_position.pool_address {
                    error!(
                        ?borrow_position,
                        ?deposit_position,
                        "same pool borrow/deposit positions"
                    );

                    continue;
                }
                if !deposit_position.j_tokens.0.is_positive()
                    && !deposit_position.collateral.0.is_positive()
                {
                    error!(?deposit_position, "empty deposit, skipping");

                    continue;
                }

                let Some(collateral_pool) = market_data
                    .pools_data
                    .iter()
                    .find(|p| p.pool_address == deposit_position.pool_address)
                else {
                    error!(?deposit_position, "collateral pool missing from market data");

                    continue;
                };

                let Some(plan) = self
                    .build_liquidation_plan_for_asset_pool_pair(
                        is_insolvent,
                        borrow_position.d_tokens.0,
                        borrow_pool,
                        market_data,
                        collateral_pool,
                        borrower_obligation,
                        deposit_position,
                        borrower_obligation_key,
                    )
                    .await
                else {
                    warn!(
                        ?borrow_pool,
                        ?collateral_pool,
                        "failed to build liquidation plan for pair"
                    );

                    continue;
                };

                // TODO: Introduce repay_value and prioritize according to it
                if best_liquidation_plan
                    .as_ref()
                    .is_none_or(|b| plan.net_profit_value > b.net_profit_value)
                {
                    best_liquidation_plan = Some(plan);
                }
            }
        }

        if let Some(plan) = best_liquidation_plan {
            metrics::record_expected_profit(market, plan.net_profit_value);
            info!(?plan, "selected best (borrow, deposit) pair",);

            self.execute_liquidation_plan(market, plan, &liquidator_obl_key).await
        } else {
            warn!("failed to come up with a liquidation plan");
            // TODO: Add metric?

            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_liquidation_plan_for_asset_pool_pair(
        &self,
        is_insolvent: bool,
        borrow_d_tokens: i128,
        borrow_pool: &PoolData,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        borrower_obligation: &Obligation,
        deposit_position: &DepositPosition,
        borrower_obligation_key: &ObligationKey,
    ) -> Option<LiquidationPlan> {
        let borrow_token = borrow_pool.token_address.as_str();

        let position_debt_tokens = borrow_pool
            .d_tokens_to_tokens_ceil(borrow_d_tokens.into())
            .inspect_err(|e| error!(%e))
            .ok()?;
        if position_debt_tokens <= Underlying::ZERO {
            error!(?borrower_obligation_key, "empty borrow position");

            return None;
        }

        let close_factor_cap = liquidation::compute_close_factor_repay_cap(
            is_insolvent,
            position_debt_tokens.0,
            borrow_pool.liquidation_close_factor_bps,
        );

        let position_j_tokens_as_tokens = collateral_pool
            .j_tokens_to_tokens_floor(deposit_position.j_tokens)
            .inspect_err(|e| error!(%e))
            .ok()?
            .0;
        let position_plain_collateral = deposit_position.collateral.0;

        let position_collateral_sum = position_j_tokens_as_tokens + position_plain_collateral;

        let position_collateral_value = position_collateral_sum
            .saturating_mul(collateral_pool.oracle_asset_price)
            .saturating_div(10_i128.pow(collateral_pool.token_decimals));

        let min_collateral_threshold_value = market_data
            .min_collateral_value_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);

        if !position_collateral_sum.is_positive()
            || position_collateral_value < min_collateral_threshold_value
        {
            warn!(
                position_collateral_sum,
                position_collateral_value,
                min_collateral_threshold_value,
                "position below minimum collateral threshold, skipping"
            );
            SkipReason::BelowCollateralThreshold.record();

            return None;
        }

        let profit_margin_borrow = profitability::compute_profit_margin_in_borrow_token(
            self.config.min_profit_margin_cents,
            market_data.oracle_price_decimals,
            borrow_pool,
        )
        .inspect_err(|e| error!(%e))
        .map(|u| u.0)
        .unwrap_or(0); // TODO: error?

        let max_feasible_repay = position_debt_tokens.0.min(close_factor_cap);
        if !max_feasible_repay.is_positive() {
            error!(max_feasible_repay, "non-positive max_feasible_repay");

            return None;
        }

        // The solvent-path repay cap needs the obligation-wide debt and
        // collateral values to bound the seizure to an LTV-improving amount
        // (mirrors the contract's `liquidate`).
        let (obligation_debt_value, obligation_collateral_value) = (
            liquidation::compute_obligation_debt_value(borrower_obligation, market_data)
                .inspect_err(|e| error!(%e))
                .ok()?,
            liquidation::compute_obligation_collateral_value(borrower_obligation, market_data)
                .inspect_err(|e| error!(%e))
                .ok()?,
        );

        let max_profitable_repay = profitability::compute_repay_cap_from_collateral(
            !is_insolvent,
            borrow_pool,
            max_feasible_repay,
            collateral_pool,
            position_collateral_sum,
            obligation_debt_value,
            profit_margin_borrow,
            obligation_collateral_value,
        )
        .inspect_err(|e| error!(%e))
        .ok()?;

        if max_profitable_repay <= 0 {
            warn!(max_profitable_repay, "non-positive max profitable repay");

            return None;
        }

        let raw_borrow_balance = self
            .liquidator_capital
            .try_get_balance(borrow_token, &*self.ledger_reader)
            .await
            .inspect_err(|e| {
                warn!(?e, %borrow_token, "balance query failed");
                SkipReason::BalanceQueryFailed.record();
            })
            .unwrap_or(0);

        let usable_borrow = if borrow_token == self.config.xlm_address {
            raw_borrow_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_borrow_balance
        };
        let min_profit_margin_value = self
            .config
            .min_profit_margin_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);

        // Each liquidation type draws on a different capital source, so each can
        // afford a different slice of `profitable_repay` (the collateral and
        // margin-capped ceiling on profitable repay for this pair):
        //   - Direct  → keeper wallet balance of the borrow token.
        //   - Flash   → the borrow pool's available liquidity (protocol capital).
        //   - PreSwap → other held assets swapped into the borrow token.
        // Build every feasible candidate, then let the caller pick the one that
        // liquidates the most debt.
        let mut candidates: Vec<LiquidationPlan> = Vec::new();

        // -- DIRECT LIQUIDATION --

        let direct_repay = max_profitable_repay.min(usable_borrow);
        if direct_repay.is_positive()
            && let Some(plan) = self
                .try_direct_plan(
                    is_insolvent,
                    direct_repay,
                    borrow_pool,
                    borrower_obligation,
                    market_data,
                    collateral_pool,
                    borrower_obligation_key,
                    min_profit_margin_value,
                    deposit_position,
                )
                .await
        {
            candidates.push(plan);
        }

        // -- FLASH LIQUIDATION --

        let flash_repay = {
            let all_liquidated_position_deposit = collateral_pool
                .j_tokens_to_tokens_floor(deposit_position.j_tokens)
                .inspect_err(|e| error!(%e))
                .ok()?
                .0;
            // NB: all that's available for borrowing is available
            // to be withdrawn without applying scarcity fees
            let available_to_withdraw_without_scarcity_fees =
                collateral_pool.available_for_borrow().inspect_err(|e| error!(%e)).ok()?.0;

            let (instantly_available_plain_collateral, instantly_available_withdrawable_deposit) = (
                deposit_position.collateral.0,
                all_liquidated_position_deposit.min(available_to_withdraw_without_scarcity_fees),
            );
            let instantly_available_collateral =
                instantly_available_plain_collateral + instantly_available_withdrawable_deposit;

            let updated_max_profitable_repay = profitability::compute_repay_cap_from_collateral(
                !is_insolvent,
                borrow_pool,
                max_feasible_repay,
                collateral_pool,
                instantly_available_collateral, // must use instantly available collateral only
                obligation_debt_value,
                profit_margin_borrow,
                obligation_collateral_value,
            )
            .inspect_err(|e| error!(%e))
            .ok()?;

            if updated_max_profitable_repay <= 0 {
                warn!(updated_max_profitable_repay, "non-positive updated max profitable repay");

                return None;
            }

            updated_max_profitable_repay.min(borrow_pool.total_available_adjusted.0)
        };

        if flash_repay.is_positive()
            && let Some(plan) = self
                .try_flash_plan(
                    is_insolvent,
                    flash_repay,
                    borrow_pool,
                    borrower_obligation,
                    market_data,
                    collateral_pool,
                    borrower_obligation_key,
                    min_profit_margin_value,
                    deposit_position,
                )
                .await
        {
            candidates.push(plan);
        }

        // -- PRESWAP LIQUIDATION --

        if let Some(plan) = self
            .try_preswap_plan(
                is_insolvent,
                borrow_pool,
                borrower_obligation,
                market_data,
                collateral_pool,
                max_profitable_repay,
                borrower_obligation_key,
                min_profit_margin_value,
                deposit_position,
            )
            .await
        {
            candidates.push(plan);
        }

        // Prefer the type that liquidates the most debt (biggest repay_amount).
        candidates.into_iter().max_by_key(|plan| plan.repay_amount)
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_direct_plan(
        &self,
        is_insolvent: bool,
        repay_amount: i128,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        borrower_key: &ObligationKey,
        min_profit_margin_value: i128,
        deposit_position: &DepositPosition,
    ) -> Option<LiquidationPlan> {
        let expected_seized_collateral = self.compute_seized(
            repay_amount,
            is_insolvent,
            borrow_pool,
            obligation,
            market_data,
            collateral_pool,
            deposit_position,
        )?;

        let (repay_value, expected_received_value) = (
            repay_amount
                .saturating_mul(borrow_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(borrow_pool.token_decimals)),
            expected_seized_collateral
                .saturating_mul(collateral_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(collateral_pool.token_decimals)),
        );
        let profitability = profitability::compute_liquidation_profitability(
            expected_received_value,
            repay_value,
            min_profit_margin_value,
        )
        .inspect_err(|e| error!(%e))
        .ok()?;
        if !profitability.is_acceptable {
            info!(?profitability, "DIRECT liquidation's profitability is not acceptable");

            return None;
        }

        info!(
            ?borrower_key,
            borrow_pool = %borrow_pool.pool_address,
            collateral_pool = %collateral_pool.pool_address,
            repay_amount, expected_received_value, ?profitability,
            "considering DIRECT liquidation plan..."
        );

        Some(LiquidationPlan {
            repay_amount,
            expected_seized_collateral,
            borrower_key: borrower_key.clone(),
            net_profit_value: profitability.net_value,
            liquidation_type: LiquidationType::Direct,
            borrow_pool_address: borrow_pool.pool_address.clone(),
            collateral_pool_address: collateral_pool.pool_address.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_flash_plan(
        &self,
        is_insolvent: bool,
        repay_amount: i128,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        borrower_key: &ObligationKey,
        min_profit_margin_value: i128,
        deposit_position: &DepositPosition,
    ) -> Option<LiquidationPlan> {
        let seized_amount = self.compute_seized(
            repay_amount,
            is_insolvent,
            borrow_pool,
            obligation,
            market_data,
            collateral_pool,
            deposit_position,
        )?;
        let all_plain_collateral = deposit_position.collateral.0;

        let (plain_collateral_amount, immediate_withdraw_amount) =
            if seized_amount <= all_plain_collateral {
                (seized_amount, 0_i128)
            } else {
                let all_deposit = collateral_pool
                    .j_tokens_to_tokens_floor(deposit_position.j_tokens)
                    .inspect_err(|e| error!(%e))
                    .ok()?
                    .0;
                let left = seized_amount - all_plain_collateral;

                (all_plain_collateral, all_deposit.min(left))
            };
        // NB: Just-in-case recalculation
        let de_facto_seized_amount =
            plain_collateral_amount.checked_add(immediate_withdraw_amount)?;

        let flash_fee = profitability::compute_flash_fee(
            Underlying(repay_amount),
            borrow_pool.flash_loan_fee_bps,
        )
        .inspect_err(|e| error!(%e))
        .ok()?
        .0;
        let flash_repay_amount = repay_amount.saturating_add(flash_fee);

        let (collateral_token, borrow_token) =
            (collateral_pool.token_address.as_str(), borrow_pool.token_address.as_str());

        let (best_provider, amount_out) = match self
            .best_swap_quote(de_facto_seized_amount, collateral_token, borrow_token)
            .await
        {
            Some(q) => q,
            None => {
                SkipReason::FlashSwapShortfall.record();

                return None;
            }
        };
        let amount_out_minus_slippage = amount_out
            .saturating_mul(BPS_FACTOR - self.config.max_allowed_swap_slippage_bps)
            .saturating_div(BPS_FACTOR);

        if amount_out_minus_slippage < flash_repay_amount {
            info!(
                amount_out_minus_slippage,
                flash_repay_amount, de_facto_seized_amount, "unprofitable post-liquidation swap"
            );
            SkipReason::FlashSwapShortfall.record();

            return None;
        }

        let (flash_repay_value, received_collateral_value) = (
            flash_repay_amount
                .saturating_mul(borrow_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(borrow_pool.token_decimals)),
            de_facto_seized_amount
                .saturating_mul(collateral_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(collateral_pool.token_decimals)),
        );
        let profitability = profitability::compute_liquidation_profitability(
            received_collateral_value,
            flash_repay_value,
            min_profit_margin_value, // TODO: Can we lose some value here?
        )
        .inspect_err(|e| error!(%e))
        .ok()?;
        if !profitability.is_acceptable {
            info!(?profitability, "FLASH liquidation's profitability is not acceptable");

            return None;
        }

        info!(
            flash_fee,
            repay_amount,
            ?borrower_key,
            seized_amount,
            ?profitability,
            flash_repay_amount,
            de_facto_seized_amount,
            borrow_pool = %borrow_pool.pool_address,
            collateral_pool = %collateral_pool.pool_address,
            "considering FLASH liquidation plan...",
        );

        Some(LiquidationPlan {
            repay_amount,
            liquidation_type: LiquidationType::Flash {
                flash_repay_amount,
                swap_provider: best_provider,
                immediate_withdraw_amount,
            },
            borrower_key: borrower_key.clone(),
            net_profit_value: profitability.net_value,
            expected_seized_collateral: seized_amount,
            borrow_pool_address: borrow_pool.pool_address.clone(),
            collateral_pool_address: collateral_pool.pool_address.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_preswap_plan(
        &self,
        is_insolvent: bool,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        max_profitable_repay: i128,
        borrower_key: &ObligationKey,
        min_profit_margin_value: i128,
        deposit_position: &DepositPosition,
    ) -> Option<LiquidationPlan> {
        let borrow_token = borrow_pool.token_address.as_str();

        let mut best_plan: Option<LiquidationPlan> = None;
        for source_asset in &self.config.assets_to_hold {
            if source_asset == borrow_token {
                // NB: Preswap plan is about swapping assets for
                // what must be repaid during the liquidation, hence the asset
                // itself is not a candidate to be swapped.
                continue;
            }

            let Ok(raw_balance) = self
                .liquidator_capital
                .try_get_balance(source_asset, &*self.ledger_reader)
                .await
                .inspect_err(|e| {
                    warn!(?e, %source_asset, "balance query failed");
                })
            else {
                continue;
            };
            let swappable_balance = if source_asset.as_ref() == self.config.xlm_address {
                raw_balance.saturating_sub(self.config.xlm_safety_margin)
            } else {
                raw_balance
            };
            if !swappable_balance.is_positive() {
                info!(
                    raw_balance,
                    swappable_balance, source_asset, "non-positive liquidator usable balance"
                );

                continue;
            }

            let Some((best_provider, max_amount_in)) =
                self.best_swap_base(max_profitable_repay, source_asset, borrow_token).await
            else {
                error!(%source_asset, "no swap base");

                continue;
            };
            let max_amount_in_plus_slippage = max_amount_in
                .saturating_mul(BPS_FACTOR + self.config.max_allowed_swap_slippage_bps)
                / BPS_FACTOR;

            let (best_provider, amount_in, amount_out) =
                if max_amount_in_plus_slippage <= swappable_balance {
                    // NB: The best provider promised these conditions, and we can afford them
                    (best_provider, max_amount_in_plus_slippage, max_profitable_repay)
                } else {
                    // NB: We can't afford to repay the full debt with our own liquidity
                    let Some((new_best_provider, min_amount_out)) =
                        self.best_swap_quote(swappable_balance, source_asset, borrow_token).await
                    else {
                        error!(%source_asset, "no swap quote");

                        continue;
                    };
                    let min_amount_in_minus_slippage = min_amount_out
                        .saturating_mul(BPS_FACTOR - self.config.max_allowed_swap_slippage_bps)
                        / BPS_FACTOR;

                    (new_best_provider, swappable_balance, min_amount_in_minus_slippage)
                };
            let repay_amount = amount_out;

            let expected_seized = self.compute_seized(
                repay_amount,
                is_insolvent,
                borrow_pool,
                obligation,
                market_data,
                collateral_pool,
                deposit_position,
            )?;

            let Some(source_pool) =
                market_data.pools_data.iter().find(|p| p.token_address == *source_asset)
            else {
                error!(%source_asset, "source asset pool not in market data");

                continue;
            };

            let (cost_value, gain_value) = (
                amount_in
                    .saturating_mul(source_pool.oracle_asset_price)
                    .saturating_div(10_i128.pow(source_pool.token_decimals)),
                expected_seized
                    .saturating_mul(collateral_pool.oracle_asset_price)
                    .saturating_div(10_i128.pow(collateral_pool.token_decimals)),
            );
            let profitability = profitability::compute_liquidation_profitability(
                gain_value,
                cost_value,
                min_profit_margin_value,
            )
            .inspect_err(|e| error!(%e))
            .ok()?;
            if !profitability.is_acceptable {
                info!(
                    ?source_pool,
                    ?profitability,
                    ?collateral_pool,
                    "PRESWAP liquidation's profitability is not acceptable",
                );

                continue;
            }

            info!(
                repay_amount,
                ?borrow_pool,
                ?borrower_key,
                ?profitability,
                expected_seized,
                ?collateral_pool,
                "considering PRESWAP liquidation plan..."
            );

            if best_plan.as_ref().is_none_or(|bp| bp.repay_amount < repay_amount) {
                best_plan = Some(LiquidationPlan {
                    repay_amount,
                    liquidation_type: LiquidationType::PreSwap {
                        swap_provider: best_provider,
                        min_amount_out: amount_out,
                        source_amount_in: amount_in,
                        source_asset: source_asset.clone(),
                    },
                    borrower_key: borrower_key.clone(),
                    net_profit_value: profitability.net_value,
                    expected_seized_collateral: expected_seized,
                    borrow_pool_address: borrow_pool.pool_address.clone(),
                    collateral_pool_address: collateral_pool.pool_address.clone(),
                })
            }
        }

        best_plan
    }

    async fn best_swap_quote(
        &self,
        amount_in: i128,
        asset_in: &str,
        asset_out: &str,
    ) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;
        for provider in &self.config.swap_providers {
            match self.ledger_reader.get_amount_out(amount_in, asset_in, asset_out, provider).await
            {
                Ok(out) if out > 0 => {
                    let is_better = best.as_ref().is_none_or(|(_, prev)| out > *prev);
                    if is_better {
                        best = Some((provider.clone(), out));
                    }
                }
                Ok(_) => {}
                Err(e) => warn!(?e, %provider, "swap quote failed"),
            }
        }

        best
    }

    async fn best_swap_base(
        &self,
        amount_out: i128,
        asset_in: &str,
        asset_out: &str,
    ) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;
        for provider in &self.config.swap_providers {
            match self.ledger_reader.get_amount_in(amount_out, asset_in, asset_out, provider).await
            {
                Ok(amount_in) if amount_in > 0 => {
                    let is_better = best.as_ref().is_none_or(|(_, prev)| amount_in < *prev);
                    if is_better {
                        best = Some((provider.clone(), amount_in));
                    }
                }
                Ok(_) => {}
                Err(e) => warn!(?e, %provider, "get_amount_in failed"),
            }
        }

        best
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_seized(
        &self,
        repay_amount: i128,
        is_insolvent: bool,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
    ) -> Option<i128> {
        let (obligation_debt_value, obligation_collateral_value) = (
            liquidation::compute_obligation_debt_value(obligation, market_data)
                .inspect_err(|e| error!(%e))
                .ok()?,
            liquidation::compute_obligation_collateral_value(obligation, market_data)
                .inspect_err(|e| error!(%e))
                .ok()?,
        );

        let seized = liquidation::compute_expected_seized_collateral(
            repay_amount,
            borrow_pool,
            collateral_pool,
            deposit_pos,
            obligation_debt_value,
            obligation_collateral_value,
            is_insolvent,
            market_data.min_collateral_value_cents,
            market_data.oracle_price_decimals,
        )
        .inspect_err(|e| error!(%e))
        .ok()?;

        if !seized.is_positive() {
            warn!(repay_amount, "expected seized collateral is zero");
            SkipReason::UnprofitableSeizeZero.record();

            None
        } else {
            Some(seized)
        }
    }

    async fn execute_liquidation_plan(
        &self,
        market: &str,
        plan: LiquidationPlan,
        liquidator_obl_key: &ObligationKey,
    ) -> Option<Action> {
        let requests = self.build_batch_requests(&plan)?;

        match self.gateway.simulate_batch(market, liquidator_obl_key, &requests).await {
            Ok(true) => {
                info!(
                    ?plan.borrower_key,
                    "batch simulation OK"
                );
            }
            Ok(false) => {
                warn!(?plan.borrower_key, "batch simulation failed, dropping plan");
                SkipReason::BatchSimFailed.record();

                return None;
            }
            Err(e) => {
                warn!(?e, ?plan.borrower_key, "batch simulation error");
                SkipReason::BatchSimFailed.record();

                return None;
            }
        }

        // Reserve in-flight capital so other opportunities (and strategies like balancer)
        // don't double-spend the wallet balance. The reservation is released by
        // the executor's SettleHook on every terminal tx outcome; the 5-minute
        // ledger TTL is now only a safety ceiling for hooks lost to executor
        // task panics.

        let (reserve_token, reserve_amount) = match &plan.liquidation_type {
            LiquidationType::Direct => {
                // NB: Relying on token_address == pool_addresss
                let borrow_token = plan.borrow_pool_address;

                (borrow_token, plan.repay_amount)
            }
            LiquidationType::PreSwap { source_asset, source_amount_in, .. } => {
                (source_asset.clone(), *source_amount_in)
            }
            // Flash liquidations source their capital from the protocol — the
            // keeper's wallet is not at risk for the repay amount, so there is
            // nothing to reserve.
            LiquidationType::Flash { .. } => {
                // NB: Relying on token_address == pool_addresss
                let borrow_token = plan.borrow_pool_address;

                (borrow_token, 0)
            }
        };

        // Direct / PreSwap spend wallet capital and must pass the reservation
        // gate. Flash spends protocol capital, so it mints a standalone op_id
        // for the SettleHook (releasing an unreserved id is a no-op).
        let op_id = if reserve_amount > 0 {
            let raw_balance = match self
                .liquidator_capital
                .try_get_balance(&reserve_token, &*self.ledger_reader)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(?e, %reserve_token, "balance query failed at reservation time");
                    SkipReason::BalanceQueryFailed.record();

                    return None;
                }
            };
            let available = if reserve_token == self.config.xlm_address {
                raw_balance.saturating_sub(self.config.xlm_safety_margin)
            } else {
                raw_balance
            };

            match self.liquidator_capital.reserve(reserve_amount, available, &reserve_token) {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        ?e,
                        repay_amount = reserve_amount,
                        %reserve_token,
                        "skipping liquidation: insufficient available balance after pending reservations"
                    );
                    SkipReason::InsufficientBalanceAfterReservations.record();

                    return None;
                }
            }
        } else {
            random_id()
        };

        match self.gateway.batch_op(market, liquidator_obl_key, &requests) {
            Ok(op) => {
                // Counts plans handed to the executor — NOT confirmed liquidations.
                // The tx may still fail, have bad_seq retry(though, unexpectedly) or confirmation
                // poll; those outcomes are tracked separately by the executor's
                // own counters.
                let kind = match &plan.liquidation_type {
                    LiquidationType::Direct => LiquidationKind::Direct,
                    LiquidationType::PreSwap { .. } => LiquidationKind::PreSwap,
                    LiquidationType::Flash { .. } => LiquidationKind::Flash,
                };
                metrics::record_plan_dispatched(market, kind);
                Some(Action::SubmitTx(SubmitStellarTx {
                    op,
                    signing_key: self.skey.clone(),
                    max_submission_retries: self.config.max_retries,
                    on_settle: Some(TransactionSettleHook {
                        op_id,
                        liquidation_outcome: Some(LiquidationOutcomeMetric {
                            market: market.to_string(),
                            expected_net_value: plan.net_profit_value,
                        }),
                        liquidator_capital: self.liquidator_capital.clone(),
                    }),
                }))
            }
            Err(e) => {
                error!(?e, ?plan.borrower_key, "failed to build batch op");
                SkipReason::OpBuildFailed.record();
                self.liquidator_capital.release(op_id);

                None
            }
        }
    }
}

impl Liquidator {
    fn build_batch_requests(
        &self,
        plan: &LiquidationPlan,
    ) -> Option<Vec<<Gateway as OperationBuilder>::Request>> {
        let mut requests = Vec::new();

        match &plan.liquidation_type {
            LiquidationType::Flash {
                swap_provider,
                flash_repay_amount,
                immediate_withdraw_amount,
            } => {
                // Flash-borrow the borrow asset
                let flash_req = match self
                    .gateway
                    .flash_borrow_request(&plan.borrow_pool_address, plan.repay_amount)
                {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build flash_borrow request");

                        return None;
                    }
                };
                requests.push(flash_req);

                // Liquidate — pays repay_amount of borrow asset, receives
                //    seized_amount of collateral. Only the plain-collateral
                //    portion lands in the wallet; any jToken-sourced portion is
                //    credited to the keeper's OWN obligation deposit position.
                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    plan.repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build liquidate request (flash path)");

                        return None;
                    }
                };
                requests.push(liquidate_req);

                // Withdraw the jToken-sourced collateral into the wallet.
                //    Without this, only the plain-collateral portion is swappable
                //    and the flash loan cannot be repaid. SwapExactTokens (step 4)
                //    flushes pending transfers first, so this withdrawal settles
                //    to the wallet before the swap pulls from it. Skipped when the
                //    seizure is entirely plain collateral.
                if *immediate_withdraw_amount > 0 {
                    let withdraw_req = match self
                        .gateway
                        .withdraw_request(&plan.collateral_pool_address, *immediate_withdraw_amount)
                    {
                        Ok(r) => r,
                        Err(e) => {
                            error!(?e, "failed to build withdraw request (flash path)");

                            return None;
                        }
                    };
                    requests.push(withdraw_req);
                }

                // Swap the full seized collateral → borrow asset. The swap
                //    output must be ≥ min_swap_out so the end-of-batch flash
                //    repayment can be satisfied. If the swap delivers less, the
                //    whole batch reverts atomically.
                let collateral_token = &plan.collateral_pool_address; // NB: Relying on pool_address == token-address
                let borrow_token = &plan.borrow_pool_address;
                let swap_path = [collateral_token.as_str(), borrow_token.as_str()];
                let swap_req = match self.gateway.swap_exact_tokens_request(
                    swap_provider,
                    plan.expected_seized_collateral,
                    *flash_repay_amount,
                    &swap_path,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build swap_exact_tokens request (flash path)");

                        return None;
                    }
                };
                requests.push(swap_req);

                // The flash repayment (repay_amount + fee) is automatic —
                // RequestTransfers::execute_transfers handles it at end-of-batch.
                // No explicit Repay request is needed.
            }

            LiquidationType::PreSwap {
                source_asset,
                source_amount_in,
                min_amount_out,
                swap_provider,
            } => {
                // Swap a held asset → borrow asset. SwapExactTokens flushes
                //    pending transfers first, so the acquired borrow tokens land
                //    in the wallet before Liquidate pulls from it. min_amount_out
                //    guarantees enough output to fund the repay.
                let borrow_token = &plan.borrow_pool_address;
                let preswap_path = [source_asset.as_str(), borrow_token.as_str()];

                let preswap_req = match self.gateway.swap_exact_tokens_request(
                    swap_provider,
                    *source_amount_in,
                    *min_amount_out,
                    &preswap_path,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build pre-swap request");

                        return None;
                    }
                };
                requests.push(preswap_req);

                // Liquidate, paying the just-acquired borrow asset.
                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    plan.repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build liquidate request (preswap path)");

                        return None;
                    }
                };
                requests.push(liquidate_req);
            }

            LiquidationType::Direct => {
                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    plan.repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build DIRECT liquidation request");

                        return None;
                    }
                };
                requests.push(liquidate_req);
            }
        }

        Some(requests)
    }
}

fn emit_keeper_position_metrics(market: &str, obligation: &Obligation, market_data: &MarketData) {
    for deposit in &obligation.deposits {
        let Some(pool) =
            market_data.pools_data.iter().find(|p| p.pool_address == deposit.pool_address)
        else {
            error!(%market, pool = %deposit.pool_address, "emit_keeper_position_metrics: pool not found");

            continue;
        };
        let j_tokens_underlying =
            pool.j_tokens_to_tokens_floor(deposit.j_tokens).map(|u| u.0).unwrap_or(0);
        metrics::set_keeper_deposit(
            market,
            &pool.pool_address,
            &pool.token_symbol,
            deposit.j_tokens.0,
            deposit.collateral.0,
            j_tokens_underlying,
        );
    }

    for borrow in &obligation.borrows {
        let Some(pool) =
            market_data.pools_data.iter().find(|p| p.pool_address == borrow.pool_address)
        else {
            warn!(%market, pool = %borrow.pool_address, "emit_keeper_position_metrics: pool not found");

            continue;
        };
        let d_tokens_underlying =
            pool.d_tokens_to_tokens_ceil(borrow.d_tokens).map(|u| u.0).unwrap_or(0);
        metrics::set_keeper_borrow(
            market,
            &pool.pool_address,
            &pool.token_symbol,
            borrow.d_tokens.0,
            d_tokens_underlying,
        );
    }
}

#[cfg(test)]
mod tests {
    use engine::lending_model::BorrowPosition;
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;
    use crate::{liquidator_capital::LiquidatorCapitalConfig, storage::SqliteStore};

    const MARKET: &str = "MARKET_A";
    const MAX_AGE: Duration = Duration::from_secs(60);

    fn liquidator() -> Liquidator {
        let pkey = "G".repeat(56);
        let store = SqliteStore::open(":memory:").expect("in-memory sqlite");
        let rpc_url = "http://127.0.0.1:1".parse().expect("valid url");
        let gateway = Arc::new(Gateway::new(&rpc_url, pkey.clone()).expect("gateway"));

        Liquidator::new(
            pkey.clone(),
            SigningKey::from_bytes(&[7_u8; 32]),
            gateway.clone(),
            store.cursor(),
            LiquidatorConfig {
                max_retries: 1,
                xlm_address: "C".repeat(56),
                markets: vec![MARKET.to_string()],
                xlm_safety_margin: 0,
                assets_to_hold: vec![],
                swap_providers: vec![],
                refresh_interval_blocks: 2,
                min_profit_margin_cents: 0,
                max_market_data_age: MAX_AGE,
                max_allowed_swap_slippage_bps: 0,
            },
            store.obligations(),
            gateway,
            Arc::new(LiquidatorCapital::new(
                &pkey,
                LiquidatorCapitalConfig {
                    xlm_address: "C".repeat(56),
                    balance_cache_ttl: Duration::from_secs(1),
                    reservation_ttl: Duration::from_secs(1),
                },
            )),
        )
    }

    fn pool_data(pool_address: &str) -> PoolData {
        PoolData {
            pool_address: pool_address.to_string(),
            token_address: pool_address.to_string(),
            token_symbol: "TST".to_string(),
            token_decimals: 7,
            total_borrowed: 0.into(),
            total_d_tokens: 0.into(),
            total_j_tokens: 0.into(),
            total_available: 0.into(),
            total_available_adjusted: 0.into(),
            total_supply: 0.into(),
            total_collateral: 0.into(),
            j_token_rate_floor_bps: 10_000,
            d_token_rate_ceil_bps: 10_000,
            oracle_asset_price: 1,
            open_ltv_bps: 8_000,
            close_ltv_bps: 8_500,
            liability_factor_bps: 10_000,
            liquidation_close_factor_bps: 5_000,
            max_liquidation_incentive_bps: 1_000,
            flash_loan_fee_bps: 5,
            status_flags: u32::MAX,
            utilization_ratio_limit_bps: 9_000,
        }
    }

    fn market_data(priced_pools: &[&str], unavailable_pools: &[&str]) -> MarketData {
        MarketData {
            insolvency_ltv_bps: 10_000,
            pools_data: priced_pools.iter().map(|p| pool_data(p)).collect(),
            oracle_price_decimals: 14,
            min_collateral_value_cents: 0,
            unavailable_pools: unavailable_pools.iter().map(|p| p.to_string()).collect(),
        }
    }

    fn snapshot(
        age: Duration,
        priced_pools: &[&str],
        unavailable_pools: &[&str],
    ) -> MarketSnapshot {
        let refreshed_at =
            Instant::now().checked_sub(age).expect("monotonic clock older than the test age");

        MarketSnapshot { data: market_data(priced_pools, unavailable_pools), refreshed_at }
    }

    /// A `LedgerReader` serving one canned market view. Only `read_market_data`
    /// is on the refresh path these tests drive; the rest of the port is
    /// unreachable from there.
    struct StubReader(MarketData);

    impl LedgerReader for StubReader {
        fn read_market_data<'a>(
            &'a self,
            _market_address: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<MarketData>> {
            Box::pin(async move { Ok(self.0.clone()) })
        }

        fn read_pool_data<'a>(
            &'a self,
            _market_address: &'a str,
            _pool_address: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<PoolData>> {
            unimplemented!("not on the refresh path")
        }

        fn read_user_obligation<'a>(
            &'a self,
            _market_address: &'a str,
            _key: &'a ObligationKey,
        ) -> BoxFuture<'a, anyhow::Result<Obligation>> {
            unimplemented!("not on the refresh path")
        }

        fn read_all_obligations_keys<'a>(
            &'a self,
            _market_address: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<Vec<ObligationKey>>> {
            unimplemented!("not on the refresh path")
        }

        fn read_token_balance<'a>(
            &'a self,
            _token_address: &'a str,
            _account: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            unimplemented!("not on the refresh path")
        }

        fn read_account_token_balance<'a>(
            &'a self,
            _token_address: &'a str,
            _account: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            unimplemented!("not on the refresh path")
        }

        fn get_amount_out<'a>(
            &'a self,
            _amount_in: i128,
            _asset_in: &'a str,
            _asset_out: &'a str,
            _swap_provider_address: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            unimplemented!("not on the refresh path")
        }

        fn get_amount_in<'a>(
            &'a self,
            _amount_out: i128,
            _asset_in: &'a str,
            _asset_out: &'a str,
            _swap_provider_address: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            unimplemented!("not on the refresh path")
        }

        fn get_is_obligation_liquidatable<'a>(
            &'a self,
            _market: &'a str,
            _liquidator_address: &'a str,
            _borrower: &'a ObligationKey,
            _borrow_pool: &'a str,
            _collateral_pool: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<bool>> {
            unimplemented!("not on the refresh path")
        }
    }

    /// A borrow with no deposits: enough to make the obligation loop do real
    /// work, not enough to reach the network-bound planning path.
    fn borrower() -> HashMap<ObligationKey, Obligation> {
        HashMap::from([(
            ObligationKey::new("GBORROWER".to_string()),
            Obligation {
                borrows: vec![BorrowPosition {
                    d_tokens: 1_000.into(),
                    pool_address: "POOL_A".to_string(),
                }],
                deposits: Vec::new(),
            },
        )])
    }

    /// Borrows from POOL_A and deposits into POOL_Z, so a snapshot missing both
    /// must name both — the case that separates per-pool attribution from
    /// recording only the first offender.
    fn two_pool_borrower() -> HashMap<ObligationKey, Obligation> {
        HashMap::from([(
            ObligationKey::new("GBORROWER".to_string()),
            Obligation {
                borrows: vec![BorrowPosition {
                    d_tokens: 1_000.into(),
                    pool_address: "POOL_A".to_string(),
                }],
                deposits: vec![DepositPosition {
                    j_tokens: 1_000.into(),
                    collateral: 0.into(),
                    pool_address: "POOL_Z".to_string(),
                }],
            },
        )])
    }

    /// Runs `f` under a scoped recorder (no global install) and returns its
    /// value with the rendered exposition.
    fn under_recorder<T>(f: impl FnOnce() -> T) -> (T, String) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // `::metrics` — the crate's own `metrics` module is in scope here.
        let value = ::metrics::with_local_recorder(&recorder, f);

        (value, handle.render())
    }

    /// Runs one market evaluation under a scoped recorder and returns the
    /// rendered exposition alongside the actions produced.
    fn evaluate_with_snapshot(
        snapshot: MarketSnapshot,
        obligations: HashMap<ObligationKey, Obligation>,
    ) -> (Vec<Action>, String) {
        let mut liquidator = liquidator();
        liquidator.market_data.insert(MARKET.to_string(), snapshot);
        liquidator.obligations.insert(MARKET.to_string(), obligations);

        let runtime =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");

        under_recorder(|| runtime.block_on(liquidator.evaulate_market(MARKET)))
    }

    #[test]
    fn stale_snapshot_is_not_used_for_liquidation_decisions() {
        let (actions, exposition) = evaluate_with_snapshot(
            snapshot(MAX_AGE + Duration::from_secs(30), &["POOL_A"], &[]),
            borrower(),
        );

        assert!(actions.is_empty(), "a stale snapshot must not produce liquidations");
        assert!(
            exposition.contains(
                "liquidator_scan_completed_total{market=\"MARKET_A\",outcome=\"stale_market_data\"\
                 } 1"
            ),
            "stale scan not recorded:\n{exposition}"
        );
        // `set_scan_counts` only runs once the obligation loop completes, so its
        // absence proves no obligation was judged against the stale prices.
        assert!(
            !exposition.contains("liquidator_obligations_total"),
            "obligations were consulted despite a stale snapshot:\n{exposition}"
        );
    }

    #[test]
    fn fresh_snapshot_is_evaluated() {
        let (actions, exposition) = evaluate_with_snapshot(
            snapshot(MAX_AGE - Duration::from_secs(30), &["POOL_A"], &[]),
            borrower(),
        );

        assert!(actions.is_empty(), "the borrower has no collateral, so nothing is liquidatable");
        assert!(
            exposition
                .contains("liquidator_scan_completed_total{market=\"MARKET_A\",outcome=\"ok\"} 1"),
            "fresh scan did not complete:\n{exposition}"
        );
        assert!(
            exposition.contains("liquidator_obligations_total{market=\"MARKET_A\"} 1"),
            "fresh scan did not judge the obligation:\n{exposition}"
        );
    }

    #[test]
    fn partial_snapshot_skips_only_the_obligations_it_cannot_price() {
        let (actions, exposition) = evaluate_with_snapshot(
            snapshot(Duration::ZERO, &["POOL_M"], &["POOL_A", "POOL_Z"]),
            borrower(),
        );

        assert!(actions.is_empty());
        assert!(
            exposition.contains("liquidator_skip_total{reason=\"unpriced_pool\"} 1"),
            "unpriced-pool skip not recorded:\n{exposition}"
        );
        // The market itself still scans — only the borrower touching POOL_A is
        // withheld, which is the whole point of degrading per pool.
        assert!(
            exposition
                .contains("liquidator_scan_completed_total{market=\"MARKET_A\",outcome=\"ok\"} 1"),
            "a partial snapshot must not abort the whole market scan:\n{exposition}"
        );
        assert!(
            exposition.contains(
                "liquidator_unpriced_pool_skips_total{market=\"MARKET_A\",pool_address=\"POOL_A\"\
                 } 1"
            ),
            "the skip was not attributed to the blocking pool:\n{exposition}"
        );
        // POOL_Z is unpriced too, but this borrower holds nothing in it — the
        // series must follow what actually blocked an obligation, not the
        // snapshot's list of gaps.
        assert!(
            !exposition.contains("pool_address=\"POOL_Z\""),
            "an untouched unpriced pool was attributed a skip:\n{exposition}"
        );
    }

    #[test]
    fn every_unpriced_pool_touched_gets_its_own_series() {
        let (_, exposition) = evaluate_with_snapshot(
            snapshot(Duration::ZERO, &["POOL_M"], &["POOL_A", "POOL_Z"]),
            two_pool_borrower(),
        );

        for pool in ["POOL_A", "POOL_Z"] {
            assert!(
                exposition.contains(&format!(
                    "liquidator_unpriced_pool_skips_total{{market=\"MARKET_A\",pool_address=\"\
                     {pool}\"}} 1"
                )),
                "{pool} blocked the obligation but got no series:\n{exposition}"
            );
        }
        // Two blocking pools, one withheld obligation: the aggregate skip
        // counter must stay per-obligation or the *Skip reasons* panel starts
        // over-reporting.
        assert!(
            exposition.contains("liquidator_skip_total{reason=\"unpriced_pool\"} 1"),
            "per-pool attribution leaked into the aggregate skip count:\n{exposition}"
        );
    }

    #[test]
    fn a_snapshot_exactly_at_the_budget_is_still_fresh() {
        assert!(snapshot(MAX_AGE, &["POOL_A"], &[]).age(Instant::now()) >= MAX_AGE);
        assert!(snapshot(Duration::ZERO, &["POOL_A"], &[]).age(Instant::now()) < MAX_AGE);
    }

    #[test]
    fn an_obligation_skipped_for_price_is_not_counted_as_checked() {
        let (_, exposition) =
            evaluate_with_snapshot(snapshot(Duration::ZERO, &["POOL_M"], &["POOL_A"]), borrower());

        // `checked` is the denominator `liquidatable` is read against: counting
        // an obligation nobody could price makes a blind scan look like a scan
        // that found everyone healthy.
        assert!(
            exposition.contains("liquidator_obligations_total{market=\"MARKET_A\"} 0"),
            "a skipped obligation was counted as judged:\n{exposition}"
        );
    }

    /// Two borrow pools valued just under `i128::MAX` plus one ordinary deposit
    /// pool. `compute_is_liquidatable` shrinks each debt term through a
    /// saturating bps scale before summing and answers `true`;
    /// `compute_is_insolvent` sums the same terms unscaled and overflows. That
    /// is the arithmetic route left open once the unpriced-pool guard closed the
    /// missing-pool one.
    fn market_data_overflowing_only_the_insolvency_check() -> MarketData {
        let unscaled_near_max = |address: &str| PoolData {
            token_decimals: 0,
            oracle_asset_price: 10_000,
            ..pool_data(address)
        };

        MarketData {
            pools_data: vec![
                unscaled_near_max("POOL_X"),
                unscaled_near_max("POOL_Y"),
                pool_data("POOL_C"),
            ],
            ..market_data(&[], &[])
        }
    }

    /// The overflowing borrower alongside an ordinary one the scan must still
    /// reach.
    fn overflow_and_ordinary_borrowers() -> HashMap<ObligationKey, Obligation> {
        let d_tokens = (i128::MAX / 10_000).into();

        HashMap::from([
            (
                ObligationKey::new("GOVERFLOW".to_string()),
                Obligation {
                    borrows: vec![
                        BorrowPosition { d_tokens, pool_address: "POOL_X".to_string() },
                        BorrowPosition { d_tokens, pool_address: "POOL_Y".to_string() },
                    ],
                    deposits: vec![DepositPosition {
                        j_tokens: 1_000.into(),
                        collateral: 0.into(),
                        pool_address: "POOL_C".to_string(),
                    }],
                },
            ),
            (
                ObligationKey::new("GORDINARY".to_string()),
                Obligation {
                    borrows: vec![BorrowPosition {
                        d_tokens: 1_000.into(),
                        pool_address: "POOL_C".to_string(),
                    }],
                    deposits: Vec::new(),
                },
            ),
        ])
    }

    #[test]
    fn an_obligation_whose_arithmetic_fails_does_not_abort_the_market_scan() {
        let (actions, exposition) = evaluate_with_snapshot(
            MarketSnapshot::new(market_data_overflowing_only_the_insolvency_check()),
            overflow_and_ordinary_borrowers(),
        );

        assert!(actions.is_empty());
        // Aborting skipped `record_scan`, so the last-successful-scan gauge froze
        // and KeeperScanStalled fired minutes later on a loop that was running.
        assert!(
            exposition
                .contains("liquidator_scan_completed_total{market=\"MARKET_A\",outcome=\"ok\"} 1"),
            "one obligation's arithmetic aborted the whole market scan:\n{exposition}"
        );
        assert!(
            exposition
                .contains("liquidator_last_successful_scan_timestamp_seconds{market=\"MARKET_A\"}"),
            "the liveness gauge was never advanced:\n{exposition}"
        );
        assert!(
            exposition.contains("liquidator_skip_total{reason=\"insolvency_check_failed\"} 1"),
            "the withheld obligation left no skip reason for the not-dispatched \
             alert:\n{exposition}"
        );
        // Both borrowers were judged, and the withheld one is still counted
        // liquidatable — it is, and that is what the not-dispatched alert reads.
        assert!(
            exposition.contains("liquidator_obligations_total{market=\"MARKET_A\"} 2"),
            "the scan stopped short of the obligations after the failure:\n{exposition}"
        );
        assert!(
            exposition.contains("liquidator_liquidatable_positions{market=\"MARKET_A\"} 1"),
            "the withheld opportunity vanished from the liquidatable count:\n{exposition}"
        );
    }

    #[test]
    fn a_pool_missing_from_the_snapshot_blocks_the_obligation() {
        // Nothing is flagged unavailable — POOL_A is simply absent from
        // `pools_data`, which is what the aggregate path produces when
        // `get_all_pools` omits a pool the borrower holds.
        let (actions, exposition) =
            evaluate_with_snapshot(snapshot(Duration::ZERO, &["POOL_M"], &[]), borrower());

        assert!(actions.is_empty());
        assert!(
            exposition.contains("liquidator_skip_total{reason=\"unpriced_pool\"} 1"),
            "an unpriced pool on no list at all was judged anyway:\n{exposition}"
        );
    }

    /// The junction of the two guards: the aggregate `get_market_data` failed
    /// **and** every per-pool read failed, so the fallback returns `Ok` with no
    /// prices and every pool flagged. Installing that would re-stamp freshness
    /// over an empty view and the age guard would never fire again.
    #[test]
    fn a_rebuild_that_prices_nothing_leaves_the_age_guard_armed() {
        let mut liquidator = liquidator();
        liquidator.ledger_reader = Arc::new(StubReader(market_data(&[], &["POOL_A"])));
        liquidator.market_data.insert(
            MARKET.to_string(),
            snapshot(MAX_AGE + Duration::from_secs(30), &["POOL_A"], &[]),
        );
        liquidator.obligations.insert(MARKET.to_string(), borrower());

        let runtime =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        let (actions, exposition) = under_recorder(|| {
            runtime.block_on(async {
                liquidator.refresh_market_data().await;

                liquidator.evaulate_market(MARKET).await
            })
        });

        assert!(actions.is_empty());
        assert!(
            exposition.contains(
                "liquidator_scan_completed_total{market=\"MARKET_A\",outcome=\"stale_market_data\"\
                 } 1"
            ),
            "a snapshot that priced nothing passed as fresh:\n{exposition}"
        );
    }

    #[test]
    fn a_partial_rebuild_is_visible_before_any_obligation_trips_over_it() {
        let mut liquidator = liquidator();
        liquidator.ledger_reader = Arc::new(StubReader(market_data(&["POOL_A"], &["POOL_B"])));

        let runtime =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        let (_, exposition) = under_recorder(|| runtime.block_on(liquidator.refresh_market_data()));

        // No obligation touches POOL_B, so the skip counters stay silent — this
        // gauge is the only evidence that decisions run off a partial rebuild.
        assert!(
            exposition.contains("liquidator_snapshot_unavailable_pools{market=\"MARKET_A\"} 1"),
            "a partial rebuild left no trace:\n{exposition}"
        );
    }
}
