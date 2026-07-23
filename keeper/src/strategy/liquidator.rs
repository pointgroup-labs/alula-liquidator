//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits liquidations.

use std::{collections::HashMap, collections::HashSet, sync::Arc};

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
    config::LiquidationKindArg,
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
    pub max_allowed_swap_slippage_bps: i128,
    /// Liquidation types the liquidator may build candidates for.
    pub enabled_liquidation_types: HashSet<LiquidationKindArg>,
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
    market_data: HashMap<String, MarketData>,
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
            for market in &self.config.markets {
                match self.ledger_reader.read_market_data(market).await {
                    Ok(md) => {
                        info!(market, ?md);
                        self.market_data.insert(market.clone(), md);
                    }
                    Err(e) => error!(?e, ?market, "get_market_data failed"),
                }

                let cached_obligations =
                    self.obligations_repo.load_all(market).unwrap_or_else(|e| {
                        warn!(?e, ?market, "load_all obligations failed");

                        HashMap::new()
                    });
                if cached_obligations.is_empty() {
                    info!(?market, "no cached obligations");
                }

                self.obligations.insert(market.clone(), cached_obligations);
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
                    && let Some(md) = self.market_data.get(market)
                {
                    emit_keeper_position_metrics(market, &obligation, md);
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
                    let own_key = ObligationKey::new(self.pkey.clone());
                    if let Some(obl) = self.obligations.get(&market).and_then(|m| m.get(&own_key)) {
                        emit_keeper_position_metrics(&market, obl, &market_data);
                    }

                    self.market_data.insert(market, market_data);
                }
                Err(e) => warn!(?e, %market, "refresh failed"),
            }
        }
    }

    async fn evaulate_market(&self, market: &str) -> Vec<Action> {
        let started = std::time::Instant::now();
        let Some(market_data) = self.market_data.get(market) else {
            metrics::record_scan(
                market,
                ScanOutcome::NoMarketData,
                started.elapsed().as_secs_f64(),
            );

            return vec![];
        };
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
            checked += 1;
            if obligation_key.user == self.pkey {
                // NB: self liquidations are not supported on the market currently
                continue;
            }

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
            let Ok(is_insolvent) = liquidation::compute_is_insolvent(obligation, market_data)
                .inspect_err(|e| error!(?e))
            else {
                return vec![];
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

        let available_borrow_balance = self
            .liquidator_capital
            .try_get_available_balance(borrow_token, &*self.ledger_reader)
            .await
            .inspect_err(|e| {
                warn!(?e, %borrow_token, "balance query failed");
                SkipReason::BalanceQueryFailed.record();
            })
            .unwrap_or(0);

        let usable_borrow = if borrow_token == self.config.xlm_address {
            available_borrow_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            available_borrow_balance
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
        if self.config.enabled_liquidation_types.contains(&LiquidationKindArg::Direct)
            && direct_repay.is_positive()
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

        if self.config.enabled_liquidation_types.contains(&LiquidationKindArg::Flash) {
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

                let (
                    instantly_available_plain_collateral,
                    instantly_available_withdrawable_deposit,
                ) = (
                    deposit_position.collateral.0,
                    all_liquidated_position_deposit
                        .min(available_to_withdraw_without_scarcity_fees),
                );
                let instantly_available_collateral =
                    instantly_available_plain_collateral + instantly_available_withdrawable_deposit;

                let updated_max_profitable_repay =
                    profitability::compute_repay_cap_from_collateral(
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
                    warn!(
                        updated_max_profitable_repay,
                        "non-positive updated max profitable repay"
                    );

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
        }

        // -- PRESWAP LIQUIDATION --

        if self.config.enabled_liquidation_types.contains(&LiquidationKindArg::Preswap)
            && let Some(plan) = self
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

            // Size against capital not already pledged to in-flight ops
            let Ok(available_balance) = self
                .liquidator_capital
                .try_get_available_balance(source_asset, &*self.ledger_reader)
                .await
                .inspect_err(|e| {
                    warn!(?e, %source_asset, "balance query failed");
                })
            else {
                continue;
            };
            let swappable_balance = if source_asset.as_ref() == self.config.xlm_address {
                available_balance.saturating_sub(self.config.xlm_safety_margin)
            } else {
                available_balance
            };
            if !swappable_balance.is_positive() {
                info!(
                    available_balance,
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
                        op_ids: vec![op_id],
                        liquidator_capital: self.liquidator_capital.clone(),
                        liquidation_outcome: Some(LiquidationOutcomeMetric {
                            market: market.to_string(),
                            expected_net_value: plan.net_profit_value,
                        }),
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
