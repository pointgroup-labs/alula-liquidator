//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits Direct or PreSwap
//! liquidations.

use {
    crate::{
        collect::{Event, stellar_ledger::NewLedger},
        execute::{
            Action,
            stellar_tx::{LiquidationOutcomeMetric, SettleHook, SubmitStellarTx},
        },
        stellar::Gateway,
        storage::{cursor::CursorRepo, obligations::ObligationsRepo},
        strategy::{CapitalLedger, capital::random_op_id},
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending_model::{
            DepositPosition, MarketData, Obligation, ObligationKey, PoolData, Underlying,
            liquidation, profitability,
        },
        ports::{BatchSimulator, EventCodec, LedgerReader, OperationBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    metrics::{counter, gauge, histogram},
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

const REFRESH_INTERVAL_BLOCKS: u32 = 12;
const MAX_RETRIES: u32 = 3;

pub struct LiquidatorConfig {
    pub markets: Vec<String>,
    pub min_profit_margin_cents: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    /// Haircut on `gain_oracle` (bps): out-leg slippage + oracle drift.
    pub gain_haircut_bps: i128,
    /// Absolute oracle-units allowance for the Stellar tx fee.
    pub inclusion_fee_oracle_units: i128,
    /// Enable flash-borrow based liquidations (off by default).
    pub flash_enabled: bool,
    /// Additional haircut (bps) on top of `gain_haircut_bps` for flash
    /// candidates only. Guards against the extra cost of the
    /// collateral → borrow swap that the repayment depends on.
    pub flash_safety_haircut_bps: i128,
}

#[derive(Debug, Clone)]
enum LiquidationType {
    Direct {
        repay_amount: i128,
    },
    PreSwap {
        repay_amount: i128,
        source_asset: String,
        source_amount_in: i128,
        min_source_out: i128,
        swap_provider: String,
    },
    /// Flash-borrow `repay_amount` of the borrow asset, seize collateral,
    /// swap seized collateral back to the borrow asset, auto-repay.
    /// The wallet never needs to hold the borrow asset.
    Flash {
        repay_amount: i128,
        /// Flash fee = ceil(repay_amount * flash_fee_bps / 10_000).
        flash_fee: i128,
        /// Underlying collateral token address (swap input).
        collateral_token: String,
        /// Amount of collateral underlying seized by the Liquidate request.
        seized_amount: i128,
        /// `min_amount_out` passed to SwapExactTokens.
        /// Must be ≥ repay_amount + flash_fee.
        min_swap_out: i128,
        swap_provider: String,
    },
}

#[derive(Debug)]
struct LiquidationPlan {
    liquidation_type: LiquidationType,
    borrower_key: ObligationKey,
    borrow_pool_address: String,
    collateral_pool_address: String,
    expected_seized_collateral: i128,
    /// Ranking key across (borrow, deposit) pairs. Computed in oracle units
    /// so it's comparable across pools with different decimals/prices.
    net_profit_oracle: i128,
}

pub struct Liquidator {
    chain: Arc<dyn LedgerReader>,
    gateway: Arc<Gateway>,
    skey: SigningKey,
    pkey: String,
    config: LiquidatorConfig,
    obligations_repo: ObligationsRepo,
    cursor_repo: CursorRepo,
    last_refresh_ledger: u32,
    market_data: HashMap<String, MarketData>,
    obligations: HashMap<String, HashMap<ObligationKey, Obligation>>,
    ledger: Arc<CapitalLedger>,
}

impl Liquidator {
    // 8/7 args triggers clippy::too_many_arguments. Each parameter is a
    // distinct collaborator (chain, gateway, signing key, public key,
    // config, two repos, ledger) with no natural sub-grouping. A
    // builder/config-struct refactor is tracked separately; not worth
    // forcing here just to silence the lint.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Arc<dyn LedgerReader>,
        gateway: Arc<Gateway>,
        skey: SigningKey,
        pkey: String,
        config: LiquidatorConfig,
        obligations_repo: ObligationsRepo,
        cursor_repo: CursorRepo,
        ledger: Arc<CapitalLedger>,
    ) -> Self {
        Self {
            chain,
            gateway,
            skey,
            pkey,
            config,
            obligations_repo,
            cursor_repo,
            last_refresh_ledger: 0,
            obligations: HashMap::new(),
            market_data: HashMap::new(),
            ledger,
        }
    }
}

impl Strategy<Event, Action> for Liquidator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewBlock(b) => self.handle_new_block(b).await,
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(?self.config.markets, "sync_state: loading market(s)");
            for market in &self.config.markets.clone() {
                match self.chain.read_market_data(market).await {
                    Ok(md) => {
                        info!(market, ?md);
                        self.market_data.insert(market.clone(), md);
                    }
                    Err(e) => error!(?e, ?market, "get_market_data failed"),
                }

                let cached = self.obligations_repo.load_all(market).unwrap_or_else(|e| {
                    warn!(?e, ?market, "load_all failed; falling back to RPC");
                    HashMap::new()
                });

                if cached.is_empty() {
                    info!(?market, "no cached obligations, fetching from RPC");
                    let obl_map = self.fetch_obligations_from_rpc(market).await?;
                    self.obligations.insert(market.clone(), obl_map);
                } else {
                    info!(count = cached.len(), "loaded obligations from DB");
                    self.obligations.insert(market.clone(), cached);
                }
            }
            info!("sync_state: done");
            Ok(())
        })
    }
}

impl Liquidator {
    async fn fetch_obligations_from_rpc(
        &self,
        market: &str,
    ) -> anyhow::Result<HashMap<ObligationKey, Obligation>> {
        let keys = self.chain.read_all_obligations_keys(market).await?;
        let total = keys.len();
        info!(total, "fetching obligations...");

        let mut obl_map = HashMap::new();
        for (i, key) in keys.into_iter().enumerate() {
            info!(?key, idx = i + 1, total, "fetching obligation");
            let obl = self
                .chain
                .read_user_obligation(market, &key)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "market={market}: failed to fetch obligation user={}: {e:#}",
                        key.user,
                    )
                })?;
            debug!(?obl);
            if let Err(e) = self.obligations_repo.put(market, &key, &obl) {
                warn!(?e, "failed to save obligation to DB");
            }
            obl_map.insert(key, obl);
        }
        Ok(obl_map)
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();

        let Ok(op_event) = self.gateway.decode_operation(&event) else {
            return vec![];
        };

        let name = match &op_event {
            OperationEvent::Repay => "repay",
            OperationEvent::Borrow => "borrow",
            OperationEvent::Deposit => "deposit",
            OperationEvent::Withdraw => "withdraw",
            OperationEvent::Liquidate => "liquidate",
            OperationEvent::AddCollateral => "addcollateral",
            OperationEvent::RemoveCollateral => "removecollateral",
        };
        match op_event {
            OperationEvent::Deposit
            | OperationEvent::Borrow
            | OperationEvent::AddCollateral
            | OperationEvent::Repay
            | OperationEvent::Withdraw
            | OperationEvent::RemoveCollateral => {
                let pool = self.gateway.decode_topic(&event, 1);
                let obl_display = self.gateway.decode_topic(&event, 2);
                debug!(%name, ledger = event.ledger, %market, %pool, %obl_display, "position event");
                let Ok(key) = self.gateway.parse_obligation_key_from_topic(&event, 2) else {
                    warn!(%name, "cannot parse obligation key");
                    return vec![];
                };
                self.apply_obligation_snapshot(&name, &market, &event.value, "obligation", &key);
            }
            OperationEvent::Liquidate => {
                let liquidator = self.gateway.decode_topic(&event, 1);
                let borrower = self.gateway.decode_topic(&event, 2);
                let borrow_pool = self.gateway.decode_topic(&event, 3);
                let collateral_pool = self.gateway.decode_topic(&event, 4);
                info!(ledger = event.ledger, %market, %liquidator, %borrower, %borrow_pool, %collateral_pool, "liquidation event");

                let Ok(borrower_key) = self.gateway.parse_obligation_key_from_topic(&event, 2)
                else {
                    warn!("cannot parse borrower obligation key");
                    return vec![];
                };
                self.apply_obligation_snapshot(
                    &name,
                    &market,
                    &event.value,
                    "borrower_obligation",
                    &borrower_key,
                );

                let liquidator_key = ObligationKey {
                    user: liquidator,
                    seed: None,
                };
                self.apply_obligation_snapshot(
                    &name,
                    &market,
                    &event.value,
                    "liquidator_obligation",
                    &liquidator_key,
                );
            }
        }

        if let Err(e) = self.cursor_repo.set(&event.id, event.ledger) {
            warn!(?e, id = %event.id, "failed to save cursor");
            counter!(
                "keeper_cursor_save_failures_total",
                "source" => "liquidator_event_cursor",
            )
            .increment(1);
        }

        vec![]
    }

    fn apply_obligation_snapshot(
        &mut self,
        event_name: &str,
        market: &str,
        value_xdr_base64: &str,
        field_name: &str,
        key: &ObligationKey,
    ) {
        match self
            .gateway
            .parse_obligation_from_event_value(value_xdr_base64, field_name, key)
        {
            Ok(Some(obl)) => {
                debug!(?key, ?obl, %event_name, %market, "obligation snapshot");
                if let Err(e) = self.obligations_repo.put(market, key, &obl) {
                    warn!(?e, %event_name, %market, "failed to save obligation");
                }
                self.obligations
                    .entry(market.to_string())
                    .or_default()
                    .insert(key.clone(), obl.clone());

                if key.user == self.pkey
                    && let Some(md) = self.market_data.get(market)
                {
                    emit_self_position_metrics(market, &obl, md);
                }
            }
            Ok(None) => {
                info!(?key, %event_name, %market, "obligation deleted");
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

    async fn handle_new_block(&mut self, block: NewLedger) -> Vec<Action> {
        let ledger = block.seq_num;
        if ledger.saturating_sub(self.last_refresh_ledger) < REFRESH_INTERVAL_BLOCKS {
            return vec![];
        }
        self.last_refresh_ledger = ledger;

        info!(ledger, "refresh + evaluate");
        self.refresh_market_data().await;

        let mut actions = Vec::new();
        for market in self.config.markets.clone() {
            actions.extend(self.evaluate_market(&market).await);
        }
        if !actions.is_empty() {
            info!(ledger, count = actions.len(), "submitting liquidations");
        }
        actions
    }

    async fn refresh_market_data(&mut self) {
        for market in self.config.markets.clone() {
            match self.chain.read_market_data(&market).await {
                Ok(md) => {
                    let prices: Vec<String> = md
                        .pools_data
                        .iter()
                        .map(|p| format!("{}={}", p.token_symbol, p.oracle_asset_price))
                        .collect();
                    debug!(%market, ?prices, "refreshed prices");
                    self.market_data.insert(market.clone(), md);

                    let own_key = ObligationKey::new(self.pkey.clone());
                    if let (Some(obl), Some(md)) = (
                        self.obligations.get(&market).and_then(|m| m.get(&own_key)),
                        self.market_data.get(&market),
                    ) {
                        emit_self_position_metrics(&market, obl, md);
                    }
                }
                Err(e) => warn!(?e, %market, "refresh failed"),
            }
        }
    }
}

impl Liquidator {
    async fn evaluate_market(&self, market: &str) -> Vec<Action> {
        let started = std::time::Instant::now();
        let Some(market_data) = self.market_data.get(market) else {
            counter!(
                "liquidator_scan_completed_total",
                "market" => market.to_string(),
                "outcome" => "no_market_data",
            )
            .increment(1);
            histogram!(
                "liquidator_market_scan_duration_seconds",
                "market" => market.to_string(),
                "outcome" => "no_market_data",
            )
            .record(started.elapsed().as_secs_f64());
            return vec![];
        };
        let Some(obligations) = self.obligations.get(market).filter(|m| !m.is_empty()) else {
            counter!(
                "liquidator_scan_completed_total",
                "market" => market.to_string(),
                "outcome" => "no_obligations",
            )
            .increment(1);
            histogram!(
                "liquidator_market_scan_duration_seconds",
                "market" => market.to_string(),
                "outcome" => "no_obligations",
            )
            .record(started.elapsed().as_secs_f64());
            return vec![];
        };

        let mut actions = Vec::new();
        let mut checked = 0u64;
        let mut liquidatable_count = 0u64;
        for (obl_key, obligation) in obligations {
            if obl_key.user == self.pkey {
                continue;
            }
            checked += 1;
            if !liquidation::compute_is_liquidatable(obligation, market_data) {
                continue;
            }
            liquidatable_count += 1;
            let is_insolvent = liquidation::compute_is_insolvent(obligation, market_data);
            debug!(?obl_key, ?obligation, is_insolvent, "locally liquidatable");

            if let Some(action) = self
                .try_liquidate(market, market_data, obl_key, obligation, is_insolvent)
                .await
            {
                info!(?action, "liquidation");
                actions.push(action);
            }
        }

        gauge!("liquidator_obligations_total", "market" => market.to_string()).set(checked as f64);
        gauge!("liquidator_liquidatable_positions", "market" => market.to_string())
            .set(liquidatable_count as f64);
        counter!(
            "liquidator_scan_completed_total",
            "market" => market.to_string(),
            "outcome" => "ok",
        )
        .increment(1);
        histogram!(
            "liquidator_market_scan_duration_seconds",
            "market" => market.to_string(),
            "outcome" => "ok",
        )
        .record(started.elapsed().as_secs_f64());
        // Liveness gauge. Pair with `time() - …` alerts to detect stalled scans.
        if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            gauge!(
                "liquidator_last_successful_scan_timestamp_seconds",
                "market" => market.to_string(),
            )
            .set(now.as_secs() as f64);
        }
        info!(%market, checked, liquidatable = liquidatable_count, "market evaluation complete");
        actions
    }

    async fn try_liquidate(
        &self,
        market: &str,
        market_data: &MarketData,
        borrower_obl_key: &ObligationKey,
        obligation: &Obligation,
        is_insolvent: bool,
    ) -> Option<Action> {
        let liquidator_obl_key = ObligationKey::new(self.pkey.clone());

        // Score every (borrow, deposit) pair and keep the highest-profit plan.
        // Picking the first viable pair (the old behaviour) could shadow a
        // strictly better one later in the iteration order.
        let mut best: Option<LiquidationPlan> = None;
        for borrow_pos in &obligation.borrows {
            let Some(borrow_pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == borrow_pos.pool_address)
            else {
                error!(?borrow_pos, "borrow pool missing from market data");
                continue;
            };

            for deposit_pos in &obligation.deposits {
                if deposit_pos.pool_address == borrow_pos.pool_address {
                    error!(?borrow_pos, ?deposit_pos, "same pool borrow/deposit");
                    continue;
                }
                if deposit_pos.j_tokens.0 <= 0 && deposit_pos.collateral.0 <= 0 {
                    error!(?deposit_pos, "empty deposit, skipping");
                    continue;
                }

                let Some(collateral_pool) = market_data
                    .pools_data
                    .iter()
                    .find(|p| p.pool_address == deposit_pos.pool_address)
                else {
                    error!(?deposit_pos, "collateral pool missing from market data");
                    continue;
                };

                let Some(plan) = self
                    .build_liquidation_plan(
                        market_data,
                        obligation,
                        borrower_obl_key,
                        borrow_pos.d_tokens.0,
                        borrow_pool,
                        collateral_pool,
                        deposit_pos,
                        is_insolvent,
                    )
                    .await
                else {
                    continue;
                };

                if best
                    .as_ref()
                    .is_none_or(|b| plan.net_profit_oracle > b.net_profit_oracle)
                {
                    best = Some(plan);
                }
            }
        }

        let plan = best?;
        // Expected profitability of the dispatched plan. Net oracle units
        // are non-negative here by construction (gated upstream); the
        // `.max(0)` is a defensive belt-and-braces against future invariant
        // drift. Oracle units divide by 10^7 for USD at standard SAC
        // 7-decimal scaling (`cents * 10^oracle_decimals / 100`).
        let net_oracle = plan.net_profit_oracle.max(0);
        histogram!(
            "liquidator_plan_expected_net_profit_oracle_units",
            "market" => market.to_string(),
        )
        .record(net_oracle as f64);
        counter!(
            "liquidator_plan_expected_net_profit_oracle_units_total",
            "market" => market.to_string(),
        )
        .increment(net_oracle as u64);
        info!(
            ?plan.borrower_key,
            borrow_pool = %plan.borrow_pool_address,
            collateral_pool = %plan.collateral_pool_address,
            net = plan.net_profit_oracle,
            "selected best (borrow, deposit) pair",
        );
        self.execute_liquidation_plan(market, &liquidator_obl_key, plan)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_liquidation_plan(
        &self,
        market_data: &MarketData,
        obligation: &Obligation,
        borrower_key: &ObligationKey,
        borrow_d_tokens: i128,
        borrow_pool: &PoolData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
        is_insolvent: bool,
    ) -> Option<LiquidationPlan> {
        let borrow_token = borrow_pool.token_address.as_str();

        let position_debt_tokens = borrow_pool
            .d_tokens_to_tokens_ceil(borrow_d_tokens.into())
            .ok()?
            .0;
        if position_debt_tokens <= 0 {
            error!(?borrower_key, "empty borrow position");
            return None;
        }

        let close_factor_cap = liquidation::compute_close_factor_repay_cap(
            position_debt_tokens,
            borrow_pool.liquidation_close_factor_bps,
            is_insolvent,
        );

        let position_collateral_sum = collateral_pool
            .j_tokens_to_tokens_floor(deposit_pos.j_tokens)
            .ok()?
            .0
            + deposit_pos.collateral.0;

        let position_collateral_value = position_collateral_sum
            .saturating_mul(collateral_pool.oracle_asset_price)
            .saturating_div(10_i128.pow(collateral_pool.token_decimals));
        let min_collateral_threshold = market_data
            .min_collateral_value_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);
        if position_collateral_sum <= 0 || position_collateral_value < min_collateral_threshold {
            warn!(
                position_collateral_sum,
                position_collateral_value,
                min_collateral_threshold,
                "position below minimum collateral threshold, skipping"
            );
            counter!("liquidator_skip_total", "reason" => "below_collateral_threshold")
                .increment(1);
            return None;
        }

        let profit_margin_borrow = profitability::compute_profit_margin_in_borrow_token(
            self.config.min_profit_margin_cents,
            market_data.oracle_price_decimals,
            borrow_pool,
        )
        .map(|u| u.0)
        .unwrap_or(0);

        let max_feasible_repay = position_debt_tokens.min(close_factor_cap);
        if max_feasible_repay <= 0 {
            error!(max_feasible_repay, "max_feasible_repay non-positive");
            return None;
        }

        // i128-only arithmetic via engine::lending_model::profitability so the
        // engine-level tests cover it.
        let profitable_repay = profitability::compute_repay_cap_from_collateral(
            max_feasible_repay,
            position_collateral_sum,
            borrow_pool,
            collateral_pool,
            profit_margin_borrow,
        )?;

        let raw_borrow_balance = match self
            .ledger
            .cached_balance(borrow_token, &self.pkey, &*self.chain)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(?e, %borrow_token, "balance query failed");
                counter!("liquidator_skip_total", "reason" => "balance_query_failed").increment(1);
                return None;
            }
        };
        let usable_borrow = if borrow_token == self.config.xlm_address {
            raw_borrow_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_borrow_balance
        };

        if usable_borrow >= profitable_repay {
            let expected_seized = self.compute_seized(
                profitable_repay,
                borrow_pool,
                collateral_pool,
                deposit_pos,
                obligation,
                market_data,
                is_insolvent,
            )?;

            // Same oracle-units accounting as the PreSwap branch, so the
            // ranking key in `try_liquidate` is consistent across branches.
            let cost_oracle = profitable_repay
                .saturating_mul(borrow_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(borrow_pool.token_decimals));
            let gain_oracle = expected_seized
                .saturating_mul(collateral_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(collateral_pool.token_decimals));
            let profit_margin_oracle = self
                .config
                .min_profit_margin_cents
                .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
                .saturating_div(100);
            let check = profitability::compute_liquidation_profitability(
                gain_oracle,
                cost_oracle,
                profit_margin_oracle,
                self.config.gain_haircut_bps,
                self.config.inclusion_fee_oracle_units,
            )
            .ok()?;
            if !check.is_profitable {
                debug!(
                    net = check.net_value,
                    "direct branch fails profitability gate",
                );
                return None;
            }

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                profitable_repay, expected_seized, net = check.net_value,
                "DIRECT liquidation plan"
            );

            return Some(LiquidationPlan {
                liquidation_type: LiquidationType::Direct {
                    repay_amount: profitable_repay,
                },
                borrower_key: borrower_key.clone(),
                borrow_pool_address: borrow_pool.pool_address.clone(),
                collateral_pool_address: collateral_pool.pool_address.clone(),
                expected_seized_collateral: expected_seized,
                net_profit_oracle: check.net_value,
            });
        }

        self.try_flash_plan(
            market_data,
            obligation,
            borrower_key,
            borrow_pool,
            collateral_pool,
            deposit_pos,
            profitable_repay,
            is_insolvent,
        )
        .await
        .or(
            // Flash disabled or no viable flash path — fall back to pre-swap.
            self.try_preswap_plan(
                market_data,
                obligation,
                borrower_key,
                borrow_pool,
                collateral_pool,
                deposit_pos,
                profitable_repay,
                is_insolvent,
            )
            .await,
        )
    }

    /// Attempt to build a flash-borrow liquidation plan.
    ///
    /// Batch shape:
    ///   `[FlashBorrow(borrow_pool, R),
    ///     Liquidate(borrower, borrow_pool, collateral_pool, R, S_min),
    ///     SwapExactTokens(C → B, seized_amount, min_swap_out)]`
    ///
    /// The protocol's end-of-batch `execute_transfers` automatically pulls
    /// `R + fee` from the keeper's wallet to repay the flash borrow — so the
    /// swap output *must* land in the wallet before that happens. Since
    /// `SwapExactTokens` flushes+settles immediately and the flash repayment
    /// is last, this ordering is safe.
    ///
    /// Returns `None` when:
    /// - flash is disabled in config,
    /// - the borrow pool has no flash liquidity,
    /// - no swap venue can deliver ≥ `R + fee + safety` for the seized C,
    /// - the plan fails the profitability gate.
    #[allow(clippy::too_many_arguments)]
    async fn try_flash_plan(
        &self,
        market_data: &MarketData,
        obligation: &Obligation,
        borrower_key: &ObligationKey,
        borrow_pool: &PoolData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
        profitable_repay: i128,
        is_insolvent: bool,
    ) -> Option<LiquidationPlan> {
        if !self.config.flash_enabled {
            return None;
        }

        // Preflight: the borrow pool must have enough liquidity to fund the
        // flash borrow. `total_available` is already cached in PoolData.
        if borrow_pool.total_available.0 < profitable_repay {
            counter!(
                "liquidator_skip_total",
                "reason" => "flash_pool_insufficient_liquidity"
            )
            .increment(1);
            return None;
        }

        // Compute the flash fee (ceiling arithmetic, mirrors the on-chain
        // `compute_flash_fee` in `request.rs::execute_transfers`).
        let flash_fee = profitability::compute_flash_fee(
            Underlying(profitable_repay),
            borrow_pool.flash_loan_fee_bps,
        )
        .ok()?
        .0;

        // Seized collateral: same helper used by Direct / PreSwap branches.
        let seized_amount = self.compute_seized(
            profitable_repay,
            borrow_pool,
            collateral_pool,
            deposit_pos,
            obligation,
            market_data,
            is_insolvent,
        )?;

        // The swap leg is: collateral_underlying → borrow_token.
        // `min_swap_out` must cover the full repayment (principal + fee).
        // We add the `flash_safety_haircut` to require a comfortable margin
        // above the bare minimum so we don't dispatch batches that will
        // revert on sub-1-bps slippage.
        let repay_total = profitable_repay.saturating_add(flash_fee);
        let safety_cushion = repay_total
            .saturating_mul(self.config.flash_safety_haircut_bps)
            .saturating_add(9_999) // ceiling
            .saturating_div(10_000);
        let min_swap_out = repay_total.saturating_add(safety_cushion);

        let collateral_token = collateral_pool.token_address.clone();
        let borrow_token = borrow_pool.token_address.as_str();
        let swap_path = [collateral_token.as_str(), borrow_token];

        // Ask each swap provider for a quote on seized_amount of collateral.
        let (best_provider, quoted_out) =
            match self.best_swap_quote(seized_amount, &swap_path).await {
                Some(q) => q,
                None => {
                    counter!(
                        "liquidator_skip_total",
                        "reason" => "flash_swap_shortfall"
                    )
                    .increment(1);
                    return None;
                }
            };

        if quoted_out < min_swap_out {
            debug!(
                quoted_out,
                min_swap_out,
                seized_amount,
                "flash swap quote below min_swap_out threshold"
            );
            counter!(
                "liquidator_skip_total",
                "reason" => "flash_swap_shortfall"
            )
            .increment(1);
            return None;
        }

        // Profitability gate. Cost = repay_total (principal + flash fee) in
        // oracle units. Gain = seized collateral in oracle units.
        // The combined haircut is the base haircut + flash safety haircut.
        let combined_haircut_bps = self
            .config
            .gain_haircut_bps
            .saturating_add(self.config.flash_safety_haircut_bps);

        let cost_oracle = repay_total
            .saturating_mul(borrow_pool.oracle_asset_price)
            .saturating_div(10_i128.pow(borrow_pool.token_decimals));
        let gain_oracle = seized_amount
            .saturating_mul(collateral_pool.oracle_asset_price)
            .saturating_div(10_i128.pow(collateral_pool.token_decimals));
        let profit_margin_oracle = self
            .config
            .min_profit_margin_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);

        let check = profitability::compute_liquidation_profitability(
            gain_oracle,
            cost_oracle,
            profit_margin_oracle,
            combined_haircut_bps,
            self.config.inclusion_fee_oracle_units,
        )
        .ok()?;

        if !check.is_profitable {
            debug!(
                net = check.net_value,
                flash_fee,
                "flash branch fails profitability gate"
            );
            return None;
        }

        info!(
            ?borrower_key,
            borrow_pool = %borrow_pool.pool_address,
            collateral_pool = %collateral_pool.pool_address,
            profitable_repay,
            flash_fee,
            seized_amount,
            min_swap_out,
            net = check.net_value,
            "FLASH liquidation plan"
        );

        Some(LiquidationPlan {
            liquidation_type: LiquidationType::Flash {
                repay_amount: profitable_repay,
                flash_fee,
                collateral_token,
                seized_amount,
                min_swap_out,
                swap_provider: best_provider,
            },
            borrower_key: borrower_key.clone(),
            borrow_pool_address: borrow_pool.pool_address.clone(),
            collateral_pool_address: collateral_pool.pool_address.clone(),
            expected_seized_collateral: seized_amount,
            net_profit_oracle: check.net_value,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_preswap_plan(
        &self,
        market_data: &MarketData,
        obligation: &Obligation,
        borrower_key: &ObligationKey,
        borrow_pool: &PoolData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
        profitable_repay: i128,
        is_insolvent: bool,
    ) -> Option<LiquidationPlan> {
        let borrow_token = borrow_pool.token_address.as_str();

        for source_asset in &self.config.assets_to_hold {
            if source_asset == borrow_token {
                continue;
            }

            let raw_balance = match self
                .ledger
                .cached_balance(source_asset, &self.pkey, &*self.chain)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(?e, %source_asset, "balance query failed");
                    continue;
                }
            };
            let usable_balance = if source_asset.as_str() == self.config.xlm_address {
                raw_balance.saturating_sub(self.config.xlm_safety_margin)
            } else {
                raw_balance
            };
            if usable_balance <= 0 {
                continue;
            }

            let path = &[source_asset.as_str(), borrow_token];
            let (best_provider, quoted_out) = match self.best_swap_quote(usable_balance, path).await
            {
                Some(q) => q,
                None => {
                    debug!(%source_asset, "no swap quote");
                    continue;
                }
            };
            if quoted_out <= 0 {
                continue;
            }

            // ceil(profitable_repay * usable_balance / quoted_out), overflow-safe
            let needed_source = profitable_repay
                .saturating_mul(usable_balance)
                .saturating_add(quoted_out - 1)
                .saturating_div(quoted_out);
            if needed_source > usable_balance {
                debug!(%source_asset, needed_source, usable_balance, "insufficient balance");
                continue;
            }

            let quoted_out_for_needed = match self
                .chain
                .get_amount_out(needed_source, path[0], path[1], &best_provider)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    warn!(?e, %source_asset, "re-quote failed");
                    continue;
                }
            };

            let min_source_out = profitable_repay;
            if quoted_out_for_needed < min_source_out {
                debug!(%source_asset, quoted_out_for_needed, min_source_out, "below min_source_out");
                continue;
            }

            let expected_seized = self.compute_seized(
                profitable_repay,
                borrow_pool,
                collateral_pool,
                deposit_pos,
                obligation,
                market_data,
                is_insolvent,
            )?;

            let Some(source_pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.token_address == *source_asset)
            else {
                debug!(%source_asset, "source asset pool not in market data");
                continue;
            };

            let cost_oracle = needed_source
                .saturating_mul(source_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(source_pool.token_decimals));
            let gain_oracle = expected_seized
                .saturating_mul(collateral_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(collateral_pool.token_decimals));

            let profit_margin_oracle = self
                .config
                .min_profit_margin_cents
                .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
                .saturating_div(100);

            let check = match profitability::compute_liquidation_profitability(
                gain_oracle,
                cost_oracle,
                profit_margin_oracle,
                self.config.gain_haircut_bps,
                self.config.inclusion_fee_oracle_units,
            ) {
                Ok(c) => c,
                Err(_) => continue,
            };

            if !check.is_profitable {
                debug!(
                    %source_asset,
                    effective_gain = check.effective_gain_value,
                    required = check.required_value,
                    gain_haircut_bps = self.config.gain_haircut_bps,
                    inclusion_fee_oracle_units = self.config.inclusion_fee_oracle_units,
                    "pre-swap not profitable"
                );
                continue;
            }

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                %source_asset, needed_source, min_source_out, profitable_repay, expected_seized,
                "PRE-SWAP liquidation plan"
            );

            return Some(LiquidationPlan {
                liquidation_type: LiquidationType::PreSwap {
                    repay_amount: profitable_repay,
                    source_asset: source_asset.clone(),
                    source_amount_in: needed_source,
                    min_source_out,
                    swap_provider: best_provider,
                },
                borrower_key: borrower_key.clone(),
                borrow_pool_address: borrow_pool.pool_address.clone(),
                collateral_pool_address: collateral_pool.pool_address.clone(),
                expected_seized_collateral: expected_seized,
                net_profit_oracle: check.net_value,
            });
        }

        debug!("no viable pre-swap strategy found");
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_seized(
        &self,
        repay_amount: i128,
        borrow_pool: &PoolData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
        obligation: &Obligation,
        market_data: &MarketData,
        is_insolvent: bool,
    ) -> Option<i128> {
        let obligation_debt_value =
            liquidation::compute_obligation_debt_value(obligation, market_data).ok()?;
        let obligation_collateral_value =
            liquidation::compute_obligation_collateral_value(obligation, market_data).ok()?;
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
        );
        if seized <= 0 {
            warn!(repay_amount, "expected seized collateral is zero");
            counter!("liquidator_skip_total", "reason" => "unprofitable_seize_zero").increment(1);
            None
        } else {
            Some(seized)
        }
    }

    async fn best_swap_quote(&self, amount_in: i128, path: &[&str]) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .chain
                .get_amount_out(amount_in, path[0], path[1], provider)
                .await
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
}

impl Liquidator {
    fn build_batch_requests(
        &self,
        plan: &LiquidationPlan,
    ) -> Option<Vec<<Gateway as OperationBuilder>::Request>> {
        let mut requests = Vec::new();

        match &plan.liquidation_type {
            LiquidationType::Flash {
                repay_amount,
                collateral_token,
                seized_amount,
                min_swap_out,
                swap_provider,
                ..
            } => {
                // 1. Flash-borrow the borrow asset.
                let flash_req = match self
                    .gateway
                    .flash_borrow_request(&plan.borrow_pool_address, *repay_amount)
                {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build flash_borrow request");
                        return None;
                    }
                };
                requests.push(flash_req);

                // 2. Liquidate — pays repay_amount of borrow asset, receives
                //    seized_amount of collateral underlying.
                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    *repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build liquidate request (flash path)");
                        return None;
                    }
                };
                requests.push(liquidate_req);

                // 3. Swap seized collateral → borrow asset. The swap output
                //    must be ≥ min_swap_out so the end-of-batch flash repayment
                //    can be satisfied. If the swap delivers less, the whole
                //    batch reverts atomically.
                let borrow_token = self.find_token_for_pool(&plan.borrow_pool_address)?;
                let swap_path = [collateral_token.as_str(), borrow_token.as_str()];
                let swap_req = match self.gateway.swap_exact_tokens_request(
                    swap_provider,
                    *seized_amount,
                    *min_swap_out,
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
                min_source_out,
                swap_provider,
                repay_amount,
                ..
            } => {
                let borrow_token_address = self.find_token_for_pool(&plan.borrow_pool_address)?;
                let preswap_path = [source_asset.as_str(), borrow_token_address.as_str()];

                let preswap_req = match self.gateway.swap_exact_tokens_request(
                    swap_provider,
                    *source_amount_in,
                    *min_source_out,
                    &preswap_path,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build pre-swap request");
                        return None;
                    }
                };
                requests.push(preswap_req);

                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    *repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build liquidate request");
                        return None;
                    }
                };
                requests.push(liquidate_req);
            }

            LiquidationType::Direct { repay_amount } => {
                let liquidate_req = match self.gateway.liquidate_request(
                    &plan.borrower_key,
                    &plan.borrow_pool_address,
                    &plan.collateral_pool_address,
                    *repay_amount,
                    plan.expected_seized_collateral,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(?e, "failed to build liquidate request");
                        return None;
                    }
                };
                requests.push(liquidate_req);
            }
        }

        Some(requests)
    }

    fn find_token_for_pool(&self, pool_address: &str) -> Option<String> {
        for md in self.market_data.values() {
            if let Some(pool) = md
                .pools_data
                .iter()
                .find(|p| p.pool_address == pool_address)
            {
                return Some(pool.token_address.clone());
            }
        }
        None
    }

    async fn execute_liquidation_plan(
        &self,
        market: &str,
        liquidator_obl_key: &ObligationKey,
        plan: LiquidationPlan,
    ) -> Option<Action> {
        let requests = self.build_batch_requests(&plan)?;

        match self
            .gateway
            .simulate_batch(market, liquidator_obl_key, &requests)
            .await
        {
            Ok(true) => {
                let flash_fee_log = if let LiquidationType::Flash { flash_fee, .. } =
                    &plan.liquidation_type
                {
                    *flash_fee
                } else {
                    0
                };
                info!(
                    ?plan.borrower_key,
                    flash_fee = flash_fee_log,
                    "batch simulation OK"
                );
            }
            Ok(false) => {
                warn!(?plan.borrower_key, "batch simulation failed, dropping plan");
                counter!("liquidator_skip_total", "reason" => "batch_sim_failed").increment(1);
                return None;
            }
            Err(e) => {
                warn!(?e, ?plan.borrower_key, "batch simulation error");
                counter!("liquidator_skip_total", "reason" => "batch_sim_failed").increment(1);
                return None;
            }
        }

        // Reserve in-flight capital so other opportunities (and the balancer)
        // don't double-spend the wallet balance. The reservation is released by
        // the executor's SettleHook on every terminal tx outcome; the 5-minute
        // ledger TTL is now only a safety ceiling for hooks lost to executor
        // task panics.
        let (reserve_token, reserve_amount) = match &plan.liquidation_type {
            LiquidationType::Direct { repay_amount } => {
                let borrow_token = self.find_token_for_pool(&plan.borrow_pool_address)?;
                (borrow_token, *repay_amount)
            }
            LiquidationType::PreSwap {
                source_asset,
                source_amount_in,
                ..
            } => (source_asset.clone(), *source_amount_in),
            // Flash liquidations source their capital from the protocol — the
            // keeper's wallet is not at risk for the repay amount, so there is
            // nothing to reserve.
            LiquidationType::Flash { .. } => {
                let borrow_token = self.find_token_for_pool(&plan.borrow_pool_address)?;
                (borrow_token, 0)
            }
        };

        // Direct / PreSwap spend wallet capital and must pass the reservation
        // gate. Flash spends protocol capital, so it mints a standalone op_id
        // for the SettleHook (releasing an unreserved id is a no-op).
        let op_id = if reserve_amount > 0 {
            let raw_balance = match self
                .ledger
                .cached_balance(&reserve_token, &self.pkey, &*self.chain)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(?e, %reserve_token, "balance query failed at reservation time");
                    counter!("liquidator_skip_total", "reason" => "balance_query_failed")
                        .increment(1);
                    return None;
                }
            };
            let available = if reserve_token == self.config.xlm_address {
                raw_balance.saturating_sub(self.config.xlm_safety_margin)
            } else {
                raw_balance
            };
            match self
                .ledger
                .reserve(&reserve_token, &self.pkey, reserve_amount, available)
            {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        ?e,
                        repay_amount = reserve_amount,
                        %reserve_token,
                        "skipping liquidation: insufficient available balance after pending reservations"
                    );
                    counter!(
                        "liquidator_skip_total",
                        "reason" => "insufficient_balance_after_reservations",
                    )
                    .increment(1);
                    return None;
                }
            }
        } else {
            random_op_id()
        };

        match self.gateway.batch_op(market, liquidator_obl_key, &requests) {
            Ok(op) => {
                // Counts plans handed to the executor — NOT confirmed liquidations.
                // The tx may still fail at simulate, bad_seq retry, or confirmation
                // poll; those outcomes are tracked separately by the executor's
                // own counters.
                let liq_type_label = match &plan.liquidation_type {
                    LiquidationType::Direct { .. } => "direct",
                    LiquidationType::PreSwap { .. } => "preswap",
                    LiquidationType::Flash { .. } => "flash",
                };
                counter!(
                    "liquidator_liquidation_plans_dispatched_total",
                    "market" => market.to_string(),
                    "type" => liq_type_label,
                )
                .increment(1);
                Some(Action::SubmitTx(SubmitStellarTx {
                    op,
                    signing_key: self.skey.clone(),
                    max_retries: MAX_RETRIES,
                    on_settle: Some(SettleHook {
                        ledger: self.ledger.clone(),
                        op_id,
                        liquidation_outcome: Some(LiquidationOutcomeMetric {
                            market: market.to_string(),
                            expected_net_oracle: plan.net_profit_oracle,
                        }),
                    }),
                }))
            }
            Err(e) => {
                error!(?e, ?plan.borrower_key, "failed to build batch op");
                counter!("liquidator_skip_total", "reason" => "op_build_failed").increment(1);
                self.ledger.release(op_id);
                None
            }
        }
    }
}

fn emit_self_position_metrics(market: &str, obligation: &Obligation, market_data: &MarketData) {
    for deposit in &obligation.deposits {
        let Some(pool) = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == deposit.pool_address)
        else {
            warn!(%market, pool = %deposit.pool_address, "emit_self_position_metrics: pool not found");
            continue;
        };
        let labels = [
            ("market", market.to_string()),
            ("pool_address", pool.pool_address.clone()),
            ("token_symbol", pool.token_symbol.clone()),
        ];
        gauge!("liquidator_self_j_tokens", &labels).set(deposit.j_tokens.0 as f64);
        gauge!("liquidator_self_plain_collateral", &labels).set(deposit.collateral.0 as f64);
        gauge!("liquidator_self_j_tokens_underlying", &labels).set(
            pool.j_tokens_to_tokens_floor(deposit.j_tokens)
                .map(|u| u.0)
                .unwrap_or(0) as f64,
        );
    }
    for borrow in &obligation.borrows {
        let Some(pool) = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == borrow.pool_address)
        else {
            warn!(%market, pool = %borrow.pool_address, "emit_self_position_metrics: pool not found");
            continue;
        };
        let labels = [
            ("market", market.to_string()),
            ("pool_address", pool.pool_address.clone()),
            ("token_symbol", pool.token_symbol.clone()),
        ];
        gauge!("liquidator_self_d_tokens", &labels).set(borrow.d_tokens.0 as f64);
        gauge!("liquidator_self_d_tokens_underlying", &labels).set(
            pool.d_tokens_to_tokens_ceil(borrow.d_tokens)
                .map(|u| u.0)
                .unwrap_or(0) as f64,
        );
    }
}
