//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits Direct or PreSwap
//! liquidations.

use {
    crate::{
        collect::{Event, block::NewBlock},
        execute::{
            Action,
            stellar_tx::{SettleHook, SubmitStellarTx},
        },
        stellar::Gateway,
        storage::{CursorRepo, ObligationsRepo},
        strategy::capital::{CapitalLedger, random_op_id},
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending::{
            DepositPosition, JToken, MarketData, Obligation, ObligationKey, PoolData, liquidation,
            profitability,
        },
        ports::{BatchSimulator, ChainReader, EventCodec, OpBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    metrics::{counter, gauge},
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

const REFRESH_INTERVAL_BLOCKS: u32 = 12;

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
    chain: Arc<dyn ChainReader>,
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
        chain: Arc<dyn ChainReader>,
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
        let keys = self.chain.read_all_obligation_keys(market).await?;
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

        let name = format!("{op_event:?}").to_lowercase();
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

    async fn handle_new_block(&mut self, block: NewBlock) -> Vec<Action> {
        let ledger = block.number;
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
        let Some(market_data) = self.market_data.get(market) else {
            counter!(
                "liquidator_scan_completed_total",
                "market" => market.to_string(),
                "outcome" => "no_market_data",
            )
            .increment(1);
            return vec![];
        };
        let Some(obligations) = self.obligations.get(market).filter(|m| !m.is_empty()) else {
            counter!(
                "liquidator_scan_completed_total",
                "market" => market.to_string(),
                "outcome" => "no_obligations",
            )
            .increment(1);
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
                if deposit_pos.j_tokens <= 0 && deposit_pos.collateral <= 0 {
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
                        borrow_pos.d_tokens,
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

        let position_debt_tokens = borrow_pool.d_tokens_to_tokens_ceil(borrow_d_tokens);
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
            .j_to_underlying_floor(JToken(deposit_pos.j_tokens))
            .raw()
            + deposit_pos.collateral;

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
        );

        let max_feasible_repay = position_debt_tokens.min(close_factor_cap);
        if max_feasible_repay <= 0 {
            error!(max_feasible_repay, "max_feasible_repay non-positive");
            return None;
        }

        // Bug fix #2: i128-only arithmetic, replacing the f64 math that used to
        // live inline. Uses engine::lending::profitability so the engine-level
        // tests cover it.
        let profitable_repay = profitability::compute_repay_cap_from_collateral(
            max_feasible_repay,
            position_collateral_sum,
            borrow_pool,
            collateral_pool,
            profit_margin_borrow,
        )?;

        let raw_borrow_balance = match self
            .ledger
            .cached_balance(&*self.chain, borrow_token, &self.pkey)
            .await
        {
            Ok(b) => {
                gauge!("liquidator_asset_balance", "token_address" => borrow_token.to_string())
                    .set(b as f64);
                b
            }
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
        let usable_after_reservations =
            self.ledger
                .available_after_reservations(borrow_token, &self.pkey, usable_borrow);

        if usable_after_reservations >= profitable_repay {
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
            let check = profitability::is_liquidation_profitable(
                gain_oracle,
                cost_oracle,
                profit_margin_oracle,
                self.config.gain_haircut_bps,
                self.config.inclusion_fee_oracle_units,
            );
            if !check.profitable {
                debug!(
                    net = check.net_oracle,
                    "direct branch fails profitability gate",
                );
                return None;
            }

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                profitable_repay, expected_seized, net = check.net_oracle,
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
                net_profit_oracle: check.net_oracle,
            });
        }

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
        .await
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
                .cached_balance(&*self.chain, source_asset, &self.pkey)
                .await
            {
                Ok(b) => {
                    gauge!("liquidator_asset_balance", "token_address" => source_asset.clone())
                        .set(b as f64);
                    b
                }
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
            let usable_balance =
                self.ledger
                    .available_after_reservations(source_asset, &self.pkey, usable_balance);
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
                .quote_amount_out(&best_provider, needed_source, path[0], path[1])
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

            let check = profitability::is_liquidation_profitable(
                gain_oracle,
                cost_oracle,
                profit_margin_oracle,
                self.config.gain_haircut_bps,
                self.config.inclusion_fee_oracle_units,
            );

            if !check.profitable {
                debug!(
                    %source_asset,
                    effective_gain = check.effective_gain_oracle,
                    required = check.required_oracle,
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
                net_profit_oracle: check.net_oracle,
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
                .quote_amount_out(provider, amount_in, path[0], path[1])
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
    ) -> Option<Vec<<Gateway as OpBuilder>::Request>> {
        let mut requests = Vec::new();

        if let LiquidationType::PreSwap {
            source_asset,
            source_amount_in,
            min_source_out,
            swap_provider,
            ..
        } = &plan.liquidation_type
        {
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
        }

        let repay_amount = match &plan.liquidation_type {
            LiquidationType::Direct { repay_amount } => *repay_amount,
            LiquidationType::PreSwap { repay_amount, .. } => *repay_amount,
        };

        let liquidate_req = match self.gateway.liquidate_request(
            &plan.borrower_key,
            &plan.borrow_pool_address,
            &plan.collateral_pool_address,
            repay_amount,
            plan.expected_seized_collateral,
        ) {
            Ok(r) => r,
            Err(e) => {
                error!(?e, "failed to build liquidate request");
                return None;
            }
        };
        requests.push(liquidate_req);

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
            Ok(true) => info!(?plan.borrower_key, "batch simulation OK"),
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

        // Reserve in-flight capital so other opportunities (and the rebalancer)
        // don't double-spend the wallet balance. The reservation is released by
        // the executor's SettleHook on every terminal tx outcome; the 5-minute
        // ledger TTL is now only a safety ceiling for hooks lost to executor
        // task panics.
        let op_id = random_op_id();
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
        };
        let raw_balance = match self
            .ledger
            .cached_balance(&*self.chain, &reserve_token, &self.pkey)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(?e, %reserve_token, "balance query failed at reservation time");
                counter!("liquidator_skip_total", "reason" => "balance_query_failed").increment(1);
                return None;
            }
        };
        let available = if reserve_token == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        if !self
            .ledger
            .reserve(op_id, &reserve_token, &self.pkey, reserve_amount, available)
        {
            warn!(
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

        match self.gateway.batch_op(market, liquidator_obl_key, &requests) {
            Ok(op) => {
                // Counts plans handed to the executor — NOT confirmed liquidations.
                // The tx may still fail at simulate, bad_seq retry, or confirmation
                // poll; those outcomes are tracked separately by the executor's
                // own counters.
                counter!(
                    "liquidator_liquidation_plans_dispatched_total",
                    "market" => market.to_string()
                )
                .increment(1);
                Some(Action::SubmitTx(SubmitStellarTx {
                    op,
                    signing_key: self.skey.clone(),
                    max_retries: 3,
                    on_settle: Some(SettleHook {
                        ledger: self.ledger.clone(),
                        op_id,
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
        gauge!("liquidator_self_j_tokens", &labels).set(deposit.j_tokens as f64);
        gauge!("liquidator_self_plain_collateral", &labels).set(deposit.collateral as f64);
        gauge!("liquidator_self_j_tokens_underlying", &labels)
            .set(pool.j_to_underlying_floor(JToken(deposit.j_tokens)).raw() as f64);
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
        gauge!("liquidator_self_d_tokens", &labels).set(borrow.d_tokens as f64);
        gauge!("liquidator_self_d_tokens_underlying", &labels)
            .set(pool.d_tokens_to_tokens_ceil(borrow.d_tokens) as f64);
    }
}
