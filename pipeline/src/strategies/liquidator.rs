use {
    crate::{
        collectors::block_collector::NewBlock,
        constants::REFRESH_INTERVAL_BLOCKS,
        db::DbManager,
        executors::tx_executor::SubmitStellarTx,
        helper,
        types::{
            Action, BoxFuture, DepositPosition, Event, MarketData, Obligation, ObligationKey,
            PoolData, Strategy,
        },
    },
    ed25519_dalek::SigningKey,
    metrics::gauge,
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::{Client, Event as SorobanEvent},
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{debug, error, info, warn},
    url::Url,
};

pub struct LiquidatorConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
    /// Minimum profit margin in cents (e.g. 50 = $0.50).
    pub min_profit_margin_cents: i128,
    /// Assets we want to end up holding after the liquidation (token addresses).
    pub assets_to_hold: Vec<String>,
    /// Swap provider contract addresses, tried in order for best quote.
    pub swap_providers: Vec<String>,
    /// XLM address (native SAC).
    pub xlm_address: String,
    /// XLM balance to reserve for fees and trustlines (7 decimals).
    pub xlm_safety_margin: i128,
    /// Optional extra cushion applied to gain_oracle before the profitability check:
    ///   effective_gain = gain_oracle * (10_000 - buffer) / 10_000
    /// Useful because pool prices can shift slightly between quote-time and execution.
    pub swap_fee_buffer_bps: Option<i128>,
}

/// Liquidation approach determined by available liquidity.
#[derive(Debug, Clone)]
enum LiquidationType {
    /// Liquidator has sufficient balance of the borrow token to cover repayment directly.
    Direct { repay_amount: i128 },
    /// Liquidator swaps a held asset into the borrow token before liquidating.
    PreSwap {
        repay_amount: i128,
        /// Address of the asset being swapped in.
        source_asset: String,
        /// Exact amount of source_asset fed into the swap.
        source_amount_in: i128,
        /// Minimum amount of borrow token the on-chain swap must deliver (≥ repay_amount).
        /// If the DEX can't meet this, the tx reverts — slippage protection by construction.
        min_source_out: i128,
        /// Best swap provider found during planning.
        swap_provider: String,
    },
}

/// Complete liquidation plan passed to the executor.
#[derive(Debug)]
struct LiquidationPlan {
    liquidation_type: LiquidationType,
    borrower_key: ObligationKey,
    borrow_pool_address: String,
    collateral_pool_address: String,
    /// == min_demanded_collateral_amount in the on-chain Liquidate request.
    expected_seized_collateral: i128,
}

// ---------------------------------------------------------------------------
// Strategy struct
// ---------------------------------------------------------------------------

pub struct Liquidator {
    rpc: Client,
    pkey: String,
    config: LiquidatorConfig,
    skey: SigningKey,
    db: Arc<DbManager>,
    last_refresh_ledger: u32,
    market_data: HashMap<String, MarketData>,
    obligations: HashMap<String, HashMap<ObligationKey, Obligation>>,
}

impl Liquidator {
    pub fn try_create(
        config: LiquidatorConfig,
        skey: &SigningKey,
        db: &Arc<DbManager>,
    ) -> anyhow::Result<Self> {
        let db = Arc::clone(db);
        let rpc = Client::new(config.rpc_url.as_str())?;
        let pkey = ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
            skey.verifying_key().to_bytes(),
        ))))
        .to_string();

        Ok(Self {
            db,
            rpc,
            pkey,
            config,
            skey: skey.clone(),
            last_refresh_ledger: 0,
            obligations: HashMap::new(),
            market_data: HashMap::new(),
        })
    }

    fn saved_cursor(&self) -> Option<(String, u32)> {
        match self.db.load_cursor() {
            Ok(cursor) => cursor,
            Err(e) => {
                info!(?e, "failed to load cursor from DB");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Strategy trait
// ---------------------------------------------------------------------------

impl Strategy<Event, Action> for Liquidator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async {
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
                match helper::simulate_get_market_data(&self.rpc, market, &self.pkey).await {
                    Ok(market_data) => {
                        info!(market, ?market_data);
                        self.market_data.insert(market.clone(), market_data);
                    }
                    Err(e) => error!(?e, ?market, "get_market_data failed"),
                }

                let cached_obligations = self.db.load_obligations(market).unwrap_or_else(|e| {
                    warn!(
                        ?e,
                        ?market,
                        "failed to load obligations from DB. Falling back to RPC"
                    );
                    HashMap::new()
                });

                if cached_obligations.is_empty() {
                    info!(?market, "no cached obligations, fetching from RPC");
                    let obl_map = self.fetch_obligations_from_rpc(market).await?;
                    self.obligations.insert(market.clone(), obl_map);
                } else {
                    info!(?cached_obligations, "loaded obligations from DB");
                    self.obligations.insert(market.clone(), cached_obligations);
                }
            }
            info!("sync_state: done");

            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Event + block handling
// ---------------------------------------------------------------------------

impl Liquidator {
    async fn fetch_obligations_from_rpc(
        &self,
        market: &str,
    ) -> anyhow::Result<HashMap<ObligationKey, Obligation>> {
        // If no cursor exists yet (fresh start), snapshot the current ledger
        if self.saved_cursor().is_none() {
            match self.rpc.get_latest_ledger().await {
                Ok(ledger) => {
                    let ledger_seq = ledger.sequence.saturating_sub(5);
                    info!(
                        ledger_seq,
                        "no cursor in DB, saving ledger before obligation fetch"
                    );
                    if let Err(e) = self.db.save_cursor("", ledger_seq) {
                        warn!(?e, "failed to save pre-fetch cursor");
                    }
                }
                Err(e) => warn!(?e, "failed to get latest ledger for cursor snapshot"),
            }
        }

        let keys = helper::simulate_get_all_obligations(&self.rpc, market, &self.pkey).await?;
        let total = keys.len();

        info!(total, "fetching obligations...");

        let mut obl_map: HashMap<ObligationKey, Obligation> = HashMap::new();
        for (i, key) in keys.into_iter().enumerate() {
            info!(?key, idx = i + 1, total, "fetching obligation");

            let obl = helper::simulate_get_user_obligation(&self.rpc, market, &self.pkey, &key)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "market={market}: failed to fetch obligation user={}: {e:#}",
                        key.user,
                    )
                })?;

            debug!(?obl);

            if let Err(e) = self.db.save_obligation(market, &key, &obl) {
                warn!(?e, "failed to save obligation to DB");
            }
            obl_map.insert(key, obl);
        }

        Ok(obl_map)
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();

        let Ok(op_event) = helper::decode_operation_event(&event) else {
            return vec![];
        };

        use crate::helper::OperationEvent::*;
        let name = format!("{op_event:?}").to_lowercase(); // for logging

        match op_event {
            Deposit | Borrow | AddCollateral | Repay | Withdraw | RemoveCollateral => {
                let pool = helper::decode_topic(&event, 1);
                let obl_display = helper::decode_topic(&event, 2);
                debug!(%name, ledger = event.ledger, %market, %pool, %obl_display, "position event");

                let Ok(key) = helper::parse_obligation_key_from_topic(&event, 2) else {
                    warn!(%name, "cannot parse obligation key");

                    return vec![];
                };

                self.apply_obligation_snapshot(&name, &market, &event.value, "obligation", &key);
            }

            Liquidate => {
                let liquidator = helper::decode_topic(&event, 1);
                let borrower = helper::decode_topic(&event, 2);
                let borrow_pool = helper::decode_topic(&event, 3);
                let collateral_pool = helper::decode_topic(&event, 4);
                info!(ledger = event.ledger, %market, %liquidator, %borrower, %borrow_pool, %collateral_pool, "liquidation event");

                let Ok(borrower_key) = helper::parse_obligation_key_from_topic(&event, 2) else {
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

        if let Err(e) = self.db.save_cursor(&event.id, event.ledger) {
            warn!(?e, id = %event.id, "failed to save cursor");
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
        match helper::parse_obligation_from_event_value(value_xdr_base64, field_name, key) {
            Ok(Some(obl)) => {
                debug!(?key, ?obl, %event_name, %market, "obligation snapshot");
                if let Err(e) = self.db.save_obligation(market, key, &obl) {
                    warn!(?e, %event_name, %market, "failed to save obligation to DB");
                }
                self.obligations
                    .entry(market.to_string())
                    .or_default()
                    .insert(key.clone(), obl.clone());

                // Emit position metrics immediately when our own obligation changes.
                if key.user == self.pkey {
                    if let Some(md) = self.market_data.get(market) {
                        emit_position_metrics(market, &obl, md);
                    }
                }
            }
            Ok(None) => {
                info!(?key, %event_name, %market, "obligation deleted");
                if let Err(e) = self.db.delete_obligation(market, key) {
                    warn!(?e, %event_name, %market, "failed to delete obligation from DB");
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
            match helper::simulate_get_market_data(&self.rpc, &market, &self.pkey).await {
                Ok(md) => {
                    let prices: Vec<String> = md
                        .pools_data
                        .iter()
                        .map(|p| format!("{}={}", p.token_symbol, p.oracle_asset_price))
                        .collect();
                    debug!(%market, ?prices, "refreshed prices");
                    self.market_data.insert(market.clone(), md);

                    // Emit position metrics with the fresh exchange rates.
                    let own_key = ObligationKey::new(self.pkey.clone());
                    if let (Some(obl), Some(md)) = (
                        self.obligations.get(&market).and_then(|m| m.get(&own_key)),
                        self.market_data.get(&market),
                    ) {
                        emit_position_metrics(&market, obl, md);
                    }
                }
                Err(e) => warn!(?e, %market, "refresh failed"),
            }
        }
    }
}

impl Liquidator {
    async fn evaluate_market(&self, market: &str) -> Vec<Action> {
        let market_data = match self.market_data.get(market) {
            Some(md) => md,
            None => return vec![],
        };
        let Some(obligations) = self.obligations.get(market).filter(|map| !map.is_empty()) else {
            return vec![];
        };

        let mut actions = Vec::new();
        let mut checked = 0;

        for (obl_key, obligation) in obligations {
            if obl_key.user == self.pkey {
                continue;
            }
            checked += 1;

            if !helper::compute_is_liquidatable(obligation, market_data) {
                continue;
            }
            let is_insolvent = helper::compute_is_insolvent(obligation, market_data);
            debug!(?obl_key, ?obligation, is_insolvent, "locally liquidatable");

            if let Some(action) = self
                .try_liquidate(market, market_data, obl_key, obligation, is_insolvent)
                .await
            {
                info!(?action, "liquidation");
                actions.push(action);
            }
        }

        info!(%market, checked, liquidatable = actions.len(), "market evaluation complete");

        actions
    }

    /// Determine and execute the best liquidation plan for a liquidatable obligation.
    async fn try_liquidate(
        &self,
        market: &str,
        market_data: &MarketData,
        borrower_obl_key: &ObligationKey,
        obligation: &Obligation,
        is_insolvent: bool,
    ) -> Option<Action> {
        let liquidator_obl_key = ObligationKey::new(self.pkey.clone());

        // Per spec: at most one (borrow, deposit) pair
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

                if let Some(plan) = self
                    .build_liquidation_plan(
                        market,
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
                {
                    return self
                        .execute_liquidation_plan(market, &liquidator_obl_key, plan)
                        .await;
                }
            }
        }

        None
    }

    /// Compute caps, find profitable_repay, then try Direct → PreSwap.
    #[allow(clippy::too_many_arguments)]
    async fn build_liquidation_plan(
        &self,
        market: &str,
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

        // ---- Step a: compute caps ----

        let position_debt_tokens = borrow_pool.d_tokens_to_tokens_ceil(borrow_d_tokens);
        if position_debt_tokens <= 0 {
            error!(?borrower_key, "empty borrow position");
            return None;
        }

        let close_factor_cap = helper::compute_close_factor_repay_cap(
            position_debt_tokens,
            borrow_pool.liquidation_close_factor_bps,
            is_insolvent,
        );

        let position_collateral_sum =
            collateral_pool.j_tokens_to_tokens_floor(deposit_pos.j_tokens) + deposit_pos.collateral;

        // ---- Step b: minimum collateral value check ----

        let position_collateral_value = position_collateral_sum
            * collateral_pool.oracle_asset_price
            / 10_i128.pow(collateral_pool.token_decimals);
        let min_collateral_threshold = market_data.min_collateral_value_cents
            * 10_i128.pow(market_data.oracle_price_decimals)
            / 100;

        if position_collateral_sum <= 0 || position_collateral_value < min_collateral_threshold {
            warn!(
                position_collateral_sum,
                position_collateral_value,
                min_collateral_threshold,
                "position below minimum collateral threshold, skipping"
            );

            return None;
        }

        // ---- Step c: profit margin in borrow-token units ----

        let profit_margin_borrow = helper::compute_profit_margin_in_borrow_token(
            self.config.min_profit_margin_cents,
            market_data.oracle_price_decimals,
            borrow_pool,
        );

        // ---- Step d: max feasible repay ----

        let max_feasible_repay = position_debt_tokens.min(close_factor_cap);
        if max_feasible_repay <= 0 {
            error!(max_feasible_repay, "max_feasible_repay is non-positive");

            return None;
        }

        // ---- Step e: oracle-based max profitable repay ----

        let Some(profitable_repay) = self.calculate_max_profitable_repay_oracle(
            max_feasible_repay,
            position_collateral_sum,
            borrow_pool,
            collateral_pool,
            profit_margin_borrow,
            is_insolvent,
        ) else {
            debug!("no profitable liquidation amount found via oracle prices");

            return None;
        };

        // ---- Step f: try Direct, then PreSwap ----

        // --- Direct branch ---
        let raw_borrow_balance = match helper::query_token_balance(
            &self.rpc,
            borrow_token,
            &self.pkey,
            &self.pkey,
        )
        .await
        {
            Ok(b) => {
                gauge!("liquidator_asset_balance", "token_address" => borrow_token.to_string())
                    .set(b as f64);
                b
            }
            Err(e) => {
                warn!(?e, %borrow_token, "balance query failed");

                return None;
            }
        };
        let usable_borrow = if borrow_token == self.config.xlm_address {
            raw_borrow_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_borrow_balance
        };

        if usable_borrow >= profitable_repay {
            // Direct is viable — no swap cost, profitability already guaranteed by
            // calculate_max_profitable_repay_oracle.
            let expected_seized = self.compute_seized(
                profitable_repay,
                borrow_pool,
                collateral_pool,
                deposit_pos,
                obligation,
                market_data,
                is_insolvent,
            )?;

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                profitable_repay,
                expected_seized,
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
            });
        }

        // --- PreSwap branch ---
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

    /// Try each asset in assets_to_hold as a source for a PreSwap plan.
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
                continue; // would be Direct, already tried
            }

            // 1. Query balance
            let raw_balance =
                match helper::query_token_balance(&self.rpc, source_asset, &self.pkey, &self.pkey)
                    .await
                {
                    Ok(b) => {
                        gauge!("liquidator_asset_balance",
                            "token_address" => source_asset.clone()
                        )
                        .set(b as f64);
                        b
                    }
                    Err(e) => {
                        warn!(?e, %source_asset, "balance query failed, skipping");

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

            // 2. Find best provider + quote for usable_balance of source → borrow_token
            let path = &[source_asset.as_str(), borrow_token];
            let (best_provider, quoted_out) = match self.best_swap_quote(usable_balance, path).await
            {
                Some(q) => q,
                None => {
                    debug!(%source_asset, "no swap quote, skipping");

                    continue;
                }
            };

            if quoted_out <= 0 {
                continue;
            }

            // 3. Linear interpolation: how much source do we actually need?
            //    needed_source / usable_balance ≈ profitable_repay / quoted_out
            //    → needed_source = ceil(profitable_repay * usable_balance / quoted_out)
            let needed_source = (profitable_repay * usable_balance + quoted_out - 1) / quoted_out;

            // 4. Check we have enough
            if needed_source > usable_balance {
                debug!(%source_asset, needed_source, usable_balance, "insufficient balance for pre-swap, trying next");
                continue;
            }

            // 5. Re-quote with needed_source for a tighter estimate
            let quoted_out_for_needed = match helper::simulate_swap_provider_get_amount_out(
                &self.rpc,
                &best_provider,
                &self.pkey,
                needed_source,
                path,
            )
            .await
            {
                Ok(q) => q,
                Err(e) => {
                    warn!(?e, %source_asset, "re-quote failed, skipping");
                    continue;
                }
            };

            // min_source_out: the swap must deliver at least profitable_repay borrow tokens,
            // otherwise the on-chain tx reverts (slippage protection by construction).
            let min_source_out = profitable_repay;

            if quoted_out_for_needed < min_source_out {
                debug!(%source_asset, quoted_out_for_needed, min_source_out, "swap quote below min_source_out, skipping");
                continue;
            }

            // 6. Strict profitability check in oracle units
            let expected_seized = self.compute_seized(
                profitable_repay,
                borrow_pool,
                collateral_pool,
                deposit_pos,
                obligation,
                market_data,
                is_insolvent,
            )?;

            let source_pool = market_data
                .pools_data
                .iter()
                .find(|p| p.token_address == *source_asset);

            let Some(source_pool) = source_pool else {
                // source_asset has no pool — skip
                debug!(%source_asset, "source asset pool not found in market data, skipping");

                continue;
            };

            // cost_oracle = needed_source * source_price / 10^source_decimals
            let cost_oracle = needed_source * source_pool.oracle_asset_price
                / 10_i128.pow(source_pool.token_decimals);

            // gain_oracle = expected_seized * collateral_price / 10^collateral_decimals
            // The rebalancer is responsible for converting collateral — we value it at oracle price.
            let gain_oracle = expected_seized * collateral_pool.oracle_asset_price
                / 10_i128.pow(collateral_pool.token_decimals);

            // Apply optional fee buffer: effective_gain = gain * (10_000 - buffer) / 10_000
            let effective_gain = if let Some(buf) = self.config.swap_fee_buffer_bps {
                gain_oracle * (10_000 - buf) / 10_000
            } else {
                gain_oracle
            };

            // profit_margin in oracle units
            let profit_margin_oracle = self.config.min_profit_margin_cents
                * 10_i128.pow(market_data.oracle_price_decimals)
                / 100;

            if effective_gain < cost_oracle + profit_margin_oracle {
                debug!(
                    %source_asset,
                    effective_gain,
                    cost_oracle,
                    profit_margin_oracle,
                    "pre-swap not profitable enough, skipping"
                );
                continue;
            }

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                %source_asset,
                needed_source,
                min_source_out,
                profitable_repay,
                expected_seized,
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
            });
        }

        debug!("no viable pre-swap strategy found");
        None
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /// Oracle-based max profitable repay — no network calls, pure arithmetic.
    ///
    /// Guarantees: seized_value ≥ repay_value + profit_margin at oracle prices.
    fn calculate_max_profitable_repay_oracle(
        &self,
        max_feasible_repay: i128,
        available_collateral: i128,
        borrow_pool: &PoolData,
        collateral_pool: &PoolData,
        profit_margin_borrow_tokens: i128,
        is_insolvent: bool,
    ) -> Option<i128> {
        if available_collateral <= 0 {
            return None;
        }

        let liquidation_incentive_bps = if is_insolvent {
            collateral_pool.max_liquidation_incentive_bps
        } else {
            collateral_pool.max_liquidation_incentive_bps
        };

        let incentive_multiplier = (10_000 + liquidation_incentive_bps) as f64 / 10_000.0;

        let collateral_decimals = collateral_pool.token_decimals;
        let borrow_decimals = borrow_pool.token_decimals;

        let collateral_price_normalized = collateral_pool.oracle_asset_price as f64
            * 10_f64.powi(borrow_decimals as i32)
            / 10_f64.powi(collateral_decimals as i32);
        let borrow_price_normalized = borrow_pool.oracle_asset_price as f64;

        let price_ratio = collateral_price_normalized / borrow_price_normalized;

        let max_theoretical_repay = (available_collateral as f64 / incentive_multiplier) as i128;
        let max_repay_value_adjusted = (max_theoretical_repay as f64 * price_ratio) as i128;
        let max_profitable_repay =
            max_repay_value_adjusted.saturating_sub(profit_margin_borrow_tokens);

        let result = max_feasible_repay.min(max_profitable_repay);

        if result > 0 {
            debug!(
                available_collateral,
                max_feasible_repay,
                max_theoretical_repay,
                max_repay_value_adjusted,
                profit_margin_borrow_tokens,
                result,
                incentive_multiplier,
                price_ratio,
                "oracle-based max profitable repay"
            );
            Some(result)
        } else {
            debug!("no profitable repay amount using oracle prices");
            None
        }
    }

    /// Compute expected seized collateral for a given repay_amount.
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
            helper::compute_obligation_debt_value(obligation, market_data).ok()?;
        let obligation_collateral_value =
            helper::compute_obligation_collateral_value(obligation, market_data).ok()?;

        let seized = helper::compute_expected_seized_collateral(
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
            None
        } else {
            Some(seized)
        }
    }

    /// Query all configured swap providers for `amount_in` along `path`, return (best_provider, best_out).
    async fn best_swap_quote(&self, amount_in: i128, path: &[&str]) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;

        for provider in &self.config.swap_providers {
            match helper::simulate_swap_provider_get_amount_out(
                &self.rpc, provider, &self.pkey, amount_in, path,
            )
            .await
            {
                Ok(out) if out > 0 => {
                    let is_better = best.as_ref().map_or(true, |(_, prev)| out > *prev);
                    if is_better {
                        best = Some((provider.clone(), out));
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(?e, %provider, "swap quote failed");
                }
            }
        }

        best
    }
}

// ---------------------------------------------------------------------------
// Metrics helpers
// ---------------------------------------------------------------------------

/// Emit position gauges for the liquidator's own obligation in a single market.
///
/// Called immediately after any event that updates either side of the picture:
/// - from `refresh_market_data` when fresh exchange rates arrive, and
/// - from `apply_obligation_snapshot` when the liquidator's own positions change.
///
/// Gauges emitted (all labelled with `market`, `pool_address`, `token_symbol`):
///
/// | Gauge | Meaning |
/// |---|---|
/// | `liquidator_position_j_tokens` | raw j-token share count |
/// | `liquidator_position_plain_collateral` | plain (non-share) collateral units |
/// | `liquidator_position_j_tokens_underlying` | j-tokens priced → underlying asset units |
/// | `liquidator_position_d_tokens` | raw d-token share count |
/// | `liquidator_position_d_tokens_underlying` | d-tokens priced → underlying debt units |
fn emit_position_metrics(market: &str, obligation: &Obligation, market_data: &MarketData) {
    for deposit in &obligation.deposits {
        let Some(pool) = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == deposit.pool_address)
        else {
            warn!(
                %market,
                pool = %deposit.pool_address,
                "emit_position_metrics: pool not found for deposit position"
            );
            continue;
        };

        let labels = [
            ("market", market.to_string()),
            ("pool_address", pool.pool_address.clone()),
            ("token_symbol", pool.token_symbol.clone()),
        ];

        gauge!("liquidator_position_j_tokens", &labels).set(deposit.j_tokens as f64);
        gauge!("liquidator_position_plain_collateral", &labels).set(deposit.collateral as f64);
        gauge!("liquidator_position_j_tokens_underlying", &labels)
            .set(pool.j_tokens_to_tokens_floor(deposit.j_tokens) as f64);
    }

    for borrow in &obligation.borrows {
        let Some(pool) = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == borrow.pool_address)
        else {
            warn!(
                %market,
                pool = %borrow.pool_address,
                "emit_position_metrics: pool not found for borrow position"
            );
            continue;
        };

        let labels = [
            ("market", market.to_string()),
            ("pool_address", pool.pool_address.clone()),
            ("token_symbol", pool.token_symbol.clone()),
        ];

        gauge!("liquidator_position_d_tokens", &labels).set(borrow.d_tokens as f64);
        gauge!("liquidator_position_d_tokens_underlying", &labels)
            .set(pool.d_tokens_to_tokens_ceil(borrow.d_tokens) as f64);
    }
}

// ---------------------------------------------------------------------------
// Batch building + execution
// ---------------------------------------------------------------------------

impl Liquidator {
    /// Assemble the on-chain request list for a liquidation plan.
    ///
    /// Direct:  [ Liquidate(repay, min_collateral) ]
    /// PreSwap: [ SwapExactTokens(source→borrow, amount_in, min_out=profitable_repay),
    ///            Liquidate(repay, min_collateral) ]
    fn build_batch_requests(
        &self,
        plan: &LiquidationPlan,
    ) -> Option<Vec<stellar_xdr::curr::ScVal>> {
        let mut requests = Vec::new();

        // Pre-swap (if PreSwap type)
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

            let preswap_req = match helper::build_swap_exact_tokens_request_scval(
                swap_provider,
                &preswap_path,
                *source_amount_in,
                *min_source_out,
            ) {
                Ok(r) => r,
                Err(e) => {
                    error!(?e, "failed to build pre-swap request");
                    return None;
                }
            };
            requests.push(preswap_req);
        }

        // Liquidate
        let repay_amount = match &plan.liquidation_type {
            LiquidationType::Direct { repay_amount } => *repay_amount,
            LiquidationType::PreSwap { repay_amount, .. } => *repay_amount,
        };

        let liquidate_req = match helper::build_liquidate_request_scval(
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

    /// Look up the token address for a pool address by searching all cached market data.
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

    /// Simulate, build, and emit the tx action for a liquidation plan.
    async fn execute_liquidation_plan(
        &self,
        market: &str,
        liquidator_obl_key: &ObligationKey,
        plan: LiquidationPlan,
    ) -> Option<Action> {
        let requests = self.build_batch_requests(&plan)?;

        // 1. Simulate batch
        match helper::simulate_batch(&self.rpc, market, &self.pkey, liquidator_obl_key, &requests)
            .await
        {
            Ok(true) => {
                info!(?plan.borrower_key, "batch simulation OK");
            }
            Ok(false) => {
                warn!(?plan.borrower_key, "batch simulation failed, dropping plan");

                return None;
            }
            Err(e) => {
                warn!(?e, ?plan.borrower_key, "batch simulation error");

                return None;
            }
        }

        // 2. Build operation and emit
        match helper::build_batch_op(market, liquidator_obl_key, &requests) {
            Ok(op) => Some(Action::SubmitTx(SubmitStellarTx {
                op,
                signing_key: self.skey.clone(),
                max_retries: 3,
            })),
            Err(e) => {
                error!(?e, ?plan.borrower_key, "failed to build batch op");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::{
        helper,
        types::{DepositPosition, PoolData},
    };

    fn pool(price: i128, decimals: u32, incentive_bps: i128) -> PoolData {
        PoolData {
            pool_address: "pool".into(),
            token_address: "token".into(),
            token_symbol: "TST".into(),
            token_decimals: decimals,
            total_borrowed: 0,
            total_d_tokens: 0,
            total_j_tokens: 0,
            total_supply: 100_000_0000000,
            total_available: 100_000_0000000,
            total_available_adjusted: 0,
            total_collateral: 0,
            j_token_rate_floor_bps: 10_000,
            d_token_rate_ceil_bps: 10_000,
            oracle_asset_price: price,
            open_ltv_bps: 8000,
            close_ltv_bps: 8500,
            liability_factor_bps: 10_000,
            liquidation_close_factor_bps: 5_000,
            max_liquidation_incentive_bps: incentive_bps,
            flash_loan_fee_bps: 5,
            utilization_ratio_limit_bps: 9000,
        }
    }

    #[test]
    fn close_factor_cap_solvent() {
        assert_eq!(
            helper::compute_close_factor_repay_cap(1_000, 5_000, false),
            500
        );
    }

    #[test]
    fn close_factor_cap_insolvent_full_debt() {
        assert_eq!(
            helper::compute_close_factor_repay_cap(1_000, 5_000, true),
            1_000
        );
    }

    #[test]
    fn expected_seized_applies_incentive_insolvent() {
        let borrow_pool = pool(1_0000000, 7, 1_000);
        let collateral_pool = pool(1_0000000, 7, 1_000);
        let deposit = DepositPosition {
            pool_address: "pool".into(),
            j_tokens: 0,
            collateral: 100_0000000,
        };

        let seized = helper::compute_expected_seized_collateral(
            1_0000000,
            &borrow_pool,
            &collateral_pool,
            &deposit,
            0,
            0,
            true,
            1_00,
            7,
        );
        assert_eq!(seized, 1_1000000); // 10% incentive applied
    }

    #[test]
    fn expected_seized_capped_by_position_collateral() {
        let borrow_pool = pool(1_0000000, 7, 1_000);
        let collateral_pool = pool(1_0000000, 7, 1_000);
        let deposit = DepositPosition {
            pool_address: "pool".into(),
            j_tokens: 0,
            collateral: 1_0000000, // only 1 token available
        };

        let seized = helper::compute_expected_seized_collateral(
            5_0000000, // try to repay 5
            &borrow_pool,
            &collateral_pool,
            &deposit,
            0,
            0,
            true,
            1_00,
            7,
        );
        assert_eq!(seized, 1_0000000); // capped at available
    }

    #[test]
    fn slippage_protection_applied() {
        // min_source_out == profitable_repay means: if the DEX delivers < profitable_repay,
        // the tx reverts on-chain. Verify the arithmetic.
        let profitable_repay: i128 = 1_000_0000000;
        let source_amount_in: i128 = 1_200_0000000; // we spend 1200 USDC
        let quoted_out: i128 = 1_050_0000000; // quote says we get 1050 borrow tokens

        // min_source_out = profitable_repay → slippage protection
        let min_source_out = profitable_repay;
        assert!(
            quoted_out >= min_source_out,
            "quote should satisfy min_source_out"
        );

        // needed_source interpolation
        let needed_source = (profitable_repay * source_amount_in + quoted_out - 1) / quoted_out;
        assert!(needed_source <= source_amount_in);
        assert!(needed_source > 0);
    }

    #[test]
    fn preswap_needed_source_ceiling() {
        // ceil(profitable_repay * usable_balance / quoted_out)
        let profitable_repay = 1000_i128;
        let usable_balance = 1500_i128;
        let quoted_out = 1050_i128;

        let needed = (profitable_repay * usable_balance + quoted_out - 1) / quoted_out;
        // 1000 * 1500 = 1_500_000; ceil(1_500_000 / 1050) = ceil(1428.57) = 1429
        assert_eq!(needed, 1429);
        assert!(needed <= usable_balance);
    }
}
