//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits liquidations.

use {
    crate::{
        collect::{Event, stellar_ledger::NewLedger},
        execute::{
            Action,
            stellar_tx::{LiquidationOutcomeMetric, SettleHook, SubmitStellarTx},
        },
        stellar::Gateway,
        storage::{cursor::CursorRepo, obligations::ObligationsRepo},
        strategy::{LiquidatorCapital, capital::random_op_id},
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

pub struct LiquidatorConfig {
    pub max_retries: u32,
    pub xlm_address: String,
    pub markets: Vec<String>,
    pub xlm_safety_margin: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub min_profit_margin_cents: i128,
    pub inclusion_fee_oracle_units: i128,
}

#[derive(Debug, Clone)]
enum LiquidationType {
    /// Direct liquidation using the available liquidator's liquidity
    Direct {
        /// 'repay_amount', passed to the 'liquidate' endpoint
        repay_amount: i128,
    },
    /// PreSwap liquidation via swapping the available liquidator's liquidity
    /// for the repaid asset during liquidation
    PreSwap {
        /// 'repay_amount', passed to the 'liquidate' endpoint
        repay_amount: i128,
        /// asset, swapped for the 'repaid asset' during liquidation
        source_asset: String,
        /// amount of 'source_asset' swapped for the 'repaid asset'
        source_amount_in: i128,
        /// 'repay_amount', passed to the 'liquidate' endpoint(TODO: check)
        min_amount_out: i128,
        /// address of the swap provider to use
        swap_provider: String,
    },
    /// Liquidation utilizing the flash-borrow of `repay_amount` of the borrow asset, seizing collateral,
    /// swapping seized collateral back to the borrow asset, auto-repay.
    Flash {
        // TODO
        /// 'repay_amount', passed to the 'liquidate' endpoint
        repay_amount: i128,
        /// flash borrowed asset flash fee in underlying token
        flash_fee: i128, // TODO: Underlying?
        /// `min_amount_out` passed to SwapExactTok
        min_swap_out: i128,
        swap_provider: String,
    },
}

#[derive(Debug)]
struct LiquidationPlan {
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
            info!(?self.config.markets, "sync_state: loading market(s)");
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
                debug!(%event_name, ledger = event.ledger, %market, %pool, %obl_display, "position update event");

                let Ok(key) = self.gateway.parse_obligation_key_from_topic(&event, 2) else {
                    warn!(%event_name, "cannot parse obligation key");

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

                let Ok(borrower_key) = self
                    .gateway
                    .parse_obligation_key_from_topic(&event, 2)
                    .inspect_err(|e| {
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

                let liquidator_key = ObligationKey {
                    user: liquidator,
                    seed: None,
                };
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
        market: &str,
        field_name: &str,
        event_name: &str,
        key: &ObligationKey,
        value_xdr_base64: &str,
    ) {
        match self
            .gateway
            .parse_obligation_from_event_value(value_xdr_base64, field_name, key)
        {
            Ok(Some(obligation)) => {
                debug!(?key, ?obligation, %event_name, %market, "obligation snapshot");
                if let Err(e) = self.obligations_repo.put(market, key, &obligation) {
                    warn!(?e, %event_name, %market, "failed to save obligation");
                }
                if key.user == self.pkey
                    && let Some(md) = self.market_data.get(market)
                {
                    emit_self_position_metrics(market, &obligation, md);
                }

                self.obligations
                    .entry(market.to_string())
                    .or_default()
                    .insert(key.clone(), obligation);
            }
            Ok(None) => {
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

        info!(ledger, "refresh + evaluate");

        self.refresh_market_data().await;

        let mut actions = vec![];
        for market in &self.config.markets {
            actions.extend(self.evaulate_market(market).await);
        }

        if !actions.is_empty() {
            info!(ledger, count = actions.len(), "submitting liquidations");
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
                        emit_self_position_metrics(&market, obl, &market_data);
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
        let (mut checked, mut liquidatable) = (0_u64, 0_u64);
        for (obligation_key, obligation) in obligations {
            checked += 1;
            if obligation_key.user == self.pkey {
                continue;
            }
            if !liquidation::compute_is_unhealthy(obligation, market_data) {
                continue;
            }

            liquidatable += 1;
            let is_insolvent = liquidation::compute_is_insolvent(obligation, market_data);

            info!(?obligation_key, ?obligation, is_insolvent, "LIQUIDATABLE");

            if let Some(action) = self
                .try_liquidate(
                    market,
                    is_insolvent,
                    obligation,
                    market_data,
                    obligation_key,
                )
                .await
            {
                info!(?action, "LIQUIDATION ACTION");
                actions.push(action);
            }
        }

        gauge!("liquidator_obligations_total", "market" => market.to_string()).set(checked as f64);
        gauge!("liquidator_liquidatable_positions", "market" => market.to_string())
            .set(liquidatable as f64);
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

        let mut best: Option<LiquidationPlan> = None;
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
                    error!(
                        ?deposit_position,
                        "collateral pool missing from market data"
                    );

                    continue;
                };

                let Some(plan) = self
                    .build_liquidation_plan(
                        is_insolvent,
                        borrow_position.d_tokens.0,
                        borrow_pool,
                        market_data,
                        collateral_pool,
                        deposit_position,
                        borrower_obligation,
                        borrower_obligation_key,
                    )
                    .await
                else {
                    warn!("Failed to build liquidation plan");

                    continue;
                };

                if best
                    .as_ref()
                    .is_none_or(|b| plan.net_profit_value > b.net_profit_value)
                {
                    best = Some(plan);
                }
            }
        }

        if let Some(plan) = best {
            let net_value = plan.net_profit_value.max(0);

            histogram!(
                "liquidator_plan_expected_net_profit_value_units",
                "market" => market.to_string(),
            )
            .record(net_oracle as f64);
            counter!(
                "liquidator_plan_expected_net_profit_value_units_total",
                "market" => market.to_string(),
            )
            .increment(net_oracle as u64);
            info!(
                ?plan.borrower_key,
                borrow_pool = %plan.borrow_pool_address,
                collateral_pool = %plan.collateral_pool_address,
                net = plan.net_profit_value,
                "selected best (borrow, deposit) pair",
            );

            self.execute_liquidation_plan(market, plan, &liquidator_obl_key)
                .await
        } else {
            error!("failed to come up with a liquidation plan");

            // TODO: metric?

            None
        }
    }

    async fn build_liquidation_plan(
        &self,
        is_insolvent: bool,
        borrow_d_tokens: i128,
        borrow_pool: &PoolData,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        deposit_pos: &DepositPosition,
        borrower_obligation: &Obligation,
        borrower_obligation_key: &ObligationKey,
    ) -> Option<LiquidationPlan> {
        let borrow_token = borrow_pool.token_address.as_str();

        let position_debt_tokens = borrow_pool
            .d_tokens_to_tokens_ceil(borrow_d_tokens.into())
            .ok()?;
        if position_debt_tokens <= Underlying::ZERO {
            error!(?borrower_obligation_key, "empty borrow position");

            return None;
        }

        let close_factor_cap = liquidation::compute_close_factor_repay_cap(
            position_debt_tokens.0,
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

        None
    }

    async fn execute_liquidation_plan(
        &self,
        market: &str,
        plan: LiquidationPlan,
        liquidator_obligation_key: &ObligationKey,
    ) -> Option<Action> {
        let requests = self.build_batch_requests(&plan)?;

        None
    }

    fn build_batch_requests(
        &self,
        plan: &LiquidationPlan,
    ) -> Option<Vec<<Gateway as OperationBuilder>::Request>> {
        let mut requests = vec![];

        // TODO: WTF
        None
    }
}

fn emit_self_position_metrics(market: &str, obligation: &Obligation, market_data: &MarketData) {
    // for deposit in &obligation.deposits {
    //     let Some(pool) = market_data
    //         .pools_data
    //         .iter()
    //         .find(|p| p.pool_address == deposit.pool_address)
    //     else {
    //         warn!(%market, pool = %deposit.pool_address, "emit_self_position_metrics: pool not found");
    //         continue;
    //     };
    //     let labels = [
    //         ("market", market.to_string()),
    //         ("pool_address", pool.pool_address.clone()),
    //         ("token_symbol", pool.token_symbol.clone()),
    //     ];
    //     gauge!("liquidator_self_j_tokens", &labels).set(deposit.j_tokens.0 as f64);
    //     gauge!("liquidator_self_plain_collateral", &labels).set(deposit.collateral.0 as f64);
    //     gauge!("liquidator_self_j_tokens_underlying", &labels).set(
    //         pool.j_tokens_to_tokens_floor(deposit.j_tokens)
    //             .map(|u| u.0)
    //             .unwrap_or(0) as f64,
    //     );
    // }
    // for borrow in &obligation.borrows {
    //     let Some(pool) = market_data
    //         .pools_data
    //         .iter()
    //         .find(|p| p.pool_address == borrow.pool_address)
    //     else {
    //         warn!(%market, pool = %borrow.pool_address, "emit_self_position_metrics: pool not found");
    //         continue;
    //     };
    //     let labels = [
    //         ("market", market.to_string()),
    //         ("pool_address", pool.pool_address.clone()),
    //         ("token_symbol", pool.token_symbol.clone()),
    //     ];
    //     gauge!("liquidator_self_d_tokens", &labels).set(borrow.d_tokens.0 as f64);
    //     gauge!("liquidator_self_d_tokens_underlying", &labels).set(
    //         pool.d_tokens_to_tokens_ceil(borrow.d_tokens)
    //             .map(|u| u.0)
    //             .unwrap_or(0) as f64,
    //     );
    // }
}
