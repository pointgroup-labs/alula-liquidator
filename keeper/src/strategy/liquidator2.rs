//! Liquidator strategy: scans cached obligations after every refresh interval,
//! picks profitable (borrow, deposit) pairs, and submits liquidations.

use {
    crate::{
        collect::{Event, stellar_ledger::NewLedger},
        execute::{
            Action,
            stellar_tx::{SettleHook, SubmitStellarTx},
        },
        stellar::Gateway,
        storage::{cursor::CursorRepo, obligations::ObligationsRepo},
        strategy::{LiquidatorCapital, liquidator_capital::random_id},
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

const BPS_FACTOR: i128 = 10_000;

pub struct LiquidatorConfig {
    pub max_retries: u32,
    pub xlm_address: String,
    pub markets: Vec<String>,
    pub xlm_safety_margin: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub min_profit_margin_cents: i128,
    pub allowed_swap_slippage_bps: i128,
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
                    market_data,
                    obligation,
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
            .record(net_value as f64);
            counter!(
                "liquidator_plan_expected_net_profit_value_units_total",
                "market" => market.to_string(),
            )
            .increment(net_value as u64);
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
        deposit_position: &DepositPosition,
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
            .j_tokens_to_tokens_floor(deposit_position.j_tokens)
            .ok()?
            .0
            + deposit_position.collateral.0;
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

        let max_feasible_repay = position_debt_tokens.0.min(close_factor_cap);
        if max_feasible_repay <= 0 {
            error!(max_feasible_repay, "max_feasible_repay non-positive");

            return None;
        }

        let profitable_repay = profitability::compute_repay_cap_from_collateral(
            max_feasible_repay,
            position_collateral_sum,
            borrow_pool,
            collateral_pool,
            profit_margin_borrow,
        )?;

        let raw_borrow_balance = self
            .liquidator_capital
            .try_get_balance(borrow_token, &*self.ledger_reader)
            .await
            .inspect_err(|e| {
                warn!(?e, %borrow_token, "balance query failed");
                counter!("liquidator_skip_total", "reason" => "balance_query_failed").increment(1);
            })
            .unwrap_or(0);

        let usable_borrow = if borrow_token == self.config.xlm_address {
            raw_borrow_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_borrow_balance
        };

        // let plan = if usable_borrow >= profitable_repay {
        //     self.try_direct_plan(
        //         is_insolvent,
        //         profitable_repay,
        //         borrow_pool,
        //         borrower_obligation,
        //         market_data,
        //         collateral_pool,
        //         borrower_obligation_key,
        //         deposit_position,
        //     )
        //     .await
        // } else {
        //     // self.try_flash_plan();
        //     todo!()
        // };

        self.try_preswap_plan(
            is_insolvent,
            profitable_repay,
            borrow_pool,
            borrower_obligation,
            market_data,
            collateral_pool,
            borrower_obligation_key,
            deposit_position,
        )
        .await

        // None
    }

    async fn try_direct_plan(
        &self,
        is_insolvent: bool,
        repay_amount: i128,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        borrower_key: &ObligationKey,
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
        let profit_margin_value = self
            .config
            .min_profit_margin_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);
        let profitability = profitability::compute_liquidation_profitability(
            expected_received_value,
            repay_value,
            profit_margin_value,
            self.config.inclusion_fee_oracle_units,
        )
        .ok()?;
        if !profitability.is_profitable {
            debug!(
                net = profitability.net_value,
                "direct branch fails profitability gate",
            );

            return None;
        }

        info!(
            ?borrower_key,
            borrow_pool = %borrow_pool.pool_address,
            collateral_pool = %collateral_pool.pool_address,
            repay_amount, expected_received_value, net = profitability.net_value,
            "DIRECT liquidation plan"
        );

        return Some(LiquidationPlan {
            liquidation_type: LiquidationType::Direct { repay_amount },
            borrower_key: borrower_key.clone(),
            borrow_pool_address: borrow_pool.pool_address.clone(),
            collateral_pool_address: collateral_pool.pool_address.clone(),
            expected_seized_collateral,
            net_profit_value: profitability.net_value,
        });
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
        deposit_position: &DepositPosition,
    ) -> Option<LiquidationPlan> {
        if borrow_pool.total_available.0 < repay_amount {
            counter!(
                "liquidator_skip_total",
                "reason" => "flash_pool_insufficient_liquidity"
            )
            .increment(1);

            return None;
        }

        let flash_fee = profitability::compute_flash_fee(
            Underlying(repay_amount),
            borrow_pool.flash_loan_fee_bps,
        )
        .ok()?
        .0;
        let seized_amount = self.compute_seized(
            repay_amount,
            is_insolvent,
            borrow_pool,
            obligation,
            market_data,
            collateral_pool,
            deposit_position,
        )?;

        let flash_repay_amount = repay_amount.saturating_add(flash_fee);
        let min_swap_out = flash_repay_amount;

        let collateral_token = collateral_pool.token_address.clone();
        let borrow_token = borrow_pool.token_address.as_str();
        let swap_path = [collateral_token.as_str(), borrow_token];

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
                min_swap_out, seized_amount, "flash swap quote below min_swap_out threshold"
            );
            counter!(
                "liquidator_skip_total",
                "reason" => "flash_swap_shortfall"
            )
            .increment(1);

            return None;
        }

        if quoted_out < min_swap_out {
            debug!(
                quoted_out,
                min_swap_out, seized_amount, "flash swap quote below min_swap_out threshold"
            );
            counter!(
                "liquidator_skip_total",
                "reason" => "flash_swap_shortfall"
            )
            .increment(1);

            return None;
        }

        let (flash_repay_value, received_collateral_value) = (
            flash_repay_amount
                .saturating_mul(borrow_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(borrow_pool.token_decimals)),
            seized_amount
                .saturating_mul(collateral_pool.oracle_asset_price)
                .saturating_div(10_i128.pow(collateral_pool.token_decimals)),
        );
        let profit_margin_value = self
            .config
            .min_profit_margin_cents
            .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
            .saturating_div(100);

        let profitability = profitability::compute_liquidation_profitability(
            received_collateral_value,
            flash_repay_value,
            profit_margin_value,
            self.config.inclusion_fee_oracle_units,
        )
        .ok()?;
        if !profitability.is_profitable {
            debug!(
                net = profitability.net_value,
                flash_fee, "flash branch fails profitability gate"
            );

            return None;
        }

        info!(
            ?borrower_key,
            borrow_pool = %borrow_pool.pool_address,
            collateral_pool = %collateral_pool.pool_address,
            repay_amount,
            flash_fee,
            seized_amount,
            min_swap_out,
            net = profitability.net_value,
            "FLASH liquidation plan"
        );

        Some(LiquidationPlan {
            liquidation_type: LiquidationType::Flash {
                flash_fee,
                repay_amount,
                min_swap_out,
                swap_provider: best_provider,
            },
            borrower_key: borrower_key.clone(),
            net_profit_value: profitability.net_value,
            expected_seized_collateral: seized_amount,
            borrow_pool_address: borrow_pool.pool_address.clone(),
            collateral_pool_address: collateral_pool.pool_address.clone(),
        })
    }

    async fn try_preswap_plan(
        &self,
        is_insolvent: bool,
        repay_amount: i128,
        borrow_pool: &PoolData,
        obligation: &Obligation,
        market_data: &MarketData,
        collateral_pool: &PoolData,
        borrower_key: &ObligationKey,
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
                    swappable_balance, source_asset, "non-positive liqudator usable balance"
                );

                continue;
            }

            let Some((best_provider, max_amount_in)) = self
                .best_swap_base(swappable_balance, source_asset, borrow_token)
                .await
            else {
                error!(%source_asset, "no swap base");

                continue;
            };

            let max_amount_in = max_amount_in
                .saturating_mul(BPS_FACTOR + self.config.allowed_swap_slippage_bps)
                / BPS_FACTOR;
            if max_amount_in > swappable_balance {
                error!(
                    max_amount_in,
                    source_asset, borrow_token, "insufficient swappable balance"
                );

                continue;
            }

            let expected_seized = self.compute_seized(
                repay_amount,
                is_insolvent,
                borrow_pool,
                obligation,
                market_data,
                collateral_pool,
                deposit_position,
            )?;

            let Some(source_pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.token_address == *source_asset)
            else {
                error!(%source_asset, "source asset pool not in market data");

                continue;
            };

            let (cost_value, gain_value, profit_margin_value) = (
                max_amount_in
                    .saturating_mul(source_pool.oracle_asset_price)
                    .saturating_div(10_i128.pow(source_pool.token_decimals)),
                expected_seized
                    .saturating_mul(collateral_pool.oracle_asset_price)
                    .saturating_div(10_i128.pow(collateral_pool.token_decimals)),
                self.config
                    .min_profit_margin_cents
                    .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
                    .saturating_div(100),
            );

            let profitability = profitability::compute_liquidation_profitability(
                gain_value,
                cost_value,
                profit_margin_value,
                self.config.inclusion_fee_oracle_units,
            )
            .ok()?;
            if !profitability.is_profitable {
                debug!(net = profitability.net_value, "preswap is not profitable",);

                continue;
            }

            info!(
                ?borrower_key,
                borrow_pool = %borrow_pool.pool_address,
                collateral_pool = %collateral_pool.pool_address,
                repay_amount, expected_seized, net = profitability.net_value,
                "PRESWAP liquidation plan"
            );

            if let Some(ref bp) = best_plan {
                if bp.net_profit_value < profitability.net_value {
                    best_plan = Some(LiquidationPlan {
                        liquidation_type: LiquidationType::PreSwap {
                            repay_amount,
                            source_asset: source_asset.clone(),
                            source_amount_in: max_amount_in,
                            min_amount_out: repay_amount,
                            swap_provider: best_provider,
                        },
                        borrower_key: borrower_key.clone(),
                        borrow_pool_address: borrow_pool.pool_address.clone(),
                        collateral_pool_address: collateral_pool.pool_address.clone(),
                        expected_seized_collateral: expected_seized,
                        net_profit_value: profitability.net_value,
                    })
                }
            };
        }

        best_plan
    }

    async fn best_swap_quote(&self, amount_in: i128, path: &[&str]) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .ledger_reader
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

    async fn best_swap_base(
        &self,
        amount_out: i128,
        asset_in: &str,
        asset_out: &str,
    ) -> Option<(String, i128)> {
        let mut best: Option<(String, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .ledger_reader
                .get_amount_in(amount_out, asset_in, asset_out, provider)
                .await
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
            liquidation::compute_obligation_debt_value(obligation, market_data).ok()?,
            liquidation::compute_obligation_collateral_value(obligation, market_data).ok()?,
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
        );
        if !seized.is_positive() {
            warn!(repay_amount, "expected seized collateral is zero");
            counter!("liquidator_skip_total", "reason" => "unprofitable_seize_zero").increment(1);

            None
        } else {
            Some(seized)
        }
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
        // let mut requests = vec![];

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
