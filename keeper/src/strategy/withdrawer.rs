//! Withdrawer strategy: opportunistically pulls the liquidator's liquidity out of pools
//! if such a withdrawal doesn't yield a scarcity fee

use {
    crate::{
        collect::{Event, stellar_ledger::NewLedger},
        execute::{Action, stellar_tx::SubmitStellarTx},
        stellar::Gateway,
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending_model::{MarketData, Obligation, ObligationKey, PoolData, Underlying},
        ports::OperationEvent,
        ports::{EventCodec, LedgerReader, OperationBuilder},
        reactor::{BoxFuture, Strategy},
    },
    metrics::counter,
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, trace, warn},
};

pub struct WithdrawerConfig {
    pub max_retries: u32,
    pub markets: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub min_withdraw_value_cents: i128,
    pub utilization_safety_margin_bps: i128,
}

pub struct Withdrawer {
    skey: SigningKey,
    gateway: Arc<Gateway>,
    config: WithdrawerConfig,
    liquidator_key: ObligationKey,
    ledger_reader: Arc<dyn LedgerReader>,
    liquidator_obligations: HashMap<String, Obligation>,
}

impl Withdrawer {
    pub fn new(
        pkey: String,
        skey: SigningKey,
        gateway: Arc<Gateway>,
        config: WithdrawerConfig,
        ledger_reader: Arc<dyn LedgerReader>,
    ) -> Self {
        let liquidator_key = ObligationKey::new(pkey.clone());

        Self {
            skey,
            config,
            gateway,
            ledger_reader,
            liquidator_key,
            liquidator_obligations: HashMap::new(),
        }
    }
}

impl Strategy<Event, Action> for Withdrawer {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewLedger(b) => self.handle_new_ledger(b).await,
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl Withdrawer {
    /*
    TODO: Logically speaking, this can be omitted in the future since only events
     that increase available liquidity or a liquidation event for this liquidator
     are reasonable causes for withdrawal. Acts as a safety measure for now
    */
    async fn handle_new_ledger(&mut self, block: NewLedger) -> Vec<Action> {
        if !block
            .seq_num
            .is_multiple_of(self.config.refresh_interval_blocks)
        {
            return vec![];
        }

        let mut actions = vec![];
        let markets: Vec<String> = self.config.markets.clone();

        for market in &markets {
            self.refresh_liquidator_obligation(market).await;
            // A failed read for one market must not discard actions already
            // accumulated for earlier markets — skip just this market.
            let Ok(market_data) = self
                .ledger_reader
                .read_market_data(market)
                .await
                .inspect_err(|err| {
                    warn!(?err, ?market, "failed to read market data");
                    counter!("withdrawer_outcome_total", "outcome" => "failed_to_read_market_data")
                        .increment(1);
                })
            else {
                continue;
            };

            actions.extend(self.find_withdrawal_opportunities(market, market_data));
        }

        actions
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();
        if !self.config.markets.contains(&market) {
            warn!(%market, "event from non-configured market");

            return vec![];
        }

        match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Deposit)
            | Ok(OperationEvent::Repay)
            | Ok(OperationEvent::Liquidate) => {
                info!(?event, "detected increasing available liquidity event");
            }
            _ => {
                debug!(?event, "received non-increasing available liquidity event");

                return vec![];
            }
        }

        self.refresh_liquidator_obligation(&event.contract_id).await;
        let Ok(market_data) = self
            .ledger_reader
            .read_market_data(&market)
            .await
            .inspect_err(|err| {
                warn!(?err, ?market, "failed to read market data");
                counter!("withdrawer_outcome_total", "outcome" => "failed_to_read_market_data")
                    .increment(1);
            })
        else {
            return vec![];
        };

        self.find_withdrawal_opportunities(&market, market_data)
    }

    fn find_withdrawal_opportunities(&self, market: &str, market_data: MarketData) -> Vec<Action> {
        let Some(liquidator_obligation) = self.liquidator_obligations.get(market) else {
            info!(%market, "no liquidator obligations for market");
            counter!("withdrawer_outcome_total", "outcome" => "no_obligations").increment(1);

            return vec![];
        };

        let mut actions = vec![];
        for deposit_pos in &liquidator_obligation.deposits {
            let Some(pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == deposit_pos.pool_address)
            else {
                warn!(pool = deposit_pos.pool_address, "Pool not found");
                counter!("withdrawer_outcome_total", "outcome" => "pool_missing").increment(1);

                continue;
            };

            // PoolData::compute_max_safe_withdrawal — returns Underlying::ZERO
            // if `utilization_considered_safe` collapses to <= 0.
            let max_withdrawal =
                match pool.compute_max_safe_withdrawal(self.config.utilization_safety_margin_bps) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(?e, pool = %pool.pool_address, "max_safe_withdrawal failed");
                        counter!("withdrawer_outcome_total", "outcome" => "max_withdrawal_error")
                            .increment(1);
                        continue;
                    }
                };
            if max_withdrawal == Underlying::ZERO {
                counter!("withdrawer_outcome_total", "outcome" => "pool_at_capacity").increment(1);
                continue;
            }

            let liquidator_underlying = match pool.j_tokens_to_tokens_floor(deposit_pos.j_tokens) {
                Ok(u) => u,
                Err(e) => {
                    warn!(?e, pool = %pool.pool_address, "j_tokens_to_tokens_floor failed");
                    counter!("withdrawer_outcome_total", "outcome" => "conversion_error")
                        .increment(1);
                    continue;
                }
            };
            // A zero-balance deposit row would otherwise dribble into the
            // `below_threshold` bucket below (any min_withdraw_value_cents
            // > 0 dwarfs a 0-token position), misdirecting the operator
            // toward tuning the threshold that wouldn't fix anything.
            // Worse, the `withdrawal_amount == liquidator_underlying.0`
            // branch immediately below would flip `withdrawal_amount` to
            // i128::MAX on a 0-balance row, building a withdraw-everything
            // op against a position that has nothing — surface this as its
            // own outcome and skip.
            if liquidator_underlying.0 == 0 {
                counter!("withdrawer_outcome_total", "outcome" => "empty_position").increment(1);
                continue;
            }
            let mut withdrawal_amount = liquidator_underlying.0.min(max_withdrawal.0);
            let withdrawal_value_cents =
                self.calculate_withdrawal_value_cents(&market_data, pool, withdrawal_amount);

            if withdrawal_amount == liquidator_underlying.0 {
                withdrawal_amount = i128::MAX;
            }

            if withdrawal_value_cents >= self.config.min_withdraw_value_cents {
                info!(
                    pool_address = %pool.pool_address,
                    current_utilization = pool.utilization_ratio_bps().unwrap_or_default(),
                    max_withdrawal = max_withdrawal.0,
                    liquidator_tokens = liquidator_underlying.0,
                    withdrawal_amount,
                    value_cents = withdrawal_value_cents,
                    "Creating withdrawal action"
                );

                match self.build_withdraw_action(market, &pool.pool_address, withdrawal_amount) {
                    Ok(action) => {
                        actions.push(action);
                        counter!("withdrawer_outcome_total", "outcome" => "dispatched")
                            .increment(1);
                    }
                    Err(e) => {
                        error!(?e, "Failed to build withdrawal action");
                        counter!("withdrawer_outcome_total", "outcome" => "build_error")
                            .increment(1);
                    }
                };
            } else {
                counter!("withdrawer_outcome_total", "outcome" => "below_threshold").increment(1);
            }
        }

        actions
    }

    fn build_withdraw_action(
        &self,
        market_address: &str,
        pool_address: &str,
        amount: i128,
    ) -> anyhow::Result<Action> {
        let op =
            self.gateway
                .withdraw_op(market_address, &self.liquidator_key, pool_address, amount)?;
        Ok(Action::SubmitTx(SubmitStellarTx {
            op,
            on_settle: None,
            signing_key: self.skey.clone(),
            max_retries: self.config.max_retries,
        }))
    }

    fn calculate_withdrawal_value_cents(
        &self,
        market_data: &MarketData,
        pool: &PoolData,
        token_amount: i128,
    ) -> i128 {
        // Saturating math: the workspace builds with `overflow-checks = true`
        // and `panic = "abort"`, so a raw multiply here could kill the whole
        // keeper on pathological oracle values. Mirrors the balancer's
        // `compute_value_cents`.
        let price_with_decimals = pool.oracle_asset_price;
        if price_with_decimals <= 0 {
            return 0;
        }
        let oracle_decimals = market_data.oracle_price_decimals;
        let token_decimals = pool.token_decimals;
        let pow = token_decimals + oracle_decimals;
        if pow < 2 {
            return 0;
        }
        let value_raw = token_amount.saturating_mul(price_with_decimals) / 10_i128.pow(pow - 2);
        value_raw.max(0)
    }

    async fn refresh_liquidator_obligation(&mut self, market_address: &str) {
        match self
            .ledger_reader
            .read_user_obligation(market_address, &self.liquidator_key)
            .await
        {
            Ok(obligation) => {
                info!(
                    %market_address,
                    deposits_count = obligation.deposits.len(),
                    "Refreshed liquidator obligation"
                );
                self.liquidator_obligations
                    .insert(market_address.to_string(), obligation);
            }
            Err(e) => {
                trace!(?e, %market_address, "no liquidator obligation");
                self.liquidator_obligations.remove(market_address);
            }
        }
    }
}
