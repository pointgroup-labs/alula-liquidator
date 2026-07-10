//! Withdrawer strategy: opportunistically pulls the liquidator's liquidity out of pools
//! if such a withdrawal doesn't yield a scarcity fee

use std::sync::Arc;

use ed25519_dalek::SigningKey;
use engine::{
    lending_model::{MarketData, Obligation, ObligationKey, PoolData, Underlying},
    ports::{EventCodec, LedgerReader, OperationBuilder, OperationEvent},
    reactor::{BoxFuture, Strategy},
};
use stellar_rpc_client::Event as SorobanEvent;
use tracing::{debug, error, info, warn};

use crate::{
    collect::{Event, stellar_ledger::NewLedger},
    execute::{Action, stellar_tx::SubmitStellarTx},
    metrics::WithdrawerOutcome,
    stellar::client::Gateway,
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

        Self { skey, config, gateway, ledger_reader, liquidator_key }
    }
}

impl Strategy<Event, Action> for Withdrawer {
    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewLedger(b) => self.handle_new_ledger(b).await,
            }
        })
    }
}

impl Withdrawer {
    // TODO: Logically speaking, this can be omitted in the future since only events
    // that increase available liquidity or a liquidation event for this liquidator
    // are reasonable causes for withdrawal. Acts as a safety measure for now
    async fn handle_new_ledger(&mut self, ledger: NewLedger) -> Vec<Action> {
        if !ledger.seq_num.is_multiple_of(self.config.refresh_interval_blocks) {
            return vec![];
        }

        let mut actions = vec![];
        let markets = self.config.markets.clone();

        for market in markets {
            let Ok(liquidator_obligation) = self
                .ledger_reader
                .read_user_obligation(&market, &self.liquidator_key)
                .await
                .inspect_err(|e| {
                    debug!(%e);
                    info!(
                        market,
                        "no liquidator obligation on the market when handling new ledger"
                    );
                })
            else {
                continue;
            };
            let Ok(market_data) =
                self.ledger_reader.read_market_data(&market).await.inspect_err(|err| {
                    warn!(?err, ?market, "failed to read market data");
                    WithdrawerOutcome::NoMarketData.record();
                })
            else {
                continue;
            };

            actions.extend(self.find_withdrawal_opportunities(
                &market,
                market_data,
                liquidator_obligation,
            ))
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
                info!(?event, "received increasing available liquidity event");
            }
            _ => {
                debug!(?event, "received non-increasing available liquidity event");

                return vec![];
            }
        }

        let Ok(liquidator_obligation) = self
            .ledger_reader
            .read_user_obligation(&market, &self.liquidator_key)
            .await
            .inspect_err(|e| {
                debug!(%e);
                info!(%market, "no liquidator obligation on the market when handling new event");
            })
        else {
            return vec![];
        };
        let Ok(market_data) =
            self.ledger_reader.read_market_data(&market).await.inspect_err(|err| {
                warn!(?err, ?market, "failed to read market data");
                WithdrawerOutcome::NoMarketData.record();
            })
        else {
            return vec![];
        };

        self.find_withdrawal_opportunities(&market, market_data, liquidator_obligation)
    }

    fn find_withdrawal_opportunities(
        &self,
        market: &str,
        market_data: MarketData,
        liquidator_obligation: Obligation,
    ) -> Vec<Action> {
        let mut actions = vec![];

        for deposit_pos in &liquidator_obligation.deposits {
            let Some(pool) =
                market_data.pools_data.iter().find(|p| p.pool_address == deposit_pos.pool_address)
            else {
                warn!(pool = deposit_pos.pool_address, "pool not found");
                WithdrawerOutcome::PoolMissing.record();

                continue;
            };

            let Ok(max_withdrawal) = pool
                .compute_max_safe_withdrawal(self.config.utilization_safety_margin_bps)
                .inspect_err(|e| {
                    warn!(?e, pool = %pool.pool_address, "max_safe_withdrawal failed");
                    WithdrawerOutcome::MaxWithdrawalError.record();
                })
            else {
                continue;
            };
            if max_withdrawal == Underlying::ZERO {
                WithdrawerOutcome::PoolAtCapacity.record();

                continue;
            }

            let Ok(liquidator_underlying) =
                pool.j_tokens_to_tokens_floor(deposit_pos.j_tokens).inspect_err(|e| {
                    warn!(?e, pool = %pool.pool_address, "j_tokens_to_tokens_floor failed");
                    WithdrawerOutcome::ConversionError.record();
                })
            else {
                continue;
            };
            if liquidator_underlying == Underlying::ZERO {
                WithdrawerOutcome::EmptyPosition.record();

                continue;
            }

            let mut withdrawal_amount: Underlying = liquidator_underlying.min(max_withdrawal);
            let Some(withdrawal_value_cents) =
                self.compute_withdrawal_value_cents(pool, &market_data, withdrawal_amount)
            else {
                error!("failed to compute withdrawal value cents");

                return vec![];
            };

            if withdrawal_amount == liquidator_underlying {
                withdrawal_amount = Underlying(i128::MAX);
            }

            if withdrawal_value_cents >= self.config.min_withdraw_value_cents {
                info!(
                    pool_address = %pool.pool_address,
                    current_utilization = pool.utilization_ratio_bps().unwrap_or_default(),
                    ?max_withdrawal,
                    ?liquidator_underlying,
                    ?withdrawal_amount,
                    withdrawal_value_cents,
                    "creating withdrawal action"
                );

                match self.build_withdrawal_action(market, &pool.pool_address, withdrawal_amount) {
                    Ok(action) => {
                        actions.push(action);
                        WithdrawerOutcome::Dispatched.record();
                    }
                    Err(e) => {
                        error!(?e, "failed to build withdrawal action");
                        WithdrawerOutcome::BuildError.record();
                    }
                };
            } else {
                WithdrawerOutcome::BelowThreshold.record();
            }
        }

        actions
    }

    fn build_withdrawal_action(
        &self,
        market: &str,
        pool_address: &str,
        amount: Underlying,
    ) -> anyhow::Result<Action> {
        let op = self.gateway.withdraw_op(market, &self.liquidator_key, pool_address, amount)?;
        Ok(Action::SubmitTx(SubmitStellarTx {
            op,
            on_settle: None,
            signing_key: self.skey.clone(),
            max_submission_retries: self.config.max_retries,
        }))
    }

    fn compute_withdrawal_value_cents(
        &self,
        pool: &PoolData,
        market_data: &MarketData,
        token_amount: Underlying,
    ) -> Option<i128> {
        let price_with_decimals = pool.oracle_asset_price;
        if price_with_decimals <= 0 {
            warn!(price_with_decimals, "negative oracle price");

            return None;
        }

        let oracle_decimals = market_data.oracle_price_decimals;
        let token_decimals = pool.token_decimals;
        let pow = token_decimals + oracle_decimals;
        if pow < 2 {
            warn!(?pool, pow, token_decimals, oracle_decimals, "unexpected asset/oracle decimals");

            return None;
        }

        let numerator = token_amount.checked_mul(price_with_decimals).ok()?;
        let value_raw = numerator / 10_i128.pow(pow - 2);

        Some(value_raw.max(0))
    }
}
