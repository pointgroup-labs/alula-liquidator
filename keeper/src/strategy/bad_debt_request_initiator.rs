//! Bad-debt request initiator strategy. Initiates the bad debt request if needed.

use std::sync::Arc;

use ed25519_dalek::SigningKey;
use engine::{
    lending_model::{
        MarketData, Obligation, ObligationKey, profitability::cents_to_oracle_value_ceil,
    },
    ports::{EventCodec, LedgerReader, OperationEvent},
    reactor::{BoxFuture, Strategy},
};
use stellar_rpc_client::Event as SorobanEvent;
use tracing::{debug, error, info, warn};

use crate::{
    collect::{Event, stellar_ledger::NewLedger},
    execute::{Action, stellar_tx::SubmitStellarTx},
    metrics::BadDebtOutcome,
    stellar::client::Gateway,
    storage::obligations::ObligationsRepo,
};

pub struct BadDebtRequestInitiatorConfig {
    pub max_retries: u32,
    pub markets: Vec<String>,
    pub refresh_interval_blocks: u32,
}

pub struct BadDebtRequestInitiator {
    skey: SigningKey,
    gateway: Arc<Gateway>,
    obligations_repo: ObligationsRepo,
    ledger_reader: Arc<dyn LedgerReader>,
    config: BadDebtRequestInitiatorConfig,
}

impl BadDebtRequestInitiator {
    pub fn new(
        skey: SigningKey,
        gateway: Arc<Gateway>,
        obligations_repo: ObligationsRepo,
        ledger_reader: Arc<dyn LedgerReader>,
        config: BadDebtRequestInitiatorConfig,
    ) -> Self {
        Self { skey, config, gateway, ledger_reader, obligations_repo }
    }
}

impl Strategy<Event, Action> for BadDebtRequestInitiator {
    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewLedger(nl) => self.handle_new_ledger(nl).await,
            }
        })
    }
}

impl BadDebtRequestInitiator {
    async fn handle_new_ledger(&mut self, ledger: NewLedger) -> Vec<Action> {
        if !ledger.seq_num.is_multiple_of(self.config.refresh_interval_blocks) {
            return vec![];
        }

        let mut actions = vec![];

        let markets = self.config.markets.clone();
        for market in markets {
            let Ok(loaded_obligations) = self
                .obligations_repo
                .load_all(&market)
                .inspect_err(|e| error!(?e, "failed to load obligations"))
            else {
                continue;
            };

            let Ok(market_data) =
                self.ledger_reader.read_market_data(&market).await.inspect_err(|e| {
                    warn!(?e, %market, "failed to fetch market data");
                })
            else {
                continue;
            };

            // - Check eligibility for every obligation on a market -

            for (obligation_key, obligation) in &loaded_obligations {
                let Ok(eligible) = self
                    .is_eligible_for_bad_debt_request_issuance(
                        obligation_key,
                        obligation,
                        &market_data,
                    )
                    .inspect_err(|e| {
                        warn!(?e, ?obligation_key, "bad debt eligibility check failed");
                        BadDebtOutcome::EligibilityError.record();
                    })
                else {
                    continue;
                };

                if !eligible {
                    BadDebtOutcome::Ineligible.record();

                    continue;
                }
                info!(?obligation_key, "obligation eligible for bad debt request issuance");

                if let Ok(action) = self.build_issue_bad_debt(&market, obligation_key) {
                    BadDebtOutcome::Dispatched.record();
                    actions.push(action);
                } else {
                    BadDebtOutcome::BuildFailed.record();
                }
            }
        }

        actions
    }

    async fn handle_soroban_event(&self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();
        if !self.config.markets.contains(&market) {
            warn!(%market, "event from non-configured market");

            return vec![];
        }

        // TODO: Add 'claim_cover_bad_debt_result' and remove the obligation from the storage repo
        let Ok(OperationEvent::Liquidate) =
            self.gateway.decode_operation(&event).inspect_err(|e| {
                debug!(?e, "decode_operation failed");
                BadDebtOutcome::DecodeOpError.record();
            })
        else {
            return vec![];
        };

        let (borrower, borrow_pool, collateral_pool) = (
            self.gateway.decode_topic(&event, 2),
            self.gateway.decode_topic(&event, 3),
            self.gateway.decode_topic(&event, 4),
        );

        info!(
            %market, %borrower, %borrow_pool, %collateral_pool,
            ledger = event.ledger,
            "liquidation event detected"
        );

        let Ok(borrower_key) =
            self.gateway.parse_obligation_key_from_topic(&event, 2).inspect_err(|e| {
                warn!(?e, "cannot parse borrower obligation key");
                BadDebtOutcome::ParseError.record();
            })
        else {
            return vec![];
        };

        let Ok(obligation_opt) = self
            .gateway
            .parse_obligation_from_event_value(&event.value, "borrower_obligation", &borrower_key)
            .inspect_err(|e| {
                warn!(?e, ?borrower_key, "failed to parse borrower obligation");
                BadDebtOutcome::ParseError.record();
            })
        else {
            return vec![];
        };
        let Some(borrower_obligation) = obligation_opt else {
            debug!(?borrower_key, "borrower obligation removed; nothing to do");
            BadDebtOutcome::ObligationCleared.record();

            return vec![];
        };

        // - Check eligibility -

        let Ok(market_data) = self.ledger_reader.read_market_data(&market).await.inspect_err(|e| {
            warn!(?e, %market, "failed to fetch market data");
        }) else {
            return vec![];
        };
        let Ok(eligible) = self
            .is_eligible_for_bad_debt_request_issuance(
                &borrower_key,
                &borrower_obligation,
                &market_data,
            )
            .inspect_err(|e| {
                warn!(?e, ?borrower_key, "bad debt eligibility check failed");
                BadDebtOutcome::EligibilityError.record();
            })
        else {
            return vec![];
        };

        if !eligible {
            BadDebtOutcome::Ineligible.record();

            return vec![];
        }

        info!(?borrower_key, "obligation eligible for bad debt request issuance");

        if let Ok(action) = self.build_issue_bad_debt(&market, &borrower_key) {
            BadDebtOutcome::Dispatched.record();
            vec![action]
        } else {
            BadDebtOutcome::BuildFailed.record();

            vec![]
        }
    }

    fn is_eligible_for_bad_debt_request_issuance(
        &self,
        obl_key: &ObligationKey,
        obligation: &Obligation,
        market_data: &MarketData,
    ) -> anyhow::Result<bool> {
        if obligation.borrows.is_empty() {
            debug!(?obl_key, "no borrows, not eligible");

            return Ok(false);
        }
        if obligation.deposits.is_empty() {
            info!(?obl_key, "obligation eligible: debt with no collateral");

            return Ok(true);
        }

        let min_collateral_threshold_value = cents_to_oracle_value_ceil(
            // TODO: must be floor?
            market_data.min_collateral_value_cents,
            market_data.oracle_price_decimals,
        )?;

        for deposit_pos in &obligation.deposits {
            let Some(pool) =
                market_data.pools_data.iter().find(|p| p.pool_address == deposit_pos.pool_address)
            else {
                warn!(
                    ?obl_key,
                    pool_address = %deposit_pos.pool_address,
                    "pool not in market data; bailing out of eligibility check"
                );
                return Ok(false);
            };

            let underlying_from_j = pool.j_tokens_to_tokens_ceil(deposit_pos.j_tokens)?;
            let total_collateral = underlying_from_j.checked_add(deposit_pos.collateral)?;

            let collateral_value = total_collateral
                .checked_mul(pool.oracle_asset_price)?
                .saturating_div(10_i128.pow(pool.token_decimals));
            if collateral_value > min_collateral_threshold_value {
                info!(
                    ?obl_key,
                    pool_address = %pool.pool_address,
                    collateral_value,
                    min_collateral_threshold_value,
                    "still has liquidatable collateral, not eligible"
                );

                return Ok(false);
            }
        }

        info!(?obl_key, "obligation eligible: all deposits below dust threshold");

        Ok(true)
    }

    // Swapped to return Result instead of Option for better caller integration.
    fn build_issue_bad_debt(
        &self,
        market: &str,
        obl_key: &ObligationKey,
    ) -> anyhow::Result<Action> {
        let op = self.gateway.cover_bad_debt_op(market, obl_key).inspect_err(|e| {
            error!(?e, ?obl_key, %market, "failed to build cover_bad_debt op");
        })?;

        Ok(Action::SubmitTx(SubmitStellarTx {
            op,
            on_settle: None,
            signing_key: self.skey.clone(),
            max_submission_retries: self.config.max_retries,
        }))
    }
}
