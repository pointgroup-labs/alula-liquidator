//! Bad-debt request initiator strategy. Initiates the bad debt request if needed.

use std::sync::Arc;

use ed25519_dalek::SigningKey;
use engine::{
    lending_model::{
        MarketData, Obligation, ObligationKey,
        liquidation::{DustRules, has_liquidatable_collateral, seizable_collateral_covers_debt},
        profitability::cents_to_oracle_value_ceil,
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
    stellar::{client::Gateway, errors::is_obligation_does_not_exist},
    storage::obligations::ObligationsRepo,
};

pub struct BadDebtRequestInitiatorConfig {
    pub max_retries: u32,
    pub markets: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub dust_rules: DustRules,
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
                if !self.judge_eligibility(obligation_key, obligation, &market_data) {
                    continue;
                }

                // The cache is only as fresh as the last event applied to it, and
                // events behind the saved cursor are never replayed.
                let fresh = match self
                    .ledger_reader
                    .read_user_obligation(&market, obligation_key)
                    .await
                {
                    Ok(fresh) => fresh,
                    Err(e) if is_obligation_does_not_exist(&e) => {
                        info!(?obligation_key, %market, "obligation gone on chain; dropping row");
                        if let Err(e) = self.obligations_repo.delete(&market, obligation_key) {
                            warn!(?e, ?obligation_key, %market, "failed to delete obligation");
                        }
                        BadDebtOutcome::ObligationCleared.record();

                        continue;
                    }
                    Err(e) => {
                        warn!(?e, ?obligation_key, %market, "obligation re-read failed; skipping");
                        BadDebtOutcome::EligibilityError.record();

                        continue;
                    }
                };
                if !self.judge_eligibility(obligation_key, &fresh, &market_data) {
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

    /// Eligibility with its outcome recorded; an evaluation error counts as ineligible.
    fn judge_eligibility(
        &self,
        obl_key: &ObligationKey,
        obligation: &Obligation,
        market_data: &MarketData,
    ) -> bool {
        match self.is_eligible_for_bad_debt_request_issuance(obl_key, obligation, market_data) {
            Ok(true) => true,
            Ok(false) => {
                BadDebtOutcome::Ineligible.record();

                false
            }
            Err(e) => {
                warn!(?e, ?obl_key, "bad debt eligibility check failed");
                BadDebtOutcome::EligibilityError.record();

                false
            }
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
        if obligation.open_bad_debt_requests > 0 {
            debug!(
                ?obl_key,
                open = obligation.open_bad_debt_requests,
                "bad debt request already open, not eligible until claimed"
            );

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

        if self.config.dust_rules == DustRules::NoDust {
            if seizable_collateral_covers_debt(obligation, market_data)? {
                info!(?obl_key, "solvent, left to liquidation; not eligible");

                return Ok(false);
            }
            // Measured on $2.40 against $3: covering first costs the debt's
            // lenders $3.00, liquidating first $0.82.
            if has_liquidatable_collateral(obligation, market_data)? {
                info!(?obl_key, "collateral a liquidation can still take; not eligible");

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

#[cfg(test)]
mod tests {
    use engine::lending_model::{BorrowPosition, DepositPosition};

    use super::*;
    use crate::{
        storage::SqliteStore,
        strategy::liquidator::tests::{OBLIGATION_GONE, StubReader, market_data},
    };

    const MARKET: &str = "CBP76I2FRMUKYKIYYBKN3DH7TSWMFAJF2WTDC7Q2OHQFBIVWP7CAAI5Q";
    const USER: &str = "GDGRMW55DHLHUBOCM6OFWAKCQWASJTBCFYC3NRW3NAHLPR7QVPR6JXQI";

    /// Debt with no collateral left: eligible on its face.
    fn residual_debt(open_bad_debt_requests: u32) -> Obligation {
        Obligation {
            borrows: vec![BorrowPosition {
                d_tokens: 1_000.into(),
                pool_address: "POOL_A".to_string(),
            }],
            deposits: Vec::new(),
            open_bad_debt_requests,
        }
    }

    /// An initiator whose cache holds [`USER`]'s residual debt as the keeper last
    /// saw it, and whose chain read answers `on_chain`.
    fn initiator(on_chain: Result<Obligation, String>) -> BadDebtRequestInitiator {
        initiator_with(DustRules::Sweep, residual_debt(0), market_data(&[], &[]), on_chain)
    }

    fn initiator_with(
        dust_rules: DustRules,
        cached: Obligation,
        market_data: MarketData,
        on_chain: Result<Obligation, String>,
    ) -> BadDebtRequestInitiator {
        let store = SqliteStore::open(":memory:").expect("in-memory sqlite");
        let rpc_url = "http://127.0.0.1:1".parse().expect("valid url");
        let gateway = Arc::new(Gateway::new(&rpc_url, "G".repeat(56)).expect("gateway"));
        let obligations_repo = store.obligations();
        obligations_repo
            .put(MARKET, &ObligationKey::new(USER.to_string()), &cached)
            .expect("seed row");

        BadDebtRequestInitiator::new(
            SigningKey::from_bytes(&[7_u8; 32]),
            gateway,
            obligations_repo,
            Arc::new(StubReader::new(market_data).with_obligation(on_chain)),
            BadDebtRequestInitiatorConfig {
                max_retries: 1,
                markets: vec![MARKET.to_string()],
                refresh_interval_blocks: 5,
                dust_rules,
            },
        )
    }

    /// The measured $4.50 / $1 obligation: its one deposit is under the $5
    /// floor, yet it covers its debt.
    fn solvent_dust() -> (Obligation, MarketData) {
        let mut market_data = market_data(&["POOL_A", "POOL_B"], &[]);
        market_data.min_collateral_value_cents = 500;
        for pool in &mut market_data.pools_data {
            pool.oracle_asset_price = 100_000_000_000_000;
            pool.total_borrowed = 1_000_000_000.into();
            pool.total_d_tokens = 1_000_000_000.into();
        }
        let obligation = Obligation {
            borrows: vec![BorrowPosition {
                d_tokens: 10_000_000.into(),
                pool_address: "POOL_A".to_string(),
            }],
            deposits: vec![DepositPosition {
                j_tokens: 0.into(),
                collateral: 45_000_000.into(),
                pool_address: "POOL_B".to_string(),
            }],
            open_bad_debt_requests: 0,
        };

        (obligation, market_data)
    }

    fn sweep(initiator: &mut BadDebtRequestInitiator) -> Vec<Action> {
        let runtime =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");

        runtime.block_on(initiator.handle_new_ledger(NewLedger { seq_num: 5 }))
    }

    fn is_cached(initiator: &BadDebtRequestInitiator) -> bool {
        let rows = initiator.obligations_repo.load_all(MARKET).expect("load");

        rows.contains_key(&ObligationKey::new(USER.to_string()))
    }

    #[test]
    fn an_obligation_gone_on_chain_is_dropped_not_dispatched() {
        // Mainnet since 2026-09-29: the claim closed it, the cache never heard.
        let mut initiator = initiator(Err(OBLIGATION_GONE.to_string()));

        assert!(sweep(&mut initiator).is_empty());
        assert!(!is_cached(&initiator));
    }

    #[test]
    fn a_still_eligible_obligation_is_dispatched_once() {
        let mut initiator = initiator(Ok(residual_debt(0)));

        assert_eq!(sweep(&mut initiator).len(), 1);
        assert!(is_cached(&initiator));
    }

    #[test]
    fn an_open_request_on_chain_holds_dispatch() {
        let mut initiator = initiator(Ok(residual_debt(1)));

        assert!(sweep(&mut initiator).is_empty());
        assert!(is_cached(&initiator));
    }

    #[test]
    fn a_no_dust_market_leaves_solvent_dust_to_liquidation() {
        for (dust_rules, dispatched) in [(DustRules::Sweep, 1), (DustRules::NoDust, 0)] {
            let (obligation, market_data) = solvent_dust();
            let mut initiator =
                initiator_with(dust_rules, obligation.clone(), market_data, Ok(obligation));

            assert_eq!(sweep(&mut initiator).len(), dispatched, "{dust_rules:?}");
        }
    }

    /// The measured $2.40 / $3 obligation: under the floor and insolvent.
    fn insolvent_dust() -> (Obligation, MarketData) {
        let (mut obligation, market_data) = solvent_dust();
        obligation.borrows[0].d_tokens = 30_000_000.into();
        obligation.deposits[0].collateral = 24_000_000.into();

        (obligation, market_data)
    }

    #[test]
    fn a_no_dust_market_liquidates_insolvent_dust_before_covering_it() {
        for (dust_rules, dispatched) in [(DustRules::Sweep, 1), (DustRules::NoDust, 0)] {
            let (obligation, market_data) = insolvent_dust();
            let mut initiator =
                initiator_with(dust_rules, obligation.clone(), market_data, Ok(obligation));

            assert_eq!(sweep(&mut initiator).len(), dispatched, "{dust_rules:?}");
        }
    }

    #[test]
    fn collateral_too_small_to_liquidate_is_covered() {
        // Worth one stroop of debt at most, which burns no d-token at a 1.1
        // d-rate (`NonPositiveSharesAmount`).
        let (obligation, mut market_data) = insolvent_dust();
        market_data.pools_data[0].total_borrowed = 1_100_000_000.into();
        market_data.pools_data[1].oracle_asset_price = 1;
        let mut initiator =
            initiator_with(DustRules::NoDust, obligation.clone(), market_data, Ok(obligation));

        assert_eq!(sweep(&mut initiator).len(), 1);
    }

    #[test]
    fn a_failed_reread_holds_dispatch_and_keeps_the_row() {
        let mut initiator = initiator(Err("connection refused".to_string()));

        assert!(sweep(&mut initiator).is_empty());
        assert!(is_cached(&initiator));
    }
}
