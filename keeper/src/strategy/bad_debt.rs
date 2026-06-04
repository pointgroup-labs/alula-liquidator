//! Bad-debt request initiator strategy.
//!
//! On a `Liquidate` event, inspects the borrower's residual obligation. When
//! the obligation is uncollateralized (or all deposits are below the dust
//! threshold) but still has debt, submits an `issue_cover_bad_debt` op so the
//! market socializes the loss promptly.

use {
    crate::{collect::Event, execute::Action, execute::stellar_tx::SubmitStellarTx, stellar::Gateway},
    ed25519_dalek::SigningKey,
    engine::{
        lending_model::{
            Obligation, ObligationKey,
            profitability::cents_to_oracle_value_floor,
        },
        ports::{EventCodec, LedgerReader, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    metrics::counter,
    std::sync::Arc,
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

pub struct BadDebtRequestInitiatorConfig {
    pub markets: Vec<String>,
    pub max_retries: u32,
}

pub struct BadDebtRequestInitiator {
    chain: Arc<dyn LedgerReader>,
    gateway: Arc<Gateway>,
    skey: SigningKey,
    pkey: String,
    config: BadDebtRequestInitiatorConfig,
}

impl BadDebtRequestInitiator {
    pub fn new(
        chain: Arc<dyn LedgerReader>,
        gateway: Arc<Gateway>,
        skey: SigningKey,
        pkey: String,
        config: BadDebtRequestInitiatorConfig,
    ) -> Self {
        Self {
            chain,
            gateway,
            skey,
            pkey,
            config,
        }
    }
}

impl Strategy<Event, Action> for BadDebtRequestInitiator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewBlock(_) => vec![],
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        // TODO: Iterate once in a while and check for eligibility
        Box::pin(async { Ok(()) })
    }
}

impl BadDebtRequestInitiator {
    async fn handle_soroban_event(&self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();

        // Filter to configured markets so the strategy only reacts to liquidations
        // on the markets it was wired up for.
        if !self.config.markets.iter().any(|m| m == &market) {
            debug!(%market, "ignoring event from non-configured market");

            return vec![];
        }

        match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Liquidate) => {}
            Ok(_) => return vec![],
            Err(e) => {
                debug!(?e, "decode_operation failed");
                counter!("bad_debt_outcome_total", "outcome" => "decode_op_error").increment(1);

                return vec![];
            }
        }

        let borrower = self.gateway.decode_topic(&event, 2);
        let borrow_pool = self.gateway.decode_topic(&event, 3);
        let collateral_pool = self.gateway.decode_topic(&event, 4);

        info!(
            %market, %borrower, %borrow_pool, %collateral_pool,
            ledger = event.ledger,
            "liquidation event detected"
        );

        let borrower_key = match self.gateway.parse_obligation_key_from_topic(&event, 2) {
            Ok(k) => k,
            Err(e) => {
                warn!(?e, "cannot parse borrower obligation key");
                counter!("bad_debt_outcome_total", "outcome" => "parse_error").increment(1);

                return vec![];
            }
        };

        let borrower_obligation = match self.gateway.parse_obligation_from_event_value(
            &event.value,
            "borrower_obligation",
            &borrower_key,
        ) {
            Ok(Some(obl)) => obl,
            Ok(None) => {
                debug!(?borrower_key, "borrower obligation removed; nothing to do");
                counter!("bad_debt_outcome_total", "outcome" => "obligation_cleared").increment(1);

                return vec![];
            }
            Err(e) => {
                warn!(?e, ?borrower_key, "failed to parse borrower obligation");
                counter!("bad_debt_outcome_total", "outcome" => "parse_error").increment(1);

                return vec![];
            }
        };

        let eligible = match self
            .is_eligible_for_bad_debt_request_issuance(&market, &borrower_key, &borrower_obligation)
            .await
        {
            Ok(eligible) => eligible,
            Err(e) => {
                warn!(?e, ?borrower_key, "bad debt eligibility check failed");
                counter!("bad_debt_outcome_total", "outcome" => "eligibility_error").increment(1);

                return vec![];
            }
        };

        if eligible {
            info!(
                ?borrower_key,
                "obligation eligible for bad debt request issuance"
            );
            if let Some(action) = self.build_issue_bad_debt(&market, &borrower_key) {
                counter!("bad_debt_outcome_total", "outcome" => "dispatched").increment(1);
                return vec![action];
            }
            counter!("bad_debt_outcome_total", "outcome" => "build_failed").increment(1);
        } else {
            counter!("bad_debt_outcome_total", "outcome" => "ineligible").increment(1);
        }

        vec![]
    }

    async fn is_eligible_for_bad_debt_request_issuance(
        &self,
        market: &str,
        obl_key: &ObligationKey,
        obligation: &Obligation,
    ) -> anyhow::Result<bool> {
        if obligation.borrows.is_empty() {
            debug!(?obl_key, "no borrows, not eligible");

            return Ok(false);
        }
        if obligation.deposits.is_empty() {
            info!(?obl_key, "obligation eligible: debt with no collateral");

            return Ok(true);
        }

        let market_data = match self.chain.read_market_data(market).await {
            Ok(md) => md,
            Err(e) => {
                warn!(?e, ?obl_key, %market, "failed to fetch market data");

                return Ok(false);
            }
        };

        let min_collateral_threshold_value = cents_to_oracle_value_floor(
            market_data.min_collateral_value_cents,
            market_data.oracle_price_decimals,
        )?;

        for deposit_pos in &obligation.deposits {
            let pool = match market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == deposit_pos.pool_address)
            {
                Some(p) => p,
                None => {
                    warn!(?obl_key, pool_address = %deposit_pos.pool_address, "pool not in market data; bailing out of eligibility check");

                    // TODO: Internal Error?
                    return Ok(false);
                }
            };

            let underlying_from_j = pool.j_tokens_to_tokens_ceil(deposit_pos.j_tokens)?;
            let total_collateral = underlying_from_j + deposit_pos.collateral;

            let collateral_value = total_collateral.checked_mul(pool.oracle_asset_price)?;

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
        
        info!(
            ?obl_key,
            "obligation eligible: all deposits below dust threshold"
        );

        Ok(true)
    }

    fn build_issue_bad_debt(&self, market: &str, obl_key: &ObligationKey) -> Option<Action> {
        // pkey is captured for symmetry with other strategies; this op only
        // needs the market + obligation key.
        let _ = &self.pkey;
        match self.gateway.cover_bad_debt_op(market, obl_key) {
            Ok(op) => Some(Action::SubmitTx(SubmitStellarTx {
                op,
                signing_key: self.skey.clone(),
                max_retries: self.config.max_retries,
                on_settle: None,
            })),
            Err(e) => {
                error!(?e, ?obl_key, %market, "failed to build cover_bad_debt op");
                None
            }
        }
    }
}
