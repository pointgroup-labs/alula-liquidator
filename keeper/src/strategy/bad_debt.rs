//! Bad-debt request initiator strategy.
//!
//! On a `Liquidate` event, inspects the borrower's residual obligation. When
//! the obligation is uncollateralized (or all deposits are below the dust
//! threshold) but still has debt, submits an `issue_cover_bad_debt` op so the
//! market socializes the loss promptly.

use {
    crate::{
        collect::Event,
        execute::{Action, stellar_tx::SubmitStellarTx},
        stellar::Gateway,
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending::{JToken, Obligation, ObligationKey, Underlying},
        ports::{ChainReader, EventCodec, OpBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    std::sync::Arc,
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

const MAX_RETRIES: u32 = 3;

pub struct BadDebtRequestInitiatorConfig {
    pub markets: Vec<String>,
}

pub struct BadDebtRequestInitiator {
    chain: Arc<dyn ChainReader>,
    gateway: Arc<Gateway>,
    skey: SigningKey,
    pkey: String,
    config: BadDebtRequestInitiatorConfig,
}

impl BadDebtRequestInitiator {
    pub fn new(
        chain: Arc<dyn ChainReader>,
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

        // Bug fix #4 reorder: decode the operation kind first, return early
        // if it's not Liquidate, only then parse topics that may not exist.
        match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Liquidate) => {}
            Ok(_) => return vec![],
            Err(e) => {
                debug!(?e, "decode_operation failed");
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
                return vec![];
            }
            Err(e) => {
                warn!(?e, ?borrower_key, "failed to parse borrower obligation");
                return vec![];
            }
        };

        if self
            .is_eligible_for_bad_debt_request_issuance(&market, &borrower_key, &borrower_obligation)
            .await
        {
            info!(
                ?borrower_key,
                "obligation eligible for bad debt request issuance"
            );
            if let Some(action) = self.build_issue_bad_debt(&market, &borrower_key) {
                return vec![action];
            }
        }

        vec![]
    }

    /// See pipeline doc-comment. Bug fix #1 lives here: the original code
    /// called `tokens_to_j_tokens_floor(j_tokens)` — passing j-tokens to a
    /// function expecting underlying tokens. We now use the typed
    /// `j_to_underlying_floor(JToken(...))` conversion that is impossible to
    /// invert by accident.
    async fn is_eligible_for_bad_debt_request_issuance(
        &self,
        market: &str,
        obl_key: &ObligationKey,
        obligation: &Obligation,
    ) -> bool {
        if obligation.borrows.is_empty() {
            debug!(?obl_key, "no borrows, not eligible");
            return false;
        }
        if obligation.deposits.is_empty() {
            info!(?obl_key, "obligation eligible: debt with no collateral");
            return true;
        }

        let market_data = match self.chain.read_market_data(market).await {
            Ok(md) => md,
            Err(e) => {
                warn!(?e, ?obl_key, %market, "failed to fetch market data");
                return false;
            }
        };

        let min_collateral_threshold_value = market_data.min_collateral_value_cents
            * 10_i128.pow(market_data.oracle_price_decimals)
            / 100;

        for deposit_pos in &obligation.deposits {
            let pool = match market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == deposit_pos.pool_address)
            {
                Some(p) => p,
                None => {
                    // Conservative: if we can't find the pool we can't value
                    // this deposit. Skipping it would let the loop fall through
                    // to the `eligible` default at the end and emit a spurious
                    // cover_bad_debt op.
                    warn!(?obl_key, pool_address = %deposit_pos.pool_address, "pool not in market data; bailing out of eligibility check");
                    return false;
                }
            };

            // Bug fix #1: typed conversion (j-tokens -> underlying), not the
            // inverse the original called by accident.
            let underlying_from_j = pool.j_to_underlying_floor(JToken(deposit_pos.j_tokens));
            let total_collateral = Underlying(
                underlying_from_j
                    .raw()
                    .saturating_add(deposit_pos.collateral),
            );

            let collateral_value = match total_collateral
                .raw()
                .checked_mul(pool.oracle_asset_price)
                .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            {
                Some(v) => v,
                None => {
                    // Same conservative posture as the missing-pool branch:
                    // unverifiable collateral value must not bias us toward
                    // eligibility.
                    warn!(?obl_key, pool_address = %pool.pool_address, "overflow calculating collateral value; bailing out");
                    return false;
                }
            };

            if collateral_value > min_collateral_threshold_value {
                info!(
                    ?obl_key,
                    pool_address = %pool.pool_address,
                    collateral_value,
                    min_collateral_threshold_value,
                    "still has liquidatable collateral, not eligible"
                );
                return false;
            }
        }

        info!(
            ?obl_key,
            "obligation eligible: all deposits below dust threshold"
        );
        true
    }

    fn build_issue_bad_debt(&self, market: &str, obl_key: &ObligationKey) -> Option<Action> {
        // pkey is captured for symmetry with other strategies; this op only
        // needs the market + obligation key.
        let _ = &self.pkey;
        match self.gateway.cover_bad_debt_op(market, obl_key) {
            Ok(op) => Some(Action::SubmitTx(SubmitStellarTx {
                op,
                signing_key: self.skey.clone(),
                max_retries: MAX_RETRIES,
                on_settle: None,
            })),
            Err(e) => {
                error!(?e, ?obl_key, %market, "failed to build cover_bad_debt op");
                None
            }
        }
    }
}
