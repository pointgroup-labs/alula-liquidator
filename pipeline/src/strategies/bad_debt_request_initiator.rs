use {
    crate::{
        executors::tx_executor::SubmitStellarTx,
        helper::{self, OperationEvent},
        types::{Action, BoxFuture, Event, ObligationKey, Strategy},
    },
    anyhow,
    ed25519_dalek::SigningKey,
    stellar_rpc_client::{Client, Event as SorobanEvent},
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{debug, error, info, warn},
    url::Url,
};

pub struct BadDebtRequestInitiatorConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
}

#[derive(Debug)]
pub struct BadDebtRequestInitiator {
    rpc: Client,
    pkey: String,
    skey: SigningKey,
}

impl BadDebtRequestInitiator {
    pub fn try_create(
        config: BadDebtRequestInitiatorConfig,
        skey: &SigningKey,
    ) -> anyhow::Result<Self> {
        let rpc = Client::new(config.rpc_url.as_str())?;
        let pkey = ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
            skey.verifying_key().to_bytes(),
        ))))
        .to_string();

        Ok(Self {
            rpc,
            pkey,
            skey: skey.clone(),
        })
    }
}

impl Strategy<Event, Action> for BadDebtRequestInitiator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewBlock(_) => vec![],
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        // no syncing needed
        Box::pin(async { Ok(()) })
    }
}

impl BadDebtRequestInitiator {
    async fn handle_soroban_event(&self, event: SorobanEvent) -> Vec<Action> {
        let market = event.contract_id.clone();

        // TODO: Generally, this has to account for cases when
        // no-one has liquidated a position and it contains bad debt
        match helper::decode_operation_event(&event) {
            Ok(e) => {
                if !matches!(e, OperationEvent::Liquidate) {
                    return vec![];
                }
            }
            Err(e) => {
                info!(?e);

                return vec![];
            }
        };

        let borrower = helper::decode_topic(&event, 2);
        let borrow_pool = helper::decode_topic(&event, 3);
        let collateral_pool = helper::decode_topic(&event, 4);

        info!(
            %market,
            %borrower,
            %borrow_pool,
            %collateral_pool,
            ledger = event.ledger,
            "liquidation event detected"
        );

        let borrower_key = match helper::parse_obligation_key_from_topic(&event, 2) {
            Ok(k) => k,
            Err(e) => {
                warn!(?e, "cannot parse borrower obligation key");

                return vec![];
            }
        };

        let borrower_obligation = match helper::parse_obligation_from_event_value(
            &event.value,
            "borrower_obligation",
            &borrower_key,
        ) {
            Ok(Some(obl)) => obl,
            Ok(None) => {
                debug!(
                    ?borrower_key,
                    "borrower obligation is completely removed, no bad debt request needed"
                );

                return vec![];
            }
            Err(e) => {
                warn!(
                    ?e,
                    ?borrower_key,
                    "failed to parse borrower obligation from event"
                );

                return vec![];
            }
        };

        // Check if obligation is eligible for bad debt coverage
        if self
            .is_eligible_for_bad_debt_request_issuance(&market, &borrower_key, &borrower_obligation)
            .await
        {
            info!(
                ?borrower_key,
                "obligation eligible for bad debt request issuance"
            );

            if let Some(action) = self
                .build_issue_bad_debt_request(&market, &borrower_key)
                .await
            {
                return vec![action];
            }
        }

        vec![]
    }

    /// Check if obligation is eligible for bad debt request.
    ///
    /// # Eligibility Criteria (from contract):
    /// 1. Obligation must have borrows (has debt)
    /// 2. Either:
    ///    a. No deposits exist at all (no collateral), OR
    ///    b. Every collateral position has value < min_collateral_value_cents
    async fn is_eligible_for_bad_debt_request_issuance(
        &self,
        market: &str,
        obl_key: &ObligationKey,
        obligation: &crate::types::Obligation,
    ) -> bool {
        // Criterion 1: Must have borrows
        if obligation.borrows.is_empty() {
            debug!(?obl_key, "no borrows, not eligible");

            return false;
        }

        // Criterion 2a: If no deposits exist, eligible immediately
        if obligation.deposits.is_empty() {
            info!(
                ?obl_key,
                borrows_count = obligation.borrows.len(),
                "obligation eligible: has debt but no collateral deposits"
            );

            return true;
        }

        // Criterion 2b: Fetch market data to check if all deposits are below threshold
        let market_data =
            match helper::simulate_get_market_data(&self.rpc, market, &self.pkey).await {
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
                .find(|p| p.pool_address == deposit_pos.pool_address) // TODO: Optimize, maybe
            {
                Some(p) => p,
                None => {
                    warn!(?obl_key, pool_address = %deposit_pos.pool_address, "pool not found in market data");

                    continue;
                }
            };

            // Calculate total collateral in tokens
            let tokens_from_j_tokens = pool.tokens_to_j_tokens_floor(deposit_pos.j_tokens);
            let total_collateral = tokens_from_j_tokens + deposit_pos.collateral;

            // Calculate collateral value
            let collateral_value = match total_collateral
                .checked_mul(pool.oracle_asset_price)
                .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            {
                Some(v) => v,
                None => {
                    warn!(?obl_key, pool_address = %pool.pool_address, "overflow calculating collateral value");

                    continue;
                }
            };

            // If at least one position has liquidatable collateral → NOT eligible
            if collateral_value > min_collateral_threshold_value {
                info!(
                    ?obl_key,
                    pool_address = %pool.pool_address,
                    collateral_value,
                    min_collateral_threshold_value,
                    "obligation has liquidatable collateral, not eligible for bad debt"
                );

                return false;
            }
        }

        info!(
            ?obl_key,
            borrows_count = obligation.borrows.len(),
            deposits_count = obligation.deposits.len(),
            "obligation eligible: has debt but all collateral positions are below threshold"
        );

        true
    }

    /// Build bad debt request transaction
    async fn build_issue_bad_debt_request(
        &self,
        market: &str,
        obl_key: &ObligationKey,
    ) -> Option<Action> {
        const MAX_RETRIES: u32 = 3; // let's have one such constant per file

        match helper::build_issue_cover_bad_debt_op(market, obl_key) {
            Ok(op) => Some(Action::SubmitTx(SubmitStellarTx {
                op,
                signing_key: self.skey.clone(),
                max_retries: MAX_RETRIES,
            })),
            Err(e) => {
                error!(?e, ?obl_key, %market, "failed to build issue_cover_bad_debt op");

                None
            }
        }
    }
}
