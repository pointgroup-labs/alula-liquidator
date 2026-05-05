use {
    crate::{
        constants::REFRESH_INTERVAL_BLOCKS,
        executors::tx_executor::SubmitStellarTx,
        helper::{self, OperationEvent},
        types::{
            Action, BoxFuture, Event, MarketData, Obligation, ObligationKey, PoolData, Strategy,
        },
    },
    ed25519_dalek::SigningKey,
    std::collections::HashMap,
    stellar_rpc_client::{Client, Event as SorobanEvent},
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{error, info, warn},
    url::Url,
};

const UTILIZATION_SAFETY_MARGIN_BPS: i128 = 500;
const BPS_FACTOR: i128 = 10_000;

pub struct WithdrawerConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
    /// Minimum withdrawal value in cents (e.g. 500 = $5.00)
    pub min_withdraw_value_cents: i128,
}

#[derive(Debug, Clone)]
pub struct WithdrawerState {
    /// Tracks liquidator's j_token balance for each pool
    pub liquidator_shares: HashMap<String, i128>, // pool_address -> j_tokens
}

pub struct Withdrawer {
    rpc: Client,
    pkey: String,
    skey: SigningKey,
    config: WithdrawerConfig,
    liquidator_key: ObligationKey,
    market_data: HashMap<String, MarketData>,
    liquidator_obligations: HashMap<String, Obligation>,
}

impl Withdrawer {
    pub fn try_create(config: WithdrawerConfig, skey: &SigningKey) -> anyhow::Result<Self> {
        let rpc = Client::new(config.rpc_url.as_str())?;
        let pkey = ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
            skey.verifying_key().to_bytes(),
        ))))
        .to_string();

        let liquidator_key = ObligationKey::new(pkey.clone());

        Ok(Self {
            rpc,
            pkey,
            skey: skey.clone(),
            config,
            market_data: HashMap::new(),
            liquidator_obligations: HashMap::new(),
            liquidator_key,
        })
    }

    /// Computes the max safe withdrawal amount that doesn't exceed the utilization ratio limit given `[UTILIZATION_SAFETY_MARGIN_BPS]`
    fn compute_max_safe_withdrawal(&self, pool: &PoolData) -> i128 {
        let current_utilization_bps = pool.utilization_ratio_bps();
        let utilization_considered_safe = pool
            .utilization_ratio_limit_bps
            .saturating_sub(UTILIZATION_SAFETY_MARGIN_BPS);

        if current_utilization_bps >= utilization_considered_safe {
            return 0;
        }

        let min_allowed_total_supply =
            (pool.total_borrowed * BPS_FACTOR) / utilization_considered_safe;
        let max_withdrawal = pool.total_supply.saturating_sub(min_allowed_total_supply);

        max_withdrawal
    }

    /// Calculate the USD value of a token amount in cents
    fn calculate_withdrawal_value_cents(
        &self,
        market_data: &MarketData,
        pool: &PoolData,
        token_amount: i128,
    ) -> i128 {
        let price_with_decimals = pool.oracle_asset_price;
        let oracle_decimals = market_data.oracle_price_decimals as u32;
        let token_decimals = pool.token_decimals;

        let value_raw = (token_amount * price_with_decimals)
            / (10_i128.pow(token_decimals + oracle_decimals - 2)); // -2 for cents

        value_raw.max(0)
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        let Ok(operation_event) = helper::decode_operation_event(&event) else {
            return vec![];
        };

        if matches!(
            operation_event,
            OperationEvent::Deposit | OperationEvent::Repay
        ) {
            info!("Detected liquidity increase event: {:?}", event);
            return self
                .check_withdrawal_opportunities(&event.contract_id)
                .await;
        }

        vec![]
    }

    async fn check_withdrawal_opportunities(&self, market_address: &str) -> Vec<Action> {
        let mut actions = vec![];

        let market_data = match self.market_data.get(market_address) {
            Some(data) => data,
            None => {
                warn!(?market_address, "No market data available");

                return actions;
            }
        };

        let liquidator_obligation = match self.liquidator_obligations.get(market_address) {
            Some(obligations) => obligations,
            None => {
                info!("No liquidator obligations for market {}", market_address);

                return actions;
            }
        };

        for deposit_pos in &liquidator_obligation.deposits {
            let Some(pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == deposit_pos.pool_address)
            else {
                warn!("Pool not found: {}", deposit_pos.pool_address);

                continue;
            };

            let max_withdrawal = self.compute_max_safe_withdrawal(pool);

            let liquidator_underlying_tokens = pool.j_tokens_to_tokens_floor(deposit_pos.j_tokens);
            let mut withdrawal_amount = liquidator_underlying_tokens.min(max_withdrawal);

            let withdrawal_value_cents =
                self.calculate_withdrawal_value_cents(market_data, pool, withdrawal_amount);

            if withdrawal_amount == liquidator_underlying_tokens {
                withdrawal_amount = i128::MAX;
            }

            if withdrawal_value_cents >= self.config.min_withdraw_value_cents {
                info!(
                    pool_address = %pool.pool_address,
                    current_utilization = pool.utilization_ratio_bps(),
                    max_withdrawal,
                    liquidator_tokens = liquidator_underlying_tokens,
                    withdrawal_amount,
                    value_cents = withdrawal_value_cents,
                    "Creating withdrawal action"
                );

                if let Some(action) = self
                    .build_withdraw_action(market_address, &pool.pool_address, withdrawal_amount)
                    .await
                {
                    actions.push(action);
                }
            }
        }

        actions
    }

    async fn build_withdraw_action(
        &self,
        market_address: &str,
        pool_address: &str,
        amount: i128,
    ) -> Option<Action> {
        // Use direct withdraw operation instead of batch
        match helper::build_withdraw_op(market_address, &self.liquidator_key, pool_address, amount)
        {
            Ok(op) => Some(Action::SubmitTx(SubmitStellarTx {
                op,
                signing_key: self.skey.clone(),
                max_retries: 3,
            })),
            Err(e) => {
                error!(?e, %market_address, %pool_address, amount, "failed to build withdraw op");
                None
            }
        }
    }
}

impl Strategy<Event, Action> for Withdrawer {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewBlock(block) => {
                    // Periodic check for withdrawal opportunities
                    if block.number % REFRESH_INTERVAL_BLOCKS == 0 {
                        let mut actions = vec![];
                        for market_address in &self.config.markets {
                            let market_actions =
                                self.check_withdrawal_opportunities(market_address).await;

                            actions.extend(market_actions);
                        }
                        actions
                    } else {
                        vec![]
                    }
                }
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(markets = ?self.config.markets, "Withdrawer: syncing state");

            for market in &self.config.markets {
                match helper::simulate_get_market_data(&self.rpc, market, &self.pkey).await {
                    Ok(market_data) => {
                        info!(market, "Loaded market data for withdrawer");
                        self.market_data.insert(market.clone(), market_data);
                    }
                    Err(e) => error!(?e, ?market, "Failed to get market data for withdrawer"),
                }

                match helper::simulate_get_user_obligation(
                    &self.rpc,
                    market,
                    &self.pkey,
                    &self.liquidator_key,
                )
                .await
                {
                    Ok(obligation) => {
                        info!(
                            market,
                            deposits_count = obligation.deposits.len(),
                            "Loaded liquidator obligation for withdrawer"
                        );

                        self.liquidator_obligations
                            .insert(market.clone(), obligation);
                    }
                    Err(e) => {
                        info!(?e, market, "Liquidator has no obligation in this market");
                    }
                }
            }

            info!("Withdrawer: sync_state completed");

            Ok(())
        })
    }
}
