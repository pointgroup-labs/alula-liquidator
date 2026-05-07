use {
    crate::{
        collectors::block_collector::NewBlock,
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
    tracing::{error, info, trace, warn},
    url::Url,
};

const BPS_FACTOR: i128 = 10_000;
const REFRESH_INTERVAL_BLOCKS: u32 = 2;
const MAX_WITHDRAWAL_RETRIES: u32 = 3;
const UTILIZATION_SAFETY_MARGIN_BPS: i128 = 500;

pub struct WithdrawerConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
    pub min_withdraw_value_cents: i128,
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

impl Strategy<Event, Action> for Withdrawer {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async {
            match event {
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewBlock(b) => self.handle_new_block(b).await,
            }
        })
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(markets = ?self.config.markets, "Withdrawer: syncing state");

            let markets: Vec<String> = self.config.markets.clone();
            for market in &markets {
                self.refresh_market_data(market).await;
                self.refresh_liquidator_obligation(market).await;
            }

            info!("Withdrawer: sync_state completed");

            Ok(())
        })
    }
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

    async fn handle_new_block(&mut self, block: NewBlock) -> Vec<Action> {
        if block.number % REFRESH_INTERVAL_BLOCKS == 0 {
            let markets: Vec<String> = self.config.markets.clone();
            let mut actions = vec![];

            for market_address in &markets {
                self.refresh_market_data(market_address).await;
                self.refresh_liquidator_obligation(market_address).await;

                actions.extend(self.find_withdrawal_opportunities(market_address).await);
            }

            actions
        } else {
            vec![]
        }
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        if matches!(
            helper::decode_operation_event(&event),
            Ok(OperationEvent::Deposit) | Ok(OperationEvent::Repay)
        ) {
            // Ignore events triggered by the liquidator itself to avoid complexity
            if let Ok(key) = helper::parse_obligation_key_from_topic(&event, 2) {
                if key.user == self.pkey {
                    return vec![];
                }
            }

            info!(?event, "Detected liquidity increase event");
            self.refresh_market_data(&event.contract_id).await;
            self.refresh_liquidator_obligation(&event.contract_id).await;

            return self.find_withdrawal_opportunities(&event.contract_id).await;
        }

        vec![]
    }

    async fn find_withdrawal_opportunities(&self, market_address: &str) -> Vec<Action> {
        let market_data = match self.market_data.get(market_address) {
            Some(data) => data,
            None => {
                warn!(?market_address, "No market data available");

                return vec![];
            }
        };

        let liquidator_obligation = match self.liquidator_obligations.get(market_address) {
            Some(obl) => obl,
            None => {
                info!("No liquidator obligations for market {}", market_address);

                return vec![];
            }
        };

        let mut actions = vec![];
        for deposit_pos in &liquidator_obligation.deposits {
            let Some(pool) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == deposit_pos.pool_address)
            else {
                warn!(pool = deposit_pos.pool_address, "Pool not found");

                continue;
            };

            let max_withdrawal = self.compute_max_safe_withdrawal(pool);
            if max_withdrawal == 0 {
                continue;
            }

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

                match self.build_withdraw_action(
                    market_address,
                    &pool.pool_address,
                    withdrawal_amount,
                ) {
                    Ok(action) => {
                        actions.push(action);
                    }
                    Err(e) => {
                        error!(?e, "Failed to build withdrawal action");
                    }
                };
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
            helper::build_withdraw_op(market_address, &self.liquidator_key, pool_address, amount)?;

        Ok(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_retries: MAX_WITHDRAWAL_RETRIES,
        }))
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

    async fn refresh_market_data(&mut self, market_address: &str) {
        match helper::simulate_get_market_data(&self.rpc, market_address, &self.pkey).await {
            Ok(market_data) => {
                info!(%market_address, "Refreshed market data");
                self.market_data
                    .insert(market_address.to_string(), market_data);
            }
            Err(e) => error!(?e, %market_address, "Failed to refresh market data"),
        }
    }

    async fn refresh_liquidator_obligation(&mut self, market_address: &str) {
        match helper::simulate_get_user_obligation(
            &self.rpc,
            market_address,
            &self.pkey,
            &self.liquidator_key,
        )
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
                trace!(?e, %market_address, "Liquidator has no obligation in this market (post-refresh)");
                self.liquidator_obligations.remove(market_address);
            }
        }
    }
}
