//! Withdrawer strategy: opportunistically pulls liquidity out of pools when
//! utilization is well below the configured limit.

use {
    crate::{
        collect::{Event, block::NewBlock},
        execute::{Action, stellar_tx::SubmitStellarTx},
        stellar::Gateway,
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending::{JToken, MarketData, Obligation, ObligationKey, PoolData, Underlying},
        ports::{ChainReader, EventCodec, OpBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{error, info, trace, warn},
};

const REFRESH_INTERVAL_BLOCKS: u32 = 2;
const MAX_WITHDRAWAL_RETRIES: u32 = 3;
const UTILIZATION_SAFETY_MARGIN_BPS: i128 = 500;

pub struct WithdrawerConfig {
    pub markets: Vec<String>,
    pub min_withdraw_value_cents: i128,
}

pub struct Withdrawer {
    chain: Arc<dyn ChainReader>,
    gateway: Arc<Gateway>,
    skey: SigningKey,
    pkey: String,
    config: WithdrawerConfig,
    liquidator_key: ObligationKey,
    market_data: HashMap<String, MarketData>,
    liquidator_obligations: HashMap<String, Obligation>,
}

impl Withdrawer {
    pub fn new(
        chain: Arc<dyn ChainReader>,
        gateway: Arc<Gateway>,
        skey: SigningKey,
        pkey: String,
        config: WithdrawerConfig,
    ) -> Self {
        let liquidator_key = ObligationKey::new(pkey.clone());
        Self {
            chain,
            gateway,
            skey,
            pkey,
            config,
            liquidator_key,
            market_data: HashMap::new(),
            liquidator_obligations: HashMap::new(),
        }
    }
}

impl Strategy<Event, Action> for Withdrawer {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
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
    async fn handle_new_block(&mut self, block: NewBlock) -> Vec<Action> {
        if !block.number.is_multiple_of(REFRESH_INTERVAL_BLOCKS) {
            return vec![];
        }
        let markets: Vec<String> = self.config.markets.clone();
        let mut actions = vec![];
        for market_address in &markets {
            self.refresh_market_data(market_address).await;
            self.refresh_liquidator_obligation(market_address).await;
            actions.extend(self.find_withdrawal_opportunities(market_address));
        }
        actions
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        // Reorder: kind first, then optional topic parsing.
        match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Deposit) | Ok(OperationEvent::Repay) => {}
            _ => return vec![],
        }

        if let Ok(key) = self.gateway.parse_obligation_key_from_topic(&event, 2)
            && key.user == self.pkey
        {
            return vec![];
        }

        info!(?event, "Detected liquidity increase event");
        self.refresh_market_data(&event.contract_id).await;
        self.refresh_liquidator_obligation(&event.contract_id).await;
        self.find_withdrawal_opportunities(&event.contract_id)
    }

    fn find_withdrawal_opportunities(&self, market_address: &str) -> Vec<Action> {
        let Some(market_data) = self.market_data.get(market_address) else {
            warn!(?market_address, "No market data available");
            return vec![];
        };
        let Some(liquidator_obligation) = self.liquidator_obligations.get(market_address) else {
            info!(market = %market_address, "No liquidator obligations for market");
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
                continue;
            };

            // Bug fix #3: divide-by-zero guard now lives inside
            // PoolData::compute_max_safe_withdrawal — returns Underlying::ZERO
            // if `utilization_considered_safe` collapses to ≤ 0.
            let max_withdrawal = pool.compute_max_safe_withdrawal(UTILIZATION_SAFETY_MARGIN_BPS);
            if max_withdrawal == Underlying::ZERO {
                continue;
            }

            let liquidator_underlying = pool.j_to_underlying_floor(JToken(deposit_pos.j_tokens));
            let mut withdrawal_amount = liquidator_underlying.raw().min(max_withdrawal.raw());
            let withdrawal_value_cents =
                self.calculate_withdrawal_value_cents(market_data, pool, withdrawal_amount);

            if withdrawal_amount == liquidator_underlying.raw() {
                withdrawal_amount = i128::MAX;
            }

            if withdrawal_value_cents >= self.config.min_withdraw_value_cents {
                info!(
                    pool_address = %pool.pool_address,
                    current_utilization = pool.utilization_ratio_bps(),
                    max_withdrawal = max_withdrawal.raw(),
                    liquidator_tokens = liquidator_underlying.raw(),
                    withdrawal_amount,
                    value_cents = withdrawal_value_cents,
                    "Creating withdrawal action"
                );

                match self.build_withdraw_action(
                    market_address,
                    &pool.pool_address,
                    withdrawal_amount,
                ) {
                    Ok(action) => actions.push(action),
                    Err(e) => error!(?e, "Failed to build withdrawal action"),
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
            self.gateway
                .withdraw_op(market_address, &self.liquidator_key, pool_address, amount)?;
        Ok(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_retries: MAX_WITHDRAWAL_RETRIES,
            on_settle: None,
        }))
    }

    fn calculate_withdrawal_value_cents(
        &self,
        market_data: &MarketData,
        pool: &PoolData,
        token_amount: i128,
    ) -> i128 {
        let price_with_decimals = pool.oracle_asset_price;
        let oracle_decimals = market_data.oracle_price_decimals;
        let token_decimals = pool.token_decimals;
        let pow = token_decimals + oracle_decimals;
        if pow < 2 {
            return 0;
        }
        let value_raw = (token_amount * price_with_decimals) / 10_i128.pow(pow - 2);
        value_raw.max(0)
    }

    async fn refresh_market_data(&mut self, market_address: &str) {
        match self.chain.read_market_data(market_address).await {
            Ok(md) => {
                info!(%market_address, "Refreshed market data");
                self.market_data.insert(market_address.to_string(), md);
            }
            Err(e) => error!(?e, %market_address, "Failed to refresh market data"),
        }
    }

    async fn refresh_liquidator_obligation(&mut self, market_address: &str) {
        match self
            .chain
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
