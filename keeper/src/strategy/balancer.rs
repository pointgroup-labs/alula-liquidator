//! Rebalancer: swaps non-target assets back into the configured target asset if
//! the swap doesn't exceed the predefined max price impact.

use {
    crate::{
        collect::{Event, stellar_ledger::NewLedger},
        execute::{
            Action,
            stellar_tx::{SettleHook, SubmitStellarTx},
        },
        liquidator_capital::LiquidatorCapital,
        stellar::client::Gateway,
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending_model::{MarketData, ObligationKey},
        ports::{EventCodec, LedgerReader, OperationBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    metrics::{counter, histogram},
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

const BPS_FACTOR: i128 = 10_000;

pub struct BalancerConfig {
    pub market: String,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    /// Max price impact of the swapped asset, compared to the oracle's asset price
    pub max_price_impact_bps: i128,
    pub max_retries: u32,
    /// Allowed slippage applied to the swap after `price impact` checks
    pub allowed_swap_slippage_bps: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub refresh_interval_blocks: u32,
    pub max_swap_provider_probes: u32,
    pub min_swap_amount_value_cents: i128,
}

pub struct Balancer {
    pkey: String,
    skey: SigningKey,
    gateway: Arc<Gateway>,
    config: BalancerConfig,
    liquidator_key: ObligationKey,
    ledger_reader: Arc<dyn LedgerReader>,
    asset_index: HashMap<String, AssetInfo>,
    liquidator_capital: Arc<LiquidatorCapital>,
}

struct AssetInfo {
    decimals: u32,
    oracle_price: i128,
    oracle_decimals: u32,
}

impl Strategy<Event, Action> for Balancer {
    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::NewLedger(b) => self.handle_new_ledger(b).await,
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
            }
        })
    }
}

impl Balancer {
    pub fn new(
        pkey: String,
        skey: SigningKey,
        gateway: Arc<Gateway>,
        config: BalancerConfig,
        ledger_reader: Arc<dyn LedgerReader>,
        liquidator_capital: Arc<LiquidatorCapital>,
    ) -> Self {
        let liquidator_key = ObligationKey::new(pkey.clone());

        Self {
            skey,
            pkey,
            config,
            gateway,
            ledger_reader,
            liquidator_key,
            liquidator_capital,
            asset_index: HashMap::new(),
        }
    }

    async fn handle_new_ledger(&mut self, ledger: NewLedger) -> Vec<Action> {
        if !ledger
            .seq_num
            .is_multiple_of(self.config.refresh_interval_blocks)
            || !self.is_swap_reasonable()
        {
            return vec![];
        }

        let market = &self.config.market;
        let Ok(market_data) = self
            .ledger_reader
            .read_market_data(market)
            .await
            .inspect_err(|e| {
                warn!(?e, %market, "failed to fetch market data");
            })
        else {
            return vec![];
        };

        let _ = self.find_rebalance_actions(&market_data).await;

        vec![]
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        if !self.is_swap_reasonable() {
            return vec![];
        }

        let asset_to_swap = match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Liquidate) => self.try_parse_asset_from_liquidate_event(&event),
            Ok(OperationEvent::Withdraw) => self.try_parse_asset_from_withdraw_event(&event),
            Ok(_) => None,
            Err(e) => {
                warn!("Failed to decode operation from event: {}", e);

                None
            }
        };

        let market = self.config.market.clone();
        if asset_to_swap.is_some() {
            let Ok(market_data) = self
                .ledger_reader
                .read_market_data(&market)
                .await
                .inspect_err(|e| {
                    warn!(?e, %market, "failed to fetch market data");
                })
            else {
                return vec![];
            };

            self.find_rebalance_actions(&market_data).await
        } else {
            vec![]
        }
    }

    fn try_parse_asset_from_liquidate_event(&self, event: &SorobanEvent) -> Option<String> {
        let (liquidator, collateral_pool) = (
            self.gateway.decode_topic(event, 1),
            self.gateway.decode_topic(event, 4),
        );
        if liquidator != self.pkey || self.config.assets_to_hold.contains(&collateral_pool) {
            return None;
        }

        let Ok(Some(liquidation_result)) = self
            .gateway
            .parse_liquidation_result_from_liquidation_event_value(&event.value)
        else {
            error!("Couldn't parse liquidation_result from the liquidation event");

            return None;
        };

        if liquidation_result.plain_collateral_seized == 0 {
            return None;
        }

        Some(collateral_pool)
    }

    fn try_parse_asset_from_withdraw_event(&self, event: &SorobanEvent) -> Option<String> {
        let Ok(withdrawer) = self.gateway.parse_obligation_key_from_topic(event, 2) else {
            error!("Failed to parse withdrawer from the withdrawer event");
            return None;
        };

        if withdrawer.user != self.pkey {
            return None;
        }

        Some(self.gateway.decode_topic(event, 1))
    }

    async fn find_rebalance_actions(&mut self, market_data: &MarketData) -> Vec<Action> {
        self.update_asset_index(market_data);
        let target_asset = self.config.assets_to_hold[0].clone();

        let Some(target_oracle_price) = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == target_asset)
            .map(|p| p.oracle_asset_price)
        else {
            error!(%target_asset, "target asset missing from pools data");

            return vec![];
        };
        if !target_oracle_price.is_positive() {
            error!(
                target_asset,
                target_oracle_price, "non-positive target's oracle price"
            );
        }

        let mut actions = vec![];
        for candidate in self
            .asset_index
            .keys()
            .filter(|addr| !self.config.assets_to_hold.contains(*addr))
        {
            let Some(candidate_oracle_price) = market_data
                .pools_data
                .iter()
                .find(|p| p.pool_address == *candidate)
                .map(|p| p.oracle_asset_price)
            else {
                error!(%candidate, "candidate asset missing from pools data; skipping");

                continue;
            };
            if !candidate_oracle_price.is_positive() {
                error!(
                    candidate,
                    candidate_oracle_price, "non-positive candidate's oracle price"
                );
            }

            match self
                .evaluate_rebalancable_candidate(
                    &target_asset,
                    candidate,
                    market_data.oracle_price_decimals,
                    target_oracle_price,
                    candidate_oracle_price,
                )
                .await
            {
                Ok(Some(a)) => actions.push(a),
                Ok(None) => {}
                Err(e) => {
                    warn!(?e, %candidate, "candidate evaluation failed");
                    counter!("rebalancer_outcome_total", "outcome" => "evaluation_error")
                        .increment(1);
                }
            }
        }

        actions
    }

    async fn evaluate_rebalancable_candidate(
        &self,
        target: &str,
        candidate: &str,
        oracle_decimals: u32,
        target_oracle_price: i128,
        candidate_oracle_price: i128,
    ) -> anyhow::Result<Option<Action>> {
        let raw_balance = self
            .liquidator_capital
            .try_get_balance(candidate, &*self.ledger_reader)
            .await?;
        let swappable_balance = if candidate == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        if !swappable_balance.is_positive() {
            debug!(%candidate, raw_balance, swappable_balance, "Nothing to swap");
            counter!("rebalancer_outcome_total", "outcome" => "nothing_to_swap").increment(1);

            return Ok(None);
        }

        let sc14 = 10_i128.pow(oracle_decimals);
        let candidate_quoted_in_target = (candidate_oracle_price * sc14) / target_oracle_price;

        let mut best_provider: Option<(String, i128, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .probe_provider(
                    provider,
                    candidate,
                    target,
                    candidate_quoted_in_target,
                    swappable_balance,
                )
                .await
            {
                Ok(Some((amount_in, amount_out))) => {
                    debug!(%provider, %candidate, amount_in, amount_out, "found a route that doesn't exceed the max price impact");

                    let take = match best_provider {
                        None => true,
                        Some((_, best_in, _)) => amount_in > best_in,
                    };
                    if take {
                        best_provider = Some((provider.clone(), amount_in, amount_out));
                    }
                }
                Ok(None) => debug!(%provider, %candidate, "provider unviable"),
                Err(e) => {
                    warn!(?e, %candidate, "candidate evaluation failed");
                    counter!("rebalancer_outcome_total", "outcome" => "evaluation_error")
                        .increment(1);
                }
            }
        }
        let Some((provider, amount_in, amount_out)) = best_provider else {
            counter!("rebalancer_outcome_total", "outcome" => "no_viable_provider").increment(1);

            return Ok(None);
        };

        let info = self
            .asset_index
            .get(candidate)
            .expect("candidate sourced from asset_index");
        let swap_value_cents = compute_value_cents(amount_in, info);
        if swap_value_cents < self.config.min_swap_amount_value_cents {
            info!(%candidate, amount_in, swap_value_cents, "swap below dust threshold");
            counter!("rebalancer_outcome_total", "outcome" => "below_dust").increment(1);

            return Ok(None);
        }

        let min_amount_out = amount_out
            .saturating_mul(BPS_FACTOR - self.config.allowed_swap_slippage_bps)
            / BPS_FACTOR;

        let path = [candidate, target];
        let request =
            self.gateway
                .swap_exact_tokens_request(&provider, amount_in, min_amount_out, &path)?;

        let op = self
            .gateway
            .batch_op(&self.config.market, &self.liquidator_key, &[request])?;
        let op_id = match self
            .liquidator_capital
            .reserve(amount_in, swappable_balance, candidate)
        {
            Ok(id) => id,
            Err(e) => {
                warn!(?e, %candidate, amount_in, swappable_balance,
                    "rebalancer: reservation lost race; skipping submission");
                counter!("rebalancer_outcome_total", "outcome" => "reservation_lost").increment(1);

                return Ok(None);
            }
        };

        info!(
            %candidate, swap_value_cents, %target, %provider, amount_in, amount_out,
            min_amount_out, market = %self.config.market,
            "Rebalancer: submitting swap"
        );
        counter!("rebalancer_outcome_total", "outcome" => "dispatched").increment(1);
        histogram!("rebalancer_dispatched_swap_value_cents").record(swap_value_cents.max(0) as f64);

        Ok(Some(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_submission_retries: self.config.max_retries,
            on_settle: Some(SettleHook {
                op_id,
                liquidation_outcome: None,
                liquidator_capital: self.liquidator_capital.clone(),
            }),
        })))
    }

    async fn probe_provider(
        &self,
        provider: &str,
        asset_in: &str,
        asset_out: &str,
        oracle_price: i128,
        swappable_balance: i128,
    ) -> anyhow::Result<Option<(i128, i128)>> {
        let mut amount_in = swappable_balance;

        for _ in 0..self.config.max_swap_provider_probes {
            let amount_out = self
                .ledger_reader
                .get_amount_out(amount_in, asset_in, asset_out, provider)
                .await?;

            let price_impact = compute_swap_price_impact_bps(oracle_price, amount_in, amount_out);
            if price_impact > self.config.max_price_impact_bps {
                amount_in /= 2;

                continue;
            } else {
                return Ok(Some((amount_in, amount_out)));
            }
        }

        Ok(None)
    }

    fn update_asset_index(&mut self, market_data: &MarketData) {
        for pool in &market_data.pools_data {
            self.asset_index.insert(
                pool.pool_address.clone(),
                AssetInfo {
                    decimals: pool.token_decimals,
                    oracle_price: pool.oracle_asset_price,
                    oracle_decimals: market_data.oracle_price_decimals,
                },
            );
        }
    }

    fn is_swap_reasonable(&self) -> bool {
        if self.config.assets_to_hold.is_empty() {
            warn!("Rebalancer: assets_to_hold is empty; skipping rebalance");

            false
        } else if self.config.swap_providers.is_empty() {
            warn!("Rebalancer: swap_providers is empty; skipping rebalance");

            false
        } else {
            true
        }
    }
}

/// Computes the swap price impact in BPS.
/// Assumes oracle_price is denominated as (Amount Out / Amount In) with 14 decimals.
fn compute_swap_price_impact_bps(oracle_price: i128, amount_in: i128, amount_out: i128) -> i128 {
    // 1. Prevent division by zero panic
    if amount_in == 0 {
        return 0; // Or revert with a custom error
    }

    // 2. Compute execution price (scaled to 14 decimals)
    // Note: This strictly assumes amount_in and amount_out have identical decimals.
    let execution_price_scaled = (amount_out * 10i128.pow(14)) / amount_in;

    // 3. Compute the raw difference
    // If execution_price < oracle_price, impact is positive (user lost value).
    // If execution_price > oracle_price, impact is negative (user gained value).
    let price_diff = oracle_price - execution_price_scaled;

    // 4. Convert the difference into BPS relative to the oracle price
    // Multiply by 10_000 before dividing to maintain precision.

    (price_diff * 10_000) / oracle_price
}

fn compute_value_cents(token_amount: i128, info: &AssetInfo) -> i128 {
    if info.oracle_price <= 0 {
        return 0;
    }
    let denom_pow = info.decimals + info.oracle_decimals;
    if denom_pow < 2 {
        return 0;
    }
    let denom = 10_i128.pow(denom_pow - 2);
    if denom == 0 {
        return 0;
    }
    (token_amount.saturating_mul(info.oracle_price) / denom).max(0)
}
