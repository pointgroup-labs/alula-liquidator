//! Balancer: swaps non-target assets back into the configured target asset if
//! the swap doesn't exceed the predefined max price impact.

use std::{collections::HashMap, sync::Arc};

use ed25519_dalek::SigningKey;
use engine::{
    lending_model::{MarketData, ObligationKey},
    ports::{EventCodec, LedgerReader, OperationBuilder, OperationEvent},
    reactor::{BoxFuture, Strategy},
};
use stellar_rpc_client::Event as SorobanEvent;
use tracing::{debug, error, info, warn};

use crate::{
    collect::{Event, stellar_ledger::NewLedger},
    execute::{
        Action,
        stellar_tx::{SubmitStellarTx, TransactionSettleHook},
    },
    liquidator_capital::LiquidatorCapital,
    metrics::{self, BalancerOutcome},
    stellar::client::Gateway,
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
        if !ledger.seq_num.is_multiple_of(self.config.refresh_interval_blocks)
            || !self.is_swap_reasonable()
        {
            return vec![];
        }

        let market = &self.config.market;
        let Ok(market_data) = self.ledger_reader.read_market_data(market).await.inspect_err(|e| {
            warn!(?e, %market, "failed to fetch market data");
        }) else {
            return vec![];
        };

        self.find_rebalance_actions(&market_data).await
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
                warn!("failed to decode operation from event: {}", e);

                None
            }
        };

        let market = self.config.market.clone();
        if asset_to_swap.is_some() {
            let Ok(market_data) =
                self.ledger_reader.read_market_data(&market).await.inspect_err(|e| {
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
        let (liquidator, collateral_pool) =
            (self.gateway.decode_topic(event, 1), self.gateway.decode_topic(event, 4));
        if liquidator != self.pkey || self.config.assets_to_hold.contains(&collateral_pool) {
            return None;
        }

        let Ok(Some(liquidation_result)) =
            self.gateway.parse_liquidation_result_from_liquidation_event_value(&event.value)
        else {
            error!("couldn't parse liquidation_result from the liquidation event");

            return None;
        };

        if liquidation_result.plain_collateral_seized == 0 {
            return None;
        }

        Some(collateral_pool)
    }

    fn try_parse_asset_from_withdraw_event(&self, event: &SorobanEvent) -> Option<String> {
        let Ok(withdrawer) = self.gateway.parse_obligation_key_from_topic(event, 2) else {
            error!("failed to parse withdrawer from the withdrawer event");
            return None;
        };

        if withdrawer.user != self.pkey {
            return None;
        }

        Some(self.gateway.decode_topic(event, 1))
    }

    /// Each returned Action holds the sole release hook for a capital
    /// reservation taken here; a caller that drops them leaks it until TTL.
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
            error!(target_asset, target_oracle_price, "non-positive target's oracle price");
        }

        let mut actions = vec![];
        for candidate in
            self.asset_index.keys().filter(|addr| !self.config.assets_to_hold.contains(*addr))
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
                error!(candidate, candidate_oracle_price, "non-positive candidate's oracle price");
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
                    BalancerOutcome::EvaluationError.record();
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
        let raw_balance =
            self.liquidator_capital.try_get_balance(candidate, &*self.ledger_reader).await?;
        let swappable_balance = if candidate == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        if !swappable_balance.is_positive() {
            debug!(%candidate, raw_balance, swappable_balance, "nothing to swap");
            BalancerOutcome::NothingToSwap.record();

            return Ok(None);
        }

        if target_oracle_price <= 0 {
            error!(%target, target_oracle_price, "non-positive target oracle price; skipping candidate");
            BalancerOutcome::BadOraclePrice.record();

            return Ok(None);
        }
        let oracle_scale = 10_i128.pow(oracle_decimals);
        let candidate_quoted_in_target =
            (candidate_oracle_price * oracle_scale) / target_oracle_price;

        let mut best_provider: Option<(String, i128, i128, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .probe_provider(
                    provider,
                    candidate,
                    target,
                    candidate_quoted_in_target,
                    oracle_decimals,
                    swappable_balance,
                )
                .await
            {
                Ok(Some((amount_in, amount_out, realised_price_scaled))) => {
                    debug!(%provider, %candidate, amount_in, amount_out, "found a route that doesn't exceed the max price impact");

                    let take = match best_provider {
                        None => true,
                        Some((_, best_in, _, _)) => amount_in > best_in,
                    };
                    if take {
                        best_provider =
                            Some((provider.clone(), amount_in, amount_out, realised_price_scaled));
                    }
                }
                Ok(None) => debug!(%provider, %candidate, "provider unviable"),
                Err(e) => {
                    warn!(?e, %candidate, "candidate evaluation failed");
                    BalancerOutcome::EvaluationError.record();
                }
            }
        }
        let Some((provider, amount_in, amount_out, realised_price_scaled)) = best_provider else {
            BalancerOutcome::NoViableProvider.record();

            return Ok(None);
        };

        let info = self.asset_index.get(candidate).expect("candidate sourced from asset_index");
        let swap_value_cents = compute_value_cents(amount_in, info);
        if swap_value_cents < self.config.min_swap_amount_value_cents {
            info!(%candidate, amount_in, swap_value_cents, "swap below dust threshold");
            BalancerOutcome::BelowDust.record();

            return Ok(None);
        }

        let min_amount_out = amount_out
            .saturating_mul(BPS_FACTOR - self.config.allowed_swap_slippage_bps)
            / BPS_FACTOR;

        let path = [candidate, target];
        let request =
            self.gateway.swap_exact_tokens_request(&provider, amount_in, min_amount_out, &path)?;

        let op = self.gateway.batch_op(&self.config.market, &self.liquidator_key, &[request])?;
        let op_id = match self.liquidator_capital.reserve(amount_in, swappable_balance, candidate) {
            Ok(id) => id,
            Err(e) => {
                warn!(?e, %candidate, amount_in, swappable_balance,
                    "balancer: reservation lost race; skipping submission");
                BalancerOutcome::ReservationLost.record();

                return Ok(None);
            }
        };

        metrics::record_balancer_realised_price(candidate, realised_price_scaled);

        info!(
            %candidate, swap_value_cents, %target, %provider, amount_in, amount_out,
            min_amount_out, market = %self.config.market,
            realised_price_scaled, oracle_price_scaled = candidate_quoted_in_target,
            "submitting swap..."
        );
        BalancerOutcome::Dispatched.record();
        metrics::record_balancer_dispatched_value(swap_value_cents);

        Ok(Some(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_submission_retries: self.config.max_retries,
            on_settle: Some(TransactionSettleHook {
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
        oracle_decimals: u32,
        swappable_balance: i128,
    ) -> anyhow::Result<Option<(i128, i128, i128)>> {
        let (Some(in_info), Some(out_info)) =
            (self.asset_index.get(asset_in), self.asset_index.get(asset_out))
        else {
            warn!(%provider, %asset_in, %asset_out, "asset decimals unknown; skipping probe");

            return Ok(None);
        };

        let mut amount_in = swappable_balance;

        for _ in 0..self.config.max_swap_provider_probes {
            let amount_out =
                self.ledger_reader.get_amount_out(amount_in, asset_in, asset_out, provider).await?;

            let Some(impact) = compute_swap_price_impact(
                oracle_price,
                amount_in,
                amount_out,
                in_info.decimals,
                out_info.decimals,
                oracle_decimals,
            ) else {
                debug!(
                    %provider, %asset_in, amount_in, amount_out, oracle_price,
                    "price impact not computable (non-positive input); treating probe as unviable"
                );

                return Ok(None);
            };

            // Observe the oracle-vs-DEX price gap for every probe, tagged by
            // whether it cleared the configured ceiling. This is the realised
            // spread the balancer is gating on — useful for tuning
            // `max_price_impact_bps` and spotting oracle/DEX divergence.
            let admitted = impact.bps <= self.config.max_price_impact_bps;
            metrics::record_balancer_price_impact(asset_in, admitted, impact.bps);

            if admitted {
                debug!(
                    %provider, %asset_in, amount_in, amount_out,
                    price_impact_bps = impact.bps,
                    execution_price_scaled = impact.execution_price_scaled,
                    "probe within max price impact"
                );

                return Ok(Some((amount_in, amount_out, impact.execution_price_scaled)));
            }

            debug!(
                %provider, %asset_in, amount_in, amount_out,
                price_impact_bps = impact.bps,
                max = self.config.max_price_impact_bps,
                "probe exceeds max price impact; halving amount_in"
            );
            amount_in /= 2;
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
            warn!("assets_to_hold is empty; skipping rebalance");

            false
        } else if self.config.swap_providers.is_empty() {
            warn!("swap_providers is empty; skipping rebalance");

            false
        } else {
            true
        }
    }
}

/// Outcome of comparing a DEX quote against the oracle price.
struct SwapPriceImpact {
    /// Price impact in BPS relative to the oracle price. Positive means the
    /// realised (DEX) price is *worse* than the oracle — we receive less
    /// `amount_out` than the oracle-implied value, i.e. we lose value.
    /// Negative means the DEX executed better than the oracle.
    bps: i128,
    /// Realised execution price (`amount_out / amount_in`) scaled by
    /// `10^oracle_decimals` — i.e. on the *same* scale as the oracle ratio it is
    /// compared against. Kept scaled to avoid float precision loss; divide by
    /// `10^oracle_decimals` for a human-readable ratio.
    execution_price_scaled: i128,
}

/// Compares a realised DEX quote (`amount_out` per `amount_in`) against the
/// oracle price ratio, returning both the price impact (BPS) and the realised
/// execution price.
///
/// `oracle_price` is the oracle-implied `candidate/target` ratio already scaled
/// by `10^oracle_decimals`. The realised price is scaled by the **same**
/// `10^oracle_decimals` so the subtraction is meaningful — previously this was a
/// hardcoded `10^14`, which silently produced garbage whenever the oracle used a
/// different number of decimals. `oracle_price_decimals` is a runtime value on
/// `MarketData`, not a constant, so the scale must be read from it.
///
/// `amount_in` and `amount_out` are raw units of two different tokens, so they
/// are put on a common basis before dividing — the oracle ratio is quoted per
/// *whole* token, and without this a decimals mismatch bakes a
/// `10^(out_decimals - in_decimals)` factor into the realised price (for a 7/8
/// mismatch that is ±9000 bps of pure artefact, enough to make the caller's
/// ceiling either reject everything or accept anything).
///
/// Returns `None` when the inputs can't yield a meaningful comparison
/// (non-positive `amount_in` or `oracle_price`, or arithmetic overflow), so the
/// caller can treat the probe as unviable instead of dividing by zero.
fn compute_swap_price_impact(
    oracle_price: i128,
    amount_in: i128,
    amount_out: i128,
    amount_in_decimals: u32,
    amount_out_decimals: u32,
    oracle_price_decimals: u32,
) -> Option<SwapPriceImpact> {
    // Guard both divisions: `amount_in` scales the realised price, `oracle_price`
    // scales the BPS conversion.
    if amount_in <= 0 || oracle_price <= 0 {
        return None;
    }

    let scale = 10_i128.checked_pow(oracle_price_decimals)?;

    // Only the difference is applied, on whichever side keeps the intermediate
    // product smallest.
    let correction = 10_i128.checked_pow(amount_in_decimals.abs_diff(amount_out_decimals))?;
    let (numerator, denominator) = if amount_in_decimals >= amount_out_decimals {
        (amount_out.checked_mul(scale)?.checked_mul(correction)?, amount_in)
    } else {
        (amount_out.checked_mul(scale)?, amount_in.checked_mul(correction)?)
    };
    let execution_price_scaled = numerator.checked_div(denominator)?;

    // execution < oracle → positive impact (received less than oracle value)
    // execution > oracle → negative impact (did better than oracle)
    let price_diff = oracle_price.checked_sub(execution_price_scaled)?;
    let bps = price_diff.checked_mul(BPS_FACTOR)?.checked_div(oracle_price)?;

    Some(SwapPriceImpact { bps, execution_price_scaled })
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

#[cfg(test)]
mod tests {
    use super::*;

    const ORACLE_DECIMALS: u32 = 14;

    /// Quote that exactly matches the oracle: 100 whole `in` tokens for 200
    /// whole `out` tokens at a 2:1 oracle ratio.
    fn perfect_swap(in_decimals: u32, out_decimals: u32) -> SwapPriceImpact {
        let oracle_price = 2 * 10_i128.pow(ORACLE_DECIMALS);

        compute_swap_price_impact(
            oracle_price,
            100 * 10_i128.pow(in_decimals),
            200 * 10_i128.pow(out_decimals),
            in_decimals,
            out_decimals,
            ORACLE_DECIMALS,
        )
        .expect("inputs are positive and in range")
    }

    #[test]
    fn perfect_swap_across_mismatched_decimals_is_zero_impact() {
        assert_eq!(perfect_swap(7, 7).bps, 0);
        assert_eq!(perfect_swap(7, 8).bps, 0);
        assert_eq!(perfect_swap(8, 7).bps, 0);
    }

    #[test]
    fn realised_price_is_on_the_oracle_scale_regardless_of_decimals() {
        let expected = 2 * 10_i128.pow(ORACLE_DECIMALS);

        assert_eq!(perfect_swap(7, 8).execution_price_scaled, expected);
        assert_eq!(perfect_swap(8, 7).execution_price_scaled, expected);
    }

    #[test]
    fn worse_than_oracle_quote_is_positive_impact() {
        let oracle_price = 2 * 10_i128.pow(ORACLE_DECIMALS);
        let impact = compute_swap_price_impact(
            oracle_price,
            100 * 10_i128.pow(7),
            180 * 10_i128.pow(8),
            7,
            8,
            ORACLE_DECIMALS,
        )
        .expect("inputs are positive and in range");

        assert_eq!(impact.bps, 1_000);
    }

    #[test]
    fn non_positive_inputs_are_unviable() {
        assert!(compute_swap_price_impact(1, 0, 1, 7, 7, ORACLE_DECIMALS).is_none());
        assert!(compute_swap_price_impact(0, 1, 1, 7, 7, ORACLE_DECIMALS).is_none());
    }
}
