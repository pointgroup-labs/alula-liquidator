//! Balancer: keeps the held portfolio near its configured target weights.
//!
//! Each cycle it values the whole portfolio in the hub asset's numeraire(expected to be USD stablecoin),
//! finds assets that have drifted past the threshold, then **sells** surpluses
//! into the hub and **buys** deficits with the hub — all packed into a single
//! atomic `submit_requests_batch` (sells ordered first, so they fund the buys inside
//! the same transaction). Every leg is initially sized to fully
//! correct its drift, then halved until it clears the execution-impact ceiling.

use std::collections::HashMap;
use std::sync::Arc;

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
    pub max_retries: u32,
    pub xlm_address: String,
    /// Stablecoin hub(expected to be USD stablecoin): the numeraire the portfolio is priced in and the
    /// counterparty of every rebalance swap. Not a key in `assets_to_hold`.
    pub hub_address: String,
    pub xlm_safety_margin: i128,
    /// Upper bound on swap legs packed into one atomic rebalance batch. Exists because of hitting `ResourceLimit`
    /// per single submit_requests_batch if too many providers are invoked within a single operation scope.
    pub max_swaps_per_batch: u32,
    pub swap_providers: Vec<String>,
    /// Tolerance band around each asset's target weight, in bps. An asset is
    /// only rebalanced once `|current_weight - target_weight|` reaches this.
    pub rebalance_threshold_bps: u16,
    pub refresh_interval_blocks: u32,
    /// Max execution price impact of a single leg, compared to the oracle price.
    pub max_execution_impact_bps: i128,
    /// Allowed slippage applied to the swap after `execution impact` checks.
    pub allowed_swap_slippage_bps: i128,
    pub min_swap_amount_value_cents: i128,
    /// How many times a leg's input is halved while probing a provider for a
    /// size that satisfies the execution-impact constraint.
    pub max_swap_provider_halving_probes: u32,
    /// Volatile assets mapped to their target share of held value, in bps. The
    /// hub **IS NOT** listed here.
    pub assets_to_hold: HashMap<String, u16>,
}

pub struct Balancer {
    pkey: String,
    skey: SigningKey,
    gateway: Arc<Gateway>,
    config: BalancerConfig,
    liquidator_key: ObligationKey,
    ledger_reader: Arc<dyn LedgerReader>,
    assets_info: HashMap<String, AssetInfo>,
    liquidator_capital: Arc<LiquidatorCapital>,
}

#[derive(Clone)]
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
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
                Event::NewLedger(b) => self.handle_new_ledger(b).await,
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
            assets_info: HashMap::new(),
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
            /* TODO: What about `RemoveCollateral` ? */
            Ok(OperationEvent::Withdraw) => self.try_parse_asset_from_withdraw_event(&event),
            Ok(OperationEvent::Liquidate) => self.try_parse_asset_from_liquidate_event(&event),
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
        if liquidator != self.pkey
        /*|| self.config.assets_to_hold.contains_key(&collateral_pool)*/ // WRONG, right?
        {
            return None;
        }

        let Ok(Some(liquidation_result)) =
            self.gateway.parse_liquidation_result_from_liquidation_event_value(&event.value)
        else {
            error!("couldn't parse liquidation_result from the liquidation event");

            return None;
        };

        if liquidation_result.plain_collateral_seized == 0 {
            // No liquidity received right away - nothing to rebalance
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

    /// Runs one rebalance cycle and returns at most one Action: a single atomic
    /// batch of swaps (sells ordered before buys). The Action holds the sole
    /// release hooks for every capital reservation taken.
    async fn find_rebalance_actions(&mut self, market_data: &MarketData) -> Vec<Action> {
        self.update_assets_info(market_data);

        let Some(hub_info) = self.assets_info.get(&self.config.hub_address).cloned() else {
            error!(hub = %self.config.hub_address, "hub asset missing from pools data");
            BalancerOutcome::BadOraclePrice.record();

            return vec![];
        };
        if hub_info.oracle_price <= 0 {
            error!(hub = %self.config.hub_address, "non-positive hub oracle price");
            BalancerOutcome::BadOraclePrice.record();

            return vec![];
        }

        // --- Value the whole portfolio (hub included) in cents. ---
        // `hub_raw` is the gross on-chain balance (fed to the reserve gate,
        // which nets out committed capital itself). `hub_net` subtracts live
        // reservations and is what we value and size against.
        let hub_raw = match self
            .liquidator_capital
            .try_get_balance(&self.config.hub_address, &*self.ledger_reader)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(?e, "failed to read hub balance");

                return vec![];
            }
        };
        let hub_net = match self
            .liquidator_capital
            .try_get_available_balance(&self.config.hub_address, &*self.ledger_reader)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(?e, "failed to read hub available balance");

                return vec![];
            }
        };
        let hub_value_cents = compute_value_cents(hub_net, &hub_info);

        // Per-non-hub-asset snapshot: (address, info, raw balance, swappable, value)
        struct Held {
            address: String,
            info: AssetInfo,
            /// Gross swappable (post XLM safety margin) — the reserve gate's
            /// `available` arg, which nets out committed capital itself.
            swappable: i128,
            /// Net of live reservations — what we value and size sells against.
            deployable: i128,
            value_cents: i128,
        }

        // TODO: Use oracle's value instead of cents for value here?

        let mut held: Vec<Held> = Vec::new();
        let mut total_value_cents = hub_value_cents;
        for address in self.config.assets_to_hold.keys() {
            let Some(info) = self.assets_info.get(address).cloned() else {
                error!(%address, "held asset missing from pools data; skipping");

                continue;
            };
            if info.oracle_price <= 0 {
                error!(%address, "non-positive oracle price; skipping");
                BalancerOutcome::BadOraclePrice.record();

                continue;
            }

            let raw = match self
                .liquidator_capital
                .try_get_balance(address, &*self.ledger_reader)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(?e, %address, "failed to read asset balance; skipping");

                    continue;
                }
            };
            let available = match self
                .liquidator_capital
                .try_get_available_balance(address, &*self.ledger_reader)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(?e, %address, "failed to read asset available balance; skipping");

                    continue;
                }
            };
            // `swappable` (gross) feeds the reserve gate. `deployable` caps how
            // much of a sell we may size — it excludes the XLM safety margin,
            // which we hold but won't spend. Valuation, however, must COUNT that
            // margin: it is real XLM allocation and belongs in the target-weight
            // math, so we value on `available` (reservations netted, margin kept)
            // and only exclude the margin from the sell cap.
            let swappable = if address == &self.config.xlm_address {
                raw.saturating_sub(self.config.xlm_safety_margin)
            } else {
                raw
            };
            let deployable = if address == &self.config.xlm_address {
                available.saturating_sub(self.config.xlm_safety_margin)
            } else {
                available
            };

            let value_cents = compute_value_cents(available, &info);
            total_value_cents = total_value_cents.saturating_add(value_cents);

            held.push(Held { address: address.clone(), info, swappable, deployable, value_cents });
        }

        if total_value_cents <= 0 {
            debug!("portfolio has no value; nothing to rebalance");

            return vec![];
        }

        // --- Classify offenders (past the threshold) into sells/buys. ---

        let threshold = i128::from(self.config.rebalance_threshold_bps);
        let (mut sells, mut buys): (Vec<Drift>, Vec<Drift>) = (vec![], vec![]);
        for h in &held {
            let target_bps =
                i128::from(self.config.assets_to_hold.get(&h.address).copied().unwrap_or(0)); // WARN: swallowing error here

            let current_bps = h.value_cents * BPS_FACTOR / total_value_cents; // safe?
            let drift_bps = current_bps - target_bps;
            if drift_bps.abs() < threshold {
                BalancerOutcome::ThresholdHold.record();

                continue;
            }

            let target_value_cents = total_value_cents.saturating_mul(target_bps) / BPS_FACTOR;
            let delta_cents = (h.value_cents - target_value_cents).abs();
            let drift = Drift {
                delta_cents,
                info: h.info.clone(),
                swappable: h.swappable,
                deployable: h.deployable,
                address: h.address.clone(),
                abs_drift_bps: drift_bps.abs(),
            };
            if drift_bps > 0 {
                sells.push(drift);
            } else {
                buys.push(drift);
            }
        }

        // Worst drift first; deterministic address tiebreak.
        let sort_worst_first = |v: &mut Vec<Drift>| {
            v.sort_by(|a, b| {
                b.abs_drift_bps.cmp(&a.abs_drift_bps).then_with(|| a.address.cmp(&b.address))
            });
        };
        sort_worst_first(&mut sells);
        sort_worst_first(&mut buys);

        let oracle_scale = 10_i128.pow(hub_info.oracle_decimals);
        let cap = self.config.max_swaps_per_batch as usize;

        // --- Size sells (asset -> hub), worst-first, up to the cap. ---

        let (mut op_ids, mut requests) = (vec![], vec![]);
        let (mut expected_hub_out, mut dispatched_value_cents) = (0_i128, 0_i128);

        for sell in &sells {
            if requests.len() >= cap {
                break;
            }

            let want_in = value_to_token_amount(sell.delta_cents, &sell.info).min(sell.deployable);
            if want_in <= 0 {
                continue;
            }
            // Oracle price of the sold asset expressed in hub units, oracle-scaled.
            let sold_asset_price_in_hub =
                (sell.info.oracle_price * oracle_scale) / hub_info.oracle_price;

            let Some((provider, amount_in, amount_out)) = self
                .best_route(
                    want_in,
                    &sell.address,
                    &self.config.hub_address,
                    sold_asset_price_in_hub,
                )
                .await
            else {
                BalancerOutcome::NoViableProvider.record();

                continue;
            };

            let value_cents = compute_value_cents(amount_in, &sell.info);
            if value_cents < self.config.min_swap_amount_value_cents {
                debug!(asset = %sell.address, amount_in, value_cents, "sell below dust threshold");
                BalancerOutcome::BelowDust.record();

                continue;
            }

            let op_id = match self.liquidator_capital.reserve(
                amount_in,
                sell.swappable,
                &sell.address,
            ) {
                Ok(id) => id,
                Err(e) => {
                    warn!(?e, asset = %sell.address, amount_in, "sell reservation lost; skipping");
                    BalancerOutcome::ReservationLost.record();

                    continue;
                }
            };

            let min_amount_out = amount_out
                .saturating_mul(BPS_FACTOR - self.config.allowed_swap_slippage_bps)
                / BPS_FACTOR;
            let path = [sell.address.as_str(), self.config.hub_address.as_str()];
            match self.gateway.swap_exact_tokens_request(
                &provider,
                amount_in,
                min_amount_out,
                &path,
            ) {
                Ok(req) => {
                    requests.push(req);
                    op_ids.push(op_id);

                    expected_hub_out = expected_hub_out.saturating_add(min_amount_out);
                    dispatched_value_cents = dispatched_value_cents.saturating_add(value_cents);

                    metrics::record_balancer_realised_price(
                        &sell.address,
                        amount_out.saturating_mul(oracle_scale).checked_div(amount_in).unwrap_or(0),
                    );
                    BalancerOutcome::SellLegDispatched.record();

                    info!(asset = %sell.address, %provider, amount_in, amount_out, min_amount_out, "sell leg queued");
                }
                Err(e) => {
                    warn!(?e, asset = %sell.address, "failed to build sell request; releasing");

                    self.liquidator_capital.release(op_id);
                }
            }
        }

        // --- Size buys (hub -> asset) within the leg cap and the hub
        // budget actually available after the included sells. The batch is
        // atomic, so hub minted by the sells above funds these buys. ---

        let hub_swappable = if self.config.hub_address == self.config.xlm_address {
            hub_raw.saturating_sub(self.config.xlm_safety_margin)
        } else {
            hub_raw
        };
        let hub_deployable = if self.config.hub_address == self.config.xlm_address {
            hub_net.saturating_sub(self.config.xlm_safety_margin)
        } else {
            hub_net
        };

        // Gross budget gates reservations; net budget (minus live reservations)
        // bounds how much we actually size buys for. Both credit the hub minted
        // by this batch's sells, which is real because the batch is atomic.
        let hub_available = hub_swappable.saturating_add(expected_hub_out);
        let hub_deployable_budget = hub_deployable.saturating_add(expected_hub_out);
        if hub_deployable_budget <= 0 {
            warn!("no available hub liquidity; nothing to buy");

            return vec![];
        }
        let mut hub_remaining = hub_deployable_budget;

        for buy in &buys {
            if requests.len() >= cap || hub_remaining <= 0 {
                break;
            }
            // Hub tokens needed to buy back `delta_cents` of value, capped by the
            // hub we can still spend this cycle.
            let want_in = value_to_token_amount(buy.delta_cents, &hub_info).min(hub_remaining);
            if want_in <= 0 {
                continue;
            }
            // Oracle price of the hub expressed in the bought asset's units.
            let hub_price_in_bought_asset =
                (hub_info.oracle_price * oracle_scale) / buy.info.oracle_price;

            let Some((provider, amount_in, amount_out)) = self
                .best_route(
                    want_in,
                    &self.config.hub_address,
                    &buy.address,
                    hub_price_in_bought_asset,
                )
                .await
            else {
                BalancerOutcome::NoViableProvider.record();

                continue;
            };

            let value_cents = compute_value_cents(amount_in, &hub_info);
            if value_cents < self.config.min_swap_amount_value_cents {
                debug!(asset = %buy.address, amount_in, value_cents, "buy below dust threshold");
                BalancerOutcome::BelowDust.record();

                continue;
            }

            let op_id = match self.liquidator_capital.reserve(
                amount_in,
                hub_available,
                &self.config.hub_address,
            ) {
                Ok(id) => id,
                Err(e) => {
                    warn!(?e, asset = %buy.address, amount_in, "buy reservation lost; skipping");
                    BalancerOutcome::ReservationLost.record();

                    continue;
                }
            };

            let min_amount_out = amount_out
                .saturating_mul(BPS_FACTOR - self.config.allowed_swap_slippage_bps)
                / BPS_FACTOR;
            let path = [self.config.hub_address.as_str(), buy.address.as_str()];
            match self.gateway.swap_exact_tokens_request(
                &provider,
                amount_in,
                min_amount_out,
                &path,
            ) {
                Ok(req) => {
                    requests.push(req);
                    op_ids.push(op_id);

                    hub_remaining = hub_remaining.saturating_sub(amount_in);
                    dispatched_value_cents = dispatched_value_cents.saturating_add(value_cents);

                    BalancerOutcome::BuyLegDispatched.record();

                    info!(asset = %buy.address, %provider, amount_in, amount_out, min_amount_out, "buy leg queued");
                }
                Err(e) => {
                    warn!(?e, asset = %buy.address, "failed to build buy request; releasing");
                    self.liquidator_capital.release(op_id);
                }
            }
        }

        if requests.is_empty() {
            return vec![];
        }

        // --- Assemble one atomic batch: sells already precede buys. ---

        let op = match self.gateway.batch_op(&self.config.market, &self.liquidator_key, &requests) {
            Ok(op) => op,
            Err(e) => {
                error!(?e, "failed to build rebalance batch op; releasing reservations");
                for op_id in &op_ids {
                    self.liquidator_capital.release(*op_id);
                }

                BalancerOutcome::EvaluationError.record();

                return vec![];
            }
        };

        let legs = requests.len();
        metrics::record_balancer_batch_legs(legs); // TODO: Rename legs to something more readable?
        metrics::record_balancer_dispatched_value(dispatched_value_cents);
        BalancerOutcome::Dispatched.record();
        info!(legs, dispatched_value_cents, market = %self.config.market, "submitting rebalance batch...");

        vec![Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_submission_retries: self.config.max_retries,
            on_settle: Some(TransactionSettleHook {
                op_ids,
                liquidation_outcome: None,
                liquidator_capital: self.liquidator_capital.clone(),
            }),
        })]
    }

    /// Probes every configured provider for the best
    /// `asset_in -> asset_out` route starting from `base_in`, halving on execution\oracle price impact.
    /// Returns `(provider, amount_in, amount_out)` of the winner, if any.
    async fn best_route(
        &self,
        base_in: i128,
        asset_in: &str,
        asset_out: &str,
        oracle_cross_price: i128,
    ) -> Option<(String, i128, i128)> {
        let mut best: Option<(String, i128, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .probe_provider(
                    base_in,
                    provider,
                    asset_in,
                    asset_out,
                    self.assets_info.get(asset_in).map(|i| i.oracle_decimals).unwrap_or_default(),
                    oracle_cross_price,
                )
                .await
            {
                Ok(Some((amount_in, amount_out))) => {
                    let take = match &best {
                        None => true,
                        Some((_, _, best_out)) => amount_out > *best_out,
                    };
                    if take {
                        best = Some((provider.clone(), amount_in, amount_out));
                    }
                }
                Ok(None) => warn!(%provider, %asset_in, "provider unviable"),
                Err(e) => {
                    warn!(?e, %asset_in, "route probe failed");
                    BalancerOutcome::EvaluationError.record();
                }
            }
        }

        best
    }

    /// Probes swap provider for a swap (amount_in, amount_out) pair that adheres to the execution price
    /// constraint by halving the amount_in iteratively in case of exceeding the impact.
    async fn probe_provider(
        &self,
        base_in: i128,
        provider: &str,
        asset_in: &str,
        asset_out: &str,
        oracle_decimals: u32,
        oracle_cross_price: i128,
    ) -> anyhow::Result<Option<(i128, i128)>> {
        let mut amount_in = base_in;

        for _ in 0..self.config.max_swap_provider_halving_probes {
            if amount_in <= 0 {
                return Ok(None);
            }

            let amount_out =
                self.ledger_reader.get_amount_out(amount_in, asset_in, asset_out, provider).await?;
            let Some(impact) = compute_swap_execution_impact(
                amount_in,
                amount_out,
                oracle_cross_price,
                oracle_decimals,
            ) else {
                error!(
                    %provider, %asset_in, amount_in, amount_out, oracle_cross_price,
                    "price impact not computable (non-positive input); treating probe as unviable"
                );

                return Ok(None);
            };

            // Observe the oracle-vs-DEX price gap for every probe, tagged by
            // whether it cleared the configured ceiling. This is the realised
            // spread the balancer is gating on — useful for tuning
            // `max_execution_impact_bps` and spotting oracle/DEX divergence.
            let admitted = impact.bps <= self.config.max_execution_impact_bps;
            metrics::record_balancer_price_impact(asset_in, admitted, impact.bps);

            if admitted {
                info!(
                    %provider, %asset_in, amount_in, amount_out,
                    price_impact_bps = impact.bps,
                    execution_price_scaled = impact.execution_price_scaled,
                    "probe within max execution impact"
                );

                return Ok(Some((amount_in, amount_out)));
            }

            warn!(
                %provider, %asset_in, amount_in, amount_out,
                price_impact_bps = impact.bps,
                max = self.config.max_execution_impact_bps,
                "probe exceeds max execution impact; halving amount_in"
            );
            amount_in /= 2;
        }

        Ok(None)
    }

    fn update_assets_info(&mut self, market_data: &MarketData) {
        for pool in &market_data.pools_data {
            self.assets_info.insert(
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

            return false;
        }

        if self.config.swap_providers.is_empty() {
            warn!("swap_providers is empty; skipping rebalance");

            return false;
        }

        true
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
/// oracle cross-price ratio, returning both the price impact (BPS) and the
/// realised execution price.
fn compute_swap_execution_impact(
    amount_in: i128,
    amount_out: i128,
    oracle_cross_price: i128,
    oracle_price_decimals: u32,
) -> Option<SwapPriceImpact> {
    // Guard both divisions: `amount_in` scales the realised price,
    // `oracle_cross_price` scales the BPS conversion.
    if amount_in <= 0 || oracle_cross_price <= 0 {
        return None;
    }

    let scale = 10_i128.checked_pow(oracle_price_decimals)?;
    let execution_price_scaled = amount_out.checked_mul(scale)?.checked_div(amount_in)?;

    // execution < oracle → positive impact (received less than oracle value)
    // execution > oracle → negative impact (did better than oracle)
    let price_diff = oracle_cross_price.checked_sub(execution_price_scaled)?;
    let bps = price_diff.checked_mul(BPS_FACTOR)?.checked_div(oracle_cross_price)?;

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

    token_amount.saturating_mul(info.oracle_price) / denom
}

/// Inverse of [`compute_value_cents`]: the token amount whose oracle value is
/// approximately `value_cents`. Rounds down (truncating integer division), so a
/// leg sized from this never over-shoots the intended value correction.
fn value_to_token_amount(value_cents: i128, info: &AssetInfo) -> i128 {
    if info.oracle_price <= 0 || value_cents <= 0 {
        return 0;
    }
    let denom_pow = info.decimals + info.oracle_decimals;
    if denom_pow < 2 {
        return 0;
    }
    // value_cents = token_amount * oracle_price / 10^(denom_pow - 2)
    //  => token_amount = value_cents * 10^(denom_pow - 2) / oracle_price
    let scale = 10_i128.checked_pow(denom_pow - 2).unwrap_or(0); // TODO: Fix error swallowing
    if scale == 0 {
        return 0;
    }
    value_cents.saturating_mul(scale) / info.oracle_price
}

/// An asset that has drifted past the threshold and needs correcting. Direction
/// is implied by which bucket it lands in (surplus → sell, deficit → buy).
struct Drift {
    address: String,
    info: AssetInfo,
    /// Gross swappable (post XLM safety margin) — passed to the reserve gate.
    swappable: i128,
    /// Net of live reservations — bounds how much of a sell we actually size.
    deployable: i128,
    /// Absolute value gap between current holding and target, in cents.
    delta_cents: i128,
    /// Absolute drift from target weight, in bps — the sort key.
    abs_drift_bps: i128,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(decimals: u32, oracle_price: i128, oracle_decimals: u32) -> AssetInfo {
        AssetInfo { decimals, oracle_price, oracle_decimals }
    }

    #[test]
    fn value_and_token_amount_round_trip() {
        // 7-decimal token, oracle price 1.00 at 7 oracle decimals => $1/token.
        let i = info(7, 10_000_000, 7);
        // 5 whole tokens = 5 * 10^7 base units => $5.00 => 500 cents.
        let tokens = 5 * 10_i128.pow(7);
        let value = compute_value_cents(tokens, &i);
        assert_eq!(value, 500);

        // Inverse recovers the token amount (exact here, no truncation).
        assert_eq!(value_to_token_amount(value, &i), tokens);
    }

    #[test]
    fn value_to_token_amount_rounds_down() {
        let i = info(7, 10_000_000, 7);
        // 501 cents doesn't divide evenly by the per-token value, but must never
        // round up (over-shoot the correction).
        let recovered = value_to_token_amount(501, &i);
        assert!(compute_value_cents(recovered, &i) <= 501);
    }

    #[test]
    fn non_positive_inputs_are_safe() {
        let i = info(7, 10_000_000, 7);
        assert_eq!(value_to_token_amount(0, &i), 0);
        assert_eq!(value_to_token_amount(-100, &i), 0);
        assert_eq!(compute_value_cents(-100, &i), 0);
        assert_eq!(compute_value_cents(100, &info(7, 0, 7)), 0);
    }

    fn drift(address: &str, abs_drift_bps: i128) -> Drift {
        Drift {
            address: address.to_string(),
            info: info(7, 10_000_000, 7),
            swappable: 0,
            deployable: 0,
            delta_cents: 0,
            abs_drift_bps,
        }
    }

    #[test]
    fn worst_drift_first_with_address_tiebreak() {
        let mut v = vec![drift("B", 100), drift("A", 300), drift("C", 300)];
        v.sort_by(|a, b| {
            b.abs_drift_bps.cmp(&a.abs_drift_bps).then_with(|| a.address.cmp(&b.address))
        });
        let order: Vec<_> = v.iter().map(|d| d.address.as_str()).collect();
        // Largest drift first; equal drifts broken by ascending address.
        assert_eq!(order, ["A", "C", "B"]);
    }
}
