use {
    crate::{
        collectors::block_collector::NewBlock,
        executors::tx_executor::SubmitStellarTx,
        helper,
        types::{Action, BoxFuture, Event, MarketData, ObligationKey, Strategy},
    },
    ed25519_dalek::SigningKey,
    std::collections::{HashMap, HashSet},
    stellar_rpc_client::Client,
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{debug, error, info, trace, warn},
    url::Url,
};

const BPS_FACTOR: i128 = 10_000;
const MAX_REBALANCE_RETRIES: u32 = 3;

/// Probe sizes for the AMM_Max_Swap_Estimator algorithm. The doc explicitly
/// recommends larger probes (1e6 / 2e6) so the `(2y1 - y2)` denominator is
/// resilient to integer rounding. We fall back to (1, 2) when the input
/// balance can't accommodate the larger probe.
const PROBE_LARGE_LO: i128 = 1_000_000;
const PROBE_LARGE_HI: i128 = 2_000_000;

pub struct RebalancerConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    /// First element is the swap target. May be empty, in which case the
    /// rebalancer is a no-op.
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    /// Price-impact cap, in basis points.
    pub max_price_impact_bps: i128,
    /// External slippage cap, in basis points.
    pub slippage_bps: i128,
    /// Skip swaps below this dollar-value threshold (in cents).
    pub min_swap_amount_value_cents: i128,
    pub interval_blocks: u32,
}

pub struct AssetInfo {
    decimals: u32,
    oracle_price: i128,
    oracle_decimals: u32,
}

pub struct Rebalancer {
    rpc: Client,
    pkey: String,
    skey: SigningKey,
    config: RebalancerConfig,
    liquidator_key: ObligationKey,
    asset_index: HashMap<String, AssetInfo>,
    market_data: HashMap<String, MarketData>,
}

impl Strategy<Event, Action> for Rebalancer {
    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(markets = ?self.config.markets, "Rebalancer: syncing state");

            let markets = self.config.markets.clone(); // TODO: Fighting borrowck here
            for market in self.config.markets.clone() {
                self.refresh_market_data(&market).await;
            }

            // TODO: Do something about liquidator's obligations

            Ok(())
        })
    }

    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async {
            match event {
                Event::NewBlock(b) => self.handle_new_block(b).await,
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
            }
        })
    }
}

impl Rebalancer {
    pub fn try_create(config: RebalancerConfig, skey: &SigningKey) -> anyhow::Result<Self> {
        let rpc = Client::new(config.rpc_url.as_str())?;
        let pkey = ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
            skey.verifying_key().to_bytes(),
        ))))
        .to_string();

        let liquidator_key = ObligationKey::new(pkey.clone());

        Ok(Self {
            rpc,
            pkey,
            config,
            liquidator_key,
            skey: skey.clone(),
            market_data: HashMap::new(),
            asset_index: HashMap::new(),
        })
    }

    async fn handle_new_block(&mut self, block: NewBlock) -> Vec<Action> {
        if block.number % self.config.interval_blocks != 0 {
            return vec![];
        }
        if self.config.assets_to_hold.is_empty() {
            warn!("Rebalancer: assets_to_hold is empty; skipping tick");

            return vec![];
        }
        if self.config.swap_providers.is_empty() {
            warn!("Rebalancer: swap_providers is empty; skipping tick");

            return vec![];
        }

        let markets: Vec<String> = self.config.markets.clone();
        for market in &markets {
            self.refresh_market_data(market).await;
        }

        self.find_rebalance_actions().await
    }

    /// Compute the set of assets the liquidator should NOT be holding.
    fn build_candidate_set(&mut self) -> Vec<String> {
        self.rebuild_asset_index();

        let held: HashSet<&str> = self
            .config
            .assets_to_hold
            .iter()
            .map(String::as_str)
            .collect();

        self.asset_index
            .keys()
            .filter(|addr| !held.contains(addr.as_str()))
            .cloned()
            .collect()
    }

    /// Rebuild `asset_index` from the freshly refreshed `market_data`. If a
    /// token appears in multiple markets, the *last* market wins — pricing
    /// should be identical across markets that share the same token.
    fn rebuild_asset_index(&mut self) {
        self.asset_index.clear();
        for md in self.market_data.values() {
            for pool in &md.pools_data {
                self.asset_index.insert(
                    pool.token_address.clone(),
                    AssetInfo {
                        decimals: pool.token_decimals,
                        oracle_price: pool.oracle_asset_price,
                        oracle_decimals: md.oracle_price_decimals,
                    },
                );
            }
        }
    }

    async fn find_rebalance_actions(&mut self) -> Vec<Action> {
        let candidates = self.build_candidate_set();
        let target_asset = &self.config.assets_to_hold[0];

        let mut actions = vec![];
        for candidate in &candidates {
            if candidate == target_asset {
                continue;
            }

            match self.evaluate_candidate(candidate, target_asset).await {
                Ok(Some(action)) => actions.push(action),
                Ok(None) => {}
                Err(e) => warn!(?e, %candidate, "candidate evaluation failed"),
            }
        }

        actions
    }

    async fn handle_soroban_event(&mut self, event: stellar_rpc_client::Event) -> Vec<Action> {
        todo!()
    }

    async fn refresh_market_data(&mut self, market_address: &str) {
        match helper::simulate_get_market_data(&self.rpc, market_address, &self.pkey).await {
            Ok(market_data) => {
                info!(%market_address, "Rebalancer: refreshed market data");
                self.market_data
                    .insert(market_address.to_string(), market_data);
            }
            Err(e) => error!(?e, %market_address, "Rebalancer: failed to refresh market data"),
        }
    }

    /// Returns `Ok(Some(action))` if a viable swap was found, `Ok(None)` if
    /// the candidate should be skipped (zero balance, dust, infeasible probes,
    /// etc.), or `Err` for unexpected RPC failures we want to log.
    async fn evaluate_candidate(
        &self,
        candidate: &str,
        target: &str,
    ) -> anyhow::Result<Option<Action>> {
        let raw_balance =
            helper::query_token_balance(&self.rpc, candidate, &self.pkey, &self.pkey).await?;

        let balance_to_swap = if candidate == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        if !balance_to_swap.is_positive() {
            trace!(%candidate, raw_balance, balance_to_swap, "Nothing to swap");

            return Ok(None);
        }

        // Deduce the liquidity reserves

        const ENSURE_RESERVES_ATTEMPTS: u32 = 3;

        let mut reserves: Option<(i128, i128)> = None;
        // `get_amount_out` is a read-only simulation, so the probe size is
        // independent of the liquidator's balance — it only needs to be large
        // enough to expose the AMM's curvature (the (2y1 - y2) denominator
        // collapses to 0 for tiny probes because the formula degenerates to
        // a linear approximation). The cap by `balance_to_swap` happens later
        // on the algorithm's output Δx.
        let (probe_lo, probe_hi) = (PROBE_LARGE_LO, PROBE_LARGE_HI);
        let path: [&str; 2] = [candidate, target];

        // (provider, dx, expected_output)
        let mut best: Option<(String, i128, i128)> = None;

        for provider in &self.config.swap_providers {
            match self
                .probe_provider(provider, &path, balance_to_swap, probe_lo, probe_hi)
                .await
            {
                Ok(Some((dx, out))) => {
                    debug!(
                        %provider,
                        %candidate,
                        dx,
                        out,
                        "Rebalancer: provider probe ok"
                    );
                    let take = best.as_ref().map(|(_, _, o)| out > *o).unwrap_or(true);
                    if take {
                        best = Some((provider.clone(), dx, out));
                    }
                }
                Ok(None) => {
                    debug!(%provider, %candidate, "Rebalancer: provider unviable");
                }
                Err(e) => {
                    warn!(?e, %provider, %candidate, "Rebalancer: provider probe failed");
                }
            }
        }
        let Some((provider, dx_final, expected_out)) = best else {
            return Ok(None);
        };

        let info = self
            .asset_index
            .get(candidate)
            .expect("candidate sourced from asset_index");
        let value_cents = self.calculate_value_cents(dx_final, info);
        if value_cents < self.config.min_swap_amount_value_cents {
            info!(
                %candidate,
                dx_final,
                value_cents,
                min_required = self.config.min_swap_amount_value_cents,
                "Rebalancer: candidate below dust threshold; skipping"
            );
            return Ok(None);
        }

        let min_amount_out =
            expected_out.saturating_mul(BPS_FACTOR - self.config.slippage_bps) / BPS_FACTOR;

        let request = helper::build_swap_exact_tokens_request_scval(
            &provider,
            &path,
            dx_final,
            min_amount_out,
        )?;

        let market_address = &self.config.markets[0];
        let op = helper::build_batch_op(market_address, &self.liquidator_key, &[request])?;

        info!(
            %candidate,
            %target,
            %provider,
            dx_final,
            expected_out,
            min_amount_out,
            value_cents,
            market = %market_address,
            "Rebalancer: submitting swap"
        );

        Ok(Some(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_retries: MAX_REBALANCE_RETRIES,
        })))
    }

    fn calculate_value_cents(&self, token_amount: i128, info: &AssetInfo) -> i128 {
        if info.oracle_price <= 0 {
            return 0;
        }
        let denom_pow = info.decimals + info.oracle_decimals;
        // Subtract 2 to express the result in cents instead of whole dollars.
        if denom_pow < 2 {
            return 0;
        }
        let denom = 10_i128.pow(denom_pow - 2);
        if denom == 0 {
            return 0;
        }
        (token_amount.saturating_mul(info.oracle_price) / denom).max(0)
    }

    async fn probe_provider(
        &self,
        provider: &str,
        path: &[&str],
        balance_to_swap: i128,
        probe_lo: i128,
        probe_hi: i128,
    ) -> anyhow::Result<Option<(i128, i128)>> {
        // Maximum number of times we'll double the probe size searching for
        // measurable curvature. Each doubling roughly quadruples the
        // (2y_lo - y_hi) gap, so 12 doublings = ~4096× the starting probe.
        const MAX_DOUBLINGS: u32 = 12;
        // The denominator (2y_lo - y_hi) must be at least 1/this fraction of
        // y_lo to be considered statistically meaningful. 1000 ≈ 0.1%
        // measurement error on the derived virtual reserve, which in turn
        // bounds the price-impact estimate's error.
        const RELATIVE_CURVATURE_THRESHOLD_RATIO: i128 = 1_000;
        // For each probe size we issue the query twice and require identical
        // answers. Catches mid-tick reserve drift that would otherwise leak
        // into the reserve estimate.
        const STABILITY_RETRIES: u32 = 2;

        let (mut probe_lo, mut probe_hi) = (probe_lo, probe_hi);
        let mut last_good: Option<(i128, i128, i128, i128)> = None; // (probe_lo, probe_hi, y_lo, y_hi)

        for doubling in 0..=MAX_DOUBLINGS {
            // Stability check: query twice, require identical results.
            let mut last_pair: Option<(i128, i128)> = None;
            let mut stable_pair: Option<(i128, i128)> = None;
            let mut probe_failed = false;

            for _ in 0..STABILITY_RETRIES {
                let y_lo_res = helper::simulate_swap_provider_get_amount_out(
                    &self.rpc, provider, &self.pkey, probe_lo, path,
                )
                .await;
                let y_hi_res = helper::simulate_swap_provider_get_amount_out(
                    &self.rpc, provider, &self.pkey, probe_hi, path,
                )
                .await;

                let (y_lo, y_hi) = match (y_lo_res, y_hi_res) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => {
                        // Pool couldn't quote this size — too thin. Fall back
                        // to the largest probe pair we've already accepted.
                        probe_failed = true;
                        break;
                    }
                };

                if !y_lo.is_positive() || !y_hi.is_positive() {
                    warn!(provider, y_lo, y_hi, "Rebalancer: probes returned non-positive");
                    return Ok(None);
                }

                if let Some(prev) = last_pair {
                    if prev == (y_lo, y_hi) {
                        stable_pair = Some((y_lo, y_hi));
                        break;
                    }
                    // Differed — replace and try once more.
                }
                last_pair = Some((y_lo, y_hi));
            }

            if probe_failed {
                // Pool can't quote `probe_hi` at this size. Stop doubling.
                break;
            }

            let (y_lo, y_hi) = match stable_pair.or(last_pair) {
                Some(p) => p,
                None => return Ok(None), // shouldn't happen: STABILITY_RETRIES >= 1
            };
            if stable_pair.is_none() {
                debug!(provider, probe_lo, probe_hi, "Rebalancer: probes unstable, using last");
            }

            // Curvature check: is (2y_lo - y_hi) big enough to be meaningful?
            let denom = 2 * y_lo - y_hi;
            let threshold = y_lo / RELATIVE_CURVATURE_THRESHOLD_RATIO;
            if denom > threshold && denom > 0 {
                last_good = Some((probe_lo, probe_hi, y_lo, y_hi));
                debug!(
                    provider,
                    probe_lo, probe_hi, y_lo, y_hi, denom, threshold, doubling,
                    "Rebalancer: probes curvature acceptable"
                );
                break;
            }

            // Save what we have in case the next doubling fails.
            if denom > 0 {
                last_good = Some((probe_lo, probe_hi, y_lo, y_hi));
            }

            if doubling == MAX_DOUBLINGS {
                warn!(
                    provider, probe_lo, probe_hi, y_lo, y_hi, denom,
                    "Rebalancer: gave up doubling probes; curvature still below threshold"
                );
                break;
            }

            // Double and try again. checked_mul guards against overflow on
            // pathological pool sizes.
            let Some(next_lo) = probe_lo.checked_mul(2) else { break };
            let Some(next_hi) = probe_hi.checked_mul(2) else { break };
            probe_lo = next_lo;
            probe_hi = next_hi;
        }

        let Some((_, _, y_lo, y_hi)) = last_good else {
            // No probe pair ever succeeded enough to derive a reserve.
            return Ok(None);
        };

        const DEFAULT_FEE_BPS: i128 = 30;
        let Some(dx_undounded) = compute_max_swap_input(
            y_lo,
            y_hi,
            DEFAULT_FEE_BPS,
            self.config.max_price_impact_bps,
        ) else {
            warn!(
                provider, y_lo, y_hi,
                "Rebalancer: degenerate probes (2y1 - y2 <= 0) after adaptive doubling"
            );
            return Ok(None);
        };

        let dx_final = dx_undounded.min(balance_to_swap);
        if !dx_final.is_positive() {
            return Ok(None);
        }

        let realized = helper::simulate_swap_provider_get_amount_out(
            &self.rpc, provider, &self.pkey, dx_final, path,
        )
        .await?;
        if realized <= 0 {
            return Ok(None);
        }

        Ok(Some((dx_final, realized)))
    }
}

/// Returns the maximum `Δx` that can be swapped without exceeding the price
/// impact cap `S = slippage_bps / 10_000`, using the constant-product virtual
/// reserve derived from the two probes `y1 = get_amount_out(p1)` and
/// `y2 = get_amount_out(p2)` where `p2 = 2 * p1`.
///
/// All math stays in `i128`; multiplications are performed before divisions to
/// preserve precision. The reserve estimate is ceil-rounded (conservative,
/// per the doc); the final Δx is floor-rounded so we never *exceed* the cap.
///
/// Returns `None` when:
///   - the denominator `(2 * y1 - y2)` is non-positive (probes too small or
///     the pool is degenerate), or
///   - the resulting Δx would be negative (shouldn't happen with sane inputs).
fn compute_max_swap_input(
    y_lo: i128,
    y_hi: i128,
    fee_bps: i128,
    max_price_impact_bps: i128,
) -> Option<i128> {
    if !max_price_impact_bps.is_positive() {
        return Some(0);
    }
    if !(0..=BPS_FACTOR).contains(&fee_bps) {
        return None;
    }

    let gamma_bps = BPS_FACTOR - fee_bps;

    let reserve_denom = y_lo.checked_mul(2)?.checked_sub(y_hi)?;
    if !reserve_denom.is_positive() {
        return None;
    }

    let dy = y_hi.checked_sub(y_lo)?;
    if !dy.is_positive() {
        return None;
    }

    let num = 2_i128.checked_mul(gamma_bps)?.checked_mul(dy)?;
    let denom = reserve_denom.checked_mul(BPS_FACTOR)?;

    let reserve = (num + denom - 1) / denom; // ceil
    if !reserve.is_positive() {
        return None;
    }

    let dx_num = max_price_impact_bps
        .checked_mul(reserve)?
        .checked_mul(BPS_FACTOR)?;
    let dx_denom = gamma_bps.checked_mul(BPS_FACTOR - max_price_impact_bps)?;
    if dx_denom <= 0 {
        return None;
    }

    Some(dx_num / dx_denom)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plan-prescribed sanity check: y1=997, y2=1990, fee=30bps (0.3%),
    /// slippage=100bps (1%).
    ///
    /// γ_bps = 9970, reserve_denom = 2*997 - 1990 = 4
    /// reserve = ceil( 2 * 9970 * (1990-997) / (4 * 10_000) )
    ///         = ceil( 19,800,420 / 40,000 ) = ceil(495.0105) = 496
    /// dx     = floor( 100 * 496 * 10_000 / (9970 * 9_900) )
    ///         = floor( 496,000,000 / 98,703,000 ) = 5
    #[test]
    fn compute_max_swap_input_known_values() {
        let dx = compute_max_swap_input(997, 1990, 30, 100).unwrap();
        assert_eq!(dx, 5);
    }

    #[test]
    fn compute_max_swap_input_degenerate_denominator_returns_none() {
        // 2*y1 - y2 = 0 → undefined virtual reserve.
        assert!(compute_max_swap_input(100, 200, 30, 100).is_none());
        // 2*y1 - y2 < 0 (probes lying outside CPMM curve due to rounding).
        assert!(compute_max_swap_input(100, 250, 30, 100).is_none());
    }

    #[test]
    fn compute_max_swap_input_zero_slippage_returns_some_zero() {
        assert_eq!(compute_max_swap_input(997, 1990, 30, 0), Some(0));
    }

    #[test]
    fn compute_max_swap_input_full_slippage_invalid() {
        // S >= 100% would divide by zero in the (1-S) term.
        assert!(compute_max_swap_input(997, 1990, 30, BPS_FACTOR).is_none());
    }

    #[test]
    fn compute_max_swap_input_invalid_fee() {
        assert!(compute_max_swap_input(997, 1990, -1, 100).is_none());
        assert!(compute_max_swap_input(997, 1990, BPS_FACTOR, 100).is_none());
    }

    #[test]
    fn compute_max_swap_input_larger_probes_yield_same_dx() {
        // Δx is a function of the *virtual reserve* (a property of the pool)
        // and the slippage cap S — not of the probe size. Larger probes only
        // improve the precision of the (2y1 - y2) denominator. So scaling
        // both probes by the same factor must yield (roughly) the same Δx.
        let dx_small = compute_max_swap_input(997, 1990, 30, 100).unwrap();
        let dx_large = compute_max_swap_input(997_000_000, 1_990_000_000, 30, 100).unwrap();
        // Allow ceil-rounding noise on the reserve (a few atoms either way).
        assert!(
            (dx_small - dx_large).abs() <= 5,
            "dx_small={dx_small} dx_large={dx_large}"
        );
    }
}
