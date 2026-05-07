use {
    crate::{
        collectors::block_collector::NewBlock,
        executors::tx_executor::SubmitStellarTx,
        helper,
        types::{Action, BoxFuture, Event, MarketData, ObligationKey, Strategy},
    },
    ed25519_dalek::SigningKey,
    helper::OperationEvent,
    std::collections::{HashMap, HashSet},
    stellar_rpc_client::Client,
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{debug, error, info, warn},
    url::Url,
};

const BPS_FACTOR: i128 = 10_000;
const MAX_SWAP_RETRIES: u32 = 3;
const MAX_PROBE_STABILITY_RETRIES: u32 = 2;

// Probe sizes used to invert the AMM curve in `probe_provider`. The 10×
// spread (vs. the original 2×) widens the range over which `y_hi - y_lo`
// must be non-linear, making the curvature-blindness degeneracy in
// `compute_swap_amounts`'s `denom = y_lo*x_hi - x_lo*y_hi` term far less
// likely to collapse on deep pools. This is a *mitigation*, not a true
// fix — see `[BUG] #1` in the module-level review notes for the proper
// curvature-gate solution.
const PROBE_LARGE_LO: i128 = 10i128.pow(9);
const PROBE_LARGE_HI: i128 = 10i128.pow(10);

pub struct RebalancerConfig {
    pub rpc_url: Url,
    pub markets: Vec<String>,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    /// First element is the swap target. May be empty, in which case the
    /// rebalancer is a no-op.
    // TODO: Come up with a smarter strategy to choose a target asset
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    /// Price-impact cap, in basis points.
    pub max_price_impact_bps: i128,
    /// External slippage cap, in basis points.
    pub max_slippage_bps: i128,
    /// Upper-bound DEX swap fee across all configured `swap_providers`, in
    /// basis points. We deliberately use the maximum (not the per-provider
    /// actual) so that:
    ///   * `dx_max`  is sized as if the fee were as bad as it can be →
    ///     realized price impact stays under the cap even on the worst
    ///     provider.
    ///   * predicted `amount_out` is conservative → `min_amount_out` is
    ///     looser than reality, so swaps don't revert from over-tight
    ///     slippage on lower-fee providers.
    ///     Both biases land in the safe direction; the cost is leaving a small
    ///     amount of capital efficiency on the table for sub-max-fee providers.
    pub max_fee_bps: i128,
    pub refresh_interval_blocks: u32,
    /// Skip swaps below this dollar-value threshold (in cents).
    pub min_swap_amount_value_cents: i128,
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
        Box::pin(async { Ok(()) })
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
        if !block.number.is_multiple_of(self.config.refresh_interval_blocks) {
            return vec![];
        }
        if !self.rebalance_preconditions_met() {
            return vec![];
        }

        self.refresh_all_markets().await;
        self.find_rebalance_actions().await
    }

    async fn handle_soroban_event(&mut self, event: stellar_rpc_client::Event) -> Vec<Action> {
        if let Ok(key) = helper::parse_obligation_key_from_topic(&event, 2)
            && key.user != self.pkey {
                // Ignore events triggered not by the liquidator
                return vec![];
            }

        if !matches!(
            helper::decode_operation_event(&event),
            Ok(OperationEvent::Liquidate) | Ok(OperationEvent::Withdraw)
        ) {
            return vec![];
        }

        info!(
            ?event,
            "Detected liquidator possible balance increase event"
        );

        if !self.rebalance_preconditions_met() {
            return vec![];
        }

        self.refresh_all_markets().await;
        self.find_rebalance_actions().await
    }

    /// Returns false (and emits a warn-level log explaining why) when the
    /// rebalancer can't make progress on this trigger. Centralized here so
    /// `handle_new_block` and `handle_soroban_event` can't drift on the
    /// preconditions or on the log wording.
    fn rebalance_preconditions_met(&self) -> bool {
        if self.config.assets_to_hold.is_empty() {
            warn!("Rebalancer: assets_to_hold is empty; skipping rebalance");
            return false;
        }
        if self.config.swap_providers.is_empty() {
            warn!("Rebalancer: swap_providers is empty; skipping rebalance");
            return false;
        }
        true
    }

    /// Refresh market data for every configured market, sequentially. The
    /// clone is to release the borrow on `self.config.markets` before the
    /// `&mut self` call to `refresh_market_data`.
    async fn refresh_all_markets(&mut self) {
        let markets: Vec<String> = self.config.markets.clone();
        for market in &markets {
            self.refresh_market_data(market).await;
        }
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

    async fn find_rebalance_actions(&mut self) -> Vec<Action> {
        let rebalancable_assets = self.get_rebalancable_assets();
        let target_asset = &self.config.assets_to_hold[0];

        let mut actions = vec![];
        for candidate in &rebalancable_assets {
            if candidate == target_asset {
                error!(
                    candidate,
                    target_asset, "Rebalancable candidate equals to target asset"
                );

                continue;
            }

            match self
                .evaluate_rebalancable_candidate(target_asset, candidate)
                .await
            {
                Ok(opt) => {
                    if let Some(action) = opt {
                        actions.push(action);
                    }
                }
                Err(e) => warn!(?e, %candidate, "candidate evaluation failed"),
            }
        }

        actions
    }

    fn get_rebalancable_assets(&mut self) -> Vec<String> {
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

    async fn evaluate_rebalancable_candidate(
        &self,
        target: &str,
        candidate: &str,
    ) -> anyhow::Result<Option<Action>> {
        let raw_balance =
            helper::query_token_balance(&self.rpc, candidate, &self.pkey, &self.pkey).await?;

        let balance_to_swap = if candidate == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        if !balance_to_swap.is_positive() {
            debug!(%candidate, raw_balance, balance_to_swap, "Nothing to swap");

            return Ok(None);
        }

        let (probe_lo, probe_hi) = (PROBE_LARGE_LO, PROBE_LARGE_HI);
        let path: [&str; 2] = [candidate, target];

        // (provider, dx, expected_output)
        let mut best_provider: Option<(String, i128, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .probe_provider(provider, &path, balance_to_swap, probe_lo, probe_hi)
                .await
            {
                Ok(opt) => match opt {
                    Some((amount_in, amount_out)) => {
                        debug!(
                            %provider,
                            %candidate,
                            amount_in,
                            amount_out,
                            "Rebalancer: provider probe ok"
                        );

                        // Pick the provider with the best *price per unit
                        // input*, i.e. the highest `amount_out / amount_in`
                        // ratio. Comparing absolute `amount_out` is wrong
                        // because each provider's `dx_max` is derived from
                        // its own `res_x`, so different providers can
                        // quote with different `amount_in` values — the
                        // one allowed to spend more would always look
                        // "better" by raw output.
                        //
                        // Cross-multiply to avoid float math:
                        //   new_out / new_in > best_out / best_in
                        //   ⇔ new_out * best_in > best_out * new_in
                        // (all four operands are positive, checked above).
                        // On the (extremely unlikely) i128 overflow we
                        // fall back to absolute-out comparison so we
                        // still pick *something* rather than silently
                        // skip the candidate.
                        let take = match best_provider.as_ref() {
                            None => true,
                            Some((_, best_in, best_out)) => {
                                match (
                                    amount_out.checked_mul(*best_in),
                                    best_out.checked_mul(amount_in),
                                ) {
                                    (Some(lhs), Some(rhs)) => lhs > rhs,
                                    _ => amount_out > *best_out,
                                }
                            }
                        };
                        if take {
                            best_provider = Some((provider.clone(), amount_in, amount_out));
                        }
                    }
                    None => {
                        debug!(%provider, %candidate, "Rebalancer: provider unviable");
                    }
                },
                Err(e) => {
                    warn!(?e, %provider, %candidate, "Rebalancer: provider probe failed");
                }
            }
        }
        let Some((provider, amount_in, amount_out)) = best_provider else {
            return Ok(None);
        };

        let info = self
            .asset_index
            .get(candidate)
            .expect("candidate sourced from asset_index");
        let value_cents = compute_value_cents(amount_in, info);
        if value_cents < self.config.min_swap_amount_value_cents {
            info!(
                %candidate,
                amount_in,
                value_cents,
                min_required = self.config.min_swap_amount_value_cents,
                "Rebalancer: candidate below dust threshold; skipping"
            );

            return Ok(None);
        }

        let min_amount_out =
            amount_out.saturating_mul(BPS_FACTOR - self.config.max_slippage_bps) / BPS_FACTOR;

        let request = helper::build_swap_exact_tokens_request_scval(
            &provider,
            &path,
            amount_in,
            min_amount_out,
        )?;

        // TODO: when supporting more than one market, route each
        // candidate to the market it actually came from rather than
        // hardcoding `markets[0]`.
        // submits the swap to the wrong contract.
        let market_address = &self.config.markets[0];
        let op = helper::build_batch_op(market_address, &self.liquidator_key, &[request])?;

        info!(
            %candidate,
            value_cents,
            %target,
            %provider,
            amount_in,
            amount_out,
            min_amount_out,
            market = %market_address,
            "Rebalancer: submitting swap"
        );

        Ok(Some(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_retries: MAX_SWAP_RETRIES,
        })))
    }

    /// Derive a provider's *virtual* CPMM reserves by issuing two
    /// on-chain `get_amount_out` simulations and inverting the curve.
    ///
    /// Why probe instead of reading reserves directly?
    /// `ProxySwap` on market doesn't have such an interface.
    ///
    /// The inner loop guards against *probe drift*: if the pool's reserves
    /// change between the two simulations (e.g. another bot front-runs us
    /// or the ledger advances mid-call), the inversion is meaningless. We
    /// require two consecutive identical (y_lo, y_hi) reads before
    /// trusting the result; otherwise we bail with `Ok(None)` so the
    /// caller skips this provider for this cycle rather than acting on
    /// stale numbers.
    ///
    /// Returns `Ok(Some((amount_in, amount_out)))` when probes are stable
    /// and the derived reserves yield a viable swap under the configured
    /// price-impact and balance constraints; `Ok(None)` for any
    /// soft-failure (unstable probes, non-positive quotes, overflow,
    /// inconsistent geometry); and `Err` only for unrecoverable RPC or
    /// transport failures.
    async fn probe_provider(
        &self,
        provider: &str,
        path: &[&str],
        liquidator_balance: i128,
        probe_lo: i128,
        probe_hi: i128,
    ) -> anyhow::Result<Option<(i128, i128)>> {
        let mut stable_ys: Option<(i128, i128)> = None;
        let mut is_stable = false;

        for _ in 0..MAX_PROBE_STABILITY_RETRIES {
            let (y_lo, y_hi) = (
                helper::simulate_swap_provider_get_amount_out(
                    &self.rpc, provider, &self.pkey, probe_lo, path,
                )
                .await,
                helper::simulate_swap_provider_get_amount_out(
                    &self.rpc, provider, &self.pkey, probe_hi, path,
                )
                .await,
            );

            let Ok(y_lo) = y_lo.inspect_err(|e| error!(%e, probe_lo, "Couldn't get amount_out"))
            else {
                continue;
            };
            let Ok(y_hi) = y_hi.inspect_err(|e| error!(%e, probe_hi, "Couldn't get amount_out"))
            else {
                continue;
            };

            if !y_lo.is_positive() || !y_hi.is_positive() {
                error!(
                    %provider,
                    y_lo, y_hi, "Rebalancer: probes returned non-positive"
                );

                return Ok(None);
            }

            if let Some(prev) = stable_ys
                && prev == (y_lo, y_hi)
            {
                is_stable = true;

                break;
            }
            stable_ys = Some((y_lo, y_hi));
        }
        if !is_stable {
            warn!(%provider, "Probes aren't stable");

            return Ok(None);
        }

        let (y_lo, y_hi) = stable_ys.expect("Must be Some because of 'is_stable'");

        compute_swap_amounts(
            provider,
            probe_hi,
            probe_lo,
            y_lo,
            y_hi,
            self.config.max_fee_bps,
            liquidator_balance,
            self.config.max_price_impact_bps,
        )
    }
}

/// Turn two probe quotes into an actionable `(amount_in, amount_out)`
/// pair, capped by both price-impact and on-hand balance.
///
/// Pipeline (each step has a per-block comment below):
///   1. **Invert the CPMM curve** to recover `res_x` (the virtual input
///      reserve) from `(probe_lo, probe_hi, y_lo, y_hi)` and the
///      assumed worst-case fee `fee_bps`.
///   2. **Size the input** by the price-impact cap:
///      `dx_max = p_bps * res_x * B / (gamma_bps * (B - p_bps))`
///      then take `min(dx_max, liquidator_balance)` so we never try to
///      spend more than we hold.
///   3. **Recover `res_y`** from the `y_hi` probe and the now-known
///      `res_x`, so we can evaluate the curve locally.
///   4. **Compute `amount_out`** from the standard CPMM formula
///      `amount_out = res_y * gamma_bps * dx
///                    / (res_x * B + gamma_bps * dx)`
///      using the capped `amount_in`.
///
/// Returns:
///   * `Ok(Some((amount_in, amount_out)))` on success.
///   * `Ok(None)` for any soft-failure: i128 overflow, non-positive
///     intermediate (probes inconsistent / pool degenerate), or
///     out-of-range `max_price_impact_bps`. Caller should skip this
///     provider for this cycle.
///   * Currently no `Err` paths; the signature returns `Result` to leave
///     room for future hard failures without a breaking change.
///
/// All arithmetic is in i128 raw token units (assumed 7 decimals) with
/// fees and impact encoded in basis points (`B = BPS_FACTOR = 10_000`).
/// Every multiply is `checked_mul` to fail closed (return `None`) on
/// overflow rather than panic; divisions are floor-rounded so any
/// rounding error biases us *under* the safety caps, never over.
#[allow(clippy::too_many_arguments)]
fn compute_swap_amounts(
    provider: &str,
    probe_hi: i128,
    probe_lo: i128,
    y_lo: i128,
    y_hi: i128,
    fee_bps: i128,
    liquidator_balance: i128,
    max_price_impact_bps: i128,
) -> anyhow::Result<Option<(i128, i128)>> {
    // res_x = probe_lo * probe_hi * (1-fee) * (y_hi - y_lo) / (y_lo * x_hi - x_lo * y_hi)
    //
    // All quantities are in raw token units (assumed 7 decimals everywhere
    // for now). The (1 - fee) factor is encoded in basis points as
    // gamma_bps / BPS_FACTOR; we apply that division last to preserve as
    // much precision as integer math allows.
    //
    // TODO: source the per-provider fee from chain state instead of using
    // the AMM-default 0.3%. Once `RebalancerConfig` (or the provider
    // metadata) exposes it, thread it in as a parameter.
    let denom = match y_lo
        .checked_mul(probe_hi)
        // How easy for this to overflow?
        .and_then(|a| probe_lo.checked_mul(y_hi).and_then(|b| a.checked_sub(b)))
    {
        Some(d) => d,
        None => {
            error!(%provider, "Rebalancer: reserve-X denominator overflowed");

            return Ok(None);
        }
    };
    if denom <= 0 {
        error!(
            %provider,
            denom, y_lo, y_hi, probe_lo, probe_hi,
            "Rebalancer: non-positive reserve-X denominator (probes inconsistent)"
        );
        return Ok(None);
    }

    let gamma_bps = BPS_FACTOR - fee_bps;
    let dy = y_hi - y_lo; // positive: probe_hi > probe_lo on a concave curve
    let numer = match probe_lo
        .checked_mul(probe_hi)
        .and_then(|a| a.checked_mul(gamma_bps))
        .and_then(|a| a.checked_mul(dy))
    {
        Some(n) => n,
        None => {
            error!(%provider, "Rebalancer: reserve-X numerator overflowed");

            return Ok(None);
        }
    };

    let res_x = numer / denom / BPS_FACTOR;
    if res_x <= 0 {
        error!(
            %provider,
            res_x, "Rebalancer: derived non-positive X reserve"
        );
        return Ok(None);
    }

    debug!(
        %provider,
        res_x, y_lo, y_hi, probe_lo, probe_hi, fee_bps,
        "Rebalancer: derived virtual X reserve"
    );

    // max_amount_in = (max price impact * x) / gamma * (1 - max price impact)
    //
    // Derivation: for a CPMM with reserves (X, Y) and fee f, an input dx
    // gives realized price dy/dx = Y*gamma / (X + gamma*dx). The price
    // impact relative to the marginal price Y/X is
    //   PI = 1 - (dy/dx) / (Y/X) = gamma*dx / (X + gamma*dx).
    // Solving PI = p for dx gives  dx = p*X / (gamma * (1 - p)).
    //
    // Encoded in basis points (gamma_bps from above, p_bps below, both
    // scaled by BPS_FACTOR=B), the algebra simplifies to
    //   dx_max = p_bps * X * B  /  (gamma_bps * (B - p_bps))
    // which we evaluate as a single division to retain precision and to
    // floor-round in the *safe* direction (slightly under the cap).

    let p_bps = max_price_impact_bps;
    if p_bps <= 0 || p_bps >= BPS_FACTOR {
        error!(
            %provider,
            p_bps,
            "Rebalancer: max_price_impact_bps out of (0, BPS_FACTOR); refusing to size swap"
        );

        return Ok(None);
    }

    let numer = match p_bps
        .checked_mul(res_x)
        .and_then(|a| a.checked_mul(BPS_FACTOR))
    {
        Some(n) => n,
        None => {
            error!(%provider, "Rebalancer: max_amount_in numerator overflowed");

            return Ok(None);
        }
    };
    // gamma_bps and (BPS_FACTOR - p_bps) are both <= BPS_FACTOR = 1e4, so
    // their product is <= 1e8 and can't overflow an i128.
    let denom = gamma_bps * (BPS_FACTOR - p_bps);
    let dx_max = numer / denom;
    if dx_max <= 0 {
        error!(
            %provider,
            dx_max, res_x, p_bps,
            "Rebalancer: derived non-positive max amount_in"
        );
        return Ok(None);
    }

    // Cap the input by both constraints: the model-derived price-impact
    // bound (dx_max) and what the keeper actually holds. Whichever is
    // smaller wins.
    let amount_in = dx_max.min(liquidator_balance);

    debug!(
        %provider,
        dx_max, liquidator_balance, amount_in, res_x, p_bps,
        "Rebalancer: capped swap input by price impact and balance"
    );

    // Compute amount_out locally from the same model that produced dx_max.
    // No extra RPC: we already paid for two probe quotes above, and the
    // virtual reserves we derived from them are enough to evaluate the
    // CPMM curve at the chosen amount_in. `slippage_bps` absorbs the
    // model-vs-execution drift downstream.
    //
    // Caveat: this assumes the probe-derived (res_x, res_y) are accurate.
    // The curvature-blindness bug in `probe_provider` (no relative-
    // curvature gate like `probe_provider2`'s
    // RELATIVE_CURVATURE_THRESHOLD_RATIO) can inflate res_x for deep,
    // near-linear pools, which propagates straight into amount_out here.

    // Re-derive the second virtual reserve from the curve. Inverting
    //   y = res_y * gamma * dx / (res_x + gamma * dx)
    // gives  res_y = y * (res_x + gamma * dx) / (gamma * dx).
    // In BPS-encoded integer form (res_x in raw units, gamma_bps from
    // above) that's
    //   res_y = y_hi * (res_x * B + gamma_bps * probe_hi)
    //         / (gamma_bps * probe_hi)
    // We pick `probe_hi` over `probe_lo` because the larger probe carries
    // more signal against integer-rounding inside the pool's own quote.
    let inner = match res_x.checked_mul(BPS_FACTOR).and_then(|a| {
        gamma_bps
            .checked_mul(probe_hi)
            .and_then(|b| a.checked_add(b))
    }) {
        Some(v) => v,
        Option::None => {
            error!(%provider, "Rebalancer: res_y inner term overflowed");
            return Ok(None);
        }
    };
    let res_y_numer = match y_hi.checked_mul(inner) {
        Some(v) => v,
        Option::None => {
            error!(%provider, "Rebalancer: res_y numerator overflowed");
            return Ok(None);
        }
    };
    // gamma_bps * probe_hi ≤ 1e4 * 1e10 = 1e14, comfortably non-overflowing.
    let res_y_denom = gamma_bps * probe_hi;
    let res_y = res_y_numer / res_y_denom;
    if res_y <= 0 {
        error!(%provider, res_y, "Rebalancer: derived non-positive Y reserve");

        return Ok(None);
    }

    //   amount_out = res_y * gamma_bps * amount_in
    //              / (res_x * B + gamma_bps * amount_in)
    let g_dx = match gamma_bps.checked_mul(amount_in) {
        Some(v) => v,
        Option::None => {
            error!(%provider, "Rebalancer: amount_out g*dx overflowed");

            return Ok(None);
        }
    };
    let out_numer = match res_y.checked_mul(g_dx) {
        Some(v) => v,
        Option::None => {
            error!(%provider, "Rebalancer: amount_out numerator overflowed");

            return Ok(None);
        }
    };
    let out_denom = match res_x
        .checked_mul(BPS_FACTOR)
        .and_then(|a| a.checked_add(g_dx))
    {
        Some(v) => v,
        Option::None => {
            error!(%provider, "Rebalancer: amount_out denominator overflowed");

            return Ok(None);
        }
    };
    let amount_out = out_numer / out_denom;
    if amount_out <= 0 {
        error!(
            %provider,
            amount_out, amount_in, res_x, res_y,
            "Rebalancer: locally-computed amount_out non-positive"
        );

        return Ok(None);
    }

    debug!(
        %provider,
        amount_in, amount_out, res_x, res_y,
        "Rebalancer: amount_out from local CPMM formula"
    );

    Ok(Some((amount_in, amount_out)))
}

fn compute_value_cents(token_amount: i128, info: &AssetInfo) -> i128 {
    if info.oracle_price <= 0 {
        return 0;
    }
    let denom_pow = info.decimals + info.oracle_decimals;
    if denom_pow < 2 {
        return 0;
    }
    // Subtract 2 to express the result in cents instead of whole dollars.
    let denom = 10_i128.pow(denom_pow - 2);
    if denom == 0 {
        return 0;
    }

    (token_amount.saturating_mul(info.oracle_price) / denom).max(0)
}
