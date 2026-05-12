//! Rebalancer: swaps non-target assets back into the configured target asset
//! by inverting the AMM curve via two probe quotes per provider.

use {
    crate::{
        collect::{Event, block::NewBlock},
        execute::{
            Action,
            stellar_tx::{SettleHook, SubmitStellarTx},
        },
        stellar::Gateway,
        strategy::capital::{CapitalLedger, random_op_id},
    },
    ed25519_dalek::SigningKey,
    engine::{
        lending::{MarketData, ObligationKey},
        ports::{ChainReader, EventCodec, OpBuilder, OperationEvent},
        reactor::{BoxFuture, Strategy},
    },
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    stellar_rpc_client::Event as SorobanEvent,
    tracing::{debug, error, info, warn},
};

const BPS_FACTOR: i128 = 10_000;
const MAX_SWAP_RETRIES: u32 = 3;
const MAX_PROBE_STABILITY_RETRIES: u32 = 2;

const PROBE_LARGE_LO: i128 = 10i128.pow(9);
const PROBE_LARGE_HI: i128 = 10i128.pow(10);

pub struct RebalancerConfig {
    /// Bug fix #5: chosen the simpler interpretation — the rebalancer is
    /// single-market by design. Multi-market routing would need per-candidate
    /// market plumbing; the original code silently used `markets[0]` for all
    /// candidates regardless of provenance.
    pub market: String,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    /// First element is the swap target.
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub max_price_impact_bps: i128,
    pub max_slippage_bps: i128,
    pub max_fee_bps: i128,
    pub refresh_interval_blocks: u32,
    pub min_swap_amount_value_cents: i128,
}

struct AssetInfo {
    decimals: u32,
    oracle_price: i128,
    oracle_decimals: u32,
}

pub struct Rebalancer {
    chain: Arc<dyn ChainReader>,
    gateway: Arc<Gateway>,
    skey: SigningKey,
    pkey: String,
    config: RebalancerConfig,
    liquidator_key: ObligationKey,
    asset_index: HashMap<String, AssetInfo>,
    market_data: Option<MarketData>,
    ledger: Arc<CapitalLedger>,
}

impl Rebalancer {
    pub fn new(
        chain: Arc<dyn ChainReader>,
        gateway: Arc<Gateway>,
        skey: SigningKey,
        pkey: String,
        config: RebalancerConfig,
        ledger: Arc<CapitalLedger>,
    ) -> Self {
        let liquidator_key = ObligationKey::new(pkey.clone());
        Self {
            chain,
            gateway,
            skey,
            pkey,
            config,
            liquidator_key,
            asset_index: HashMap::new(),
            market_data: None,
            ledger,
        }
    }
}

impl Strategy<Event, Action> for Rebalancer {
    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        Box::pin(async move {
            match event {
                Event::NewBlock(b) => self.handle_new_block(b).await,
                Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
            }
        })
    }
}

impl Rebalancer {
    async fn handle_new_block(&mut self, block: NewBlock) -> Vec<Action> {
        if !block.number.is_multiple_of(self.config.refresh_interval_blocks) {
            return vec![];
        }
        if !self.preconditions_met() {
            return vec![];
        }
        self.refresh_market().await;
        self.find_rebalance_actions().await
    }

    async fn handle_soroban_event(&mut self, event: SorobanEvent) -> Vec<Action> {
        // Bug fix #C1: the topic index that identifies "us" depends on the
        // operation kind. Liquidate emits topic[1]=liquidator (topic[2] is the
        // borrower being seized); Withdraw emits topic[2]=obligation key whose
        // .user is the withdrawer. Previously this gate read topic[2] for both
        // and silently suppressed every self-liquidation.
        let is_ours = match self.gateway.decode_operation(&event) {
            Ok(OperationEvent::Liquidate) => self.gateway.decode_topic(&event, 1) == self.pkey,
            Ok(OperationEvent::Withdraw) => self
                .gateway
                .parse_obligation_key_from_topic(&event, 2)
                .map(|k| k.user == self.pkey)
                .unwrap_or(false),
            _ => return vec![],
        };
        if !is_ours {
            return vec![];
        }

        info!(?event, "Detected liquidator possible balance increase event");

        if !self.preconditions_met() {
            return vec![];
        }
        self.refresh_market().await;
        self.find_rebalance_actions().await
    }

    fn preconditions_met(&self) -> bool {
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

    async fn refresh_market(&mut self) {
        match self.chain.read_market_data(&self.config.market).await {
            Ok(md) => {
                info!(market = %self.config.market, "Rebalancer: refreshed market data");
                self.market_data = Some(md);
            }
            Err(e) => error!(?e, market = %self.config.market, "Rebalancer: refresh failed"),
        }
    }

    async fn find_rebalance_actions(&mut self) -> Vec<Action> {
        self.rebuild_asset_index();
        let target_asset = self.config.assets_to_hold[0].clone();

        let held: HashSet<&str> = self
            .config
            .assets_to_hold
            .iter()
            .map(String::as_str)
            .collect();
        let candidates: Vec<String> = self
            .asset_index
            .keys()
            .filter(|addr| !held.contains(addr.as_str()))
            .cloned()
            .collect();

        let mut actions = vec![];
        for candidate in &candidates {
            if candidate == &target_asset {
                error!(
                    candidate,
                    target_asset, "Rebalancable candidate equals target asset"
                );
                continue;
            }
            match self
                .evaluate_rebalancable_candidate(&target_asset, candidate)
                .await
            {
                Ok(Some(a)) => actions.push(a),
                Ok(None) => {}
                Err(e) => warn!(?e, %candidate, "candidate evaluation failed"),
            }
        }
        actions
    }

    fn rebuild_asset_index(&mut self) {
        self.asset_index.clear();
        let Some(md) = &self.market_data else { return };
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

    async fn evaluate_rebalancable_candidate(
        &self,
        target: &str,
        candidate: &str,
    ) -> anyhow::Result<Option<Action>> {
        // Use the shared ledger for balance reads so the cache is hot for the
        // liquidator (and vice versa), and so `available_after_reservations`
        // accounts for in-flight liquidations against this token.
        let raw_balance = self
            .ledger
            .cached_balance(&*self.chain, candidate, &self.pkey)
            .await?;
        let safety_floor = if candidate == self.config.xlm_address {
            raw_balance.saturating_sub(self.config.xlm_safety_margin)
        } else {
            raw_balance
        };
        let balance_to_swap =
            self.ledger
                .available_after_reservations(candidate, &self.pkey, safety_floor);
        if !balance_to_swap.is_positive() {
            debug!(%candidate, raw_balance, balance_to_swap, "Nothing to swap");
            return Ok(None);
        }

        let (probe_lo, probe_hi) = (PROBE_LARGE_LO, PROBE_LARGE_HI);
        let path: [&str; 2] = [candidate, target];

        let mut best_provider: Option<(String, i128, i128)> = None;
        for provider in &self.config.swap_providers {
            match self
                .probe_provider(provider, &path, balance_to_swap, probe_lo, probe_hi)
                .await
            {
                Ok(Some((amount_in, amount_out))) => {
                    debug!(%provider, %candidate, amount_in, amount_out, "probe ok");
                    let take = match best_provider.as_ref() {
                        None => true,
                        Some((_, best_in, best_out)) => match (
                            amount_out.checked_mul(*best_in),
                            best_out.checked_mul(amount_in),
                        ) {
                            (Some(lhs), Some(rhs)) => lhs > rhs,
                            _ => amount_out > *best_out,
                        },
                    };
                    if take {
                        best_provider = Some((provider.clone(), amount_in, amount_out));
                    }
                }
                Ok(None) => debug!(%provider, %candidate, "provider unviable"),
                Err(e) => warn!(?e, %provider, %candidate, "provider probe failed"),
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
            info!(%candidate, amount_in, value_cents, "below dust threshold");
            return Ok(None);
        }

        let min_amount_out =
            amount_out.saturating_mul(BPS_FACTOR - self.config.max_slippage_bps) / BPS_FACTOR;
        let request = self.gateway.swap_exact_tokens_request(
            &provider,
            amount_in,
            min_amount_out,
            &path,
        )?;

        // Single-market by design (bug fix #5): use the configured market.
        let op = self
            .gateway
            .batch_op(&self.config.market, &self.liquidator_key, &[request])?;

        // Reserve against the shared ledger before emitting; if reservation
        // fails (some other in-flight tx already committed the capacity) we
        // skip rather than risk a double-spend on the same token.
        let op_id = random_op_id();
        if !self
            .ledger
            .reserve(op_id, candidate, &self.pkey, amount_in, balance_to_swap)
        {
            warn!(%candidate, amount_in, balance_to_swap,
                "rebalancer: reservation lost race; skipping submission");
            return Ok(None);
        }

        info!(
            %candidate, value_cents, %target, %provider, amount_in, amount_out,
            min_amount_out, market = %self.config.market,
            "Rebalancer: submitting swap"
        );

        Ok(Some(Action::SubmitTx(SubmitStellarTx {
            op,
            signing_key: self.skey.clone(),
            max_retries: MAX_SWAP_RETRIES,
            on_settle: Some(SettleHook {
                ledger: self.ledger.clone(),
                op_id,
            }),
        })))
    }

    async fn probe_provider(
        &self,
        provider: &str,
        path: &[&str; 2],
        liquidator_balance: i128,
        probe_lo: i128,
        probe_hi: i128,
    ) -> anyhow::Result<Option<(i128, i128)>> {
        let mut stable_ys: Option<(i128, i128)> = None;
        let mut is_stable = false;

        for _ in 0..MAX_PROBE_STABILITY_RETRIES {
            let (y_lo, y_hi) = (
                self.chain
                    .quote_amount_out(provider, probe_lo, path[0], path[1])
                    .await,
                self.chain
                    .quote_amount_out(provider, probe_hi, path[0], path[1])
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
                error!(%provider, y_lo, y_hi, "probes returned non-positive");
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

        let (y_lo, y_hi) = stable_ys.expect("Some because is_stable");

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
    let denom = match y_lo
        .checked_mul(probe_hi)
        .and_then(|a| probe_lo.checked_mul(y_hi).and_then(|b| a.checked_sub(b)))
    {
        Some(d) => d,
        None => {
            error!(%provider, "reserve-X denominator overflowed");
            return Ok(None);
        }
    };
    if denom <= 0 {
        error!(%provider, denom, "non-positive reserve-X denominator");
        return Ok(None);
    }

    let gamma_bps = BPS_FACTOR - fee_bps;
    let dy = y_hi - y_lo;
    let numer = match probe_lo
        .checked_mul(probe_hi)
        .and_then(|a| a.checked_mul(gamma_bps))
        .and_then(|a| a.checked_mul(dy))
    {
        Some(n) => n,
        None => {
            error!(%provider, "reserve-X numerator overflowed");
            return Ok(None);
        }
    };

    let res_x = numer / denom / BPS_FACTOR;
    if res_x <= 0 {
        error!(%provider, res_x, "non-positive X reserve");
        return Ok(None);
    }

    let p_bps = max_price_impact_bps;
    if p_bps <= 0 || p_bps >= BPS_FACTOR {
        error!(%provider, p_bps, "max_price_impact_bps out of (0, BPS_FACTOR)");
        return Ok(None);
    }

    let numer = match p_bps
        .checked_mul(res_x)
        .and_then(|a| a.checked_mul(BPS_FACTOR))
    {
        Some(n) => n,
        None => {
            error!(%provider, "max_amount_in numerator overflowed");
            return Ok(None);
        }
    };
    let denom = gamma_bps * (BPS_FACTOR - p_bps);
    let dx_max = numer / denom;
    if dx_max <= 0 {
        error!(%provider, dx_max, res_x, p_bps, "non-positive max amount_in");
        return Ok(None);
    }
    let amount_in = dx_max.min(liquidator_balance);

    let inner = match res_x.checked_mul(BPS_FACTOR).and_then(|a| {
        gamma_bps
            .checked_mul(probe_hi)
            .and_then(|b| a.checked_add(b))
    }) {
        Some(v) => v,
        None => {
            error!(%provider, "res_y inner term overflowed");
            return Ok(None);
        }
    };
    let res_y_numer = match y_hi.checked_mul(inner) {
        Some(v) => v,
        None => {
            error!(%provider, "res_y numerator overflowed");
            return Ok(None);
        }
    };
    let res_y_denom = gamma_bps * probe_hi;
    let res_y = res_y_numer / res_y_denom;
    if res_y <= 0 {
        error!(%provider, res_y, "non-positive Y reserve");
        return Ok(None);
    }

    let g_dx = match gamma_bps.checked_mul(amount_in) {
        Some(v) => v,
        None => {
            error!(%provider, "amount_out g*dx overflowed");
            return Ok(None);
        }
    };
    let out_numer = match res_y.checked_mul(g_dx) {
        Some(v) => v,
        None => {
            error!(%provider, "amount_out numerator overflowed");
            return Ok(None);
        }
    };
    let out_denom = match res_x
        .checked_mul(BPS_FACTOR)
        .and_then(|a| a.checked_add(g_dx))
    {
        Some(v) => v,
        None => {
            error!(%provider, "amount_out denominator overflowed");
            return Ok(None);
        }
    };
    let amount_out = out_numer / out_denom;
    if amount_out <= 0 {
        error!(%provider, amount_out, amount_in, "amount_out non-positive");
        return Ok(None);
    }

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
    let denom = 10_i128.pow(denom_pow - 2);
    if denom == 0 {
        return 0;
    }
    (token_amount.saturating_mul(info.oracle_price) / denom).max(0)
}
