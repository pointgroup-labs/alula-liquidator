//! CLI args + config schema for the keeper binary.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use ::config::{Config, Environment, File};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Deserializer};
use url::Url;
use validator::{Validate, ValidationError};

/// Selectable strategies. When neither the CLI flag nor the config field names
/// any, every strategy runs (see [`resolve_selection`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum StrategyKind {
    Liquidator,
    Withdrawer,
    Balancer,
    BadDebt,
}

impl StrategyKind {
    pub const ALL: [StrategyKind; 4] =
        [Self::Liquidator, Self::Withdrawer, Self::Balancer, Self::BadDebt];
}

/// Selectable liquidation types the liquidator may build candidates for. When
/// neither the CLI flag nor the config field names any, all types run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum LiquidationKindArg {
    Direct,
    Preswap,
    Flash,
}

impl LiquidationKindArg {
    pub const ALL: [LiquidationKindArg; 3] = [Self::Direct, Self::Preswap, Self::Flash];
}

/// Resolves a selection with precedence: CLI flag wins, else the config file
/// value, else all variants (`all`).
pub fn resolve_selection<T: Copy + Eq + std::hash::Hash>(
    cli: &[T],
    config: &[T],
    all: &[T],
) -> std::collections::HashSet<T> {
    let source = if !cli.is_empty() {
        cli
    } else if !config.is_empty() {
        config
    } else {
        all
    };

    source.iter().copied().collect()
}

#[derive(Parser, Debug)]
pub struct Args {
    // Env-var fallback so deploys (docker compose `env_file:`, k8s
    // secrets) can supply the key without leaking it on the process
    // command line. CLI flag still wins when both are present.
    #[arg(short, long, env = "STELLAR_SKEY", hide_env_values = true)]
    pub skey: String,
    #[arg(short, long)]
    pub config: PathBuf,

    /// Strategies to run (repeat or comma-separate). Overrides the config
    /// file's `strategies`. When neither is set, all strategies run.
    #[arg(long, value_enum, value_delimiter = ',')]
    pub strategies: Vec<StrategyKind>,

    /// Liquidation types the liquidator may use (repeat or comma-separate).
    /// Overrides the config file's `liquidation_types`. When neither is set,
    /// all types run.
    #[arg(long = "liquidation-types", value_enum, value_delimiter = ',')]
    pub liquidation_types: Vec<LiquidationKindArg>,
}

#[derive(Deserialize, Debug, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "validate_hub_invariants", skip_on_field_errors = false))]
#[non_exhaustive]
pub struct CliConfig {
    // -------------------------------------------------------------------------
    // General
    // -------------------------------------------------------------------------
    pub rpc_url: Url,

    /// Optional fallback RPC endpoints, tried in order whenever the primary
    /// [`Self::rpc_url`] is on cooldown or fails with a transport error.
    pub fallback_rpc_urls: Vec<Url>,

    /// Wall-clock budget for a single logical RPC call across all failover
    /// attempts. Once exceeded, the failover loop stops trying further
    /// endpoints and returns the last transport error.
    #[validate(range(min = 1))]
    pub rpc_max_call_duration_secs: u64,

    pub db_path: PathBuf,

    #[validate(length(min = 1, message = "At least one market must be specified"))]
    #[validate(custom(function = "validate_stellar_addresses"))]
    pub markets: Vec<String>,

    #[validate(length(equal = 56, message = "XLM address must be exactly 56 characters"))]
    pub xlm_address: String,

    /// The stablecoin "hub" asset the Balancer prices the portfolio in and
    /// routes every rebalance swap through. It has no explicit target weight in
    /// `assets_to_hold`; its share is the residual left after the volatile
    /// assets' targets. Must be a valid stellar address and must NOT appear as
    /// a key in `assets_to_hold`.
    #[validate(length(equal = 56, message = "Hub address must be exactly 56 characters"))]
    pub hub_address: String,

    #[validate(range(min = 1, message = "XLM safety margin must be positive"))]
    #[serde(deserialize_with = "de_i128")]
    pub xlm_safety_margin: i128,

    #[validate(range(
        min = 100,
        message = "Simulation fee must be reasonable (e.g. >= 100 stroops)"
    ))]
    pub default_simulation_fee: u32,

    #[validate(length(min = 1, message = "Network passphrase cannot be empty"))]
    pub network_passphrase: String,

    /// Assets the keeper wants to keep on its balance, mapped to the share of
    /// held value each should target (in basis points; 10_000 = 100%). These are mostly
    /// *volatile* assets; the hub ([`Self::hub_address`]) is the residual and
    /// is NOT listed here. The distributions must sum to at most 10_000 — the
    /// remainder is the implied hub floor.
    #[validate(length(min = 1, message = "At least one asset to hold must be specified"))]
    #[validate(custom(function = "validate_asset_distribution"))]
    pub assets_to_hold: HashMap<String, u16>,

    #[validate(length(min = 1, message = "At least one swap provider must be specified"))]
    #[validate(custom(function = "validate_stellar_addresses"))]
    pub swap_providers: Vec<String>,

    pub metrics_bind_addr: SocketAddr,

    /// `/readyz` staleness budget: seconds without a completed scan before the
    /// keeper reports not-ready. Set it above the slowest strategy refresh
    /// cadence so an idle-but-healthy keeper doesn't flap. Optional; defaults
    /// to 120.
    #[serde(default = "default_readiness_staleness_budget_secs")]
    #[validate(range(min = 1))]
    pub readiness_staleness_budget_secs: u64,

    pub event_collector_start_ledger: u32,

    #[validate(range(min = 1, max = 10))]
    pub keeper_capital_balance_ttl_secs: u64,

    #[validate(range(min = 1, max = 30))]
    pub keeper_capital_reservation_ttl_secs: u64,

    #[validate(range(
        min = 1,
        message = "Ledger collector's polling interval must be at least 1 second"
    ))]
    pub ledger_collector_polling_interval_secs: u64,

    // -------------------------------------------------------------------------
    // Bad Debt Request Initiator
    // -------------------------------------------------------------------------
    #[validate(range(min = 1, max = 50))]
    pub bad_debt_request_initiator_max_retries: u32,

    #[validate(range(min = 1))]
    pub bad_debt_request_initiator_refresh_interval_blocks: u32,

    // -------------------------------------------------------------------------
    // Withdrawer
    // -------------------------------------------------------------------------
    #[validate(range(min = 1, max = 50))]
    pub withdrawer_max_retries: u32,

    #[validate(range(min = 1))]
    pub withdrawer_refresh_interval_blocks: u32,

    #[validate(range(min = 0))]
    #[serde(deserialize_with = "de_i128")]
    pub withdrawer_min_withdraw_value_cents: i128,

    #[validate(range(min = 0, max = 10000))]
    #[serde(deserialize_with = "de_i128")]
    pub withdrawer_utilization_safety_margin_bps: i128,

    // -------------------------------------------------------------------------
    // Liquidator
    // -------------------------------------------------------------------------
    #[validate(range(min = 1, max = 50))]
    pub liquidator_max_retries: u32,

    #[validate(range(min = 1))]
    pub liquidator_refresh_interval_blocks: u32,

    // Negative assumes the liquidator is run by the entity prioritizing the protocol safety over
    // profitability(like Alula team)
    #[validate(range(min = -10_00))]
    #[serde(deserialize_with = "de_i128")]
    pub liquidator_min_profit_margin_cents: i128,

    #[validate(range(min = 0, max = 10000, message = "Slippage BPS must be between 0 and 10000"))]
    #[serde(deserialize_with = "de_i128")]
    pub liquidator_max_allowed_swap_slippage_bps: i128,

    // -------------------------------------------------------------------------
    // Balancer
    // -------------------------------------------------------------------------
    #[validate(range(min = 1, max = 50))]
    pub balancer_max_retries: u32,

    #[validate(range(min = 1))]
    pub balancer_refresh_interval_blocks: u32,

    #[validate(range(min = 0, max = 10000, message = "Slippage BPS must be between 0 and 10000"))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_max_allowed_swap_slippage_bps: i128,

    #[validate(range(
        min = 0,
        max = 10000,
        message = "Execution impact BPS must be between 0 and 10000"
    ))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_max_execution_impact_bps: i128,

    /// Per-asset tolerance band around its target weight, in basis points. An
    /// asset is only rebalanced once `|current_weight - target_weight|` reaches
    /// this many bps (500 = 5 percentage points).
    #[validate(range(
        min = 0,
        max = 10000,
        message = "Rebalance threshold BPS must be between 0 and 10000"
    ))]
    pub balancer_rebalance_threshold_bps: u16,

    /// Upper bound on the number of swap legs packed into a single atomic
    /// rebalance batch (sells first, then buys).
    #[validate(range(min = 1))]
    pub balancer_max_swaps_per_batch: u32,

    #[validate(range(min = 1))]
    pub balancer_max_swap_provider_halving_probes: u32,

    #[validate(range(min = 0))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_min_swap_amount_value_cents: i128,

    /// Maximum tolerated cross-market oracle price spread for any relevant
    /// asset, in basis points (`(max - min) / median`). If exceeded, the whole
    /// rebalance cycle is aborted (200 = 2%).
    #[validate(range(
        min = 0,
        max = 10000,
        message = "Oracle price spread BPS must be between 0 and 10000"
    ))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_max_oracle_price_spread_bps: i128,

    /// Strategies to run. Empty = all (a CLI `--strategies` flag overrides this).
    #[serde(default)]
    pub strategies: Vec<StrategyKind>,

    /// Liquidation types the liquidator may use. Empty = all (a CLI
    /// `--liquidation-types` flag overrides this).
    #[serde(default)]
    pub liquidation_types: Vec<LiquidationKindArg>,
}

/// Validates `assets_to_hold`: every key must be a valid stellar address and
/// the raw distribution values (basis points) must sum to at most 10_000 (100%).
///
/// This is only a *necessary* bound. The *sufficient* liquidity invariant — that
/// enough of the residual is reserved for the hub even when every asset drifts
/// to the top of its tolerance band — also depends on
/// `balancer_rebalance_threshold_bps`, which a field-level validator can't see,
/// so it lives in [`validate_hub_liquidity_headroom`].
fn validate_asset_distribution(holdings: &HashMap<String, u16>) -> Result<(), ValidationError> {
    for address in holdings.keys() {
        validate_stellar_address(address)?;
    }

    let total: u32 = holdings.values().map(|bps| u32::from(*bps)).sum();
    if total > 10_000 {
        let mut err = ValidationError::new("asset_distribution_sum");
        err.message = Some(
            format!("assets_to_hold distributions must sum to at most 10000 bps, got {total}")
                .into(),
        );

        return Err(err);
    }

    Ok(())
}

fn default_readiness_staleness_budget_secs() -> u64 {
    120
}

/// Runs every whole-config invariant tying the hub to `assets_to_hold`. The
/// `validate` derive allows only one `schema` function, so the individual checks
/// are composed here rather than stacked as separate attributes.
fn validate_hub_invariants(config: &CliConfig) -> Result<(), ValidationError> {
    validate_hub_not_held(config)?;
    validate_hub_liquidity_headroom(config)?;

    Ok(())
}

/// The hub asset is the residual counterparty for every rebalance swap. It must
/// not appear as a targeted asset in `assets_to_hold`.
fn validate_hub_not_held(config: &CliConfig) -> Result<(), ValidationError> {
    if config.assets_to_hold.contains_key(&config.hub_address) {
        let mut err = ValidationError::new("hub_in_assets_to_hold");
        err.message = Some("hub_address must not also be a key in assets_to_hold".into());

        return Err(err);
    }

    Ok(())
}

/// Guarantees the hub keeps a positive residual share even in the worst case,
/// so it always has liquidity to *buy* deficit assets during a rebalance.
///
/// The rebalance threshold is a tolerance band: an asset isn't sold until its
/// weight exceeds `target + threshold`. In the worst case every volatile asset
/// sits at the top of its band simultaneously, so the volatile side can occupy
/// up to `Σ target_i + N·threshold` of total value. The hub's residual is
/// `10_000 − (Σ target_i + N·threshold)`; if that isn't strictly positive the
/// buy phase can be starved of hub liquidity. So we require:
///
/// ```text
/// Σ target_i + N·threshold < 10_000
/// ```
fn validate_hub_liquidity_headroom(config: &CliConfig) -> Result<(), ValidationError> {
    let target_sum: u32 = config.assets_to_hold.values().map(|bps| u32::from(*bps)).sum();
    let n = config.assets_to_hold.len() as u32;
    let threshold = u32::from(config.balancer_rebalance_threshold_bps);
    let worst_case_volatile = worst_case_volatile_bps(target_sum, n, threshold);

    if worst_case_volatile >= 10_000 {
        let mut err = ValidationError::new("hub_liquidity_headroom");
        err.message = Some(
            format!(
                "assets_to_hold targets ({target_sum} bps) plus worst-case drift \
                 ({n} assets × {threshold} bps threshold) reach {worst_case_volatile} bps, \
                 leaving no residual for the hub; keep the sum below 10000"
            )
            .into(),
        );

        return Err(err);
    }

    Ok(())
}

/// Worst-case share of total value the volatile assets can occupy: every asset
/// sitting at the top of its tolerance band at once. Saturating so an over-large
/// config can't wrap — it just reports the ceiling and fails the check.
fn worst_case_volatile_bps(target_sum: u32, asset_count: u32, threshold_bps: u32) -> u32 {
    target_sum.saturating_add(asset_count.saturating_mul(threshold_bps))
}

fn de_i128<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
    Ok(i64::deserialize(deserializer)? as i128)
}

fn validate_stellar_address(addr: &str) -> Result<(), ValidationError> {
    if addr.len() != 56 || (!addr.starts_with('C') && !addr.starts_with('G')) {
        return Err(ValidationError::new("invalid_stellar_address"));
    }

    Ok(())
}

fn validate_stellar_addresses(addresses: &[String]) -> Result<(), ValidationError> {
    for addr in addresses {
        validate_stellar_address(addr)?;
    }

    Ok(())
}

impl CliConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let config: Self = Config::builder()
            .add_source(File::from(path))
            .add_source(Environment::with_prefix("KEEPER").try_parsing(true))
            .build()?
            .try_deserialize()?;
        config.validate()?;

        Ok(config)
    }
}

#[cfg(test)]
mod example_config_tests {
    use super::*;

    #[test]
    fn example_toml_loads_and_validates() {
        let cfg = CliConfig::load(Path::new("../config.example.toml")).expect("toml loads");
        let sum: u32 = cfg.assets_to_hold.values().map(|bps| u32::from(*bps)).sum();
        // Volatile targets must leave headroom for the residual hub share.
        assert!(sum <= 10_000);
        // The hub must not double as a targeted volatile asset.
        assert!(!cfg.assets_to_hold.contains_key(&cfg.hub_address));
    }

    #[test]
    fn example_json_loads_and_validates() {
        let cfg = CliConfig::load(Path::new("../config.example.json")).expect("json loads");
        assert!(!cfg.assets_to_hold.is_empty());
    }

    #[test]
    fn resolve_selection_precedence() {
        let all = StrategyKind::ALL;
        let cli = [StrategyKind::Balancer];
        let cfg = [StrategyKind::Withdrawer];

        // CLI wins when present.
        assert_eq!(
            resolve_selection(&cli, &cfg, &all),
            std::collections::HashSet::from([StrategyKind::Balancer])
        );
        // Config used when CLI empty.
        assert_eq!(
            resolve_selection(&[], &cfg, &all),
            std::collections::HashSet::from([StrategyKind::Withdrawer])
        );
        // All when both empty.
        assert_eq!(resolve_selection(&[], &[], &all), all.into_iter().collect());
    }

    #[test]
    fn hub_headroom_accounts_for_worst_case_drift() {
        // Two assets targeting 4500 bps each = 9000 bps of targets, which alone
        // clears the naive `≤ 10000` bound. But with a 500 bps threshold each can
        // drift to 5000, so the volatile side can reach 10000 and starve the hub.
        assert_eq!(worst_case_volatile_bps(9_000, 2, 500), 10_000);
        assert!(worst_case_volatile_bps(9_000, 2, 500) >= 10_000, "must reject");

        // Same targets, no threshold: the hub keeps its 1000 bps residual.
        assert_eq!(worst_case_volatile_bps(9_000, 2, 0), 9_000);
        assert!(worst_case_volatile_bps(9_000, 2, 0) < 10_000, "must accept");
    }

    #[test]
    fn hub_headroom_saturates_instead_of_wrapping() {
        // Absurd inputs must not wrap around u32; they just pin to the ceiling.
        assert_eq!(worst_case_volatile_bps(u32::MAX, u32::MAX, u32::MAX), u32::MAX);
    }
}
