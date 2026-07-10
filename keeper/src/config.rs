//! CLI args + config schema for the keeper binary.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use ::config::{Config, Environment, File};
use clap::Parser;
use serde::{Deserialize, Deserializer};
use url::Url;
use validator::{Validate, ValidationError};

#[derive(Parser, Debug)]
pub struct Args {
    // Env-var fallback so deploys (docker compose `env_file:`, k8s
    // secrets) can supply the key without leaking it on the process
    // command line. CLI flag still wins when both are present.
    #[arg(short, long, env = "STELLAR_SKEY", hide_env_values = true)]
    pub skey: String,
    #[arg(short, long)]
    pub config: PathBuf,
}

#[derive(Deserialize, Debug, Validate)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CliConfig {
    // -------------------------------------------------------------------------
    // General
    // -------------------------------------------------------------------------
    pub rpc_url: Url,

    pub db_path: PathBuf,

    #[validate(length(min = 1, message = "At least one market must be specified"))]
    #[validate(custom(function = "validate_stellar_addresses"))]
    pub markets: Vec<String>,

    #[validate(length(equal = 56, message = "XLM address must be exactly 56 characters"))]
    pub xlm_address: String,

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

    #[validate(length(min = 1, message = "At least one asset to hold must be specified"))]
    #[validate(custom(function = "validate_stellar_addresses"))]
    pub assets_to_hold: Vec<String>,

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
        message = "Price impact BPS must be between 0 and 10000"
    ))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_max_price_impact_bps: i128,

    #[validate(range(min = 1))]
    pub balancer_max_swap_provider_probes: u32,

    #[validate(range(min = 0))]
    #[serde(deserialize_with = "de_i128")]
    pub balancer_min_swap_amount_value_cents: i128,
}

fn default_readiness_staleness_budget_secs() -> u64 {
    120
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
