//! CLI args + JSON config schema for the keeper binary.
//!
//! Lives outside `main.rs` so the composition root reads as composition,
//! not as schema definitions.

use {
    clap::Parser,
    serde::Deserialize,
    std::{fs::File, net::SocketAddr, path::Path, path::PathBuf},
    url::Url,
};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    pub config: PathBuf,
    // Env-var fallback so deploys (docker compose `env_file:`, k8s
    // secrets) can supply the key without leaking it on the process
    // command line. CLI flag still wins when both are present.
    #[arg(short, long, env = "STELLAR_SKEY", hide_env_values = true)]
    pub skey: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CliConfig {
    pub rpc_url: Url,
    pub db_path: String,
    pub xlm_address: String,
    pub markets: Vec<String>,
    pub xlm_safety_margin: i128,
    pub network_passphrase: String,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub min_profit_margin_cents: i128,
    pub min_withdraw_value_cents: i128,
    /// Headroom (bps) below pool utilization cap that the withdrawer
    /// refuses to consume — leaves the keeper from pulling its own deposits
    /// out so aggressively that borrowers can't draw.
    #[serde(default = "default_withdrawer_utilization_safety_margin_bps")]
    pub withdrawer_utilization_safety_margin_bps: i128,
    pub rebalancer_max_price_impact_bps: i128,
    pub rebalancer_slippage_bps: i128,
    pub rebalancer_interval_blocks: u32,
    pub rebalancer_min_swap_amount_value_cents: i128,
    pub rebalancer_max_fee_bps: i128,
    /// Haircut on `gain_oracle` (bps): out-leg slippage + oracle drift.
    #[serde(default = "default_liquidator_gain_haircut_bps")]
    pub liquidator_gain_haircut_bps: i128,
    /// Absolute oracle-units allowance for the Stellar tx fee.
    #[serde(default)]
    pub liquidator_inclusion_fee_oracle_units: i128,
    /// Address the Prometheus `/metrics` endpoint binds to.
    pub metrics_bind_addr: SocketAddr,
}

// Preserves the legacy hardcoded 500 bps when the field is absent.
fn default_liquidator_gain_haircut_bps() -> i128 {
    500
}

// Preserves the legacy hardcoded 500 bps when the field is absent.
fn default_withdrawer_utilization_safety_margin_bps() -> i128 {
    500
}

impl CliConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Ok(serde_json::from_reader(File::open(path)?)?)
    }
}
