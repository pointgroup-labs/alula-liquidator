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
    #[arg(short, long)]
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

impl CliConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Ok(serde_json::from_reader(File::open(path)?)?)
    }
}
