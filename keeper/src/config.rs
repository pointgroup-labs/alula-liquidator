//! CLI args + JSON config schema for the keeper binary.

use {
    clap::Parser,
    serde::Deserialize,
    std::{fs::File, net::SocketAddr, path::Path, path::PathBuf},
    url::Url,
};

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

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CliConfig {
    pub rpc_url: Url,
    pub db_path: PathBuf,
    pub markets: Vec<String>,
    pub xlm_address: String,
    pub xlm_safety_margin: i128,
    pub default_simulation_fee: u32,
    pub network_passphrase: String,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    pub metrics_bind_addr: SocketAddr,
    pub event_collector_start_ledger: u32,
    pub ledger_collector_polling_interval_secs: u64,

    pub bad_debt_request_initiator_max_retries: u32,
    pub bad_debt_request_initiator_refresh_interval_blocks: u32,

    pub withdrawer_max_retries: u32,
    pub withdrawrer_refresh_interval_blocks: u32,
    pub withdrawer_min_withdraw_value_cents: i128,
    pub withdrawer_utilization_safety_margin_bps: i128,

    pub liquidator_max_retries: u32,
    pub liquidator_refresh_interval_blocks: u32,
    pub liquidator_min_profit_margin_cents: i128,
    pub liquidator_capital_balance_ttl_secs: u64,
    pub liquidator_capital_reservation_ttl_secs: u64,
    pub liquidator_max_allowed_swap_slippage_bps: i128,

    pub balancer_max_retries: u32,
    pub balancer_refresh_interval_blocks: u32,
    pub balancer_max_allowed_swap_slippage_bps: i128,

    pub balancer_max_price_impact_bps: i128,
    pub balancer_max_swap_provider_probes: u32,
    pub balancer_min_swap_amount_value_cents: i128,
}

impl CliConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Ok(serde_json::from_reader(File::open(path)?)?)
    }
}
