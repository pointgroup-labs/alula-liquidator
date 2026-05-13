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

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config_json() -> serde_json::Value {
        serde_json::json!({
            "rpc_url": "https://rpc.example/",
            "db_path": "/tmp/keeper.db",
            "xlm_address": "CXLM",
            "markets": ["MARKET1"],
            "xlm_safety_margin": 200_000_000_i128,
            "network_passphrase": "Test SDF Network ; September 2015",
            "assets_to_hold": ["AUSDC"],
            "swap_providers": ["PROVIDER1"],
            "min_profit_margin_cents": 50_i128,
            "min_withdraw_value_cents": 500_i128,
            "rebalancer_max_price_impact_bps": 100_i128,
            "rebalancer_slippage_bps": 30_i128,
            "rebalancer_interval_blocks": 10_u32,
            "rebalancer_min_swap_amount_value_cents": 100_i128,
            "rebalancer_max_fee_bps": 50_i128,
            "metrics_bind_addr": "0.0.0.0:9000",
        })
    }

    #[test]
    fn optional_fields_fall_back_to_legacy_defaults_when_absent() {
        let cfg: CliConfig = serde_json::from_value(minimal_config_json()).unwrap();
        assert_eq!(cfg.liquidator_gain_haircut_bps, 500);
        assert_eq!(cfg.liquidator_inclusion_fee_oracle_units, 0);
        assert_eq!(cfg.withdrawer_utilization_safety_margin_bps, 500);
    }

    #[test]
    fn optional_fields_round_trip_explicit_values() {
        let mut json = minimal_config_json();
        let obj = json.as_object_mut().unwrap();
        obj.insert("liquidator_gain_haircut_bps".into(), 250.into());
        obj.insert(
            "liquidator_inclusion_fee_oracle_units".into(),
            7_000_000.into(),
        );
        obj.insert(
            "withdrawer_utilization_safety_margin_bps".into(),
            1_000.into(),
        );
        let cfg: CliConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.liquidator_gain_haircut_bps, 250);
        assert_eq!(cfg.liquidator_inclusion_fee_oracle_units, 7_000_000);
        assert_eq!(cfg.withdrawer_utilization_safety_margin_bps, 1_000);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut json = minimal_config_json();
        json.as_object_mut()
            .unwrap()
            .insert("definitely_not_a_real_field".into(), 1.into());
        let err = serde_json::from_value::<CliConfig>(json).unwrap_err();
        assert!(
            err.to_string().contains("definitely_not_a_real_field"),
            "expected deny_unknown_fields error to name the field, got: {err}"
        );
    }

    #[test]
    fn example_config_loads_via_load_fn() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("config.example.json");
        CliConfig::load(&path).expect("config.example.json must round-trip the schema");
    }
}
