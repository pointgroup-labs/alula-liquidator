use {
    artemis_alula::{
        collectors::{
            block_collector::BlockCollector,
            event_collector::{EventCollector, EventFilter},
        },
        // engine::Engine,
        executors::tx_executor::SorobanExecutor,
        types::{Action, Event},
    },
    clap::Parser,
    ed25519_dalek::SigningKey,
    serde::Deserialize,
    std::{fs::File, path::PathBuf},
    stellar_rpc_client::EventType,
    stellar_strkey::ed25519::PrivateKey,
    tokio::runtime::Runtime,
    tracing::info,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
    url::Url,
};

pub const BPS_FACTOR: i128 = 10_000;
pub const REBALANCER_INTERVAL_BLOCKS: u32 = 2;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    pub config: PathBuf,
    #[arg(short, long)]
    pub skey: String,
}

#[derive(Deserialize, Debug)]
struct CliConfig {
    rpc_url: Url,
    db_path: String,
    xlm_address: String,
    markets: Vec<String>,
    xlm_safety_margin: i128,
    network_passphrase: String,
    assets_to_hold: Vec<String>,
    swap_providers: Vec<String>,
    min_profit_margin_cents: i128,
    #[serde(default = "default_rebalancer_max_slippage_bps")]
    rebalancer_max_slippage_bps: i128,
    #[serde(default = "default_rebalancer_interval_blocks")]
    rebalancer_interval_blocks: u32,
}

impl CliConfig {
    pub fn try_load(config: PathBuf) -> anyhow::Result<Self> {
        let res = serde_json::from_reader(File::open(config)?)?;

        Ok(res)
    }
}

const fn default_rebalancer_max_slippage_bps() -> i128 {
    BPS_FACTOR / 100 // 1%
}

const fn default_rebalancer_interval_blocks() -> u32 {
    REBALANCER_INTERVAL_BLOCKS
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let Args { config, skey } = Args::parse();

    let CliConfig {
        rpc_url,
        db_path,
        markets,
        xlm_address,
        assets_to_hold,
        swap_providers,
        xlm_safety_margin,
        network_passphrase,
        min_profit_margin_cents,
        rebalancer_max_slippage_bps,
        rebalancer_interval_blocks,
    } = CliConfig::try_load(config)?;
    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);

    // --- Setup Engine ---

    // let mut engine: Engine<Event, Action> = ();

    // -- Strategies --

    // - Liquidator -

    // - ShareSeller -

    // - PortfolioRebalancer -

    // -- Collectors --

    // -- Executor --

    todo!()
}

// -- Helpers --

fn setup_tracing() {
    let filter = EnvFilter::new("warn")
        .add_directive("artemis_alula=debug".parse().unwrap())
        .add_directive("cli=info".parse().unwrap());
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
