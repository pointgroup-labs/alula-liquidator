use {
    artemis_alula::{
        collectors::{
            block_collector::BlockCollector,
            event_collector::{EventCollector, EventFilter},
        },
        db::DbManager,
        engine::Engine,
        executors::tx_executor::SorobanExecutor,
        strategies::{
            bad_debt_request_initiator::{BadDebtRequestInitiator, BadDebtRequestInitiatorConfig},
            liquidator::{Liquidator, LiquidatorConfig},
            rebalancer::{Rebalancer, RebalancerConfig},
            withdrawer::{Withdrawer, WithdrawerConfig},
        },
        types::{Action, Event},
    },
    clap::Parser,
    ed25519_dalek::SigningKey,
    serde::Deserialize,
    std::{fs::File, net::SocketAddr, path::PathBuf, sync::Arc},
    stellar_rpc_client::EventType,
    stellar_strkey::ed25519::PrivateKey,
    tokio::signal,
    tracing::info,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
    url::Url,
};

mod metrics_server;

pub const BPS_FACTOR: i128 = 10_000;
pub const REBALANCER_INTERVAL_BLOCKS: u32 = 2;
pub const REBALANCER_MIN_SWAP_AMOUNT_VALUE_CENTS: i128 = 100;
/// Default external slippage buffer applied to the realized quote when
/// constructing `min_amount_out` (0.3%).
pub const REBALANCER_SLIPPAGE_BPS: i128 = 30;
/// Conservative upper-bound DEX swap fee assumed across all configured
/// `swap_providers` (0.3%). Used so that price-impact sizing and
/// `min_amount_out` predictions stay safe on the worst-fee provider.
pub const REBALANCER_MAX_FEE_BPS: i128 = 30;
pub const WITHDRAWER_MIN_WITHDRAW_VALUE_CENTS: i128 = 500;

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
    #[serde(default = "default_min_withdraw_value_cents")]
    min_withdraw_value_cents: i128,
    #[serde(default = "default_rebalancer_max_price_impact_bps")]
    rebalancer_max_price_impact_bps: i128,
    #[serde(default = "default_rebalancer_slippage_bps")]
    rebalancer_slippage_bps: i128,
    #[serde(default = "default_rebalancer_interval_blocks")]
    rebalancer_interval_blocks: u32,
    #[serde(default = "default_rebalancer_min_swap_amount_value_cents")]
    rebalancer_min_swap_amount_value_cents: i128,
    #[serde(default = "default_rebalancer_max_fee_bps")]
    rebalancer_max_fee_bps: i128,
    /// Address the Prometheus `/metrics` endpoint binds to. Required.
    metrics_bind_addr: SocketAddr,
}

impl CliConfig {
    pub fn try_load(config: PathBuf) -> anyhow::Result<Self> {
        let res = serde_json::from_reader(File::open(config)?)?;

        Ok(res)
    }
}

const fn default_rebalancer_max_price_impact_bps() -> i128 {
    BPS_FACTOR / 100 // 1%
}

const fn default_rebalancer_slippage_bps() -> i128 {
    REBALANCER_SLIPPAGE_BPS
}

const fn default_min_withdraw_value_cents() -> i128 {
    WITHDRAWER_MIN_WITHDRAW_VALUE_CENTS
}

const fn default_rebalancer_interval_blocks() -> u32 {
    REBALANCER_INTERVAL_BLOCKS
}

const fn default_rebalancer_min_swap_amount_value_cents() -> i128 {
    REBALANCER_MIN_SWAP_AMOUNT_VALUE_CENTS
}

const fn default_rebalancer_max_fee_bps() -> i128 {
    REBALANCER_MAX_FEE_BPS
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
        min_withdraw_value_cents,
        rebalancer_max_price_impact_bps,
        rebalancer_slippage_bps,
        rebalancer_interval_blocks,
        rebalancer_min_swap_amount_value_cents,
        rebalancer_max_fee_bps,
        metrics_bind_addr,
    } = CliConfig::try_load(config)?;
    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);
    let db_manager = Arc::new(DbManager::try_create(&db_path)?);

    // --- Setup Engine ---

    let mut engine: Engine<Event, Action> = Engine::new();

    // -- Strategies --

    // - BadDebtRequestInitiator -

    let bad_debt_request_initiator_config = BadDebtRequestInitiatorConfig {
        rpc_url: rpc_url.clone(),
        markets: markets.clone(),
    };
    let _bad_debt_request_initiator =
        BadDebtRequestInitiator::try_create(bad_debt_request_initiator_config, &skey)?;

    // engine.add_strategy(Box::new(bad_debt_request_initiator));

    // - Liquidator -

    // let liqudidator_config = LiquidatorConfig {
    //     xlm_safety_margin,
    //     min_profit_margin_cents,
    //     markets: markets.clone(),
    //     rpc_url: rpc_url.clone(),
    //     xlm_address: xlm_address.clone(),
    //     swap_providers: swap_providers.clone(),
    //     assets_to_hold: assets_to_hold.clone(),
    // };
    // let _liquidator = Liquidator::try_create(liqudidator_config, &skey, &db_manager)?;

    // engine.add_strategy(Box::new(liquidator));

    // - Withdrawer -

    let withdrawer_config = WithdrawerConfig {
        rpc_url: rpc_url.clone(),
        markets: markets.clone(),
        min_withdraw_value_cents,
    };
    let _withdrawer = Withdrawer::try_create(withdrawer_config, &skey)?;

    // engine.add_strategy(Box::new(withdrawer));

    //

    // - ShareSeller -

    // - PortfolioRebalancer -

    let rebalancer_config = RebalancerConfig {
        xlm_safety_margin,
        rpc_url: rpc_url.clone(),
        markets: markets.clone(),
        xlm_address: xlm_address.clone(),
        max_fee_bps: rebalancer_max_fee_bps,
        max_slippage_bps: rebalancer_slippage_bps,
        assets_to_hold: assets_to_hold.clone(),
        swap_providers: swap_providers.clone(),
        refresh_interval_blocks: rebalancer_interval_blocks,
        max_price_impact_bps: rebalancer_max_price_impact_bps,
        min_swap_amount_value_cents: rebalancer_min_swap_amount_value_cents,
    };
    let rebalancer = Rebalancer::try_create(rebalancer_config, &skey)?;

    engine.add_strategy(Box::new(rebalancer));

    // -- Collectors --

    // - Event -

    let event_collector = EventCollector::new(
        &rpc_url,
        EventFilter {
            event_type: EventType::Contract,
            contract_ids: markets,
            topics: vec![],
        },
        &db_manager,
    );
    engine.add_collector(Box::new(event_collector));

    // - Block -

    let block_collector = BlockCollector::new(&rpc_url);
    engine.add_collector(Box::new(block_collector));

    // -- Executor --

    let executor = SorobanExecutor::new(rpc_url.as_str(), &network_passphrase)?;
    engine.add_executor(Box::new(executor));

    let engine_fut = async {
        if let Ok(mut set) = engine.run().await {
            while let Some(res) = set.join_next().await {
                info!(?res);
            }
        }
    };

    tokio::select! {
        _ = engine_fut => {},
        res = metrics_server::serve(metrics_bind_addr) => {
            if let Err(e) = res {
                tracing::error!(?e, "metrics_server stopped");
            }
        },
        _ = shutdown_future() => {},
    }

    Ok(())
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

async fn shutdown_future() {
    let ctrl_c = signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C"),
        _ = terminate => info!("Received SIGTERM"),
    }
}
