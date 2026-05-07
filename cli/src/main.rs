use {
    clap::Parser,
    ed25519_dalek::SigningKey,
    pipeline::{
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

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    pub config: PathBuf,
    #[arg(short, long)]
    pub skey: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
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
    min_withdraw_value_cents: i128,
    rebalancer_max_price_impact_bps: i128,
    rebalancer_slippage_bps: i128,
    rebalancer_interval_blocks: u32,
    rebalancer_min_swap_amount_value_cents: i128,
    rebalancer_max_fee_bps: i128,
    /// Address the Prometheus `/metrics` endpoint binds to.
    metrics_bind_addr: SocketAddr,
}

impl CliConfig {
    pub fn try_load(config: PathBuf) -> anyhow::Result<Self> {
        let res = serde_json::from_reader(File::open(config)?)?;

        Ok(res)
    }
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

    let bad_debt_request_initiator_config = BadDebtRequestInitiatorConfig {
        rpc_url: rpc_url.clone(),
        markets: markets.clone(),
    };
    let bad_debt_request_initiator =
        BadDebtRequestInitiator::try_create(bad_debt_request_initiator_config, &skey)?;

    let liquidator_config = LiquidatorConfig {
        xlm_safety_margin,
        min_profit_margin_cents,
        markets: markets.clone(),
        rpc_url: rpc_url.clone(),
        swap_fee_buffer_bps: Some(500),
        xlm_address: xlm_address.clone(),
        swap_providers: swap_providers.clone(),
        assets_to_hold: assets_to_hold.clone(),
    };
    let liquidator = Liquidator::try_create(liquidator_config, &skey, &db_manager)?;

    let withdrawer_config = WithdrawerConfig {
        rpc_url: rpc_url.clone(),
        markets: markets.clone(),
        min_withdraw_value_cents,
    };
    let withdrawer = Withdrawer::try_create(withdrawer_config, &skey)?;

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

    engine.add_strategy(Box::new(bad_debt_request_initiator));
    engine.add_strategy(Box::new(withdrawer));
    engine.add_strategy(Box::new(rebalancer));
    engine.add_strategy(Box::new(liquidator));

    // -- Collectors --

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
        .add_directive("pipeline=debug".parse().unwrap())
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
