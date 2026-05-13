//! Liquidator bot for Alula lending pools on Stellar/Soroban.

mod collect;
mod config;
mod execute;
mod metrics;
mod stellar;
mod storage;
mod strategy;

use {
    crate::{
        collect::{Event, block::BlockCollector, soroban_events::SorobanEventCollector},
        config::{Args, CliConfig},
        execute::{Action, stellar_tx::SorobanExecutor},
        stellar::{Gateway, pubkey_to_strkey},
        storage::SqliteStore,
        strategy::{
            BadDebtRequestInitiator, BadDebtRequestInitiatorConfig, CapitalLedger, Liquidator,
            LiquidatorConfig, Rebalancer, RebalancerConfig, Withdrawer, WithdrawerConfig,
        },
    },
    ::metrics::gauge,
    clap::Parser,
    ed25519_dalek::SigningKey,
    engine::{ports::ChainReader, reactor::Engine},
    std::sync::Arc,
    stellar_rpc_client::EventType,
    stellar_strkey::ed25519::PrivateKey,
    tokio::signal,
    tracing::{error, info},
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let Args { config, skey } = Args::parse();
    let CliConfig {
        rpc_url,
        db_path,
        markets,
        xlm_address,
        xlm_safety_margin,
        network_passphrase,
        assets_to_hold,
        swap_providers,
        min_profit_margin_cents,
        min_withdraw_value_cents,
        rebalancer_max_price_impact_bps,
        rebalancer_slippage_bps,
        rebalancer_interval_blocks,
        rebalancer_min_swap_amount_value_cents,
        rebalancer_max_fee_bps,
        liquidator_gain_haircut_bps,
        liquidator_inclusion_fee_oracle_units,
        metrics_bind_addr,
    } = CliConfig::load(&config)?;

    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);
    let pkey = pubkey_to_strkey(&skey);
    info!(%pkey, "keeper identity");

    let store = SqliteStore::open(&db_path)?;
    let gateway = Arc::new(Gateway::new(&rpc_url, pkey.clone())?);
    // Same Arc<Gateway> satisfies Arc<dyn ChainReader> for the read surface;
    // the firewall stays intact at the trait level.
    let chain: Arc<dyn ChainReader> = gateway.clone();

    let metrics_handle = metrics::install_prometheus_exporter();

    // Surface the configured safety margin as a gauge so the dashboard can
    // overlay it against the live XLM balance. Emitted once at startup —
    // the value is immutable for the process lifetime.
    gauge!("liquidator_xlm_safety_margin_stroops").set(xlm_safety_margin as f64);

    let mut engine: Engine<Event, Action> = Engine::new();

    // Single shared capital ledger across all balance-spending strategies so
    // Liquidator and Rebalancer cannot double-commit the same wallet capacity.
    // The executor releases reservations on every terminal tx outcome; the
    // ledger TTL is only a safety ceiling for hooks lost to task panics.
    let capital = Arc::new(CapitalLedger::new(xlm_address.clone()));

    let bad_debt = BadDebtRequestInitiator::new(
        chain.clone(),
        gateway.clone(),
        skey.clone(),
        pkey.clone(),
        BadDebtRequestInitiatorConfig {
            markets: markets.clone(),
        },
    );

    let withdrawer = Withdrawer::new(
        chain.clone(),
        gateway.clone(),
        skey.clone(),
        pkey.clone(),
        WithdrawerConfig {
            markets: markets.clone(),
            min_withdraw_value_cents,
        },
    );

    // Rebalancer is single-market by design; fan out per-market if needed later.
    let rebalancer_market = markets
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("config.markets must not be empty"))?;
    let rebalancer = Rebalancer::new(
        chain.clone(),
        gateway.clone(),
        skey.clone(),
        pkey.clone(),
        RebalancerConfig {
            market: rebalancer_market,
            xlm_address: xlm_address.clone(),
            xlm_safety_margin,
            assets_to_hold: assets_to_hold.clone(),
            swap_providers: swap_providers.clone(),
            max_price_impact_bps: rebalancer_max_price_impact_bps,
            max_slippage_bps: rebalancer_slippage_bps,
            max_fee_bps: rebalancer_max_fee_bps,
            refresh_interval_blocks: rebalancer_interval_blocks,
            min_swap_amount_value_cents: rebalancer_min_swap_amount_value_cents,
        },
        capital.clone(),
    );

    let liquidator = Liquidator::new(
        chain.clone(),
        gateway.clone(),
        skey.clone(),
        pkey.clone(),
        LiquidatorConfig {
            markets: markets.clone(),
            min_profit_margin_cents,
            assets_to_hold,
            swap_providers,
            xlm_address,
            xlm_safety_margin,
            gain_haircut_bps: liquidator_gain_haircut_bps,
            inclusion_fee_oracle_units: liquidator_inclusion_fee_oracle_units,
        },
        store.obligations(),
        store.cursor(),
        capital,
    );

    engine.add_strategy(Box::new(bad_debt));
    engine.add_strategy(Box::new(withdrawer));
    engine.add_strategy(Box::new(rebalancer));
    engine.add_strategy(Box::new(liquidator));

    let cursor_repo = Arc::new(store.cursor());
    engine.add_collector(Box::new(SorobanEventCollector::new(
        &rpc_url,
        crate::collect::soroban_events::EventFilter {
            event_type: EventType::Contract,
            contract_ids: markets,
            topics: vec![],
        },
        cursor_repo,
    )?));
    engine.add_collector(Box::new(BlockCollector::new(&rpc_url)));

    engine.add_executor(Box::new(SorobanExecutor::new(gateway, network_passphrase)));

    tokio::select! {
        _ = run_engine(engine) => {}
        res = metrics::serve(metrics_handle, metrics_bind_addr) => {
            if let Err(e) = res {
                error!(?e, "metrics server stopped");
            }
        }
        _ = shutdown_future() => info!("shutdown signal received"),
    }

    Ok(())
}

async fn run_engine(engine: Engine<Event, Action>) {
    match engine.run().await {
        Ok(mut set) => {
            while let Some(res) = set.join_next().await {
                info!(?res, "engine task finished");
            }
        }
        Err(e) => error!(?e, "engine failed to start"),
    }
}

fn setup_tracing() {
    let filter = EnvFilter::new("warn")
        .add_directive("keeper=info".parse().unwrap())
        .add_directive("engine=info".parse().unwrap());
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
