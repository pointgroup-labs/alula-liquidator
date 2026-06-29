//! Liquidator bot for Alula lending pools on Soroban.

use {
    crate::{
        collect::{
            Event,
            stellar_event::{EventFilter, SorobanEventCollector},
            stellar_ledger::LedgerCollector,
        },
        config::{Args, CliConfig},
        execute::{Action, stellar_tx::SorobanExecutor},
        liquidator_capital::{LiquidatorCapital, LiquidatorCapitalConfig},
        stellar::{client::Gateway, pubkey_to_strkey},
        storage::SqliteStore,
        strategy::{
            BadDebtRequestInitiator, BadDebtRequestInitiatorConfig, Balancer, BalancerConfig,
            Liquidator, LiquidatorConfig, Withdrawer, WithdrawerConfig,
        },
    },
    ::metrics::gauge,
    clap::Parser,
    ed25519_dalek::SigningKey,
    engine::{ports::LedgerReader, reactor::Engine},
    std::{sync::Arc, time::Duration},
    stellar_rpc_client::EventType,
    stellar_strkey::ed25519::PrivateKey,
    tokio::signal,
    tracing::{error, info},
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
};

mod collect;
mod config;
mod constants;
mod error;
mod execute;
mod liquidator_capital;
mod metrics;
mod stellar;
mod storage;
mod strategy;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let Args { config, skey } = Args::parse();
    let CliConfig {
        // -- General --
        rpc_url,
        db_path,
        markets,
        xlm_address,
        swap_providers,
        assets_to_hold,
        metrics_bind_addr,
        xlm_safety_margin,
        network_passphrase,
        default_simulation_fee,
        event_collector_start_ledger,
        ledger_collector_polling_interval_secs,
        // -- Bad Debt Request Initiator --
        bad_debt_request_initiator_max_retries,
        bad_debt_request_initiator_refresh_interval_blocks,
        // -- Withdrawer --
        withdrawer_max_retries,
        withdrawrer_refresh_interval_blocks,
        withdrawer_min_withdraw_value_cents,
        withdrawer_utilization_safety_margin_bps,
        // -- Liquidator --
        liquidator_max_retries,
        liquidator_refresh_interval_blocks,
        liquidator_capital_balance_ttl_secs,
        liquidator_capital_reservation_ttl_secs,
        liquidator_max_allowed_swap_slippage_bps,
        liquidator_min_profit_margin_cents,
        // -- Balancer --
        balancer_max_retries,
        balancer_max_price_impact_bps,
        balancer_refresh_interval_blocks,
        balancer_max_swap_provider_probes,
        balancer_min_swap_amount_value_cents,
        balancer_max_allowed_swap_slippage_bps,
    } = CliConfig::load(&config)?;
    assert_eq!(
        markets.len(),
        1,
        "multiple markets aren't supported for now"
    );

    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);
    let pkey = pubkey_to_strkey(&skey);

    info!(%pkey, "starting keeper...");

    let store = SqliteStore::open(&db_path)?;
    let metrics_handle = metrics::install_prometheus_recorder();
    let gateway = Arc::new(Gateway::new(&rpc_url, pkey.clone())?);
    let ledger_reader: Arc<dyn LedgerReader> = gateway.clone();

    gauge!("liquidator_xlm_safety_margin_stroops").set(xlm_safety_margin as f64);

    let liquidator_capital_config = LiquidatorCapitalConfig {
        xlm_address: xlm_address.clone(),
        balance_cache_ttl: Duration::from_secs(liquidator_capital_balance_ttl_secs),
        reservation_ttl: Duration::from_secs(liquidator_capital_reservation_ttl_secs),
    };
    let liquidator_capital = Arc::new(LiquidatorCapital::new(&pkey, liquidator_capital_config));

    let mut engine = Engine::<Event, Action>::new();

    let bad_debt_request_initiator = BadDebtRequestInitiator::new(
        skey.clone(),
        gateway.clone(),
        store.obligations(),
        Arc::clone(&ledger_reader),
        BadDebtRequestInitiatorConfig {
            markets: markets.clone(),
            max_retries: bad_debt_request_initiator_max_retries,
            refresh_interval_blocks: bad_debt_request_initiator_refresh_interval_blocks,
        },
    );

    let liquidator = Liquidator::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        store.cursor(),
        LiquidatorConfig {
            markets: markets.clone(),
            min_profit_margin_cents: liquidator_min_profit_margin_cents,
            assets_to_hold: assets_to_hold.clone(),
            swap_providers: swap_providers.clone(),
            xlm_address: xlm_address.clone(),
            xlm_safety_margin,
            max_allowed_swap_slippage_bps: liquidator_max_allowed_swap_slippage_bps,
            max_retries: liquidator_max_retries,
            refresh_interval_blocks: liquidator_refresh_interval_blocks,
        },
        store.obligations(),
        Arc::clone(&ledger_reader),
        Arc::clone(&liquidator_capital),
    );

    let withdrawer = Withdrawer::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        WithdrawerConfig {
            markets: markets.clone(),
            max_retries: withdrawer_max_retries,
            refresh_interval_blocks: withdrawrer_refresh_interval_blocks,
            min_withdraw_value_cents: withdrawer_min_withdraw_value_cents,
            utilization_safety_margin_bps: withdrawer_utilization_safety_margin_bps,
        },
        Arc::clone(&ledger_reader),
    );

    let balancer = Balancer::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        BalancerConfig {
            market: markets[0].clone(),
            xlm_address: xlm_address.clone(),
            xlm_safety_margin,
            max_price_impact_bps: balancer_max_price_impact_bps,
            max_retries: balancer_max_retries,
            allowed_swap_slippage_bps: balancer_max_allowed_swap_slippage_bps,
            assets_to_hold: assets_to_hold.clone(),
            swap_providers: swap_providers.clone(),
            refresh_interval_blocks: balancer_refresh_interval_blocks,
            max_swap_provider_probes: balancer_max_swap_provider_probes,
            min_swap_amount_value_cents: balancer_min_swap_amount_value_cents,
        },
        Arc::clone(&ledger_reader),
        Arc::clone(&liquidator_capital),
    );

    engine.add_strategy(Box::new(bad_debt_request_initiator));
    engine.add_strategy(Box::new(liquidator));
    engine.add_strategy(Box::new(withdrawer));
    engine.add_strategy(Box::new(balancer));

    let cursor_repo = Arc::new(store.cursor());
    engine.add_collector(Box::new(SorobanEventCollector::try_new(
        &rpc_url,
        event_collector_start_ledger,
        EventFilter {
            topics: vec![],
            contract_ids: markets,
            event_type: EventType::Contract,
        },
        cursor_repo,
    )?));
    engine.add_collector(Box::new(LedgerCollector::new(
        &rpc_url,
        ledger_collector_polling_interval_secs,
    )));

    engine.add_executor(Box::new(SorobanExecutor::new(
        &pkey,
        gateway,
        default_simulation_fee,
        network_passphrase,
    )));

    tokio::select! {
        _ = run_engine(engine) => {},
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
            if let Some(res) = set.join_next().await {
                match res {
                    Ok(_) => {
                        error!("core engine task terminated unexpectedly. shutting down...");
                    }
                    Err(e) => {
                        error!(?e, "engine task panicked");
                    }
                }
            }
        }
        Err(e) => error!(?e, "engine failed to start"),
    }
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

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,keeper=info,engine=info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
