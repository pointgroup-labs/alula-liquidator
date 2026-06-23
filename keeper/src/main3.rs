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
        stellar::{Gateway, pubkey_to_strkey},
        storage::SqliteStore,
        strategy::{
            BadDebtRequestInitiator, BadDebtRequestInitiatorConfig, Balancer, BalancerConfig,
            Liquidator, LiquidatorCapital, LiquidatorConfig, Withdrawer, WithdrawerConfig,
            liquidator_capital::LiquidatorCapitalConfig,
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
mod error;
mod execute;
mod metrics;
mod stellar;
mod storage;
mod strategy;

// Reservation TTL is a safety ceiling for hooks lost to task panics; the
// balance cache TTL amortizes the per-opportunity balance RPC roundtrip.
const RESERVATION_TTL: Duration = Duration::from_secs(300);
const BALANCE_CACHE_TTL: Duration = Duration::from_secs(5);

// Liquidator tuning not exposed through the JSON config yet; defaults mirror
// the historical inline values.
const LIQUIDATOR_INCLUSION_FEE_ORACLE_UNITS: i128 = 0;
const BAD_DEBT_MAX_RETRIES: u32 = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let Args { config, skey } = Args::parse();

    let CliConfig {
        rpc_url,
        db_path,
        markets,
        xlm_address,
        swap_providers,
        assets_to_hold,
        metrics_bind_addr,
        xlm_safety_margin,
        network_passphrase,
        rebalancer_max_fee_bps,
        rebalancer_slippage_bps,
        min_profit_margin_cents,
        min_withdraw_value_cents,
        rebalancer_interval_blocks,
        ledger_polling_interval_secs,
        rebalancer_max_price_impact_bps,
        withdrawer_utilization_safety_margin_bps,
        rebalancer_min_swap_amount_value_cents,
    } = CliConfig::load(&config)?;

    assert_eq!(markets.len(), 1, "multiple markets are not yet supported");
    let market = markets[0].clone();

    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);
    let pkey = pubkey_to_strkey(&skey);
    info!(%pkey, "keeper identity");

    let store = SqliteStore::open(&db_path)?;
    let gateway = Arc::new(Gateway::new(&rpc_url, pkey.clone())?);
    let ledger_reader: Arc<dyn LedgerReader> = gateway.clone();
    let metrics_handle = metrics::install_prometheus_recorder();

    gauge!("liquidator_xlm_safety_margin_stroops").set(xlm_safety_margin as f64);

    let mut engine: Engine<Event, Action> = Engine::new();

    let liquidator_capital_config = LiquidatorCapitalConfig {
        xlm_address: xlm_address.clone(),
        reservation_ttl: RESERVATION_TTL,
        balance_cache_ttl: BALANCE_CACHE_TTL,
    };

    let liquidator_capital = Arc::new(LiquidatorCapital::new(&pkey, liquidator_capital_config));

    let bad_debt = BadDebtRequestInitiator::new(
        skey.clone(),
        gateway.clone(),
        store.obligations(),
        Arc::clone(&ledger_reader),
        BadDebtRequestInitiatorConfig {
            markets: markets.clone(),
            max_retries: BAD_DEBT_MAX_RETRIES,
            refresh_interval_blocks: 5, // TODO: Take from config
        },
    );

    let withdrawer = Withdrawer::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        WithdrawerConfig {
            max_retries: 5, // TODO: From config,
            markets: markets.clone(),
            min_withdraw_value_cents,
            refresh_interval_blocks: 2, // TODO: From config
            utilization_safety_margin_bps: withdrawer_utilization_safety_margin_bps,
        },
        ledger_reader.clone(),
    );

    // Balancer is single-market by design; fan out per-market if needed later.
    let balancer_market = markets
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("config.markets must not be empty"))?;
    let balancer = Balancer::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        BalancerConfig {
            market,
            xlm_safety_margin,
            max_submission_retries: 5,
            max_swap_provider_probes: 10,
            xlm_address: xlm_address.clone(),
            assets_to_hold: assets_to_hold.clone(),
            swap_providers: swap_providers.clone(),
            max_price_impact_bps: rebalancer_max_price_impact_bps,
            allowed_swap_slippage_bps: rebalancer_slippage_bps,
            refresh_interval_blocks: rebalancer_interval_blocks,
            min_swap_amount_value_cents: rebalancer_min_swap_amount_value_cents,
        },
        ledger_reader.clone(),
        liquidator_capital.clone(),
    );

    let liquidator = Liquidator::new(
        pkey.clone(),
        skey.clone(),
        gateway.clone(),
        store.cursor(),
        LiquidatorConfig {
            markets: markets.clone(),
            min_profit_margin_cents,
            assets_to_hold,
            swap_providers,
            xlm_address,
            xlm_safety_margin,
            allowed_swap_slippage_bps: 100,
            max_retries: 4,
            refresh_interval_blocks: 5,
            // gain_haircut_bps: LIQUIDATOR_GAIN_HAIRCUT_BPS,
            inclusion_fee_oracle_units: LIQUIDATOR_INCLUSION_FEE_ORACLE_UNITS,
            // flash_enabled: LIQUIDATOR_FLASH_ENABLED,
            // flash_safety_haircut_bps: LIQUIDATOR_FLASH_SAFETY_HAIRCUT_BPS,
        },
        store.obligations(),
        ledger_reader.clone(),
        liquidator_capital,
    );

    engine.add_strategy(Box::new(liquidator));
    // engine.add_strategy(Box::new(withdrawer));
    // engine.add_strategy(Box::new(bad_debt));
    // engine.add_strategy(Box::new(balancer));

    let cursor_repo = Arc::new(store.cursor());
    engine.add_collector(Box::new(SorobanEventCollector::new(
        &rpc_url,
        3213202, // TODO: Take from config
        EventFilter {
            event_type: EventType::Contract,
            contract_ids: markets,
            topics: vec![],
        },
        cursor_repo,
    )?));

    let secs: u64 = 2;
    engine.add_collector(Box::new(LedgerCollector::new(&rpc_url, secs)));

    const DEFAULT_SIMULATION_FEE: u32 = 100_000;

    engine.add_executor(Box::new(SorobanExecutor::new(
        &pkey,
        gateway,
        DEFAULT_SIMULATION_FEE,
        network_passphrase,
    )));

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
            if let Some(res) = set.join_next().await {
                match res {
                    Ok(_) => {
                        error!(
                            "core engine task terminated unexpectedly. Initiating full shutdown..."
                        );
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

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,keeper=info,engine=info"));
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
