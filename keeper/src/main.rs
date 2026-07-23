//! Liquidator bot for Alula lending pools on Soroban.

use std::{sync::Arc, time::Duration};

use clap::Parser;
use ed25519_dalek::SigningKey;
use engine::{ports::LedgerReader, reactor::Engine};
use stellar_rpc_client::EventType;
use stellar_strkey::ed25519::PrivateKey;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    collect::{
        Event,
        stellar_event::{EventFilter, SorobanEventCollector},
        stellar_ledger::LedgerCollector,
    },
    config::{Args, CliConfig, LiquidationKindArg, StrategyKind, resolve_selection},
    execute::{Action, stellar_tx::SorobanExecutor},
    liquidator_capital::{LiquidatorCapital, LiquidatorCapitalConfig},
    stellar::{client::Gateway, pubkey_to_strkey},
    storage::SqliteStore,
    strategy::{
        BadDebtRequestInitiator, BadDebtRequestInitiatorConfig, Balancer, BalancerConfig,
        Liquidator, LiquidatorConfig, Withdrawer, WithdrawerConfig,
    },
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

    let Args { config, skey, strategies: cli_strategies, liquidation_types: cli_liquidation_types } =
        Args::parse();
    let CliConfig {
        // -- General --
        rpc_url,
        db_path,
        markets,
        xlm_address,
        hub_address,
        swap_providers,
        assets_to_hold,
        metrics_bind_addr,
        fallback_rpc_urls,
        xlm_safety_margin,
        network_passphrase,
        default_simulation_fee,
        rpc_max_call_duration_secs,
        event_collector_start_ledger,
        keeper_capital_balance_ttl_secs,
        readiness_staleness_budget_secs,
        keeper_capital_reservation_ttl_secs,
        ledger_collector_polling_interval_secs,
        // -- Bad Debt Request Initiator --
        bad_debt_request_initiator_max_retries,
        bad_debt_request_initiator_refresh_interval_blocks,
        // -- Withdrawer --
        withdrawer_max_retries,
        withdrawer_refresh_interval_blocks,
        withdrawer_min_withdraw_value_cents,
        withdrawer_utilization_safety_margin_bps,
        // -- Liquidator --
        liquidator_max_retries,
        liquidator_refresh_interval_blocks,
        liquidator_min_profit_margin_cents,
        liquidator_max_allowed_swap_slippage_bps,
        // -- Balancer --
        balancer_max_retries,
        balancer_max_swaps_per_batch,
        balancer_rebalance_threshold_bps,
        balancer_max_execution_impact_bps,
        balancer_refresh_interval_blocks,
        balancer_max_swap_provider_halving_probes,
        balancer_min_swap_amount_value_cents,
        balancer_max_allowed_swap_slippage_bps,
        balancer_max_oracle_price_spread_bps,
        // -- Strategy / liquidation-type selection --
        strategies: cfg_strategies,
        liquidation_types: cfg_liquidation_types,
    } = CliConfig::load(&config)?;
    let skey = SigningKey::from_bytes(&PrivateKey::from_string(&skey)?.0);
    let pkey = pubkey_to_strkey(&skey);
    let version = env!("CARGO_PKG_VERSION");

    // Resolve selections: CLI flag wins, else config file, else all.[TODO: must be all or maybe only Direct?]
    let (enabled_strategies, enabled_liquidation_types) = (
        resolve_selection(&cli_strategies, &cfg_strategies, &StrategyKind::ALL),
        resolve_selection(&cli_liquidation_types, &cfg_liquidation_types, &LiquidationKindArg::ALL),
    );

    info!(%pkey, %version, ?enabled_strategies, ?enabled_liquidation_types, "starting keeper...");

    let store = SqliteStore::open(&db_path)?;
    let metrics_handle = metrics::install_prometheus_recorder();
    let start_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    metrics::emit_build_info(start_unix_secs);

    // The liquidator consumes only the ordered list of asset addresses (its
    // pre-swap source candidates); the Balancer consumes the full weighted map
    // plus the hub. Order the flat list by descending target weight so the
    // liquidator prefers larger holdings first.
    let mut assets_to_hold_addresses: Vec<String> = assets_to_hold.keys().cloned().collect();
    assets_to_hold_addresses
        .sort_by(|a, b| assets_to_hold[b].cmp(&assets_to_hold[a]).then_with(|| a.cmp(b)));

    // The hub isn't a key in `assets_to_hold` (it's the balancer's residual
    // numeraire), but the liquidator still holds it and can swap it into a
    // repay token during a pre-swap liquidation. It typically carries the
    // largest balance, so surface it first as a source candidate.
    let liquidator_source_assets: Vec<String> = std::iter::once(hub_address.clone())
        .chain(assets_to_hold_addresses.iter().cloned())
        .collect();

    // Primary endpoint first, then any configured fallbacks. The
    // FailoverClient routes calls through this list, sticking to the
    // last-known-good node and demoting endpoints that fail with transport
    // errors.
    let rpc_urls: Vec<url::Url> =
        std::iter::once(rpc_url.clone()).chain(fallback_rpc_urls).collect();
    let gateway = Arc::new(Gateway::new(
        rpc_urls,
        pkey.clone(),
        Duration::from_secs(rpc_max_call_duration_secs),
    )?);
    let ledger_reader: Arc<dyn LedgerReader> = gateway.clone();

    metrics::set_xlm_safety_margin(xlm_safety_margin);

    let liquidator_capital_config = LiquidatorCapitalConfig {
        xlm_address: xlm_address.clone(),
        balance_cache_ttl: Duration::from_secs(keeper_capital_balance_ttl_secs),
        reservation_ttl: Duration::from_secs(keeper_capital_reservation_ttl_secs),
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
            xlm_safety_margin,
            markets: markets.clone(),
            xlm_address: xlm_address.clone(),
            max_retries: liquidator_max_retries,
            swap_providers: swap_providers.clone(),
            assets_to_hold: liquidator_source_assets.clone(),
            min_profit_margin_cents: liquidator_min_profit_margin_cents,
            refresh_interval_blocks: liquidator_refresh_interval_blocks,
            enabled_liquidation_types: enabled_liquidation_types.clone(),
            max_allowed_swap_slippage_bps: liquidator_max_allowed_swap_slippage_bps,
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
            refresh_interval_blocks: withdrawer_refresh_interval_blocks,
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
            xlm_safety_margin,
            markets: markets.clone(),
            hub_address: hub_address.clone(),
            xlm_address: xlm_address.clone(),
            max_retries: balancer_max_retries,
            assets_to_hold: assets_to_hold.clone(),
            swap_providers: swap_providers.clone(),
            max_swaps_per_batch: balancer_max_swaps_per_batch,
            refresh_interval_blocks: balancer_refresh_interval_blocks,
            rebalance_threshold_bps: balancer_rebalance_threshold_bps,
            max_execution_impact_bps: balancer_max_execution_impact_bps,
            min_swap_amount_value_cents: balancer_min_swap_amount_value_cents,
            allowed_swap_slippage_bps: balancer_max_allowed_swap_slippage_bps,
            max_oracle_price_spread_bps: balancer_max_oracle_price_spread_bps,
            max_swap_provider_halving_probes: balancer_max_swap_provider_halving_probes,
        },
        Arc::clone(&ledger_reader),
        Arc::clone(&liquidator_capital),
    );

    if enabled_strategies.contains(&StrategyKind::BadDebt) {
        engine.add_strategy(Box::new(bad_debt_request_initiator));
    }
    if enabled_strategies.contains(&StrategyKind::Liquidator) {
        engine.add_strategy(Box::new(liquidator));
    }
    if enabled_strategies.contains(&StrategyKind::Withdrawer) {
        engine.add_strategy(Box::new(withdrawer));
    }
    if enabled_strategies.contains(&StrategyKind::Balancer) {
        engine.add_strategy(Box::new(balancer));
    }

    let cursor_repo = Arc::new(store.cursor());
    engine.add_collector(Box::new(SorobanEventCollector::try_new(
        gateway.clone(),
        event_collector_start_ledger,
        EventFilter { topics: vec![], contract_ids: markets, event_type: EventType::Contract },
        cursor_repo,
    )?));
    engine.add_collector(Box::new(LedgerCollector::new(
        gateway.clone(),
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
        res = metrics::serve(metrics_handle, metrics_bind_addr, readiness_staleness_budget_secs) => {
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
        _ = ctrl_c => info!("received Ctrl+C"),
        _ = terminate => info!("received SIGTERM"),
    }
}

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,keeper=info,engine=info"));
    tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer()).init();
}
