//! Prometheus exposition (`/metrics`), liveness (`/healthz`), and readiness
//! (`/readyz`). The recorder is installed process-globally so any crate can
//! emit through `metrics::{counter,gauge,histogram}`; [`catalog`] is the typed
//! single source of truth for every series and its `# HELP`/`# TYPE` metadata.

mod catalog;

pub use catalog::*;

use {
    axum::{Router, extract::State, http::StatusCode, routing::get},
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    std::{
        net::SocketAddr,
        sync::atomic::{AtomicI64, Ordering},
    },
    tracing::info,
};

/// Explicit buckets for the `*_seconds` latency histograms, spanning a few
/// milliseconds to 10 s. The 5 s boundary lines up with `KeeperScanSlow`.
const LATENCY_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// Wall-clock unix seconds of the most recent scan tick. Bridges the reactor
/// (writer, via [`catalog::record_scan`]) and the readiness probe (reader)
/// without threading a handle through the engine.
static LAST_SCAN_TICK_UNIX: AtomicI64 = AtomicI64::new(0);

pub(crate) fn mark_tick(now_unix: i64) {
    LAST_SCAN_TICK_UNIX.store(now_unix, Ordering::Relaxed);
}

#[derive(Clone)]
struct AppState {
    metrics: PrometheusHandle,
    /// Past this many seconds without a scan tick, `/readyz` reports 503.
    /// Operators set it above their slowest `*_refresh_interval_blocks`
    /// cadence so a healthy-but-idle keeper doesn't flap.
    ready_budget_secs: i64,
}

/// Three-state readiness verdict, factored out of the handler so the staleness
/// math is unit-testable without an HTTP server or a real clock.
#[derive(Debug, PartialEq, Eq)]
enum Readiness {
    Warming,
    Ready,
    Stalled,
}

fn readiness_state(last_tick_unix: i64, now_unix: i64, budget_secs: i64) -> Readiness {
    if last_tick_unix == 0 {
        Readiness::Warming
    } else if now_unix.saturating_sub(last_tick_unix) <= budget_secs {
        Readiness::Ready
    } else {
        Readiness::Stalled
    }
}

/// Bind `addr` and serve the metrics + health routes until the server stops.
/// `ready_budget_secs` is the `/readyz` staleness budget.
pub async fn serve(
    handle: PrometheusHandle,
    addr: SocketAddr,
    ready_budget_secs: u64,
) -> anyhow::Result<()> {
    let state = AppState {
        metrics: handle,
        ready_budget_secs: ready_budget_secs.try_into().unwrap_or(i64::MAX),
    };
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!(bind = %addr, "metrics_server listening");

    axum::serve(listener, app).await?;

    Ok(())
}

/// Install the global Prometheus recorder and register every series'
/// description. Panics if a recorder is already installed.
pub fn install_prometheus_recorder() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        // Render the `*_seconds` latency histograms as true Prometheus
        // histograms (explicit buckets) instead of the default rolling
        // summaries, so quantiles can be re-aggregated across markets /
        // functions at query time via `histogram_quantile()`. The
        // value-distribution histograms stay as summaries — fixed buckets
        // don't fit their wide monetary ranges.
        .set_buckets_for_metric(Matcher::Suffix("_seconds".to_owned()), &LATENCY_BUCKETS)
        .expect("latency bucket set is non-empty")
        .install_recorder()
        .expect("global prometheus recorder already installed");
    catalog::describe_all();

    handle
}

async fn metrics_handler(State(state): State<AppState>) -> String {
    state.metrics.render()
}

/// Liveness: the process is up and the HTTP server is accepting connections.
async fn healthz_handler() -> &'static str {
    "ok"
}

/// Readiness: the scan loop has completed at least one tick and is not stalled.
/// Distinct from liveness so an orchestrator can hold traffic/restarts off a
/// process that is up but wedged.
async fn readyz_handler(State(state): State<AppState>) -> (StatusCode, &'static str) {
    let last = LAST_SCAN_TICK_UNIX.load(Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(last);

    match readiness_state(last, now, state.ready_budget_secs) {
        Readiness::Warming => (
            StatusCode::SERVICE_UNAVAILABLE,
            "warming up: no scan completed yet",
        ),
        Readiness::Ready => (StatusCode::OK, "ready"),
        Readiness::Stalled => (StatusCode::SERVICE_UNAVAILABLE, "scan loop stalled"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Readiness, readiness_state};

    #[test]
    fn warming_until_first_tick() {
        assert_eq!(readiness_state(0, 1_000, 120), Readiness::Warming);
    }

    #[test]
    fn ready_within_budget() {
        assert_eq!(readiness_state(1_000, 1_090, 120), Readiness::Ready);
        // Exactly at the budget boundary still counts as ready.
        assert_eq!(readiness_state(1_000, 1_120, 120), Readiness::Ready);
    }

    #[test]
    fn stalled_past_budget() {
        assert_eq!(readiness_state(1_000, 1_121, 120), Readiness::Stalled);
    }

    #[test]
    fn clock_skew_backwards_stays_ready() {
        // A now earlier than last_tick (clock stepped back) must not underflow
        // into a huge positive staleness and report Stalled.
        assert_eq!(readiness_state(1_000, 900, 120), Readiness::Ready);
    }
}
