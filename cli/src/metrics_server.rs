//! Minimal Axum HTTP server exposing Prometheus metrics.
//!
//! Routes:
//!   * `GET /metrics`  - Prometheus text exposition (scrape target).
//!   * `GET /healthz`  - liveness probe, always returns "ok".
//!
//! The Prometheus recorder is installed as a global, so any code in the
//! process can emit metrics via the `metrics::{counter, gauge, histogram}`
//! macros and they will show up at `/metrics`.

use {
    axum::{Router, extract::State, routing::get},
    metrics::gauge,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
    std::net::SocketAddr,
    tracing::info,
};

#[derive(Clone)]
struct AppState {
    metrics: PrometheusHandle,
}

async fn metrics_handler(State(state): State<AppState>) -> String {
    state.metrics.render()
}

async fn healthz_handler() -> &'static str {
    "ok"
}

/// Install the global Prometheus recorder, bind the listener, and return a
/// future that drives the Axum server until it stops. Caller is responsible
/// for awaiting / select!-ing on the returned future so shutdown signals can
/// preempt it.
pub async fn serve(bind_addr: SocketAddr) -> anyhow::Result<()> {
    let handle = PrometheusBuilder::new().install_recorder()?;

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .with_state(AppState { metrics: handle });

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(bind = %bind_addr, "metrics_server listening");

    axum::serve(listener, app).await?;

    Ok(())
}
