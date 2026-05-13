//! Prometheus exposition (`/metrics`) and liveness (`/healthz`). The recorder
//! installs globally so any crate can emit via `metrics::{counter, gauge,
//! histogram}` without plumbing a handle through.

use {
    axum::{Router, extract::State, routing::get},
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

/// Install the global Prometheus recorder and return its handle.
///
/// Must be called exactly once for the lifetime of the process.
pub fn install_prometheus_exporter() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("global prometheus recorder already installed")
}

/// Bind `addr` and serve the metrics + healthz routes until the server stops.
pub async fn serve(handle: PrometheusHandle, addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .with_state(AppState { metrics: handle });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(bind = %addr, "metrics_server listening");

    axum::serve(listener, app).await?;
    Ok(())
}
