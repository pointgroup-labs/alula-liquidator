# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the keeper binary. Runtime is debian-slim because
# rusqlite is bundled and TLS is rustls, so we only need ca-certs + tini +
# curl (for the compose healthcheck).

# Prebuilt image with cargo-chef already installed — avoids re-running
# `cargo install cargo-chef` on every cache miss.
FROM lukemathwalker/cargo-chef:0.1.77-rust-1.95-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Cache the crates.io registry and the target/ directory across builds.
# `sharing=locked` serialises concurrent builds touching the same cache.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
# The cache mount means /app/target is NOT in the final layer, so we copy
# the binary out within the same RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --locked --bin keeper \
    && cp /app/target/release/keeper /usr/local/bin/keeper

FROM debian:bookworm-slim AS runtime

# Build args populated by CI / docker-compose so `docker inspect` reveals
# which commit produced this image. Defaults keep ad-hoc `docker build .`
# working without ceremony.
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
ARG VERSION=0.0.1

LABEL org.opencontainers.image.title="alula-keeper" \
      org.opencontainers.image.description="Stellar/Soroban liquidator keeper" \
      org.opencontainers.image.source="https://github.com/pointgroup-labs/alula-liquidator" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 keeper \
    && useradd  --system --uid 10001 --gid keeper --no-create-home keeper \
    && mkdir -p /var/lib/keeper /etc/keeper \
    && chown keeper:keeper /var/lib/keeper

COPY --link --from=builder /usr/local/bin/keeper /usr/local/bin/keeper

USER keeper
# WORKDIR matches the keeper-data volume mount so `db_path: "./data.db"` in
# config.example.json lands on the persistent volume.
WORKDIR /var/lib/keeper

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=4 \
    CMD curl -fsS http://localhost:9090/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/keeper"]
