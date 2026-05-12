# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the keeper binary using cargo-chef for dep caching.
# Runtime is debian-slim because rusqlite is bundled and TLS is rustls, so
# we only need ca-certs + tini + curl (for the compose healthcheck).

FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --locked --version 0.1.71
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin keeper

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 keeper \
    && useradd  --system --uid 10001 --gid keeper \
                --home-dir /home/keeper --create-home keeper \
    && mkdir -p /var/lib/keeper /etc/keeper \
    && chown keeper:keeper /var/lib/keeper

COPY --from=builder /app/target/release/keeper /usr/local/bin/keeper

USER keeper
# WORKDIR matches the keeper-data volume mount so `db_path: "./data.db"` in
# config.example.json lands on the persistent volume.
WORKDIR /var/lib/keeper

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/keeper"]
