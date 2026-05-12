# syntax=docker/dockerfile:1.7

# ----------------------------------------------------------------------------
# Multi-stage build for the alula-liquidator keeper binary.
#
# Stages:
#   1. chef    — base image with cargo-chef.
#   2. planner — produces `recipe.json` describing the dependency graph so the
#                next stage can build deps in a layer that is reused as long
#                as Cargo.lock / manifests are unchanged.
#   3. builder — cooks the recipe (cached) then builds `keeper`.
#   4. runtime — debian-slim with the binary, ca-certs, tini, curl.
#
# Why debian-slim and not distroless:
#   * `cargo install rusqlite` uses the `bundled` feature → SQLite is static.
#   * TLS stack is rustls (per Cargo.lock) → no libssl needed.
#   * tini + curl give us PID-1 signal forwarding and a usable healthcheck;
#     distroless would force a custom healthcheck binary and lose `sh` for
#     operator inspection.
# ----------------------------------------------------------------------------

FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --locked --version 0.1.71
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build deps only — this layer is cached across source-only edits.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin keeper

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/*

# Unprivileged runtime user. Owns /var/lib/keeper for the SQLite DB.
RUN groupadd --system --gid 10001 keeper \
    && useradd  --system --uid 10001 --gid keeper \
                --home-dir /home/keeper --create-home keeper \
    && mkdir -p /var/lib/keeper /etc/keeper \
    && chown keeper:keeper /var/lib/keeper

COPY --from=builder /app/target/release/keeper /usr/local/bin/keeper

USER keeper
WORKDIR /home/keeper
EXPOSE 9090

# tini reaps zombies and forwards SIGTERM/SIGINT so the keeper's tokio
# signal handler can shut down cleanly when `docker stop` fires.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/keeper"]
