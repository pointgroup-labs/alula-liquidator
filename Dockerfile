# syntax=docker/dockerfile:1.7
#
# debian-slim runtime: rusqlite is bundled and TLS is rustls, so only
# ca-certs + tini + curl (for the healthcheck) are needed at runtime.

# Prebuilt image — skips `cargo install cargo-chef` on every cache miss.
FROM lukemathwalker/cargo-chef:0.1.77-rust-1.95-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

# Incremental is pure cost in cache-mounted container builds (no prior
# state to update against). line-tables-only pairs with runtime
# RUST_BACKTRACE=1 for file:line panic frames at ~5-10% size cost.
ENV CARGO_INCREMENTAL=0 \
    CARGO_NET_RETRY=10 \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_PROFILE_RELEASE_DEBUG=line-tables-only

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --release --locked --bin keeper --recipe-path recipe.json
COPY . .
# Cache mounts don't persist into the layer; cp out within the RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --locked --bin keeper \
    && cp /app/target/release/keeper /usr/local/bin/keeper

FROM debian:bookworm-slim AS runtime

ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
ARG VERSION=unknown

LABEL org.opencontainers.image.title="alula-keeper" \
      org.opencontainers.image.source="https://github.com/pointgroup-labs/alula-liquidator" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends --no-install-suggests \
       ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 keeper \
    && useradd  --system --uid 10001 --gid keeper --no-create-home keeper \
    && mkdir -p /var/lib/keeper \
    && chown keeper:keeper /var/lib/keeper

COPY --link --from=builder /usr/local/bin/keeper /usr/local/bin/keeper

USER keeper
# WORKDIR matches the keeper-data volume mount so `db_path: "./data.db"`
# in config.example.json lands on the persistent volume.
WORKDIR /var/lib/keeper

ENV RUST_BACKTRACE=1 \
    TINI_KILL_PROCESS_GROUP=1

EXPOSE 9000
STOPSIGNAL SIGTERM

# Compose redeclares this; kept here so `docker run` standalone works too.
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=4 \
    CMD curl -fsS http://localhost:9000/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/keeper"]
