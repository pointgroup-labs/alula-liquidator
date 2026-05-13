# Operations

Running, observing, and debugging a deployed keeper.

## Deployment with docker compose

The included [`docker-compose.yml`](../docker-compose.yml) stands up four services on a private `obs` network:

| Service | Purpose | Host port |
|---|---|---|
| `keeper` | The bot itself. Builds from [`Dockerfile`](../Dockerfile). | `127.0.0.1:9000` (metrics scraped internally) |
| `prometheus` | Scrapes the keeper's `/metrics` every 10 s. | `127.0.0.1:9090` |
| `alertmanager` | Routes Prometheus alerts to the configured webhook. | `127.0.0.1:9093` |
| `grafana` | Renders the provisioned dashboard. | `3000` |

```bash
cp .env.example .env          # fill STELLAR_SKEY
cp config.example.json config.json
docker compose up -d
docker compose logs -f keeper
```

Grafana is reachable at <http://localhost:3000> (initial login `admin`/`admin`, change on first use). Prometheus is intentionally bound to `127.0.0.1` so it is not exposed to other hosts; tunnel to it if you need raw query access.

### Environment

[`.env.example`](../.env.example) documents the contract:

| Var | Required? | Description |
|---|---|---|
| `STELLAR_SKEY` | yes | Stellar secret key (`S...`, 56 chars). Read by clap as the env-var fallback for `--skey`, so the secret never lands on the command line. |
| `RUST_LOG` | no | `tracing-subscriber` directive. Defaults to `info,alula_keeper=info,alula_engine=info`. |
| `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` | no | Grafana bootstrap credentials. |

Both `GIT_SHA` and `BUILD_DATE` are recognised as optional build-args (used to populate the OCI image labels) and fall back to `dev` / `unknown` when unset.

### Production image

The published image is `ghcr.io/pointgroup-labs/alula-keeper`. Tag matrix:

- `:edge` — moving pointer at the default-branch tip.
- `:main` (or other sanitised branch name) — latest from that branch.
- `:sha-<short>` — per-commit pointer. Not cryptographically immutable (BUILD_DATE varies); pin by `@sha256:` digest for that.
- `:vX.Y.Z`, `:vX.Y` — semver tags from `v*` git tags.

## Dashboard tour

The provisioned dashboard `Alula Liquidator` is organised into rows by what question you'd ask it.

**Is the bot alive?** Look at *Scrape up* and *Time since last scan*. Both should be green within 30 s of startup; both go red on a fresh deploy if you forgot to mount the config, used the wrong RPC URL, or the network is unreachable. (Fresh-deploy panels use `or on() vector(0)` so the absence of data shows as red rather than grey.)

**Is the bot doing anything?** *Plans 1h* and *TXs confirmed 1h* are activity counters. *Scan completed by outcome* shows the per-scan verdict mix — a healthy testnet typically idles on `no_opportunity` with the occasional `liquidatable`.

**Is the bot doing the right thing?** The *Funnel*, *Skip reasons*, *Simulation outcomes*, *TX submission*, and *TX confirmation* panels form a pipeline view. Each step's drop-off rate tells you where opportunities are being lost — for example, a sudden rise in `simulation: insufficient_capital` against rising opportunity counts is the rebalancer falling behind, not a bug.

**Is the bot positioned correctly?** *Obligations vs liquidatable* shows the universe being modelled against the subset eligible to act on. The two series live on separate Y-axes because they differ by orders of magnitude. The *XLM funding* tile reads `liquidator_asset_balance{token_address="$xlm"}` — set the dashboard's `xlm` variable to the XLM SAC address (the `xlm_address` from your `config.json`) on first use so the tile shows live balance instead of "No data". The companion *XLM balance vs safety margin* overlay plots the same balance against `liquidator_xlm_safety_margin_stroops`; when the green line approaches the dashed red floor, the keeper is about to start refusing to spend XLM to preserve inclusion-fee headroom.

**Is the bot keeping up?** *Back-pressure* surfaces the event-channel watermark. Sustained non-zero values mean the strategy stage cannot consume events as fast as the collectors emit them; usually a slow database or RPC. *State persistence* is the SQLite write-rate signal — if this goes to zero while *Plans 1h* is non-zero, the keeper is taking actions it cannot durably remember, and a crash will replay them. *Market scan latency* shows p50/p95/p99 of one full `evaluate_market` call; sustained p95 above ~1 s is almost always RPC degradation upstream and trips `KeeperScanSlow` at 5 s — earlier than `KeeperScanStalled` so you can switch providers before scans drop entirely. *RPC simulate latency* drills down by contract function so you can pinpoint which call slowed: `get_market_data`, `get_user_obligation`, swap-provider `get_amount_out`, etc. *RPC simulate failures* splits `transport` (network) from `sim_error` (contract-level) so you can decide whether to swap providers or fix inputs.

**Is the bot making money?** *Expected profit per plan* shows the per-opportunity USD distribution at dispatch time (p50/p95). *Expected vs realised profit dispatched (USD/hour)* overlays the dispatch-time expected total against the confirm-time realised total — the gap between the two lines is the realisation tax: submission failures, simulation drift, retries exhausted. Both lines are still modelled (use `plan.net_profit_oracle`, not on-chain seized amounts), but the realised line filters to txs that actually confirmed, so the gap is operationally meaningful.

**Is the rebalancer working?** *Rebalancer outcomes* stacks every per-candidate decision (`dispatched`, `nothing_to_swap`, `below_dust`, `no_viable_provider`, `reservation_lost`, `evaluation_error`, `precondition_no_target`, `precondition_no_providers`). A silent panel means the rebalancer isn't being invoked at all — check the soroban-event topic filter and `refresh_interval_blocks`. *Rebalancer dispatched swap size* shows the p50/p95 USD value of actually-emitted swaps against the `rebalancer_min_swap_amount_value_cents` floor; p50 hugging the floor means dust-only activity and the threshold may want tuning.

**Are the side strategies working?** *Withdrawer outcomes* stacks the per-deposit verdict (`dispatched`, `below_threshold`, `pool_at_capacity`, `pool_missing`, `no_market_data`, `no_obligations`, `build_error`). A panel dominated by `pool_at_capacity` means the configured `utilization_safety_margin` is too tight for the current pool state — the keeper is preserving headroom rather than yanking liquidity. `below_threshold` dominance is the same diagnosis as the rebalancer's `below_dust`: `min_withdraw_value_cents` is too high for the deposit sizes you're actually holding. *Bad-debt initiator outcomes* stacks the post-`Liquidate`-event verdict (`dispatched`, `ineligible`, `obligation_cleared`, `parse_error`, `decode_op_error`, `build_failed`). The expected steady-state is `ineligible` plus the occasional `obligation_cleared` (the liquidator already cleaned it up first); `parse_error` or `decode_op_error` climbing means an event schema drift against the contract version — re-check the gateway codec.

## Troubleshooting

**"Scrape up is 0."** Prometheus cannot reach `keeper:9000`. Confirm `metrics_bind_addr` in the config matches the address Prometheus targets (`keeper:9000` for the docker-compose stack — note the `keeper` here is the *service* name, not the `container_name`). If you bound to `127.0.0.1`, change it to `0.0.0.0`. A frequent footgun on upgraded deploys: a stale `config/keeper.json` from before the port unification still binds `:9090` — re-sync it from `config.example.json` if the panels stay empty after a clean restart.

**"Time since last scan keeps growing."** The collector is stuck. Inspect `docker compose logs keeper` for repeated RPC errors. The most common cause is a stale event cursor in the SQLite db (`db_path`) after switching networks — delete the db file and restart; the keeper re-derives from head.

**"TX submission is non-zero, TX confirmation is zero."** The RPC accepts the transaction but the network does not confirm. Usually a fee/sequence issue. Bump `liquidator_inclusion_fee_oracle_units` for the inclusion-fee margin and check the keeper's XLM balance against `xlm_safety_margin`.

**"Skip reason `dust` dominates everything."** The configured thresholds (`min_profit_margin_cents`, `rebalancer_min_swap_amount_value_cents`, `min_withdraw_value_cents`) are too high for the current pool sizes. Tune them down.

**"`config.example.json` doesn't load."** The keeper enforces `deny_unknown_fields`. A typo in any key fails the entire load. The error message names the offending field — copy it verbatim or remove it.

## Alert reference

Source of truth is [`deploy/prometheus/rules.yml`](../deploy/prometheus/rules.yml). The table below is the on-call shortcut from page → first action; the rules file itself carries the long-form `description:` annotation.

| Alert | Severity | What it means | First move |
|---|---|---|---|
| `KeeperDown` | critical | Scrape failing >1m. | `docker compose logs keeper`; check crash loop. |
| `KeeperScanStalled` | critical | No successful scan in 5m. | Inspect logs for repeated `get_events` errors; rotate RPC. |
| `KeeperScanSlow` | warning | Scan p95 > 5s for 10m. | Pre-cursor to stall — switch RPC before it tips. |
| `KeeperRpcSimulateTransportFailing` | warning | Network-layer simulate failures >1/min for 10m. | RPC degraded — switch provider. |
| `KeeperEventsDropped` / `KeeperActionsDropped` | critical | Reactor channels overflowing. | Raise channel capacity or speed up the lagging stage. |
| `KeeperCollectorLagging` | warning | Upstream of strategy lag. | Inspect the named `collector` label. |
| `KeeperOpportunitiesNotDispatched` | critical | Liquidatable positions visible but no plans in 10m. | Money-leaving. Check `liquidator_skip_total` and capital ledger. |
| `KeeperTxConfirmationRateLow` | critical | <50% confirm over 15m. | Fee/sequence — bump `liquidator_inclusion_fee_oracle_units`. |
| `KeeperBadSeqRetriesElevated` | warning | Sequence racing. | Check for parallel submitters on the same source account. |
| `KeeperXlmFundingLow` / `KeeperXlmFundingCritical` | warning / critical | XLM at 2x / 1.1x of margin. | Refill the keeper account. |
| `KeeperBadDebtSchemaDrift` | warning | Bad-debt strategy failing to decode events. | Contract event topology likely drifted — re-check gateway codec. |
| `KeeperWithdrawerErrors` | warning | Withdrawer red-path outcomes climbing. | `no_market_data`/`pool_missing`/`build_error` — RPC or gateway, not config. |
| `KeeperCursorSaveFailing` | critical | SQLite cursor writes failing. | Check `db_path` mount and disk space. |

## Security advisories

[`.cargo/audit.toml`](../.cargo/audit.toml) lists the rustsec advisories we accept temporarily, with per-entry rationale and an explicit re-evaluation trigger (every `stellar-rpc-client` or `soroban-sdk` bump). Re-run `cargo audit` after any dependabot PR merges to refresh the picture.
