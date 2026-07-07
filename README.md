# Alula Liquidator

[![ci](https://github.com/pointgroup-labs/alula-liquidator/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pointgroup-labs/alula-liquidator/actions/workflows/ci.yml)

An automated keeper bot for [Alula](https://alula.finance/) lending pools on Stellar/Soroban.

The bot tracks on-chain events from Alula's money-market contracts and runs several strategies that keep the keeper's positions healthy and capture liquidation opportunities. The architecture is loosely inspired by [Artemis](https://github.com/paradigmxyz/artemis): collectors stream events into a shared engine, strategies turn events into actions, and a Soroban executor signs and submits transactions.

The current strategies are:

- `bad_debt_request_initiator` monitors liquidation events. If the borrower is left with residual debt and no viable collateral, it initiates bad-debt coverage through the insurance fund.
- `liquidator` scans obligations for profitable liquidation opportunities and executes them, using pre-swaps or flash loans when the keeper doesn't hold enough of the repayment asset.
- `rebalancer` (the `Balancer` strategy) converts non-target assets in the keeper's wallet into the configured `assets_to_hold` via on-chain AMMs.
- The `withdrawer` pulls the keeper's deposits back out of pools when utilisation allows.

See [`docs/`](./docs) for the full configuration reference and operations guide.

## Quickstart

### With docker compose (recommended)

Brings up the keeper, Prometheus, and a provisioned Grafana dashboard in one command.

```bash
git clone https://github.com/pointgroup-labs/alula-liquidator.git
cd alula-liquidator
cp .env.example .env          # fill STELLAR_SKEY
cp config.example.json config.json
docker compose up -d
```

Then:

- Grafana dashboard: <http://localhost:3000> (login `admin`/`admin`, change on first use)
- Prometheus: <http://127.0.0.1:9090>
- Keeper `/metrics`: scraped internally by Prometheus, not exposed on the host

See [`docs/operations.md`](./docs/operations.md) for the dashboard tour, environment variables, and troubleshooting.

### From source

```bash
cargo run --release -- --config config.json --skey "S..."
```

The keeper binary expects a `--config` JSON path and a `--skey` Stellar secret key (`S...`, 56 chars). Everything else lives in the config file; see [`docs/configuration.md`](./docs/configuration.md).

## How it works

Collectors, strategies, and an executor run as concurrent tasks connected by broadcast channels:

1. **Collectors** subscribe to new ledgers and contract events via the Soroban RPC, keeping the local view of every monitored pool current.
2. **Strategies** each maintain the state they need (market data, obligations, oracle prices, wallet balances) and turn incoming events into candidate actions.
3. **Executor** takes each action, wraps it in a transaction (composing multiple operations via `submit_requests_batch` when needed), signs it, and submits it to the network.

The `engine` crate contains the lending model, the reactor loop, and the trait definitions that adapters implement. The `keeper` crate is the runtime layer: RPC clients, SQLite store, signing, metrics, wired together in `main.rs`.

### Strategies in detail

**`bad_debt_request_initiator`** listens for `Liquidate` events and inspects the borrower's residual obligation. When the obligation has no viable collateral left but still carries debt, it submits `issue_cover_bad_debt` so the insurance fund can socialize the loss. Stateless; no startup sync needed.

**`liquidator`** models obligations and oracle prices per market, estimates net profit after gas, swap costs, and `liquidator_min_profit_margin_cents`, and picks the most profitable (borrow, deposit) pair to liquidate. It chooses between three execution modes based on the keeper's on-hand liquidity: `Direct` (the keeper already holds enough of the repayment asset), `PreSwap` (swap a held asset into the repayment asset first, then liquidate), or `Flash` (flash-borrow the repayment asset from the pool, seize the collateral, swap it back, and repay the flash loan atomically). Non-target collateral received from liquidations is later swapped by the rebalancer; assets in `assets_to_hold` are kept.

**`rebalancer`** (the `Balancer` strategy, config prefix `balancer_*`) runs every `balancer_refresh_interval_blocks` ledgers. It walks the wallet and swaps each non-target asset whose dollar value exceeds `balancer_min_swap_amount_value_cents` into the rebalancer target (the first entry of `assets_to_hold`). Trade size is capped so on-chain price impact stays under `balancer_max_price_impact_bps`, probing progressively smaller sizes up to `balancer_max_swap_provider_probes` times per provider; `balancer_max_allowed_swap_slippage_bps` is applied on top when constructing `min_amount_out`. Retries up to `balancer_max_retries` times on failure.

The **`withdrawer`** watches the keeper's own deposits and pulls idle supply out of pools once it can do so without pushing utilisation past `withdrawer_utilization_safety_margin_bps`. Withdrawals below `withdrawer_min_withdraw_value_cents` are skipped.

## Development

Standard Rust workspace. The CI matrix enforces:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit                   # honours .cargo/audit.toml ignore list
```

PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `chore:`, …). Dependabot opens weekly bumps for cargo and github-actions; see `.github/dependabot.yml`.

## Acknowledgements

- [Artemis](https://github.com/paradigmxyz/artemis), the MEV bot framework that inspired the architecture
- [Alula](https://alula.finance/), the underlying lending protocol

## Disclaimer

This software is provided as-is with no guarantees. Running a keeper carries inherent financial risk — you may lose funds due to price movements, failed transactions, swap slippage, or bugs. Always test thoroughly on Stellar testnet before deploying to mainnet. The authors accept no liability for losses incurred while running this bot.

## License

Licensed under the [MIT License](./LICENSE). Contributions are accepted under the same terms unless explicitly stated otherwise.
