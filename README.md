# Alula Liquidator

[![ci](https://github.com/pointgroup-labs/alula-liquidator/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pointgroup-labs/alula-liquidator/actions/workflows/ci.yml)

An automated keeper bot for [Alula](https://alula.finance/) lending pools on Stellar/Soroban.

The bot tracks on-chain events from Alula's money-market contracts and runs a set of cooperating strategies that keep the keeper's positions healthy and capture liquidation opportunities. Architecture is loosely inspired by [Artemis](https://github.com/paradigmxyz/artemis): collectors stream events into a shared engine, strategies turn events into intents, and a Soroban executor signs and submits the resulting transactions.

The current strategies are:

- `bad_debt_request_initiator` — flags under-collateralised obligations as bad debt so the insurance fund contract can process them.
- `liquidator` — participates in liquidation auctions, optionally using flash borrows or pre-swaps for capital efficiency.
- `rebalancer` — converts non-target assets in the keeper's wallet into the configured `assets_to_hold` via on-chain AMMs.
- `withdrawer` — pulls the keeper's own liquidity back out of pools when utilisation allows.

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

See [`docs/operations.md`](./docs/operations.md) for the dashboard tour, env-var contract, and troubleshooting.

### From source

```bash
cargo run --release -- --config config.json --skey "S..."
```

The keeper binary expects a `--config` JSON path and a `--skey` Stellar secret key (`S...`, 56 chars). Everything else lives in the config file — see [`docs/configuration.md`](./docs/configuration.md).

## How it works

The bot runs a continuous loop with three stages:

1. **Collection** — A block collector and an event collector subscribe to new ledgers and contract events via the Soroban RPC, keeping the local view of every monitored pool in sync.
2. **Evaluation** — Each strategy maintains the slice of state it cares about (market data, the keeper's own obligation, oracle prices, wallet balances, …) and turns incoming events into candidate actions.
3. **Execution** — A Soroban executor batches the resulting operations into a single transaction, signs it with the configured key, and submits it to the network.

The `engine` crate holds the deterministic, side-effect-free core (lending model + reactor loop + the trait surface that adapters plug into). The `keeper` crate is the I/O shell — RPC clients, SQLite store, signing, metrics — wired together in `main.rs`.

### Strategies in one paragraph each

**`bad_debt_request_initiator`** — Listens for borrow/repay/liquidate events that change an obligation's health, and submits the protocol call that flags any newly under-collateralised obligation as bad debt. This unlocks the auction path the liquidator participates in. Stateless; no startup sync needed.

**`liquidator`** — The core opportunity-taker. Models obligations and oracle prices per market, estimates net profit after gas / swap costs / `min_profit_margin_cents`, and chooses one of three execution modes automatically based on on-hand liquidity: `Own` (enough repayment asset already on the balance), `FlashLoan` (borrow from the pool, repay in the same tx), or `PreSwap` (swap an idle asset into the repayment asset first, optionally topped up with a smaller flash borrow). Any non-target collateral received is routed through `swap_providers` in the same transaction; assets listed in `assets_to_hold` skip the swap.

**`rebalancer`** — Runs every `rebalancer_interval_blocks` ledgers. Walks the wallet, picks the first non-target asset whose dollar value exceeds `rebalancer_min_swap_amount_value_cents`, and swaps it into the rebalancer target (the first entry of `assets_to_hold`). Trade size is capped so on-chain price impact stays under `rebalancer_max_price_impact_bps`; `rebalancer_slippage_bps` is layered on top as an external slippage buffer when constructing `min_amount_out`. Bounded retries on failure.

**`withdrawer`** — Watches the keeper's own deposits and withdraws idle liquidity once it can do so without pushing the pool's utilisation past a built-in safety margin. Withdrawals worth less than `min_withdraw_value_cents` are skipped to avoid dust. Bounded retries on failure.

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

- [Artemis](https://github.com/paradigmxyz/artemis) — MEV bot framework that inspired the architecture
- [Alula](https://alula.finance/) — the underlying lending protocol

## Disclaimer

This software is provided as-is with no guarantees. Running a keeper carries inherent financial risk — you may lose funds due to price movements, failed transactions, swap slippage, or bugs. Always test thoroughly on Stellar testnet before deploying to mainnet. The authors accept no liability for losses incurred while running this bot.

## License

Dual-licensed under either of:

- [MIT License](./LICENSE-MIT) ([opensource.org](https://opensource.org/licenses/MIT))
- [Apache License, Version 2.0](./LICENSE-APACHE) ([apache.org](http://www.apache.org/licenses/LICENSE-2.0))

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.
