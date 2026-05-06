# Alula Keeper

An automated keeper bot for [Alula](https://alula.finance/) lending pools on Stellar/Soroban.

The bot tracks on-chain events from Alula's money-market contracts and runs a set of cooperating strategies that keep the keeper's positions healthy and capture liquidation opportunities. It uses an event-driven architecture loosely inspired by [Artemis](https://github.com/paradigmxyz/artemis): collectors stream events into a shared engine, strategies turn events into intents, and a Soroban executor signs and submits the resulting transactions.

The current strategies are:

- `bad_debt_request_initiator` — flags under-collateralized obligations as bad debt so the insurance fund contract can process them.
- `liquidator` — participates in liquidation auctions, optionally using flash borrows or pre-swaps for capital efficiency.
- `rebalancer` — converts non-target assets in the keeper's wallet into the configured `assets_to_hold` via on-chain AMMs.
- `withdrawer` — pulls the keeper's own liquidity back out of pools when utilization allows.

## Getting Started

### Requirements

- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain)
- A funded Stellar account with enough balance to participate in liquidations and pay fees
- Access to a Soroban RPC endpoint

### Build & Run

```bash
git clone <repo-url>
cd alula-liquidator-2
cargo run -- --config "PATH.json" --skey "SECRET"
```

### Configuration

Copy the included `config.json` at the project root and adjust it for your environment:

```json
{
    "rpc_url": "https://soroban-testnet.stellar.org",
    "db_path": "./data.db",
    "markets": ["CCWYUCUX7QHREMI6R74RAQGCIOXRTELECS2TKYMRD4MDPIYNFSSQUFUS"],
    "network_passphrase": "Test SDF Network ; September 2015",
    "xlm_address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
    "swap_providers": [
        "CAAMNEXA7BOLMJKHDWNWLW6NQONLW3D6EXIKBBDJJEIDOGJYXRD7PJG4"
    ],
    "assets_to_hold": [
        "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
    ],
    "xlm_safety_margin": 200000000,
    "min_profit_margin_cents": 50,
    "min_withdraw_value_cents": 500,
    "rebalancer_interval_blocks": 2,
    "rebalancer_max_price_impact_bps": 100,
    "rebalancer_slippage_bps": 30,
    "rebalancer_min_swap_amount_value_cents": 100
}
```

| Field | Description |
| --- | --- |
| `rpc_url` | Soroban RPC endpoint URL |
| `db_path` | Local path for the bot's SQLite database (event cursors, state) |
| `markets` | Alula pool contract IDs to watch and act on |
| `network_passphrase` | Stellar network passphrase (`Test SDF Network ; September 2015` for testnet) |
| `xlm_address` | SAC contract address for native XLM |
| `swap_providers` | DEX-adapter contract IDs the market contract may route swaps through (rebalancer + integrated liquidation swaps) |
| `assets_to_hold` | Asset contract IDs the keeper wants to keep on its balance. The rebalancer treats the first entry as its swap target; the liquidator skips post-liquidation swaps for any asset in this list |
| `xlm_safety_margin` | Minimum XLM balance to keep in reserve for fees and trustlines, in stroops (1 XLM = 10 000 000 stroops) |
| `min_profit_margin_cents` | Skip liquidations below this estimated profit (USD cents) |
| `min_withdraw_value_cents` | Skip withdrawals whose value is below this threshold (USD cents) — avoids dust transactions |
| `rebalancer_interval_blocks` | How often (in ledgers) the rebalancer re-evaluates the wallet |
| `rebalancer_max_price_impact_bps` | Hard cap on AMM price impact for a rebalance swap, in basis points |
| `rebalancer_slippage_bps` | External slippage buffer applied to `min_amount_out`, in basis points |
| `rebalancer_min_swap_amount_value_cents` | Skip rebalance swaps below this dollar value (USD cents) |

## How It Works

The bot runs a continuous loop with three stages:

1. **Collection** — A block collector and an event collector subscribe to new ledgers and contract events via the Soroban RPC, keeping the local view of every monitored pool in sync.
2. **Evaluation** — Each strategy maintains the slice of state it cares about (market data, the keeper's own obligation, oracle prices, wallet balances, …) and turns incoming events into candidate actions.
3. **Execution** — A Soroban executor batches the resulting operations into a single transaction, signs it with the configured key, and submits it to the network.

### Strategies

#### `bad_debt_request_initiator`

Listens for borrow/repay/liquidate events that change an obligation's health and, when it spots an obligation that has crossed into bad-debt territory, submits the protocol call that requests it be flagged as bad debt. This is what unlocks the auction path that the liquidator later participates in. The strategy is stateless — it doesn't need to sync historical state on startup.

#### `liquidator`

The core opportunity-taker. For every monitored pool it keeps a local model of obligations and oracle prices, and whenever an under-collateralized position becomes liquidatable it estimates net profit (after gas, swap costs, and the configured `min_profit_margin_cents` floor) before acting. Three execution modes are chosen automatically based on what liquidity the keeper has on hand:

- **Own** — the keeper has enough of the repayment asset to cover the auction outright.
- **FlashLoan** — the keeper flash-borrows the repayment asset from the pool, liquidates the position, and repays the loan in the same transaction, keeping the profit delta. No upfront capital is required.
- **PreSwap** — the keeper first swaps an idle asset on its balance into the repayment asset (optionally topped up with a smaller flash borrow), then liquidates.

After a liquidation the bot may receive collateral in an asset it doesn't want to hold. To handle this, it invokes the market contract's integrated swap functionality, routing the seized collateral through one of the configured `swap_providers` to convert it into a desired asset — all within the same transaction. `assets_to_hold` lets you skip the swap for assets you'd rather keep.

#### `rebalancer`

Runs every `rebalancer_interval_blocks` ledgers. It walks the keeper's wallet, picks the first non-target asset whose dollar value exceeds `rebalancer_min_swap_amount_value_cents`, and swaps it into the rebalancer's target asset (the first entry of `assets_to_hold`) through one of the configured `swap_providers`. The trade size is sized so that the on-chain price impact stays under `rebalancer_max_price_impact_bps`; `rebalancer_slippage_bps` is then applied as an external slippage buffer when constructing `min_amount_out`. Failed swaps are retried up to a small bounded number of times before the strategy gives up for the current interval.

#### `withdrawer`

Watches the keeper's own deposit/borrow obligations and withdraws idle liquidity once it can do so without pushing the pool's utilization beyond a built-in safety margin. Withdrawals worth less than `min_withdraw_value_cents` are skipped to avoid paying fees on dust, and failed withdrawals are retried a bounded number of times before being deferred to the next refresh.

## Acknowledgements

- [Artemis](https://github.com/paradigmxyz/artemis) — MEV bot framework that inspired the architecture
- [Alula](https://alula.finance/) — the underlying lending protocol

## Disclaimer

This software is provided as-is with no guarantees. Running a keeper carries inherent financial risk — you may lose funds due to price movements, failed transactions, swap slippage, or bugs. Always test thoroughly on Stellar testnet before deploying to mainnet. The authors accept no liability for losses incurred while running this bot.
