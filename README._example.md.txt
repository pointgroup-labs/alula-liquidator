# Alula Liquidator

An automated liquidation keeper for [Alula](https://alula.finance/) lending pools on Stellar/Soroban.

The bot monitors on-chain events from Alula's money-market contracts, evaluates under-collateralized positions, and participates in liquidation auctions when they exceed a configurable profit threshold. It uses an event-driven architecture loosely inspired by [Artemis](https://github.com/paradigmxyz/artemis).

## Getting Started

### Requirements

- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain)
- A funded Stellar account with enough balance to participate in liquidations.
- Access to a Soroban RPC endpoint

### Build & Run

```bash
git clone <repo-url>
cd alula-liquidator
cargo run -p cli -- --config "PATH.json" --skey "SECRET"
```

### Configuration

Copy the included `config.json` at the project root and adjust it for your environment:

```json
{
    "rpc_url": "https://soroban-testnet.stellar.org",
    "db_path": "./data.db",
    "markets": ["CAVC47LW2WQ2WBJSRIUXICWIZZT6JY6NURFUYWRKT2VLNJ27NUBPJVJO"],
    "network_passphrase": "Test SDF Network ; September 2015",
    "xlm_address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
    "swap_providers": {
        "CCYUQMA3KYXEQWGZM5PIJQVZR3ZMBXP56LBTT5LDN3JBP2WG6RUBSFYN": "CCJUD55AG6W5HAI5LRVNKAE5WDP5XGZBUDS5WNTIVDU7O264UZZE7BRD"
    },
    "assets_to_hold": [],
    "xlm_safety_margin": 200000000,
    "min_profit_margin_cents": 50
}
```

| Field | Description |
| --- | --- |
| `rpc_url` | Soroban RPC endpoint URL |
| `db_path` | Local path for the bot's SQLite database (event cursors, state) |
| `markets` | Alula pool contract IDs to watch for liquidation auctions |
| `network_passphrase` | Stellar network passphrase (`Test SDF Network ; September 2015` for testnet) |
| `xlm_address` | SAC contract address for native XLM |
| `swap_providers` | Custom swap providers that act as existing DEXes adapters to be used from the market contract → DEX router contract used to convert liquidated collateral |
| `assets_to_hold` | Asset contract IDs the bot should *not* swap after a liquidation (kept as-is) |
| `xlm_safety_margin` | Minimum XLM balance to keep in reserve to fund the trustlines and fees, in stroops (1 XLM = 10 000 000 stroops) |
| `min_profit_margin_cents` | Skip liquidations below this estimated profit (USD cents) |

## How It Works

The bot runs a continuous loop with three stages:

1. **Collection** — Subscribes to contract events and new ledgers via the Soroban RPC to stay in sync with on-chain state.
2. **Evaluation** — Maintains a local model of each monitored pool. When a liquidation auction is detected, it estimates profitability after gas and swap costs.
3. **Execution** — If the opportunity meets the profit threshold, it assembles and submits the liquidation transaction.

### Flash-Borrow Backed Liquidations

The bot leverages Alula's flash-borrow facility to execute liquidations without requiring upfront capital. It flash-borrows the repayment asset from the pool, liquidates the under-collateralized position, and repays the flash loan within the same transaction — keeping the profit delta.

### Integrated Swaps

After a liquidation, the bot may receive collateral in an asset it doesn't want to hold. To handle this, it invokes the market contract's integrated swap functionality, routing the seized collateral through a configured swap provider (DEX adapter) to convert it into the desired asset — all within the same transaction. The `swap_providers` config field controls which router is used for each asset, and `assets_to_hold` lets you skip the swap for assets you'd rather keep.

## Acknowledgements

- [Artemis](https://github.com/paradigmxyz/artemis) — MEV bot framework that inspired the architecture
- [Alula](https://alula.finance/) — the underlying lending protocol

## Disclaimer

This software is provided as-is with no guarantees. Liquidation carries inherent financial risk — you may lose funds due to price movements, failed transactions, or bugs. Always test thoroughly on Stellar testnet before deploying to mainnet. The authors accept no liability for losses incurred while running this bot.
