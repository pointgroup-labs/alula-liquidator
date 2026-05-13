# Configuration

The keeper reads a single JSON config file passed via `--config`. All fields are required unless the **Default** column says otherwise, and unknown fields are rejected (`#[serde(deny_unknown_fields)]`) — a typo will fail loudly at startup rather than silently disable a behaviour.

The schema lives in [`keeper/src/config.rs`](../keeper/src/config.rs). [`config.example.json`](../config.example.json) is a starting point for testnet.

## Network

| Field | Type | Description |
|---|---|---|
| `rpc_url` | URL | Soroban RPC endpoint. Use a provider you trust — the keeper is a TLS client to this host. |
| `network_passphrase` | string | Stellar network passphrase. Testnet is `Test SDF Network ; September 2015`; mainnet is `Public Global Stellar Network ; September 2015`. |
| `xlm_address` | strkey | SAC contract address for native XLM on the chosen network. Used by the rebalancer and the fee-margin check. |

## Storage & observability

| Field | Type | Description |
|---|---|---|
| `db_path` | path | Local SQLite file. Holds event cursors and persisted obligation state — losing it forces a full re-sync on next start. Mount this onto a volume in production. |
| `metrics_bind_addr` | `host:port` | Address the Prometheus `/metrics` endpoint binds to. Use `0.0.0.0:9000` inside docker compose so the `prometheus` container can scrape it; bind to `127.0.0.1` for local-only exposure. |

## Markets

| Field | Type | Description |
|---|---|---|
| `markets` | strkey[] | Alula pool contract IDs to watch. Every strategy is fanned out across this list. |

## Capital

| Field | Type | Description |
|---|---|---|
| `assets_to_hold` | strkey[] | Asset contract IDs the keeper wants on its balance. The **first entry** is the rebalancer's swap target; the liquidator skips its post-liquidation swap for any asset in this list. |
| `swap_providers` | strkey[] | DEX-adapter contract IDs the market contract may route through. Used by the rebalancer and by integrated post-liquidation swaps. |
| `xlm_safety_margin` | i128 (stroops) | Minimum XLM balance to keep in reserve for fees and trustlines. 1 XLM = 10 000 000 stroops, so `200000000` = 20 XLM. |

## Liquidator

| Field | Type | Default | Description |
|---|---|---|---|
| `min_profit_margin_cents` | i128 (USD cents) | — | Floor on estimated net profit; opportunities below this are skipped. Computed after gas, swap costs, and the gain haircut. |
| `liquidator_gain_haircut_bps` | i128 (bps) | `500` | Haircut applied to `gain_oracle` to absorb out-leg slippage and oracle drift. `500` = 5 %. |
| `liquidator_inclusion_fee_oracle_units` | i128 (oracle units) | `0` | Absolute allowance for the Stellar transaction inclusion fee when computing net profit. `0` means the fee is treated as negligible — fine for testnet, raise for mainnet. |

## Rebalancer

| Field | Type | Description |
|---|---|---|
| `rebalancer_interval_blocks` | u32 (ledgers) | How often the rebalancer re-evaluates the wallet. Stellar ledgers close every ~5 s, so `2` = every ~10 s. |
| `rebalancer_min_swap_amount_value_cents` | i128 (USD cents) | Skip rebalance swaps below this dollar value. Stops the keeper from paying fees on dust. |
| `rebalancer_max_price_impact_bps` | i128 (bps) | Hard cap on AMM price impact per rebalance swap. Trade size is reduced until the on-chain quote stays under this number. |
| `rebalancer_slippage_bps` | i128 (bps) | External slippage buffer applied on top of the price-impact cap when constructing `min_amount_out`. |
| `rebalancer_max_fee_bps` | i128 (bps) | Maximum total fee (price impact + provider fee) the rebalancer will accept on a swap quote. |

## Withdrawer

| Field | Type | Description |
|---|---|---|
| `min_withdraw_value_cents` | i128 (USD cents) | Skip withdrawals below this dollar value. The withdrawer's pool-utilisation safety margin is a constant in code, not a config knob. |

## CLI flags

The `--skey` flag is deliberately *not* in the config file — it is the only secret and is passed on the command line (or, in docker compose, via the `STELLAR_SKEY` env var, see [`.env.example`](../.env.example)).

| Flag | Description |
|---|---|
| `--config <path>` | Path to the JSON config file. |
| `--skey <S...>` | Stellar secret key, 56 characters, `S...` strkey form. |

## A note on units

The config is unit-rich on purpose: it makes review and audit easier when each number's domain is unambiguous.

- **Stroops** are Stellar's atomic XLM unit. 1 XLM = 10⁷ stroops. Used only for native-XLM balances.
- **Basis points (bps)** are 1/100 of a percent. `30 bps` = 0.30 %. Used for fees, slippage, and price-impact caps.
- **USD cents** are the keeper's internal value unit for profit/threshold math. Conversion to USD happens once via the oracle and is then used as the comparison currency throughout.
- **Oracle units** are a protocol-specific fixed-point used by Alula's oracle. The single field denominated in them (`liquidator_inclusion_fee_oracle_units`) is documented in the protocol spec.
