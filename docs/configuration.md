# Configuration

The keeper reads a single config file passed via `--config`; the parser is chosen by the file extension — JSON or TOML. [`config.example.toml`](../config.example.toml) is a working testnet starting point. The schema is the `CliConfig` struct in [`keeper/src/config.rs`](../keeper/src/config.rs). Any field can also be overridden by a `KEEPER_`-prefixed environment variable (e.g. `KEEPER_RPC_URL`), subject to the same strict unknown-field check. Integer values load through an `i64`→`i128` shim, so they cap at `i64` — far above any real config value.

Two rules govern the whole file and are worth internalising before you touch anything:

- **Every field is required.** `CliConfig` has no `Option` fields and no `#[serde(default)]` — a missing key fails the load.
- **Unknown fields are rejected** (`#[serde(deny_unknown_fields)]`).

On top of serde, the [`validator`](https://docs.rs/validator) crate enforces per-field ranges (shown in the **Constraints** column below). Validation runs once in `CliConfig::load` right after parsing.

> **Naming:** every tunable is prefixed with the strategy that owns it (`liquidator_*`, `balancer_*`, `withdrawer_*`, `bad_debt_request_initiator_*`) or is a shared/`General` field.

## General

Shared infrastructure and cross-strategy capital settings. These are consumed in [`keeper/src/main.rs`](../keeper/src/main.rs) and handed to strategies, collectors, and the executor.

| Field | Type / Unit | Constraints | Description |
|---|---|---|---|
| `rpc_url` | URL | valid URL | Soroban RPC endpoint. The keeper is a TLS client to this host for **all** on-chain reads and writes (event polling, ledger polling, simulation, submission). Use a provider you trust and can rely on — RPC latency directly caps how fast the bot reacts. |
| `db_path` | path | — | Local SQLite file. Persists the event cursor and the obligation cache. Losing it forces a full event re-sync from `event_collector_start_ledger` on the next start. |
| `markets` | strkey[] | ≥ 1 entry, each a 56-char `C…`/`G…` address | Alula pool (money-market) contract IDs to watch. The liquidator, withdrawer, and bad-debt initiator fan out across **all** of them(currently, one only single market is supported).|
| `xlm_address` | strkey | exactly 56 chars | SAC contract address for native XLM on the chosen network. Identifies which token the `xlm_safety_margin` reserve applies to; balances of this asset are shaved by the margin before the liquidator or balancer will spend them. |
| `xlm_safety_margin` | i128 (stroops) | ≥ 1 | Minimum XLM balance to hold back for inclusion fees and trustline reserves. 1 XLM = 10 000 000 stroops (`2000000000` = 200 XLM). Whenever XLM is a spend/swap source, the usable amount is `balance − xlm_safety_margin`.|
| `default_simulation_fee` | u32 (stroops) | ≥ 100 | Starting `fee` on every transaction the executor builds, and a **floor** on the final fee.|
| `network_passphrase` | string | non-empty | Stellar network passphrase; hashed into the signature payload, so it must match the network `rpc_url` points at or every submission is rejected. Testnet: `Test SDF Network ; September 2015`. Mainnet: `Public Global Stellar Network ; September 2015`. |
| `assets_to_hold` | strkey[] | ≥ 1 entry, valid addresses | Assets the keeper wants to keep on its balance. **The first entry is the Balancer's swap target(for now)** — everything not in this list gets rebalanced into `assets_to_hold[0]`. The liquidator also treats these as the candidate *source* assets for pre-swap liquidations (swaps a held asset into the repay asset). |
| `swap_providers` | strkey[] | ≥ 1 entry, valid addresses | DEX-adapter contract IDs the keeper may route swaps through. The liquidator probes each to price its flash/pre-swap legs; the Balancer probes each to price rebalances. More providers = better fills but more RPC calls per evaluation. |
| `metrics_bind_addr` | `host:port` | valid socket addr | Address the Prometheus `/metrics` endpoint binds to. Use `0.0.0.0:9000` inside docker compose so the `prometheus` container can scrape it; use `127.0.0.1:9000` for local-only exposure. The same address also serves `/healthz` (liveness) and `/readyz` (readiness). |
| `readiness_staleness_budget_secs` | u64 (seconds) | ≥ 1, **optional** (default `120`) | How long the keeper may go without completing a scan tick before `/readyz` returns `503`. Set it comfortably above your slowest `*_refresh_interval_blocks` cadence (blocks × ~5 s) so an idle-but-healthy keeper doesn't flap between ready and not-ready. |
| `event_collector_start_ledger` | u32 (ledger seq) | — | Ledger to begin contract-event collection from **when there is no saved cursor** in `db_path` (i.e. first run or after deleting the DB). Once a cursor exists it wins and this is ignored. Set it too far in the past (older than the standard RPC's ~7-day retention) and the collector hits a terminal error on its first poll and shuts down. Must pick a ledger that's more recent AND not older than the ledger where the tracked markets were deployed.|
| `keeper_capital_balance_ttl_secs` | u64 (seconds) | 1–10 | How long a fetched token balance is cached before the next RPC read. Shorter = fresher balances but more RPC load; longer = fewer calls but staler view when balances move quickly between strategies. |
| `keeper_capital_reservation_ttl_secs` | u64 (seconds) | 1–30 | Safety ceiling on an in-flight capital *reservation*. Reservations stop strategies double-spending the same wallet balance and are normally released by the executor's settle hook on any terminal tx outcome; this TTL only reclaims a reservation whose hook was lost to a task panic. Keep it comfortably longer than a typical submit-and-confirm cycle. |
| `ledger_collector_polling_interval_secs` | u64 (seconds) | ≥ 1 | Sleep between "what's the latest ledger?" polls. Stellar ledgers close every ~5 s, so `2` polls a bit faster than block time. This paces every strategy's per-ledger refresh, since those are driven by `NewLedger` events. |

★ Insight ─────────────────────────────────────
- **`xlm_address` + `xlm_safety_margin` are a pair.** The margin is meaningless without knowing *which* SAC is native XLM, so the address field is what lets the spend paths recognise "this is the fee asset, hold some back". Every place that computes a usable balance special-cases `token == xlm_address`.
- **`default_simulation_fee` is a floor, not the fee.** The real fee is discovered by simulation and padded 50%.
─────────────────────────────────────────────────

---

## Bad Debt Request Initiator

Listens for `Liquidate` events (and sweeps cached obligations each interval). When a borrower is left with debt but no viable collateral, it submits `cover_bad_debt` so the insurance fund can socialise the loss.

| Field | Type / Unit | Constraints | Description |
|---|---|---|---|
| `bad_debt_request_initiator_max_retries` | u32 | 1–50 | Max submission attempts for the `cover_bad_debt` transaction before giving up. Each retry re-fetches the sequence number and backs off (~250 ms × attempt). |
| `bad_debt_request_initiator_refresh_interval_blocks` | u32 (ledgers) | ≥ 1 | Periodic sweep cadence. The strategy also reacts immediately to `Liquidate` events; this interval is the belt-and-braces re-scan of all cached obligations.|

---

## Withdrawer

Pulls the keeper's own deposits back out of pools when it can do so without pushing utilisation past a safety band (i.e. without incurring a scarcity fee).

| Field | Type / Unit | Constraints | Description |
|---|---|---|---|
| `withdrawer_max_retries` | u32 | 1–50 | Max submission attempts for a withdraw transaction. |
| `withdrawer_refresh_interval_blocks` | u32 (ledgers) | ≥ 1 | Periodic re-scan cadence for withdrawal opportunities; fires on ledgers that are exact multiples of this value.|
| `withdrawer_min_withdraw_value_cents` | i128 (USD cents) | ≥ 0 | Skip any withdrawal whose oracle value is below this. Stops the keeper paying fees to pull out dust.|
| `withdrawer_utilization_safety_margin_bps` | i128 (bps) | 0–10000 | Headroom kept below the pool's utilisation cap when sizing a withdrawal. The withdrawer will not consume capacity above `utilization_limit − this`. **Lower is more aggressive** (pulls more out, risks borrowers being unable to draw and risks the withdrawal itself tipping utilisation and burning a scarcity fee); **higher leaves more idle deposits**. A panel dominated by `pool_at_capacity` means this is too tight for current utilisation. `500` = 5 %. |

---

## Liquidator

The core strategy: scans cached obligations each interval, finds under-collateralised positions, and repays debt to seize discounted collateral. It picks the most profitable `(borrow, deposit)` pair and the best-sized execution among three modes — **Direct** (repay from the wallet), **Pre-Swap** (swap a held asset into the repay asset first), and **Flash** (flash-borrow the repay asset from the pool, seize, swap back, auto-repay).

| Field | Type / Unit | Constraints | Description |
|---|---|---|---|
| `liquidator_max_retries` | u32 | 1–50 | Max submission attempts for a liquidation transaction. |
| `liquidator_refresh_interval_blocks` | u32 (ledgers) | ≥ 1 | Minimum gap between full obligation re-evaluations. |
| `liquidator_min_profit_margin_cents` | i128 (USD cents, **signed**) | ≥ −1000 | Floor on estimated net profit; a plan is accepted only when `net_value > this`. **It is signed on purpose.** `0` = strict break-even-or-better. A **negative** value is a deliberate, bounded loss budget for an operator who runs the keeper for *protocol safety* rather than profit (e.g. the Alula team): because the value is *subtracted* inside the repay-cap math, a negative margin actually **widens** the repay cap, letting the keeper clear more debt than a break-even bot would. The `−1000` (= −$10.00) floor caps how much loss you can opt into per liquidation. |
| `liquidator_max_allowed_swap_slippage_bps` | i128 (bps) | 0–10000 | Slippage buffer applied to the liquidator's swap legs (Flash and Pre-Swap). On the outgoing quote it shrinks the accepted `amount_out` (`× (10000 − bps)/10000`) so the plan still clears the flash repayment if the fill is worse than quoted; on the input side it pads `amount_in`. `0` is fine on testnet with deep synthetic liquidity; raise it on mainnet. |

★ Insight ─────────────────────────────────────
- **Signed profit margin is the single most important liquidator knob to understand.** A profit-seeking third party sets it ≥ 0. A protocol-defence operator sets it negative to guarantee positions get cleared even at a small loss — the sign flips the bot's *purpose*, not just its threshold.
- **Direct vs Pre-Swap vs Flash is chosen automatically**, but your config shapes which are viable: Direct needs wallet balance of the borrow token; Pre-Swap needs a held asset in `assets_to_hold` plus a route through `swap_providers`; Flash needs pool liquidity and a profitable collateral→borrow swap. Sparse `swap_providers` or an empty wallet quietly narrows the bot to Flash-only.
─────────────────────────────────────────────────

---

## Balancer

Converts non-target assets in the keeper's wallet back into the target asset (`assets_to_hold[0]`) via on-chain AMMs, so collateral seized in liquidations doesn't sit around as odd tokens. **Operates on `markets[0]` only.** Runs each interval and also reacts to the keeper's own `Liquidate`/`Withdraw` events.

| Field | Type / Unit | Constraints | Description |
|---|---|---|---|
| `balancer_max_retries` | u32 | 1–50 | Max submission attempts for a rebalance swap transaction. |
| `balancer_refresh_interval_blocks` | u32 (ledgers) | ≥ 1 | Re-evaluation cadence; fires on ledgers that are exact multiples of this value. `2` ⇒ every other ledger (~10 s on testnet). |
| `balancer_max_allowed_swap_slippage_bps` | i128 (bps) | 0–10000 | External slippage buffer applied **after** the price-impact check, when constructing `min_amount_out` (`amount_out × (10000 − bps)/10000`). Protects the swap from moving between quote and execution. `30` = 0.30 %. |
| `balancer_max_price_impact_bps` | i128 (bps) | 0–10000 | Hard cap on a swap's price impact **relative to the oracle price**. For each provider the balancer starts at the full swappable balance and, if the quote's impact exceeds this cap, halves the size and re-probes. `1200` = 12 %.|
| `balancer_max_swap_provider_probes` | u32 | ≥ 1 | How many halving attempts the size-search makes per provider before giving up on that route. With `max_price_impact_bps`, this bounds the binary-search-style descent: `3` probes tries full, ½, ¼ of the balance. Higher = more chances to find a fitting size, at more RPC calls. |
| `balancer_min_swap_amount_value_cents` | i128 (USD cents) | ≥ 0 | Skip rebalance swaps whose input value is below this. Stops the keeper paying fees on dust.|

★ Insight ─────────────────────────────────────
- **`max_price_impact_bps` and `max_swap_provider_probes` work together as a sizing search.** The cap says "don't move the price more than X"; the probe count says "how hard to look for a size that fits under X". A tight cap with only 1 probe often yields `no_viable_provider` because the first (full-size) quote blows the cap and there's no room to shrink and retry.
- **Price impact is measured against the oracle, not against zero.** A positive impact means the DEX fill is *worse* than the oracle-implied price. That's why the cap is meaningful even for large, "liquid" pairs — it's gating oracle/DEX divergence, not just raw slippage.
─────────────────────────────────────────────────

---


## CLI flags

The `--skey` secret is deliberately **not** in the config file — it's the only secret, and is passed on the command line or (preferably) via the `STELLAR_SKEY` env var, which clap reads as a fallback so it never lands in the process's argv. See [`.env.example`](../.env.example).

| Flag | Env fallback | Description |
|---|---|---|
| `--config <path>` | — | Path to the config file (JSON or TOML by extension). |
| `--skey <S…>` | `STELLAR_SKEY` | Stellar secret key, 56 chars, `S…` strkey form. The CLI flag wins if both are set. |

---

## A note on units

- **Stroops** are Stellar's atomic XLM unit. 1 XLM = 10⁷ stroops. Used for native-XLM balances (`xlm_safety_margin`) and transaction fees (`default_simulation_fee`).
- **Ledgers (blocks)** are Stellar's ~5 s consensus rounds. All `*_refresh_interval_blocks` and `event_collector_start_ledger` are counted in ledgers.
- **Seconds** are wall-clock, used by the collector poll interval and the two capital TTLs.
- **Basis points (bps)** are 1/100 of a percent (`30 bps` = 0.30 %; `10000 bps` = 100 %). Used for slippage, price-impact, and utilisation-headroom caps.
- **USD cents** are the keeper's internal value unit for profit/threshold math. Oracle prices convert token amounts to cents once, and cents are the comparison currency for every "is this worth doing?" gate (`*_min_*_value_cents`, `liquidator_min_profit_margin_cents`).

---

