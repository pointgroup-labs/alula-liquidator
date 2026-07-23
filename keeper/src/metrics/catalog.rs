//! The keeper's metric catalog: the single source of truth for every series
//! exposed on `/metrics`. Call sites emit exclusively through the typed API
//! here — never raw `metrics::{counter,gauge,histogram}!` string literals — so
//! that names and label vocabularies are compiler-checked, exhaustive, and
//! self-documenting via [`describe_all`].
//!
//! Adding a metric means: add its name constant, describe it in
//! [`describe_all`], and expose a typed emitter. Grepping this file lists the
//! entire observable surface of the keeper.

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

// Build provenance for `keeper_build_info`. `GIT_SHA` is injected at compile
// time by the Docker builder (and CI); local `cargo` builds fall back to
// "unknown". The workspace version is pinned, so the commit is what actually
// distinguishes deployed images — hence no build.rs, just the one env var.
const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_SHA: &str = match option_env!("GIT_SHA") {
    Some(sha) => sha,
    None => "unknown",
};

// Label keys. Centralised so a rename is one edit, not a grep-and-pray.
const OUTCOME: &str = "outcome";
const REASON: &str = "reason";
const MARKET: &str = "market";
const CALL: &str = "call";
const KIND: &str = "kind";
const FUNCTION: &str = "function";
const SOURCE: &str = "source";
const COLLECTOR: &str = "collector";
const TYPE: &str = "type";
const TOKEN_ADDRESS: &str = "token_address";
const POOL_ADDRESS: &str = "pool_address";
const TOKEN_SYMBOL: &str = "token_symbol";
const ASSET: &str = "asset";
const ADMITTED: &str = "admitted";

// Metric names. These strings are the wire contract the Grafana dashboard and
// `rules.yml` query against — treat them as public API.
//
// Engine-owned series: emitted from `engine/` (which cannot import this crate)
// through the global recorder. Described here because `describe_*` binds by
// name only. Keep these literals in sync with `engine/src/reactor/mod.rs`.
const STRATEGY_LAGGED: &str = "engine_strategy_lagged_events_total";
const EXECUTOR_LAGGED: &str = "engine_executor_lagged_actions_total";

const COLLECTOR_LAGGED: &str = "keeper_collector_lagged_events_total";
const CURSOR_SAVE_FAILURES: &str = "keeper_cursor_save_failures_total";
const TX_BAD_SEQ_RETRIES: &str = "keeper_tx_bad_seq_retries_total";
const TX_SUBMITTED: &str = "keeper_tx_submitted_total";
const TX_CONFIRMED: &str = "keeper_tx_confirmed_total";
const SIMULATION: &str = "keeper_simulation_total";
const RPC_SIMULATE_FAILURES: &str = "keeper_rpc_simulate_failures_total";
const RPC_SIMULATE_DURATION: &str = "keeper_rpc_simulate_duration_seconds";
const BUILD_INFO: &str = "keeper_build_info";
const START_TIME: &str = "keeper_start_time_seconds";

const SCAN_COMPLETED: &str = "liquidator_scan_completed_total";
const SCAN_DURATION: &str = "liquidator_market_scan_duration_seconds";
const SKIP: &str = "liquidator_skip_total";
const PLANS_DISPATCHED: &str = "liquidator_liquidation_plans_dispatched_total";
const OBLIGATIONS: &str = "liquidator_obligations_total";
const LIQUIDATABLE: &str = "liquidator_liquidatable_positions";
const LAST_SCAN_TS: &str = "liquidator_last_successful_scan_timestamp_seconds";
// Renamed from `..._value_units` for symmetry with the realised counterpart and
// to match the dashboard/rules, which already query `_oracle_units`.
const EXPECTED_PROFIT: &str = "liquidator_plan_expected_net_profit_oracle_units";
const EXPECTED_PROFIT_TOTAL: &str = "liquidator_plan_expected_net_profit_oracle_units_total";
const REALISED_PROFIT: &str = "liquidator_plan_realised_net_profit_oracle_units";
const REALISED_PROFIT_TOTAL: &str = "liquidator_plan_realised_net_profit_oracle_units_total";
const ASSET_BALANCE: &str = "liquidator_asset_balance";
const XLM_BALANCE: &str = "liquidator_xlm_balance_stroops";
const XLM_SAFETY_MARGIN: &str = "liquidator_xlm_safety_margin_stroops";
const SELF_J_TOKENS: &str = "liquidator_self_j_tokens";
const SELF_PLAIN_COLLATERAL: &str = "liquidator_self_plain_collateral";
const SELF_J_TOKENS_UNDERLYING: &str = "liquidator_self_j_tokens_underlying";
const SELF_D_TOKENS: &str = "liquidator_self_d_tokens";
const SELF_D_TOKENS_UNDERLYING: &str = "liquidator_self_d_tokens_underlying";

const BALANCER_OUTCOME: &str = "balancer_outcome_total";
const BALANCER_DISPATCHED_VALUE: &str = "balancer_dispatched_swap_value_cents";
const BALANCER_REALISED_PRICE: &str = "balancer_realised_swap_price_scaled";
const BALANCER_PRICE_IMPACT: &str = "balancer_swap_price_impact_bps";
const BALANCER_BATCH_LEGS: &str = "balancer_batch_legs";

const WITHDRAWER_OUTCOME: &str = "withdrawer_outcome_total";

const BAD_DEBT_OUTCOME: &str = "bad_debt_outcome_total";

/// Terminal outcome of a transaction submission attempt.
#[derive(Clone, Copy, Debug)]
pub enum TxSubmitOutcome {
    Ok,
    SimEmpty,
    SeqFetchFailed,
    RetryExhausted,
}

impl TxSubmitOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::SimEmpty => "sim_empty",
            Self::SeqFetchFailed => "seq_fetch_failed",
            Self::RetryExhausted => "retry_exhausted",
        }
    }

    pub fn record(self) {
        counter!(TX_SUBMITTED, OUTCOME => self.as_str()).increment(1);
    }
}

/// Outcome of waiting for a submitted transaction to confirm on-chain.
#[derive(Clone, Copy, Debug)]
pub enum TxConfirmOutcome {
    Confirmed,
    HashDecodeFailed,
    FailedOnChain,
    SubmissionTimeout,
    UnexpectedStatus,
    TransportError,
}

impl TxConfirmOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::HashDecodeFailed => "hash_decode_failed",
            Self::FailedOnChain => "failed_on_chain",
            Self::SubmissionTimeout => "submission_timeout",
            Self::UnexpectedStatus => "unexpected_status",
            Self::TransportError => "transport_error",
        }
    }

    pub fn record(self) {
        counter!(TX_CONFIRMED, OUTCOME => self.as_str()).increment(1);
    }
}

/// Which contract entry point a simulation exercised.
#[derive(Clone, Copy, Debug)]
pub enum SimulationCall {
    Liquidate,
    Batch,
}

impl SimulationCall {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Liquidate => "liquidate",
            Self::Batch => "batch",
        }
    }
}

/// Verdict of a higher-level `liquidate` / `submit_requests_batch` simulation.
#[derive(Clone, Copy, Debug)]
pub enum SimulationOutcome {
    Ok,
    NotLiquidatable,
    Error,
    Failed,
}

impl SimulationOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotLiquidatable => "not_liquidatable",
            Self::Error => "error",
            Self::Failed => "failed",
        }
    }
}

/// Result label on the low-level RPC `simulate_transaction` latency histogram.
#[derive(Clone, Copy, Debug)]
pub enum SimulateOutcome {
    Ok,
    TransportError,
}

impl SimulateOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::TransportError => "transport_error",
        }
    }
}

/// Failure layer of an RPC `simulate_transaction` call.
#[derive(Clone, Copy, Debug)]
pub enum SimulateFailureKind {
    Transport,
    SimError,
}

impl SimulateFailureKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::SimError => "sim_error",
        }
    }
}

/// Verdict of a single market scan.
#[derive(Clone, Copy, Debug)]
pub enum ScanOutcome {
    Ok,
    NoMarketData,
    NoObligations,
}

impl ScanOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NoMarketData => "no_market_data",
            Self::NoObligations => "no_obligations",
        }
    }
}

/// Reason the liquidator declined to act on an opportunity.
#[derive(Clone, Copy, Debug)]
pub enum SkipReason {
    BelowCollateralThreshold,
    BalanceQueryFailed,
    UnprofitableSeizeZero,
    BatchSimFailed,
    FlashSwapShortfall,
    InsufficientBalanceAfterReservations,
    OpBuildFailed,
}

impl SkipReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BelowCollateralThreshold => "below_collateral_threshold",
            Self::BalanceQueryFailed => "balance_query_failed",
            Self::UnprofitableSeizeZero => "unprofitable_seize_zero",
            Self::BatchSimFailed => "batch_sim_failed",
            Self::FlashSwapShortfall => "flash_swap_shortfall",
            Self::InsufficientBalanceAfterReservations => "insufficient_balance_after_reservations",
            Self::OpBuildFailed => "op_build_failed",
        }
    }

    pub fn record(self) {
        counter!(SKIP, REASON => self.as_str()).increment(1);
    }
}

/// Execution mode of a dispatched liquidation plan.
#[derive(Clone, Copy, Debug)]
pub enum LiquidationKind {
    Direct,
    PreSwap,
    Flash,
}

impl LiquidationKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::PreSwap => "preswap",
            Self::Flash => "flash",
        }
    }
}

/// Per-liquidation-event verdict of the bad-debt initiator.
#[derive(Clone, Copy, Debug)]
pub enum BadDebtOutcome {
    EligibilityError,
    Ineligible,
    Dispatched,
    BuildFailed,
    DecodeOpError,
    ParseError,
    ObligationCleared,
}

impl BadDebtOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::EligibilityError => "eligibility_error",
            Self::Ineligible => "ineligible",
            Self::Dispatched => "dispatched",
            Self::BuildFailed => "build_failed",
            Self::DecodeOpError => "decode_op_error",
            Self::ParseError => "parse_error",
            Self::ObligationCleared => "obligation_cleared",
        }
    }

    pub fn record(self) {
        counter!(BAD_DEBT_OUTCOME, OUTCOME => self.as_str()).increment(1);
    }
}

/// Per-candidate verdict of the rebalancer.
#[derive(Clone, Copy, Debug)]
pub enum BalancerOutcome {
    EvaluationError,
    BadOraclePrice,
    NoViableProvider,
    BelowDust,
    ReservationLost,
    ThresholdHold,
    SellLegDispatched,
    BuyLegDispatched,
    OracleSpreadBreach,
    Dispatched,
}

impl BalancerOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BelowDust => "below_dust",
            Self::Dispatched => "dispatched",
            Self::ThresholdHold => "threshold_hold",
            Self::BadOraclePrice => "bad_oracle_price",
            Self::ReservationLost => "reservation_lost",
            Self::EvaluationError => "evaluation_error",
            Self::BuyLegDispatched => "buy_leg_dispatched",
            Self::NoViableProvider => "no_viable_provider",
            Self::SellLegDispatched => "sell_leg_dispatched",
            Self::OracleSpreadBreach => "oracle_price_spread_breach",
        }
    }

    pub fn record(self) {
        counter!(BALANCER_OUTCOME, OUTCOME => self.as_str()).increment(1);
    }
}

/// Per-position verdict of the withdrawer.
#[derive(Clone, Copy, Debug)]
pub enum WithdrawerOutcome {
    NoMarketData,
    PoolMissing,
    MaxWithdrawalError,
    PoolAtCapacity,
    ConversionError,
    EmptyPosition,
    Dispatched,
    BuildError,
    BelowThreshold,
}

impl WithdrawerOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NoMarketData => "no_market_data",
            Self::PoolMissing => "pool_missing",
            Self::MaxWithdrawalError => "max_withdrawal_error",
            Self::PoolAtCapacity => "pool_at_capacity",
            Self::ConversionError => "conversion_error",
            Self::EmptyPosition => "empty_position",
            Self::Dispatched => "dispatched",
            Self::BuildError => "build_error",
            Self::BelowThreshold => "below_threshold",
        }
    }

    pub fn record(self) {
        counter!(WITHDRAWER_OUTCOME, OUTCOME => self.as_str()).increment(1);
    }
}

/// Which cursor writer failed to persist to SQLite.
#[derive(Clone, Copy, Debug)]
pub enum CursorSource {
    LiquidatorEventCursor,
    EventCollectorCursor,
}

impl CursorSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LiquidatorEventCursor => "liquidator_event_cursor",
            Self::EventCollectorCursor => "event_collector_cursor",
        }
    }

    pub fn record(self) {
        counter!(CURSOR_SAVE_FAILURES, SOURCE => self.as_str()).increment(1);
    }
}

pub fn record_tx_bad_seq_retry() {
    counter!(TX_BAD_SEQ_RETRIES).increment(1);
}

pub fn record_collector_lag(collector: &'static str, dropped: u64) {
    counter!(COLLECTOR_LAGGED, COLLECTOR => collector).increment(dropped);
}

pub fn record_simulation(call: SimulationCall, outcome: SimulationOutcome) {
    counter!(SIMULATION, CALL => call.as_str(), OUTCOME => outcome.as_str()).increment(1);
}

pub fn record_rpc_simulate_duration(function: &str, outcome: SimulateOutcome, secs: f64) {
    histogram!(RPC_SIMULATE_DURATION, FUNCTION => function.to_string(), OUTCOME => outcome.as_str())
        .record(secs);
}

pub fn record_rpc_simulate_failure(function: &str, kind: SimulateFailureKind) {
    counter!(RPC_SIMULATE_FAILURES, FUNCTION => function.to_string(), KIND => kind.as_str())
        .increment(1);
}

/// Records a market scan's outcome + latency and, on success, advances the
/// last-successful-scan gauge. Every outcome marks a readiness tick so `/readyz`
/// tracks scan-loop liveness independently of whether a market had obligations.
pub fn record_scan(market: &str, outcome: ScanOutcome, duration_secs: f64) {
    counter!(SCAN_COMPLETED, MARKET => market.to_string(), OUTCOME => outcome.as_str())
        .increment(1);
    histogram!(SCAN_DURATION, MARKET => market.to_string(), OUTCOME => outcome.as_str())
        .record(duration_secs);

    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        let secs = now.as_secs() as i64;
        super::mark_tick(secs);
        if matches!(outcome, ScanOutcome::Ok) {
            gauge!(LAST_SCAN_TS, MARKET => market.to_string()).set(secs as f64);
        }
    }
}

pub fn set_scan_counts(market: &str, checked: u64, liquidatable: u64) {
    gauge!(OBLIGATIONS, MARKET => market.to_string()).set(checked as f64);
    gauge!(LIQUIDATABLE, MARKET => market.to_string()).set(liquidatable as f64);
}

pub fn record_plan_dispatched(market: &str, kind: LiquidationKind) {
    counter!(PLANS_DISPATCHED, MARKET => market.to_string(), TYPE => kind.as_str()).increment(1);
}

/// Modelled net profit of a dispatched plan (oracle-price units), recorded both
/// as a distribution and a running total for hourly run-rate panels.
pub fn record_expected_profit(market: &str, net_value: i128) {
    let v = net_value.max(0);
    histogram!(EXPECTED_PROFIT, MARKET => market.to_string()).record(v as f64);
    counter!(EXPECTED_PROFIT_TOTAL, MARKET => market.to_string()).increment(v as u64);
}

/// Realised net profit, booked only once the liquidation tx confirms on-chain.
pub fn record_realised_profit(market: &str, net_value: i128) {
    let v = net_value.max(0);
    histogram!(REALISED_PROFIT, MARKET => market.to_string()).record(v as f64);
    counter!(REALISED_PROFIT_TOTAL, MARKET => market.to_string()).increment(v as u64);
}

pub fn set_asset_balance(token_address: &str, amount: i128) {
    gauge!(ASSET_BALANCE, TOKEN_ADDRESS => token_address.to_string()).set(amount as f64);
}

pub fn set_xlm_balance(stroops: i128) {
    gauge!(XLM_BALANCE).set(stroops as f64);
}

pub fn set_xlm_safety_margin(stroops: i128) {
    gauge!(XLM_SAFETY_MARGIN).set(stroops as f64);
}

/// Keeper's own supply-side position in a pool (jTokens, plain collateral, and
/// the jToken value in underlying).
pub fn set_keeper_deposit(
    market: &str,
    pool_address: &str,
    token_symbol: &str,
    j_tokens: i128,
    plain_collateral: i128,
    j_tokens_underlying: i128,
) {
    let labels = [
        (MARKET, market.to_string()),
        (POOL_ADDRESS, pool_address.to_string()),
        (TOKEN_SYMBOL, token_symbol.to_string()),
    ];
    gauge!(SELF_J_TOKENS, &labels).set(j_tokens as f64);
    gauge!(SELF_PLAIN_COLLATERAL, &labels).set(plain_collateral as f64);
    gauge!(SELF_J_TOKENS_UNDERLYING, &labels).set(j_tokens_underlying as f64);
}

/// Keeper's own borrow-side position in a pool (dTokens and their underlying).
pub fn set_keeper_borrow(
    market: &str,
    pool_address: &str,
    token_symbol: &str,
    d_tokens: i128,
    d_tokens_underlying: i128,
) {
    let labels = [
        (MARKET, market.to_string()),
        (POOL_ADDRESS, pool_address.to_string()),
        (TOKEN_SYMBOL, token_symbol.to_string()),
    ];
    gauge!(SELF_D_TOKENS, &labels).set(d_tokens as f64);
    gauge!(SELF_D_TOKENS_UNDERLYING, &labels).set(d_tokens_underlying as f64);
}

pub fn record_balancer_dispatched_value(value_cents: i128) {
    histogram!(BALANCER_DISPATCHED_VALUE).record(value_cents.max(0) as f64);
}

pub fn record_balancer_realised_price(asset: &str, price_scaled: i128) {
    histogram!(BALANCER_REALISED_PRICE, ASSET => asset.to_string()).record(price_scaled as f64);
}

pub fn record_balancer_price_impact(asset: &str, admitted: bool, bps: i128) {
    let admitted = if admitted { "true" } else { "false" };
    histogram!(BALANCER_PRICE_IMPACT, ASSET => asset.to_string(), ADMITTED => admitted)
        .record(bps as f64);
}

/// Records the number of swap legs packed into a dispatched rebalance batch.
pub fn record_balancer_batch_legs(legs: usize) {
    histogram!(BALANCER_BATCH_LEGS).record(legs as f64);
}

/// Emits the constant `keeper_build_info` identity series (value `1`, provenance
/// in labels) and stamps `keeper_start_time_seconds` for uptime panels.
pub fn emit_build_info(start_unix_secs: f64) {
    gauge!(BUILD_INFO, "version" => VERSION, "commit" => GIT_SHA).set(1.0);
    gauge!(START_TIME).set(start_unix_secs);
}

/// Registers `# HELP` / `# TYPE` / unit metadata for every series. Called once
/// right after the recorder is installed; `describe_*` binds by name, so this
/// also documents the engine-owned series.
pub(super) fn describe_all() {
    describe_counter!(
        STRATEGY_LAGGED,
        "Events dropped because the strategy stage could not keep up with the broadcast channel."
    );
    describe_counter!(
        EXECUTOR_LAGGED,
        "Actions dropped because the executor stage could not keep up with the broadcast channel."
    );
    describe_counter!(
        COLLECTOR_LAGGED,
        "Events a collector dropped on its outbound channel to the reactor, by collector."
    );
    describe_counter!(
        CURSOR_SAVE_FAILURES,
        "Soroban event-cursor writes to SQLite that failed, by source."
    );
    describe_counter!(
        TX_BAD_SEQ_RETRIES,
        "Transaction retries triggered by a stale local sequence number."
    );
    describe_counter!(TX_SUBMITTED, "Transaction submission attempts by terminal outcome.");
    describe_counter!(TX_CONFIRMED, "Submitted transactions by on-chain confirmation outcome.");
    describe_counter!(SIMULATION, "Higher-level contract-call simulations by call and verdict.");
    describe_counter!(
        RPC_SIMULATE_FAILURES,
        "Failed RPC simulate_transaction calls by contract function and failure layer."
    );
    describe_histogram!(
        RPC_SIMULATE_DURATION,
        Unit::Seconds,
        "RPC simulate_transaction roundtrip latency by function and outcome."
    );
    describe_gauge!(
        BUILD_INFO,
        "Build provenance identity series (always 1); version and commit in labels."
    );
    describe_gauge!(
        START_TIME,
        Unit::Seconds,
        "Unix time the process started; subtract from time() for uptime."
    );

    describe_counter!(SCAN_COMPLETED, "Completed market scans by market and verdict.");
    describe_histogram!(
        SCAN_DURATION,
        Unit::Seconds,
        "Wall-clock time to evaluate one market end-to-end, by market and verdict."
    );
    describe_counter!(
        SKIP,
        "Liquidation opportunities the strategy declined to act on, by reason."
    );
    describe_counter!(
        PLANS_DISPATCHED,
        "Liquidation plans handed to the executor, by market and execution mode."
    );
    describe_gauge!(OBLIGATIONS, "Borrower obligations modelled per market.");
    describe_gauge!(LIQUIDATABLE, "Obligations currently flagged liquidatable per market.");
    describe_gauge!(
        LAST_SCAN_TS,
        Unit::Seconds,
        "Unix time of the last successful scan per market; a liveness gauge."
    );
    describe_histogram!(
        EXPECTED_PROFIT,
        "Modelled net profit per dispatched plan in oracle-price units, by market."
    );
    describe_counter!(
        EXPECTED_PROFIT_TOTAL,
        "Running total of modelled net profit in oracle-price units, by market."
    );
    describe_histogram!(
        REALISED_PROFIT,
        "Net profit of on-chain-confirmed plans in oracle-price units, by market."
    );
    describe_counter!(
        REALISED_PROFIT_TOTAL,
        "Running total of realised net profit in oracle-price units, by market."
    );
    describe_gauge!(ASSET_BALANCE, "On-chain wallet balance held by the keeper, by token address.");
    describe_gauge!(XLM_BALANCE, "Keeper XLM wallet balance in stroops.");
    describe_gauge!(XLM_SAFETY_MARGIN, "Configured XLM safety-margin floor in stroops.");
    describe_gauge!(SELF_J_TOKENS, "Keeper's own jToken supply, by pool.");
    describe_gauge!(SELF_PLAIN_COLLATERAL, "Keeper's own plain collateral, by pool.");
    describe_gauge!(
        SELF_J_TOKENS_UNDERLYING,
        "Keeper's own jToken supply valued in underlying, by pool."
    );
    describe_gauge!(SELF_D_TOKENS, "Keeper's own dToken borrow, by pool.");
    describe_gauge!(
        SELF_D_TOKENS_UNDERLYING,
        "Keeper's own dToken borrow valued in underlying, by pool."
    );

    describe_counter!(BALANCER_OUTCOME, "Rebalancer per-candidate verdicts.");
    describe_histogram!(
        BALANCER_DISPATCHED_VALUE,
        "USD-cent value of swaps the rebalancer actually emitted."
    );
    describe_histogram!(
        BALANCER_REALISED_PRICE,
        "Realised execution price of a winning swap route (oracle-scaled), by asset."
    );
    describe_histogram!(
        BALANCER_PRICE_IMPACT,
        "Oracle-vs-DEX price impact per probe in bps, by asset and admission verdict."
    );
    describe_histogram!(
        BALANCER_BATCH_LEGS,
        "Number of swap legs packed into a dispatched rebalance batch."
    );

    describe_counter!(WITHDRAWER_OUTCOME, "Withdrawer per-position verdicts.");
    describe_counter!(BAD_DEBT_OUTCOME, "Bad-debt initiator per-liquidation-event verdicts.");
}

#[cfg(test)]
mod tests {
    use super::*;

    // Characterization tests: these strings are the wire contract the Grafana
    // dashboard and rules.yml query. Any drift here is a breaking change and
    // must be mirrored in `deploy/grafana` + `deploy/prometheus`.

    #[test]
    fn metric_names_are_stable() {
        assert_eq!(TX_SUBMITTED, "keeper_tx_submitted_total");
        assert_eq!(TX_CONFIRMED, "keeper_tx_confirmed_total");
        assert_eq!(SIMULATION, "keeper_simulation_total");
        assert_eq!(RPC_SIMULATE_DURATION, "keeper_rpc_simulate_duration_seconds");
        assert_eq!(SCAN_COMPLETED, "liquidator_scan_completed_total");
        assert_eq!(SCAN_DURATION, "liquidator_market_scan_duration_seconds");
        assert_eq!(SKIP, "liquidator_skip_total");
        assert_eq!(PLANS_DISPATCHED, "liquidator_liquidation_plans_dispatched_total");
        assert_eq!(EXPECTED_PROFIT, "liquidator_plan_expected_net_profit_oracle_units");
        assert_eq!(REALISED_PROFIT, "liquidator_plan_realised_net_profit_oracle_units");
        assert_eq!(BALANCER_DISPATCHED_VALUE, "balancer_dispatched_swap_value_cents");
        assert_eq!(WITHDRAWER_OUTCOME, "withdrawer_outcome_total");
        assert_eq!(BAD_DEBT_OUTCOME, "bad_debt_outcome_total");
        assert_eq!(BUILD_INFO, "keeper_build_info");
        assert_eq!(START_TIME, "keeper_start_time_seconds");
    }

    #[test]
    fn tx_submit_outcome_labels_are_stable() {
        assert_eq!(TxSubmitOutcome::Ok.as_str(), "ok");
        assert_eq!(TxSubmitOutcome::SimEmpty.as_str(), "sim_empty");
        assert_eq!(TxSubmitOutcome::SeqFetchFailed.as_str(), "seq_fetch_failed");
        assert_eq!(TxSubmitOutcome::RetryExhausted.as_str(), "retry_exhausted");
    }

    #[test]
    fn tx_confirm_outcome_labels_are_stable() {
        assert_eq!(TxConfirmOutcome::Confirmed.as_str(), "confirmed");
        assert_eq!(TxConfirmOutcome::HashDecodeFailed.as_str(), "hash_decode_failed");
        assert_eq!(TxConfirmOutcome::FailedOnChain.as_str(), "failed_on_chain");
        assert_eq!(TxConfirmOutcome::SubmissionTimeout.as_str(), "submission_timeout");
        assert_eq!(TxConfirmOutcome::UnexpectedStatus.as_str(), "unexpected_status");
        assert_eq!(TxConfirmOutcome::TransportError.as_str(), "transport_error");
    }

    #[test]
    fn simulation_labels_are_stable() {
        assert_eq!(SimulationCall::Liquidate.as_str(), "liquidate");
        assert_eq!(SimulationCall::Batch.as_str(), "batch");
        assert_eq!(SimulationOutcome::Ok.as_str(), "ok");
        assert_eq!(SimulationOutcome::NotLiquidatable.as_str(), "not_liquidatable");
        assert_eq!(SimulationOutcome::Error.as_str(), "error");
        assert_eq!(SimulationOutcome::Failed.as_str(), "failed");
        assert_eq!(SimulateOutcome::Ok.as_str(), "ok");
        assert_eq!(SimulateOutcome::TransportError.as_str(), "transport_error");
        assert_eq!(SimulateFailureKind::Transport.as_str(), "transport");
        assert_eq!(SimulateFailureKind::SimError.as_str(), "sim_error");
    }

    #[test]
    fn scan_outcome_labels_are_stable() {
        assert_eq!(ScanOutcome::Ok.as_str(), "ok");
        assert_eq!(ScanOutcome::NoMarketData.as_str(), "no_market_data");
        assert_eq!(ScanOutcome::NoObligations.as_str(), "no_obligations");
    }

    #[test]
    fn skip_reason_labels_are_stable() {
        assert_eq!(SkipReason::BelowCollateralThreshold.as_str(), "below_collateral_threshold");
        assert_eq!(SkipReason::BalanceQueryFailed.as_str(), "balance_query_failed");
        assert_eq!(SkipReason::UnprofitableSeizeZero.as_str(), "unprofitable_seize_zero");
        assert_eq!(SkipReason::BatchSimFailed.as_str(), "batch_sim_failed");
        assert_eq!(SkipReason::FlashSwapShortfall.as_str(), "flash_swap_shortfall");
        assert_eq!(
            SkipReason::InsufficientBalanceAfterReservations.as_str(),
            "insufficient_balance_after_reservations"
        );
        assert_eq!(SkipReason::OpBuildFailed.as_str(), "op_build_failed");
    }

    #[test]
    fn liquidation_kind_labels_are_stable() {
        assert_eq!(LiquidationKind::Direct.as_str(), "direct");
        assert_eq!(LiquidationKind::PreSwap.as_str(), "preswap");
        assert_eq!(LiquidationKind::Flash.as_str(), "flash");
    }

    #[test]
    fn bad_debt_outcome_labels_are_stable() {
        assert_eq!(BadDebtOutcome::EligibilityError.as_str(), "eligibility_error");
        assert_eq!(BadDebtOutcome::Ineligible.as_str(), "ineligible");
        assert_eq!(BadDebtOutcome::Dispatched.as_str(), "dispatched");
        assert_eq!(BadDebtOutcome::BuildFailed.as_str(), "build_failed");
        assert_eq!(BadDebtOutcome::DecodeOpError.as_str(), "decode_op_error");
        assert_eq!(BadDebtOutcome::ParseError.as_str(), "parse_error");
        assert_eq!(BadDebtOutcome::ObligationCleared.as_str(), "obligation_cleared");
    }

    #[test]
    fn balancer_outcome_labels_are_stable() {
        assert_eq!(BalancerOutcome::EvaluationError.as_str(), "evaluation_error");
        assert_eq!(BalancerOutcome::BadOraclePrice.as_str(), "bad_oracle_price");
        assert_eq!(BalancerOutcome::NoViableProvider.as_str(), "no_viable_provider");
        assert_eq!(BalancerOutcome::BelowDust.as_str(), "below_dust");
        assert_eq!(BalancerOutcome::ReservationLost.as_str(), "reservation_lost");
        assert_eq!(BalancerOutcome::ThresholdHold.as_str(), "threshold_hold");
        assert_eq!(BalancerOutcome::SellLegDispatched.as_str(), "sell_leg_dispatched");
        assert_eq!(BalancerOutcome::BuyLegDispatched.as_str(), "buy_leg_dispatched");
        assert_eq!(BalancerOutcome::Dispatched.as_str(), "dispatched");
    }

    #[test]
    fn withdrawer_outcome_labels_are_stable() {
        assert_eq!(WithdrawerOutcome::NoMarketData.as_str(), "no_market_data");
        assert_eq!(WithdrawerOutcome::PoolMissing.as_str(), "pool_missing");
        assert_eq!(WithdrawerOutcome::MaxWithdrawalError.as_str(), "max_withdrawal_error");
        assert_eq!(WithdrawerOutcome::PoolAtCapacity.as_str(), "pool_at_capacity");
        assert_eq!(WithdrawerOutcome::ConversionError.as_str(), "conversion_error");
        assert_eq!(WithdrawerOutcome::EmptyPosition.as_str(), "empty_position");
        assert_eq!(WithdrawerOutcome::Dispatched.as_str(), "dispatched");
        assert_eq!(WithdrawerOutcome::BuildError.as_str(), "build_error");
        assert_eq!(WithdrawerOutcome::BelowThreshold.as_str(), "below_threshold");
    }

    #[test]
    fn cursor_source_labels_are_stable() {
        assert_eq!(CursorSource::LiquidatorEventCursor.as_str(), "liquidator_event_cursor");
        assert_eq!(CursorSource::EventCollectorCursor.as_str(), "event_collector_cursor");
    }

    #[test]
    fn describe_all_runs_without_a_recorder() {
        // Without an installed recorder the describe_* macros are no-ops; this
        // guards against a malformed name/unit arg regressing into a panic.
        describe_all();
    }

    #[test]
    fn rendered_exposition_carries_metadata_and_exact_wire_format() {
        // End-to-end proof against the real Prometheus exporter: the typed
        // emitters must produce the exact `# HELP` / `# TYPE` lines and the
        // exact `name{label="value"}` series the dashboard + rules query. A
        // scoped local recorder keeps this hermetic (no global install).
        use metrics_exporter_prometheus::PrometheusBuilder;

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            describe_all();
            emit_build_info(1_700_000_000.0);
            TxSubmitOutcome::Ok.record();
            SkipReason::UnprofitableSeizeZero.record();
            record_simulation(SimulationCall::Batch, SimulationOutcome::Failed);
            record_scan("MARKET_A", ScanOutcome::NoObligations, 0.01);
        });
        let out = handle.render();

        assert!(out.contains("# HELP keeper_tx_submitted_total"), "missing HELP metadata:\n{out}");
        assert!(
            out.contains("# TYPE keeper_tx_submitted_total counter"),
            "missing TYPE metadata:\n{out}"
        );
        assert!(
            out.contains("keeper_tx_submitted_total{outcome=\"ok\"} 1"),
            "wrong tx-submitted wire format:\n{out}"
        );
        assert!(
            out.contains("liquidator_skip_total{reason=\"unprofitable_seize_zero\"} 1"),
            "wrong skip wire format:\n{out}"
        );
        assert!(
            out.contains("keeper_simulation_total{call=\"batch\",outcome=\"failed\"} 1"),
            "wrong simulation wire format:\n{out}"
        );
        assert!(
            out.contains("keeper_build_info{") && out.contains("version=\""),
            "missing build_info series:\n{out}"
        );
    }
}
