//! Structured view over the opaque errors returned by `stellar-rpc-client`.

/// Classified RPC / simulation failure surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SorobanRpcError {
    /// Contract aborted with a typed error code, parsed from
    /// `Error(Contract, #N)` in the rendered diagnostic.
    Contract { code: u32 },
    /// Sequence number out of sync (`tx_bad_seq` family).
    BadSequence,
    /// `simulate_transaction` returned an empty `results` list, which we emit
    /// from [`crate::stellar::client`] when the host function produced no
    /// return value.
    NoSimulationResults,
    /// RPC rejected the cursor (cursor expired / out of retention).
    /// Terminal: the caller must reset to a fresh head ledger.
    TerminalCursor,
    /// Transaction made it to the network but ended in `FAILED` status —
    /// gas was spent, no seized amount realised. This is the realisation-tax
    /// tail, distinct from RPC transport errors during the confirm poll.
    TxFailedOnChain,
    /// `get_transaction_polling` exhausted its retry budget without seeing
    /// SUCCESS or FAILED. The tx may yet land; not a definitive verdict.
    SubmissionTimeout,
    /// RPC returned a status string that isn't SUCCESS / FAILED / NOT_FOUND.
    /// Either a protocol bump on the RPC or a serialization bug — surface
    /// it distinctly so it doesn't get filed as a transient transport error.
    UnexpectedStatus,
    /// Anything we did not recognise.
    Other,
}

/// Contract-error *variant names* raised by the lending market during
/// `liquidate` simulation that mean "this borrower cannot be liquidated right
/// now" rather than a genuine adapter/RPC failure. Used as a fallback when
/// the rendered error does not include a numeric `#N` code, or includes a
/// code that isn't in [`KNOWN_LIQUIDATION_FAILURE_CODES`].
///
/// Release builds of the contract usually strip debug strings, so the numeric
/// list below is the long-term home for this knowledge — the names just
/// preserve the previous behaviour until that list is populated.
const EXPECTED_LIQUIDATION_FAILURE_NAMES: &[&str] = &[
    "ObligationIsHealthy",
    "ObligationDoesNotExist",
    "InvalidLiquidationInputs",
    "BorrowPoolDoesNotExist",
    "CollateralPoolDoesNotExist",
];

/// Numeric `#[contracterror]` codes from the Alula lending-market contract
/// (`MCError`) that classify as "expected liquidation failure": the borrower
/// isn't eligible, the inputs are stale, or the pool was removed between
/// our state cache and the on-chain check.
///
/// Mirrors [`EXPECTED_LIQUIDATION_FAILURE_NAMES`]; both must be kept in sync
/// when extended. Numeric codes are the source of truth — names are only
/// there for the case where the contract is built with debug strings.
///
/// Keep this list strictly conservative. A code that doesn't belong here
/// will be surfaced as a real failure (warning + retry), which is what we
/// want for genuine bugs; a code that wrongly *does* belong here gets
/// silently swallowed, which we want to avoid.
const KNOWN_LIQUIDATION_FAILURE_CODES: &[u32] = &[
    105, // BorrowPoolDoesNotExist
    106, // CollateralPoolDoesNotExist
    200, // ObligationDoesNotExist
    600, // InvalidLiquidationInputs
    601, // ObligationIsHealthy
];

impl SorobanRpcError {
    /// Classify any `Display`-able error.
    pub fn classify<E: std::fmt::Display>(err: &E) -> Self {
        Self::classify_str(&format!("{err:#}"))
    }

    /// Classify a raw message. Exposed for tests and call sites that already
    /// hold a `String` (e.g. simulation responses that surface the error
    /// directly).
    pub fn classify_str(msg: &str) -> Self {
        if let Some(code) = extract_contract_code(msg) {
            return Self::Contract { code };
        }

        let lower = msg.to_lowercase();

        if lower.contains("tx_bad_seq") || lower.contains("bad_seq") || lower.contains("bad seq") {
            return Self::BadSequence;
        }
        if lower.contains("simulation returned no results") {
            return Self::NoSimulationResults;
        }
        if lower.contains("cursor not found")
            || lower.contains("invalid cursor")
            || lower.contains("invalid argument")
            || (lower.contains("ledger") && lower.contains("too old"))
        {
            return Self::TerminalCursor;
        }
        if lower.contains("transaction submission timeout") {
            return Self::SubmissionTimeout;
        }
        if lower.contains("expected transaction status") {
            return Self::UnexpectedStatus;
        }
        // Order matters: `transaction submission failed` is a substring of
        // some `transaction failed` renders too, so this stays after the
        // more specific status/timeout checks above and before `Other`.
        if lower.contains("transaction failed") || lower.contains("transaction submission failed") {
            return Self::TxFailedOnChain;
        }

        Self::Other
    }
}

/// Parse `Error(Contract, #N)` from a Soroban diagnostic string.
///
/// The rendered form is stable across `soroban-env-host` versions; the digits
/// follow the literal `#` marker. Returns `None` if the marker is absent or
/// the digits don't fit in a `u32`.
fn extract_contract_code(s: &str) -> Option<u32> {
    const MARKER: &str = "Error(Contract, #";
    let start = s.find(MARKER)? + MARKER.len();
    let digits: String = s[start..].chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

// Convenience predicates take `anyhow::Error` (what every call site holds)
// and route through `classify()`. They replace the `.contains("…")` checks
// previously scattered across the keeper.

/// `true` if the error means "this borrower is not liquidatable", i.e. one of
/// the lending-market preconditions failed.
///
/// Resolution order:
/// 1. A typed `Error(Contract, #N)` whose `N` is in
///    [`KNOWN_LIQUIDATION_FAILURE_CODES`] — strict, version-independent,
///    survives release builds that strip debug strings.
/// 2. Otherwise the rendered error is matched against
///    [`EXPECTED_LIQUIDATION_FAILURE_NAMES`] — safety net for debug-built
///    contracts and for diagnostics that surface the name without a `#N`
///    marker.
pub fn is_expected_liquidation_failure(err: &anyhow::Error) -> bool {
    if let SorobanRpcError::Contract { code } = SorobanRpcError::classify(err)
        && KNOWN_LIQUIDATION_FAILURE_CODES.contains(&code)
    {
        return true;
    }
    let rendered = format!("{err:#}");
    EXPECTED_LIQUIDATION_FAILURE_NAMES.iter().any(|n| rendered.contains(n))
}

/// Numeric `MCError` code for `ObligationDoesNotExist` (see the lending-market
/// `error.rs`). The contract's `get_user_obligation` aborts with this code when
/// the obligation is absent from storage.
const OBLIGATION_DOES_NOT_EXIST_CODE: u32 = 200;

pub fn is_obligation_does_not_exist(err: &anyhow::Error) -> bool {
    if let SorobanRpcError::Contract { code } = SorobanRpcError::classify(err) {
        return code == OBLIGATION_DOES_NOT_EXIST_CODE;
    }
    let rendered = format!("{err:#}");
    rendered.contains("ObligationDoesNotExist")
}

pub fn is_bad_seq_error(err: &anyhow::Error) -> bool {
    matches!(SorobanRpcError::classify(err), SorobanRpcError::BadSequence)
}

pub fn is_no_simulation_results_error(err: &anyhow::Error) -> bool {
    matches!(SorobanRpcError::classify(err), SorobanRpcError::NoSimulationResults)
}

pub fn is_terminal_cursor_error<E: std::fmt::Display>(err: &E) -> bool {
    matches!(SorobanRpcError::classify(err), SorobanRpcError::TerminalCursor)
}
