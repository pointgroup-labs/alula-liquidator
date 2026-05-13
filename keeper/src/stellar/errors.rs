//! Structured view over the opaque errors returned by `stellar-rpc-client`.
//!
//! `stellar-rpc-client` v25 erases all RPC failures into `anyhow::Error`,
//! preserving their structure only in the rendered `Display` string. The rest
//! of the keeper used to deal with this by sprinkling `.contains("…")` checks
//! over the formatted error at every call site. That pattern is fragile:
//!
//! * the message format is upstream-controlled and can shift between client
//!   versions,
//! * each site uses a slightly different set of substrings,
//! * there is no test coverage for the matches.
//!
//! This module concentrates the parsing in one place. Call sites convert an
//! `anyhow::Error` (or a raw message) to `SorobanRpcError` via
//! [`SorobanRpcError::classify`] and then `match` on variants. The numeric
//! contract-error code is exposed as a `u32` so we can eventually match
//! against the contract's `#[contracterror]` enum directly, without going
//! through human-readable variant names.
//!
//! The substring-match against debug variant names is still present, but it
//! lives in a single named constant ([`EXPECTED_LIQUIDATION_FAILURE_NAMES`])
//! and only as a fallback when no `#N` code is rendered.

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
    let digits: String = s[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
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
    EXPECTED_LIQUIDATION_FAILURE_NAMES
        .iter()
        .any(|n| rendered.contains(n))
}

pub fn is_bad_seq_error(err: &anyhow::Error) -> bool {
    matches!(SorobanRpcError::classify(err), SorobanRpcError::BadSequence)
}

pub fn is_no_simulation_results_error(err: &anyhow::Error) -> bool {
    matches!(
        SorobanRpcError::classify(err),
        SorobanRpcError::NoSimulationResults
    )
}

pub fn is_terminal_cursor_error<E: std::fmt::Display>(err: &E) -> bool {
    matches!(
        SorobanRpcError::classify(err),
        SorobanRpcError::TerminalCursor
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_contract_code() {
        let e = SorobanRpcError::classify_str(
            "HostError: Error(Contract, #7) (transaction simulation failed)",
        );
        assert_eq!(e, SorobanRpcError::Contract { code: 7 });
    }

    #[test]
    fn classify_contract_code_large() {
        let e = SorobanRpcError::classify_str("Error(Contract, #4294967295)");
        assert_eq!(e, SorobanRpcError::Contract { code: u32::MAX });
    }

    #[test]
    fn classify_contract_code_overflow_falls_back() {
        // u32::MAX + 1
        let e = SorobanRpcError::classify_str("Error(Contract, #4294967296)");
        assert_eq!(e, SorobanRpcError::Other);
    }

    #[test]
    fn classify_bad_seq_variants() {
        for s in [
            "tx_bad_seq",
            "TX_BAD_SEQ detected",
            "bad_seq from RPC",
            "bad seq",
        ] {
            assert_eq!(
                SorobanRpcError::classify_str(s),
                SorobanRpcError::BadSequence,
                "input: {s}"
            );
        }
    }

    #[test]
    fn classify_no_simulation_results() {
        let e =
            SorobanRpcError::classify_str("simulation returned no results for get_user_obligation");
        assert_eq!(e, SorobanRpcError::NoSimulationResults);
    }

    #[test]
    fn classify_terminal_cursor() {
        for s in [
            "cursor not found",
            "Invalid cursor format",
            "invalid argument: cursor",
            "ledger 1234 is too old",
        ] {
            assert_eq!(
                SorobanRpcError::classify_str(s),
                SorobanRpcError::TerminalCursor,
                "input: {s}"
            );
        }
    }

    #[test]
    fn classify_other() {
        assert_eq!(
            SorobanRpcError::classify_str("nothing to see"),
            SorobanRpcError::Other
        );
        assert_eq!(SorobanRpcError::classify_str(""), SorobanRpcError::Other);
    }

    #[test]
    fn contract_code_takes_priority_over_substring() {
        // Even if the message also mentions "bad seq", a contract code wins.
        let e = SorobanRpcError::classify_str(
            "HostError: Error(Contract, #3) (something something bad seq elsewhere)",
        );
        assert_eq!(e, SorobanRpcError::Contract { code: 3 });
    }

    #[test]
    fn expected_liquidation_failure_matches_known_names() {
        for name in EXPECTED_LIQUIDATION_FAILURE_NAMES {
            let err = anyhow::anyhow!("HostError raised with {name} from contract");
            assert!(is_expected_liquidation_failure(&err), "name: {name}");
        }
    }

    #[test]
    fn expected_liquidation_failure_rejects_unknown() {
        let err = anyhow::anyhow!("HostError: SomethingElseWentWrong");
        assert!(!is_expected_liquidation_failure(&err));
    }

    #[test]
    fn expected_liquidation_failure_rejects_unrecognised_contract_code() {
        // 99 is in the unused 12..99 range of MCError's "core errors" block,
        // so it cannot collide with a real code today or after plausible
        // future additions. A numeric-only error with this code MUST classify
        // as unexpected — otherwise the keeper would silently swallow real
        // contract failures.
        let err = anyhow::anyhow!("HostError: Error(Contract, #99)");
        assert!(!is_expected_liquidation_failure(&err));
    }

    #[test]
    fn expected_liquidation_failure_accepts_every_known_code() {
        // Lock the numeric path: every code in KNOWN_LIQUIDATION_FAILURE_CODES
        // must classify as expected, even with no debug name in the message
        // (the release-build case the numeric path was added for).
        for &code in KNOWN_LIQUIDATION_FAILURE_CODES {
            let err = anyhow::anyhow!("HostError: Error(Contract, #{code})");
            assert!(
                is_expected_liquidation_failure(&err),
                "code {code} should classify as expected"
            );
        }
    }

    #[test]
    fn expected_liquidation_failure_with_code_and_name_uses_name_fallback() {
        // Realistic shape from a debug-built contract: numeric code AND the
        // debug variant name in the same diagnostic. Until the numeric list
        // is populated, the name list carries the classification.
        let err = anyhow::anyhow!("HostError: Error(Contract, #42) ObligationIsHealthy");
        assert!(is_expected_liquidation_failure(&err));
    }

    #[test]
    fn predicate_is_bad_seq_error() {
        assert!(is_bad_seq_error(&anyhow::anyhow!("tx_bad_seq detected")));
        assert!(!is_bad_seq_error(&anyhow::anyhow!("happy path")));
    }

    #[test]
    fn predicate_is_terminal_cursor_error_with_anyhow() {
        let err = anyhow::anyhow!("cursor not found");
        assert!(is_terminal_cursor_error(&err));
    }

    #[test]
    fn predicate_is_terminal_cursor_error_with_string() {
        let err = "invalid cursor".to_string();
        assert!(is_terminal_cursor_error(&err));
    }
}
