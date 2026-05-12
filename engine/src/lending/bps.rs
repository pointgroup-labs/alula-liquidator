//! Basis-point constants and fixed-point multiplication helpers.
//!
//! All multiplications saturate at `i128::{MIN,MAX}` rather than wrapping.
//! With workspace-level `overflow-checks = true` enabled in release, any raw
//! `*`/`+`/`-` we missed elsewhere will trap; the saturating helpers here keep
//! pure-math paths defined even under pathological pool config (oracle prices
//! and decimals come from on-chain data we don't fully control).

pub const BPS_DENOMINATOR: i128 = 10_000;

/// 102% — used to inflate the position debt when computing the maximum
/// repayable amount, so that recently-accrued interest is also closed out.
pub const LIQUIDATION_INTEREST_BUFFER_BPS: i128 = 10_200;

pub fn fixed_mul_ceil(a: i128, b_bps: i128) -> i128 {
    let prod = a.saturating_mul(b_bps);
    prod.saturating_add(BPS_DENOMINATOR - 1)
        .saturating_div(BPS_DENOMINATOR)
}

pub fn fixed_mul_floor(a: i128, b_bps: i128) -> i128 {
    a.saturating_mul(b_bps).saturating_div(BPS_DENOMINATOR)
}
