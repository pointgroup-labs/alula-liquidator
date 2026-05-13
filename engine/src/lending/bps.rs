//! Basis-point constants and saturating fixed-point multiplication helpers.
//! Saturating (not wrapping) so adversarial on-chain inputs — oracle prices,
//! decimals — can't flip signs and corrupt downstream math.

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
