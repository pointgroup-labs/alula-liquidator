//! Basis-point constants and fixed-point multiplication helpers.

pub const BPS_DENOMINATOR: i128 = 10_000;
pub const LIQUIDATION_INTEREST_BUFFER_BPS: i128 = 10_200;

pub fn fixed_mul_ceil(a: i128, b_bps: i128) -> i128 {
    (a * b_bps + (BPS_DENOMINATOR - 1)) / BPS_DENOMINATOR
}

pub fn fixed_mul_floor(a: i128, b_bps: i128) -> i128 {
    a * b_bps / BPS_DENOMINATOR
}
