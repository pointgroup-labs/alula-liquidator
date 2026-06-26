//! Profitability and flash-loan helpers.

use crate::lending_model::{
    BPS_FACTOR, Underlying,
    error::{LMError, MapArithmeticError},
    pool::PoolData,
};
use tracing::error;

pub fn compute_flash_fee(
    amount: Underlying,
    flash_loan_fee_bps: i128,
) -> Result<Underlying, LMError> {
    amount.scale_with_bps_ceil(flash_loan_fee_bps)
}

/// Profit margin expressed in borrow-token units (signed).
///
/// `min_profit_margin_cents` is converted to oracle value
/// (`cents * 10^oracle_decimals / 100`) and then to borrow tokens
/// (`value * 10^borrow_decimals / borrow_oracle_price`).
///
/// The margin is **signed**. A positive value is the usual profit cushion the
/// repay cap subtracts. A negative value is a deliberate, bounded loss budget:
/// an operator running the keeper for protocol safety may accept liquidations
/// that lose money. Because `compute_repay_cap_from_collateral` *subtracts*
/// this slack, a negative margin *widens* the repay cap (subtracting a
/// negative adds magnitude), letting the keeper repay more than a strictly
/// break-even amount.
///
/// The `_ceil` helpers only accept non-negative inputs, so the magnitude is
/// computed on `|cents|` and the sign is restored afterwards. Returns
/// `Underlying::ZERO` for a zero margin (no slack).
pub fn compute_profit_margin_in_borrow_token(
    min_profit_margin_cents: i128,
    oracle_price_decimals: u32,
    borrow_pool: &PoolData,
) -> Result<Underlying, LMError> {
    if !borrow_pool.oracle_asset_price.is_positive() {
        return Err(LMError::InternalError);
    }
    if min_profit_margin_cents == 0 {
        return Ok(Underlying::ZERO);
    }

    // `i128::MIN.abs()` overflows; reject it explicitly rather than panic.
    let magnitude_cents = min_profit_margin_cents
        .checked_abs()
        .map_over_or_underflow()?;

    let margin_value = cents_to_oracle_value_ceil(magnitude_cents, oracle_price_decimals)?;

    let magnitude = value_to_underlying_asset_amount_ceil(
        margin_value,
        borrow_pool.oracle_asset_price,
        borrow_pool.token_decimals,
    )?;

    if min_profit_margin_cents.is_negative() {
        Ok(Underlying(magnitude.0.checked_neg().map_over_or_underflow()?))
    } else {
        Ok(magnitude)
    }
}

pub fn compute_repay_cap_from_collateral(
    is_solvent: bool,
    borrow_pool: &PoolData,
    max_feasible_repay: i128,
    collateral_pool: &PoolData,
    available_collateral: i128,
    obligation_debt_value: i128,
    profit_margin_borrow_tokens: i128,
    obligation_collateral_value: i128,
) -> Option<i128> {
    if available_collateral <= 0 {
        return None;
    }
    let (borrow_price, collateral_price) = (
        borrow_pool.oracle_asset_price,
        collateral_pool.oracle_asset_price,
    );
    if borrow_price <= 0 || collateral_price <= 0 {
        return None;
    }

    let pool_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);

    let effective_incentive_bps = if is_solvent {
        let max_allowed_multiplier = obligation_collateral_value
            .checked_mul(9990)?
            .checked_div(obligation_debt_value)?;

        let max_ltv_incentive_bps = max_allowed_multiplier.saturating_sub(10000);

        pool_incentive_bps.min(max_ltv_incentive_bps)
    } else {
        pool_incentive_bps
    };

    let incentive_bps_factor = BPS_FACTOR.checked_add(effective_incentive_bps as i128)?;
    if incentive_bps_factor <= 0 {
        return None;
    }

    let (borrow_decimals_pow, collateral_decimals_pow) = (
        10_i128.pow(borrow_pool.token_decimals),
        10_i128.pow(collateral_pool.token_decimals),
    );

    let all_collateral_value = available_collateral
        .checked_mul(collateral_price)?
        .checked_div(collateral_decimals_pow)?;

    let numerator = all_collateral_value.checked_mul(borrow_decimals_pow)?;
    let denominator = borrow_price
        .checked_mul(incentive_bps_factor)?
        .checked_div(BPS_FACTOR)?;

    let max_repay_borrow_units = numerator.checked_div(denominator)?;

    let max_profitable_repay = max_repay_borrow_units.saturating_sub(profit_margin_borrow_tokens);

    let result = max_feasible_repay.min(max_profitable_repay);

    if result > 0 { Some(result) } else { None }
}

/// Largest repay (in borrow tokens) such that the bonus-inflated collateral
/// seizure still fits in `available_collateral`, minus a profit-margin slack.
///
/// The incentive is `min(borrow_pool, collateral_pool)` — the same pair-wise
/// minimum the contract's `liquidate` applies (and that
/// `compute_expected_seized_collateral` mirrors). Using only the collateral
/// pool's incentive would over-estimate the bonus whenever the borrow pool's
/// is smaller, under-sizing the repay cap.
///
/// Returns `min(max_feasible_repay, R_max − profit_margin)`, or `None` if any
/// factor collapses (zero/negative prices or collateral, arithmetic overflow,
/// or the cap falls to zero).
pub fn compute_repay_cap_from_collateral4(
    max_feasible_repay: i128,
    available_collateral: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    profit_margin_borrow_tokens: i128,
) -> Option<i128> {
    if available_collateral <= 0 {
        return None;
    }
    let (borrow_price, collateral_price) = (
        borrow_pool.oracle_asset_price,
        collateral_pool.oracle_asset_price,
    );
    if borrow_price <= 0 || collateral_price <= 0 {
        return None;
    }

    let (borrow_decimals_pow, collateral_decimals_pow) = (
        10_i128.pow(borrow_pool.token_decimals),
        10_i128.pow(collateral_pool.token_decimals),
    );

    let liquidation_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);
    let incentive_bps = BPS_FACTOR.checked_add(liquidation_incentive_bps)?;
    if incentive_bps <= 0 {
        return None;
    }

    let all_collateral_value = available_collateral
        .checked_mul(collateral_price)?
        .checked_div(collateral_decimals_pow)?;

    let numerator = all_collateral_value.checked_mul(borrow_decimals_pow)?;
    let denominator = borrow_price
        .checked_mul(incentive_bps)?
        .checked_div(BPS_FACTOR)?;

    let max_repay_borrow_units = numerator.checked_div(denominator)?;
    let max_profitable_repay = max_repay_borrow_units.saturating_sub(profit_margin_borrow_tokens);

    let result = max_feasible_repay.min(max_profitable_repay);

    if result > 0 { Some(result) } else { None }
}

/// Largest repay (in borrow tokens) such that the bonus-inflated collateral
/// seizure still fits in `available_collateral`, minus a profit-margin slack.
///
/// The incentive is `min(borrow_pool, collateral_pool)` — the same pair-wise
/// minimum the contract's `liquidate` applies (and that
/// `compute_expected_seized_collateral` mirrors). Using only the collateral
/// pool's incentive would over-estimate the bonus whenever the borrow pool's
/// is smaller, under-sizing the repay cap.
///
/// Returns `min(max_feasible_repay, R_max − profit_margin)`, or `None` if any
/// factor collapses (zero/negative prices or collateral, arithmetic overflow,
/// or the cap falls to zero).
pub fn compute_repay_cap_from_collateral3(
    max_feasible_repay: i128,
    available_collateral: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    profit_margin_borrow_tokens: i128,
    // TODO: Account for solvent cases here separately?
) -> Option<i128> {
    if available_collateral <= 0 {
        return None;
    }
    if borrow_pool.oracle_asset_price <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return None;
    }

    let liquidation_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);
    let incentive_bps = BPS_FACTOR.checked_add(liquidation_incentive_bps)?;
    if incentive_bps <= 0 {
        return None;
    }

    // max_theoretical_repay (in collateral units) = collateral / (1 + incentive)
    // = collateral * 10_000 / (10_000 + incentive_bps)   (integer floor)
    let max_theoretical_repay_collateral_units = available_collateral
        .checked_mul(BPS_FACTOR)?
        .checked_div(incentive_bps)?;

    // Convert collateral units to borrow units using oracle prices and decimals.
    //   borrow_units = collateral_units
    //                * collateral_oracle_price * 10^borrow_decimals
    //                / (borrow_oracle_price * 10^collateral_decimals)
    let borrow_decimals_pow = 10_i128.pow(borrow_pool.token_decimals);
    let collateral_decimals_pow = 10_i128.pow(collateral_pool.token_decimals);

    let numerator = max_theoretical_repay_collateral_units
        .checked_mul(collateral_pool.oracle_asset_price)?
        .checked_mul(borrow_decimals_pow)?;
    let denominator = borrow_pool
        .oracle_asset_price
        .checked_mul(collateral_decimals_pow)?;
    if denominator <= 0 {
        return None;
    }

    let max_repay_borrow_units = numerator / denominator; // floor → biases under cap
    let max_profitable_repay = max_repay_borrow_units.saturating_sub(profit_margin_borrow_tokens); // TODO: Should we do this?

    let result = max_feasible_repay.min(max_profitable_repay);
    if result > 0 { Some(result) } else { None }
}

pub struct LiquidationProfitability {
    pub net_value: i128,
    pub gain_value: i128,
    pub is_profitable: bool,
    pub required_value: i128,
}

pub fn compute_liquidation_profitability(
    gain_value: i128,
    cost_value: i128,
    min_profit_margin_value: i128,
) -> Result<LiquidationProfitability, LMError> {
    if !gain_value.is_positive() || !cost_value.is_positive() {
        return Err(LMError::InternalError);
    }

    let required_value = cost_value
        .checked_add(min_profit_margin_value)
        .map_over_or_underflow()?;
    let net_value = gain_value.saturating_sub(required_value);

    Ok(LiquidationProfitability {
        net_value,
        gain_value,
        required_value,
        is_profitable: net_value > 0,
    })
}

pub fn cents_to_oracle_value_ceil(
    cents: i128,
    oracle_price_decimals: u32,
) -> Result<i128, LMError> {
    if cents < 0 {
        return Err(LMError::InternalError);
    }

    let multiplier = 10_i128
        .checked_pow(oracle_price_decimals)
        .map_over_or_underflow()?;

    cents
        .checked_mul(multiplier)
        .map_over_or_underflow()?
        .checked_add(99)
        .map_over_or_underflow()?
        .checked_div(100)
        .map_over_or_underflow()
}

pub fn value_to_underlying_asset_amount_ceil(
    value: i128,
    oracle_asset_price: i128,
    token_decimals: u32,
) -> Result<Underlying, LMError> {
    if value.is_negative() {
        error!(value, "negative value passed to underlying conversion");

        return Err(LMError::InternalError);
    }
    if !oracle_asset_price.is_positive() {
        error!(oracle_asset_price, "non-positive oracle asset price");

        return Err(LMError::InternalError);
    }

    let multiplier = 10_i128
        .checked_pow(token_decimals)
        .map_over_or_underflow()?;
    let numerator = value
        .checked_mul(multiplier)
        .map_over_or_underflow()?
        .checked_add(oracle_asset_price - 1)
        .map_over_or_underflow()?;
    let denominator = oracle_asset_price;

    Ok(Underlying(
        numerator.checked_div(denominator).map_over_or_underflow()?,
    ))
}
// #[cfg(test)]
// mod tests {
//     use super::*;

//     // Non-proptest unit cases that pin specific numbers from the keeper's old
//     // inline math, so we know the new helper is a drop-in replacement at the
//     // historical default of 500 bps haircut.
//     #[test]
//     fn profitable_matches_legacy_5pct_buffer() -> Result<(), LMError> {
//         // gain * (10_000 - 500) / 10_000 = gain * 0.95
//         let gain = 1_000_000i128;
//         let cost = 940_000i128;
//         let r = compute_liquidation_profitability(gain, cost, 0, 500, 0)?;
//         assert_eq!(r.effective_gain_value, 950_000);
//         assert!(r.is_profitable);

//         // Same gain, cost just above effective_gain → rejected.
//         let r2 = compute_liquidation_profitability(gain, 951_000, 0, 500, 0)?;
//         assert!(!r2.is_profitable);

//         Ok(())
//     }

//     #[test]
//     fn profitable_inclusion_fee_tips_the_balance() -> Result<(), LMError> {
//         // Without fee: passes by 1 unit. With fee = 1: fails.
//         let gain = 1_000_000i128;
//         let cost = 950_000i128;
//         let r_no_fee = compute_liquidation_profitability(gain, cost - 1, 0, 500, 0)?;
//         assert!(r_no_fee.is_profitable);
//         let r_fee = compute_liquidation_profitability(gain, cost - 1, 0, 500, 2)?;
//         assert!(!r_fee.is_profitable);

//         Ok(())
//     }

//     #[test]
//     fn profitable_net_oracle_is_signed_difference() -> Result<(), LMError> {
//         // Positive net.
//         let r = compute_liquidation_profitability(1_000_000, 800_000, 0, 0, 0)?;
//         assert_eq!(r.net_value, 200_000);
//         assert!(r.is_profitable);

//         // Zero net: boundary.
//         let r = compute_liquidation_profitability(1_000_000, 1_000_000, 0, 0, 0)?;
//         assert_eq!(r.net_value, 0);
//         assert!(!r.is_profitable);

//         // Negative net: rejected and ranks below positives.
//         let r = compute_liquidation_profitability(1_000_000, 1_500_000, 0, 0, 0)?;
//         assert_eq!(r.net_value, -500_000);
//         assert!(!r.is_profitable);

//         Ok(())
//     }
// }
