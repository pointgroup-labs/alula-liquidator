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
    let magnitude_cents = min_profit_margin_cents.checked_abs().m_ou()?;

    let margin_value = cents_to_oracle_value_ceil(magnitude_cents, oracle_price_decimals)?;

    let magnitude = value_to_underlying_asset_amount_ceil(
        margin_value,
        borrow_pool.oracle_asset_price,
        borrow_pool.token_decimals,
    )?;

    if min_profit_margin_cents.is_negative() {
        Ok(Underlying(magnitude.0.checked_neg().m_ou()?))
    } else {
        Ok(magnitude)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn compute_repay_cap_from_collateral(
    is_solvent: bool,
    borrow_pool: &PoolData,
    max_feasible_repay: i128,
    collateral_pool: &PoolData,
    available_collateral: i128,
    obligation_debt_value: i128,
    profit_margin_borrow_tokens: i128,
    obligation_collateral_value: i128,
) -> Result<i128, LMError> {
    if available_collateral <= 0 {
        error!(available_collateral, "non-positive available collateral");

        return Err(LMError::InternalError);
    }
    let (borrow_asset_price, collateral_asset_price) = (
        borrow_pool.oracle_asset_price,
        collateral_pool.oracle_asset_price,
    );
    if borrow_asset_price <= 0 || collateral_asset_price <= 0 {
        error!(
            borrow_asset_price,
            collateral_asset_price, "non-positive price(s)"
        );

        return Err(LMError::InternalError);
    }

    let pool_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);

    let effective_incentive_bps = if is_solvent {
        let max_allowed_multiplier = obligation_collateral_value
            .checked_mul(9990)
            .m_ou()?
            .checked_div(obligation_debt_value)
            .m_ou()?;

        let max_ltv_incentive_bps = max_allowed_multiplier.saturating_sub(10000);

        pool_incentive_bps.min(max_ltv_incentive_bps)
    } else {
        pool_incentive_bps
    };

    let incentive_bps_factor = BPS_FACTOR.checked_add(effective_incentive_bps).m_ou()?;
    if incentive_bps_factor <= 0 {
        error!(incentive_bps_factor, "non-positive incentive factor");

        return Err(LMError::InternalError);
    }

    let (borrow_decimals_pow, collateral_decimals_pow) = (
        10_i128.pow(borrow_pool.token_decimals),
        10_i128.pow(collateral_pool.token_decimals),
    );

    let all_collateral_value = available_collateral
        .checked_mul(collateral_asset_price)
        .m_ou()?
        .checked_div(collateral_decimals_pow)
        .m_ou()?;

    let numerator = all_collateral_value
        .checked_mul(borrow_decimals_pow)
        .m_ou()?;
    let denominator = borrow_asset_price
        .checked_mul(incentive_bps_factor)
        .m_ou()?
        .checked_div(BPS_FACTOR)
        .m_ou()?;

    let max_repay_borrow_units = numerator.checked_div(denominator).m_ou()?;
    let max_profitable_repay = max_repay_borrow_units.saturating_sub(profit_margin_borrow_tokens);
    let result = max_feasible_repay.min(max_profitable_repay);

    Ok(result)
}

#[derive(Debug)]
pub struct LiquidationProfitability {
    pub net_value: i128,
    pub gain_value: i128,
    pub is_acceptable: bool,
}

pub fn compute_liquidation_profitability(
    gain_value: i128,
    cost_value: i128,
    min_profit_margin_value: i128,
) -> Result<LiquidationProfitability, LMError> {
    if !gain_value.is_positive() || !cost_value.is_positive() {
        return Err(LMError::InternalError);
    }

    let net_value = gain_value.checked_sub(cost_value).m_ou()?;

    Ok(LiquidationProfitability {
        net_value,
        gain_value,
        is_acceptable: net_value > min_profit_margin_value,
    })
}

pub fn cents_to_oracle_value_ceil(
    cents: i128,
    oracle_price_decimals: u32,
) -> Result<i128, LMError> {
    if cents < 0 {
        return Err(LMError::InternalError);
    }

    let multiplier = 10_i128.checked_pow(oracle_price_decimals).m_ou()?;

    cents
        .checked_mul(multiplier)
        .m_ou()?
        .checked_add(99)
        .m_ou()?
        .checked_div(100)
        .m_ou()
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

    let multiplier = 10_i128.checked_pow(token_decimals).m_ou()?;
    let numerator = value
        .checked_mul(multiplier)
        .m_ou()?
        .checked_add(oracle_asset_price - 1)
        .m_ou()?;
    let denominator = oracle_asset_price;

    Ok(Underlying(numerator.checked_div(denominator).m_ou()?))
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
