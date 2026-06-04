//! Profitability and flash-loan helpers.

use crate::lending_model::{
    BPS_FACTOR, Underlying,
    error::{LMError, MapArithmeticError},
    pool::PoolData,
};

pub fn compute_flash_fee(
    amount: Underlying,
    flash_loan_fee_bps: i128,
) -> Result<Underlying, LMError> {
    amount.scale_with_bps_ceil(flash_loan_fee_bps)
}

/// Profit margin expressed in borrow-token units.
///
/// `min_profit_margin_cents` is converted to oracle value
/// (`cents * 10^oracle_decimals / 100`) and then to borrow tokens
/// (`value * 10^borrow_decimals / borrow_oracle_price`).
pub fn compute_profit_margin_in_borrow_token(
    min_profit_margin_cents: i128,
    oracle_price_decimals: u32,
    borrow_pool: &PoolData,
) -> Result<Underlying, LMError> {
    if !borrow_pool.oracle_asset_price.is_positive() || !min_profit_margin_cents.is_positive() {
        return Err(LMError::InternalError);
    }

    let margin_value = cents_to_oracle_value_floor(min_profit_margin_cents, oracle_price_decimals)?;

    value_to_underlying_asset_amount_floor(
        margin_value,
        borrow_pool.oracle_asset_price,
        borrow_pool.token_decimals,
    )
}

// /// Cap on `repay_amount` when bridging the keeper's liquidity gap with a
// /// flash borrow. Three branches: the wallet covers `required_amount` outright,
// /// the pool can fund the shortfall (so `required_amount` is achievable), or
// /// the pool lacks the capacity (return 0).
// pub fn compute_flash_loan_repay_cap(
//     required_amount: i128,
//     liquidator_balance: i128,
//     pool_total_available: i128,
//     _flash_fee_bps: i128,
// ) -> i128 {
//     if liquidator_balance >= needed_amount {
//         return needed_amount;
//     }

//     let flash_amount = needed_amount - liquidator_balance;
//     if flash_amount <= 0 {
//         return needed_amount;
//     }

//     if pool_total_available < flash_amount {
//         return 0;
//     }

//     needed_amount
// }

/// Largest repay (in borrow tokens) such that the bonus-inflated collateral
/// seizure still fits in `available_collateral`, minus a profit-margin slack.
///
/// Returns `min(max_feasible_repay, R_max − profit_margin)`, or `None` if any
/// factor collapses (zero/negative prices or collateral, arithmetic overflow,
/// or the cap falls to zero).
pub fn compute_repay_cap_from_collateral(
    max_feasible_repay: i128,
    available_collateral: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    profit_margin_borrow_tokens: i128,
) -> Option<i128> {
    if available_collateral <= 0 {
        return None;
    }
    if borrow_pool.oracle_asset_price <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return None;
    }

    let liquidation_incentive_bps = collateral_pool.max_liquidation_incentive_bps;
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
    let max_profitable_repay = max_repay_borrow_units.saturating_sub(profit_margin_borrow_tokens);

    let result = max_feasible_repay.min(max_profitable_repay);
    if result > 0 { Some(result) } else { None }
}

pub struct LiquidationProfitability {
    pub effective_gain_value: i128,
    pub required_value: i128,
    pub net_value: i128,
    pub is_profitable: bool,
}

pub fn compute_liquidation_profitability(
    gain_value: i128,
    cost_value: i128,
    min_profit_margin_value: i128,
    gain_haircut_bps: i128,
    inclusion_fee_value: i128,
) -> Result<LiquidationProfitability, LMError> {
    if !(0..=BPS_FACTOR).contains(&gain_haircut_bps) {
        return Err(LMError::InternalError);
    }
    if !gain_value.is_positive() || !cost_value.is_positive() {
        return Err(LMError::InternalError);
    }

    let multiplier = BPS_FACTOR.saturating_sub(gain_haircut_bps);
    let effective_gain_value = gain_value
        .checked_mul(multiplier)
        .map_over_or_underflow()?
        .checked_div(BPS_FACTOR)
        .map_over_or_underflow()?;

    let required_value = cost_value
        .checked_add(min_profit_margin_value)
        .map_over_or_underflow()?
        .checked_add(inclusion_fee_value)
        .map_over_or_underflow()?;

    let net_value = effective_gain_value.saturating_sub(required_value);

    Ok(LiquidationProfitability {
        effective_gain_value,
        required_value,
        net_value,
        is_profitable: net_value > 0,
    })
}

pub fn cents_to_oracle_value_floor(
    cents: i128,
    oracle_price_decimals: u32,
) -> Result<i128, LMError> {
    cents
        .checked_mul(10_i128.pow(oracle_price_decimals))
        .map_over_or_underflow()?
        .checked_div(100)
        .map_over_or_underflow()
}

pub fn value_to_underlying_asset_amount_floor(
    value: i128,
    oracle_asset_price: i128,
    token_decimals: u32,
) -> Result<Underlying, LMError> {
    Ok(Underlying(
        value
            .checked_mul(10_i128.pow(token_decimals))
            .map_over_or_underflow()?
            .checked_div(oracle_asset_price)
            .map_over_or_underflow()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(
        token_decimals: u32,
        oracle_asset_price: i128,
        max_liquidation_incentive_bps: i128,
    ) -> PoolData {
        PoolData {
            pool_address: "p".into(),
            token_address: "t".into(),
            token_symbol: "T".into(),
            token_decimals,
            total_borrowed: 1_000_000.into(),
            total_d_tokens: 1_000_000.into(),
            total_j_tokens: 1_000_000.into(),
            total_available: 1_000_000.into(),
            total_available_adjusted: 1_000_000.into(),
            total_supply: 1_000_000.into(),
            total_collateral: 1_000_000.into(),
            j_token_rate_floor_bps: 0,
            d_token_rate_ceil_bps: 0,
            oracle_asset_price,
            open_ltv_bps: 8000,
            close_ltv_bps: 8500,
            liability_factor_bps: 10_000,
            liquidation_close_factor_bps: 5_000,
            max_liquidation_incentive_bps,
            flash_loan_fee_bps: 0,
            utilization_ratio_limit_bps: 9_000,
        }
    }

    // Non-proptest unit cases that pin specific numbers from the keeper's old
    // inline math, so we know the new helper is a drop-in replacement at the
    // historical default of 500 bps haircut.
    #[test]
    fn profitable_matches_legacy_5pct_buffer() -> Result<(), LMError> {
        // gain * (10_000 - 500) / 10_000 = gain * 0.95
        let gain = 1_000_000i128;
        let cost = 940_000i128;
        let r = compute_liquidation_profitability(gain, cost, 0, 500, 0)?;
        assert_eq!(r.effective_gain_value, 950_000);
        assert!(r.is_profitable);

        // Same gain, cost just above effective_gain → rejected.
        let r2 = compute_liquidation_profitability(gain, 951_000, 0, 500, 0)?;
        assert!(!r2.is_profitable);

        Ok(())
    }

    #[test]
    fn profitable_inclusion_fee_tips_the_balance() -> Result<(), LMError> {
        // Without fee: passes by 1 unit. With fee = 1: fails.
        let gain = 1_000_000i128;
        let cost = 950_000i128;
        let r_no_fee = compute_liquidation_profitability(gain, cost - 1, 0, 500, 0)?;
        assert!(r_no_fee.is_profitable);
        let r_fee = compute_liquidation_profitability(gain, cost - 1, 0, 500, 2)?;
        assert!(!r_fee.is_profitable);

        Ok(())
    }

    #[test]
    fn profitable_net_oracle_is_signed_difference() -> Result<(), LMError> {
        // Positive net.
        let r = compute_liquidation_profitability(1_000_000, 800_000, 0, 0, 0)?;
        assert_eq!(r.net_value, 200_000);
        assert!(r.is_profitable);

        // Zero net: boundary.
        let r = compute_liquidation_profitability(1_000_000, 1_000_000, 0, 0, 0)?;
        assert_eq!(r.net_value, 0);
        assert!(!r.is_profitable);

        // Negative net: rejected and ranks below positives.
        let r = compute_liquidation_profitability(1_000_000, 1_500_000, 0, 0, 0)?;
        assert_eq!(r.net_value, -500_000);
        assert!(!r.is_profitable);

        Ok(())
    }
}
