//! Pure liquidation math: health, debt/collateral valuation, expected seize.

use core::cmp::Ord;

use crate::lending_model::{
    amount::{BPS_FACTOR, bps_fixed_div_ceil},
    error::{LMError, MapArithmeticError},
    market::MarketData,
    obligation::{DepositPosition, Obligation},
    pool::PoolData,
};
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationResult {
    pub debt_repaid: i128,
    pub j_tokens_seized: i128,
    pub d_tokens_burned: i128,
    pub amount_to_send_back: i128,
    pub plain_collateral_seized: i128,
    pub tokens_from_j_tokens_seized: i128,
}

/// `floor(amount * bps / BPS_FACTOR)` with saturating intermediates.
fn fixed_mul_floor(amount: i128, bps: i128) -> i128 {
    amount.saturating_mul(bps).saturating_div(BPS_FACTOR)
}

/// `ceil(amount * bps / BPS_FACTOR)` with saturating intermediates.
fn fixed_mul_ceil(amount: i128, bps: i128) -> i128 {
    amount
        .saturating_mul(bps)
        .saturating_add(BPS_FACTOR - 1)
        .saturating_div(BPS_FACTOR)
}

// /// Upper bound on tokens repayable in one `liquidate` call. Converts
// /// `d_tokens` to underlying with **ceil** rounding, inflates by
// /// `liquidation_interest_buffer_bps` (ceil) so a "full" liquidation also
// /// clears recently accrued interest, then applies the close factor (ceil).
// pub fn compute_max_repay_amount(
//     d_tokens: DTokens,
//     borrow_pool: &PoolData,
//     liquidation_close_factor_bps: i128,
//     liquidation_interest_buffer_bps: i128,
// ) -> Result<Underlying, LMError> {
//     let position_debt: Underlying = borrow_pool.d_tokens_to_tokens_ceil(d_tokens)?;
//     let position_debt_inflated: Underlying =
//         position_debt.scale_with_bps_ceil(liquidation_interest_buffer_bps)?;

//     // TODO: Take liquidation_close_factor from 2 pools, no?
//     let res = position_debt_inflated.scale_with_bps_ceil(liquidation_close_factor_bps)?;

//     Ok(res)
// }

// /// Estimate the collateral received from a liquidation.
// ///
// /// Computes the uncapped seize from `repay_amount × price × (1 + incentive)`,
// /// then applies two caps: the tokens actually held in the deposit position,
// /// and a reserve of `min_collateral_value_cents` left for the final liquidator.
// pub fn compute_received_collatera2l(
//     repay_amount: i128,
//     borrow_pool: &PoolData,
//     collateral_pool: &PoolData,
//     deposit: &DepositPosition,
//     min_collateral_value_cents: i128,
//     oracle_price_decimals: u32,
// ) -> i128 {
//     if collateral_pool.oracle_asset_price <= 0 {
//         return 0;
//     }

//     // Uncapped estimate from repay value + bonus.
//     let repay_value = repay_amount
//         .saturating_mul(borrow_pool.oracle_asset_price)
//         .saturating_div(10_i128.pow(borrow_pool.token_decimals));
//     let repay_value_with_bonus = repay_value
//         .saturating_mul(BPS_FACTOR.saturating_add(collateral_pool.max_liquidation_incentive_bps))
//         .saturating_div(BPS_FACTOR);
//     let uncapped = repay_value_with_bonus
//         .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
//         .saturating_div(collateral_pool.oracle_asset_price);

//     // Cap 1: available tokens in the deposit position.
//     let real_supply = fixed_mul_floor(deposit.j_tokens.0, collateral_pool.j_token_rate_floor_bps);
//     let available_tokens = real_supply.saturating_add(deposit.collateral.0);

//     // Cap 2: reserve min_collateral_value_cents for the last liquidator.
//     let reserved_tokens = if min_collateral_value_cents > 0 {
//         let reserved_value = min_collateral_value_cents
//             .saturating_mul(10_i128.pow(oracle_price_decimals))
//             .saturating_div(100);
//         reserved_value
//             .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
//             .saturating_div(collateral_pool.oracle_asset_price)
//     } else {
//         0
//     };

//     let seizeable = available_tokens.saturating_sub(reserved_tokens).max(0);

//     uncapped.min(seizeable)
// }

/// Determine whether an obligation is liquidatable using only cached local data.
///
/// Mirrors the contract's liquidate gate (`obligation.rs::liquidate`):
/// `debt_value_scaled_w_liability_factors > collateral_value_scaled_w_close_ltvs`,
/// with ceil rounding on the debt side and floor on the collateal side.
pub fn compute_is_liquidatable(obligation: &Obligation, market_data: &MarketData) -> bool {
    // TODO: Rewrite according to amount! types
    let pools = &market_data.pools_data;

    if !has_any_collateral(obligation, pools) {
        return false;
    }

    let mut debt_value: i128 = 0;
    for borrow in &obligation.borrows {
        let pool = match pools.iter().find(|p| p.pool_address == borrow.pool_address) {
            Some(p) => p,
            None => {
                error!(?borrow, "missing borrow pool");

                continue;
            }
        };
        let real_debt = fixed_mul_ceil(borrow.d_tokens.0, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = real_debt
            .saturating_mul(pool.oracle_asset_price)
            .saturating_add(decimals_divisor - 1)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value = debt_value.saturating_add(scaled);
    }

    let mut collateral_value: i128 = 0;
    for deposit in &obligation.deposits {
        let pool = match pools
            .iter()
            .find(|p| p.pool_address == deposit.pool_address)
        {
            Some(p) => p,
            None => {
                error!(?deposit, "missing deposit pool");

                continue;
            }
        };
        let real_supply = fixed_mul_floor(deposit.j_tokens.0, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply.saturating_add(deposit.collateral.0);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens
            .saturating_mul(pool.oracle_asset_price)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value = collateral_value.saturating_add(scaled);
    }

    let min_collateral_value_cents = market_data.min_collateral_value_cents;
    let min_collateral_value_per_deposit_position = min_collateral_value_cents
        .saturating_mul(10_i128.pow(market_data.oracle_price_decimals))
        .saturating_div(100);
    let collateral_value_to_substract =
        min_collateral_value_per_deposit_position * (obligation.deposits.len() as i128);

    if collateral_value_to_substract > collateral_value {
        return false;
    }
    let collateral_value_adjusted = collateral_value - collateral_value_to_substract;

    debt_value > collateral_value_adjusted
}

/// Cheap pre-filter used by [`compute_is_liquidatable`] to short-circuit
/// obligations with no real collateral (`j_tokens × j_rate_floor + plain
/// collateral == 0` across every deposit).
pub fn has_any_collateral(obligation: &Obligation, pools: &[PoolData]) -> bool {
    obligation.deposits.iter().any(|dep| {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => {
                error!(?dep, "no deposit pool exists");

                return false;
            }
        };
        let real_supply = fixed_mul_floor(dep.j_tokens.0, pool.j_token_rate_floor_bps);
        real_supply.saturating_add(dep.collateral.0) > 0
    })
}

/// Total obligation debt value (unscaled, oracle units).
///
/// Per-pool division by `10^token_decimals` rounds **up**, mirroring the
/// contract's `compute_debt_value` → `compute_asset_value_scaled_ceil`.
/// This value feeds both the insolvency LTV and the LTV-improving seize cap,
/// exactly as `obligation_debt_value` does in the contract's `liquidate`.
pub fn compute_obligation_debt_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LMError> {
    let mut total: i128 = 0;
    for bor in &obligation.borrows {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
            .ok_or(LMError::InternalError)?;
        let real_debt = fixed_mul_ceil(bor.d_tokens.0, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_add(decimals_divisor - 1))
            .and_then(|v| v.checked_div(decimals_divisor))
            .map_over_or_underflow()?;
        total = total.checked_add(value).map_over_or_underflow()?;
    }

    Ok(total)
}

/// Total obligation collateral value (unscaled, oracle units).
pub fn compute_obligation_collateral_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LMError> {
    let mut total: i128 = 0;
    for dep in &obligation.deposits {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
            .ok_or(LMError::InternalError)?;
        let real_supply = fixed_mul_floor(dep.j_tokens.0, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply
            .checked_add(dep.collateral.0)
            .map_over_or_underflow()?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            .map_over_or_underflow()?;
        total = total.checked_add(value).map_over_or_underflow()?;
    }
    Ok(total)
}

/// Close-factor cap on `repay_amount`. Insolvent obligations skip the cap
/// — any participant may close the whole debt — while solvent ones apply
/// `liquidation_close_factor_bps` per the protocol's partial-liquidation rule.
pub fn compute_close_factor_repay_cap(
    is_insolvent: bool,
    position_debt_tokens: i128,
    liquidation_close_factor_bps: i128,
) -> i128 {
    if is_insolvent {
        position_debt_tokens
    } else {
        fixed_mul_floor(position_debt_tokens, liquidation_close_factor_bps)
    }
}

/// Mirror the contract's seized-collateral computation for a given `repay_amount`.
#[allow(clippy::too_many_arguments)]
pub fn compute_expected_seized_collateral(
    repay_amount: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    deposit: &DepositPosition,
    obligation_debt_value: i128,
    obligation_collateral_value: i128,
    is_insolvent: bool,
    min_collateral_value_cents: i128,
    oracle_price_decimals: u32,
) -> i128 {
    if repay_amount <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return 0;
    }

    let real_supply = fixed_mul_floor(deposit.j_tokens.0, collateral_pool.j_token_rate_floor_bps);

    let position_collateral_sum = real_supply.saturating_add(deposit.collateral.0);
    if position_collateral_sum <= 0 {
        return 0;
    }

    let liquidated_value = repay_amount
        .saturating_mul(borrow_pool.oracle_asset_price)
        .saturating_div(10_i128.pow(borrow_pool.token_decimals));

    let min_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);

    let collateral_amount_no_bonus = liquidated_value
        .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
        .saturating_div(collateral_pool.oracle_asset_price);

    let with_incentive = collateral_amount_no_bonus
        .saturating_mul(BPS_FACTOR.saturating_add(min_incentive_bps))
        .saturating_div(BPS_FACTOR);

    let ltv_cap = if !is_insolvent {
        // LTV-improvement assertion

        if obligation_debt_value <= 0 || obligation_collateral_value <= 0 {
            return 0;
        }
        let max_value_recv = liquidated_value
            .saturating_mul(obligation_collateral_value)
            .saturating_div(obligation_debt_value);

        let strict_max_value_recv = max_value_recv.saturating_mul(999).saturating_div(1000);

        let ltv_collateral = strict_max_value_recv
            .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
            .saturating_div(collateral_pool.oracle_asset_price);

        // receiving ltv improving amount of collateral for that specific repayment
        Some(ltv_collateral)
    } else {
        None
    };

    let mut seized = position_collateral_sum.min(with_incentive);
    if let Some(cap) = ltv_cap {
        seized = seized.min(cap);
    }

    let collateral_left = position_collateral_sum.saturating_sub(seized);
    let collateral_value_left = collateral_left
        .saturating_mul(collateral_pool.oracle_asset_price)
        .saturating_div(10_i128.pow(collateral_pool.token_decimals));

    let min_collateral_threshold = min_collateral_value_cents
        .saturating_mul(10_i128.pow(oracle_price_decimals))
        .saturating_div(100);

    if collateral_value_left < min_collateral_threshold {
        seized = position_collateral_sum;
    }

    seized.max(0)
}

/// Checks whether the unparameterized obligation is insolvent
pub fn compute_is_insolvent(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<bool, LMError> {
    let debt_value = compute_obligation_debt_value(obligation, market_data)?;

    // 1. Debt Checks
    if debt_value < 0 {
        error!(debt_value, "negative debt value");
        return Err(LMError::InternalError);
    } else if debt_value == 0 {
        // Optimization & 0/0 fix: No debt = impossible to be insolvent
        return Ok(false);
    }

    let collateral_value = compute_obligation_collateral_value(obligation, market_data)?;

    // 2. Collateral Checks
    if collateral_value < 0 {
        error!(collateral_value, "negative collateral value");
        return Err(LMError::InternalError);
    } else if collateral_value == 0 {
        // Debt is > 0, but collateral is 0 = heavily insolvent
        return Ok(true);
    }

    // 3. LTV Calculation
    Ok(match bps_fixed_div_ceil(debt_value, collateral_value) {
        Some(unparameterized_ltv_bps) => unparameterized_ltv_bps >= market_data.insolvency_ltv_bps,
        // Overflow fallback: Debt astronomically dwarfs collateral
        None => true,
    })
}
