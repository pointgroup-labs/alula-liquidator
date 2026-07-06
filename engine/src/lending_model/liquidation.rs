//! Pure liquidation math: health, debt/collateral valuation, expected seize.

use core::cmp::Ord;

use crate::lending_model::{
    amount::{BPS_FACTOR, bps_fixed_div_ceil},
    error::{LMError, LendingModelError, MapArithmeticError},
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

/// Determine whether an obligation is liquidatable using only cached local data.
///
/// Mirrors the contract's liquidate gate (`obligation.rs::liquidate`):
/// `debt_value_scaled_w_liability_factors > collateral_value_scaled_w_close_ltvs`,
/// with ceil rounding on the debt side and floor on the collateal side.
pub fn compute_is_liquidatable(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<bool, LendingModelError> {
    // TODO: Rewrite according to amount! types
    let pools = &market_data.pools_data;

    if !has_any_collateral(obligation, pools) {
        return Ok(false);
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
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .m_ou()?
            .checked_add(decimals_divisor - 1)
            .m_ou()?
            .checked_div(decimals_divisor)
            .m_ou()?;
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value = debt_value.checked_add(scaled).m_ou()?;
    }
    if debt_value == 0 {
        return Ok(false);
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
        let total_tokens = real_supply.checked_add(deposit.collateral.0).m_ou()?;
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .m_ou()?
            .checked_div(decimals_divisor)
            .m_ou()?;
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value = collateral_value.checked_add(scaled).m_ou()?;
    }

    let min_collateral_value_cents = market_data.min_collateral_value_cents;
    let min_collateral_value_per_deposit_position = min_collateral_value_cents
        .checked_mul(
            10_i128
                .checked_pow(market_data.oracle_price_decimals)
                .m_ou()?,
        )
        .m_ou()?
        .checked_div(100)
        .m_ou()?;
    let collateral_value_to_substract = min_collateral_value_per_deposit_position
        .checked_mul(obligation.deposits.len() as i128)
        .m_ou()?;

    if collateral_value_to_substract > collateral_value {
        return Ok(false);
    }
    let collateral_value_adjusted = collateral_value - collateral_value_to_substract;
    let res = debt_value > collateral_value_adjusted;

    Ok(res)
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
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_add(decimals_divisor - 1))
            .and_then(|v| v.checked_div(decimals_divisor))
            .m_ou()?;
        total = total.checked_add(value).m_ou()?;
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
        let total_tokens = real_supply.checked_add(dep.collateral.0).m_ou()?;
        let pool_token_factor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(pool_token_factor))
            .m_ou()?;
        total = total.checked_add(value).m_ou()?;
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
) -> Result<i128, LMError> {
    if repay_amount <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return Ok(0);
    }

    let real_supply = fixed_mul_floor(deposit.j_tokens.0, collateral_pool.j_token_rate_floor_bps);

    let position_collateral_sum = real_supply.checked_add(deposit.collateral.0).m_ou()?;
    if position_collateral_sum <= 0 {
        return Ok(0);
    }

    let borrow_token_factor = 10_i128.checked_pow(borrow_pool.token_decimals).m_ou()?;
    let liquidated_value = repay_amount
        .checked_mul(borrow_pool.oracle_asset_price)
        .m_ou()?
        .checked_div(borrow_token_factor)
        .m_ou()?;

    let min_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);

    let collateral_token_factor = 10_i128.checked_pow(collateral_pool.token_decimals).m_ou()?;
    let collateral_amount_no_bonus = liquidated_value
        .checked_mul(collateral_token_factor)
        .m_ou()?
        .checked_div(collateral_pool.oracle_asset_price)
        .m_ou()?;

    let with_incentive = collateral_amount_no_bonus
        .checked_mul(BPS_FACTOR.checked_add(min_incentive_bps).m_ou()?)
        .m_ou()?
        .checked_div(BPS_FACTOR)
        .m_ou()?;

    let ltv_cap = if !is_insolvent {
        // LTV-improvement assertion
        if obligation_debt_value <= 0 || obligation_collateral_value <= 0 {
            return Ok(0);
        }
        let max_value_recv = liquidated_value
            .checked_mul(obligation_collateral_value)
            .m_ou()?
            .checked_div(obligation_debt_value)
            .m_ou()?;

        let strict_max_value_recv = max_value_recv
            .checked_mul(999)
            .m_ou()?
            .checked_div(1000)
            .m_ou()?;

        let ltv_collateral = strict_max_value_recv
            .checked_mul(collateral_token_factor)
            .m_ou()?
            .checked_div(collateral_pool.oracle_asset_price)
            .m_ou()?;

        // receiving ltv improving amount of collateral for that specific repayment
        Some(ltv_collateral)
    } else {
        None
    };

    let mut seized = position_collateral_sum.min(with_incentive);
    if let Some(cap) = ltv_cap {
        seized = seized.min(cap);
    }

    let collateral_left = position_collateral_sum.checked_sub(seized).m_ou()?;
    let collateral_value_left = collateral_left
        .checked_mul(collateral_pool.oracle_asset_price)
        .m_ou()?
        .checked_div(collateral_token_factor)
        .m_ou()?;

    let oracle_price_factor = 10_i128.checked_pow(oracle_price_decimals).m_ou()?;
    let min_collateral_threshold = min_collateral_value_cents
        .checked_mul(oracle_price_factor)
        .m_ou()?
        .checked_div(100)
        .m_ou()?;

    if collateral_value_left < min_collateral_threshold {
        seized = position_collateral_sum;
    }

    Ok(seized.max(0))
}

/// Checks whether the unparameterized obligation is insolvent
pub fn compute_is_insolvent(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<bool, LMError> {
    let debt_value = compute_obligation_debt_value(obligation, market_data)?;

    if debt_value < 0 {
        error!(debt_value, "negative debt value");

        return Err(LMError::InternalError);
    } else if debt_value == 0 {
        return Ok(false);
    }

    let collateral_value = compute_obligation_collateral_value(obligation, market_data)?;

    if collateral_value < 0 {
        error!(collateral_value, "negative collateral value");

        return Err(LMError::InternalError);
    } else if collateral_value == 0 {
        return Ok(true);
    }

    let unparameterized_ltv = bps_fixed_div_ceil(debt_value, collateral_value).m_ou()?;
    let is_insolvent = unparameterized_ltv >= market_data.insolvency_ltv_bps;

    Ok(is_insolvent)
}
