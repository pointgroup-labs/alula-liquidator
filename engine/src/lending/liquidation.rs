//! Pure liquidation math: health, debt/collateral valuation, expected seize.

use {
    crate::lending::{
        bps::{fixed_mul_ceil, fixed_mul_floor},
        market::MarketData,
        obligation::{DepositPosition, Obligation},
        pool::PoolData,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum LendingError {
    #[error("Arithmetic overflow during calculation")]
    ArithmeticOverflow,

    #[error("Pool not found: {pool_address}")]
    PoolNotFound { pool_address: String },
}

/// Compute the maximum repayable amount for a single borrow position.
pub fn compute_max_repay_amount(
    d_tokens: i128,
    d_token_rate_ceil_bps: i128,
    liquidation_close_factor_bps: i128,
) -> i128 {
    let position_debt = d_tokens * d_token_rate_ceil_bps / 10_000;
    let position_debt_plus_percents = (position_debt * 102) / 100; // TODO: Maybe add to consts
    // NB: For now we take 102% to allow full liquidations that close the entire position
    // including the most recently accrued interest rate

    position_debt_plus_percents * liquidation_close_factor_bps / 10_000
}

/// Estimate the collateral received from a liquidation.
pub fn compute_received_collateral(
    repay_amount: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    deposit: &DepositPosition,
    min_collateral_value_cents: i128,
    oracle_price_decimals: u32,
) -> i128 {
    if collateral_pool.oracle_asset_price <= 0 {
        return 0;
    }

    // Uncapped estimate from repay value + bonus
    let repay_value =
        repay_amount * borrow_pool.oracle_asset_price / 10_i128.pow(borrow_pool.token_decimals);
    let repay_value_with_bonus =
        repay_value * (10_000 + collateral_pool.max_liquidation_incentive_bps) / 10_000;
    let uncapped = repay_value_with_bonus * 10_i128.pow(collateral_pool.token_decimals)
        / collateral_pool.oracle_asset_price;

    // Cap 1: available tokens in the deposit position
    let real_supply = fixed_mul_floor(deposit.j_tokens, collateral_pool.j_token_rate_floor_bps);
    let available_tokens = real_supply + deposit.collateral;

    // Cap 2: reserve min_collateral_value_cents for the last liquidator
    let reserved_tokens = if min_collateral_value_cents > 0 {
        let reserved_value = min_collateral_value_cents * 10_i128.pow(oracle_price_decimals) / 100;
        reserved_value * 10_i128.pow(collateral_pool.token_decimals)
            / collateral_pool.oracle_asset_price
    } else {
        0
    };

    let seizeable = (available_tokens - reserved_tokens).max(0);

    uncapped.min(seizeable)
}

/// Determine whether an obligation is liquidatable using only cached local data.
pub fn compute_is_liquidatable(obligation: &Obligation, md: &MarketData) -> bool {
    let pools = &md.pools_data;

    if !has_any_collateral(obligation, pools) {
        return false;
    }

    let mut debt_value_scaled: i128 = 0;
    for bor in &obligation.borrows {
        let pool = match pools.iter().find(|p| p.pool_address == bor.pool_address) {
            Some(p) => p,
            None => continue,
        };
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value =
            (real_debt * pool.oracle_asset_price + (decimals_divisor - 1)) / decimals_divisor;
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled += scaled;
    }

    let mut collateral_value_scaled: i128 = 0;
    let mut borrow_backing_positions: i128 = 0;
    for dep in &obligation.deposits {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => continue,
        };
        if pool.close_ltv_bps > 0 {
            borrow_backing_positions += 1;
        }
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply + dep.collateral;
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens * pool.oracle_asset_price / decimals_divisor;
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value_scaled += scaled;
    }

    let min_collateral_threshold =
        md.min_collateral_value_cents * 10_i128.pow(md.oracle_price_decimals) / 100;
    let buffer = min_collateral_threshold * borrow_backing_positions;

    debt_value_scaled > collateral_value_scaled.saturating_sub(buffer)
}

/// Returns `true` if the obligation has any collateral value across all deposits.
pub fn has_any_collateral(obligation: &Obligation, pools: &[PoolData]) -> bool {
    obligation.deposits.iter().any(|dep| {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => return false,
        };
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        real_supply + dep.collateral > 0
    })
}

/// Total obligation debt value (unscaled, oracle units).
pub fn compute_obligation_debt_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LendingError> {
    let mut total: i128 = 0;
    for bor in &obligation.borrows {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
            .ok_or_else(|| LendingError::PoolNotFound {
                pool_address: bor.pool_address.clone(),
            })?;
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            .ok_or(LendingError::ArithmeticOverflow)?;
        total = total
            .checked_add(value)
            .ok_or(LendingError::ArithmeticOverflow)?;
    }
    Ok(total)
}

/// Total obligation collateral value (unscaled, oracle units).
pub fn compute_obligation_collateral_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LendingError> {
    let mut total: i128 = 0;
    for dep in &obligation.deposits {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
            .ok_or_else(|| LendingError::PoolNotFound {
                pool_address: dep.pool_address.clone(),
            })?;
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply
            .checked_add(dep.collateral)
            .ok_or(LendingError::ArithmeticOverflow)?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            .ok_or(LendingError::ArithmeticOverflow)?;
        total = total
            .checked_add(value)
            .ok_or(LendingError::ArithmeticOverflow)?;
    }
    Ok(total)
}

/// Cap on `repay_amount` from the close factor.
pub fn compute_close_factor_repay_cap(
    position_debt_tokens: i128,
    liquidation_close_factor_bps: i128,
    is_insolvent: bool,
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

    let real_supply = fixed_mul_floor(deposit.j_tokens, collateral_pool.j_token_rate_floor_bps);
    let position_collateral_sum = real_supply + deposit.collateral;
    if position_collateral_sum <= 0 {
        return 0;
    }

    let liquidated_value =
        repay_amount * borrow_pool.oracle_asset_price / 10_i128.pow(borrow_pool.token_decimals);

    let min_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);
    let collateral_amount_no_bonus = liquidated_value * 10_i128.pow(collateral_pool.token_decimals)
        / collateral_pool.oracle_asset_price;
    let with_incentive = collateral_amount_no_bonus * (10_000 + min_incentive_bps) / 10_000;

    let ltv_cap = if !is_insolvent {
        if obligation_debt_value <= 0 || obligation_collateral_value <= 0 {
            return 0;
        }
        let max_value_recv =
            liquidated_value.saturating_mul(obligation_collateral_value) / obligation_debt_value;
        let strict_max_value_recv = max_value_recv * 999 / 1000;
        let ltv_collateral = strict_max_value_recv * 10_i128.pow(collateral_pool.token_decimals)
            / collateral_pool.oracle_asset_price;
        Some(ltv_collateral)
    } else {
        None
    };

    let mut seized = position_collateral_sum.min(with_incentive);
    if let Some(cap) = ltv_cap {
        seized = seized.min(cap);
    }

    let collateral_left = position_collateral_sum.saturating_sub(seized);
    let collateral_value_left = collateral_left * collateral_pool.oracle_asset_price
        / 10_i128.pow(collateral_pool.token_decimals);

    let min_collateral_threshold =
        min_collateral_value_cents * 10_i128.pow(oracle_price_decimals) / 100;

    if collateral_value_left < min_collateral_threshold {
        seized = position_collateral_sum;
    }

    seized.max(0)
}

/// Returns `true` if the obligation is insolvent.
pub fn compute_is_insolvent(obligation: &Obligation, market_data: &MarketData) -> bool {
    let mut debt_value_scaled: i128 = 0;
    for bor in &obligation.borrows {
        let pool = match market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
        {
            Some(p) => p,
            None => continue,
        };
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value =
            (real_debt * pool.oracle_asset_price + (decimals_divisor - 1)) / decimals_divisor;
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled += scaled;
    }

    let mut collateral_value_scaled: i128 = 0;
    for dep in &obligation.deposits {
        let pool = match market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
        {
            Some(p) => p,
            None => continue,
        };
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply + dep.collateral;
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens * pool.oracle_asset_price / decimals_divisor;
        let scaled = fixed_mul_floor(value, market_data.insolvency_ltv_bps);
        collateral_value_scaled += scaled;
    }

    debt_value_scaled > collateral_value_scaled
}
