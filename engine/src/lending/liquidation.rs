//! Pure liquidation math: health, debt/collateral valuation, expected seize.

use {
    crate::lending::{
        bps::{BPS_DENOMINATOR, LIQUIDATION_INTEREST_BUFFER_BPS, fixed_mul_ceil, fixed_mul_floor},
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

/// Upper bound on tokens repayable in one `liquidate` call. Multiplies
/// `d_tokens` by `d_token_rate_ceil_bps` using **floor** rounding (the field
/// name reflects the rate's role in the protocol, not the rounding direction
/// used here — see the inline note), inflates by `LIQUIDATION_INTEREST_BUFFER_BPS`
/// so a "full" liquidation also clears recently accrued interest, then
/// applies the close factor.
pub fn compute_max_repay_amount(
    d_tokens: i128,
    d_token_rate_ceil_bps: i128,
    liquidation_close_factor_bps: i128,
) -> i128 {
    // Preserves the original floor semantics for d_token conversion; the field
    // is named `_ceil_bps` so this is a known inconsistency with the
    // gold-standard `compute_obligation_debt_value` (which uses ceil). Left
    // unchanged here to keep Phase 1 a pure arithmetic-safety refactor.
    let position_debt = fixed_mul_floor(d_tokens, d_token_rate_ceil_bps);

    // Inflate by LIQUIDATION_INTEREST_BUFFER_BPS (=10_200) so a full
    // liquidation can also close the most recently accrued interest.
    let position_debt_plus_percents =
        fixed_mul_floor(position_debt, LIQUIDATION_INTEREST_BUFFER_BPS);

    fixed_mul_floor(position_debt_plus_percents, liquidation_close_factor_bps)
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
    let repay_value = repay_amount
        .saturating_mul(borrow_pool.oracle_asset_price)
        .saturating_div(10_i128.pow(borrow_pool.token_decimals));
    let repay_value_with_bonus = repay_value
        .saturating_mul(
            BPS_DENOMINATOR.saturating_add(collateral_pool.max_liquidation_incentive_bps),
        )
        .saturating_div(BPS_DENOMINATOR);
    let uncapped = repay_value_with_bonus
        .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
        .saturating_div(collateral_pool.oracle_asset_price);

    // Cap 1: available tokens in the deposit position
    let real_supply = fixed_mul_floor(deposit.j_tokens, collateral_pool.j_token_rate_floor_bps);
    let available_tokens = real_supply.saturating_add(deposit.collateral);

    // Cap 2: reserve min_collateral_value_cents for the last liquidator
    let reserved_tokens = if min_collateral_value_cents > 0 {
        let reserved_value = min_collateral_value_cents
            .saturating_mul(10_i128.pow(oracle_price_decimals))
            .saturating_div(100);
        reserved_value
            .saturating_mul(10_i128.pow(collateral_pool.token_decimals))
            .saturating_div(collateral_pool.oracle_asset_price)
    } else {
        0
    };

    let seizeable = available_tokens.saturating_sub(reserved_tokens).max(0);

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
        let value = real_debt
            .saturating_mul(pool.oracle_asset_price)
            .saturating_add(decimals_divisor - 1)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled = debt_value_scaled.saturating_add(scaled);
    }

    let mut collateral_value_scaled: i128 = 0;
    let mut borrow_backing_positions: i128 = 0;
    for dep in &obligation.deposits {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => continue,
        };
        if pool.close_ltv_bps > 0 {
            borrow_backing_positions = borrow_backing_positions.saturating_add(1);
        }
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply.saturating_add(dep.collateral);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens
            .saturating_mul(pool.oracle_asset_price)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value_scaled = collateral_value_scaled.saturating_add(scaled);
    }

    let min_collateral_threshold = md
        .min_collateral_value_cents
        .saturating_mul(10_i128.pow(md.oracle_price_decimals))
        .saturating_div(100);
    let buffer = min_collateral_threshold.saturating_mul(borrow_backing_positions);

    debt_value_scaled > collateral_value_scaled.saturating_sub(buffer)
}

/// Cheap pre-filter used by [`compute_is_liquidatable`] to short-circuit
/// obligations with no real collateral (`j_tokens × j_rate_floor + plain
/// collateral == 0` across every deposit).
pub fn has_any_collateral(obligation: &Obligation, pools: &[PoolData]) -> bool {
    obligation.deposits.iter().any(|dep| {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => return false,
        };
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        real_supply.saturating_add(dep.collateral) > 0
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

/// Close-factor cap on `repay_amount`. Insolvent obligations skip the cap
/// — any participant may close the whole debt — while solvent ones apply
/// `liquidation_close_factor_bps` per the protocol's partial-liquidation rule.
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
    let position_collateral_sum = real_supply.saturating_add(deposit.collateral);
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
        .saturating_mul(BPS_DENOMINATOR.saturating_add(min_incentive_bps))
        .saturating_div(BPS_DENOMINATOR);

    let ltv_cap = if !is_insolvent {
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

/// Insolvency check: total debt (scaled by per-pool liability factor, ceil)
/// exceeds total collateral (scaled by the market-wide `insolvency_ltv_bps`,
/// floor). Distinct from [`compute_is_liquidatable`], which uses per-pool
/// `close_ltv_bps` and a `min_collateral_value_cents` buffer.
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
        let value = real_debt
            .saturating_mul(pool.oracle_asset_price)
            .saturating_add(decimals_divisor - 1)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled = debt_value_scaled.saturating_add(scaled);
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
        let total_tokens = real_supply.saturating_add(dep.collateral);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens
            .saturating_mul(pool.oracle_asset_price)
            .saturating_div(decimals_divisor);
        let scaled = fixed_mul_floor(value, market_data.insolvency_ltv_bps);
        collateral_value_scaled = collateral_value_scaled.saturating_add(scaled);
    }

    debt_value_scaled > collateral_value_scaled
}
