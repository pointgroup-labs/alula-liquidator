//! Profitability and flash-loan helpers.

use crate::lending::{
    bps::{BPS_DENOMINATOR, fixed_mul_floor},
    pool::PoolData,
};

/// Compute the flash loan fee (ceiling) for a given amount and fee rate.
///
/// `fee = ceil(amount * flash_loan_fee_bps / 10_000)`
pub fn compute_flash_fee(amount: i128, flash_loan_fee_bps: i128) -> i128 {
    (amount * flash_loan_fee_bps + 9_999) / 10_000
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
) -> i128 {
    if borrow_pool.oracle_asset_price <= 0 {
        return 0;
    }
    let margin_value = min_profit_margin_cents * 10_i128.pow(oracle_price_decimals) / 100;

    margin_value * 10_i128.pow(borrow_pool.token_decimals) / borrow_pool.oracle_asset_price
}

/// Cap on `repay_amount` when using flash loans to bridge liquidity gaps.
pub fn compute_flash_loan_repay_cap(
    needed_amount: i128,
    liquidator_balance: i128,
    pool_total_available: i128,
    _flash_fee_bps: i128,
) -> i128 {
    if liquidator_balance >= needed_amount {
        return needed_amount;
    }

    let flash_amount = needed_amount - liquidator_balance;
    if flash_amount <= 0 {
        return needed_amount;
    }

    if pool_total_available < flash_amount {
        return 0;
    }

    needed_amount
}

/// Largest repay (in borrow tokens) such that the bonus-inflated collateral
/// seizure still fits in `available_collateral`, minus a profit-margin slack.
///
/// Derivation. Repaying `R` borrow tokens entitles the liquidator to seize
/// collateral worth `R × P_b × (1 + bonus)` in oracle value. Converting to
/// collateral-token units and constraining ≤ `available_collateral` gives:
///
/// ```text
///   R_max = available_collateral × P_c × 10^borrow_dec
///         / ( P_b × (1 + bonus) × 10^collateral_dec )
/// ```
///
/// The function returns `min(max_feasible_repay, R_max − profit_margin)`,
/// or `None` if any factor collapses (zero/negative prices or collateral,
/// arithmetic overflow, or the cap falls to zero).
///
/// Monotonicity (relied on by callers and proptests):
/// * **monotonic** in `available_collateral` — more collateral ⇒ no smaller cap.
/// * **anti-monotonic** in `max_liquidation_incentive_bps` — a richer bonus
///   makes each repay unit consume more collateral, shrinking the cap. This
///   is intentional: the function answers "how much *can* I repay" not
///   "how much *should* I repay to maximize profit".
/// * **monotonic** in `max_feasible_repay` (it's an upper-bound floor).
///
/// I128-only — replaces the float-based heuristic that used to live in
/// `pipeline::strategies::liquidator::calculate_max_profitable_repay_oracle`.
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
    let incentive_bps = BPS_DENOMINATOR.checked_add(liquidation_incentive_bps)?;
    if incentive_bps <= 0 {
        return None;
    }

    // max_theoretical_repay (in collateral units) = collateral / (1 + incentive)
    // = collateral * 10_000 / (10_000 + incentive_bps)   (integer floor)
    let max_theoretical_repay_collateral_units = available_collateral
        .checked_mul(BPS_DENOMINATOR)?
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

/// Marker use to silence unused-import warning in some configurations.
#[allow(dead_code)]
fn _floor_marker(a: i128, b: i128) -> i128 {
    fixed_mul_floor(a, b)
}

#[cfg(test)]
mod tests {
    use {super::*, proptest::prelude::*};

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
            total_borrowed: 1_000_000,
            total_d_tokens: 1_000_000,
            total_j_tokens: 1_000_000,
            total_available: 1_000_000,
            total_available_adjusted: 1_000_000,
            total_supply: 1_000_000,
            total_collateral: 1_000_000,
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

    proptest! {
        // Property 1: With zero incentive, the function provides no actual
        // liquidation bonus. We assert the weakest sound property: if it
        // returns a value, that value is strictly positive.
        #[test]
        fn prop_zero_incentive_result_is_positive_or_none(
            max_feasible_repay in 1i128..=1_000_000_000i128,
            available_collateral in 1i128..=1_000_000_000i128,
            profit_margin in 0i128..=1_000_000i128,
            borrow_price in 1i128..=1_000_000i128,
            collateral_price in 1i128..=1_000_000i128,
        ) {
            let borrow = make_pool(7, borrow_price, 0);
            let collat = make_pool(7, collateral_price, 0);
            let r = compute_repay_cap_from_collateral(
                max_feasible_repay,
                available_collateral,
                &borrow,
                &collat,
                profit_margin,
            );
            if let Some(v) = r {
                prop_assert!(v > 0, "Some(v) must be positive, got {}", v);
            }
        }

        // Property 2a: ANTI-monotonic in collateral_pool.max_liquidation_incentive_bps.
        // Higher bonus ⇒ each repay unit consumes more collateral ⇒ smaller cap.
        // (See function doc for the derivation.) This was originally written as
        // a monotonic-in-incentive test based on an incorrect intuition that
        // "higher bonus = more profit = larger answer"; in fact the function
        // computes a feasibility cap, and the bonus tightens it.
        #[test]
        fn prop_anti_monotonic_in_incentive(
            max_feasible_repay in 1i128..=1_000_000_000i128,
            available_collateral in 1i128..=1_000_000_000i128,
            profit_margin in 0i128..=1_000_000i128,
            (i_lo, i_hi) in (0i128..=5_000, 0i128..=5_000),
        ) {
            prop_assume!(i_lo < i_hi);
            let borrow = make_pool(7, 1_000_000, 0);
            let lo = compute_repay_cap_from_collateral(
                max_feasible_repay,
                available_collateral,
                &borrow,
                &make_pool(7, 1_000_000, i_lo),
                profit_margin,
            ).unwrap_or(0);
            let hi = compute_repay_cap_from_collateral(
                max_feasible_repay,
                available_collateral,
                &borrow,
                &make_pool(7, 1_000_000, i_hi),
                profit_margin,
            ).unwrap_or(0);
            prop_assert!(
                lo >= hi,
                "expected anti-monotonic in incentive: lo({})={} hi({})={}",
                i_lo, lo, i_hi, hi,
            );
        }

        // Property 2b: monotonic in available_collateral. More collateral can
        // only widen (or preserve) the cap, never shrink it.
        #[test]
        fn prop_monotonic_in_collateral(
            max_feasible_repay in 1i128..=1_000_000_000i128,
            (c_lo, c_hi) in (1i128..=1_000_000_000i128, 1i128..=1_000_000_000i128),
            profit_margin in 0i128..=1_000_000i128,
            incentive_bps in 0i128..=5_000,
        ) {
            prop_assume!(c_lo < c_hi);
            let borrow = make_pool(7, 1_000_000, 0);
            let collat = make_pool(7, 1_000_000, incentive_bps);
            let lo = compute_repay_cap_from_collateral(
                max_feasible_repay, c_lo, &borrow, &collat, profit_margin,
            ).unwrap_or(0);
            let hi = compute_repay_cap_from_collateral(
                max_feasible_repay, c_hi, &borrow, &collat, profit_margin,
            ).unwrap_or(0);
            prop_assert!(
                hi >= lo,
                "expected monotonic in collateral: lo({})={} hi({})={}",
                c_lo, lo, c_hi, hi,
            );
        }

        // Property 3: result is bounded above by the collateral cap converted
        // to borrow units (using oracle prices and decimals).
        #[test]
        fn prop_result_bounded_by_collateral_cap(
            max_feasible_repay in 1i128..=1_000_000_000i128,
            available_collateral in 1i128..=1_000_000_000i128,
            profit_margin in 0i128..=1_000i128,
            borrow_price in 1i128..=1_000i128,
            collateral_price in 1i128..=1_000i128,
            incentive_bps in 0i128..=5_000,
        ) {
            let borrow = make_pool(7, borrow_price, 0);
            let collat = make_pool(7, collateral_price, incentive_bps);
            let r = compute_repay_cap_from_collateral(
                max_feasible_repay,
                available_collateral,
                &borrow,
                &collat,
                profit_margin,
            );
            if let Some(v) = r {
                // collateral_cap (in borrow units) =
                //   available_collateral * collateral_price / borrow_price
                // (decimals cancel since both pools are 7 decimals here)
                let cap = available_collateral
                    .saturating_mul(collateral_price)
                    / borrow_price;
                prop_assert!(
                    v <= cap.max(max_feasible_repay),
                    "v={} cap={} max_feasible={}",
                    v, cap, max_feasible_repay,
                );
                prop_assert!(v <= max_feasible_repay);
            }
        }

        // Property 4: extreme inputs do not panic — only return Some/None.
        #[test]
        fn prop_no_panic_on_extreme_inputs(
            max_feasible_repay in 1i128..=i64::MAX as i128,
            available_collateral in 1i128..=i64::MAX as i128,
            profit_margin in 0i128..=i32::MAX as i128,
            borrow_price in 1i128..=1_000_000_000i128,
            collateral_price in 1i128..=1_000_000_000i128,
            incentive_bps in 0i128..=10_000,
        ) {
            let borrow = make_pool(7, borrow_price, 0);
            let collat = make_pool(7, collateral_price, incentive_bps);
            let _ = compute_repay_cap_from_collateral(
                max_feasible_repay,
                available_collateral,
                &borrow,
                &collat,
                profit_margin,
            );
        }
    }
}
