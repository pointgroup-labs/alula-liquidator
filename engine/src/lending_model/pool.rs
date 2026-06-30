//! Pool data with j-token / d-token / underlying conversion math.

use crate::lending_model::{
    BPS_FACTOR,
    amount::{DTokens, JTokens, Underlying},
    error::{LMError, MapArithmeticError},
};

#[derive(Debug, Clone)]
pub struct PoolData {
    pub total_supply: Underlying,
    pub open_ltv_bps: i128,
    pub close_ltv_bps: i128,
    pub token_decimals: u32,
    pub token_symbol: String,
    pub pool_address: String,
    pub total_borrowed: Underlying,
    pub total_d_tokens: DTokens,
    pub total_j_tokens: JTokens,
    pub token_address: String,
    pub total_available: Underlying,
    pub total_collateral: Underlying,
    pub oracle_asset_price: i128,
    pub flash_loan_fee_bps: i128,
    pub liability_factor_bps: i128,
    pub d_token_rate_ceil_bps: i128,
    pub j_token_rate_floor_bps: i128,
    pub total_available_adjusted: Underlying,
    pub utilization_ratio_limit_bps: i128,
    pub liquidation_close_factor_bps: i128,
    pub max_liquidation_incentive_bps: i128,
}

impl PoolData {
    pub fn utilization_ratio_bps(&self) -> Result<i128, LMError> {
        if self.total_supply == Underlying::ZERO {
            Ok(0)
        } else {
            self.total_borrowed.bps_fixed_div_floor(self.total_supply.0)
        }
    }

    pub fn available_for_borrow(&self) -> Result<Underlying, LMError> {
        let utilization_ratio_bps = self.utilization_ratio_bps()?;

        if utilization_ratio_bps >= self.utilization_ratio_limit_bps {
            return Ok(Underlying::ZERO);
        }
        let utilization_ratio_left_bps = self.utilization_ratio_limit_bps - utilization_ratio_bps; // safe

        let res = self
            .total_supply
            .0
            .saturating_mul(utilization_ratio_left_bps)
            .saturating_div(BPS_FACTOR);

        Ok(Underlying(res))
    }

    pub fn j_tokens_to_tokens_floor(&self, j_tokens: JTokens) -> Result<Underlying, LMError> {
        if self.total_j_tokens == JTokens::ZERO {
            return Ok(Underlying(0));
        }

        let denominator = self.total_j_tokens.0;
        let numerator = j_tokens.checked_mul(self.total_supply.0)?;

        let res = numerator / denominator;

        Ok(Underlying(res))
    }

    pub fn j_tokens_to_tokens_ceil(&self, j_tokens: JTokens) -> Result<Underlying, LMError> {
        if self.total_j_tokens == JTokens::ZERO {
            return Ok(Underlying(0));
        }

        let denominator = self.total_j_tokens.0;
        let numerator = j_tokens.checked_mul(self.total_supply.0)?;
        let numerator_adjusted = numerator.checked_add(denominator - 1).m_ou()?;

        let res = numerator_adjusted / denominator;

        Ok(Underlying(res))
    }

    pub fn tokens_to_j_tokens_floor(&self, tokens: Underlying) -> Result<JTokens, LMError> {
        if self.total_supply == Underlying::ZERO {
            // 1:1 if no deposits
            return Ok(JTokens(tokens.0));
        }

        let denominator = self.total_supply.0;
        let numerator = tokens.checked_mul(self.total_j_tokens.0)?;

        let res = numerator / denominator;

        Ok(JTokens(res))
    }

    pub fn tokens_to_j_tokens_ceil(&self, tokens: Underlying) -> Result<JTokens, LMError> {
        if self.total_supply == Underlying::ZERO {
            // 1:1 if no deposits
            return Ok(JTokens(tokens.0));
        }

        let denominator = self.total_supply.0;
        let numerator = tokens.checked_mul(self.total_j_tokens.0)?;
        let numerator_adjusted = numerator.checked_add(denominator - 1).m_ou()?;

        let res = numerator_adjusted / denominator;

        Ok(JTokens(res))
    }

    pub fn d_tokens_to_tokens_floor(&self, d_tokens: DTokens) -> Result<Underlying, LMError> {
        if self.total_d_tokens == DTokens::ZERO {
            return Ok(Underlying(0));
        }

        let denominator = self.total_d_tokens.0;
        let numerator = d_tokens.checked_mul(self.total_borrowed.0)?;

        let res = numerator / denominator;

        Ok(Underlying(res))
    }

    pub fn d_tokens_to_tokens_ceil(&self, d_tokens: DTokens) -> Result<Underlying, LMError> {
        if self.total_d_tokens == DTokens::ZERO {
            return Ok(Underlying(0));
        }

        let denominator = self.total_d_tokens.0;
        let numerator = d_tokens.checked_mul(self.total_borrowed.0)?;
        let numerator_adjusted = numerator.checked_add(denominator - 1).m_ou()?;

        let res = numerator_adjusted / denominator;

        Ok(Underlying(res))
    }

    pub fn tokens_to_d_tokens_floor(&self, tokens: Underlying) -> Result<DTokens, LMError> {
        if self.total_borrowed == Underlying::ZERO {
            // 1:1 if no borrows
            return Ok(DTokens(tokens.0));
        }

        let denominator = self.total_borrowed.0;
        let numerator = tokens.checked_mul(self.total_d_tokens.0)?;

        let res = numerator / denominator;

        Ok(DTokens(res))
    }

    pub fn tokens_to_d_tokens_ceil(&self, tokens: Underlying) -> Result<DTokens, LMError> {
        if self.total_borrowed == Underlying::ZERO {
            // 1:1 if no borrows
            return Ok(DTokens(tokens.0));
        }

        let denominator = self.total_borrowed.0;
        let numerator = tokens.checked_mul(self.total_d_tokens.0)?;
        let numerator_adjusted = numerator.checked_add(denominator - 1).m_ou()?;

        let res = numerator_adjusted / denominator;

        Ok(DTokens(res))
    }
}

impl PoolData {
    /// Maximum amount of underlying tokens that can be withdrawn while
    /// keeping pool utilization at-or-below
    /// `utilization_ratio_limit_bps - safety_margin_bps`.
    ///
    /// `min_allowed_total_supply` is the **ceiling** of
    /// `total_borrowed × BPS / safe_utilization`: the contract recomputes
    /// post-withdrawal utilization with `fixed_div_ceil`, so flooring here
    /// could leave utilization a rounding hair above the safe band and burn
    /// the scarcity fee.
    ///
    /// Returns [`Underlying::ZERO`] when the pool is already past the safe
    /// utilization band, when the safe band is degenerate (≤ 0), or when
    /// the supply is at-or-below the implied min-supply floor. The
    /// zero-denominator guard makes this safe to call on freshly initialized
    /// or misconfigured pools — the original `pipeline` code panicked on
    /// `total_borrowed * BPS_FACTOR / 0`.
    pub fn compute_max_safe_withdrawal(
        &self,
        safety_margin_bps: i128,
    ) -> Result<Underlying, LMError> {
        let current_utilization_bps = self.utilization_ratio_bps()?;
        let utilization_considered_safe = self
            .utilization_ratio_limit_bps
            .saturating_sub(safety_margin_bps);

        if utilization_considered_safe <= 0 {
            return Ok(Underlying::ZERO);
        }
        if current_utilization_bps >= utilization_considered_safe {
            return Ok(Underlying::ZERO);
        }

        let min_allowed_total_supply = self
            .total_borrowed
            .bps_fixed_div_ceil(utilization_considered_safe)?;

        if min_allowed_total_supply >= self.total_supply.0 {
            Ok(Underlying::ZERO)
        } else {
            Ok(self.total_supply - Underlying(min_allowed_total_supply))
        }
    }
}

#[cfg(test)]
mod tests {
    // Stellar amounts are denominated in stroops (1 unit = 10^7 stroops).
    // Test literals deliberately separate the whole-unit portion from the
    // stroop suffix, e.g. `1_000_000_0000000`, which clippy reads as
    // inconsistent digit grouping. The convention is load-bearing for
    // readability against the contract spec — keep it.
    #![allow(clippy::inconsistent_digit_grouping)]

    use super::*;

    fn create_test_pool() -> PoolData {
        PoolData {
            pool_address: "test_pool".to_string(),
            token_address: "test_token".to_string(),
            token_symbol: "TST".to_string(),
            token_decimals: 7,
            total_borrowed: 1_000_000_0000000.into(),
            total_d_tokens: 900_000_0000000.into(),
            total_j_tokens: 5_000_000_0000000.into(),
            total_available: 4_000_000_0000000.into(),
            total_available_adjusted: 4_000_000_0000000.into(),
            total_supply: 5_000_000_0000000.into(),
            total_collateral: 3_000_000_0000000.into(),
            j_token_rate_floor_bps: 100,
            d_token_rate_ceil_bps: 200,
            oracle_asset_price: 1_0000000,
            open_ltv_bps: 8000,
            close_ltv_bps: 8500,
            liability_factor_bps: 10000,
            liquidation_close_factor_bps: 5000,
            max_liquidation_incentive_bps: 1000,
            flash_loan_fee_bps: 5,
            utilization_ratio_limit_bps: 9000,
        }
    }

    #[test]
    fn test_j_tokens_to_tokens_floor() -> Result<(), LMError> {
        let pool = create_test_pool();
        let tokens = pool.j_tokens_to_tokens_floor(1_000_000_0000000.into())?;
        assert_eq!(tokens, 1_000_000_0000000.into());

        let tokens = pool.j_tokens_to_tokens_floor(2_500_000_0000000.into())?;
        assert_eq!(tokens, 2_500_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_j_tokens_to_tokens_ceil() -> Result<(), LMError> {
        let pool = create_test_pool();
        let tokens = pool.j_tokens_to_tokens_ceil(1_000_000_0000000.into())?;
        assert_eq!(tokens, 1_000_000_0000000.into());

        let tokens = pool.j_tokens_to_tokens_ceil(1.into())?;
        assert!(tokens >= 1.into());

        Ok(())
    }

    #[test]
    fn test_tokens_to_j_tokens_floor() -> Result<(), LMError> {
        let pool = create_test_pool();
        let j_tokens = pool.tokens_to_j_tokens_floor(1_000_000_0000000.into())?;
        assert_eq!(j_tokens, 1_000_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_tokens_to_j_tokens_ceil() -> Result<(), LMError> {
        let pool = create_test_pool();
        let j_tokens = pool.tokens_to_j_tokens_ceil(1_000_000_0000000.into())?;
        assert_eq!(j_tokens, 1_000_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_d_tokens_to_tokens_floor() -> Result<(), LMError> {
        let pool = create_test_pool();
        let tokens = pool.d_tokens_to_tokens_floor(900_000_0000000.into())?;
        assert_eq!(tokens, 1_000_000_0000000.into());

        let tokens = pool.d_tokens_to_tokens_floor(450_000_0000000.into())?;
        assert_eq!(tokens, 500_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_d_tokens_to_tokens_ceil() -> Result<(), LMError> {
        let pool = create_test_pool();
        let tokens = pool.d_tokens_to_tokens_ceil(900_000_0000000.into())?;
        assert_eq!(tokens, 1_000_000_0000000.into());

        let tokens = pool.d_tokens_to_tokens_ceil(1.into())?;
        assert!(tokens >= 1.into());

        Ok(())
    }

    #[test]
    fn test_tokens_to_d_tokens_floor() -> Result<(), LMError> {
        let pool = create_test_pool();
        let d_tokens = pool.tokens_to_d_tokens_floor(1_000_000_0000000.into())?;
        assert_eq!(d_tokens, 900_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_tokens_to_d_tokens_ceil() -> Result<(), LMError> {
        let pool = create_test_pool();
        let d_tokens = pool.tokens_to_d_tokens_ceil(1_000_000_0000000.into())?;
        assert_eq!(d_tokens, 900_000_0000000.into());

        Ok(())
    }

    #[test]
    fn test_zero_inputs() -> Result<(), LMError> {
        let pool = create_test_pool();

        assert_eq!(pool.j_tokens_to_tokens_floor(0.into())?, 0.into());
        assert_eq!(pool.j_tokens_to_tokens_ceil(0.into())?, 0.into());
        assert_eq!(pool.tokens_to_j_tokens_floor(0.into())?, 0.into());
        assert_eq!(pool.tokens_to_j_tokens_ceil(0.into())?, 0.into());

        assert_eq!(pool.d_tokens_to_tokens_floor(0.into())?, 0.into());
        assert_eq!(pool.d_tokens_to_tokens_ceil(0.into())?, 0.into());
        assert_eq!(pool.tokens_to_d_tokens_floor(0.into())?, 0.into());
        assert_eq!(pool.tokens_to_d_tokens_ceil(0.into())?, 0.into());

        Ok(())
    }

    #[test]
    fn test_empty_pool_edge_cases() -> Result<(), LMError> {
        let mut pool = create_test_pool();
        pool.total_j_tokens = 0.into();
        pool.total_available = 0.into();
        pool.total_borrowed = 0.into();
        // Test author intent: an empty pool. Without zeroing total_supply too,
        // the 1:1 fallback in tokens_to_j_tokens never fires. Pre-existing bug
        // in the pipeline copy of this test.
        pool.total_supply = 0.into();

        assert_eq!(pool.j_tokens_to_tokens_floor(1000.into())?, 0.into());
        assert_eq!(pool.j_tokens_to_tokens_ceil(1000.into())?, 0.into());

        assert_eq!(pool.tokens_to_j_tokens_floor(1000.into())?, 1000.into());
        assert_eq!(pool.tokens_to_j_tokens_ceil(1000.into())?, 1000.into());

        Ok(())
    }

    #[test]
    fn test_roundtrip_conversions() -> Result<(), LMError> {
        let pool = create_test_pool();

        let j_tokens = 1_000_000_0000000.into();
        let tokens = pool.j_tokens_to_tokens_floor(j_tokens)?;
        let j_tokens_back = pool.tokens_to_j_tokens_floor(tokens)?;
        assert_eq!(j_tokens, j_tokens_back);

        let d_tokens = 900_000_0000000.into();
        let tokens = pool.d_tokens_to_tokens_floor(d_tokens)?;
        let d_tokens_back = pool.tokens_to_d_tokens_floor(tokens)?;
        assert_eq!(d_tokens, d_tokens_back);

        Ok(())
    }

    // Regression: the original comparison was inverted (`min_allowed <=
    // supply` → ZERO), which made every reachable case return ZERO and the
    // boundary case return a negative amount — the Withdrawer never withdrew.
    #[test]
    fn test_max_safe_withdrawal_positive_and_tight() -> Result<(), LMError> {
        use crate::lending_model::amount::bps_fixed_div_ceil;

        // create_test_pool: borrowed 1M, supply 5M → 2_000 bps utilization,
        // limit 9_000 bps.
        let pool = create_test_pool();
        let margin = 100;
        let safe = pool.utilization_ratio_limit_bps - margin;

        let w = pool.compute_max_safe_withdrawal(margin)?;
        assert!(
            w.0 > 0,
            "20% utilization must allow a withdrawal, got {w:?}"
        );
        assert!(w.0 < pool.total_supply.0);

        // Post-withdrawal utilization — recomputed with the contract's ceil
        // division — stays inside the safe band…
        let supply_after = pool.total_supply.0 - w.0;
        let util_after = bps_fixed_div_ceil(pool.total_borrowed.0, supply_after).unwrap();
        assert!(util_after <= safe, "util_after={util_after} > safe={safe}");

        // …and withdrawing a single extra token would breach it (the cap is
        // tight, not just safe).
        let util_breach = bps_fixed_div_ceil(pool.total_borrowed.0, supply_after - 1).unwrap();
        assert!(
            util_breach > safe,
            "cap is not tight: {util_breach} <= {safe}"
        );

        Ok(())
    }

    #[test]
    fn test_max_safe_withdrawal_zero_cases() -> Result<(), LMError> {
        // Already above the safe band: 4.6M / 5M = 9_200 bps ≥ 9_000.
        let mut over = create_test_pool();
        over.total_borrowed = 4_600_000_0000000.into();
        assert_eq!(over.compute_max_safe_withdrawal(0)?, Underlying::ZERO);

        // Degenerate band: margin consumes the whole limit.
        let pool = create_test_pool();
        let margin = pool.utilization_ratio_limit_bps;
        assert_eq!(pool.compute_max_safe_withdrawal(margin)?, Underlying::ZERO);

        // Nothing borrowed and nothing supplied: min_allowed = 0 = supply.
        let mut empty = create_test_pool();
        empty.total_borrowed = 0.into();
        empty.total_supply = 0.into();
        assert_eq!(empty.compute_max_safe_withdrawal(0)?, Underlying::ZERO);

        Ok(())
    }

    // -------- Property tests ------------------------------------------------

    // use proptest::prelude::*;

    // fn arb_pool() -> impl Strategy<Value = PoolData> {
    //     (
    //         1i128..=1_000_000_000_000_000,
    //         1i128..=1_000_000_000_000_000,
    //         1i128..=1_000_000_000_000_000,
    //         1i128..=1_000_000_000_000_000,
    //     )
    //         .prop_map(|(total_supply, total_j, total_d, total_borrowed)| {
    //             let mut p = create_test_pool();
    //             p.total_supply = total_supply;
    //             p.total_j_tokens = total_j;
    //             p.total_d_tokens = total_d;
    //             p.total_borrowed = total_borrowed;
    //             p
    //         })
    // }

    // proptest! {
    //     #[test]
    //     fn prop_j_round_trip_floor(
    //         pool in arb_pool(),
    //         j in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let tokens = pool.j_tokens_to_tokens_floor(j);
    //         let back = pool.tokens_to_j_tokens_floor(tokens);
    //         prop_assert!(back <= j, "floor∘floor should not increase: j={} back={}", j, back);
    //     }

    //     #[test]
    //     fn prop_j_round_trip_ceil(
    //         pool in arb_pool(),
    //         j in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let tokens = pool.j_tokens_to_tokens_ceil(j);
    //         let back = pool.tokens_to_j_tokens_ceil(tokens);
    //         prop_assert!(back >= j, "ceil∘ceil should not decrease: j={} back={}", j, back);
    //     }

    //     #[test]
    //     fn prop_j_floor_le_ceil(
    //         pool in arb_pool(),
    //         j in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let f = pool.j_tokens_to_tokens_floor(j);
    //         let c = pool.j_tokens_to_tokens_ceil(j);
    //         prop_assert!(f <= c);
    //         prop_assert!(c - f <= 1);
    //     }

    //     #[test]
    //     fn prop_j_monotonic(
    //         pool in arb_pool(),
    //         (a, b) in (1i128..=1_000_000_000_000_000, 1i128..=1_000_000_000_000_000),
    //     ) {
    //         let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    //         prop_assert!(pool.j_tokens_to_tokens_floor(lo) <= pool.j_tokens_to_tokens_floor(hi));
    //         prop_assert!(pool.j_tokens_to_tokens_ceil(lo) <= pool.j_tokens_to_tokens_ceil(hi));
    //     }

    //     #[test]
    //     fn prop_d_round_trip_floor(
    //         pool in arb_pool(),
    //         d in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let tokens = pool.d_tokens_to_tokens_floor(d);
    //         let back = pool.tokens_to_d_tokens_floor(tokens);
    //         prop_assert!(back <= d);
    //     }

    //     #[test]
    //     fn prop_d_round_trip_ceil(
    //         pool in arb_pool(),
    //         d in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let tokens = pool.d_tokens_to_tokens_ceil(d);
    //         let back = pool.tokens_to_d_tokens_ceil(tokens);
    //         prop_assert!(back >= d);
    //     }

    //     #[test]
    //     fn prop_d_floor_le_ceil(
    //         pool in arb_pool(),
    //         d in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         let f = pool.d_tokens_to_tokens_floor(d);
    //         let c = pool.d_tokens_to_tokens_ceil(d);
    //         prop_assert!(f <= c);
    //         prop_assert!(c - f <= 1);
    //     }

    //     #[test]
    //     fn prop_d_monotonic(
    //         pool in arb_pool(),
    //         (a, b) in (1i128..=1_000_000_000_000_000, 1i128..=1_000_000_000_000_000),
    //     ) {
    //         let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    //         prop_assert!(pool.d_tokens_to_tokens_floor(lo) <= pool.d_tokens_to_tokens_floor(hi));
    //         prop_assert!(pool.d_tokens_to_tokens_ceil(lo) <= pool.d_tokens_to_tokens_ceil(hi));
    //     }

    //     #[test]
    //     fn prop_typed_wrappers_match(
    //         pool in arb_pool(),
    //         v in 1i128..=1_000_000_000_000_000,
    //     ) {
    //         prop_assert_eq!(
    //             pool.j_to_underlying_floor(JToken(v)).raw(),
    //             pool.j_tokens_to_tokens_floor(v),
    //         );
    //         prop_assert_eq!(
    //             pool.j_to_underlying_ceil(JToken(v)).raw(),
    //             pool.j_tokens_to_tokens_ceil(v),
    //         );
    //         prop_assert_eq!(
    //             pool.underlying_to_j_floor(Underlying(v)).raw(),
    //             pool.tokens_to_j_tokens_floor(v),
    //         );
    //         prop_assert_eq!(
    //             pool.underlying_to_j_ceil(Underlying(v)).raw(),
    //             pool.tokens_to_j_tokens_ceil(v),
    //         );
    //         prop_assert_eq!(
    //             pool.d_to_underlying_floor(DToken(v)).raw(),
    //             pool.d_tokens_to_tokens_floor(v),
    //         );
    //         prop_assert_eq!(
    //             pool.d_to_underlying_ceil(DToken(v)).raw(),
    //             pool.d_tokens_to_tokens_ceil(v),
    //         );
    //         prop_assert_eq!(
    //             pool.underlying_to_d_floor(Underlying(v)).raw(),
    //             pool.tokens_to_d_tokens_floor(v),
    //         );
    //         prop_assert_eq!(
    //             pool.underlying_to_d_ceil(Underlying(v)).raw(),
    //             pool.tokens_to_d_tokens_ceil(v),
    //         );
    //     }
    // }
}
