//! Pool data with j-token / d-token / underlying conversion math.

use crate::lending::amount::{DToken, JToken, Underlying};

#[derive(Debug, Clone)]
pub struct PoolData {
    pub total_supply: i128,
    pub open_ltv_bps: i128,
    pub close_ltv_bps: i128,
    pub token_decimals: u32,
    pub token_symbol: String,
    pub pool_address: String,
    pub total_borrowed: i128,
    pub total_d_tokens: i128,
    pub total_j_tokens: i128,
    pub token_address: String,
    pub total_available: i128,
    pub total_collateral: i128,
    pub oracle_asset_price: i128,
    pub flash_loan_fee_bps: i128,
    pub liability_factor_bps: i128,
    pub d_token_rate_ceil_bps: i128,
    pub j_token_rate_floor_bps: i128,
    pub total_available_adjusted: i128,
    pub utilization_ratio_limit_bps: i128,
    pub liquidation_close_factor_bps: i128,
    pub max_liquidation_incentive_bps: i128,
}

impl PoolData {
    pub fn utilization_ratio_bps(&self) -> i128 {
        let total_supply = self.total_borrowed + self.total_available_adjusted;
        if total_supply == 0 {
            0
        } else {
            (self.total_borrowed * 10_000) / total_supply
        }
    }

    pub fn j_tokens_to_tokens_floor(&self, j_tokens: i128) -> i128 {
        if self.total_j_tokens == 0 {
            return 0;
        }

        j_tokens
            .saturating_mul(self.total_supply)
            .saturating_div(self.total_j_tokens)
    }

    pub fn j_tokens_to_tokens_ceil(&self, j_tokens: i128) -> i128 {
        if self.total_j_tokens == 0 {
            return 0;
        }

        let numerator = j_tokens.saturating_mul(self.total_supply);
        let denominator = self.total_j_tokens;

        numerator
            .saturating_add(denominator - 1)
            .saturating_div(denominator)
    }

    pub fn tokens_to_j_tokens_floor(&self, tokens: i128) -> i128 {
        if self.total_supply == 0 {
            // 1:1 if no deposits
            return tokens;
        }

        tokens
            .saturating_mul(self.total_j_tokens)
            .saturating_div(self.total_supply)
    }

    pub fn tokens_to_j_tokens_ceil(&self, tokens: i128) -> i128 {
        if self.total_supply == 0 {
            // 1:1 ratio if no deposits
            return tokens;
        }

        let numerator = tokens.saturating_mul(self.total_j_tokens);
        let denominator = self.total_supply;

        numerator
            .saturating_add(denominator - 1)
            .saturating_div(denominator)
    }

    pub fn d_tokens_to_tokens_floor(&self, d_tokens: i128) -> i128 {
        if self.total_d_tokens == 0 {
            return 0;
        }

        d_tokens
            .saturating_mul(self.total_borrowed)
            .saturating_div(self.total_d_tokens)
    }

    pub fn d_tokens_to_tokens_ceil(&self, d_tokens: i128) -> i128 {
        if self.total_d_tokens == 0 {
            return 0;
        }

        let numerator = d_tokens.saturating_mul(self.total_borrowed);
        let denominator = self.total_d_tokens;

        numerator
            .saturating_add(denominator - 1)
            .saturating_div(denominator)
    }

    pub fn tokens_to_d_tokens_floor(&self, tokens: i128) -> i128 {
        if self.total_borrowed == 0 {
            // 1:1 ratio if no borrows
            return tokens;
        }

        tokens
            .saturating_mul(self.total_d_tokens)
            .saturating_div(self.total_borrowed)
    }

    pub fn tokens_to_d_tokens_ceil(&self, tokens: i128) -> i128 {
        if self.total_borrowed == 0 {
            // 1:1 ratio if no borrows
            return tokens;
        }

        let numerator = tokens.saturating_mul(self.total_d_tokens);
        let denominator = self.total_borrowed;

        numerator
            .saturating_add(denominator - 1)
            .saturating_div(denominator)
    }
}

// --- Typed conversion layer ----------------------------------------------------
// Strategies in step 7 should migrate to these methods. They surface the
// j-token <-> underlying inversion bug at compile time.

impl PoolData {
    pub fn j_to_underlying_floor(&self, j: JToken) -> Underlying {
        Underlying(self.j_tokens_to_tokens_floor(j.raw()))
    }
    pub fn j_to_underlying_ceil(&self, j: JToken) -> Underlying {
        Underlying(self.j_tokens_to_tokens_ceil(j.raw()))
    }
    pub fn underlying_to_j_floor(&self, u: Underlying) -> JToken {
        JToken(self.tokens_to_j_tokens_floor(u.raw()))
    }
    pub fn underlying_to_j_ceil(&self, u: Underlying) -> JToken {
        JToken(self.tokens_to_j_tokens_ceil(u.raw()))
    }
    pub fn d_to_underlying_floor(&self, d: DToken) -> Underlying {
        Underlying(self.d_tokens_to_tokens_floor(d.raw()))
    }
    pub fn d_to_underlying_ceil(&self, d: DToken) -> Underlying {
        Underlying(self.d_tokens_to_tokens_ceil(d.raw()))
    }
    pub fn underlying_to_d_floor(&self, u: Underlying) -> DToken {
        DToken(self.tokens_to_d_tokens_floor(u.raw()))
    }
    pub fn underlying_to_d_ceil(&self, u: Underlying) -> DToken {
        DToken(self.tokens_to_d_tokens_ceil(u.raw()))
    }
}

impl PoolData {
    /// Maximum amount of underlying tokens that can be withdrawn while
    /// keeping pool utilization at-or-below
    /// `utilization_ratio_limit_bps - safety_margin_bps`.
    ///
    /// Returns [`Underlying::ZERO`] when the pool is already past the safe
    /// utilization band, when the safe band is degenerate (≤ 0), or when
    /// the supply is below the implied min-supply floor. The zero-denominator
    /// guard makes this safe to call on freshly initialized or misconfigured
    /// pools — the original `pipeline` code panicked on
    /// `total_borrowed * BPS_FACTOR / 0`.
    pub fn compute_max_safe_withdrawal(&self, safety_margin_bps: i128) -> Underlying {
        let current_utilization_bps = self.utilization_ratio_bps();
        let utilization_considered_safe = self
            .utilization_ratio_limit_bps
            .saturating_sub(safety_margin_bps);

        if utilization_considered_safe <= 0 {
            return Underlying::ZERO;
        }
        if current_utilization_bps >= utilization_considered_safe {
            return Underlying::ZERO;
        }

        let min_allowed_total_supply = self
            .total_borrowed
            .saturating_mul(crate::lending::bps::BPS_DENOMINATOR)
            / utilization_considered_safe;

        Underlying(
            self.total_supply
                .saturating_sub(min_allowed_total_supply)
                .max(0),
        )
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
            total_borrowed: 1_000_000_0000000, // 1M tokens borrowed (7 decimals)
            total_d_tokens: 900_000_0000000,   // 900k dTokens (interest accrued)
            total_j_tokens: 5_000_000_0000000, // 5M jTokens
            total_available: 4_000_000_0000000, // 4M tokens available
            total_available_adjusted: 4_000_000_0000000,
            total_supply: 5_000_000_0000000, // 7M
            total_collateral: 3_000_000_0000000,
            j_token_rate_floor_bps: 100,
            d_token_rate_ceil_bps: 200,
            oracle_asset_price: 1_0000000, // $1
            open_ltv_bps: 8000,            // 80%
            close_ltv_bps: 8500,           // 85%
            liability_factor_bps: 10000,
            liquidation_close_factor_bps: 5000,  // 50%
            max_liquidation_incentive_bps: 1000, // 10%
            flash_loan_fee_bps: 5,               // 0.05%
            utilization_ratio_limit_bps: 9000,   // 90%
        }
    }

    #[test]
    fn test_j_tokens_to_tokens_floor() {
        let pool = create_test_pool();
        let tokens = pool.j_tokens_to_tokens_floor(1_000_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        let tokens = pool.j_tokens_to_tokens_floor(2_500_000_0000000);
        assert_eq!(tokens, 2_500_000_0000000);
    }

    #[test]
    fn test_j_tokens_to_tokens_ceil() {
        let pool = create_test_pool();
        let tokens = pool.j_tokens_to_tokens_ceil(1_000_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        let tokens = pool.j_tokens_to_tokens_ceil(1);
        assert!(tokens >= 1);
    }

    #[test]
    fn test_tokens_to_j_tokens_floor() {
        let pool = create_test_pool();
        let j_tokens = pool.tokens_to_j_tokens_floor(1_000_000_0000000);
        assert_eq!(j_tokens, 1_000_000_0000000);
    }

    #[test]
    fn test_tokens_to_j_tokens_ceil() {
        let pool = create_test_pool();
        let j_tokens = pool.tokens_to_j_tokens_ceil(1_000_000_0000000);
        assert_eq!(j_tokens, 1_000_000_0000000);
    }

    #[test]
    fn test_d_tokens_to_tokens_floor() {
        let pool = create_test_pool();
        let tokens = pool.d_tokens_to_tokens_floor(900_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        let tokens = pool.d_tokens_to_tokens_floor(450_000_0000000);
        assert_eq!(tokens, 500_000_0000000);
    }

    #[test]
    fn test_d_tokens_to_tokens_ceil() {
        let pool = create_test_pool();
        let tokens = pool.d_tokens_to_tokens_ceil(900_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        let tokens = pool.d_tokens_to_tokens_ceil(1);
        assert!(tokens >= 1);
    }

    #[test]
    fn test_tokens_to_d_tokens_floor() {
        let pool = create_test_pool();
        let d_tokens = pool.tokens_to_d_tokens_floor(1_000_000_0000000);
        assert_eq!(d_tokens, 900_000_0000000);
    }

    #[test]
    fn test_tokens_to_d_tokens_ceil() {
        let pool = create_test_pool();
        let d_tokens = pool.tokens_to_d_tokens_ceil(1_000_000_0000000);
        assert_eq!(d_tokens, 900_000_0000000);
    }

    #[test]
    fn test_zero_inputs() {
        let pool = create_test_pool();

        assert_eq!(pool.j_tokens_to_tokens_floor(0), 0);
        assert_eq!(pool.j_tokens_to_tokens_ceil(0), 0);
        assert_eq!(pool.tokens_to_j_tokens_floor(0), 0);
        assert_eq!(pool.tokens_to_j_tokens_ceil(0), 0);

        assert_eq!(pool.d_tokens_to_tokens_floor(0), 0);
        assert_eq!(pool.d_tokens_to_tokens_ceil(0), 0);
        assert_eq!(pool.tokens_to_d_tokens_floor(0), 0);
        assert_eq!(pool.tokens_to_d_tokens_ceil(0), 0);
    }

    #[test]
    fn test_empty_pool_edge_cases() {
        let mut pool = create_test_pool();
        pool.total_j_tokens = 0;
        pool.total_available = 0;
        pool.total_borrowed = 0;
        // Test author intent: an empty pool. Without zeroing total_supply too,
        // the 1:1 fallback in tokens_to_j_tokens never fires. Pre-existing bug
        // in the pipeline copy of this test.
        pool.total_supply = 0;

        assert_eq!(pool.j_tokens_to_tokens_floor(1000), 0);
        assert_eq!(pool.j_tokens_to_tokens_ceil(1000), 0);

        assert_eq!(pool.tokens_to_j_tokens_floor(1000), 1000);
        assert_eq!(pool.tokens_to_j_tokens_ceil(1000), 1000);
    }

    #[test]
    fn test_roundtrip_conversions() {
        let pool = create_test_pool();

        let j_tokens = 1_000_000_0000000;
        let tokens = pool.j_tokens_to_tokens_floor(j_tokens);
        let j_tokens_back = pool.tokens_to_j_tokens_floor(tokens);
        assert_eq!(j_tokens, j_tokens_back);

        let d_tokens = 900_000_0000000;
        let tokens = pool.d_tokens_to_tokens_floor(d_tokens);
        let d_tokens_back = pool.tokens_to_d_tokens_floor(tokens);
        assert_eq!(d_tokens, d_tokens_back);
    }

    // -------- Property tests ------------------------------------------------

    use proptest::prelude::*;

    fn arb_pool() -> impl Strategy<Value = PoolData> {
        (
            1i128..=1_000_000_000_000_000,
            1i128..=1_000_000_000_000_000,
            1i128..=1_000_000_000_000_000,
            1i128..=1_000_000_000_000_000,
        )
            .prop_map(|(total_supply, total_j, total_d, total_borrowed)| {
                let mut p = create_test_pool();
                p.total_supply = total_supply;
                p.total_j_tokens = total_j;
                p.total_d_tokens = total_d;
                p.total_borrowed = total_borrowed;
                p
            })
    }

    proptest! {
        #[test]
        fn prop_j_round_trip_floor(
            pool in arb_pool(),
            j in 1i128..=1_000_000_000_000_000,
        ) {
            let tokens = pool.j_tokens_to_tokens_floor(j);
            let back = pool.tokens_to_j_tokens_floor(tokens);
            prop_assert!(back <= j, "floor∘floor should not increase: j={} back={}", j, back);
        }

        #[test]
        fn prop_j_round_trip_ceil(
            pool in arb_pool(),
            j in 1i128..=1_000_000_000_000_000,
        ) {
            let tokens = pool.j_tokens_to_tokens_ceil(j);
            let back = pool.tokens_to_j_tokens_ceil(tokens);
            prop_assert!(back >= j, "ceil∘ceil should not decrease: j={} back={}", j, back);
        }

        #[test]
        fn prop_j_floor_le_ceil(
            pool in arb_pool(),
            j in 1i128..=1_000_000_000_000_000,
        ) {
            let f = pool.j_tokens_to_tokens_floor(j);
            let c = pool.j_tokens_to_tokens_ceil(j);
            prop_assert!(f <= c);
            prop_assert!(c - f <= 1);
        }

        #[test]
        fn prop_j_monotonic(
            pool in arb_pool(),
            (a, b) in (1i128..=1_000_000_000_000_000, 1i128..=1_000_000_000_000_000),
        ) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(pool.j_tokens_to_tokens_floor(lo) <= pool.j_tokens_to_tokens_floor(hi));
            prop_assert!(pool.j_tokens_to_tokens_ceil(lo) <= pool.j_tokens_to_tokens_ceil(hi));
        }

        #[test]
        fn prop_d_round_trip_floor(
            pool in arb_pool(),
            d in 1i128..=1_000_000_000_000_000,
        ) {
            let tokens = pool.d_tokens_to_tokens_floor(d);
            let back = pool.tokens_to_d_tokens_floor(tokens);
            prop_assert!(back <= d);
        }

        #[test]
        fn prop_d_round_trip_ceil(
            pool in arb_pool(),
            d in 1i128..=1_000_000_000_000_000,
        ) {
            let tokens = pool.d_tokens_to_tokens_ceil(d);
            let back = pool.tokens_to_d_tokens_ceil(tokens);
            prop_assert!(back >= d);
        }

        #[test]
        fn prop_d_floor_le_ceil(
            pool in arb_pool(),
            d in 1i128..=1_000_000_000_000_000,
        ) {
            let f = pool.d_tokens_to_tokens_floor(d);
            let c = pool.d_tokens_to_tokens_ceil(d);
            prop_assert!(f <= c);
            prop_assert!(c - f <= 1);
        }

        #[test]
        fn prop_d_monotonic(
            pool in arb_pool(),
            (a, b) in (1i128..=1_000_000_000_000_000, 1i128..=1_000_000_000_000_000),
        ) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(pool.d_tokens_to_tokens_floor(lo) <= pool.d_tokens_to_tokens_floor(hi));
            prop_assert!(pool.d_tokens_to_tokens_ceil(lo) <= pool.d_tokens_to_tokens_ceil(hi));
        }

        #[test]
        fn prop_typed_wrappers_match(
            pool in arb_pool(),
            v in 1i128..=1_000_000_000_000_000,
        ) {
            prop_assert_eq!(
                pool.j_to_underlying_floor(JToken(v)).raw(),
                pool.j_tokens_to_tokens_floor(v),
            );
            prop_assert_eq!(
                pool.j_to_underlying_ceil(JToken(v)).raw(),
                pool.j_tokens_to_tokens_ceil(v),
            );
            prop_assert_eq!(
                pool.underlying_to_j_floor(Underlying(v)).raw(),
                pool.tokens_to_j_tokens_floor(v),
            );
            prop_assert_eq!(
                pool.underlying_to_j_ceil(Underlying(v)).raw(),
                pool.tokens_to_j_tokens_ceil(v),
            );
            prop_assert_eq!(
                pool.d_to_underlying_floor(DToken(v)).raw(),
                pool.d_tokens_to_tokens_floor(v),
            );
            prop_assert_eq!(
                pool.d_to_underlying_ceil(DToken(v)).raw(),
                pool.d_tokens_to_tokens_ceil(v),
            );
            prop_assert_eq!(
                pool.underlying_to_d_floor(Underlying(v)).raw(),
                pool.tokens_to_d_tokens_floor(v),
            );
            prop_assert_eq!(
                pool.underlying_to_d_ceil(Underlying(v)).raw(),
                pool.tokens_to_d_tokens_ceil(v),
            );
        }
    }
}
