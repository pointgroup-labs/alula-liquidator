use {
    crate::collectors::block_collector::NewBlock,
    crate::executors::tx_executor::SubmitStellarTx,
    anyhow::Result,
    serde::{Deserialize, Serialize},
    std::{future::Future, pin::Pin},
    stellar_rpc_client::Event as SorobanEvent,
    tokio_stream::Stream,
};

pub trait Collector<E>: Send + Sync {
    fn get_event_stream(&mut self) -> BoxFuture<'_, Result<CollectorStream<'_, E>>>;
}

pub trait Strategy<E, A>: Send + Sync {
    fn sync_state(&mut self) -> BoxFuture<'_, Result<()>>;
    fn process_event(&mut self, event: E) -> BoxFuture<'_, Vec<A>>;
}

pub trait Executor<A>: Send + Sync {
    fn execute(&self, action: A) -> BoxFuture<'_, Result<()>>;
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

#[derive(Debug, Clone)]
pub enum Event {
    SorobanEvents(SorobanEvent),
    NewBlock(NewBlock),
}

#[derive(Debug, Clone)]
pub enum Action {
    SubmitTx(SubmitStellarTx),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObligationKey {
    pub user: String,
    pub seed: Option<String>,
}

impl ObligationKey {
    pub fn new(user: String) -> Self {
        Self { user, seed: None }
    }

    pub fn new_with_seed(user: String, seed: String) -> Self {
        Self {
            user,
            seed: Some(seed),
        }
    }

    pub fn seed_as_str(&self) -> &str {
        self.seed.as_deref().unwrap_or("")
    }
}

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
    pub liquidation_close_factor_bps: i128,
    pub max_liquidation_incentive_bps: i128,
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub insolvency_ltv_bps: i128,
    pub pools_data: Vec<PoolData>,
    pub oracle_price_decimals: u32,
    pub min_collateral_value_cents: i128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositPosition {
    pub j_tokens: i128,
    pub collateral: i128,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowPosition {
    pub d_tokens: i128,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Obligation {
    pub borrows: Vec<BorrowPosition>,
    pub deposits: Vec<DepositPosition>,
}

impl PoolData {
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
        if self.total_borrowed == 0{
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

#[cfg(test)]
mod tests {
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
        }
    }

    #[test]
    fn test_j_tokens_to_tokens_floor() {
        let pool = create_test_pool();
        // total_supply = 4M + 1M = 5M
        // j_tokens_to_tokens = (j_tokens * total_supply) / total_j_tokens
        // (1M jTokens * 5M) / 5M = 1M tokens
        let tokens = pool.j_tokens_to_tokens_floor(1_000_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        // Test with half jTokens
        let tokens = pool.j_tokens_to_tokens_floor(2_500_000_0000000);
        assert_eq!(tokens, 2_500_000_0000000);
    }

    #[test]
    fn test_j_tokens_to_tokens_ceil() {
        let pool = create_test_pool();
        let tokens = pool.j_tokens_to_tokens_ceil(1_000_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        // Test rounding up with odd division
        let tokens = pool.j_tokens_to_tokens_ceil(1);
        assert!(tokens >= 1);
    }

    #[test]
    fn test_tokens_to_j_tokens_floor() {
        let pool = create_test_pool();
        // tokens_to_j_tokens = (tokens * total_j_tokens) / total_supply
        // (1M tokens * 5M jTokens) / 5M = 1M jTokens
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
        // d_tokens_to_tokens = (d_tokens * total_borrowed) / total_d_tokens
        // (900k dTokens * 1M borrowed) / 900k dTokens = 1M tokens
        let tokens = pool.d_tokens_to_tokens_floor(900_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        // Test with half dTokens
        let tokens = pool.d_tokens_to_tokens_floor(450_000_0000000);
        assert_eq!(tokens, 500_000_0000000);
    }

    #[test]
    fn test_d_tokens_to_tokens_ceil() {
        let pool = create_test_pool();
        let tokens = pool.d_tokens_to_tokens_ceil(900_000_0000000);
        assert_eq!(tokens, 1_000_000_0000000);

        // Test rounding up
        let tokens = pool.d_tokens_to_tokens_ceil(1);
        assert!(tokens >= 1);
    }

    #[test]
    fn test_tokens_to_d_tokens_floor() {
        let pool = create_test_pool();
        // tokens_to_d_tokens = (tokens * total_d_tokens) / total_borrowed
        // (1M tokens * 900k dTokens) / 1M borrowed = 900k dTokens
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

        // jTokens conversions should return 0 when pool is empty
        assert_eq!(pool.j_tokens_to_tokens_floor(1000), 0);
        assert_eq!(pool.j_tokens_to_tokens_ceil(1000), 0);

        // tokens to jTokens should use 1:1 ratio when pool is empty
        assert_eq!(pool.tokens_to_j_tokens_floor(1000), 1000);
        assert_eq!(pool.tokens_to_j_tokens_ceil(1000), 1000);
    }

    #[test]
    fn test_roundtrip_conversions() {
        let pool = create_test_pool();

        // jTokens roundtrip (floor)
        let j_tokens = 1_000_000_0000000;
        let tokens = pool.j_tokens_to_tokens_floor(j_tokens);
        let j_tokens_back = pool.tokens_to_j_tokens_floor(tokens);
        // Should be equal since pool has 1:1 ratio in this test case
        assert_eq!(j_tokens, j_tokens_back);

        // dTokens roundtrip (floor)
        let d_tokens = 900_000_0000000;
        let tokens = pool.d_tokens_to_tokens_floor(d_tokens);
        let d_tokens_back = pool.tokens_to_d_tokens_floor(tokens);
        assert_eq!(d_tokens, d_tokens_back);
    }
}
