//! Pure lending market protocol model. No I/O, no async, no ledger types.

pub mod amount;
pub mod error;
pub mod liquidation;
pub mod market;
pub mod obligation;
pub mod pool;
pub mod profitability;

pub use amount::{BPS_FACTOR, DTokens, JTokens, Underlying};
pub use liquidation::LiquidationResult;
pub use market::MarketData;
pub use obligation::{BorrowPosition, DepositPosition, Obligation, ObligationKey};
pub use pool::PoolData;
