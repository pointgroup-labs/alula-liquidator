//! Pure protocol model. No I/O, no async, no chain types.
//!
//! Code in this module MUST NOT reference `engine::reactor` or `engine::ports`.

pub mod amount;
pub mod bps;
pub mod liquidation;
pub mod market;
pub mod obligation;
pub mod pool;
pub mod profitability;

pub use amount::{DToken, JToken, Underlying};
pub use bps::{BPS_DENOMINATOR, LIQUIDATION_INTEREST_BUFFER_BPS, fixed_mul_ceil, fixed_mul_floor};
pub use market::MarketData;
pub use obligation::{BorrowPosition, DepositPosition, Obligation, ObligationKey};
pub use pool::PoolData;
