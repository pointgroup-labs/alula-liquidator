//! Strategy implementations. Use `Arc<dyn ChainReader>` for reads (real
//! firewall) and a single concrete `Arc<Gateway>` for the ops / batch-sim /
//! codec slots — Gateway is the only adapter and the generic-trait dance for
//! three associated types per slot wasn't pulling its weight.

mod bad_debt;
mod balancer;
pub mod capital;
mod liquidator;
mod withdrawer;

pub use bad_debt::{BadDebtRequestInitiator, BadDebtRequestInitiatorConfig};
pub use capital::CapitalLedger;
pub use liquidator::{Liquidator, LiquidatorConfig};
pub use balancer::{Balancer, BalancerConfig};
pub use withdrawer::{Withdrawer, WithdrawerConfig};
