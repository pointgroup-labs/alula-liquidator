//! Strategy implementations. Use `Arc<dyn ChainReader>` for reads (real
//! firewall) and a single concrete `Arc<Gateway>` for the ops / batch-sim /
//! codec slots — Gateway is the only adapter and the generic-trait dance for
//! three associated types per slot wasn't pulling its weight.

mod bad_debt_request_initiator;
mod balancer;
mod liquidator;
mod withdrawer;

pub use bad_debt_request_initiator::{BadDebtRequestInitiator, BadDebtRequestInitiatorConfig};
pub use balancer::{Balancer, BalancerConfig};
pub use liquidator::{Liquidator, LiquidatorConfig};
pub use withdrawer::{Withdrawer, WithdrawerConfig};
