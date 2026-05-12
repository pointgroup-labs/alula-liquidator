//! `engine` — the deterministic core of the keeper.
//!
//! Two top-level modules with **no cross-imports**:
//!
//! * [`lending`] — the protocol model: pure, sync, fast-tested.
//! * [`reactor`] — a generic event-driven runner; knows nothing about lending.
//!
//! Plus a third surface used by the binary crate to plug in I/O:
//!
//! * [`ports`] — trait surface that adapters in `keeper` implement and
//!   strategies depend on. This is the architectural firewall.

pub mod lending;
pub mod ports;
pub mod reactor;
