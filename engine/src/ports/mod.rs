//! Trait surface that adapter crates implement and strategies depend on.
//!
//! Strategies receive `Arc<dyn Trait>` from these modules and never import
//! adapter types directly. That is how we keep chain plumbing out of strategy
//! code.
//!
//! Three traits cover the whole adapter surface:
//!
//! * [`ChainReader`] — async reads of market state, obligations, balances,
//!   swap quotes, and liquidation dry-runs.
//! * [`OpBuilder`] / [`BatchSimulator`] — sync construction of raw chain
//!   operations and async dry-run of bundled batches.
//! * [`EventCodec`] — sync decoding of raw chain events into the engine's
//!   `OperationEvent` vocabulary.

pub mod chain_reader;
pub mod event_codec;
pub mod op_builder;
pub mod operation_event;

pub use chain_reader::ChainReader;
pub use event_codec::EventCodec;
pub use op_builder::{BatchSimulator, OpBuilder};
pub use operation_event::{OperationEvent, OperationEventError};
