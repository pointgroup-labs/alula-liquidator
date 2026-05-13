//! Trait surface that adapter crates implement and strategies depend on.
//!
//! Strategies receive `Arc<dyn Trait>` from these modules and never import
//! adapter types directly — that is how chain plumbing stays out of strategy
//! code.

pub mod chain_reader;
pub mod event_codec;
pub mod op_builder;
pub mod operation_event;

pub use chain_reader::ChainReader;
pub use event_codec::EventCodec;
pub use op_builder::{BatchSimulator, OpBuilder};
pub use operation_event::{OperationEvent, OperationEventError};
