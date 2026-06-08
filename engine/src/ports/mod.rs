//! Trait surface that adapter crates implement and strategies depend on.
//!
//! Strategies depend on `Arc<dyn Trait>` from these modules and never import
//! adapter types directly.

pub mod operation_builder;
pub mod operation_event;

pub mod event_codec;
pub mod ledger_reader;

pub use event_codec::EventCodec;
pub use ledger_reader::LedgerReader;
pub use operation_builder::{BatchSimulator, OperationBuilder};
pub use operation_event::{OperationEvent, OperationEventError};
