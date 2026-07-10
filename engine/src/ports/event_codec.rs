//! `EventCodec` — sync port for decoding raw chain events into the engine's
//! `OperationEvent` vocabulary, plus extracting obligation keys and embedded
//! XDR-encoded payloads.

use anyhow::Result;

use crate::{
    lending_model::{LiquidationResult, Obligation, ObligationKey},
    ports::operation_event::OperationEvent,
};

pub trait EventCodec: Send + Sync {
    /// Opaque raw event type (e.g. `stellar_rpc_client::Event`).
    type RawEvent: Send + Sync;

    fn decode_operation(&self, event: &Self::RawEvent) -> Result<OperationEvent>;

    fn decode_topic(&self, event: &Self::RawEvent, index: usize) -> String;

    fn parse_obligation_key_from_topic(
        &self,
        event: &Self::RawEvent,
        index: usize,
    ) -> Result<ObligationKey>;

    /// Returns `Ok(None)` when `field_name` is present-but-empty (e.g.
    /// obligation was deleted by the operation), `Ok(Some(_))` when it
    /// decoded successfully, and `Err` only on parse failure.
    fn parse_obligation_from_event_value(
        &self,
        value_xdr_base64: &str,
        field_name: &str,
        key: &ObligationKey,
    ) -> Result<Option<Obligation>>;

    fn parse_liquidation_result_from_liquidation_event_value(
        &self,
        value_xdr_base64: &str,
    ) -> Result<Option<LiquidationResult>>;
}
