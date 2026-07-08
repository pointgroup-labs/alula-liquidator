//! `EventCodec` impl for `Gateway` plus event-payload XDR parsers.

use {
    super::{
        client::Gateway,
        xdr_codec::{
            ParseError, map_get, parse_obligation, scval_as_map, scval_display, scval_type_name,
        },
    },
    crate::stellar::xdr_codec::parse_liquidation_result,
    anyhow::{Context, anyhow},
    engine::{
        lending_model::{LiquidationResult, Obligation, ObligationKey},
        ports::{EventCodec, OperationEvent},
    },
    stellar_xdr::{Limits, ReadXdr as _, ScMap, ScVal},
};

fn parse_obligation_from_event_value_inner(
    value_xdr_base64: &str, // event value
    obligation_field_name: &str,
    key: &ObligationKey,
) -> anyhow::Result<Option<Obligation>> {
    let val = ScVal::from_xdr_base64(value_xdr_base64.as_bytes(), Limits::none())
        .context("decode event value XDR")?;
    let map = scval_as_map(&val)?;

    match map_get(map, obligation_field_name) {
        None | Some(ScVal::Void) => Ok(None),
        Some(ScVal::Vec(None)) => Ok(None),
        // he gets this
        Some(inner) => {
            let obl = parse_obligation(inner, key)
                .with_context(|| format!("parse obligation field '{obligation_field_name}'"))?;
            Ok(Some(obl))
        }
    }
}

fn parse_liquidation_result_from_liquidation_event_value_inner(
    value_xdr_base64: &str,
) -> anyhow::Result<Option<LiquidationResult>> {
    let val = ScVal::from_xdr_base64(value_xdr_base64.as_bytes(), Limits::none())
        .context("decode event value XDR")?;
    let map = scval_as_map(&val)?;

    match map_get(map, "liquidation_result") {
        None | Some(ScVal::Void) => Ok(None),
        Some(ScVal::Vec(None)) => Ok(None),
        Some(inner) => {
            let liquidation_result = parse_liquidation_result(inner)
                .with_context(|| "parse liquidation_result".to_string())?;

            Ok(Some(liquidation_result))
        }
    }
}

impl EventCodec for Gateway {
    type RawEvent = stellar_rpc_client::Event;

    fn decode_operation(&self, event: &Self::RawEvent) -> anyhow::Result<OperationEvent> {
        if event.topic.is_empty() {
            return Err(ParseError::InvalidXdr {
                reason: "Event has no topics".to_string(),
            }
            .into());
        }

        let val =
            ScVal::from_xdr_base64(event.topic[0].as_bytes(), Limits::none()).map_err(|e| {
                ParseError::InvalidXdr {
                    reason: format!("Failed to decode XDR: {}", e),
                }
            })?;

        match val {
            ScVal::Symbol(sym) => {
                let utf8_str = std::str::from_utf8(sym.0.as_ref())
                    .map_err(|e| ParseError::InvalidUtf8 { source: e })?;
                let op = OperationEvent::try_from(utf8_str)?;
                Ok(op)
            }
            other => Err(ParseError::TypeMismatch {
                expected: "Symbol".to_string(),
                found: scval_type_name(&other).to_string(),
            }
            .into()),
        }
    }

    fn decode_topic(&self, event: &Self::RawEvent, index: usize) -> String {
        if index >= event.topic.len() {
            return "<missing>".into();
        }
        match ScVal::from_xdr_base64(event.topic[index].as_bytes(), Limits::none()) {
            Ok(val) => scval_display(&val),
            Err(_) => "<decode_error>".into(),
        }
    }

    fn parse_obligation_key_from_topic(
        &self,
        event: &Self::RawEvent,
        index: usize,
    ) -> anyhow::Result<ObligationKey> {
        if index >= event.topic.len() {
            anyhow::bail!("topic index {index} out of range");
        }
        let val = ScVal::from_xdr_base64(event.topic[index].as_bytes(), Limits::none())?;
        let ScVal::Map(Some(ScMap(entries))) = &val else {
            anyhow::bail!("topic[{index}] is not a Map");
        };

        let mut user = None;
        let mut seed = None;
        for entry in entries.iter() {
            if let ScVal::Symbol(sym) = &entry.key {
                match sym.0.to_string().as_str() {
                    "user" => {
                        if let ScVal::Address(addr) = &entry.val {
                            user = Some(addr.to_string());
                        }
                    }
                    "seed" => {
                        if let ScVal::Bytes(b) = &entry.val {
                            seed = Some(hex::encode(AsRef::<[u8]>::as_ref(b)));
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(ObligationKey {
            user: user.ok_or_else(|| anyhow!("missing user in ObligationKey"))?,
            seed,
        })
    }

    fn parse_obligation_from_event_value(
        &self,
        value_xdr_base64: &str,
        field_name: &str,
        key: &ObligationKey,
    ) -> anyhow::Result<Option<Obligation>> {
        parse_obligation_from_event_value_inner(value_xdr_base64, field_name, key)
    }

    fn parse_liquidation_result_from_liquidation_event_value(
        &self,
        value_xdr_base64: &str,
    ) -> anyhow::Result<Option<LiquidationResult>> {
        parse_liquidation_result_from_liquidation_event_value_inner(value_xdr_base64)
    }
}
