//! `OperationEvent` — a closed enum of decoded operation kinds emitted by the
//! lending market. This is the domain-level vocabulary; the raw chain event
//! shape stays in the adapter.

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationEvent {
    Repay,
    Borrow,
    Deposit,
    Withdraw,
    Liquidate,
    AddCollateral,
    RemoveCollateral,
}

#[derive(Debug, Error)]
pub enum OperationEventError {
    #[error("unknown operation event: {0}")]
    Unknown(String),
}

impl TryFrom<&str> for OperationEvent {
    type Error = OperationEventError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        use OperationEvent::*;
        Ok(match s {
            "repay_event" => Repay,
            "borrow_event" => Borrow,
            "deposit_event" => Deposit,
            "withdraw_event" => Withdraw,
            "liquidate_event" => Liquidate,
            "add_collateral_event" => AddCollateral,
            "remove_collateral_event" => RemoveCollateral,
            other => return Err(OperationEventError::Unknown(other.to_string())),
        })
    }
}
