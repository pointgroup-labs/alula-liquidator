//! Implementation of `engine::reactor::Executor` for the keeper.

pub mod stellar_tx;

use stellar_tx::SubmitStellarTx;

#[derive(Debug, Clone)]
pub enum Action {
    SubmitTx(SubmitStellarTx),
}
