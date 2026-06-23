//! Implementation of `engine::reactor::Executor` for the keeper.

pub mod stellar_tx;
pub mod stellar_tx2;

use stellar_tx2::SubmitStellarTx;

#[derive(Debug, Clone)]
pub enum Action {
    SubmitTx(SubmitStellarTx),
}
