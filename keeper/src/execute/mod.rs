//! Concrete `engine::reactor::Executor` implementations for the keeper.
//!
//! The `Action` enum lives here (next to its consumers) rather than in a
//! top-level `wire`/`messages` module: the consumer side owns its alphabet.

pub mod stellar_tx;

use stellar_tx::SubmitStellarTx;

/// Top-level action flowing from strategies → engine → executors.
#[derive(Debug, Clone)]
pub enum Action {
    SubmitTx(SubmitStellarTx),
}
