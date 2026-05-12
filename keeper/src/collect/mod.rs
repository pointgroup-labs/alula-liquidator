//! Source-of-truth event collectors for the reactor.
//!
//! Each collector implements `engine::reactor::Collector<Event>` and produces
//! exactly one `Event` variant. The `Event` enum lives here (next to its
//! producers) rather than in a top-level `wire`/`messages` module: the
//! producer side owns its alphabet.

pub mod block;
pub mod soroban_events;

use {block::NewBlock, stellar_rpc_client::Event as SorobanEvent};

/// Top-level event flowing from collectors → engine → strategies.
#[derive(Debug, Clone)]
pub enum Event {
    SorobanEvents(SorobanEvent),
    NewBlock(NewBlock),
}
