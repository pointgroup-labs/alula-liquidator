//! Source-of-truth event collectors for the reactor.
//!
//! Each collector implements `engine::reactor::Collector<Event>` and produces
//! exactly one `Event` variant. The `Event` enum lives here (next to its
//! producers) rather than in a top-level `wire`/`messages` module: the
//! producer side owns its alphabet.

pub mod block;
pub mod soroban_events;

use {
    block::NewBlock,
    metrics::counter,
    stellar_rpc_client::Event as SorobanEvent,
    tokio::sync::broadcast::Receiver,
    tokio_stream::{
        Stream, StreamExt,
        wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
    },
};

/// Top-level event flowing from collectors → engine → strategies.
#[derive(Debug, Clone)]
pub enum Event {
    SorobanEvents(SorobanEvent),
    NewBlock(NewBlock),
}

/// Wrap a `broadcast::Receiver` as a `Stream<Item = T>`, dropping `Lagged`
/// items but counting them in `keeper_collector_lagged_events_total` so the
/// loss is observable in Prometheus instead of silent.
///
/// Each collector owns its own internal `broadcast::channel` to ferry items
/// from its polling task to the reactor; that channel is the only place
/// `tokio_stream::wrappers::BroadcastStream` is constructed in the keeper.
/// Centralising the wrapping here ensures both collectors (and any future
/// one) participate in the same metric without duplicating the match arm.
pub(crate) fn lag_counted_stream<T>(
    receiver: Receiver<T>,
    collector: &'static str,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Clone + Send + 'static,
{
    BroadcastStream::new(receiver).filter_map(move |item| match item {
        Ok(v) => Some(v),
        Err(BroadcastStreamRecvError::Lagged(n)) => {
            counter!("keeper_collector_lagged_events_total", "collector" => collector)
                .increment(n);
            None
        }
    })
}
