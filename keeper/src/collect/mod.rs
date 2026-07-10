pub mod stellar_event;
pub mod stellar_ledger;

use stellar_ledger::NewLedger;
use stellar_rpc_client::Event as SorobanEvent;
use tokio::sync::broadcast::Receiver;
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use crate::metrics;

/// Top-level event flowing from collectors → engine → strategies.
#[derive(Debug, Clone)]
pub enum Event {
    SorobanEvents(SorobanEvent),
    NewLedger(NewLedger),
}

/// Wrap a `broadcast::Receiver` as a `Stream<Item = T>`, dropping `Lagged`
/// items but counting them in `keeper_collector_lagged_events_total` so the
/// loss is observable in Prometheus instead of silent.
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
            metrics::record_collector_lag(collector, n);

            None
        }
    })
}
