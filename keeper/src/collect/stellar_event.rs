//! Polls the Stellar RPC for contract events and emits them, persisting the
//! cursor across restarts via [`crate::storage::CursorRepo`].

use std::{sync::Arc, time::Duration};

use engine::reactor::{BoxFuture, Collector, CollectorStream};
use stellar_rpc_client::{EventStart, EventType};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use super::{Event, lag_counted_stream};
use crate::{
    metrics::CursorSource, stellar::client::Gateway, stellar::errors::is_terminal_cursor_error,
    storage::cursor::CursorRepo,
};

/// Subscription filter forwarded to [`Client::get_events`]. Each entry in
/// `topics` is a positional `TopicFilter`; segments are exact XDR-encoded
/// strings or `"*"` for a single-segment wildcard, and the filter's segment
/// count must match the event topic length. An empty *outer* vec disables
/// topic filtering entirely.
#[derive(Debug, Clone)]
pub struct EventFilter {
    pub topics: Vec<Vec<String>>,
    pub event_type: EventType,
    pub contract_ids: Vec<String>,
}

/// Polls the Stellar RPC for contract events.
pub struct SorobanEventCollector {
    gateway: Arc<Gateway>,
    last_ledger: u32,
    filter: EventFilter,
    cursor_repo: Arc<CursorRepo>,
    last_cursor_id: Option<String>,
}

impl SorobanEventCollector {
    pub fn try_new(
        gateway: Arc<Gateway>,
        start_ledger: u32,
        filter: EventFilter,
        cursor_repo: Arc<CursorRepo>,
    ) -> anyhow::Result<Self> {
        let (last_ledger, last_cursor_id) = match cursor_repo.get()? {
            Some(saved) => {
                info!(?saved, "resuming from saved cursor/ledger");
                let last_cursor_id =
                    if saved.cursor_id.is_empty() { None } else { Some(saved.cursor_id) };

                (saved.ledger, last_cursor_id)
            }
            None => {
                info!(start_ledger, "no saved cursor");

                (start_ledger, None)
            }
        };

        Ok(Self { filter, last_ledger, cursor_repo, last_cursor_id, gateway })
    }
}

impl Collector<Event> for SorobanEventCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, anyhow::Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);

            let mut last_ledger = self.last_ledger;
            let mut last_cursor_id = self.last_cursor_id.clone();
            let filter = self.filter.clone();
            let gateway = Arc::clone(&self.gateway);
            let cursor_repo = Arc::clone(&self.cursor_repo);

            tokio::spawn(async move {
                let mut is_first_poll = true;

                loop {
                    let start = match last_cursor_id {
                        Some(ref cursor) => EventStart::Cursor(cursor.clone()),
                        None => EventStart::Ledger(last_ledger),
                    };

                    match gateway
                        .rpc
                        .get_events(
                            start,
                            Some(filter.event_type),
                            &filter.contract_ids,
                            &filter.topics,
                            None,
                        )
                        .await
                    {
                        Ok(response) => {
                            is_first_poll = false;
                            let has_events = !response.events.is_empty();

                            for event in response.events {
                                last_ledger = event.ledger;
                                if let Err(err) = sender.send(Event::SorobanEvents(event)) {
                                    warn!(?err, "no receivers, stopping");

                                    return;
                                }
                            }

                            if !response.cursor.is_empty() {
                                last_cursor_id = Some(response.cursor.clone());

                                if let Err(e) = cursor_repo.set(&response.cursor, last_ledger) {
                                    warn!(?e, "failed to persist cursor");
                                    CursorSource::EventCollectorCursor.record();
                                }
                            }

                            // If we didn't find any events, we are likely at the network head.
                            // Pause briefly so we don't spam the RPC while waiting for the next ledger to close.
                            if !has_events {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                        Err(e) => {
                            let terminal = is_terminal_cursor_error(&e);

                            if terminal && !is_first_poll {
                                warn!(
                                    err = %e,
                                    cursor = ?last_cursor_id,
                                    "TERMINAL cursor error — \
                                     resetting to head ledger"
                                );
                                last_cursor_id = None;
                                if let Ok(ledger) = gateway.rpc.get_latest_ledger().await {
                                    last_ledger = ledger.sequence;
                                }
                            } else if terminal && is_first_poll {
                                // We configured a start_ledger that the RPC no longer has (older than ~7 days).
                                // Do NOT retry. Kill the collector task so the developer knows the config is bad.
                                error!(
                                    err = %e,
                                    ledger = last_ledger,
                                    "TERMINAL ERROR on first poll! Your configured `start_ledger` is likely too old and has been pruned by the RPC. Shutting down collector."
                                );

                                return;
                            } else {
                                warn!(
                                    err = %e,
                                    "transient get_events error, \
                                     retrying from same cursor"
                                );
                            }

                            is_first_poll = false;

                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });

            let stream = lag_counted_stream(receiver, "soroban_events");

            Ok(Box::pin(stream) as CollectorStream<'_, Event>)
        })
    }
}
