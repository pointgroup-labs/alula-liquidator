//! Polls the Stellar RPC for contract events and emits them, persisting the
//! cursor across restarts via [`crate::storage::CursorRepo`].

use {
    super::{Event, lag_counted_stream},
    crate::{stellar::errors::is_terminal_cursor_error, storage::CursorRepo},
    engine::reactor::{BoxFuture, Collector, CollectorStream},
    metrics::counter,
    std::{sync::Arc, time::Duration},
    stellar_rpc_client::{Client, EventStart, EventType},
    tokio::sync::broadcast,
    tracing::{error, info, warn},
    url::Url,
};

/// Filter passed to `get_events`.
#[derive(Debug, Clone)]
pub struct EventFilter {
    pub topics: Vec<Vec<String>>,
    pub event_type: EventType,
    pub contract_ids: Vec<String>,
}

/// Polls the Stellar RPC for contract events.
///
/// Resume position is loaded from `CursorRepo` on construction and updated
/// after each successful page.
pub struct SorobanEventCollector {
    cursor_repo: Arc<CursorRepo>,
    filter: EventFilter,
    network_url: Url,
    last_event_timestamp: u32,
    last_cursor_id: Option<String>,
}

impl SorobanEventCollector {
    /// Construct a collector, seeding the resume cursor from `cursor_repo`
    /// if one was previously persisted. (This is the only constructor — the
    /// previous "head-only" `new` was footgunny and has been removed.)
    pub fn new(
        network_url: &Url,
        filter: EventFilter,
        cursor_repo: Arc<CursorRepo>,
    ) -> anyhow::Result<Self> {
        let mut me = Self {
            cursor_repo,
            filter,
            network_url: network_url.clone(),
            last_event_timestamp: 0,
            last_cursor_id: None,
        };

        match me.cursor_repo.get()? {
            Some(saved) => {
                info!(
                    cursor_id = %saved.cursor_id,
                    ledger = saved.last_event_timestamp,
                    "SorobanEventCollector: resuming from saved cursor"
                );
                // Empty cursor_id is used as a "ledger-only" snapshot
                // (matches the historical pre-fetch path); only honor it when set.
                me.last_cursor_id = if saved.cursor_id.is_empty() {
                    None
                } else {
                    Some(saved.cursor_id)
                };
                me.last_event_timestamp = saved.last_event_timestamp;
            }
            None => {
                info!("SorobanEventCollector: no saved cursor, will start at head");
            }
        }

        Ok(me)
    }
}

// Heuristic: which RPC errors mean the saved cursor is permanently bad and
// must be replaced with a fresh head ledger? Routed through
// [`crate::stellar::errors`] so the substring patterns are unit-tested
// alongside the other classification helpers.

impl Collector<Event> for SorobanEventCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, anyhow::Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);

            let mut last_event_timestamp = self.last_event_timestamp;
            let mut last_cursor_id = self.last_cursor_id.clone();
            let filter = self.filter.clone();
            let network_url = self.network_url.clone();
            let cursor_repo = Arc::clone(&self.cursor_repo);

            tokio::spawn(async move {
                let client = match Client::new(network_url.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(?e, "SorobanEventCollector: failed to create RPC client");
                        return;
                    }
                };

                // Bootstrap: if no starting ledger exists, grab the latest one.
                if last_event_timestamp == 0 {
                    match client.get_latest_ledger().await {
                        Ok(ledger) => {
                            last_event_timestamp = ledger.sequence;
                            info!(
                                ledger = last_event_timestamp,
                                "SorobanEventCollector: starting from head ledger"
                            );
                        }
                        Err(e) => {
                            error!(?e, "SorobanEventCollector: failed to get initial ledger");
                            return;
                        }
                    }
                }

                let mut is_first_poll = true;

                loop {
                    let start = match last_cursor_id {
                        Some(ref cursor) => EventStart::Cursor(cursor.clone()),
                        None => EventStart::Ledger(last_event_timestamp),
                    };

                    match client
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
                                last_event_timestamp = event.ledger;
                                if let Err(err) = sender.send(Event::SorobanEvents(event)) {
                                    warn!(?err, "SorobanEventCollector: no receivers, stopping");
                                    return;
                                }
                            }

                            if has_events || last_cursor_id.is_some() {
                                last_cursor_id = Some(response.cursor.clone());
                                if let Err(e) =
                                    cursor_repo.set(&response.cursor, last_event_timestamp)
                                {
                                    warn!(?e, "SorobanEventCollector: failed to persist cursor");
                                    counter!(
                                        "keeper_cursor_save_failures_total",
                                        "source" => "event_collector_cursor",
                                    )
                                    .increment(1);
                                }
                            }

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
                                    "SorobanEventCollector: TERMINAL cursor error — \
                                     resetting to head ledger"
                                );
                                last_cursor_id = None;
                                if let Ok(ledger) = client.get_latest_ledger().await {
                                    last_event_timestamp = ledger.sequence;
                                }
                            } else if terminal && is_first_poll {
                                // Refuse to wipe the cursor on the very first poll;
                                // a startup hiccup should not erase the resume position.
                                warn!(
                                    err = %e,
                                    "SorobanEventCollector: terminal-looking error on \
                                     first poll — keeping cursor and retrying"
                                );
                            } else {
                                warn!(
                                    err = %e,
                                    "SorobanEventCollector: transient get_events error, \
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
