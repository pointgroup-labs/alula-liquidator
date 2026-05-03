use {
    crate::types::{BoxFuture, Collector, CollectorStream, Event},
    std::time::Duration,
    stellar_rpc_client::{Client, EventStart, EventType},
    tokio::sync::broadcast,
    tokio_stream::{StreamExt, wrappers::BroadcastStream},
    tracing::{error, info, warn},
    url::Url,
};

/// Describes how to filter the queried events
#[derive(Debug, Clone)]
pub struct EventFilter {
    pub topics: Vec<String>,
    pub event_type: EventType,
    pub contract_ids: Vec<String>,
}

/// Polls the Stellar RPC for contract events and emits them as a stream.
pub struct EventCollector {
    filter: EventFilter,
    network_url: Url,
    last_event_timestamp: u32,
    last_cursor_id: Option<String>,
}

impl EventCollector {
    pub fn new(network_url: &Url, filter: EventFilter) -> Self {
        Self {
            filter,
            last_cursor_id: None,
            last_event_timestamp: 0,
            network_url: network_url.clone(),
        }
    }

    /// Set a starting cursor and ledger to resume event collection from a saved position.
    /// If `cursor_id` is empty, only the ledger is used (EventStart::Ledger).
    pub fn with_cursor(mut self, cursor_id: String, ledger: u32) -> Self {
        self.last_cursor_id = if cursor_id.is_empty() {
            None
        } else {
            Some(cursor_id)
        };
        self.last_event_timestamp = ledger;

        self
    }
}

impl Collector<Event> for EventCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, anyhow::Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);

            let mut last_event_timestamp = self.last_event_timestamp;
            let mut last_cursor_id = self.last_cursor_id.clone();
            let filter = self.filter.clone();
            let network_url = self.network_url.clone();

            tokio::spawn(async move {
                let client = match Client::new(network_url.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(?e, "LogCollector: failed to create RPC client");

                        return;
                    }
                };

                // Bootstrap: if no starting ledger exists, grab the latest one
                if last_event_timestamp == 0 {
                    match client.get_latest_ledger().await {
                        Ok(ledger) => {
                            last_event_timestamp = ledger.sequence;

                            info!(?last_event_timestamp, "LogCollector: starting from ledger");
                        }
                        Err(e) => {
                            error!(?e, "LogCollector: failed to get initial ledger");

                            return;
                        }
                    }
                }

                loop {
                    let start = match last_cursor_id {
                        Some(ref cursor) => EventStart::Cursor(cursor.clone()),
                        None => EventStart::Ledger(last_event_timestamp),
                    };

                    match client
                        .get_events(
                            start,
                            Some(EventType::Contract),
                            &filter.contract_ids,
                            &filter.topics,
                            None,
                        )
                        .await
                    {
                        Ok(response) => {
                            let has_events = !response.events.is_empty();

                            for event in response.events {
                                // X: One must check if this is a good thing to have
                                let cursor = if event.paging_token.is_empty() {
                                    event.id.clone()
                                } else {
                                    event.paging_token.clone()
                                };

                                last_cursor_id = Some(cursor);
                                last_event_timestamp = event.ledger;

                                if let Err(err) = sender.send(Event::SorobanEvents(event)) {
                                    warn!(?err, "LogCollector: no receivers left, stopping");

                                    return;
                                }
                            }

                            // If we got events, there may be more — poll again immediately.
                            // Otherwise we're caught up, sleep to avoid hammering the RPC.
                            if !has_events {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                        Err(e) => {
                            warn!(?e, "get_events failed, resetting to latest ledger");
                            // Stale cursor / ledger too old — reset to head
                            last_cursor_id = None;
                            if let Ok(ledger) = client.get_latest_ledger().await {
                                last_event_timestamp = ledger.sequence;
                            }
                            // Back off on error before retrying
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
            let stream = BroadcastStream::new(receiver).filter_map(|item| item.ok());

            Ok(Box::pin(stream) as CollectorStream<'_, Event>)
        })
    }
}
