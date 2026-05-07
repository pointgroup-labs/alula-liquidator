use {
    crate::{
        db::DbManager,
        types::{BoxFuture, Collector, CollectorStream, Event},
    },
    std::{sync::Arc, time::Duration},
    stellar_rpc_client::{Client, EventStart, EventType},
    tokio::sync::broadcast,
    tokio_stream::{StreamExt, wrappers::BroadcastStream},
    tracing::{error, info, warn},
    url::Url,
};

/// Describes how to filter the queried events
#[derive(Debug, Clone)]
pub struct EventFilter {
    pub topics: Vec<Vec<String>>,
    pub event_type: EventType,
    pub contract_ids: Vec<String>,
}

/// Polls the Stellar RPC for contract events and emits them as a stream.
pub struct EventCollector {
    db: Arc<DbManager>,
    filter: EventFilter,
    network_url: Url,
    last_event_timestamp: u32,
    last_cursor_id: Option<String>,
}

impl EventCollector {
    pub fn new(network_url: &Url, filter: EventFilter, db: &Arc<DbManager>) -> Self {
        Self {
            filter,
            db: Arc::clone(db),
            last_cursor_id: None,
            last_event_timestamp: 0,
            network_url: network_url.clone(),
        }
    }

    /// Like `new`, but seeds `last_cursor_id` / `last_event_timestamp` from the
    /// saved cursor in the DB if one exists. This lets the collector resume
    /// from where it left off across restarts instead of starting at the
    /// current head ledger.
    pub fn try_create(
        network_url: &Url,
        filter: EventFilter,
        db: &Arc<DbManager>,
    ) -> anyhow::Result<Self> {
        let mut collector = Self::new(network_url, filter, db);

        match db.load_cursor()? {
            Some((cursor_id, ledger)) => {
                info!(
                    %cursor_id,
                    ledger,
                    "EventCollector: resuming from saved cursor"
                );
                // An empty cursor_id is used as a "ledger-only" snapshot
                // (see liquidator.rs pre-fetch path); only honor it when set.
                collector.last_cursor_id = if cursor_id.is_empty() {
                    None
                } else {
                    Some(cursor_id)
                };
                collector.last_event_timestamp = ledger;
            }
            None => {
                info!("EventCollector: no saved cursor in DB, will start at head");
            }
        }

        Ok(collector)
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
                                last_event_timestamp = event.ledger;

                                if let Err(err) = sender.send(Event::SorobanEvents(event)) {
                                    warn!(?err, "LogCollector: no receivers left, stopping");

                                    return;
                                }
                            }

                            // Update cursor once per page, using the response-level cursor
                            if has_events || last_cursor_id.is_some() {
                                last_cursor_id = Some(response.cursor);
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
