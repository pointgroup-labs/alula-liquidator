//! Polls the Stellar RPC for the latest ledger and emits [`NewLedger`] events.

use {
    super::{Event, lag_counted_stream},
    anyhow::Result,
    engine::reactor::{BoxFuture, Collector, CollectorStream},
    std::time::Duration,
    stellar_rpc_client::Client,
    tokio::sync::broadcast,
    tracing::{debug, error, warn},
    url::Url,
};

/// Emitted whenever the ledger sequence advances.
#[derive(Debug, Clone)]
pub struct NewLedger {
    pub seq_num: u32,
}

/// Polls the Stellar RPC for the latest ledger sequence and emits a
/// [`NewLedger`] each time it advances.
pub struct LedgerCollector {
    network_url: Url,
    last_seq_num: u32,
}

impl LedgerCollector {
    pub fn new(url: &Url) -> Self {
        Self {
            network_url: url.clone(),
            last_seq_num: 0,
        }
    }
}

impl Collector<Event> for LedgerCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);
            let url = self.network_url.clone();
            let mut last_seq_num = self.last_seq_num;

            tokio::spawn(async move {
                let server = match Client::new(url.as_str()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(?e, "LedgerCollector: failed to create RPC client");
                        return;
                    }
                };

                loop {
                    match server.get_latest_ledger().await {
                        Ok(ledger) if ledger.sequence > last_seq_num => {
                            last_seq_num = ledger.sequence;
                            debug!(seq = ledger.sequence, "new ledger");
                            if let Err(err) = sender.send(Event::NewBlock(NewLedger {
                                seq_num: ledger.sequence,
                            })) {
                                warn!(?err, "LedgerCollector: no receivers left, stopping");
                                return;
                            }
                        }
                        Ok(_) => { /* ledger hasn't advanced yet */ }
                        Err(e) => {
                            warn!("LedgerCollector: get_latest_ledger failed: {e:#}");
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });

            let stream = lag_counted_stream(receiver, "ledger");
            Ok(Box::pin(stream) as CollectorStream<'_, Event>)
        })
    }
}
