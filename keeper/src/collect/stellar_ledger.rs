//! Polls the Stellar RPC for the latest ledger and emits [`NewLedger`] events.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use engine::reactor::{BoxFuture, Collector, CollectorStream};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::{Event, lag_counted_stream};
use crate::stellar::client::Gateway;

/// Emitted whenever the ledger sequence advances.
#[derive(Debug, Clone)]
pub struct NewLedger {
    pub seq_num: u32,
}

/// Polls the Stellar RPC for the latest ledger sequence and emits a
/// [`NewLedger`] each time it advances.
pub struct LedgerCollector {
    gateway: Arc<Gateway>,
    last_seq_num: u32,
    ledger_polling_interval_secs: u64,
}

impl LedgerCollector {
    pub fn new(gateway: Arc<Gateway>, ledger_polling_interval_secs: u64) -> Self {
        Self { last_seq_num: 0, gateway, ledger_polling_interval_secs }
    }
}

impl Collector<Event> for LedgerCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);

            let gateway = Arc::clone(&self.gateway);
            let mut last_seq_num = self.last_seq_num;
            let ledger_polling_interval_secs = self.ledger_polling_interval_secs;

            tokio::spawn(async move {
                loop {
                    match gateway.rpc.get_latest_ledger().await {
                        Ok(ledger) if ledger.sequence > last_seq_num => {
                            last_seq_num = ledger.sequence;
                            debug!(seq = ledger.sequence, "new ledger");

                            if let Err(err) = sender
                                .send(Event::NewLedger(NewLedger { seq_num: ledger.sequence }))
                            {
                                warn!(?err, "no receivers left, stopping");

                                return;
                            }
                        }
                        Ok(_) => { /* ledger hasn't advanced yet */ }
                        Err(e) => {
                            warn!("get_latest_ledger failed: {e:#}");
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(ledger_polling_interval_secs)).await;
                }
            });

            let stream = lag_counted_stream(receiver, "ledger");
            Ok(Box::pin(stream) as CollectorStream<'_, Event>)
        })
    }
}
