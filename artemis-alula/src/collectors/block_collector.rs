use {
    crate::types::{BoxFuture, Collector, CollectorStream, Event},
    anyhow::Result,
    std::time::Duration,
    stellar_rpc_client::Client,
    tokio::sync::broadcast,
    tokio_stream::{StreamExt, wrappers::BroadcastStream},
    tracing::{error, info, warn},
    url::Url,
};

/// Polls the Stellar RPC for the latest ledger sequence and emits a
/// [`NewBlock`] each time it advances.
pub struct BlockCollector {
    network_url: Url,
    last_block_num: u32,
}

impl BlockCollector {
    pub fn new(url: &Url) -> Self {
        Self {
            network_url: url.clone(),
            last_block_num: 0,
        }
    }
}

/// Emitted whenever the ledger sequence advances.
#[derive(Debug, Clone)]
pub struct NewBlock {
    pub number: u32,
}

impl Collector<Event> for BlockCollector {
    fn get_event_stream(&mut self) -> BoxFuture<'_, Result<CollectorStream<'_, Event>>> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(512);

            let url = self.network_url.clone();
            let mut last_block_num = self.last_block_num;

            tokio::spawn(async move {
                let server = match Client::new(url.as_str()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(?e, "BlockCollector: failed to create RPC client");

                        return;
                    }
                };

                loop {
                    match server.get_latest_ledger().await {
                        Ok(block) if block.sequence > last_block_num => {
                            last_block_num = block.sequence;

                            info!(?block, "Received block");

                            // If all receivers have dropped the send will fail — that's fine,
                            // we just stop naturally on the next iteration
                            if let Err(err) = sender.send(Event::NewBlock(NewBlock {
                                number: block.sequence,
                            })) {
                                warn!(?err, "BlockCollector: no receivers left, stopping");

                                return;
                            }
                        }
                        Ok(_) => { /* ledger hasn't advanced yet */ }
                        Err(e) => {
                            warn!("BlockCollector: get_latest_ledger failed: {e:#}");
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            let stream = BroadcastStream::new(receiver).filter_map(|item| item.ok());

            Ok(Box::pin(stream) as CollectorStream<'_, Event>)
        })
    }
}
