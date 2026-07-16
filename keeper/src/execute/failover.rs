//! A thin failover wrapper over one or more [`stellar_rpc_client::Client`]s.
use core::pin::Pin;
use std::{
    future::Future,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use stellar_rpc_client::{
    AuthMode, Client, Error, GetTransactionResponse, SimulateTransactionResponse,
};
use stellar_xdr::{AccountEntry, Hash, TransactionEnvelope};
use tokio::time::Instant;

/// How long a node is skipped after a transport failure before it is
/// eligible for the fast (pass-0) path again.
const COOLDOWN: Duration = Duration::from_secs(30);

pub struct FailoverClient {
    clients: Vec<Client>,
    base: Instant,
    urls: Vec<String>,
    max_call_duration: Duration,
    current_rpc_index: AtomicUsize,
    uncallable_untill: Vec<AtomicU64>,
}

impl FailoverClient {
    /// Build a failover client from a non-empty list of RPC endpoint URLs.
    ///
    /// The first URL is treated as the primary; the remaining URLs are
    /// fallbacks that are tried in order whenever a preceding endpoint is on
    /// cooldown or fails with a transport error.
    pub fn new(urls: Vec<url::Url>, max_call_duration: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(!urls.is_empty(), "FailoverClient requires at least one RPC url");

        let mut clients = Vec::with_capacity(urls.len());
        for url in &urls {
            // Map the (large) rpc error into anyhow immediately so we don't
            // propagate `stellar_rpc_client::Error` by value.
            let client = Client::new(url.as_str())
                .map_err(|e| anyhow::anyhow!("failed to build rpc client for {url}: {e}"))?;
            clients.push(client);
        }

        let urls: Vec<String> = urls.into_iter().map(String::from).collect();
        let uncallable_untill = (0..clients.len()).map(|_| AtomicU64::new(0)).collect();

        Ok(Self {
            urls,
            clients,
            uncallable_untill,
            max_call_duration,
            base: Instant::now(),
            current_rpc_index: AtomicUsize::new(0),
        })
    }

    fn elapsed_nanos(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    fn is_uncallable(&self, i: usize) -> bool {
        self.uncallable_untill[i].load(Ordering::Relaxed) > self.elapsed_nanos()
    }

    /// Clear the cooldown on endpoint `i` and make it the preferred endpoint
    /// for subsequent calls (sticky routing to the last-known-good node).
    fn mark_callable(&self, i: usize) {
        self.uncallable_untill[i].store(0, Ordering::Relaxed);
        self.current_rpc_index.store(i, Ordering::Relaxed);
    }

    fn mark_uncallable(&self, i: usize) {
        let uncallable_until = self.elapsed_nanos() + COOLDOWN.as_nanos() as u64;

        self.uncallable_untill[i].store(uncallable_until, Ordering::Relaxed);
        tracing::warn!(uncallable_until, url = %self.urls[i], "rpc endpoint is marked uncallable");
    }

    async fn with_failover<'a, T>(
        &'a self,
        op: impl Fn(&'a Client) -> Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>,
    ) -> Result<T, Error> {
        let n = self.clients.len();
        let start = Instant::now();

        let start_from = self.current_rpc_index.load(Ordering::Relaxed);

        let mut last_err: Option<Error> = None;
        let mut attempted = false;

        for pass in 0..2 {
            if pass == 1 && attempted {
                break;
            }

            for offset in 0..n {
                let idx = (start_from + offset) % n;

                if pass == 0 && self.is_uncallable(idx) {
                    continue;
                }

                if attempted && start.elapsed() >= self.max_call_duration {
                    break;
                }
                attempted = true;

                match op(&self.clients[idx]).await {
                    Ok(response) => {
                        self.mark_callable(idx);

                        return Ok(response);
                    }
                    Err(e) => {
                        // Check if the error is network related (timeout, 502)
                        // or application related (bad signature, missing sequence).
                        if !is_transport_error(&e) {
                            self.mark_callable(idx);

                            return Err(e);
                        }

                        // It's a network failure. Penalize this node and save the error.
                        self.mark_uncallable(idx);
                        last_err = Some(e);
                    }
                }
            }
        }

        Err(last_err.unwrap())
    }

    // --- Public API Wrappers ---

    pub async fn simulate_transaction_envelope(
        &self,
        tx_envelope: &TransactionEnvelope,
    ) -> Result<SimulateTransactionResponse, Error> {
        self.with_failover(|client| {
            Box::pin(client.simulate_transaction_envelope(tx_envelope, Some(AuthMode::Record)))
        })
        .await
    }

    pub async fn send_transaction(&self, tx_envelope: &TransactionEnvelope) -> Result<Hash, Error> {
        self.with_failover(|client| Box::pin(client.send_transaction(tx_envelope))).await
    }

    pub async fn get_account(&self, address: &str) -> Result<AccountEntry, Error> {
        self.with_failover(|client| Box::pin(client.get_account(address))).await
    }

    pub async fn get_transaction_polling(
        &self,
        tx_id: &Hash,
        timeout_s: Option<Duration>,
    ) -> Result<GetTransactionResponse, Error> {
        self.with_failover(|client| Box::pin(client.get_transaction_polling(tx_id, timeout_s)))
            .await
    }
}

/// Decide whether an error is a *transport* failure (the endpoint is
/// unreachable / unhealthy and we should fail over to another node) versus an
/// *application* failure (the endpoint answered, but the request itself was
/// rejected — e.g. bad sequence number, simulation error). Only transport
/// failures put a node on cooldown; application errors are returned verbatim
/// to the caller because retrying them on another node would yield the same
/// result.
///
/// `stellar_rpc_client::Error` wraps the underlying `jsonrpsee` client error
/// in its opaque `JsonRpc` variant, which we cannot destructure without taking
/// a direct dependency on a specific `jsonrpsee` version. To stay decoupled we
/// classify off the rendered diagnostic — the same approach the rest of the
/// keeper uses (see `stellar::errors::SorobanRpcError`).
fn is_transport_error(e: &Error) -> bool {
    match e {
        // A submission that timed out never got a definitive verdict from the
        // node; treat it as a transport-level failure so we can retry
        // elsewhere.
        Error::TransactionSubmissionTimeout => true,
        // The RPC layer surfaced a low-level networking / protocol problem.
        // `jsonrpsee`'s transport-flavoured variants render with these
        // stable markers; a JSON-RPC `Call` (application) error does not.
        Error::JsonRpc(_) => {
            let rendered = e.to_string().to_lowercase();
            rendered.contains("transport error")
                || rendered.contains("request timeout")
                || rendered.contains("restart required")
                || rendered.contains("rpc service disconnected")
        }
        // Everything else (invalid address, XDR/serde issues, contract-level
        // rejections, unexpected statuses, …) is an application error: the
        // node responded and another node would answer the same way.
        _ => false,
    }
}
