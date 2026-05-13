//! Submits Soroban transactions. Fire-and-forget: `execute` returns once the
//! tx is sent; a detached task polls for the receipt.
//!
//! Sequence numbers are tracked in a local cursor and only refreshed from
//! `get_account` on init or `tx_bad_seq` — back-to-back submissions otherwise
//! race the RPC's lagging view of `seq_num`.

use {
    super::Action,
    crate::{
        stellar::{
            Gateway,
            errors::{is_bad_seq_error, is_no_simulation_results_error},
        },
        strategy::CapitalLedger,
    },
    anyhow::Result,
    ed25519_dalek::{Signer, SigningKey},
    engine::reactor::{BoxFuture, Executor},
    metrics::counter,
    sha2::{Digest, Sha256},
    std::{sync::Arc, time::Duration},
    stellar_rpc_client::{AuthMode, Client},
    stellar_xdr::curr::{
        DecoratedSignature, Hash, Limits, Memo, MuxedAccount, Operation, OperationBody,
        Preconditions, ReadXdr, SequenceNumber, Signature, SignatureHint,
        SorobanAuthorizationEntry, Transaction, TransactionEnvelope, TransactionExt,
        TransactionSignaturePayload, TransactionSignaturePayloadTaggedTransaction,
        TransactionV1Envelope, Uint256, VecM, WriteXdr as _,
    },
    tokio::sync::Mutex as AsyncMutex,
    tracing::{error, info, warn},
};

/// Default fee for simulation transactions (in stroops).
const DEFAULT_SIMULATION_FEE: u32 = 100_000;

/// Released by the executor on every terminal path: confirm-success,
/// confirm-failure, simulation-no-results skip, or retry-exhaustion. The
/// `op_id` was minted by the issuing strategy when it called
/// `CapitalLedger::reserve`.
#[derive(Debug, Clone)]
pub struct SettleHook {
    pub ledger: Arc<CapitalLedger>,
    pub op_id: u64,
}

impl SettleHook {
    fn release(&self) {
        self.ledger.release(self.op_id);
    }
}

#[derive(Debug, Clone)]
pub struct SubmitStellarTx {
    pub op: Operation,
    pub signing_key: SigningKey,
    pub max_retries: u32,
    /// Some(_) for balance-spending strategies (Liquidator, Rebalancer);
    /// None for capital-neutral ops (BadDebtRequestInitiator, Withdrawer).
    pub on_settle: Option<SettleHook>,
}

/// Submits transactions to the Stellar network using the shared
/// [`Gateway`] RPC client.
pub struct SorobanExecutor {
    network_passphrase: String,
    gateway: Arc<Gateway>,
    /// Locally-tracked next-to-use sequence number. `None` means "fetch from
    /// chain on the next submission". Reset to `None` whenever the RPC
    /// reports a `tx_bad_seq`-style error so we resynchronize.
    seq_cursor: AsyncMutex<Option<i64>>,
}

impl SorobanExecutor {
    pub fn new(gateway: Arc<Gateway>, network_passphrase: impl Into<String>) -> Self {
        Self {
            network_passphrase: network_passphrase.into(),
            gateway,
            seq_cursor: AsyncMutex::new(None),
        }
    }

    /// Return the sequence number to use for the next outgoing tx, bumping
    /// the cursor by 1. Initializes from `get_account` if the cursor is
    /// empty.
    async fn acquire_seq(&self, rpc: &Client, source_address: &str) -> Result<i64> {
        let mut guard = self.seq_cursor.lock().await;
        if guard.is_none() {
            // Hold the lock across the RPC call: concurrent callers will
            // queue and observe the initialized cursor. The lock is small
            // and only contended at startup / after a bad_seq reset.
            let account = rpc.get_account(source_address).await?;
            *guard = Some(account.seq_num.0);
        }
        let current = guard.expect("seq_cursor initialized above");
        let next = current.saturating_add(1);
        *guard = Some(next);
        Ok(next)
    }

    /// Drop the cursor so the next call refetches from RPC. Use on
    /// `tx_bad_seq` errors.
    async fn invalidate_seq(&self) {
        *self.seq_cursor.lock().await = None;
    }
}

impl Executor<Action> for SorobanExecutor {
    fn execute(&self, action: Action) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            match action {
                Action::SubmitTx(tx) => {
                    let rpc: Arc<Client> = Arc::clone(self.gateway.rpc());
                    let max = tx.max_retries.max(1);
                    let on_settle = tx.on_settle.clone();

                    let source_strkey = stellar_strkey::ed25519::PublicKey(
                        tx.signing_key.verifying_key().to_bytes(),
                    );
                    let source_address = source_strkey.to_string();

                    for attempt in 1..=max {
                        let seq_to_use = match self.acquire_seq(&rpc, &source_address).await {
                            Ok(n) => n,
                            Err(e) => {
                                warn!(%e, attempt, "failed to acquire sequence number");
                                if attempt >= max {
                                    counter!(
                                        "keeper_tx_submitted_total",
                                        "outcome" => "seq_acquire_failed",
                                    )
                                    .increment(1);
                                    if let Some(hook) = &on_settle {
                                        hook.release();
                                    }
                                    return Err(e);
                                }
                                let backoff = Duration::from_millis(250 * (attempt as u64 + 1));
                                tokio::time::sleep(backoff).await;
                                continue;
                            }
                        };

                        match build_and_send(
                            &rpc,
                            &self.network_passphrase,
                            &tx,
                            &source_strkey,
                            seq_to_use,
                        )
                        .await
                        {
                            Ok(hash_hex) => {
                                info!(hash = %hash_hex, "tx submitted; polling in background");
                                counter!(
                                    "keeper_tx_submitted_total",
                                    "outcome" => "ok",
                                )
                                .increment(1);
                                spawn_confirmation_poll(rpc.clone(), hash_hex, on_settle);
                                return Ok(());
                            }
                            Err(e) => {
                                // Non-retryable: simulation itself produced no results.
                                if is_no_simulation_results_error(&e) {
                                    warn!(%e, "simulation returned no results; giving up");
                                    counter!(
                                        "keeper_tx_submitted_total",
                                        "outcome" => "sim_empty",
                                    )
                                    .increment(1);
                                    if let Some(hook) = &on_settle {
                                        hook.release();
                                    }
                                    return Ok(());
                                }
                                // bad_seq → resync local cursor; the next
                                // attempt will refetch from RPC.
                                if is_bad_seq_error(&e) {
                                    warn!(%e, attempt, "tx_bad_seq; resyncing seq cursor");
                                    counter!("keeper_tx_bad_seq_retries_total").increment(1);
                                    self.invalidate_seq().await;
                                }
                                if attempt >= max {
                                    error!(%e, attempt, "tx failed after all retries");
                                    counter!(
                                        "keeper_tx_submitted_total",
                                        "outcome" => "retry_exhausted",
                                    )
                                    .increment(1);
                                    if let Some(hook) = &on_settle {
                                        hook.release();
                                    }
                                    return Err(e);
                                }
                                let backoff = Duration::from_millis(250 * (attempt as u64 + 1));
                                warn!(%e, attempt, ?backoff, "tx attempt failed, retrying");
                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }
                    // Loop guard fall-through (max == 0 was clamped to 1, so unreachable
                    // in practice). Release defensively.
                    counter!(
                        "keeper_tx_submitted_total",
                        "outcome" => "unreachable",
                    )
                    .increment(1);
                    if let Some(hook) = &on_settle {
                        hook.release();
                    }
                    Ok(())
                }
            }
        })
    }
}

/// Spawn a detached task that polls for the tx receipt. Logs the outcome and
/// releases the capital reservation regardless of success/failure.
fn spawn_confirmation_poll(rpc: Arc<Client>, hash_hex: String, on_settle: Option<SettleHook>) {
    tokio::spawn(async move {
        let hash_bytes = match hex::decode(&hash_hex) {
            Ok(b) if b.len() == 32 => {
                let mut a = [0u8; 32];
                a.copy_from_slice(&b);
                Hash(a)
            }
            _ => {
                warn!(hash = %hash_hex, "could not decode hash for polling");
                counter!(
                    "keeper_tx_confirmed_total",
                    "outcome" => "hash_decode_failed",
                )
                .increment(1);
                if let Some(hook) = &on_settle {
                    hook.release();
                }
                return;
            }
        };
        match rpc.get_transaction_polling(&hash_bytes, None).await {
            Ok(_) => {
                info!(hash = %hash_hex, "tx confirmed");
                counter!(
                    "keeper_tx_confirmed_total",
                    "outcome" => "confirmed",
                )
                .increment(1);
            }
            Err(e) => {
                warn!(hash = %hash_hex, %e, "tx confirmation poll failed");
                counter!(
                    "keeper_tx_confirmed_total",
                    "outcome" => "poll_failed",
                )
                .increment(1);
            }
        }
        if let Some(hook) = &on_settle {
            hook.release();
        }
    });
}

async fn build_and_send(
    rpc: &Client,
    network_passphrase: &str,
    action: &SubmitStellarTx,
    source_strkey: &stellar_strkey::ed25519::PublicKey,
    seq_num_to_use: i64,
) -> Result<String> {
    let tx = Transaction {
        source_account: MuxedAccount::Ed25519(Uint256(source_strkey.0)),
        fee: DEFAULT_SIMULATION_FEE,
        seq_num: SequenceNumber(seq_num_to_use),
        cond: Preconditions::None,
        memo: Memo::None,
        operations: vec![action.op.clone()].try_into()?,
        ext: TransactionExt::V0,
    };

    let temp_envelope = TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: tx.clone(),
        signatures: vec![].try_into()?,
    });

    let sim_response = rpc
        .simulate_transaction_envelope(&temp_envelope, Some(AuthMode::Record))
        .await?;

    if sim_response.results.is_empty() {
        error!(
            "simulation returned no results — contract execution likely failed, skipping submission"
        );
        return Err(anyhow::anyhow!("simulation returned no results"));
    }

    let mut assembled_tx = tx.clone();
    if !sim_response.transaction_data.is_empty() {
        let transaction_data = stellar_xdr::curr::SorobanTransactionData::from_xdr_base64(
            &sim_response.transaction_data,
            Limits::none(),
        )?;
        assembled_tx.ext = TransactionExt::V1(transaction_data);
    }

    // Only invoke-host-function ops with non-empty recorded auth need the
    // simulator's auth payload merged back in; the inner gate avoids touching
    // ops the simulator didn't decorate.
    let operations_vec: Vec<Operation> = assembled_tx.operations.iter().cloned().collect();
    let mut new_operations: Vec<Operation> = Vec::new();
    for (i, mut operation) in operations_vec.into_iter().enumerate() {
        if let Some(result) = sim_response.results.get(i)
            && let OperationBody::InvokeHostFunction(invoke_op) = &mut operation.body
            && !result.auth.is_empty()
        {
            let auth_entries: Result<VecM<SorobanAuthorizationEntry>, _> = result
                .auth
                .iter()
                .map(|auth_str| {
                    SorobanAuthorizationEntry::from_xdr_base64(auth_str, Limits::none())
                })
                .collect::<Result<Vec<_>, _>>()?
                .try_into();

            if let Ok(entries) = auth_entries {
                invoke_op.auth = entries;
            } else {
                warn!("Failed to parse authorization entries for operation {}", i);
            }
        }
        new_operations.push(operation);
    }
    assembled_tx.operations = new_operations.try_into()?;

    let min_fee_u32: u32 = u32::try_from(sim_response.min_resource_fee).map_err(|_| {
        anyhow::anyhow!(
            "simulated min_resource_fee ({}) exceeds u32::MAX",
            sim_response.min_resource_fee
        )
    })?;

    // 50% headroom over the simulated fee.
    let buffered_fee = min_fee_u32.saturating_mul(3).saturating_div(2);
    assembled_tx.fee = assembled_tx.fee.max(buffered_fee);

    let signed = sign_transaction(&assembled_tx, &action.signing_key, network_passphrase)?;

    let hash = rpc.send_transaction(&signed).await?;
    let hash_hex = hex::encode(hash.0);
    info!(hash = %hash_hex, "tx sent");
    Ok(hash_hex)
}

fn sign_transaction(
    tx: &Transaction,
    signing_key: &SigningKey,
    network_passphrase: &str,
) -> Result<TransactionEnvelope> {
    let payload = TransactionSignaturePayload {
        network_id: Hash(Sha256::digest(network_passphrase).into()),
        tagged_transaction: TransactionSignaturePayloadTaggedTransaction::Tx(tx.clone()),
    };
    let hash = Sha256::digest(payload.to_xdr(Limits::none())?);
    let signature = signing_key.sign(&hash);

    let pk_bytes = signing_key.verifying_key().to_bytes();
    let hint = SignatureHint([pk_bytes[28], pk_bytes[29], pk_bytes[30], pk_bytes[31]]);

    let decorated = DecoratedSignature {
        hint,
        signature: Signature(signature.to_bytes().to_vec().try_into()?),
    };

    Ok(TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: tx.clone(),
        signatures: vec![decorated].try_into()?,
    }))
}
