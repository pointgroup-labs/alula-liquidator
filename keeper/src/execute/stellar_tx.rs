//! Submits Soroban transactions.
//!
//! Layout:
//! - `execute(action)` builds → simulates → signs → SENDS the tx, then
//!   spawns a detached tokio task that polls for the receipt. The future
//!   returns once the tx is *submitted*, never blocks on confirmation.
//! - The polling task logs success/failure but does not feed back into the
//!   reactor (executors are fire-and-forget by `Executor` contract).
//!
//! Retries between build/send attempts use a linear backoff
//! (`250 * (attempt + 1) ms`). Simulation failures ("simulation returned no
//! results") short-circuit and are not retried.

use {
    super::Action,
    crate::{stellar::Gateway, strategy::CapitalLedger},
    anyhow::Result,
    ed25519_dalek::{Signer, SigningKey},
    engine::reactor::{BoxFuture, Executor},
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
}

impl SorobanExecutor {
    pub fn new(gateway: Arc<Gateway>, network_passphrase: impl Into<String>) -> Self {
        Self {
            network_passphrase: network_passphrase.into(),
            gateway,
        }
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

                    for attempt in 1..=max {
                        match build_and_send(&rpc, &self.network_passphrase, &tx).await {
                            Ok(hash_hex) => {
                                info!(hash = %hash_hex, "tx submitted; polling in background");
                                spawn_confirmation_poll(rpc.clone(), hash_hex, on_settle);
                                return Ok(());
                            }
                            Err(e) => {
                                // Non-retryable: simulation itself produced no results.
                                if e.to_string().contains("simulation returned no results") {
                                    warn!(%e, "simulation returned no results; giving up");
                                    if let Some(hook) = &on_settle {
                                        hook.release();
                                    }
                                    return Ok(());
                                }
                                if attempt >= max {
                                    error!(%e, attempt, "tx failed after all retries");
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
                if let Some(hook) = &on_settle {
                    hook.release();
                }
                return;
            }
        };
        match rpc.get_transaction_polling(&hash_bytes, None).await {
            Ok(_) => info!(hash = %hash_hex, "tx confirmed"),
            Err(e) => warn!(hash = %hash_hex, %e, "tx confirmation poll failed"),
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
) -> Result<String> {
    let source_strkey =
        stellar_strkey::ed25519::PublicKey(action.signing_key.verifying_key().to_bytes());
    let source_address = source_strkey.to_string();

    let account = rpc.get_account(&source_address).await?;
    let seq_num = account.seq_num.0;

    let tx = Transaction {
        source_account: MuxedAccount::Ed25519(Uint256(source_strkey.0)),
        fee: DEFAULT_SIMULATION_FEE,
        seq_num: SequenceNumber(seq_num + 1),
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

    // Apply recorded authorization data to operations.
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
