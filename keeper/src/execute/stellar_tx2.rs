//! Submits Soroban transactions. Fire-and-forget: `execute` returns once the
//! tx is sent; a detached task polls for the receipt.
//!
//! Sequence numbers are tracked in a local cursor and only refreshed from
//! `get_account` on init or `tx_bad_seq` — back-to-back submissions otherwise
//! race the RPC's lagging view of `seq_num`.
//!
use {
    super::Action,
    crate::{
        stellar::{
            Gateway,
            errors::{SorobanRpcError, is_bad_seq_error, is_no_simulation_results_error},
        },
        strategy::LiquidatorCapital,
    },
    anyhow::Result,
    ed25519_dalek::{Signer, SigningKey},
    engine::reactor::{BoxFuture, Executor},
    metrics::{counter, histogram},
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

// #[derive(Debug, Clone)]
// pub struct LiquidationOutcomeMetric {
//     pub market: String,
//     pub expected_net_oracle: i128,
// }

#[derive(Debug, Clone)]
pub struct SettleHook {
    pub op_id: u64,
    pub liquidator_capital: Arc<LiquidatorCapital>,
}

impl SettleHook {
    fn release(&self) {
        // this is very unidiomatic, no?
        self.liquidator_capital.release(self.op_id);
    }
}

#[derive(Debug, Clone)]
pub struct SubmitStellarTx {
    pub op: Operation,
    pub signing_key: SigningKey,
    pub max_submission_retries: u32,
    pub on_settle: Option<SettleHook>,
}

/// Submits transactions to the Stellar network using the shared
/// [`Gateway`] RPC client.
pub struct SorobanExecutor {
    gateway: Arc<Gateway>,
    network_passphrase: String,
    /// Locally-tracked next-to-use sequence number. `None` means "fetch from
    /// chain on the next submission". Reset to `None` whenever the RPC
    /// reports a `tx_bad_seq`-style error.
    seq_num_cursor: AsyncMutex<Option<i64>>,
}

impl SorobanExecutor {
    pub fn new(gateway: Arc<Gateway>, network_passphrase: impl Into<String>) -> Self {
        Self {
            gateway,
            seq_num_cursor: AsyncMutex::new(None),
            network_passphrase: network_passphrase.into(),
        }
    }

    /// Return the sequence number to use for the next outgoing tx, bumping
    /// the cursor by 1. Initializes from `get_account` if the cursor is
    /// empty.
    async fn acquire_seq(&self, rpc: &Client, source_address: &str) -> Result<i64> {
        let mut guard = self.seq_num_cursor.lock().await;

        if guard.is_none() {
            // NB: Holding the lock across the RPC call: concurrent callers will
            // queue and observe the initialized cursor. The lock is small
            // and only contended at startup / after a bad_seq reset.
            let account = rpc.get_account(source_address).await?;
            *guard = Some(account.seq_num.0);
        }
        let current = guard.expect("guard checked for 'None' above");
        let next = current.saturating_add(1);

        *guard = Some(next);

        Ok(next)
    }

    /// Clears the seq num cache. Used on
    /// `tx_bad_seq` errors.
    async fn clear_seq_num_cache(&self) {
        *self.seq_num_cursor.lock().await = None;
    }
}

impl Executor<Action> for SorobanExecutor {
    fn execute(&mut self, action: Action) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            match action {
                Action::SubmitTx(tx) => {
                    let max_retries = tx.max_submission_retries;

                    let source_strkey = stellar_strkey::ed25519::PublicKey(
                        tx.signing_key.verifying_key().to_bytes(),
                    );
                    let source_address = source_strkey.to_string();

                    for attempt in 0..max_retries {
                        // Isn't this like fucking horrible here?
                        let seq_num_to_use =
                            match self.acquire_seq(&self.gateway.rpc, &source_address).await {
                                Ok(n) => n,
                                Err(e) => todo!(),
                            };

                        match build_and_send(
                            &self.gateway.rpc,
                            &self.network_passphrase,
                            &tx,
                            &source_strkey,
                            seq_num_to_use,
                        )
                        .await
                        {
                            Ok(hash_hex) => {}
                            Err(e) => {}
                        }
                    }

                    Ok(())
                }
            }
        })
    }
}

async fn build_and_send(
    rpc: &Client,
    seq_num_to_use: i64,
    network_passphrase: &str,
    action: &SubmitStellarTx,
    source_strkey: &stellar_strkey::ed25519::PublicKey,
) -> Result<String> {
    const DEFAULT_SIMULATION_FEE: u32 = 100_000;

    // This here must lock a sequence number mutex, man..

    let tx = Transaction {
        memo: Memo::None,
        ext: TransactionExt::V0,
        cond: Preconditions::None,
        fee: DEFAULT_SIMULATION_FEE,
        seq_num: SequenceNumber(seq_num_to_use),
        operations: vec![action.op.clone()].try_into()?,
        source_account: MuxedAccount::Ed25519(Uint256(source_strkey.0)),
    };
    let simulation_envelope = TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: tx.clone(),
        signatures: vec![].try_into()?,
    });

    let simulation_response = rpc
        .simulate_transaction_envelope(&simulation_envelope, Some(AuthMode::Record))
        .await?;
    if simulation_response.results.is_empty() {
        error!(
            ?simulation_response,
            "simulation returned no results — skipping submission"
        );

        return Err(anyhow::anyhow!("simulation returned no results"));
    }

    let mut assembled_tx = tx.clone();
    if !simulation_response.transaction_data.is_empty() {
        let transaction_data = stellar_xdr::curr::SorobanTransactionData::from_xdr_base64(
            &simulation_response.transaction_data,
            Limits::none(),
        )?;
        assembled_tx.ext = TransactionExt::V1(transaction_data);
    } // TODO else?

    let mut new_operations = Vec::<Operation>::new();
    for (i, mut operation) in assembled_tx.operations.iter().enumerate() {
        if let Some(result) = simulation_response.results.get(i)
            && let OperationBody::InvokeHostFunction(invoke_op) = &operation.body
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
    }

    todo!()
}
