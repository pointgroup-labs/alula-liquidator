use {
    crate::{
        constants::DEFAULT_SIMULATION_FEE,
        types::{Action, BoxFuture, Executor},
    },
    anyhow::Result,
    ed25519_dalek::{Signer, SigningKey},
    sha2::{Digest, Sha256},
    std::time::Duration,
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

#[derive(Debug, Clone)]
pub struct SubmitStellarTx {
    pub op: Operation,
    pub signing_key: SigningKey,
    pub max_retries: u32,
}

/// Submits transactions to the Stellar network.
pub struct SorobanExecutor {
    network_passphrase: String,
    rpc: Client,
}

impl SorobanExecutor {
    pub fn new(rpc_url: &str, network_passphrase: &str) -> Result<Self> {
        Ok(Self {
            rpc: Client::new(rpc_url)?,
            network_passphrase: network_passphrase.to_string(),
        })
    }
}

impl Executor<Action> for SorobanExecutor {
    fn execute(&self, action: Action) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            match action {
                Action::SubmitTx(action) => {
                    for attempt in 1..=action.max_retries.max(1) {
                        match submit(&self.rpc, &self.network_passphrase, &action).await {
                            Ok(hash) => {
                                info!(?hash, "tx submitted successfully");

                                return Ok(());
                            }
                            Err(e) => {
                                if attempt >= action.max_retries.max(1) {
                                    error!(%e, attempt, ?action, "tx failed");
                                    return Err(e);
                                }
                                // Non-retryable: simulation itself failed
                                if e.to_string().contains("simulation returned no results") {
                                    return Ok(());
                                }
                                warn!(%e, attempt, "tx attempt failed, retrying...");

                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }

                    Ok(())
                }
            }
        })
    }
}

async fn submit(
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

    // Create a temporary envelope for simulation
    let temp_envelope = TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: tx.clone(),
        signatures: vec![].try_into()?,
    });

    // Simulate the transaction
    let sim_response = rpc
        .simulate_transaction_envelope(&temp_envelope, Some(AuthMode::Record))
        .await?;

    if sim_response.results.is_empty() {
        error!("simulation returned no results — contract execution likely failed, skipping submission");
        return Err(anyhow::anyhow!("simulation returned no results"));
    }

    // Assemble the transaction with simulation data
    let mut assembled_tx = tx.clone();
    if !sim_response.transaction_data.is_empty() {
        // Parse the transaction data from simulation and apply it
        let transaction_data = stellar_xdr::curr::SorobanTransactionData::from_xdr_base64(
            &sim_response.transaction_data,
            Limits::none(),
        )?;
        assembled_tx.ext = TransactionExt::V1(transaction_data);
    }

    // Apply recorded authorization data to operations
    if !sim_response.results.is_empty() {
        // Convert VecM to Vec for easier manipulation
        let operations_vec: Vec<Operation> = assembled_tx.operations.iter().cloned().collect();
        let mut new_operations: Vec<Operation> = Vec::new();

        for (i, mut operation) in operations_vec.into_iter().enumerate() {
            if let Some(result) = sim_response.results.get(i)
                && let OperationBody::InvokeHostFunction(invoke_op) = &mut operation.body {
                    // Extract auth entries from simulation result
                    if !result.auth.is_empty() {
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
            new_operations.push(operation);
        }

        assembled_tx.operations = new_operations.try_into()?;
    }

    // Update fee with simulation result.
    // `Transaction.fee` is u32, but `min_resource_fee` is u64 — clamp safely.
    let min_fee_u32: u32 = u32::try_from(sim_response.min_resource_fee).map_err(|_| {
        anyhow::anyhow!(
            "simulated min_resource_fee ({}) exceeds u32::MAX",
            sim_response.min_resource_fee
        )
    })?;

    // Add a 50% buffer to the simulated fee to account for network fluctuations
    let buffered_fee = min_fee_u32.saturating_mul(3).saturating_div(2);
    assembled_tx.fee = assembled_tx.fee.max(buffered_fee);

    let signed = sign_transaction(&assembled_tx, &action.signing_key, network_passphrase)?;

    let hash = rpc.send_transaction(&signed).await?;
    let hash_hex = hex::encode(hash.0);

    info!(?hash_hex, "tx sent, polling for confirmation...");
    rpc.get_transaction_polling(&hash, None).await?;

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
