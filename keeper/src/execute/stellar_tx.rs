//! Submits Soroban transactions. Fire-and-forget: `execute` returns once the
//! tx is sent; a detached task polls for the receipt.
//!
use {
    super::Action,
    crate::{
        liquidator_capital::LiquidatorCapital,
        stellar::{
            client::Gateway,
            errors::{SorobanRpcError, is_bad_seq_error, is_no_simulation_results_error},
        },
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
    tracing::{error, info, warn},
};

#[derive(Debug, Clone)]
pub struct LiquidationOutcomeMetric {
    pub market: String,
    pub expected_net_value: i128,
}

#[derive(Debug, Clone)]
pub struct SettleHook {
    pub op_id: u64,
    pub liquidator_capital: Arc<LiquidatorCapital>,
    pub liquidation_outcome: Option<LiquidationOutcomeMetric>,
}

impl SettleHook {
    pub fn release(&self) {
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
    pkey: String,
    gateway: Arc<Gateway>,
    network_passphrase: String,
    default_simulation_fee: u32,
}

impl SorobanExecutor {
    pub fn new(
        pkey: &str,
        gateway: Arc<Gateway>,
        default_simulation_fee: u32,
        network_passphrase: impl Into<String>,
    ) -> Self {
        Self {
            gateway,
            pkey: pkey.to_string(),
            default_simulation_fee,
            network_passphrase: network_passphrase.into(),
        }
    }

    async fn fetch_seq_num(&self, rpc: &Client) -> Result<i64> {
        let account = rpc.get_account(&self.pkey).await?;
        let next = account.seq_num.0.saturating_add(1);

        Ok(next)
    }
}

impl Executor<Action> for SorobanExecutor {
    fn execute(&mut self, action: Action) -> BoxFuture<'_, Result<()>> {
        const BACKOFF_STEP_MILLIS: u64 = 250;

        Box::pin(async move {
            match action {
                Action::SubmitTx(tx) => {
                    let on_settle = tx.on_settle.clone();
                    let max_retries = tx.max_submission_retries;
                    let default_simulation_fee = self.default_simulation_fee;

                    let source_strkey = stellar_strkey::ed25519::PublicKey(
                        tx.signing_key.verifying_key().to_bytes(),
                    );

                    for attempt in 0..max_retries {
                        let seq_num_to_use = match self.fetch_seq_num(&self.gateway.rpc).await {
                            Ok(n) => n,
                            Err(e) => {
                                warn!(%e, attempt, "failed to fetch sequence number");
                                if attempt >= max_retries {
                                    counter!(
                                        "keeper_tx_submitted_total",
                                        "outcome" => "seq_fetch_failed",
                                    )
                                    .increment(1);
                                    if let Some(hook) = &on_settle {
                                        hook.release();
                                    }

                                    return Err(e);
                                }

                                let backoff = Duration::from_millis(
                                    BACKOFF_STEP_MILLIS * (attempt as u64 + 1),
                                );
                                tokio::time::sleep(backoff).await;

                                continue;
                            }
                        };

                        match build_and_send(
                            &self.gateway.rpc,
                            seq_num_to_use,
                            &self.network_passphrase,
                            &tx,
                            default_simulation_fee,
                            &source_strkey,
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
                                spawn_confirmation_poll(
                                    Arc::clone(&self.gateway),
                                    hash_hex,
                                    on_settle,
                                );
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
                                // attempt will refetch from RPC and likely succeed.
                                if is_bad_seq_error(&e) {
                                    warn!(%e, attempt, "tx_bad_seq; resyncing seq cursor");
                                    counter!("keeper_tx_bad_seq_retries_total").increment(1);
                                }
                                if attempt >= max_retries {
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

                                let backoff = Duration::from_millis(
                                    BACKOFF_STEP_MILLIS * (attempt as u64 + 1),
                                );
                                warn!(%e, attempt, ?backoff, "tx attempt failed, retrying");

                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }

                    Ok(())
                }
            }
        })
    }
}

fn spawn_confirmation_poll(gateway: Arc<Gateway>, hash_hex: String, on_settle: Option<SettleHook>) {
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

        match gateway.rpc.get_transaction_polling(&hash_bytes, None).await {
            Ok(_) => {
                info!(hash = %hash_hex, "tx confirmed");
                counter!(
                    "keeper_tx_confirmed_total",
                    "outcome" => "confirmed",
                )
                .increment(1);
                if let Some(hook) = &on_settle
                    && let Some(metric) = &hook.liquidation_outcome
                {
                    let net = metric.expected_net_value.max(0);
                    counter!(
                        "liquidator_plan_realised_net_profit_oracle_units_total",
                        "market" => metric.market.clone(),
                    )
                    .increment(net as u64);
                    histogram!(
                        "liquidator_plan_realised_net_profit_oracle_units",
                        "market" => metric.market.clone(),
                    )
                    .record(net as f64);
                }
            }
            Err(e) => {
                warn!(hash = %hash_hex, %e, "tx confirmation poll failed");
                let outcome = match SorobanRpcError::classify(&e) {
                    SorobanRpcError::TxFailedOnChain | SorobanRpcError::Contract { .. } => {
                        "failed_on_chain"
                    }
                    SorobanRpcError::SubmissionTimeout => "submission_timeout",
                    SorobanRpcError::UnexpectedStatus => "unexpected_status",
                    _ => "transport_error",
                };
                counter!(
                    "keeper_tx_confirmed_total",
                    "outcome" => outcome,
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
    seq_num_to_use: i64,
    network_passphrase: &str,
    action: &SubmitStellarTx,
    default_simulation_fee: u32,
    source_strkey: &stellar_strkey::ed25519::PublicKey,
) -> Result<String> {
    info!(
        seq_num_to_use,
        ?action,
        default_simulation_fee,
        %source_strkey
    );

    let tx = Transaction {
        source_account: MuxedAccount::Ed25519(Uint256(source_strkey.0)),
        fee: default_simulation_fee,
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
            ?sim_response,
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
