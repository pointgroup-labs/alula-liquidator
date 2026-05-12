//! Low-level RPC plumbing on top of `stellar_rpc_client::Client`.
//!
//! Internal to the `stellar` adapter. Strategies never reach here.

use {
    super::{
        Gateway,
        xdr_codec::{account_strkey_to_muxed, contract_strkey_to_hash},
    },
    anyhow::anyhow,
    stellar_rpc_client::AuthMode,
    stellar_xdr::curr::{
        ContractId, Hash, HostFunction, InvokeContractArgs, Memo, Operation, OperationBody,
        Preconditions, ScAddress, ScSymbol, ScVal, SequenceNumber, Transaction,
        TransactionEnvelope, TransactionExt, TransactionV1Envelope, VecM,
    },
};

/// Fee passed in simulation envelopes; ignored by simulate but required by the
/// XDR. Mirrors `pipeline::constants::DEFAULT_SIMULATION_FEE`.
const DEFAULT_SIMULATION_FEE: u32 = 100_000;

impl Gateway {
    pub(super) async fn simulate_contract_call(
        &self,
        contract_address: &str,
        function_name: &str,
        args: &[ScVal],
    ) -> anyhow::Result<ScVal> {
        let contract_hash = contract_strkey_to_hash(contract_address)?;
        let source_account_id = account_strkey_to_muxed(&self.source_account)?;

        let invoke_args = InvokeContractArgs {
            contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
            function_name: ScSymbol(function_name.try_into()?),
            args: args.to_vec().try_into()?,
        };
        let tx = Transaction {
            source_account: source_account_id,
            fee: DEFAULT_SIMULATION_FEE,
            seq_num: SequenceNumber(0),
            cond: Preconditions::None,
            memo: Memo::None,
            operations: vec![Operation {
                source_account: None,
                body: OperationBody::InvokeHostFunction(stellar_xdr::curr::InvokeHostFunctionOp {
                    host_function: HostFunction::InvokeContract(invoke_args),
                    auth: VecM::default(),
                }),
            }]
            .try_into()?,
            ext: TransactionExt::V0,
        };
        let envelope = TransactionEnvelope::Tx(TransactionV1Envelope {
            tx,
            signatures: VecM::default(),
        });

        let sim_response = self
            .rpc
            .simulate_transaction_envelope(&envelope, Some(AuthMode::Record))
            .await?;
        if let Some(error) = &sim_response.error {
            return Err(anyhow!("simulation failed for {function_name}: {error}"));
        }

        let results = sim_response.results()?;
        if results.is_empty() {
            return Err(anyhow!(
                "simulation returned no results for {function_name}"
            ));
        }

        Ok(results[0].xdr.clone())
    }
}
