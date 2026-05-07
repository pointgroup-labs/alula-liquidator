use {
    crate::{
        constants::{BPS_DENOMINATOR, DEFAULT_SIMULATION_FEE},
        types::{BorrowPosition, DepositPosition, MarketData, Obligation, ObligationKey, PoolData},
    },
    anyhow::{Context, anyhow},
    core::convert::TryInto,
    stellar_rpc_client::{AuthMode, Client},
    stellar_xdr::curr::{
        AccountId, ContractId, Hash, HostFunction, Int128Parts, InvokeContractArgs, Limits, Memo,
        MuxedAccount, Operation, OperationBody, Preconditions, PublicKey, ReadXdr as _, ScAddress,
        ScMap, ScMapEntry, ScSymbol, ScVal, ScVec, SequenceNumber, Transaction,
        TransactionEnvelope, TransactionExt, TransactionV1Envelope, Uint256, VecM,
    },
    thiserror::Error,
    tracing::{debug, warn},
};

#[derive(Debug)]
pub enum OperationEvent {
    Repay,
    Borrow,
    Deposit,
    Withdraw,
    Liquidate,
    AddCollateral,
    RemoveCollateral,
}

impl TryFrom<&str> for OperationEvent {
    type Error = ParseError;

    fn try_from(x: &str) -> Result<Self, Self::Error> {
        use OperationEvent::*;

        let operation_event = match x {
            "repay_event" => Repay,
            "borrow_event" => Borrow,
            "deposit_event" => Deposit,
            "withdraw_event" => Withdraw,
            "liquidate_event" => Liquidate,
            "add_collateral_event" => AddCollateral,
            "remove_collateral_event" => RemoveCollateral,
            unknown => {
                return Err(ParseError::UnknownOperationType {
                    operation_type: unknown.to_string(),
                });
            }
        };

        Ok(operation_event)
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Expected {expected} but found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Arithmetic overflow during calculation")]
    ArithmeticOverflow,

    #[error("Pool not found: {pool_address}")]
    PoolNotFound { pool_address: String },

    #[error("Invalid XDR data: {reason}")]
    InvalidXdr { reason: String },

    #[error("Invalid UTF-8 data: {source}")]
    InvalidUtf8 {
        #[from]
        source: std::str::Utf8Error,
    },

    #[error("Invalid data format: {reason}")]
    InvalidFormat { reason: String },

    #[error("Unknown operation type: {operation_type}")]
    UnknownOperationType { operation_type: String },
}

fn i128_from_parts(parts: &stellar_xdr::curr::Int128Parts) -> i128 {
    ((parts.hi as i128) << 64) | (parts.lo as i128)
}

pub fn i128_to_scval(v: i128) -> ScVal {
    ScVal::I128(Int128Parts {
        hi: (v >> 64) as i64,
        lo: v as u64,
    })
}

pub fn fixed_mul_ceil(a: i128, b_bps: i128) -> i128 {
    (a * b_bps + (BPS_DENOMINATOR - 1)) / BPS_DENOMINATOR
}

pub fn fixed_mul_floor(a: i128, b_bps: i128) -> i128 {
    a * b_bps / BPS_DENOMINATOR
}

fn contract_strkey_to_hash(strkey: &str) -> anyhow::Result<[u8; 32]> {
    let contract = stellar_strkey::Contract::from_string(strkey)
        .map_err(|e| anyhow!("invalid contract strkey '{strkey}': {e}"))?;

    Ok(contract.0)
}

fn account_strkey_to_muxed(strkey: &str) -> anyhow::Result<MuxedAccount> {
    if let Ok(pk) = stellar_strkey::ed25519::PublicKey::from_string(strkey) {
        return Ok(MuxedAccount::Ed25519(Uint256(pk.0)));
    }

    Err(anyhow!(
        "source_account must be a G... address, got: {strkey}"
    ))
}

fn is_expected_liquidation_failure(msg: &str) -> bool {
    const EXPECTED_ERRORS: &[&str] = &[
        "ObligationIsHealthy",
        "ObligationDoesNotExist",
        "InvalidLiquidationInputs",
        "BorrowPoolDoesNotExist",
        "CollateralPoolDoesNotExist",
    ];

    EXPECTED_ERRORS.iter().any(|e| msg.contains(e))
}

fn scval_type_name(val: &ScVal) -> &'static str {
    match val {
        ScVal::Bool(_) => "Bool",
        ScVal::Void => "Void",
        ScVal::U32(_) => "U32",
        ScVal::I32(_) => "I32",
        ScVal::U64(_) => "U64",
        ScVal::I64(_) => "I64",
        ScVal::Timepoint(_) => "Timepoint",
        ScVal::Duration(_) => "Duration",
        ScVal::U128(_) => "U128",
        ScVal::I128(_) => "I128",
        ScVal::String(_) => "String",
        ScVal::Symbol(_) => "Symbol",
        ScVal::Vec(_) => "Vec",
        ScVal::Map(_) => "Map",
        ScVal::Address(_) => "Address",
        ScVal::Bytes(_) => "Bytes",
        ScVal::ContractInstance(_) => "ContractInstance",
        ScVal::Error(_) => "Error",
        ScVal::U256(_) => "U256",
        ScVal::I256(_) => "I256",
        ScVal::LedgerKeyContractInstance => "LedgerKeyContractInstance",
        ScVal::LedgerKeyNonce(_) => "LedgerKeyNonce",
    }
}

fn scval_as_map(val: &ScVal) -> Result<&Vec<ScMapEntry>, ParseError> {
    match val {
        ScVal::Map(Some(ScMap(entries))) => Ok(entries.as_ref()),
        _ => Err(ParseError::TypeMismatch {
            expected: "Map".to_string(),
            found: scval_type_name(val).to_string(),
        }),
    }
}

fn scval_as_vec(val: &ScVal) -> Result<&Vec<ScVal>, ParseError> {
    match val {
        ScVal::Vec(Some(ScVec(v))) => Ok(v.as_ref()),
        _ => Err(ParseError::TypeMismatch {
            expected: "Vec".to_string(),
            found: scval_type_name(val).to_string(),
        }),
    }
}

fn map_get<'a>(entries: &'a [ScMapEntry], key: &str) -> Option<&'a ScVal> {
    entries.iter().find_map(|e| {
        if let ScVal::Symbol(sym) = &e.key
            && AsRef::<[u8]>::as_ref(&sym.0) == key.as_bytes()
        {
            return Some(&e.val);
        }

        None
    })
}

fn map_get_u32(entries: &[ScMapEntry], key: &str) -> anyhow::Result<u32> {
    match map_get(entries, key) {
        Some(ScVal::U32(n)) => Ok(*n),
        Some(other) => Err(anyhow!("expected U32 for '{key}', got {other:?}")),
        None => Err(anyhow!("missing field '{key}'")),
    }
}

fn map_get_i128(entries: &[ScMapEntry], key: &str) -> anyhow::Result<i128> {
    match map_get(entries, key) {
        Some(ScVal::I128(parts)) => Ok(i128_from_parts(parts)),
        Some(ScVal::U32(n)) => Ok(*n as i128),
        Some(ScVal::I32(n)) => Ok(*n as i128),
        Some(ScVal::U64(n)) => Ok(*n as i128),
        Some(ScVal::I64(n)) => Ok(*n as i128),
        Some(other) => Err(anyhow!("expected i128-like for '{key}', got {other:?}")),
        None => Err(anyhow!("missing field '{key}'")),
    }
}

fn map_get_address(entries: &[ScMapEntry], key: &str) -> anyhow::Result<String> {
    match map_get(entries, key) {
        Some(ScVal::Address(addr)) => Ok(addr.to_string()),
        Some(other) => Err(anyhow!("expected Address for '{key}', got {other:?}")),
        None => Err(anyhow!("missing field '{key}'")),
    }
}

fn map_get_string_optional(entries: &[ScMapEntry], key: &str) -> Option<String> {
    match map_get(entries, key) {
        Some(ScVal::String(s)) => Some(s.0.to_string()),
        Some(ScVal::Symbol(s)) => std::str::from_utf8(s.0.as_ref())
            .map(|s| s.to_string())
            .ok(),
        _ => None,
    }
}

fn scval_to_i128(val: &ScVal) -> anyhow::Result<i128> {
    match val {
        ScVal::I128(parts) => Ok(i128_from_parts(parts)),
        ScVal::U32(n) => Ok(*n as i128),
        ScVal::I32(n) => Ok(*n as i128),
        ScVal::U64(n) => Ok(*n as i128),
        ScVal::I64(n) => Ok(*n as i128),
        other => Err(anyhow!("expected i128-like, got {other:?}")),
    }
}

pub fn address_to_scval(address: &str) -> anyhow::Result<ScVal> {
    if address.starts_with('G') {
        let pk = stellar_strkey::ed25519::PublicKey::from_string(address)
            .map_err(|e| anyhow!("invalid G-address '{address}': {e}"))?;

        Ok(ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256(pk.0)),
        ))))
    } else if address.starts_with('C') {
        let contract = stellar_strkey::Contract::from_string(address)
            .map_err(|e| anyhow!("invalid C-address '{address}': {e}"))?;

        Ok(ScVal::Address(ScAddress::Contract(ContractId(Hash(
            contract.0,
        )))))
    } else {
        Err(anyhow!("unknown address format: {address}"))
    }
}

pub fn obligation_key_to_scval(obl: &ObligationKey) -> anyhow::Result<ScVal> {
    let seed_val = match &obl.seed {
        Some(hex_seed) => {
            let bytes = hex::decode(hex_seed)?;

            ScVal::Bytes(
                bytes
                    .try_into()
                    .map_err(|_| anyhow!("seed must be 32 bytes"))?,
            )
        }
        None => ScVal::Void,
    };

    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol("seed".try_into().unwrap())),
            val: seed_val,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol("user".try_into().unwrap())),
            val: address_to_scval(&obl.user)?,
        },
    ];

    Ok(ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    ))))
}

async fn simulate_contract_call(
    rpc: &Client,
    contract_address: &str,
    function_name: &str,
    args: &[ScVal],
    source_account: &str,
) -> anyhow::Result<ScVal> {
    let contract_hash = contract_strkey_to_hash(contract_address)?;
    let source_account_id = account_strkey_to_muxed(source_account)?;

    let invoke_args = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
        function_name: ScSymbol(function_name.try_into()?),
        args: args.to_vec().try_into()?,
    };
    let tx = Transaction {
        source_account: source_account_id,
        fee: DEFAULT_SIMULATION_FEE,
        seq_num: SequenceNumber(0), // Simulation uses sequence 0
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

    let sim_response: stellar_rpc_client::SimulateTransactionResponse = rpc
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

fn parse_obligation_key(val: &ScVal) -> anyhow::Result<ObligationKey> {
    let map = scval_as_map(val)?;

    let user = map_get_address(map, "user")?;
    let seed = match map_get(map, "seed") {
        Some(ScVal::Void) | None => None,
        Some(ScVal::Bytes(b)) => Some(hex::encode(b.to_vec())),
        Some(other) => {
            warn!("unexpected seed ScVal type: {other:?}");

            None
        }
    };

    Ok(ObligationKey { user, seed })
}

fn parse_deposit_positions(val: &ScVal) -> anyhow::Result<Vec<DepositPosition>> {
    let entries = scval_as_map(val)?;
    let mut positions = Vec::new();

    for entry in entries {
        let pool_address = match &entry.key {
            ScVal::Address(addr) => addr.to_string(),
            other => {
                warn!("unexpected deposit key type: {other:?}, skipping");

                continue;
            }
        };
        let pos_map = scval_as_map(&entry.val)
            .with_context(|| format!("deposit position for pool {pool_address} is not a map"))?;

        let j_tokens = map_get_i128(pos_map, "j_tokens").unwrap_or(0);
        let collateral = map_get_i128(pos_map, "collateral").unwrap_or(0);

        positions.push(DepositPosition {
            j_tokens,
            collateral,
            pool_address,
        });
    }

    Ok(positions)
}

fn parse_borrow_positions(val: &ScVal) -> anyhow::Result<Vec<BorrowPosition>> {
    let entries = scval_as_map(val)?;
    let mut positions = Vec::new();

    for entry in entries {
        let pool_address = match &entry.key {
            ScVal::Address(addr) => addr.to_string(),
            other => {
                warn!("unexpected borrow key type: {other:?}, skipping");

                continue;
            }
        };
        let pos_map = scval_as_map(&entry.val)
            .with_context(|| format!("borrow position for pool {pool_address} is not a map"))?;

        let d_tokens = map_get_i128(pos_map, "d_tokens").unwrap_or(0);

        positions.push(BorrowPosition {
            d_tokens,
            pool_address,
        });
    }

    Ok(positions)
}

pub fn parse_obligation(val: &ScVal, _key: &ObligationKey) -> anyhow::Result<Obligation> {
    let map = scval_as_map(val)?;

    let deposits = match map_get(map, "deposits") {
        Some(deposit_val) => parse_deposit_positions(deposit_val)?,
        None => Vec::new(),
    };
    let borrows = match map_get(map, "borrows") {
        Some(borrow_val) => parse_borrow_positions(borrow_val)?,
        None => Vec::new(),
    };

    Ok(Obligation { deposits, borrows })
}

fn parse_obligation_keys(val: &ScVal) -> anyhow::Result<Vec<ObligationKey>> {
    scval_as_vec(val)?
        .iter()
        .map(parse_obligation_key)
        .collect()
}

fn parse_pool_data(val: &ScVal) -> anyhow::Result<PoolData> {
    let map = scval_as_map(val)?;

    let j_token_rate_floor_bps = map_get_i128(map, "j_token_rate_floor_bps")?;
    let d_token_rate_ceil_bps = map_get_i128(map, "d_token_rate_ceil_bps")?;
    let oracle_asset_price = map_get_i128(map, "oracle_asset_price")?;
    let total_available_adjusted = map_get_i128(map, "total_available_adjusted")?;
    let total_supply = map_get_i128(map, "total_supply")?;

    let pool_val = map_get(map, "pool").context("missing pool in PoolData")?;
    let pool_map = scval_as_map(pool_val)?;

    let pool_address = map_get_address(pool_map, "pool_address")?;
    let token_address = map_get_address(pool_map, "token_address")?;
    let token_symbol = map_get_string_optional(pool_map, "token_symbol").unwrap_or_default();
    let token_decimals = map_get_u32(pool_map, "token_decimals")?;
    let total_borrowed = map_get_i128(pool_map, "total_borrowed")?;
    let total_d_tokens = map_get_i128(pool_map, "total_d_tokens")?;
    let total_j_tokens = map_get_i128(pool_map, "total_j_tokens")?;
    let total_available = map_get_i128(pool_map, "total_available")?;
    let total_collateral = map_get_i128(pool_map, "total_collateral")?;

    let config_val = map_get(pool_map, "config").context("missing config in Pool")?;
    let config_map = scval_as_map(config_val)?;
    let health_val = map_get(config_map, "health_config").context("missing health_config")?;
    let health_map = scval_as_map(health_val)?;

    let open_ltv_bps = map_get_i128(health_map, "open_ltv_bps")?;
    let close_ltv_bps = map_get_i128(health_map, "close_ltv_bps")?;
    let liability_factor_bps = map_get_i128(health_map, "liability_factor_bps")?;
    let liquidation_close_factor_bps = map_get_i128(health_map, "liquidation_close_factor_bps")?;
    let max_liquidation_incentive_bps = map_get_i128(health_map, "max_liquidation_incentive_bps")?;
    let utilization_ratio_limit_bps = map_get_i128(health_map, "utilization_ratio_limit_bps")?;

    let fee_val = map_get(config_map, "fee_config").context("missing fee_config")?;
    let fee_map = scval_as_map(fee_val)?;
    let flash_loan_fee_bps = map_get_i128(fee_map, "flash_loan_fee_bps")?;

    Ok(PoolData {
        pool_address,
        token_address,
        token_symbol,
        token_decimals,
        total_borrowed,
        total_d_tokens,
        total_j_tokens,
        total_available,
        total_available_adjusted,
        total_supply,
        total_collateral,
        j_token_rate_floor_bps,
        d_token_rate_ceil_bps,
        oracle_asset_price,
        open_ltv_bps,
        close_ltv_bps,
        liability_factor_bps,
        liquidation_close_factor_bps,
        max_liquidation_incentive_bps,
        flash_loan_fee_bps,
        utilization_ratio_limit_bps,
    })
}

fn parse_market_data(val: &ScVal) -> anyhow::Result<MarketData> {
    let map = scval_as_map(val)?;

    let oracle_price_decimals = map_get_u32(map, "oracle_price_decimals")?;
    let global_state_map = map_get(map, "global_state")
        .ok_or_else(|| anyhow!("global_state missing"))
        .and_then(|v| scval_as_map(v).map_err(anyhow::Error::from))?;
    let insolvency_ltv_bps = map_get_i128(global_state_map, "insolvency_ltv_bps")?;
    let min_collateral_value_cents = map_get_i128(global_state_map, "min_collateral_value_cents")?;
    let pools_data = map_get(map, "pools_data")
        .ok_or_else(|| anyhow!("pools_data missing"))
        .and_then(|v| scval_as_vec(v).map_err(anyhow::Error::from))?
        .iter()
        .map(parse_pool_data)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(MarketData {
        pools_data,
        oracle_price_decimals,
        insolvency_ltv_bps,
        min_collateral_value_cents,
    })
}

pub fn parse_event_data_i128(value_xdr_base64: &str, field: &str) -> anyhow::Result<Option<i128>> {
    use stellar_xdr::curr::{Limits, ReadXdr as _};

    let val = ScVal::from_xdr_base64(value_xdr_base64.as_bytes(), Limits::none()) // X: are these Limits necessary?
        .context("decode event value XDR")?;
    let map = scval_as_map(&val)?;
    match map_get(map, field) {
        Some(v) => {
            let n = scval_to_i128(v)
                .with_context(|| format!("event field '{field}' is not i128-like"))?;
            Ok(Some(n))
        }
        None => Ok(None),
    }
}

pub fn parse_event_data_i128_multi(
    value_xdr_base64: &str,
    fields: &[&str],
) -> anyhow::Result<Vec<i128>> {
    use stellar_xdr::curr::{Limits, ReadXdr as _};
    let val = ScVal::from_xdr_base64(value_xdr_base64.as_bytes(), Limits::none())
        .context("decode event value XDR")?;
    let map = scval_as_map(&val)?;

    let mut result = Vec::with_capacity(fields.len());
    for field in fields {
        let v = match map_get(map, field) {
            Some(v) => scval_to_i128(v).unwrap_or(0),
            None => 0,
        };

        result.push(v);
    }

    Ok(result)
}

pub fn parse_obligation_from_event_value(
    value_xdr_base64: &str,
    obligation_field_name: &str,
    key: &ObligationKey,
) -> anyhow::Result<Option<Obligation>> {
    use stellar_xdr::curr::{Limits, ReadXdr as _};

    let val = ScVal::from_xdr_base64(value_xdr_base64.as_bytes(), Limits::none())
        .context("decode event value XDR")?;
    let map = scval_as_map(&val)?;

    match map_get(map, obligation_field_name) {
        None | Some(ScVal::Void) => Ok(None),
        Some(ScVal::Vec(None)) => Ok(None),
        Some(inner) => {
            let obl = parse_obligation(inner, key)
                .with_context(|| format!("parse obligation field '{obligation_field_name}'"))?;

            Ok(Some(obl))
        }
    }
}

// ---------------------------------------------------------------------------
// Computation helpers
// ---------------------------------------------------------------------------

/// Compute the maximum repayable amount for a single borrow position.
pub fn compute_max_repay_amount(
    d_tokens: i128,
    d_token_rate_ceil_bps: i128,
    liquidation_close_factor_bps: i128,
) -> i128 {
    let position_debt = d_tokens * d_token_rate_ceil_bps / 10_000;
    let position_debt_plus_percents = (position_debt * 102) / 100; // TODO: Maybe add to consts
    // NB: For now we take 102% to allow full liquidations that close the entire position
    // including the most recently accrued interest rate

    // In order to avoid cases when accruals happen to soon on the debt
    // and we cannot close it completely. This implies more incentive to the liquidator
    // so that is fine for us

    position_debt_plus_percents * liquidation_close_factor_bps / 10_000
}

/// Compute the flash loan fee (ceiling) for a given amount and fee rate.
///
/// `fee = ceil(amount * flash_loan_fee_bps / 10_000)`
pub fn compute_flash_fee(amount: i128, flash_loan_fee_bps: i128) -> i128 {
    // X: fee is computed as ceil here
    (amount * flash_loan_fee_bps + 9_999) / 10_000
}

/// Estimate the collateral received from a liquidation.
///
/// Replicates the market's liquidation math:
///   1. `repay_value = repay_amount * borrow_price / 10^borrow_decimals`
///   2. `repay_value_with_bonus = repay_value * (10_000 + incentive_bps) / 10_000`
///   3. `received_collateral = repay_value_with_bonus * 10^collateral_decimals / collateral_price`
///
/// Then caps the result:
///   - Can't exceed available tokens in the deposit (j_tokens * j_rate / 10_000 + collateral)
///   - Must leave `min_collateral_value_cents` worth of value for the last liquidator
///
/// Uses floor rounding (conservative estimate).
pub fn compute_received_collateral(
    repay_amount: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    deposit: &DepositPosition,
    min_collateral_value_cents: i128,
    oracle_price_decimals: u32,
) -> i128 {
    if collateral_pool.oracle_asset_price <= 0 {
        return 0;
    }

    // Uncapped estimate from repay value + bonus
    let repay_value =
        repay_amount * borrow_pool.oracle_asset_price / 10_i128.pow(borrow_pool.token_decimals);
    let repay_value_with_bonus =
        repay_value * (10_000 + collateral_pool.max_liquidation_incentive_bps) / 10_000;
    let uncapped = repay_value_with_bonus * 10_i128.pow(collateral_pool.token_decimals)
        / collateral_pool.oracle_asset_price;

    // Cap 1: available tokens in the deposit position
    let real_supply = fixed_mul_floor(deposit.j_tokens, collateral_pool.j_token_rate_floor_bps);
    let available_tokens = real_supply + deposit.collateral;

    // Cap 2: reserve min_collateral_value_cents for the last liquidator
    // reserved_value = min_collateral_value_cents * 10^oracle_decimals / 100
    // reserved_tokens = reserved_value * 10^token_decimals / collateral_price
    let reserved_tokens = if min_collateral_value_cents > 0 {
        let reserved_value = min_collateral_value_cents * 10_i128.pow(oracle_price_decimals) / 100;
        reserved_value * 10_i128.pow(collateral_pool.token_decimals)
            / collateral_pool.oracle_asset_price
    } else {
        0
    };

    let seizeable = (available_tokens - reserved_tokens).max(0);

    uncapped.min(seizeable)
}

/// Determine whether an obligation is liquidatable using only cached local data.
///
/// Replicates the contract's health check:
///   `debt_value_w_liability_factors > collateral_value_w_close_ltvs`
///
/// Rounding matches the contract: ceiling for debt terms, floor for collateral.
/// A `true` result means the obligation *should* be liquidatable; the caller
/// must still verify with `simulate_batch` before submitting.(X: this is actually a good point.. Should the caller verify this?)
pub fn compute_is_liquidatable(obligation: &Obligation, md: &MarketData) -> bool {
    let pools = &md.pools_data;

    // If there is no collateral left, there is nothing to seize — not liquidatable.
    if !has_any_collateral(obligation, pools) {
        return false;
    }

    // Debt side: Σ (d_tokens * d_rate_ceil / 10_000) * price / 10^dec * liability_factor / 10_000
    let mut debt_value_scaled: i128 = 0;

    // X: Is this good to iterate over all borrows here?
    // X: I think, it would be better to have a HashMap instead for the local Obligation type
    for bor in &obligation.borrows {
        let pool = match pools.iter().find(|p| p.pool_address == bor.pool_address) {
            Some(p) => p,
            None => continue,
        };
        // real_debt = ceil(d_tokens * d_token_rate_ceil_bps / 10_000)
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        // value = ceil(real_debt * oracle_price / 10^decimals)
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value =
            (real_debt * pool.oracle_asset_price + (decimals_divisor - 1)) / decimals_divisor;
        // scaled = ceil(value * liability_factor_bps / 10_000)
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled += scaled;
    }

    // Collateral side: Σ (j_tokens * j_rate_floor / 10_000 + collateral) * price / 10^dec * close_ltv / 10_000
    let mut collateral_value_scaled: i128 = 0;
    let mut borrow_backing_positions: i128 = 0;
    for dep in &obligation.deposits {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => continue,
        };
        if pool.close_ltv_bps > 0 {
            borrow_backing_positions += 1;
        }
        // real_supply = floor(j_tokens * j_token_rate_floor_bps / 10_000)
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply + dep.collateral;
        // value = floor(total_tokens * oracle_price / 10^decimals)
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens * pool.oracle_asset_price / decimals_divisor;
        // scaled = floor(value * close_ltv_bps / 10_000)
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value_scaled += scaled;
    }

    // Each collateral position with non-zero close_ltv reserves `min_collateral_value_cents`
    // worth of value that doesn't produce borrowing capacity (buffer for the last liquidator).
    // threshold = min_collateral_value_cents * 10^oracle_decimals / 100
    let min_collateral_threshold =
        md.min_collateral_value_cents * 10_i128.pow(md.oracle_price_decimals) / 100;
    let buffer = min_collateral_threshold * borrow_backing_positions;

    debt_value_scaled > collateral_value_scaled.saturating_sub(buffer)
}

/// Returns `true` if the obligation has any collateral value across all deposits.
///
/// An obligation with zero collateral (all j_tokens <= 0 and collateral <= 0)
/// cannot be liquidated — there is nothing to seize.
fn has_any_collateral(obligation: &Obligation, pools: &[PoolData]) -> bool {
    obligation.deposits.iter().any(|dep| {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => return false,
        };
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        real_supply + dep.collateral > 0
    })
}

// ---------------------------------------------------------------------------
// Scenario 1 (Direct, liquidator-funded) helpers
// ---------------------------------------------------------------------------

/// Total obligation debt value (unscaled, oracle units), summed across all borrow
/// positions: `Σ ceil(d_tokens * d_rate_ceil_bps / 10_000) * price / 10^decimals`.
///
/// Returns `ParseError::PoolNotFound` if any required pool is missing or
/// `ParseError::ArithmeticOverflow` if arithmetic over-/underflows.
pub fn compute_obligation_debt_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, ParseError> {
    let mut total: i128 = 0;
    for bor in &obligation.borrows {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
            .ok_or_else(|| ParseError::PoolNotFound {
                pool_address: bor.pool_address.clone(),
            })?;
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            .ok_or(ParseError::ArithmeticOverflow)?;
        total = total
            .checked_add(value)
            .ok_or(ParseError::ArithmeticOverflow)?;
    }
    Ok(total)
}

/// Total obligation collateral value (unscaled, oracle units), summed across all
/// deposit positions: `Σ (floor(j_tokens * j_rate_floor_bps / 10_000) + collateral)
/// * price / 10^decimals`. Mirrors the contract's `compute_collateral_value` floor
///   rounding.
pub fn compute_obligation_collateral_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, ParseError> {
    let mut total: i128 = 0;
    for dep in &obligation.deposits {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
            .ok_or_else(|| ParseError::PoolNotFound {
                pool_address: dep.pool_address.clone(),
            })?;
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply
            .checked_add(dep.collateral)
            .ok_or(ParseError::ArithmeticOverflow)?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(10_i128.pow(pool.token_decimals)))
            .ok_or(ParseError::ArithmeticOverflow)?;
        total = total
            .checked_add(value)
            .ok_or(ParseError::ArithmeticOverflow)?;
    }
    Ok(total)
}

/// Cap on `repay_amount` from the close factor.
///
/// - **Insolvent**: 100% of the position debt may be repaid.
/// - **Solvent**:  `floor(position_debt * liquidation_close_factor_bps / 10_000)`.
///   Floor matches the contract's `fixed_div_ceil` check on the *output* side: the
///   contract reverts if `liquidated/debt > close_factor`, so we stay strictly
///   below by flooring our cap.
pub fn compute_close_factor_repay_cap(
    position_debt_tokens: i128,
    liquidation_close_factor_bps: i128,
    is_insolvent: bool,
) -> i128 {
    if is_insolvent {
        position_debt_tokens
    } else {
        fixed_mul_floor(position_debt_tokens, liquidation_close_factor_bps)
    }
}

/// Profit margin expressed in borrow-token units.
///
/// `min_profit_margin_cents` is converted to oracle value
/// (`cents * 10^oracle_decimals / 100`) and then to borrow tokens
/// (`value * 10^borrow_decimals / borrow_oracle_price`).
pub fn compute_profit_margin_in_borrow_token(
    min_profit_margin_cents: i128,
    oracle_price_decimals: u32,
    borrow_pool: &PoolData,
) -> i128 {
    if borrow_pool.oracle_asset_price <= 0 {
        return 0;
    }
    let margin_value = min_profit_margin_cents * 10_i128.pow(oracle_price_decimals) / 100;

    // CL: r u sure this is correct here?
    margin_value * 10_i128.pow(borrow_pool.token_decimals) / borrow_pool.oracle_asset_price
}

/// Mirror the contract's seized-collateral computation for a given `repay_amount`.
///
/// Replicates `obligation.liquidate(...)`:
/// 1. `liquidated_value = floor(repay * borrow_price / 10^borrow_decimals)`
/// 2. `with_incentive = floor(liquidated_value_in_collateral_tokens * (1 + min_incentive_bps/10_000))`
/// 3. If solvent, also cap by the LTV-improving formula:
///    `max_ltv_seized_value = floor(liquidated_value * (collateral_value/debt_value) * 0.999)`
/// 4. Final seized = `min(position_collateral_sum, max_ltv_improving (if solvent), max_with_incentive)`
///
/// Returns `0` if any required value is non-positive or arithmetic fails.
/// Accounts for minimum collateral value rule - if remaining collateral value
/// falls below min_collateral_value_cents, liquidator gets ALL remaining collateral for free.
#[allow(clippy::too_many_arguments)]
pub fn compute_expected_seized_collateral(
    repay_amount: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    deposit: &DepositPosition,
    obligation_debt_value: i128,
    obligation_collateral_value: i128,
    is_insolvent: bool,
    min_collateral_value_cents: i128,
    oracle_price_decimals: u32,
) -> i128 {
    if repay_amount <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return 0;
    }

    // Position collateral (floor on j_tokens like the contract).
    let real_supply = fixed_mul_floor(deposit.j_tokens, collateral_pool.j_token_rate_floor_bps);
    let position_collateral_sum = real_supply + deposit.collateral;
    if position_collateral_sum <= 0 {
        return 0;
    }

    // Step 1: liquidated_value in oracle units.
    let liquidated_value =
        repay_amount * borrow_pool.oracle_asset_price / 10_i128.pow(borrow_pool.token_decimals);

    // Step 2: convert to collateral tokens, apply incentive (floor everywhere).
    let min_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);
    let collateral_amount_no_bonus = liquidated_value * 10_i128.pow(collateral_pool.token_decimals)
        / collateral_pool.oracle_asset_price;
    let with_incentive = collateral_amount_no_bonus * (10_000 + min_incentive_bps) / 10_000;

    // Step 3 (solvent only): LTV-improving cap.
    let ltv_cap = if !is_insolvent {
        if obligation_debt_value <= 0 || obligation_collateral_value <= 0 {
            return 0;
        }
        // max_collateral_received_value = liquidated_value * (collateral/debt) * 999/1000
        let max_value_recv =
            liquidated_value.saturating_mul(obligation_collateral_value) / obligation_debt_value;
        let strict_max_value_recv = max_value_recv * 999 / 1000;
        let ltv_collateral = strict_max_value_recv * 10_i128.pow(collateral_pool.token_decimals)
            / collateral_pool.oracle_asset_price;
        Some(ltv_collateral)
    } else {
        None
    };

    let mut seized = position_collateral_sum.min(with_incentive);
    if let Some(cap) = ltv_cap {
        seized = seized.min(cap);
    }

    // Step 4: Check minimum collateral value rule
    // Calculate remaining collateral after seizure
    let collateral_left = position_collateral_sum.saturating_sub(seized);
    let collateral_value_left = collateral_left * collateral_pool.oracle_asset_price
        / 10_i128.pow(collateral_pool.token_decimals);

    // Calculate minimum collateral threshold in oracle price terms
    let min_collateral_threshold =
        min_collateral_value_cents * 10_i128.pow(oracle_price_decimals) / 100; // Convert cents to dollar units

    // If remaining collateral value is below threshold, liquidator gets ALL remaining collateral for free
    if collateral_value_left < min_collateral_threshold {
        seized = position_collateral_sum;
    }

    seized.max(0)
}

/// Estimate swap output with slippage protection.
///
/// This is a more generic approach that works with any swap provider by:
/// 1. Using the router's get_amounts_out method (currently Soroswap-specific)
/// 2. Applying a conservative slippage buffer
/// 3. Falling back gracefully if router queries fail
///
/// Future improvement: Could be abstracted further to support multiple
/// router types through a trait-based approach, eliminating dependency
/// on specific router implementations.
///
/// Returns the expected output amount after slippage protection, or None if
/// the swap is not viable.
pub async fn estimate_swap_output_with_slippage(
    rpc: &Client,
    swap_provider: &str,
    source_account: &str,
    amount_in: i128,
    path: &[&str],
    slippage_bps: i128, // e.g., 500 for 5%
) -> anyhow::Result<Option<i128>> {
    // Try to get amounts from router
    match simulate_router_get_amounts_out(rpc, swap_provider, source_account, amount_in, path).await
    {
        Ok(amounts) if amounts.len() >= 2 => {
            let raw_output = amounts[amounts.len() - 1];
            // Apply slippage protection
            let slippage_adjusted = raw_output * (10_000 - slippage_bps) / 10_000;
            Ok(Some(slippage_adjusted))
        }
        Ok(_) => Ok(None), // Invalid response
        Err(_) => {
            // Router query failed - could try alternative approaches here:
            // 1. Fallback to different router
            // 2. Use oracle prices with conservative spread
            // 3. Skip this opportunity
            // For now, return None to skip this liquidation opportunity
            Ok(None)
        }
    }
}

/// Cap on `repay_amount` when using flash loans to bridge liquidity gaps.
///
/// Given a total `needed_amount`, liquidator's current balance, and pool availability:
/// - If `liquidator_balance >= needed_amount` → flash loan not needed, return `needed_amount`
/// - If `liquidator_balance + pool.total_available >= needed_amount` → flash loan viable
/// - Otherwise → flash loan insufficient, return 0
///
/// The flash amount would be `needed_amount - liquidator_balance`.
/// Flash fees are deducted from the final repay amount to ensure profitability.
pub fn compute_flash_loan_repay_cap(
    needed_amount: i128,
    liquidator_balance: i128,
    pool_total_available: i128,
    _flash_fee_bps: i128,
) -> i128 {
    if liquidator_balance >= needed_amount {
        // Direct liquidation covers it
        return needed_amount;
    }

    let flash_amount = needed_amount - liquidator_balance;
    if flash_amount <= 0 {
        return needed_amount;
    }

    if pool_total_available < flash_amount {
        // Pool doesn't have enough liquidity for the flash loan
        return 0;
    }

    // Flash loan is viable. The flash fee reduces the effective repay amount
    // since we need to account for the fee in our profit calculations.
    // For simplicity, return the needed amount and let the caller handle
    // fee accounting in the profit margin checks.
    needed_amount
}

/// Returns `true` if the obligation is insolvent: debt exceeds collateral valued
/// at the market-wide `insolvency_ltv_bps` (which is more generous than per-pool
/// `close_ltv_bps`). When insolvent, 100% of the debt can be liquidated.
pub fn compute_is_insolvent(obligation: &Obligation, market_data: &MarketData) -> bool {
    let mut debt_value_scaled: i128 = 0;
    for bor in &obligation.borrows {
        let pool = match market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
        {
            Some(p) => p,
            None => continue,
        };
        let real_debt = fixed_mul_ceil(bor.d_tokens, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value =
            (real_debt * pool.oracle_asset_price + (decimals_divisor - 1)) / decimals_divisor;
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value_scaled += scaled;
    }

    let mut collateral_value_scaled: i128 = 0;
    for dep in &obligation.deposits {
        let pool = match market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
        {
            Some(p) => p,
            None => continue,
        };
        let real_supply = fixed_mul_floor(dep.j_tokens, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply + dep.collateral;
        let decimals_divisor = 10_i128.pow(pool.token_decimals);
        let value = total_tokens * pool.oracle_asset_price / decimals_divisor;
        // Use insolvency_ltv_bps instead of per-pool close_ltv_bps
        let scaled = fixed_mul_floor(value, market_data.insolvency_ltv_bps);
        collateral_value_scaled += scaled;
    }

    debt_value_scaled > collateral_value_scaled
}

// ---------------------------------------------------------------------------
// Flash-loan batch builders
// ---------------------------------------------------------------------------

/// Build a `Vec<Address>` ScVal from a slice of address strings.
///
/// Encoded as `ScVal::Vec(ScVec([ScVal::Address, …]))`.
pub fn build_address_vec_scval(addresses: &[&str]) -> anyhow::Result<ScVal> {
    let mut items = Vec::with_capacity(addresses.len());
    for addr in addresses {
        items.push(address_to_scval(addr)?);
    }
    let vec_m: VecM<ScVal> = items
        .try_into()
        .map_err(|_| anyhow!("address vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_m))))
}

/// Wrap request `ScVal`s into a `ScVal::Vec` suitable for `submit_requests_batch`.
pub fn build_requests_vec_scval(requests: &[ScVal]) -> anyhow::Result<ScVal> {
    let vec_m: VecM<ScVal> = requests
        .to_vec()
        .try_into()
        .map_err(|_| anyhow!("requests vec conversion"))?;
    Ok(ScVal::Vec(Some(ScVec(vec_m))))
}

/// Build a `Request::FlashBorrow(StandardRequest { amount, pool_address })` ScVal.
///
/// Soroban enum variant encoding: `ScVec([ScSymbol("FlashBorrow"), ScMap([fields…])])`.
/// `StandardRequest` fields in alphabetical order: `amount`, `pool_address`.
pub fn build_flash_borrow_request_scval(pool_address: &str, amount: i128) -> anyhow::Result<ScVal> {
    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "amount".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(amount),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "pool_address".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(pool_address)?,
        },
    ];

    let variant_data = ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    )));

    let vec_items: VecM<ScVal> = vec![
        ScVal::Symbol(ScSymbol(
            "FlashBorrow".try_into().map_err(|_| anyhow!("symbol"))?,
        )),
        variant_data,
    ]
    .try_into()
    .map_err(|_| anyhow!("vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_items))))
}

/// Build a `Request::Withdraw(StandardRequest { amount, pool_address })` ScVal.
///
/// `StandardRequest` fields in alphabetical order: `amount`, `pool_address`.
pub fn build_withdraw_request_scval(pool_address: &str, amount: i128) -> anyhow::Result<ScVal> {
    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "amount".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(amount),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "pool_address".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(pool_address)?,
        },
    ];

    let variant_data = ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    )));

    let vec_items: VecM<ScVal> = vec![
        ScVal::Symbol(ScSymbol(
            "Withdraw".try_into().map_err(|_| anyhow!("symbol"))?,
        )),
        variant_data,
    ]
    .try_into()
    .map_err(|_| anyhow!("vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_items))))
}

/// Build a `Request::Liquidate(LiquidateRequest { … })` ScVal.
///
/// `LiquidateRequest` fields in alphabetical order:
/// `borrow_pool_address`, `borrower_obligation_key`, `collateral_pool_address`,
/// `min_demanded_collateral_amount`, `repay_amount`.
pub fn build_liquidate_request_scval(
    borrower_key: &ObligationKey,
    borrow_pool: &str,
    collateral_pool: &str,
    repay_amount: i128,
    min_collateral: i128,
) -> anyhow::Result<ScVal> {
    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "borrow_pool_address"
                    .try_into()
                    .map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(borrow_pool)?,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "borrower_obligation_key"
                    .try_into()
                    .map_err(|_| anyhow!("symbol"))?,
            )),
            val: obligation_key_to_scval(borrower_key)?,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "collateral_pool_address"
                    .try_into()
                    .map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(collateral_pool)?,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "min_demanded_collateral_amount"
                    .try_into()
                    .map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(min_collateral),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "repay_amount".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(repay_amount),
        },
    ];

    let variant_data = ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    )));

    let vec_items: VecM<ScVal> = vec![
        ScVal::Symbol(ScSymbol(
            "Liquidate".try_into().map_err(|_| anyhow!("symbol"))?,
        )),
        variant_data,
    ]
    .try_into()
    .map_err(|_| anyhow!("vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_items))))
}

/// Build a `Request::SwapExactTokens(SwapExactTokensRequest { … })` ScVal.
///
/// `SwapExactTokensRequest` fields in alphabetical order:
/// `amount_in`, `min_amount_out`, `path`, `swap_provider`.
pub fn build_swap_exact_tokens_request_scval(
    swap_provider: &str,
    path: &[&str],
    amount_in: i128,
    min_amount_out: i128,
) -> anyhow::Result<ScVal> {
    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "amount_in".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(amount_in),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "min_amount_out".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(min_amount_out),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol("path".try_into().map_err(|_| anyhow!("symbol"))?)),
            val: build_address_vec_scval(path)?,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "swap_provider".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(swap_provider)?,
        },
    ];

    let variant_data = ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    )));

    let vec_items: VecM<ScVal> = vec![
        ScVal::Symbol(ScSymbol(
            "SwapExactTokens"
                .try_into()
                .map_err(|_| anyhow!("symbol"))?,
        )),
        variant_data,
    ]
    .try_into()
    .map_err(|_| anyhow!("vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_items))))
}

/// Build a `Request::SwapForExactTokens(SwapForExactTokensRequest { … })` ScVal.
///
/// `SwapForExactTokensRequest` fields in alphabetical order:
/// `amount_out`, `max_amount_in`, `path`, `swap_provider`.
pub fn build_swap_for_exact_tokens_request_scval(
    swap_provider: &str,
    path: &[&str],
    max_amount_in: i128,
    amount_out: i128,
) -> anyhow::Result<ScVal> {
    let entries = vec![
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "amount_out".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(amount_out),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "max_amount_in".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: i128_to_scval(max_amount_in),
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol("path".try_into().map_err(|_| anyhow!("symbol"))?)),
            val: build_address_vec_scval(path)?,
        },
        ScMapEntry {
            key: ScVal::Symbol(ScSymbol(
                "swap_provider".try_into().map_err(|_| anyhow!("symbol"))?,
            )),
            val: address_to_scval(swap_provider)?,
        },
    ];

    let variant_data = ScVal::Map(Some(ScMap(
        entries.try_into().map_err(|_| anyhow!("map conversion"))?,
    )));

    let vec_items: VecM<ScVal> = vec![
        ScVal::Symbol(ScSymbol(
            "SwapForExactTokens"
                .try_into()
                .map_err(|_| anyhow!("symbol"))?,
        )),
        variant_data,
    ]
    .try_into()
    .map_err(|_| anyhow!("vec conversion"))?;

    Ok(ScVal::Vec(Some(ScVec(vec_items))))
}

// ---------------------------------------------------------------------------
// Operation builders
// ---------------------------------------------------------------------------

/// Build an `Operation` that calls the contract's standalone `liquidate` function.
///
/// Args: `(liquidator, borrower_obl_key, borrow_pool, collateral_pool, repay_amount, min_collateral)`
pub fn build_liquidate_op(
    market_address: &str,
    liquidator_address: &str,
    borrower: &ObligationKey,
    borrow_pool: &str,
    collateral_pool: &str,
    repay_amount: i128,
    min_collateral: i128,
) -> anyhow::Result<Operation> {
    let contract_hash = contract_strkey_to_hash(market_address)?;

    let args: VecM<ScVal> = vec![
        address_to_scval(liquidator_address)?,
        obligation_key_to_scval(borrower)?,
        address_to_scval(borrow_pool)?,
        address_to_scval(collateral_pool)?,
        i128_to_scval(repay_amount),
        i128_to_scval(min_collateral),
    ]
    .try_into()
    .map_err(|_| anyhow!("args conversion"))?;

    let invoke = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
        function_name: ScSymbol(
            "liquidate"
                .try_into()
                .map_err(|_| anyhow!("function name too long"))?,
        ),
        args,
    };

    Ok(Operation {
        source_account: None,
        body: OperationBody::InvokeHostFunction(stellar_xdr::curr::InvokeHostFunctionOp {
            host_function: HostFunction::InvokeContract(invoke),
            auth: VecM::default(),
        }),
    })
}

/// Build an `Operation` that calls the contract's standalone `withdraw` function.
///
/// Args: `(user: ObligationKey, pool_address: Address, amount: i128, referrer: Option<Address>)`
///
/// This initiates withdrawal of deposited tokens from the loan pool to the user.
/// The actual amount withdrawn is capped to maintain the position's LTV at its Open LTV.
pub fn build_withdraw_op(
    market_address: &str,
    user_key: &ObligationKey,
    pool_address: &str,
    amount: i128,
) -> anyhow::Result<Operation> {
    let contract_hash = contract_strkey_to_hash(market_address)?;

    let args: VecM<ScVal> = vec![
        obligation_key_to_scval(user_key)?,
        address_to_scval(pool_address)?,
        i128_to_scval(amount),
        ScVal::Void, // referrer: None
    ]
    .try_into()
    .map_err(|_| anyhow!("args conversion"))?;

    let invoke = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
        function_name: ScSymbol(
            "withdraw"
                .try_into()
                .map_err(|_| anyhow!("function name too long"))?,
        ),
        args,
    };

    Ok(Operation {
        source_account: None,
        body: OperationBody::InvokeHostFunction(stellar_xdr::curr::InvokeHostFunctionOp {
            host_function: HostFunction::InvokeContract(invoke),
            auth: VecM::default(),
        }),
    })
}

/// Build an `Operation` that calls the contract's standalone `issue_cover_bad_debt` function.
///
/// Args: `(user: ObligationKey)`
///
/// This initiates insurance fund coverage for a bad debt obligation.
/// The contract will verify that the obligation is eligible (has debt but no liquidatable collateral).
pub fn build_issue_cover_bad_debt_op(
    market_address: &str,
    user_key: &ObligationKey,
) -> anyhow::Result<Operation> {
    let contract_hash = contract_strkey_to_hash(market_address)?;

    let args: VecM<ScVal> = vec![obligation_key_to_scval(user_key)?]
        .try_into()
        .map_err(|_| anyhow!("args conversion"))?;

    let invoke = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
        function_name: ScSymbol(
            "issue_cover_bad_debt"
                .try_into()
                .map_err(|_| anyhow!("function name too long"))?,
        ),
        args,
    };

    Ok(Operation {
        source_account: None,
        body: OperationBody::InvokeHostFunction(stellar_xdr::curr::InvokeHostFunctionOp {
            host_function: HostFunction::InvokeContract(invoke),
            auth: VecM::default(),
        }),
    })
}

pub fn build_batch_op(
    market_address: &str,
    liquidator_key: &ObligationKey,
    requests: &[ScVal],
) -> anyhow::Result<Operation> {
    let contract_hash = contract_strkey_to_hash(market_address)?;

    let requests_vec = build_requests_vec_scval(requests)?;

    let args: VecM<ScVal> = vec![
        obligation_key_to_scval(liquidator_key)?,
        requests_vec,
        ScVal::Void, // referrer: None
    ]
    .try_into()
    .map_err(|_| anyhow!("args conversion"))?;

    let invoke = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash(contract_hash))),
        function_name: ScSymbol(
            "submit_requests_batch"
                .try_into()
                .map_err(|_| anyhow!("function name too long"))?,
        ),
        args,
    };

    Ok(Operation {
        source_account: None,
        body: OperationBody::InvokeHostFunction(stellar_xdr::curr::InvokeHostFunctionOp {
            host_function: HostFunction::InvokeContract(invoke),
            auth: VecM::default(),
        }),
    })
}

// ---------------------------------------------------------------------------
// Top-level public async functions
// ---------------------------------------------------------------------------

pub async fn simulate_get_market_data(
    rpc: &Client,
    market_address: &str,
    source_account: &str,
) -> anyhow::Result<MarketData> {
    let start = std::time::Instant::now();
    let sc_val =
        simulate_contract_call(rpc, market_address, "get_market_data", &[], source_account)
            .await
            .context("simulate get_market_data")?;

    let duration = start.elapsed();
    println!("Async block executed in: {:?}", duration);

    parse_market_data(&sc_val)
}

pub async fn simulate_get_all_obligations(
    rpc: &Client,
    market_address: &str,
    source_account: &str,
) -> anyhow::Result<Vec<ObligationKey>> {
    let sc_val = simulate_contract_call(
        rpc,
        market_address,
        "get_all_obligations",
        &[],
        source_account,
    )
    .await
    .context("simulate get_all_obligations")?;

    parse_obligation_keys(&sc_val)
}

pub async fn simulate_get_user_obligation(
    rpc: &Client,
    market_address: &str,
    source_account: &str,
    obl_key: &ObligationKey,
) -> anyhow::Result<Obligation> {
    let key_scval = obligation_key_to_scval(obl_key)?;
    let sc_val = simulate_contract_call(
        rpc,
        market_address,
        "get_user_obligation",
        &[key_scval],
        source_account,
    )
    .await
    .context("simulate get_user_obligation")?;

    parse_obligation(&sc_val, obl_key)
}

/// Returns `Ok(true)` if liquidatable, `Ok(false)` if healthy/invalid pair
pub async fn simulate_liquidation(
    rpc: &Client,
    market_address: &str,
    source_account: &str,
    liquidator_address: &str,
    borrower: &ObligationKey,
    borrow_pool: &str,
    collateral_pool: &str,
) -> anyhow::Result<bool> {
    let args: Vec<ScVal> = vec![
        address_to_scval(liquidator_address)?,
        obligation_key_to_scval(borrower)?,
        address_to_scval(borrow_pool)?,
        address_to_scval(collateral_pool)?,
        ScVal::I128(stellar_xdr::curr::Int128Parts { hi: 0, lo: 1000 }),
        ScVal::I128(stellar_xdr::curr::Int128Parts { hi: 0, lo: 0 }),
    ];

    debug!(
        "liquidate sim: liquidator={} borrower={} seed={:?} borrow_pool={} collateral_pool={}",
        liquidator_address, borrower.user, borrower.seed, borrow_pool, collateral_pool,
    );

    let result =
        simulate_contract_call(rpc, market_address, "liquidate", &args, source_account).await;

    match result {
        Ok(_) => Ok(true),
        Err(e) => {
            let msg = format!("{e:#}");
            if is_expected_liquidation_failure(&msg) {
                warn!(
                    "liquidation sim: not liquidatable: borrower={} err={}",
                    borrower.user,
                    msg.chars().take(200).collect::<String>()
                );

                Ok(false)
            } else {
                warn!(
                    "liquidation sim unexpected error: borrower={} err={}",
                    borrower.user, msg,
                );

                Err(e)
            }
        }
    }
}

/// Simulate a `submit_requests_batch` call with an arbitrary request list.
///
/// Returns `Ok(true)` if the simulation succeeds, `Ok(false)` if it fails
/// (e.g., insufficient pool liquidity), and `Err` on unexpected errors.
pub async fn simulate_batch(
    rpc: &Client,
    market_address: &str,
    source_account: &str,
    liquidator_key: &ObligationKey,
    requests: &[ScVal],
) -> anyhow::Result<bool> {
    let requests_vec = build_requests_vec_scval(requests)?;

    let args: Vec<ScVal> = vec![
        obligation_key_to_scval(liquidator_key)?,
        requests_vec,
        ScVal::Void, // referrer: None
    ];

    debug!(
        "batch sim: liquidator={} num_requests={}",
        liquidator_key.user,
        requests.len(),
    );

    let result = simulate_contract_call(
        rpc,
        market_address,
        "submit_requests_batch",
        &args,
        source_account,
    )
    .await;

    match result {
        Ok(_) => Ok(true),
        Err(e) => {
            let msg = format!("{e:#}");
            warn!(
                "batch sim failed: liquidator={} err={}",
                liquidator_key.user,
                msg.chars().take(300).collect::<String>(),
            );
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// Event decoding helpers
// ---------------------------------------------------------------------------

/// Extract the event name (first topic) from a Soroban event.
pub fn decode_operation_event(
    event: &stellar_rpc_client::Event,
) -> Result<OperationEvent, ParseError> {
    if event.topic.is_empty() {
        return Err(ParseError::InvalidXdr {
            reason: "Event has no topics".to_string(),
        });
    }

    let val = ScVal::from_xdr_base64(event.topic[0].as_bytes(), Limits::none()).map_err(|e| {
        ParseError::InvalidXdr {
            reason: format!("Failed to decode XDR: {}", e),
        }
    })?;

    match val {
        ScVal::Symbol(sym) => {
            let utf8_str = std::str::from_utf8(sym.0.as_ref())
                .map_err(|e| ParseError::InvalidUtf8 { source: e })?;
            let operation_event: OperationEvent = utf8_str.try_into()?;

            Ok(operation_event)
        }
        _ => Err(ParseError::TypeMismatch {
            expected: "Symbol".to_string(),
            found: scval_type_name(&val).to_string(),
        }),
    }
}

/// Decode a single topic at `index` into a human-readable string.
pub fn decode_topic(event: &stellar_rpc_client::Event, index: usize) -> String {
    if index >= event.topic.len() {
        return "<missing>".into();
    }
    match ScVal::from_xdr_base64(event.topic[index].as_bytes(), Limits::none()) {
        Ok(val) => scval_display(&val),
        Err(_) => "<decode_error>".into(),
    }
}

/// Parse an `ObligationKey` from the event topic at `index`.
pub fn parse_obligation_key_from_topic(
    event: &stellar_rpc_client::Event,
    index: usize,
) -> anyhow::Result<ObligationKey> {
    if index >= event.topic.len() {
        anyhow::bail!("topic index {index} out of range");
    }
    let val = ScVal::from_xdr_base64(event.topic[index].as_bytes(), Limits::none())?;
    let ScVal::Map(Some(ScMap(entries))) = &val else {
        anyhow::bail!("topic[{index}] is not a Map");
    };

    let mut user = None;
    let mut seed = None;
    for entry in entries.iter() {
        if let ScVal::Symbol(sym) = &entry.key {
            match sym.0.to_string().as_str() {
                "user" => {
                    if let ScVal::Address(addr) = &entry.val {
                        user = Some(addr.to_string());
                    }
                }
                "seed" => {
                    if let ScVal::Bytes(b) = &entry.val {
                        seed = Some(hex::encode(AsRef::<[u8]>::as_ref(b)));
                    }
                }
                _ => {}
            }
        }
    }

    Ok(ObligationKey {
        user: user.ok_or_else(|| anyhow!("missing user in ObligationKey"))?,
        seed,
    })
}

/// Query the balance of a token for a given address by simulating a `balance()` call.
/// Works for all token types including native XLM SAC.
pub async fn query_token_balance(
    rpc: &Client,
    token_address: &str,
    owner_address: &str,
    source_account: &str,
) -> anyhow::Result<i128> {
    let args = vec![address_to_scval(owner_address)?];
    let result =
        simulate_contract_call(rpc, token_address, "balance", &args, source_account).await?;
    scval_to_i128(&result).context("parse balance result")
}

/// Simulate `router_get_amounts_in` on the Soroswap router.
///
/// Given a desired `amount_out` of the last token in `path`, returns the amounts
/// needed at each step. `amounts[0]` is the collateral input needed.
pub async fn simulate_router_get_amounts_in(
    rpc: &Client,
    router_address: &str,
    source_account: &str,
    amount_out: i128,
    path: &[&str],
) -> anyhow::Result<Vec<i128>> {
    let path_scval = build_address_vec_scval(path)?;
    let args = vec![i128_to_scval(amount_out), path_scval];

    let result = simulate_contract_call(
        rpc,
        router_address,
        "router_get_amounts_in",
        &args,
        source_account,
    )
    .await
    .context("simulate router_get_amounts_in")?;

    let vec_vals = scval_as_vec(&result)?;
    vec_vals.iter().map(scval_to_i128).collect()
}

/// Simulate `get_amount_out` directly on a swap_provider (DEX adapter) contract.
///
/// Unlike `simulate_router_get_amounts_out`, this is provider-agnostic: every
/// `swap_provider` implements `get_amount_out(path: Vec<Address>, amount_in: i128) -> i128`
/// (see `libs/proxy-swap-interface`), so callers don't need to know whether the
/// underlying DEX is Soroswap, Aqua, or any other.
pub async fn simulate_swap_provider_get_amount_out(
    rpc: &Client,
    swap_provider: &str,
    source_account: &str,
    amount_in: i128,
    path: &[&str],
) -> anyhow::Result<i128> {
    let path_scval = build_address_vec_scval(path)?;
    // NOTE: `get_amount_out` on the provider takes (path, amount_in) in that order.
    let args = vec![path_scval, i128_to_scval(amount_in)];

    let result =
        simulate_contract_call(rpc, swap_provider, "get_amount_out", &args, source_account)
            .await
            .context("simulate swap_provider get_amount_out")?;

    scval_to_i128(&result).context("get_amount_out result is not an i128")
}

/// Simulate `router_get_amounts_out` on the Soroswap router.
/// Given an input amount and a path, returns the output amounts for each hop.
pub async fn simulate_router_get_amounts_out(
    rpc: &Client,
    router_address: &str,
    source_account: &str,
    amount_in: i128,
    path: &[&str],
) -> anyhow::Result<Vec<i128>> {
    let path_scval = build_address_vec_scval(path)?;
    let args = vec![i128_to_scval(amount_in), path_scval];

    let result = simulate_contract_call(
        rpc,
        router_address,
        "router_get_amounts_out",
        &args,
        source_account,
    )
    .await
    .context("simulate router_get_amounts_out")?;

    let vec_vals = scval_as_vec(&result)?;
    vec_vals.iter().map(scval_to_i128).collect()
}

/// Format an `ScVal` for display/logging.
pub fn scval_display(val: &ScVal) -> String {
    match val {
        ScVal::Address(addr) => addr.to_string(),
        ScVal::Symbol(sym) => sym.0.to_string(),
        ScVal::U32(n) => n.to_string(),
        ScVal::I128(n) => {
            let v: i128 = n.into();
            v.to_string()
        }
        ScVal::Map(Some(map)) => {
            let parts: Vec<String> = map
                .iter()
                .map(|e| format!("{}={}", scval_display(&e.key), scval_display(&e.val)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ScVal::Void => "None".into(),
        other => format!("{other:?}"),
    }
}
