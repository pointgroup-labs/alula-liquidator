//! ScVal <-> domain conversions, address parsing, small XDR helpers.
//!
//! Internal to the `stellar` adapter — `pub(super)` is the rule, except for
//! display helpers and a few items that `event_decode` and `simulation` share.

use {
    anyhow::{Context, anyhow},
    engine::lending::{BorrowPosition, DepositPosition, MarketData, Obligation, ObligationKey, PoolData},
    stellar_xdr::curr::{
        AccountId, ContractId, Hash, Int128Parts, MuxedAccount, PublicKey, ScAddress, ScMap,
        ScMapEntry, ScSymbol, ScVal, ScVec, Uint256, VecM,
    },
    thiserror::Error,
    tracing::warn,
};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Expected {expected} but found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Invalid XDR data: {reason}")]
    InvalidXdr { reason: String },

    #[error("Invalid UTF-8 data: {source}")]
    InvalidUtf8 {
        #[from]
        source: std::str::Utf8Error,
    },
}

// ---------------------------------------------------------------------------
// i128 / ScVal primitives
// ---------------------------------------------------------------------------

pub(super) fn i128_from_parts(parts: &Int128Parts) -> i128 {
    ((parts.hi as i128) << 64) | (parts.lo as i128)
}

pub(super) fn i128_to_scval(v: i128) -> ScVal {
    ScVal::I128(Int128Parts {
        hi: (v >> 64) as i64,
        lo: v as u64,
    })
}

pub(super) fn contract_strkey_to_hash(strkey: &str) -> anyhow::Result<[u8; 32]> {
    let contract = stellar_strkey::Contract::from_string(strkey)
        .map_err(|e| anyhow!("invalid contract strkey '{strkey}': {e}"))?;
    Ok(contract.0)
}

pub(super) fn account_strkey_to_muxed(strkey: &str) -> anyhow::Result<MuxedAccount> {
    if let Ok(pk) = stellar_strkey::ed25519::PublicKey::from_string(strkey) {
        return Ok(MuxedAccount::Ed25519(Uint256(pk.0)));
    }
    Err(anyhow!(
        "source_account must be a G... address, got: {strkey}"
    ))
}

pub(super) fn is_expected_liquidation_failure(msg: &str) -> bool {
    const EXPECTED_ERRORS: &[&str] = &[
        "ObligationIsHealthy",
        "ObligationDoesNotExist",
        "InvalidLiquidationInputs",
        "BorrowPoolDoesNotExist",
        "CollateralPoolDoesNotExist",
    ];
    EXPECTED_ERRORS.iter().any(|e| msg.contains(e))
}

pub fn scval_type_name(val: &ScVal) -> &'static str {
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

pub(super) fn scval_as_map(val: &ScVal) -> Result<&Vec<ScMapEntry>, ParseError> {
    match val {
        ScVal::Map(Some(ScMap(entries))) => Ok(entries.as_ref()),
        _ => Err(ParseError::TypeMismatch {
            expected: "Map".to_string(),
            found: scval_type_name(val).to_string(),
        }),
    }
}

pub(super) fn scval_as_vec(val: &ScVal) -> Result<&Vec<ScVal>, ParseError> {
    match val {
        ScVal::Vec(Some(ScVec(v))) => Ok(v.as_ref()),
        _ => Err(ParseError::TypeMismatch {
            expected: "Vec".to_string(),
            found: scval_type_name(val).to_string(),
        }),
    }
}

pub(super) fn map_get<'a>(entries: &'a [ScMapEntry], key: &str) -> Option<&'a ScVal> {
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

pub(super) fn scval_to_i128(val: &ScVal) -> anyhow::Result<i128> {
    match val {
        ScVal::I128(parts) => Ok(i128_from_parts(parts)),
        ScVal::U32(n) => Ok(*n as i128),
        ScVal::I32(n) => Ok(*n as i128),
        ScVal::U64(n) => Ok(*n as i128),
        ScVal::I64(n) => Ok(*n as i128),
        other => Err(anyhow!("expected i128-like, got {other:?}")),
    }
}

pub(super) fn address_to_scval(address: &str) -> anyhow::Result<ScVal> {
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

pub(super) fn obligation_key_to_scval(obl: &ObligationKey) -> anyhow::Result<ScVal> {
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

// ---------------------------------------------------------------------------
// Domain parsers
// ---------------------------------------------------------------------------

pub(super) fn parse_obligation_key(val: &ScVal) -> anyhow::Result<ObligationKey> {
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

pub(super) fn parse_obligation(val: &ScVal, _key: &ObligationKey) -> anyhow::Result<Obligation> {
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

pub(super) fn parse_obligation_keys(val: &ScVal) -> anyhow::Result<Vec<ObligationKey>> {
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

pub(super) fn parse_market_data(val: &ScVal) -> anyhow::Result<MarketData> {
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

// ---------------------------------------------------------------------------
// Request / op builders (low-level ScVal constructors)
// ---------------------------------------------------------------------------

pub(super) fn build_address_vec_scval(addresses: &[&str]) -> anyhow::Result<ScVal> {
    let mut items = Vec::with_capacity(addresses.len());
    for addr in addresses {
        items.push(address_to_scval(addr)?);
    }
    let vec_m: VecM<ScVal> = items
        .try_into()
        .map_err(|_| anyhow!("address vec conversion"))?;
    Ok(ScVal::Vec(Some(ScVec(vec_m))))
}

pub(super) fn build_requests_vec_scval(requests: &[ScVal]) -> anyhow::Result<ScVal> {
    let vec_m: VecM<ScVal> = requests
        .to_vec()
        .try_into()
        .map_err(|_| anyhow!("requests vec conversion"))?;
    Ok(ScVal::Vec(Some(ScVec(vec_m))))
}

// ---------------------------------------------------------------------------
// Display helper
// ---------------------------------------------------------------------------

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
