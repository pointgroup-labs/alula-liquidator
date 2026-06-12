//! `OperationBuilder` impl for `Gateway` plus the request-/operation-builder helpers.

use {
    super::{
        Gateway,
        xdr_codec::{
            address_to_scval, build_address_vec_scval, build_requests_vec_scval,
            contract_strkey_to_hash, i128_to_scval, obligation_key_to_scval,
        },
    },
    anyhow::anyhow,
    engine::{
        lending_model::{ObligationKey, amount::Underlying},
        ports::OperationBuilder,
    },
    stellar_xdr::curr::{
        ContractId, Hash, HostFunction, InvokeContractArgs, Operation, OperationBody, ScAddress,
        ScMap, ScMapEntry, ScSymbol, ScVal, ScVec, VecM,
    },
};

fn build_flash_borrow_request_scval(pool_address: &str, amount: i128) -> anyhow::Result<ScVal> {
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

fn build_withdraw_request_scval(pool_address: &str, amount: i128) -> anyhow::Result<ScVal> {
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

fn build_liquidate_request_scval(
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

fn build_swap_exact_tokens_request_scval(
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

fn build_swap_for_exact_tokens_request_scval(
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

fn build_withdraw_op(
    market_address: &str,
    user_key: &ObligationKey,
    pool_address: &str,
    amount: Underlying,
) -> anyhow::Result<Operation> {
    let amount = amount.0;
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

fn build_issue_cover_bad_debt_op(
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

fn build_batch_op(
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

impl Gateway {
    /// Build a `issue_cover_bad_debt` operation. Not part of the
    /// `OperationBuilder` trait — only the bad-debt initiator strategy needs it.
    pub fn cover_bad_debt_op(
        &self,
        market: &str,
        obligation: &ObligationKey,
    ) -> anyhow::Result<Operation> {
        build_issue_cover_bad_debt_op(market, obligation)
    }
}

impl OperationBuilder for Gateway {
    type Op = Operation;
    type Request = ScVal;

    fn liquidate_request(
        &self,
        borrower: &ObligationKey,
        borrow_pool: &str,
        collateral_pool: &str,
        repay_amount: i128,
        min_collateral: i128,
    ) -> anyhow::Result<Self::Request> {
        // The Liquidate request variant is keyed on the borrower; the
        // liquidator's identity comes through `batch_op` via `liquidator_key`.
        build_liquidate_request_scval(
            borrower,
            borrow_pool,
            collateral_pool,
            repay_amount,
            min_collateral,
        )
    }

    fn flash_borrow_request(&self, pool: &str, amount: i128) -> anyhow::Result<Self::Request> {
        build_flash_borrow_request_scval(pool, amount)
    }

    fn withdraw_request(&self, pool: &str, amount: i128) -> anyhow::Result<Self::Request> {
        build_withdraw_request_scval(pool, amount)
    }

    fn swap_exact_tokens_request(
        &self,
        provider: &str,
        amount_in: i128,
        min_amount_out: i128,
        path: &[&str],
    ) -> anyhow::Result<Self::Request> {
        build_swap_exact_tokens_request_scval(provider, path, amount_in, min_amount_out)
    }

    fn swap_for_exact_tokens_request(
        &self,
        provider: &str,
        amount_out: i128,
        max_amount_in: i128,
        path: &[&str],
    ) -> anyhow::Result<Self::Request> {
        build_swap_for_exact_tokens_request_scval(provider, path, max_amount_in, amount_out)
    }

    fn batch_op(
        &self,
        market: &str,
        liquidator: &ObligationKey,
        requests: &[Self::Request],
    ) -> anyhow::Result<Self::Op> {
        build_batch_op(market, liquidator, requests)
    }

    fn withdraw_op(
        &self,
        market: &str,
        liquidator: &ObligationKey,
        pool: &str,
        amount: Underlying,
    ) -> anyhow::Result<Self::Op> {
        build_withdraw_op(market, liquidator, pool, amount)
    }
}
