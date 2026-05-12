//! `ChainReader` and `BatchSimulator` impls for `Gateway`.

use {
    super::{
        Gateway,
        errors::is_expected_liquidation_failure,
        xdr_codec::{
            address_to_scval, build_address_vec_scval, build_requests_vec_scval,
            i128_to_scval, obligation_key_to_scval,
            parse_market_data, parse_obligation, parse_obligation_keys, scval_as_vec,
            scval_to_i128,
        },
    },
    anyhow::Context,
    engine::{
        lending::{MarketData, Obligation, ObligationKey},
        ports::{BatchSimulator, ChainReader},
        reactor::BoxFuture,
    },
    stellar_xdr::curr::ScVal,
    tracing::{debug, warn},
};

// ---------------------------------------------------------------------------
// ChainReader
// ---------------------------------------------------------------------------

impl ChainReader for Gateway {
    fn read_market_data<'a>(
        &'a self,
        market_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<MarketData>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            let sc_val = self
                .simulate_contract_call(market_address, "get_market_data", &[])
                .await
                .context("simulate get_market_data")?;
            debug!(
                duration_ms = start.elapsed().as_millis() as u64,
                market = market_address,
                "get_market_data simulated",
            );
            parse_market_data(&sc_val)
        })
    }

    fn read_user_obligation<'a>(
        &'a self,
        market_address: &'a str,
        key: &'a ObligationKey,
    ) -> BoxFuture<'a, anyhow::Result<Obligation>> {
        Box::pin(async move {
            let key_scval = obligation_key_to_scval(key)?;
            let sc_val = self
                .simulate_contract_call(market_address, "get_user_obligation", &[key_scval])
                .await
                .context("simulate get_user_obligation")?;
            parse_obligation(&sc_val, key)
        })
    }

    fn read_all_obligation_keys<'a>(
        &'a self,
        market_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Vec<ObligationKey>>> {
        Box::pin(async move {
            let sc_val = self
                .simulate_contract_call(market_address, "get_all_obligations", &[])
                .await
                .context("simulate get_all_obligations")?;
            parse_obligation_keys(&sc_val)
        })
    }

    fn read_token_balance<'a>(
        &'a self,
        token_address: &'a str,
        account: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<i128>> {
        Box::pin(async move {
            let args = vec![address_to_scval(account)?];
            let result = self
                .simulate_contract_call(token_address, "balance", &args)
                .await?;
            scval_to_i128(&result).context("parse balance result")
        })
    }

    fn quote_amount_out<'a>(
        &'a self,
        provider: &'a str,
        amount_in: i128,
        asset_in: &'a str,
        asset_out: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<i128>> {
        Box::pin(async move {
            // The provider's `get_amount_out(path, amount_in) -> i128` is
            // provider-agnostic — every adapter implements it.
            let path_scval = build_address_vec_scval(&[asset_in, asset_out])?;
            let args = vec![path_scval, i128_to_scval(amount_in)];

            let result = self
                .simulate_contract_call(provider, "get_amount_out", &args)
                .await
                .context("simulate swap_provider get_amount_out")?;
            scval_to_i128(&result).context("get_amount_out result is not an i128")
        })
    }

    fn router_quote_out<'a>(
        &'a self,
        router: &'a str,
        amount_in: i128,
        path: &'a [&'a str],
    ) -> BoxFuture<'a, anyhow::Result<Vec<i128>>> {
        Box::pin(async move {
            let path_scval = build_address_vec_scval(path)?;
            let args = vec![i128_to_scval(amount_in), path_scval];

            let result = self
                .simulate_contract_call(router, "router_get_amounts_out", &args)
                .await
                .context("simulate router_get_amounts_out")?;

            let vec_vals = scval_as_vec(&result)?;
            vec_vals.iter().map(scval_to_i128).collect()
        })
    }

    fn router_quote_in<'a>(
        &'a self,
        router: &'a str,
        amount_out: i128,
        path: &'a [&'a str],
    ) -> BoxFuture<'a, anyhow::Result<Vec<i128>>> {
        Box::pin(async move {
            let path_scval = build_address_vec_scval(path)?;
            let args = vec![i128_to_scval(amount_out), path_scval];

            let result = self
                .simulate_contract_call(router, "router_get_amounts_in", &args)
                .await
                .context("simulate router_get_amounts_in")?;

            let vec_vals = scval_as_vec(&result)?;
            vec_vals.iter().map(scval_to_i128).collect()
        })
    }

    fn simulate_liquidation<'a>(
        &'a self,
        market: &'a str,
        liquidator_address: &'a str,
        borrower: &'a ObligationKey,
        borrow_pool: &'a str,
        collateral_pool: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move {
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

            match self
                .simulate_contract_call(market, "liquidate", &args)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    if is_expected_liquidation_failure(&e) {
                        let msg = format!("{e:#}");
                        warn!(
                            "liquidation sim: not liquidatable: borrower={} err={}",
                            borrower.user,
                            msg.chars().take(200).collect::<String>()
                        );
                        Ok(false)
                    } else {
                        warn!(
                            "liquidation sim unexpected error: borrower={} err={:#}",
                            borrower.user, e,
                        );
                        Err(e)
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// BatchSimulator
// ---------------------------------------------------------------------------

impl BatchSimulator for Gateway {
    type Request = ScVal;

    fn simulate_batch<'a>(
        &'a self,
        market: &'a str,
        liquidator: &'a ObligationKey,
        requests: &'a [Self::Request],
    ) -> BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move {
            let requests_vec = build_requests_vec_scval(requests)?;
            let args: Vec<ScVal> = vec![
                obligation_key_to_scval(liquidator)?,
                requests_vec,
                ScVal::Void, // referrer: None
            ];

            debug!(
                "batch sim: liquidator={} num_requests={}",
                liquidator.user,
                requests.len(),
            );

            match self
                .simulate_contract_call(market, "submit_requests_batch", &args)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    let msg = format!("{e:#}");
                    warn!(
                        "batch sim failed: liquidator={} err={}",
                        liquidator.user,
                        msg.chars().take(300).collect::<String>(),
                    );
                    Ok(false)
                }
            }
        })
    }
}
