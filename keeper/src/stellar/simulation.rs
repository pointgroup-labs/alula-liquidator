//! `LedgerReader` and `BatchSimulator` impls for `Gateway`.

use anyhow::Context;
use engine::{
    lending_model::{MarketData, Obligation, ObligationKey, PoolData},
    ports::{BatchSimulator, LedgerReader},
    reactor::BoxFuture,
};
use stellar_xdr::ScVal;
use tracing::{debug, warn};

use super::{
    client::Gateway,
    errors::is_expected_liquidation_failure,
    xdr_codec::{
        address_to_scval, build_address_vec_scval, build_requests_vec_scval, i128_to_scval,
        obligation_key_to_scval, parse_global_state_thresholds, parse_market_data,
        parse_obligation, parse_obligation_keys, parse_pool_addresses, parse_pool_data,
        scval_to_i128, scval_to_u32,
    },
};
use crate::metrics::{self, SimulationCall, SimulationOutcome};

impl LedgerReader for Gateway {
    fn read_market_data<'a>(
        &'a self,
        market_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<MarketData>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            match self.simulate_contract_call(market_address, "get_market_data", &[]).await {
                Ok(sc_val) => {
                    debug!(
                        duration_ms = start.elapsed().as_millis() as u64,
                        market = market_address,
                        "get_market_data simulated",
                    );
                    parse_market_data(&sc_val)
                }
                // `get_market_data` iterates every pool and aborts the whole
                // call on the first one it cannot price, so a single dead
                // oracle would blind the keeper on every other asset. Rebuild
                // the snapshot pool by pool instead.
                Err(e) => {
                    warn!(
                        ?e,
                        market = market_address,
                        "get_market_data failed; rebuilding snapshot per pool"
                    );
                    self.read_market_data_per_pool(market_address).await
                }
            }
        })
    }

    fn read_pool_data<'a>(
        &'a self,
        market_address: &'a str,
        pool_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<PoolData>> {
        Box::pin(async move {
            let args = vec![address_to_scval(pool_address)?];
            let sc_val = self
                .simulate_contract_call(market_address, "get_pool_data", &args)
                .await
                .context("simulate get_pool_data")?;
            parse_pool_data(&sc_val)
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

    fn read_all_obligations_keys<'a>(
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
            let result = self.simulate_contract_call(token_address, "balance", &args).await?;
            scval_to_i128(&result).context("parse balance result")
        })
    }

    fn read_account_token_balance<'a>(
        &'a self,
        token_address: &'a str,
        account: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<i128>> {
        // Account and contract balances both resolve through the SEP-41
        // `balance(account)` entrypoint.
        self.read_token_balance(token_address, account)
    }

    fn get_amount_out<'a>(
        &'a self,
        amount_in: i128,
        asset_in: &'a str,
        asset_out: &'a str,
        swap_provider_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<i128>> {
        Box::pin(async move {
            // The provider's `get_amount_out(path, amount_in) -> i128` is
            // provider-agnostic — every adapter implements it.
            let path_scval = build_address_vec_scval(&[asset_in, asset_out])?;
            let args = vec![path_scval, i128_to_scval(amount_in)];

            let result = self
                .simulate_contract_call(swap_provider_address, "get_amount_out", &args)
                .await
                .context("simulate swap_provider get_amount_out")?;
            scval_to_i128(&result).context("get_amount_out result is not an i128")
        })
    }

    fn get_amount_in<'a>(
        &'a self,
        amount_out: i128,
        asset_in: &'a str,
        asset_out: &'a str,
        swap_provider_address: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<i128>> {
        Box::pin(async move {
            let path_scval = build_address_vec_scval(&[asset_in, asset_out])?;
            let args = vec![path_scval, i128_to_scval(amount_out)];

            let result = self
                .simulate_contract_call(swap_provider_address, "get_amount_in", &args)
                .await
                .context("simulate swap_provider get_amount_in")?;
            scval_to_i128(&result).context("get_amount_in result is not an i128")
        })
    }

    fn get_is_obligation_liquidatable<'a>(
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
                ScVal::I128(stellar_xdr::Int128Parts { hi: 0, lo: 1000 }),
                ScVal::I128(stellar_xdr::Int128Parts { hi: 0, lo: 0 }),
            ];

            debug!(
                "liquidate sim: liquidator={} borrower={} seed={:?} borrow_pool={} \
                 collateral_pool={}",
                liquidator_address, borrower.user, borrower.seed, borrow_pool, collateral_pool,
            );

            match self.simulate_contract_call(market, "liquidate", &args).await {
                Ok(_) => {
                    metrics::record_simulation(SimulationCall::Liquidate, SimulationOutcome::Ok);
                    Ok(true)
                }
                Err(e) => {
                    if is_expected_liquidation_failure(&e) {
                        let msg = format!("{e:#}");
                        warn!(
                            "liquidation sim: not liquidatable: borrower={} err={}",
                            borrower.user,
                            msg.chars().take(200).collect::<String>()
                        );
                        metrics::record_simulation(
                            SimulationCall::Liquidate,
                            SimulationOutcome::NotLiquidatable,
                        );
                        Ok(false)
                    } else {
                        warn!(
                            "liquidation sim unexpected error: borrower={} err={:#}",
                            borrower.user, e,
                        );
                        metrics::record_simulation(
                            SimulationCall::Liquidate,
                            SimulationOutcome::Error,
                        );
                        Err(e)
                    }
                }
            }
        })
    }
}

impl Gateway {
    /// Rebuilds a market snapshot from `get_all_pools` plus one `get_pool_data`
    /// per pool, tolerating per-pool failures: only `get_pool_data` prices an
    /// asset, so one dead price feed costs one pool instead of the whole call.
    ///
    /// `get_all_pools` and `get_global_state` read storage only, but
    /// `get_oracle_price_decimals` does call the oracle — `decimals()`, which
    /// the aggregated oracle answers from its own storage without consulting a
    /// feed. So this fallback survives a per-asset price outage and fails
    /// closed, by design, when the oracle contract itself is unreachable.
    async fn read_market_data_per_pool(&self, market_address: &str) -> anyhow::Result<MarketData> {
        let pools_sc_val = self
            .simulate_contract_call(market_address, "get_all_pools", &[])
            .await
            .context("simulate get_all_pools")?;
        let pool_addresses = parse_pool_addresses(&pools_sc_val)?;

        let global_state_sc_val = self
            .simulate_contract_call(market_address, "get_global_state", &[])
            .await
            .context("simulate get_global_state")?;
        let (insolvency_ltv_bps, min_collateral_value_cents) =
            parse_global_state_thresholds(&global_state_sc_val)?;

        let decimals_sc_val = self
            .simulate_contract_call(market_address, "get_oracle_price_decimals", &[])
            .await
            .context("simulate get_oracle_price_decimals")?;
        let oracle_price_decimals = scval_to_u32(&decimals_sc_val)?;

        let mut reads = Vec::with_capacity(pool_addresses.len());
        for pool_address in pool_addresses {
            let read = self.read_pool_data(market_address, &pool_address).await;
            reads.push((pool_address, read));
        }
        let (pools_data, unavailable_pools) = partition_pool_reads(market_address, reads);

        Ok(MarketData {
            pools_data,
            oracle_price_decimals,
            insolvency_ltv_bps,
            min_collateral_value_cents,
            unavailable_pools,
        })
    }
}

/// Splits per-pool reads into the pools that resolved and the addresses that did
/// not, so a partial snapshot arrives explicitly marked rather than silently
/// short of a pool.
fn partition_pool_reads(
    market_address: &str,
    reads: Vec<(String, anyhow::Result<PoolData>)>,
) -> (Vec<PoolData>, Vec<String>) {
    let mut pools_data = Vec::with_capacity(reads.len());
    let mut unavailable_pools = Vec::new();

    for (pool_address, read) in reads {
        match read {
            Ok(pool_data) => pools_data.push(pool_data),
            Err(e) => {
                warn!(
                    ?e,
                    market = market_address,
                    pool = %pool_address,
                    "pool unavailable; snapshot is partial"
                );
                unavailable_pools.push(pool_address);
            }
        }
    }

    (pools_data, unavailable_pools)
}

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

            debug!("batch sim: liquidator={} num_requests={}", liquidator.user, requests.len(),);

            match self.simulate_contract_call(market, "submit_requests_batch", &args).await {
                Ok(_) => {
                    metrics::record_simulation(SimulationCall::Batch, SimulationOutcome::Ok);
                    Ok(true)
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    warn!(
                        "batch sim failed: liquidator={} err={}",
                        liquidator.user,
                        msg.chars().take(300).collect::<String>(),
                    );
                    // Batch simulation collapses every failure mode (contract
                    // precondition, RPC error, malformed args) into `Ok(false)`,
                    // so the metric uses a single `failed` bucket. If the
                    // taxonomy ever splits, mirror simulate_liquidate's enum.
                    metrics::record_simulation(SimulationCall::Batch, SimulationOutcome::Failed);
                    Ok(false)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    fn pool_data(pool_address: &str) -> PoolData {
        PoolData {
            pool_address: pool_address.to_string(),
            token_address: pool_address.to_string(),
            token_symbol: "TST".to_string(),
            token_decimals: 7,
            total_borrowed: 0.into(),
            total_d_tokens: 0.into(),
            total_j_tokens: 0.into(),
            total_available: 0.into(),
            total_available_adjusted: 0.into(),
            total_supply: 0.into(),
            total_collateral: 0.into(),
            j_token_rate_floor_bps: 10_000,
            d_token_rate_ceil_bps: 10_000,
            oracle_asset_price: 1,
            open_ltv_bps: 8_000,
            close_ltv_bps: 8_500,
            liability_factor_bps: 10_000,
            liquidation_close_factor_bps: 5_000,
            max_liquidation_incentive_bps: 1_000,
            flash_loan_fee_bps: 5,
            utilization_ratio_limit_bps: 9_000,
        }
    }

    #[test]
    fn every_pool_readable_yields_a_complete_snapshot() {
        let (pools_data, unavailable_pools) = partition_pool_reads(
            "MARKET_A",
            vec![
                ("POOL_A".to_string(), Ok(pool_data("POOL_A"))),
                ("POOL_B".to_string(), Ok(pool_data("POOL_B"))),
            ],
        );

        assert_eq!(pools_data.len(), 2);
        assert!(unavailable_pools.is_empty(), "a complete snapshot must not be marked partial");
    }

    #[test]
    fn one_unreadable_pool_degrades_only_itself_and_is_marked() {
        let (pools_data, unavailable_pools) = partition_pool_reads(
            "MARKET_A",
            vec![
                ("POOL_A".to_string(), Ok(pool_data("POOL_A"))),
                ("POOL_B".to_string(), Err(anyhow!("oracle price unavailable"))),
                ("POOL_C".to_string(), Ok(pool_data("POOL_C"))),
            ],
        );

        let addresses: Vec<&str> = pools_data.iter().map(|p| p.pool_address.as_str()).collect();
        assert_eq!(addresses, ["POOL_A", "POOL_C"]);
        assert_eq!(unavailable_pools, ["POOL_B"]);
    }

    #[test]
    fn every_pool_unreadable_yields_a_snapshot_that_prices_nothing() {
        // The rebuild itself still succeeds structurally — `get_all_pools` and
        // the global-state reads worked. It is the resulting view that is
        // empty, which is what the liquidator refuses to install.
        let (pools_data, unavailable_pools) = partition_pool_reads(
            "MARKET_A",
            vec![
                ("POOL_A".to_string(), Err(anyhow!("oracle price unavailable"))),
                ("POOL_B".to_string(), Err(anyhow!("oracle price unavailable"))),
            ],
        );
        let market_data = MarketData {
            pools_data,
            oracle_price_decimals: 14,
            insolvency_ltv_bps: 10_000,
            min_collateral_value_cents: 0,
            unavailable_pools,
        };

        assert!(market_data.prices_nothing());
    }

    #[test]
    fn a_partial_snapshot_is_distinguishable_from_a_short_complete_one() {
        // Two pools, one unreadable, vs. a market that genuinely has one pool:
        // both carry a single `PoolData`, so only `unavailable_pools` separates
        // "we could not price POOL_B" from "POOL_B does not exist".
        let (partial, unavailable) = partition_pool_reads(
            "MARKET_A",
            vec![
                ("POOL_A".to_string(), Ok(pool_data("POOL_A"))),
                ("POOL_B".to_string(), Err(anyhow!("oracle price unavailable"))),
            ],
        );
        let (complete, none_missing) =
            partition_pool_reads("MARKET_A", vec![("POOL_A".to_string(), Ok(pool_data("POOL_A")))]);

        assert_eq!(partial.len(), complete.len());
        assert!(!unavailable.is_empty());
        assert!(none_missing.is_empty());
    }
}
