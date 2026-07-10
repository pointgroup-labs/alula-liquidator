//! `LedgerReader` — async port for reading lending protocol state.

use anyhow::Result;

use crate::{
    lending_model::{MarketData, Obligation, ObligationKey, PoolData},
    reactor::BoxFuture,
};

pub trait LedgerReader: Send + Sync {
    fn read_token_balance<'a>(
        &'a self,
        token_address: &'a str,
        account: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    fn read_market_data<'a>(&'a self, market_address: &'a str)
    -> BoxFuture<'a, Result<MarketData>>;

    fn read_pool_data<'a>(
        &'a self,
        market_address: &'a str,
        pool_address: &'a str,
    ) -> BoxFuture<'a, Result<PoolData>>;

    fn read_user_obligation<'a>(
        &'a self,
        market_address: &'a str,
        key: &'a ObligationKey,
    ) -> BoxFuture<'a, Result<Obligation>>;

    fn read_all_obligations_keys<'a>(
        &'a self,
        market_address: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ObligationKey>>>;

    fn read_account_token_balance<'a>(
        &'a self,
        token_address: &'a str,
        account: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    /// Get the `amount_out` of `asset_out` received for `amount_in` of `asset_in`
    /// from a swap provider.
    fn get_amount_out<'a>(
        &'a self,
        amount_in: i128,
        asset_in: &'a str,
        asset_out: &'a str,
        swap_provider_address: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    /// Get the `amount_in` of `asset_in` to receive the `amount_in` of `asset_in`
    /// from a swap provider.
    fn get_amount_in<'a>(
        &'a self,
        amount_out: i128,
        asset_in: &'a str,
        asset_out: &'a str,
        swap_provider_address: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    /// Dry-run a single liquidation; returns `true` if the borrower is
    /// liquidatable at the given pool pair, `false` if not.
    fn get_is_obligation_liquidatable<'a>(
        &'a self,
        market: &'a str,
        liquidator_address: &'a str,
        borrower: &'a ObligationKey,
        borrow_pool: &'a str,
        collateral_pool: &'a str,
    ) -> BoxFuture<'a, Result<bool>>;
}
