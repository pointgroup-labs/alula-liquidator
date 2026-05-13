//! `ChainReader` — async port for reading lending state from a chain.
//!
//! Implementations live in adapter crates (e.g. `keeper::stellar::simulation`).
//! Strategies depend on `Arc<dyn ChainReader>` so they cannot reach raw RPC.

use {
    crate::{
        lending::{MarketData, Obligation, ObligationKey},
        reactor::BoxFuture,
    },
    anyhow::Result,
};

pub trait ChainReader: Send + Sync {
    fn read_market_data<'a>(&'a self, market_address: &'a str)
    -> BoxFuture<'a, Result<MarketData>>;

    fn read_user_obligation<'a>(
        &'a self,
        market_address: &'a str,
        key: &'a ObligationKey,
    ) -> BoxFuture<'a, Result<Obligation>>;

    fn read_all_obligation_keys<'a>(
        &'a self,
        market_address: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ObligationKey>>>;

    fn read_token_balance<'a>(
        &'a self,
        token_address: &'a str,
        account: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    /// Quote the amount of `asset_out` received for `amount_in` of `asset_in`
    /// from a specific swap provider (DEX pool).
    fn quote_amount_out<'a>(
        &'a self,
        provider: &'a str,
        amount_in: i128,
        asset_in: &'a str,
        asset_out: &'a str,
    ) -> BoxFuture<'a, Result<i128>>;

    /// Multi-hop router quote: amounts received along `path` for `amount_in`.
    /// Returns the full intermediate amounts vector; the final element is
    /// `amount_out`.
    fn router_quote_out<'a>(
        &'a self,
        router: &'a str,
        amount_in: i128,
        path: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<i128>>>;

    /// Multi-hop router quote: amounts required along `path` to receive
    /// `amount_out`. The first element is the required `amount_in`.
    fn router_quote_in<'a>(
        &'a self,
        router: &'a str,
        amount_out: i128,
        path: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<i128>>>;

    /// Dry-run a single liquidation; returns `true` if the borrower is actually
    /// liquidatable at the given pool pair, `false` if not.
    fn simulate_liquidation<'a>(
        &'a self,
        market: &'a str,
        liquidator_address: &'a str,
        borrower: &'a ObligationKey,
        borrow_pool: &'a str,
        collateral_pool: &'a str,
    ) -> BoxFuture<'a, Result<bool>>;
}
