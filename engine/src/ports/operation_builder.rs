//! `OpBuilder` and `BatchSimulator` — sync operation construction and async batch
//! dry-run.

use {
    crate::{
        lending_model::{ObligationKey, Underlying},
        reactor::BoxFuture,
    },
    anyhow::Result,
};

/// Construct the raw chain operations and request payloads strategies need to
/// submit. Sync because building an op is a pure computation; nothing in
/// here touches the network.
pub trait OperationBuilder: Send + Sync {
    /// Opaque operation type (e.g. `stellar_xdr::Operation`).
    type Op: Send + Clone + std::fmt::Debug + 'static;

    /// Opaque per-request payload (e.g. `ScVal` for a `Request` map).
    type Request: Send + Clone + std::fmt::Debug + 'static;

    fn liquidate_request(
        &self,
        borrower: &ObligationKey,
        borrow_pool: &str,
        collateral_pool: &str,
        repay_amount: i128,
        min_collateral: i128,
    ) -> Result<Self::Request>;

    fn flash_borrow_request(&self, pool: &str, amount: i128) -> Result<Self::Request>;

    fn withdraw_request(&self, pool: &str, amount: i128) -> Result<Self::Request>;

    fn swap_exact_tokens_request(
        &self,
        provider: &str,
        amount_in: i128,
        min_amount_out: i128,
        path: &[&str],
    ) -> Result<Self::Request>;

    fn swap_for_exact_tokens_request(
        &self,
        provider: &str,
        amount_out: i128,
        max_amount_in: i128,
        path: &[&str],
    ) -> Result<Self::Request>;

    /// Bundle one or more requests into a `submit_requests_batch` operation.
    fn batch_op(
        &self,
        market: &str,
        liquidator: &ObligationKey,
        requests: &[Self::Request],
    ) -> Result<Self::Op>;

    /// One-shot withdraw operation.
    fn withdraw_op(
        &self,
        market: &str,
        liquidator: &ObligationKey,
        pool: &str,
        amount: Underlying,
    ) -> Result<Self::Op>;
}

/// Dry-run a batch of requests against the contract before paying gas to
/// submit it for real.
pub trait BatchSimulator: Send + Sync {
    type Request: Send + Clone + 'static;

    fn simulate_batch<'a>(
        &'a self,
        market: &'a str,
        liquidator: &'a ObligationKey,
        requests: &'a [Self::Request],
    ) -> BoxFuture<'a, Result<bool>>;
}
