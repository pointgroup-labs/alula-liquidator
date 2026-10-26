//! Pure liquidation math: health, debt/collateral valuation, expected seize.

use core::cmp::Ord;

use serde::{Deserialize, Serialize};
use tracing::error;

use crate::lending_model::{
    amount::{BPS_FACTOR, Underlying, bps_fixed_div_ceil},
    error::{LMError, LendingModelError, MapArithmeticError},
    market::MarketData,
    obligation::{DepositPosition, Obligation},
    pool::PoolData,
    profitability::{
        compute_liquidation_profitability, compute_profit_margin_in_borrow_token,
        compute_repay_cap_from_collateral,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationResult {
    pub debt_repaid: i128,
    pub j_tokens_seized: i128,
    pub d_tokens_burned: i128,
    pub amount_to_send_back: i128,
    pub plain_collateral_seized: i128,
    pub tokens_from_j_tokens_seized: i128,
}

/// The `liquidate` rules a market enforces around its dust floor
/// (`min_collateral_value_cents`). Configured rather than read: the contract
/// exposes no version, and both rule sets share every entry point.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DustRules {
    /// The market as deployed through v1.2.0: collateral left under the floor
    /// goes to the liquidator, and the close factor binds every solvent
    /// obligation.
    #[default]
    Sweep,
    /// No sweep. Below twice the floor the close factor and the LTV cap are
    /// lifted and a repay worth more than the position is refunded; a
    /// liquidation leaving either side under the floor is refused
    /// (`LiquidationLeavesDust`, #606), and so is covering a solvent
    /// obligation (#603).
    NoDust,
}

/// Margin on an offer that clears a side of a pair, for prices that move and
/// interest that accrues past the keeper's estimate before it lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClearingMargin {
    /// Where the market refunds the excess: past the debt, or past a small
    /// position's whole collateral.
    pub refunded_bps: i128,
    /// Where it does not: past a larger position's whole collateral, so the
    /// excess buys no collateral.
    pub unrefunded_bps: i128,
}

impl ClearingMargin {
    pub const INITIAL: Self = Self { refunded_bps: 100, unrefunded_bps: 0 };
    /// For the one retry after the market refused the initial offer as dust.
    pub const RETRY: Self = Self { refunded_bps: 500, unrefunded_bps: 100 };
}

/// `floor(amount * bps / BPS_FACTOR)` with saturating intermediates.
fn fixed_mul_floor(amount: i128, bps: i128) -> i128 {
    amount.saturating_mul(bps).saturating_div(BPS_FACTOR)
}

/// `ceil(amount * bps / BPS_FACTOR)` with saturating intermediates.
fn fixed_mul_ceil(amount: i128, bps: i128) -> i128 {
    amount.saturating_mul(bps).saturating_add(BPS_FACTOR - 1).saturating_div(BPS_FACTOR)
}

/// Determine whether an obligation is liquidatable using only cached local data.
///
/// Mirrors the contract's liquidate gate (`obligation.rs::liquidate`):
/// `debt_value_scaled_w_liability_factors > collateral_value_scaled_w_close_ltvs`,
/// with ceil rounding on the debt side and floor on the collateal side.
pub fn compute_is_liquidatable(
    obligation: &Obligation,
    market_data: &MarketData,
    rules: DustRules,
) -> Result<bool, LendingModelError> {
    // TODO: Rewrite according to amount! types
    let pools = &market_data.pools_data;

    if !has_any_collateral(obligation, pools) {
        return Ok(false);
    }

    let mut debt_value: i128 = 0;
    for borrow in &obligation.borrows {
        let pool = match pools.iter().find(|p| p.pool_address == borrow.pool_address) {
            Some(p) => p,
            None => {
                error!(?borrow, "missing borrow pool");

                continue;
            }
        };
        let real_debt = fixed_mul_ceil(borrow.d_tokens.0, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .m_ou()?
            .checked_add(decimals_divisor - 1)
            .m_ou()?
            .checked_div(decimals_divisor)
            .m_ou()?;
        let scaled = fixed_mul_ceil(value, pool.liability_factor_bps);
        debt_value = debt_value.checked_add(scaled).m_ou()?;
    }
    if debt_value == 0 {
        return Ok(false);
    }

    let mut collateral_value: i128 = 0;
    for deposit in &obligation.deposits {
        let pool = match pools.iter().find(|p| p.pool_address == deposit.pool_address) {
            Some(p) => p,
            None => {
                error!(?deposit, "missing deposit pool");

                continue;
            }
        };
        let real_supply = fixed_mul_floor(deposit.j_tokens.0, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply.checked_add(deposit.collateral.0).m_ou()?;
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .m_ou()?
            .checked_div(decimals_divisor)
            .m_ou()?;
        let scaled = fixed_mul_floor(value, pool.close_ltv_bps);
        collateral_value = collateral_value.checked_add(scaled).m_ou()?;
    }

    let min_collateral_value_cents = market_data.min_collateral_value_cents;
    let min_collateral_value_per_deposit_position = min_collateral_value_cents
        .checked_mul(10_i128.checked_pow(market_data.oracle_price_decimals).m_ou()?)
        .m_ou()?
        .checked_div(100)
        .m_ou()?;
    let collateral_value_to_substract = min_collateral_value_per_deposit_position
        .checked_mul(obligation.deposits.len() as i128)
        .m_ou()?;

    // The market lets the adjusted collateral go negative, so such an obligation
    // is liquidatable; a sweep market's cover takes it instead, a no-dust
    // market's refuses it while it is solvent.
    if rules == DustRules::Sweep && collateral_value_to_substract > collateral_value {
        return Ok(false);
    }
    let collateral_value_adjusted =
        collateral_value.checked_sub(collateral_value_to_substract).m_ou()?;
    let res = debt_value > collateral_value_adjusted;

    Ok(res)
}

/// The pools this obligation holds a position in that the snapshot does not
/// price. [`compute_is_liquidatable`] skips unknown pools, so judging such an
/// obligation would silently under-count its debt or its collateral — wrong in
/// both directions. A non-empty result means the caller must skip it until
/// those pools are priced again, and names them so the skip can be attributed
/// to the pool at fault rather than to the whole market.
///
/// The test is presence in `pools_data`, not membership of `unavailable_pools`:
/// the latter is empty by construction on the aggregate path and never names a
/// pool the market simply did not report, so it would let exactly the missing
/// pools through. Deduplicated, since a borrow and a deposit may share a pool.
pub fn unpriced_pools_touched<'a>(
    obligation: &'a Obligation,
    market_data: &MarketData,
) -> Vec<&'a str> {
    let mut unpriced: Vec<&str> = Vec::new();
    for held in obligation
        .borrows
        .iter()
        .map(|b| &b.pool_address)
        .chain(obligation.deposits.iter().map(|d| &d.pool_address))
    {
        let priced = market_data.pools_data.iter().any(|p| p.pool_address == *held);
        if !priced && !unpriced.contains(&held.as_str()) {
            unpriced.push(held);
        }
    }

    unpriced
}

/// Cheap pre-filter used by [`compute_is_liquidatable`] to short-circuit
/// obligations with no real collateral (`j_tokens × j_rate_floor + plain
/// collateral == 0` across every deposit).
pub fn has_any_collateral(obligation: &Obligation, pools: &[PoolData]) -> bool {
    obligation.deposits.iter().any(|dep| {
        let pool = match pools.iter().find(|p| p.pool_address == dep.pool_address) {
            Some(p) => p,
            None => {
                error!(?dep, "no deposit pool exists");

                return false;
            }
        };
        let real_supply = fixed_mul_floor(dep.j_tokens.0, pool.j_token_rate_floor_bps);

        real_supply.saturating_add(dep.collateral.0) > 0
    })
}

/// Total obligation debt value (unscaled, oracle units).
///
/// Per-pool division by `10^token_decimals` rounds **up**, mirroring the
/// contract's `compute_debt_value` → `compute_asset_value_scaled_ceil`.
/// This value feeds both the insolvency LTV and the LTV-improving seize cap,
/// exactly as `obligation_debt_value` does in the contract's `liquidate`.
pub fn compute_obligation_debt_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LMError> {
    let mut total: i128 = 0;
    for bor in &obligation.borrows {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == bor.pool_address)
            .ok_or(LMError::InternalError)?;
        let real_debt = fixed_mul_ceil(bor.d_tokens.0, pool.d_token_rate_ceil_bps);
        let decimals_divisor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = real_debt
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_add(decimals_divisor - 1))
            .and_then(|v| v.checked_div(decimals_divisor))
            .m_ou()?;
        total = total.checked_add(value).m_ou()?;
    }

    Ok(total)
}

/// Total obligation collateral value (unscaled, oracle units).
pub fn compute_obligation_collateral_value(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<i128, LMError> {
    let mut total: i128 = 0;
    for dep in &obligation.deposits {
        let pool = market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == dep.pool_address)
            .ok_or(LMError::InternalError)?;
        let real_supply = fixed_mul_floor(dep.j_tokens.0, pool.j_token_rate_floor_bps);
        let total_tokens = real_supply.checked_add(dep.collateral.0).m_ou()?;
        let pool_token_factor = 10_i128.checked_pow(pool.token_decimals).m_ou()?;
        let value = total_tokens
            .checked_mul(pool.oracle_asset_price)
            .and_then(|v| v.checked_div(pool_token_factor))
            .m_ou()?;
        total = total.checked_add(value).m_ou()?;
    }
    Ok(total)
}

/// Close-factor cap on `repay_amount`. Insolvent obligations skip the cap
/// — any participant may close the whole debt — and so do small positions on a
/// no-dust market ([`LiquidationPair::lifts_close_factor`]), while the rest
/// apply `liquidation_close_factor_bps` per the protocol's partial-liquidation
/// rule.
pub fn compute_close_factor_repay_cap(
    is_lifted: bool,
    position_debt_tokens: i128,
    liquidation_close_factor_bps: i128,
) -> i128 {
    if is_lifted {
        position_debt_tokens
    } else {
        fixed_mul_floor(position_debt_tokens, liquidation_close_factor_bps)
    }
}

/// Mirror the contract's seized-collateral computation for a given `repay_amount`
/// on a sweep market.
#[allow(clippy::too_many_arguments)]
fn compute_expected_seized_collateral(
    repay_amount: i128,
    borrow_pool: &PoolData,
    collateral_pool: &PoolData,
    deposit: &DepositPosition,
    obligation_debt_value: i128,
    obligation_collateral_value: i128,
    is_insolvent: bool,
    min_collateral_value_cents: i128,
    oracle_price_decimals: u32,
) -> Result<i128, LMError> {
    if repay_amount <= 0 || collateral_pool.oracle_asset_price <= 0 {
        return Ok(0);
    }

    let real_supply = fixed_mul_floor(deposit.j_tokens.0, collateral_pool.j_token_rate_floor_bps);

    let position_collateral_sum = real_supply.checked_add(deposit.collateral.0).m_ou()?;
    if position_collateral_sum <= 0 {
        return Ok(0);
    }

    let borrow_token_factor = 10_i128.checked_pow(borrow_pool.token_decimals).m_ou()?;
    let liquidated_value = repay_amount
        .checked_mul(borrow_pool.oracle_asset_price)
        .m_ou()?
        .checked_div(borrow_token_factor)
        .m_ou()?;

    let min_incentive_bps = borrow_pool
        .max_liquidation_incentive_bps
        .min(collateral_pool.max_liquidation_incentive_bps);

    let collateral_token_factor = 10_i128.checked_pow(collateral_pool.token_decimals).m_ou()?;
    let collateral_amount_no_bonus = liquidated_value
        .checked_mul(collateral_token_factor)
        .m_ou()?
        .checked_div(collateral_pool.oracle_asset_price)
        .m_ou()?;

    let with_incentive = collateral_amount_no_bonus
        .checked_mul(BPS_FACTOR.checked_add(min_incentive_bps).m_ou()?)
        .m_ou()?
        .checked_div(BPS_FACTOR)
        .m_ou()?;

    let ltv_cap = if !is_insolvent {
        // LTV-improvement assertion
        if obligation_debt_value <= 0 || obligation_collateral_value <= 0 {
            return Ok(0);
        }
        let max_value_recv = liquidated_value
            .checked_mul(obligation_collateral_value)
            .m_ou()?
            .checked_div(obligation_debt_value)
            .m_ou()?;

        let strict_max_value_recv =
            max_value_recv.checked_mul(999).m_ou()?.checked_div(1000).m_ou()?;

        let ltv_collateral = strict_max_value_recv
            .checked_mul(collateral_token_factor)
            .m_ou()?
            .checked_div(collateral_pool.oracle_asset_price)
            .m_ou()?;

        // receiving ltv improving amount of collateral for that specific repayment
        Some(ltv_collateral)
    } else {
        None
    };

    let mut seized = position_collateral_sum.min(with_incentive);
    if let Some(cap) = ltv_cap {
        seized = seized.min(cap);
    }

    let collateral_left = position_collateral_sum.checked_sub(seized).m_ou()?;
    let collateral_value_left = collateral_left
        .checked_mul(collateral_pool.oracle_asset_price)
        .m_ou()?
        .checked_div(collateral_token_factor)
        .m_ou()?;

    let oracle_price_factor = 10_i128.checked_pow(oracle_price_decimals).m_ou()?;
    let min_collateral_threshold = min_collateral_value_cents
        .checked_mul(oracle_price_factor)
        .m_ou()?
        .checked_div(100)
        .m_ou()?;

    if collateral_value_left < min_collateral_threshold {
        seized = position_collateral_sum;
    }

    Ok(seized.max(0))
}

/// What the market does with a repay offer, per [`LiquidationPair::quote`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seizure {
    /// The liquidator receives `seized` collateral for `repaid` debt; the rest
    /// of the offer is refunded.
    Accepted { seized: i128, repaid: i128 },
    /// `InvalidLiquidationInputs` (#600): the repay is over the close factor.
    OverCloseFactor,
    /// `LiquidationLeavesDust` (#606).
    LeavesDust,
}

/// One (borrow, collateral) pair of an obligation, as `liquidate` judges it.
#[derive(Debug, Clone)]
pub struct LiquidationPair<'a> {
    pub rules: DustRules,
    pub borrow_pool: &'a PoolData,
    pub collateral_pool: &'a PoolData,
    pub deposit: &'a DepositPosition,
    /// The borrow position's debt in tokens, rounded up.
    pub position_debt: i128,
    pub obligation_debt_value: i128,
    pub obligation_collateral_value: i128,
    pub is_insolvent: bool,
    pub min_collateral_value_cents: i128,
    pub oracle_price_decimals: u32,
    /// Interest accrued on chain since the pools' last update, in bps of the
    /// debt and of the supplied collateral; see [`Self::accruing_until`].
    pub debt_accrual_bps: i128,
    pub supply_accrual_bps: i128,
    pub margin: ClearingMargin,
}

/// The smallest repay that clears a side of a pair at the snapshot, and the
/// offer that still does once interest has accrued.
struct Clearing {
    at_snapshot: i128,
    offer: i128,
}

impl<'a> LiquidationPair<'a> {
    pub fn new(
        rules: DustRules,
        obligation: &Obligation,
        market_data: &MarketData,
        borrow_pool: &'a PoolData,
        collateral_pool: &'a PoolData,
        deposit: &'a DepositPosition,
        is_insolvent: bool,
    ) -> Result<Self, LMError> {
        let borrow = obligation
            .borrows
            .iter()
            .find(|b| b.pool_address == borrow_pool.pool_address)
            .ok_or(LMError::InternalError)?;

        Ok(Self {
            rules,
            borrow_pool,
            collateral_pool,
            deposit,
            position_debt: borrow_pool.d_tokens_to_tokens_ceil(borrow.d_tokens)?.0,
            obligation_debt_value: compute_obligation_debt_value(obligation, market_data)?,
            obligation_collateral_value: compute_obligation_collateral_value(
                obligation,
                market_data,
            )?,
            is_insolvent,
            min_collateral_value_cents: market_data.min_collateral_value_cents,
            oracle_price_decimals: market_data.oracle_price_decimals,
            debt_accrual_bps: 0,
            supply_accrual_bps: 0,
            margin: ClearingMargin::INITIAL,
        })
    }

    /// Sizes clearing offers for the interest both pools accrue on chain from
    /// their last update until `now` (unix seconds).
    pub fn accruing_until(self, now: u64) -> Self {
        Self {
            debt_accrual_bps: self.borrow_pool.accrual_since_update_bps(now).0,
            supply_accrual_bps: self.collateral_pool.accrual_since_update_bps(now).1,
            ..self
        }
    }

    pub fn with_margin(self, margin: ClearingMargin) -> Self {
        Self { margin, ..self }
    }

    /// Whether the market lifts the close factor and the LTV cap for this pair.
    pub fn lifts_close_factor(&self) -> Result<bool, LMError> {
        Ok(self.is_insolvent || (self.rules == DustRules::NoDust && self.is_small_position()?))
    }

    /// Mirror of `liquidate` for a `repay` offer at the snapshot's prices and
    /// interest. A sweep market is quoted without its close-factor check, which
    /// callers apply through [`compute_close_factor_repay_cap`].
    pub fn quote(&self, repay: i128) -> Result<Seizure, LMError> {
        match self.rules {
            DustRules::Sweep => Ok(Seizure::Accepted {
                seized: compute_expected_seized_collateral(
                    repay,
                    self.borrow_pool,
                    self.collateral_pool,
                    self.deposit,
                    self.obligation_debt_value,
                    self.obligation_collateral_value,
                    self.is_insolvent,
                    self.min_collateral_value_cents,
                    self.oracle_price_decimals,
                )?,
                repaid: repay,
            }),
            DustRules::NoDust => self.quote_no_dust(repay),
        }
    }

    /// The offer for a planned `repay` that the market accepts, at most
    /// `max_offer`; `None` when there is none.
    ///
    /// On a no-dust market a repay that clears the debt or the whole collateral
    /// position is padded for interest accrued after the snapshot. One the
    /// market would refuse clears a side instead when `max_offer` allows, and
    /// otherwise shrinks until both sides stay at or above the floor.
    pub fn fit_repay(&self, repay: i128, max_offer: i128) -> Result<Option<i128>, LMError> {
        if self.rules == DustRules::Sweep {
            return Ok(Some(repay));
        }
        let accepts = |offer: i128| -> Result<bool, LMError> {
            Ok(matches!(self.quote(offer)?, Seizure::Accepted { .. }))
        };

        let clearing = self.clearing()?;
        if let Some(clear) = &clearing
            && repay >= clear.at_snapshot
        {
            let offer = repay.max(clear.offer).min(max_offer);
            if accepts(offer)? {
                return Ok(Some(offer));
            }
        } else {
            if accepts(repay)? {
                return Ok(Some(repay));
            }
            if let Some(clear) = &clearing
                && clear.offer <= max_offer
                && accepts(clear.offer)?
            {
                return Ok(Some(clear.offer));
            }
        }

        self.largest_partial_repay(repay.min(max_offer))
    }

    /// The liquidator's Direct repay with unlimited capital: its close-factor
    /// and collateral caps net of the profit margin, fitted to the dust rules.
    fn direct_repay(&self, min_profit_margin_cents: i128) -> Result<Option<i128>, LMError> {
        let lifted = self.lifts_close_factor()?;
        let max_feasible = self.position_debt.min(compute_close_factor_repay_cap(
            lifted,
            self.position_debt,
            self.borrow_pool.liquidation_close_factor_bps,
        ));
        let collateral = self.position_collateral()?;
        if max_feasible <= 0 || collateral <= 0 {
            return Ok(None);
        }
        let margin = compute_profit_margin_in_borrow_token(
            min_profit_margin_cents,
            self.oracle_price_decimals,
            self.borrow_pool,
        )
        .map_or(0, |m| m.0);
        let Some(max_profitable) = compute_repay_cap_from_collateral(
            !lifted,
            self.borrow_pool,
            max_feasible,
            self.collateral_pool,
            collateral,
            self.obligation_debt_value,
            margin,
            self.obligation_collateral_value,
        )
        .ok()
        .filter(|repay| *repay > 0) else {
            return Ok(None);
        };

        self.fit_repay(max_profitable, i128::MAX)
    }

    /// The liquidator's acceptance of a Direct plan.
    fn nets_more_than(&self, seized: i128, repaid: i128, min_profit_margin_cents: i128) -> bool {
        let value = |amount: i128, pool: &PoolData| {
            amount
                .saturating_mul(pool.oracle_asset_price)
                .saturating_div(10_i128.saturating_pow(pool.token_decimals))
        };
        let min_profit_margin_value = min_profit_margin_cents
            .saturating_mul(10_i128.saturating_pow(self.oracle_price_decimals))
            .saturating_div(100);

        compute_liquidation_profitability(
            value(seized, self.collateral_pool),
            value(repaid, self.borrow_pool),
            min_profit_margin_value,
        )
        .is_ok_and(|p| p.is_acceptable)
    }

    fn quote_no_dust(&self, repay: i128) -> Result<Seizure, LMError> {
        let (position_debt, position_collateral) =
            (self.position_debt, self.position_collateral()?);
        if repay <= 0
            || position_debt <= 0
            || position_collateral <= 0
            || self.borrow_pool.oracle_asset_price <= 0
            || self.collateral_pool.oracle_asset_price <= 0
        {
            return Ok(Seizure::Accepted { seized: 0, repaid: 0 });
        }

        let lifted = self.lifts_close_factor()?;
        let mut repaid = repay.min(position_debt);
        if !lifted {
            let repaid_bps = repaid
                .checked_mul(BPS_FACTOR)
                .m_ou()?
                .checked_add(position_debt - 1)
                .m_ou()?
                .checked_div(position_debt)
                .m_ou()?;
            if repaid_bps > self.borrow_pool.liquidation_close_factor_bps {
                return Ok(Seizure::OverCloseFactor);
            }
        }

        let seized = position_collateral.min(self.buys(repaid, lifted)?);
        if seized == position_collateral && self.is_small_position()? {
            repaid = repaid.min(self.whole_position_repay(position_collateral)?);
        }
        if self.leaves_dust(position_collateral - seized, position_debt - repaid)? {
            return Ok(Seizure::LeavesDust);
        }

        Ok(Seizure::Accepted { seized, repaid })
    }

    /// Collateral `repaid` debt buys before the position caps it.
    fn buys(&self, repaid: i128, lifted: bool) -> Result<i128, LMError> {
        let repaid_value = value_floor(repaid, self.borrow_pool)?;
        let with_incentive = amount_floor(repaid_value, self.collateral_pool)?
            .checked_mul(BPS_FACTOR.checked_add(self.incentive_bps()).m_ou()?)
            .m_ou()?
            .checked_div(BPS_FACTOR)
            .m_ou()?;
        if lifted {
            return Ok(with_incentive);
        }
        if self.obligation_debt_value <= 0 || self.obligation_collateral_value <= 0 {
            return Ok(0);
        }
        let ltv_improving_value = repaid_value
            .checked_mul(self.obligation_collateral_value)
            .m_ou()?
            .checked_div(self.obligation_debt_value)
            .m_ou()?
            .checked_mul(999)
            .m_ou()?
            .checked_div(1000)
            .m_ou()?;

        Ok(with_incentive.min(amount_floor(ltv_improving_value, self.collateral_pool)?))
    }

    fn incentive_bps(&self) -> i128 {
        self.borrow_pool
            .max_liquidation_incentive_bps
            .min(self.collateral_pool.max_liquidation_incentive_bps)
    }

    /// What a small position's whole collateral costs; the market refunds the
    /// rest of the offer.
    fn whole_position_repay(&self, position_collateral: i128) -> Result<i128, LMError> {
        let incentive_factor = BPS_FACTOR.checked_add(self.incentive_bps()).m_ou()?;
        let value = value_ceil(position_collateral, self.collateral_pool)?
            .checked_mul(BPS_FACTOR)
            .m_ou()?
            .checked_add(incentive_factor - 1)
            .m_ou()?
            .checked_div(incentive_factor)
            .m_ou()?;

        amount_ceil(value, self.borrow_pool)
    }

    fn leaves_dust(&self, collateral_left: i128, debt_left: i128) -> Result<bool, LMError> {
        if collateral_left <= 0 || debt_left <= 0 {
            return Ok(false);
        }
        let floor = self.dust_floor()?;

        Ok(value_floor(collateral_left, self.collateral_pool)? < floor
            || value_ceil(debt_left, self.borrow_pool)? < floor)
    }

    fn is_small_position(&self) -> Result<bool, LMError> {
        let small = self.dust_floor()?.checked_mul(2).m_ou()?;

        Ok(value_floor(self.position_collateral()?, self.collateral_pool)? < small
            || value_ceil(self.position_debt, self.borrow_pool)? < small)
    }

    fn dust_floor(&self) -> Result<i128, LMError> {
        self.min_collateral_value_cents
            .checked_mul(10_i128.checked_pow(self.oracle_price_decimals).m_ou()?)
            .m_ou()?
            .checked_div(100)
            .m_ou()
    }

    /// Plain collateral plus supplied tokens, converted as the market does
    /// rather than through the rounded `j_token_rate_floor_bps`.
    fn position_collateral(&self) -> Result<i128, LMError> {
        self.supplied_tokens()?.checked_add(self.deposit.collateral.0).m_ou()
    }

    fn supplied_tokens(&self) -> Result<i128, LMError> {
        Ok(self.collateral_pool.j_tokens_to_tokens_floor(self.deposit.j_tokens)?.0)
    }

    fn clearing(&self) -> Result<Option<Clearing>, LMError> {
        let (debt, collateral) = (self.position_debt, self.position_collateral()?);
        if debt <= 0 || collateral <= 0 {
            return Ok(None);
        }
        let lifted = self.lifts_close_factor()?;
        let repay_buying = |target: i128, debt: i128| -> Result<Option<i128>, LMError> {
            if self.buys(debt, lifted)? < target {
                return Ok(None);
            }
            let (mut low, mut high) = (1, debt);
            while low < high {
                let mid = low + (high - low) / 2;
                if self.buys(mid, lifted)? >= target {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }

            Ok(Some(high))
        };
        let grown =
            |amount: i128, bps: i128| amount.checked_add(fixed_mul_ceil(amount, bps)).m_ou();
        let grown_debt = grown(debt, self.debt_accrual_bps)?;
        let grown_collateral = self
            .deposit
            .collateral
            .0
            .checked_add(grown(self.supplied_tokens()?, self.supply_accrual_bps)?)
            .m_ou()?;
        let offer = match repay_buying(grown_collateral, grown_debt)? {
            Some(repay) if self.is_small_position()? => grown(repay, self.margin.refunded_bps)?,
            Some(repay) => grown(repay, self.margin.unrefunded_bps)?,
            None => grown(grown_debt, self.margin.refunded_bps)?,
        };

        Ok(Some(Clearing { at_snapshot: repay_buying(collateral, debt)?.unwrap_or(debt), offer }))
    }

    fn largest_partial_repay(&self, upper: i128) -> Result<Option<i128>, LMError> {
        let (debt, collateral) = (self.position_debt, self.position_collateral()?);
        let (lifted, floor) = (self.lifts_close_factor()?, self.dust_floor()?);
        let keeps_both_sides = |repay: i128| -> Result<bool, LMError> {
            let repaid = repay.min(debt);
            let collateral_left = collateral - collateral.min(self.buys(repaid, lifted)?);
            let debt_left = debt - repaid;

            Ok(collateral_left > 0
                && debt_left > 0
                && value_floor(collateral_left, self.collateral_pool)? >= floor
                && value_ceil(debt_left, self.borrow_pool)? >= floor)
        };
        if upper < 1 || !keeps_both_sides(1)? {
            return Ok(None);
        }
        let (mut low, mut high) = (1, upper);
        while low < high {
            let mid = low + (high - low + 1) / 2;
            if keeps_both_sides(mid)? {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        Ok(matches!(self.quote(low)?, Seizure::Accepted { .. }).then_some(low))
    }
}

fn token_factor(pool: &PoolData) -> Result<i128, LMError> {
    10_i128.checked_pow(pool.token_decimals).m_ou()
}

fn value_floor(amount: i128, pool: &PoolData) -> Result<i128, LMError> {
    amount.checked_mul(pool.oracle_asset_price).m_ou()?.checked_div(token_factor(pool)?).m_ou()
}

fn value_ceil(amount: i128, pool: &PoolData) -> Result<i128, LMError> {
    let factor = token_factor(pool)?;

    amount
        .checked_mul(pool.oracle_asset_price)
        .m_ou()?
        .checked_add(factor - 1)
        .m_ou()?
        .checked_div(factor)
        .m_ou()
}

fn amount_floor(value: i128, pool: &PoolData) -> Result<i128, LMError> {
    value.checked_mul(token_factor(pool)?).m_ou()?.checked_div(pool.oracle_asset_price).m_ou()
}

fn amount_ceil(value: i128, pool: &PoolData) -> Result<i128, LMError> {
    let price = pool.oracle_asset_price;

    value
        .checked_mul(token_factor(pool)?)
        .m_ou()?
        .checked_add(price - 1)
        .m_ou()?
        .checked_div(price)
        .m_ou()
}

/// Whether the liquidator acts on this obligation on a no-dust market: some
/// seizable pair has a plan that burns a debt share and nets more than
/// `min_profit_margin_cents`, sized and judged as its Direct plan with
/// unlimited capital, the cheapest of its plans. Collateral worth less than one
/// unit of the debt asset cannot be taken (`NonPositiveSharesAmount`), nor can
/// any in a pool the snapshot does not price.
pub fn has_acceptable_liquidation(
    obligation: &Obligation,
    market_data: &MarketData,
    min_profit_margin_cents: i128,
    now: u64,
) -> Result<bool, LMError> {
    if !unpriced_pools_touched(obligation, market_data).is_empty() {
        return Ok(false);
    }
    let is_insolvent = compute_is_insolvent(obligation, market_data)?;
    let pool_of = |address: &str| {
        market_data
            .pools_data
            .iter()
            .find(|p| p.pool_address == address)
            .ok_or(LMError::InternalError)
    };

    for borrow in &obligation.borrows {
        let borrow_pool = pool_of(&borrow.pool_address)?;
        for deposit in &obligation.deposits {
            let collateral_pool = pool_of(&deposit.pool_address)?;
            if collateral_pool.pool_address == borrow_pool.pool_address
                || collateral_pool.close_ltv_bps <= 0
            {
                continue;
            }
            let pair = LiquidationPair::new(
                DustRules::NoDust,
                obligation,
                market_data,
                borrow_pool,
                collateral_pool,
                deposit,
                is_insolvent,
            )?
            .accruing_until(now);
            let Some(repay) = pair.direct_repay(min_profit_margin_cents)? else {
                continue;
            };
            if let Seizure::Accepted { seized, repaid } = pair.quote(repay)?
                && seized > 0
                && (repaid == pair.position_debt
                    || borrow_pool.tokens_to_d_tokens_floor(Underlying(repaid))?.0 > 0)
                && pair.nets_more_than(seized, repaid, min_profit_margin_cents)
            {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Whether a no-dust market refuses to cover this obligation's bad debt
/// (#603): its seizable collateral, deposits with a positive close LTV, is
/// worth something and at least its debt, so liquidation has to clear it. Debt
/// the snapshot cannot price does not count as covered, as on chain.
pub fn seizable_collateral_covers_debt(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<bool, LMError> {
    let pool_of = |address: &str| market_data.pools_data.iter().find(|p| p.pool_address == address);

    let mut debt_value: i128 = 0;
    for borrow in &obligation.borrows {
        let Some(value) = pool_of(&borrow.pool_address).and_then(|pool| {
            pool.d_tokens_to_tokens_ceil(borrow.d_tokens)
                .and_then(|debt| value_ceil(debt.0, pool))
                .ok()
        }) else {
            return Ok(false);
        };
        let Some(sum) = debt_value.checked_add(value) else {
            return Ok(false);
        };
        debt_value = sum;
    }

    let mut collateral_value: i128 = 0;
    for deposit in &obligation.deposits {
        let pool = pool_of(&deposit.pool_address).ok_or(LMError::InternalError)?;
        if pool.close_ltv_bps <= 0 {
            continue;
        }
        let tokens =
            pool.j_tokens_to_tokens_floor(deposit.j_tokens)?.checked_add(deposit.collateral)?;
        collateral_value = collateral_value.checked_add(value_floor(tokens.0, pool)?).m_ou()?;
    }

    Ok(collateral_value > 0 && collateral_value >= debt_value)
}

/// Checks whether the unparameterized obligation is insolvent
pub fn compute_is_insolvent(
    obligation: &Obligation,
    market_data: &MarketData,
) -> Result<bool, LMError> {
    let debt_value = compute_obligation_debt_value(obligation, market_data)?;

    if debt_value < 0 {
        error!(debt_value, "negative debt value");

        return Err(LMError::InternalError);
    } else if debt_value == 0 {
        return Ok(false);
    }

    let collateral_value = compute_obligation_collateral_value(obligation, market_data)?;

    if collateral_value < 0 {
        error!(collateral_value, "negative collateral value");

        return Err(LMError::InternalError);
    } else if collateral_value == 0 {
        return Ok(true);
    }

    let unparameterized_ltv = bps_fixed_div_ceil(debt_value, collateral_value).m_ou()?;
    let is_insolvent = unparameterized_ltv >= market_data.insolvency_ltv_bps;

    Ok(is_insolvent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lending_model::obligation::BorrowPosition;

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
            // 1.0 at the fixture's 14 oracle decimals.
            oracle_asset_price: 100_000_000_000_000,
            open_ltv_bps: 8_000,
            close_ltv_bps: 8_500,
            liability_factor_bps: 10_000,
            liquidation_close_factor_bps: 5_000,
            max_liquidation_incentive_bps: 1_000,
            flash_loan_fee_bps: 5,
            status_flags: u32::MAX,
            utilization_ratio_limit_bps: 9_000,
            last_accrual_timestamp: 0,
            borrow_apy_bps: 0,
            supply_apy_bps: 0,
        }
    }

    /// A snapshot that prices `priced_pools` and flags `unavailable_pools` as
    /// unread. The two lists are independent on purpose: the aggregate path
    /// always leaves the second empty, so tests must be able to price nothing
    /// while flagging nothing.
    fn snapshot(priced_pools: &[&str], unavailable_pools: &[&str]) -> MarketData {
        MarketData {
            insolvency_ltv_bps: 10_000,
            pools_data: priced_pools.iter().map(|p| pool_data(p)).collect(),
            oracle_price_decimals: 14,
            min_collateral_value_cents: 0,
            unavailable_pools: unavailable_pools.iter().map(|p| p.to_string()).collect(),
        }
    }

    fn obligation(borrow_pool: &str, deposit_pool: &str) -> Obligation {
        Obligation {
            borrows: vec![BorrowPosition {
                d_tokens: 1_000.into(),
                pool_address: borrow_pool.to_string(),
            }],
            deposits: vec![DepositPosition {
                j_tokens: 1_000.into(),
                collateral: 0.into(),
                pool_address: deposit_pool.to_string(),
            }],
            open_bad_debt_requests: 0,
        }
    }

    #[test]
    fn complete_snapshot_never_blocks_an_obligation() {
        assert!(
            unpriced_pools_touched(
                &obligation("POOL_A", "POOL_B"),
                &snapshot(&["POOL_A", "POOL_B"], &[])
            )
            .is_empty()
        );
    }

    #[test]
    fn unavailable_borrow_pool_blocks_the_obligation() {
        assert_eq!(
            unpriced_pools_touched(
                &obligation("POOL_A", "POOL_B"),
                &snapshot(&["POOL_B"], &["POOL_A"])
            ),
            ["POOL_A"]
        );
    }

    #[test]
    fn unavailable_deposit_pool_blocks_the_obligation() {
        assert_eq!(
            unpriced_pools_touched(
                &obligation("POOL_A", "POOL_B"),
                &snapshot(&["POOL_A"], &["POOL_B"])
            ),
            ["POOL_B"]
        );
    }

    #[test]
    fn partial_snapshot_still_admits_untouched_obligations() {
        assert!(
            unpriced_pools_touched(
                &obligation("POOL_A", "POOL_B"),
                &snapshot(&["POOL_A", "POOL_B"], &["POOL_C"])
            )
            .is_empty()
        );
    }

    #[test]
    fn every_touched_pool_is_named_and_untouched_ones_are_not() {
        assert_eq!(
            unpriced_pools_touched(
                &obligation("POOL_A", "POOL_B"),
                &snapshot(&[], &["POOL_B", "POOL_C", "POOL_A"])
            ),
            ["POOL_A", "POOL_B"]
        );
    }

    #[test]
    fn a_pool_held_on_both_sides_is_named_once() {
        assert_eq!(
            unpriced_pools_touched(&obligation("POOL_A", "POOL_A"), &snapshot(&[], &["POOL_A"])),
            ["POOL_A"]
        );
    }

    #[test]
    fn a_pool_missing_from_both_lists_still_blocks_the_obligation() {
        // The aggregate path always reports `unavailable_pools` empty, so a pool
        // `get_all_pools` never returned is on no list at all. Judging that
        // obligation would silently drop its debt or its collateral.
        assert_eq!(
            unpriced_pools_touched(&obligation("POOL_A", "POOL_B"), &snapshot(&["POOL_A"], &[])),
            ["POOL_B"]
        );
    }

    #[test]
    fn a_missing_deposit_pool_would_have_judged_a_healthy_position_liquidatable() {
        // Most of the collateral sits in POOL_C. Dropping it — what
        // `compute_is_liquidatable` does with an unknown pool — flips a
        // comfortably healthy obligation to liquidatable, so the guard has to
        // catch it before the verdict is computed. POOL_B keeps
        // `has_any_collateral` satisfied, which is what makes the wrong verdict
        // reachable at all.
        let obligation = Obligation {
            borrows: vec![BorrowPosition { d_tokens: 1_000.into(), pool_address: "POOL_A".into() }],
            deposits: vec![
                DepositPosition {
                    j_tokens: 100.into(),
                    collateral: 0.into(),
                    pool_address: "POOL_B".into(),
                },
                DepositPosition {
                    j_tokens: 10_000.into(),
                    collateral: 0.into(),
                    pool_address: "POOL_C".into(),
                },
            ],
            open_bad_debt_requests: 0,
        };
        let full = snapshot(&["POOL_A", "POOL_B", "POOL_C"], &[]);
        let missing_collateral = snapshot(&["POOL_A", "POOL_B"], &[]);

        assert!(
            !compute_is_liquidatable(&obligation, &full, DustRules::Sweep)
                .expect("healthy verdict")
        );
        assert!(
            compute_is_liquidatable(&obligation, &missing_collateral, DustRules::Sweep)
                .expect("degraded verdict"),
            "the degraded snapshot must be the dangerous one this guard exists to block"
        );
        assert_eq!(unpriced_pools_touched(&obligation, &missing_collateral), ["POOL_C"]);
    }

    /// Market-default risk parameters (10 % incentive, 50 % close factor,
    /// 98.5 % insolvency LTV, $5 floor) with every asset worth $1 a token
    /// unless repriced, so token amounts read as dollars.
    mod dust {
        use super::*;

        const UNIT: i128 = 10_000_000;
        const USD: i128 = 100_000_000_000_000;
        const USDC: &str = "USDC";
        const GOLD: &str = "GOLD";
        const BTC: &str = "BTC";

        fn pool(address: &str, price: i128) -> PoolData {
            PoolData {
                total_borrowed: (1_000_000 * UNIT).into(),
                total_d_tokens: (1_000_000 * UNIT).into(),
                total_j_tokens: (1_000_000 * UNIT).into(),
                total_supply: (1_000_000 * UNIT).into(),
                oracle_asset_price: price,
                close_ltv_bps: 8_000,
                ..pool_data(address)
            }
        }

        fn market(gold_price: i128) -> MarketData {
            MarketData {
                insolvency_ltv_bps: 9_850,
                pools_data: vec![pool(USDC, USD), pool(GOLD, gold_price), pool(BTC, USD)],
                oracle_price_decimals: 14,
                min_collateral_value_cents: 500,
                unavailable_pools: Vec::new(),
            }
        }

        fn deposit(pool_address: &str, collateral: i128) -> DepositPosition {
            DepositPosition {
                j_tokens: 0.into(),
                collateral: collateral.into(),
                pool_address: pool_address.to_string(),
            }
        }

        /// `usdc_debt` borrowed against plain GOLD, plus any other deposits.
        fn borrower(usdc_debt: i128, gold: i128, others: Vec<DepositPosition>) -> Obligation {
            Obligation {
                borrows: vec![BorrowPosition {
                    d_tokens: usdc_debt.into(),
                    pool_address: USDC.to_string(),
                }],
                deposits: [vec![deposit(GOLD, gold)], others].concat(),
                open_bad_debt_requests: 0,
            }
        }

        /// The (USDC, GOLD) pair, judged insolvent or not as the keeper does.
        fn gold_pair<'a>(
            rules: DustRules,
            obligation: &'a Obligation,
            market: &'a MarketData,
        ) -> LiquidationPair<'a> {
            let is_insolvent = compute_is_insolvent(obligation, market).expect("insolvency");

            LiquidationPair::new(
                rules,
                obligation,
                market,
                &market.pools_data[0],
                &market.pools_data[1],
                &obligation.deposits[0],
                is_insolvent,
            )
            .expect("pair")
        }

        fn accepted(seized: i128, repaid: i128) -> Seizure {
            Seizure::Accepted { seized, repaid }
        }

        #[test]
        fn sweep_hands_a_small_position_whole_to_a_half_repay() {
            let market = market(USD);
            // $4.50 / $1 and s1's $6.20 / $3: the close factor binds, the sweep
            // pays out everything left.
            for (gold, debt) in [(45 * UNIT / 10, UNIT), (62 * UNIT / 10, 3 * UNIT)] {
                let obligation = borrower(debt, gold, vec![]);
                let pair = gold_pair(DustRules::Sweep, &obligation, &market);

                assert!(!pair.lifts_close_factor().unwrap());
                assert_eq!(pair.quote(debt / 2).unwrap(), accepted(gold, debt / 2));
                assert_eq!(pair.fit_repay(debt / 2, i128::MAX).unwrap(), Some(debt / 2));
            }
        }

        #[test]
        fn sweep_hands_an_insolvent_leftover_under_the_floor_to_the_liquidator() {
            let market = market(USD);
            let obligation = borrower(95 * UNIT, 90 * UNIT, vec![]);
            let pair = gold_pair(DustRules::Sweep, &obligation, &market);

            // $78 buys $85.80 at the incentive; the $4.20 left is swept too.
            assert!(pair.is_insolvent);
            assert_eq!(pair.quote(78 * UNIT).unwrap(), accepted(90 * UNIT, 78 * UNIT));
        }

        #[test]
        fn only_a_no_dust_market_liquidates_collateral_under_the_floor() {
            let market = market(USD);
            let obligation = borrower(UNIT, 45 * UNIT / 10, vec![]);

            assert!(!compute_is_liquidatable(&obligation, &market, DustRules::Sweep).unwrap());
            assert!(compute_is_liquidatable(&obligation, &market, DustRules::NoDust).unwrap());
        }

        #[test]
        fn no_dust_needs_the_whole_debt_of_a_small_position() {
            let market = market(USD);
            let obligation = borrower(UNIT, 45 * UNIT / 10, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);

            assert!(pair.lifts_close_factor().unwrap());
            assert_eq!(pair.quote(UNIT / 2).unwrap(), Seizure::LeavesDust);

            // The keeper plans the whole $1 and offers 1 % over it: it pays the
            // $1 and receives $1.10.
            let offer = pair.fit_repay(UNIT, i128::MAX).unwrap().expect("an offer");
            assert_eq!(offer, UNIT + UNIT / 100);
            assert_eq!(pair.quote(offer).unwrap(), accepted(11 * UNIT / 10, UNIT));

            // Interest accrued since the quote: the exact quoted debt now leaves
            // a unit behind, the padded offer still clears it.
            let accrued = LiquidationPair { position_debt: UNIT + 1, ..pair.clone() };
            assert_eq!(accrued.quote(UNIT).unwrap(), Seizure::LeavesDust);
            assert!(matches!(
                accrued.quote(offer).unwrap(),
                Seizure::Accepted { repaid, .. } if repaid == UNIT + 1
            ));
        }

        #[test]
        fn no_dust_s1_refuses_half_and_refunds_a_double_offer() {
            let market = market(USD);
            let obligation = borrower(3 * UNIT, 62 * UNIT / 10, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);

            assert_eq!(pair.quote(3 * UNIT / 2).unwrap(), Seizure::LeavesDust);
            assert_eq!(pair.quote(6 * UNIT).unwrap(), accepted(33 * UNIT / 10, 3 * UNIT));
            assert_eq!(pair.fit_repay(3 * UNIT, i128::MAX).unwrap(), Some(303 * UNIT / 100));
        }

        #[test]
        fn no_dust_refunds_a_small_position_down_to_its_whole_collateral() {
            let market = market(USD);
            // T10 $1.05 / $1: the keeper's collateral cap is a unit short of
            // the whole position, which would leave dust.
            let obligation = borrower(UNIT, 105 * UNIT / 100, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);

            assert_eq!(pair.quote(9_545_454).unwrap(), Seizure::LeavesDust);
            let offer = pair.fit_repay(9_545_454, i128::MAX).unwrap().expect("an offer");
            assert_eq!(pair.quote(offer).unwrap(), accepted(105 * UNIT / 100, 9_545_455));
            assert_eq!(pair.quote(2 * UNIT).unwrap(), accepted(105 * UNIT / 100, 9_545_455));
        }

        #[test]
        fn no_dust_insolvent_repay_must_take_the_whole_collateral() {
            let market = market(USD);
            let obligation = borrower(95 * UNIT, 90 * UNIT, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);

            assert_eq!(pair.quote(78 * UNIT).unwrap(), Seizure::LeavesDust);
            // The collateral cap floors to 818_181_818, a unit short of $90.
            assert_eq!(pair.quote(818_181_818).unwrap(), Seizure::LeavesDust);
            let offer = pair.fit_repay(818_181_818, i128::MAX).unwrap().expect("an offer");
            assert_eq!(offer, 818_181_819);
            assert_eq!(pair.quote(offer).unwrap(), accepted(90 * UNIT, offer));
        }

        #[test]
        fn no_dust_keeps_the_close_factor_at_twice_the_floor() {
            let market = market(USD);
            let obligation = borrower(10 * UNIT, 105 * UNIT / 10, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);

            assert!(!pair.lifts_close_factor().unwrap());
            // $5 repaid leaves $5.25525 of collateral and $5 of debt.
            assert_eq!(pair.quote(5 * UNIT).unwrap(), accepted(52_447_500, 5 * UNIT));
            assert_eq!(pair.quote(5 * UNIT + 1).unwrap(), Seizure::OverCloseFactor);
        }

        #[test]
        fn no_dust_lifts_the_close_factor_one_price_unit_under_twice_the_floor() {
            let btc = deposit(BTC, 100 * UNIT);
            let obligation = borrower(95 * UNIT, 10 * UNIT, vec![btc]);

            let at_twice = market(USD);
            let pair = gold_pair(DustRules::NoDust, &obligation, &at_twice);
            assert!(!pair.lifts_close_factor().unwrap());
            assert_eq!(pair.quote(190 * UNIT).unwrap(), Seizure::OverCloseFactor);

            let under = market(USD - 1);
            let pair = gold_pair(DustRules::NoDust, &obligation, &under);
            assert!(pair.lifts_close_factor().unwrap());
            assert_eq!(pair.quote(190 * UNIT).unwrap(), accepted(10 * UNIT, 90_909_091));
        }

        #[test]
        fn no_dust_shrinks_an_unaffordable_clear_to_a_repay_above_the_floor() {
            let market = market(USD);
            // Debt-bound $9 / $8: leaving $5 of debt bounds the repay at $3.
            let obligation = borrower(8 * UNIT, 9 * UNIT, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);
            assert_eq!(pair.quote(4 * UNIT).unwrap(), Seizure::LeavesDust);
            assert_eq!(pair.fit_repay(4 * UNIT, 4 * UNIT).unwrap(), Some(3 * UNIT));
            assert_eq!(pair.quote(3 * UNIT).unwrap(), accepted(33 * UNIT / 10, 3 * UNIT));
            assert_eq!(pair.quote(3 * UNIT + 1).unwrap(), Seizure::LeavesDust);

            // Collateral-bound $9 / $8.80: leaving $5 of collateral binds first.
            let obligation = borrower(88 * UNIT / 10, 9 * UNIT, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);
            assert_eq!(pair.fit_repay(4 * UNIT, 4 * UNIT).unwrap(), Some(36_363_637));
            assert_eq!(pair.quote(36_363_637).unwrap(), accepted(4 * UNIT, 36_363_637));
            assert_eq!(pair.quote(36_363_638).unwrap(), Seizure::LeavesDust);

            // Nothing fits a $1 debt short of the whole of it.
            let obligation = borrower(UNIT, 45 * UNIT / 10, vec![]);
            let pair = gold_pair(DustRules::NoDust, &obligation, &market);
            assert_eq!(pair.fit_repay(UNIT, UNIT).unwrap(), Some(UNIT));
            assert_eq!(pair.fit_repay(UNIT / 2, UNIT / 2).unwrap(), None);
        }

        const THIRTY_DAYS: u64 = 30 * 86_400;
        /// The keeper's collateral cap on $3 of GOLD: $3 / 1.1, rounded down.
        const CAP_ON_THREE_DOLLARS: i128 = 27_272_727;

        #[test]
        fn no_dust_sizes_a_collateral_clear_for_supply_accrued_since_the_pool_update() {
            // 20 GOLD of supply shares at $0.15 against $3; the GOLD pool idled
            // 30 days at an 18.25 % supply APY, so the shares are worth 1.49 %
            // more on chain than its view says, past the 1 % margin.
            let mut stale = market(USD * 15 / 100);
            stale.pools_data[1].supply_apy_bps = 1_825;
            let mut obligation = borrower(3 * UNIT, 0, vec![]);
            obligation.deposits[0].j_tokens = (20 * UNIT).into();
            let mut chain = stale.clone();
            chain.pools_data[1].total_supply = (1_014_900 * UNIT).into();
            let on_chain = gold_pair(DustRules::NoDust, &obligation, &chain);

            let pair =
                gold_pair(DustRules::NoDust, &obligation, &stale).accruing_until(THIRTY_DAYS);
            assert_eq!(pair.supply_accrual_bps, 150);
            let offer = pair.fit_repay(CAP_ON_THREE_DOLLARS, i128::MAX).unwrap().expect("offer");
            assert!(matches!(
                on_chain.quote(offer).unwrap(),
                Seizure::Accepted { seized: 202_980_000, .. }
            ));

            let unaware = gold_pair(DustRules::NoDust, &obligation, &stale);
            let offer = unaware.fit_repay(CAP_ON_THREE_DOLLARS, i128::MAX).unwrap().expect("offer");
            assert_eq!(on_chain.quote(offer).unwrap(), Seizure::LeavesDust);
        }

        #[test]
        fn no_dust_sizes_a_debt_clear_for_borrow_interest_accrued_since_the_pool_update() {
            // $4.50 / $1 with the USDC pool idle 30 days at a 20 % borrow APY:
            // the debt is 1.64 % above the view, past the 1 % margin.
            let mut stale = market(USD);
            stale.pools_data[0].borrow_apy_bps = 2_000;
            let obligation = borrower(UNIT, 45 * UNIT / 10, vec![]);
            let mut chain = stale.clone();
            chain.pools_data[0].total_borrowed = (1_016_400 * UNIT).into();
            let on_chain = gold_pair(DustRules::NoDust, &obligation, &chain);

            let pair =
                gold_pair(DustRules::NoDust, &obligation, &stale).accruing_until(THIRTY_DAYS);
            let offer = pair.fit_repay(UNIT, i128::MAX).unwrap().expect("offer");
            assert_eq!(on_chain.quote(offer).unwrap(), accepted(11_180_400, 10_164_000));

            let unaware = gold_pair(DustRules::NoDust, &obligation, &stale);
            let offer = unaware.fit_repay(UNIT, i128::MAX).unwrap().expect("offer");
            assert_eq!(on_chain.quote(offer).unwrap(), Seizure::LeavesDust);
        }

        #[test]
        fn no_dust_pads_a_small_collateral_clear_for_a_price_tick() {
            // 20 plain GOLD at $0.15 against $3: the exact clear is refused once
            // GOLD rises a basis point; the refunded 1 % margin is not.
            let gold_price = USD * 15 / 100;
            let obligation = borrower(3 * UNIT, 20 * UNIT, vec![]);
            let snapshot = market(gold_price);
            let pair = gold_pair(DustRules::NoDust, &obligation, &snapshot);
            let ticked = market(gold_price + gold_price / 10_000);
            let on_chain = gold_pair(DustRules::NoDust, &obligation, &ticked);

            let offer = pair.fit_repay(CAP_ON_THREE_DOLLARS, i128::MAX).unwrap().expect("offer");
            assert!(matches!(
                on_chain.quote(offer).unwrap(),
                Seizure::Accepted { seized, .. } if seized == 20 * UNIT
            ));
            assert_eq!(on_chain.quote(27_272_728).unwrap(), Seizure::LeavesDust);

            // A 3 % move outruns the initial margin; the retry's covers it.
            let jumped = market(gold_price + gold_price * 3 / 100);
            let on_chain = gold_pair(DustRules::NoDust, &obligation, &jumped);
            assert_eq!(on_chain.quote(offer).unwrap(), Seizure::LeavesDust);
            let retry = pair.clone().with_margin(ClearingMargin::RETRY);
            let offer = retry.fit_repay(CAP_ON_THREE_DOLLARS, i128::MAX).unwrap().expect("offer");
            assert!(matches!(on_chain.quote(offer).unwrap(), Seizure::Accepted { .. }));
        }

        #[test]
        fn no_dust_collateral_is_liquidated_only_by_a_plan_the_liquidator_accepts() {
            // $2.40 of GOLD against $3 nets $0.22: enough for a zero margin,
            // not for a 50-cent one.
            let obligation = borrower(3 * UNIT, 30 * UNIT, vec![]);
            let cheap = market(USD * 8 / 100);
            assert!(has_acceptable_liquidation(&obligation, &cheap, 0, 0).unwrap());
            assert!(!has_acceptable_liquidation(&obligation, &cheap, 50, 0).unwrap());

            // GOLD at one price unit: the whole position costs one stroop of
            // USDC, which burns no d-token once the d-rate is above 1.
            let mut worthless = market(1);
            worthless.pools_data[0].total_borrowed = (1_100_000 * UNIT).into();
            assert!(!has_acceptable_liquidation(&obligation, &worthless, -1_000, 0).unwrap());

            let mut supply_only = cheap.clone();
            supply_only.pools_data[1].close_ltv_bps = 0;
            assert!(!has_acceptable_liquidation(&obligation, &supply_only, 0, 0).unwrap());

            let mut no_deposits = obligation.clone();
            no_deposits.deposits.clear();
            assert!(!has_acceptable_liquidation(&no_deposits, &market(USD), 0, 0).unwrap());
        }

        #[test]
        fn no_dust_cover_is_refused_while_seizable_collateral_covers_the_debt() {
            let market = market(USD);
            let covers = |debt: i128, gold: i128, others: Vec<DepositPosition>| {
                seizable_collateral_covers_debt(&borrower(debt, gold, others), &market).unwrap()
            };

            assert!(covers(UNIT, 45 * UNIT / 10, vec![]));
            assert!(covers(3 * UNIT, 3 * UNIT, vec![]));
            assert!(!covers(3 * UNIT, 3 * UNIT - 1, vec![]));
            assert!(!covers(3 * UNIT, 24 * UNIT / 10, vec![]));

            let mut supply_only = market.clone();
            supply_only.pools_data[2].close_ltv_bps = 0;
            let obligation = borrower(3 * UNIT, 0, vec![deposit(BTC, 100 * UNIT)]);
            assert!(!seizable_collateral_covers_debt(&obligation, &supply_only).unwrap());
        }
    }
}
