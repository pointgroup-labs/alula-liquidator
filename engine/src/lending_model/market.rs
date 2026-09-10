//! Market-wide configuration: collection of pools + global parameters.

use crate::lending_model::pool::PoolData;

#[derive(Debug, Clone)]
pub struct MarketData {
    pub insolvency_ltv_bps: i128,
    // TODO: HashMap<String, PoolData> instead
    pub pools_data: Vec<PoolData>,
    pub oracle_price_decimals: u32,
    pub min_collateral_value_cents: i128,
    /// Pools the snapshot could not read (an unpriced asset degrades only
    /// itself), for attribution and metrics. Not the gate on judging an
    /// obligation — that is presence in `pools_data`, since a pool can be
    /// absent from both lists.
    pub unavailable_pools: Vec<String>,
}

impl MarketData {
    /// True when a rebuild read no pool at all. Such a snapshot holds no price,
    /// so it must not replace the previous one: installing it would stamp a
    /// fresh timestamp over an empty view and the age guard would never fire.
    pub fn prices_nothing(&self) -> bool {
        self.pools_data.is_empty() && !self.unavailable_pools.is_empty()
    }
}
