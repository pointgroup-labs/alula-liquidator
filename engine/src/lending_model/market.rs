//! Market-wide configuration: collection of pools + global parameters.

use crate::lending_model::pool::PoolData;

#[derive(Debug, Clone)]
pub struct MarketData {
    pub insolvency_ltv_bps: i128,
    // TODO: HashMap<String, PoolData> instead
    pub pools_data: Vec<PoolData>,
    pub oracle_price_decimals: u32,
    pub min_collateral_value_cents: i128,
}
