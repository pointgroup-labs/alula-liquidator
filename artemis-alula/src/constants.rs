pub const REFRESH_INTERVAL_BLOCKS: u32 = 1;

// -- Transaction constants --

/// Default fee for simulation transactions (in stroops).
pub const DEFAULT_SIMULATION_FEE: u32 = 100_000;

/// Basis points denominator (10,000 = 100%).
pub const BPS_DENOMINATOR: i128 = 10_000;

/// Interest accrual buffer for liquidations (102% = 2% buffer).
pub const LIQUIDATION_INTEREST_BUFFER_BPS: i128 = 10_200;

// -- Event name constants --

pub const EVT_REPAY: &str = "repay_event";
pub const EVT_BORROW: &str = "borrow_event";
pub const EVT_DEPOSIT: &str = "deposit_event";
pub const EVT_WITHDRAW: &str = "withdraw_event";
pub const EVT_LIQUIDATE: &str = "liquidate_event";
pub const EVT_ADD_COLLATERAL: &str = "add_collateral_event";
pub const EVT_REMOVE_COLLATERAL: &str = "remove_collateral_event";