//! Obligation, deposit, and borrow position model.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObligationKey {
    pub user: String,
    pub seed: Option<String>,
}

impl ObligationKey {
    pub fn new(user: String) -> Self {
        Self { user, seed: None }
    }

    pub fn new_with_seed(user: String, seed: String) -> Self {
        Self {
            user,
            seed: Some(seed),
        }
    }

    pub fn seed_as_str(&self) -> &str {
        self.seed.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositPosition {
    pub j_tokens: i128,
    pub collateral: i128,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowPosition {
    pub d_tokens: i128,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Obligation {
    pub borrows: Vec<BorrowPosition>,
    pub deposits: Vec<DepositPosition>,
}
