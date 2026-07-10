//! Obligation, deposit, and borrow position model.

use serde::{Deserialize, Serialize};

use crate::lending_model::{DTokens, JTokens, Underlying};

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
        Self { user, seed: Some(seed) }
    }

    pub fn seed_as_str(&self) -> &str {
        self.seed.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositPosition {
    pub j_tokens: JTokens,
    pub collateral: Underlying,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowPosition {
    pub d_tokens: DTokens,
    pub pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Obligation {
    pub borrows: Vec<BorrowPosition>,
    pub deposits: Vec<DepositPosition>,
}
