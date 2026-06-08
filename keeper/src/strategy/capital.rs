//! Cross-strategy in-flight capital reservation.
//!
//! Design choice: reservations are released *explicitly* by the executor when
//! a tx settles (success or failure) — see `execute::stellar_tx::SettleHook`.
//! The TTL (default 5 min) is a safety ceiling that catches dropped
//! release calls.
//!
//! Balance lookups are cached for a short TTL to amortize the per-opportunity
//! `read_token_balance` RPC roundtrip.

use {
    crate::error::KeeperError,
    engine::ports::LedgerReader,
    metrics::gauge,
    parking_lot::Mutex,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

#[derive(Debug, Clone)]
struct Reservation {
    amount: i128,
    token: String,
    account: String, // TODO: Remove account?
    created_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct CachedBalance {
    amount: i128,
    fetched_at: Instant,
}

#[derive(Debug)]
struct LiquidatorCapitalInner {
    reservations: HashMap<u64, Reservation>,
    balances: HashMap<(String, String), CachedBalance>, // TODO: Remove account
}

#[derive(Debug)]
pub struct LiquidatorCapital {
    xlm_address: String,
    balance_ttl: Duration,
    inner: Mutex<LiquidatorCapitalInner>,
    reservation_ttl: Duration,
}

impl LiquidatorCapital {
    pub fn new(xlm_address: String, reservation_ttl: Duration, balance_ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(LiquidatorCapitalInner {
                reservations: HashMap::new(),
                balances: HashMap::new(),
            }),
            reservation_ttl,
            balance_ttl,
            xlm_address,
        }
    }

    /// Records the reservation iff doing so keeps the total
    /// committed amount for `(token, account)` at or below `available`.
    pub fn reserve(
        &self,
        token: &str,
        account: &str,
        amount: i128,
        available: i128, // TODO: Must be from balances?
    ) -> anyhow::Result<u64> {
        if !amount.is_positive() {
            // TODO: log
            return Err(KeeperError::InternalError.into());
        }

        let mut ledger = self.inner.lock();
        self.unlock_expired(&mut ledger);

        let committed: i128 = ledger
            .reservations
            .values()
            .filter(|r| r.token == token && r.account == account)
            .map(|r| r.amount)
            .sum();
        if committed.saturating_add(amount) > available {
            return Err(KeeperError::InternalError.into());
        }

        let op_id = random_op_id();
        ledger.reservations.insert(
            op_id,
            Reservation {
                amount,
                token: token.to_string(),
                created_at: Instant::now(),
                account: account.to_string(),
            },
        );

        Ok(op_id)
    }

    pub fn release(&self, op_id: u64) {
        let mut g = self.inner.lock();
        g.reservations.remove(&op_id);
    }

    /// Returns a cached balance if fresh, else fetches and caches.
    pub async fn cached_balance(
        &self,
        token: &str,
        account: &str,
        ledger_reader: &dyn LedgerReader, // TODO: Shouldn't this be on the LedgerReader?
    ) -> anyhow::Result<i128> {
        let key = (token.to_owned(), account.to_owned());
        if let Some(b) = self.freshly_cached(&key) {
            return Ok(b);
        }

        let token_balance = ledger_reader.read_token_balance(token, account).await?;
        gauge!("liquidator_asset_balance", "token_address" => token.to_string())
            .set(token_balance as f64);
        if token == self.xlm_address {
            gauge!("liquidator_xlm_balance_stroops").set(token_balance as f64);
        }
        let mut ledger = self.inner.lock();
        ledger.balances.insert(
            key,
            CachedBalance {
                amount: token_balance,
                fetched_at: Instant::now(),
            },
        );

        Ok(token_balance)
    }

    fn freshly_cached(&self, key: &(String, String)) -> Option<i128> {
        let g = self.inner.lock();
        g.balances
            .get(key)
            .filter(|c| c.fetched_at.elapsed() < self.balance_ttl)
            .map(|c| c.amount)
    }

    fn unlock_expired(&self, g: &mut LiquidatorCapitalInner) {
        let ttl = self.reservation_ttl;
        g.reservations.retain(|_, r| r.created_at.elapsed() < ttl);
    }
}

pub fn random_op_id() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    nanos ^ ((std::process::id() as u64) << 32) ^ rand_seed()
}

fn rand_seed() -> u64 {
    static C: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    C.fetch_add(0x9E37_79B9_7F4A_7C15, std::sync::atomic::Ordering::Relaxed)
}
