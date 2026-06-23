//! Cross-strategy in-flight capital reservation.

use {
    crate::error::KeeperError,
    engine::ports::LedgerReader,
    metrics::gauge,
    parking_lot::Mutex,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
    tracing::{error, info},
};

#[derive(Debug)]
struct Reservation {
    amount: i128,
    created_at: Instant,
    token_address: String,
}

#[derive(Debug)]
struct CachedBalance {
    amount: i128,
    fetched_at: Instant,
}

#[derive(Debug)]
struct CapitalInner {
    // reservation ID => balance reservation
    reservations: HashMap<u64, Reservation>,
    // Token address => cached balance
    balances: HashMap<String, CachedBalance>,
}

impl CapitalInner {
    fn unlock_expired(&mut self, reservation_ttl: Duration) {
        self.reservations
            .retain(|_, r| r.created_at.elapsed() < reservation_ttl);
    }
}

#[derive(Debug)]
pub struct LiquidatorCapitalConfig {
    xlm_address: String,
    reservation_ttl: Duration,
    balance_cache_ttl: Duration,
}

#[derive(Debug)]
pub struct LiquidatorCapital {
    pkey: String,
    inner: Mutex<CapitalInner>,
    config: LiquidatorCapitalConfig,
}

impl LiquidatorCapital {
    pub fn new(pkey: String, config: LiquidatorCapitalConfig) -> Self {
        Self {
            pkey,
            config,
            inner: Mutex::new(CapitalInner {
                balances: HashMap::new(),
                reservations: HashMap::new(),
            }),
        }
    }

    pub fn reserve(
        &self,
        amount: i128,
        available: i128,
        token_address: &str,
    ) -> anyhow::Result<u64> {
        if !amount.is_positive() {
            error!(amount, "non-positive reservation amount");

            return Err(KeeperError::InternalError.into());
        }

        let mut guard = self.inner.lock();

        guard.unlock_expired(self.config.reservation_ttl);

        let committed: i128 = guard
            .reservations
            .values()
            .filter(|r| r.token_address == token_address)
            .map(|r| r.amount)
            .sum();
        if committed.saturating_add(amount) > available {
            info!(committed, amount, available);

            return Err(KeeperError::NotEnoughAvailableBalance.into());
        }

        let reservation_id = random_id();
        guard.reservations.insert(
            reservation_id,
            Reservation {
                amount,
                created_at: Instant::now(),
                token_address: token_address.to_string(),
            },
        );

        Ok(reservation_id)
    }

    pub fn release(&self, reservation_id: u64) {
        let mut guard = self.inner.lock();
        guard.reservations.remove(&reservation_id);
    }

    pub async fn try_get_balance(
        &self,
        token_address: &str,
        ledger_reader: &dyn LedgerReader,
    ) -> anyhow::Result<i128> {
        if let Some(b) = self.get_freshly_cached_balance(token_address) {
            return Ok(b);
        }
        // NB: No freshly cached balance exists, so read it directly

        let amount = ledger_reader
            .read_token_balance(token_address, &self.pkey)
            .await?;
        gauge!("liquidator_asset_balance", "token_address" => token_address.to_string())
            .set(amount as f64);
        if token_address == self.config.xlm_address {
            gauge!("liquidator_xlm_balance_stroops").set(amount as f64);
        }

        let mut guard = self.inner.lock();
        guard.balances.insert(
            token_address.to_string(),
            CachedBalance {
                amount,
                fetched_at: Instant::now(),
            },
        );

        Ok(amount)
    }

    fn get_freshly_cached_balance(&self, token_address: &str) -> Option<i128> {
        let guard = self.inner.lock();

        guard
            .balances
            .get(token_address)
            .filter(|c| c.fetched_at.elapsed() < self.config.balance_cache_ttl)
            .map(|c| c.amount)
    }
}

pub fn random_id() -> u64 {
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
