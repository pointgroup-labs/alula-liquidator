//! Cross-strategy in-flight capital reservation.
//!
//! Design choice: reservations are released *explicitly* by the executor when
//! a tx settles (success or failure) — see `execute::stellar_tx::SettleHook`.
//! The TTL (default 5 min) is now only a safety ceiling that catches dropped
//! release calls; it is no longer the primary release mechanism.
//!
//! Balance lookups are cached for a short TTL to amortize the per-opportunity
//! `read_token_balance` RPC roundtrip. The ledger is `Send + Sync`, and the
//! same `Arc<CapitalLedger>` is shared by every balance-spending strategy
//! (Liquidator, Rebalancer) so they can't double-commit the same wallet
//! capacity within a block.

use {
    engine::ports::ChainReader,
    parking_lot::Mutex,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

pub const DEFAULT_RESERVATION_TTL: Duration = Duration::from_secs(300);
pub const DEFAULT_BALANCE_TTL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct Reservation {
    token: String,
    account: String,
    amount: i128,
    created_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct CachedBalance {
    value: i128,
    fetched_at: Instant,
}

#[derive(Debug)]
struct LedgerInner {
    reservations: HashMap<u64, Reservation>,
    balances: HashMap<(String, String), CachedBalance>,
}

#[derive(Debug)]
pub struct CapitalLedger {
    inner: Mutex<LedgerInner>,
    reservation_ttl: Duration,
    balance_ttl: Duration,
}

impl CapitalLedger {
    pub fn new() -> Self {
        Self::with_ttls(DEFAULT_RESERVATION_TTL, DEFAULT_BALANCE_TTL)
    }

    pub fn with_ttls(reservation_ttl: Duration, balance_ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(LedgerInner {
                reservations: HashMap::new(),
                balances: HashMap::new(),
            }),
            reservation_ttl,
            balance_ttl,
        }
    }

    /// Returns true and records the reservation iff doing so keeps the total
    /// committed amount for `(token, account)` at or below `available`.
    pub fn reserve(
        &self,
        op_id: u64,
        token: &str,
        account: &str,
        amount: i128,
        available: i128,
    ) -> bool {
        if amount <= 0 {
            return false;
        }
        let mut g = self.inner.lock();
        self.expire_locked(&mut g);
        let committed: i128 = g
            .reservations
            .values()
            .filter(|r| r.token == token && r.account == account)
            .map(|r| r.amount)
            .sum();
        if committed.saturating_add(amount) > available {
            return false;
        }
        g.reservations.insert(
            op_id,
            Reservation {
                token: token.to_string(),
                account: account.to_string(),
                amount,
                created_at: Instant::now(),
            },
        );
        true
    }

    pub fn release(&self, op_id: u64) {
        let mut g = self.inner.lock();
        g.reservations.remove(&op_id);
    }

    pub fn available_after_reservations(&self, token: &str, account: &str, balance: i128) -> i128 {
        let mut g = self.inner.lock();
        self.expire_locked(&mut g);
        let committed: i128 = g
            .reservations
            .values()
            .filter(|r| r.token == token && r.account == account)
            .map(|r| r.amount)
            .sum();
        balance.saturating_sub(committed)
    }

    /// Returns a cached balance if fresh, else fetches via `chain` and caches.
    pub async fn cached_balance(
        &self,
        chain: &dyn ChainReader,
        token: &str,
        account: &str,
    ) -> anyhow::Result<i128> {
        let key = (token.to_string(), account.to_string());
        if let Some(b) = self.fresh_cached(&key) {
            return Ok(b);
        }
        let value = chain.read_token_balance(token, account).await?;
        let mut g = self.inner.lock();
        g.balances.insert(
            key,
            CachedBalance {
                value,
                fetched_at: Instant::now(),
            },
        );
        Ok(value)
    }

    fn fresh_cached(&self, key: &(String, String)) -> Option<i128> {
        let g = self.inner.lock();
        g.balances
            .get(key)
            .filter(|c| c.fetched_at.elapsed() < self.balance_ttl)
            .map(|c| c.value)
    }

    fn expire_locked(&self, g: &mut LedgerInner) {
        let ttl = self.reservation_ttl;
        g.reservations.retain(|_, r| r.created_at.elapsed() < ttl);
    }
}

pub fn random_op_id() -> u64 {
    use std::time::SystemTime;
    // Simple unique-ish id; collisions are extremely unlikely in practice and
    // would only cause one reservation to overwrite another (still safe).
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 32) ^ rand_seed()
}

fn rand_seed() -> u64 {
    // Cheap thread-local-ish entropy — XOR with a counter to avoid same-nanos
    // collisions when multiple reservations fire in the same instant.
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    C.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        engine::{
            lending::{MarketData, Obligation, ObligationKey},
            reactor::BoxFuture,
        },
        std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct StubChain {
        balances: Mutex<Vec<i128>>,
        calls: AtomicUsize,
    }
    impl StubChain {
        fn new(seq: Vec<i128>) -> Self {
            Self {
                balances: Mutex::new(seq),
                calls: AtomicUsize::new(0),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }
    impl ChainReader for StubChain {
        fn read_market_data<'a>(&'a self, _: &'a str) -> BoxFuture<'a, anyhow::Result<MarketData>> {
            Box::pin(async { unreachable!() })
        }
        fn read_user_obligation<'a>(
            &'a self,
            _: &'a str,
            _: &'a ObligationKey,
        ) -> BoxFuture<'a, anyhow::Result<Obligation>> {
            Box::pin(async { unreachable!() })
        }
        fn read_all_obligation_keys<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<Vec<ObligationKey>>> {
            Box::pin(async { unreachable!() })
        }
        fn read_token_balance<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let mut g = self.balances.lock();
                Ok(if g.len() > 1 { g.remove(0) } else { g[0] })
            })
        }
        fn quote_amount_out<'a>(
            &'a self,
            _: &'a str,
            _: i128,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<i128>> {
            Box::pin(async { unreachable!() })
        }
        fn router_quote_out<'a>(
            &'a self,
            _: &'a str,
            _: i128,
            _: &'a [&'a str],
        ) -> BoxFuture<'a, anyhow::Result<Vec<i128>>> {
            Box::pin(async { unreachable!() })
        }
        fn router_quote_in<'a>(
            &'a self,
            _: &'a str,
            _: i128,
            _: &'a [&'a str],
        ) -> BoxFuture<'a, anyhow::Result<Vec<i128>>> {
            Box::pin(async { unreachable!() })
        }
        fn simulate_liquidation<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a ObligationKey,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, anyhow::Result<bool>> {
            Box::pin(async { unreachable!() })
        }
    }

    #[test]
    fn reserve_up_to_balance_succeeds() {
        let l = CapitalLedger::new();
        assert!(l.reserve(1, "T", "A", 60, 100));
        assert!(l.reserve(2, "T", "A", 40, 100));
        assert_eq!(l.available_after_reservations("T", "A", 100), 0);
    }

    #[test]
    fn reserve_over_balance_fails() {
        let l = CapitalLedger::new();
        assert!(l.reserve(1, "T", "A", 70, 100));
        assert!(!l.reserve(2, "T", "A", 31, 100));
        // failed reservation must not have committed
        assert_eq!(l.available_after_reservations("T", "A", 100), 30);
    }

    #[test]
    fn reserve_then_release_returns_capacity() {
        let l = CapitalLedger::new();
        assert!(l.reserve(1, "T", "A", 80, 100));
        assert!(!l.reserve(2, "T", "A", 30, 100));
        l.release(1);
        assert!(l.reserve(3, "T", "A", 80, 100));
    }

    #[test]
    fn parallel_reservations_interleave_correctly() {
        let l = Arc::new(CapitalLedger::new());
        let mut handles = Vec::new();
        // 10 threads each try to reserve 15 against a balance of 100; only
        // 6 should succeed (sum 90 ≤ 100, 7th would push to 105).
        for i in 0..10u64 {
            let l = l.clone();
            handles.push(std::thread::spawn(move || {
                l.reserve(i + 1, "T", "A", 15, 100)
            }));
        }
        let mut successes = 0;
        for h in handles {
            if h.join().unwrap() {
                successes += 1;
            }
        }
        let _ = successes;
        // Re-run cleanly: read state from ledger.
        let avail = l.available_after_reservations("T", "A", 100);
        let used = 100 - avail;
        assert!(used <= 100, "should never overcommit");
        assert_eq!(used % 15, 0, "either 0 or multiple-of-15 reserved");
        assert!(used >= 90, "should pack at least 6 reservations of 15");
    }

    #[tokio::test]
    async fn balance_cache_ttl_refresh() {
        let chain = Arc::new(StubChain::new(vec![100, 200]));
        let l = CapitalLedger::with_ttls(Duration::from_secs(30), Duration::from_millis(50));
        let v1 = l.cached_balance(&*chain, "T", "A").await.unwrap();
        let v2 = l.cached_balance(&*chain, "T", "A").await.unwrap();
        assert_eq!(v1, 100);
        assert_eq!(v2, 100, "second call inside TTL should be cached");
        assert_eq!(chain.call_count(), 1);
        std::thread::sleep(Duration::from_millis(70));
        let v3 = l.cached_balance(&*chain, "T", "A").await.unwrap();
        assert_eq!(v3, 200, "after TTL the new value is fetched");
        assert_eq!(chain.call_count(), 2);
    }
}
