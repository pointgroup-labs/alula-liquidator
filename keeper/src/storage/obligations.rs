//! Per-market obligation cache.
//!
//! Storage layout: one row per `(market, user_address, seed)`. The
//! `Obligation` itself is stored as JSON in `data_json` so the engine's
//! domain type does not need to know about sqlite — we just round-trip via
//! `serde_json` here.

use std::{collections::HashMap, sync::Arc};

use engine::lending_model::{Obligation, ObligationKey};
use parking_lot::Mutex;
use rusqlite::{Connection, params};

/// Private DTO mirroring a single sqlite row.
struct ObligationRow {
    user: String,
    seed: String,
    data_json: String,
}

pub struct ObligationsRepo {
    conn: Arc<Mutex<Connection>>, // TODO: Unify Arc visibility across the repo
}

impl ObligationsRepo {
    pub(crate) fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Load every obligation persisted for `market`.
    pub fn load_all(&self, market: &str) -> anyhow::Result<HashMap<ObligationKey, Obligation>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT user_address, seed, data_json FROM obligations WHERE market = ?1")?;

        let rows = stmt.query_map(params![market], |row| {
            Ok(ObligationRow { user: row.get(0)?, seed: row.get(1)?, data_json: row.get(2)? })
        })?;

        let mut out = HashMap::new();
        for row in rows {
            let row = row?;
            let key = if row.seed.is_empty() {
                ObligationKey::new(row.user)
            } else {
                ObligationKey::new_with_seed(row.user, row.seed)
            };
            let obligation: Obligation = serde_json::from_str(&row.data_json)?;
            out.insert(key, obligation);
        }

        Ok(out)
    }

    /// Insert or replace an obligation.
    pub fn put(
        &self,
        market: &str,
        key: &ObligationKey,
        obligation: &Obligation,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock();

        let data_json = serde_json::to_string(obligation)?;
        conn.execute(
            "INSERT OR REPLACE INTO obligations (market, user_address, seed, data_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![market, &key.user, key.seed_as_str(), data_json],
        )?;

        Ok(())
    }

    /// Delete an obligation (no-op if absent).
    pub fn delete(&self, market: &str, key: &ObligationKey) -> anyhow::Result<()> {
        let conn = self.conn.lock();

        conn.execute(
            "DELETE FROM obligations WHERE market = ?1 AND user_address = ?2 AND seed = ?3",
            params![market, &key.user, key.seed_as_str()],
        )?;

        Ok(())
    }
}
