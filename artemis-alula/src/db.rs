use {
    crate::types::{Obligation, ObligationKey},
    rusqlite::{Connection, OptionalExtension, params},
    std::{collections::HashMap, sync::Mutex},
};

pub struct DbManager {
    conn: Mutex<Connection>,
}

impl DbManager {
    pub fn new(db_path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(db_path)?;

        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS obligations (
                market       TEXT NOT NULL,
                user_address TEXT NOT NULL,
                seed         TEXT NOT NULL DEFAULT '',
                data_json    TEXT NOT NULL,
                PRIMARY KEY (market, user_address, seed)
            );

            CREATE TABLE IF NOT EXISTS event_cursor (
                id        INTEGER PRIMARY KEY CHECK (id = 1),
                cursor_id TEXT NOT NULL,
                ledger    INTEGER NOT NULL
            );",
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Load all obligations for a given market from the database
    pub fn load_obligations(
        &self,
        market: &str,
    ) -> anyhow::Result<HashMap<ObligationKey, Obligation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT user_address, seed, data_json FROM obligations WHERE market = ?1")?;

        let rows = stmt.query_map(params![market], |row| {
            let row: (String, String, String) = (
                row.get(0)?, // user
                row.get(1)?, // seed_raw
                row.get(2)?, // data_json
            );

            Ok(row)
        })?;

        let mut map = HashMap::new();

        for row in rows {
            let (user, seed_raw, data_json) = row?;

            let obligation_key = if seed_raw.is_empty() {
                ObligationKey::new(user)
            } else {
                ObligationKey::new_with_seed(user, seed_raw)
            };
            let obligation: Obligation = serde_json::from_str(&data_json)?;

            map.insert(obligation_key, obligation);
        }

        Ok(map)
    }

    /// Insert or replace an obligation in the database
    pub fn save_obligation(
        &self,
        market: &str,
        key: &ObligationKey,
        obl: &Obligation,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();

        let user = &key.user;
        let seed = key.seed_as_str();
        let data_json = serde_json::to_string(obl)?;

        conn.execute(
            "INSERT OR REPLACE INTO obligations (market, user_address, seed, data_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![market, user, seed, data_json],
        )?;

        Ok(())
    }

    /// Delete an obligation from the database
    pub fn delete_obligation(&self, market: &str, key: &ObligationKey) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();

        let user = &key.user;
        let seed = key.seed_as_str();

        conn.execute(
            "DELETE FROM obligations WHERE market = ?1 AND user_address = ?2 AND seed = ?3",
            params![market, user, seed],
        )?;

        Ok(())
    }

    /// Load the saved event cursor, if any
    pub fn load_cursor(&self) -> anyhow::Result<Option<(String, u32)>> {
        let conn = self.conn.lock().unwrap();

        let opt_row = conn
            .query_row(
                "SELECT cursor_id, ledger FROM event_cursor WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        Ok(opt_row)
    }

    /// Save the event cursor (upserts the single row with id=1)
    pub fn save_cursor(&self, cursor_id: &str, ledger: u32) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO event_cursor (id, cursor_id, ledger) VALUES (1, ?1, ?2)",
            params![cursor_id, ledger],
        )?;

        Ok(())
    }
}
