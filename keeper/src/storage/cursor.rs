//! Soroban event-stream cursor persistence.
//!
//! A single row (`id = 1`) holds the last seen cursor id and the ledger it
//! belongs to. The collector reads it on startup to resume from where it
//! left off and writes it after each successful page.

use {
    parking_lot::Mutex,
    rusqlite::{Connection, OptionalExtension, params},
    std::sync::Arc,
};

/// Saved resume position for the Soroban event stream.
#[derive(Debug, Clone)]
pub struct EventCursor {
    pub ledger: u32,
    pub cursor_id: String,
}

pub struct CursorRepo {
    conn: Arc<Mutex<Connection>>,
}

impl CursorRepo {
    pub(crate) fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Load the saved cursor, if any.
    pub fn get(&self) -> anyhow::Result<Option<EventCursor>> {
        let conn = self.conn.lock();
        let row = conn
            .query_row(
                "SELECT ledger, cursor_id FROM event_cursor WHERE id = 1",
                [],
                |row| {
                    Ok(EventCursor {
                        ledger: row.get(1)?,
                        cursor_id: row.get(0)?,
                    })
                },
            )
            .optional()?;

        Ok(row)
    }

    /// Upsert the single-row cursor.
    pub fn set(&self, cursor_id: &str, ledger: u32) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO event_cursor (id, cursor_id, ledger) VALUES (1, ?1, ?2)",
            params![cursor_id, ledger],
        )?;

        Ok(())
    }
}
