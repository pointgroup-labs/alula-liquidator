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
    pub cursor_id: String,
    /// Ledger sequence the cursor refers to. Stored as an `i64` in sqlite,
    /// surfaced as `u32` here to match the RPC's ledger-sequence type.
    pub last_event_timestamp: u32,
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
                "SELECT cursor_id, ledger FROM event_cursor WHERE id = 1",
                [],
                |row| {
                    Ok(EventCursor {
                        cursor_id: row.get(0)?,
                        last_event_timestamp: row.get(1)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// Upsert the single-row cursor.
    pub fn set(&self, cursor_id: &str, last_event_timestamp: u32) -> anyhow::Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO event_cursor (id, cursor_id, ledger) VALUES (1, ?1, ?2)",
            params![cursor_id, last_event_timestamp],
        )?;
        Ok(())
    }
}
