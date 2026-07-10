use std::{path::Path, sync::Arc};

use parking_lot::Mutex;
use rusqlite::Connection;

use crate::storage::{cursor::CursorRepo, obligations::ObligationsRepo};

pub mod cursor;
pub mod obligations;

/// Shared handle to the keeper's local SQLite database.
#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    /// Open (or create) the SQLite database at `path`, enable WAL, and apply
    /// the schema. Safe to call repeatedly — the schema uses `IF NOT EXISTS`.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(include_str!("schema.sql"))?;

        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub fn obligations(&self) -> ObligationsRepo {
        ObligationsRepo::new(self.conn.clone())
    }

    pub fn cursor(&self) -> CursorRepo {
        CursorRepo::new(self.conn.clone())
    }
}
