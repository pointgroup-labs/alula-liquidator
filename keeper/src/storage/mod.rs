//! SQLite-backed local state for the keeper.
//!
//! The store is split into two narrow repos:
//! - [`obligations::ObligationsRepo`] — per-market `Obligation` cache.
//! - [`cursor::CursorRepo`] — last seen Soroban event cursor.
//!
//! Both repos share a single `Mutex<Connection>` behind an `Arc`. The DTOs
//! that map sqlite rows are private to each repo file — the engine's domain
//! types stay free of `rusqlite` and `serde` plumbing concerns.

pub mod cursor;
pub mod obligations;

use {
    rusqlite::Connection,
    std::{
        path::Path,
        sync::{Arc, Mutex},
    },
};

pub use {cursor::CursorRepo, obligations::ObligationsRepo};

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
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn obligations(&self) -> ObligationsRepo {
        ObligationsRepo::new(self.conn.clone())
    }

    pub fn cursor(&self) -> CursorRepo {
        CursorRepo::new(self.conn.clone())
    }
}
