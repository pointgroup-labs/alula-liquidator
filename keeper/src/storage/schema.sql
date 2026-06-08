-- SQLite schema for the keeper's local persistence.

CREATE TABLE IF NOT EXISTS obligations (
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
);
