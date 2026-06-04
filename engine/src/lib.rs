//! `engine` — the deterministic core of the keeper.
//!
//! * [`lending_model`] — the lending protocol model: pure, sync.
//! * [`reactor`] — a generic event-driven runner.
//! * [`ports`] — trait surface implemented in `keeper`.

pub mod lending_model;
pub mod ports;
pub mod reactor;
