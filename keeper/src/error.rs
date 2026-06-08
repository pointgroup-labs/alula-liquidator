use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeeperError {
    #[error("Internal keepere error caused by invariant breakage")]
    InternalError,
}
