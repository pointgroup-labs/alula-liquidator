use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeeperError {
    #[error("Internal keeper error caused by invariant breakage")]
    InternalError,
    #[error("Not enough of avaialbe balance to continue the operation")]
    NotEnoughAvailableBalance,
}
