use thiserror::Error;

#[derive(Debug, Error)]
pub enum LendingModelError {
    #[error("Internal error caused by invariant breakage")]
    InternalError,
    #[error("Arithmetic overflow or underflow during calculation")]
    OverOrUnderflow,
}

pub type LMError = LendingModelError;

pub trait MapArithmeticError<T> {
    fn m_ou(self) -> Result<T, LMError>;
}

impl<T> MapArithmeticError<T> for Option<T> {
    fn m_ou(self) -> Result<T, LMError> {
        self.ok_or(LMError::OverOrUnderflow)
    }
}
