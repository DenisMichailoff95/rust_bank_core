use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("YDB error: {0}")]
    Ydb(#[from] ydb::YdbError),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type CoreResult<T> = Result<T, CoreError>;