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

impl From<CoreError> for tonic::Status {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::Ydb(e) => tonic::Status::internal(format!("YDB error: {e}")),
            CoreError::NotFound(m) => tonic::Status::not_found(m),
            CoreError::InvalidArgument(m) => tonic::Status::invalid_argument(m),
            CoreError::Internal(m) => tonic::Status::internal(m),
        }
    }
}