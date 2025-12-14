use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Missing `{0}` environment variable")]
    MissingEnvVar(String),

    #[error("Invalid ChainID: `{0}`")]
    InvalidChainID(String),

    #[error("Database error: ${0}")]
    Database(#[from] sqlx::Error),

    #[error("RPC error: {0}")]
    RPCError(String),

    #[error("Invalid address: ${0}")]
    InvalidAddress(String)
}
