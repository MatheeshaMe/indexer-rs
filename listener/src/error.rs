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

    #[error("WebSocket error: {0}")]
    #[allow(dead_code)] // Reserved for future WebSocket implementation
    WebSocketError(String),

    #[error("Invalid address: ${0}")]
    InvalidAddress(String),

    #[error("Not saved Error {0}")]
    EVMLog(String),

    #[error("Backfill complete - reached safe head")]
    BackfillComplete,

    #[error("Reorg detected at block {0}")]
    #[allow(dead_code)] // Reserved for future use if needed
    ReorgDetected(u64),

    #[error("Block hash mismatch - possible reorg")]
    #[allow(dead_code)] // Reserved for future use
    BlockHashMismatch,
}
