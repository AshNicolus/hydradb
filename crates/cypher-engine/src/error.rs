use hydradb_cypher_ast::QueryFailureReason;
use thiserror::Error;

use crate::GraphPlanError;

pub type EngineResult<T> = Result<T, EngineError>;
pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("graph storage error: {message}")]
pub struct StorageError {
    message: String,
}

impl StorageError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Cypher 25 parse error: {0}")]
    Parse(String),
    #[error(transparent)]
    Plan(#[from] GraphPlanError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid graph storage response: {0}")]
    InvalidStorageResponse(String),
    #[error("unsupported graph physical plan: {1}")]
    UnsupportedPlan(QueryFailureReason, String),
}

impl EngineError {
    /// The bucket a query-failure metric files this under, or `None` for a
    /// failure that is not the query's.
    ///
    /// Storage failures reach the engine only because the adapter reports
    /// them through it, as strings: a timeout while reading looks the same as
    /// a missing record. They are not a gap in the query language, so they get
    /// no reason rather than a guess.
    pub fn failure_reason(&self) -> Option<QueryFailureReason> {
        match self {
            Self::Parse(_) => Some(QueryFailureReason::ParseError),
            Self::Plan(error) => Some(error.failure_reason()),
            Self::UnsupportedPlan(reason, _) => Some(*reason),
            Self::Storage(_) | Self::InvalidStorageResponse(_) => None,
        }
    }
}
