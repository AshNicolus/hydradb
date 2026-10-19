use std::{error::Error, fmt};

use hydradb_cypher_ast::QueryFailureReason;

pub type GraphPlanResult<T> = Result<T, GraphPlanError>;

/// The bucket travels beside the message rather than inside it, so the
/// message stays free text for logs and the bucket stays a closed label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPlanError {
    UnsupportedCypher(QueryFailureReason, String),
    InvalidLogicalPlan(QueryFailureReason, String),
    MissingParameter(String),
}

impl GraphPlanError {
    pub fn failure_reason(&self) -> QueryFailureReason {
        match self {
            Self::UnsupportedCypher(reason, _) | Self::InvalidLogicalPlan(reason, _) => *reason,
            Self::MissingParameter(_) => QueryFailureReason::Parameter,
        }
    }
}

impl fmt::Display for GraphPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCypher(_, reason) => {
                write!(formatter, "unsupported Cypher AST: {reason}")
            }
            Self::InvalidLogicalPlan(_, reason) => {
                write!(formatter, "invalid logical plan: {reason}")
            }
            Self::MissingParameter(name) => write!(formatter, "missing query parameter ${name}"),
        }
    }
}

impl Error for GraphPlanError {}
