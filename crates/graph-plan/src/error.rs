use std::{error::Error, fmt};

pub type GraphPlanResult<T> = Result<T, GraphPlanError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPlanError {
    UnsupportedCypher(String),
    InvalidLogicalPlan(String),
    MissingParameter(String),
}

impl fmt::Display for GraphPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCypher(reason) => {
                write!(formatter, "unsupported Cypher AST: {reason}")
            }
            Self::InvalidLogicalPlan(reason) => write!(formatter, "invalid logical plan: {reason}"),
            Self::MissingParameter(name) => write!(formatter, "missing query parameter ${name}"),
        }
    }
}

impl Error for GraphPlanError {}
