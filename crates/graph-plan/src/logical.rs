use std::fmt;

use crate::ScalarValue;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol(Box<str>);

impl Symbol {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalBinaryOperator {
    Equal,
    Or,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExpression {
    Binding(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Parameter(Symbol),
    Literal(ScalarValue),
    Binary {
        left: Box<LogicalExpression>,
        operator: LogicalBinaryOperator,
        right: Box<LogicalExpression>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProjection {
    pub expression: LogicalExpression,
    pub alias: Option<Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphLogicalPlan {
    NodeScan {
        binding: Symbol,
        labels: Vec<Symbol>,
    },
    Filter {
        input: Box<GraphLogicalPlan>,
        predicate: LogicalExpression,
    },
    Project {
        input: Box<GraphLogicalPlan>,
        items: Vec<LogicalProjection>,
    },
}
