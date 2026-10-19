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
pub enum ExpandDirection {
    Outgoing,
    Incoming,
}

impl fmt::Display for ExpandDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
        })
    }
}

/// The direction written in a pattern, which is not the same thing as the
/// direction a storage expansion runs in.
///
/// `ExpandDirection` stays two-valued because it is the storage contract: an
/// edge is stored pointing one way, and the HydraDB adapter answers one
/// orientation per request. Undirected is a plan-level concept that the
/// executor satisfies by running both orientations and merging, so adding a
/// third `ExpandDirection` variant would push a case into the storage adapter
/// that storage cannot answer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PatternDirection {
    Outgoing,
    Incoming,
    Undirected,
}

impl PatternDirection {
    /// The storage expansions that together satisfy this pattern direction.
    pub fn storage_directions(self) -> &'static [ExpandDirection] {
        match self {
            Self::Outgoing => &[ExpandDirection::Outgoing],
            Self::Incoming => &[ExpandDirection::Incoming],
            Self::Undirected => &[ExpandDirection::Outgoing, ExpandDirection::Incoming],
        }
    }
}

impl fmt::Display for PatternDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
            Self::Undirected => "undirected",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl fmt::Display for SortDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalBinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StartsWith,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalUnaryOperator {
    Not,
    Plus,
    Minus,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Collect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExpression {
    Binding(Symbol),
    Identity(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Parameter(Symbol),
    Literal(ScalarValue),
    Aggregate {
        function: AggregateFunction,
        expression: Option<Box<LogicalExpression>>,
    },
    Unary {
        operator: LogicalUnaryOperator,
        expression: Box<LogicalExpression>,
    },
    Binary {
        left: Box<LogicalExpression>,
        operator: LogicalBinaryOperator,
        right: Box<LogicalExpression>,
    },
    /// `<expression> IN <list>`. The candidate list is kept out of
    /// `LogicalExpression` on purpose: a list is not a scalar, and giving
    /// `ScalarValue` a list variant would force every exhaustive match in the
    /// engine and in the HydraDB storage adapter to grow an arm for a value
    /// that can never be stored as a property.
    In {
        expression: Box<LogicalExpression>,
        list: LogicalInList,
    },
}

/// The right-hand side of `IN`, before parameters are resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalInList {
    /// An inline list literal, one entry per element.
    Values(Vec<LogicalExpression>),
    /// A parameter bound to a list. Resolved during physical planning from the
    /// planning context's list parameters.
    Parameter(Symbol),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProjection {
    pub expression: LogicalExpression,
    pub alias: Option<Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSort {
    pub expression: LogicalExpression,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphLogicalPlan {
    Union {
        arms: Vec<GraphLogicalPlan>,
        all: bool,
    },
    NodeScan {
        binding: Symbol,
        labels: Vec<Symbol>,
    },
    Expand {
        input: Box<GraphLogicalPlan>,
        from: Symbol,
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    VariableExpand {
        input: Box<GraphLogicalPlan>,
        from: Symbol,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        min_hops: u8,
        max_hops: u8,
    },
    NaturalJoin {
        left: Box<GraphLogicalPlan>,
        right: Box<GraphLogicalPlan>,
        optional: bool,
    },
    Filter {
        input: Box<GraphLogicalPlan>,
        predicate: LogicalExpression,
    },
    Sort {
        input: Box<GraphLogicalPlan>,
        items: Vec<LogicalSort>,
    },
    Skip {
        input: Box<GraphLogicalPlan>,
        count: LogicalExpression,
    },
    Limit {
        input: Box<GraphLogicalPlan>,
        count: LogicalExpression,
    },
    Project {
        input: Box<GraphLogicalPlan>,
        items: Vec<LogicalProjection>,
        distinct: bool,
        post_sort: Vec<LogicalSort>,
        post_skip: Option<LogicalExpression>,
        post_limit: Option<LogicalExpression>,
    },
}
