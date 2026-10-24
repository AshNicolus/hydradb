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
    /// The least non-null input under the engine's value order; null when
    /// there is none.
    Min,
    /// The greatest non-null input under the engine's value order; null when
    /// there is none.
    Max,
}

impl AggregateFunction {
    /// The function a Cypher name denotes, matched case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Average),
            "collect" => Some(Self::Collect),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    /// The name EXPLAIN and column naming print.
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Average => "avg",
            Self::Collect => "collect",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

/// A function that reads one row and returns one value, as opposed to an
/// [`AggregateFunction`], which reads a group.
///
/// Kept apart from the aggregates on purpose: a scalar call inside RETURN does
/// not make the projection group, and an aggregate inside a scalar call does.
/// Treating `coalesce` as an aggregate because the lowering's function arm
/// used to recognise only aggregates would group rows that must not be.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScalarFunction {
    /// Internal MATCH-clause trail constraint; not callable by Cypher name.
    RelationshipsUnique,
    /// `coalesce(a, b, ...)`: the first argument that is not null, else null.
    Coalesce,
    /// `toUpper(s)`: the string in upper case, by Unicode case mapping.
    ToUpper,
    /// `toLower(s)`: the string in lower case, by Unicode case mapping.
    ToLower,
    /// `toString(v)`: a value's string spelling.
    ToString,
    /// `toInteger(v)`: a value as an integer, or null when it has none.
    ToInteger,
    /// `size(s)`: the number of characters in a string.
    Size,
}

impl ScalarFunction {
    /// The function a Cypher name denotes, matched case-insensitively as
    /// Cypher function names are.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "coalesce" => Some(Self::Coalesce),
            "toupper" => Some(Self::ToUpper),
            "tolower" => Some(Self::ToLower),
            "tostring" => Some(Self::ToString),
            "tointeger" => Some(Self::ToInteger),
            "size" => Some(Self::Size),
            _ => None,
        }
    }

    /// The name EXPLAIN and column naming print.
    pub fn name(self) -> &'static str {
        match self {
            Self::RelationshipsUnique => "relationships_unique",
            Self::Coalesce => "coalesce",
            Self::ToUpper => "toUpper",
            Self::ToLower => "toLower",
            Self::ToString => "toString",
            Self::ToInteger => "toInteger",
            Self::Size => "size",
        }
    }

    /// Whether the function accepts `count` arguments.
    pub fn accepts_arity(self, count: usize) -> bool {
        match self {
            Self::RelationshipsUnique => true,
            Self::Coalesce => count >= 1,
            Self::ToUpper | Self::ToLower | Self::ToString | Self::ToInteger | Self::Size => {
                count == 1
            }
        }
    }
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
        /// `count(DISTINCT x)`: the group's distinct non-null inputs only.
        distinct: bool,
        expression: Option<Box<LogicalExpression>>,
    },
    /// A scalar function call; see [`ScalarFunction`].
    Function {
        function: ScalarFunction,
        arguments: Vec<LogicalExpression>,
    },
    /// A list literal outside `IN`, such as `[1, n.name]`: a value, evaluated
    /// per row. A list never becomes a stored property, which is why this is
    /// an expression rather than a `ScalarValue` variant.
    List(Vec<LogicalExpression>),
    /// `CASE`; see `hydradb_cypher_ast::Expression::Case`. Evaluated lazily:
    /// only the branch taken is computed.
    Case {
        operand: Option<Box<LogicalExpression>>,
        branches: Vec<(LogicalExpression, LogicalExpression)>,
        default: Option<Box<LogicalExpression>>,
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
    /// One row that binds nothing: where a query that does not open with
    /// MATCH starts. `RETURN 1`, `WITH 1 AS x` and an opening OPTIONAL MATCH
    /// all read from it. Under a correlated input it is that input's row.
    SingleRow,
    /// `UNWIND expression AS binding`: each input row once per element of the
    /// list the expression evaluates to, with `binding` holding the element.
    /// A null list yields no rows and a non-list value one.
    Unwind {
        input: Box<GraphLogicalPlan>,
        expression: LogicalExpression,
        binding: Symbol,
    },
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
        /// `[rs:R*1..3]`: the path's relationships, in walk order, bound to
        /// `rs` as a list value.
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        min_hops: u8,
        /// `None` for an unbounded pattern (`*`, `*2..`): trail semantics end
        /// every walk, and the query's row budgets bound how far it gets.
        max_hops: Option<u8>,
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
