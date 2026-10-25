use crate::{CypherDialect, Expression, Identifier, SyntaxFragment};

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub dialect: CypherDialect,
    pub statements: Vec<Statement>,
}

impl Document {
    pub fn new(dialect: CypherDialect, statements: Vec<Statement>) -> Self {
        Self {
            dialect,
            statements,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Query(Query),
    Syntax(SyntaxFragment),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query {
    pub clauses: Vec<Clause>,
    pub unions: Vec<UnionArm>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnionArm {
    pub all: bool,
    pub query: Box<Query>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Clause {
    Match(MatchClause),
    With(WithClause),
    Unwind(UnwindClause),
    Let(LetClause),
    Filter(FilterClause),
    Return(ReturnClause),
    Syntax(SyntaxFragment),
}

/// A `WITH` clause: a projection that is also a scope boundary.
///
/// Carries everything [`ReturnClause`] does, because `WITH` projects the same
/// way `RETURN` does, plus the `WHERE` that may follow it - which `RETURN` has
/// no form of. `group_by` is GQL's explicit grouping; without it, grouping
/// follows from the aggregates among `items`, as it does for `RETURN`.
#[derive(Clone, Debug, PartialEq)]
pub struct WithClause {
    pub quantifier: Option<SetQuantifier>,
    pub items: ProjectionItems,
    pub group_by: Vec<Expression>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
    pub predicate: Option<Expression>,
}

/// GQL's `LET a = <expression>, ...`: new names for the rest of the query,
/// each computed from what was in scope before the clause.
#[derive(Clone, Debug, PartialEq)]
pub struct LetClause {
    pub bindings: Vec<(Identifier, Expression)>,
}

/// GQL's `FILTER [WHERE] <predicate>`: keep the rows the predicate holds for.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterClause {
    pub predicate: Expression,
}

/// `UNWIND <expression> AS <variable>`: one row per element of a list.
#[derive(Clone, Debug, PartialEq)]
pub struct UnwindClause {
    pub expression: Expression,
    pub variable: Identifier,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MatchMode {
    RepeatableElements,
    DifferentRelationships,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MatchClause {
    pub optional: bool,
    pub mode: Option<MatchMode>,
    pub patterns: Vec<PathPattern>,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PathPattern {
    pub binding: Option<Identifier>,
    pub selector: Option<SyntaxFragment>,
    pub elements: Vec<PatternElement>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub variable: Option<Identifier>,
    /// Labels in source order. Complex label expressions not represented by
    /// this initial typed slice remain in [`SyntaxFragment`] elsewhere.
    pub labels: Vec<Identifier>,
    pub properties: Option<Expression>,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
    Bidirectional,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipPattern {
    pub variable: Option<Identifier>,
    /// Relationship types in source order.
    pub types: Vec<Identifier>,
    pub direction: Direction,
    pub length: Option<PathLength>,
    pub properties: Option<Expression>,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PathLength {
    pub minimum: Option<u8>,
    pub maximum: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProjectionItems {
    /// `*`, then any further items: `WITH *, n.score * 10 AS scaled`. The
    /// star stands for every name in scope, which only lowering knows.
    All(Vec<Projection>),
    Items(Vec<Projection>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Projection {
    pub expression: Expression,
    pub alias: Option<Identifier>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortItem {
    pub expression: Expression,
    pub direction: Option<OrderDirection>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReturnClause {
    pub quantifier: Option<SetQuantifier>,
    pub items: ProjectionItems,
    pub group_by: Vec<Expression>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
}
