//! Parser-independent Cypher abstract syntax tree.
//!
//! Parser implementations depend on this crate, never the other way around.
//! These types describe Cypher syntax; graph planning and execution semantics
//! belong to the later graph IR.

#![forbid(unsafe_code)]

mod dialect;
mod expression;
mod failure;
mod query;

pub use dialect::{CypherDialect, CypherVersion};
pub use expression::{
    BinaryOperator, Expression, Identifier, Literal, SyntaxFragment, UnaryOperator,
};
pub use failure::QueryFailureReason;
pub use query::{
    Clause, Direction, Document, FilterClause, LetClause, MatchClause, MatchMode, NodePattern,
    OrderDirection, PathLength, PathPattern, PatternElement, Projection, ProjectionItems, Query,
    RelationshipPattern, ReturnClause, SetQuantifier, SortItem, Statement, UnionArm, UnwindClause,
    WithClause,
};
