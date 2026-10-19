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
    Clause, Direction, Document, MatchClause, MatchMode, NodePattern, OrderDirection, PathLength,
    PathPattern, PatternElement, Projection, ProjectionItems, Query, RelationshipPattern,
    ReturnClause, SetQuantifier, SortItem, Statement, UnionArm,
};

#[cfg(test)]
mod tests {
    use super::{
        BinaryOperator, Clause, CypherDialect, CypherVersion, Document, Expression, Identifier,
        Literal, Projection, ProjectionItems, Query, ReturnClause, Statement,
    };

    #[test]
    fn identifiers_are_owned_normalized_values() {
        let identifier = Identifier::new("display name");

        assert_eq!(identifier.as_str(), "display name");
        assert_eq!(identifier.to_string(), "display name");
    }

    #[test]
    fn core_query_construction_has_no_parser_types() {
        let document = Document::new(
            CypherDialect::Neo4j(CypherVersion::V25),
            vec![Statement::Query(Query {
                clauses: vec![Clause::Return(ReturnClause {
                    quantifier: None,
                    items: ProjectionItems::Items(vec![Projection {
                        expression: Expression::Binary {
                            left: Box::new(Expression::Literal(Literal::Integer(1))),
                            operator: BinaryOperator::Add,
                            right: Box::new(Expression::Literal(Literal::Integer(2))),
                        },
                        alias: Some(Identifier::from("total")),
                    }]),
                    group_by: Vec::new(),
                    order_by: Vec::new(),
                    skip: None,
                    limit: None,
                })],
                unions: Vec::new(),
            })],
        );

        assert_eq!(document.statements.len(), 1);
    }
}
