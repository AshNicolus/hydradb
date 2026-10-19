//! A pure-Rust ANTLR Cypher 25 frontend generated from Neo4j's current grammar.
//!
//! The crate deliberately stops at the syntax boundary:
//!
//! ```text
//! query text -> generated lexer -> generated parser -> validated syntax tree
//!                                                   ---------------------
//!                                                    `parse_cypher25_syntax`
//!                                                          |
//!                                                          v
//!                                                owned shared Cypher AST
//!                                                    `parse_cypher25`
//! ```
//!
//! Semantic lowering into HydraDB's future graph logical IR belongs after the
//! shared-AST boundary. Keeping those stages separate avoids leaking generated
//! parser details into planning or execution.

pub mod generated;

mod lower;

use generated::{
    cypher25_lexer::Cypher25Lexer,
    cypher25_parser::{self, Cypher25Parser},
};

pub use generated::cypher25_parser::{Cypher25ValidatedTree, Cypher25ValidationError};

/// Parse one or more Cypher 25 statements into a syntax-clean, structurally
/// validated generated tree.
///
/// The returned tree provides typed contexts for every production in Neo4j's
/// Cypher 25 grammar. It is the source for the canonical AST builder that will
/// sit between this parser and HydraDB's common graph IR.
pub fn parse_cypher25_syntax(
    source: &str,
) -> Result<Cypher25ValidatedTree, Cypher25ValidationError> {
    cypher25_parser::parse_validated(source, Cypher25Lexer::new, Cypher25Parser::statements)
}

/// Parse Cypher 25 into the parser-independent, owned syntax model.
///
/// The generated syntax tree remains available from [`parse_cypher25_syntax`]
/// for consumers that need full grammar coverage. The typed shared-AST slice
/// currently covers a representative `MATCH`/`WHERE`/`RETURN` query; other
/// valid syntax is retained as an owned [`hydradb_cypher_ast::SyntaxFragment`]
/// while adapter coverage grows.
pub fn parse_cypher25(
    source: &str,
) -> Result<hydradb_cypher_ast::Document, Cypher25ValidationError> {
    let syntax = parse_cypher25_syntax(source)?;
    Ok(lower::lower_cypher25(source, &syntax))
}

/// Compatibility spelling for the former generated-tree-only API.
pub fn parse(source: &str) -> Result<Cypher25ValidatedTree, Cypher25ValidationError> {
    parse_cypher25_syntax(source)
}
