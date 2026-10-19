/// The language family that produced an AST.
///
/// Dialect provenance remains at the syntax boundary. Semantic lowering is
/// responsible for erasing it when different spellings have the same meaning.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CypherDialect {
    OpenCypher,
    Neo4j(CypherVersion),
    Other(Box<str>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CypherVersion {
    V5,
    V25,
}
