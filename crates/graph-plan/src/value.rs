use crate::Symbol;

/// A storage-independent scalar whose exact KV encoding is chosen by HydraDB.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(Box<str>),
    String(Box<str>),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ValueOrigin {
    Literal,
    Parameter(Symbol),
}

/// A planning-time value with provenance retained for safe EXPLAIN output.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BoundValue {
    pub value: ScalarValue,
    pub origin: ValueOrigin,
}
