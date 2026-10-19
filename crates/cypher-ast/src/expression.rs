use std::fmt;

use crate::QueryFailureReason;

/// A normalized Cypher identifier without quoting delimiters.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Identifier(Box<str>);

impl Identifier {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for Identifier {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Syntax retained until a typed AST variant is implemented.
///
/// This explicit escape hatch lets parser adapters grow incrementally without
/// leaking their generated context types into the shared AST.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SyntaxFragment {
    text: Box<str>,
    unsupported: Option<QueryFailureReason>,
}

impl SyntaxFragment {
    pub fn new(text: impl Into<Box<str>>) -> Self {
        Self {
            text: text.into(),
            unsupported: None,
        }
    }

    /// A fragment retained because typed lowering stopped inside a clause it
    /// could name, so a caller rejecting it can say which one.
    pub fn unsupported(text: impl Into<Box<str>>, reason: QueryFailureReason) -> Self {
        Self {
            text: text.into(),
            unsupported: Some(reason),
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    /// Where typed lowering stopped, when the adapter could tell.
    pub fn unsupported_reason(&self) -> Option<QueryFailureReason> {
        self.unsupported
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i128),
    /// The original normalized spelling is retained to avoid losing decimal
    /// precision before semantic type analysis.
    Float(Box<str>),
    String(Box<str>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnaryOperator {
    Not,
    Plus,
    Minus,
    /// Postfix `<expression> IS NULL`.
    IsNull,
    /// Postfix `<expression> IS NOT NULL`.
    IsNotNull,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BinaryOperator {
    Or,
    Xor,
    And,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Concatenate,
    StartsWith,
    /// `<expression> IN <list>`. The right side is a list literal or a
    /// parameter bound to a list; it is never an ordinary scalar expression.
    In,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Identifier(Identifier),
    Parameter(Identifier),
    Literal(Literal),
    Property {
        target: Box<Expression>,
        key: Identifier,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<Expression>,
    },
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Function {
        name: Vec<Identifier>,
        distinct: bool,
        arguments: Vec<Expression>,
    },
    List(Vec<Expression>),
    Map(Vec<(Identifier, Expression)>),
    Syntax(SyntaxFragment),
}

impl Expression {
    pub fn syntax(text: impl Into<Box<str>>) -> Self {
        Self::Syntax(SyntaxFragment::new(text))
    }
}
