//! Cypher 25 generated-tree adapter.
//!
//! This module is intentionally private: only this ANTLR adapter knows about
//! generated parser types. Everything it returns is owned by
//! `hydradb-cypher-ast`.

use hydradb_cypher_ast::{
    BinaryOperator, Clause, CypherDialect, CypherVersion, Direction, Document, Expression,
    Identifier, Literal, MatchClause, NodePattern, OrderDirection, PathLength, PathPattern,
    PatternElement, Projection, ProjectionItems, Query, QueryFailureReason, RelationshipPattern,
    ReturnClause, SortItem, Statement, SyntaxFragment, UnaryOperator, UnionArm,
};
use std::collections::BTreeSet;

use crate::generated::cypher25_parser::{StatementsContext, ValidatedTreeContext};
use crate::Cypher25ValidatedTree;

pub(super) fn lower_cypher25(source: &str, syntax: &Cypher25ValidatedTree) -> Document {
    // Parsing must happen first: the fallback represents valid, not arbitrary,
    // Cypher text. Keep the generated tree in this module so no generated
    // context type can cross the shared-AST boundary.
    let root = syntax
        .tree()
        .downcast_ref::<StatementsContext<'_, ValidatedTreeContext>>()
        .expect("the Cypher 25 entry rule is statements");
    if root.statement_children().count() != 1 {
        return Document::new(
            CypherDialect::Neo4j(CypherVersion::V25),
            vec![Statement::Syntax(SyntaxFragment::unsupported(
                source,
                QueryFailureReason::UntypedStatement,
            ))],
        );
    }

    let mut parser = Parser::new(source);
    let statement = match parser.parse_match_return() {
        Some(query) => Statement::Query(query),
        None => Statement::Syntax(SyntaxFragment::unsupported(source, parser.stopped_reason())),
    };

    Document::new(CypherDialect::Neo4j(CypherVersion::V25), vec![statement])
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Identifier(String),
    Parameter(String),
    Literal(Literal),
    Keyword(&'static str),
    Punctuation(char),
    ArrowRight,
    ArrowLeft,
    Comparison(BinaryOperator),
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    /// The clause being read, so a failure can be reported against it rather
    /// than as an untyped statement. Set on entering each clause.
    context: QueryFailureReason,
}

impl Parser {
    fn new(source: &str) -> Self {
        Self {
            tokens: tokenize(source),
            cursor: 0,
            context: QueryFailureReason::UntypedStatement,
        }
    }

    /// Which clause typed lowering gave up in.
    ///
    /// A clause keyword at the point of failure names the clause this adapter
    /// does not lower at all -- `UNWIND`, `CREATE`, `CALL` -- and wins over the
    /// clause being read, which only says where the unsupported construct sat.
    /// A tokenizer rejection leaves no tokens and stays untyped.
    fn stopped_reason(&self) -> QueryFailureReason {
        let at = |index: usize| match self.tokens.get(index)? {
            Token::Identifier(word) => QueryFailureReason::for_clause_keyword(word),
            Token::Keyword(word) => QueryFailureReason::for_clause_keyword(word),
            _ => None,
        };
        at(self.cursor).unwrap_or(self.context)
    }

    fn parse_match_return(&mut self) -> Option<Query> {
        let mut query = self.query_arm()?;
        while self.keyword("UNION").is_some() {
            self.context = QueryFailureReason::Union;
            let all = self.keyword("ALL").is_some();
            query.unions.push(UnionArm {
                all,
                query: Box::new(self.query_arm()?),
            });
        }
        if self.next().is_some() {
            return None;
        }
        Some(query)
    }

    fn query_arm(&mut self) -> Option<Query> {
        let mut clauses = Vec::new();
        // Set by a WITH that is the last clause before RETURN: what it kept,
        // and its SKIP/LIMIT. Neither has an AST node of its own -- the typed
        // AST has no WITH clause -- so both are applied to RETURN below.
        let mut trailing_with: Option<(BTreeSet<String>, Option<Expression>, Option<Expression>)> =
            None;
        loop {
            let optional = self.keyword("OPTIONAL").is_some();
            self.keyword("MATCH")?;
            self.context = QueryFailureReason::Pattern;
            let mut patterns = vec![self.path()?];
            while self.punctuation(',').is_some() {
                patterns.push(self.path()?);
            }
            let predicate = if self.keyword("WHERE").is_some() {
                self.context = QueryFailureReason::Where;
                Some(self.expression()?)
            } else {
                None
            };
            clauses.push(Clause::Match(MatchClause {
                optional,
                mode: None,
                patterns,
                predicate,
            }));
            if self.keyword("WITH").is_some() {
                self.context = QueryFailureReason::Return;
                let mut projected = BTreeSet::from([self.identifier()?.to_string()]);
                while self.punctuation(',').is_some() {
                    projected.insert(self.identifier()?.to_string());
                }
                let skip = if self.keyword("SKIP").is_some() {
                    self.context = QueryFailureReason::OrderWindow;
                    Some(self.expression()?)
                } else {
                    None
                };
                let limit = if self.keyword("LIMIT").is_some() {
                    self.context = QueryFailureReason::OrderWindow;
                    Some(self.expression()?)
                } else {
                    None
                };
                let bindings = match_bindings(&clauses);
                if matches!(self.peek(), Some(Token::Keyword("RETURN"))) {
                    // Trailing: may narrow, since nothing after it can
                    // re-bind a dropped name and RETURN is checked against
                    // what it kept.
                    if !projected.is_subset(&bindings) {
                        return None;
                    }
                    trailing_with = Some((projected, skip, limit));
                } else if projected != bindings || skip.is_some() || limit.is_some() {
                    // Mid-query: a window or a dropped binding would change
                    // what the next MATCH sees, and there is no operator for
                    // either.
                    return None;
                }
            }
            if !matches!(
                self.peek(),
                Some(Token::Keyword("MATCH") | Token::Keyword("OPTIONAL"))
            ) {
                break;
            }
        }
        self.keyword("RETURN")?;
        self.context = QueryFailureReason::Return;
        let quantifier = if self.keyword("DISTINCT").is_some() {
            Some(hydradb_cypher_ast::SetQuantifier::Distinct)
        } else {
            None
        };
        let mut projections = vec![self.projection()?];
        while self.punctuation(',').is_some() {
            projections.push(self.projection()?);
        }

        let mut order_by = Vec::new();
        if self.keyword("ORDER").is_some() {
            self.context = QueryFailureReason::OrderWindow;
            self.keyword("BY")?;
            loop {
                let expression = self.expression()?;
                let direction = if self.keyword("ASC").is_some() {
                    Some(OrderDirection::Ascending)
                } else if self.keyword("DESC").is_some() {
                    Some(OrderDirection::Descending)
                } else {
                    None
                };
                order_by.push(SortItem {
                    expression,
                    direction,
                });
                if self.punctuation(',').is_none() {
                    break;
                }
            }
        }

        let skip = if self.keyword("SKIP").is_some() {
            self.context = QueryFailureReason::OrderWindow;
            Some(self.expression()?)
        } else {
            None
        };
        let limit = if self.keyword("LIMIT").is_some() {
            self.context = QueryFailureReason::OrderWindow;
            Some(self.expression()?)
        } else {
            None
        };

        let (skip, limit) = match trailing_with {
            None => (skip, limit),
            Some((visible, with_skip, with_limit)) => {
                self.context = QueryFailureReason::Return;
                let aliases: BTreeSet<String> = projections
                    .iter()
                    .filter_map(|projection| projection.alias.as_ref().map(ToString::to_string))
                    .collect();
                let reads_only_visible = projections
                    .iter()
                    .all(|projection| references_only(&projection.expression, &visible))
                    && order_by.iter().all(|item| {
                        references_only(&item.expression, &visible)
                            || references_only(&item.expression, &aliases)
                    });
                if !reads_only_visible {
                    return None;
                }
                if with_skip.is_none() && with_limit.is_none() {
                    (skip, limit)
                } else {
                    // `WITH x LIMIT n RETURN x.p` is `RETURN x.p LIMIT n` only
                    // when RETURN is a plain per-row projection: DISTINCT,
                    // ORDER BY and aggregates read the whole bounded row set.
                    // A RETURN window of its own would have to be composed
                    // with this one, which needs the values, not expressions.
                    self.context = QueryFailureReason::OrderWindow;
                    if quantifier.is_some()
                        || !order_by.is_empty()
                        || skip.is_some()
                        || limit.is_some()
                        || projections
                            .iter()
                            .any(|projection| contains_aggregate(&projection.expression))
                    {
                        return None;
                    }
                    (with_skip, with_limit)
                }
            }
        };

        clauses.push(Clause::Return(ReturnClause {
            quantifier,
            items: ProjectionItems::Items(projections),
            group_by: Vec::new(),
            order_by,
            skip,
            limit,
        }));
        Some(Query {
            clauses,
            unions: Vec::new(),
        })
    }

    fn path(&mut self) -> Option<PathPattern> {
        let mut elements = vec![PatternElement::Node(self.node()?)];
        while matches!(
            self.peek(),
            Some(Token::ArrowLeft | Token::Punctuation('-'))
        ) {
            // Only the left side is known here. Which direction the hop has
            // depends on what follows the bracket, so that decision is made in
            // `relationship` once the closing side has been read.
            let arrow_on_the_left = self.consume(&Token::ArrowLeft);
            if !arrow_on_the_left {
                self.punctuation('-')?;
            }
            elements.push(PatternElement::Relationship(
                self.relationship(arrow_on_the_left)?,
            ));
            elements.push(PatternElement::Node(self.node()?));
        }
        Some(PathPattern {
            binding: None,
            selector: None,
            elements,
        })
    }

    fn node(&mut self) -> Option<NodePattern> {
        self.punctuation('(')?;
        let variable = self.identifier();
        let mut labels = Vec::new();
        while self.consume(&Token::Punctuation(':')) {
            labels.push(self.identifier()?);
        }
        let properties = if self.peek() == Some(&Token::Punctuation('{')) {
            Some(self.map()?)
        } else {
            None
        };
        self.punctuation(')')?;
        Some(NodePattern {
            variable,
            labels,
            properties,
            predicate: None,
        })
    }

    fn relationship(&mut self, arrow_on_the_left: bool) -> Option<RelationshipPattern> {
        self.punctuation('[')?;
        let variable = self.identifier();
        let mut types = Vec::new();
        while self.consume(&Token::Punctuation(':')) {
            types.push(self.identifier()?);
        }
        let length = if self.punctuation('*').is_some() {
            let first = self.path_length_bound();
            if self.punctuation('.').is_some() {
                self.punctuation('.')?;
                Some(PathLength {
                    minimum: first,
                    maximum: self.path_length_bound(),
                })
            } else {
                Some(PathLength {
                    minimum: first,
                    maximum: first,
                })
            }
        } else {
            None
        };
        let properties = if self.peek() == Some(&Token::Punctuation('{')) {
            Some(self.map()?)
        } else {
            None
        };
        self.punctuation(']')?;

        // The closing side decides the direction: `-[..]->` is outgoing,
        // `<-[..]-` incoming, `-[..]-` undirected, and `<-[..]->` the
        // bidirectional spelling, which means the same as undirected.
        let arrow_on_the_right = self.consume(&Token::ArrowRight);
        if !arrow_on_the_right {
            self.punctuation('-')?;
        }
        let direction = match (arrow_on_the_left, arrow_on_the_right) {
            (false, true) => Direction::Outgoing,
            (true, false) => Direction::Incoming,
            (false, false) => Direction::Undirected,
            (true, true) => Direction::Bidirectional,
        };

        Some(RelationshipPattern {
            variable,
            types,
            direction,
            length,
            properties,
            predicate: None,
        })
    }

    fn path_length_bound(&mut self) -> Option<u8> {
        let Some(Token::Literal(Literal::Integer(value))) = self.peek() else {
            return None;
        };
        let value = u8::try_from(*value).ok()?;
        self.cursor += 1;
        Some(value)
    }

    fn projection(&mut self) -> Option<Projection> {
        let expression = self.expression()?;
        let alias = if self.keyword("AS").is_some() {
            Some(self.identifier()?)
        } else {
            None
        };
        Some(Projection { expression, alias })
    }

    fn expression(&mut self) -> Option<Expression> {
        let mut expression = self.and()?;
        while self.keyword("OR").is_some() {
            expression = Expression::Binary {
                left: Box::new(expression),
                operator: BinaryOperator::Or,
                right: Box::new(self.and()?),
            };
        }
        Some(expression)
    }

    fn and(&mut self) -> Option<Expression> {
        let mut expression = self.not()?;
        while self.keyword("AND").is_some() {
            expression = Expression::Binary {
                left: Box::new(expression),
                operator: BinaryOperator::And,
                right: Box::new(self.not()?),
            };
        }
        Some(expression)
    }

    fn not(&mut self) -> Option<Expression> {
        if self.keyword("NOT").is_some() {
            return Some(Expression::Unary {
                operator: UnaryOperator::Not,
                expression: Box::new(self.not()?),
            });
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Option<Expression> {
        let mut left = self.null_predicate()?;
        let mut comparison = None;
        loop {
            let operator = if let Some(Token::Comparison(operator)) = self.peek().cloned() {
                self.next();
                operator
            } else if self.keyword("STARTS").is_some() {
                self.keyword("WITH")?;
                BinaryOperator::StartsWith
            } else if self.keyword("IN").is_some() {
                BinaryOperator::In
            } else {
                break;
            };
            let right = self.null_predicate()?;
            let next = Expression::Binary {
                left: Box::new(left),
                operator,
                right: Box::new(right.clone()),
            };
            comparison = Some(match comparison {
                None => next,
                Some(previous) => Expression::Binary {
                    left: Box::new(previous),
                    operator: BinaryOperator::And,
                    right: Box::new(next),
                },
            });
            left = right;
        }
        Some(comparison.unwrap_or(left))
    }

    /// `<expression> IS [NOT] NULL`, which binds tighter than a comparison.
    ///
    /// `IS` is not tokenized as a keyword, so it stays usable as a property
    /// key or binding; in this postfix position an identifier could not
    /// otherwise follow an expression, so reading it here is unambiguous.
    fn null_predicate(&mut self) -> Option<Expression> {
        let expression = self.additive()?;
        if !matches!(self.peek(), Some(Token::Identifier(word)) if word.eq_ignore_ascii_case("IS"))
        {
            return Some(expression);
        }
        self.next();
        let operator = if self.keyword("NOT").is_some() {
            UnaryOperator::IsNotNull
        } else {
            UnaryOperator::IsNull
        };
        self.consume(&Token::Literal(Literal::Null))
            .then(|| Expression::Unary {
                operator,
                expression: Box::new(expression),
            })
    }

    fn additive(&mut self) -> Option<Expression> {
        let mut expression = self.multiplicative()?;
        loop {
            let operator = if self.punctuation('+').is_some() {
                BinaryOperator::Add
            } else if self.punctuation('-').is_some() {
                BinaryOperator::Subtract
            } else {
                break;
            };
            expression = Expression::Binary {
                left: Box::new(expression),
                operator,
                right: Box::new(self.multiplicative()?),
            };
        }
        Some(expression)
    }

    fn multiplicative(&mut self) -> Option<Expression> {
        let mut expression = self.atom()?;
        loop {
            let operator = if self.punctuation('*').is_some() {
                BinaryOperator::Multiply
            } else if self.punctuation('/').is_some() {
                BinaryOperator::Divide
            } else if self.punctuation('%').is_some() {
                BinaryOperator::Modulo
            } else {
                break;
            };
            expression = Expression::Binary {
                left: Box::new(expression),
                operator,
                right: Box::new(self.atom()?),
            };
        }
        Some(expression)
    }

    fn atom(&mut self) -> Option<Expression> {
        if self.punctuation('-').is_some() {
            return Some(Expression::Unary {
                operator: UnaryOperator::Minus,
                expression: Box::new(self.atom()?),
            });
        }
        if self.punctuation('+').is_some() {
            return Some(Expression::Unary {
                operator: UnaryOperator::Plus,
                expression: Box::new(self.atom()?),
            });
        }
        if self.punctuation('(').is_some() {
            let expression = self.expression()?;
            self.punctuation(')')?;
            return Some(expression);
        }
        // A list literal. `IN` is the only consumer today, but the AST node is
        // general, so this is parsed wherever an expression is allowed.
        if self.punctuation('[').is_some() {
            let mut elements = Vec::new();
            if self.peek() != Some(&Token::Punctuation(']')) {
                elements.push(self.expression()?);
                while self.punctuation(',').is_some() {
                    elements.push(self.expression()?);
                }
            }
            self.punctuation(']')?;
            return Some(Expression::List(elements));
        }
        let expression = match self.next()? {
            Token::Identifier(value) if self.punctuation('(').is_some() => {
                let distinct = self.keyword("DISTINCT").is_some();
                let mut arguments = Vec::new();
                if self.punctuation('*').is_some() {
                    arguments.push(Expression::Syntax(SyntaxFragment::new("*")));
                } else if self.peek() != Some(&Token::Punctuation(')')) {
                    arguments.push(self.expression()?);
                    while self.punctuation(',').is_some() {
                        arguments.push(self.expression()?);
                    }
                }
                self.punctuation(')')?;
                Expression::Function {
                    name: vec![Identifier::from(value)],
                    distinct,
                    arguments,
                }
            }
            Token::Identifier(value) => Expression::Identifier(Identifier::from(value)),
            Token::Parameter(value) => Expression::Parameter(Identifier::from(value)),
            Token::Literal(value) => Expression::Literal(value),
            _ => return None,
        };

        if self.consume(&Token::Punctuation('.')) {
            let key = self.identifier()?;
            Some(Expression::Property {
                target: Box::new(expression),
                key,
            })
        } else {
            Some(expression)
        }
    }

    fn map(&mut self) -> Option<Expression> {
        self.punctuation('{')?;
        let mut entries = Vec::new();
        if self.punctuation('}').is_some() {
            return Some(Expression::Map(entries));
        }
        loop {
            let key = self.identifier()?;
            self.punctuation(':')?;
            entries.push((key, self.expression()?));
            if self.punctuation('}').is_some() {
                break;
            }
            self.punctuation(',')?;
        }
        Some(Expression::Map(entries))
    }

    fn identifier(&mut self) -> Option<Identifier> {
        match self.peek()? {
            Token::Identifier(value) => {
                let identifier = Identifier::from(value.clone());
                self.cursor += 1;
                Some(identifier)
            }
            _ => None,
        }
    }

    fn keyword(&mut self, expected: &'static str) -> Option<()> {
        match self.peek()? {
            Token::Keyword(actual) if *actual == expected => {
                self.cursor += 1;
                Some(())
            }
            _ => None,
        }
    }

    fn punctuation(&mut self, expected: char) -> Option<()> {
        self.consume(&Token::Punctuation(expected)).then_some(())
    }

    fn consume(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor)?.clone();
        self.cursor += 1;
        Some(token)
    }
}

/// Whether every variable `expression` reads is in `visible`. Property keys,
/// map keys and parameter names are not variables.
fn references_only(expression: &Expression, visible: &BTreeSet<String>) -> bool {
    match expression {
        Expression::Identifier(name) => visible.contains(name.as_str()),
        Expression::Parameter(_) | Expression::Literal(_) => true,
        Expression::Property { target, .. } => references_only(target, visible),
        Expression::Unary { expression, .. } => references_only(expression, visible),
        Expression::Binary { left, right, .. } => {
            references_only(left, visible) && references_only(right, visible)
        }
        Expression::Function { arguments, .. } => arguments
            .iter()
            .all(|argument| references_only(argument, visible)),
        Expression::List(items) => items.iter().all(|item| references_only(item, visible)),
        Expression::Map(entries) => entries
            .iter()
            .all(|(_, value)| references_only(value, visible)),
        // Unknown text: cannot prove what it reads.
        Expression::Syntax(_) => false,
    }
}

fn contains_aggregate(expression: &Expression) -> bool {
    const AGGREGATES: [&str; 8] = [
        "count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp",
    ];
    match expression {
        Expression::Function {
            name, arguments, ..
        } => {
            let is_aggregate = matches!(name.as_slice(), [function]
                if AGGREGATES.iter().any(|aggregate| function.as_str().eq_ignore_ascii_case(aggregate)));
            is_aggregate || arguments.iter().any(contains_aggregate)
        }
        Expression::Property { target, .. } => contains_aggregate(target),
        Expression::Unary { expression, .. } => contains_aggregate(expression),
        Expression::Binary { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::List(items) => items.iter().any(contains_aggregate),
        Expression::Map(entries) => entries.iter().any(|(_, value)| contains_aggregate(value)),
        Expression::Identifier(_)
        | Expression::Parameter(_)
        | Expression::Literal(_)
        | Expression::Syntax(_) => false,
    }
}

fn match_bindings(clauses: &[Clause]) -> BTreeSet<String> {
    clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::Match(matched) => Some(matched),
            Clause::Return(_) | Clause::Syntax(_) => None,
        })
        .flat_map(|matched| &matched.patterns)
        .flat_map(|pattern| &pattern.elements)
        .filter_map(|element| match element {
            PatternElement::Node(node) => node.variable.as_ref(),
            PatternElement::Relationship(relationship) => relationship.variable.as_ref(),
        })
        .map(ToString::to_string)
        .collect()
}

fn tokenize(source: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut input = source.chars().peekable();
    while let Some(character) = input.next() {
        if character.is_whitespace() {
            continue;
        }
        if character == '`' {
            let mut identifier = String::new();
            while let Some(next) = input.next() {
                if next == '`' {
                    if input.peek() == Some(&'`') {
                        input.next();
                        identifier.push('`');
                    } else {
                        break;
                    }
                } else {
                    identifier.push(next);
                }
            }
            tokens.push(Token::Identifier(identifier));
            continue;
        }
        if character == '$' {
            let mut name = String::new();
            while input
                .peek()
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
            {
                name.push(input.next().expect("peeked parameter character"));
            }
            if name.is_empty() {
                return Vec::new();
            }
            tokens.push(Token::Parameter(name));
            continue;
        }
        if character.is_ascii_digit() {
            let mut number = character.to_string();
            while input.peek().is_some_and(|next| next.is_ascii_digit()) {
                number.push(input.next().expect("peeked digit"));
            }
            let is_float = if input.peek() == Some(&'.') {
                let mut lookahead = input.clone();
                lookahead.next();
                lookahead.peek().is_some_and(|next| next.is_ascii_digit())
            } else {
                false
            };
            if is_float {
                number.push(input.next().expect("peeked decimal point"));
                while input.peek().is_some_and(|next| next.is_ascii_digit()) {
                    number.push(input.next().expect("peeked fractional digit"));
                }
                tokens.push(Token::Literal(Literal::Float(number.into())));
            } else if let Ok(value) = number.parse() {
                tokens.push(Token::Literal(Literal::Integer(value)));
            }
            continue;
        }
        if character == '\'' || character == '"' {
            let quote = character;
            let mut value = String::new();
            let mut closed = false;
            while let Some(next) = input.next() {
                if next == quote {
                    if input.peek() == Some(&quote) {
                        input.next();
                        value.push(quote);
                    } else {
                        closed = true;
                        break;
                    }
                } else if next == '\\' {
                    let Some(escaped) = input.next() else {
                        return Vec::new();
                    };
                    value.push(match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        other => other,
                    });
                } else {
                    value.push(next);
                }
            }
            if !closed {
                return Vec::new();
            }
            tokens.push(Token::Literal(Literal::String(value.into())));
            continue;
        }
        if character.is_ascii_alphabetic() || character == '_' {
            let mut word = character.to_string();
            while input
                .peek()
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
            {
                word.push(input.next().expect("peeked identifier character"));
            }
            let uppercase = word.to_ascii_uppercase();
            tokens.push(match uppercase.as_str() {
                "MATCH" => Token::Keyword("MATCH"),
                "OPTIONAL" => Token::Keyword("OPTIONAL"),
                "WHERE" => Token::Keyword("WHERE"),
                "RETURN" => Token::Keyword("RETURN"),
                "DISTINCT" => Token::Keyword("DISTINCT"),
                "UNION" => Token::Keyword("UNION"),
                "ALL" => Token::Keyword("ALL"),
                "AS" => Token::Keyword("AS"),
                "ORDER" => Token::Keyword("ORDER"),
                "BY" => Token::Keyword("BY"),
                "SKIP" => Token::Keyword("SKIP"),
                "LIMIT" => Token::Keyword("LIMIT"),
                "ASC" => Token::Keyword("ASC"),
                "DESC" => Token::Keyword("DESC"),
                "OR" => Token::Keyword("OR"),
                "AND" => Token::Keyword("AND"),
                "NOT" => Token::Keyword("NOT"),
                "STARTS" => Token::Keyword("STARTS"),
                "WITH" => Token::Keyword("WITH"),
                "IN" => Token::Keyword("IN"),
                "TRUE" => Token::Literal(Literal::Boolean(true)),
                "FALSE" => Token::Literal(Literal::Boolean(false)),
                "NULL" => Token::Literal(Literal::Null),
                _ => Token::Identifier(word),
            });
            continue;
        }
        match character {
            '-' if input.peek() == Some(&'>') => {
                input.next();
                tokens.push(Token::ArrowRight);
            }
            '<' if input.peek() == Some(&'-') => {
                input.next();
                tokens.push(Token::ArrowLeft);
            }
            '!' if input.peek() == Some(&'=') => {
                input.next();
                tokens.push(Token::Comparison(BinaryOperator::NotEqual));
            }
            '>' if input.peek() == Some(&'=') => {
                input.next();
                tokens.push(Token::Comparison(BinaryOperator::GreaterThanOrEqual));
            }
            '<' if input.peek() == Some(&'=') => {
                input.next();
                tokens.push(Token::Comparison(BinaryOperator::LessThanOrEqual));
            }
            '<' if input.peek() == Some(&'>') => {
                input.next();
                tokens.push(Token::Comparison(BinaryOperator::NotEqual));
            }
            '>' => tokens.push(Token::Comparison(BinaryOperator::GreaterThan)),
            '<' => tokens.push(Token::Comparison(BinaryOperator::LessThan)),
            '=' => tokens.push(Token::Comparison(BinaryOperator::Equal)),
            punctuation @ ('(' | ')' | '[' | ']' | '{' | '}' | ':' | '.' | ',' | '-' | '+'
            | '*' | '/' | '%') => {
                tokens.push(Token::Punctuation(punctuation));
            }
            _ => return Vec::new(),
        }
    }
    tokens
}
