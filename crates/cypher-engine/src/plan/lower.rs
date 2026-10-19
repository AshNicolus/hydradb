use std::collections::BTreeSet;

use hydradb_cypher_ast::{
    BinaryOperator, Clause, Direction, Document, Expression, Literal, MatchClause, NodePattern,
    OrderDirection, PathPattern, PatternElement, ProjectionItems, Query, QueryFailureReason,
    Statement, UnaryOperator,
};

use crate::{
    AggregateFunction, GraphLogicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator,
    LogicalExpression, LogicalInList, LogicalProjection, LogicalSort, LogicalUnaryOperator,
    PatternDirection, ScalarValue, SortDirection, Symbol,
};

/// Upper bound on `IN` candidates. A list this long is a caller that should be
/// paging, and an unbounded list would turn one predicate into an unbounded
/// number of index seeks.
pub(crate) const MAX_IN_LIST_VALUES: usize = 1024;

pub fn lower_cypher_ast(document: &Document) -> GraphPlanResult<GraphLogicalPlan> {
    let query = match document.statements.as_slice() {
        [Statement::Query(query)] => query,
        // The message stays the one production logs have always carried; the
        // reason says which clause the Cypher 25 lowering stopped in, which
        // the message alone never could.
        [Statement::Syntax(fragment)] => {
            return Err(GraphPlanError::UnsupportedCypher(
                fragment
                    .unsupported_reason()
                    .unwrap_or(QueryFailureReason::UntypedStatement),
                "expected exactly one typed query statement".to_string(),
            ));
        }
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::UntypedStatement,
                "expected exactly one typed query statement".to_string(),
            ));
        }
    };
    lower_query(query)
}

fn lower_query(query: &Query) -> GraphPlanResult<GraphLogicalPlan> {
    let Some((Clause::Return(returned), matched_clauses)) = query.clauses.split_last() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "expected one or more MATCH clauses followed by RETURN".to_string(),
        ));
    };
    if matched_clauses.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "expected at least one MATCH clause".to_string(),
        ));
    }
    let mut plan = None;
    let mut declared: BTreeSet<Symbol> = BTreeSet::new();
    for clause in matched_clauses {
        let Clause::Match(matched) = clause else {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Other,
                "only MATCH clauses may precede RETURN".to_string(),
            ));
        };
        if matched.mode.is_some() || matched.patterns.is_empty() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "MATCH modes and empty patterns are not lowered yet".to_string(),
            ));
        }
        declared.extend(clause_bindings(matched));
        let mut patterns = matched.patterns.iter();
        let mut clause_plan = lower_path(patterns.next().expect("non-empty patterns"))?;
        for pattern in patterns {
            clause_plan = GraphLogicalPlan::NaturalJoin {
                left: Box::new(clause_plan),
                right: Box::new(lower_path(pattern)?),
                optional: false,
            };
        }
        let predicate = matched
            .predicate
            .as_ref()
            .map(lower_expression)
            .transpose()?;
        if let Some(predicate) = &predicate {
            validate_declared_bindings(predicate, &declared)?;
        }
        // A predicate on an OPTIONAL MATCH belongs to the optional pattern,
        // not to the rows the clause is optional *for*: a row whose optional
        // side fails it keeps its place with that side null, exactly as a row
        // whose pattern never matched does. Filtering after the join would
        // drop it instead, turning "whose optional side is not X" into "which
        // has no optional side at all". An ordinary MATCH is an inner join, so
        // there the two positions agree and the filter stays above it, where
        // the planner already reads it.
        if matched.optional {
            if let Some(predicate) = predicate.clone() {
                clause_plan = GraphLogicalPlan::Filter {
                    input: Box::new(clause_plan),
                    predicate,
                };
            }
        }
        clause_plan = match plan {
            Some(previous) => GraphLogicalPlan::NaturalJoin {
                left: Box::new(previous),
                right: Box::new(clause_plan),
                optional: matched.optional,
            },
            None if matched.optional => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "OPTIONAL MATCH requires an earlier required MATCH".to_string(),
                ));
            }
            None => clause_plan,
        };
        if !matched.optional {
            if let Some(predicate) = predicate {
                clause_plan = GraphLogicalPlan::Filter {
                    input: Box::new(clause_plan),
                    predicate,
                };
            }
        }
        plan = Some(clause_plan);
    }
    let mut plan = plan.expect("at least one MATCH plan");
    if !returned.group_by.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "explicit GROUP BY is not lowered yet".to_string(),
        ));
    }
    let ProjectionItems::Items(items) = &returned.items else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "RETURN * is not lowered yet".to_string(),
        ));
    };
    let items = items
        .iter()
        .map(|item| {
            Ok(LogicalProjection {
                expression: lower_expression(&item.expression)?,
                alias: item
                    .alias
                    .as_ref()
                    .map(|alias| Symbol::from(alias.as_str())),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;

    let sort_items = returned
        .order_by
        .iter()
        .map(|item| {
            Ok(LogicalSort {
                expression: lower_sort_expression(&item.expression, &items)?,
                direction: match item.direction.unwrap_or(OrderDirection::Ascending) {
                    OrderDirection::Ascending => SortDirection::Ascending,
                    OrderDirection::Descending => SortDirection::Descending,
                },
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let aggregates = items
        .iter()
        .any(|item| contains_aggregate(&item.expression));
    if !aggregates && !sort_items.is_empty() {
        plan = GraphLogicalPlan::Sort {
            input: Box::new(plan),
            items: sort_items.clone(),
        };
    }
    if !aggregates {
        if let Some(skip) = &returned.skip {
            plan = GraphLogicalPlan::Skip {
                input: Box::new(plan),
                count: lower_window_expression(skip, "SKIP")?,
            };
        }
    }
    if !aggregates {
        if let Some(limit) = &returned.limit {
            plan = GraphLogicalPlan::Limit {
                input: Box::new(plan),
                count: lower_window_expression(limit, "LIMIT")?,
            };
        }
    }
    let base = GraphLogicalPlan::Project {
        input: Box::new(plan),
        items,
        distinct: matches!(
            returned.quantifier,
            Some(hydradb_cypher_ast::SetQuantifier::Distinct)
        ),
        post_sort: if aggregates { sort_items } else { Vec::new() },
        post_skip: if aggregates {
            returned
                .skip
                .as_ref()
                .map(|skip| lower_window_expression(skip, "SKIP"))
                .transpose()?
        } else {
            None
        },
        post_limit: if aggregates {
            returned
                .limit
                .as_ref()
                .map(|limit| lower_window_expression(limit, "LIMIT"))
                .transpose()?
        } else {
            None
        },
    };
    if query.unions.is_empty() {
        return Ok(base);
    }
    let all = query.unions[0].all;
    if query.unions.iter().any(|arm| arm.all != all) {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Union,
            "a statement cannot mix UNION and UNION ALL".to_string(),
        ));
    }
    let mut arms = Vec::with_capacity(query.unions.len() + 1);
    arms.push(base);
    for arm in &query.unions {
        arms.push(lower_query(&arm.query)?);
    }
    Ok(GraphLogicalPlan::Union { arms, all })
}

/// `<expression> IN <list>`.
///
/// The right side is either a list literal or a parameter that will be bound to
/// a list. A scalar parameter is deliberately still accepted here and resolved
/// as a one-element membership test during physical planning, so a caller that
/// pages an id set down to a single value does not have to rewrite the query.
fn lower_in_expression(
    left: &Expression,
    right: &Expression,
) -> GraphPlanResult<LogicalExpression> {
    let expression = Box::new(lower_expression(left)?);
    let list = match right {
        Expression::List(elements) => {
            if elements.len() > MAX_IN_LIST_VALUES {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Where,
                    format!(
                        "IN list of {} values exceeds the {MAX_IN_LIST_VALUES}-value limit",
                        elements.len()
                    ),
                ));
            }
            LogicalInList::Values(
                elements
                    .iter()
                    .map(lower_expression)
                    .collect::<GraphPlanResult<Vec<_>>>()?,
            )
        }
        Expression::Parameter(parameter) => {
            LogicalInList::Parameter(Symbol::from(parameter.as_str()))
        }
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "IN requires a list literal or a list parameter".to_string(),
            ))
        }
    };
    Ok(LogicalExpression::In { expression, list })
}

fn contains_aggregate(expression: &LogicalExpression) -> bool {
    match expression {
        LogicalExpression::Aggregate { .. } => true,
        LogicalExpression::Unary { expression, .. } => contains_aggregate(expression),
        LogicalExpression::Binary { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        LogicalExpression::In { expression, .. } => contains_aggregate(expression),
        LogicalExpression::Binding(_)
        | LogicalExpression::Identity(_)
        | LogicalExpression::Property { .. }
        | LogicalExpression::Parameter(_)
        | LogicalExpression::Literal(_) => false,
    }
}

fn lower_sort_expression(
    expression: &Expression,
    projections: &[LogicalProjection],
) -> GraphPlanResult<LogicalExpression> {
    if let Expression::Identifier(identifier) = expression {
        if let Some(projection) = projections.iter().find(|projection| {
            projection
                .alias
                .as_ref()
                .is_some_and(|alias| alias.as_str() == identifier.as_str())
        }) {
            return Ok(projection.expression.clone());
        }
    }
    lower_expression(expression)
}

fn lower_window_expression(
    window: &Expression,
    field: &'static str,
) -> GraphPlanResult<LogicalExpression> {
    if is_integer_window_expression(window) {
        lower_expression(window)
    } else {
        Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::OrderWindow,
            format!("{field} supports integer literals, parameters, and constant arithmetic"),
        ))
    }
}

fn is_integer_window_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parameter(_) | Expression::Literal(Literal::Integer(_)) => true,
        Expression::Unary {
            operator: UnaryOperator::Plus | UnaryOperator::Minus,
            expression,
        } => is_integer_window_expression(expression),
        Expression::Binary {
            left,
            operator:
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo,
            right,
        } => is_integer_window_expression(left) && is_integer_window_expression(right),
        _ => false,
    }
}

fn lower_path(path: &PathPattern) -> GraphPlanResult<GraphLogicalPlan> {
    let Some(PatternElement::Node(first)) = path.elements.first() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a path must begin with a node".to_string(),
        ));
    };
    let mut from = node_symbol(first)?;
    let mut plan =
        apply_pattern_properties(lower_node_scan(first)?, &from, first.properties.as_ref())?;
    let mut remaining = &path.elements[1..];
    let mut hop = 0_usize;

    while let [PatternElement::Relationship(relationship), PatternElement::Node(target), tail @ ..] =
        remaining
    {
        if relationship.predicate.is_some() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "relationship-local WHERE predicates are not lowered yet".to_string(),
            ));
        }
        let GraphLogicalPlan::NodeScan {
            binding: to,
            labels: target_labels,
        } = lower_node_scan(target)?
        else {
            unreachable!("node lowering returns a scan")
        };
        if from == to {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "relationship expansion currently requires distinct adjacent node bindings"
                    .to_string(),
            ));
        }
        let relationship_binding = relationship
            .variable
            .as_ref()
            .map(|binding| Symbol::from(binding.as_str()))
            .or_else(|| {
                relationship
                    .properties
                    .as_ref()
                    .map(|_| Symbol::from(format!("__hydradb_inline_relationship_{hop}").as_str()))
            });
        if relationship_binding
            .as_ref()
            .is_some_and(|binding| binding == &from || binding == &to)
        {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "relationship and node bindings must be distinct".to_string(),
            ));
        }
        let direction = match relationship.direction {
            Direction::Outgoing => PatternDirection::Outgoing,
            Direction::Incoming => PatternDirection::Incoming,
            // `-[:R]-` and `<-[:R]->` both mean "either orientation satisfies
            // this hop"; the executor runs both and merges.
            Direction::Undirected | Direction::Bidirectional => PatternDirection::Undirected,
        };
        if matches!(direction, PatternDirection::Undirected) && relationship.length.is_some() {
            // Undirected reachability is a different search: the two
            // orientations compound at every hop, so it cannot be expressed as
            // two runs of the directed traversal. Left explicitly unsupported
            // rather than silently wrong, matching the legacy engine.
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "variable-length undirected relationships are not supported".to_string(),
            ));
        }
        let relationship_types = relationship
            .types
            .iter()
            .map(|relationship_type| Symbol::from(relationship_type.as_str()))
            .collect();
        plan = if let Some(length) = relationship.length {
            if relationship_binding.is_some() || relationship.properties.is_some() {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length relationship bindings and properties are not supported"
                        .to_string(),
                ));
            }
            let min_hops = length.minimum.unwrap_or(1);
            let Some(max_hops) = length.maximum else {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length traversal requires an explicit maximum".to_string(),
                ));
            };
            if min_hops > max_hops {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length traversal minimum exceeds maximum".to_string(),
                ));
            }
            GraphLogicalPlan::VariableExpand {
                input: Box::new(plan),
                from,
                relationship_types,
                direction,
                to: to.clone(),
                target_labels,
                min_hops,
                max_hops,
            }
        } else {
            GraphLogicalPlan::Expand {
                input: Box::new(plan),
                from,
                relationship: relationship_binding.clone(),
                relationship_types,
                direction,
                to: to.clone(),
                target_labels,
            }
        };
        plan = apply_pattern_properties(plan, &to, target.properties.as_ref())?;
        if let Some(properties) = &relationship.properties {
            let binding = relationship_binding
                .as_ref()
                .expect("inline properties synthesize a binding");
            plan = apply_pattern_properties(plan, binding, Some(properties))?;
        }
        from = to;
        remaining = tail;
        hop += 1;
    }
    if !remaining.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "path elements must alternate relationships and nodes".to_string(),
        ));
    }
    Ok(plan)
}

fn lower_node_scan(node: &NodePattern) -> GraphPlanResult<GraphLogicalPlan> {
    if node.predicate.is_some() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            "node-local WHERE predicates are not lowered yet".to_string(),
        ));
    }
    let binding = node_symbol(node)?;
    Ok(GraphLogicalPlan::NodeScan {
        binding,
        labels: node
            .labels
            .iter()
            .map(|label| Symbol::from(label.as_str()))
            .collect(),
    })
}

fn node_symbol(node: &NodePattern) -> GraphPlanResult<Symbol> {
    node.variable
        .as_ref()
        .map(|binding| Symbol::from(binding.as_str()))
        .ok_or_else(|| {
            GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "matched nodes must have bindings".to_string(),
            )
        })
}

fn apply_pattern_properties(
    plan: GraphLogicalPlan,
    binding: &Symbol,
    properties: Option<&Expression>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let Some(properties) = properties else {
        return Ok(plan);
    };
    let Expression::Map(entries) = properties else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "pattern properties must be a map".to_string(),
        ));
    };
    let mut predicate = None;
    for (property, value) in entries {
        let left = if property.as_str() == "id" {
            LogicalExpression::Identity(binding.clone())
        } else {
            LogicalExpression::Property {
                binding: binding.clone(),
                property: Symbol::from(property.as_str()),
            }
        };
        let equality = LogicalExpression::Binary {
            left: Box::new(left),
            operator: LogicalBinaryOperator::Equal,
            right: Box::new(lower_expression(value)?),
        };
        predicate = Some(match predicate {
            None => equality,
            Some(previous) => LogicalExpression::Binary {
                left: Box::new(previous),
                operator: LogicalBinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }
    Ok(match predicate {
        Some(predicate) => GraphLogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    })
}

/// Every binding a MATCH clause declares. A pattern element without a
/// variable declares nothing.
fn clause_bindings(matched: &MatchClause) -> BTreeSet<Symbol> {
    let mut declared = BTreeSet::new();
    for pattern in &matched.patterns {
        for element in &pattern.elements {
            let variable = match element {
                PatternElement::Node(node) => node.variable.as_ref(),
                PatternElement::Relationship(relationship) => relationship.variable.as_ref(),
            };
            declared.extend(variable.map(|variable| Symbol::from(variable.as_str())));
        }
    }
    declared
}

/// Reject a predicate naming a binding no clause has declared. Cypher scopes
/// run left to right, so a name a later clause introduces is not in scope
/// here either. Without this a bare binding reads as null: `WHERE typo IS
/// NULL` would match every row instead of saying the query is wrong.
fn validate_declared_bindings(
    expression: &LogicalExpression,
    declared: &BTreeSet<Symbol>,
) -> GraphPlanResult<()> {
    let check = |binding: &Symbol| -> GraphPlanResult<()> {
        if declared.contains(binding) {
            return Ok(());
        }
        Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            format!("unbound variable {binding}"),
        ))
    };
    match expression {
        LogicalExpression::Binding(binding)
        | LogicalExpression::Identity(binding)
        | LogicalExpression::Property { binding, .. } => check(binding),
        LogicalExpression::Unary { expression, .. } => {
            validate_declared_bindings(expression, declared)
        }
        LogicalExpression::Binary { left, right, .. } => {
            validate_declared_bindings(left, declared)?;
            validate_declared_bindings(right, declared)
        }
        LogicalExpression::Aggregate { expression, .. } => {
            expression.as_deref().map_or(Ok(()), |expression| {
                validate_declared_bindings(expression, declared)
            })
        }
        LogicalExpression::In { expression, list } => {
            validate_declared_bindings(expression, declared)?;
            match list {
                LogicalInList::Values(values) => values
                    .iter()
                    .try_for_each(|value| validate_declared_bindings(value, declared)),
                LogicalInList::Parameter(_) => Ok(()),
            }
        }
        LogicalExpression::Parameter(_) | LogicalExpression::Literal(_) => Ok(()),
    }
}

fn lower_expression(expression: &Expression) -> GraphPlanResult<LogicalExpression> {
    Ok(match expression {
        Expression::Identifier(identifier) => {
            LogicalExpression::Binding(Symbol::from(identifier.as_str()))
        }
        Expression::Property { target, key } => {
            let Expression::Identifier(binding) = target.as_ref() else {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Other,
                    "property targets must be bindings".to_string(),
                ));
            };
            let binding = Symbol::from(binding.as_str());
            if key.as_str() == "id" {
                LogicalExpression::Identity(binding)
            } else {
                LogicalExpression::Property {
                    binding,
                    property: Symbol::from(key.as_str()),
                }
            }
        }
        Expression::Parameter(parameter) => {
            LogicalExpression::Parameter(Symbol::from(parameter.as_str()))
        }
        Expression::Literal(literal) => LogicalExpression::Literal(match literal {
            Literal::Null => ScalarValue::Null,
            Literal::Boolean(value) => ScalarValue::Boolean(*value),
            Literal::Integer(value) => ScalarValue::Integer(*value),
            Literal::Float(value) => ScalarValue::Float(value.clone()),
            Literal::String(value) => ScalarValue::String(value.clone()),
        }),
        Expression::Unary {
            operator,
            expression,
        } => LogicalExpression::Unary {
            operator: match operator {
                UnaryOperator::Not => LogicalUnaryOperator::Not,
                UnaryOperator::Plus => LogicalUnaryOperator::Plus,
                UnaryOperator::Minus => LogicalUnaryOperator::Minus,
                UnaryOperator::IsNull => LogicalUnaryOperator::IsNull,
                UnaryOperator::IsNotNull => LogicalUnaryOperator::IsNotNull,
            },
            expression: Box::new(lower_expression(expression)?),
        },
        // `IN` is matched ahead of the generic binary arm: it lowers to its own
        // node rather than to a binary operator, because its right side is a
        // list rather than a scalar expression.
        Expression::Binary {
            left,
            operator: BinaryOperator::In,
            right,
        } => lower_in_expression(left, right)?,
        Expression::Binary {
            left,
            operator,
            right,
        } => LogicalExpression::Binary {
            left: Box::new(lower_expression(left)?),
            operator: match operator {
                BinaryOperator::Equal => LogicalBinaryOperator::Equal,
                BinaryOperator::NotEqual => LogicalBinaryOperator::NotEqual,
                BinaryOperator::LessThan => LogicalBinaryOperator::LessThan,
                BinaryOperator::LessThanOrEqual => LogicalBinaryOperator::LessThanOrEqual,
                BinaryOperator::GreaterThan => LogicalBinaryOperator::GreaterThan,
                BinaryOperator::GreaterThanOrEqual => LogicalBinaryOperator::GreaterThanOrEqual,
                BinaryOperator::And => LogicalBinaryOperator::And,
                BinaryOperator::Or => LogicalBinaryOperator::Or,
                BinaryOperator::Add => LogicalBinaryOperator::Add,
                BinaryOperator::Subtract => LogicalBinaryOperator::Subtract,
                BinaryOperator::Multiply => LogicalBinaryOperator::Multiply,
                BinaryOperator::Divide => LogicalBinaryOperator::Divide,
                BinaryOperator::Modulo => LogicalBinaryOperator::Modulo,
                BinaryOperator::StartsWith => LogicalBinaryOperator::StartsWith,
                // `In` is handled by the arm above; reaching it here would mean
                // the match order changed.
                BinaryOperator::In
                | BinaryOperator::Xor
                | BinaryOperator::Power
                | BinaryOperator::Concatenate => {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Other,
                        format!("binary operator {operator:?} is not lowered yet"),
                    ))
                }
            },
            right: Box::new(lower_expression(right)?),
        },
        Expression::Function {
            name,
            distinct,
            arguments,
        } => {
            if *distinct || name.len() != 1 {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "distinct or namespaced aggregate functions are not supported".to_string(),
                ));
            }
            let function_name = name[0].as_str().to_ascii_lowercase();
            let function = match function_name.as_str() {
                "count" => AggregateFunction::Count,
                "sum" => AggregateFunction::Sum,
                "avg" => AggregateFunction::Average,
                "collect" => AggregateFunction::Collect,
                _ => {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Return,
                        format!("function {} is not supported", name[0]),
                    ));
                }
            };
            let expression = match arguments.as_slice() {
                [Expression::Syntax(fragment)]
                    if function == AggregateFunction::Count && fragment.text() == "*" =>
                {
                    None
                }
                [argument] => Some(Box::new(lower_expression(argument)?)),
                _ => {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Return,
                        format!("function {} requires exactly one argument", name[0]),
                    ));
                }
            };
            LogicalExpression::Aggregate {
                function,
                expression,
            }
        }
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Other,
                "expression is outside the initial typed slice".to_string(),
            ))
        }
    })
}
