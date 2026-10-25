use std::collections::BTreeSet;

use hydradb_cypher_ast::{
    BinaryOperator, Clause, Direction, Document, Expression, Identifier, Literal, MatchClause,
    NodePattern, OrderDirection, PathPattern, PatternElement, Projection, ProjectionItems, Query,
    QueryFailureReason, ReturnClause, SetQuantifier, Statement, UnaryOperator, WithClause,
};

use crate::{
    AggregateFunction, GraphLogicalPlan, GraphPlanError, GraphPlanResult, LogicalBinaryOperator,
    LogicalExpression, LogicalInList, LogicalProjection, LogicalSort, LogicalUnaryOperator,
    PatternDirection, ScalarFunction, ScalarValue, SortDirection, Symbol,
};

/// Upper bound on `IN` candidates. A list this long is a caller that should be
/// paging, and an unbounded list would turn one predicate into an unbounded
/// number of index seeks.
///
/// Matches legacy's ceiling (`src/query/opencypher.rs`). It used to be 1,024,
/// which refused a list of 2,000 that the legacy route answered - the same
/// public failure class on both, so nothing watching error classes could see
/// the difference. What bounds the work is the query's shared index-candidate
/// tally, which the multi-value seek now charges; the list length only bounds
/// how many seeks may be attempted before that tally stops them.
pub(crate) const MAX_IN_LIST_VALUES: usize = 10_000;

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
    lower_query(query).map(resolve_relationship_ids)
}

/// The name `n.id` and `{id: ...}` read.
const ID_PROPERTY: &str = "id";

/// Make `r.id` on a relationship read the relationship's stored `id`
/// property, the identity the client wrote.
///
/// Expression lowering cannot tell a node from a relationship, so it lowers
/// every `x.id` to [`LogicalExpression::Identity`]. For a vertex that is right:
/// a vertex's identity *is* the client's `id`. A relationship's identity is an
/// internal counter the client never sees, and the client's `id` -- what
/// `MERGE (s)-[r:T {id: $h}]->(d)` writes, and what the mutation engine's
/// `MATCH ()-[r:T {id: $h}]->() SET ...` and the legacy reader's `[r:T {id:
/// $h}]` match on -- is stored as an ordinary property named `id`. Reading the
/// counter instead silently answered nothing for `WHERE r.id = $h`.
///
/// Rewritten after lowering, when the plan says which names an `Expand` bound
/// to a relationship and which projections carried them on. The pattern map
/// form already lowers to the property; see [`apply_pattern_properties`].
/// Relationship uniqueness compares bindings, not `.id`, so it is untouched.
///
/// The answer does not depend on this rewrite: execution reads a
/// relationship's `.id` from its stored property however the relationship
/// reached its name, including the UNWIND, list and computed-value forms
/// this pass cannot see. What it buys is the access path -- the planner
/// recognises `WHERE r.id = $h` as a relationship-property seek only once it
/// reads as a property.
fn resolve_relationship_ids(plan: GraphLogicalPlan) -> GraphLogicalPlan {
    relationship_ids_in_plan(plan, &BTreeSet::new()).0
}

/// Rewrite `plan` given the relationship names its correlated input binds, and
/// return it with the relationship names its own rows carry.
fn relationship_ids_in_plan(
    plan: GraphLogicalPlan,
    outer: &BTreeSet<Symbol>,
) -> (GraphLogicalPlan, BTreeSet<Symbol>) {
    match plan {
        plan @ (GraphLogicalPlan::SingleRow | GraphLogicalPlan::NodeScan { .. }) => {
            (plan, outer.clone())
        }
        GraphLogicalPlan::Unwind {
            input,
            expression,
            binding,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let expression = relationship_ids_in_expression(expression, &relationships);
            (
                GraphLogicalPlan::Unwind {
                    input: Box::new(input),
                    expression,
                    binding,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Union { arms, all } => {
            let mut relationships = BTreeSet::new();
            let arms = arms
                .into_iter()
                .map(|arm| {
                    let (arm, carried) = relationship_ids_in_plan(arm, outer);
                    relationships.extend(carried);
                    arm
                })
                .collect();
            (GraphLogicalPlan::Union { arms, all }, relationships)
        }
        GraphLogicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => {
            let (input, mut relationships) = relationship_ids_in_plan(*input, outer);
            relationships.extend(relationship.clone());
            (
                GraphLogicalPlan::Expand {
                    input: Box::new(input),
                    from,
                    relationship,
                    relationship_types,
                    direction,
                    to,
                    target_labels,
                },
                relationships,
            )
        }
        // `rs` of `[rs:R*1..3]` is a list of relationships, not one; `rs.id`
        // reads nothing either way.
        GraphLogicalPlan::VariableExpand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            (
                GraphLogicalPlan::VariableExpand {
                    input: Box::new(input),
                    from,
                    relationship,
                    relationship_types,
                    direction,
                    to,
                    target_labels,
                    min_hops,
                    max_hops,
                },
                relationships,
            )
        }
        // The right side runs once per left row and reads its names.
        GraphLogicalPlan::NaturalJoin {
            left,
            right,
            optional,
        } => {
            let (left, mut relationships) = relationship_ids_in_plan(*left, outer);
            let (right, carried) = relationship_ids_in_plan(*right, &relationships);
            relationships.extend(carried);
            (
                GraphLogicalPlan::NaturalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    optional,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Filter { input, predicate } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let predicate = relationship_ids_in_expression(predicate, &relationships);
            (
                GraphLogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Sort { input, items } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let items = relationship_ids_in_sorts(items, &relationships);
            (
                GraphLogicalPlan::Sort {
                    input: Box::new(input),
                    items,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Skip { input, count } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let count = relationship_ids_in_expression(count, &relationships);
            (
                GraphLogicalPlan::Skip {
                    input: Box::new(input),
                    count,
                },
                relationships,
            )
        }
        GraphLogicalPlan::Limit { input, count } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let count = relationship_ids_in_expression(count, &relationships);
            (
                GraphLogicalPlan::Limit {
                    input: Box::new(input),
                    count,
                },
                relationships,
            )
        }
        // A projection replaces the scope: what it carries on is what a bare
        // relationship binding item names. Its own ORDER BY may read both.
        GraphLogicalPlan::Project {
            input,
            items,
            distinct,
            post_sort,
            post_skip,
            post_limit,
        } => {
            let (input, relationships) = relationship_ids_in_plan(*input, outer);
            let carried = items
                .iter()
                .filter_map(|item| match &item.expression {
                    LogicalExpression::Binding(binding) if relationships.contains(binding) => {
                        Some(item.alias.clone().unwrap_or_else(|| binding.clone()))
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let items = items
                .into_iter()
                .map(|item| LogicalProjection {
                    expression: relationship_ids_in_expression(item.expression, &relationships),
                    alias: item.alias,
                })
                .collect();
            let sortable = relationships
                .union(&carried)
                .cloned()
                .collect::<BTreeSet<_>>();
            (
                GraphLogicalPlan::Project {
                    input: Box::new(input),
                    items,
                    distinct,
                    post_sort: relationship_ids_in_sorts(post_sort, &sortable),
                    post_skip: post_skip
                        .map(|count| relationship_ids_in_expression(count, &sortable)),
                    post_limit: post_limit
                        .map(|count| relationship_ids_in_expression(count, &sortable)),
                },
                carried,
            )
        }
    }
}

fn relationship_ids_in_sorts(
    items: Vec<LogicalSort>,
    relationships: &BTreeSet<Symbol>,
) -> Vec<LogicalSort> {
    items
        .into_iter()
        .map(|item| LogicalSort {
            expression: relationship_ids_in_expression(item.expression, relationships),
            direction: item.direction,
        })
        .collect()
}

fn relationship_ids_in_expression(
    expression: LogicalExpression,
    relationships: &BTreeSet<Symbol>,
) -> LogicalExpression {
    let rewrite =
        |expression: LogicalExpression| relationship_ids_in_expression(expression, relationships);
    let rewrite_box = |expression: Box<LogicalExpression>| {
        Box::new(relationship_ids_in_expression(*expression, relationships))
    };
    match expression {
        LogicalExpression::Identity(binding) if relationships.contains(&binding) => {
            LogicalExpression::Property {
                binding,
                property: Symbol::from(ID_PROPERTY),
            }
        }
        expression @ (LogicalExpression::Binding(_)
        | LogicalExpression::Identity(_)
        | LogicalExpression::Property { .. }
        | LogicalExpression::Parameter(_)
        | LogicalExpression::Literal(_)) => expression,
        LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
        } => LogicalExpression::Aggregate {
            function,
            distinct,
            expression: expression.map(rewrite_box),
        },
        LogicalExpression::Function {
            function,
            arguments,
        } => LogicalExpression::Function {
            function,
            arguments: arguments.into_iter().map(rewrite).collect(),
        },
        LogicalExpression::List(items) => {
            LogicalExpression::List(items.into_iter().map(rewrite).collect())
        }
        LogicalExpression::Case {
            operand,
            branches,
            default,
        } => LogicalExpression::Case {
            operand: operand.map(rewrite_box),
            branches: branches
                .into_iter()
                .map(|(when, then)| (rewrite(when), rewrite(then)))
                .collect(),
            default: default.map(rewrite_box),
        },
        LogicalExpression::Unary {
            operator,
            expression,
        } => LogicalExpression::Unary {
            operator,
            expression: rewrite_box(expression),
        },
        LogicalExpression::Binary {
            left,
            operator,
            right,
        } => LogicalExpression::Binary {
            left: rewrite_box(left),
            operator,
            right: rewrite_box(right),
        },
        LogicalExpression::In { expression, list } => LogicalExpression::In {
            expression: rewrite_box(expression),
            list: match list {
                LogicalInList::Values(values) => {
                    LogicalInList::Values(values.into_iter().map(rewrite).collect())
                }
                list @ LogicalInList::Parameter(_) => list,
            },
        },
    }
}

fn lower_query(query: &Query) -> GraphPlanResult<GraphLogicalPlan> {
    // GQL's explicit GROUP BY is rewritten onto the grouping lowering already
    // does, then lowered as if it had never been written.
    if let Some(rewritten) = without_explicit_grouping(query)? {
        return lower_query(&rewritten);
    }
    let Some((Clause::Return(returned), matched_clauses)) = query.clauses.split_last() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "expected clauses followed by RETURN".to_string(),
        ));
    };
    // `None` until a clause produces rows. A clause that needs input before
    // any has been produced -- WITH, or an OPTIONAL MATCH opening the query --
    // reads `SingleRow`, the one row that binds nothing, as does a query that
    // is only a RETURN.
    let mut plan = None;
    let mut declared: BTreeSet<Symbol> = BTreeSet::new();
    // The names still readable here. Identical to `declared` until a WITH
    // narrows, after which `declared` stays cumulative for the synthesized
    // collision check while this follows the projection boundary.
    let mut visible: BTreeSet<Symbol> = BTreeSet::new();
    // Every expression is checked against the names visible at its boundary.
    // What earlier patterns have produced. Not `declared`, which carries
    // every name the clause mentions before any of it is lowered, so a
    // pattern would find its own bindings already bound and anchor on
    // nothing.
    //
    // An OPTIONAL clause's names belong here too. A miss binds them to null
    // rather than leaving them absent, and a pattern anchored on a null name
    // matches nothing and eliminates the row -- so the anchor is answered on
    // a hit and short-circuits on a miss. Neither case scans.
    let mut bound: BTreeSet<Symbol> = BTreeSet::new();
    // Names `lower_path` synthesized for a relationship carrying inline
    // properties but no variable of its own. Numbered across the query, not
    // per pattern: two patterns each numbering their own hops from zero would
    // both synthesize `__hydradb_inline_relationship_0`, and a row cannot hold
    // one name for two unrelated edges.
    //
    // Kept rather than counted, so the check at the end can compare against
    // the names this query actually produced.
    let mut synthesized: BTreeSet<Symbol> = BTreeSet::new();
    // Names the most recent WITH projected as values rather than bindings. A
    // pattern may read one in a predicate, and may not bind one as a node or a
    // relationship: a scalar is not a graph element, and anchoring a pattern on
    // it silently answers a cross product.
    let mut values: BTreeSet<Symbol> = BTreeSet::new();
    for clause in matched_clauses {
        // GQL's LET and FILTER are the WITH * they mean, lowered by the same
        // path so scope, value tracking and checks are shared.
        let desugared;
        let clause = match clause {
            Clause::Let(binding) => {
                desugared = Clause::With(let_as_with(binding)?);
                &desugared
            }
            Clause::Filter(filter) => {
                validate_filter(filter)?;
                check_scope(&filter.predicate, &visible, "FILTER")?;
                plan = Some(GraphLogicalPlan::Filter {
                    input: Box::new(plan.take().unwrap_or(GraphLogicalPlan::SingleRow)),
                    predicate: lower_expression(&filter.predicate)?,
                });
                continue;
            }
            clause => clause,
        };
        let matched = match clause {
            Clause::Match(matched) => matched,
            // A pass-through WITH contributes nothing to the plan, which is
            // also all legacy does with it. Lowering checks it here rather
            // than the parser discarding it, so the forms that are not
            // pass-through can say what is wrong with them instead of
            // collapsing into "expected exactly one typed query statement".
            Clause::With(with) => {
                let input = plan.take().unwrap_or(GraphLogicalPlan::SingleRow);
                let (projected, projected_scope, projected_values) =
                    lower_with(with, input, &visible, &values)?;
                plan = Some(projected);
                // Cumulative: a name the projection dropped can still collide
                // with a synthesized one, and that check runs over the query.
                declared.extend(projected_scope.iter().cloned());
                // A later pattern may anchor on what the projection produced,
                // and may read nothing else.
                bound = projected_scope.clone();
                visible = projected_scope;
                values = projected_values;
                continue;
            }
            Clause::Unwind(unwind) => {
                let binding = Symbol::from(unwind.variable.as_str());
                // Checked against what is readable here, as any expression is.
                check_scope(&unwind.expression, &visible, "UNWIND")?;
                let expression = lower_expression(&unwind.expression)?;
                validate_declared_bindings(&expression, &visible)?;
                if visible.contains(&binding) {
                    return Err(GraphPlanError::UnsupportedCypher(
                        QueryFailureReason::Unwind,
                        format!("UNWIND cannot rebind {binding}, which is already in scope"),
                    ));
                }
                plan = Some(GraphLogicalPlan::Unwind {
                    input: Box::new(plan.take().unwrap_or(GraphLogicalPlan::SingleRow)),
                    expression,
                    binding: binding.clone(),
                });
                // The element is a value: readable by what follows, and never
                // a node or relationship a later pattern could bind.
                declared.insert(binding.clone());
                visible.insert(binding.clone());
                values.insert(binding);
                continue;
            }
            _ => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Other,
                    "only MATCH, WITH and UNWIND clauses may precede RETURN".to_string(),
                ));
            }
        };
        if matched.mode.is_some() || matched.patterns.is_empty() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                "MATCH modes and empty patterns are not lowered yet".to_string(),
            ));
        }
        // A value the last WITH computed cannot be bound by a pattern. Checked
        // before the clause is lowered, because by the time `lower_path`
        // anchors on the name the mistake looks like an ordinary join.
        if let Some(collision) = clause_bindings(matched).intersection(&values).next() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Pattern,
                format!("{collision} holds a value projected by WITH and cannot also bind a node or relationship"),
            ));
        }
        declared.extend(clause_bindings(matched));
        visible.extend(clause_bindings(matched));
        // Patterns of one clause join unconditionally, so a later pattern may
        // anchor on an earlier one's bindings. Whether the clause as a whole
        // survives to bind anything for the *next* clause is what `optional`
        // decides, below.
        // A predicate written inside a pattern reads the same scope a clause
        // WHERE does, so it is held to the same declared-binding check.
        for predicate in element_predicates(matched) {
            validate_declared_bindings(&lower_expression(predicate)?, &visible)?;
        }
        // Cypher matches a clause's patterns relationship-isomorphically: no
        // relationship is used twice within one MATCH. With two or more
        // hops this can happen across fixed and variable-length paths, so
        // every hop is named and their relationship identities held disjoint.
        let clause_hops = matched
            .patterns
            .iter()
            .flat_map(|path| &path.elements)
            .filter(|element| matches!(element, PatternElement::Relationship(_)))
            .count();
        let name_every_hop = clause_hops >= 2;
        let mut hop_names: Vec<Symbol> = Vec::new();
        let mut clause_bound = bound.clone();
        let mut lowered = None;
        let mut clause_deferred = Vec::new();
        for pattern in &matched.patterns {
            let pattern_plan = lower_path(
                pattern,
                &clause_bound,
                &mut synthesized,
                name_every_hop,
                &mut hop_names,
                &mut clause_deferred,
            )?;
            clause_bound.extend(path_bindings(pattern));
            lowered = Some(match lowered {
                Some(previous) => GraphLogicalPlan::NaturalJoin {
                    left: Box::new(previous),
                    right: Box::new(pattern_plan),
                    optional: false,
                },
                None => pattern_plan,
            });
        }
        bound = clause_bound;
        let mut clause_plan = lowered.expect("non-empty patterns");
        if let Some(distinct) = distinct_relationships(&hop_names) {
            clause_deferred.push(distinct);
        }
        for predicate in clause_deferred {
            clause_plan = GraphLogicalPlan::Filter {
                input: Box::new(clause_plan),
                predicate,
            };
        }
        let predicate = matched
            .predicate
            .as_ref()
            .map(lower_expression)
            .transpose()?;
        if let Some(predicate) = &predicate {
            validate_declared_bindings(predicate, &visible)?;
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
            // An OPTIONAL MATCH that opens the query is optional for the one
            // row nothing has bound: a miss still yields that row, its names
            // null, which is what makes `OPTIONAL MATCH (n:Missing) RETURN n`
            // answer one row rather than none.
            None if matched.optional => GraphLogicalPlan::NaturalJoin {
                left: Box::new(GraphLogicalPlan::SingleRow),
                right: Box::new(clause_plan),
                optional: true,
            },
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
    // Every clause has been read, so both sets are complete: what the caller
    // named, and what the lowering named for it.
    reject_synthesized_collisions(&declared, &synthesized)?;
    let mut plan = plan.unwrap_or(GraphLogicalPlan::SingleRow);
    let items = &star_expanded(&returned.items, &visible, "RETURN")?;
    {
        // Validate every result reference, including initial OPTIONAL MATCH.
        for item in items {
            check_scope(&item.expression, &visible, "RETURN")?;
        }
        // ORDER BY at the result may also name a column the projection just
        // created, which is not a binding at all.
        let mut sortable = visible.clone();
        sortable.extend(item_aliases(items));
        for item in &returned.order_by {
            check_scope(&item.expression, &sortable, "ORDER BY")?;
        }
    }
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
    let distinct = matches!(returned.quantifier, Some(SetQuantifier::Distinct));
    // SKIP and LIMIT count result rows. Aggregation and DISTINCT both change
    // how many rows there are, so their window applies after the projection,
    // never to the binding rows beneath it.
    let window_after_projection = aggregates || distinct;
    if !window_after_projection {
        if let Some(skip) = &returned.skip {
            plan = GraphLogicalPlan::Skip {
                input: Box::new(plan),
                count: lower_window_expression(skip, "SKIP")?,
            };
        }
    }
    if !window_after_projection {
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
        distinct,
        post_sort: if aggregates { sort_items } else { Vec::new() },
        post_skip: if window_after_projection {
            returned
                .skip
                .as_ref()
                .map(|skip| lower_window_expression(skip, "SKIP"))
                .transpose()?
        } else {
            None
        },
        post_limit: if window_after_projection {
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
        LogicalExpression::Function { arguments, .. } | LogicalExpression::List(arguments) => {
            arguments.iter().any(contains_aggregate)
        }
        LogicalExpression::Case { .. } => {
            case_parts(expression).into_iter().any(contains_aggregate)
        }
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

/// Lower a `WITH` into the projection that closes the segment before it.
///
/// This is what makes `WITH` a scope boundary rather than a comment: what it
/// does not project is gone, and what it names is all the rest of the query
/// can see. `ORDER BY`, `SKIP` and `LIMIT` ride on the projection, so they
/// see the projected names rather than the ones it dropped.
/// Lower a WITH boundary.
///
/// Returns the plan, the scope it projects, and which of those names hold a
/// *value* rather than a graph element. The third is not a detail: `WITH e` and
/// `WITH e AS x` pass a node binding through under some name, while
/// `WITH e.rank AS n` computes a scalar, and a later `MATCH (n:Person)` binding
/// that name is asking a number to be a node. Without the distinction the
/// pattern anchors on `n`, finds a scalar where a vertex should be, and answers
/// a cross product instead of refusing.
///
/// `incoming_values` is what the previous boundary decided, because the property
/// travels: an identifier carries through whatever its name already held, so a
/// chain of renames keeps a value a value.
fn lower_with(
    with: &WithClause,
    input: GraphLogicalPlan,
    scope: &BTreeSet<Symbol>,
    incoming_values: &BTreeSet<Symbol>,
) -> GraphPlanResult<(GraphLogicalPlan, BTreeSet<Symbol>, BTreeSet<Symbol>)> {
    let items = &star_expanded(&with.items, scope, "WITH")?;

    let mut projected_scope = BTreeSet::new();
    let mut projected_values = BTreeSet::new();
    let mut projections = Vec::with_capacity(items.len());
    for item in items {
        check_scope(&item.expression, scope, "WITH")?;
        let name = match (&item.alias, &item.expression) {
            (Some(alias), _) => Symbol::from(alias.as_str()),
            (None, Expression::Identifier(binding)) => Symbol::from(binding.as_str()),
            (None, _) => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "a computed WITH item must be given a name with AS".to_string(),
                ));
            }
        };
        if !projected_scope.insert(name.clone()) {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Return,
                format!("WITH projects {name} more than once"),
            ));
        }
        // A bare identifier carries through whatever the name already held, so
        // renaming a value leaves it a value: `WITH e.score AS n WITH n AS x`
        // makes `x` a scalar just as surely as `n`. Reading only the expression
        // shape would call `x` a binding and let a later pattern bind it.
        // Anything other than an identifier computes a value outright.
        let carries_value = match &item.expression {
            Expression::Identifier(binding) => {
                incoming_values.contains(&Symbol::from(binding.as_str()))
            }
            _ => true,
        };
        if carries_value {
            projected_values.insert(name.clone());
        }
        projections.push(LogicalProjection {
            expression: lower_expression(&item.expression)?,
            alias: Some(name),
        });
    }

    let mut post_sort = Vec::with_capacity(with.order_by.len());
    for item in &with.order_by {
        check_scope(&item.expression, &projected_scope, "ORDER BY after WITH")?;
        post_sort.push(LogicalSort {
            expression: lower_expression(&item.expression)?,
            direction: match item.direction.unwrap_or(OrderDirection::Ascending) {
                OrderDirection::Ascending => SortDirection::Ascending,
                OrderDirection::Descending => SortDirection::Descending,
            },
        });
    }

    let plan = GraphLogicalPlan::Project {
        input: Box::new(input),
        items: projections,
        distinct: with.quantifier.is_some(),
        post_sort,
        post_skip: with
            .skip
            .as_ref()
            .map(|skip| lower_window_expression(skip, "SKIP"))
            .transpose()?,
        post_limit: with
            .limit
            .as_ref()
            .map(|limit| lower_window_expression(limit, "LIMIT"))
            .transpose()?,
    };

    let plan = match &with.predicate {
        Some(predicate) => {
            check_scope(predicate, &projected_scope, "WHERE after WITH")?;
            GraphLogicalPlan::Filter {
                input: Box::new(plan),
                predicate: lower_expression(predicate)?,
            }
        }
        None => plan,
    };
    Ok((plan, projected_scope, projected_values))
}

/// How an explicit GROUP BY maps onto the grouping lowering already does.
enum ExplicitGrouping {
    /// Every key is a returned item and something aggregates: implicit
    /// grouping by the non-aggregate items forms the same groups.
    Implicit,
    /// Every key is a returned item and nothing aggregates: grouping is then
    /// deduplication of the rows.
    Distinct,
    /// Some keys are not returned. They are projected under reserved names by
    /// an inner WITH, which groups by them, and an outer projection drops them.
    Hidden(Vec<Expression>),
}

/// The prefix hidden GROUP BY keys are projected under; see
/// [`ExplicitGrouping::Hidden`].
const GROUP_KEY_PREFIX: &str = "__hydradb_group_";

/// Classify an explicit GROUP BY against the items it groups.
///
/// A key naming an item's alias stands for that item's expression. A
/// non-aggregate item must be a key or be computed from keys that are bare
/// names, as Neo4j requires: `RETURN x AS k, x * 2 AS d ... GROUP BY k` is
/// grouped, `RETURN x AS k ... GROUP BY x + 1` is refused.
fn explicit_grouping(
    items: &[Projection],
    keys: &[Expression],
    clause: &str,
) -> GraphPlanResult<ExplicitGrouping> {
    let mut resolved: Vec<Expression> = Vec::with_capacity(keys.len());
    for key in keys {
        let key = match key {
            Expression::Identifier(name) => items
                .iter()
                .find(|item| item.alias.as_ref() == Some(name))
                .map_or_else(|| key.clone(), |item| item.expression.clone()),
            _ => key.clone(),
        };
        if !resolved.contains(&key) {
            resolved.push(key);
        }
    }
    let key_names: BTreeSet<Symbol> = resolved
        .iter()
        .filter_map(|key| match key {
            Expression::Identifier(name) => Some(Symbol::from(name.as_str())),
            _ => None,
        })
        .collect();
    let aggregates = items
        .iter()
        .any(|item| ast_contains_aggregate(&item.expression));
    for item in items
        .iter()
        .filter(|item| !ast_contains_aggregate(&item.expression))
    {
        if resolved.contains(&item.expression) {
            continue;
        }
        let derived =
            expression_bindings(&item.expression).is_some_and(|reads| reads.is_subset(&key_names));
        if !derived {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Return,
                format!("{clause} item is neither a GROUP BY key nor computed from one"),
            ));
        }
    }
    let hidden: Vec<Expression> = resolved
        .into_iter()
        .filter(|key| {
            !items.iter().any(|item| &item.expression == key)
                || expression_bindings(key).is_some_and(|reads| reads.is_empty())
        })
        .collect();
    Ok(match (hidden.is_empty(), aggregates) {
        (true, true) => ExplicitGrouping::Implicit,
        (true, false) => ExplicitGrouping::Distinct,
        (false, _) => ExplicitGrouping::Hidden(hidden),
    })
}

/// Whether an AST expression calls an aggregate anywhere.
fn ast_contains_aggregate(expression: &Expression) -> bool {
    match expression {
        Expression::Function {
            name, arguments, ..
        } => {
            matches!(name.as_slice(), [name] if AggregateFunction::from_name(name.as_str()).is_some())
                || arguments.iter().any(ast_contains_aggregate)
        }
        Expression::Property { target, .. } => ast_contains_aggregate(target),
        Expression::Unary { expression, .. } => ast_contains_aggregate(expression),
        Expression::Binary { left, right, .. } => {
            ast_contains_aggregate(left) || ast_contains_aggregate(right)
        }
        Expression::List(items) => items.iter().any(ast_contains_aggregate),
        Expression::Case {
            operand,
            branches,
            default,
        } => {
            operand.as_deref().is_some_and(ast_contains_aggregate)
                || branches.iter().any(|(when, then)| {
                    ast_contains_aggregate(when) || ast_contains_aggregate(then)
                })
                || default.as_deref().is_some_and(ast_contains_aggregate)
        }
        Expression::Map(entries) => entries
            .iter()
            .any(|(_, value)| ast_contains_aggregate(value)),
        Expression::Identifier(_)
        | Expression::Parameter(_)
        | Expression::Literal(_)
        | Expression::Syntax(_) => false,
    }
}

/// The inner and outer item lists for hidden keys: the inner projects every
/// key and every item under a name, the outer keeps only the items' names.
struct HiddenGroupItems {
    input: Vec<Projection>,
    inner: Vec<Projection>,
    outer: Vec<Projection>,
    replacements: Vec<(Expression, Identifier)>,
}

fn hidden_key_items(
    items: &[Projection],
    hidden: Vec<Expression>,
    clause: &str,
    used: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<HiddenGroupItems> {
    let mut input = Vec::new();
    let mut inner = Vec::new();
    let mut replacements = Vec::new();
    for key in hidden {
        let mut index = 0;
        let name = loop {
            let candidate = Symbol::from(format!("{GROUP_KEY_PREFIX}{index}").as_str());
            if used.insert(candidate.clone()) {
                break Identifier::from(candidate.as_str());
            }
            index += 1;
        };
        // Bind each key before aggregation. Even a constant is then an actual
        // input-row key, so empty input forms zero groups, not a global group.
        input.push(Projection {
            expression: key.clone(),
            alias: Some(name.clone()),
        });
        inner.push(Projection {
            expression: Expression::Identifier(name.clone()),
            alias: Some(name.clone()),
        });
        replacements.push((key, name));
    }
    let mut outer = Vec::with_capacity(items.len());
    for item in items {
        let name = match (&item.alias, &item.expression) {
            (Some(alias), _) => alias.clone(),
            (None, Expression::Identifier(name)) => name.clone(),
            (None, _) => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    format!("a computed {clause} item must be named with AS when grouping by a key it does not return"),
                ));
            }
        };
        inner.push(Projection {
            expression: item.expression.clone(),
            alias: Some(name.clone()),
        });
        outer.push(Projection {
            expression: Expression::Identifier(name.clone()),
            alias: Some(name),
        });
    }
    Ok(HiddenGroupItems {
        input,
        inner,
        outer,
        replacements,
    })
}

// ORDER BY can still read hidden keys after grouping. Rewrite their entire
// expression (including compound sort expressions) to the retained binding.
fn replace_group_keys(
    expression: &Expression,
    replacements: &[(Expression, Identifier)],
) -> Expression {
    if let Some((_, name)) = replacements.iter().find(|(key, _)| key == expression) {
        return Expression::Identifier(name.clone());
    }
    match expression {
        Expression::Unary {
            operator,
            expression,
        } => Expression::Unary {
            operator: *operator,
            expression: Box::new(replace_group_keys(expression, replacements)),
        },
        Expression::Binary {
            left,
            operator,
            right,
        } => Expression::Binary {
            left: Box::new(replace_group_keys(left, replacements)),
            operator: *operator,
            right: Box::new(replace_group_keys(right, replacements)),
        },
        Expression::Function {
            name,
            distinct,
            arguments,
        } => Expression::Function {
            name: name.clone(),
            distinct: *distinct,
            arguments: arguments
                .iter()
                .map(|a| replace_group_keys(a, replacements))
                .collect(),
        },
        Expression::List(items) => Expression::List(
            items
                .iter()
                .map(|a| replace_group_keys(a, replacements))
                .collect(),
        ),
        Expression::Property { target, key } => Expression::Property {
            target: Box::new(replace_group_keys(target, replacements)),
            key: key.clone(),
        },
        Expression::Map(entries) => Expression::Map(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), replace_group_keys(value, replacements)))
                .collect(),
        ),
        Expression::Case {
            operand,
            branches,
            default,
        } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(replace_group_keys(e, replacements))),
            branches: branches
                .iter()
                .map(|(when, then)| {
                    (
                        replace_group_keys(when, replacements),
                        replace_group_keys(then, replacements),
                    )
                })
                .collect(),
            default: default
                .as_ref()
                .map(|e| Box::new(replace_group_keys(e, replacements))),
        },
        _ => expression.clone(),
    }
}

fn group_input(input: Vec<Projection>) -> Clause {
    Clause::With(WithClause {
        quantifier: None,
        items: ProjectionItems::All(input),
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate: None,
    })
}

fn reserved_query_names(query: &Query) -> BTreeSet<Symbol> {
    let mut names = BTreeSet::new();
    for clause in &query.clauses {
        let expressions: Vec<&Expression> = match clause {
            Clause::With(with) => with
                .group_by
                .iter()
                .chain(with.order_by.iter().map(|s| &s.expression))
                .chain(with.predicate.iter())
                .collect(),
            Clause::Return(returned) => returned
                .group_by
                .iter()
                .chain(returned.order_by.iter().map(|s| &s.expression))
                .collect(),
            Clause::Unwind(unwind) => vec![&unwind.expression],
            Clause::Let(let_clause) => let_clause
                .bindings
                .iter()
                .map(|(_, expression)| expression)
                .collect(),
            Clause::Filter(filter) => vec![&filter.predicate],
            Clause::Match(matched) => matched
                .predicate
                .iter()
                .chain(element_predicates(matched))
                .collect(),
            Clause::Syntax(_) => Vec::new(),
        };
        for expression in expressions {
            if let Some(reads) = expression_bindings(expression) {
                names.extend(reads);
            }
        }
        let items = match clause {
            Clause::Match(matched) => {
                names.extend(clause_bindings(matched));
                continue;
            }
            Clause::Unwind(unwind) => {
                names.insert(Symbol::from(unwind.variable.as_str()));
                continue;
            }
            Clause::Let(let_clause) => {
                names.extend(
                    let_clause
                        .bindings
                        .iter()
                        .map(|(name, _)| Symbol::from(name.as_str())),
                );
                continue;
            }
            Clause::Filter(_) => continue,
            Clause::With(with) => &with.items,
            Clause::Return(returned) => &returned.items,
            Clause::Syntax(_) => continue,
        };
        let (ProjectionItems::Items(items) | ProjectionItems::All(items)) = items;
        for item in items {
            if let Some(alias) = &item.alias {
                names.insert(Symbol::from(alias.as_str()));
            }
            if let Some(reads) = expression_bindings(&item.expression) {
                names.extend(reads);
            }
        }
    }
    names
}

/// The inner WITH of a hidden-key rewrite: projection and grouping only.
fn grouping_with(items: Vec<Projection>, aggregates: bool) -> Clause {
    Clause::With(WithClause {
        // With nothing aggregating, the groups are the distinct rows.
        quantifier: (!aggregates).then_some(SetQuantifier::Distinct),
        items: ProjectionItems::Items(items),
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate: None,
    })
}

/// `query` with every explicit GROUP BY rewritten away, or `None` when it has
/// none. Unions keep their arms; each arm is rewritten when it is lowered.
fn without_explicit_grouping(query: &Query) -> GraphPlanResult<Option<Query>> {
    let grouped = query.clauses.iter().any(|clause| match clause {
        Clause::With(with) => !with.group_by.is_empty(),
        Clause::Return(returned) => !returned.group_by.is_empty(),
        _ => false,
    });
    if !grouped {
        return Ok(None);
    }
    let star_refused = |clause: &str| {
        GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("GROUP BY with {clause} * is not supported"),
        )
    };
    let mut used = reserved_query_names(query);
    let mut clauses = Vec::with_capacity(query.clauses.len() + 1);
    for clause in &query.clauses {
        match clause {
            Clause::With(with) if !with.group_by.is_empty() => {
                let ProjectionItems::Items(items) = &with.items else {
                    return Err(star_refused("WITH"));
                };
                match explicit_grouping(items, &with.group_by, "WITH")? {
                    ExplicitGrouping::Implicit => clauses.push(Clause::With(WithClause {
                        group_by: Vec::new(),
                        ..with.clone()
                    })),
                    ExplicitGrouping::Distinct => clauses.push(Clause::With(WithClause {
                        quantifier: Some(SetQuantifier::Distinct),
                        group_by: Vec::new(),
                        ..with.clone()
                    })),
                    ExplicitGrouping::Hidden(hidden) => {
                        let aggregates = items
                            .iter()
                            .any(|item| ast_contains_aggregate(&item.expression));
                        let hidden = hidden_key_items(items, hidden, "WITH", &mut used)?;
                        clauses.push(group_input(hidden.input));
                        clauses.push(grouping_with(hidden.inner, aggregates));
                        let mut projected = hidden.outer.clone();
                        let mut with = with.clone();
                        for sort in &mut with.order_by {
                            sort.expression =
                                replace_group_keys(&sort.expression, &hidden.replacements);
                        }
                        // A WITH sort sees projected names. Carry hidden keys
                        // through that boundary, then remove them before the next
                        // caller clause. Do not widen DISTINCT's equality key.
                        let reads_hidden = with.order_by.iter().any(|sort| {
                            expression_bindings(&sort.expression).is_some_and(|reads| {
                                hidden
                                    .replacements
                                    .iter()
                                    .any(|(_, name)| reads.contains(&Symbol::from(name.as_str())))
                            })
                        });
                        if reads_hidden && with.quantifier.is_some() {
                            return Err(GraphPlanError::UnsupportedCypher(
                                QueryFailureReason::Return,
                                "DISTINCT WITH cannot order by a key it does not project".into(),
                            ));
                        }
                        if reads_hidden {
                            projected.extend(hidden.replacements.iter().map(|(_, name)| {
                                Projection {
                                    expression: Expression::Identifier(name.clone()),
                                    alias: Some(name.clone()),
                                }
                            }));
                        }
                        clauses.push(Clause::With(WithClause {
                            items: ProjectionItems::Items(projected),
                            group_by: Vec::new(),
                            ..with
                        }));
                        if reads_hidden {
                            clauses.push(grouping_with(hidden.outer, true));
                        }
                    }
                }
            }
            Clause::Return(returned) if !returned.group_by.is_empty() => {
                let ProjectionItems::Items(items) = &returned.items else {
                    return Err(star_refused("RETURN"));
                };
                match explicit_grouping(items, &returned.group_by, "RETURN")? {
                    ExplicitGrouping::Implicit => clauses.push(Clause::Return(ReturnClause {
                        group_by: Vec::new(),
                        ..returned.clone()
                    })),
                    ExplicitGrouping::Distinct => clauses.push(Clause::Return(ReturnClause {
                        quantifier: Some(SetQuantifier::Distinct),
                        group_by: Vec::new(),
                        ..returned.clone()
                    })),
                    ExplicitGrouping::Hidden(hidden) => {
                        let aggregates = items
                            .iter()
                            .any(|item| ast_contains_aggregate(&item.expression));
                        let hidden = hidden_key_items(items, hidden, "RETURN", &mut used)?;
                        clauses.push(group_input(hidden.input));
                        clauses.push(grouping_with(hidden.inner, aggregates));
                        let outer = hidden.outer;
                        let mut returned = returned.clone();
                        for sort in &mut returned.order_by {
                            sort.expression =
                                replace_group_keys(&sort.expression, &hidden.replacements);
                        }
                        clauses.push(Clause::Return(ReturnClause {
                            items: ProjectionItems::Items(outer),
                            group_by: Vec::new(),
                            ..returned.clone()
                        }));
                    }
                }
            }
            clause => clauses.push(clause.clone()),
        }
    }
    Ok(Some(Query {
        clauses,
        unions: query.unions.clone(),
    }))
}

/// `LET a = x, b = y` as `WITH *, x AS a, y AS b`.
///
/// Each binding reads only what was in scope before the clause -- in Neo4j
/// `LET a = 1, b = a + 1` refuses `a` -- which is exactly what the WITH
/// checks. Rebinding a name in scope is refused there too, and an aggregate
/// is refused here, as Neo4j refuses it: LET does not group.
fn let_as_with(binding: &hydradb_cypher_ast::LetClause) -> GraphPlanResult<WithClause> {
    let items = binding
        .bindings
        .iter()
        .map(|(name, expression)| {
            if ast_contains_aggregate(expression) {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "LET cannot aggregate".to_string(),
                ));
            }
            Ok(Projection {
                expression: expression.clone(),
                alias: Some(name.clone()),
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    Ok(passing_with(ProjectionItems::All(items), None))
}

/// FILTER keeps the same scope and never aggregates.
fn validate_filter(filter: &hydradb_cypher_ast::FilterClause) -> GraphPlanResult<()> {
    if ast_contains_aggregate(&filter.predicate) {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            "FILTER cannot aggregate".to_string(),
        ));
    }
    Ok(())
}

fn passing_with(items: ProjectionItems, predicate: Option<Expression>) -> WithClause {
    WithClause {
        quantifier: None,
        items,
        group_by: Vec::new(),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        predicate,
    }
}

/// A projection list with its `*` spelled out: one bare item per name in
/// `scope`, in name order -- the column order Neo4j gives `RETURN *` -- then
/// whatever items followed the star.
///
/// `scope` holds only names the query wrote, so the names lowering
/// synthesized for anonymous elements never become columns. `RETURN *` over
/// an empty scope is refused, as Neo4j refuses it; `WITH *` over one is the
/// one row that binds nothing, as it is there.
fn star_expanded(
    items: &ProjectionItems,
    scope: &BTreeSet<Symbol>,
    clause: &str,
) -> GraphPlanResult<Vec<Projection>> {
    let extra = match items {
        ProjectionItems::Items(items) => return Ok(items.clone()),
        ProjectionItems::All(extra) => extra,
    };
    let mut expanded: Vec<Projection> = scope
        .iter()
        .map(|name| Projection {
            expression: Expression::Identifier(Identifier::from(name.as_str())),
            alias: None,
        })
        .collect();
    if expanded.is_empty() && clause == "RETURN" {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "RETURN * is not allowed when there are no variables in scope".to_string(),
        ));
    }
    expanded.extend(extra.iter().cloned());
    Ok(expanded)
}

/// Whether `expression` is `count(*)`, whose `*` is carried as an unparsed
/// fragment but reads no binding at all.
fn is_count_star(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Function { name, arguments, .. }
            if matches!(name.as_slice(), [function] if function.as_str().eq_ignore_ascii_case("count"))
                && matches!(arguments.as_slice(), [Expression::Syntax(fragment)] if fragment.text() == "*")
    )
}

fn check_scope(
    expression: &Expression,
    scope: &BTreeSet<Symbol>,
    clause: &str,
) -> GraphPlanResult<()> {
    // `count(*)` is the one fragment whose reads are known: none. It is a row
    // counter, not a reference, and refusing it would take `count(*)` away
    // from every query with a WITH in it.
    if is_count_star(expression) {
        return Ok(());
    }
    // Any other `None` is a fragment this crate has not parsed, so it cannot
    // be shown to stay inside the scope. Refusing beats guessing: the
    // alternative is admitting a reference to a binding the projection
    // dropped.
    let Some(bindings) = expression_bindings(expression) else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("{clause} reads syntax this engine cannot scope-check"),
        ));
    };
    for binding in bindings {
        if !scope.contains(&binding) {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Return,
                format!("{clause} references out-of-scope binding {binding}"),
            ));
        }
    }
    Ok(())
}

/// Lower one path pattern.
///
/// With `name_every_hop`, an unnamed fixed or variable-length hop gets a
/// synthesized relationship name. The clause holds each relationship or
/// relationship-list binding disjoint from the other named trails.
fn lower_path(
    path: &PathPattern,
    bound: &BTreeSet<Symbol>,
    synthesized: &mut BTreeSet<Symbol>,
    name_every_hop: bool,
    hop_names: &mut Vec<Symbol>,
    clause_deferred: &mut Vec<LogicalExpression>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let anchored = anchor_on_bound_binding(path, bound);
    let elements = anchored.as_deref().unwrap_or(path.elements.as_slice());
    let Some(PatternElement::Node(first)) = elements.first() else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "a path must begin with a node".to_string(),
        ));
    };
    let first_scan = lower_node_scan(first, synthesized)?;
    let GraphLogicalPlan::NodeScan {
        binding: first_binding,
        ..
    } = &first_scan
    else {
        unreachable!("node lowering returns a scan")
    };
    let mut from = first_binding.clone();
    let mut plan = apply_pattern_properties(
        first_scan,
        &from,
        first.properties.as_ref(),
        PatternElementKind::Node,
    )?;
    // Names this path has bound so far, on top of what earlier patterns and
    // clauses bound. An element's WHERE is applied as soon as everything it
    // reads is in here, so a selective one narrows the rows before the next
    // hop rather than after the whole walk; one that reads further along the
    // path waits in `deferred` until the path is complete.
    let mut available = bound.clone();
    available.insert(from.clone());
    let mut deferred = Vec::new();
    plan = apply_element_predicate(plan, first.predicate.as_ref(), &available, &mut deferred)?;
    let mut remaining = &elements[1..];

    while let [PatternElement::Relationship(relationship), PatternElement::Node(target), tail @ ..] =
        remaining
    {
        if relationship.predicate.is_some() && relationship.length.is_some() {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Where,
                "a WHERE inside a variable-length relationship is not supported".to_string(),
            ));
        }
        let GraphLogicalPlan::NodeScan {
            binding: written_to,
            labels: target_labels,
        } = lower_node_scan(target, synthesized)?
        else {
            unreachable!("node lowering returns a scan")
        };
        // `(n)-[:R]->(n)` names one node at both ends of a hop: it matches the
        // R relationships from n back to n. The hop expands into a fresh name,
        // unspellable like any synthesized one, and a filter holds that end to
        // be n itself; the path then continues from n.
        let to = if written_to == from {
            let index = synthesized
                .iter()
                .filter(|name| name.as_str().starts_with(ANONYMOUS_NODE_PREFIX))
                .count();
            let fresh = Symbol::from(format!("{ANONYMOUS_NODE_PREFIX}{index}").as_str());
            synthesized.insert(fresh.clone());
            fresh
        } else {
            written_to.clone()
        };
        let relationship_binding = relationship
            .variable
            .as_ref()
            .map(|binding| Symbol::from(binding.as_str()))
            .or_else(|| {
                let needs_name = relationship.properties.is_some() || name_every_hop;
                needs_name.then(|| {
                    // Counts this prefix only: nodes synthesize into the same
                    // set and their names are not part of this sequence.
                    let index = synthesized
                        .iter()
                        .filter(|name| name.as_str().starts_with(INLINE_RELATIONSHIP_PREFIX))
                        .count();
                    let name =
                        Symbol::from(format!("{INLINE_RELATIONSHIP_PREFIX}{index}").as_str());
                    synthesized.insert(name.clone());
                    name
                })
            });
        if name_every_hop {
            hop_names.extend(relationship_binding.clone());
        }
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
        let relationship_types = relationship
            .types
            .iter()
            .map(|relationship_type| Symbol::from(relationship_type.as_str()))
            .collect();
        plan = if let Some(length) = relationship.length {
            if relationship.properties.is_some() {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length relationship properties are not supported".to_string(),
                ));
            }
            let min_hops = length.minimum.unwrap_or(1);
            // No maximum is Cypher's unbounded walk. It terminates because a
            // path never reuses a relationship; how much work it may do is the
            // query's row budgets' to decide, which fail it loudly rather than
            // truncating the answer at an invented depth.
            let max_hops = length.maximum;
            if max_hops.is_some_and(|max_hops| min_hops > max_hops) {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Pattern,
                    "variable-length traversal minimum exceeds maximum".to_string(),
                ));
            }
            GraphLogicalPlan::VariableExpand {
                input: Box::new(plan),
                from,
                relationship: relationship_binding.clone(),
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
        if to != written_to {
            plan = GraphLogicalPlan::Filter {
                input: Box::new(plan),
                predicate: LogicalExpression::Binary {
                    left: Box::new(LogicalExpression::Identity(to.clone())),
                    operator: LogicalBinaryOperator::Equal,
                    right: Box::new(LogicalExpression::Identity(written_to.clone())),
                },
            };
        }
        plan = apply_pattern_properties(
            plan,
            &to,
            target.properties.as_ref(),
            PatternElementKind::Node,
        )?;
        if let Some(properties) = &relationship.properties {
            let binding = relationship_binding
                .as_ref()
                .expect("inline properties synthesize a binding");
            plan = apply_pattern_properties(
                plan,
                binding,
                Some(properties),
                PatternElementKind::Relationship,
            )?;
        }
        available.insert(written_to.clone());
        available.extend(relationship_binding);
        plan = apply_element_predicate(
            plan,
            relationship.predicate.as_ref(),
            &available,
            &mut deferred,
        )?;
        plan = apply_element_predicate(plan, target.predicate.as_ref(), &available, &mut deferred)?;
        from = written_to;
        remaining = tail;
    }
    if !remaining.is_empty() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            "path elements must alternate relationships and nodes".to_string(),
        ));
    }
    clause_deferred.extend(deferred);
    Ok(plan)
}

/// `r1 <> r2 AND ...` over every pair of distinct hop names, or `None` when
/// there is no pair. A name written twice is one relationship on purpose and
/// is not compared with itself.
fn distinct_relationships(names: &[Symbol]) -> Option<LogicalExpression> {
    // Repeating one variable is an identity constraint, not another trail.
    let names = names.iter().collect::<BTreeSet<_>>();
    (names.len() >= 2).then(|| LogicalExpression::Function {
        function: ScalarFunction::RelationshipsUnique,
        arguments: names
            .into_iter()
            .map(|name| LogicalExpression::Binding(name.clone()))
            .collect(),
    })
}

/// Filter `plan` by an element's own `WHERE`, or hold the predicate back.
///
/// `(n WHERE n.score > 2)` means what a clause `WHERE n.score > 2` would for
/// the same pattern, so it lowers to the same Filter. It is applied here only
/// when every name it reads is already bound; a predicate reading an element
/// later in the path, or one this crate cannot read the bindings of, goes to
/// `deferred` and filters the completed path instead.
fn apply_element_predicate(
    plan: GraphLogicalPlan,
    predicate: Option<&Expression>,
    available: &BTreeSet<Symbol>,
    deferred: &mut Vec<LogicalExpression>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let Some(predicate) = predicate else {
        return Ok(plan);
    };
    let lowered = lower_expression(predicate)?;
    let ready = expression_bindings(predicate).is_some_and(|reads| reads.is_subset(available));
    if !ready {
        deferred.push(lowered);
        return Ok(plan);
    }
    Ok(GraphLogicalPlan::Filter {
        input: Box::new(plan),
        predicate: lowered,
    })
}

/// Every `WHERE` written inside one of a clause's node or relationship
/// patterns.
fn element_predicates(matched: &MatchClause) -> impl Iterator<Item = &Expression> {
    matched
        .patterns
        .iter()
        .flat_map(|path| &path.elements)
        .filter_map(|element| match element {
            PatternElement::Node(node) => node.predicate.as_ref(),
            PatternElement::Relationship(relationship) => relationship.predicate.as_ref(),
        })
}

/// Re-read a single-hop pattern from the end an earlier pattern already bound,
/// when only that far end is bound.
///
/// `lower_path` anchors at the node written first and expands away from it,
/// and `NaturalJoin` runs correlated: the left row seeds the right side, and a
/// scan whose binding the seed already holds returns that row instead of
/// scanning. So a pattern written *toward* its bound end anchors on the wrong
/// one: `(c:Chunk)-[:HAS_CHUNK]->(s)` with `s` already bound anchors on `c`,
/// scans every Chunk, hydrates each one, expands it, then discards all but the
/// row the seed already held. Written the other way round --
/// `(s)<-[:HAS_CHUNK]-(c:Chunk)` -- the seed answers the anchor and the
/// expansion walks one vertex's edges. The two spell the same pattern, so
/// which one the caller typed should not decide the cost.
///
/// Reversing the elements and flipping the hop is the whole rewrite; the walk
/// that follows is unchanged. With one hop the synthesized relationship
/// binding keeps index 0, so a pattern this leaves alone keeps its plan hash.
///
/// Only when exactly one end is bound. Neither bound is an ordinary anchored
/// walk. Both bound wants an expand-into access path rather than a reversal,
/// which is a separate change. Variable-length hops are left alone: reversing
/// one is sound, but `VariableExpand` earns its own differential cases first.
///
/// Reversing moves each end's inline map across the expansion, so a map may
/// only be moved when it reads the same bindings on both sides of that move.
/// The hop produces two names -- the far end and the relationship -- and those
/// are the only ones whose availability the move changes.
///
/// The bound end's map moves *earlier*, losing both:
/// `(c:Chunk)-[:HAS_CHUNK]->(s {source_id: c.owner})` would read an absent
/// `c`, compare against null, and drop every row the written order keeps.
///
/// The far end's map moves *later*, gaining the relationship:
/// `(c:Chunk {owner: r.kind})-[r:HAS_CHUNK]->(s)` reads a null `r` where it
/// was written and a bound one after the reversal, admitting rows the written
/// order rejects.
///
/// A name from an earlier clause is unaffected in either direction, optional
/// or not: the move cannot change whether the seed row carries it, so reading
/// one never blocks the rewrite.
///
/// An optional name anchors like any other. A miss binds it to null, and a
/// pattern on a null name matches nothing, so the reversed anchor short-
/// circuits to no rows rather than falling through to a scan.
///
/// This once excluded OPTIONAL names, because a miss left the binding absent
/// rather than null: the seed could not answer the anchor, the scan ran for
/// real, and the bare `(s)` it reverses to carries no labels, so a Chunk label
/// scan became a scan of every vertex in the cell. Recording a miss as null
/// removed that cliff -- the anchor now matches nothing instead of
/// everything -- so the exclusion went with it.
fn anchor_on_bound_binding(
    path: &PathPattern,
    bound: &BTreeSet<Symbol>,
) -> Option<Vec<PatternElement>> {
    // A named path is written in the caller's direction, so it is not ours to
    // reverse even though nothing reads the name yet.
    if path.binding.is_some() {
        return None;
    }
    let [source, hop, target] = path.elements.as_slice() else {
        return None;
    };
    let PatternElement::Relationship(relationship) = hop else {
        return None;
    };
    let (PatternElement::Node(source), PatternElement::Node(target)) = (source, target) else {
        return None;
    };
    if relationship.length.is_some() {
        return None;
    }
    // An element's WHERE is placed by what it can read at its position in the
    // written order; reversing the hop would move it. Declined rather than
    // re-derived, as an inline map that cannot move is.
    if source.predicate.is_some()
        || relationship.predicate.is_some()
        || target.predicate.as_ref().is_some_and(|predicate| {
            !expression_bindings(predicate).is_some_and(|reads| reads.is_subset(bound))
        })
    {
        return None;
    }
    let is_bound = |node: &NodePattern| {
        node.variable
            .as_ref()
            .is_some_and(|variable| bound.contains(&Symbol::from(variable.as_str())))
    };
    if is_bound(source) || !is_bound(target) {
        return None;
    }
    // Moving a map changes *when* it is evaluated as well as what it can
    // read, so a map that can fail is not moved at all. Every leaf yields a
    // value or null; arithmetic divides by zero and overflows.
    if !map_is_total(target.properties.as_ref()) || !map_is_total(source.properties.as_ref()) {
        return None;
    }
    // Only what this hop itself produces can make a map read differently on
    // the far side of the move; see the note above. A name an earlier clause
    // already bound is in the seed row either way, even when this pattern
    // writes it again.
    let hop_binding: BTreeSet<Symbol> = relationship
        .variable
        .as_ref()
        .map(|variable| Symbol::from(variable.as_str()))
        .filter(|binding| !bound.contains(binding))
        .into_iter()
        .collect();
    let mut produced = hop_binding.clone();
    if let Some(variable) = &source.variable {
        produced.insert(Symbol::from(variable.as_str()));
    }
    if reads_any(target.properties.as_ref(), &produced)
        || reads_any(source.properties.as_ref(), &hop_binding)
    {
        return None;
    }
    let mut reversed = relationship.clone();
    // Preserve the bound target's WHERE after expansion. Moving it onto the
    // reversed anchor would evaluate it even when no edge exists.
    reversed.predicate = target.predicate.clone();
    let mut anchor = target.clone();
    anchor.predicate = None;
    reversed.direction = match relationship.direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        // Both already mean "either orientation satisfies this hop".
        direction @ (Direction::Undirected | Direction::Bidirectional) => direction,
    };
    Some(vec![
        PatternElement::Node(anchor),
        PatternElement::Relationship(reversed),
        PatternElement::Node(source.clone()),
    ])
}

/// Whether every value in an inline property map can only evaluate to a value
/// or null, never to an error. The planner's `total_value` draws the same line
/// one stage later: the leaves qualify, and every compound value expression is
/// arithmetic or a call, which divides by zero, overflows, or rejects its
/// argument.
///
/// Reversing moves a map across the expansion, so it is evaluated over a
/// different set of rows. For a total map that only changes cost. For one that
/// can fail it decides whether the query errors at all, which is not a
/// difference a cost rewrite may introduce.
fn map_is_total(properties: Option<&Expression>) -> bool {
    let Some(Expression::Map(entries)) = properties else {
        // A non-map is rejected by `apply_pattern_properties` anyway; leave
        // that error to it rather than reversing around it here.
        return properties.is_none();
    };
    entries.iter().all(|(_, value)| total_map_value(value))
}

/// Whether one inline map value can only evaluate to a value or null.
fn total_map_value(value: &Expression) -> bool {
    match value {
        Expression::Literal(_) | Expression::Parameter(_) | Expression::Identifier(_) => true,
        Expression::Property { target, .. } => matches!(**target, Expression::Identifier(_)),
        // A sign on an integer literal is how `-1` is written: the literal is
        // within range by the time it parses, so negating it cannot fail. Not
        // any literal -- the executor takes a sign on an integer and nothing
        // else, so `-true` and `-'text'` raise. And not a sign on a
        // non-literal, which is arithmetic over a value this cannot see.
        Expression::Unary {
            operator: UnaryOperator::Plus | UnaryOperator::Minus,
            expression,
        } => matches!(**expression, Expression::Literal(Literal::Integer(_))),
        _ => false,
    }
}

/// Whether an inline property map reads any of `forbidden`, or cannot be read
/// at all. A map this crate has not parsed counts as reading everything, so
/// the caller declines instead of guessing.
fn reads_any(properties: Option<&Expression>, forbidden: &BTreeSet<Symbol>) -> bool {
    let Some(properties) = properties else {
        return false;
    };
    match expression_bindings(properties) {
        Some(reads) => !reads.is_disjoint(forbidden),
        None => true,
    }
}

/// Every binding an expression reads, or `None` when it holds a syntax
/// fragment this crate has not parsed and so cannot answer for. Callers weigh
/// an unreadable expression as reading something unknown.
fn expression_bindings(expression: &Expression) -> Option<BTreeSet<Symbol>> {
    let mut bindings = BTreeSet::new();
    collect_expression_bindings(expression, &mut bindings).then_some(bindings)
}

/// Walks `expression`, collecting binding names. Returns false as soon as it
/// meets a fragment it cannot read, leaving `into` partly filled: the caller
/// discards it.
fn collect_expression_bindings(expression: &Expression, into: &mut BTreeSet<Symbol>) -> bool {
    if is_count_star(expression) {
        return true;
    }
    match expression {
        Expression::Identifier(identifier) => {
            into.insert(Symbol::from(identifier.as_str()));
            true
        }
        // A parameter is substituted before the plan runs, so it reads no
        // binding.
        Expression::Parameter(_) | Expression::Literal(_) => true,
        Expression::Property { target, .. } => collect_expression_bindings(target, into),
        Expression::Unary { expression, .. } => collect_expression_bindings(expression, into),
        Expression::Binary { left, right, .. } => {
            collect_expression_bindings(left, into) && collect_expression_bindings(right, into)
        }
        Expression::Function { arguments, .. } => arguments
            .iter()
            .all(|argument| collect_expression_bindings(argument, into)),
        Expression::List(items) => items
            .iter()
            .all(|item| collect_expression_bindings(item, into)),
        Expression::Map(entries) => entries
            .iter()
            .all(|(_, value)| collect_expression_bindings(value, into)),
        Expression::Case {
            operand,
            branches,
            default,
        } => {
            operand
                .as_deref()
                .is_none_or(|operand| collect_expression_bindings(operand, into))
                && branches.iter().all(|(when, then)| {
                    collect_expression_bindings(when, into)
                        && collect_expression_bindings(then, into)
                })
                && default
                    .as_deref()
                    .is_none_or(|default| collect_expression_bindings(default, into))
        }
        Expression::Syntax(_) => false,
    }
}

/// Every sub-expression of a CASE, in written order.
pub(crate) fn case_parts(expression: &LogicalExpression) -> Vec<&LogicalExpression> {
    let LogicalExpression::Case {
        operand,
        branches,
        default,
    } = expression
    else {
        return Vec::new();
    };
    let mut parts: Vec<&LogicalExpression> = operand.as_deref().into_iter().collect();
    for (when, then) in branches {
        parts.push(when);
        parts.push(then);
    }
    parts.extend(default.as_deref());
    parts
}

/// The scan a node pattern starts from. Its labels only: the inline map and
/// any `WHERE` it carries are applied by `lower_path`, which knows what else
/// the path has bound by then.
fn lower_node_scan(
    node: &NodePattern,
    synthesized: &mut BTreeSet<Symbol>,
) -> GraphPlanResult<GraphLogicalPlan> {
    let binding = node_symbol(node, synthesized)?;
    Ok(GraphLogicalPlan::NodeScan {
        binding,
        labels: node
            .labels
            .iter()
            .map(|label| Symbol::from(label.as_str()))
            .collect(),
    })
}

/// The name a node pattern is known by, synthesizing one when it has none.
///
/// `(a)-[:R]->()-[:R]->(c)` names its middle node nowhere, but the two hops
/// still have to meet there: without a shared name they cross instead of
/// joining, and the query answers a cartesian product of the two hops. The
/// name is invented on the same terms as an inline relationship's -- reserved
/// prefix, checked for collisions with the query's own variables -- and since
/// nothing can spell it, nothing downstream can read it.
fn node_symbol(node: &NodePattern, synthesized: &mut BTreeSet<Symbol>) -> GraphPlanResult<Symbol> {
    if let Some(binding) = node.variable.as_ref() {
        return Ok(Symbol::from(binding.as_str()));
    }
    let index = synthesized
        .iter()
        .filter(|name| name.as_str().starts_with(ANONYMOUS_NODE_PREFIX))
        .count();
    let name = Symbol::from(format!("{ANONYMOUS_NODE_PREFIX}{index}").as_str());
    synthesized.insert(name.clone());
    Ok(name)
}

/// Which kind of pattern element an inline property map belongs to. It
/// decides what `{id: ...}` means; see [`resolve_relationship_ids`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PatternElementKind {
    Node,
    Relationship,
}

fn apply_pattern_properties(
    plan: GraphLogicalPlan,
    binding: &Symbol,
    properties: Option<&Expression>,
    kind: PatternElementKind,
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
        // A vertex's identity is the client's `id`. A relationship's is an
        // internal counter, and the client's `id` is an ordinary property.
        let left = if property.as_str() == ID_PROPERTY && kind == PatternElementKind::Node {
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

/// The prefix `lower_path` synthesizes relationship names under. A
/// relationship with inline properties and no variable of its own needs a name
/// to hang the property filter on, and that name shares the row with the
/// caller's own.
const INLINE_RELATIONSHIP_PREFIX: &str = "__hydradb_inline_relationship_";

/// The prefix `node_symbol` synthesizes anonymous node names under. Same
/// contract as [`INLINE_RELATIONSHIP_PREFIX`]: reserved, collision-checked,
/// and unspellable by a query.
const ANONYMOUS_NODE_PREFIX: &str = "__hydradb_anonymous_node_";

/// Refuse a query whose own variables collide with a name the lowering
/// synthesized for it.
///
/// Compares the names actually produced, not the shape they are spelled in. A
/// query that synthesizes nothing collides with nothing, however its variables
/// are spelled, and one that synthesizes `_0` still leaves `_1` free.
/// Reserving the shape instead refused
/// `MATCH (__hydradb_inline_relationship_0) RETURN ...`, which produces no
/// synthesized name at all and so has nothing to collide with.
///
/// A real collision is refused rather than renamed around: the two names would
/// share one row slot, the node would be seeded with a relationship and join
/// with nothing, and the query would answer empty -- silently, and only for
/// patterns that synthesize that many names.
fn reject_synthesized_collisions(
    declared: &BTreeSet<Symbol>,
    synthesized: &BTreeSet<Symbol>,
) -> GraphPlanResult<()> {
    if let Some(collision) = declared.intersection(synthesized).next() {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Pattern,
            format!("{collision} is reserved for a relationship this query names itself"),
        ));
    }
    Ok(())
}

/// Every binding a MATCH clause declares. A pattern element without a
/// variable declares nothing.
fn clause_bindings(matched: &MatchClause) -> BTreeSet<Symbol> {
    matched.patterns.iter().flat_map(path_bindings).collect()
}

/// Every binding one path declares, nodes and relationships alike.
fn path_bindings(path: &PathPattern) -> BTreeSet<Symbol> {
    path.elements
        .iter()
        .filter_map(|element| match element {
            PatternElement::Node(node) => node.variable.as_ref(),
            PatternElement::Relationship(relationship) => relationship.variable.as_ref(),
        })
        .map(|variable| Symbol::from(variable.as_str()))
        .collect()
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
        LogicalExpression::Function { arguments, .. } | LogicalExpression::List(arguments) => {
            arguments
                .iter()
                .try_for_each(|argument| validate_declared_bindings(argument, declared))
        }
        LogicalExpression::Case { .. } => case_parts(expression)
            .into_iter()
            .try_for_each(|part| validate_declared_bindings(part, declared)),
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
            if key.as_str() == ID_PROPERTY {
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
        } => lower_function(name, *distinct, arguments)?,
        Expression::List(items) => LogicalExpression::List(
            items
                .iter()
                .map(lower_expression)
                .collect::<GraphPlanResult<Vec<_>>>()?,
        ),
        Expression::Case {
            operand,
            branches,
            default,
        } => LogicalExpression::Case {
            operand: operand
                .as_deref()
                .map(lower_expression)
                .transpose()?
                .map(Box::new),
            branches: branches
                .iter()
                .map(|(when, then)| Ok((lower_expression(when)?, lower_expression(then)?)))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            default: default
                .as_deref()
                .map(lower_expression)
                .transpose()?
                .map(Box::new),
        },
        _ => {
            return Err(GraphPlanError::UnsupportedCypher(
                QueryFailureReason::Other,
                "expression is outside the initial typed slice".to_string(),
            ))
        }
    })
}

/// A function call: an aggregate, which reads a group, or a scalar function,
/// which reads one row. Which one decides whether the projection around it
/// groups, so the two never share a representation.
fn lower_function(
    name: &[Identifier],
    distinct: bool,
    arguments: &[Expression],
) -> GraphPlanResult<LogicalExpression> {
    let [name] = name else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            "namespaced functions are not supported".to_string(),
        ));
    };
    let function_name = name.as_str().to_ascii_lowercase();
    if let Some(function) = AggregateFunction::from_name(&function_name) {
        let expression = match arguments {
            // The permissive parser accepts this spelling; reject it explicitly.
            [Expression::Syntax(fragment)]
                if function == AggregateFunction::Count && fragment.text() == "*" && distinct =>
            {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    "count(DISTINCT *) is not supported; DISTINCT requires an expression"
                        .to_string(),
                ));
            }
            [Expression::Syntax(fragment)]
                if function == AggregateFunction::Count && fragment.text() == "*" && !distinct =>
            {
                None
            }
            [argument] => Some(Box::new(lower_expression(argument)?)),
            _ => {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Return,
                    format!("function {name} requires exactly one argument"),
                ));
            }
        };
        return Ok(LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
        });
    }

    let Some(function) = ScalarFunction::from_name(&function_name) else {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("function {name} is not supported"),
        ));
    };
    // DISTINCT only means something to a function that reads many values.
    if distinct {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!("DISTINCT is only valid in an aggregate, not in {name}"),
        ));
    }
    if !function.accepts_arity(arguments.len()) {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Return,
            format!(
                "function {name} does not take {} arguments",
                arguments.len()
            ),
        ));
    }
    Ok(LogicalExpression::Function {
        function,
        arguments: arguments
            .iter()
            .map(lower_expression)
            .collect::<GraphPlanResult<Vec<_>>>()?,
    })
}

/// The names a RETURN projection creates, which ORDER BY may read although
/// they are columns rather than bindings.
fn item_aliases(items: &[Projection]) -> BTreeSet<Symbol> {
    items
        .iter()
        .filter_map(|item| {
            item.alias
                .as_ref()
                .map(|alias| Symbol::from(alias.as_str()))
        })
        .collect()
}
