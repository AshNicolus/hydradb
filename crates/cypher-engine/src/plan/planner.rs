use hydradb_cypher_ast::QueryFailureReason;
use std::collections::{BTreeMap, BTreeSet};

use super::lower::MAX_IN_LIST_VALUES;

use crate::{
    BoundValue, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError, GraphPlanResult,
    LogicalBinaryOperator, LogicalExpression, LogicalInList, LogicalProjection, PatternDirection,
    PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, PhysicalSort,
    PhysicalUnaryOperator, ScalarValue, StatisticsProvider, Symbol, UnknownStatistics, ValueOrigin,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalPlanningContext {
    pub parameters: BTreeMap<Symbol, ScalarValue>,
    /// List-valued parameters, kept apart from `parameters` because
    /// `ScalarValue` has no list variant and must not grow one: it is also the
    /// stored-property type, and a list is not storable as a property. Only
    /// `IN` reads this map.
    pub list_parameters: BTreeMap<Symbol, Vec<ScalarValue>>,
}

impl PhysicalPlanningContext {
    pub fn with_parameter(mut self, name: impl Into<Symbol>, value: ScalarValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    pub fn with_list_parameter(
        mut self,
        name: impl Into<Symbol>,
        values: Vec<ScalarValue>,
    ) -> Self {
        self.list_parameters.insert(name.into(), values);
        self
    }
}

pub fn plan_physical(
    logical: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<GraphPhysicalPlan> {
    plan_physical_with_statistics(logical, context, &UnknownStatistics)
}

pub fn plan_physical_with_statistics(
    logical: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> GraphPlanResult<GraphPhysicalPlan> {
    match logical {
        GraphLogicalPlan::Union { arms, all } => Ok(GraphPhysicalPlan::Union {
            arms: arms
                .iter()
                .map(|arm| plan_physical_with_statistics(arm, context, statistics))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            all: *all,
        }),
        GraphLogicalPlan::NodeScan { binding, labels } => {
            Ok(fallback_scan(binding, labels, statistics))
        }
        GraphLogicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => Ok(physical_expand(
            plan_physical_with_statistics(input, context, statistics)?,
            from,
            relationship,
            relationship_types,
            *direction,
            to,
            target_labels,
        )),
        GraphLogicalPlan::VariableExpand {
            input,
            from,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
        } => Ok(GraphPhysicalPlan::VariableExpand {
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            from: from.clone(),
            relationship_types: relationship_types.clone(),
            direction: *direction,
            to: to.clone(),
            target_labels: target_labels.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
        }),
        GraphLogicalPlan::NaturalJoin {
            left,
            right,
            optional,
        } => Ok(GraphPhysicalPlan::NaturalJoin {
            left: Box::new(plan_physical_with_statistics(left, context, statistics)?),
            right: Box::new(plan_physical_with_statistics(right, context, statistics)?),
            optional: *optional,
        }),
        GraphLogicalPlan::Filter { input, predicate } => {
            plan_filter(input, predicate, context, statistics)
        }
        GraphLogicalPlan::Sort { input, items } => Ok(GraphPhysicalPlan::Sort {
            estimated_input_rows: estimate_rows(input, context, statistics),
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            items: items
                .iter()
                .map(|item| {
                    Ok(PhysicalSort {
                        expression: bind_expression(&item.expression, context)?,
                        direction: item.direction,
                    })
                })
                .collect::<GraphPlanResult<Vec<_>>>()?,
        }),
        GraphLogicalPlan::Skip { input, count } => Ok(GraphPhysicalPlan::Skip {
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            count: bind_window_count(count, "SKIP", context)?,
        }),
        GraphLogicalPlan::Limit { input, count } => {
            let count = bind_window_count(count, "LIMIT", context)?;
            if let Some(window) = plan_ordered_window(input, &count, context)? {
                return Ok(window);
            }
            Ok(GraphPhysicalPlan::Limit {
                input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
                count,
            })
        }
        GraphLogicalPlan::Project {
            input,
            items,
            distinct,
            post_sort,
            post_skip,
            post_limit,
        } => Ok(GraphPhysicalPlan::Project {
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            items: items
                .iter()
                .map(|item| bind_projection(item, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
            distinct: *distinct,
            post_sort: post_sort
                .iter()
                .map(|item| {
                    Ok(PhysicalSort {
                        expression: bind_expression(&item.expression, context)?,
                        direction: item.direction,
                    })
                })
                .collect::<GraphPlanResult<Vec<_>>>()?,
            post_skip: post_skip
                .as_ref()
                .map(|count| bind_window_count(count, "SKIP", context))
                .transpose()?,
            post_limit: post_limit
                .as_ref()
                .map(|count| bind_window_count(count, "LIMIT", context))
                .transpose()?,
        }),
    }
}

/// Restrictions on one string property that an ordered property index can
/// prove on its own, so no residual `Filter` is needed above the scan.
#[derive(Default)]
struct OrderedWindowPredicate {
    prefix: Option<BoundValue>,
    lower: Option<(BoundValue, bool)>,
    upper: Option<(BoundValue, bool)>,
}

/// Rewrite `LIMIT n` over `[SKIP s] ORDER BY b.p [, b.id] FILTER(b.p ...)
/// SCAN b` into a bounded ordered index walk. This is the legacy engine's
/// ordered-string-index fast path expressed as a physical operator: it only
/// fires when the index alone proves every predicate term and the sort order,
/// so semantics are unchanged and every other shape keeps the generic plan.
fn plan_ordered_window(
    input: &GraphLogicalPlan,
    limit: &BoundValue,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let (skip, sorted) = match input {
        GraphLogicalPlan::Skip { input, count } => (
            Some(bind_window_count(count, "SKIP", context)?),
            input.as_ref(),
        ),
        other => (None, other),
    };
    let GraphLogicalPlan::Sort {
        input: filtered,
        items,
    } = sorted
    else {
        return Ok(None);
    };
    // An inline property map and a WHERE clause lower to separate Filters, so
    // the sort sits over a stack of them rather than over one. Peel the whole
    // stack: each layer is a conjunct of the same predicate.
    let mut predicates = Vec::new();
    let mut scanned = filtered.as_ref();
    while let GraphLogicalPlan::Filter { input, predicate } = scanned {
        predicates.push(predicate);
        scanned = input.as_ref();
    }
    let GraphLogicalPlan::NodeScan { binding, labels } = scanned else {
        return Ok(None);
    };

    let items = items
        .iter()
        .map(|item| {
            Ok(PhysicalSort {
                expression: bind_expression(&item.expression, context)?,
                direction: item.direction,
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let [first, rest @ ..] = items.as_slice() else {
        return Ok(None);
    };
    let PhysicalExpression::Property {
        binding: sort_binding,
        property,
    } = &first.expression
    else {
        return Ok(None);
    };
    if sort_binding != binding {
        return Ok(None);
    }
    // The index orders ties by vertex ID. A tie key it does not store is still
    // serviceable, because rows sharing a primary value are all read before the
    // walk moves off that value: the scan hands the operator the complete tie
    // group and it sorts within it. Anything past that group loses on the
    // primary key, so no tie key can promote it. What the index cannot do is
    // order a tie group it never sees whole, which is why only one tie key on
    // this binding qualifies.
    match rest {
        [] => {}
        [tie] => match &tie.expression {
            PhysicalExpression::Identity(tie_binding) if tie_binding == binding => {}
            PhysicalExpression::Property {
                binding: tie_binding,
                ..
            } if tie_binding == binding => {}
            _ => return Ok(None),
        },
        _ => return Ok(None),
    }

    let mut window = OrderedWindowPredicate::default();
    let mut residual = Vec::new();
    for predicate in predicates {
        let bound = bind_expression(predicate, context)?;
        collect_ordered_window_predicate(&bound, binding, property, &mut window, &mut residual);
    }
    // An equality on the sorted property names the exact values wanted, and a
    // seek on them returns those rows already in order. Walking the index from
    // its start and rejecting everything else cannot beat that, so leave the
    // query to the ordinary access-path choice. Without this the ordered walk
    // wins every race it enters, including the ones it should lose.
    if residual.iter().any(|conjunct| {
        matches!(
            equality_constraint(conjunct),
            Some((constraint_binding, constraint_property, _))
                if constraint_binding == *binding && constraint_property == *property
        )
    }) {
        return Ok(None);
    }
    // Every term went to the residual, so the index bounds nothing: the walk
    // would start at the property's first key and run until the residual
    // happened to yield enough rows. That is a full scan wearing an operator's
    // name. A query that leaves the index nothing to stand on keeps the generic
    // plan, where the ordinary access-path choice applies.
    if window.prefix.is_none() && window.lower.is_none() && window.upper.is_none() {
        return Ok(None);
    }
    let residual = fold_conjuncts(residual);

    let limit_count = window_count_usize(limit);
    let skip_count = skip.as_ref().map(window_count_usize).unwrap_or(0);
    let Some(required) = limit_count.checked_add(skip_count) else {
        return Ok(None);
    };

    let mut plan = GraphPhysicalPlan::OrderedVertexPropertyScan {
        binding: binding.clone(),
        labels: labels.clone(),
        property: property.clone(),
        direction: first.direction,
        prefix: window.prefix,
        lower: window.lower,
        upper: window.upper,
        required,
        items,
        residual,
    };
    if let Some(skip) = skip {
        plan = GraphPhysicalPlan::Skip {
            input: Box::new(plan),
            count: skip,
        };
    }
    Ok(Some(GraphPhysicalPlan::Limit {
        input: Box::new(plan),
        count: limit.clone(),
    }))
}

/// Rebuild one predicate from conjuncts, so the scan carries a single
/// expression rather than a list `evaluate_expression` would have to be taught
/// about.
fn fold_conjuncts(conjuncts: Vec<PhysicalExpression>) -> Option<PhysicalExpression> {
    conjuncts
        .into_iter()
        .reduce(|left, right| PhysicalExpression::Binary {
            left: Box::new(left),
            operator: PhysicalBinaryOperator::And,
            right: Box::new(right),
        })
}

fn window_count_usize(value: &BoundValue) -> usize {
    match value.value {
        // `bind_window_count` already rejected negatives and overflow.
        ScalarValue::Integer(count) => usize::try_from(count).unwrap_or(usize::MAX),
        _ => usize::MAX,
    }
}

/// Accept only a conjunction of string comparisons on exactly `binding.property`
/// with literal or parameter string operands. Anything else — another
/// property, a non-string operand, OR, NOT, IN, equality — returns false so the
/// caller keeps the generic filter/sort plan.
/// Split a predicate into the restrictions this ordered index can enforce and
/// the conjuncts it cannot.
///
/// Only `And` is split. Anything else is offered to the window whole and kept
/// as residual when it does not fit, because one side of an `Or` restricts
/// nothing on its own -- the walk would drop rows the other side accepts.
///
/// Nothing is rejected. A conjunct the index cannot express is work for the
/// scan to do per row, not a reason to abandon the ordered walk, which is the
/// difference between reading a bounded prefix of the index and reading every
/// vertex the query's broadest equality matches.
fn collect_ordered_window_predicate(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
    window: &mut OrderedWindowPredicate,
    residual: &mut Vec<PhysicalExpression>,
) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_ordered_window_predicate(left, binding, property, window, residual);
        collect_ordered_window_predicate(right, binding, property, window, residual);
        return;
    }
    if !fill_ordered_window_slot(predicate, binding, property, window) {
        residual.push(predicate.clone());
    }
}

/// Place one comparison in the window's prefix, lower or upper slot.
///
/// `false` means the index cannot express it and the caller keeps it as
/// residual; the window is only mutated on success, so a rejected comparison
/// leaves no trace.
fn fill_ordered_window_slot(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
    window: &mut OrderedWindowPredicate,
) -> bool {
    let PhysicalExpression::Binary {
        left,
        operator,
        right,
    } = predicate
    else {
        return false;
    };
    let is_target = |expression: &PhysicalExpression| {
        matches!(
            expression,
            PhysicalExpression::Property { binding: b, property: p } if b == binding && p == property
        )
    };
    let string_value = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Value(value) if matches!(value.value, ScalarValue::String(_)) => {
            Some(value.clone())
        }
        _ => None,
    };
    // Normalize `value OP property` into `property OP' value`.
    let (value, operator) = if is_target(left) {
        (string_value(right), *operator)
    } else if is_target(right) {
        let flipped = match operator {
            PhysicalBinaryOperator::LessThan => PhysicalBinaryOperator::GreaterThan,
            PhysicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::GreaterThanOrEqual,
            PhysicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::LessThan,
            PhysicalBinaryOperator::GreaterThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
            // `'x' STARTS WITH n.p` is not an index restriction on n.p.
            _ => return false,
        };
        (string_value(left), flipped)
    } else {
        return false;
    };
    let Some(value) = value else {
        return false;
    };
    let slot = match operator {
        PhysicalBinaryOperator::StartsWith => &mut window.prefix,
        PhysicalBinaryOperator::GreaterThan | PhysicalBinaryOperator::GreaterThanOrEqual => {
            let inclusive = operator == PhysicalBinaryOperator::GreaterThanOrEqual;
            return window.lower.is_none() && {
                window.lower = Some((value, inclusive));
                true
            };
        }
        PhysicalBinaryOperator::LessThan | PhysicalBinaryOperator::LessThanOrEqual => {
            let inclusive = operator == PhysicalBinaryOperator::LessThanOrEqual;
            return window.upper.is_none() && {
                window.upper = Some((value, inclusive));
                true
            };
        }
        _ => return false,
    };
    if slot.is_some() {
        return false;
    }
    *slot = Some(value);
    true
}

fn plan_filter(
    input: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> GraphPlanResult<GraphPhysicalPlan> {
    let bound_predicate = bind_expression(predicate, context)?;
    if let GraphLogicalPlan::Expand {
        input: source,
        from,
        relationship,
        relationship_types,
        direction,
        to,
        target_labels,
    } = input
    {
        if let (
            Some(relationship),
            [relationship_type],
            GraphLogicalPlan::NodeScan {
                binding: source_binding,
                labels: source_labels,
            },
            Some((constraint_binding, property, values)),
        ) = (
            relationship,
            relationship_types.as_slice(),
            source.as_ref(),
            relationship
                .as_ref()
                .and_then(|binding| equality_constraint_for_binding(&bound_predicate, binding)),
        ) {
            if source_binding == from && constraint_binding == *relationship && values.len() == 1 {
                let value = values.into_iter().next().expect("one value");
                let seek_rows = statistics
                    .relationship_property_value_count(relationship_types, &property, &value.value)
                    .or_else(|| {
                        statistics
                            .relationship_bloom_may_contain(
                                relationship_types,
                                &property,
                                &value.value,
                            )
                            .and_then(|possibly_present| (!possibly_present).then_some(0))
                    })
                    .or_else(|| {
                        statistics
                            .relationship_property_statistics(relationship_types, &property)
                            .and_then(|stats| property_equality_estimate(&stats))
                    });
                let seek = GraphPhysicalPlan::RelationshipPropertySeek {
                    from: from.clone(),
                    source_labels: source_labels.clone(),
                    relationship: relationship.clone(),
                    relationship_type: relationship_type.clone(),
                    property,
                    value,
                    direction: *direction,
                    to: to.clone(),
                    target_labels: target_labels.clone(),
                };
                let expand_rows = estimate_anchored_expansion_rows(
                    source,
                    relationship_types,
                    *direction,
                    statistics,
                );
                let access = if prefer_index(seek_rows, expand_rows) {
                    seek
                } else {
                    physical_expand(
                        plan_physical_with_statistics(source, context, statistics)?,
                        from,
                        &Some(relationship.clone()),
                        relationship_types,
                        *direction,
                        to,
                        target_labels,
                    )
                };
                return Ok(GraphPhysicalPlan::Filter {
                    input: Box::new(access),
                    predicate: bound_predicate,
                });
            }
        }
        let bindings = expression_bindings(predicate);
        if bindings.iter().all(|binding| *binding == from) {
            return Ok(physical_expand(
                plan_filter(source, predicate, context, statistics)?,
                from,
                relationship,
                relationship_types,
                *direction,
                to,
                target_labels,
            ));
        }
    }

    let access = match input {
        GraphLogicalPlan::NodeScan { binding, labels } => {
            let scan = fallback_scan(binding, labels, statistics);
            let scan_rows = best_vertex_count(labels, statistics);
            match identity_constraint(&bound_predicate) {
                Some((constraint_binding, value)) if constraint_binding == *binding => {
                    GraphPhysicalPlan::VertexIdSeek {
                        binding: binding.clone(),
                        labels: labels.clone(),
                        value,
                    }
                }
                _ => match equality_constraint(&bound_predicate) {
                    Some((constraint_binding, property, values))
                        if constraint_binding == *binding =>
                    {
                        let seek_rows =
                            property_values_estimate(labels, &property, &values, statistics);
                        if !prefer_index(seek_rows, scan_rows) {
                            scan
                        } else if values.len() == 1 {
                            GraphPhysicalPlan::VertexPropertySeek {
                                binding: binding.clone(),
                                labels: labels.clone(),
                                property,
                                value: values.into_iter().next().expect("one value"),
                            }
                        } else {
                            GraphPhysicalPlan::VertexPropertyMultiSeek {
                                binding: binding.clone(),
                                labels: labels.clone(),
                                property,
                                values,
                            }
                        }
                    }
                    _ => match property_scan_constraint(&bound_predicate) {
                        Some((constraint_binding, property)) if constraint_binding == *binding => {
                            let property_rows = statistics
                                .property_statistics(labels, &property)
                                .and_then(|stats| stats.non_null_count);
                            if prefer_index(property_rows, scan_rows) {
                                GraphPhysicalPlan::VertexPropertyScan {
                                    binding: binding.clone(),
                                    labels: labels.clone(),
                                    property,
                                }
                            } else {
                                scan
                            }
                        }
                        _ => scan,
                    },
                },
            }
        }
        _ => plan_physical_with_statistics(input, context, statistics)?,
    };
    Ok(GraphPhysicalPlan::Filter {
        input: Box::new(access),
        predicate: bound_predicate,
    })
}

fn physical_expand(
    input: GraphPhysicalPlan,
    from: &Symbol,
    relationship: &Option<Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
) -> GraphPhysicalPlan {
    GraphPhysicalPlan::Expand {
        input: Box::new(input),
        from: from.clone(),
        relationship: relationship.clone(),
        relationship_types: relationship_types.to_vec(),
        direction,
        to: to.clone(),
        target_labels: target_labels.to_vec(),
    }
}

/// Resolves an `IN` right-hand side to concrete bound values.
///
/// An empty list is accepted and matches nothing. Callers page id sets down to
/// empty often enough that rejecting it would be a correctness trap rather than
/// a helpful error, and Cypher already defines `x IN []` as false.
fn bind_in_list(
    list: &LogicalInList,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<Vec<BoundValue>> {
    let values = match list {
        LogicalInList::Values(elements) => elements
            .iter()
            .map(|element| match bind_expression(element, context)? {
                PhysicalExpression::Value(value) => Ok(value),
                _ => Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Where,
                    "IN list elements must be literals or parameters".to_string(),
                )),
            })
            .collect::<GraphPlanResult<Vec<_>>>()?,
        LogicalInList::Parameter(parameter) => {
            if let Some(values) = context.list_parameters.get(parameter) {
                values
                    .iter()
                    .map(|value| BoundValue {
                        value: value.clone(),
                        origin: ValueOrigin::Parameter(parameter.clone()),
                    })
                    .collect()
            } else if let Some(value) = context.parameters.get(parameter) {
                // A scalar bound where a list was expected is a one-element
                // membership test rather than an error.
                vec![BoundValue {
                    value: value.clone(),
                    origin: ValueOrigin::Parameter(parameter.clone()),
                }]
            } else {
                return Err(GraphPlanError::MissingParameter(
                    parameter.as_str().to_string(),
                ));
            }
        }
    };
    if values.len() > MAX_IN_LIST_VALUES {
        return Err(GraphPlanError::UnsupportedCypher(
            QueryFailureReason::Where,
            format!(
                "IN list of {} values exceeds the {MAX_IN_LIST_VALUES}-value limit",
                values.len()
            ),
        ));
    }
    Ok(values)
}

fn expression_bindings(expression: &LogicalExpression) -> BTreeSet<&Symbol> {
    let mut bindings = BTreeSet::new();
    collect_expression_bindings(expression, &mut bindings);
    bindings
}

fn collect_expression_bindings<'a>(
    expression: &'a LogicalExpression,
    bindings: &mut BTreeSet<&'a Symbol>,
) {
    match expression {
        LogicalExpression::Binding(binding)
        | LogicalExpression::Identity(binding)
        | LogicalExpression::Property { binding, .. } => {
            bindings.insert(binding);
        }
        LogicalExpression::Unary { expression, .. } => {
            collect_expression_bindings(expression, bindings);
        }
        LogicalExpression::Aggregate { expression, .. } => {
            if let Some(expression) = expression {
                collect_expression_bindings(expression, bindings);
            }
        }
        LogicalExpression::Binary { left, right, .. } => {
            collect_expression_bindings(left, bindings);
            collect_expression_bindings(right, bindings);
        }
        LogicalExpression::In { expression, list } => {
            collect_expression_bindings(expression, bindings);
            if let LogicalInList::Values(elements) = list {
                for element in elements {
                    collect_expression_bindings(element, bindings);
                }
            }
        }
        LogicalExpression::Parameter(_) | LogicalExpression::Literal(_) => {}
    }
}

fn fallback_scan(
    binding: &Symbol,
    labels: &[Symbol],
    statistics: &dyn StatisticsProvider,
) -> GraphPhysicalPlan {
    match labels {
        [_, ..] => {
            let label = labels
                .iter()
                .min_by_key(|label| {
                    statistics
                        .vertex_count(std::slice::from_ref(label))
                        .unwrap_or(u64::MAX)
                })
                .expect("non-empty labels");
            GraphPhysicalPlan::VertexLabelScan {
                binding: binding.clone(),
                label: label.clone(),
                labels: labels.to_vec(),
            }
        }
        [] => GraphPhysicalPlan::AllVertexScan {
            binding: binding.clone(),
        },
    }
}

fn bind_projection(
    projection: &LogicalProjection,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<PhysicalProjection> {
    Ok(PhysicalProjection {
        expression: bind_expression(&projection.expression, context)?,
        alias: projection.alias.clone(),
    })
}

fn bind_expression(
    expression: &LogicalExpression,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<PhysicalExpression> {
    Ok(match expression {
        LogicalExpression::Binding(binding) => PhysicalExpression::Binding(binding.clone()),
        LogicalExpression::Identity(binding) => PhysicalExpression::Identity(binding.clone()),
        LogicalExpression::Property { binding, property } => PhysicalExpression::Property {
            binding: binding.clone(),
            property: property.clone(),
        },
        LogicalExpression::Parameter(parameter) => {
            let value =
                context.parameters.get(parameter).cloned().ok_or_else(|| {
                    GraphPlanError::MissingParameter(parameter.as_str().to_string())
                })?;
            PhysicalExpression::Value(BoundValue {
                value,
                origin: ValueOrigin::Parameter(parameter.clone()),
            })
        }
        LogicalExpression::Literal(value) => PhysicalExpression::Value(BoundValue {
            value: value.clone(),
            origin: ValueOrigin::Literal,
        }),
        LogicalExpression::Aggregate {
            function,
            expression,
        } => PhysicalExpression::Aggregate {
            function: *function,
            expression: expression
                .as_ref()
                .map(|expression| bind_expression(expression, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::Unary {
            operator,
            expression,
        } => PhysicalExpression::Unary {
            operator: match operator {
                crate::LogicalUnaryOperator::Not => PhysicalUnaryOperator::Not,
                crate::LogicalUnaryOperator::Plus => PhysicalUnaryOperator::Plus,
                crate::LogicalUnaryOperator::Minus => PhysicalUnaryOperator::Minus,
                crate::LogicalUnaryOperator::IsNull => PhysicalUnaryOperator::IsNull,
                crate::LogicalUnaryOperator::IsNotNull => PhysicalUnaryOperator::IsNotNull,
            },
            expression: Box::new(bind_expression(expression, context)?),
        },
        LogicalExpression::In { expression, list } => PhysicalExpression::InList {
            expression: Box::new(bind_expression(expression, context)?),
            values: bind_in_list(list, context)?,
        },
        LogicalExpression::Binary {
            left,
            operator,
            right,
        } => PhysicalExpression::Binary {
            left: Box::new(bind_expression(left, context)?),
            operator: match operator {
                LogicalBinaryOperator::Equal => PhysicalBinaryOperator::Equal,
                LogicalBinaryOperator::NotEqual => PhysicalBinaryOperator::NotEqual,
                LogicalBinaryOperator::LessThan => PhysicalBinaryOperator::LessThan,
                LogicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
                LogicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::GreaterThan,
                LogicalBinaryOperator::GreaterThanOrEqual => {
                    PhysicalBinaryOperator::GreaterThanOrEqual
                }
                LogicalBinaryOperator::And => PhysicalBinaryOperator::And,
                LogicalBinaryOperator::Or => PhysicalBinaryOperator::Or,
                LogicalBinaryOperator::Add => PhysicalBinaryOperator::Add,
                LogicalBinaryOperator::Subtract => PhysicalBinaryOperator::Subtract,
                LogicalBinaryOperator::Multiply => PhysicalBinaryOperator::Multiply,
                LogicalBinaryOperator::Divide => PhysicalBinaryOperator::Divide,
                LogicalBinaryOperator::Modulo => PhysicalBinaryOperator::Modulo,
                LogicalBinaryOperator::StartsWith => PhysicalBinaryOperator::StartsWith,
            },
            right: Box::new(bind_expression(right, context)?),
        },
    })
}

fn bind_window_count(
    expression: &LogicalExpression,
    field: &'static str,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<BoundValue> {
    let bound = bind_expression(expression, context)?;
    let (value, origin) = constant_integer(&bound, field)?;
    if value < 0 {
        return Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            format!("{field} cannot be negative"),
        ));
    }
    if field == "SKIP" && u64::try_from(value).is_err() {
        return Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            "SKIP exceeds the supported u64 range".to_string(),
        ));
    }
    if field == "LIMIT" && usize::try_from(value).is_err() {
        return Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            "LIMIT exceeds the supported usize range".to_string(),
        ));
    }
    Ok(BoundValue {
        value: ScalarValue::Integer(value),
        origin,
    })
}

fn constant_integer(
    expression: &PhysicalExpression,
    field: &'static str,
) -> GraphPlanResult<(i128, ValueOrigin)> {
    match expression {
        PhysicalExpression::Value(BoundValue {
            value: ScalarValue::Integer(value),
            origin,
        }) => Ok((*value, origin.clone())),
        PhysicalExpression::Unary {
            operator,
            expression,
        } => {
            let (value, origin) = constant_integer(expression, field)?;
            match operator {
                PhysicalUnaryOperator::Not => Err(GraphPlanError::InvalidLogicalPlan(
                    QueryFailureReason::OrderWindow,
                    format!("{field} does not accept NOT"),
                )),
                PhysicalUnaryOperator::IsNull | PhysicalUnaryOperator::IsNotNull => {
                    Err(GraphPlanError::InvalidLogicalPlan(
                        QueryFailureReason::OrderWindow,
                        format!("{field} does not accept IS NULL"),
                    ))
                }
                PhysicalUnaryOperator::Plus => Ok((value, origin)),
                PhysicalUnaryOperator::Minus => value
                    .checked_neg()
                    .map(|value| (value, origin))
                    .ok_or_else(|| {
                        GraphPlanError::InvalidLogicalPlan(
                            QueryFailureReason::OrderWindow,
                            format!("{field} constant expression overflowed"),
                        )
                    }),
            }
        }
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => {
            let (left, left_origin) = constant_integer(left, field)?;
            let (right, right_origin) = constant_integer(right, field)?;
            let origin = match (left_origin, right_origin) {
                (ValueOrigin::Parameter(parameter), _) | (_, ValueOrigin::Parameter(parameter)) => {
                    ValueOrigin::Parameter(parameter)
                }
                _ => ValueOrigin::Literal,
            };
            let value = match operator {
                PhysicalBinaryOperator::Add => left.checked_add(right),
                PhysicalBinaryOperator::Subtract => left.checked_sub(right),
                PhysicalBinaryOperator::Multiply => left.checked_mul(right),
                PhysicalBinaryOperator::Divide if right != 0 => left.checked_div(right),
                PhysicalBinaryOperator::Modulo if right != 0 => left.checked_rem(right),
                PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo => {
                    return Err(GraphPlanError::InvalidLogicalPlan(
                        QueryFailureReason::OrderWindow,
                        format!("{field} divides by zero"),
                    ))
                }
                _ => {
                    return Err(GraphPlanError::InvalidLogicalPlan(
                        QueryFailureReason::OrderWindow,
                        format!("{field} contains a non-arithmetic operator"),
                    ))
                }
            }
            .ok_or_else(|| {
                GraphPlanError::InvalidLogicalPlan(
                    QueryFailureReason::OrderWindow,
                    format!("{field} constant expression overflowed"),
                )
            })?;
            Ok((value, origin))
        }
        PhysicalExpression::Value(BoundValue {
            origin: ValueOrigin::Parameter(parameter),
            ..
        }) => Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            format!("{field} parameter ${parameter} must be an integer"),
        )),
        _ => Err(GraphPlanError::InvalidLogicalPlan(
            QueryFailureReason::OrderWindow,
            format!("{field} must be a constant integer"),
        )),
    }
}

fn estimate_rows(
    plan: &GraphLogicalPlan,
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    match plan {
        GraphLogicalPlan::Union { arms, .. } => {
            let total = arms.iter().try_fold(0_u64, |total, arm| {
                Some(total.saturating_add(estimate_rows(arm, context, statistics)?))
            })?;
            Some(total)
        }
        GraphLogicalPlan::NodeScan { labels, .. } => best_vertex_count(labels, statistics),
        GraphLogicalPlan::Filter { input, predicate } => {
            let input_rows = estimate_rows(input, context, statistics);
            let bound = bind_expression(predicate, context).ok()?;
            estimate_filter_rows(input, &bound, input_rows, statistics)
        }
        GraphLogicalPlan::Sort { input, .. } | GraphLogicalPlan::Project { input, .. } => {
            estimate_rows(input, context, statistics)
        }
        GraphLogicalPlan::Skip { input, count } => {
            let rows = estimate_rows(input, context, statistics)?;
            let skip = bind_window_count(count, "SKIP", context).ok()?;
            let ScalarValue::Integer(skip) = skip.value else {
                return None;
            };
            Some(rows.saturating_sub(u64::try_from(skip).ok()?))
        }
        GraphLogicalPlan::Limit { input, count } => {
            let rows = estimate_rows(input, context, statistics)?;
            let limit = bind_window_count(count, "LIMIT", context).ok()?;
            let ScalarValue::Integer(limit) = limit.value else {
                return None;
            };
            Some(rows.min(u64::try_from(limit).ok()?))
        }
        GraphLogicalPlan::Expand {
            input,
            relationship_types,
            direction,
            target_labels,
            ..
        } => estimate_expand_rows(
            input,
            relationship_types,
            *direction,
            target_labels,
            context,
            statistics,
        ),
        GraphLogicalPlan::VariableExpand {
            input,
            relationship_types,
            direction,
            target_labels,
            min_hops,
            max_hops,
            ..
        } => {
            let one_hop = estimate_expand_rows(
                input,
                relationship_types,
                *direction,
                target_labels,
                context,
                statistics,
            )?;
            let hops = u64::from(max_hops.saturating_sub(*min_hops).saturating_add(1));
            Some(one_hop.saturating_mul(hops))
        }
        GraphLogicalPlan::NaturalJoin { left, right, .. } => Some(
            estimate_rows(left, context, statistics)?
                .saturating_mul(estimate_rows(right, context, statistics)?),
        ),
    }
}

fn best_vertex_count(labels: &[Symbol], statistics: &dyn StatisticsProvider) -> Option<u64> {
    statistics.vertex_count(labels).or_else(|| {
        labels
            .iter()
            .filter_map(|label| statistics.vertex_count(std::slice::from_ref(label)))
            .min()
    })
}

fn property_equality_estimate(statistics: &crate::PropertyStatistics) -> Option<u64> {
    let count = statistics.non_null_count?;
    if count == 0 {
        return Some(0);
    }
    let distinct = statistics.distinct_count?;
    (distinct > 0).then(|| count.saturating_add(distinct - 1) / distinct)
}

fn property_values_estimate(
    labels: &[Symbol],
    property: &Symbol,
    values: &[BoundValue],
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let histogram_estimate = statistics
        .property_statistics(labels, property)
        .as_ref()
        .and_then(property_equality_estimate);
    values.iter().try_fold(0_u64, |total, value| {
        let estimate = statistics
            .property_value_count(labels, property, &value.value)
            .or_else(|| {
                statistics
                    .bloom_may_contain(labels, property, &value.value)
                    .and_then(|possibly_present| (!possibly_present).then_some(0))
            })
            .or(histogram_estimate)?;
        Some(total.saturating_add(estimate))
    })
}

fn prefer_index(index_rows: Option<u64>, scan_rows: Option<u64>) -> bool {
    match (index_rows, scan_rows) {
        (Some(index), Some(scan)) => index <= scan,
        (Some(_), None) | (None, _) => true,
    }
}

fn estimate_filter_rows(
    input: &GraphLogicalPlan,
    predicate: &PhysicalExpression,
    input_rows: Option<u64>,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    if let GraphLogicalPlan::NodeScan { binding, labels } = input {
        if let Some((constraint_binding, property, values)) = equality_constraint(predicate) {
            if constraint_binding == *binding {
                if let Some(rows) = property_values_estimate(labels, &property, &values, statistics)
                {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
        if let Some((constraint_binding, property)) = property_scan_constraint(predicate) {
            if constraint_binding == *binding {
                if let Some(rows) = statistics
                    .property_statistics(labels, &property)
                    .and_then(|stats| stats.non_null_count)
                {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
    }
    if let GraphLogicalPlan::Expand {
        relationship: Some(relationship),
        relationship_types,
        ..
    } = input
    {
        if let Some((binding, property, values)) =
            equality_constraint_for_binding(predicate, relationship)
        {
            if binding == *relationship {
                let histogram_estimate = statistics
                    .relationship_property_statistics(relationship_types, &property)
                    .as_ref()
                    .and_then(property_equality_estimate);
                let rows = values.iter().try_fold(0_u64, |total, value| {
                    let estimate = statistics
                        .relationship_property_value_count(
                            relationship_types,
                            &property,
                            &value.value,
                        )
                        .or_else(|| {
                            statistics
                                .relationship_bloom_may_contain(
                                    relationship_types,
                                    &property,
                                    &value.value,
                                )
                                .and_then(|possibly_present| (!possibly_present).then_some(0))
                        })
                        .or(histogram_estimate)?;
                    Some(total.saturating_add(estimate))
                });
                if let Some(rows) = rows {
                    return Some(input_rows.map_or(rows, |input| input.min(rows)));
                }
            }
        }
    }
    // Unknown selectivity is not evidence. Preserve the input estimate rather
    // than inventing a reduction that could make a scan appear cheaper than a
    // known index access.
    input_rows
}

fn estimate_expand_rows(
    input: &GraphLogicalPlan,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    _target_labels: &[Symbol],
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let input_rows = estimate_rows(input, context, statistics);
    if input_rows == Some(0) {
        return Some(0);
    }
    estimate_anchored_expansion_rows(input, relationship_types, direction, statistics)
        .or_else(|| statistics.relationship_count(relationship_types))
}

fn estimate_anchored_expansion_rows(
    input: &GraphLogicalPlan,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let source_labels = vertex_labels_for_plan(input)?;
    statistics.relationship_expansion_count(&source_labels, relationship_types, direction)
}

fn vertex_labels_for_plan(plan: &GraphLogicalPlan) -> Option<Vec<Symbol>> {
    match plan {
        GraphLogicalPlan::NodeScan { labels, .. } => Some(labels.clone()),
        GraphLogicalPlan::Filter { input, .. }
        | GraphLogicalPlan::Sort { input, .. }
        | GraphLogicalPlan::Skip { input, .. }
        | GraphLogicalPlan::Limit { input, .. }
        | GraphLogicalPlan::Project { input, .. } => vertex_labels_for_plan(input),
        _ => None,
    }
}

fn equality_constraint(
    predicate: &PhysicalExpression,
) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => property_value(left, right).or_else(|| property_value(right, left)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let (left_binding, left_property, left_values) = equality_constraint(left)?;
            let (right_binding, right_property, right_values) = equality_constraint(right)?;
            if left_binding != right_binding || left_property != right_property {
                return None;
            }
            let mut unique = BTreeSet::new();
            unique.extend(left_values);
            unique.extend(right_values);
            Some((left_binding, left_property, unique.into_iter().collect()))
        }
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => equality_constraint(left).or_else(|| equality_constraint(right)),
        // `x.p IN [a, b, c]` is the same candidate set as `x.p = a OR x.p = b
        // OR x.p = c`, so it reaches the existing multi-seek rather than
        // degrading to a scan. An empty list yields no constraint, which
        // leaves the residual filter to reject every row.
        PhysicalExpression::InList { expression, values } => {
            let (binding, property) = expression_property(expression)?;
            // Filter out NULL candidates: NULL cannot be a property seek key,
            // and Cypher's three-valued IN semantics already handle NULLs via
            // the residual predicate (x IN [1, NULL] → true when x=1, NULL
            // otherwise).
            let unique: BTreeSet<BoundValue> = values
                .iter()
                .filter(|v| !matches!(v.value, ScalarValue::Null))
                .cloned()
                .collect();
            if unique.is_empty() {
                return None;
            }
            Some((binding, property, unique.into_iter().collect()))
        }
        _ => None,
    }
}

fn equality_constraint_for_binding(
    predicate: &PhysicalExpression,
    wanted: &Symbol,
) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        return equality_constraint_for_binding(left, wanted)
            .or_else(|| equality_constraint_for_binding(right, wanted));
    }
    equality_constraint(predicate).filter(|(binding, ..)| binding == wanted)
}

fn property_scan_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, Symbol)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => property_scan_constraint(left).or_else(|| property_scan_constraint(right)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let left = property_scan_constraint(left)?;
            let right = property_scan_constraint(right)?;
            (left == right).then_some(left)
        }
        PhysicalExpression::Unary {
            operator: PhysicalUnaryOperator::Not,
            expression,
        } => property_scan_constraint(expression),
        PhysicalExpression::Binary {
            left,
            operator:
                PhysicalBinaryOperator::Equal
                | PhysicalBinaryOperator::NotEqual
                | PhysicalBinaryOperator::LessThan
                | PhysicalBinaryOperator::LessThanOrEqual
                | PhysicalBinaryOperator::GreaterThan
                | PhysicalBinaryOperator::GreaterThanOrEqual
                | PhysicalBinaryOperator::StartsWith,
            right,
        } => expression_property(left).or_else(|| expression_property(right)),
        // An `IN` that did not become a multi-seek still anchors a scan on the
        // property it tests, which beats falling through to an all-node scan.
        PhysicalExpression::InList { expression, .. } => expression_property(expression),
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Aggregate { .. }
        | PhysicalExpression::Unary { .. }
        | PhysicalExpression::Binary { .. } => None,
    }
}

fn expression_property(expression: &PhysicalExpression) -> Option<(Symbol, Symbol)> {
    let PhysicalExpression::Property { binding, property } = expression else {
        return None;
    };
    Some((binding.clone(), property.clone()))
}

fn identity_constraint(predicate: &PhysicalExpression) -> Option<(Symbol, BoundValue)> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => identity_value(left, right).or_else(|| identity_value(right, left)),
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::And,
            right,
        } => identity_constraint(left).or_else(|| identity_constraint(right)),
        _ => None,
    }
}

fn identity_value(
    identity: &PhysicalExpression,
    value: &PhysicalExpression,
) -> Option<(Symbol, BoundValue)> {
    let PhysicalExpression::Identity(binding) = identity else {
        return None;
    };
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    Some((binding.clone(), value.clone()))
}

fn property_value(
    property: &PhysicalExpression,
    value: &PhysicalExpression,
) -> Option<(Symbol, Symbol, Vec<BoundValue>)> {
    let PhysicalExpression::Property { binding, property } = property else {
        return None;
    };
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    (!matches!(value.value, ScalarValue::Null))
        .then(|| (binding.clone(), property.clone(), vec![value.clone()]))
}
