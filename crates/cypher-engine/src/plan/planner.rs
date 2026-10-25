use hydradb_cypher_ast::QueryFailureReason;
use std::collections::{BTreeMap, BTreeSet};

use super::lower::MAX_IN_LIST_VALUES;

use crate::model::compare_property_values;
use crate::{
    BoundValue, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError, GraphPlanResult,
    GuardedAccess, LogicalBinaryOperator, LogicalExpression, LogicalInList, LogicalProjection,
    PatternDirection, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection, PhysicalSort,
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
        GraphLogicalPlan::SingleRow => Ok(GraphPhysicalPlan::SingleRow),
        GraphLogicalPlan::Unwind {
            input,
            expression,
            binding,
        } => Ok(GraphPhysicalPlan::Unwind {
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            expression: bind_expression(expression, context)?,
            binding: binding.clone(),
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
        } => Ok(expand_or_type_probe(
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
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
        } => Ok(GraphPhysicalPlan::VariableExpand {
            input: Box::new(plan_physical_with_statistics(input, context, statistics)?),
            from: from.clone(),
            relationship: relationship.clone(),
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
/// ordered-string-index fast path expressed as a physical operator, extended
/// to every stored scalar type: the index walks booleans, numbers and strings
/// in the same total order `ORDER BY` uses, under
/// [`compare_property_values`](crate::compare_property_values).
///
/// Terms the index proves become exact restrictions; the rest stay as a
/// residual the scan evaluates on each walked row. The walk still needs
/// somewhere to start, so a predicate that leaves every term residual keeps
/// the generic plan.
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
    // A sort on a relationship property walks the relationship index instead,
    // so it is a different operator with the same shape. It takes the peeled
    // stack as one predicate, which is what it was before lowering split it.
    if matches!(scanned, GraphLogicalPlan::Expand { .. }) {
        let Some(predicate) = fold_logical_conjuncts(&predicates) else {
            return Ok(None);
        };
        return plan_ordered_relationship_window(scanned, &predicate, items, skip, limit, context);
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

/// The relationship counterpart of [`plan_ordered_window`]: rewrite
/// `LIMIT n` over `[SKIP s] ORDER BY r.p, ... FILTER(...) EXPAND(a)-[r:T]->(b)
/// FROM SCAN a` into a bounded ordered walk of the `T.p` relationship index.
///
/// Planned only where the walk yields exactly the expansion's rows: one
/// relationship type, a named relationship, a directed hop whose source is a
/// plain node scan of `a`, and a leading sort key on `r`. Any later sort keys
/// are allowed, because the executor reads the whole boundary tie of `r.p`
/// before it settles the window. Predicate tiers, the equality-anchor
/// exclusion and the start-point rule are the vertex walk's.
fn plan_ordered_relationship_window(
    expanded: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    items: &[crate::LogicalSort],
    skip: Option<BoundValue>,
    limit: &BoundValue,
    context: &PhysicalPlanningContext,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let GraphLogicalPlan::Expand {
        input: source,
        from,
        relationship: Some(relationship),
        relationship_types,
        direction,
        to,
        target_labels,
    } = expanded
    else {
        return Ok(None);
    };
    let [relationship_type] = relationship_types.as_slice() else {
        return Ok(None);
    };
    if *direction == PatternDirection::Undirected {
        return Ok(None);
    }
    let GraphLogicalPlan::NodeScan {
        binding: source_binding,
        labels: source_labels,
    } = source.as_ref()
    else {
        return Ok(None);
    };
    if source_binding != from || from == to {
        return Ok(None);
    }
    let items = items
        .iter()
        .map(|item| {
            Ok(PhysicalSort {
                expression: bind_expression(&item.expression, context)?,
                direction: item.direction,
            })
        })
        .collect::<GraphPlanResult<Vec<_>>>()?;
    let Some(PhysicalSort {
        expression:
            PhysicalExpression::Property {
                binding: sort_binding,
                property,
            },
        direction: sort_direction,
    }) = items.first()
    else {
        return Ok(None);
    };
    if sort_binding != relationship {
        return Ok(None);
    }
    let (property, sort_direction) = (property.clone(), *sort_direction);
    let bound_predicate = bind_expression(predicate, context)?;
    let Some((window, residual)) =
        ordered_window_restrictions(bound_predicate, relationship, &property)
    else {
        return Ok(None);
    };
    let skip_count = skip.as_ref().map(window_count_usize).unwrap_or(0);
    let Some(required) = window_count_usize(limit).checked_add(skip_count) else {
        return Ok(None);
    };
    let mut plan = GraphPhysicalPlan::OrderedRelationshipPropertyScan {
        from: from.clone(),
        source_labels: source_labels.clone(),
        relationship: relationship.clone(),
        relationship_type: relationship_type.clone(),
        direction: *direction,
        to: to.clone(),
        target_labels: target_labels.clone(),
        property,
        sort_direction,
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

/// The index restrictions and residual for an ordered walk of
/// `binding.property` under `predicate`, or `None` when the walk should not be
/// planned.
///
/// Shared by the vertex and relationship walks so one change of mind about
/// what the index may stand on reaches both.
fn ordered_window_restrictions(
    predicate: PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
) -> Option<(OrderedWindowPredicate, Option<PhysicalExpression>)> {
    let mut window = OrderedWindowPredicate::default();
    let mut residual = Vec::new();
    collect_ordered_window_predicate(&predicate, binding, property, &mut window, &mut residual);
    // An equality on the sorted property names the exact values wanted, and a
    // seek on them returns those rows already in order.
    if residual.iter().any(|conjunct| {
        matches!(
            equality_constraint(conjunct),
            Some((constraint_binding, constraint_property, _))
                if constraint_binding == *binding && constraint_property == *property
        )
    }) {
        return None;
    }
    // Every term went to the residual, so the index bounds nothing: that is a
    // full scan wearing an operator's name.
    if window.prefix.is_none() && window.lower.is_none() && window.upper.is_none() {
        return None;
    }
    Some((window, fold_conjuncts(residual)))
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

/// The same fold before binding, for a path that hands its predicate on as one
/// logical expression rather than binding each conjunct itself.
fn fold_logical_conjuncts(conjuncts: &[&LogicalExpression]) -> Option<LogicalExpression> {
    conjuncts
        .iter()
        .rev()
        .copied()
        .cloned()
        .reduce(|left, right| LogicalExpression::Binary {
            left: Box::new(left),
            operator: crate::LogicalBinaryOperator::And,
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
        // An `Or` stays residual -- neither side restricts on its own -- but a
        // bound every branch implies is still sound for the walk to start
        // from. This is the keyset shape `p > x OR (p = x AND id > y)`, and
        // its descending mirror `p < x OR (p = x AND id < y)`, so both ends
        // are tried: a descending walk starts from the upper bound.
        for lower in [true, false] {
            let slot = if lower {
                &mut window.lower
            } else {
                &mut window.upper
            };
            if slot.is_none() {
                if let Some(bound) = implied_window_bound(predicate, binding, property, lower) {
                    *slot = Some(bound);
                }
            }
        }
        residual.push(predicate.clone());
    }
}

/// A non-null constant operand of an ordered-window comparison.
///
/// `NULL` is excluded because the walk skips vertices without the property,
/// and a null bound would compare above every value under
/// [`compare_property_values`](crate::compare_property_values).
///
/// A negative integer literal lowers as unary minus over its magnitude and
/// folds here; the executor does not negate floats, so a negated float stays
/// an expression and the generic plan keeps reporting it.
fn ordered_window_operand(expression: &PhysicalExpression) -> Option<BoundValue> {
    match expression {
        PhysicalExpression::Value(value) if !matches!(value.value, ScalarValue::Null) => {
            Some(value.clone())
        }
        PhysicalExpression::Unary {
            operator: PhysicalUnaryOperator::Minus,
            expression,
        } => match expression.as_ref() {
            PhysicalExpression::Value(BoundValue {
                value: ScalarValue::Integer(value),
                origin,
            }) => value.checked_neg().map(|value| BoundValue {
                value: ScalarValue::Integer(value),
                origin: origin.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

/// The lowest (`lower`) or highest `(value, inclusive)` of `property` that
/// `predicate` can accept, when it proves one. `Or` takes the weaker branch,
/// `And` the stronger.
///
/// The bound only has to be *sound* -- no satisfying row may fall outside it
/// -- not tight, because the predicate stays as a residual that decides every
/// row the walk returns.
fn implied_window_bound(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    property: &Symbol,
    lower: bool,
) -> Option<(BoundValue, bool)> {
    let PhysicalExpression::Binary {
        left,
        operator,
        right,
    } = predicate
    else {
        return None;
    };
    // Ordered by the same total order the index walks, so a bound on any
    // stored scalar type participates, not only a string one.
    let order = |left: &(BoundValue, bool), right: &(BoundValue, bool)| {
        compare_property_values(&left.0.value, &right.0.value)
    };
    match operator {
        // Either branch may be the one that matches, so only a bound both
        // imply is sound -- the weaker of the two.
        PhysicalBinaryOperator::Or => {
            let left = implied_window_bound(left, binding, property, lower)?;
            let right = implied_window_bound(right, binding, property, lower)?;
            let weaker = if lower {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
            Some(match order(&left, &right) {
                ordering if ordering == weaker => left,
                std::cmp::Ordering::Equal => (left.0, left.1 || right.1),
                _ => right,
            })
        }
        PhysicalBinaryOperator::And => {
            match (
                implied_window_bound(left, binding, property, lower),
                implied_window_bound(right, binding, property, lower),
            ) {
                // Both branches hold, so the stronger bound is sound.
                (Some(left), Some(right)) => {
                    let stronger = if lower {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                    Some(match order(&left, &right) {
                        ordering if ordering == stronger => left,
                        std::cmp::Ordering::Equal => (left.0, left.1 && right.1),
                        _ => right,
                    })
                }
                (bound @ Some(_), None) | (None, bound @ Some(_)) => bound,
                (None, None) => None,
            }
        }
        _ => {
            let is_target = |expression: &PhysicalExpression| {
                matches!(
                    expression,
                    PhysicalExpression::Property { binding: b, property: p }
                        if b == binding && p == property
                )
            };
            let (value, operator) = if is_target(left) {
                (ordered_window_operand(right)?, *operator)
            } else if is_target(right) {
                let flipped = match operator {
                    PhysicalBinaryOperator::LessThan => PhysicalBinaryOperator::GreaterThan,
                    PhysicalBinaryOperator::LessThanOrEqual => {
                        PhysicalBinaryOperator::GreaterThanOrEqual
                    }
                    PhysicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::LessThan,
                    PhysicalBinaryOperator::GreaterThanOrEqual => {
                        PhysicalBinaryOperator::LessThanOrEqual
                    }
                    PhysicalBinaryOperator::Equal => PhysicalBinaryOperator::Equal,
                    _ => return None,
                };
                (ordered_window_operand(left)?, flipped)
            } else {
                return None;
            };
            match (operator, lower) {
                (PhysicalBinaryOperator::GreaterThan, true)
                | (PhysicalBinaryOperator::LessThan, false) => Some((value, false)),
                (PhysicalBinaryOperator::GreaterThanOrEqual, true)
                | (PhysicalBinaryOperator::LessThanOrEqual, false)
                | (PhysicalBinaryOperator::Equal, _) => Some((value, true)),
                _ => None,
            }
        }
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
    // Any non-null operand: a range comparison on a stored value decides
    // exactly what the index order does. Only a prefix needs a string.
    let scalar_value = ordered_window_operand;
    // Normalize `value OP property` into `property OP' value`.
    let (value, operator) = if is_target(left) {
        (scalar_value(right), *operator)
    } else if is_target(right) {
        let flipped = match operator {
            PhysicalBinaryOperator::LessThan => PhysicalBinaryOperator::GreaterThan,
            PhysicalBinaryOperator::LessThanOrEqual => PhysicalBinaryOperator::GreaterThanOrEqual,
            PhysicalBinaryOperator::GreaterThan => PhysicalBinaryOperator::LessThan,
            PhysicalBinaryOperator::GreaterThanOrEqual => PhysicalBinaryOperator::LessThanOrEqual,
            // `'x' STARTS WITH n.p` is not an index restriction on n.p.
            _ => return false,
        };
        (scalar_value(left), flipped)
    } else {
        return false;
    };
    let Some(value) = value else {
        return false;
    };
    let slot = match operator {
        PhysicalBinaryOperator::StartsWith if matches!(value.value, ScalarValue::String(_)) => {
            &mut window.prefix
        }
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
    if statistics.ranks_access_candidates() {
        if let Some(plan) = plan_ranked_filter_stack(input, predicate, context, statistics)? {
            return Ok(plan);
        }
    }
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
            // One value seeks once; several -- `r.p = $a OR r.p = $b`,
            // `r.p IN $list` -- seek once per value, under the cap an `IN`
            // list already has, so an `OR` chain cannot fan out further than
            // a list may.
            if source_binding == from
                && constraint_binding == *relationship
                && !values.is_empty()
                && values.len() <= MAX_IN_LIST_VALUES
            {
                let seek_rows = relationship_values_estimate(
                    relationship_types,
                    &property,
                    &values,
                    statistics,
                );
                let seek = if values.len() == 1 {
                    GraphPhysicalPlan::RelationshipPropertySeek {
                        from: from.clone(),
                        source_labels: source_labels.clone(),
                        relationship: relationship.clone(),
                        relationship_type: relationship_type.clone(),
                        property,
                        value: values.into_iter().next().expect("one value"),
                        direction: *direction,
                        to: to.clone(),
                        target_labels: target_labels.clone(),
                    }
                } else {
                    GraphPhysicalPlan::RelationshipPropertyMultiSeek {
                        from: from.clone(),
                        source_labels: source_labels.clone(),
                        relationship: relationship.clone(),
                        relationship_type: relationship_type.clone(),
                        property,
                        values,
                        direction: *direction,
                        to: to.clone(),
                        target_labels: target_labels.clone(),
                    }
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
                return Ok(filter_over(access, bound_predicate));
            }
        }
        if let (
            Some(relationship),
            [relationship_type],
            GraphLogicalPlan::NodeScan {
                binding: source_binding,
                labels: source_labels,
            },
        ) = (relationship, relationship_types.as_slice(), source.as_ref())
        {
            // The probe never hands the filter a relationship below the
            // bound, and evaluation is eager (see `filter_over`): a residual
            // conjunct that errors on such a row -- `1 / r.timestamp` at zero
            // -- would turn the expansion's error into an empty result. Only
            // a predicate that cannot fail may lose those rows unseen.
            if source_binding == from && total_condition(&bound_predicate) {
                if let Some((property, lower, inclusive)) =
                    integer_relationship_lower_bound(&bound_predicate, relationship)
                {
                    let fallback = physical_expand(
                        plan_physical_with_statistics(source, context, statistics)?,
                        from,
                        &Some(relationship.clone()),
                        relationship_types,
                        *direction,
                        to,
                        target_labels,
                    );
                    return Ok(filter_over(
                        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
                            from: from.clone(),
                            source_labels: source_labels.clone(),
                            relationship: relationship.clone(),
                            relationship_type: relationship_type.clone(),
                            property,
                            lower,
                            inclusive,
                            direction: *direction,
                            to: to.clone(),
                            target_labels: target_labels.clone(),
                            fallback: Box::new(fallback),
                        },
                        bound_predicate,
                    ));
                }
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
            legacy_node_scan_access(binding, labels, &bound_predicate, statistics)
        }
        _ => plan_physical_with_statistics(input, context, statistics)?,
    };
    Ok(filter_over(access, bound_predicate))
}

/// The access path for `binding` under `predicate` without ranking: an
/// identity seek, else the **first** equality when the index does not look
/// costlier than the label, else a property scan, else the label. This is the
/// planner as it was before statistics could rank candidates, and stays the
/// plan for every provider that does not opt in.
fn legacy_node_scan_access(
    binding: &Symbol,
    labels: &[Symbol],
    bound_predicate: &PhysicalExpression,
    statistics: &dyn StatisticsProvider,
) -> GraphPhysicalPlan {
    let scan = fallback_scan(binding, labels, statistics);
    let scan_rows = best_vertex_count(labels, statistics);
    match identity_constraint(bound_predicate) {
        Some((constraint_binding, value)) if constraint_binding == *binding => {
            GraphPhysicalPlan::VertexIdSeek {
                binding: binding.clone(),
                labels: labels.to_vec(),
                value,
            }
        }
        _ => match equality_constraint(bound_predicate) {
            Some((constraint_binding, property, values)) if constraint_binding == *binding => {
                let seek_rows = property_values_estimate(labels, &property, &values, statistics);
                if !prefer_index(seek_rows, scan_rows) {
                    scan
                } else {
                    property_seek(binding, labels, property, values)
                }
            }
            _ => match property_scan_constraint(bound_predicate) {
                Some((constraint_binding, property)) if constraint_binding == *binding => {
                    let property_rows = statistics
                        .property_statistics(labels, &property)
                        .and_then(|stats| stats.non_null_count);
                    if prefer_index(property_rows, scan_rows) {
                        GraphPhysicalPlan::VertexPropertyScan {
                            binding: binding.clone(),
                            labels: labels.to_vec(),
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

fn property_seek(
    binding: &Symbol,
    labels: &[Symbol],
    property: Symbol,
    values: Vec<BoundValue>,
) -> GraphPhysicalPlan {
    if values.len() == 1 {
        GraphPhysicalPlan::VertexPropertySeek {
            binding: binding.clone(),
            labels: labels.to_vec(),
            property,
            value: values.into_iter().next().expect("one value"),
        }
    } else {
        GraphPhysicalPlan::VertexPropertyMultiSeek {
            binding: binding.clone(),
            labels: labels.to_vec(),
            property,
            values,
        }
    }
}

/// Index entries a statistics-chosen access may read for each one it
/// expects, before it declines to its fallback.
const GUARD_READ_FACTOR: u64 = 10;
/// The smallest read limit, so a stale estimate of zero still reads a page.
/// Equal to the runtime probe's entry budget.
const GUARD_MIN_READ_LIMIT: u64 = 256;
/// The largest read limit for a range seek. Its reads are bounded by this
/// limit rather than charged to the query's shared index-candidate tally, so
/// this is what bounds the entries a declined walk costs before the fallback
/// runs. Well below the default cap, which the backend no longer enforces on
/// this read; the executor also clamps it under a lower configured cap.
const RANGE_SEEK_MAX_READ_LIMIT: u64 = 50_000;

/// Plan `Filter* (NodeScan)` by ranking every equality and range on the
/// binding, with a provider that opts in to ranking. `None` when the stack is
/// not over a NodeScan, leaving it to the unranked planner; a stack ranking
/// declines is planned unranked here, as one.
///
/// Lowering splits one pattern's predicate into stacked Filters -- the inline
/// map inside, `WHERE` outside -- and the unranked planner chooses the access
/// from the innermost alone. Ranking sees the outer layers too, but only
/// those [`filter_over`] would merge: a layer that is not a
/// [`total_condition`] stays a separate Filter, and so does everything above
/// it, because narrowing the read by an outer conjunct would skip evaluating
/// the layer below it on rows it would have failed on. The physical Filter
/// stack is rebuilt exactly as the unranked planner builds it; only the
/// access underneath differs.
fn plan_ranked_filter_stack(
    input: &GraphLogicalPlan,
    predicate: &LogicalExpression,
    context: &PhysicalPlanningContext,
    statistics: &dyn StatisticsProvider,
) -> GraphPlanResult<Option<GraphPhysicalPlan>> {
    let mut layers = vec![predicate];
    let mut scanned = input;
    while let GraphLogicalPlan::Filter { input, predicate } = scanned {
        layers.push(predicate);
        scanned = input.as_ref();
    }
    let GraphLogicalPlan::NodeScan { binding, labels } = scanned else {
        return Ok(None);
    };
    // Bound outermost first, the order the unranked planner binds them in, so
    // a missing parameter is reported the same way either way.
    let mut bound = layers
        .into_iter()
        .map(|layer| bind_expression(layer, context))
        .collect::<GraphPlanResult<Vec<_>>>()?;
    bound.reverse();
    let peeled = 1 + bound[1..]
        .iter()
        .take_while(|layer| total_condition(layer))
        .count();
    let candidates = fold_conjuncts(bound[..peeled].to_vec()).expect("at least one layer");
    let Some(access) = plan_ranked_node_scan(binding, labels, &candidates, statistics) else {
        // Plan the whole stack unranked here rather than returning `None`:
        // the caller would recurse into the inner layers with ranking still
        // on, and those see only a subset of the conjuncts, so they could
        // rank the very equalities this call declined over -- an inline map's
        // tenant filters without the `WHERE` property nobody estimated. Built
        // as the unranked planner builds it, outermost layer last.
        let mut bound = bound.into_iter();
        let innermost = bound.next().expect("innermost layer");
        let access = legacy_node_scan_access(binding, labels, &innermost, statistics);
        let mut plan = filter_over(access, innermost);
        for layer in bound {
            plan = filter_over(plan, layer);
        }
        return Ok(Some(plan));
    };
    let mut bound = bound.into_iter();
    let mut plan = GraphPhysicalPlan::Filter {
        input: Box::new(access),
        predicate: bound.next().expect("innermost layer"),
    };
    for layer in bound {
        plan = filter_over(plan, layer);
    }
    Ok(Some(plan))
}

/// One way into a vertex population, with what statistics expect it to read.
enum AccessCandidate {
    Equality {
        property: Symbol,
        values: Vec<BoundValue>,
    },
    Range {
        property: Symbol,
        lower: Option<(BoundValue, bool)>,
        upper: Option<(BoundValue, bool)>,
    },
}

impl AccessCandidate {
    /// Equalities win ties: a seek reads exactly its values' entries, while a
    /// range estimate assumes values spread evenly between min and max.
    fn kind_rank(&self) -> u8 {
        match self {
            Self::Equality { .. } => 0,
            Self::Range { .. } => 1,
        }
    }
}

/// Rank the equalities and ranges on `binding` against each other and the
/// label, and start from the smallest. `None` when statistics estimate no
/// candidate, or cannot estimate one of the seekable equalities, so the
/// caller plans as it would without them.
///
/// The predicate is never reduced: the Filter above the access still applies
/// all of it, so a stale estimate can change what is read, never what is
/// returned.
fn plan_ranked_node_scan(
    binding: &Symbol,
    labels: &[Symbol],
    predicate: &PhysicalExpression,
    statistics: &dyn StatisticsProvider,
) -> Option<GraphPhysicalPlan> {
    if let Some((constraint_binding, value)) = identity_constraint(predicate) {
        if constraint_binding == *binding {
            return Some(GraphPhysicalPlan::VertexIdSeek {
                binding: binding.clone(),
                labels: labels.to_vec(),
                value,
            });
        }
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(predicate, &mut conjuncts);

    let mut candidates: Vec<(u64, AccessCandidate)> = Vec::new();
    let mut equality_properties = BTreeSet::new();
    for conjunct in &conjuncts {
        let Some((constraint_binding, property, values)) = equality_constraint(conjunct) else {
            continue;
        };
        if constraint_binding != *binding {
            continue;
        }
        equality_properties.insert(property.clone());
        // No index key holds NaN, an infinity or an integer past i64/u64 a
        // seek could find, and a backend may refuse to encode one; the Filter
        // answers it instead. Skipped rather than declined over, since the
        // unranked planner would seek it first if written first.
        if !values.iter().all(|value| seekable_value(&value.value)) {
            continue;
        }
        // An equality statistics cannot estimate may be the narrowest one, and
        // ranking only the rest would run a known-broad seek without the
        // runtime probe that could have found it. Leave the binding to the
        // unranked planner, whose probe weighs every equality as it reads.
        let estimate = property_values_estimate(labels, &property, &values, statistics)?;
        candidates.push((estimate, AccessCandidate::Equality { property, values }));
    }
    // A range on a property an equality already restricts reads at least the
    // equality's entries, so only the equality is offered for it.
    let mut range_properties = Vec::new();
    for conjunct in &conjuncts {
        if let Some(property) = range_comparison_property(conjunct, binding) {
            if !equality_properties.contains(&property) && !range_properties.contains(&property) {
                range_properties.push(property);
            }
        }
    }
    for property in range_properties {
        // Two bounds on one property fill one window, so `p >= a AND p < b`
        // is one candidate with both ends set.
        let mut window = OrderedWindowPredicate::default();
        let mut residual = Vec::new();
        collect_ordered_window_predicate(predicate, binding, &property, &mut window, &mut residual);
        if window.lower.is_none() && window.upper.is_none() {
            continue;
        }
        // Unlike an equality, a range with no estimate is only skipped: the
        // unranked planner never seeks a range and the probe never weighs
        // one, so declining to rank would not let anything find it narrow.
        if let Some(estimate) = range_estimate(
            labels,
            &property,
            window.lower.as_ref(),
            window.upper.as_ref(),
            statistics,
        ) {
            candidates.push((
                estimate,
                AccessCandidate::Range {
                    property,
                    lower: window.lower,
                    upper: window.upper,
                },
            ));
        }
    }
    let (estimate, best) = candidates
        .into_iter()
        .enumerate()
        .min_by_key(|(order, (estimate, candidate))| (*estimate, candidate.kind_rank(), *order))
        .map(|(_, candidate)| candidate)?;
    // Whatever statistics choose declines to the plan chosen without them,
    // which the enclosing Filter may still probe: a wrong estimate is never
    // worse than today.
    let fallback = || {
        Box::new(legacy_node_scan_access(
            binding,
            labels,
            predicate,
            &UnknownStatistics,
        ))
    };
    if let Some(label_rows) =
        best_vertex_count(labels, statistics).filter(|label_rows| estimate > *label_rows)
    {
        return Some(match fallback_scan(binding, labels, statistics) {
            GraphPhysicalPlan::VertexLabelScan { label, labels, .. } => {
                let estimated_rows = statistics
                    .vertex_count(std::slice::from_ref(&label))
                    .unwrap_or(label_rows);
                GraphPhysicalPlan::VertexGuardedAccess {
                    binding: binding.clone(),
                    labels,
                    access: GuardedAccess::LabelScan { label },
                    estimated_rows,
                    read_limit: guard_read_limit(estimated_rows),
                    fallback: fallback(),
                }
            }
            // Unreachable while only a labelled binding has a count, and
            // harmless if not: the unguarded scan is what step 3 planned.
            other => other,
        });
    }
    Some(match best {
        AccessCandidate::Equality { property, values } => GraphPhysicalPlan::VertexGuardedAccess {
            binding: binding.clone(),
            labels: labels.to_vec(),
            access: GuardedAccess::PropertySeek { property, values },
            estimated_rows: estimate,
            read_limit: guard_read_limit(estimate),
            fallback: fallback(),
        },
        AccessCandidate::Range {
            property,
            lower,
            upper,
        } => GraphPhysicalPlan::VertexPropertyRangeSeek {
            binding: binding.clone(),
            labels: labels.to_vec(),
            property,
            lower,
            upper,
            estimated_rows: estimate,
            read_limit: range_read_limit(estimate),
            fallback: fallback(),
        },
    })
}

/// The guard on a statistics-chosen equality seek or label scan: ten times
/// the estimate, at least [`GUARD_MIN_READ_LIMIT`]. No upper bound here; the
/// backend caps it at what one query may read from an index, so an
/// over-large estimate declines rather than fails.
fn guard_read_limit(estimate: u64) -> usize {
    let limit = estimate
        .saturating_mul(GUARD_READ_FACTOR)
        .max(GUARD_MIN_READ_LIMIT);
    usize::try_from(limit).unwrap_or(usize::MAX)
}

fn range_read_limit(estimate: u64) -> usize {
    let limit = estimate
        .saturating_mul(GUARD_READ_FACTOR)
        .clamp(GUARD_MIN_READ_LIMIT, RANGE_SEEK_MAX_READ_LIMIT);
    usize::try_from(limit).unwrap_or(usize::MAX)
}

/// The `And`-separated conjuncts of `predicate`, left to right.
fn collect_conjuncts<'a>(
    predicate: &'a PhysicalExpression,
    conjuncts: &mut Vec<&'a PhysicalExpression>,
) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_conjuncts(left, conjuncts);
        collect_conjuncts(right, conjuncts);
    } else {
        conjuncts.push(predicate);
    }
}

/// The property of `binding` a conjunct bounds with `<`, `<=`, `>` or `>=`
/// against a constant, in either operand order.
fn range_comparison_property(conjunct: &PhysicalExpression, binding: &Symbol) -> Option<Symbol> {
    let PhysicalExpression::Binary {
        left,
        operator,
        right,
    } = conjunct
    else {
        return None;
    };
    if !matches!(
        operator,
        PhysicalBinaryOperator::LessThan
            | PhysicalBinaryOperator::LessThanOrEqual
            | PhysicalBinaryOperator::GreaterThan
            | PhysicalBinaryOperator::GreaterThanOrEqual
    ) {
        return None;
    }
    let (property, operand) = match (left.as_ref(), right.as_ref()) {
        (
            PhysicalExpression::Property {
                binding: b,
                property,
            },
            operand,
        ) if b == binding => (property, operand),
        (
            operand,
            PhysicalExpression::Property {
                binding: b,
                property,
            },
        ) if b == binding => (property, operand),
        _ => return None,
    };
    ordered_window_operand(operand).map(|_| property.clone())
}

/// Index entries a range on `property` reads: the share of the numeric domain
/// the bounds keep, times the numeric entries that domain spans (the
/// uniform-spread rule). The property's other entries -- strings, booleans --
/// lie outside it and are no part of the share.
///
/// `None` -- not a candidate -- when the share is unknown: no statistics, a
/// string or boolean bound, or a property with no numeric entries. Treating
/// an unknown share as "keeps everything" could only make the range as large
/// as the property, which never beats the index it would replace.
///
/// A bound open on one side also walks the other types on that side -- the
/// index orders booleans below numbers and strings above them -- so those
/// entries are added rather than assumed away. The Filter rejects them, but
/// the walk still reads them.
fn range_estimate(
    labels: &[Symbol],
    property: &Symbol,
    lower: Option<&(BoundValue, bool)>,
    upper: Option<&(BoundValue, bool)>,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let total = statistics
        .property_statistics(labels, property)?
        .non_null_count?;
    let range = statistics.property_numeric_range(labels, property)?;
    let lower = match lower {
        Some((value, inclusive)) => Some((numeric_bound(value)?, *inclusive)),
        None => None,
    };
    let upper = match upper {
        Some((value, inclusive)) => Some((numeric_bound(value)?, *inclusive)),
        None => None,
    };
    let share = numeric_share(range, lower, upper)?;
    let numeric_entries = range.entries.min(total);
    let numeric = ((share * numeric_entries as f64).ceil() as u64).min(numeric_entries);
    let other_types = if lower.is_none() || upper.is_none() {
        total.saturating_sub(range.entries)
    } else {
        0
    };
    Some(numeric.saturating_add(other_types).min(total))
}

/// Whether an equality on `value` may become a statistics-chosen seek: any
/// value but a float that is not finite or an integer outside i64/u64, the
/// widest an index key holds.
fn seekable_value(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float(value) => value.parse::<f64>().is_ok_and(f64::is_finite),
        ScalarValue::Integer(value) => {
            (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(value)
        }
        ScalarValue::Null | ScalarValue::Boolean(_) | ScalarValue::String(_) => true,
    }
}

/// A finite numeric bound as `f64`; anything else is a type the numeric
/// domain cannot place.
fn numeric_bound(value: &BoundValue) -> Option<f64> {
    let number = match &value.value {
        ScalarValue::Integer(value) => *value as f64,
        ScalarValue::Float(value) => value.parse::<f64>().ok()?,
        ScalarValue::Null | ScalarValue::Boolean(_) | ScalarValue::String(_) => return None,
    };
    number.is_finite().then_some(number)
}

/// The share of `[min, max]` that `[lower, upper]` keeps, clamped to `0..=1`.
fn numeric_share(
    range: crate::PropertyNumericRange,
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Option<f64> {
    let (min, max) = (range.min, range.max);
    if !(min.is_finite() && max.is_finite()) || min > max {
        return None;
    }
    if min == max {
        let above =
            lower.is_none_or(|(bound, inclusive)| min > bound || (inclusive && min == bound));
        let below =
            upper.is_none_or(|(bound, inclusive)| min < bound || (inclusive && min == bound));
        return Some(if above && below { 1.0 } else { 0.0 });
    }
    let low = lower.map_or(min, |(bound, _)| bound.max(min));
    let high = upper.map_or(max, |(bound, _)| bound.min(max));
    Some(((high - low) / (max - min)).clamp(0.0, 1.0))
}

/// An AND term may provide a safe lower bound even when other terms (for
/// example `IS NULL OR = ''`) stay as residual predicates. Do not infer a
/// bound through OR: either branch could admit a value below it.
fn integer_relationship_lower_bound(
    predicate: &PhysicalExpression,
    relationship: &Symbol,
) -> Option<(Symbol, BoundValue, bool)> {
    let PhysicalExpression::Binary {
        left,
        operator,
        right,
    } = predicate
    else {
        return None;
    };
    if *operator == PhysicalBinaryOperator::And {
        return integer_relationship_lower_bound(left, relationship)
            .or_else(|| integer_relationship_lower_bound(right, relationship));
    }
    let property = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Property { binding, property } if binding == relationship => {
            Some(property.clone())
        }
        _ => None,
    };
    let integer = |expression: &PhysicalExpression| match expression {
        PhysicalExpression::Value(value) if matches!(value.value, ScalarValue::Integer(_)) => {
            Some(value.clone())
        }
        _ => None,
    };
    match operator {
        PhysicalBinaryOperator::GreaterThan | PhysicalBinaryOperator::GreaterThanOrEqual => Some((
            property(left)?,
            integer(right)?,
            *operator == PhysicalBinaryOperator::GreaterThanOrEqual,
        )),
        PhysicalBinaryOperator::LessThan | PhysicalBinaryOperator::LessThanOrEqual => Some((
            property(right)?,
            integer(left)?,
            *operator == PhysicalBinaryOperator::LessThanOrEqual,
        )),
        _ => None,
    }
}

/// A `Filter` over `access`, merged into it when `access` is already one.
///
/// Two logical filters over one access -- a pattern's inline equalities and a
/// `WHERE` clause naming the same binding -- otherwise plan as a stack. That
/// costs a second pass over every row, and it hides the outer predicate from
/// `probe_replaceable_access`, which looks through a filter only to a bare
/// access: a narrow equality in the outer filter never reaches the probe, so
/// the query stays anchored on whichever property the planner picked. Merging
/// is also what keeps the probe's replacement sound, because the single
/// predicate it re-applies is the conjunction the stack enforced.
///
/// Only when the outer predicate cannot fail. Evaluation is eager -- `Binary`
/// evaluates both operands before combining them -- so a conjunction does not
/// short-circuit. A stack discards a row the inner predicate rejects before the
/// outer one is ever evaluated on it; a merged `And` evaluates both, so an outer
/// predicate that errors on such a row would fail a query that used to succeed.
/// `total_condition` is what rules that out, and a predicate it rejects keeps
/// the stack and forgoes the probe.
///
/// The inner predicate stays on the left so the merged predicate reads, and
/// explains, in the order the stack enforced.
fn filter_over(access: GraphPhysicalPlan, predicate: PhysicalExpression) -> GraphPhysicalPlan {
    match access {
        GraphPhysicalPlan::Filter {
            input,
            predicate: inner,
        } if total_condition(&predicate) => GraphPhysicalPlan::Filter {
            input,
            predicate: PhysicalExpression::Binary {
                left: Box::new(inner),
                operator: PhysicalBinaryOperator::And,
                right: Box::new(predicate),
            },
        },
        access => GraphPhysicalPlan::Filter {
            input: Box::new(access),
            predicate,
        },
    }
}

/// Whether evaluating `expression` as a condition can only yield a truth value,
/// never an error.
///
/// Comparisons yield unknown for operands that do not compare, and the null
/// tests are total by definition. Arithmetic is not: it divides by zero and
/// overflows. `STARTS WITH` is not: it rejects a non-string. `Not`, `And` and
/// `Or` are total only over operands that are themselves truth-valued.
fn total_condition(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => match operator {
            PhysicalBinaryOperator::Equal
            | PhysicalBinaryOperator::NotEqual
            | PhysicalBinaryOperator::LessThan
            | PhysicalBinaryOperator::LessThanOrEqual
            | PhysicalBinaryOperator::GreaterThan
            | PhysicalBinaryOperator::GreaterThanOrEqual => total_value(left) && total_value(right),
            PhysicalBinaryOperator::And | PhysicalBinaryOperator::Or => {
                total_condition(left) && total_condition(right)
            }
            _ => false,
        },
        PhysicalExpression::Unary {
            operator,
            expression,
        } => match operator {
            PhysicalUnaryOperator::IsNull | PhysicalUnaryOperator::IsNotNull => {
                total_value(expression)
            }
            PhysicalUnaryOperator::Not => total_condition(expression),
            PhysicalUnaryOperator::Plus | PhysicalUnaryOperator::Minus => false,
        },
        PhysicalExpression::InList { expression, .. } => total_value(expression),
        _ => false,
    }
}

/// Whether evaluating `expression` as a value can only yield a value, never an
/// error. Only the leaves qualify: every compound value expression is either
/// arithmetic or an aggregate.
fn total_value(expression: &PhysicalExpression) -> bool {
    matches!(
        expression,
        PhysicalExpression::Property { .. }
            | PhysicalExpression::Value(_)
            | PhysicalExpression::Binding(_)
            | PhysicalExpression::Identity(_)
    )
}

/// Enter through the relationship type instead of the anchor label, when the
/// anchor is a bare scan.
///
/// A bare scan means no index narrowed the anchor, so the expansion visits the
/// whole label population and pays a neighbour read per vertex to discover that
/// most of them have no edge of this type. Entering through the type instead
/// costs one read per edge that exists.
///
/// No statistic decides this, because none is needed and none may be available:
/// the probe measures the type at execution time and declines a type too broad
/// to read whole, falling back to exactly the plan this replaced. The cost of
/// guessing wrong is therefore bounded by the probe's own entry budget, which is
/// why this is emitted whenever it *could* pay rather than when statistics say
/// it will.
fn expand_or_type_probe(
    input: GraphPhysicalPlan,
    from: &Symbol,
    relationship: &Option<Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
) -> GraphPhysicalPlan {
    // A narrowed anchor is already cheap to expand from, and the probe would
    // read the whole type to serve a handful of rows. Only a bare scan is worth
    // replacing.
    let source_labels = match &input {
        GraphPhysicalPlan::VertexLabelScan { labels, .. } => labels.clone(),
        GraphPhysicalPlan::AllVertexScan { .. } => Vec::new(),
        _ => {
            return physical_expand(
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
            );
        }
    };
    // One named type and one relationship binding: the probe reads a single
    // type's key space, and it binds the relationship on every row it returns
    // rather than optionally.
    let ([relationship_type], Some(relationship_binding)) = (relationship_types, relationship)
    else {
        return physical_expand(
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        );
    };
    // Undirected is deferred, not unsupported: the storage walk is orientation
    // agnostic and `relationship_seek_rows` already yields both assignments, but
    // the surrounding undirected semantics are in flight and this is not the
    // change that should settle them.
    if matches!(direction, PatternDirection::Undirected) {
        return physical_expand(
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        );
    }
    let relationship_type = relationship_type.clone();
    let relationship_binding = relationship_binding.clone();
    let fallback = physical_expand(
        input,
        from,
        relationship,
        relationship_types,
        direction,
        to,
        target_labels,
    );
    GraphPhysicalPlan::RelationshipTypeProbe {
        from: from.clone(),
        source_labels,
        relationship: relationship_binding,
        relationship_type,
        direction,
        to: to.clone(),
        target_labels: target_labels.to_vec(),
        fallback: Box::new(fallback),
    }
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
        LogicalExpression::Function { arguments, .. } | LogicalExpression::List(arguments) => {
            for argument in arguments {
                collect_expression_bindings(argument, bindings);
            }
        }
        LogicalExpression::Case { .. } => {
            for part in super::lower::case_parts(expression) {
                collect_expression_bindings(part, bindings);
            }
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
            if let Some(value) = context.parameters.get(parameter) {
                PhysicalExpression::Value(BoundValue {
                    value: value.clone(),
                    origin: ValueOrigin::Parameter(parameter.clone()),
                })
            } else if let Some(values) = context.list_parameters.get(parameter) {
                // A list parameter read as a value -- `UNWIND $values`,
                // `RETURN $values` -- is the list of its elements.
                PhysicalExpression::List(
                    values
                        .iter()
                        .map(|value| {
                            PhysicalExpression::Value(BoundValue {
                                value: value.clone(),
                                origin: ValueOrigin::Parameter(parameter.clone()),
                            })
                        })
                        .collect(),
                )
            } else {
                return Err(GraphPlanError::MissingParameter(
                    parameter.as_str().to_string(),
                ));
            }
        }
        LogicalExpression::Case {
            operand,
            branches,
            default,
        } => PhysicalExpression::Case {
            operand: operand
                .as_deref()
                .map(|operand| bind_expression(operand, context).map(Box::new))
                .transpose()?,
            branches: branches
                .iter()
                .map(|(when, then)| {
                    Ok((
                        bind_expression(when, context)?,
                        bind_expression(then, context)?,
                    ))
                })
                .collect::<GraphPlanResult<Vec<_>>>()?,
            default: default
                .as_deref()
                .map(|default| bind_expression(default, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::List(items) => PhysicalExpression::List(
            items
                .iter()
                .map(|item| bind_expression(item, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
        ),
        LogicalExpression::Literal(value) => PhysicalExpression::Value(BoundValue {
            value: value.clone(),
            origin: ValueOrigin::Literal,
        }),
        LogicalExpression::Aggregate {
            function,
            distinct,
            expression,
        } => PhysicalExpression::Aggregate {
            function: *function,
            distinct: *distinct,
            expression: expression
                .as_ref()
                .map(|expression| bind_expression(expression, context).map(Box::new))
                .transpose()?,
        },
        LogicalExpression::Function {
            function,
            arguments,
        } => PhysicalExpression::Function {
            function: *function,
            arguments: arguments
                .iter()
                .map(|argument| bind_expression(argument, context))
                .collect::<GraphPlanResult<Vec<_>>>()?,
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
        GraphLogicalPlan::SingleRow => Some(1),
        GraphLogicalPlan::Unwind {
            input, expression, ..
        } => {
            let input_rows = estimate_rows(input, context, statistics)?;
            match bind_expression(expression, context).ok()? {
                PhysicalExpression::List(items) => {
                    Some(input_rows.saturating_mul(items.len() as u64))
                }
                _ => Some(input_rows),
            }
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
            // An unbounded walk has no estimate worth giving.
            let hops = u64::from((*max_hops)?.saturating_sub(*min_hops).saturating_add(1));
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

/// Relationships an equality on `relationship.property` matches across all of
/// `values`: each value's exact count, else a bloom filter's proof of absence,
/// else the histogram's average per distinct value. `None` when any value has
/// no estimate.
fn relationship_values_estimate(
    relationship_types: &[Symbol],
    property: &Symbol,
    values: &[BoundValue],
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    let histogram_estimate = statistics
        .relationship_property_statistics(relationship_types, property)
        .as_ref()
        .and_then(property_equality_estimate);
    values.iter().try_fold(0_u64, |total, value| {
        let estimate = statistics
            .relationship_property_value_count(relationship_types, property, &value.value)
            .or_else(|| {
                statistics
                    .relationship_bloom_may_contain(relationship_types, property, &value.value)
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
                if let Some(rows) =
                    relationship_values_estimate(relationship_types, &property, &values, statistics)
                {
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
        | PhysicalExpression::Function { .. }
        | PhysicalExpression::List(_)
        | PhysicalExpression::Case { .. }
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
