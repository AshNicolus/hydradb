//! Row estimates per physical operator, for comparing against what execution
//! actually produced.
//!
//! This is an observer, not the cost model: the planner chooses plans from
//! logical-plan estimates in `planner.rs`, and nothing here feeds back into
//! that choice. It answers "what did statistics predict for this operator",
//! using the same statistics calls and the same conventions the planner uses,
//! so a large miss here points at the statistics or the model rather than at
//! a second, divergent estimator. The rules follow the planner's:
//!
//! - unknown selectivity is not evidence, so a `Filter` keeps its input's
//!   estimate rather than inventing a reduction;
//! - a join multiplies its inputs;
//! - a window clamps its input.

use crate::{BoundValue, GraphPhysicalPlan, PropertyStatistics, ScalarValue, StatisticsProvider};

/// Estimates for every operator of `plan`, in pre-order (the order
/// [`GraphPhysicalPlan::children`] walks). `None` where statistics give no
/// answer.
pub fn estimate_operator_rows(
    plan: &GraphPhysicalPlan,
    statistics: &dyn StatisticsProvider,
) -> Vec<Option<u64>> {
    let mut estimates = Vec::new();
    estimate_into(plan, statistics, &mut estimates);
    estimates
}

fn estimate_into(
    plan: &GraphPhysicalPlan,
    statistics: &dyn StatisticsProvider,
    estimates: &mut Vec<Option<u64>>,
) -> Option<u64> {
    let slot = estimates.len();
    estimates.push(None);
    let inputs = plan
        .children()
        .into_iter()
        .map(|child| estimate_into(child, statistics, estimates))
        .collect::<Vec<_>>();
    let input = inputs.first().copied().flatten();
    let estimate = match plan {
        GraphPhysicalPlan::Union { .. } => inputs
            .iter()
            .try_fold(0_u64, |total, arm| Some(total.saturating_add((*arm)?))),
        GraphPhysicalPlan::VertexIdSeek { .. } => Some(1),
        GraphPhysicalPlan::VertexPropertySeek {
            labels,
            property,
            value,
            ..
        } => value_count(statistics, labels, property, value),
        GraphPhysicalPlan::VertexPropertyMultiSeek {
            labels,
            property,
            values,
            ..
        } => values.iter().try_fold(0_u64, |total, value| {
            Some(total.saturating_add(value_count(statistics, labels, property, value)?))
        }),
        GraphPhysicalPlan::VertexPropertyScan {
            labels, property, ..
        } => statistics
            .property_statistics(labels, property)
            .and_then(|stats| stats.non_null_count),
        GraphPhysicalPlan::OrderedVertexPropertyScan {
            labels,
            property,
            required,
            ..
        } => {
            let required = *required as u64;
            Some(
                statistics
                    .property_statistics(labels, property)
                    .and_then(|stats| stats.non_null_count)
                    .map_or(required, |rows| rows.min(required)),
            )
        }
        GraphPhysicalPlan::VertexLabelScan { label, labels, .. } => statistics
            .vertex_count(labels)
            .or_else(|| statistics.vertex_count(std::slice::from_ref(label))),
        GraphPhysicalPlan::AllVertexScan { .. } => statistics.vertex_count(&[]),
        GraphPhysicalPlan::RelationshipPropertySeek {
            relationship_type,
            property,
            value,
            ..
        } => {
            let types = std::slice::from_ref(relationship_type);
            statistics
                .relationship_property_value_count(types, property, &value.value)
                .or_else(|| {
                    statistics
                        .relationship_property_statistics(types, property)
                        .as_ref()
                        .and_then(equality_rows)
                })
        }
        GraphPhysicalPlan::Expand {
            input: source,
            relationship_types,
            direction,
            ..
        } => expand_rows(input, source, relationship_types, *direction, statistics),
        GraphPhysicalPlan::VariableExpand {
            input: source,
            relationship_types,
            direction,
            min_hops,
            max_hops,
            ..
        } => {
            expand_rows(input, source, relationship_types, *direction, statistics).map(|one_hop| {
                one_hop.saturating_mul(u64::from(
                    max_hops.saturating_sub(*min_hops).saturating_add(1),
                ))
            })
        }
        GraphPhysicalPlan::NaturalJoin { .. } => Some(
            inputs
                .first()
                .copied()??
                .saturating_mul(inputs.get(1).copied()??),
        ),
        GraphPhysicalPlan::Filter { .. }
        | GraphPhysicalPlan::Sort { .. }
        | GraphPhysicalPlan::Project { .. } => input,
        GraphPhysicalPlan::Skip { count, .. } => {
            Some(input?.saturating_sub(constant_count(count)?))
        }
        GraphPhysicalPlan::Limit { count, .. } => Some(input?.min(constant_count(count)?)),
    };
    estimates[slot] = estimate;
    estimate
}

fn value_count(
    statistics: &dyn StatisticsProvider,
    labels: &[crate::Symbol],
    property: &crate::Symbol,
    value: &BoundValue,
) -> Option<u64> {
    statistics
        .property_value_count(labels, property, &value.value)
        .or_else(|| {
            statistics
                .bloom_may_contain(labels, property, &value.value)
                .and_then(|possibly_present| (!possibly_present).then_some(0))
        })
        .or_else(|| {
            statistics
                .property_statistics(labels, property)
                .as_ref()
                .and_then(equality_rows)
        })
}

/// Rows per distinct value, rounded up: the planner's uniform-histogram rule.
fn equality_rows(statistics: &PropertyStatistics) -> Option<u64> {
    let count = statistics.non_null_count?;
    if count == 0 {
        return Some(0);
    }
    let distinct = statistics.distinct_count?;
    (distinct > 0).then(|| count.saturating_add(distinct - 1) / distinct)
}

fn expand_rows(
    input_rows: Option<u64>,
    source: &GraphPhysicalPlan,
    relationship_types: &[crate::Symbol],
    direction: crate::PatternDirection,
    statistics: &dyn StatisticsProvider,
) -> Option<u64> {
    if input_rows == Some(0) {
        return Some(0);
    }
    source_labels(source)
        .and_then(|labels| {
            statistics.relationship_expansion_count(labels, relationship_types, direction)
        })
        .or_else(|| statistics.relationship_count(relationship_types))
}

fn source_labels(plan: &GraphPhysicalPlan) -> Option<&[crate::Symbol]> {
    match plan {
        GraphPhysicalPlan::VertexIdSeek { labels, .. }
        | GraphPhysicalPlan::VertexPropertySeek { labels, .. }
        | GraphPhysicalPlan::VertexPropertyMultiSeek { labels, .. }
        | GraphPhysicalPlan::VertexPropertyScan { labels, .. }
        | GraphPhysicalPlan::OrderedVertexPropertyScan { labels, .. }
        | GraphPhysicalPlan::VertexLabelScan { labels, .. } => Some(labels),
        GraphPhysicalPlan::Filter { input, .. }
        | GraphPhysicalPlan::Sort { input, .. }
        | GraphPhysicalPlan::Skip { input, .. }
        | GraphPhysicalPlan::Limit { input, .. } => source_labels(input),
        _ => None,
    }
}

fn constant_count(count: &BoundValue) -> Option<u64> {
    match &count.value {
        ScalarValue::Integer(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}
