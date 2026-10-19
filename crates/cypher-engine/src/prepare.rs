use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{
    explain_physical_plan, explain_physical_plan_shape, lower_cypher_ast,
    plan_physical_with_statistics, GraphLogicalPlan, GraphPhysicalPlan, PhysicalPlanningContext,
    PhysicalProjection, StatisticsProvider, Symbol, UnknownStatistics,
};
use hydradb_cypher_ast::{Document, QueryFailureReason};
use hydradb_cypher_parser_antlr::parse_cypher25;

use crate::{EngineError, EngineResult, ListParameterMap, ParameterMap};

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedQuery {
    pub ast: Arc<Document>,
    pub logical: Arc<GraphLogicalPlan>,
    pub physical: GraphPhysicalPlan,
}

impl PreparedQuery {
    pub fn explain(&self) -> String {
        explain_physical_plan(&self.physical)
    }

    pub fn column_names(&self) -> EngineResult<Vec<String>> {
        column_names(&self.physical)
    }

    /// `EXPLAIN` with literals and value lists elided: no tenant value, so it
    /// may go on a sampled trace.
    pub fn plan_shape(&self) -> String {
        explain_physical_plan_shape(&self.physical)
    }

    /// A stable identity for [`Self::plan_shape`]: FNV-1a, 64 bits, rendered
    /// by callers as 16 hex digits. Two requests share it exactly when they
    /// chose the same operators in the same order over the same schema names,
    /// so a plan flip between two requests of one query is visible as a hash
    /// change. It is a trace attribute, never a metric label: its cardinality
    /// is the number of query shapes, which clients control.
    pub fn plan_hash(&self) -> u64 {
        plan_shape_hash(&self.plan_shape())
    }
}

/// The hash [`PreparedQuery::plan_hash`] applies to a rendered shape, for a
/// caller that already holds the shape and should not render it twice.
pub fn plan_shape_hash(shape: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    shape.bytes().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

fn column_names(plan: &GraphPhysicalPlan) -> EngineResult<Vec<String>> {
    match plan {
        GraphPhysicalPlan::Project { items, .. } => {
            Ok(items.iter().map(PhysicalProjection::column_name).collect())
        }
        GraphPhysicalPlan::Union { arms, .. } => arms
            .first()
            .ok_or_else(|| {
                EngineError::UnsupportedPlan(
                    QueryFailureReason::Union,
                    "UNION has no arms".to_string(),
                )
            })
            .and_then(column_names),
        _ => Err(EngineError::UnsupportedPlan(
            QueryFailureReason::Other,
            "a result-producing plan must have Project or Union at its root".to_string(),
        )),
    }
}

pub(crate) fn prepare_query(query: &str, parameters: &ParameterMap) -> EngineResult<PreparedQuery> {
    prepare_query_with_statistics(
        query,
        parameters,
        &ListParameterMap::new(),
        &UnknownStatistics,
    )
}

/// Parsed and lowered query awaiting a coherent statistics view. Lowering is
/// storage-free; callers can load statistics asynchronously before planning.
#[derive(Clone, Debug)]
pub struct LogicalQuery {
    pub ast: Arc<Document>,
    pub logical: Arc<GraphLogicalPlan>,
    planning: PhysicalPlanningContext,
}

impl LogicalQuery {
    /// Bound scalar and list values used by physical planning. Storage-backed
    /// adapters use this read-only view to load only point statistics that the
    /// prepared predicates can consume.
    pub fn planning_context(&self) -> &PhysicalPlanningContext {
        &self.planning
    }

    pub fn plan_with_statistics(
        self,
        statistics: &dyn StatisticsProvider,
    ) -> EngineResult<PreparedQuery> {
        let physical = plan_physical_with_statistics(&self.logical, &self.planning, statistics)?;
        Ok(PreparedQuery {
            ast: self.ast,
            logical: self.logical,
            physical,
        })
    }
}

/// Wall time of the two halves of [`LoweredTemplate::lower_timed`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LowerTimings {
    pub parse: Duration,
    pub lower: Duration,
}

/// The parameter-independent half of preparation: one query text, parsed
/// and lowered once. Parsing and lowering never read parameter *values* —
/// `$name` stays a `LogicalExpression::Parameter` until physical planning —
/// so a template keyed by query text alone can be shared across requests
/// and bound to each request's parameters with [`LoweredTemplate::bind`].
/// Both halves are `Arc`s so binding is two pointer copies, not a tree clone.
#[derive(Clone, Debug)]
pub struct LoweredTemplate {
    ast: Arc<Document>,
    logical: Arc<GraphLogicalPlan>,
}

impl LoweredTemplate {
    /// Parse and lower `query` with no parameters in sight.
    pub fn lower(query: &str) -> EngineResult<Self> {
        Self::lower_timed(query).map(|(template, _)| template)
    }

    /// [`Self::lower`], also reporting how long each half took, so a slow
    /// preparation can be attributed to the parser or to lowering.
    pub fn lower_timed(query: &str) -> EngineResult<(Self, LowerTimings)> {
        let started = Instant::now();
        let ast =
            parse_cypher25(query).map_err(|error| EngineError::Parse(format!("{error:?}")))?;
        let parse = started.elapsed();
        let started = Instant::now();
        let logical = lower_cypher_ast(&ast)?;
        let lower = started.elapsed();
        Ok((
            Self {
                ast: Arc::new(ast),
                logical: Arc::new(logical),
            },
            LowerTimings { parse, lower },
        ))
    }

    /// A conservative estimate of the heap this template retains, for cache
    /// accounting. The `Debug` renderings of the tree and plan are a cheap
    /// stand-in for walking every node: they spell out each string and enum
    /// once, so they track allocated size within a small constant factor.
    /// Computed once per lowering, never per request.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + format!("{:?}", self.ast).len()
            + format!("{:?}", self.logical).len()
    }

    /// Attach one request's parameter values, producing the same
    /// [`LogicalQuery`] that lowering from scratch would.
    pub fn bind(&self, parameters: &ParameterMap, lists: &ListParameterMap) -> LogicalQuery {
        LogicalQuery {
            ast: Arc::clone(&self.ast),
            logical: Arc::clone(&self.logical),
            planning: PhysicalPlanningContext {
                parameters: parameters
                    .iter()
                    .map(|(name, value)| (Symbol::from(name.as_str()), value.clone()))
                    .collect(),
                list_parameters: lists
                    .iter()
                    .map(|(name, values)| (Symbol::from(name.as_str()), values.clone()))
                    .collect(),
            },
        }
    }
}

pub(crate) fn lower_query(
    query: &str,
    parameters: &ParameterMap,
    lists: &ListParameterMap,
) -> EngineResult<LogicalQuery> {
    Ok(LoweredTemplate::lower(query)?.bind(parameters, lists))
}

pub(crate) fn prepare_query_with_statistics(
    query: &str,
    parameters: &ParameterMap,
    lists: &ListParameterMap,
    statistics: &dyn StatisticsProvider,
) -> EngineResult<PreparedQuery> {
    lower_query(query, parameters, lists)?.plan_with_statistics(statistics)
}

#[cfg(test)]
mod observability_tests {
    use super::*;
    use crate::ScalarValue;

    fn prepared(query: &str) -> PreparedQuery {
        prepare_query(query, &ParameterMap::new()).expect("prepares")
    }

    #[test]
    fn the_plan_shape_carries_no_literal_and_hashes_by_structure() {
        let alpha = prepared("MATCH (n:Entity {entity_id: 'tenant-secret'}) RETURN n.entity_id");
        let beta = prepared("MATCH (n:Entity {entity_id: 'other'}) RETURN n.entity_id");
        assert!(alpha.explain().contains("tenant-secret"));
        assert!(!alpha.plan_shape().contains("tenant-secret"));
        assert_eq!(alpha.plan_hash(), beta.plan_hash());
        // Rendering the shape must not leak redaction into a later EXPLAIN.
        assert!(alpha.explain().contains("tenant-secret"));

        let scan = prepared("MATCH (n:Entity) RETURN n.entity_id");
        assert_ne!(alpha.plan_hash(), scan.plan_hash());
    }

    #[test]
    fn a_list_does_not_change_the_shape_by_its_length() {
        let lists = |values: usize| {
            ListParameterMap::from([(
                "ids".to_string(),
                (0..values)
                    .map(|value| ScalarValue::String(value.to_string().into()))
                    .collect(),
            )])
        };
        let query = "MATCH (n:Entity) WHERE n.entity_id IN $ids RETURN n.entity_id";
        let plan = |values| {
            prepare_query_with_statistics(
                query,
                &ParameterMap::new(),
                &lists(values),
                &UnknownStatistics,
            )
            .expect("prepares")
        };
        assert_eq!(plan(2).plan_hash(), plan(40).plan_hash());
    }

    #[test]
    fn lowering_reports_both_halves() {
        let (_, timings) = LoweredTemplate::lower_timed("MATCH (n) RETURN n").expect("lowers");
        assert!(timings.parse > Duration::ZERO);
    }
}
