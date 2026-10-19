//! Structural facts about a physical plan that observability needs and
//! execution does not: a stable operator name and the child edges.
//!
//! Kept apart from the executor on purpose. Every consumer that walks a plan
//! for telemetry goes through [`GraphPhysicalPlan::children`], so a new
//! operator variant is one match arm here rather than one in every walker.

use crate::GraphPhysicalPlan;

impl GraphPhysicalPlan {
    /// The operator's name as `EXPLAIN` spells it, without arguments. A closed
    /// vocabulary, so it is safe as a metric label.
    pub fn operator_name(&self) -> &'static str {
        match self {
            Self::Union { .. } => "UnionExec",
            Self::VertexIdSeek { .. } => "VertexIdSeek",
            Self::VertexPropertySeek { .. } => "VertexPropertySeek",
            Self::VertexPropertyMultiSeek { .. } => "VertexPropertyMultiSeek",
            Self::VertexPropertyScan { .. } => "VertexPropertyScan",
            Self::OrderedVertexPropertyScan { .. } => "OrderedVertexPropertyScan",
            Self::VertexLabelScan { .. } => "VertexLabelScan",
            Self::AllVertexScan { .. } => "AllVertexScan",
            Self::RelationshipPropertySeek { .. } => "RelationshipPropertySeek",
            Self::Expand { .. } => "ExpandExec",
            Self::VariableExpand { .. } => "VariableExpandExec",
            // The two join behaviours are separate operators to telemetry for
            // the same reason `EXPLAIN` names them apart: a regression in
            // `OPTIONAL MATCH` is not a regression in the inner join.
            Self::NaturalJoin { optional: true, .. } => "LeftOuterNaturalJoinExec",
            Self::NaturalJoin {
                optional: false, ..
            } => "NaturalJoinExec",
            Self::Filter { .. } => "FilterExec",
            Self::Sort { .. } => "SortExec",
            Self::Skip { .. } => "SkipExec",
            Self::Limit { .. } => "LimitExec",
            Self::Project { .. } => "ProjectExec",
        }
    }

    /// Direct inputs, left to right, in the order `EXPLAIN` renders them.
    pub fn children(&self) -> Vec<&GraphPhysicalPlan> {
        match self {
            Self::Union { arms, .. } => arms.iter().collect(),
            Self::VertexIdSeek { .. }
            | Self::VertexPropertySeek { .. }
            | Self::VertexPropertyMultiSeek { .. }
            | Self::VertexPropertyScan { .. }
            | Self::OrderedVertexPropertyScan { .. }
            | Self::VertexLabelScan { .. }
            | Self::AllVertexScan { .. }
            | Self::RelationshipPropertySeek { .. } => Vec::new(),
            Self::Expand { input, .. }
            | Self::VariableExpand { input, .. }
            | Self::Filter { input, .. }
            | Self::Sort { input, .. }
            | Self::Skip { input, .. }
            | Self::Limit { input, .. }
            | Self::Project { input, .. } => vec![input],
            Self::NaturalJoin { left, right, .. } => vec![left, right],
        }
    }

    /// Whether any operator reads every vertex. The same verdict the legacy
    /// route reports as `hydradb.query.full_scan`.
    pub fn contains_full_scan(&self) -> bool {
        matches!(self, Self::AllVertexScan { .. })
            || self.children().into_iter().any(Self::contains_full_scan)
    }
}

#[cfg(test)]
mod tests {
    use crate::{plan_physical, prepare::lower_query, ListParameterMap, ParameterMap};

    fn plan(query: &str) -> crate::GraphPhysicalPlan {
        let logical =
            lower_query(query, &ParameterMap::new(), &ListParameterMap::new()).expect("lowers");
        plan_physical(&logical.logical, logical.planning_context()).expect("plans")
    }

    #[test]
    fn children_follow_explain_order_and_find_a_full_scan() {
        let scan = plan("MATCH (n) RETURN n");
        assert_eq!(scan.operator_name(), "ProjectExec");
        assert_eq!(scan.children()[0].operator_name(), "AllVertexScan");
        assert!(scan.contains_full_scan());
        let seek = plan("MATCH (n:Entity {entity_id: 'a'}) RETURN n");
        assert!(!seek.contains_full_scan());
    }
}
