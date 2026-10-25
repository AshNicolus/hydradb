//! Structural facts about a physical plan that observability needs and
//! execution does not: a stable operator name and the child edges.
//!
//! Kept apart from the executor on purpose. Every consumer that walks a plan
//! for telemetry goes through [`GraphPhysicalPlan::children`], so a new
//! operator variant is one match arm here rather than one in every walker.

use std::collections::BTreeSet;

use crate::{GraphPhysicalPlan, Symbol};

impl GraphPhysicalPlan {
    /// The operator's name as `EXPLAIN` spells it, without arguments. A closed
    /// vocabulary, so it is safe as a metric label.
    pub fn operator_name(&self) -> &'static str {
        match self {
            Self::SingleRow => "SingleRowExec",
            Self::Unwind { .. } => "UnwindExec",
            Self::Union { .. } => "UnionExec",
            Self::VertexIdSeek { .. } => "VertexIdSeek",
            Self::VertexPropertySeek { .. } => "VertexPropertySeek",
            Self::VertexPropertyMultiSeek { .. } => "VertexPropertyMultiSeek",
            Self::VertexPropertyScan { .. } => "VertexPropertyScan",
            Self::OrderedVertexPropertyScan { .. } => "OrderedVertexPropertyScan",
            Self::OrderedRelationshipPropertyScan { .. } => "OrderedRelationshipPropertyScan",
            Self::VertexLabelScan { .. } => "VertexLabelScan",
            Self::AllVertexScan { .. } => "AllVertexScan",
            Self::RelationshipPropertySeek { .. } => "RelationshipPropertySeek",
            Self::RelationshipPropertyMultiSeek { .. } => "RelationshipPropertyMultiSeek",
            Self::VertexPropertyRangeSeek { .. } => "VertexPropertyRangeSeek",
            Self::VertexGuardedAccess { .. } => "VertexGuardedAccess",
            Self::RelationshipPropertyRangeProbe { .. } => "RelationshipPropertyRangeProbe",
            Self::RelationshipTypeProbe { .. } => "RelationshipTypeProbe",
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
            Self::SingleRow
            | Self::VertexIdSeek { .. }
            | Self::VertexPropertySeek { .. }
            | Self::VertexPropertyMultiSeek { .. }
            | Self::VertexPropertyScan { .. }
            | Self::OrderedVertexPropertyScan { .. }
            | Self::OrderedRelationshipPropertyScan { .. }
            | Self::VertexLabelScan { .. }
            | Self::AllVertexScan { .. }
            | Self::RelationshipPropertySeek { .. }
            | Self::RelationshipPropertyMultiSeek { .. } => Vec::new(),
            Self::VertexPropertyRangeSeek { fallback, .. }
            | Self::VertexGuardedAccess { fallback, .. }
            | Self::RelationshipPropertyRangeProbe { fallback, .. }
            | Self::RelationshipTypeProbe { fallback, .. } => vec![fallback],
            Self::Unwind { input, .. }
            | Self::Expand { input, .. }
            | Self::VariableExpand { input, .. }
            | Self::Filter { input, .. }
            | Self::Sort { input, .. }
            | Self::Skip { input, .. }
            | Self::Limit { input, .. }
            | Self::Project { input, .. } => vec![input],
            Self::NaturalJoin { left, right, .. } => vec![left, right],
        }
    }

    /// Every binding this plan can put on a row, nodes and relationships
    /// alike.
    ///
    /// An OPTIONAL join needs this to say what its right side *would* have
    /// bound when it matches nothing. Cypher binds those names to null rather
    /// than leaving them unbound, and only the plan knows which names they
    /// are.
    pub fn bindings(&self) -> BTreeSet<Symbol> {
        let mut bindings = BTreeSet::new();
        self.collect_bindings(&mut bindings);
        bindings
    }

    fn collect_bindings(&self, into: &mut BTreeSet<Symbol>) {
        match self {
            Self::VertexIdSeek { binding, .. }
            | Self::VertexPropertySeek { binding, .. }
            | Self::VertexPropertyMultiSeek { binding, .. }
            | Self::VertexPropertyScan { binding, .. }
            | Self::OrderedVertexPropertyScan { binding, .. }
            | Self::VertexPropertyRangeSeek { binding, .. }
            | Self::VertexGuardedAccess { binding, .. }
            | Self::VertexLabelScan { binding, .. }
            | Self::AllVertexScan { binding } => {
                into.insert(binding.clone());
            }
            // Both bind the whole hop: the source, the relationship and the
            // target are all produced by the operator itself.
            Self::RelationshipPropertySeek {
                from,
                relationship,
                to,
                ..
            }
            | Self::RelationshipPropertyMultiSeek {
                from,
                relationship,
                to,
                ..
            }
            | Self::OrderedRelationshipPropertyScan {
                from,
                relationship,
                to,
                ..
            }
            | Self::RelationshipPropertyRangeProbe {
                from,
                relationship,
                to,
                ..
            }
            | Self::RelationshipTypeProbe {
                from,
                relationship,
                to,
                ..
            } => {
                into.insert(from.clone());
                into.insert(relationship.clone());
                into.insert(to.clone());
            }
            Self::Expand {
                from,
                relationship,
                to,
                ..
            } => {
                into.insert(from.clone());
                if let Some(relationship) = relationship {
                    into.insert(relationship.clone());
                }
                into.insert(to.clone());
            }
            Self::VariableExpand {
                from,
                relationship,
                to,
                ..
            } => {
                into.insert(from.clone());
                into.extend(relationship.clone());
                into.insert(to.clone());
            }
            // Bind nothing of their own; their children do.
            // An unwound element is a value, never a node or relationship, so
            // an OPTIONAL miss has nothing of it to bind to null.
            Self::SingleRow
            | Self::Unwind { .. }
            | Self::Union { .. }
            | Self::Filter { .. }
            | Self::Sort { .. }
            | Self::Skip { .. }
            | Self::Limit { .. }
            | Self::Project { .. }
            | Self::NaturalJoin { .. } => {}
        }
        for child in self.children() {
            child.collect_bindings(into);
        }
    }

    /// Whether any operator reads every vertex. The same verdict the legacy
    /// route reports as `hydradb.query.full_scan`.
    pub fn contains_full_scan(&self) -> bool {
        matches!(self, Self::AllVertexScan { .. })
            || self.children().into_iter().any(Self::contains_full_scan)
    }
}
