use crate::{
    AggregateFunction, BoundValue, PatternDirection, ScalarFunction, ScalarValue, SortDirection,
    Symbol,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalBinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StartsWith,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalUnaryOperator {
    Not,
    Plus,
    Minus,
    /// Postfix; never unknown, since it is the test for unknown.
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalExpression {
    Binding(Symbol),
    Identity(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Value(BoundValue),
    Aggregate {
        function: AggregateFunction,
        distinct: bool,
        expression: Option<Box<PhysicalExpression>>,
    },
    Function {
        function: ScalarFunction,
        arguments: Vec<PhysicalExpression>,
    },
    /// A list value: a literal, or a list parameter bound element by element.
    List(Vec<PhysicalExpression>),
    Case {
        operand: Option<Box<PhysicalExpression>>,
        branches: Vec<(PhysicalExpression, PhysicalExpression)>,
        default: Option<Box<PhysicalExpression>>,
    },
    Unary {
        operator: PhysicalUnaryOperator,
        expression: Box<PhysicalExpression>,
    },
    Binary {
        left: Box<PhysicalExpression>,
        operator: PhysicalBinaryOperator,
        right: Box<PhysicalExpression>,
    },
    /// `<expression> IN <list>` with every candidate already resolved to a
    /// bound value, so execution never revisits parameters.
    InList {
        expression: Box<PhysicalExpression>,
        values: Vec<BoundValue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProjection {
    pub expression: PhysicalExpression,
    pub alias: Option<Symbol>,
}

impl PhysicalProjection {
    pub fn column_name(&self) -> String {
        self.alias
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| expression_name(&self.expression))
    }
}

fn expression_name(expression: &PhysicalExpression) -> String {
    match expression {
        PhysicalExpression::Binding(binding) => binding.to_string(),
        PhysicalExpression::Identity(binding) => format!("{binding}.id"),
        PhysicalExpression::Property { binding, property } => format!("{binding}.{property}"),
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => "NULL".to_string(),
            ScalarValue::Boolean(value) => value.to_string(),
            ScalarValue::Integer(value) => value.to_string(),
            ScalarValue::Float(value) | ScalarValue::String(value) => value.to_string(),
        },
        PhysicalExpression::InList { expression, values } => {
            format!(
                "{} IN [{} values]",
                expression_name(expression),
                values.len()
            )
        }
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
        } => format!(
            "{}({}{})",
            function.name(),
            if *distinct { "DISTINCT " } else { "" },
            expression
                .as_deref()
                .map(expression_name)
                .unwrap_or_else(|| "*".to_string())
        ),
        PhysicalExpression::Case { .. } => "CASE".to_string(),
        PhysicalExpression::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(expression_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalExpression::Function {
            function,
            arguments,
        } => format!(
            "{}({})",
            function.name(),
            arguments
                .iter()
                .map(expression_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalExpression::Unary {
            operator,
            expression,
        } => {
            let operand = expression_name(expression);
            match operator {
                PhysicalUnaryOperator::Not => format!("NOT {operand}"),
                PhysicalUnaryOperator::Plus => format!("+{operand}"),
                PhysicalUnaryOperator::Minus => format!("-{operand}"),
                PhysicalUnaryOperator::IsNull => format!("{operand} IS NULL"),
                PhysicalUnaryOperator::IsNotNull => format!("{operand} IS NOT NULL"),
            }
        }
        PhysicalExpression::Binary { .. } => "expression".to_string(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSort {
    pub expression: PhysicalExpression,
    pub direction: SortDirection,
}

/// What a [`GraphPhysicalPlan::VertexGuardedAccess`] reads while its guard
/// holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedAccess {
    /// Seek the vertices whose `property` equals any of `values`.
    PropertySeek {
        property: Symbol,
        values: Vec<BoundValue>,
    },
    /// Scan every vertex carrying `label`; the node's `labels` are checked on
    /// the rows.
    LabelScan { label: Symbol },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPhysicalPlan {
    /// See [`crate::GraphLogicalPlan::SingleRow`].
    SingleRow,
    /// See [`crate::GraphLogicalPlan::Unwind`].
    Unwind {
        input: Box<GraphPhysicalPlan>,
        expression: PhysicalExpression,
        binding: Symbol,
    },
    Union {
        arms: Vec<GraphPhysicalPlan>,
        all: bool,
    },
    VertexIdSeek {
        binding: Symbol,
        labels: Vec<Symbol>,
        value: BoundValue,
    },
    VertexPropertySeek {
        binding: Symbol,
        labels: Vec<Symbol>,
        property: Symbol,
        value: BoundValue,
    },
    VertexPropertyMultiSeek {
        binding: Symbol,
        labels: Vec<Symbol>,
        property: Symbol,
        values: Vec<BoundValue>,
    },
    VertexPropertyScan {
        binding: Symbol,
        labels: Vec<Symbol>,
        property: Symbol,
    },
    /// Bounded ordered walk of one vertex property index. Replaces
    /// `Sort(Filter(VertexPropertyScan))` beneath a constant window when the
    /// index alone proves the whole predicate and the requested order, so the
    /// executor reads only the leading `required` rows (plus the final tie)
    /// instead of hydrating and sorting the complete property population.
    OrderedVertexPropertyScan {
        binding: Symbol,
        labels: Vec<Symbol>,
        property: Symbol,
        direction: SortDirection,
        /// `property STARTS WITH prefix`, when present.
        prefix: Option<BoundValue>,
        /// Exclusive or inclusive lower bound on the property, when present.
        lower: Option<(BoundValue, bool)>,
        /// Exclusive or inclusive upper bound on the property, when present.
        upper: Option<(BoundValue, bool)>,
        /// `SKIP + LIMIT` of the enclosing window: the number of leading rows
        /// the scan must prove before it may stop.
        required: usize,
        /// The complete requested order, applied in memory to the bounded
        /// window so tie-breakers such as `n.id` stay deterministic.
        items: Vec<PhysicalSort>,
        /// The conjuncts the index cannot prove, which the scan applies to
        /// each row itself. This is the only thing standing in for the Filter
        /// the scan replaced, so a row reaching the caller has passed it.
        ///
        /// Its presence is what makes the walk unbounded in index entries
        /// while staying bounded in rows: rejected rows do not count against
        /// `required`, so the scan must keep reading until that many survive.
        residual: Option<PhysicalExpression>,
    },
    /// Bounded ordered walk of one relationship property index: the
    /// relationship counterpart of `OrderedVertexPropertyScan`. Replaces
    /// `Sort(Filter(Expand(NodeScan)))` beneath a constant window when the
    /// window's leading sort key is a property of the expanded relationship,
    /// so only the leading `required` relationships (plus the boundary tie)
    /// are read and their endpoints hydrated.
    ///
    /// `residual` has the same meaning as on the vertex walk. Endpoint labels
    /// are always checked in memory.
    OrderedRelationshipPropertyScan {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        /// Pattern direction from `from` to `to`; never undirected.
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        property: Symbol,
        sort_direction: SortDirection,
        prefix: Option<BoundValue>,
        lower: Option<(BoundValue, bool)>,
        upper: Option<(BoundValue, bool)>,
        required: usize,
        items: Vec<PhysicalSort>,
        residual: Option<PhysicalExpression>,
    },
    VertexLabelScan {
        binding: Symbol,
        /// Label used to seed the storage scan. The planner chooses the
        /// smallest persisted marginal when more than one label is required.
        label: Symbol,
        /// Complete semantic label predicate. Keeping this separate from the
        /// scan label prevents a multi-label pattern from silently accepting
        /// vertices that carry only the chosen anchor label.
        labels: Vec<Symbol>,
    },
    AllVertexScan {
        binding: Symbol,
    },
    RelationshipPropertySeek {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        value: BoundValue,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    /// [`GraphPhysicalPlan::RelationshipPropertySeek`] for several values of
    /// one property: `r.p = $a OR r.p = $b` or `r.p IN $list`. The union of one
    /// index seek per distinct value, each relationship reported once however
    /// many values matched it. Like the single seek, the `Filter` above it
    /// still applies the whole predicate.
    RelationshipPropertyMultiSeek {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        values: Vec<BoundValue>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    /// Try a bounded range-index probe before a label-wide expansion. The
    /// fallback preserves the original plan when the range is too broad.
    RelationshipPropertyRangeProbe {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        property: Symbol,
        lower: BoundValue,
        inclusive: bool,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        fallback: Box<GraphPhysicalPlan>,
    },
    /// Bounded read of one vertex property index between optional bounds,
    /// chosen from stored statistics because the range looked narrower than
    /// every equality and the label. Same shape as
    /// `RelationshipPropertyRangeProbe`: the read is a candidate superset, the
    /// `Filter` above it still applies the whole predicate, and once it has
    /// read more than `read_limit` index entries it declines and `fallback`
    /// -- the plan the planner picks without statistics -- runs, with the
    /// enclosing `Filter`'s runtime probe allowed as it is on that plan
    /// today. See [`GraphPhysicalPlan::VertexGuardedAccess`].
    ///
    /// `estimated_rows` and `read_limit` are derived from statistics, so they
    /// render redacted in a plan shape and a refresh does not churn its hash.
    VertexPropertyRangeSeek {
        binding: Symbol,
        labels: Vec<Symbol>,
        property: Symbol,
        /// Exclusive or inclusive lower bound, when present.
        lower: Option<(BoundValue, bool)>,
        /// Exclusive or inclusive upper bound, when present.
        upper: Option<(BoundValue, bool)>,
        estimated_rows: u64,
        read_limit: usize,
        fallback: Box<GraphPhysicalPlan>,
    },
    /// An equality seek or a label scan chosen from stored statistics. The
    /// planner is confident in it, so it runs without the runtime equality
    /// probe; its read limit is the guard instead. Once it has read more than
    /// `read_limit` index entries it declines and `fallback` -- the plan the
    /// planner picks without statistics -- runs, under the enclosing `Filter`
    /// with the probe allowed exactly as before statistics existed. A wrong
    /// estimate therefore costs the entries read before the decline, and
    /// after it no more than today's plan.
    ///
    /// Like the range seek, the read is a candidate superset: the `Filter`
    /// above it applies the whole predicate. `estimated_rows` and
    /// `read_limit` are derived from statistics and render redacted in a plan
    /// shape.
    VertexGuardedAccess {
        binding: Symbol,
        labels: Vec<Symbol>,
        access: GuardedAccess,
        estimated_rows: u64,
        read_limit: usize,
        fallback: Box<GraphPhysicalPlan>,
    },
    /// Enter the pattern through the relationship type instead of a vertex
    /// label, for a type rare enough that reading its whole edge population
    /// beats sweeping the anchor label to discover that most of its vertices
    /// have no such edge.
    ///
    /// Selectivity is measured, not estimated: the probe walks the type's
    /// edges under a fixed entry budget and declines when it exhausts it, so
    /// no statistic decides this and a type that grows dense stops being
    /// probed on its own. `fallback` is the label-anchored plan this replaced
    /// and runs verbatim when the probe declines.
    RelationshipTypeProbe {
        from: Symbol,
        source_labels: Vec<Symbol>,
        relationship: Symbol,
        relationship_type: Symbol,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        fallback: Box<GraphPhysicalPlan>,
    },
    Expand {
        input: Box<GraphPhysicalPlan>,
        from: Symbol,
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
    },
    VariableExpand {
        input: Box<GraphPhysicalPlan>,
        from: Symbol,
        /// `[rs:R*1..3]`: the path's relationships, in walk order, bound to
        /// `rs` as a list value.
        relationship: Option<Symbol>,
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        min_hops: u8,
        /// `None` for an unbounded pattern (`*`, `*2..`): trail semantics end
        /// every walk, and the query's row budgets bound how far it gets.
        max_hops: Option<u8>,
    },
    NaturalJoin {
        left: Box<GraphPhysicalPlan>,
        right: Box<GraphPhysicalPlan>,
        optional: bool,
    },
    Filter {
        input: Box<GraphPhysicalPlan>,
        predicate: PhysicalExpression,
    },
    Sort {
        input: Box<GraphPhysicalPlan>,
        items: Vec<PhysicalSort>,
        estimated_input_rows: Option<u64>,
    },
    Skip {
        input: Box<GraphPhysicalPlan>,
        count: BoundValue,
    },
    Limit {
        input: Box<GraphPhysicalPlan>,
        count: BoundValue,
    },
    Project {
        input: Box<GraphPhysicalPlan>,
        items: Vec<PhysicalProjection>,
        distinct: bool,
        post_sort: Vec<PhysicalSort>,
        post_skip: Option<BoundValue>,
        post_limit: Option<BoundValue>,
    },
}
