use crate::{AggregateFunction, BoundValue, PatternDirection, ScalarValue, SortDirection, Symbol};

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
        expression: Option<Box<PhysicalExpression>>,
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
            expression,
        } => format!(
            "{}({})",
            match function {
                AggregateFunction::Count => "count",
                AggregateFunction::Sum => "sum",
                AggregateFunction::Average => "avg",
                AggregateFunction::Collect => "collect",
            },
            expression
                .as_deref()
                .map(expression_name)
                .unwrap_or_else(|| "*".to_string())
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPhysicalPlan {
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
    /// Bounded ordered walk of one string property index. Replaces
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
        relationship_types: Vec<Symbol>,
        direction: PatternDirection,
        to: Symbol,
        target_labels: Vec<Symbol>,
        min_hops: u8,
        max_hops: u8,
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
