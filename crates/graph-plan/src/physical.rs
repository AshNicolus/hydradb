use crate::{BoundValue, Symbol};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalBinaryOperator {
    Equal,
    Or,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalExpression {
    Binding(Symbol),
    Property {
        binding: Symbol,
        property: Symbol,
    },
    Value(BoundValue),
    Binary {
        left: Box<PhysicalExpression>,
        operator: PhysicalBinaryOperator,
        right: Box<PhysicalExpression>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProjection {
    pub expression: PhysicalExpression,
    pub alias: Option<Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphPhysicalPlan {
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
    VertexLabelScan {
        binding: Symbol,
        label: Symbol,
    },
    AllVertexScan {
        binding: Symbol,
    },
    Filter {
        input: Box<GraphPhysicalPlan>,
        predicate: PhysicalExpression,
    },
    Project {
        input: Box<GraphPhysicalPlan>,
        items: Vec<PhysicalProjection>,
    },
}
