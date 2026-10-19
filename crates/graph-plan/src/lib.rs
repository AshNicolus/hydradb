//! Semantic graph IR and tightly coupled KV physical plans.
//!
//! The logical plan records what the graph query means. The physical plan
//! records the exact HydraDB KV operations selected for that meaning.

#![forbid(unsafe_code)]

mod error;
mod explain;
mod logical;
mod lower;
mod physical;
mod planner;
mod value;

pub use error::{GraphPlanError, GraphPlanResult};
pub use explain::explain_physical_plan;
pub use logical::{
    GraphLogicalPlan, LogicalBinaryOperator, LogicalExpression, LogicalProjection, Symbol,
};
pub use lower::lower_cypher_ast;
pub use physical::{
    GraphPhysicalPlan, PhysicalBinaryOperator, PhysicalExpression, PhysicalProjection,
};
pub use planner::{plan_physical, PhysicalPlanningContext};
pub use value::{BoundValue, ScalarValue, ValueOrigin};
