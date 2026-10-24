//! Isolated Cypher preparation and physical-plan execution.
//!
//! This crate deliberately has no dependency on HydraDB's root package. It
//! develops the new parser/AST/planner/executor path beside the production
//! libcypher `GraphShard` path until a storage adapter is ready. Its plan types
//! are private to this experimental path so new operators cannot force changes
//! into the production executor.

#![forbid(unsafe_code)]

mod capability;
mod error;
mod execute;
mod hydration;
mod model;
mod observe;
mod plan;
mod prepare;
mod storage;

pub use capability::{
    logical_capability, physical_capability, EngineCapability, UnsupportedShape,
    EXPAND_FROM_UNBOUND, NESTED_PROJECT_AGGREGATE, NESTED_PROJECT_UNNAMED, RESULT_PLAN_ROOT,
    UNDIRECTED_VARIABLE_LENGTH, UNION_BINDING_MISMATCH, UNION_COLUMN_MISMATCH, UNION_EMPTY,
};
pub use error::{EngineError, EngineResult, StorageError, StorageResult};
pub use execute::CypherEngine;
pub use model::{
    compare_property_values, scalar_values_equal, AccessPathKind, BoundedVertexIds, ExpandNeighbor,
    ExpandRequest, ListParameterMap, OrderedPropertyScanPage, OrderedPropertyScanRequest,
    OrderedRelationshipPropertyScanPage, OrderedRelationshipPropertyScanRequest, OrderedScanCursor,
    OrderedScanPosition, ParameterMap, PropertyBound, PropertyEqualityCandidate, QueryColumn,
    QueryResult, QueryRow, QueryValue, ReadRequest, RelationshipId, RelationshipRecord,
    StatisticsAccessReport, VertexId, VertexRecord,
};
pub use observe::{ExecutionProfile, OperatorProfile, OperatorProfiler};
pub use plan::{
    estimate_operator_rows, explain_physical_plan, explain_physical_plan_shape, lower_cypher_ast,
    plan_physical, plan_physical_with_statistics, AggregateFunction, BloomFilterStatistics,
    BoundValue, ExpandDirection, GraphLogicalPlan, GraphPhysicalPlan, GraphPlanError,
    GraphPlanResult, GuardedAccess, LogicalBinaryOperator, LogicalExpression, LogicalInList,
    LogicalProjection, LogicalSort, LogicalUnaryOperator, PatternDirection, PhysicalBinaryOperator,
    PhysicalExpression, PhysicalPlanningContext, PhysicalProjection, PhysicalSort,
    PhysicalUnaryOperator, PropertyNumericRange, PropertyStatistics, ScalarFunction, ScalarValue,
    SortDirection, StatisticsProvider, Symbol, UnknownStatistics, ValueOrigin,
};
pub use prepare::{plan_shape_hash, LogicalQuery, LowerTimings, LoweredTemplate, PreparedQuery};
pub use storage::{GraphRead, GraphStorage};
