use std::collections::{BTreeMap, BTreeSet};

use crate::{ExpandDirection, ScalarValue};

pub type VertexId = u64;
pub type RelationshipId = u64;
pub type ParameterMap = BTreeMap<String, ScalarValue>;

/// List-valued query parameters. Separate from `ParameterMap` because
/// `ScalarValue` is also the stored-property type and must not gain a list
/// variant. Only `IN` reads these.
pub type ListParameterMap = BTreeMap<String, Vec<ScalarValue>>;

/// Cypher scalar equality used by both residual expressions and semantic
/// property seeks. Numeric types compare by value, so integer `1` equals float
/// `1.0`; `NULL` remains unknown and is handled by expression evaluation.
pub fn scalar_values_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Integer(left), ScalarValue::Float(right))
        | (ScalarValue::Float(right), ScalarValue::Integer(left)) => right
            .parse::<f64>()
            .ok()
            .is_some_and(|right| integer_float_equal(*left, right)),
        (ScalarValue::Float(left), ScalarValue::Float(right)) => {
            match (left.parse::<f64>(), right.parse::<f64>()) {
                (Ok(left), Ok(right)) => left == right,
                _ => false,
            }
        }
        _ => left == right,
    }
}

fn integer_float_equal(integer: i128, float: f64) -> bool {
    const I128_INCLUSIVE_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_EXCLUSIVE_UPPER: f64 = 170141183460469231731687303715884105728.0;
    float.is_finite()
        && float.fract() == 0.0
        && (I128_INCLUSIVE_LOWER..I128_EXCLUSIVE_UPPER).contains(&float)
        && (float as i128) == integer
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReadRequest {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpandRequest {
    pub input_vertex_ids: Vec<VertexId>,
    pub direction: ExpandDirection,
    /// Empty means every relationship type.
    pub relationship_types: Vec<String>,
}

/// One equality restriction offered to a runtime access-path probe.
///
/// `values` is the complete set a vertex may hold for `property` and still
/// satisfy the restriction, so the vertices matching any of them are a
/// superset of the query's answer. A candidate is only ever built from a
/// conjunct of the predicate; a disjunct over two different properties is not
/// a candidate, because neither side alone covers the result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyEqualityCandidate {
    pub property: String,
    pub values: Vec<ScalarValue>,
}

/// One side of a range restriction on an ordered property scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

/// A position in an ordered walk, naming the last entry a page returned.
///
/// The index orders entries by `(value, vertex_id)`, so both are needed: many
/// vertices share a value, and a page may end part-way through them. `value`
/// is the property's own string, never a backend's key encoding of it: the
/// engine compares it with the values on the rows it holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedScanPosition {
    pub value: String,
    pub vertex_id: VertexId,
}

/// One page of a bounded, ordered walk over a vertex property index.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderedPropertyScanPage {
    pub vertices: Vec<VertexRecord>,
    /// The last entry this page reached, to resume after.
    pub last: Option<OrderedScanPosition>,
    /// The index holds nothing further within the window. Reported rather
    /// than inferred from a short page: the caller cannot tell a page the
    /// backend chose to end from one the index ran out of, and guessing wrong
    /// either loops forever or silently returns fewer rows than were asked
    /// for.
    pub exhausted: bool,
}

/// A bounded, ordered walk over one vertex property index.
///
/// The backend must return only vertices that carry every label in `labels`,
/// whose stored `property` is a string satisfying `prefix`, `lower` and
/// `upper`, ordered by that string (`ascending`) and then by vertex ID, using
/// `ascending` for the property and `id_ascending` for ties. It may return a
/// superset; the engine defensively sorts and truncates.
///
/// `required` is a page size, not an answer size. The caller applies a
/// residual the backend knows nothing about, so it cannot say how many of the
/// rows it returns will survive -- it returns that many and reports whether
/// more exist. The caller resumes with `after` until enough rows survive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedPropertyScanRequest {
    pub property: String,
    pub labels: Vec<String>,
    pub ascending: bool,
    pub id_ascending: bool,
    pub prefix: Option<String>,
    pub lower: Option<PropertyBound>,
    pub upper: Option<PropertyBound>,
    /// How many entries to return in this page.
    pub required: usize,
    /// Resume strictly after this position. `None` starts at the window's
    /// beginning.
    pub after: Option<OrderedScanPosition>,
    /// Return entries in the order the backend walks them and stop at exactly
    /// `required`, dropping none, rather than finishing the last value's run
    /// and keeping the best `required` of it by the backend's own ranking.
    ///
    /// That ranking is correct only while it is the caller's and nothing
    /// downstream can reject a row it kept. Neither holds once the caller
    /// orders ties by a key the index does not store, or applies a residual
    /// the backend cannot see: an entry the backend dropped may rank ahead of
    /// one it kept, or be the only survivor of the filter, and the cursor has
    /// moved past it for good. Handing back the whole run instead would be
    /// unbounded -- a tie group can be the entire index -- so the page stops
    /// at `required` and `after` resumes inside the run; the caller reads the
    /// run in pages and keeps only what its window needs.
    pub index_order: bool,
}

impl OrderedPropertyScanRequest {
    /// Whether a stored string value satisfies every restriction carried by
    /// this request. Backends without a native seek can filter with it.
    pub fn accepts(&self, value: &str) -> bool {
        if self
            .prefix
            .as_deref()
            .is_some_and(|prefix| !value.starts_with(prefix))
        {
            return false;
        }
        if let Some(lower) = &self.lower {
            let ScalarValue::String(bound) = &lower.value else {
                return false;
            };
            let ok = if lower.inclusive {
                value >= bound.as_ref()
            } else {
                value > bound.as_ref()
            };
            if !ok {
                return false;
            }
        }
        if let Some(upper) = &self.upper {
            let ScalarValue::String(bound) = &upper.value else {
                return false;
            };
            let ok = if upper.inclusive {
                value <= bound.as_ref()
            } else {
                value < bound.as_ref()
            };
            if !ok {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexRecord {
    pub id: VertexId,
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, ScalarValue>,
}

impl VertexRecord {
    pub fn new(id: VertexId) -> Self {
        Self {
            id,
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.insert(label.into());
        self
    }

    pub fn with_property(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipRecord {
    pub id: RelationshipId,
    /// Canonical stored direction, independent of the query pattern direction.
    pub source: VertexId,
    pub target: VertexId,
    pub relationship_type: String,
    pub properties: BTreeMap<String, ScalarValue>,
}

impl RelationshipRecord {
    pub fn new(
        id: RelationshipId,
        source: VertexId,
        target: VertexId,
        relationship_type: impl Into<String>,
    ) -> Self {
        Self {
            id,
            source,
            target,
            relationship_type: relationship_type.into(),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_property(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryValue {
    Null,
    VertexId(VertexId),
    RelationshipId(RelationshipId),
    Count(u64),
    Scalar(ScalarValue),
    List(Vec<QueryValue>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
}
