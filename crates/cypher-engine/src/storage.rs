use async_trait::async_trait;

use crate::{
    ExpandRequest, GraphPhysicalPlan, OrderedPropertyScanPage, OrderedPropertyScanRequest,
    PropertyEqualityCandidate, ReadRequest, RelationshipRecord, ScalarValue, StorageResult,
    VertexId, VertexRecord,
};

/// One consistent graph read view used for an entire physical plan.
#[async_trait]
pub trait GraphRead: Send {
    /// Cooperative execution checkpoint for CPU-side operator work. Storage
    /// adapters can use this to surface cancellation or deadline expiry while
    /// the engine is processing already-fetched records.
    fn checkpoint(&self, _operation: &'static str) -> StorageResult<()> {
        Ok(())
    }

    /// Admit an intermediate row collection before it grows further. The
    /// default keeps standalone backends unconstrained; production adapters
    /// should enforce their configured query work limits here.
    fn check_intermediate_rows(&self, operation: &'static str, _rows: usize) -> StorageResult<()> {
        self.checkpoint(operation)
    }

    /// Return vertices whose stored property is Cypher-equal to `value`.
    /// Implementations must include equivalent numeric representations such as
    /// integer `1` and float `1.0`; exact KV encoding is a backend concern.
    async fn seek_vertices_by_property(
        &mut self,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<VertexId>>;

    /// Return vertices whose stored property is Cypher-equal to any of
    /// `values`, as the union of one seek per value. Backends that can issue
    /// the seeks together should override this: the default runs them one
    /// after another, which on a remote object store is one round trip per
    /// value. Duplicates across values may be returned; the engine dedupes.
    async fn seek_vertices_by_property_values(
        &mut self,
        property: &str,
        values: &[ScalarValue],
    ) -> StorageResult<Vec<VertexId>> {
        let mut ids = Vec::new();
        for value in values {
            self.checkpoint("cypher_multi_seek")?;
            ids.extend(self.seek_vertices_by_property(property, value).await?);
        }
        Ok(ids)
    }

    /// Return vertices carrying `property`, regardless of its value. This is
    /// the anchored fallback for non-equality predicates such as ranges,
    /// inequality, and string prefixes.
    async fn scan_vertices_by_property(&mut self, property: &str) -> StorageResult<Vec<VertexId>>;

    /// Walk one string property index in order and return hydrated vertices
    /// for a bounded window. See [`OrderedPropertyScanRequest`] for the
    /// contract; this is what lets `ORDER BY p LIMIT n` and keyset pages read
    /// only the prefix of the index they need instead of the whole property.
    async fn scan_vertices_by_property_ordered(
        &mut self,
        request: &OrderedPropertyScanRequest,
    ) -> StorageResult<OrderedPropertyScanPage>;

    /// Return matching vertices in ascending ID order. `max_results` is a
    /// semantic bound from a safe LIMIT pushdown, not a best-effort hint.
    async fn scan_vertices_by_label(
        &mut self,
        label: &str,
        max_results: Option<usize>,
    ) -> StorageResult<Vec<VertexId>>;

    async fn scan_all_vertices(&mut self) -> StorageResult<Vec<VertexId>>;

    /// Return relationships whose stored property is Cypher-equal to `value`.
    /// Implementations must consult both structural-edge and identified-
    /// relationship property indexes when the backing graph distinguishes them.
    async fn seek_relationships_by_property(
        &mut self,
        relationship_type: &str,
        property: &str,
        value: &ScalarValue,
    ) -> StorageResult<Vec<RelationshipRecord>>;

    /// Return relationships incident to the requested input vertices. Results
    /// may be unordered and duplicated; the engine validates direction/type,
    /// deduplicates by relationship identity, and imposes result order.
    async fn expand_relationships(
        &mut self,
        request: &ExpandRequest,
    ) -> StorageResult<Vec<RelationshipRecord>>;

    async fn hydrate_vertices(
        &mut self,
        vertex_ids: &[VertexId],
    ) -> StorageResult<Vec<VertexRecord>>;

    /// Rank equality candidates by a bounded index probe and return the
    /// vertices of the most selective one, or `None` to keep the planned
    /// access path.
    ///
    /// The planner picks an access path before it knows how many vertices any
    /// index actually holds. When several equalities restrict the same
    /// binding, this lets the backend measure instead: give each candidate a
    /// slice of one fixed budget, walk its index, and discard any candidate
    /// that exhausts its slice as too broad to be worth seeking on. A
    /// candidate that finishes inside its slice has proven both that it is
    /// selective and what it matches, so its vertices are returned directly.
    ///
    /// Returning `Some` asserts the vertices are a complete superset of the
    /// rows satisfying that one candidate; the caller still applies labels and
    /// the full predicate. `None` is always safe and is the default, because a
    /// backend without index statistics or a cheap bounded walk has nothing to
    /// add to the planner's choice.
    ///
    /// The engine offers every equality it found rather than a shortlist,
    /// because only the backend knows what a walk costs. A backend is expected
    /// to rank a bounded prefix and ignore the rest: the walks are concurrent,
    /// and one candidate per property of a wide pattern is fan-out no caller
    /// asked for.
    async fn probe_selective_equality(
        &mut self,
        _candidates: &[PropertyEqualityCandidate],
    ) -> StorageResult<Option<Vec<VertexId>>> {
        Ok(None)
    }

    // ---- Operator observation (Workstream 11) ----------------------------
    //
    // The executor calls the two dispatch hooks around every physical
    // operator invocation, and nothing else in it knows profiling exists. A
    // backend that wants per-operator numbers keeps a
    // [`crate::OperatorProfiler`] and forwards these to it; the defaults cost
    // one virtual call each.

    /// A physical operator is about to run. Invocations nest: a parent's
    /// hook fires before its inputs' and finishes after them.
    fn operator_started(&mut self, _plan: &GraphPhysicalPlan) {}

    /// The operator started by the matching [`Self::operator_started`]
    /// produced `rows_out` rows (zero when it failed).
    fn operator_finished(&mut self, _plan: &GraphPhysicalPlan, _rows_out: usize) {}

    /// An operator is holding `rows` rows in memory right now. Report it at
    /// the point a buffer is largest; the profiler keeps the peak per
    /// operator. [`Self::check_intermediate_rows`] callers need not also call
    /// this: a backend that profiles records both.
    fn record_retained_rows(&self, _rows: usize) {}

    /// An operator materialized `vertices` whole vertex records outside
    /// [`Self::hydrate_vertices`], for example inside a bounded scan that
    /// hydrates as it walks.
    fn record_hydrated_vertices(&self, _vertices: usize) {}
}

/// Starts a snapshot-consistent read session for one physical-plan execution.
#[async_trait]
pub trait GraphStorage: Send + Sync {
    async fn begin_read(&self, request: ReadRequest) -> StorageResult<Box<dyn GraphRead + '_>>;
}
