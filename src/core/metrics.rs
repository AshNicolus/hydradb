use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
mod tests;

use crate::engine;
use crate::{AtomicDurationHistogram, DurationHistogramSnapshot, GraphError, SparseKernelBackend};

/// One counter per [`GraphError`] class, indexed by
/// [`GraphError::class_index`].
///
/// An array and not a map: the vocabulary is closed and ten long, so the label
/// lookup is an index rather than a hash, and the length is
/// [`GraphError::CLASS_COUNT`] so that a class added to the vocabulary widens
/// every counter that is dimensioned by one.
pub(crate) type ErrorClassCounters = [AtomicU64; GraphError::CLASS_COUNT];

/// Read a whole per-class counter array into its snapshot form.
///
/// The loads are individually relaxed and the array is therefore not a
/// consistent cut — the same property every other counter on these snapshots
/// has, and for the same reason: these are monotone counters read for rates.
pub(crate) fn load_class_counters(counters: &ErrorClassCounters) -> [u64; GraphError::CLASS_COUNT] {
    std::array::from_fn(|index| counters[index].load(Ordering::Relaxed))
}

/// Where in a client request a failure was raised.
///
/// `Prepare` is everything before the first page is requested: validation,
/// authorization, parameters and -- on both Bolt and HTTP -- parsing, lowering
/// and planning. `Execute` is running the prepared query. The split exists
/// because a prepare failure used to be counted nowhere at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFailureStage {
    Prepare,
    Execute,
}

impl QueryFailureStage {
    pub const ALL: [Self; 2] = [Self::Prepare, Self::Execute];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Execute => "execute",
        }
    }

    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Query-class failures by stage and [`QueryFailureReason`](crate::QueryFailureReason).
///
/// Only failures with a reason are counted: the query language the engines
/// cannot run, parse, request and evaluation errors. Timeouts, admission,
/// routing, storage and the other classes are left to
/// `queries_failed_by_class`; this family answers "which Cypher fails", not
/// "which requests fail".
///
/// Relaxed atomics in a fixed array, like [`ErrorClassCounters`]: recording
/// is an index computation and one `fetch_add` on a failure path.
#[cfg(feature = "client-api")]
pub(crate) struct QueryFailureCounters {
    counts: [[AtomicU64; crate::QueryFailureReason::COUNT]; 2],
}

#[cfg(feature = "client-api")]
impl Default for QueryFailureCounters {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }
}

#[cfg(feature = "client-api")]
impl QueryFailureCounters {
    /// Count `error` if it is a query failure; anything without a
    /// [`GraphError::failure_reason`] is not this family's to count.
    pub(crate) fn record(&self, stage: QueryFailureStage, error: &GraphError) {
        if let Some(reason) = error.failure_reason() {
            self.counts[stage.index()][reason.index()].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> QueryFailureCountsSnapshot {
        QueryFailureCountsSnapshot {
            counts: std::array::from_fn(|stage| {
                std::array::from_fn(|reason| self.counts[stage][reason].load(Ordering::Relaxed))
            }),
        }
    }
}

/// A read of the client's query-failure counters; enumerate it with
/// [`Self::rows`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryFailureCountsSnapshot {
    counts: [[u64; crate::QueryFailureReason::COUNT]; 2],
}

impl QueryFailureCountsSnapshot {
    /// Every `(stage, reason, count)`, zeros included, so a series exists
    /// before its first failure and `rate()` starts at zero rather than at the
    /// first scrape that saw one: 2 stages x 14 reasons = 28 rows.
    pub fn rows(&self) -> impl Iterator<Item = (&'static str, &'static str, u64)> + '_ {
        QueryFailureStage::ALL.into_iter().flat_map(move |stage| {
            crate::QueryFailureReason::ALL
                .into_iter()
                .map(move |reason| {
                    (
                        stage.as_str(),
                        reason.as_str(),
                        self.counts[stage.index()][reason.index()],
                    )
                })
        })
    }
}

/// Which of the four watched shapes one row-query plan contains.
///
/// A struct of flags rather than four bare arguments so a call site cannot
/// silently transpose two of them — they are all `bool` and the compiler would
/// not notice. Built by the planner, which is the only place that can see a
/// plan; the counters it feeds live here because that is where every other
/// operational counter lives.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QueryPlanShapes {
    pub(crate) label_scan: bool,
    pub(crate) property_index: bool,
    pub(crate) full_scan: bool,
    pub(crate) equality_pushdown: bool,
}

/// Enumerate a metrics snapshot's fields, keyed by the **Rust identifier**.
///
/// Generates `counter_fields()`, `histogram_fields()` and
/// `class_counter_fields()` on `$ty`. The key is the identifier and nothing
/// else: a Prometheus `graph_*` name and an OTel `db.*`/`hydradb.*` name are
/// exposition vocabulary, and neither may appear in this crate. The binaries
/// hold the name tables, which is also where the test that the two exports
/// cannot disagree about a name belongs.
///
/// The destructuring pattern is deliberately **exhaustive** — no `..` arm. Add
/// a field to the snapshot struct and every accessor stops compiling until it is
/// classified as a counter, a histogram or a per-class counter, which is what
/// turns "adding a counter must not silently reach one export and not the other"
/// from a review comment into a build failure. The binary's
/// `every_histogram_field_reaches_both_exports` picks up where this leaves off:
/// this macro proves the field is *enumerated*, that test proves it is *named*.
///
/// The `class_counters` block is optional, and only because making it mandatory
/// would be a mechanical edit to every existing call site rather than because
/// omitting it is a different thing from writing it empty.
macro_rules! snapshot_fields {
    (
        $ty:ident {
            counters { $($counter:ident),* $(,)? }
            histograms { $($histogram:ident),* $(,)? }
            $(class_counters { $($class_counter:ident),* $(,)? })?
            $(failure_counters { $($failure_counter:ident),* $(,)? })?
            $(structured { $($structured:ident),* $(,)? })?
        }
    ) => {
        impl $ty {
            /// Every scalar counter on this snapshot, keyed by its Rust
            /// identifier, in declaration order.
            pub fn counter_fields(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter,)*
                    $($histogram: _,)*
                    $($($class_counter: _,)*)?
                    $($($failure_counter: _,)*)?
                    $($($structured: _,)*)?
                } = self;
                [$((stringify!($counter), *$counter),)*].into_iter()
            }

            /// Every duration histogram on this snapshot, keyed by its Rust
            /// identifier, in declaration order.
            pub fn histogram_fields(
                &self,
            ) -> impl Iterator<Item = (&'static str, &$crate::DurationHistogramSnapshot)> + '_
            {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter: _,)*
                    $($histogram,)*
                    $($($class_counter: _,)*)?
                    $($($failure_counter: _,)*)?
                    $($($structured: _,)*)?
                } = self;
                [$((stringify!($histogram), $histogram),)*].into_iter()
            }

            /// Every counter dimensioned by [`crate::GraphError::class`], as
            /// `(field, class, count)` triples: one row per field per class, in
            /// declaration order and then in [`crate::GraphError::CLASSES`]
            /// order.
            ///
            /// Flattened rather than handed over as an array because the class
            /// name is the label value an export needs, and pairing it with its
            /// count here is the one place that pairing can be got wrong. The
            /// export layer sees rows, not offsets.
            pub fn class_counter_fields(
                &self,
            ) -> impl Iterator<Item = (&'static str, &'static str, u64)> + '_ {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter: _,)*
                    $($histogram: _,)*
                    $($($class_counter,)*)?
                    $($($failure_counter: _,)*)?
                    $($($structured: _,)*)?
                } = self;
                [$($((stringify!($class_counter), $class_counter),)*)?]
                    .into_iter()
                    .flat_map(
                        |(field, counts): (
                            &'static str,
                            &[u64; $crate::GraphError::CLASS_COUNT],
                        )| {
                            $crate::GraphError::CLASSES
                                .into_iter()
                                .zip(counts.iter().copied())
                                .map(move |(class, count)| (field, class, count))
                        },
                    )
            }

            /// Every counter dimensioned by stage and failure reason, as
            /// `(field, stage, reason, count)` rows in declaration order and
            /// then in [`crate::QueryFailureCountsSnapshot::rows`] order.
            pub fn failure_counter_fields(
                &self,
            ) -> impl Iterator<Item = (&'static str, &'static str, &'static str, u64)> + '_ {
                // Exhaustive on purpose; see the macro's documentation.
                let Self {
                    $($counter: _,)*
                    $($histogram: _,)*
                    $($($class_counter: _,)*)?
                    $($($failure_counter,)*)?
                    $($($structured: _,)*)?
                } = self;
                let fields: Vec<(&'static str, &$crate::QueryFailureCountsSnapshot)> =
                    vec![$($((stringify!($failure_counter), $failure_counter),)*)?];
                fields.into_iter().flat_map(|(field, counts)| {
                    counts
                        .rows()
                        .map(move |(stage, reason, count)| (field, stage, reason, count))
                })
            }
        }
    };
}

// Both out-of-module users sit behind `query-transport` -- `ClientQueryMetrics`
// behind `client-api`, which implies it, and `QueryTransportMetrics` directly --
// while `GraphOperationalMetricsSnapshot` below invokes the macro by name and
// does not go through the re-export. Under default features the re-export is
// therefore genuinely unused, and cfg-ing it is more honest than an `allow`.
#[cfg(feature = "query-transport")]
pub(crate) use snapshot_fields;

/// Non-exhaustive; see [`crate::GraphOpenOptions`] for the construction pattern.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GraphCachePolicy {
    pub max_matrix_artifacts: usize,
    pub max_matrix_adjacencies: usize,
    pub max_graphblas_matrices: usize,
    /// Which rung of the sparse-kernel ladder this shard traverses on. Read
    /// once, at matrix-compile time, and baked into the compiled artifact.
    pub sparse_kernel: SparseKernelBackend,
    #[cfg(feature = "opencypher")]
    pub max_parsed_row_queries: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_row_sets: usize,
    #[cfg(feature = "opencypher")]
    pub max_relationship_property_row_sets: usize,
    pub max_entries_per_cell: Option<usize>,
    pub pin_matrix_min_edges: u64,
    pub max_concurrent_hydrations: usize,
}

impl Default for GraphCachePolicy {
    fn default() -> Self {
        Self {
            max_matrix_artifacts: 1_024,
            max_matrix_adjacencies: 0,
            max_graphblas_matrices: 64,
            sparse_kernel: crate::sparse_kernel::env_default_kernel(),
            #[cfg(feature = "opencypher")]
            max_parsed_row_queries: 4_096,
            #[cfg(feature = "opencypher")]
            max_relationship_row_sets: 1_024,
            #[cfg(feature = "opencypher")]
            max_relationship_property_row_sets: 4_096,
            max_entries_per_cell: Some(8_192),
            pin_matrix_min_edges: 1_000_000,
            max_concurrent_hydrations: 16,
        }
    }
}

impl GraphCachePolicy {
    pub(crate) fn hydration_permits(&self) -> usize {
        self.max_concurrent_hydrations.max(1)
    }

    pub(crate) fn pin_matrix_artifact(&self, artifact: &engine::MatrixArtifact) -> bool {
        artifact.edge_count >= self.pin_matrix_min_edges
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphCacheKind {
    MatrixArtifact,
    MatrixAdjacency,
    GraphBlas,
    ParsedRowQuery,
    RelationshipRows,
    RelationshipPropertyRows,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphCacheMetricsSnapshot {
    pub matrix_artifact_hits: u64,
    pub matrix_artifact_misses: u64,
    pub matrix_adjacency_hits: u64,
    pub matrix_adjacency_misses: u64,
    pub graphblas_hits: u64,
    pub graphblas_misses: u64,
    pub parsed_row_query_hits: u64,
    pub parsed_row_query_misses: u64,
    pub relationship_rows_hits: u64,
    pub relationship_rows_misses: u64,
    pub relationship_property_rows_hits: u64,
    pub relationship_property_rows_misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub pinned_insertions: u64,
    pub tenant_quota_rejections: u64,
    pub hydration_started: u64,
    pub hydration_waited: u64,
    pub hydration_completed: u64,
}

// Nineteen counters that reached no export at all until M2. The `cache` field
// they arrive on (`crate::GraphShardRuntimeMetrics::cache`) was always
// populated by `local_shard_runtime_metrics` -- the gap was on the export side,
// not the plumbing side.
//
// No histograms, and the empty block is written out rather than made optional:
// "this type records no durations" is a claim worth stating, and a duration
// added here later has to delete the empty block to compile, which is exactly
// when somebody should be looking at it.
snapshot_fields!(GraphCacheMetricsSnapshot {
    counters {
        matrix_artifact_hits,
        matrix_artifact_misses,
        matrix_adjacency_hits,
        matrix_adjacency_misses,
        graphblas_hits,
        graphblas_misses,
        parsed_row_query_hits,
        parsed_row_query_misses,
        relationship_rows_hits,
        relationship_rows_misses,
        relationship_property_rows_hits,
        relationship_property_rows_misses,
        insertions,
        evictions,
        pinned_insertions,
        tenant_quota_rejections,
        hydration_started,
        hydration_waited,
        hydration_completed,
    }
    histograms {}
});

/// One committed `import_relationships_batch_txn_locked` transaction's storage
/// time, split by phase. Built incrementally by the write path and handed to
/// `GraphShard::record_relationship_import_profile` after the commit, so the
/// counters only ever see whole, successful batches.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RelationshipImportProfile {
    pub(crate) endpoint_check: std::time::Duration,
    pub(crate) identity_scan: std::time::Duration,
    pub(crate) identity_pointer_hits: u64,
    pub(crate) identity_pointer_misses: u64,
    pub(crate) record_read: std::time::Duration,
    pub(crate) structural_check: std::time::Duration,
    pub(crate) segment_scans: u64,
    pub(crate) segment_neighbors: u64,
    pub(crate) counter_read: std::time::Duration,
    pub(crate) commit: std::time::Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOperationalMetricsSnapshot {
    pub write_attempts: u64,
    pub write_commits: u64,
    pub write_retries: u64,
    pub bulk_import_batches_profiled: u64,
    pub bulk_import_preflight_us: u64,
    pub bulk_import_batch_build_us: u64,
    pub bulk_import_counter_read_us: u64,
    pub bulk_import_commit_us: u64,
    /// Batches profiled through `import_relationships_batch_txn_locked` — the
    /// relationship CREATE/MERGE path every Bolt `UNWIND` write lands on. The
    /// `relationship_import_*_us` sums below share this denominator, exactly as
    /// the `bulk_import_*_us` sums share `bulk_import_batches_profiled`.
    pub relationship_import_batches_profiled: u64,
    /// Endpoint label validation: reads of every distinct endpoint vertex.
    pub relationship_import_endpoint_check_us: u64,
    /// MERGE identity resolution: one `rmerge_idx` point get per row, plus a
    /// property-index prefix scan on the rows whose pointer missed.
    pub relationship_import_identity_scan_us: u64,
    /// MERGE rows whose identity resolved by `rmerge_idx` point get alone.
    pub relationship_import_identity_pointer_hits: u64,
    /// MERGE rows that fell back to the property-index prefix scan (no
    /// pointer yet, or a stale one). Each miss heals the pointer, so under a
    /// steady workload this trends toward first-touch rows only.
    pub relationship_import_identity_pointer_misses: u64,
    /// Relationship id-pointer and record reads.
    pub relationship_import_record_read_us: u64,
    /// Structural-edge existence reconciliation: direct `out_edge` reads plus,
    /// on a miss, adjacency segment scans. Skipped entirely on a fresh cell.
    pub relationship_import_structural_check_us: u64,
    /// Source vertices whose adjacency segments were scanned during the
    /// structural check.
    pub relationship_import_segment_scans: u64,
    /// Destinations materialised by those segment scans — the volume the scan
    /// decoded to answer per-pair existence questions. If write latency tracks
    /// this rather than batch size, the cost is the source's fan-out.
    pub relationship_import_segment_neighbors: u64,
    /// Degree and relationship-count counter reads. Skipped on a fresh cell.
    pub relationship_import_counter_read_us: u64,
    /// The transaction commit, including the durable WAL wait.
    pub relationship_import_commit_us: u64,
    /// Relationship import batches that hit a durable idempotency record and
    /// returned the cached result without proceeding to the write phase. The
    /// transaction is already open when this fires (the idempotency key is read
    /// inside a `SerializableSnapshot` transaction), so this measures avoided
    /// writes, not avoided transactions.
    pub relationship_import_idempotency_replays: u64,
    /// Batches where `merge_vertex_metadata_batch` read all vertices and found
    /// nothing changed — the transaction returned `Ok(0)` without committing.
    pub merge_vertex_metadata_nochange_exits: u64,
    /// Batches where `delete_vertex_mutations_batch` found every deletion was
    /// an idempotency replay — the transaction returned cached results without
    /// committing.
    pub delete_vertex_batch_all_replays: u64,
    /// Batches profiled through `merge_vertex_metadata_batch_txn_locked`.
    pub merge_vertex_metadata_batches_profiled: u64,
    /// Items (vertices) in the batch.
    pub merge_vertex_metadata_batch_items: u64,
    /// Microseconds spent reading vertex keys (the sequential read phase).
    pub merge_vertex_metadata_read_us: u64,
    /// Microseconds for the full transaction (excluding permit/lock wait).
    pub merge_vertex_metadata_txn_us: u64,
    /// Batches profiled through `delete_vertex_mutations_batch_txn_locked`.
    pub delete_vertex_batch_batches_profiled: u64,
    /// Items (vertices) in the batch.
    pub delete_vertex_batch_items: u64,
    /// Microseconds spent reading idempotency + vertex keys.
    pub delete_vertex_batch_read_us: u64,
    /// Microseconds for the full transaction (excluding permit/lock wait).
    pub delete_vertex_batch_txn_us: u64,
    /// Batches profiled through `reserve_edge_delete_noops_batch_txn_locked`.
    pub reserve_edge_delete_noops_batches_profiled: u64,
    /// Items (mutations) in the batch.
    pub reserve_edge_delete_noops_batch_items: u64,
    /// Microseconds spent reading idempotency keys.
    pub reserve_edge_delete_noops_read_us: u64,
    /// Microseconds for the full transaction (excluding permit/lock wait).
    pub reserve_edge_delete_noops_txn_us: u64,
    pub artifact_builds_started: u64,
    pub artifact_builds_completed: u64,
    pub artifact_build_duration_us: u64,
    pub artifact_publish_batches: u64,
    pub artifact_records_published: u64,
    pub artifact_publish_duration_us: u64,
    pub gc_jobs_started: u64,
    pub gc_jobs_completed: u64,
    pub gc_keys_deleted: u64,
    pub gc_duration_us: u64,
    pub verifier_runs: u64,
    pub verifier_failures: u64,
    pub verifier_duration_us: u64,
    pub query_rows_started: u64,
    pub query_rows_completed: u64,
    pub query_rows_failed: u64,
    /// The same failures as [`Self::query_rows_failed`], split by
    /// [`crate::GraphError::class`] and indexed by
    /// [`crate::GraphError::class_index`].
    ///
    /// Total by construction, not by convention: both are incremented by the
    /// same call, so the array sums to the scalar and a dashboard can use one to
    /// check the other. Enumerate it with
    /// [`Self::class_counter_fields`] rather than by offset.
    pub query_rows_failed_by_class: [u64; GraphError::CLASS_COUNT],
    pub query_rows_returned: u64,
    /// Total microseconds spent in the row-query path.
    ///
    /// Retained under its old name and type so nothing that read the sum has to
    /// change, but derived from [`Self::query_rows_latency`] rather than stored:
    /// one `fetch_add` on one quantity, so the sum and the distribution cannot
    /// drift apart.
    pub query_rows_duration_us: u64,
    /// The same measurement as a distribution. Every row query -- one-shot and
    /// streaming, success and failure -- lands here.
    pub query_rows_latency: DurationHistogramSnapshot,
    /// Property-index seek calls issued by the experimental Cypher engine.
    ///
    /// One multi-seek value is one storage request, so a two-value OR adds two.
    /// This is deliberately separate from the legacy plan-shape counters: it
    /// measures work the selected physical plan actually asked storage to do.
    pub query_experimental_property_seek_requests: u64,
    /// Batched relationship-expansion calls issued by the experimental engine.
    ///
    /// One physical `ExpandExec` invocation adds one even when it carries many
    /// input vertices; lower-level SlateDB get/scan counters retain the storage
    /// amplification underneath that semantic request.
    pub query_experimental_relationship_expand_requests: u64,
    /// Bounded ordered property-index walks issued by the experimental engine's
    /// `OrderedVertexPropertyScan` operator.
    ///
    /// Kept apart from [`Self::query_experimental_property_seek_requests`] on
    /// purpose: that counter also covers the unbounded property scan this
    /// operator replaces, so a planner regression back to the broad scan would
    /// otherwise be invisible. One physical operator invocation adds one.
    pub query_experimental_ordered_property_scan_requests: u64,
    /// Experimental read requests that reached lowering. The denominator of every `query_experimental_*_us` sum below; EXPLAIN is excluded, as it is from `query_rows_started`.
    pub query_experimental_requests: u64,
    /// Parser time, microseconds. Zero on a lowered-template cache hit.
    pub query_experimental_parse_us: u64,
    /// AST-to-logical lowering time, microseconds. Zero on a cache hit.
    pub query_experimental_lower_us: u64,
    /// Parameter conversion, template-cache lookup and binding the template
    /// to this request's parameters, microseconds. Paid on every request,
    /// cache hit or not.
    pub query_experimental_bind_us: u64,
    /// Time to pin the SlateDB snapshot, microseconds.
    pub query_experimental_snapshot_us: u64,
    /// Statistics wait: cell readiness plus loading planner statistics, microseconds.
    pub query_experimental_statistics_us: u64,
    /// Physical planning time, microseconds.
    pub query_experimental_plan_us: u64,
    /// Physical plan execution, microseconds: storage I/O plus operator CPU. Operator CPU is this minus `query_experimental_storage_us`.
    pub query_experimental_execute_us: u64,
    /// Time inside storage-adapter calls during execution, microseconds. Overlapping calls (a batched multi-seek) are counted once, as wall time of the call that issued them.
    pub query_experimental_storage_us: u64,
    /// Storage-adapter calls issued during execution, of every kind.
    pub query_experimental_storage_calls: u64,
    /// Result windowing and conversion to HydraDB values after execution, microseconds.
    pub query_experimental_result_us: u64,
    /// Experimental requests that tripped a slow-plan sampling trigger (latency, width, full scan or unexpected fallback) and were marked for tail sampling.
    pub query_experimental_sampled_plans: u64,
    /// Row queries whose context selected the legacy engine.
    pub query_route_legacy_requests: u64,
    /// Row queries executed by the experimental read engine.
    pub query_route_experimental_requests: u64,
    /// Experimental-context queries served by the shared native path-procedure executor instead of the experimental read engine.
    pub query_route_native_path_fallbacks: u64,
    /// Experimental-context queries served by the shared mutation executor.
    pub query_route_mutation_fallbacks: u64,
    pub query_property_fetches: u64,
    pub query_property_fetch_latency: DurationHistogramSnapshot,
    pub query_artifact_lookup_us: u64,
    pub query_graphblas_cache_us: u64,
    pub query_graphblas_artifact_snapshots: u64,
    pub query_graphblas_rebuilt_snapshots: u64,
    pub query_rust_sparse_fallbacks: u64,
    /// Row-query plans built, and the shapes among them worth watching.
    ///
    /// One increment per executed row query — the per-group re-planning inside
    /// the match loop runs once per input row and is deliberately not counted,
    /// or the denominator would scale with result size rather than with
    /// traffic. EXPLAIN does not count either: it plans without executing, and
    /// mixing the two would make the ratios below unreadable.
    ///
    /// [`Self::query_plans_total`] is the denominator the other four are read
    /// against. A plan can contribute to more than one of them — a query with
    /// two patterns can seek one and scan the other — so they do not sum to the
    /// total and are not meant to.
    pub query_plans_total: u64,
    /// Plans containing at least one `VertexLabelScan`.
    ///
    /// The series that would have shown the WHERE-equality pathology: a label
    /// scan is not a full scan, so nothing else counted it. Read as a fraction
    /// of [`Self::query_plans_total`], per cell.
    pub query_plans_with_label_scan: u64,
    /// Plans containing at least one `VertexPropertyIndex` seek — the healthy
    /// counterpart of [`Self::query_plans_with_label_scan`].
    pub query_plans_with_property_index: u64,
    /// Plans containing an `AllVertexScan` or `FullEdgeScan`, or that fell back
    /// to one. The same verdict the `query.plan` span reports as
    /// `hydradb.query.full_scan`, made chartable.
    pub query_plans_with_full_scan: u64,
    /// Plans where a WHERE equality was folded into a node pattern.
    ///
    /// Adoption, not health: it answers "is the pushdown reaching production
    /// traffic", which is otherwise only visible one trace at a time.
    pub query_plans_with_equality_pushdown: u64,
    pub graph_compute_tasks: u64,
    pub graph_compute_queue_us: u64,
    pub graph_compute_duration_us: u64,
    pub backpressure_waits: u64,
    pub create_relationships_batch_latency: DurationHistogramSnapshot,
    pub delete_relationship_mutations_batch_latency: DurationHistogramSnapshot,
    pub delete_vertices_and_isolated_candidates_batch_latency: DurationHistogramSnapshot,
    pub detach_delete_vertices_batch_latency: DurationHistogramSnapshot,
    pub merge_relationships_batch_latency: DurationHistogramSnapshot,
    pub merge_vertex_metadata_batch_latency: DurationHistogramSnapshot,
    pub reserve_edge_delete_noops_batch_latency: DurationHistogramSnapshot,
    /// Per physical-operator totals from the experimental engine, one entry
    /// per operator that has run on this shard, sorted by operator name.
    /// Structured rather than enumerated as counters: the operator is a label
    /// on its own families, not a field. Empty without the
    /// `experimental-cypher-engine` feature.
    pub experimental_operators: Vec<ExperimentalOperatorMetricsSnapshot>,
}

/// Cumulative work of one physical operator kind on one shard.
///
/// Every field but `operator` is a counter. `peak_retained_rows` is the sum
/// over requests of the operator's peak in that request, so divided by
/// `requests` it is the mean peak per request; it is not a high-water mark.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExperimentalOperatorMetricsSnapshot {
    /// `GraphPhysicalPlan::operator_name`: a closed vocabulary, safe as a
    /// label.
    pub operator: &'static str,
    /// Requests in which the operator ran at least once.
    pub requests: u64,
    pub invocations: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub self_us: u64,
    pub storage_us: u64,
    pub storage_requests: u64,
    pub storage_bytes: u64,
    pub hydrated_vertices: u64,
    pub scanned_relationships: u64,
    pub peak_retained_rows: u64,
    /// Requests in which the operator's estimate missed its actual rows by
    /// the sampling ratio.
    pub estimate_misses: u64,
}

impl ExperimentalOperatorMetricsSnapshot {
    /// Every counter, keyed by its field name, in declaration order. The
    /// export tables are keyed by these names.
    pub fn counter_fields(&self) -> [(&'static str, u64); 12] {
        [
            ("requests", self.requests),
            ("invocations", self.invocations),
            ("rows_in", self.rows_in),
            ("rows_out", self.rows_out),
            ("self_us", self.self_us),
            ("storage_us", self.storage_us),
            ("storage_requests", self.storage_requests),
            ("storage_bytes", self.storage_bytes),
            ("hydrated_vertices", self.hydrated_vertices),
            ("scanned_relationships", self.scanned_relationships),
            ("peak_retained_rows", self.peak_retained_rows),
            ("estimate_misses", self.estimate_misses),
        ]
    }

    /// Add `other`'s counters into `self`. Used to fold one request into a
    /// shard's totals and to sum shards sharing a cell.
    pub fn accumulate(&mut self, other: &Self) {
        self.requests += other.requests;
        self.invocations += other.invocations;
        self.rows_in += other.rows_in;
        self.rows_out += other.rows_out;
        self.self_us += other.self_us;
        self.storage_us += other.storage_us;
        self.storage_requests += other.storage_requests;
        self.storage_bytes += other.storage_bytes;
        self.hydrated_vertices += other.hydrated_vertices;
        self.scanned_relationships += other.scanned_relationships;
        self.peak_retained_rows += other.peak_retained_rows;
        self.estimate_misses += other.estimate_misses;
    }
}

snapshot_fields!(GraphOperationalMetricsSnapshot {
    counters {
        write_attempts,
        write_commits,
        write_retries,
        bulk_import_batches_profiled,
        bulk_import_preflight_us,
        bulk_import_batch_build_us,
        bulk_import_counter_read_us,
        bulk_import_commit_us,
        relationship_import_batches_profiled,
        relationship_import_endpoint_check_us,
        relationship_import_identity_scan_us,
        relationship_import_identity_pointer_hits,
        relationship_import_identity_pointer_misses,
        relationship_import_record_read_us,
        relationship_import_structural_check_us,
        relationship_import_segment_scans,
        relationship_import_segment_neighbors,
        relationship_import_counter_read_us,
        relationship_import_commit_us,
        relationship_import_idempotency_replays,
        merge_vertex_metadata_nochange_exits,
        delete_vertex_batch_all_replays,
        merge_vertex_metadata_batches_profiled,
        merge_vertex_metadata_batch_items,
        merge_vertex_metadata_read_us,
        merge_vertex_metadata_txn_us,
        delete_vertex_batch_batches_profiled,
        delete_vertex_batch_items,
        delete_vertex_batch_read_us,
        delete_vertex_batch_txn_us,
        reserve_edge_delete_noops_batches_profiled,
        reserve_edge_delete_noops_batch_items,
        reserve_edge_delete_noops_read_us,
        reserve_edge_delete_noops_txn_us,
        artifact_builds_started,
        artifact_builds_completed,
        artifact_build_duration_us,
        artifact_publish_batches,
        artifact_records_published,
        artifact_publish_duration_us,
        gc_jobs_started,
        gc_jobs_completed,
        gc_keys_deleted,
        gc_duration_us,
        verifier_runs,
        verifier_failures,
        verifier_duration_us,
        query_rows_started,
        query_rows_completed,
        query_rows_failed,
        query_rows_returned,
        query_rows_duration_us,
        query_experimental_property_seek_requests,
        query_experimental_relationship_expand_requests,
        query_experimental_ordered_property_scan_requests,
        query_experimental_requests,
        query_experimental_parse_us,
        query_experimental_lower_us,
        query_experimental_bind_us,
        query_experimental_snapshot_us,
        query_experimental_statistics_us,
        query_experimental_plan_us,
        query_experimental_execute_us,
        query_experimental_storage_us,
        query_experimental_storage_calls,
        query_experimental_result_us,
        query_experimental_sampled_plans,
        query_route_legacy_requests,
        query_route_experimental_requests,
        query_route_native_path_fallbacks,
        query_route_mutation_fallbacks,
        query_property_fetches,
        query_artifact_lookup_us,
        query_graphblas_cache_us,
        query_graphblas_artifact_snapshots,
        query_graphblas_rebuilt_snapshots,
        query_rust_sparse_fallbacks,
        query_plans_total,
        query_plans_with_label_scan,
        query_plans_with_property_index,
        query_plans_with_full_scan,
        query_plans_with_equality_pushdown,
        graph_compute_tasks,
        graph_compute_queue_us,
        graph_compute_duration_us,
        backpressure_waits,
    }
    histograms {
        query_rows_latency,
        query_property_fetch_latency,
        create_relationships_batch_latency,
        delete_relationship_mutations_batch_latency,
        delete_vertices_and_isolated_candidates_batch_latency,
        detach_delete_vertices_batch_latency,
        merge_relationships_batch_latency,
        merge_vertex_metadata_batch_latency,
        reserve_edge_delete_noops_batch_latency,
    }
    class_counters {
        query_rows_failed_by_class,
    }
    structured {
        experimental_operators,
    }
});

/// The executor a Cypher row query was dispatched to. A closed vocabulary:
/// each value is its own per-cell counter rather than a label value, like
/// every other shard family.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CypherRoute {
    /// The context selected the legacy engine.
    Legacy,
    /// The experimental read engine planned and executed the query.
    #[cfg(feature = "experimental-cypher-engine")]
    Experimental,
    /// The context selected the experimental engine, and a native path
    /// procedure took the shared path-procedure executor instead.
    NativePathFallback,
    /// The context selected the experimental engine, and a mutation took the
    /// shared mutation executor instead.
    MutationFallback,
}

#[derive(Default)]
pub(crate) struct GraphOperationalMetrics {
    pub(crate) write_attempts: AtomicU64,
    pub(crate) write_commits: AtomicU64,
    pub(crate) write_retries: AtomicU64,
    pub(crate) bulk_import_batches_profiled: AtomicU64,
    pub(crate) bulk_import_preflight_us: AtomicU64,
    pub(crate) bulk_import_batch_build_us: AtomicU64,
    pub(crate) bulk_import_counter_read_us: AtomicU64,
    pub(crate) bulk_import_commit_us: AtomicU64,
    pub(crate) relationship_import_batches_profiled: AtomicU64,
    pub(crate) relationship_import_endpoint_check_us: AtomicU64,
    pub(crate) relationship_import_identity_scan_us: AtomicU64,
    pub(crate) relationship_import_identity_pointer_hits: AtomicU64,
    pub(crate) relationship_import_identity_pointer_misses: AtomicU64,
    pub(crate) relationship_import_record_read_us: AtomicU64,
    pub(crate) relationship_import_structural_check_us: AtomicU64,
    pub(crate) relationship_import_segment_scans: AtomicU64,
    pub(crate) relationship_import_segment_neighbors: AtomicU64,
    pub(crate) relationship_import_counter_read_us: AtomicU64,
    pub(crate) relationship_import_commit_us: AtomicU64,
    pub(crate) relationship_import_idempotency_replays: AtomicU64,
    pub(crate) merge_vertex_metadata_nochange_exits: AtomicU64,
    pub(crate) delete_vertex_batch_all_replays: AtomicU64,
    pub(crate) merge_vertex_metadata_batches_profiled: AtomicU64,
    pub(crate) merge_vertex_metadata_batch_items: AtomicU64,
    pub(crate) merge_vertex_metadata_read_us: AtomicU64,
    pub(crate) merge_vertex_metadata_txn_us: AtomicU64,
    pub(crate) delete_vertex_batch_batches_profiled: AtomicU64,
    pub(crate) delete_vertex_batch_items: AtomicU64,
    pub(crate) delete_vertex_batch_read_us: AtomicU64,
    pub(crate) delete_vertex_batch_txn_us: AtomicU64,
    pub(crate) reserve_edge_delete_noops_batches_profiled: AtomicU64,
    pub(crate) reserve_edge_delete_noops_batch_items: AtomicU64,
    pub(crate) reserve_edge_delete_noops_read_us: AtomicU64,
    pub(crate) reserve_edge_delete_noops_txn_us: AtomicU64,
    pub(crate) artifact_builds_started: AtomicU64,
    pub(crate) artifact_builds_completed: AtomicU64,
    pub(crate) artifact_build_duration_us: AtomicU64,
    pub(crate) artifact_publish_batches: AtomicU64,
    pub(crate) artifact_records_published: AtomicU64,
    pub(crate) artifact_publish_duration_us: AtomicU64,
    pub(crate) gc_jobs_started: AtomicU64,
    pub(crate) gc_jobs_completed: AtomicU64,
    pub(crate) gc_keys_deleted: AtomicU64,
    pub(crate) gc_duration_us: AtomicU64,
    pub(crate) verifier_runs: AtomicU64,
    pub(crate) verifier_failures: AtomicU64,
    pub(crate) verifier_duration_us: AtomicU64,
    pub(crate) query_rows_started: AtomicU64,
    pub(crate) query_rows_completed: AtomicU64,
    pub(crate) query_rows_failed: AtomicU64,
    pub(crate) query_rows_failed_by_class: ErrorClassCounters,
    pub(crate) query_rows_returned: AtomicU64,
    // Replaces the `query_rows_duration_us` sum. The snapshot field of that
    // name survives, derived from this histogram's `sum_us`, so the only thing
    // that changed for a reader is that the distribution is now there too.
    pub(crate) query_rows_latency: AtomicDurationHistogram,
    pub(crate) query_experimental_property_seek_requests: AtomicU64,
    pub(crate) query_experimental_relationship_expand_requests: AtomicU64,
    pub(crate) query_experimental_ordered_property_scan_requests: AtomicU64,
    pub(crate) query_experimental_requests: AtomicU64,
    pub(crate) query_experimental_parse_us: AtomicU64,
    pub(crate) query_experimental_lower_us: AtomicU64,
    pub(crate) query_experimental_bind_us: AtomicU64,
    pub(crate) query_experimental_snapshot_us: AtomicU64,
    pub(crate) query_experimental_statistics_us: AtomicU64,
    pub(crate) query_experimental_plan_us: AtomicU64,
    pub(crate) query_experimental_execute_us: AtomicU64,
    pub(crate) query_experimental_storage_us: AtomicU64,
    pub(crate) query_experimental_storage_calls: AtomicU64,
    pub(crate) query_experimental_result_us: AtomicU64,
    pub(crate) query_experimental_sampled_plans: AtomicU64,
    pub(crate) query_route_legacy_requests: AtomicU64,
    pub(crate) query_route_experimental_requests: AtomicU64,
    pub(crate) query_route_native_path_fallbacks: AtomicU64,
    pub(crate) query_route_mutation_fallbacks: AtomicU64,
    pub(crate) query_property_fetches: AtomicU64,
    pub(crate) query_property_fetch_latency: AtomicDurationHistogram,
    pub(crate) query_artifact_lookup_us: AtomicU64,
    pub(crate) query_graphblas_cache_us: AtomicU64,
    pub(crate) query_graphblas_artifact_snapshots: AtomicU64,
    pub(crate) query_graphblas_rebuilt_snapshots: AtomicU64,
    pub(crate) query_rust_sparse_fallbacks: AtomicU64,
    pub(crate) query_plans_total: AtomicU64,
    pub(crate) query_plans_with_label_scan: AtomicU64,
    pub(crate) query_plans_with_property_index: AtomicU64,
    pub(crate) query_plans_with_full_scan: AtomicU64,
    pub(crate) query_plans_with_equality_pushdown: AtomicU64,
    pub(crate) graph_compute_tasks: AtomicU64,
    pub(crate) graph_compute_queue_us: AtomicU64,
    pub(crate) graph_compute_duration_us: AtomicU64,
    pub(crate) backpressure_waits: AtomicU64,
    pub(crate) create_relationships_batch_latency: AtomicDurationHistogram,
    pub(crate) delete_relationship_mutations_batch_latency: AtomicDurationHistogram,
    pub(crate) delete_vertices_and_isolated_candidates_batch_latency: AtomicDurationHistogram,
    pub(crate) detach_delete_vertices_batch_latency: AtomicDurationHistogram,
    pub(crate) merge_relationships_batch_latency: AtomicDurationHistogram,
    pub(crate) merge_vertex_metadata_batch_latency: AtomicDurationHistogram,
    pub(crate) reserve_edge_delete_noops_batch_latency: AtomicDurationHistogram,
    /// Folded once per experimental request, under one lock, never per
    /// operator invocation.
    pub(crate) experimental_operators: std::sync::Mutex<
        std::collections::BTreeMap<&'static str, ExperimentalOperatorMetricsSnapshot>,
    >,
}

impl GraphOperationalMetrics {
    /// Count one failed row query, both in total and under its error class.
    ///
    /// One call rather than two `fetch_add`s at each site, so the scalar and the
    /// array cannot disagree about how many failures there were — the sum over
    /// classes is the total by construction. Gated because both callers are, and
    /// every path to them runs through `opencypher`.
    #[cfg(feature = "opencypher")]
    pub(crate) fn record_query_rows_failure(&self, error: &GraphError) {
        self.query_rows_failed.fetch_add(1, Ordering::Relaxed);
        self.query_rows_failed_by_class[error.class_index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Count one property-index seek the experimental physical plan actually
    /// sent through its storage adapter.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) fn record_experimental_property_seek(&self) {
        self.query_experimental_property_seek_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count one batched relationship expansion requested by the experimental
    /// physical executor.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) fn record_experimental_relationship_expand(&self) {
        self.query_experimental_relationship_expand_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count one bounded ordered property-index walk requested by the
    /// experimental physical executor.
    #[cfg(feature = "experimental-cypher-engine")]
    pub(crate) fn record_experimental_ordered_property_scan(&self) {
        self.query_experimental_ordered_property_scan_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count which executor a Cypher row query was dispatched to.
    #[cfg(feature = "opencypher")]
    pub(crate) fn record_cypher_route(&self, route: CypherRoute) {
        match route {
            CypherRoute::Legacy => &self.query_route_legacy_requests,
            #[cfg(feature = "experimental-cypher-engine")]
            CypherRoute::Experimental => &self.query_route_experimental_requests,
            CypherRoute::NativePathFallback => &self.query_route_native_path_fallbacks,
            CypherRoute::MutationFallback => &self.query_route_mutation_fallbacks,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// Count one row-query plan and the shapes it contains.
    ///
    /// One call rather than five `fetch_add`s at the call site, for the same
    /// reason [`Self::record_query_rows_failure`] is one call: the denominator
    /// and its breakdowns are then incremented together by construction, so a
    /// ratio read off them is always over the same population. The flags are
    /// per plan, not per pattern — a plan with two label scans is one plan with
    /// a label scan in it.
    #[cfg(feature = "opencypher")]
    pub(crate) fn record_query_plan(&self, plan: QueryPlanShapes) {
        self.query_plans_total.fetch_add(1, Ordering::Relaxed);
        for (seen, counter) in [
            (plan.label_scan, &self.query_plans_with_label_scan),
            (plan.property_index, &self.query_plans_with_property_index),
            (plan.full_scan, &self.query_plans_with_full_scan),
            (
                plan.equality_pushdown,
                &self.query_plans_with_equality_pushdown,
            ),
        ] {
            if seen {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Count one stored-property read and the time it took.
    ///
    /// One call rather than a `fetch_add` beside a `record_micros` at each site,
    /// for the same reason [`Self::record_query_rows_failure`] is one call: the
    /// counter and the histogram's own `count()` are then the same number by
    /// construction, so a dashboard can use either to check the other and a new
    /// call site cannot record a duration without also being counted.
    #[cfg(feature = "opencypher")]
    pub(crate) fn record_property_fetch(&self, micros: u64) {
        self.query_property_fetches.fetch_add(1, Ordering::Relaxed);
        self.query_property_fetch_latency.record_micros(micros);
    }

    pub(crate) fn snapshot(&self) -> GraphOperationalMetricsSnapshot {
        let query_rows_latency = self.query_rows_latency.snapshot();
        GraphOperationalMetricsSnapshot {
            write_attempts: self.write_attempts.load(Ordering::Relaxed),
            write_commits: self.write_commits.load(Ordering::Relaxed),
            write_retries: self.write_retries.load(Ordering::Relaxed),
            bulk_import_batches_profiled: self.bulk_import_batches_profiled.load(Ordering::Relaxed),
            bulk_import_preflight_us: self.bulk_import_preflight_us.load(Ordering::Relaxed),
            bulk_import_batch_build_us: self.bulk_import_batch_build_us.load(Ordering::Relaxed),
            bulk_import_counter_read_us: self.bulk_import_counter_read_us.load(Ordering::Relaxed),
            bulk_import_commit_us: self.bulk_import_commit_us.load(Ordering::Relaxed),
            relationship_import_batches_profiled: self
                .relationship_import_batches_profiled
                .load(Ordering::Relaxed),
            relationship_import_endpoint_check_us: self
                .relationship_import_endpoint_check_us
                .load(Ordering::Relaxed),
            relationship_import_identity_scan_us: self
                .relationship_import_identity_scan_us
                .load(Ordering::Relaxed),
            relationship_import_identity_pointer_hits: self
                .relationship_import_identity_pointer_hits
                .load(Ordering::Relaxed),
            relationship_import_identity_pointer_misses: self
                .relationship_import_identity_pointer_misses
                .load(Ordering::Relaxed),
            relationship_import_record_read_us: self
                .relationship_import_record_read_us
                .load(Ordering::Relaxed),
            relationship_import_structural_check_us: self
                .relationship_import_structural_check_us
                .load(Ordering::Relaxed),
            relationship_import_segment_scans: self
                .relationship_import_segment_scans
                .load(Ordering::Relaxed),
            relationship_import_segment_neighbors: self
                .relationship_import_segment_neighbors
                .load(Ordering::Relaxed),
            relationship_import_counter_read_us: self
                .relationship_import_counter_read_us
                .load(Ordering::Relaxed),
            relationship_import_commit_us: self
                .relationship_import_commit_us
                .load(Ordering::Relaxed),
            relationship_import_idempotency_replays: self
                .relationship_import_idempotency_replays
                .load(Ordering::Relaxed),
            merge_vertex_metadata_nochange_exits: self
                .merge_vertex_metadata_nochange_exits
                .load(Ordering::Relaxed),
            delete_vertex_batch_all_replays: self
                .delete_vertex_batch_all_replays
                .load(Ordering::Relaxed),
            merge_vertex_metadata_batches_profiled: self
                .merge_vertex_metadata_batches_profiled
                .load(Ordering::Relaxed),
            merge_vertex_metadata_batch_items: self
                .merge_vertex_metadata_batch_items
                .load(Ordering::Relaxed),
            merge_vertex_metadata_read_us: self
                .merge_vertex_metadata_read_us
                .load(Ordering::Relaxed),
            merge_vertex_metadata_txn_us: self.merge_vertex_metadata_txn_us.load(Ordering::Relaxed),
            delete_vertex_batch_batches_profiled: self
                .delete_vertex_batch_batches_profiled
                .load(Ordering::Relaxed),
            delete_vertex_batch_items: self.delete_vertex_batch_items.load(Ordering::Relaxed),
            delete_vertex_batch_read_us: self.delete_vertex_batch_read_us.load(Ordering::Relaxed),
            delete_vertex_batch_txn_us: self.delete_vertex_batch_txn_us.load(Ordering::Relaxed),
            reserve_edge_delete_noops_batches_profiled: self
                .reserve_edge_delete_noops_batches_profiled
                .load(Ordering::Relaxed),
            reserve_edge_delete_noops_batch_items: self
                .reserve_edge_delete_noops_batch_items
                .load(Ordering::Relaxed),
            reserve_edge_delete_noops_read_us: self
                .reserve_edge_delete_noops_read_us
                .load(Ordering::Relaxed),
            reserve_edge_delete_noops_txn_us: self
                .reserve_edge_delete_noops_txn_us
                .load(Ordering::Relaxed),
            artifact_builds_started: self.artifact_builds_started.load(Ordering::Relaxed),
            artifact_builds_completed: self.artifact_builds_completed.load(Ordering::Relaxed),
            artifact_build_duration_us: self.artifact_build_duration_us.load(Ordering::Relaxed),
            artifact_publish_batches: self.artifact_publish_batches.load(Ordering::Relaxed),
            artifact_records_published: self.artifact_records_published.load(Ordering::Relaxed),
            artifact_publish_duration_us: self.artifact_publish_duration_us.load(Ordering::Relaxed),
            gc_jobs_started: self.gc_jobs_started.load(Ordering::Relaxed),
            gc_jobs_completed: self.gc_jobs_completed.load(Ordering::Relaxed),
            gc_keys_deleted: self.gc_keys_deleted.load(Ordering::Relaxed),
            gc_duration_us: self.gc_duration_us.load(Ordering::Relaxed),
            verifier_runs: self.verifier_runs.load(Ordering::Relaxed),
            verifier_failures: self.verifier_failures.load(Ordering::Relaxed),
            verifier_duration_us: self.verifier_duration_us.load(Ordering::Relaxed),
            query_rows_started: self.query_rows_started.load(Ordering::Relaxed),
            query_rows_completed: self.query_rows_completed.load(Ordering::Relaxed),
            query_rows_failed: self.query_rows_failed.load(Ordering::Relaxed),
            query_rows_failed_by_class: load_class_counters(&self.query_rows_failed_by_class),
            query_rows_returned: self.query_rows_returned.load(Ordering::Relaxed),
            query_rows_duration_us: query_rows_latency.sum_us,
            query_rows_latency,
            query_experimental_property_seek_requests: self
                .query_experimental_property_seek_requests
                .load(Ordering::Relaxed),
            query_experimental_relationship_expand_requests: self
                .query_experimental_relationship_expand_requests
                .load(Ordering::Relaxed),
            query_experimental_ordered_property_scan_requests: self
                .query_experimental_ordered_property_scan_requests
                .load(Ordering::Relaxed),
            query_experimental_requests: self.query_experimental_requests.load(Ordering::Relaxed),
            query_experimental_parse_us: self.query_experimental_parse_us.load(Ordering::Relaxed),
            query_experimental_lower_us: self.query_experimental_lower_us.load(Ordering::Relaxed),
            query_experimental_bind_us: self.query_experimental_bind_us.load(Ordering::Relaxed),
            query_experimental_snapshot_us: self
                .query_experimental_snapshot_us
                .load(Ordering::Relaxed),
            query_experimental_statistics_us: self
                .query_experimental_statistics_us
                .load(Ordering::Relaxed),
            query_experimental_plan_us: self.query_experimental_plan_us.load(Ordering::Relaxed),
            query_experimental_execute_us: self
                .query_experimental_execute_us
                .load(Ordering::Relaxed),
            query_experimental_storage_us: self
                .query_experimental_storage_us
                .load(Ordering::Relaxed),
            query_experimental_storage_calls: self
                .query_experimental_storage_calls
                .load(Ordering::Relaxed),
            query_experimental_result_us: self.query_experimental_result_us.load(Ordering::Relaxed),
            query_experimental_sampled_plans: self
                .query_experimental_sampled_plans
                .load(Ordering::Relaxed),
            query_route_legacy_requests: self.query_route_legacy_requests.load(Ordering::Relaxed),
            query_route_experimental_requests: self
                .query_route_experimental_requests
                .load(Ordering::Relaxed),
            query_route_native_path_fallbacks: self
                .query_route_native_path_fallbacks
                .load(Ordering::Relaxed),
            query_route_mutation_fallbacks: self
                .query_route_mutation_fallbacks
                .load(Ordering::Relaxed),
            query_property_fetches: self.query_property_fetches.load(Ordering::Relaxed),
            query_property_fetch_latency: self.query_property_fetch_latency.snapshot(),
            query_artifact_lookup_us: self.query_artifact_lookup_us.load(Ordering::Relaxed),
            query_graphblas_cache_us: self.query_graphblas_cache_us.load(Ordering::Relaxed),
            query_graphblas_artifact_snapshots: self
                .query_graphblas_artifact_snapshots
                .load(Ordering::Relaxed),
            query_graphblas_rebuilt_snapshots: self
                .query_graphblas_rebuilt_snapshots
                .load(Ordering::Relaxed),
            query_rust_sparse_fallbacks: self.query_rust_sparse_fallbacks.load(Ordering::Relaxed),
            query_plans_total: self.query_plans_total.load(Ordering::Relaxed),
            query_plans_with_label_scan: self.query_plans_with_label_scan.load(Ordering::Relaxed),
            query_plans_with_property_index: self
                .query_plans_with_property_index
                .load(Ordering::Relaxed),
            query_plans_with_full_scan: self.query_plans_with_full_scan.load(Ordering::Relaxed),
            query_plans_with_equality_pushdown: self
                .query_plans_with_equality_pushdown
                .load(Ordering::Relaxed),
            graph_compute_tasks: self.graph_compute_tasks.load(Ordering::Relaxed),
            graph_compute_queue_us: self.graph_compute_queue_us.load(Ordering::Relaxed),
            graph_compute_duration_us: self.graph_compute_duration_us.load(Ordering::Relaxed),
            backpressure_waits: self.backpressure_waits.load(Ordering::Relaxed),
            create_relationships_batch_latency: self.create_relationships_batch_latency.snapshot(),
            delete_relationship_mutations_batch_latency: self
                .delete_relationship_mutations_batch_latency
                .snapshot(),
            delete_vertices_and_isolated_candidates_batch_latency: self
                .delete_vertices_and_isolated_candidates_batch_latency
                .snapshot(),
            detach_delete_vertices_batch_latency: self
                .detach_delete_vertices_batch_latency
                .snapshot(),
            merge_relationships_batch_latency: self.merge_relationships_batch_latency.snapshot(),
            merge_vertex_metadata_batch_latency: self
                .merge_vertex_metadata_batch_latency
                .snapshot(),
            reserve_edge_delete_noops_batch_latency: self
                .reserve_edge_delete_noops_batch_latency
                .snapshot(),
            experimental_operators: self
                .experimental_operators
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
        }
    }
}

#[derive(Default)]
pub(crate) struct GraphCacheMetrics {
    pub(crate) matrix_artifact_hits: AtomicU64,
    pub(crate) matrix_artifact_misses: AtomicU64,
    pub(crate) matrix_adjacency_hits: AtomicU64,
    pub(crate) matrix_adjacency_misses: AtomicU64,
    pub(crate) graphblas_hits: AtomicU64,
    pub(crate) graphblas_misses: AtomicU64,
    pub(crate) parsed_row_query_hits: AtomicU64,
    pub(crate) parsed_row_query_misses: AtomicU64,
    pub(crate) relationship_rows_hits: AtomicU64,
    pub(crate) relationship_rows_misses: AtomicU64,
    pub(crate) relationship_property_rows_hits: AtomicU64,
    pub(crate) relationship_property_rows_misses: AtomicU64,
    pub(crate) insertions: AtomicU64,
    pub(crate) evictions: AtomicU64,
    pub(crate) pinned_insertions: AtomicU64,
    pub(crate) tenant_quota_rejections: AtomicU64,
    pub(crate) hydration_started: AtomicU64,
    pub(crate) hydration_waited: AtomicU64,
    pub(crate) hydration_completed: AtomicU64,
}

impl GraphCacheMetrics {
    pub(crate) fn record_hit(&self, kind: GraphCacheKind) {
        self.counter(kind, true).fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self, kind: GraphCacheKind) {
        self.counter(kind, false).fetch_add(1, Ordering::Relaxed);
    }

    fn counter(&self, kind: GraphCacheKind, hit: bool) -> &AtomicU64 {
        match (kind, hit) {
            (GraphCacheKind::MatrixArtifact, true) => &self.matrix_artifact_hits,
            (GraphCacheKind::MatrixArtifact, false) => &self.matrix_artifact_misses,
            (GraphCacheKind::MatrixAdjacency, true) => &self.matrix_adjacency_hits,
            (GraphCacheKind::MatrixAdjacency, false) => &self.matrix_adjacency_misses,
            (GraphCacheKind::GraphBlas, true) => &self.graphblas_hits,
            (GraphCacheKind::GraphBlas, false) => &self.graphblas_misses,
            (GraphCacheKind::ParsedRowQuery, true) => &self.parsed_row_query_hits,
            (GraphCacheKind::ParsedRowQuery, false) => &self.parsed_row_query_misses,
            (GraphCacheKind::RelationshipRows, true) => &self.relationship_rows_hits,
            (GraphCacheKind::RelationshipRows, false) => &self.relationship_rows_misses,
            (GraphCacheKind::RelationshipPropertyRows, true) => {
                &self.relationship_property_rows_hits
            }
            (GraphCacheKind::RelationshipPropertyRows, false) => {
                &self.relationship_property_rows_misses
            }
        }
    }

    pub(crate) fn snapshot(&self) -> GraphCacheMetricsSnapshot {
        GraphCacheMetricsSnapshot {
            matrix_artifact_hits: self.matrix_artifact_hits.load(Ordering::Relaxed),
            matrix_artifact_misses: self.matrix_artifact_misses.load(Ordering::Relaxed),
            matrix_adjacency_hits: self.matrix_adjacency_hits.load(Ordering::Relaxed),
            matrix_adjacency_misses: self.matrix_adjacency_misses.load(Ordering::Relaxed),
            graphblas_hits: self.graphblas_hits.load(Ordering::Relaxed),
            graphblas_misses: self.graphblas_misses.load(Ordering::Relaxed),
            parsed_row_query_hits: self.parsed_row_query_hits.load(Ordering::Relaxed),
            parsed_row_query_misses: self.parsed_row_query_misses.load(Ordering::Relaxed),
            relationship_rows_hits: self.relationship_rows_hits.load(Ordering::Relaxed),
            relationship_rows_misses: self.relationship_rows_misses.load(Ordering::Relaxed),
            relationship_property_rows_hits: self
                .relationship_property_rows_hits
                .load(Ordering::Relaxed),
            relationship_property_rows_misses: self
                .relationship_property_rows_misses
                .load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            pinned_insertions: self.pinned_insertions.load(Ordering::Relaxed),
            tenant_quota_rejections: self.tenant_quota_rejections.load(Ordering::Relaxed),
            hydration_started: self.hydration_started.load(Ordering::Relaxed),
            hydration_waited: self.hydration_waited.load(Ordering::Relaxed),
            hydration_completed: self.hydration_completed.load(Ordering::Relaxed),
        }
    }
}
