//! Request decomposition and slow-plan sampling for the experimental route
//! (Workstream 11).
//!
//! Every experimental read gets one `query.experimental` span. It splits the
//! request into parse, lower, snapshot, statistics wait, physical planning,
//! storage I/O, operator CPU and result conversion, and it names the plan by a
//! hash of its redacted shape. The same split feeds per-cell counters, so a
//! dashboard reads the mean and a trace reads one request. Nothing needs to be
//! added during an incident.
//!
//! What goes where:
//!
//! - **Metric labels:** `cell_id` only, through the existing per-cell counters.
//!   Route and fallback are separate counters, not labels.
//! - **Span attributes on every request:** stage times, plan hash, capability,
//!   route, storage calls and the widest intermediate result.
//! - **Only on a request that trips a sampling trigger:** the plan shape
//!   (`hydradb.query.plan`, literals already elided) and a WARN event. Such a
//!   request is also marked `hydradb.sampling.tail_keep` so the collector
//!   keeps its trace.
//!
//! Query text, parameter values and property values never appear.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hydradb_cypher_engine::{
    plan_shape_hash, ExecutionProfile, GraphPhysicalPlan, LowerTimings, OperatorProfile,
    OperatorProfiler, PreparedQuery, StatisticsProvider,
};

use super::super::query_optimizer::{slow_query_threshold_ms, wide_plan_threshold_rows};
use super::GraphShard;
use crate::core::state::ReadAccounting;
use crate::{ExperimentalOperatorMetricsSnapshot, GraphError};

/// An operator estimate is a miss when it is off from the rows produced by at
/// least this ratio...
const ESTIMATE_MISS_RATIO: f64 = 10.0;
/// ...and either side reached this many rows. Below it a large ratio is a
/// rounding error on a cheap operator, not a bad plan.
const ESTIMATE_MISS_MIN_ROWS: u64 = 1_000;

tokio::task_local! {
    /// The work counters of the experimental request being executed on this
    /// task. A task-local rather than a field on the storage adapter so the
    /// adapter's many operator bodies need no extra argument.
    static REQUEST_WORK: Arc<RequestWork>;
}

/// Storage work the adapter reports while a plan executes.
#[derive(Default)]
pub(super) struct RequestWork {
    storage_us: AtomicU64,
    storage_calls: AtomicU64,
    widest_rows: AtomicU64,
    /// SlateDB reads issued under this request, for per-operator attribution.
    reads: Arc<ReadAccounting>,
    /// Installed once the plan exists. A mutex, not a `RefCell`: the adapter
    /// reports from `&self` methods and the future must stay `Send`. The
    /// executor is sequential, so it is never contended.
    profiler: Mutex<Option<OperatorProfiler>>,
}

impl RequestWork {
    /// Run `future` with this request's counters installed for the adapter.
    pub(super) async fn scope<F: std::future::Future>(self: &Arc<Self>, future: F) -> F::Output {
        REQUEST_WORK
            .scope(Arc::clone(self), self.reads.scope(future))
            .await
    }

    /// Start attributing work to the operators of `plan`, which must be the
    /// plan value the executor will run.
    pub(super) fn profile(&self, plan: &GraphPhysicalPlan) {
        *self.lock_profiler() = Some(OperatorProfiler::new(plan));
    }

    fn lock_profiler(&self) -> std::sync::MutexGuard<'_, Option<OperatorProfiler>> {
        self.profiler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_profile(&self) -> Option<ExecutionProfile> {
        self.lock_profiler().take().map(OperatorProfiler::finish)
    }
}

fn with_profiler(charge: impl FnOnce(&mut OperatorProfiler)) {
    let _ = REQUEST_WORK.try_with(|work| {
        if let Some(profiler) = work.lock_profiler().as_mut() {
            charge(profiler);
        }
    });
}

/// SlateDB requests and point-read bytes issued so far by this request, to
/// take a delta around one adapter call.
pub(super) fn storage_totals() -> (u64, u64) {
    REQUEST_WORK
        .try_with(|work| work.reads.totals())
        .unwrap_or_default()
}

/// Count one storage-adapter call and charge it to the running operator.
/// `before` is [`storage_totals`] taken as the call started. A no-op outside a
/// scoped request, such as adapter unit tests that call it directly.
pub(super) fn record_storage_call(
    call: &'static str,
    elapsed_us: u64,
    rows_out: usize,
    before: (u64, u64),
) {
    let _ = REQUEST_WORK.try_with(|work| {
        work.storage_us.fetch_add(elapsed_us, Ordering::Relaxed);
        work.storage_calls.fetch_add(1, Ordering::Relaxed);
        work.widest_rows
            .fetch_max(rows_out as u64, Ordering::Relaxed);
        let (requests, bytes) = work.reads.totals();
        if let Some(profiler) = work.lock_profiler().as_mut() {
            profiler.record_storage_call(
                Duration::from_micros(elapsed_us),
                requests.saturating_sub(before.0),
                bytes.saturating_sub(before.1),
            );
            // Classified by adapter call rather than reported from each body,
            // so the operator bodies stay free of accounting.
            match call {
                "hydrate_vertices" | "scan_vertices_by_property_ordered" => {
                    profiler.record_hydrated_vertices(rows_out);
                }
                "expand_relationships" | "seek_relationships_by_property" => {
                    profiler.record_scanned_relationships(rows_out);
                }
                _ => {}
            }
        }
    });
}

/// Note an intermediate row collection the executor asked to admit.
pub(super) fn record_intermediate_rows(rows: usize) {
    let _ = REQUEST_WORK.try_with(|work| {
        work.widest_rows.fetch_max(rows as u64, Ordering::Relaxed);
        if let Some(profiler) = work.lock_profiler().as_mut() {
            profiler.record_retained_rows(rows);
        }
    });
}

pub(super) fn operator_started(plan: &GraphPhysicalPlan) {
    with_profiler(|profiler| profiler.operator_started(plan));
}

pub(super) fn operator_finished(plan: &GraphPhysicalPlan, rows_out: usize) {
    with_profiler(|profiler| profiler.operator_finished(plan, rows_out));
}

pub(super) fn record_retained_rows(rows: usize) {
    with_profiler(|profiler| profiler.record_retained_rows(rows));
}

pub(super) fn record_hydrated_vertices(vertices: usize) {
    with_profiler(|profiler| profiler.record_hydrated_vertices(vertices));
}

/// The span every experimental read runs under. Declared here so the field
/// list and the code that records into it cannot drift apart: `tracing` drops
/// a `record` for an undeclared field without a word.
pub(super) fn request_span() -> tracing::Span {
    tracing::info_span!(
        "query.experimental",
        hydradb.query.route = "experimental",
        hydradb.query.capability = tracing::field::Empty,
        hydradb.query.plan_hash = tracing::field::Empty,
        hydradb.query.plan = tracing::field::Empty,
        hydradb.query.full_scan = tracing::field::Empty,
        hydradb.query.stage.parse_us = tracing::field::Empty,
        hydradb.query.stage.lower_us = tracing::field::Empty,
        hydradb.query.stage.bind_us = tracing::field::Empty,
        hydradb.query.stage.lower_cache_hit = tracing::field::Empty,
        hydradb.query.stage.snapshot_us = tracing::field::Empty,
        hydradb.query.stage.statistics_us = tracing::field::Empty,
        hydradb.query.stage.plan_us = tracing::field::Empty,
        hydradb.query.stage.execute_us = tracing::field::Empty,
        hydradb.query.stage.storage_us = tracing::field::Empty,
        hydradb.query.stage.operator_cpu_us = tracing::field::Empty,
        hydradb.query.stage.result_us = tracing::field::Empty,
        hydradb.query.storage.calls = tracing::field::Empty,
        hydradb.query.rows_widest = tracing::field::Empty,
        hydradb.query.rows_materialized = tracing::field::Empty,
        hydradb.query.rows_returned = tracing::field::Empty,
        hydradb.sampling.tail_keep = tracing::field::Empty,
        error.class = tracing::field::Empty,
    )
}

/// Wall time of each stage of one experimental request, and what the plan was.
#[derive(Default)]
pub(super) struct RequestObservation {
    pub(super) lowering: Duration,
    pub(super) lower_timings: LowerTimings,
    pub(super) lower_cache_hit: bool,
    pub(super) snapshot: Duration,
    pub(super) statistics: Duration,
    pub(super) plan: Duration,
    pub(super) execute: Duration,
    pub(super) result: Duration,
    pub(super) rows_materialized: usize,
    pub(super) rows_returned: usize,
    /// Set once physical planning succeeds. Its absence after an error means
    /// the engine could not plan the query.
    pub(super) plan_identity: Option<PlanIdentity>,
    pub(super) work: Arc<RequestWork>,
    /// Per-operator work, with estimates attached, once execution returned.
    pub(super) profile: Option<ExecutionProfile>,
}

pub(super) struct PlanIdentity {
    hash: u64,
    full_scan: bool,
    /// Rendered once, because the hash needs it; attached to the span only
    /// when the request is sampled.
    shape: String,
}

impl PlanIdentity {
    pub(super) fn of(prepared: &PreparedQuery) -> Self {
        let shape = prepared.plan_shape();
        Self {
            hash: plan_shape_hash(&shape),
            full_scan: prepared.physical.contains_full_scan(),
            shape,
        }
    }
}

/// Why a request's trace is worth keeping. The plan verdicts outrank the
/// clock, as on the legacy route: a slow *and* wide plan is reported as wide,
/// because that is the half an operator can act on.
fn sampling_reason(
    full_scan: bool,
    widest_rows: u64,
    estimate_missed: bool,
    elapsed: Duration,
) -> Option<&'static str> {
    if full_scan {
        Some(reason::FULL_SCAN)
    } else if widest_rows >= wide_plan_threshold_rows() {
        Some(reason::WIDE)
    } else if estimate_missed {
        Some(reason::ESTIMATE_ERROR)
    } else if elapsed.as_millis() as u64 >= slow_query_threshold_ms() {
        Some(reason::SLOW)
    } else {
        None
    }
}

/// Tail-keep reasons. `full_scan` and `wide_plan` are the legacy route's
/// values, so one collector policy covers both engines.
mod reason {
    pub(super) const FULL_SCAN: &str = "full_scan";
    pub(super) const WIDE: &str = "wide_plan";
    pub(super) const ESTIMATE_ERROR: &str = "estimate_error";
    pub(super) const SLOW: &str = "slow";
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl RequestObservation {
    /// Split the preparation wall time into parser, AST lowering and
    /// everything else: parameter conversion, the template-cache lookup and
    /// binding. Parse and lower are what the template build measured, so both
    /// are zero on a cache hit; bind is the remainder and is paid every time.
    pub(super) fn preparation_split(&self) -> (Duration, Duration, Duration) {
        let LowerTimings { parse, lower } = self.lower_timings;
        let bind = self.lowering.saturating_sub(parse).saturating_sub(lower);
        (parse, lower, bind)
    }

    /// Collect the operator profile after execution, successful or not, and
    /// attach the estimates `statistics` gives for `plan`.
    pub(super) fn collect_profile(
        &mut self,
        plan: &GraphPhysicalPlan,
        statistics: &dyn StatisticsProvider,
    ) {
        self.profile = self.work.take_profile().map(|mut profile| {
            profile.attach_estimates(plan, statistics);
            profile
        });
    }

    /// Record the finished request on `span`, into `shard`'s counters, and,
    /// when a trigger fires, as a sampled plan. `elapsed` is the whole
    /// request as the shard saw it.
    pub(super) fn finish<T>(
        &self,
        span: &tracing::Span,
        shard: &GraphShard,
        elapsed: Duration,
        result: &Result<T, GraphError>,
    ) {
        let (parse, lower, bind) = self.preparation_split();
        let storage = Duration::from_micros(self.work.storage_us.load(Ordering::Relaxed));
        let operator_cpu = self.execute.saturating_sub(storage);
        let storage_calls = self.work.storage_calls.load(Ordering::Relaxed);
        // The storage calls and intermediate checks see each input, but an
        // operator that only combines its inputs, such as UNION, reports to
        // neither; the materialized result bounds what it produced.
        let widest_rows = self
            .work
            .widest_rows
            .load(Ordering::Relaxed)
            .max(self.rows_materialized as u64);

        span.record("hydradb.query.stage.parse_us", micros(parse));
        span.record("hydradb.query.stage.lower_us", micros(lower));
        span.record("hydradb.query.stage.bind_us", micros(bind));
        span.record("hydradb.query.stage.lower_cache_hit", self.lower_cache_hit);
        span.record("hydradb.query.stage.snapshot_us", micros(self.snapshot));
        span.record("hydradb.query.stage.statistics_us", micros(self.statistics));
        span.record("hydradb.query.stage.plan_us", micros(self.plan));
        span.record("hydradb.query.stage.execute_us", micros(self.execute));
        span.record("hydradb.query.stage.storage_us", micros(storage));
        span.record("hydradb.query.stage.operator_cpu_us", micros(operator_cpu));
        span.record("hydradb.query.stage.result_us", micros(self.result));
        span.record("hydradb.query.storage.calls", storage_calls);
        span.record("hydradb.query.rows_widest", widest_rows);
        span.record(
            "hydradb.query.rows_materialized",
            self.rows_materialized as u64,
        );
        span.record("hydradb.query.rows_returned", self.rows_returned as u64);
        span.record(
            "hydradb.query.capability",
            if self.plan_identity.is_some() {
                "supported"
            } else {
                "unsupported"
            },
        );
        if let Err(error) = result {
            span.record("error.class", error.class());
        }

        let metrics = &shard.operation_metrics;
        for (counter, value) in [
            (&metrics.query_experimental_requests, 1),
            (&metrics.query_experimental_parse_us, micros(parse)),
            (&metrics.query_experimental_lower_us, micros(lower)),
            (&metrics.query_experimental_bind_us, micros(bind)),
            (
                &metrics.query_experimental_snapshot_us,
                micros(self.snapshot),
            ),
            (
                &metrics.query_experimental_statistics_us,
                micros(self.statistics),
            ),
            (&metrics.query_experimental_plan_us, micros(self.plan)),
            (&metrics.query_experimental_execute_us, micros(self.execute)),
            (&metrics.query_experimental_storage_us, micros(storage)),
            (&metrics.query_experimental_storage_calls, storage_calls),
            (&metrics.query_experimental_result_us, micros(self.result)),
        ] {
            counter.fetch_add(value, Ordering::Relaxed);
        }

        let worst_estimate = self
            .profile
            .as_ref()
            .and_then(|profile| profile.worst_estimate(ESTIMATE_MISS_MIN_ROWS))
            .filter(|(_, ratio)| *ratio >= ESTIMATE_MISS_RATIO);
        if let Some(profile) = &self.profile {
            fold_operator_totals(shard, profile);
        }

        let Some(plan) = &self.plan_identity else {
            return;
        };
        let plan_hash = format!("{:016x}", plan.hash);
        span.record("hydradb.query.plan_hash", plan_hash.as_str());
        span.record("hydradb.query.full_scan", plan.full_scan);
        let Some(reason) = sampling_reason(
            plan.full_scan,
            widest_rows,
            worst_estimate.is_some(),
            elapsed,
        ) else {
            return;
        };
        metrics
            .query_experimental_sampled_plans
            .fetch_add(1, Ordering::Relaxed);
        let shape = plan.shape.as_str();
        span.record("hydradb.sampling.tail_keep", reason);
        span.record("hydradb.query.plan", shape);
        if let Some(profile) = &self.profile {
            emit_operator_spans(span, profile);
        }
        let hottest = self.profile.as_ref().and_then(ExecutionProfile::hottest);
        tracing::warn!(
            parent: span,
            reason,
            hydradb.query.hottest_operator = hottest.map_or("", |operator| operator.operator),
            hydradb.query.hottest_operator_self_us =
                hottest.map_or(0, |operator| micros(operator.self_elapsed)),
            hydradb.query.worst_estimate_operator =
                worst_estimate.map_or("", |(operator, _)| operator.operator),
            hydradb.query.worst_estimate_ratio = worst_estimate.map_or(0.0, |(_, ratio)| ratio),
            hydradb.query.plan_hash = %plan_hash,
            hydradb.query.full_scan = plan.full_scan,
            hydradb.query.rows_widest = widest_rows,
            hydradb.query.storage.calls = storage_calls,
            elapsed_us = micros(elapsed),
            hydradb.query.stage.parse_us = micros(parse),
            hydradb.query.stage.lower_us = micros(lower),
            hydradb.query.stage.bind_us = micros(bind),
            hydradb.query.stage.snapshot_us = micros(self.snapshot),
            hydradb.query.stage.statistics_us = micros(self.statistics),
            hydradb.query.stage.plan_us = micros(self.plan),
            hydradb.query.stage.storage_us = micros(storage),
            hydradb.query.stage.operator_cpu_us = micros(operator_cpu),
            hydradb.query.stage.result_us = micros(self.result),
            hydradb.query.plan = %shape,
            "experimental query plan warrants attention"
        );
    }
}

/// Fold one request's operator profile into the shard's per-operator totals:
/// one lock per request.
fn fold_operator_totals(shard: &GraphShard, profile: &ExecutionProfile) {
    let request = request_operator_totals(profile);
    let mut totals = shard
        .operation_metrics
        .experimental_operators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for operator in &request {
        totals
            .entry(operator.operator)
            .or_insert_with(|| ExperimentalOperatorMetricsSnapshot {
                operator: operator.operator,
                ..ExperimentalOperatorMetricsSnapshot::default()
            })
            .accumulate(operator);
    }
}

/// One request's contribution to the per-operator totals, one entry per
/// operator node that ran.
///
/// Operators of one kind can appear more than once in a plan, for example a
/// `VertexLabelScan` in each arm of a UNION. `requests` and `estimate_misses`
/// both count requests, so each kind adds at most one to either however many
/// of its nodes ran or missed; the work counters add up across nodes.
fn request_operator_totals(profile: &ExecutionProfile) -> Vec<ExperimentalOperatorMetricsSnapshot> {
    let mut seen = std::collections::BTreeSet::new();
    let mut missed_kinds = std::collections::BTreeSet::new();
    profile
        .operators
        .iter()
        .filter(|operator| operator.invocations > 0)
        .map(|operator| {
            let missed = operator.estimated_rows.is_some_and(|estimated| {
                estimated.max(operator.rows_out) >= ESTIMATE_MISS_MIN_ROWS
            }) && operator
                .estimate_error()
                .is_some_and(|ratio| ratio >= ESTIMATE_MISS_RATIO);
            ExperimentalOperatorMetricsSnapshot {
                operator: operator.operator,
                requests: u64::from(seen.insert(operator.operator)),
                invocations: operator.invocations,
                rows_in: operator.rows_in,
                rows_out: operator.rows_out,
                self_us: micros(operator.self_elapsed),
                storage_us: micros(operator.storage_elapsed),
                storage_requests: operator.storage_requests,
                storage_bytes: operator.storage_bytes,
                hydrated_vertices: operator.hydrated_vertices,
                scanned_relationships: operator.scanned_relationships,
                peak_retained_rows: operator.peak_retained_rows,
                estimate_misses: u64::from(missed && missed_kinds.insert(operator.operator)),
            }
        })
        .collect()
}

/// One `query.experimental.operator_profile` span per operator, children of
/// the request span, for a sampled request only. Opened after the fact, so a
/// span's own duration is meaningless: read `elapsed_us`, `self_us` and
/// `cpu_us`.
fn emit_operator_spans(parent: &tracing::Span, profile: &ExecutionProfile) {
    for operator in &profile.operators {
        operator_profile_span(parent, operator).in_scope(|| {});
    }
}

fn operator_profile_span(parent: &tracing::Span, operator: &OperatorProfile) -> tracing::Span {
    let span = tracing::info_span!(
        parent: parent,
        "query.experimental.operator_profile",
        hydradb.query.operator = operator.operator,
        hydradb.query.operator.id = operator.id as u64,
        hydradb.query.operator.parent_id = tracing::field::Empty,
        hydradb.query.operator.invocations = operator.invocations,
        hydradb.query.operator.rows_in = operator.rows_in,
        hydradb.query.operator.rows_out = operator.rows_out,
        hydradb.query.operator.rows_estimated = tracing::field::Empty,
        hydradb.query.operator.estimate_ratio = tracing::field::Empty,
        hydradb.query.operator.elapsed_us = micros(operator.elapsed),
        hydradb.query.operator.self_us = micros(operator.self_elapsed),
        hydradb.query.operator.cpu_us = micros(operator.cpu_elapsed()),
        hydradb.query.operator.storage_us = micros(operator.storage_elapsed),
        hydradb.query.operator.storage_requests = operator.storage_requests,
        hydradb.query.operator.storage_bytes = operator.storage_bytes,
        hydradb.query.operator.hydrated_vertices = operator.hydrated_vertices,
        hydradb.query.operator.scanned_relationships = operator.scanned_relationships,
        hydradb.query.operator.peak_retained_rows = operator.peak_retained_rows,
    );
    if let Some(parent_id) = operator.parent {
        span.record("hydradb.query.operator.parent_id", parent_id as u64);
    }
    if let Some(estimated) = operator.estimated_rows {
        span.record("hydradb.query.operator.rows_estimated", estimated);
    }
    if let Some(ratio) = operator.estimate_error() {
        span.record("hydradb.query.operator.estimate_ratio", ratio);
    }
    span
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use slatedb::object_store::memory::InMemory;

    use super::super::super::*;

    type Captured = Arc<Mutex<Vec<(String, String)>>>;

    /// Remembers every span field recorded after creation. The same shape as
    /// the capture in `query_optimizer`'s tests, for the same reason:
    /// `tracing-subscriber` is not a dependency of this library.
    struct RecordedFields(Captured);

    impl tracing::field::Visit for RecordedFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .push((field.name().to_string(), format!("{value:?}")));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0
                .lock()
                .unwrap()
                .push((field.name().to_string(), value.to_string()));
        }
    }

    impl tracing::Subscriber for RecordedFields {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            // Operator profiles are complete at creation; every other span
            // under test is filled in afterwards and captured by `record`.
            if span.metadata().name() == "query.experimental.operator_profile" {
                span.record(&mut RecordedFields(Arc::clone(&self.0)));
            }
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            values.record(&mut RecordedFields(Arc::clone(&self.0)));
        }

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    fn values_of<'a>(captured: &'a [(String, String)], field: &str) -> Vec<&'a str> {
        captured
            .iter()
            .filter(|(name, _)| name == field)
            .map(|(_, value)| value.as_str())
            .collect()
    }

    #[test]
    fn two_missed_nodes_of_one_kind_are_one_missed_request() {
        let scan = |id, estimated_rows| hydradb_cypher_engine::OperatorProfile {
            id,
            parent: Some(0),
            operator: "VertexLabelScan",
            invocations: 1,
            rows_out: 6,
            estimated_rows: Some(estimated_rows),
            ..Default::default()
        };
        let profile = hydradb_cypher_engine::ExecutionProfile {
            operators: vec![
                hydradb_cypher_engine::OperatorProfile {
                    id: 0,
                    operator: "UnionExec",
                    invocations: 1,
                    rows_in: 12,
                    rows_out: 12,
                    ..Default::default()
                },
                scan(1, 50_000),
                scan(2, 50_000),
            ],
        };
        let totals = super::request_operator_totals(&profile);
        let scans = totals
            .iter()
            .filter(|operator| operator.operator == "VertexLabelScan")
            .collect::<Vec<_>>();
        assert_eq!(scans.len(), 2, "one entry per node that ran");
        assert_eq!(scans.iter().map(|scan| scan.requests).sum::<u64>(), 1);
        assert_eq!(
            scans.iter().map(|scan| scan.estimate_misses).sum::<u64>(),
            1
        );
        assert_eq!(scans.iter().map(|scan| scan.rows_out).sum::<u64>(), 12);
    }

    #[test]
    fn preparation_splits_into_parse_lower_and_bind() {
        let built = super::RequestObservation {
            lowering: Duration::from_micros(100),
            lower_timings: super::LowerTimings {
                parse: Duration::from_micros(60),
                lower: Duration::from_micros(25),
            },
            ..Default::default()
        };
        assert_eq!(
            built.preparation_split(),
            (
                Duration::from_micros(60),
                Duration::from_micros(25),
                Duration::from_micros(15)
            )
        );
        // A cache hit neither parses nor lowers: all of it is binding.
        let hit = super::RequestObservation {
            lowering: Duration::from_micros(9),
            lower_cache_hit: true,
            ..Default::default()
        };
        assert_eq!(
            hit.preparation_split(),
            (Duration::ZERO, Duration::ZERO, Duration::from_micros(9))
        );
    }

    fn experimental(key: &str) -> QueryContext {
        QueryContext::new("cell-a", key).with_cypher_engine(CypherEngineMode::Experimental)
    }

    #[tokio::test]
    async fn a_request_decomposes_into_stages_and_a_full_scan_is_sampled() {
        let captured: Captured = Arc::default();
        let _subscriber = tracing::subscriber::set_default(RecordedFields(Arc::clone(&captured)));
        let shard = GraphShard::open_standalone_writer(
            "graph/experimental-observability",
            Arc::new(InMemory::new()),
        )
        .await
        .expect("open graph shard");
        for (id, entity_id) in [(1, "alpha"), (2, "other")] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "entity_id",
                            VertexPropertyValue::String(entity_id.to_string()),
                        ),
                )
                .await
                .unwrap();
        }

        // A full scan is sampled and carries its plan shape, with the window's
        // literals elided (the crate's tests pin that for string literals).
        shard
            .execute_cypher_rows(
                experimental("full-scan"),
                "MATCH (n) RETURN n.entity_id AS id ORDER BY id SKIP 1 LIMIT 5",
            )
            .await
            .expect("full scan executes");
        let metrics = shard.graph_operational_metrics();
        assert_eq!(metrics.query_route_experimental_requests, 1);
        assert_eq!(metrics.query_experimental_requests, 1);
        assert!(
            metrics.query_experimental_storage_calls >= 2,
            "scan and hydrate"
        );
        assert!(metrics.query_experimental_execute_us >= metrics.query_experimental_storage_us);
        assert_eq!(metrics.query_experimental_sampled_plans, 1);
        {
            let recorded = captured.lock().unwrap();
            assert_eq!(
                values_of(&recorded, "hydradb.sampling.tail_keep"),
                vec!["full_scan"]
            );
            assert_eq!(
                values_of(&recorded, "hydradb.query.capability"),
                vec!["supported"]
            );
            let hashes = values_of(&recorded, "hydradb.query.plan_hash");
            assert_eq!(hashes.len(), 1);
            assert_eq!(hashes[0].len(), 16);
            let plans = values_of(&recorded, "hydradb.query.plan");
            assert_eq!(plans.len(), 1);
            assert!(plans[0].contains("AllVertexScan"), "{}", plans[0]);
            assert!(plans[0].contains("count=?"), "{}", plans[0]);
            for field in [
                "hydradb.query.stage.parse_us",
                "hydradb.query.stage.lower_us",
                "hydradb.query.stage.bind_us",
                "hydradb.query.stage.snapshot_us",
                "hydradb.query.stage.statistics_us",
                "hydradb.query.stage.plan_us",
                "hydradb.query.stage.storage_us",
                "hydradb.query.stage.operator_cpu_us",
                "hydradb.query.stage.result_us",
            ] {
                assert_eq!(values_of(&recorded, field).len(), 1, "{field} not recorded");
            }
        }
        captured.lock().unwrap().clear();

        // An indexed seek is neither sampled nor given a plan attribute.
        shard
            .execute_cypher_rows(
                experimental("seek"),
                "MATCH (n:Entity {entity_id: 'other'}) RETURN n.entity_id AS id",
            )
            .await
            .expect("seek executes");
        assert_eq!(
            shard
                .graph_operational_metrics()
                .query_experimental_sampled_plans,
            1
        );
        {
            let recorded = captured.lock().unwrap();
            assert!(values_of(&recorded, "hydradb.sampling.tail_keep").is_empty());
            assert!(values_of(&recorded, "hydradb.query.plan").is_empty());
            assert_eq!(values_of(&recorded, "hydradb.query.plan_hash").len(), 1);
        }
        captured.lock().unwrap().clear();

        // A query the engine cannot plan reports itself as unsupported.
        let unsupported = shard
            .execute_cypher_rows(experimental("unsupported"), "MATCH (n) RETURN *")
            .await;
        assert!(unsupported.is_err());
        assert_eq!(
            values_of(&captured.lock().unwrap(), "hydradb.query.capability"),
            vec!["unsupported"]
        );

        // The legacy route and a mutation fallback are counted by route.
        shard
            .execute_cypher_rows(
                QueryContext::new("cell-a", "legacy"),
                "MATCH (n:Entity {entity_id: 'other'}) RETURN n.entity_id AS id",
            )
            .await
            .expect("legacy executes");
        shard
            .execute_cypher_rows(
                experimental("mutation"),
                "CREATE (a {id: 7})-[:KNOWS]->(b {id: 8})",
            )
            .await
            .expect("mutation executes");
        let metrics = shard.graph_operational_metrics();
        assert_eq!(metrics.query_route_legacy_requests, 1);
        assert_eq!(metrics.query_route_mutation_fallbacks, 1);
        assert_eq!(metrics.query_route_experimental_requests, 3);
        assert_eq!(metrics.query_experimental_requests, 3);

        shard.close().await.expect("close graph shard");
    }

    #[tokio::test]
    async fn operators_are_counted_always_and_profiled_on_sampled_traces_only() {
        let captured: Captured = Arc::default();
        let _subscriber = tracing::subscriber::set_default(RecordedFields(Arc::clone(&captured)));
        let shard = GraphShard::open_standalone_writer(
            "graph/experimental-operator-profile",
            Arc::new(InMemory::new()),
        )
        .await
        .expect("open graph shard");
        for (id, entity_id) in [(1, "alpha"), (2, "beta"), (3, "gamma")] {
            shard
                .set_vertex_metadata(
                    "cell-a",
                    id,
                    VertexMetadata::default()
                        .with_label("Entity")
                        .with_property(
                            "entity_id",
                            VertexPropertyValue::String(entity_id.to_string()),
                        ),
                )
                .await
                .unwrap();
        }

        // Unsampled: counted, but no operator span.
        shard
            .execute_cypher_rows(
                experimental("seek"),
                "MATCH (n:Entity {entity_id: 'beta'}) RETURN n.entity_id AS id",
            )
            .await
            .expect("seek executes");
        assert!(values_of(&captured.lock().unwrap(), "hydradb.query.operator.self_us").is_empty());
        let operators = shard.graph_operational_metrics().experimental_operators;
        let seek = operators
            .iter()
            .find(|operator| operator.operator == "VertexPropertySeek")
            .unwrap_or_else(|| panic!("{operators:#?}"));
        assert_eq!(seek.requests, 1);
        assert_eq!(seek.invocations, 1);
        assert_eq!(seek.rows_out, 1);
        assert_eq!(seek.hydrated_vertices, 1);
        assert!(
            seek.storage_requests >= 2,
            "index scan and metadata read: {seek:#?}"
        );
        assert!(seek.storage_bytes > 0, "{seek:#?}");
        let project = operators
            .iter()
            .find(|operator| operator.operator == "ProjectExec")
            .expect("the root is counted");
        assert_eq!(project.rows_in, 1);
        assert_eq!(
            project.storage_requests, 0,
            "its input's reads are not its own"
        );
        captured.lock().unwrap().clear();

        // Sampled (a full scan): one operator span per plan node, parented.
        shard
            .execute_cypher_rows(experimental("scan"), "MATCH (n) RETURN n.entity_id AS id")
            .await
            .expect("scan executes");
        {
            let recorded = captured.lock().unwrap();
            assert_eq!(
                values_of(&recorded, "hydradb.query.operator.id"),
                vec!["0", "1"]
            );
            assert_eq!(
                values_of(&recorded, "hydradb.query.operator.parent_id"),
                vec!["0"]
            );
            assert_eq!(
                values_of(&recorded, "hydradb.query.operator.hydrated_vertices"),
                vec!["0", "3"]
            );
        }
        let scan = shard
            .graph_operational_metrics()
            .experimental_operators
            .into_iter()
            .find(|operator| operator.operator == "AllVertexScan")
            .expect("the scan is counted");
        assert_eq!(scan.rows_out, 3);
        assert_eq!(scan.peak_retained_rows, 3);
        // The all-vertex scan reads the snapshot it holds directly, not
        // through `GraphStore`; its storage work must still be charged to it.
        assert!(
            scan.storage_requests >= 1,
            "the scan's snapshot reads were not counted: {scan:?}"
        );

        shard.close().await.expect("close graph shard");
    }
}
