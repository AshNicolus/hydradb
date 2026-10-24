//! The indexer-published planner statistics, as a node loads them.
//!
//! The indexer publishes one object per (scope, cell) next to the store
//! (`GraphShard::publish_planner_statistics`). With `GRAPH_PLANNER_STATISTICS`
//! on, a shard reads its cell's object at most once per
//! `GRAPH_PLANNER_STATISTICS_TTL_MS` and serves every request in between from
//! memory. With it off, nothing here reads anything.
//!
//! A stale entry is still served while one request re-reads it, so a TTL
//! expiry costs one object-store GET, not one per concurrent request. The
//! statistics only ever inform cost, so neither a missing, unreadable nor
//! corrupt object fails a query: each one plans as if there were none.

use std::collections::{HashMap, HashSet};
use std::sync::PoisonError;
use std::time::{Duration, Instant};

use hydradb_cypher_engine::{PropertyNumericRange, PropertyStatistics, StatisticsProvider, Symbol};

use super::statistics::scalar_property_keys;
use super::*;
use crate::{decode_vertex_property_value_key, PlannerStatistics, PropertyIndexStatistics};

/// Published planner statistics, prepared once per load for the planner.
///
/// Answers [`StatisticsProvider`] from memory with no I/O, and opts in to
/// ranking access candidates, which is the only thing that lets statistics
/// change a plan's access path. Property counts span every label: the index
/// is per property, not per label, so a label's share is an upper bound.
/// Anything the object does not hold -- an absent label, several labels, an
/// absent property -- is `None` (unknown), never zero.
#[derive(Debug)]
pub(crate) struct ScopeStatistics {
    /// The object as published; its header dates the plan's estimates.
    pub(crate) published: PlannerStatistics,
    vertex_properties: BTreeMap<String, PropertyEstimates>,
    edge_properties: BTreeMap<(String, String), PropertyEstimates>,
}

#[derive(Debug)]
struct PropertyEstimates {
    total: u64,
    distinct: u64,
    /// Exact count per encoded value key; for a sampled property, the
    /// length of the longest complete run a window saw.
    top: HashMap<String, u64>,
    top_sum: u64,
    top_len: u64,
    smallest_top: Option<u64>,
    numeric: Option<PropertyNumericRange>,
    /// `Some` for a property the indexer sampled past its exact cap.
    sampled: Option<SampledEstimates>,
    /// Sampled values that overflowed a window: broad, count unknown.
    saturated: HashSet<String>,
}

/// What a sampled property can still answer. Its total is unknown, so
/// nothing here feeds a histogram or a range; only an equality on one value
/// gets a number, inflated by `pessimism` because a sample underestimates
/// more easily than it overestimates, and underestimating is the expensive
/// direction (a seek that overshoots its guard is wasted work).
#[derive(Debug)]
struct SampledEstimates {
    /// Rows per value over the complete runs the windows held; `None` when
    /// no run fit inside a window, or when the object could not list every
    /// broad value it saw: an unlisted value may then be one of them, and
    /// handing it the average would send the planner to seek on a broad
    /// value, with the guard stopping it and the fallback running, on
    /// every query.
    rows_per_value: Option<u64>,
    /// One plus a term that shrinks with the number of windows that held
    /// entries: exact counts are the limit where it is one.
    pessimism: f64,
}

impl SampledEstimates {
    fn new(sample: &crate::PropertySample) -> Self {
        let rows_per_value = (sample.complete_runs > 0 && sample.saturated_dropped == 0)
            .then(|| sample.sample_entries.div_ceil(sample.complete_runs).max(1));
        let pessimism = 1.0 + 4.0 / f64::from(sample.windows.max(1)).sqrt();
        Self {
            rows_per_value,
            pessimism,
        }
    }

    fn inflate(&self, rows: u64) -> u64 {
        ((rows as f64) * self.pessimism).ceil() as u64
    }
}

impl PropertyEstimates {
    fn new(statistics: &PropertyIndexStatistics) -> Self {
        let top = statistics
            .top_values
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        Self {
            total: statistics.total_entries,
            distinct: statistics.distinct_values,
            top_sum: statistics.top_values.iter().map(|(_, count)| *count).sum(),
            top_len: statistics.top_values.len() as u64,
            smallest_top: statistics.top_values.iter().map(|(_, count)| *count).min(),
            top,
            numeric: numeric_range(statistics),
            sampled: statistics.sample.as_ref().map(SampledEstimates::new),
            saturated: statistics.saturated_values.iter().cloned().collect(),
        }
    }

    /// The histogram facts, which a sampled property does not have: its
    /// total is unknown and the planner must not take the exact prefix for
    /// the whole.
    fn property_statistics(&self) -> Option<PropertyStatistics> {
        if self.sampled.is_some() {
            return None;
        }
        Some(PropertyStatistics {
            non_null_count: Some(self.total),
            distinct_count: Some(self.distinct),
            ..PropertyStatistics::default()
        })
    }

    fn numeric_range(&self) -> Option<PropertyNumericRange> {
        if self.sampled.is_some() {
            return None;
        }
        self.numeric
    }

    /// Entries holding a value Cypher-equal to `value`: the exact count when
    /// one of its index spellings is a top value, else an even share of what
    /// the top values leave, never more than the rarest top value holds.
    ///
    /// For a sampled property: a saturated value is unknown, a value with a
    /// complete run gets that run's length, and any other the sampled rows
    /// per value, each inflated by the sample's pessimism.
    fn value_count(&self, value: &ScalarValue) -> Option<u64> {
        let keys = scalar_property_keys(value);
        if let Some(sampled) = &self.sampled {
            if keys.iter().any(|key| self.saturated.contains(key)) {
                return None;
            }
            let mut hits = keys.iter().filter_map(|key| self.top.get(key)).peekable();
            if hits.peek().is_some() {
                return Some(sampled.inflate(hits.sum()));
            }
            return sampled.rows_per_value.map(|rows| sampled.inflate(rows));
        }
        let mut hits = keys.iter().filter_map(|key| self.top.get(key)).peekable();
        if hits.peek().is_some() {
            return Some(hits.sum());
        }
        // Every distinct value is listed and this one is not.
        if self.distinct <= self.top_len {
            return Some(0);
        }
        let remainder = self
            .total
            .saturating_sub(self.top_sum)
            .div_ceil(self.distinct - self.top_len);
        Some(
            self.smallest_top
                .map_or(remainder, |smallest| remainder.min(smallest)),
        )
    }
}

/// The numeric domain of a property: its `i`, `j` and `n` tag ranges merged
/// by value, as the ordered walk merges those streams. A tag whose keys do
/// not decode is left out rather than guessed.
fn numeric_range(statistics: &PropertyIndexStatistics) -> Option<PropertyNumericRange> {
    let number = |key: &str| match decode_vertex_property_value_key(key).ok()? {
        VertexPropertyValue::Integer(value) => Some(value as f64),
        VertexPropertyValue::SignedInteger(value) => Some(value as f64),
        VertexPropertyValue::Float(value) => Some(value.0).filter(|value| !value.is_nan()),
        VertexPropertyValue::Bool(_) | VertexPropertyValue::String(_) => None,
    };
    let mut merged: Option<PropertyNumericRange> = None;
    for tag in ['i', 'j', 'n'] {
        let Some(range) = statistics.ranges.get(&tag) else {
            continue;
        };
        let (Some(min), Some(max)) = (number(&range.min), number(&range.max)) else {
            continue;
        };
        merged = Some(match merged {
            None => PropertyNumericRange {
                min,
                max,
                entries: range.entries,
            },
            Some(merged) => PropertyNumericRange {
                min: merged.min.min(min),
                max: merged.max.max(max),
                entries: merged.entries.saturating_add(range.entries),
            },
        });
    }
    merged
}

impl ScopeStatistics {
    pub(crate) fn new(published: PlannerStatistics) -> Self {
        let vertex_properties = published
            .vertex_properties
            .iter()
            .map(|(property, statistics)| (property.clone(), PropertyEstimates::new(statistics)))
            .collect();
        let edge_properties = published
            .edge_properties
            .iter()
            .map(|(key, statistics)| (key.clone(), PropertyEstimates::new(statistics)))
            .collect();
        Self {
            published,
            vertex_properties,
            edge_properties,
        }
    }

    fn edge_property(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
    ) -> Option<&PropertyEstimates> {
        let [relationship_type] = relationship_types else {
            return None;
        };
        self.edge_properties
            .get(&(relationship_type.to_string(), property.to_string()))
    }
}

impl StatisticsProvider for ScopeStatistics {
    fn ranks_access_candidates(&self) -> bool {
        true
    }

    /// A label the indexer could only bound from below is unknown: a bound
    /// would let a guarded label scan look cheaper than it is.
    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        let [label] = labels else {
            return None;
        };
        if self.published.labels_sampled.contains(label.as_str()) {
            return None;
        }
        self.published.labels.get(label.as_str()).copied()
    }

    fn property_statistics(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
    ) -> Option<PropertyStatistics> {
        self.vertex_properties
            .get(property.as_str())
            .and_then(PropertyEstimates::property_statistics)
    }

    fn property_value_count(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<u64> {
        self.vertex_properties
            .get(property.as_str())
            .and_then(|estimates| estimates.value_count(value))
    }

    fn property_numeric_range(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
    ) -> Option<PropertyNumericRange> {
        self.vertex_properties
            .get(property.as_str())?
            .numeric_range()
    }

    fn relationship_count(&self, relationship_types: &[Symbol]) -> Option<u64> {
        if relationship_types.is_empty() {
            return None;
        }
        relationship_types
            .iter()
            .try_fold(0_u64, |total, relationship_type| {
                if self
                    .published
                    .edge_types_sampled
                    .contains(relationship_type.as_str())
                {
                    return None;
                }
                Some(
                    total.saturating_add(
                        *self.published.edge_types.get(relationship_type.as_str())?,
                    ),
                )
            })
    }

    fn relationship_property_statistics(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
    ) -> Option<PropertyStatistics> {
        self.edge_property(relationship_types, property)
            .and_then(PropertyEstimates::property_statistics)
    }

    fn relationship_property_value_count(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<u64> {
        self.edge_property(relationship_types, property)
            .and_then(|estimates| estimates.value_count(value))
    }
}

/// Loaded planner statistics per cell id. One per shard, and a shard is one
/// (scope, cell) store, so this is the per-(scope, cell) cache.
#[derive(Default)]
pub(crate) struct PlannerStatisticsCache {
    entries: std::sync::Mutex<BTreeMap<String, CacheEntry>>,
}

#[derive(Default)]
struct CacheEntry {
    /// `None` when the object is absent or could not be decoded.
    value: Option<Arc<ScopeStatistics>>,
    /// When the last read finished, successful or not; `None` before the
    /// first one. A failed read also restarts the clock, which is the backoff.
    loaded_at: Option<Instant>,
    /// One request is re-reading the object; the others serve `value`.
    refreshing: bool,
}

#[derive(Debug)]
enum Lookup {
    /// Serve this, read nothing.
    Serve(Option<Arc<ScopeStatistics>>),
    /// This request reads the object and must finish or abandon the refresh.
    Refresh,
}

impl PlannerStatisticsCache {
    fn lookup(&self, cell_id: &str, now: Instant, ttl: Duration) -> Lookup {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = entries.entry(cell_id.to_string()).or_default();
        let fresh = entry
            .loaded_at
            .is_some_and(|loaded_at| now.saturating_duration_since(loaded_at) < ttl);
        if fresh || entry.refreshing {
            return Lookup::Serve(entry.value.clone());
        }
        entry.refreshing = true;
        Lookup::Refresh
    }

    /// End a refresh. `Some(value)` replaces the entry; `None` keeps the value
    /// it had, which is what a failed read does. Returns the value now held.
    fn finish(
        &self,
        cell_id: &str,
        now: Instant,
        value: Option<Option<Arc<ScopeStatistics>>>,
    ) -> Option<Arc<ScopeStatistics>> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = entries.entry(cell_id.to_string()).or_default();
        if let Some(value) = value {
            entry.value = value;
        }
        entry.loaded_at = Some(now);
        entry.refreshing = false;
        entry.value.clone()
    }

    /// A refresh whose request was dropped mid-read: the next request retries.
    fn abandon(&self, cell_id: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = entries.get_mut(cell_id) {
            entry.refreshing = false;
        }
    }
}

/// Clears `refreshing` if the reading request is cancelled, so a dropped
/// request cannot leave every later one serving a stale entry forever.
struct RefreshGuard<'a> {
    cache: &'a PlannerStatisticsCache,
    cell_id: &'a str,
    armed: bool,
}

impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.abandon(self.cell_id);
        }
    }
}

/// Where a shard sends a planner statistics request: the scoped cluster
/// installs one that writes the request marker and wakes the indexer. A
/// shard without one (a standalone writer, the indexer's own readers) counts
/// nothing and asks for nothing.
pub(crate) type PlannerStatisticsHintSink =
    Arc<dyn Fn(&str, PlannerStatisticsHintFailure) + Send + Sync>;

/// What a sink calls when the request it was handed could not be written:
/// the hint window forgets it sent, so the next query asks again instead
/// of waiting out the TTL with no marker for the indexer to find.
pub(crate) type PlannerStatisticsHintFailure = Arc<dyn Fn() + Send + Sync>;

/// Queries planned without statistics, per cell, and whether this cell has
/// already asked the indexer inside the current TTL.
///
/// Demand is what gets a scope's statistics built: the indexer builds only
/// what a node asks for. One request per TTL per cell bounds the markers a
/// hot scope writes while its build is pending, and the threshold keeps a
/// one-off query on a cold tenant from costing a build.
#[derive(Default)]
pub(crate) struct PlannerStatisticsHints {
    /// Shared with the failure callbacks handed to the sink.
    cells: Arc<std::sync::Mutex<BTreeMap<String, HintWindow>>>,
    sink: std::sync::OnceLock<PlannerStatisticsHintSink>,
}

/// How long a cell waits after a request write failed before a query may
/// send another. Shorter than the TTL, so an outage delays the first
/// build by seconds rather than a minute, and longer than a query, so a
/// busy cell does not turn every query into a failing write.
const PLANNER_STATISTICS_HINT_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
struct HintWindow {
    started: Instant,
    planned_without: u32,
    sent: bool,
    /// Set when the sent request failed to write: no retry before this.
    retry_after: Option<Instant>,
}

impl PlannerStatisticsHints {
    /// Install where requests go. The first installation wins; a scope's
    /// shards are opened once per scoped cluster, so there is one.
    pub(crate) fn install(&self, sink: PlannerStatisticsHintSink) {
        let _ = self.sink.set(sink);
    }

    /// One more query of `cell_id` planned without statistics. Returns true
    /// when this one crossed the threshold and a request was sent.
    fn observe(&self, cell_id: &str, now: Instant, ttl: Duration, threshold: u32) -> bool {
        if threshold == 0 {
            return false;
        }
        let sent_from = {
            let mut cells = self.cells.lock().unwrap_or_else(PoisonError::into_inner);
            let window = cells.entry(cell_id.to_string()).or_insert(HintWindow {
                started: now,
                planned_without: 0,
                sent: false,
                retry_after: None,
            });
            if now.saturating_duration_since(window.started) >= ttl {
                *window = HintWindow {
                    started: now,
                    planned_without: 0,
                    sent: false,
                    retry_after: None,
                };
            }
            if window.sent || window.retry_after.is_some_and(|at| now < at) {
                None
            } else {
                window.planned_without = window.planned_without.saturating_add(1);
                window.sent = window.planned_without >= threshold;
                window.sent.then_some(window.started)
            }
        };
        let Some(started) = sent_from else {
            return false;
        };
        let Some(sink) = self.sink.get() else {
            return false;
        };
        // Resets only the window that sent, so a newer window, or a cell
        // whose statistics loaded meanwhile (its window is gone), is
        // untouched; and not before a retry delay, so an outage costs one
        // failing write per delay, not one per query.
        let cells = Arc::clone(&self.cells);
        let cell = cell_id.to_string();
        let on_failure: PlannerStatisticsHintFailure = Arc::new(move || {
            let mut cells = cells.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(window) = cells.get_mut(&cell) {
                if window.started == started {
                    window.sent = false;
                    window.retry_after = Some(Instant::now() + PLANNER_STATISTICS_HINT_RETRY_DELAY);
                }
            }
        });
        sink(cell_id, on_failure);
        true
    }

    /// Statistics arrived for `cell_id`: nothing to ask for until they go
    /// missing again.
    fn clear(&self, cell_id: &str) {
        self.cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(cell_id);
    }
}

impl GraphShard {
    /// [`Self::planner_statistics_for`] for one query. An `EXPLAIN` is not an
    /// experimental request in `query_experimental_requests`, so its miss is
    /// not counted either: the share planned without statistics divides one
    /// by the other and must not pass 100%.
    pub(super) async fn planner_statistics_for_query(
        &self,
        cell_id: &str,
        budget: &QueryBudget,
        explain: bool,
    ) -> Option<Arc<ScopeStatistics>> {
        if !self.planner_statistics.enabled {
            return None;
        }
        let statistics = self.cached_planner_statistics(cell_id, budget).await;
        if statistics.is_none() && !explain {
            self.operation_metrics
                .query_experimental_planned_without_statistics
                .fetch_add(1, Ordering::Relaxed);
            let config = &self.planner_statistics;
            if self.planner_statistics_hints.observe(
                cell_id,
                Instant::now(),
                config.ttl,
                config.hint_threshold,
            ) {
                self.operation_metrics
                    .planner_statistics_hints_sent
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        statistics
    }

    /// The cache half of [`Self::planner_statistics_for`], flag on.
    async fn cached_planner_statistics(
        &self,
        cell_id: &str,
        budget: &QueryBudget,
    ) -> Option<Arc<ScopeStatistics>> {
        let metrics = &self.operation_metrics;
        let cache = &self.planner_statistics_cache;
        if let Lookup::Serve(value) =
            cache.lookup(cell_id, Instant::now(), self.planner_statistics.ttl)
        {
            return value;
        }
        let mut guard = RefreshGuard {
            cache,
            cell_id,
            armed: true,
        };
        let read = budget
            .read_only_io(
                "experimental_cypher_planner_statistics",
                self.read_planner_statistics(cell_id),
            )
            .await;
        guard.armed = false;
        let loaded = match read {
            Ok(Some(statistics)) => {
                metrics
                    .planner_statistics_reloads
                    .fetch_add(1, Ordering::Relaxed);
                self.planner_statistics_hints.clear(cell_id);
                Some(Some(Arc::new(ScopeStatistics::new(statistics))))
            }
            Ok(None) => Some(None),
            Err(error @ GraphError::CorruptValue { .. }) => {
                metrics
                    .planner_statistics_corrupt_ignored
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(hydradb.cell_id = %cell_id, error = %error, "planner statistics are unreadable; planning without them");
                Some(None)
            }
            Err(error) => {
                metrics
                    .planner_statistics_load_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(hydradb.cell_id = %cell_id, error = %error, "planner statistics read failed; keeping the loaded copy");
                None
            }
        };
        let held = cache.finish(cell_id, Instant::now(), loaded);
        metrics.set_planner_statistics_loaded(held.as_deref().map(|statistics| {
            (
                statistics.published.storage_sequence,
                statistics.published.built_at_ms,
            )
        }));
        held
    }
}
