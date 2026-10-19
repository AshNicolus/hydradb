//! Bounded-vocabulary memory attribution. Byte values are estimates, not heap ownership.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::{AtomicDurationHistogram, DurationHistogramSnapshot};

/// Fixed process-wide stages; never attach query IDs, tenants or cells.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Some fixed stages are recorded only with client-api enabled.
#[repr(usize)]
pub(crate) enum MemoryStage {
    ClientPrepared,
    ClientResultBuffer,
    ClientPrepare,
    ClientNamespaceWait,
    ClientQueryWait,
    ClientExecute,
    ClientContextParameters,
    ShardWriteWait,
    ShardWriteActive,
    CursorBuffer,
    StorageWriter,
    StorageReader,
    StorageSnapshot,
    StorageWriterClose,
    StorageReaderClose,
    StorageReaderRefresh,
    ShardWritePipelineWait,
    ShardWriteDurability,
}
const STAGES: [&str; 18] = [
    "client_prepared",
    "client_result_buffer",
    "client_prepare",
    "client_namespace_wait",
    "client_query_wait",
    "client_execute",
    "client_context_parameters",
    "shard_write_wait",
    "shard_write_active",
    "cursor_buffer",
    "storage_writer",
    "storage_reader",
    "storage_snapshot",
    "storage_writer_close",
    "storage_reader_close",
    "storage_reader_refresh",
    "shard_write_pipeline_wait",
    "shard_write_durability",
];
const STAGE_DETAIL_LIMIT: usize = 1_024;

impl MemoryStage {
    fn detail_limit(self) -> usize {
        let _ = self;
        STAGE_DETAIL_LIMIT
    }
}

#[derive(Default)]
struct State {
    next: u64,
    live: BTreeMap<(Instant, u64), u64>,
    overflow_items: u64,
    overflow_started: Option<Instant>,
    items: u64,
    bytes: u64,
    entered: u64,
    exited: u64,
}
impl State {
    fn oldest_seconds(&self) -> f64 {
        self.live
            .first_key_value()
            .map(|((start, _), _)| *start)
            .into_iter()
            .chain(self.overflow_started)
            .min()
            .map_or(0.0, |start| start.elapsed().as_secs_f64())
    }

    fn oldest_tracked_seconds(&self) -> f64 {
        self.live
            .first_key_value()
            .map(|((start, _), _)| *start)
            .map_or(0.0, |start| start.elapsed().as_secs_f64())
    }

    fn overflow_cohort_seconds(&self) -> f64 {
        self.overflow_started
            .map_or(0.0, |start| start.elapsed().as_secs_f64())
    }
}
#[derive(Default)]
struct Tracker {
    state: Mutex<State>,
    duration: AtomicDurationHistogram,
}
static TRACKERS: OnceLock<[Arc<Tracker>; STAGES.len()]> = OnceLock::new();
fn trackers() -> &'static [Arc<Tracker>; STAGES.len()] {
    TRACKERS.get_or_init(|| std::array::from_fn(|_| Arc::new(Tracker::default())))
}

/// A process-global sample. Stage byte estimates may overlap; see the runbook.
#[derive(Clone, Debug)]
pub struct MemoryDiagnosticSnapshot {
    pub stage: &'static str,
    pub items: u64,
    pub estimated_bytes: u64,
    /// Compatibility value combining retained detail and overflow cohort age.
    pub oldest_seconds: f64,
    pub oldest_tracked_seconds: f64,
    pub tracking_overflow_items: u64,
    pub overflow_cohort_seconds: f64,
    pub entered: u64,
    pub exited: u64,
    pub duration: DurationHistogramSnapshot,
}
/// All stages, including zeroes, with no I/O or async locks.
pub fn memory_diagnostic_snapshot() -> Vec<MemoryDiagnosticSnapshot> {
    trackers()
        .iter()
        .zip(STAGES)
        .map(|(tracker, stage)| {
            let state = tracker.state.lock().unwrap_or_else(|p| p.into_inner());
            let oldest_tracked_seconds = state.oldest_tracked_seconds();
            MemoryDiagnosticSnapshot {
                stage,
                items: state.items,
                estimated_bytes: state.bytes,
                oldest_seconds: state.oldest_seconds(),
                oldest_tracked_seconds,
                tracking_overflow_items: state.overflow_items,
                overflow_cohort_seconds: state.overflow_cohort_seconds(),
                entered: state.entered,
                exited: state.exited,
                duration: tracker.duration.snapshot(),
            }
        })
        .collect()
}

/// The token follows the object or future. Dropping on any exit removes it.
#[derive(Debug)]
pub(crate) struct MemoryDiagnosticGuard {
    tracker: Arc<Tracker>,
    detailed_key: Option<(Instant, u64)>,
    started: Instant,
    bytes: u64,
}
impl std::fmt::Debug for Tracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemoryTracker")
    }
}
impl MemoryDiagnosticGuard {
    pub(crate) fn new(stage: MemoryStage, bytes: u64) -> Self {
        Self::tracked_with_limit(
            Arc::clone(&trackers()[stage as usize]),
            bytes,
            stage.detail_limit(),
        )
    }
    #[cfg(test)]
    fn tracked(tracker: Arc<Tracker>, bytes: u64) -> Self {
        Self::tracked_with_limit(tracker, bytes, usize::MAX)
    }
    fn tracked_with_limit(tracker: Arc<Tracker>, bytes: u64, limit: usize) -> Self {
        let started = Instant::now();
        let mut state = tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        let id = state.next;
        state.next += 1;
        state.items += 1;
        state.entered += 1;
        state.bytes = state.bytes.saturating_add(bytes);
        let detailed_key = if state.live.len() < limit {
            let key = (started, id);
            state.live.insert(key, bytes);
            Some(key)
        } else {
            state.overflow_items += 1;
            state.overflow_started = Some(
                state
                    .overflow_started
                    .map_or(started, |oldest| oldest.min(started)),
            );
            None
        };
        drop(state);
        Self {
            tracker,
            detailed_key,
            started,
            bytes,
        }
    }
    #[cfg(test)]
    pub(crate) fn probe(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let tracker = Arc::clone(&self.tracker);
        let detailed_key = self.detailed_key;
        move || {
            detailed_key.is_some_and(|key| tracker.state.lock().unwrap().live.contains_key(&key))
        }
    }
    #[cfg(any(feature = "client-api", test))]
    pub(crate) fn set_bytes(&mut self, bytes: u64) {
        let mut state = self.tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = self.detailed_key {
            *state.live.get_mut(&key).expect("live diagnostic guard") = bytes;
        }
        state.bytes = state.bytes.saturating_sub(self.bytes).saturating_add(bytes);
        self.bytes = bytes;
    }
}
impl Drop for MemoryDiagnosticGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = self.detailed_key {
            state.live.remove(&key);
        } else {
            state.overflow_items = state.overflow_items.saturating_sub(1);
            if state.overflow_items == 0 {
                state.overflow_started = None;
            }
        }
        state.items = state.items.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(self.bytes);
        state.exited += 1;
        self.tracker.duration.record(self.started.elapsed());
    }
}

/// Cloneable handles share one token; the last wrapper clone releases it.
pub(crate) struct TrackedMemory<T> {
    value: T,
    _guard: Arc<MemoryDiagnosticGuard>,
}
impl<T> TrackedMemory<T> {
    #[cfg(test)]
    pub(crate) fn probe(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        self._guard.probe()
    }
    pub(crate) fn new(value: T, stage: MemoryStage) -> Self {
        Self {
            value,
            _guard: Arc::new(MemoryDiagnosticGuard::new(stage, 0)),
        }
    }
}
impl<T: Clone> Clone for TrackedMemory<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _guard: Arc::clone(&self._guard),
        }
    }
}
impl<T> std::ops::Deref for TrackedMemory<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

// Request estimates are a view of the parent client allocation, not a second
// allocation. Tokio task boundaries intentionally do not inherit this context.
tokio::task_local! { pub(crate) static REQUEST_ESTIMATED_BYTES: u64; }
pub(crate) fn request_estimated_bytes() -> u64 {
    REQUEST_ESTIMATED_BYTES
        .try_with(|bytes| *bytes)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "manual diagnostic accounting overhead measurement; no timing assertions"]
    fn memory_diagnostics_accounting_overhead() {
        const ITERATIONS: usize = 10_000;
        for threads in [1, 8] {
            let tracker = Arc::new(Tracker::default());
            let started = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    let tracker = Arc::clone(&tracker);
                    scope.spawn(move || {
                        for _ in 0..ITERATIONS {
                            let guard =
                                MemoryDiagnosticGuard::tracked(Arc::clone(&tracker), 24 * 1024);
                            std::hint::black_box(&guard);
                            drop(guard);
                        }
                    });
                }
            });
            let elapsed = started.elapsed();
            eprintln!("memory_diagnostic threads={threads} operations={} elapsed_ms={:.3} aggregate_ns_per_operation={:.1}",
                threads * ITERATIONS, elapsed.as_secs_f64() * 1000.0,
                elapsed.as_secs_f64() * 1e9 / (threads * ITERATIONS) as f64);
            let state = tracker.state.lock().unwrap();
            assert_eq!(state.entered, (threads * ITERATIONS) as u64);
            assert_eq!(state.entered, state.exited);
            assert_eq!(state.items, 0);
            assert_eq!(state.bytes, 0);
        }
    }
    #[test]
    fn error_and_unwind_release_accounting() {
        let tracker = Arc::new(Tracker::default());
        let fail = || -> Result<(), ()> {
            let _guard = MemoryDiagnosticGuard::tracked(Arc::clone(&tracker), 100);
            Err(())?;
            Ok(())
        };
        assert!(fail().is_err());
        assert!(std::panic::catch_unwind(|| {
            let _guard = MemoryDiagnosticGuard::tracked(Arc::clone(&tracker), 200);
            panic!("simulated executor failure");
        })
        .is_err());
        let state = tracker.state.lock().unwrap();
        assert_eq!(
            (
                state.bytes,
                state.items,
                state.live.len(),
                state.entered,
                state.exited
            ),
            (0, 0, 0, 2, 2)
        );
    }
    #[test]
    fn guard_resize_drop_and_arc_sharing_release_exactly_once() {
        let tracker = Arc::new(Tracker::default());
        let mut guard = MemoryDiagnosticGuard::tracked(Arc::clone(&tracker), 17);
        guard.set_bytes(31);
        let guard = Arc::new(guard);
        let clone = Arc::clone(&guard);
        drop(guard);
        assert_eq!(tracker.state.lock().unwrap().bytes, 31);
        drop(clone);
        let state = tracker.state.lock().unwrap();
        assert_eq!(
            (
                state.bytes,
                state.items,
                state.live.len(),
                state.entered,
                state.exited
            ),
            (0, 0, 0, 1, 1)
        );
        assert_eq!(tracker.duration.snapshot().count(), 1);
    }
    #[tokio::test]
    async fn aborted_wait_releases_estimate() {
        let tracker = Arc::new(Tracker::default());
        let task_tracker = Arc::clone(&tracker);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = MemoryDiagnosticGuard::tracked(task_tracker, 4096);
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        assert_eq!(tracker.state.lock().unwrap().bytes, 4096);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let state = tracker.state.lock().unwrap();
        assert_eq!(
            (
                state.bytes,
                state.items,
                state.live.len(),
                state.entered,
                state.exited
            ),
            (0, 0, 0, 1, 1)
        );
    }

    #[test]
    fn bounded_tracking_keeps_exact_totals_without_unbounded_detail_entries() {
        let tracker = Arc::new(Tracker::default());
        let mut guards: Vec<_> = (0..=STAGE_DETAIL_LIMIT)
            .map(|_| {
                MemoryDiagnosticGuard::tracked_with_limit(
                    Arc::clone(&tracker),
                    24,
                    STAGE_DETAIL_LIMIT,
                )
            })
            .collect();
        {
            let state = tracker.state.lock().unwrap();
            assert_eq!(state.live.len(), STAGE_DETAIL_LIMIT);
            assert_eq!(state.overflow_items, 1);
            assert_eq!(state.items, STAGE_DETAIL_LIMIT as u64 + 1);
            assert_eq!(state.bytes, (STAGE_DETAIL_LIMIT as u64 + 1) * 24);
        }
        guards.drain(..STAGE_DETAIL_LIMIT).for_each(drop);
        std::thread::sleep(std::time::Duration::from_millis(1));
        {
            let state = tracker.state.lock().unwrap();
            assert_eq!(state.live.len(), 0);
            assert_eq!(state.overflow_items, 1);
            assert_eq!(state.items, 1);
            assert!(state.oldest_seconds() > 0.0);
            assert_eq!(state.oldest_tracked_seconds(), 0.0);
            assert!(state.overflow_cohort_seconds() > 0.0);
        }
        drop(guards);
        let state = tracker.state.lock().unwrap();
        assert_eq!(
            (
                state.live.len(),
                state.overflow_items,
                state.overflow_started,
                state.items,
                state.bytes
            ),
            (0, 0, None, 0, 0)
        );
        assert_eq!(state.entered, state.exited);
        assert_eq!(tracker.duration.snapshot().count(), state.exited);
    }

    #[test]
    fn every_stage_has_the_same_detail_cap() {
        for stage in [
            MemoryStage::ClientPrepared,
            MemoryStage::ClientResultBuffer,
            MemoryStage::ClientPrepare,
            MemoryStage::ClientNamespaceWait,
            MemoryStage::ClientQueryWait,
            MemoryStage::ClientExecute,
            MemoryStage::ClientContextParameters,
            MemoryStage::ShardWriteWait,
            MemoryStage::ShardWriteActive,
            MemoryStage::CursorBuffer,
            MemoryStage::StorageWriter,
            MemoryStage::StorageReader,
            MemoryStage::StorageSnapshot,
            MemoryStage::StorageWriterClose,
            MemoryStage::StorageReaderClose,
            MemoryStage::StorageReaderRefresh,
            MemoryStage::ShardWritePipelineWait,
            MemoryStage::ShardWriteDurability,
        ] {
            assert_eq!(stage.detail_limit(), STAGE_DETAIL_LIMIT);
        }
    }
}
