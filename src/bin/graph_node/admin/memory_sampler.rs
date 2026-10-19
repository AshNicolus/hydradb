//! One-second samples retain high-water marks since admin sampler startup.
//! Peaks do not reset on scrape (multiple scrapers must observe the same state).
//! Scheduling delays and subsecond spikes are possible; this is not a profiler.
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use super::{append_process_memory_metrics, process_memory_metrics, ProcessMemoryMetrics};

const PROCESS_SAMPLING_SUPPORTED: bool = cfg!(all(target_os = "linux", target_env = "gnu"));
const CGROUP_SAMPLING_SUPPORTED: bool = cfg!(target_os = "linux");

#[derive(Clone, Debug, Default)]
struct SourceStatus {
    samples: u64,
    failures: u64,
    last_success: f64,
    available: bool,
}

impl SourceStatus {
    fn observe(&mut self, attempted: bool, succeeded: bool, now: f64) {
        self.available = succeeded;
        if !attempted {
            return;
        }
        if succeeded {
            self.samples += 1;
            self.last_success = now;
        } else {
            self.failures += 1;
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Sample {
    timestamp: f64,
    samples: u64,
    failures: u64,
    rss_peak: Option<u64>,
    live_peak: Option<u64>,
    free_peak: Option<u64>,
    open_file_descriptors_peak: Option<u64>,
    cgroup_peak: Option<u64>,
    process: Option<ProcessMemoryMetrics>,
    cgroup: Option<CgroupMemory>,
    process_status: SourceStatus,
    cgroup_status: SourceStatus,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CgroupMemory {
    current: u64,
    limit: Option<u64>,
    kernel_peak: Option<u64>,
    anon: Option<u64>,
    file: Option<u64>,
    kernel: Option<u64>,
    events: Vec<(&'static str, u64)>,
}

fn update_peak(peak: &mut Option<u64>, value: u64) {
    *peak = Some(peak.unwrap_or_default().max(value));
}

impl Sample {
    fn observe(
        &mut self,
        process: Option<ProcessMemoryMetrics>,
        cgroup: Option<CgroupMemory>,
        now: f64,
        process_attempted: bool,
        cgroup_attempted: bool,
    ) {
        self.process_status
            .observe(process_attempted, process.is_some(), now);
        self.cgroup_status
            .observe(cgroup_attempted, cgroup.is_some(), now);
        if (process_attempted || cgroup_attempted) && process.is_none() && cgroup.is_none() {
            self.failures += 1;
            // Failed reads must not publish stale current values.
            self.process = None;
            self.cgroup = None;
            return;
        }
        if process.is_some() || cgroup.is_some() {
            self.timestamp = now;
            self.samples += 1;
        }
        if let Some(memory) = &process {
            update_peak(&mut self.rss_peak, memory.resident_bytes);
            update_peak(&mut self.live_peak, memory.allocator_live_bytes);
            update_peak(&mut self.free_peak, memory.allocator_arena_free_bytes);
            if let Some(open) = memory.open_file_descriptors {
                update_peak(&mut self.open_file_descriptors_peak, open);
            }
        }
        if let Some(memory) = &cgroup {
            update_peak(&mut self.cgroup_peak, memory.current);
        }
        self.process = process;
        self.cgroup = cgroup;
    }

    fn render(&self, output: &mut String) {
        fn gauge(output: &mut String, name: &str, value: impl std::fmt::Display) {
            output.push_str(&format!("# TYPE {name} gauge\n{name} {value}\n"));
        }
        gauge(
            output,
            "graph_memory_sampler_last_success_timestamp_seconds",
            self.timestamp,
        );
        for (name, value) in [
            ("graph_memory_sampler_samples_total", self.samples),
            ("graph_memory_sampler_failures_total", self.failures),
        ] {
            output.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
        }
        output.push_str(concat!(
            "# TYPE graph_memory_sampler_source_samples_total counter\n",
            "# TYPE graph_memory_sampler_source_failures_total counter\n",
            "# TYPE graph_memory_sampler_source_last_success_timestamp_seconds gauge\n",
            "# TYPE graph_memory_sampler_source_available gauge\n",
        ));
        for (source, status) in [
            ("process", &self.process_status),
            ("cgroup", &self.cgroup_status),
        ] {
            output.push_str(&format!(
                "graph_memory_sampler_source_samples_total{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_failures_total{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_last_success_timestamp_seconds{{source=\"{source}\"}} {}\n\
                 graph_memory_sampler_source_available{{source=\"{source}\"}} {}\n",
                status.samples,
                status.failures,
                status.last_success,
                u8::from(status.available),
            ));
        }
        if let Some(memory) = self.process {
            append_process_memory_metrics(output, memory);
        }
        for (name, value) in [
            (
                "graph_process_resident_memory_sampled_peak_bytes",
                self.rss_peak,
            ),
            (
                "graph_process_allocator_live_sampled_peak_bytes",
                self.live_peak,
            ),
            (
                "graph_process_allocator_arena_free_sampled_peak_bytes",
                self.free_peak,
            ),
            (
                "graph_process_open_file_descriptors_sampled_peak",
                self.open_file_descriptors_peak,
            ),
            ("graph_cgroup_memory_sampled_peak_bytes", self.cgroup_peak),
        ] {
            if let Some(value) = value {
                gauge(output, name, value);
            }
        }
        if let Some(memory) = &self.cgroup {
            output.push_str("# TYPE graph_cgroup_memory_bytes gauge\n");
            for (kind, value) in [
                ("current", Some(memory.current)),
                ("anon", memory.anon),
                ("file", memory.file),
                ("kernel", memory.kernel),
            ] {
                if let Some(value) = value {
                    output.push_str(&format!(
                        "graph_cgroup_memory_bytes{{kind=\"{kind}\"}} {value}\n"
                    ));
                }
            }
            if let Some(value) = memory.limit {
                gauge(output, "graph_cgroup_memory_limit_bytes", value);
            }
            if let Some(value) = memory.kernel_peak {
                gauge(output, "graph_cgroup_memory_peak_bytes", value);
            }
            output.push_str("# TYPE graph_cgroup_memory_events_total counter\n");
            for (event, value) in &memory.events {
                output.push_str(&format!(
                    "graph_cgroup_memory_events_total{{event=\"{event}\"}} {value}\n"
                ));
            }
        }
    }
}

pub(super) struct MemorySampler {
    sample: Arc<Mutex<Sample>>,
    task: JoinHandle<()>,
}

impl MemorySampler {
    pub(super) fn start() -> Arc<Self> {
        let sample = Arc::new(Mutex::new(Sample::default()));
        let state = sample.clone();
        let task = tokio::spawn(async move {
            let mut cgroup = None;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let previous_cgroup = cgroup.clone();
                match run_blocking(move || collect_sample(previous_cgroup)).await {
                    Ok(collected) => {
                        cgroup = collected.cgroup_directory;
                        state.lock().unwrap_or_else(|e| e.into_inner()).observe(
                            collected.process,
                            collected.cgroup,
                            collected.now,
                            PROCESS_SAMPLING_SUPPORTED,
                            CGROUP_SAMPLING_SUPPORTED,
                        );
                    }
                    Err(_) => {
                        cgroup = None;
                        state.lock().unwrap_or_else(|e| e.into_inner()).observe(
                            None,
                            None,
                            0.0,
                            PROCESS_SAMPLING_SUPPORTED,
                            CGROUP_SAMPLING_SUPPORTED,
                        );
                    }
                }
            }
        });
        Arc::new(Self { sample, task })
    }

    pub(super) fn render(&self, output: &mut String) {
        self.sample
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .render(output);
    }
}

async fn run_blocking<F, T>(operation: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation).await
}

struct CollectedSample {
    process: Option<ProcessMemoryMetrics>,
    cgroup: Option<CgroupMemory>,
    cgroup_directory: Option<std::path::PathBuf>,
    now: f64,
}

fn collect_sample(mut directory: Option<std::path::PathBuf>) -> CollectedSample {
    // Keep allocator and procfs/cgroupfs calls off runtime workers. No heap
    // walking, task scan or trim occurs.
    let process = process_memory_metrics();
    if CGROUP_SAMPLING_SUPPORTED && directory.is_none() {
        directory = cgroup_directory();
    }
    let cgroup = directory.as_deref().and_then(read_cgroup);
    if CGROUP_SAMPLING_SUPPORTED && cgroup.is_none() {
        // Re-resolve next pass after either discovery or reading failed.
        directory = None;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    CollectedSample {
        process,
        cgroup,
        cgroup_directory: directory,
        now,
    }
}

impl Drop for MemorySampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn field(input: &str, key: &str) -> Option<u64> {
    input.lines().find_map(|line| {
        let mut words = line.split_ascii_whitespace();
        (words.next()? == key)
            .then(|| words.next()?.parse().ok())
            .flatten()
    })
}

fn read_cgroup(dir: &std::path::Path) -> Option<CgroupMemory> {
    let current = std::fs::read_to_string(dir.join("memory.current"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let optional_number = |file| {
        std::fs::read_to_string(dir.join(file))
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
    };
    let stat = std::fs::read_to_string(dir.join("memory.stat")).unwrap_or_default();
    let events = std::fs::read_to_string(dir.join("memory.events")).unwrap_or_default();
    Some(CgroupMemory {
        current,
        limit: optional_number("memory.max"), // "max" means unlimited, not zero.
        kernel_peak: optional_number("memory.peak"),
        anon: field(&stat, "anon"),
        file: field(&stat, "file"),
        kernel: field(&stat, "kernel"),
        events: ["low", "high", "max", "oom", "oom_kill", "oom_group_kill"]
            .into_iter()
            .filter_map(|name| field(&events, name).map(|value| (name, value)))
            .collect(),
    })
}

fn cgroup_directory() -> Option<std::path::PathBuf> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    resolve_cgroup_directory(&membership, &mounts)
}

/// Resolve cgroup v2 in both private container and host cgroup namespaces.
/// Mountinfo paths are kernel-escaped; unsupported escapes fail closed.
fn resolve_cgroup_directory(membership: &str, mounts: &str) -> Option<std::path::PathBuf> {
    use std::path::{Component, Path};
    let path = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    if !path.starts_with('/')
        || Path::new(path)
            .components()
            .any(|p| matches!(p, Component::ParentDir))
    {
        return None;
    }
    for line in mounts.lines() {
        let (left, right) = line.split_once(" - ")?;
        if right.split_ascii_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let parts: Vec<_> = left.split_ascii_whitespace().collect();
        let root = *parts.get(3)?;
        let mount = *parts.get(4)?;
        if root.contains('\\') || mount.contains('\\') {
            continue;
        }
        if let Ok(relative) = Path::new(path).strip_prefix(root) {
            return Some(Path::new(mount).join(relative));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_private_and_host_cgroup_namespaces_without_parent_escape() {
        let private = "30 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw";
        assert_eq!(
            resolve_cgroup_directory("0::/\n", private).unwrap(),
            std::path::Path::new("/sys/fs/cgroup")
        );
        assert_eq!(
            resolve_cgroup_directory("0::/kubepods/pod/container\n", private).unwrap(),
            std::path::Path::new("/sys/fs/cgroup/kubepods/pod/container")
        );
        let subtree = "30 20 0:28 /kubepods/pod /sys/fs/cgroup rw - cgroup2 cgroup rw";
        assert_eq!(
            resolve_cgroup_directory("0::/kubepods/pod/container\n", subtree).unwrap(),
            std::path::Path::new("/sys/fs/cgroup/container")
        );
        assert!(resolve_cgroup_directory("0::/../host\n", private).is_none());
        assert!(resolve_cgroup_directory("5:memory:/\n", private).is_none());
    }

    #[test]
    fn cgroup_optional_fields_are_omitted_and_unlimited_is_not_zero() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("memory.current"), "1024\n").unwrap();
        std::fs::write(dir.path().join("memory.max"), "max\n").unwrap();
        std::fs::write(dir.path().join("memory.stat"), "anon 700\nfile 300\n").unwrap();
        std::fs::write(dir.path().join("memory.events"), "oom 4\noom_kill 2\n").unwrap();
        let cgroup = read_cgroup(dir.path()).unwrap();
        assert_eq!(cgroup.limit, None);
        assert_eq!(cgroup.kernel, None);
        assert_eq!(cgroup.events, vec![("oom", 4), ("oom_kill", 2)]);
        let mut sample = Sample::default();
        sample.observe(None, Some(cgroup), 42.0, true, true);
        let mut output = String::new();
        sample.render(&mut output);
        assert!(output.contains("kind=\"anon\"} 700"));
        assert!(!output.contains("graph_cgroup_memory_limit_bytes"));
        assert!(!output.contains("kind=\"kernel\"}"));
    }

    #[test]
    fn sampled_peaks_survive_falling_values_and_failed_samples() {
        let mut sample = Sample::default();
        let process = |value| ProcessMemoryMetrics {
            resident_bytes: value,
            allocator_live_bytes: value / 2,
            allocator_arena_free_bytes: value / 4,
            allocator_arena_bytes: value,
            allocator_arena_in_use_bytes: value / 2,
            allocator_mmap_bytes: 0,
            open_file_descriptors: Some(value),
            open_file_descriptor_soft_limit: Some(1024),
        };
        sample.observe(
            Some(process(100)),
            Some(CgroupMemory {
                current: 200,
                ..Default::default()
            }),
            1.0,
            true,
            true,
        );
        sample.observe(
            Some(process(40)),
            Some(CgroupMemory {
                current: 50,
                ..Default::default()
            }),
            2.0,
            true,
            true,
        );
        sample.observe(None, None, 3.0, true, true);
        assert_eq!(sample.rss_peak, Some(100));
        assert_eq!(sample.live_peak, Some(50));
        assert_eq!(sample.free_peak, Some(25));
        assert_eq!(sample.open_file_descriptors_peak, Some(100));
        assert_eq!(sample.cgroup_peak, Some(200));
        assert_eq!(sample.timestamp, 2.0);
        assert_eq!(sample.samples, 2);
        assert_eq!(sample.failures, 1);
        assert!(sample.cgroup.is_none());
    }

    #[test]
    fn one_source_failure_is_visible_when_the_other_source_succeeds() {
        let process = ProcessMemoryMetrics {
            resident_bytes: 100,
            allocator_live_bytes: 50,
            allocator_arena_free_bytes: 25,
            allocator_arena_bytes: 75,
            allocator_arena_in_use_bytes: 50,
            allocator_mmap_bytes: 0,
            open_file_descriptors: Some(7),
            open_file_descriptor_soft_limit: Some(1024),
        };
        let mut sample = Sample::default();
        sample.observe(Some(process), None, 7.0, true, true);
        assert_eq!(sample.samples, 1);
        assert_eq!(sample.failures, 0);
        assert_eq!(sample.process_status.samples, 1);
        assert_eq!(sample.cgroup_status.failures, 1);
        let mut output = String::new();
        sample.render(&mut output);
        assert!(output.contains("graph_process_resident_memory_bytes 100"));
        assert!(output.contains("graph_process_open_file_descriptors 7"));
        assert!(output.contains("graph_memory_sampler_source_failures_total{source=\"cgroup\"} 1"));
        assert!(output.contains("graph_memory_sampler_source_available{source=\"cgroup\"} 0"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_cgroup_read_discards_the_path_for_next_pass_rediscovery() {
        let dir = tempfile::tempdir().unwrap();
        let collected = collect_sample(Some(dir.path().to_path_buf()));
        assert!(collected.cgroup.is_none());
        assert!(collected.cgroup_directory.is_none());
    }

    #[tokio::test]
    async fn dropping_sampler_aborts_background_task() {
        let sampler = MemorySampler::start();
        let abort = sampler.task.abort_handle();
        drop(sampler);
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sampling_work_runs_off_the_runtime_thread() {
        let runtime_thread = std::thread::current().id();
        let sampling_thread = run_blocking(|| std::thread::current().id()).await.unwrap();
        assert_ne!(sampling_thread, runtime_thread);
    }
}
