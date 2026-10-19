//! Opt-in, read-only profiling against a frozen object inventory from a real scope.
use super::*;
use futures::stream::BoxStream;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::AtomicU64;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct CapturedObject {
    key: String,
    size: u64,
    modified: String,
    etag: Option<String>,
    version: Option<String>,
}

impl CapturedObject {
    fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(self.key.clone()),
            size: self.size,
            last_modified: self.modified.parse().expect("captured object timestamp"),
            e_tag: self.etag.clone(),
            version: self.version.clone(),
        }
    }
}

#[derive(Debug)]
struct FrozenStore {
    remote: Arc<dyn ObjectStore>,
    objects: BTreeMap<Path, ObjectMeta>,
    gets: AtomicU64,
    bytes: AtomicU64,
}

impl std::fmt::Display for FrozenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrozenReadOnlyReplay")
    }
}

fn denied() -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "FrozenReadOnlyReplay",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "replay store forbids all mutations",
        )),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FrozenStore {
    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        Err(denied())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        Err(denied())
    }
    async fn get_opts(
        &self,
        path: &Path,
        mut opts: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        let Some(meta) = self.objects.get(path) else {
            return Err(slatedb::object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
            });
        };
        opts.if_match = meta.e_tag.clone();
        self.gets.fetch_add(1, Ordering::Relaxed);
        let result = self.remote.get_opts(path, opts).await?;
        self.bytes
            .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        Ok(result)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        let objects = self
            .objects
            .values()
            .filter(|meta| prefix.is_none_or(|prefix| meta.location.prefix_matches(prefix)))
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        stream::iter(objects).boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        let root = prefix.map(|p| format!("{p}/")).unwrap_or_default();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
            extensions: Default::default(),
        };
        let mut common = BTreeSet::new();
        for meta in self.objects.values() {
            let Some(suffix) = meta.location.as_ref().strip_prefix(&root) else {
                continue;
            };
            if let Some((part, _)) = suffix.split_once('/') {
                common.insert(Path::from(format!("{root}{part}")));
            } else {
                result.objects.push(meta.clone());
            }
        }
        result.common_prefixes = common.into_iter().collect();
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        paths.map(|_| Err(denied())).boxed()
    }
    async fn copy_opts(
        &self,
        _: &Path,
        _: &Path,
        _: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        Err(denied())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Fixture {
    path: String,
    epoch: u64,
    vertices: BTreeSet<VertexId>,
    detach: BTreeSet<VertexId>,
}

async fn choose_fixture(shard: &GraphShard, path: &str, cell: &str) -> Fixture {
    let epoch = shard.current_epoch(cell).await.unwrap();
    let snapshot = shard.snapshot_at(cell, epoch).await.unwrap();
    let options = ScanOptions::default();
    let prefix = keys::vertex_label_prefix(cell, "Source");
    let mut sources = snapshot
        .storage_snapshot
        .scan_prefix_with_options(prefix.as_bytes(), .., &options)
        .await
        .unwrap();
    let mut detach = BTreeSet::new();
    for _ in 0..50 {
        let Some(source) = sources.next().await.unwrap() else {
            break;
        };
        let id = decode_u64("source label", &source.value).unwrap();
        let prefix = keys::in_prefix(cell, "HAS_CHUNK", id);
        let mut chunks = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &options)
            .await
            .unwrap();
        let mut candidate = BTreeSet::from([id]);
        while candidate.len() < 4 {
            let Some(kv) = chunks.next().await.unwrap() else {
                break;
            };
            let edge = decode_edge_record(&String::from_utf8_lossy(&kv.key), &kv.value).unwrap();
            candidate.insert(edge.src);
        }
        if candidate.len() > 1 {
            detach = candidate;
            break;
        }
    }
    assert!(
        detach.len() > 1,
        "no source/chunk fixture found in first 50 sources"
    );
    let mut vertices = detach.clone();
    for vertex in &detach {
        let prefix = keys::in_prefix(cell, "PRESENT_IN", *vertex);
        let mut entities = snapshot
            .storage_snapshot
            .scan_prefix_with_options(prefix.as_bytes(), .., &options)
            .await
            .unwrap();
        while vertices.len() < 18 {
            let Some(kv) = entities.next().await.unwrap() else {
                break;
            };
            let edge = decode_edge_record(&String::from_utf8_lossy(&kv.key), &kv.value).unwrap();
            vertices.insert(edge.src);
        }
    }
    Fixture {
        path: path.into(),
        epoch,
        vertices,
        detach,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit read-only S3 replay environment and frozen inventory"]
async fn real_scope_incident_replay() {
    let path = std::env::var("HYDRADB_REPLAY_PATH").expect("explicit source cell path");
    let inventory = std::env::var("HYDRADB_REPLAY_INVENTORY").expect("inventory filename");
    let fixture_file = std::env::var("HYDRADB_REPLAY_FIXTURE").expect("fixture filename");
    let remote = crate::object_store_from_env(None).unwrap();
    let objects: Vec<CapturedObject> = if std::path::Path::new(&inventory).exists() {
        serde_json::from_slice(&std::fs::read(&inventory).unwrap()).unwrap()
    } else {
        let mut objects = Vec::new();
        // Capture the manifest first and WAL last. Never discover future WALs
        // on subsequent runs, and fail rather than replace a GC'd/changed object.
        for folder in ["manifest", "compacted", "wal"] {
            let prefix = Path::from(format!("{path}/{folder}"));
            let mut listing = remote.list(Some(&prefix));
            while let Some(meta) = listing.try_next().await.unwrap() {
                assert!(objects.len() < 100_000, "replay inventory bound exceeded");
                objects.push(CapturedObject {
                    key: meta.location.to_string(),
                    size: meta.size,
                    modified: meta.last_modified.to_rfc3339(),
                    etag: meta.e_tag,
                    version: meta.version,
                });
            }
        }
        std::fs::write(&inventory, serde_json::to_vec(&objects).unwrap()).unwrap();
        objects
    };
    assert!(objects
        .iter()
        .all(|o| o.key.starts_with(&format!("{path}/"))));
    let options = GraphOpenOptions {
        reader_mode: crate::GraphReaderMode::FollowLatest,
        reader_manifest_poll_interval: std::time::Duration::from_secs(86400),
        cache: GraphCacheConfig {
            slatedb_cache_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    let make_store = || {
        Arc::new(FrozenStore {
            remote: Arc::clone(&remote),
            objects: objects
                .iter()
                .map(|o| (Path::from(o.key.clone()), o.meta()))
                .collect(),
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    };
    let fixture: Fixture = if std::path::Path::new(&fixture_file).exists() {
        serde_json::from_slice(&std::fs::read(&fixture_file).unwrap()).unwrap()
    } else {
        eprintln!("real-scope-fixture opening reader");
        let shard = tokio::time::timeout(
            std::time::Duration::from_secs(180),
            GraphShard::open_with_options(path.clone(), make_store(), options.clone()),
        )
        .await
        .expect("fixture reader open exceeded 180 seconds")
        .unwrap();
        let fixture = tokio::time::timeout(
            std::time::Duration::from_secs(180),
            choose_fixture(&shard, &path, "cell-0"),
        )
        .await
        .expect("fixture selection exceeded 180 seconds");
        tokio::time::timeout(std::time::Duration::from_secs(30), shard.close())
            .await
            .expect("fixture reader close exceeded 30 seconds")
            .unwrap();
        std::fs::write(&fixture_file, serde_json::to_vec(&fixture).unwrap()).unwrap();
        fixture
    };
    assert_eq!(fixture.path, path);
    let guard_shard = GraphShard::open_standalone_writer(
        "replay-guard",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let guard = guard_shard
        .acquire_local_write_guard("cell-0", "readonly_replay")
        .await
        .unwrap();
    for trial in 0..1 {
        let store = make_store();
        let opened = std::time::Instant::now();
        eprintln!("real-scope-open trial={trial} starting");
        let shard = tokio::time::timeout(
            std::time::Duration::from_secs(180),
            GraphShard::open_with_options(path.clone(), store.clone(), options.clone()),
        )
        .await
        .expect("frozen reader open exceeded 180 seconds")
        .unwrap();
        eprintln!(
            "real-scope-open trial={trial} elapsed_ms={} gets={}",
            opened.elapsed().as_millis(),
            store.gets.load(Ordering::Relaxed)
        );
        assert_eq!(
            shard.current_epoch("cell-0").await.unwrap(),
            fixture.epoch,
            "frozen replay sequence changed"
        );
        let snapshot = shard.snapshot_at("cell-0", fixture.epoch).await.unwrap();
        let mut expected = None;
        for (temperature, admit) in [
            ("non-admitting-cold", false),
            ("non-admitting-repeat", false),
            ("admitting-first", true),
            ("admitting-repeat", true),
        ] {
            eprintln!("real-scope-discovery trial={trial} temperature={temperature} starting");
            let gets = store.gets.load(Ordering::Relaxed);
            let bytes = store.bytes.load(Ordering::Relaxed);
            let started = std::time::Instant::now();
            let edges = tokio::time::timeout(
                std::time::Duration::from_secs(180),
                shard.indexed_vertex_delete_discovery(
                    &snapshot,
                    &fixture.vertices,
                    Some(&fixture.detach),
                    Some(&guard),
                    &ScanOptions::default().with_cache_blocks(admit),
                ),
            )
            .await
            .unwrap()
            .unwrap()
            .edges;
            if let Some(expected) = &expected {
                assert_eq!(&edges, expected, "cache admission changed incident edges");
            } else {
                expected = Some(edges.clone());
            }
            let digest = edges.iter().fold(0_u64, |sum, edge| {
                sum.wrapping_add(edge.src).wrapping_add(edge.dst)
            });
            eprintln!("real-scope-discovery trial={trial} temperature={temperature} epoch={} vertices={} detach={} elapsed_ms={} edges={} digest={digest} s3_gets={} s3_bytes={}", fixture.epoch, fixture.vertices.len(), fixture.detach.len(), started.elapsed().as_millis(), edges.len(), store.gets.load(Ordering::Relaxed)-gets, store.bytes.load(Ordering::Relaxed)-bytes);
        }
        drop(snapshot);
        eprintln!("real-scope-close trial={trial} starting");
        tokio::time::timeout(std::time::Duration::from_secs(30), shard.close())
            .await
            .expect("replay reader close exceeded 30 seconds")
            .unwrap();
    }
    guard.release().await.unwrap();
    guard_shard.close().await.unwrap();
}

#[tokio::test]
async fn incident_discovery_reuses_immutable_blocks_within_cache_budget() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = GraphShard::open_standalone_writer_with_options(
        "incident-block-reuse",
        store.clone(),
        GraphOpenOptions {
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for index in 0..8 {
        shard
            .write_edges_batch(
                "cell-0",
                &format!("TYPE{index}"),
                [(1, 2), (3, 4)],
                &format!("seed-{index}"),
            )
            .await
            .unwrap();
    }
    shard
        .db
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);
    let shard = GraphShard::open_standalone_writer_with_options(
        "incident-block-reuse",
        store.clone(),
        GraphOpenOptions {
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let guard = shard
        .acquire_local_write_guard("cell-0", "cache-reuse-test")
        .await
        .unwrap();
    let epoch = shard.current_epoch("cell-0").await.unwrap();
    let vertices = BTreeSet::from([1, 2, 3, 4]);
    let before = store.reads();
    let first = shard
        .indexed_vertex_delete_incidents_at("cell-0", &vertices, None, epoch, &guard)
        .await
        .unwrap();
    let cold_reads = store.reads() - before;
    let before = store.reads();
    let second = shard
        .indexed_vertex_delete_incidents_at("cell-0", &vertices, None, epoch, &guard)
        .await
        .unwrap();
    let repeat_reads = store.reads() - before;
    assert_eq!(first, second);
    assert_eq!(first.len(), 16);
    guard.release().await.unwrap();
    shard.close().await.unwrap();
    assert!(cold_reads > 0, "fixture must exercise storage reads");
    assert_eq!(
        repeat_reads, 0,
        "repeated discovery reread {repeat_reads} immutable objects after {cold_reads} cold reads"
    );
}

#[tokio::test]
async fn prepared_small_delete_warms_apply_reads_before_writer_admission() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let path = "prepared-delete-apply-cache";
    let shard = GraphShard::open_standalone_writer_with_options(
        path,
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-0",
            (1..=3).map(|id| (id, VertexMetadata::default().with_label("Entity"))),
        )
        .await
        .unwrap();
    shard
        .write_edges_batch("cell-0", "EDGE", [(2, 1), (3, 1)], "incoming")
        .await
        .unwrap();
    shard
        .db
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);
    let shard = GraphShard::open_standalone_writer_with_options(
        path,
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deletions = vec![(1, "cleanup".into(), true, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::Detach;
    let before = store.compacted_reads();
    let prepared = shard
        .prepare_vertex_delete("cell-0", &deletions, mode)
        .await
        .unwrap();
    assert!(prepared.is_some());
    assert!(
        store.compacted_reads() > before,
        "preparation must read the cold SST"
    );
    // Background GC/manifest polling is independent of these writer guards.
    // Count immutable SST reads to verify the warmed apply working set.
    let before = store.compacted_reads();
    let lock = shard
        .acquire_local_write_guard_no_fence("cell-0", mode.operation())
        .await
        .unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked(
            "cell-0",
            &deletions,
            mode,
            mode.operation(),
            &lock,
            prepared,
        )
        .await
        .unwrap();
    let guarded_reads = store.compacted_reads() - before;
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    assert_eq!(result[0].incident_edges_deleted, 2);
    assert_eq!(
        guarded_reads, 0,
        "cold apply reads still held the writer guard"
    );
    assert!(shard
        .read_remote(&keys::vertex("cell-0", 1))
        .await
        .unwrap()
        .is_none());
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_revalidates_new_edge_types_but_allows_metadata_writes() {
    let shard = GraphShard::open_standalone_writer(
        "prepared-delete-revalidation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let deletions = vec![(1, "cleanup".into(), false, VertexMetadata::default())];
    let mode = VertexDeleteBatchMode::IsolatedOnly;
    let prepared = shard
        .prepare_vertex_delete("cell-0", &deletions, mode)
        .await
        .unwrap();
    assert!(prepared.as_ref().unwrap().discovery.edges.is_empty());
    shard
        .write_edges_batch("cell-0", "NEW_TYPE", [(2, 1)], "concurrent-edge")
        .await
        .unwrap();
    let lock = shard
        .acquire_local_write_guard("cell-0", "test")
        .await
        .unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked(
            "cell-0",
            &deletions,
            mode,
            mode.operation(),
            &lock,
            prepared,
        )
        .await;
    lock.release().await.unwrap();
    assert!(matches!(
        result,
        Err(GraphError::ConditionalWriteConflict { .. })
    ));
    let retained = shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions.clone(), mode)
        .await
        .unwrap();
    assert!(!retained[0].vertex_deleted);

    shard
        .set_vertex_metadata("cell-0", 3, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let deletions = vec![(3, "metadata-race".into(), false, VertexMetadata::default())];
    let prepared = shard
        .prepare_vertex_delete("cell-0", &deletions, mode)
        .await
        .unwrap();
    shard
        .set_vertex_metadata("cell-0", 3, VertexMetadata::default().with_label("Changed"))
        .await
        .unwrap();
    let lock = shard
        .acquire_local_write_guard("cell-0", "test")
        .await
        .unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked(
            "cell-0",
            &deletions,
            mode,
            mode.operation(),
            &lock,
            prepared,
        )
        .await
        .unwrap();
    lock.release().await.unwrap();
    assert!(result[0].vertex_deleted);
    // The transaction must remove the newly written label, not stale metadata.
    let snapshot = shard.snapshot("cell-0").await.unwrap();
    assert!(snapshot
        .storage_snapshot
        .get_with_options(
            keys::vertex("cell-0", 3).as_bytes(),
            &slatedb::config::ReadOptions::default()
        )
        .await
        .unwrap()
        .is_none());
    assert!(
        snapshot
            .storage_snapshot
            .get_with_options(
                keys::vertex_label("cell-0", "Changed", 3).as_bytes(),
                &slatedb::config::ReadOptions::default(),
            )
            .await
            .unwrap()
            .is_none(),
        "deletion retained the concurrently written label index"
    );
    drop(snapshot);
    shard.close().await.unwrap();
}

/// Fill the old process-wide preparation limit with fallback requests waiting
/// on one graph's writer, then prove another graph can still prepare and delete.
async fn assert_delete_preparation_fallback_does_not_block_other_graphs(
    shard: &GraphShard,
    deletions: Vec<(VertexId, String, bool, VertexMetadata)>,
) {
    let mode = VertexDeleteBatchMode::Detach;
    assert!(shard
        .prepare_vertex_delete("cell-0", &deletions, mode)
        .await
        .unwrap()
        .is_none());
    let unrelated = GraphShard::open_standalone_writer(
        "unrelated-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    unrelated
        .set_vertex_metadata("cell-1", 1, VertexMetadata::default().with_label("Entity"))
        .await
        .unwrap();
    let held = shard
        .acquire_graph_write_permit("test-held-writer")
        .await
        .unwrap();
    let waits_before = shard
        .operation_metrics
        .backpressure_waits
        .load(Ordering::Relaxed);
    let blocked = futures::future::join_all((0..4).map(|_| {
        shard.delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions.clone(), mode)
    }));
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            _ = blocked => panic!("fallback deletes passed the held writer permit"),
            result = async {
                // A writer wait proves each request has finished preparation;
                // no sleep or global available-permit count is needed.
                while shard.operation_metrics.backpressure_waits.load(Ordering::Relaxed)
                    < waits_before + 4
                {
                    tokio::task::yield_now().await;
                }
                unrelated.detach_delete_vertex("cell-1", 1, "unrelated-cleanup").await
            } => result,
        }
    })
    .await;
    // Dropping the blocked futures cancels them before they can mutate. Release
    // all resources even when running this regression against the broken code.
    drop(held);
    unrelated.close().await.unwrap();
    assert!(
        result
            .expect(
                "fallback deletes monopolized global preparation slots while waiting for a writer"
            )
            .unwrap()
            .vertex_deleted
    );
}

#[tokio::test]
async fn delete_preparation_oversized_batches_do_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "oversized-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let deletions = (0..=DELETE_PREPARATION_MAX_VERTICES)
        .map(|vertex| {
            (
                vertex as VertexId,
                format!("cleanup-{vertex}"),
                true,
                VertexMetadata::default(),
            )
        })
        .collect();
    assert_delete_preparation_fallback_does_not_block_other_graphs(&shard, deletions).await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_byte_limit_fallback_does_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "byte-limit-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    // Long but valid type names reach the actual 8 MiB retained-edge limit with
    // a small fixture. Seed the incident index directly; requests are cancelled
    // at writer admission, so this fixture never enters a delete transaction.
    let edge_type = "T".repeat(4096);
    let edge_count = DELETE_PREPARATION_BYTES / (edge_type.len() as u64 + 128) + 1;
    let mut batch = WriteBatch::new();
    batch.put(
        keys::matrix_dirty("cell-0", &edge_type).as_bytes(),
        encode_u64(1),
    );
    for dst in 2..edge_count + 2 {
        batch.put(
            keys::out_edge("cell-0", &edge_type, 1, dst).as_bytes(),
            b"graph-edge\n".as_slice(),
        );
    }
    shard.write_strict_for_test(batch).await.unwrap();
    assert_delete_preparation_fallback_does_not_block_other_graphs(
        &shard,
        vec![(1, "cleanup".into(), true, VertexMetadata::default())],
    )
    .await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_replays_do_not_block_other_graphs() {
    let shard = GraphShard::open_standalone_writer(
        "replayed-delete-preparation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard
        .detach_delete_vertex("cell-0", 1, "cleanup")
        .await
        .unwrap();
    assert_delete_preparation_fallback_does_not_block_other_graphs(
        &shard,
        vec![(1, "cleanup".into(), true, VertexMetadata::default())],
    )
    .await;
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_rechecks_partially_replayed_batches() {
    let shard = GraphShard::open_standalone_writer(
        "prepared-delete-replay",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-0",
            [
                (1, VertexMetadata::default().with_label("Source")),
                (2, VertexMetadata::default().with_label("Entity")),
            ],
        )
        .await
        .unwrap();
    let deletions = vec![
        (1, "source".into(), true, VertexMetadata::default()),
        (2, "entity".into(), false, VertexMetadata::default()),
    ];
    let mode = VertexDeleteBatchMode::DetachAndIsolated;
    let prepared = shard
        .prepare_vertex_delete("cell-0", &deletions, mode)
        .await
        .unwrap();
    shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", vec![deletions[0].clone()], mode)
        .await
        .unwrap();
    let lock = shard
        .acquire_local_write_guard("cell-0", "test")
        .await
        .unwrap();
    let result = shard
        .delete_vertex_mutations_batch_txn_locked(
            "cell-0",
            &deletions,
            mode,
            mode.operation(),
            &lock,
            prepared,
        )
        .await;
    lock.release().await.unwrap();
    assert!(matches!(
        result,
        Err(GraphError::ConditionalWriteConflict { .. })
    ));
    shard
        .set_vertex_metadata(
            "cell-0",
            1,
            VertexMetadata::default().with_label("NewSource"),
        )
        .await
        .unwrap();
    shard
        .write_edges_batch("cell-0", "NEW_EDGE", [(2, 1)], "recreated")
        .await
        .unwrap();
    let retried = shard
        .delete_vertex_mutation_requests_batch_with_mode("cell-0", deletions, mode)
        .await
        .unwrap();
    assert!(
        retried[0].vertex_deleted,
        "source returns its stored replay result"
    );
    assert!(
        !retried[1].vertex_deleted,
        "the replayed source must not detach a new relationship"
    );
    assert_eq!(shard.out_degree("cell-0", "NEW_EDGE", 2).await.unwrap(), 1);
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_uses_retained_xlog_for_unrelated_topology_changes() {
    let shard = GraphShard::open_standalone_writer(
        "prepared-delete-xlog",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata_batch(
            "cell-0",
            (1..=3).map(|id| (id, VertexMetadata::default().with_label("Entity"))),
        )
        .await
        .unwrap();
    shard
        .write_edges_batch("cell-0", "TYPE", [(900, 901)], "seed")
        .await
        .unwrap();
    let mode = VertexDeleteBatchMode::IsolatedOnly;
    for vertex in 1..=3 {
        let deletions = vec![(
            vertex,
            format!("cleanup-{vertex}"),
            false,
            VertexMetadata::default(),
        )];
        let prepared = shard
            .prepare_vertex_delete("cell-0", &deletions, mode)
            .await
            .unwrap();
        let destination = if vertex == 2 { vertex } else { 910 + vertex };
        shard
            .write_edges_batch(
                "cell-0",
                "TYPE",
                [(900, destination)],
                &format!("race-{vertex}"),
            )
            .await
            .unwrap();
        if vertex == 3 {
            // A collected suffix cannot be used to prove that no target changed.
            shard
                .db
                .writer()
                .unwrap()
                .put(
                    keys::xlog_low_water("cell-0", "TYPE").as_bytes(),
                    encode_u64(u64::MAX),
                )
                .await
                .unwrap();
        }
        let lock = shard
            .acquire_local_write_guard("cell-0", "test")
            .await
            .unwrap();
        let result = shard
            .delete_vertex_mutations_batch_txn_locked(
                "cell-0",
                &deletions,
                mode,
                mode.operation(),
                &lock,
                prepared,
            )
            .await;
        lock.release().await.unwrap();
        if vertex == 1 {
            assert!(
                result.unwrap()[0].vertex_deleted,
                "unrelated edge writes should not restart cleanup"
            );
        } else {
            assert!(
                matches!(result, Err(GraphError::ConditionalWriteConflict { .. })),
                "affected topology or a coverage gap must reject preparation"
            );
        }
    }
    shard.close().await.unwrap();
}

#[tokio::test]
async fn delete_preparation_validation_overlaps_types_and_reuses_bounded_xlog_blocks() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = GraphShard::open_standalone_writer_with_options(
        "prepared-validation-block-reuse",
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    shard
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default())
        .await
        .unwrap();
    for i in 0..8 {
        shard
            .write_edges_batch(
                "cell-0",
                &format!("TYPE{i}"),
                [(900, 901)],
                &format!("seed-{i}"),
            )
            .await
            .unwrap();
    }
    let prepared = shard
        .prepare_vertex_delete(
            "cell-0",
            &[(1, "delete-1".into(), false, VertexMetadata::default())],
            VertexDeleteBatchMode::IsolatedOnly,
        )
        .await
        .unwrap()
        .unwrap();
    // Enough unrelated changes to occupy multiple data blocks, while remaining
    // below the validation scan cap. They must not invalidate this deletion.
    for edge_type in 0..8 {
        shard
            .write_edges_batch(
                "cell-0",
                &format!("TYPE{edge_type}"),
                (0..250).map(|i| (10_000 + i, 20_000 + i)),
                &format!("unrelated-{edge_type}"),
            )
            .await
            .unwrap();
    }
    shard
        .db
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    shard.close().await.unwrap();
    drop(shard);
    let mut shard = GraphShard::open_standalone_writer_with_options(
        "prepared-validation-block-reuse",
        store.clone(),
        GraphOpenOptions {
            reader_manifest_poll_interval: std::time::Duration::from_secs(3600),
            cache: GraphCacheConfig {
                slatedb_cache_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let txn = shard
        .db
        .writer()
        .unwrap()
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    store.delay_reads_and_reset_peak(10);
    let validation_start = std::time::Instant::now();
    let before = store.compacted_reads();
    assert!(shard
        .vertex_delete_preparation_is_current(&txn, "cell-0", &prepared)
        .await
        .unwrap());
    let cold_reads = store.compacted_reads() - before;
    let peak_reads = store.peak_reads();
    let cold_validation_elapsed = validation_start.elapsed();
    store.delay_reads_and_reset_peak(0);
    let before = store.compacted_reads();
    assert!(shard
        .vertex_delete_preparation_is_current(&txn, "cell-0", &prepared)
        .await
        .unwrap());
    let repeat_reads = store.compacted_reads() - before;
    // Each history has fewer than 1,000 entries, but their shared total is
    // 2,000. The parallel validator must enforce a single allowance.
    shard.limits.max_query_scan_edges = 1_000;
    assert!(!shard
        .vertex_delete_preparation_is_current(&txn, "cell-0", &prepared)
        .await
        .unwrap());
    drop(txn);
    assert!(
        peak_reads > 1 && peak_reads <= VERTEX_DELETE_READ_CONCURRENCY as u64,
        "cold history reads did not overlap within their bound: {peak_reads}"
    );
    shard.close().await.unwrap();
    eprintln!("delete validation object reads: cold={cold_reads}, repeated={repeat_reads}, peak={peak_reads}, elapsed={cold_validation_elapsed:?}");
    assert!(cold_reads > 0, "validation must read flushed data blocks");
    assert_eq!(
        repeat_reads, 0,
        "validation repeated {repeat_reads} object reads after {cold_reads} cold reads"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_storage_wait_does_not_hold_writer_gate() {
    let store = crate::tests::ReadCountingObjectStore::new();
    let shard = Arc::new(
        GraphShard::open_standalone_writer_with_options(
            "cleanup-no-writer-blocking",
            store.clone(),
            GraphOpenOptions {
                cache: GraphCacheConfig {
                    slatedb_cache_bytes: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    shard
        .set_vertex_metadata("cell-0", 1, VertexMetadata::default().with_label("Source"))
        .await
        .unwrap();
    shard
        .set_vertex_metadata("cell-0", 2, VertexMetadata::default().with_label("Chunk"))
        .await
        .unwrap();
    shard
        .write_edges_batch("cell-0", "HAS_CHUNK", [(2, 1)], "seed")
        .await
        .unwrap();
    shard
        .db
        .writer()
        .unwrap()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    let (started, release) = store.pause_next_get();
    let deleting = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move {
            shard
                .delete_vertices_and_isolated_candidates_batch(
                    "cell-0",
                    vec![
                        (1, "cleanup".into(), true, VertexMetadata::default()),
                        (2, "candidate".into(), false, VertexMetadata::default()),
                    ],
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    let before = std::time::Instant::now();
    let changed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        shard.set_vertex_metadata(
            "cell-0",
            9000,
            VertexMetadata::default().with_label("Concurrent"),
        ),
    )
    .await;
    let latency = before.elapsed();
    let _ = release.send(());
    let deleted = deleting.await.unwrap().unwrap();
    assert!(deleted.iter().all(|result| result.vertex_deleted));
    shard.close().await.unwrap();
    changed
        .expect("cleanup storage I/O held the graph writer gate")
        .unwrap();
    eprintln!(
        "write during paused cleanup completed in {} us",
        latency.as_micros()
    );
}
