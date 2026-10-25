use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use falkor_dump_import::{
    build_snapshot_spill_selected, load_spilled_scope, ExcludedGraph, ExternalKvSorter,
    GraphSelection, OrphanEdge, RelationshipAnomaly, ResolvedScope, SnapshotGraph,
};
use falkordb_rdb::PropertyValue;
use futures::future::try_join_all;
use hydradb::{
    encode_scope_namespace_id, object_store_from_env, EdgeMetadata, GraphId, GraphLimits,
    GraphOpenOptions, GraphScope, GraphShard, NamespaceId, NamespacePath,
    ObjectStoreGraphScopeDirectory, OfflineGraphImageEncoder, QueryFloat, QueryStatsDirection,
    RelationshipMutation, SparseKernelBackend, VertexMetadata, VertexPropertyValue,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};

// v2 switched offline scopes to the canonical client namespace encoding. Keep
// its receipts separate so --resume can never accept an image written at the
// obsolete raw-identifier path.
const COMPLETION_MAGIC: &str = "hydradb-falkor-direct-import-v2";
// Older binaries ignore unknown JSON fields. A distinct run marker prevents
// them from treating an allowlisted image as a complete-snapshot import.
const INCLUDED_RUN_MAGIC: &str = "hydradb-falkor-direct-import-included-v1";

#[derive(Debug)]
struct Config {
    snapshot_root: PathBuf,
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    cell_id: String,
    workers: usize,
    scope_concurrency: usize,
    batch_size: usize,
    resume: bool,
    defer_deep_verification: bool,
    env_file: Option<String>,
    quarantine_dir: Option<PathBuf>,
    scratch_dir: Option<PathBuf>,
    selection: GraphSelection,
}

#[derive(Debug, Serialize)]
struct ScopeImportSummary {
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
    edge_types: usize,
    batches: usize,
    resumed: bool,
    deep_verified: bool,
    elapsed_milliseconds: u128,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ScopeCompletion {
    magic: String,
    snapshot_id: String,
    manifest_sha256: String,
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
    edge_types: usize,
    batches: usize,
    #[serde(default = "legacy_completion_was_deep_verified")]
    deep_verified: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    included_graphs: Vec<String>,
}

const fn legacy_completion_was_deep_verified() -> bool {
    true
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ImportRunMarker {
    magic: String,
    snapshot_id: String,
    manifest_sha256: String,
    object_count: usize,
    dump_bytes: u64,
    base_path: String,
    root_namespace: String,
    graph_id: String,
    cell_id: String,
    #[serde(default)]
    excluded_graphs: Vec<ExcludedGraph>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    included_graphs: Vec<SnapshotGraph>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ScopeAttempt {
    magic: String,
    manifest_sha256: String,
    scope_sha256: String,
    scope_content_sha256: String,
    vertices: usize,
    relationships: usize,
}

#[derive(Serialize)]
struct QuarantineReport<'a> {
    orphan_edge_records: &'a [OrphanEdge],
    relationship_anomalies: &'a [RelationshipAnomaly],
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("falkor-dump-import: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::parse()?);
    let planning = Instant::now();
    let scratch = if let Some(parent) = &config.scratch_dir {
        std::fs::create_dir_all(parent)?;
        tempfile::Builder::new()
            .prefix("falkor-direct-import-")
            .tempdir_in(parent)?
    } else {
        tempfile::Builder::new()
            .prefix("falkor-direct-import-")
            .tempdir()?
    };
    let plan = build_snapshot_spill_selected(
        &config.snapshot_root,
        config.workers,
        scratch.path(),
        &config.selection,
    )?;
    if (plan.orphan_edges > 0 || plan.relationship_anomalies > 0) && config.quarantine_dir.is_none()
    {
        return Err(format!(
            "snapshot contains {} unreachable edge records and {} relationship anomalies; pass --quarantine-dir to preserve them before importing",
            plan.orphan_edges, plan.relationship_anomalies
        )
        .into());
    }
    eprintln!(
        "direct_import_plan snapshot={} manifest_sha256={} objects={} selected_objects={} selected_dump_bytes={} included_graphs={} excluded_graphs={} scopes={} raw_scope_fragments={} raw_vertices={} raw_relationships={} orphan_edges={} relationship_anomalies={} dump_bytes={} elapsed_ms={}",
        plan.snapshot_id,
        plan.manifest_sha256,
        plan.object_count,
        plan.selected_object_count,
        plan.selected_dump_bytes,
        serde_json::to_string(&plan.included_graphs)?,
        serde_json::to_string(&plan.excluded_graphs)?,
        plan.scopes.len(),
        plan.raw_scopes,
        plan.raw_vertices,
        plan.raw_relationships,
        plan.orphan_edges,
        plan.relationship_anomalies,
        plan.dump_bytes,
        planning.elapsed().as_millis()
    );

    let object_store = object_store_from_env(config.env_file.clone())?;
    let directory = Arc::new(ObjectStoreGraphScopeDirectory::new(
        config.base_path.clone(),
        config.root_namespace.clone(),
        config.graph_id.clone(),
        Arc::clone(&object_store),
    ));
    ensure_run_marker(&object_store, &config, &plan).await?;
    let mut tasks = tokio::task::JoinSet::new();
    let mut summaries = Vec::new();
    let snapshot_id = plan.snapshot_id.clone();
    let manifest_sha256 = plan.manifest_sha256.clone();
    for spilled in plan.scopes {
        if tasks.len() >= config.scope_concurrency {
            let result = tasks
                .join_next()
                .await
                .expect("a full scope worker set is not empty");
            summaries.push(result.map_err(|error| format!("scope worker panicked: {error}"))??);
        }
        let scope = tokio::task::spawn_blocking(move || load_spilled_scope(&spilled)).await??;
        if !scope.orphan_edges.is_empty() || !scope.relationship_anomalies.is_empty() {
            let quarantine = config
                .quarantine_dir
                .as_ref()
                .expect("source anomaly count was checked before import");
            write_quarantine(quarantine, std::slice::from_ref(&scope))?;
        }
        let config = Arc::clone(&config);
        let object_store = Arc::clone(&object_store);
        let directory = Arc::clone(&directory);
        let snapshot_id = snapshot_id.clone();
        let manifest_sha256 = manifest_sha256.clone();
        tasks.spawn(async move {
            import_scope(
                &config,
                &snapshot_id,
                &manifest_sha256,
                scope,
                object_store,
                directory,
            )
            .await
            .map_err(|error| error.to_string())
        });
    }
    while let Some(result) = tasks.join_next().await {
        summaries.push(result.map_err(|error| format!("scope worker panicked: {error}"))??);
    }
    summaries.sort_by(|left, right| left.scope_sha256.cmp(&right.scope_sha256));
    let total_vertices: usize = summaries.iter().map(|summary| summary.vertices).sum();
    let total_relationships: usize = summaries.iter().map(|summary| summary.relationships).sum();
    let resumed = summaries.iter().filter(|summary| summary.resumed).count();
    let deep_verified = summaries
        .iter()
        .filter(|summary| summary.deep_verified)
        .count();
    eprintln!(
        "direct_import_complete scopes={} resumed={} deep_verified={} verification_deferred={} vertices={} relationships={} elapsed_ms={}",
        summaries.len(),
        resumed,
        deep_verified,
        summaries.len().saturating_sub(deep_verified),
        total_vertices,
        total_relationships,
        planning.elapsed().as_millis()
    );
    println!("{}", serde_json::to_string_pretty(&summaries)?);
    Ok(())
}

async fn import_scope(
    config: &Config,
    snapshot_id: &str,
    manifest_sha256: &str,
    scope: ResolvedScope,
    object_store: Arc<dyn ObjectStore>,
    directory: Arc<ObjectStoreGraphScopeDirectory>,
) -> Result<ScopeImportSummary, Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let scope_sha256 = scope_hash(&scope);
    let scope_content_sha256 = scope_content_hash(&scope)?;
    let completion_path = completion_path(config, manifest_sha256, &scope_sha256);
    let mut promoting_deferred_completion = false;
    if config.resume {
        if let Some(completion) =
            read_json::<ScopeCompletion>(&object_store, &completion_path).await?
        {
            if completion.included_graphs != config.selection.included_names() {
                return Err(format!("completion receipt graph selection does not match snapshot scope {scope_sha256}").into());
            }
            validate_completion(
                &completion,
                snapshot_id,
                manifest_sha256,
                &scope_sha256,
                &scope_content_sha256,
                &scope,
                false,
            )?;
            if config.defer_deep_verification || completion.deep_verified {
                return Ok(ScopeImportSummary {
                    scope_sha256,
                    scope_content_sha256,
                    vertices: completion.vertices,
                    relationships: completion.relationships,
                    edge_types: completion.edge_types,
                    batches: completion.batches,
                    resumed: true,
                    deep_verified: completion.deep_verified,
                    elapsed_milliseconds: started.elapsed().as_millis(),
                });
            }
            promoting_deferred_completion = true;
        }
    }
    let graph_scope = storage_scope(config, &scope)?;
    let shard_path = format!(
        "{}/{}",
        graph_scope.scoped_store_path(&config.base_path),
        config.cell_id
    );
    let destination_has_data = prefix_has_data(&object_store, &shard_path).await?;
    ensure_scope_attempt(
        &object_store,
        config,
        manifest_sha256,
        &scope_sha256,
        &scope_content_sha256,
        &scope,
        destination_has_data,
    )
    .await?;
    let manifest_path = format!("{shard_path}/manifest");
    let image_exists = prefix_has_data(&object_store, &manifest_path).await?;
    if image_exists {
        if !config.resume {
            return Err(format!(
                "destination scope already has a SlateDB manifest: {}",
                scope.scope
            )
            .into());
        }
        eprintln!(
            "direct_image_resume scope={} elapsed_ms={}",
            scope.scope,
            started.elapsed().as_millis()
        );
    } else {
        let image =
            build_direct_image(config, &scope, &shard_path, Arc::clone(&object_store)).await?;
        eprintln!(
            "direct_image_published scope={} entries={} ssts={} elapsed_ms={}",
            scope.scope,
            image.entries,
            image.ssts,
            started.elapsed().as_millis()
        );
    }
    let mut options = GraphOpenOptions::default();
    options.limits = offline_import_limits(config, &scope)?;
    options.backpressure_policy.max_concurrent_artifact_builds = config.workers.clamp(1, 4);
    // When deep verification is enabled it must be deterministic on offline
    // builders even when SuiteSparse cannot start its optional JIT compiler.
    // CompactCsc consumes the same published CSC generation without depending
    // on host compiler discovery.
    options.cache_policy.sparse_kernel = SparseKernelBackend::CompactCsc;
    let shard = GraphShard::open_standalone_writer_with_options(
        shard_path,
        Arc::clone(&object_store),
        options,
    )
    .await?;
    let result = finalize_scope(config, &scope, &shard).await;
    let close = shard.close().await;
    let summary = result?;
    close?;
    directory.register(&graph_scope).await?;
    let completion = ScopeCompletion {
        magic: COMPLETION_MAGIC.to_string(),
        snapshot_id: snapshot_id.to_string(),
        manifest_sha256: manifest_sha256.to_string(),
        scope_sha256: scope_sha256.clone(),
        scope_content_sha256: scope_content_sha256.clone(),
        vertices: scope.vertices.len(),
        relationships: scope.relationships.len(),
        edge_types: summary.0,
        batches: summary.1,
        deep_verified: summary.2,
        included_graphs: config.selection.included_names(),
    };
    put_completion(&object_store, &completion_path, &completion).await?;
    eprintln!(
        "direct_scope_complete scope={} deep_verified={} elapsed_ms={}",
        scope.scope,
        completion.deep_verified,
        started.elapsed().as_millis()
    );
    Ok(ScopeImportSummary {
        scope_sha256,
        scope_content_sha256,
        vertices: completion.vertices,
        relationships: completion.relationships,
        edge_types: completion.edge_types,
        batches: completion.batches,
        resumed: promoting_deferred_completion,
        deep_verified: completion.deep_verified,
        elapsed_milliseconds: started.elapsed().as_millis(),
    })
}

fn offline_import_limits(
    config: &Config,
    scope: &ResolvedScope,
) -> Result<GraphLimits, Box<dyn std::error::Error + Send + Sync>> {
    // The normal limits protect an online query from scanning an unexpectedly
    // broad index.  This is an offline, manifest-bounded build: refreshing the
    // indexes and statistics must be allowed to visit every record that the
    // already validated scope contains.  Keeping the online defaults here
    // makes a valid large scope fail halfway through publication (for example,
    // a label with 250,001 vertices hits max_query_index_candidates).
    let vertices = scope.vertices.len();
    let relationships = scope.relationships.len();
    let records = vertices.max(relationships).max(1);
    let relationships_u64 = u64::try_from(relationships)?;
    Ok(GraphLimits {
        max_bulk_import_edges: config.batch_size.max(4_096),
        max_artifact_build_edges: relationships_u64.max(1),
        max_query_result_vertices: vertices.max(1),
        max_query_intermediate_rows: records,
        max_query_index_candidates: records,
        max_query_scan_edges: relationships_u64.max(1),
        max_query_runtime_ms: None,
        ..GraphLimits::default()
    })
}

async fn build_direct_image(
    config: &Config,
    scope: &ResolvedScope,
    shard_path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<slatedb::OfflineImageResult, Box<dyn std::error::Error + Send + Sync>> {
    const SORT_BUFFER_BYTES: usize = 256 * 1024 * 1024;
    const TARGET_SST_BYTES: usize = 1024 * 1024 * 1024;
    let scratch = if let Some(parent) = &config.scratch_dir {
        std::fs::create_dir_all(parent)?;
        tempfile::Builder::new()
            .prefix("falkor-image-kv-")
            .tempdir_in(parent)?
    } else {
        tempfile::Builder::new()
            .prefix("falkor-image-kv-")
            .tempdir()?
    };
    let mut sorter = ExternalKvSorter::new(scratch.path(), SORT_BUFFER_BYTES)?;
    let mut encoder = OfflineGraphImageEncoder::new(&config.cell_id, true)?;
    let mut label_counts = BTreeMap::<String, u64>::new();
    let mut label_set_counts = BTreeMap::<Vec<String>, u64>::new();
    let mut vertex_property_counts = BTreeMap::<String, BTreeMap<VertexPropertyValue, u64>>::new();
    for vertex in scope.vertices.values() {
        let metadata = VertexMetadata {
            labels: vertex.labels.clone(),
            properties: property_map(&vertex.properties)?,
        };
        for label in &metadata.labels {
            *label_counts.entry(label.clone()).or_default() += 1;
        }
        *label_set_counts
            .entry(metadata.labels.iter().cloned().collect())
            .or_default() += 1;
        for (property, value) in &metadata.properties {
            *vertex_property_counts
                .entry(property.clone())
                .or_default()
                .entry(value.clone())
                .or_default() += 1;
        }
        sorter.extend(encoder.vertex_rows(vertex.id, &metadata)?)?;
    }
    let mut edge_type_counts = BTreeMap::<String, u64>::new();
    let mut expansion_counts =
        BTreeMap::<String, (BTreeMap<Vec<String>, u64>, BTreeMap<Vec<String>, u64>)>::new();
    let mut edge_property_counts =
        BTreeMap::<String, BTreeMap<String, BTreeMap<VertexPropertyValue, u64>>>::new();
    for relationship in scope.relationships.values() {
        let mutation = RelationshipMutation {
            cell_id: config.cell_id.clone(),
            edge_type: relationship.edge_type.clone(),
            src: relationship.src,
            dst: relationship.dst,
            relationship_id: relationship.id,
            metadata: EdgeMetadata {
                properties: property_map(&relationship.properties)?,
            },
        };
        *edge_type_counts
            .entry(relationship.edge_type.clone())
            .or_default() += 1;
        let source_labels = scope
            .vertices
            .get(&relationship.src)
            .expect("resolved relationship source exists")
            .labels
            .iter()
            .cloned()
            .collect();
        let destination_labels = scope
            .vertices
            .get(&relationship.dst)
            .expect("resolved relationship destination exists")
            .labels
            .iter()
            .cloned()
            .collect();
        let expansion = expansion_counts
            .entry(relationship.edge_type.clone())
            .or_default();
        *expansion.0.entry(source_labels).or_default() += 1;
        *expansion.1.entry(destination_labels).or_default() += 1;
        for (property, value) in &mutation.metadata.properties {
            *edge_property_counts
                .entry(relationship.edge_type.clone())
                .or_default()
                .entry(property.clone())
                .or_default()
                .entry(value.clone())
                .or_default() += 1;
        }
        sorter.extend(encoder.relationship_rows(&mutation)?)?;
    }

    let refreshed_at_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let scope_hash = scope_hash(scope);
    for (edge_type, count) in &edge_type_counts {
        sorter.extend(encoder.edge_type_query_stats_rows(edge_type, *count, refreshed_at_ms)?)?;
    }
    for (label, count) in &label_counts {
        sorter.extend(encoder.vertex_label_query_stats_rows(label, *count, refreshed_at_ms)?)?;
    }
    for labels in label_set_counts.keys().filter(|labels| labels.len() > 1) {
        sorter.extend(encoder.vertex_label_intersection_query_stats_rows(
            labels,
            superset_count(&label_set_counts, labels, &scope_hash)?,
            refreshed_at_ms,
        )?)?;
    }
    for (edge_type, (outbound, inbound)) in &expansion_counts {
        let label_sets = outbound
            .keys()
            .chain(inbound.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for labels in label_sets {
            let outgoing = superset_count(outbound, &labels, &scope_hash)?;
            let incoming = superset_count(inbound, &labels, &scope_hash)?;
            sorter.extend(encoder.edge_expansion_query_stats_rows(
                edge_type,
                QueryStatsDirection::Outgoing,
                &labels,
                outgoing,
                refreshed_at_ms,
            )?)?;
            sorter.extend(encoder.edge_expansion_query_stats_rows(
                edge_type,
                QueryStatsDirection::Incoming,
                &labels,
                incoming,
                refreshed_at_ms,
            )?)?;
            sorter.extend(encoder.edge_expansion_query_stats_rows(
                edge_type,
                QueryStatsDirection::Undirected,
                &labels,
                outgoing.checked_add(incoming).ok_or_else(|| {
                    hydradb::GraphError::CorruptValue {
                        key: format!("direct-import/{scope_hash}/{edge_type}/expansion"),
                        reason: "undirected expansion count overflow".to_string(),
                    }
                })?,
                refreshed_at_ms,
            )?)?;
        }
    }
    for (property, counts) in &vertex_property_counts {
        sorter.extend(encoder.vertex_property_histogram_query_stats_rows(
            property,
            counts,
            refreshed_at_ms,
        )?)?;
    }
    for (edge_type, properties) in &edge_property_counts {
        for (property, counts) in properties {
            sorter.extend(encoder.edge_property_histogram_query_stats_rows(
                edge_type,
                property,
                counts,
                refreshed_at_ms,
            )?)?;
        }
    }
    sorter.extend(encoder.finish_rows())?;
    Ok(sorter
        .publish(ObjectPath::from(shard_path), object_store, TARGET_SST_BYTES)
        .await?)
}

async fn finalize_scope(
    config: &Config,
    scope: &ResolvedScope,
    shard: &GraphShard,
) -> hydradb::Result<(usize, usize, bool)> {
    let mut batches = scope.vertices.len().div_ceil(config.batch_size);
    let mut by_type = BTreeMap::<String, Vec<(u64, u64)>>::new();
    for relationship in scope.relationships.values() {
        by_type
            .entry(relationship.edge_type.clone())
            .or_default()
            .push((relationship.src, relationship.dst));
    }
    let scope_hash = scope_hash(scope);
    for relationships in by_type.values() {
        batches += relationships.len().div_ceil(config.batch_size);
    }

    try_join_all(by_type.iter().map(|(edge_type, relationships)| {
        shard.build_graph_index_from_edges(
            &config.cell_id,
            edge_type,
            relationships.iter().copied(),
        )
    }))
    .await?;
    if config.defer_deep_verification {
        return Ok((by_type.len(), batches, false));
    }
    let reports = try_join_all(by_type.iter().map(|(edge_type, relationships)| async move {
        let report = shard
            .verify_current_graph(&config.cell_id, edge_type, 3, 32)
            .await?;
        Ok::<_, hydradb::GraphError>((edge_type, relationships.len(), report))
    }))
    .await?;
    for (edge_type, relationship_count, report) in reports {
        if !report.is_clean() {
            return Err(hydradb::GraphError::CorruptValue {
                key: format!("direct-import/{scope_hash}/{edge_type}"),
                reason: format!(
                    "post-import verification found {} mismatches: {:?}",
                    report.mismatch_count, report.mismatch_samples
                ),
            });
        }
        let expected_relationships =
            u64::try_from(relationship_count).map_err(|_| hydradb::GraphError::CorruptValue {
                key: format!("direct-import/{scope_hash}/{edge_type}"),
                reason: "relationship count does not fit u64".to_string(),
            })?;
        if report.relationship_merge_indexes != expected_relationships {
            return Err(hydradb::GraphError::CorruptValue {
                key: format!("direct-import/{scope_hash}/{edge_type}/merge-index"),
                reason: format!(
                    "verified {} relationship identity indexes for {expected_relationships} relationships",
                    report.relationship_merge_indexes
                ),
            });
        }
    }
    Ok((by_type.len(), batches, true))
}

fn superset_count(
    exact_counts: &BTreeMap<Vec<String>, u64>,
    required: &[String],
    scope_hash: &str,
) -> hydradb::Result<u64> {
    exact_counts
        .iter()
        .filter(|(labels, _)| {
            required
                .iter()
                .all(|required| labels.binary_search(required).is_ok())
        })
        .try_fold(0_u64, |total, (_, count)| {
            total
                .checked_add(*count)
                .ok_or_else(|| hydradb::GraphError::CorruptValue {
                    key: format!("direct-import/{scope_hash}/query-stats"),
                    reason: "query statistics count overflow".to_string(),
                })
        })
}

fn property_map(
    properties: &BTreeMap<String, PropertyValue>,
) -> hydradb::Result<BTreeMap<String, VertexPropertyValue>> {
    properties
        .iter()
        .filter_map(|(name, value)| {
            property_value(name, value).map(|result| result.map(|value| (name.clone(), value)))
        })
        .collect()
}

fn property_value(
    name: &str,
    value: &PropertyValue,
) -> Option<hydradb::Result<VertexPropertyValue>> {
    Some(Ok(match value {
        // A Cypher null is an absent property. Never manufacture a visible
        // string value for it.
        PropertyValue::Null => return None,
        PropertyValue::Bool(value) => VertexPropertyValue::Bool(*value),
        PropertyValue::Integer(value) => VertexPropertyValue::from_i64(*value),
        PropertyValue::Float(value) => VertexPropertyValue::Float(QueryFloat(*value)),
        PropertyValue::String(value) => VertexPropertyValue::String(value.clone()),
        PropertyValue::Array(_)
        | PropertyValue::Point { .. }
        | PropertyValue::VectorF32(_)
        | PropertyValue::Map(_) => {
            return Some(Err(hydradb::GraphError::CorruptValue {
                key: format!("property/{name}"),
                reason: "FalkorDB composite value has no lossless HydraDB property representation"
                    .into(),
            }))
        }
        PropertyValue::DateTime(_)
        | PropertyValue::Date(_)
        | PropertyValue::Time(_)
        | PropertyValue::Duration(_) => {
            return Some(Err(hydradb::GraphError::CorruptValue {
                key: format!("property/{name}"),
                reason: "temporal FalkorDB values have no lossless HydraDB property representation"
                    .into(),
            }))
        }
    }))
}

fn storage_scope(config: &Config, scope: &ResolvedScope) -> hydradb::Result<GraphScope> {
    let mut namespace = config.root_namespace.child(encode_scope_namespace_id(
        "tenant_id",
        &scope.scope.tenant_id,
    )?)?;
    if !scope.scope.sub_tenant_id.is_empty() {
        namespace = namespace.child(encode_scope_namespace_id(
            "sub_tenant_id",
            &scope.scope.sub_tenant_id,
        )?)?;
    }
    Ok(GraphScope::new(namespace, config.graph_id.clone()))
}

async fn prefix_has_data(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<bool, slatedb::object_store::Error> {
    use futures::StreamExt as _;
    Ok(store
        .list(Some(&ObjectPath::from(prefix)))
        .next()
        .await
        .transpose()?
        .is_some())
}

fn import_run_prefix(config: &Config, manifest_sha256: &str) -> String {
    format!(
        "{}/_offline_imports/v2/{manifest_sha256}",
        config.base_path.trim_end_matches('/'),
    )
}

fn completion_path(config: &Config, manifest_sha256: &str, scope_sha256: &str) -> ObjectPath {
    format!(
        "{}/complete/{scope_sha256}.json",
        import_run_prefix(config, manifest_sha256)
    )
    .into()
}

async fn read_json<T: DeserializeOwned>(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>> {
    let result = match store.get(path).await {
        Ok(result) => result,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let payload = result.bytes().await?;
    Ok(Some(serde_json::from_slice(&payload).map_err(|error| {
        format!("invalid JSON object at {path}: {error}")
    })?))
}

async fn put_json_create_exact<T>(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    value: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: DeserializeOwned + PartialEq + Serialize,
{
    let payload = Bytes::from(serde_json::to_vec(value)?);
    match store
        .put_opts(path, payload.into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing: T = read_json(store, path)
                .await?
                .ok_or_else(|| format!("object disappeared at {path}"))?;
            if existing == *value {
                Ok(())
            } else {
                Err(format!("conflicting object at {path}").into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

async fn put_completion(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    value: &ScopeCompletion,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = Bytes::from(serde_json::to_vec(value)?);
    match store
        .put_opts(path, payload.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing: ScopeCompletion = read_json(store, path)
                .await?
                .ok_or_else(|| format!("object disappeared at {path}"))?;
            if existing == *value {
                return Ok(());
            }

            let mut unverified_value = value.clone();
            unverified_value.deep_verified = false;
            if !existing.deep_verified && value.deep_verified && existing == unverified_value {
                store
                    .put_opts(path, payload.into(), PutMode::Overwrite.into())
                    .await?;
                return Ok(());
            }
            Err(format!("conflicting object at {path}").into())
        }
        Err(error) => Err(error.into()),
    }
}

fn validate_completion(
    completion: &ScopeCompletion,
    snapshot_id: &str,
    manifest_sha256: &str,
    scope_sha256: &str,
    scope_content_sha256: &str,
    scope: &ResolvedScope,
    require_deep_verification: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let edge_types = scope
        .relationships
        .values()
        .map(|relationship| relationship.edge_type.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    if completion.magic != COMPLETION_MAGIC
        || completion.snapshot_id != snapshot_id
        || completion.manifest_sha256 != manifest_sha256
        || completion.scope_sha256 != scope_sha256
        || completion.scope_content_sha256 != scope_content_sha256
        || completion.vertices != scope.vertices.len()
        || completion.relationships != scope.relationships.len()
        || completion.edge_types != edge_types
        || (require_deep_verification && !completion.deep_verified)
    {
        return Err(
            format!("completion receipt does not match snapshot scope {scope_sha256}").into(),
        );
    }
    Ok(())
}

async fn ensure_run_marker(
    store: &Arc<dyn ObjectStore>,
    config: &Config,
    plan: &falkor_dump_import::SnapshotSpill,
) -> Result<(), Box<dyn std::error::Error>> {
    let path: ObjectPath = format!(
        "{}/run.json",
        import_run_prefix(config, &plan.manifest_sha256)
    )
    .into();
    let expected = ImportRunMarker {
        magic: if matches!(config.selection, GraphSelection::Include(_)) {
            INCLUDED_RUN_MAGIC
        } else {
            COMPLETION_MAGIC
        }
        .to_string(),
        snapshot_id: plan.snapshot_id.clone(),
        manifest_sha256: plan.manifest_sha256.clone(),
        object_count: plan.object_count,
        dump_bytes: plan.dump_bytes,
        base_path: config.base_path.clone(),
        root_namespace: config.root_namespace.to_string(),
        graph_id: config.graph_id.to_string(),
        cell_id: config.cell_id.clone(),
        excluded_graphs: plan.excluded_graphs.clone(),
        included_graphs: plan.included_graphs.clone(),
    };
    let existing: Option<ImportRunMarker> = read_json(store, &path)
        .await
        .map_err(|error| error.to_string())?;
    match (config.resume, existing) {
        (false, Some(_)) => Err(format!(
            "import run already exists at {path}; use --resume for this exact snapshot"
        )
        .into()),
        (true, None) => {
            Err(format!("cannot resume without the import run marker at {path}").into())
        }
        (_, Some(existing)) if existing != expected => {
            Err(format!("import run marker does not match at {path}").into())
        }
        (true, Some(_)) => Ok(()),
        (false, None) => put_json_create_exact(store, &path, &expected)
            .await
            .map_err(|error| error.to_string().into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_scope_attempt(
    store: &Arc<dyn ObjectStore>,
    config: &Config,
    manifest_sha256: &str,
    scope_sha256: &str,
    scope_content_sha256: &str,
    scope: &ResolvedScope,
    destination_has_data: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path: ObjectPath = format!(
        "{}/attempt/{scope_sha256}.json",
        import_run_prefix(config, manifest_sha256)
    )
    .into();
    let expected = ScopeAttempt {
        magic: COMPLETION_MAGIC.to_string(),
        manifest_sha256: manifest_sha256.to_string(),
        scope_sha256: scope_sha256.to_string(),
        scope_content_sha256: scope_content_sha256.to_string(),
        vertices: scope.vertices.len(),
        relationships: scope.relationships.len(),
    };
    let existing: Option<ScopeAttempt> = read_json(store, &path).await?;
    if let Some(existing) = existing {
        if existing != expected {
            return Err(format!("scope attempt marker does not match at {path}").into());
        }
        if !config.resume {
            return Err(format!("scope import already started at {path}; use --resume").into());
        }
        return Ok(());
    }
    if destination_has_data {
        return Err(format!(
            "destination scope has data without a matching import marker: {}",
            scope.scope
        )
        .into());
    }
    put_json_create_exact(store, &path, &expected).await
}

fn scope_content_hash(
    scope: &ResolvedScope,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    Ok(format!(
        "{:x}",
        Sha256::digest(rmp_serde::to_vec_named(scope)?)
    ))
}

fn scope_hash(scope: &ResolvedScope) -> String {
    scope.scope.sha256_hex()
}
fn write_quarantine(
    directory: &Path,
    scopes: &[ResolvedScope],
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(directory)?;
    for scope in scopes
        .iter()
        .filter(|scope| !scope.orphan_edges.is_empty() || !scope.relationship_anomalies.is_empty())
    {
        let path = directory.join(format!("{}.json", scope_hash(scope)));
        let temporary = path.with_extension("json.tmp");
        let report = QuarantineReport {
            orphan_edge_records: &scope.orphan_edges,
            relationship_anomalies: &scope.relationship_anomalies,
        };
        std::fs::write(&temporary, serde_json::to_vec_pretty(&report)?)?;
        std::fs::rename(temporary, path)?;
    }
    Ok(())
}

impl Config {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        Self::parse_args(std::env::args().skip(1))
    }

    fn parse_args(
        arguments: impl IntoIterator<Item = String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut args = arguments.into_iter();
        let mut values = BTreeMap::new();
        let mut flags = BTreeSet::new();
        let mut selection = GraphSelection::All;
        while let Some(argument) = args.next() {
            if argument == "--resume" || argument == "--defer-deep-verification" {
                flags.insert(argument);
                continue;
            }
            if argument == "--exclude-graph" || argument == "--include-graph" {
                let graph = args
                    .next()
                    .ok_or_else(|| format!("{argument} requires a value"))?;
                selection.add_graph(&argument, graph)?;
                continue;
            }
            if !argument.starts_with("--") {
                return Err(format!("unexpected argument {argument}").into());
            }
            let value = args
                .next()
                .ok_or_else(|| format!("{argument} requires a value"))?;
            values.insert(argument, value);
        }
        let snapshot_root = PathBuf::from(required(&mut values, "--snapshot-root")?);
        let base_path = values
            .remove("--base-path")
            .unwrap_or_else(|| "graph/data".into());
        let root = values
            .remove("--root-namespace")
            .unwrap_or_else(|| "default".into());
        let root_namespace = NamespacePath::new(
            root.split('/')
                .map(|part| NamespaceId::new(part.to_string()))
                .collect::<hydradb::Result<Vec<_>>>()?,
        )?;
        let graph_id = GraphId::new(
            values
                .remove("--graph-id")
                .unwrap_or_else(|| "default".into()),
        )?;
        let cell_id = values
            .remove("--cell-id")
            .unwrap_or_else(|| "cell-0".into());
        let workers = parse_number(
            values.remove("--workers"),
            std::thread::available_parallelism().map_or(1, usize::from),
            "workers",
        )?;
        let scope_concurrency =
            parse_number(values.remove("--scope-concurrency"), 8, "scope concurrency")?;
        let batch_size = parse_number(values.remove("--batch-size"), 8_192, "batch size")?;
        let env_file = values.remove("--env-file");
        let quarantine_dir = values.remove("--quarantine-dir").map(PathBuf::from);
        let scratch_dir = values.remove("--scratch-dir").map(PathBuf::from);
        if !values.is_empty() {
            return Err(format!(
                "unknown options: {}",
                values.keys().cloned().collect::<Vec<_>>().join(", ")
            )
            .into());
        }
        if workers == 0 || scope_concurrency == 0 || batch_size == 0 {
            return Err("workers, scope concurrency, and batch size must be positive".into());
        }
        Ok(Self {
            snapshot_root,
            base_path,
            root_namespace,
            graph_id,
            cell_id,
            workers,
            scope_concurrency,
            batch_size,
            resume: flags.contains("--resume"),
            defer_deep_verification: flags.contains("--defer-deep-verification"),
            env_file,
            quarantine_dir,
            scratch_dir,
            selection,
        })
    }
}
fn required(
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    values
        .remove(key)
        .ok_or_else(|| format!("missing required {key}").into())
}
fn parse_number(
    value: Option<String>,
    default: usize,
    name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(value
        .map(|value| value.parse())
        .transpose()
        .map_err(|error| format!("invalid {name}: {error}"))?
        .unwrap_or(default))
}
