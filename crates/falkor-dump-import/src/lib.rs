//! Deterministic FalkorDB-to-HydraDB identity and scope resolver.
//!
//! This crate accepts only already-validated [`falkordb_rdb::GraphDump`] values.
//! It contains no object-store writer, which keeps source anomaly handling
//! independently testable from publication.

use std::collections::{BTreeMap, BTreeSet};

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use falkordb_rdb::{Entity, GraphDump, PropertyValue, RelationshipCell};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod kv_sort;
mod snapshot;
pub use kv_sort::{ExternalKvSorter, KvSortError};
pub use snapshot::{
    build_snapshot_plan, build_snapshot_spill, build_snapshot_spill_excluding,
    build_snapshot_spill_selected, load_spilled_scope, ExcludedGraph, GraphSelection,
    SnapshotError, SnapshotGraph, SnapshotPlan, SnapshotSpill, SpilledScope,
};

const IDENTITY_MASK: u64 = i64::MAX as u64;

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("graph {graph:?}, node {node}: {message}")]
    Node {
        graph: String,
        node: u64,
        message: String,
    },
    #[error("graph {graph:?}, edge {edge}: {message}")]
    Edge {
        graph: String,
        edge: u64,
        message: String,
    },
    #[error("scope {scope}: {message}")]
    Scope { scope: Scope, message: String },
}

pub type Result<T> = std::result::Result<T, ResolveError>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Scope {
    pub tenant_id: String,
    pub sub_tenant_id: String,
}

impl std::fmt::Display for Scope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.tenant_id, self.sub_tenant_id)
    }
}

impl Scope {
    /// Unambiguous SHA-256 digest key for this scope.
    ///
    /// Hashing `Display` ("{tenant}:{sub}") collides: `("a:b", "c")` and
    /// `("a", "b:c")` both render `a:b:c`, so distinct scopes would share a
    /// digest and get merged into one spill. Hash an escaped JSON array of both
    /// fields instead, the same encoding used for graph names. The snapshot
    /// spill and the import completion receipts must agree on this key.
    pub fn sha256_hex(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(
                ascii_json_array([self.tenant_id.as_str(), self.sub_tenant_id.as_str()]).as_bytes()
            )
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedVertex {
    pub id: u64,
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, PropertyValue>,
    pub sources: Vec<SourceObject>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedRelationship {
    pub id: u64,
    pub logical_id: u64,
    pub variant_id: u64,
    pub edge_type: String,
    pub src: u64,
    pub dst: u64,
    pub properties: BTreeMap<String, PropertyValue>,
    pub sources: Vec<SourceObject>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SourceObject {
    pub graph: String,
    pub falkor_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrphanEdge {
    pub source: SourceObject,
    pub properties: BTreeMap<String, PropertyValue>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipAnomalyDisposition {
    ImportedWithoutEdgeRecord,
    ImportedWithoutAdjacencyPair,
    ImportedWithFallbackIdentity,
    ImportedWithDisambiguatedIdentity,
    QuarantinedCrossScope,
    QuarantinedMissingEndpoint,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelationshipAnomaly {
    pub source: SourceObject,
    pub edge_type: String,
    pub src_falkor_id: u64,
    pub dst_falkor_id: u64,
    pub disposition: RelationshipAnomalyDisposition,
    pub properties: BTreeMap<String, PropertyValue>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolutionDiagnostics {
    pub merged_vertices: u64,
    pub merged_relationships: u64,
    pub inherited_scopes: u64,
    pub graph_key_scopes: u64,
    pub reused_edge_id_relationships: u64,
    pub relationship_anomalies: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedScope {
    pub scope: Scope,
    pub vertices: BTreeMap<u64, ResolvedVertex>,
    pub relationships: BTreeMap<u64, ResolvedRelationship>,
    pub orphan_edges: Vec<OrphanEdge>,
    pub relationship_anomalies: Vec<RelationshipAnomaly>,
    pub diagnostics: ResolutionDiagnostics,
}

#[derive(Clone)]
struct NodeResolution {
    label: String,
    identity: String,
    scope: Scope,
    id: u64,
}

pub fn stable_id(scope: &Scope, label: &str, identity: &str) -> u64 {
    let graph_name = ascii_json_array([scope.tenant_id.as_str(), scope.sub_tenant_id.as_str()]);
    let payload = ascii_json_array([graph_name.as_str(), label, identity]);
    let mut hasher = Blake2bVar::new(8).expect("8-byte BLAKE2b output is valid");
    hasher.update(payload.as_bytes());
    let mut digest = [0u8; 8];
    hasher
        .finalize_variable(&mut digest)
        .expect("output has exact requested length");
    let value = u64::from_be_bytes(digest) & IDENTITY_MASK;
    value.max(1)
}

pub fn ascii_json_array<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut output = String::from("[");
    for (index, value) in values.into_iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push('"');
        for character in value.chars() {
            match character {
                '"' => output.push_str("\\\""),
                '\\' => output.push_str("\\\\"),
                '\u{0008}' => output.push_str("\\b"),
                '\u{000c}' => output.push_str("\\f"),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\t' => output.push_str("\\t"),
                character if (' '..='~').contains(&character) => output.push(character),
                character if (character as u32) <= 0xffff => {
                    use std::fmt::Write as _;
                    write!(output, "\\u{:04x}", character as u32)
                        .expect("String writes cannot fail");
                }
                character => {
                    use std::fmt::Write as _;
                    let scalar = character as u32 - 0x1_0000;
                    let high = 0xd800 + (scalar >> 10);
                    let low = 0xdc00 + (scalar & 0x3ff);
                    write!(output, "\\u{high:04x}\\u{low:04x}").expect("String writes cannot fail");
                }
            }
        }
        output.push('"');
    }
    output.push(']');
    output
}

pub fn resolve_graph(graph: &GraphDump) -> Result<Vec<ResolvedScope>> {
    let fallback_scope = graph_key_scope(&graph.name);
    let mut neighbors: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for relationship in &graph.relationships {
        neighbors
            .entry(relationship.src)
            .or_default()
            .insert(relationship.dst);
        neighbors
            .entry(relationship.dst)
            .or_default()
            .insert(relationship.src);
    }
    let mut direct_scopes = BTreeMap::new();
    for (&id, entity) in &graph.nodes {
        if let Some(scope) = entity_scope(entity, &graph.name, id)? {
            direct_scopes.insert(id, scope);
        }
    }
    let mut scopes = direct_scopes.clone();
    let mut inherited = 0u64;
    let mut graph_key = 0u64;
    for (&id, entity) in &graph.nodes {
        if scopes.contains_key(&id) {
            continue;
        }
        let declared_sub_tenant = scope_component(entity, "sub_tenant_id", &graph.name, id)?;
        let candidates: BTreeSet<_> = neighbors
            .get(&id)
            .into_iter()
            .flatten()
            .filter_map(|neighbor| direct_scopes.get(neighbor))
            .cloned()
            .collect();
        let scope = match candidates.len() {
            1 => {
                inherited += 1;
                candidates.into_iter().next().unwrap()
            }
            0 => {
                graph_key += 1;
                fallback_scope.clone().ok_or_else(|| ResolveError::Node {
                    graph: graph.name.clone(),
                    node: id,
                    message: "has no tenant_id, no scoped neighbour, and graph key does not encode a scope".into(),
                })?
            }
            count => {
                return Err(ResolveError::Node {
                    graph: graph.name.clone(),
                    node: id,
                    message: format!("neighbours resolve to {count} different scopes"),
                })
            }
        };
        if let Some(declared_sub_tenant) = declared_sub_tenant {
            if scope.sub_tenant_id != declared_sub_tenant {
                return Err(ResolveError::Node {
                    graph: graph.name.clone(),
                    node: id,
                    message: format!("declares sub_tenant_id {declared_sub_tenant:?} but inferred scope is {scope}"),
                });
            }
        }
        scopes.insert(id, scope);
    }

    let mut node_resolutions = BTreeMap::new();
    let mut outputs = BTreeMap::<Scope, ResolvedScope>::new();
    for (&falkor_id, entity) in &graph.nodes {
        let labels = graph.labels.get(&falkor_id).cloned().unwrap_or_default();
        let identities = labels
            .iter()
            .filter_map(|label| {
                vertex_identity(label, entity).map(|identity| (label.clone(), identity))
            })
            .collect::<Vec<_>>();
        if identities.len() != 1 {
            return Err(ResolveError::Node {
                graph: graph.name.clone(),
                node: falkor_id,
                message: format!(
                    "has {} supported identity labels, expected exactly one",
                    identities.len()
                ),
            });
        }
        let (label, identity) = identities.into_iter().next().unwrap();
        let scope = scopes
            .get(&falkor_id)
            .expect("every node receives a scope")
            .clone();
        let id = stable_id(&scope, &label, &identity);
        node_resolutions.insert(
            falkor_id,
            NodeResolution {
                label: label.clone(),
                identity,
                scope: scope.clone(),
                id,
            },
        );
        let mut properties = entity.properties.clone();
        properties.insert("id".into(), PropertyValue::Integer(id as i64));
        properties.insert("falkor_id".into(), PropertyValue::Integer(falkor_id as i64));
        let candidate = ResolvedVertex {
            id,
            labels,
            properties,
            sources: vec![SourceObject {
                graph: graph.name.clone(),
                falkor_id,
            }],
        };
        let output = outputs
            .entry(scope.clone())
            .or_insert_with(|| ResolvedScope {
                scope,
                vertices: BTreeMap::new(),
                relationships: BTreeMap::new(),
                orphan_edges: Vec::new(),
                relationship_anomalies: Vec::new(),
                diagnostics: ResolutionDiagnostics::default(),
            });
        match output.vertices.get_mut(&id) {
            Some(existing) => {
                merge_vertex(existing, candidate);
                output.diagnostics.merged_vertices += 1;
            }
            None => {
                output.vertices.insert(id, candidate);
            }
        }
    }

    let mut reached_edges = BTreeSet::new();
    let mut edge_id_occurrences = BTreeMap::<u64, usize>::new();
    for cell in &graph.relationships {
        *edge_id_occurrences.entry(cell.edge_id).or_default() += 1;
    }
    for cell in &graph.relationships {
        let source = node_resolutions.get(&cell.src);
        let destination = node_resolutions.get(&cell.dst);
        if source.is_none() || destination.is_none() {
            let scope = source
                .or(destination)
                .map(|node| node.scope.clone())
                .or_else(|| {
                    if outputs.len() == 1 {
                        outputs.keys().next().cloned()
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    edge_error(
                        graph,
                        cell,
                        "both endpoint nodes are absent and no unique graph scope exists",
                    )
                })?;
            let output = outputs
                .get_mut(&scope)
                .expect("quarantine scope belongs to an existing output");
            let entity = graph.edges.get(&cell.edge_id);
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::QuarantinedMissingEndpoint,
                properties: entity
                    .map(|entity| entity.properties.clone())
                    .unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
            if entity.is_some() {
                reached_edges.insert(cell.edge_id);
            }
            continue;
        }
        let source = source.expect("checked above");
        let destination = destination.expect("checked above");
        let output = outputs
            .get_mut(&source.scope)
            .expect("source scope was created with its vertex");
        let entity = graph.edges.get(&cell.edge_id);
        if source.scope != destination.scope {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::QuarantinedCrossScope,
                properties: entity
                    .map(|entity| entity.properties.clone())
                    .unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
            if entity.is_some() {
                reached_edges.insert(cell.edge_id);
            }
            continue;
        }
        if !graph.adjacency.contains(&(cell.src, cell.dst)) {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithoutAdjacencyPair,
                properties: entity
                    .map(|entity| entity.properties.clone())
                    .unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        if entity.is_none() {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithoutEdgeRecord,
                properties: BTreeMap::new(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let falkor_id = cell.edge_id.to_string();
        let known_identity = entity
            .and_then(|entity| relationship_identity(&cell.edge_type, source, destination, entity));
        if entity.is_some() && known_identity.is_none() {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithFallbackIdentity,
                properties: entity
                    .map(|entity| entity.properties.clone())
                    .unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let identity = known_identity.unwrap_or_else(|| {
            ascii_json_array([
                "falkor_edge",
                cell.edge_type.as_str(),
                source.label.as_str(),
                source.identity.as_str(),
                destination.label.as_str(),
                destination.identity.as_str(),
                falkor_id.as_str(),
            ])
        });
        if edge_id_occurrences.get(&cell.edge_id).copied().unwrap_or(0) > 1 {
            output.diagnostics.reused_edge_id_relationships += 1;
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithDisambiguatedIdentity,
                properties: entity
                    .map(|entity| entity.properties.clone())
                    .unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let logical_id = stable_id(
            &source.scope,
            &format!("relationship_{}", cell.edge_type),
            &identity,
        );
        let variant_identity = ascii_json_array([
            identity.as_str(),
            cell.edge_type.as_str(),
            source.label.as_str(),
            source.identity.as_str(),
            destination.label.as_str(),
            destination.identity.as_str(),
        ]);
        let variant_id = stable_id(
            &source.scope,
            &format!("relationship_{}_variant", cell.edge_type),
            &variant_identity,
        );
        let id = variant_id;
        let mut properties = entity
            .map(|entity| entity.properties.clone())
            .unwrap_or_default();
        properties.insert("id".into(), PropertyValue::Integer(id as i64));
        properties.insert(
            "falkor_id".into(),
            PropertyValue::Integer(cell.edge_id as i64),
        );
        let candidate = ResolvedRelationship {
            id,
            logical_id,
            variant_id,
            edge_type: cell.edge_type.clone(),
            src: source.id,
            dst: destination.id,
            properties,
            sources: vec![SourceObject {
                graph: graph.name.clone(),
                falkor_id: cell.edge_id,
            }],
        };
        match output.relationships.get_mut(&id) {
            Some(existing)
                if existing.edge_type == candidate.edge_type
                    && existing.src == candidate.src
                    && existing.dst == candidate.dst =>
            {
                merge_relationship(existing, candidate);
                output.diagnostics.merged_relationships += 1;
            }
            Some(existing) => {
                return Err(ResolveError::Edge {
                    graph: graph.name.clone(),
                    edge: cell.edge_id,
                    message: format!(
                        "stable relationship id {id} conflicts with {} {} -> {}",
                        existing.edge_type, existing.src, existing.dst
                    ),
                })
            }
            None => {
                output.relationships.insert(id, candidate);
            }
        }
        reached_edges.insert(cell.edge_id);
    }
    for (&edge_id, entity) in &graph.edges {
        if reached_edges.contains(&edge_id) {
            continue;
        }
        let source = SourceObject {
            graph: graph.name.clone(),
            falkor_id: edge_id,
        };
        let scope = fallback_scope
            .clone()
            .or_else(|| outputs.keys().next().cloned())
            .ok_or_else(|| ResolveError::Edge {
                graph: graph.name.clone(),
                edge: edge_id,
                message: "orphan edge cannot be assigned to a report scope because the graph has no resolved vertices".into(),
            })?;
        outputs
            .entry(scope.clone())
            .or_insert_with(|| ResolvedScope {
                scope,
                vertices: BTreeMap::new(),
                relationships: BTreeMap::new(),
                orphan_edges: Vec::new(),
                relationship_anomalies: Vec::new(),
                diagnostics: ResolutionDiagnostics::default(),
            })
            .orphan_edges
            .push(OrphanEdge {
                source,
                properties: entity.properties.clone(),
            });
    }
    for output in outputs.values_mut() {
        output.diagnostics.inherited_scopes = inherited;
        output.diagnostics.graph_key_scopes = graph_key;
        output
            .orphan_edges
            .sort_by(|left, right| left.source.cmp(&right.source));
        sort_relationship_anomalies(&mut output.relationship_anomalies);
        canonicalize_relationship_ids(output)?;
    }
    Ok(outputs.into_values().collect())
}

pub fn merge_scopes(inputs: impl IntoIterator<Item = ResolvedScope>) -> Result<Vec<ResolvedScope>> {
    let mut outputs = BTreeMap::<Scope, ResolvedScope>::new();
    for input in inputs {
        let output = outputs
            .entry(input.scope.clone())
            .or_insert_with(|| ResolvedScope {
                scope: input.scope.clone(),
                vertices: BTreeMap::new(),
                relationships: BTreeMap::new(),
                orphan_edges: Vec::new(),
                relationship_anomalies: Vec::new(),
                diagnostics: ResolutionDiagnostics::default(),
            });
        for (id, candidate) in input.vertices {
            match output.vertices.get_mut(&id) {
                Some(existing) => {
                    merge_vertex(existing, candidate);
                    output.diagnostics.merged_vertices += 1;
                }
                None => {
                    output.vertices.insert(id, candidate);
                }
            }
        }
        for candidate in input.relationships.into_values() {
            let variant_id = candidate.variant_id;
            match output.relationships.get_mut(&variant_id) {
                Some(existing)
                    if existing.edge_type == candidate.edge_type && existing.src == candidate.src && existing.dst == candidate.dst =>
                {
                    merge_relationship(existing, candidate);
                    output.diagnostics.merged_relationships += 1;
                }
                Some(existing) => {
                    return Err(ResolveError::Scope {
                        scope: output.scope.clone(),
                        message: format!(
                            "relationship variant id {variant_id} conflicts between {} {} -> {} and {} {} -> {}",
                            existing.edge_type, existing.src, existing.dst, candidate.edge_type, candidate.src, candidate.dst
                        ),
                    })
                }
                None => {
                    output.relationships.insert(variant_id, candidate);
                }
            }
        }
        output.orphan_edges.extend(input.orphan_edges);
        output
            .relationship_anomalies
            .extend(input.relationship_anomalies);
        output.diagnostics.inherited_scopes += input.diagnostics.inherited_scopes;
        output.diagnostics.graph_key_scopes += input.diagnostics.graph_key_scopes;
        output.diagnostics.reused_edge_id_relationships +=
            input.diagnostics.reused_edge_id_relationships;
        output.diagnostics.relationship_anomalies += input.diagnostics.relationship_anomalies;
    }
    for output in outputs.values_mut() {
        output
            .orphan_edges
            .sort_by(|left, right| left.source.cmp(&right.source));
        sort_relationship_anomalies(&mut output.relationship_anomalies);
        for vertex in output.vertices.values_mut() {
            vertex.sources.sort();
            vertex.sources.dedup();
        }
        for relationship in output.relationships.values_mut() {
            relationship.sources.sort();
            relationship.sources.dedup();
        }
        canonicalize_relationship_ids(output)?;
    }
    Ok(outputs.into_values().collect())
}

fn canonicalize_relationship_ids(scope: &mut ResolvedScope) -> Result<()> {
    let mut by_logical_id = BTreeMap::<u64, Vec<ResolvedRelationship>>::new();
    for relationship in std::mem::take(&mut scope.relationships).into_values() {
        by_logical_id
            .entry(relationship.logical_id)
            .or_default()
            .push(relationship);
    }

    let mut relationships = BTreeMap::new();
    for (logical_id, mut variants) in by_logical_id {
        variants.sort_by_key(|relationship| relationship.variant_id);
        for (index, mut relationship) in variants.into_iter().enumerate() {
            let id = if index == 0 {
                logical_id
            } else {
                relationship.variant_id
            };
            relationship.id = id;
            relationship
                .properties
                .insert("id".into(), PropertyValue::Integer(id as i64));
            if let Some(existing) = relationships.insert(id, relationship) {
                return Err(ResolveError::Scope {
                    scope: scope.scope.clone(),
                    message: format!(
                        "canonical relationship id {id} collides between {} {} -> {} and another relationship",
                        existing.edge_type, existing.src, existing.dst
                    ),
                });
            }
        }
    }
    scope.relationships = relationships;
    Ok(())
}

fn sort_relationship_anomalies(anomalies: &mut [RelationshipAnomaly]) {
    anomalies.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.edge_type.cmp(&right.edge_type))
            .then_with(|| left.src_falkor_id.cmp(&right.src_falkor_id))
            .then_with(|| left.dst_falkor_id.cmp(&right.dst_falkor_id))
            .then_with(|| left.disposition.cmp(&right.disposition))
    });
}

fn entity_scope(entity: &Entity, graph: &str, node: u64) -> Result<Option<Scope>> {
    let Some(tenant) = scope_component(entity, "tenant_id", graph, node)? else {
        return Ok(None);
    };
    let sub = scope_component(entity, "sub_tenant_id", graph, node)?.unwrap_or_default();
    Ok(Some(Scope {
        tenant_id: tenant,
        sub_tenant_id: sub,
    }))
}

fn scope_component(entity: &Entity, key: &str, graph: &str, node: u64) -> Result<Option<String>> {
    match entity.properties.get(key) {
        None | Some(PropertyValue::Null) => Ok(None),
        Some(PropertyValue::String(value)) if value.is_empty() => Ok(None),
        Some(PropertyValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ResolveError::Node {
            graph: graph.into(),
            node,
            message: format!("{key} is not a string"),
        }),
    }
}

fn graph_key_scope(name: &str) -> Option<Scope> {
    let [tenant_id, sub_tenant_id]: [String; 2] = serde_json::from_str(name).ok()?;
    (!tenant_id.is_empty()).then_some(Scope {
        tenant_id,
        sub_tenant_id,
    })
}

fn vertex_identity(label: &str, entity: &Entity) -> Option<String> {
    let key = match label {
        "Source" => "source_id",
        "Chunk" => "chunk_id",
        "Entity" => "entity_id",
        "Actor" => "actor_id",
        "Attachment" => "attachment_id",
        "Comment" => "comment_id",
        _ => return None,
    };
    string_property(&entity.properties, key).or_else(|| {
        if label != "Source" {
            return None;
        }
        let provider = string_property(&entity.properties, "app_provider")?;
        let external = string_property(&entity.properties, "app_external_id")?;
        Some(format!("{provider}:{external}"))
    })
}

fn relationship_identity(
    edge_type: &str,
    source: &NodeResolution,
    destination: &NodeResolution,
    entity: &Entity,
) -> Option<String> {
    match edge_type {
        "RELATES" => string_property(&entity.properties, "relationship_id"),
        "ACTED_ON" => Some(format!(
            "{}:{}:{}",
            source.identity,
            destination.identity,
            string_property(&entity.properties, "role")?
        )),
        "RELATES_TO" => Some(format!(
            "{}:{}:{}",
            source.identity,
            destination.identity,
            string_property(&entity.properties, "relation_type").unwrap_or_default()
        )),
        "ALIAS_OF" if source.label == "Entity" && destination.label == "Entity" => {
            Some(source.identity.clone())
        }
        "ALIAS_OF" | "HAS_CHUNK" | "PRESENT_IN" | "HAS_ATTACHMENT" | "HAS_COMMENT"
        | "AUTHORED_COMMENT" => Some(format!("{}:{}", source.identity, destination.identity)),
        "FORCEFUL_RELATION" => Some(ascii_json_array([
            source.identity.as_str(),
            destination.identity.as_str(),
        ])),
        _ => None,
    }
}

fn string_property(properties: &BTreeMap<String, PropertyValue>, key: &str) -> Option<String> {
    match properties.get(key) {
        Some(PropertyValue::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

fn merge_vertex(existing: &mut ResolvedVertex, candidate: ResolvedVertex) {
    let candidate_newer = recency(&candidate.properties) > recency(&existing.properties);
    let (newer, older) = if candidate_newer {
        (candidate, existing.clone())
    } else {
        (existing.clone(), candidate)
    };
    let mut properties = newer.properties;
    for (key, value) in older.properties {
        if matches!(properties.get(&key), None | Some(PropertyValue::Null))
            && !matches!(value, PropertyValue::Null)
        {
            properties.insert(key, value);
        }
    }
    existing.labels.extend(newer.labels);
    existing.labels.extend(older.labels);
    existing.properties = properties;
    existing.sources.extend(newer.sources);
    existing.sources.extend(older.sources);
    existing.sources.sort();
    existing.sources.dedup();
}

fn merge_relationship(existing: &mut ResolvedRelationship, candidate: ResolvedRelationship) {
    let candidate_newer = recency(&candidate.properties) > recency(&existing.properties);
    let (newer, older) = if candidate_newer {
        (candidate, existing.clone())
    } else {
        (existing.clone(), candidate)
    };
    let mut properties = newer.properties;
    for (key, value) in older.properties {
        if matches!(properties.get(&key), None | Some(PropertyValue::Null))
            && !matches!(value, PropertyValue::Null)
        {
            properties.insert(key, value);
        }
    }
    existing.properties = properties;
    existing.sources.extend(newer.sources);
    existing.sources.extend(older.sources);
    existing.sources.sort();
    existing.sources.dedup();
}

fn recency(properties: &BTreeMap<String, PropertyValue>) -> (String, String) {
    (
        sortable_property(properties.get("updated_at")),
        sortable_property(properties.get("created_at")),
    )
}
fn sortable_property(value: Option<&PropertyValue>) -> String {
    match value {
        Some(PropertyValue::String(value)) => value.clone(),
        Some(PropertyValue::Integer(value)) => format!("{value:+020}"),
        Some(PropertyValue::Float(value)) => format!("{value:+030.9}"),
        _ => String::new(),
    }
}
fn edge_error(
    graph: &GraphDump,
    cell: &RelationshipCell,
    message: impl Into<String>,
) -> ResolveError {
    ResolveError::Edge {
        graph: graph.name.clone(),
        edge: cell.edge_id,
        message: message.into(),
    }
}
