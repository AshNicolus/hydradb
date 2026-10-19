use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
use ulid::Ulid;

use crate::{GraphError, GraphId, GraphScope, NamespaceId, NamespacePath, Result};

const SCOPE_MARKER_VERSION: &str = "graph-scope-directory1";
const SCOPE_MARKER_NAME: &str = "__scope__";
#[cfg(test)]
const SCOPE_CHANGE_VERSION: &str = "graph-scope-change1";
const SCOPE_CHANGE_SEQUENCE_VERSION: &str = "graph-scope-change2";
const SCOPE_CHANGE_SEQUENCE_PREFIX: &str = "v2";
const SCOPE_CHANGE_ACK_ATTEMPTS: usize = 8;
const MAX_SCOPE_CHANGE_ACK_BYTES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphScopeChange {
    pub scope: GraphScope,
    pub location: Path,
    pub created_at: DateTime<Utc>,
    pub cell_id: Option<String>,
    pub sequence: Option<u64>,
}

#[derive(Clone)]
pub struct ObjectStoreGraphScopeDirectory {
    base_path: String,
    root_namespace: NamespacePath,
    graph_id: GraphId,
    object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreGraphScopeDirectory {
    pub fn new(
        base_path: impl Into<String>,
        root_namespace: NamespacePath,
        graph_id: GraphId,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            base_path: base_path.into().trim_matches('/').to_string(),
            root_namespace,
            graph_id,
            object_store,
        }
    }

    pub fn root_scope(&self) -> GraphScope {
        GraphScope::new(self.root_namespace.clone(), self.graph_id.clone())
    }

    pub async fn register(&self, scope: &GraphScope) -> Result<()> {
        self.validate_scope(scope)?;
        let location = self.marker_path(scope);
        let payload = Bytes::from_static(SCOPE_MARKER_VERSION.as_bytes());
        match self
            .object_store
            .put_opts(&location, payload.into(), PutMode::Create.into())
            .await
        {
            Ok(_) | Err(slatedb::object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list(&self) -> Result<Vec<GraphScope>> {
        let prefix = self.registry_prefix();
        let mut objects = self.object_store.list(Some(&prefix));
        let mut scopes = Vec::new();
        while let Some(metadata) = objects.next().await.transpose()? {
            let scope = self.scope_from_marker_path(metadata.location.as_ref())?;
            self.validate_scope(&scope)?;
            scopes.push(scope);
        }
        scopes.sort();
        scopes.dedup();
        Ok(scopes)
    }

    /// Persist a best-effort fast-lane hint after a scoped graph mutation.
    ///
    /// Every mutation gets a unique, sequence-bearing object. The indexer can
    /// acknowledge all hints covered by one refreshed storage snapshot without
    /// deleting a later mutation. The ordinary scope directory remains the
    /// recovery path if a process exits before this asynchronous hint lands.
    pub async fn notify_changed(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        sequence: u64,
    ) -> Result<()> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.sequenced_change_path(scope, cell_id, sequence, Ulid::new());
        let payload = Bytes::from_static(SCOPE_CHANGE_SEQUENCE_VERSION.as_bytes());
        self.object_store
            .put_opts(&location, payload.into(), PutMode::Create.into())
            .await?;
        Ok(())
    }

    pub async fn list_changes(&self) -> Result<Vec<GraphScopeChange>> {
        let prefix = self.change_prefix();
        let mut objects = self.object_store.list(Some(&prefix));
        let mut changes = Vec::new();
        while let Some(metadata) = objects.next().await.transpose()? {
            let (scope, cell_id, sequence) = self.change_from_path(metadata.location.as_ref())?;
            self.validate_scope(&scope)?;
            changes.push(GraphScopeChange {
                scope,
                location: metadata.location,
                created_at: metadata.last_modified,
                cell_id,
                sequence,
            });
        }
        changes.sort_by(|left, right| {
            left.scope
                .cmp(&right.scope)
                .then_with(|| left.location.cmp(&right.location))
        });
        Ok(changes)
    }

    pub async fn clear_changes(&self, changes: &[GraphScopeChange]) -> Result<()> {
        for change in changes {
            self.validate_scope(&change.scope)?;
        }
        let locations = changes
            .iter()
            .map(|change| change.location.clone())
            .collect::<Vec<_>>();
        let deletes = futures::stream::iter(locations)
            .map(|location| {
                let object_store = Arc::clone(&self.object_store);
                async move { object_store.delete(&location).await }
            })
            .buffer_unordered(32);
        let results = deletes.collect::<Vec<_>>().await;
        for result in results {
            result?;
        }
        Ok(())
    }

    pub async fn covered_sequence(&self, scope: &GraphScope, cell_id: &str) -> Result<Option<u64>> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.change_ack_path(scope, cell_id);
        match self.object_store.get(&location).await {
            Ok(result) => {
                let value = result.bytes().await?;
                decode_covered_sequence(location.as_ref(), &value).map(Some)
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Advance the durable per-cell watermark without allowing a concurrent,
    /// older indexer pass to move it backwards.
    pub async fn acknowledge_sequence(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        sequence: u64,
    ) -> Result<()> {
        self.validate_scope(scope)?;
        crate::validate_component("cell_id", cell_id)?;
        let location = self.change_ack_path(scope, cell_id);
        let payload = Bytes::from(sequence.to_string());
        for _ in 0..SCOPE_CHANGE_ACK_ATTEMPTS {
            let current = match self.object_store.get(&location).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let value = result.bytes().await?;
                    let current = decode_covered_sequence(location.as_ref(), &value)?;
                    if current >= sequence {
                        return Ok(());
                    }
                    Some(version)
                }
                Err(slatedb::object_store::Error::NotFound { .. }) => None,
                Err(error) => return Err(error.into()),
            };
            let mode = current.map_or(PutMode::Create, PutMode::Update);
            match self
                .object_store
                .put_opts(&location, payload.clone().into(), mode.into())
                .await
            {
                Ok(_) => return Ok(()),
                Err(slatedb::object_store::Error::AlreadyExists { .. })
                | Err(slatedb::object_store::Error::Precondition { .. })
                | Err(slatedb::object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(GraphError::ConditionalWriteConflict {
            operation: "acknowledge_graph_scope_change",
            key: location.to_string(),
        })
    }

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace)
        {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!(
                "{}/graphs/{} and descendants",
                self.root_namespace, self.graph_id
            ),
            actual: scope.to_string(),
        })
    }

    fn registry_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_graph_scopes/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn change_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_graph_scope_changes/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn change_ack_prefix(&self) -> Path {
        path_from_base(
            &self.base_path,
            &format!(
                "_graph_scope_change_acks/v1/{}/{}",
                self.graph_id,
                namespace_key(&self.root_namespace)
            ),
        )
    }

    fn marker_path(&self, scope: &GraphScope) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace
            .iter()
            .map(NamespaceId::as_str)
            .collect::<Vec<_>>()
            .join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(SCOPE_MARKER_NAME);
        Path::from(format!("{}/{suffix}", self.registry_prefix()))
    }

    #[cfg(test)]
    fn change_path(&self, scope: &GraphScope, id: Ulid) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace
            .iter()
            .map(NamespaceId::as_str)
            .collect::<Vec<_>>()
            .join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&id.to_string());
        Path::from(format!("{}/{suffix}", self.change_prefix()))
    }

    fn sequenced_change_path(
        &self,
        scope: &GraphScope,
        cell_id: &str,
        sequence: u64,
        id: Ulid,
    ) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace
            .iter()
            .map(NamespaceId::as_str)
            .collect::<Vec<_>>()
            .join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&format!(
            "{SCOPE_CHANGE_SEQUENCE_PREFIX}.{}.{sequence:020}.{id}",
            hex_encode(cell_id.as_bytes())
        ));
        Path::from(format!("{}/{suffix}", self.change_prefix()))
    }

    fn change_ack_path(&self, scope: &GraphScope, cell_id: &str) -> Path {
        let relative_namespace = &scope.namespace.segments()[self.root_namespace.depth()..];
        let mut suffix = relative_namespace
            .iter()
            .map(NamespaceId::as_str)
            .collect::<Vec<_>>()
            .join("/");
        if !suffix.is_empty() {
            suffix.push('/');
        }
        suffix.push_str(&hex_encode(cell_id.as_bytes()));
        Path::from(format!("{}/{suffix}", self.change_ack_prefix()))
    }

    fn scope_from_marker_path(&self, location: &str) -> Result<GraphScope> {
        let prefix = self.registry_prefix().to_string();
        let relative = location
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope marker is outside the configured registry prefix".to_string(),
            })?;
        let segments = relative.split('/').collect::<Vec<_>>();
        if segments.last().copied() != Some(SCOPE_MARKER_NAME) {
            return Err(GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope registry object is missing its terminal marker".to_string(),
            });
        }
        let mut namespace = self.root_namespace.clone();
        for segment in &segments[..segments.len() - 1] {
            namespace = namespace.child(NamespaceId::new((*segment).to_string())?)?;
        }
        Ok(GraphScope::new(namespace, self.graph_id.clone()))
    }

    fn change_from_path(
        &self,
        location: &str,
    ) -> Result<(GraphScope, Option<String>, Option<u64>)> {
        let prefix = self.change_prefix().to_string();
        let relative = location
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_prefix('/'))
            .ok_or_else(|| GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope change is outside the configured change prefix".to_string(),
            })?;
        let segments = relative.split('/').collect::<Vec<_>>();
        let Some(change_id) = segments.last() else {
            return Err(GraphError::CorruptValue {
                key: location.to_string(),
                reason: "scope change is missing its identifier".to_string(),
            });
        };
        let (cell_id, sequence) = if let Some(encoded) =
            change_id.strip_prefix(&format!("{SCOPE_CHANGE_SEQUENCE_PREFIX}."))
        {
            let parts = encoded.split('.').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err(GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: "sequenced scope change has an invalid identifier".to_string(),
                });
            }
            let cell_id = String::from_utf8(hex_decode(parts[0], location)?).map_err(|error| {
                GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: format!("scope change cell id is not UTF-8: {error}"),
                }
            })?;
            crate::validate_component("cell_id", &cell_id)?;
            let sequence = parts[1]
                .parse::<u64>()
                .map_err(|error| GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: format!("scope change has an invalid sequence: {error}"),
                })?;
            parts[2]
                .parse::<Ulid>()
                .map_err(|error| GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: format!("scope change has an invalid identifier: {error}"),
                })?;
            (Some(cell_id), Some(sequence))
        } else {
            change_id
                .parse::<Ulid>()
                .map_err(|error| GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: format!("scope change has an invalid identifier: {error}"),
                })?;
            (None, None)
        };
        let mut namespace = self.root_namespace.clone();
        for segment in &segments[..segments.len() - 1] {
            namespace = namespace.child(NamespaceId::new((*segment).to_string())?)?;
        }
        Ok((
            GraphScope::new(namespace, self.graph_id.clone()),
            cell_id,
            sequence,
        ))
    }
}

fn decode_covered_sequence(location: &str, value: &[u8]) -> Result<u64> {
    if value.len() > MAX_SCOPE_CHANGE_ACK_BYTES {
        return Err(GraphError::CorruptValue {
            key: location.to_string(),
            reason: format!(
                "scope change acknowledgement is {} bytes; maximum is {MAX_SCOPE_CHANGE_ACK_BYTES}",
                value.len()
            ),
        });
    }
    let value = std::str::from_utf8(value).map_err(|error| GraphError::CorruptValue {
        key: location.to_string(),
        reason: format!("scope change acknowledgement is not UTF-8: {error}"),
    })?;
    value
        .parse::<u64>()
        .map_err(|error| GraphError::CorruptValue {
            key: location.to_string(),
            reason: format!("scope change acknowledgement is not a storage sequence: {error}"),
        })
}

fn hex_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str, location: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(GraphError::CorruptValue {
            key: location.to_string(),
            reason: "scope change cell id has invalid hex length".to_string(),
        });
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            match (high, low) {
                (Some(high), Some(low)) => Ok((high << 4) | low),
                _ => Err(GraphError::CorruptValue {
                    key: location.to_string(),
                    reason: "scope change cell id contains invalid hex".to_string(),
                }),
            }
        })
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn namespace_key(namespace: &NamespacePath) -> String {
    namespace
        .segments()
        .iter()
        .map(NamespaceId::as_str)
        .collect::<Vec<_>>()
        .join("/")
}

fn path_from_base(base_path: &str, suffix: &str) -> Path {
    if base_path.is_empty() {
        Path::from(suffix)
    } else {
        Path::from(format!("{base_path}/{suffix}"))
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    fn namespace(segments: &[&str]) -> NamespacePath {
        NamespacePath::new(
            segments
                .iter()
                .map(|segment| NamespaceId::new(*segment).unwrap()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn registers_and_lists_native_graph_scopes_idempotently() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            object_store,
        );
        let first = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-a"]),
            GraphId::new("hydradb").unwrap(),
        );
        let second = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-b"]),
            GraphId::new("hydradb").unwrap(),
        );
        let reserved_looking = GraphScope::new(
            namespace(&["production", "_root"]),
            GraphId::new("hydradb").unwrap(),
        );

        directory.register(&second).await.unwrap();
        directory.register(&first).await.unwrap();
        directory.register(&first).await.unwrap();
        directory.register(&reserved_looking).await.unwrap();

        assert_eq!(
            directory.list().await.unwrap(),
            vec![reserved_looking, first, second]
        );
    }

    #[tokio::test]
    async fn rejects_scopes_outside_the_configured_graph_hierarchy() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            object_store,
        );
        let wrong_graph = GraphScope::new(
            namespace(&["production", "tenant-a"]),
            GraphId::new("other").unwrap(),
        );

        assert!(matches!(
            directory.register(&wrong_graph).await,
            Err(GraphError::GraphScopeMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn change_notifications_are_unique_and_cleared_exactly() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            Arc::clone(&object_store),
        );
        let scope = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-a"]),
            GraphId::new("hydradb").unwrap(),
        );

        directory.notify_changed(&scope, "cell-0", 7).await.unwrap();
        directory.notify_changed(&scope, "cell-0", 9).await.unwrap();
        let legacy_location = directory.change_path(&scope, Ulid::new());
        object_store
            .put_opts(
                &legacy_location,
                Bytes::from_static(SCOPE_CHANGE_VERSION.as_bytes()).into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        let changes = directory.list_changes().await.unwrap();
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().all(|change| change.scope == scope));
        assert_eq!(
            changes
                .iter()
                .filter_map(|change| change.sequence)
                .collect::<Vec<_>>(),
            vec![7, 9]
        );
        let legacy = changes
            .iter()
            .find(|change| change.location == legacy_location)
            .unwrap();
        assert_eq!(legacy.cell_id, None);
        assert_eq!(legacy.sequence, None);

        directory.clear_changes(&changes[..1]).await.unwrap();
        assert_eq!(directory.list_changes().await.unwrap(), changes[1..]);
        directory.clear_changes(&changes[1..]).await.unwrap();
        assert!(directory.list_changes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn change_acknowledgements_only_advance() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let directory = ObjectStoreGraphScopeDirectory::new(
            "graph/data",
            namespace(&["production"]),
            GraphId::new("hydradb").unwrap(),
            object_store,
        );
        let scope = GraphScope::new(
            namespace(&["production", "tenant-a", "collection-a"]),
            GraphId::new("hydradb").unwrap(),
        );

        assert_eq!(
            directory.covered_sequence(&scope, "cell-0").await.unwrap(),
            None
        );
        directory
            .acknowledge_sequence(&scope, "cell-0", 12)
            .await
            .unwrap();
        directory
            .acknowledge_sequence(&scope, "cell-0", 8)
            .await
            .unwrap();
        assert_eq!(
            directory.covered_sequence(&scope, "cell-0").await.unwrap(),
            Some(12)
        );
    }
}
