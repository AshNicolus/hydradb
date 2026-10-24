//! Canonical key/value encoder for offline graph images.
//!
//! The encoder owns HydraDB's storage format while the caller owns external
//! sorting and SlateDB publication. It deliberately emits only durable graph
//! state; request idempotency receipts and topology changelog entries describe
//! online history and do not belong in a snapshot image.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

#[cfg(feature = "opencypher")]
use crate::codec::encode_query_stats_record;
use crate::codec::{
    encode_edge_record, encode_relationship_record, encode_u64, encode_vertex_metadata,
    encode_vertex_property_value_key, validate_component,
};
use crate::{
    keys, EdgeRecord, GraphError, RelationshipId, RelationshipMutation, Result, VertexId,
    VertexMetadata, VertexPropertyValue,
};
#[cfg(feature = "opencypher")]
use crate::{QueryStatsBloom, QueryStatsDirection, QueryStatsRecord};

/// One final key/value pair for a new SlateDB image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineImageRow {
    pub key: Bytes,
    pub value: Bytes,
}

impl OfflineImageRow {
    fn new(key: String, value: impl Into<Bytes>) -> Self {
        Self {
            key: Bytes::from(key),
            value: value.into(),
        }
    }
}

/// Stateful encoder for one cell's final graph state.
pub struct OfflineGraphImageEncoder {
    cell_id: String,
    write_reverse_index: bool,
    structural_edges: BTreeSet<(String, VertexId, VertexId)>,
    out_degree: BTreeMap<(String, VertexId), u64>,
    in_degree: BTreeMap<(String, VertexId), u64>,
    relationship_counts: BTreeMap<(String, VertexId, VertexId), u64>,
    merge_identities: BTreeMap<String, Option<RelationshipId>>,
    max_relationship_id: RelationshipId,
}

impl OfflineGraphImageEncoder {
    pub fn new(cell_id: impl Into<String>, write_reverse_index: bool) -> Result<Self> {
        let cell_id = cell_id.into();
        validate_component("cell_id", &cell_id)?;
        Ok(Self {
            cell_id,
            write_reverse_index,
            structural_edges: BTreeSet::new(),
            out_degree: BTreeMap::new(),
            in_degree: BTreeMap::new(),
            relationship_counts: BTreeMap::new(),
            merge_identities: BTreeMap::new(),
            max_relationship_id: 0,
        })
    }

    /// Encode one vertex and all of its lookup indexes.
    pub fn vertex_rows(
        &self,
        vertex_id: VertexId,
        metadata: &VertexMetadata,
    ) -> Result<Vec<OfflineImageRow>> {
        for label in &metadata.labels {
            validate_component("label", label)?;
        }
        for property in metadata.properties.keys() {
            validate_component("property", property)?;
        }
        let mut rows = Vec::with_capacity(1 + metadata.labels.len() + metadata.properties.len());
        rows.push(OfflineImageRow::new(
            keys::vertex(&self.cell_id, vertex_id),
            encode_vertex_metadata(metadata),
        ));
        for label in &metadata.labels {
            rows.push(OfflineImageRow::new(
                keys::vertex_label(&self.cell_id, label, vertex_id),
                encode_u64(vertex_id),
            ));
        }
        for (property, value) in &metadata.properties {
            rows.push(OfflineImageRow::new(
                keys::vertex_property_index(
                    &self.cell_id,
                    property,
                    &encode_vertex_property_value_key(value),
                    vertex_id,
                ),
                encode_u64(vertex_id),
            ));
        }
        Ok(rows)
    }

    /// Encode one relationship and its final-state indexes.
    pub fn relationship_rows(
        &mut self,
        relationship: &RelationshipMutation,
    ) -> Result<Vec<OfflineImageRow>> {
        if relationship.cell_id != self.cell_id {
            return Err(GraphError::CorruptValue {
                key: format!(
                    "offline-image/relationship/{}",
                    relationship.relationship_id
                ),
                reason: format!(
                    "relationship cell {} does not match image cell {}",
                    relationship.cell_id, self.cell_id
                ),
            });
        }
        validate_component("edge_type", &relationship.edge_type)?;
        for property in relationship.metadata.properties.keys() {
            validate_component("property", property)?;
        }
        let expected_identity = VertexPropertyValue::Integer(relationship.relationship_id);
        if relationship.metadata.properties.get("id") != Some(&expected_identity) {
            return Err(GraphError::CorruptValue {
                key: format!(
                    "offline-image/relationship/{}",
                    relationship.relationship_id
                ),
                reason: "relationship id property does not match its stable relationship id"
                    .to_string(),
            });
        }

        self.max_relationship_id = self.max_relationship_id.max(relationship.relationship_id);
        *self
            .relationship_counts
            .entry((
                relationship.edge_type.clone(),
                relationship.src,
                relationship.dst,
            ))
            .or_default() += 1;

        let structural = (
            relationship.edge_type.clone(),
            relationship.src,
            relationship.dst,
        );
        let mut rows = Vec::with_capacity(4 + relationship.metadata.properties.len());
        if self.structural_edges.insert(structural) {
            *self
                .out_degree
                .entry((relationship.edge_type.clone(), relationship.src))
                .or_default() += 1;
            if self.write_reverse_index {
                *self
                    .in_degree
                    .entry((relationship.edge_type.clone(), relationship.dst))
                    .or_default() += 1;
            }
            let record = EdgeRecord {
                cell_id: self.cell_id.clone(),
                edge_type: relationship.edge_type.clone(),
                src: relationship.src,
                dst: relationship.dst,
            };
            let value = encode_edge_record(&record);
            rows.push(OfflineImageRow::new(
                keys::out_edge(
                    &self.cell_id,
                    &relationship.edge_type,
                    relationship.src,
                    relationship.dst,
                ),
                value.clone(),
            ));
            if self.write_reverse_index {
                rows.push(OfflineImageRow::new(
                    keys::in_edge(
                        &self.cell_id,
                        &relationship.edge_type,
                        relationship.dst,
                        relationship.src,
                    ),
                    value,
                ));
            }
        }

        let record = crate::RelationshipRecord {
            cell_id: self.cell_id.clone(),
            edge_type: relationship.edge_type.clone(),
            src: relationship.src,
            dst: relationship.dst,
            relationship_id: relationship.relationship_id,
            metadata: relationship.metadata.clone(),
        };
        let record_key = keys::relationship(
            &self.cell_id,
            &relationship.edge_type,
            relationship.src,
            relationship.dst,
            relationship.relationship_id,
        );
        rows.push(OfflineImageRow::new(
            record_key.clone(),
            encode_relationship_record(&record),
        ));
        rows.push(OfflineImageRow::new(
            keys::relationship_id(&self.cell_id, relationship.relationship_id),
            record_key,
        ));
        for (property, value) in &relationship.metadata.properties {
            let encoded = encode_vertex_property_value_key(value);
            rows.push(OfflineImageRow::new(
                keys::relationship_property_index(
                    &self.cell_id,
                    &relationship.edge_type,
                    property,
                    &encoded,
                    relationship.src,
                    relationship.dst,
                    relationship.relationship_id,
                ),
                encode_u64(relationship.relationship_id),
            ));
            if property == "id" {
                let merge_key = keys::relationship_merge_index(
                    &self.cell_id,
                    &relationship.edge_type,
                    property,
                    &encoded,
                    relationship.src,
                    relationship.dst,
                );
                self.merge_identities
                    .entry(merge_key)
                    .and_modify(|identity| *identity = None)
                    .or_insert(Some(relationship.relationship_id));
            }
        }
        Ok(rows)
    }

    /// Encode a point cardinality and its optimizer metadata directly into the image.
    #[cfg(feature = "opencypher")]
    pub fn edge_type_query_stats_rows(
        &self,
        edge_type: &str,
        count: u64,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        validate_component("edge_type", edge_type)?;
        Ok(point_query_stats_rows(
            keys::query_stats_edge_type(&self.cell_id, edge_type),
            count,
            refreshed_at_ms,
        ))
    }

    #[cfg(feature = "opencypher")]
    pub fn vertex_label_query_stats_rows(
        &self,
        label: &str,
        count: u64,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        validate_component("label", label)?;
        Ok(point_query_stats_rows(
            keys::query_stats_vertex_label(&self.cell_id, label),
            count,
            refreshed_at_ms,
        ))
    }

    #[cfg(feature = "opencypher")]
    pub fn vertex_label_intersection_query_stats_rows(
        &self,
        labels: &[String],
        count: u64,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        let labels = canonical_labels(labels, 2, "label intersection")?;
        let labels = labels.iter().map(String::as_str).collect::<Vec<_>>();
        Ok(point_query_stats_rows(
            keys::query_stats_vertex_label_intersection(&self.cell_id, &labels),
            count,
            refreshed_at_ms,
        ))
    }

    #[cfg(feature = "opencypher")]
    pub fn edge_expansion_query_stats_rows(
        &self,
        edge_type: &str,
        direction: QueryStatsDirection,
        source_labels: &[String],
        count: u64,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        validate_component("edge_type", edge_type)?;
        let labels = canonical_labels(source_labels, 1, "edge expansion")?;
        let labels = labels.iter().map(String::as_str).collect::<Vec<_>>();
        Ok(point_query_stats_rows(
            keys::query_stats_edge_expansion(
                &self.cell_id,
                edge_type,
                direction.as_key_component(),
                &labels,
            ),
            count,
            refreshed_at_ms,
        ))
    }

    #[cfg(feature = "opencypher")]
    pub fn vertex_property_histogram_query_stats_rows(
        &self,
        property: &str,
        value_counts: &BTreeMap<VertexPropertyValue, u64>,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        validate_component("property", property)?;
        let buckets = encoded_histogram(value_counts);
        Ok(histogram_query_stats_rows(
            keys::query_stats_vertex_property_histogram(&self.cell_id, property),
            &buckets,
            refreshed_at_ms,
            |encoded| keys::query_stats_vertex_property(&self.cell_id, property, encoded),
        ))
    }

    #[cfg(feature = "opencypher")]
    pub fn edge_property_histogram_query_stats_rows(
        &self,
        edge_type: &str,
        property: &str,
        value_counts: &BTreeMap<VertexPropertyValue, u64>,
        refreshed_at_ms: u64,
    ) -> Result<Vec<OfflineImageRow>> {
        validate_component("edge_type", edge_type)?;
        validate_component("property", property)?;
        let buckets = encoded_histogram(value_counts);
        Ok(histogram_query_stats_rows(
            keys::query_stats_edge_property_histogram(&self.cell_id, edge_type, property),
            &buckets,
            refreshed_at_ms,
            |encoded| keys::query_stats_edge_property(&self.cell_id, edge_type, property, encoded),
        ))
    }

    /// Emit aggregate counters and proven-unique relationship identity indexes.
    pub fn finish_rows(self) -> Vec<OfflineImageRow> {
        let mut rows = Vec::with_capacity(
            self.out_degree.len()
                + self.in_degree.len()
                + self.relationship_counts.len()
                + self.merge_identities.len()
                + usize::from(self.max_relationship_id > 0),
        );
        for ((edge_type, src), count) in self.out_degree {
            rows.push(OfflineImageRow::new(
                keys::degree_out(&self.cell_id, &edge_type, src),
                encode_u64(count),
            ));
        }
        for ((edge_type, dst), count) in self.in_degree {
            rows.push(OfflineImageRow::new(
                keys::degree_in(&self.cell_id, &edge_type, dst),
                encode_u64(count),
            ));
        }
        for ((edge_type, src, dst), count) in self.relationship_counts {
            rows.push(OfflineImageRow::new(
                keys::relationship_count(&self.cell_id, &edge_type, src, dst),
                encode_u64(count),
            ));
        }
        for (key, relationship_id) in self.merge_identities {
            if let Some(relationship_id) = relationship_id {
                rows.push(OfflineImageRow::new(key, encode_u64(relationship_id)));
            }
        }
        if self.max_relationship_id > 0 {
            rows.push(OfflineImageRow::new(
                keys::last_relationship_id(&self.cell_id),
                encode_u64(self.max_relationship_id),
            ));
        }
        rows
    }
}

#[cfg(feature = "opencypher")]
fn canonical_labels(labels: &[String], minimum: usize, purpose: &str) -> Result<Vec<String>> {
    for label in labels {
        validate_component("label", label)?;
    }
    let labels = labels
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if labels.len() < minimum {
        return Err(GraphError::CorruptValue {
            key: format!("offline-image/query-stats/{purpose}"),
            reason: format!("{purpose} requires at least {minimum} distinct label(s)"),
        });
    }
    Ok(labels)
}

#[cfg(feature = "opencypher")]
fn encoded_histogram(value_counts: &BTreeMap<VertexPropertyValue, u64>) -> BTreeMap<String, u64> {
    value_counts
        .iter()
        .map(|(value, count)| (encode_vertex_property_value_key(value), *count))
        .collect()
}

#[cfg(feature = "opencypher")]
fn point_query_stats_rows(key: String, count: u64, refreshed_at_ms: u64) -> Vec<OfflineImageRow> {
    let stats = QueryStatsRecord::point_count(count, 0, refreshed_at_ms);
    vec![
        OfflineImageRow::new(key.clone(), encode_u64(count)),
        OfflineImageRow::new(
            keys::query_stats_record_key(&key),
            encode_query_stats_record(&stats),
        ),
    ]
}

#[cfg(feature = "opencypher")]
fn histogram_query_stats_rows(
    histogram_key: String,
    buckets: &BTreeMap<String, u64>,
    refreshed_at_ms: u64,
    bucket_key: impl Fn(&str) -> String,
) -> Vec<OfflineImageRow> {
    let count = buckets.values().copied().sum::<u64>();
    let mut stats = QueryStatsRecord::histogram(
        count,
        0,
        refreshed_at_ms,
        buckets.len() as u64,
        buckets.values().copied().max().unwrap_or(0),
    );
    stats.bloom = Some(QueryStatsBloom::from_encoded_values(
        buckets.keys().map(String::as_str),
    ));
    let mut rows = vec![
        OfflineImageRow::new(histogram_key.clone(), encode_u64(stats.count)),
        OfflineImageRow::new(
            keys::query_stats_record_key(&histogram_key),
            encode_query_stats_record(&stats),
        ),
    ];
    for (encoded, count) in buckets {
        let key = bucket_key(encoded);
        let bucket_stats = QueryStatsRecord {
            count: *count,
            read_epoch: stats.read_epoch,
            refreshed_at_ms: stats.refreshed_at_ms,
            distinct_values: stats.distinct_values,
            total_values: stats.total_values,
            most_common_count: stats.most_common_count,
            bloom: None,
        };
        rows.push(OfflineImageRow::new(key.clone(), encode_u64(*count)));
        rows.push(OfflineImageRow::new(
            keys::query_stats_record_key(&key),
            encode_query_stats_record(&bucket_stats),
        ));
    }
    rows
}
