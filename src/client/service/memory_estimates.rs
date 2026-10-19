//! Capacity-based estimates of the owned buffers we can inspect without allocating.
//! BTree node overhead and allocator rounding are deliberately excluded.
use super::*;

fn scalar(value: &VertexPropertyValue) -> usize {
    match value {
        VertexPropertyValue::String(value) => value.capacity(),
        _ => 0,
    }
}
// Bound estimator recursion independently of transport/parser validation.
const MAX_ESTIMATE_DEPTH: usize = 64;
fn parameters(values: &BTreeMap<String, QueryParameterValue>) -> usize {
    parameters_at_depth(values, 0)
}
fn parameters_at_depth(values: &BTreeMap<String, QueryParameterValue>, depth: usize) -> usize {
    values
        .iter()
        .map(|(key, value)| {
            key.capacity()
                + std::mem::size_of::<(String, QueryParameterValue)>()
                + if depth < MAX_ESTIMATE_DEPTH {
                    parameter_at_depth(value, depth + 1)
                } else {
                    0
                }
        })
        .sum()
}
#[cfg(test)]
fn parameter(value: &QueryParameterValue) -> usize {
    parameter_at_depth(value, 0)
}
fn parameter_at_depth(value: &QueryParameterValue, depth: usize) -> usize {
    match value {
        QueryParameterValue::Scalar(value) => scalar(value),
        QueryParameterValue::List(values) => {
            values.capacity() * std::mem::size_of::<QueryParameterValue>()
                + if depth < MAX_ESTIMATE_DEPTH {
                    values
                        .iter()
                        .map(|v| parameter_at_depth(v, depth + 1))
                        .sum::<usize>()
                } else {
                    0
                }
        }
        QueryParameterValue::Map(values) => parameters_at_depth(values, depth),
    }
}
fn properties(values: &BTreeMap<String, VertexPropertyValue>) -> usize {
    values
        .iter()
        .map(|(key, value)| {
            std::mem::size_of::<(String, VertexPropertyValue)>() + key.capacity() + scalar(value)
        })
        .sum()
}
fn metadata(value: &VertexMetadata) -> usize {
    value
        .labels
        .iter()
        .map(|label| std::mem::size_of::<String>() + label.capacity())
        .sum::<usize>()
        + properties(&value.properties)
}
fn vec_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity() * std::mem::size_of::<T>()
}
fn policy(value: &QueryBatchMergePolicy) -> usize {
    value.update_if_newer_by.capacity()
        + value
            .create_only_properties
            .iter()
            .map(|key| std::mem::size_of::<String>() + key.capacity())
            .sum::<usize>()
}
pub(super) fn request(value: &ClientQueryRequest) -> u64 {
    (value.query.capacity() + value.query_id.capacity() + parameters(&value.parameters)) as u64
}
pub(super) fn bound(
    scalars: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> u64 {
    (properties(scalars)
        + lists
            .iter()
            .map(|(key, values)| {
                std::mem::size_of::<(String, Vec<VertexPropertyValue>)>()
                    + key.capacity()
                    + vec_bytes(values)
                    + values.iter().map(scalar).sum::<usize>()
            })
            .sum::<usize>()) as u64
}
pub(super) fn batch(value: &Option<QueryBatchOperation>) -> u64 {
    use QueryBatchOperation::*;
    let Some(value) = value else { return 0 };
    (match value {
        OutNeighbors {
            edge_type, sources, ..
        } => edge_type.capacity() + vec_bytes(sources),
        CreateEdges { edge_type, edges } | DeleteEdges { edge_type, edges } => {
            edge_type.capacity() + vec_bytes(edges)
        }
        DeleteVertices { vertices, .. } => vec_bytes(vertices),
        DeleteIsolatedVertices { candidates, .. } => {
            vec_bytes(candidates)
                + candidates
                    .iter()
                    .map(|v| metadata(&v.path_node_constraints))
                    .sum::<usize>()
        }
        DeleteVerticesAndIsolatedCandidates {
            detach_vertices,
            isolated_candidates,
            ..
        } => {
            vec_bytes(detach_vertices)
                + vec_bytes(isolated_candidates)
                + isolated_candidates
                    .iter()
                    .map(|v| metadata(&v.path_node_constraints))
                    .sum::<usize>()
        }
        DeleteRelationshipsByProperty {
            edge_type,
            property,
            values,
        } => {
            edge_type.capacity()
                + property.capacity()
                + vec_bytes(values)
                + values.iter().map(scalar).sum::<usize>()
        }
        CreateEdgesBetweenLabeledVertices {
            edge_type,
            edges,
            source_label,
            destination_label,
        } => {
            edge_type.capacity()
                + vec_bytes(edges)
                + source_label.capacity()
                + destination_label.capacity()
        }
        UpsertVertices { vertices } => {
            vec_bytes(vertices)
                + vertices
                    .iter()
                    .map(|v| metadata(&v.metadata))
                    .sum::<usize>()
        }
        GuardedUpsertVertices {
            vertices,
            merge_policy,
        } => {
            vec_bytes(vertices)
                + vertices
                    .iter()
                    .map(|v| metadata(&v.metadata))
                    .sum::<usize>()
                + policy(merge_policy)
        }
        CreateRelationshipsBetweenLabeledVertices {
            edge_type,
            relationships,
            source_label,
            destination_label,
        } => {
            edge_type.capacity()
                + vec_bytes(relationships)
                + relationships
                    .iter()
                    .map(|v| properties(&v.metadata.properties))
                    .sum::<usize>()
                + source_label.capacity()
                + destination_label.capacity()
        }
        MergeRelationshipsBetweenLabeledVertices {
            edge_type,
            relationships,
            source_label,
            destination_label,
        } => {
            edge_type.capacity()
                + vec_bytes(relationships)
                + relationships
                    .iter()
                    .map(|v| properties(&v.metadata.properties))
                    .sum::<usize>()
                + source_label.capacity()
                + destination_label.capacity()
        }
        GuardedMergeRelationshipsBetweenLabeledVertices {
            edge_type,
            relationships,
            source_label,
            destination_label,
            merge_policy,
        } => {
            edge_type.capacity()
                + vec_bytes(relationships)
                + relationships
                    .iter()
                    .map(|v| properties(&v.metadata.properties))
                    .sum::<usize>()
                + source_label.capacity()
                + destination_label.capacity()
                + policy(merge_policy)
        }
    }) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_parameters_measure_spare_capacity_and_owned_copies() {
        let mut value = String::with_capacity(4096);
        value.push('x');
        let value = QueryParameterValue::List(vec![QueryParameterValue::Scalar(
            VertexPropertyValue::String(value),
        )]);
        assert!(parameter(&value) >= 4096);
        let values = BTreeMap::from([("payload".to_string(), value)]);
        assert!(parameters(&values) >= 4096);
        assert!(parameters(&values) > parameters(&values.clone()));
    }
}
