//! Skip SSTs that cannot contain a requested vertex's incident records.
use std::sync::Arc;

use slatedb::{BloomFilterPolicy, FilterPolicy, PrefixExtractor, PrefixTarget};

pub(crate) fn graph_filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![
        // Keep the existing policy name so old SSTs and old readers retain
        // their whole-key Bloom filter during a rolling upgrade or rollback.
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(
            BloomFilterPolicy::new(10)
                .with_whole_key_filtering(false)
                .with_prefix_extractor(Arc::new(IncidentPrefixExtractor)),
        ),
    ]
}

struct IncidentPrefixExtractor;

impl PrefixExtractor for IncidentPrefixExtractor {
    fn name(&self) -> &str {
        // Persisted in SST metadata. Change the name if extraction changes.
        "hydradb-incident-prefix-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let input = match target {
            PrefixTarget::Point(input) | PrefixTarget::Prefix(input) => input.as_ref(),
        };
        let cell_len = crate::locality::locality_cell_prefix_len(input)?;
        let suffix = &input[cell_len..];
        let family = [
            b"e/out/".as_slice(),
            b"e/in/",
            b"seg/out/",
            b"seg/tomb/out/",
            b"rel/",
        ]
        .into_iter()
        .find(|family| suffix.starts_with(family))?;
        let start = cell_len + family.len();
        // Exactly two complete components: edge type and source/destination.
        // The same prefix is hashed for every possible key extension. Broader
        // scans return None and must continue scanning conservatively.
        let type_end = input[start..].iter().position(|byte| *byte == b'/')? + start;
        let vertex_start = type_end + 1;
        let vertex_end = input[vertex_start..]
            .iter()
            .position(|byte| *byte == b'/')?
            + vertex_start;
        Some(vertex_end + 1)
    }
}
