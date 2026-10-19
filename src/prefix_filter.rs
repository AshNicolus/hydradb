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

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{bytes::Bytes, FilterQuery};

    async fn scan_keys(db: &slatedb::Db, prefix: &[u8]) -> Vec<Bytes> {
        let mut iter = db.scan_prefix(prefix, ..).await.unwrap();
        let mut keys = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            keys.push(row.key);
        }
        keys
    }

    async fn flush(db: &slatedb::Db) {
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn incident_prefix_filter_skips_empty_ssts_and_preserves_rolling_compatibility() {
        use slatedb::config::Settings;
        use slatedb::object_store::memory::InMemory;
        use slatedb_common::metrics::{DefaultMetricsRecorder, MetricsRecorder};

        let mut measurements = Vec::new();
        for enabled in [false, true] {
            let store = Arc::new(InMemory::new());
            let recorder = Arc::new(DefaultMetricsRecorder::new());
            let mut builder = slatedb::Db::builder("prefix-filter-replay", store.clone())
                .with_settings(Settings {
                    compactor_options: None,
                    ..Default::default()
                })
                .with_metrics_recorder(recorder.clone() as Arc<dyn MetricsRecorder>);
            if enabled {
                builder = builder.with_filter_policies(graph_filter_policies());
            }
            let db = builder.build().await.unwrap();
            // Overlapping SST ranges: range bounds alone cannot eliminate the
            // absent odd-numbered vertices between these even-numbered ones.
            // Each SST exceeds SlateDB's default 1,000-key filter threshold.
            for table in 0..4 {
                let mut batch = slatedb::WriteBatch::new();
                for source in (0..2400).step_by(2) {
                    batch.put(
                        crate::keys::out_edge("cell-0", "TYPE", source, table).as_bytes(),
                        b"edge",
                    );
                }
                db.write(batch).await.unwrap();
                flush(&db).await;
            }
            let before = crate::core::config::collect_storage_metrics(Some(&recorder), None);
            for source in (1..64).step_by(2) {
                assert!(scan_keys(
                    &db,
                    crate::keys::out_prefix("cell-0", "TYPE", source).as_bytes()
                )
                .await
                .is_empty());
            }
            let after = crate::core::config::collect_storage_metrics(Some(&recorder), None);
            let positives = after.sst_filter_prefix_positives - before.sst_filter_prefix_positives;
            let negatives = after.sst_filter_prefix_negatives - before.sst_filter_prefix_negatives;
            measurements.push((positives, negatives));
            assert_eq!(
                scan_keys(
                    &db,
                    crate::keys::out_prefix("cell-0", "TYPE", 18).as_bytes()
                )
                .await
                .len(),
                4
            );
            // A prefix shorter than the extracted vertex prefix must not use
            // that filter to omit matching keys.
            assert_eq!(scan_keys(&db, b"cell/cell-0/e/out/TYPE/").await.len(), 4800);
            db.close().await.unwrap();

            // Upgrade reads legacy SSTs; rollback reads SSTs containing the new
            // policy using the retained whole-key filter. Both must see every
            // record, including a subsequently deleted one disappearing.
            let mut builder = slatedb::Db::builder("prefix-filter-replay", store.clone())
                .with_settings(Settings {
                    compactor_options: None,
                    ..Default::default()
                });
            if !enabled {
                builder = builder.with_filter_policies(graph_filter_policies());
            }
            let reopened = builder.build().await.unwrap();
            assert_eq!(
                scan_keys(&reopened, b"cell/cell-0/e/out/TYPE/").await.len(),
                4800
            );
            let deleted = crate::keys::out_edge("cell-0", "TYPE", 18, 0);
            reopened.delete(deleted.as_bytes()).await.unwrap();
            flush(&reopened).await;
            assert_eq!(
                scan_keys(
                    &reopened,
                    crate::keys::out_prefix("cell-0", "TYPE", 18).as_bytes()
                )
                .await
                .len(),
                3
            );
            reopened.close().await.unwrap();

            let reader = slatedb::DbReader::builder("prefix-filter-replay", store)
                .with_filter_policies(graph_filter_policies())
                .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
                .build()
                .await
                .unwrap();
            let mut iter = reader
                .scan_prefix(b"cell/cell-0/e/out/TYPE/".as_slice(), ..)
                .await
                .unwrap();
            let mut count = 0;
            while let Some(row) = iter.next().await.unwrap() {
                assert_ne!(row.key.as_ref(), deleted.as_bytes());
                count += 1;
            }
            assert_eq!(count, 4799);
            drop(iter);
            reader.close().await.unwrap();
        }
        eprintln!(
            "incident prefix SST probes (positive, negative): baseline={:?}, filtered={:?}",
            measurements[0], measurements[1]
        );
        assert_eq!(measurements[0].1, 0);
        assert!(measurements[0].0 >= 32);
        assert!(measurements[1].1 >= 32);
        assert!(
            measurements[1].0 < measurements[0].0 / 4,
            "{measurements:?}"
        );
    }

    #[test]
    fn incident_prefix_filter_never_rejects_a_matching_key_or_prefix() {
        let keys = [
            crate::keys::out_edge("cell-0", "TYPE", 17, 31),
            crate::keys::in_edge("cell-0", "TYPE", 31, 17),
            crate::keys::out_segment("cell-0", "TYPE", 17, 5, "segment"),
            crate::keys::out_segment_tombstone("cell-0", "TYPE", 17, 31),
            crate::keys::relationship("cell-0", "TYPE", 17, 31, 2),
            crate::keys::vertex("cell-0", 17),
            crate::keys::matrix_dirty("cell-0", "TYPE"),
            "unknown/family/key".into(),
        ];
        for policy in graph_filter_policies() {
            let mut builder = policy.builder();
            for key in &keys {
                builder.add_entry(&slatedb::RowEntry {
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    value: slatedb::ValueDeletable::Value(Bytes::new()),
                    seq: 1,
                    create_ts: None,
                    expire_ts: None,
                });
            }
            let mut encoded = Vec::new();
            builder.build().encode(&mut encoded);
            let filter = policy.decode(&encoded);
            for key in &keys {
                assert!(
                    filter.might_match(&FilterQuery::point(Bytes::copy_from_slice(key.as_bytes())))
                );
                for end in 0..=key.len() {
                    let prefix = Bytes::copy_from_slice(&key.as_bytes()[..end]);
                    assert!(
                        filter.might_match(&FilterQuery::prefix(prefix)),
                        "{} rejected prefix {:?}",
                        policy.name(),
                        &key[..end]
                    );
                }
            }
        }
    }

    #[test]
    fn incident_prefix_extraction_is_stable_for_every_key_extension() {
        let extractor = IncidentPrefixExtractor;
        for prefix in [
            crate::keys::out_prefix("cell-0", "TYPE", 17),
            crate::keys::in_prefix("cell-0", "TYPE", 17),
            crate::keys::out_segment_src_prefix("cell-0", "TYPE", 17),
            crate::keys::out_segment_tombstone_src_prefix("cell-0", "TYPE", 17),
            "cell/cell-0/rel/TYPE/00000000000000000017/".into(),
        ] {
            for suffix in [
                "",
                "00000000000000000031",
                "31/45",
                "arbitrary/extra/components",
            ] {
                let key = format!("{prefix}{suffix}");
                for target in [
                    PrefixTarget::Point(Bytes::from(key.clone())),
                    PrefixTarget::Prefix(Bytes::from(key)),
                ] {
                    assert_eq!(extractor.prefix_len(&target), Some(prefix.len()));
                }
            }
            for end in 0..prefix.len() {
                assert_eq!(
                    extractor.prefix_len(&PrefixTarget::Prefix(Bytes::copy_from_slice(
                        &prefix.as_bytes()[..end]
                    ))),
                    None
                );
            }
        }
    }
}
