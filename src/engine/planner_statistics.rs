//! Planner statistics: one derived object per (scope, cell), counted from index
//! keys alone.
//!
//! The indexer opens scopes read-only, so the counts cannot live under a
//! SlateDB key. They are published next to the store, like the graph index
//! generations, under `{store_path}/_planner_statistics/v1/{cell}/` through
//! [`hydradb_object_publication`]: `current` is the control record (owner,
//! lease, published sequence, payload reference) and the statistics
//! themselves are immutable payloads beside it. Several indexers may build
//! one cell: a build claims the cell for a lease before counting, a stale
//! claimant is fenced, and the published sequence only moves forward, so a
//! build from an older snapshot that finishes last is dropped. The object is
//! derived data: a reader that cannot decode it plans as if it were absent.
//! A bare statistics object at `current`, the layout before the control
//! record, still reads and is replaced by the first publication.
//!
//! One pass reads five key families of a pinned snapshot: the vertex property
//! index, the label index, the out-degree counters, the edge metadata property
//! index and the relationship property index. Each index sorts by value inside a
//! property, so a value's entries form one run: a run's length is that value's
//! exact count, and the first and last run of a type tag are its minimum and
//! maximum. No record is hydrated.
//!
//! Counting is exact only while it is cheap. A property's run is read in full
//! up to [`PlannerStatisticsBuildLimits::exact_entries`]; past that the rest
//! of the run is skipped with one seek and the property is *sampled*: its
//! type tags are probed for their bounds, and a fixed number of windows are
//! read at seek points spread over each tag's range. The windows give the
//! rows per value and the values broad enough to fill a window; what they
//! cannot give -- the property's total -- is left unknown rather than
//! guessed. A label or edge type past the cap is a lower bound and marked
//! sampled, which a reader treats as unknown. Every build is bounded by
//! [`PlannerStatisticsBuildLimits::max_entries`] and fails, publishing
//! nothing, rather than exceed it.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::time::Duration;

use super::*;
use hydradb_object_publication::{
    Acquire, Current, Ownership, PublicationError, PublicationStore, Publish,
};
use slatedb::bytes::Bytes;

use crate::{decode_vertex_property_value_key, graph_now_millis, VertexPropertyValue};

pub const PLANNER_STATISTICS_FORMAT_VERSION: u32 = 1;
/// Durable format identifier; the version is part of it, so a reader rejects
/// a newer object rather than misreading it.
const PLANNER_STATISTICS_MAGIC: &str = "turbolay-planner-statistics-v1";
const PLANNER_STATISTICS_MAGIC_PREFIX: &str = "turbolay-planner-statistics-";
pub const PLANNER_STATISTICS_TOP_VALUES: usize = 32;
/// How long [`GraphShard::publish_planner_statistics`] holds the cell while
/// it publishes one object; only the indexer, which builds first, claims for
/// longer.
const PLANNER_STATISTICS_PUBLISH_LEASE: Duration = Duration::from_secs(60);
/// Saturated values an object may list per property: one list of at most
/// [`PLANNER_STATISTICS_TOP_VALUES`] per index family, and an edge property
/// merges two families (metadata and relationship), so their union fits.
/// Nothing is dropped from the union: a dropped broad value would be
/// estimated as a cheap one.
pub const PLANNER_STATISTICS_SATURATED_VALUES: usize = 2 * PLANNER_STATISTICS_TOP_VALUES;
/// Longer encoded values are still counted, but never listed as a top value,
/// so one huge string cannot bloat the object.
const MAX_TOP_VALUE_KEY_BYTES: usize = 512;
/// Properties past this many (in key order) are dropped; the planner sees
/// them as unknown, which is what it sees today.
const MAX_PROPERTIES_PER_KIND: usize = 4_096;
const PROPERTY_VALUE_TAGS: &[char] = &['b', 'i', 'j', 'n', 's'];

/// Index-derived counts for one cell of one scope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlannerStatistics {
    pub cell_id: String,
    /// Sequence of the snapshot the counts were read from.
    pub storage_sequence: StorageSequence,
    pub built_at_ms: u64,
    pub format_version: u32,
    /// Vertex count per label.
    pub labels: BTreeMap<String, u64>,
    /// Edge count per edge type: the sum of its out-degree counters.
    pub edge_types: BTreeMap<String, u64>,
    pub vertex_properties: BTreeMap<String, PropertyIndexStatistics>,
    /// Keyed by `(edge_type, property)`. Edge metadata and relationship
    /// property entries are merged, so `distinct_values` is an upper bound.
    pub edge_properties: BTreeMap<(String, String), PropertyIndexStatistics>,
    /// Labels whose count in `labels` is a lower bound: the exact cap was
    /// reached and the rest of the run skipped.
    pub labels_sampled: BTreeSet<String>,
    /// Edge types whose count in `edge_types` is a lower bound, likewise.
    pub edge_types_sampled: BTreeSet<String>,
}

/// Counts over one property's index entries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PropertyIndexStatistics {
    pub total_entries: u64,
    pub distinct_values: u64,
    pub most_common_count: u64,
    /// Encoded value key (type tag included) and its exact count, count
    /// descending then key ascending, at most [`PLANNER_STATISTICS_TOP_VALUES`].
    pub top_values: Vec<(String, u64)>,
    /// Per type tag (`b`, `i`, `j`, `n`, `s`). A tag whose only entries are
    /// NaN has no range.
    pub ranges: BTreeMap<char, TagRange>,
    /// `Some` when the property was sampled past the exact cap. Then
    /// `total_entries`, `distinct_values` and `most_common_count` describe
    /// only the exact prefix, `top_values` holds complete runs the windows
    /// saw, and `ranges` carries probed bounds with `entries` unknown (zero).
    pub sample: Option<PropertySample>,
    /// Sampled values whose run overflowed a window: broad, count unknown.
    pub saturated_values: BTreeSet<String>,
}

/// Entries of one type tag and its smallest and largest encoded value key.
/// Byte order equals value order within a tag, so the keys compare directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TagRange {
    pub entries: u64,
    pub min: String,
    pub max: String,
}

/// Bounds on one build.
///
/// Counting is exact while it is cheap and sampled once it is not. A
/// property, label or edge type is read exactly up to `exact_entries`; a
/// property past that is sampled with `sample_windows` seeks of
/// `window_entries` each, and a label or edge type past it is recorded as a
/// lower bound only. `max_entries` bounds the whole build, every family and
/// window included: a build that would read more fails with
/// [`GraphError::AdmissionRejected`] and publishes nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlannerStatisticsBuildLimits {
    pub exact_entries: u64,
    pub sample_windows: u32,
    pub window_entries: u64,
    pub max_entries: u64,
}

impl Default for PlannerStatisticsBuildLimits {
    fn default() -> Self {
        Self {
            exact_entries: 100_000,
            sample_windows: 32,
            window_entries: 2_000,
            max_entries: 2_000_000,
        }
    }
}

/// How a sampled property was read, so a reader can weigh its numbers.
///
/// `exact_entries` were counted from the start of the property's run before
/// the cap; the rest was sampled. `sample_entries` is what the windows read
/// and `complete_runs` how many whole runs of one value fit inside them: their
/// ratio is the sampled rows per value. `windows` counts the windows that
/// held at least one entry. `saturated_dropped` counts broad values the
/// windows saw but the object does not list (too long to store, or past the
/// list's cap): while it is zero, a value absent from both lists is an
/// ordinary one and the rows per value apply to it; otherwise it may be a
/// broad value, and its count is unknown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PropertySample {
    pub exact_entries: u64,
    pub sample_entries: u64,
    pub complete_runs: u64,
    pub windows: u32,
    pub saturated_dropped: u32,
}

/// Fed one window at a time; a value whose run crosses a window edge is
/// partial, so only runs that both start and end inside a window give a
/// count. A partial run at least this long is a broad value, listed as
/// saturated: its count is unknown but large.
const SATURATED_PARTIAL_RUN_DIVISOR: u64 = 4;
/// Space for the heavy-hitter list a sampled property keeps while its windows
/// are read; the top values are cut from it at the end.
const SAMPLED_TOP_CANDIDATES: usize = 128;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
/// Nibbles of a string key's payload that seek points interpolate over, and
/// that the maximum-string probe resolves. Deeper than this, two strings
/// sort by a prefix the windows cannot tell apart anyway.
const STRING_SEEK_NIBBLES: usize = 16;

/// Entries read so far against the build's cap.
struct BuildBudget {
    read: u64,
    max: u64,
}

impl BuildBudget {
    fn charge(&mut self, entries: u64) -> Result<()> {
        self.read = self.read.saturating_add(entries);
        if self.read > self.max {
            return Err(GraphError::AdmissionRejected {
                operation: "planner_statistics_build",
                actual: self.read,
                limit: self.max,
            });
        }
        Ok(())
    }
}

impl GraphShard {
    /// Count every planner statistic of `cell_id` from one pinned snapshot,
    /// reading index keys only, under the default limits.
    pub async fn build_planner_statistics(&self, cell_id: &str) -> Result<PlannerStatistics> {
        self.build_planner_statistics_with(cell_id, &PlannerStatisticsBuildLimits::default())
            .await
    }

    /// [`Self::build_planner_statistics`] under `limits`.
    pub async fn build_planner_statistics_with(
        &self,
        cell_id: &str,
        limits: &PlannerStatisticsBuildLimits,
    ) -> Result<PlannerStatistics> {
        validate_component("cell_id", cell_id)?;
        let snapshot = self.db.snapshot().await?;
        let storage_sequence = snapshot.seq();
        let mut budget = BuildBudget {
            read: 0,
            max: limits.max_entries,
        };
        let mut statistics = GraphStore::scope_snapshot(snapshot, async {
            let mut statistics = PlannerStatistics {
                cell_id: cell_id.to_string(),
                storage_sequence,
                format_version: PLANNER_STATISTICS_FORMAT_VERSION,
                ..PlannerStatistics::default()
            };
            let vertex_prefix = crate::keys::vertex_property_index_cell_prefix(cell_id);
            statistics.vertex_properties = self
                .count_property_index(&vertex_prefix, 3, 1, limits, &mut budget)
                .await?
                .into_iter()
                .map(|(mut group, statistics)| (group.remove(0), statistics))
                .collect();
            let (labels, labels_sampled) = self
                .count_id_runs(
                    &crate::keys::vertex_label_cell_prefix(cell_id),
                    false,
                    limits,
                    &mut budget,
                )
                .await?;
            statistics.labels = labels;
            statistics.labels_sampled = labels_sampled;
            let (edge_types, edge_types_sampled) = self
                .count_id_runs(
                    &crate::keys::degree_out_cell_prefix(cell_id),
                    true,
                    limits,
                    &mut budget,
                )
                .await?;
            statistics.edge_types = edge_types;
            statistics.edge_types_sampled = edge_types_sampled;
            let edge_metadata = self
                .count_edge_property_index(
                    &crate::keys::edge_property_index_cell_prefix(cell_id),
                    5,
                    limits,
                    &mut budget,
                )
                .await?;
            let relationships = self
                .count_edge_property_index(
                    &crate::keys::relationship_property_index_cell_prefix(cell_id),
                    6,
                    limits,
                    &mut budget,
                )
                .await?;
            statistics.edge_properties =
                merge_edge_property_statistics(edge_metadata, relationships);
            Ok::<_, GraphError>(statistics)
        })
        .await?;
        statistics.built_at_ms = graph_now_millis();
        Ok(statistics)
    }

    /// The publication holding `cell_id`'s statistics. A statistics object
    /// at `current` from before the control record is retained on the first
    /// claim, with its own snapshot sequence, so it stays in the forward-only
    /// order and a build that fails after claiming leaves it to readers.
    fn planner_statistics_publication(&self, cell_id: &str) -> PublicationStore {
        PublicationStore::new(
            Arc::clone(self.db.object_store()),
            planner_statistics_root(self.db.store_path(), cell_id),
        )
        .with_legacy_sequence(Arc::new(|bytes: &Bytes| {
            decode_planner_statistics("current", bytes)
                .ok()
                .map(|statistics| statistics.storage_sequence)
        }))
    }

    /// The published statistics of `cell_id`, or `None` when none exist.
    /// A claimed cell with nothing published yet is `None` too.
    pub async fn read_planner_statistics(
        &self,
        cell_id: &str,
    ) -> Result<Option<PlannerStatistics>> {
        validate_component("cell_id", cell_id)?;
        let path = planner_statistics_path(self.db.store_path(), cell_id);
        let current = self
            .planner_statistics_publication(cell_id)
            .read_current()
            .await
            .map_err(|error| publication_error(error, &path))?;
        let value = match current {
            None | Some(Current::Unpublished(_)) => return Ok(None),
            Some(Current::Published { payload, .. }) => payload,
            // The layout before the control record, not yet claimed by any
            // indexer: the statistics object itself at `current`.
            Some(Current::Opaque(bytes)) => bytes,
        };
        let statistics = decode_planner_statistics(path.as_ref(), &value)?;
        if statistics.cell_id != cell_id {
            return corrupt(path.as_ref(), "planner statistics cell does not match path");
        }
        Ok(Some(statistics))
    }

    /// Claim `cell_id` for a statistics build, as `owner`, for `lease`.
    ///
    /// `None` when another owner holds a live claim: the caller leaves the
    /// cell to it. The claim outlives a crashed holder by at most `lease`,
    /// and a holder that outlives its lease is fenced on its next write.
    pub async fn claim_planner_statistics_build(
        &self,
        cell_id: &str,
        owner: &str,
        lease: Duration,
    ) -> Result<Option<PlannerStatisticsPublisher>> {
        validate_component("cell_id", cell_id)?;
        let path = planner_statistics_path(self.db.store_path(), cell_id);
        match self
            .planner_statistics_publication(cell_id)
            .acquire(owner, lease)
            .await
        {
            Ok(Acquire::Acquired(ownership)) => Ok(Some(PlannerStatisticsPublisher {
                cell_id: cell_id.to_string(),
                path,
                ownership: *ownership,
            })),
            Ok(Acquire::Held { .. }) => Ok(None),
            Err(error) => Err(publication_error(error, &path)),
        }
    }

    /// Publish `statistics` in one step: claim the cell, publish, release.
    ///
    /// For a single writer, such as a test or a tool; the indexer claims
    /// before it counts so the claim covers the build. A cell another owner
    /// holds is a conflict, not a wait.
    pub async fn publish_planner_statistics(
        &self,
        statistics: &PlannerStatistics,
    ) -> Result<PlannerStatisticsPublish> {
        let owner = format!("publish-{}", ulid::Ulid::new());
        let Some(mut publisher) = self
            .claim_planner_statistics_build(
                &statistics.cell_id,
                &owner,
                PLANNER_STATISTICS_PUBLISH_LEASE,
            )
            .await?
        else {
            return Err(GraphError::ConditionalWriteConflict {
                operation: "planner_statistics_publish",
                key: planner_statistics_path(self.db.store_path(), &statistics.cell_id).to_string(),
            });
        };
        let outcome = publisher.publish(statistics).await;
        // The publication stands whatever the release does: a claim that
        // cannot be given back expires with its lease.
        if let Err(error) = publisher.release().await {
            tracing::warn!(
                hydradb.cell_id = %statistics.cell_id,
                error = %error,
                "could not release the planner statistics claim after publishing; it expires with its lease"
            );
        }
        outcome
    }

    /// Count an edge property index whose keys are `{type}/{prop}/{encoded}/...`
    /// with `components` segments after `prefix`.
    async fn count_edge_property_index(
        &self,
        prefix: &str,
        components: usize,
        limits: &PlannerStatisticsBuildLimits,
        budget: &mut BuildBudget,
    ) -> Result<BTreeMap<(String, String), PropertyIndexStatistics>> {
        Ok(self
            .count_property_index(prefix, components, 2, limits, budget)
            .await?
            .into_iter()
            .map(|(group, statistics)| {
                let [edge_type, property]: [String; 2] = group
                    .try_into()
                    .expect("edge property group has two components");
                ((edge_type, property), statistics)
            })
            .collect())
    }

    /// Count one property index family. A property is the first `group_len`
    /// key components, before the encoded value: one for vertices, two
    /// (`type/prop`) for edges. Each property is exact up to the cap; past
    /// it the rest of its run is skipped with a seek and the property is
    /// sampled afterwards.
    async fn count_property_index(
        &self,
        prefix: &str,
        components: usize,
        group_len: usize,
        limits: &PlannerStatisticsBuildLimits,
        budget: &mut BuildBudget,
    ) -> Result<BTreeMap<Vec<String>, PropertyIndexStatistics>> {
        let mut counter = PropertyRunCounter::new(limits.exact_entries);
        let mut capped = Vec::new();
        let mut start = None;
        'scan: loop {
            let mut iter = self.scan_remote_prefix_from(prefix, start.take()).await?;
            while let Some(kv) = iter.next().await? {
                budget.charge(1)?;
                let key = String::from_utf8_lossy(&kv.key);
                let parts = index_key_parts(&key, prefix, components)?;
                if let Pushed::Capped(group) =
                    counter.push(&key, &parts[..group_len], parts[group_len])?
                {
                    // The smallest key after this property's run: `/` sorts
                    // below `0`, so `{group}0` is past every `{group}/...`.
                    start = Some(format!("{}0", group.join("/")).into_bytes());
                    capped.push(group);
                    continue 'scan;
                }
            }
            break;
        }
        let mut finished = counter.finish(prefix)?;
        for group in capped {
            let Some(statistics) = finished.get_mut(&group) else {
                continue;
            };
            let group_prefix = format!("{prefix}{}/", group.join("/"));
            self.sample_property(&group_prefix, statistics, limits, budget)
                .await?;
        }
        Ok(finished)
    }

    /// Count `{name}/{id}` runs: labels (one entry per vertex) and out-degree
    /// counters (one per source, valued with its degree, summed when
    /// `weighted`). A name past the cap is a lower bound and listed as
    /// sampled; the planner treats its count as unknown.
    async fn count_id_runs(
        &self,
        prefix: &str,
        weighted: bool,
        limits: &PlannerStatisticsBuildLimits,
        budget: &mut BuildBudget,
    ) -> Result<(BTreeMap<String, u64>, BTreeSet<String>)> {
        let mut counts = BTreeMap::<String, u64>::new();
        let mut entries = BTreeMap::<String, u64>::new();
        let mut sampled = BTreeSet::new();
        let mut start = None;
        'scan: loop {
            let mut iter = self.scan_remote_prefix_from(prefix, start.take()).await?;
            while let Some(kv) = iter.next().await? {
                budget.charge(1)?;
                let key = String::from_utf8_lossy(&kv.key);
                let parts = index_key_parts(&key, prefix, 2)?;
                let name = parts[0];
                let weight = if weighted {
                    decode_u64(&key, &kv.value)?
                } else {
                    1
                };
                add_count(&mut counts, name, weight);
                let seen = entries.entry(name.to_string()).or_default();
                *seen += 1;
                if *seen >= limits.exact_entries {
                    sampled.insert(name.to_string());
                    start = Some(format!("{name}0").into_bytes());
                    continue 'scan;
                }
            }
            break;
        }
        Ok((counts, sampled))
    }

    /// Sample one property whose run exceeded the cap. Every type tag present
    /// is probed for its bounds, the windows are spread over each tag's
    /// range, and `statistics` (the exact prefix of the run) is replaced by
    /// what the windows show.
    async fn sample_property(
        &self,
        group_prefix: &str,
        statistics: &mut PropertyIndexStatistics,
        limits: &PlannerStatisticsBuildLimits,
        budget: &mut BuildBudget,
    ) -> Result<()> {
        let mut tags = Vec::new();
        for tag in PROPERTY_VALUE_TAGS {
            let Some(min) = self
                .first_value_at_or_after(group_prefix, &tag.to_string(), *tag, budget)
                .await?
            else {
                continue;
            };
            let max = self
                .probe_max_value(group_prefix, *tag, &min, budget)
                .await?;
            tags.push((*tag, min, max));
        }
        let mut sampler = WindowSampler::new(limits.window_entries);
        let windows_per_tag = (limits.sample_windows as usize / tags.len().max(1)).max(1);
        for (tag, min, max) in &tags {
            let seeks = seek_points(*tag, min, max, windows_per_tag);
            for seek in seeks {
                let mut iter = self
                    .scan_remote_prefix_from(group_prefix, Some(seek.into_bytes()))
                    .await?;
                sampler.open_window();
                let mut read = 0;
                while read < limits.window_entries {
                    let Some(kv) = iter.next().await? else {
                        break;
                    };
                    budget.charge(1)?;
                    read += 1;
                    let key = String::from_utf8_lossy(&kv.key);
                    let encoded = key
                        .strip_prefix(group_prefix)
                        .and_then(|rest| rest.split('/').next())
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| GraphError::CorruptValue {
                            key: key.to_string(),
                            reason: "property index key has no value segment".to_string(),
                        })?;
                    // The window ran off the end of its tag: those entries
                    // belong to the next tag's windows. The read is charged
                    // and the window ends naturally, not cut.
                    if !encoded.starts_with(*tag) {
                        break;
                    }
                    sampler.push(encoded);
                }
                sampler.close_window(read == limits.window_entries);
            }
        }
        sampler.finish(statistics, tags);
        Ok(())
    }

    /// The encoded value of the first entry of `group_prefix` at or after
    /// `suffix`, if it carries `tag`.
    async fn first_value_at_or_after(
        &self,
        group_prefix: &str,
        suffix: &str,
        tag: char,
        budget: &mut BuildBudget,
    ) -> Result<Option<String>> {
        budget.charge(1)?;
        let mut iter = self
            .scan_remote_prefix_from(group_prefix, Some(suffix.as_bytes().to_vec()))
            .await?;
        let Some(kv) = iter.next().await? else {
            return Ok(None);
        };
        let key = String::from_utf8_lossy(&kv.key);
        let encoded = key
            .strip_prefix(group_prefix)
            .and_then(|rest| rest.split('/').next())
            .filter(|value| value.starts_with(tag))
            .map(str::to_string);
        Ok(encoded)
    }

    /// The largest encoded value of `tag` under `group_prefix`, by probing:
    /// a tag's payload orders like a number (decimal or hex of fixed width)
    /// or like hex text, so "is there an entry at or above X" bisects to the
    /// maximum in about 64 probes, each a one-entry scan.
    async fn probe_max_value(
        &self,
        group_prefix: &str,
        tag: char,
        min: &str,
        budget: &mut BuildBudget,
    ) -> Result<String> {
        match tag {
            'b' => Ok(
                match self
                    .first_value_at_or_after(group_prefix, "b1", 'b', budget)
                    .await?
                {
                    Some(value) => value,
                    None => min.to_string(),
                },
            ),
            'i' | 'j' | 'n' => {
                let Some(low) = numeric_payload(tag, min) else {
                    return Ok(min.to_string());
                };
                let mut low = low;
                let mut high = u64::MAX;
                let mut best = min.to_string();
                while low < high {
                    let mid = low + (high - low) / 2 + 1;
                    match self
                        .first_value_at_or_after(group_prefix, &numeric_key(tag, mid), tag, budget)
                        .await?
                    {
                        Some(found) => {
                            low = numeric_payload(tag, &found).unwrap_or(mid).max(mid);
                            best = found;
                        }
                        None => high = mid - 1,
                    }
                }
                Ok(best)
            }
            _ => {
                // Hex text: resolve the largest existing prefix one nibble at
                // a time, each nibble by bisection over the hex alphabet.
                let mut resolved = String::new();
                let mut best = min.to_string();
                for _ in 0..STRING_SEEK_NIBBLES {
                    let mut low = 0usize;
                    let mut high = HEX_DIGITS.len();
                    let mut hit = None;
                    while low < high {
                        let mid = (low + high) / 2;
                        let probe = format!("s{resolved}{}", HEX_DIGITS[mid] as char);
                        match self
                            .first_value_at_or_after(group_prefix, &probe, 's', budget)
                            .await?
                        {
                            Some(found) => {
                                hit = Some((mid, found));
                                low = mid + 1;
                            }
                            None => high = mid,
                        }
                    }
                    let Some((digit, found)) = hit else {
                        break;
                    };
                    resolved.push(HEX_DIGITS[digit] as char);
                    best = found;
                }
                Ok(best)
            }
        }
    }
}

/// The payload of a numeric encoded value as the integer it sorts by.
fn numeric_payload(tag: char, encoded: &str) -> Option<u64> {
    let payload = encoded.strip_prefix(tag)?;
    match tag {
        'i' => payload.parse::<u64>().ok(),
        'j' | 'n' => u64::from_str_radix(payload, 16).ok(),
        _ => None,
    }
}

fn numeric_key(tag: char, payload: u64) -> String {
    match tag {
        'i' => format!("i{payload:020}"),
        _ => format!("{tag}{payload:016x}"),
    }
}

/// The payload of a string encoded value, truncated or padded to
/// [`STRING_SEEK_NIBBLES`], as the integer those nibbles sort by.
fn string_payload(encoded: &str, pad: u8) -> u64 {
    let payload = encoded.strip_prefix('s').unwrap_or("");
    let mut digits = payload
        .bytes()
        .take(STRING_SEEK_NIBBLES)
        .collect::<Vec<_>>();
    digits.resize(STRING_SEEK_NIBBLES, pad);
    digits.iter().fold(0_u64, |value, digit| {
        let nibble = HEX_DIGITS.iter().position(|hex| hex == digit).unwrap_or(0) as u64;
        (value << 4) | nibble
    })
}

/// `count` seek suffixes spread evenly over the values of `tag` between
/// `min` and `max` (inclusive); the first is `min` itself.
fn seek_points(tag: char, min: &str, max: &str, count: usize) -> Vec<String> {
    let count = count.max(1);
    let (low, high, render): (u64, u64, Box<dyn Fn(u64) -> String>) = match tag {
        'b' => {
            return vec!["b0".to_string(), "b1".to_string()]
                .into_iter()
                .take(count)
                .collect()
        }
        'i' | 'j' | 'n' => {
            let (Some(low), Some(high)) = (numeric_payload(tag, min), numeric_payload(tag, max))
            else {
                return vec![min.to_string()];
            };
            (low, high, Box::new(move |value| numeric_key(tag, value)))
        }
        _ => (
            string_payload(min, b'0'),
            string_payload(max, b'f'),
            Box::new(|value| format!("s{value:016x}")),
        ),
    };
    if high <= low || count == 1 {
        return vec![min.to_string()];
    }
    let span = high - low;
    (0..count as u64)
        .map(|index| {
            // `low + span * index / count`, without overflow.
            let offset = (span / count as u64).saturating_mul(index)
                + (span % count as u64).saturating_mul(index) / count as u64;
            if index == 0 {
                min.to_string()
            } else {
                render(low.saturating_add(offset))
            }
        })
        .collect()
}

/// Folds the windows of one sampled property.
struct WindowSampler {
    window_entries: u64,
    windows: u32,
    sample_entries: u64,
    complete_runs: u64,
    complete_entries: u64,
    /// Complete runs by value: a value seen whole in more than one window
    /// keeps its longest run, not the sum, since a window may repeat a run.
    complete: BTreeMap<String, u64>,
    saturated: BTreeSet<String>,
    /// Observed bounds per tag, which only tighten what the probes found.
    seen: BTreeMap<char, (String, String)>,
    /// The run open in the current window: value, length, started at the
    /// window's first entry.
    run: Option<(String, u64, bool)>,
    at_window_start: bool,
    window_had_entries: bool,
}

impl WindowSampler {
    fn new(window_entries: u64) -> Self {
        Self {
            window_entries,
            windows: 0,
            sample_entries: 0,
            complete_runs: 0,
            complete_entries: 0,
            complete: BTreeMap::new(),
            saturated: BTreeSet::new(),
            seen: BTreeMap::new(),
            run: None,
            at_window_start: true,
            window_had_entries: false,
        }
    }

    fn open_window(&mut self) {
        self.run = None;
        self.at_window_start = true;
        self.window_had_entries = false;
    }

    fn push(&mut self, encoded: &str) {
        self.sample_entries += 1;
        self.window_had_entries = true;
        if let Some(tag) = encoded.chars().next() {
            match self.seen.get_mut(&tag) {
                Some((min, max)) => {
                    if encoded < min.as_str() {
                        *min = encoded.to_string();
                    }
                    if encoded > max.as_str() {
                        *max = encoded.to_string();
                    }
                }
                None => {
                    self.seen
                        .insert(tag, (encoded.to_string(), encoded.to_string()));
                }
            }
        }
        match &mut self.run {
            Some((value, length, _)) if value == encoded => *length += 1,
            _ => {
                self.close_run(false);
                self.run = Some((encoded.to_string(), 1, self.at_window_start));
            }
        }
        self.at_window_start = false;
    }

    /// `cut` says the window ended by its entry limit, so the open run may
    /// continue past it.
    fn close_window(&mut self, cut: bool) {
        self.close_run(cut);
        if self.window_had_entries {
            self.windows += 1;
        }
    }

    fn close_run(&mut self, cut_at_end: bool) {
        let Some((value, length, started_at_window_start)) = self.run.take() else {
            return;
        };
        let partial = started_at_window_start || cut_at_end;
        if !partial {
            self.complete_runs += 1;
            self.complete_entries += length;
            let best = self.complete.entry(value).or_default();
            *best = (*best).max(length);
        } else if length >= self.window_entries / SATURATED_PARTIAL_RUN_DIVISOR {
            self.saturated.insert(value);
        }
    }

    fn finish(self, statistics: &mut PropertyIndexStatistics, tags: Vec<(char, String, String)>) {
        let exact_entries = statistics.total_entries;
        let mut top = self
            .complete
            .iter()
            .filter(|(value, _)| value.len() <= MAX_TOP_VALUE_KEY_BYTES)
            .map(|(value, count)| (value.clone(), *count))
            .collect::<Vec<_>>();
        top.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        top.truncate(SAMPLED_TOP_CANDIDATES);
        let mut ranges = BTreeMap::new();
        for (tag, min, max) in tags {
            let (min, max) = match self.seen.get(&tag) {
                Some((seen_min, seen_max)) => {
                    (min.min(seen_min.clone()), max.max(seen_max.clone()))
                }
                None => (min, max),
            };
            // NaN sorts outside the floats on both ends and is no bound.
            let nan = |value: &str| {
                tag == 'n'
                    && matches!(decode_vertex_property_value_key(value), Ok(VertexPropertyValue::Float(float)) if float.0.is_nan())
            };
            if nan(&min) || nan(&max) {
                continue;
            }
            ranges.insert(
                tag,
                TagRange {
                    entries: statistics.ranges.get(&tag).map_or(0, |range| range.entries),
                    min,
                    max,
                },
            );
        }
        statistics.top_values = sorted_top_values(top);
        statistics.most_common_count = statistics
            .top_values
            .first()
            .map_or(statistics.most_common_count, |(_, count)| {
                (*count).max(statistics.most_common_count)
            });
        statistics.ranges = ranges;
        let seen = self.saturated.len();
        statistics.saturated_values = self
            .saturated
            .into_iter()
            .filter(|value| value.len() <= MAX_TOP_VALUE_KEY_BYTES)
            .take(PLANNER_STATISTICS_TOP_VALUES)
            .collect();
        statistics.sample = Some(PropertySample {
            exact_entries,
            sample_entries: self.complete_entries,
            complete_runs: self.complete_runs,
            windows: self.windows,
            saturated_dropped: u32::try_from(seen - statistics.saturated_values.len())
                .unwrap_or(u32::MAX),
        });
    }
}

/// Root of a cell's publication: `current` and `payloads/` live under it.
pub(crate) fn planner_statistics_root(
    store_path: &slatedb::object_store::path::Path,
    cell_id: &str,
) -> slatedb::object_store::path::Path {
    format!("{store_path}/_planner_statistics/v1/{cell_id}").into()
}

/// The publication's control record, and before it the statistics object
/// itself. Named in errors and in the tests that corrupt it.
pub(crate) fn planner_statistics_path(
    store_path: &slatedb::object_store::path::Path,
    cell_id: &str,
) -> slatedb::object_store::path::Path {
    format!("{store_path}/_planner_statistics/v1/{cell_id}/current").into()
}

/// What publishing a build did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannerStatisticsPublish {
    Published,
    /// `current` already holds this snapshot sequence or a newer one, so the
    /// build was dropped. Another indexer got there first.
    Superseded {
        current_sequence: u64,
    },
    /// The claim was taken over while this build ran (it outlived its
    /// lease), so nothing of it may land.
    Fenced,
}

/// A claimed cell: publishes builds and gives the claim back.
pub struct PlannerStatisticsPublisher {
    cell_id: String,
    path: slatedb::object_store::path::Path,
    ownership: Ownership,
}

impl std::fmt::Debug for PlannerStatisticsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannerStatisticsPublisher")
            .field("cell_id", &self.cell_id)
            .field("owner", &self.ownership.owner())
            .field("epoch", &self.ownership.epoch())
            .finish()
    }
}

impl PlannerStatisticsPublisher {
    pub fn owner(&self) -> &str {
        self.ownership.owner()
    }

    /// Publish `statistics` as built from `statistics.storage_sequence`:
    /// the payload is uploaded whole, then `current` is moved to it only if
    /// the sequence is newer than the published one and the claim still
    /// stands.
    pub async fn publish(
        &mut self,
        statistics: &PlannerStatistics,
    ) -> Result<PlannerStatisticsPublish> {
        self.publish_with(statistics, false).await
    }

    /// [`PlannerStatisticsPublisher::publish`] for a published object that
    /// does not decode: a build of the same snapshot replaces it instead of
    /// being superseded. A newer snapshot still wins.
    pub async fn republish(
        &mut self,
        statistics: &PlannerStatistics,
    ) -> Result<PlannerStatisticsPublish> {
        self.publish_with(statistics, true).await
    }

    async fn publish_with(
        &mut self,
        statistics: &PlannerStatistics,
        replace_same: bool,
    ) -> Result<PlannerStatisticsPublish> {
        if statistics.cell_id != self.cell_id {
            return Err(GraphError::InvalidKeyComponent {
                component: "cell_id",
                value: statistics.cell_id.clone(),
            });
        }
        let payload = Bytes::from(encode_planner_statistics(statistics));
        let outcome = if replace_same {
            self.ownership
                .republish(statistics.storage_sequence, payload)
                .await
        } else {
            self.ownership
                .publish(statistics.storage_sequence, payload)
                .await
        };
        match outcome {
            Ok(Publish::Published { .. }) => Ok(PlannerStatisticsPublish::Published),
            Ok(Publish::Superseded { current_sequence }) => {
                Ok(PlannerStatisticsPublish::Superseded { current_sequence })
            }
            Err(PublicationError::Fenced) => Ok(PlannerStatisticsPublish::Fenced),
            Err(error) => Err(publication_error(error, &self.path)),
        }
    }

    /// Extend the claim by `lease` from now. Fenced if it was taken over.
    pub async fn renew(&mut self, lease: Duration) -> Result<PlannerStatisticsPublish> {
        match self.ownership.renew(lease).await {
            Ok(()) => Ok(PlannerStatisticsPublish::Published),
            Err(PublicationError::Fenced) => Ok(PlannerStatisticsPublish::Fenced),
            Err(error) => Err(publication_error(error, &self.path)),
        }
    }

    /// Give the claim back; the publication stays. A claim already taken
    /// over has nothing to give back, and that is not an error.
    pub async fn release(self) -> Result<()> {
        self.ownership
            .release()
            .await
            .map_err(|error| publication_error(error, &self.path))
    }
}

fn publication_error(
    error: PublicationError,
    path: &slatedb::object_store::path::Path,
) -> GraphError {
    match error {
        PublicationError::ObjectStore(error) => GraphError::ObjectStore(error),
        PublicationError::Fenced => GraphError::ConditionalWriteConflict {
            operation: "planner_statistics_publish",
            key: path.to_string(),
        },
        PublicationError::Conflict { attempts } => GraphError::RetryExhausted {
            operation: "planner_statistics_publish",
            attempts,
        },
        PublicationError::InvalidRecord(reason) => GraphError::CorruptValue {
            key: path.to_string(),
            reason,
        },
    }
}

/// Split an index key into its `components` validated segments after `prefix`.
fn index_key_parts<'a>(key: &'a str, prefix: &str, components: usize) -> Result<Vec<&'a str>> {
    let parts = key
        .strip_prefix(prefix)
        .map(|rest| rest.split('/').collect::<Vec<_>>())
        .filter(|parts| parts.len() == components && parts.iter().all(|part| !part.is_empty()))
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("expected {components} index key components after {prefix}"),
        })?;
    Ok(parts)
}

fn add_count(counts: &mut BTreeMap<String, u64>, name: &str, count: u64) {
    if let Some(total) = counts.get_mut(name) {
        *total = total.saturating_add(count);
    } else {
        counts.insert(name.to_string(), count);
    }
}

/// What [`PropertyRunCounter::push`] did with an entry.
#[derive(Debug, Eq, PartialEq)]
enum Pushed {
    Counted,
    /// The entry brought its property to the exact cap: the property is
    /// closed with what it has, and the caller skips the rest of its run.
    Capped(Vec<String>),
}

/// Folds index entries, fed in key order, into per-property statistics. A
/// property is a group of key components (`[property]` or `[type, property]`).
struct PropertyRunCounter {
    exact_entries: u64,
    current: Option<(Vec<String>, PropertyRuns, u64)>,
    finished: BTreeMap<Vec<String>, PropertyIndexStatistics>,
}

impl PropertyRunCounter {
    fn new(exact_entries: u64) -> Self {
        Self {
            exact_entries: exact_entries.max(1),
            current: None,
            finished: BTreeMap::new(),
        }
    }

    fn push(&mut self, key: &str, group: &[&str], encoded: &str) -> Result<Pushed> {
        let same_group = self
            .current
            .as_ref()
            .is_some_and(|(current, _, _)| current.iter().eq(group.iter()));
        if !same_group {
            self.close_group(key)?;
            if self.finished.len() < MAX_PROPERTIES_PER_KIND {
                self.current = Some((
                    group.iter().map(|part| part.to_string()).collect(),
                    PropertyRuns::default(),
                    0,
                ));
            }
        }
        match &mut self.current {
            Some((current, runs, entries)) => {
                runs.push(key, encoded)?;
                *entries += 1;
                if *entries >= self.exact_entries {
                    let group = current.clone();
                    self.close_group(key)?;
                    return Ok(Pushed::Capped(group));
                }
                Ok(Pushed::Counted)
            }
            // Past the property cap: the rest of the scan is dropped.
            None => Ok(Pushed::Counted),
        }
    }

    fn close_group(&mut self, key: &str) -> Result<()> {
        let Some((group, runs, _)) = self.current.take() else {
            return Ok(());
        };
        let statistics = runs.finish(key)?;
        if self.finished.insert(group, statistics).is_some() {
            return corrupt(key, "index keys of one property are not contiguous");
        }
        Ok(())
    }

    fn finish(mut self, key: &str) -> Result<BTreeMap<Vec<String>, PropertyIndexStatistics>> {
        self.close_group(key)?;
        Ok(self.finished)
    }
}

/// One property's runs of equal encoded values.
#[derive(Default)]
struct PropertyRuns {
    statistics: PropertyIndexStatistics,
    /// Min-heap of the best runs so far. The weakest is the lowest count and,
    /// among equal counts, the largest key, so ties keep the smallest keys.
    top: BinaryHeap<Reverse<(u64, Reverse<String>)>>,
    /// Per tag: entries, and the first and last non-NaN run key.
    ranges: BTreeMap<char, (u64, Option<(String, String)>)>,
    run: Option<(String, u64)>,
}

impl PropertyRuns {
    fn push(&mut self, key: &str, encoded: &str) -> Result<()> {
        if let Some((value, count)) = &mut self.run {
            if value == encoded {
                *count += 1;
                return Ok(());
            }
        }
        self.close_run(key)?;
        self.run = Some((encoded.to_string(), 1));
        Ok(())
    }

    fn close_run(&mut self, key: &str) -> Result<()> {
        let Some((value, count)) = self.run.take() else {
            return Ok(());
        };
        let tag = value
            .chars()
            .next()
            .filter(|tag| PROPERTY_VALUE_TAGS.contains(tag))
            .ok_or_else(|| GraphError::CorruptValue {
                key: key.to_string(),
                reason: format!("unknown property index value type in {value}"),
            })?;
        // NaN sorts outside the numbers on both ends; it is counted but
        // never a bound.
        let bounded = tag != 'n'
            || !matches!(decode_vertex_property_value_key(&value)?, VertexPropertyValue::Float(float) if float.0.is_nan());
        let statistics = &mut self.statistics;
        statistics.total_entries = statistics.total_entries.saturating_add(count);
        statistics.distinct_values += 1;
        statistics.most_common_count = statistics.most_common_count.max(count);
        let range = self.ranges.entry(tag).or_default();
        range.0 = range.0.saturating_add(count);
        if bounded {
            match &mut range.1 {
                Some((_, max)) => max.clone_from(&value),
                None => range.1 = Some((value.clone(), value.clone())),
            }
        }
        if value.len() <= MAX_TOP_VALUE_KEY_BYTES {
            let candidate = Reverse((count, Reverse(value)));
            if self.top.len() < PLANNER_STATISTICS_TOP_VALUES {
                self.top.push(candidate);
            } else if self.top.peek().is_some_and(|weakest| candidate < *weakest) {
                self.top.pop();
                self.top.push(candidate);
            }
        }
        Ok(())
    }

    fn finish(mut self, key: &str) -> Result<PropertyIndexStatistics> {
        self.close_run(key)?;
        let mut statistics = self.statistics;
        statistics.top_values = sorted_top_values(
            self.top
                .into_iter()
                .map(|Reverse((count, Reverse(value)))| (value, count)),
        );
        statistics.ranges = self
            .ranges
            .into_iter()
            .filter_map(|(tag, (entries, bounds))| {
                bounds.map(|(min, max)| (tag, TagRange { entries, min, max }))
            })
            .collect();
        Ok(statistics)
    }
}

/// Count descending, then key ascending, truncated to the top-value limit.
fn sorted_top_values(values: impl IntoIterator<Item = (String, u64)>) -> Vec<(String, u64)> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    values.truncate(PLANNER_STATISTICS_TOP_VALUES);
    values
}

fn merge_edge_property_statistics(
    mut left: BTreeMap<(String, String), PropertyIndexStatistics>,
    right: BTreeMap<(String, String), PropertyIndexStatistics>,
) -> BTreeMap<(String, String), PropertyIndexStatistics> {
    for (property, statistics) in right {
        let merged = match left.remove(&property) {
            Some(existing) => merge_property_statistics(existing, statistics),
            None => statistics,
        };
        left.insert(property, merged);
    }
    while left.len() > MAX_PROPERTIES_PER_KIND {
        left.pop_last();
    }
    left
}

/// Merge two indexes of one property. Totals and per-tag entries add and
/// ranges widen. A value present in both is counted as two distinct values, so
/// `distinct_values` is an upper bound, and a value outside both top lists can
/// be missed by the merged list.
fn merge_property_statistics(
    left: PropertyIndexStatistics,
    right: PropertyIndexStatistics,
) -> PropertyIndexStatistics {
    let mut top = BTreeMap::<String, u64>::new();
    for (value, count) in left.top_values.into_iter().chain(right.top_values) {
        *top.entry(value).or_default() += count;
    }
    let top_values = sorted_top_values(top);
    let mut ranges = left.ranges;
    for (tag, range) in right.ranges {
        match ranges.get_mut(&tag) {
            Some(existing) => {
                existing.entries = existing.entries.saturating_add(range.entries);
                if range.min < existing.min {
                    existing.min = range.min;
                }
                if range.max > existing.max {
                    existing.max = range.max;
                }
            }
            None => {
                ranges.insert(tag, range);
            }
        }
    }
    let sample = match (left.sample, right.sample) {
        (None, None) => None,
        (Some(sample), None) | (None, Some(sample)) => Some(sample),
        (Some(a), Some(b)) => Some(PropertySample {
            exact_entries: a.exact_entries.saturating_add(b.exact_entries),
            sample_entries: a.sample_entries.saturating_add(b.sample_entries),
            complete_runs: a.complete_runs.saturating_add(b.complete_runs),
            windows: a.windows.saturating_add(b.windows),
            saturated_dropped: a.saturated_dropped.saturating_add(b.saturated_dropped),
        }),
    };
    let mut saturated_values = left.saturated_values;
    saturated_values.extend(right.saturated_values);
    debug_assert!(saturated_values.len() <= PLANNER_STATISTICS_SATURATED_VALUES);
    PropertyIndexStatistics {
        total_entries: left.total_entries.saturating_add(right.total_entries),
        distinct_values: left.distinct_values.saturating_add(right.distinct_values),
        most_common_count: top_values
            .first()
            .map_or(0, |(_, count)| *count)
            .max(left.most_common_count)
            .max(right.most_common_count),
        top_values,
        ranges,
        sample,
        saturated_values,
    }
}

/// Tab-separated text, one fact per line, in the style of the query stats
/// records: the indexer build has no serde.
pub fn encode_planner_statistics(statistics: &PlannerStatistics) -> Vec<u8> {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "{PLANNER_STATISTICS_MAGIC}");
    let _ = writeln!(out, "cell\t{}", statistics.cell_id);
    let _ = writeln!(out, "sequence\t{}", statistics.storage_sequence);
    let _ = writeln!(out, "built_at_ms\t{}", statistics.built_at_ms);
    for (label, count) in &statistics.labels {
        let _ = writeln!(out, "label\t{label}\t{count}");
        if statistics.labels_sampled.contains(label) {
            let _ = writeln!(out, "labelsample\t{label}");
        }
    }
    for (edge_type, count) in &statistics.edge_types {
        let _ = writeln!(out, "edge_type\t{edge_type}\t{count}");
        if statistics.edge_types_sampled.contains(edge_type) {
            let _ = writeln!(out, "edgesample\t{edge_type}");
        }
    }
    for (property, stats) in &statistics.vertex_properties {
        encode_property_statistics(&mut out, "v", property, stats);
    }
    for ((edge_type, property), stats) in &statistics.edge_properties {
        encode_property_statistics(&mut out, "e", &format!("{edge_type}\t{property}"), stats);
    }
    out.into_bytes()
}

fn encode_property_statistics(
    out: &mut String,
    kind: &str,
    name: &str,
    statistics: &PropertyIndexStatistics,
) {
    use std::fmt::Write as _;

    let _ = writeln!(
        out,
        "{kind}prop\t{name}\t{}\t{}\t{}",
        statistics.total_entries, statistics.distinct_values, statistics.most_common_count
    );
    if let Some(sample) = &statistics.sample {
        let _ = writeln!(
            out,
            "{kind}sample\t{name}\t{}\t{}\t{}\t{}\t{}",
            sample.exact_entries,
            sample.sample_entries,
            sample.complete_runs,
            sample.windows,
            sample.saturated_dropped
        );
    }
    for (value, count) in &statistics.top_values {
        let _ = writeln!(out, "{kind}top\t{name}\t{value}\t{count}");
    }
    for value in &statistics.saturated_values {
        let _ = writeln!(out, "{kind}saturated\t{name}\t{value}");
    }
    for (tag, range) in &statistics.ranges {
        let _ = writeln!(
            out,
            "{kind}range\t{name}\t{tag}\t{}\t{}\t{}",
            range.entries, range.min, range.max
        );
    }
}

/// Inverse of [`encode_planner_statistics`]. Any other format version, and
/// any malformed line, is [`GraphError::CorruptValue`].
pub fn decode_planner_statistics(path: &str, bytes: &[u8]) -> Result<PlannerStatistics> {
    let text = text_value(path, bytes)?;
    let mut lines = text.lines();
    match lines.next() {
        Some(PLANNER_STATISTICS_MAGIC) => {}
        Some(other) if other.starts_with(PLANNER_STATISTICS_MAGIC_PREFIX) => {
            return corrupt(
                path,
                format!("unsupported planner statistics version {other}"),
            );
        }
        _ => {
            return corrupt(
                path,
                "unsupported planner statistics version: missing header",
            )
        }
    }
    let mut header = |name: &str| -> Result<String> {
        match lines.next().and_then(|line| line.split_once('\t')) {
            Some((field, value)) if field == name => Ok(value.to_string()),
            _ => corrupt(path, format!("expected planner statistics {name} line")),
        }
    };
    let cell_id = header("cell")?;
    validate_component("cell_id", &cell_id)?;
    let storage_sequence = parse_u64(path, &header("sequence")?, "sequence")?;
    let built_at_ms = parse_u64(path, &header("built_at_ms")?, "built_at_ms")?;
    let mut statistics = PlannerStatistics {
        cell_id,
        storage_sequence,
        built_at_ms,
        format_version: PLANNER_STATISTICS_FORMAT_VERSION,
        ..PlannerStatistics::default()
    };
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        let invalid = || corrupt(path, format!("invalid planner statistics line {line}"));
        match fields.as_slice() {
            ["label", label, count] => {
                validate_component("label", label)?;
                if statistics
                    .labels
                    .insert(label.to_string(), parse_u64(path, count, "label count")?)
                    .is_some()
                {
                    return invalid();
                }
            }
            ["edge_type", edge_type, count] => {
                validate_component("edge_type", edge_type)?;
                if statistics
                    .edge_types
                    .insert(
                        edge_type.to_string(),
                        parse_u64(path, count, "edge type count")?,
                    )
                    .is_some()
                {
                    return invalid();
                }
            }
            ["vprop", property, total, distinct, most_common] => {
                validate_component("property", property)?;
                let stats = decode_property_counts(path, total, distinct, most_common)?;
                if statistics
                    .vertex_properties
                    .insert(property.to_string(), stats)
                    .is_some()
                {
                    return invalid();
                }
            }
            ["labelsample", label] => {
                if !statistics.labels.contains_key(*label)
                    || !statistics.labels_sampled.insert(label.to_string())
                {
                    return invalid();
                }
            }
            ["edgesample", edge_type] => {
                if !statistics.edge_types.contains_key(*edge_type)
                    || !statistics.edge_types_sampled.insert(edge_type.to_string())
                {
                    return invalid();
                }
            }
            ["vsample", property, exact, entries, runs, windows, dropped] => {
                let Some(stats) = statistics.vertex_properties.get_mut(*property) else {
                    return invalid();
                };
                decode_sample(path, line, stats, [exact, entries, runs, windows, dropped])?;
            }
            // Before the dropped count: the list may be incomplete.
            ["vsample", property, exact, entries, runs, windows] => {
                let Some(stats) = statistics.vertex_properties.get_mut(*property) else {
                    return invalid();
                };
                decode_sample(
                    path,
                    line,
                    stats,
                    [exact, entries, runs, windows, SAMPLE_DROPPED_UNKNOWN],
                )?;
            }
            ["vsaturated", property, value] => {
                let Some(stats) = statistics.vertex_properties.get_mut(*property) else {
                    return invalid();
                };
                decode_saturated_value(path, line, stats, value)?;
            }
            ["vtop", property, value, count] => {
                let Some(stats) = statistics.vertex_properties.get_mut(*property) else {
                    return invalid();
                };
                decode_top_value(path, line, stats, value, count)?;
            }
            ["vrange", property, tag, entries, min, max] => {
                let Some(stats) = statistics.vertex_properties.get_mut(*property) else {
                    return invalid();
                };
                decode_tag_range(path, line, stats, tag, entries, min, max)?;
            }
            ["eprop", edge_type, property, total, distinct, most_common] => {
                validate_component("edge_type", edge_type)?;
                validate_component("property", property)?;
                let stats = decode_property_counts(path, total, distinct, most_common)?;
                if statistics
                    .edge_properties
                    .insert((edge_type.to_string(), property.to_string()), stats)
                    .is_some()
                {
                    return invalid();
                }
            }
            ["esample", edge_type, property, exact, entries, runs, windows, dropped] => {
                let Some(stats) = statistics
                    .edge_properties
                    .get_mut(&(edge_type.to_string(), property.to_string()))
                else {
                    return invalid();
                };
                decode_sample(path, line, stats, [exact, entries, runs, windows, dropped])?;
            }
            ["esample", edge_type, property, exact, entries, runs, windows] => {
                let Some(stats) = statistics
                    .edge_properties
                    .get_mut(&(edge_type.to_string(), property.to_string()))
                else {
                    return invalid();
                };
                decode_sample(
                    path,
                    line,
                    stats,
                    [exact, entries, runs, windows, SAMPLE_DROPPED_UNKNOWN],
                )?;
            }
            ["esaturated", edge_type, property, value] => {
                let Some(stats) = statistics
                    .edge_properties
                    .get_mut(&(edge_type.to_string(), property.to_string()))
                else {
                    return invalid();
                };
                decode_saturated_value(path, line, stats, value)?;
            }
            ["etop", edge_type, property, value, count] => {
                let Some(stats) = statistics
                    .edge_properties
                    .get_mut(&(edge_type.to_string(), property.to_string()))
                else {
                    return invalid();
                };
                decode_top_value(path, line, stats, value, count)?;
            }
            ["erange", edge_type, property, tag, entries, min, max] => {
                let Some(stats) = statistics
                    .edge_properties
                    .get_mut(&(edge_type.to_string(), property.to_string()))
                else {
                    return invalid();
                };
                decode_tag_range(path, line, stats, tag, entries, min, max)?;
            }
            [""] => {}
            _ => return invalid(),
        }
    }
    Ok(statistics)
}

fn decode_property_counts(
    path: &str,
    total: &str,
    distinct: &str,
    most_common: &str,
) -> Result<PropertyIndexStatistics> {
    Ok(PropertyIndexStatistics {
        total_entries: parse_u64(path, total, "total_entries")?,
        distinct_values: parse_u64(path, distinct, "distinct_values")?,
        most_common_count: parse_u64(path, most_common, "most_common_count")?,
        ..PropertyIndexStatistics::default()
    })
}

/// What a sample line without a dropped count is read as: one, since the
/// object cannot say its list is complete.
const SAMPLE_DROPPED_UNKNOWN: &str = "1";

fn decode_sample(
    path: &str,
    line: &str,
    statistics: &mut PropertyIndexStatistics,
    fields: [&str; 5],
) -> Result<()> {
    if statistics.sample.is_some() {
        return corrupt(path, format!("invalid planner statistics line {line}"));
    }
    let [exact, entries, runs, windows, dropped] = fields;
    let small = |value: &str, what: &str| {
        u32::try_from(parse_u64(path, value, what)?).map_err(|_| GraphError::CorruptValue {
            key: path.to_string(),
            reason: format!("invalid planner statistics line {line}"),
        })
    };
    statistics.sample = Some(PropertySample {
        exact_entries: parse_u64(path, exact, "sample exact entries")?,
        sample_entries: parse_u64(path, entries, "sample entries")?,
        complete_runs: parse_u64(path, runs, "sample complete runs")?,
        windows: small(windows, "sample windows")?,
        saturated_dropped: small(dropped, "sample saturated values dropped")?,
    });
    Ok(())
}

fn decode_saturated_value(
    path: &str,
    line: &str,
    statistics: &mut PropertyIndexStatistics,
    value: &str,
) -> Result<()> {
    if encoded_value_tag(value).is_none()
        || statistics.saturated_values.len() >= PLANNER_STATISTICS_SATURATED_VALUES
        || !statistics.saturated_values.insert(value.to_string())
    {
        return corrupt(path, format!("invalid planner statistics line {line}"));
    }
    Ok(())
}

fn decode_top_value(
    path: &str,
    line: &str,
    statistics: &mut PropertyIndexStatistics,
    value: &str,
    count: &str,
) -> Result<()> {
    if encoded_value_tag(value).is_none()
        || statistics.top_values.len() >= PLANNER_STATISTICS_TOP_VALUES
    {
        return corrupt(path, format!("invalid planner statistics line {line}"));
    }
    statistics.top_values.push((
        value.to_string(),
        parse_u64(path, count, "top value count")?,
    ));
    Ok(())
}

fn decode_tag_range(
    path: &str,
    line: &str,
    statistics: &mut PropertyIndexStatistics,
    tag: &str,
    entries: &str,
    min: &str,
    max: &str,
) -> Result<()> {
    let mut tag_chars = tag.chars();
    let tag = match (tag_chars.next(), tag_chars.next()) {
        (Some(tag), None)
            if encoded_value_tag(min) == Some(tag)
                && encoded_value_tag(max) == Some(tag)
                && min <= max =>
        {
            tag
        }
        _ => return corrupt(path, format!("invalid planner statistics line {line}")),
    };
    let range = TagRange {
        entries: parse_u64(path, entries, "range entries")?,
        min: min.to_string(),
        max: max.to_string(),
    };
    if statistics.ranges.insert(tag, range).is_some() {
        return corrupt(path, format!("invalid planner statistics line {line}"));
    }
    Ok(())
}

/// The type tag of an encoded property index value, if it is well formed.
fn encoded_value_tag(value: &str) -> Option<char> {
    let tag = value
        .chars()
        .next()
        .filter(|tag| PROPERTY_VALUE_TAGS.contains(tag))?;
    value[1..]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric())
        .then_some(tag)
}
