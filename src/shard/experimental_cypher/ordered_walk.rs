//! One page of an ordered walk over a vertex property index, for the
//! experimental engine's `OrderedVertexPropertyScan`.
//!
//! The index stores one key per vertex and property, grouped by a type tag:
//! `b` booleans, `i` non-negative integers, `j` negative integers, `n` floats
//! and `s` strings, each in its own byte order. Cypher orders booleans, then
//! numbers by value, then strings, so the walk reads each tag as a separate
//! stream and merges them under the engine's own
//! [`compare_property_values`]. The `n` stream is split at `+0.0`, because
//! `-0.0` and `0.0` are one value but sit in different byte positions, and
//! every stream must be in value order on its own for the merge to hold.
//!
//! Bounds become per-stream seek ranges that are *sound* - no entry inside
//! the request's restrictions is ever outside them - and each entry is then
//! checked exactly with [`OrderedPropertyScanRequest::accepts`], so no
//! encoding needs to agree with the engine on a boundary case.
use std::cmp::Ordering;
use std::ops::Bound;

use hydradb_cypher_engine::{compare_property_values, OrderedPropertyScanRequest};

use std::collections::BinaryHeap;

use super::*;
use crate::shard::query::{parse_vertex_property_index_key, QUERY_METADATA_HYDRATION_CONCURRENCY};
use crate::QueryFailureReason;

/// Rows of one ordered page, in index order, and where to continue from.
pub(in crate::shard) struct OrderedVertexPropertyPage {
    pub(in crate::shard) rows: Vec<(VertexId, VertexMetadata)>,
    /// The last index entry this page consumed, as a value and vertex ID.
    /// The engine passes it back as `after` and compares it against the rows
    /// it holds, so it is the property's own value, never a key encoding.
    pub(in crate::shard) last: Option<(ScalarValue, VertexId)>,
    /// Every stream inside the request's restrictions is spent. Reported
    /// rather than left for the engine to infer from a short page: a page
    /// that stopped on `required` and one that ran out of index look alike.
    pub(in crate::shard) exhausted: bool,
    /// Index entries this page read, which is what its I/O costs. A page that
    /// resumes where the last one stopped reads about what it returns; one
    /// that restarts reads everything before it again. A statistics-chosen
    /// range seek declines on this.
    pub(in crate::shard) index_entries: u64,
}

/// A retained row, ordered by the walk's *requested* order: the property
/// value in the `ascending` direction, then the vertex ID in the
/// `id_ascending` one.
///
/// `Ord` is deliberately the sort order itself, so a `BinaryHeap` of these is
/// a max-heap whose top is the worst row held. That is what makes the page's
/// retention bounded: past `required` a better tie member replaces the worst
/// rather than being appended.
///
/// The two directions are carried per row because they are the same for every
/// row in one page; comparing rows from different pages is meaningless and
/// never happens.
struct Candidate {
    value: ScalarValue,
    vertex_id: VertexId,
    ascending: bool,
    id_ascending: bool,
    metadata: VertexMetadata,
}

impl Candidate {
    fn key(&self, other: &Self) -> Ordering {
        let by_value = compare_property_values(&self.value, &other.value);
        let by_value = if self.ascending {
            by_value
        } else {
            by_value.reverse()
        };
        by_value.then_with(|| {
            if self.id_ascending {
                self.vertex_id.cmp(&other.vertex_id)
            } else {
                other.vertex_id.cmp(&self.vertex_id)
            }
        })
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key(other)
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.key(other) == Ordering::Equal
    }
}

impl Eq for Candidate {}

/// A key-suffix range below one stream prefix.
pub(super) type KeyRange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Everything above an entry's value in the key, so a range bound on a value
/// segment covers every vertex ID stored under it.
const ALL_IDS: &str = "/~";
/// The `n` suffix of `+0.0`; `-0.0` is the last key below it.
const POSITIVE_ZERO: &str = "8000000000000000";

/// One type-tagged slice of the property index.
#[derive(Clone, Copy)]
pub(super) struct Stream {
    pub(super) tag: char,
    /// The Cypher order rank of the stream's values: booleans, numbers,
    /// strings.
    pub(super) rank: u8,
    /// Where the `n` stream is split around zero.
    zero: Option<ZeroSide>,
}

#[derive(Clone, Copy)]
enum ZeroSide {
    Negative,
    Positive,
}

pub(super) const STREAMS: [Stream; 6] = [
    Stream {
        tag: 'b',
        rank: 0,
        zero: None,
    },
    Stream {
        tag: 'i',
        rank: 1,
        zero: None,
    },
    Stream {
        tag: 'j',
        rank: 1,
        zero: None,
    },
    Stream {
        tag: 'n',
        rank: 1,
        zero: Some(ZeroSide::Negative),
    },
    Stream {
        tag: 'n',
        rank: 1,
        zero: Some(ZeroSide::Positive),
    },
    Stream {
        tag: 's',
        rank: 2,
        zero: None,
    },
];

/// A sound seek restriction on one stream, in key-suffix space (the part of
/// the key after the type tag).
enum Seek {
    /// Every entry in the stream may qualify.
    All,
    /// No entry in the stream can qualify.
    Empty,
    /// Only entries at or after this key may qualify.
    From(Vec<u8>),
    /// Only entries at or before this key may qualify.
    To(Vec<u8>),
}

fn rank(value: &ScalarValue) -> Option<u8> {
    match value {
        ScalarValue::Boolean(_) => Some(0),
        ScalarValue::Integer(_) | ScalarValue::Float(_) => Some(1),
        ScalarValue::String(_) => Some(2),
        ScalarValue::Null => None,
    }
}

/// The index key segment of `value`, without its type tag.
fn segment(value: VertexPropertyValue) -> String {
    let mut encoded = encode_vertex_property_value_key(&value);
    encoded.remove(0);
    encoded
}

fn float_of(text: &str) -> Result<f64> {
    text.parse::<f64>()
        .map_err(|_| GraphError::UnsupportedQuery {
            dialect: "Cypher25",
            feature: format!("ordered property scan bound {text} is not a float"),
            reason: QueryFailureReason::OrderWindow,
        })
}

/// The numeric value of a bound, as an exact integer or a float.
enum Number {
    Integer(i128),
    Float(f64),
}

impl GraphShard {
    /// Walk one page of the property index for `request.property`: seek each
    /// type stream to the request's restrictions (or just past the cursor),
    /// merge the streams in Cypher value order, hydrate in bounded batches,
    /// and verify labels and the index entry against stored metadata. It
    /// returns up to `required` qualifying rows in index order and never
    /// decides the window: the engine owns the residual predicate, the top-k
    /// and the boundary tie, and pulls another page with the returned cursor
    /// until it has what it needs.
    pub(in crate::shard) async fn scan_ordered_vertex_property_page_at(
        &self,
        cell_id: &str,
        request: &OrderedPropertyScanRequest,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<OrderedVertexPropertyPage> {
        if request.required == 0 {
            return Ok(OrderedVertexPropertyPage {
                rows: Vec::new(),
                last: None,
                exhausted: false,
                index_entries: 0,
            });
        }
        // An `index_order` page resumes exactly where it stops, so a shorter
        // one is always safe; cap it at what this shard may hold rather than
        // failing a caller whose page grew with its residual's miss rate. A
        // ranked page cannot shrink: its `required` is the caller's window.
        let required = if request.index_order {
            request
                .required
                .min(self.limits.max_query_intermediate_rows.max(1))
        } else {
            request.required
        };
        // The engine hands back the position it was given, so the walk needs
        // no encoding of its own: the candidate budget it used to carry now
        // lives on the query's shared tally.
        let resume = request
            .after
            .as_ref()
            .map(|after| (after.value.clone(), after.vertex_id));
        let property = request.property.as_str();
        let property_prefix = keys::vertex_property_index_property_prefix(cell_id, property);
        let order = if request.ascending {
            slatedb::IterationOrder::Ascending
        } else {
            slatedb::IterationOrder::Descending
        };
        let scan_options = remote_scan_options().with_order(order);

        // Streams in walk order, grouped by rank. A group is opened only once
        // the one before it is spent, so a window that fills on numbers never
        // seeks the string stream.
        let mut groups = Vec::<Vec<Stream>>::new();
        for stream in STREAMS {
            match groups.last_mut() {
                Some(group) if group[0].rank == stream.rank => group.push(stream),
                _ => groups.push(vec![stream]),
            }
        }
        if !request.ascending {
            groups.reverse();
        }
        let mut groups = groups.into_iter();
        let mut open: Vec<OpenStream> = Vec::new();

        // Never reserve from the user-controlled window. Grow only when an
        // actual qualifying row is retained, after enforcing the row budget.
        //
        // A max-heap under the walk's requested order, so its top is the worst
        // row held. Past `required` the page is finishing the run of entries
        // tying with that row, and the worst gives way to a better one: the
        // page ranks its own boundary tie without ever holding more than the
        // window.
        let mut accepted = BinaryHeap::<Candidate>::new();
        let mut boundary: Option<ScalarValue> = None;
        let mut stop = false;
        let mut last = resume.clone();
        let mut index_entries = 0_u64;
        // Entries judged past the cursor, for `max_index_entries`. Counted
        // apart from `index_entries`, which also holds each stream's
        // look-ahead, so a small cap still lets every page make progress.
        let mut judged = 0_usize;
        let mut hydrate_us = 0_u64;
        let mut exhausted = false;
        loop {
            let batch_capacity = required
                .saturating_sub(accepted.len())
                .clamp(1, QUERY_METADATA_HYDRATION_CONCURRENCY)
                .clamp(1, QUERY_METADATA_HYDRATION_CONCURRENCY);
            let mut candidates: Vec<(String, VertexId, ScalarValue)> =
                Vec::with_capacity(batch_capacity);
            while candidates.len() < batch_capacity {
                if open.iter().all(|stream| stream.head.is_none()) {
                    let Some(group) = groups.next() else {
                        exhausted = true;
                        break;
                    };
                    open.clear();
                    for stream in group {
                        let Some(range) = stream_range(
                            stream,
                            request,
                            resume.as_ref(),
                            &|segment, vertex_id| {
                                Some(
                                    keys::vertex_property_index_entry_suffix(segment, vertex_id)
                                        .into_bytes(),
                                )
                            },
                        )?
                        else {
                            continue;
                        };
                        let prefix = format!("{property_prefix}{}", stream.tag);
                        let iter = budget
                            .read_only_io(
                                "experimental_ordered_vertex_index_seek",
                                self.db.scan_prefix_range_with_options(
                                    prefix.as_bytes(),
                                    // This API seeks from an inclusive start, so
                                    // an excluded bound becomes the immediate
                                    // successor key -- the cursor plus a zero
                                    // byte. Widening to the cursor itself would
                                    // be correct, since the walk rejects
                                    // anything at or before it, but would
                                    // re-read one entry on every page.
                                    match range.0 {
                                        Bound::Included(key) => Some(key),
                                        Bound::Excluded(mut key) => {
                                            key.push(0);
                                            Some(key)
                                        }
                                        Bound::Unbounded => None,
                                    },
                                    match range.1 {
                                        Bound::Excluded(key) => Some(key),
                                        Bound::Included(mut key) => {
                                            key.push(0);
                                            Some(key)
                                        }
                                        Bound::Unbounded => None,
                                    },
                                    &scan_options,
                                ),
                            )
                            .await?;
                        let mut stream = OpenStream { iter, head: None };
                        stream.advance(property, budget, &mut index_entries).await?;
                        open.push(stream);
                    }
                    continue;
                }
                // The smallest head in walk order; ties across streams break
                // by vertex ID in the same direction.
                let mut best: Option<usize> = None;
                for (index, stream) in open.iter().enumerate() {
                    let Some(head) = &stream.head else {
                        continue;
                    };
                    let better = best.is_none_or(|best| {
                        let current = open[best].head.as_ref().expect("selected head");
                        let ordering = compare_property_values(&head.value, &current.value)
                            .then(head.vertex_id.cmp(&current.vertex_id));
                        if request.ascending {
                            ordering == Ordering::Less
                        } else {
                            ordering == Ordering::Greater
                        }
                    });
                    if better {
                        best = Some(index);
                    }
                }
                let stream = &mut open[best.expect("a stream has a head")];
                let entry = stream.head.take().expect("selected head");
                stream.advance(property, budget, &mut index_entries).await?;
                budget.check("experimental_ordered_vertex_property_index")?;

                if let Some((resume_value, resume_id)) = &resume {
                    let ordering = compare_property_values(&entry.value, resume_value)
                        .then(entry.vertex_id.cmp(resume_id));
                    let after = if request.ascending {
                        ordering == Ordering::Greater
                    } else {
                        ordering == Ordering::Less
                    };
                    if !after {
                        continue;
                    }
                }
                // An `index_order` page stops at `required`; a ranking page
                // runs on until an entry sorts past its boundary value.
                if accepted.len() >= required
                    && (request.index_order
                        || boundary.as_ref().is_none_or(|boundary| {
                            compare_property_values(&entry.value, boundary).is_ne()
                        }))
                {
                    stop = true;
                    break;
                }
                // An entry-bounded page ends here without claiming the index is
                // spent; like the stop above, before `last` moves, so the next
                // page resumes at this entry.
                if request.max_index_entries.is_some_and(|max| judged >= max) {
                    stop = true;
                    break;
                }
                judged += 1;
                // Past this point the entry has been judged, however it is
                // judged, so a later page may resume after it. Advancing
                // before the break above would step over entries this loop
                // never returned, and the resume filter would skip them for
                // good.
                last = Some((entry.value.clone(), entry.vertex_id));
                // Charged to the budget the pages share, so the cap bounds the
                // whole walk rather than resetting with every page -- and
                // charged before the entry is judged, because reading it is
                // what the cap is counting. A restriction the seek could only
                // approximate rejects here: `> v` seeks inclusively to `v`, so
                // the whole run at `v` is read and thrown away. Charging after
                // the rejection would let a walk read an unbounded run of them
                // without ever reaching the admission error.
                //
                // An entry-bounded page is the exception: its caller counts
                // the entries every page reports and bounds the whole read
                // itself, as the statistics range seek does with its read
                // limit. That read declines to a fallback rather than failing,
                // and like the probe it must leave the fallback the allowance
                // the fallback has without it -- entries charged here would
                // stay on the tally after the decline.
                if request.max_index_entries.is_none() {
                    self.charge_query_index_candidates(
                        "experimental_ordered_vertex_property_candidates",
                        budget,
                        1,
                    )?;
                }
                if !request.accepts(&entry.value) {
                    continue;
                }
                // A probe already named the vertices that can qualify, so an
                // entry outside that set is skipped without being loaded.
                // Ignoring it would still be correct, just far more I/O.
                if request
                    .allowed
                    .as_ref()
                    .is_some_and(|allowed| allowed.binary_search(&entry.vertex_id).is_err())
                {
                    continue;
                }
                candidates.push((entry.encoded, entry.vertex_id, entry.value));
            }
            if !candidates.is_empty() {
                let vertex_ids = candidates
                    .iter()
                    .map(|(_, vertex_id, _)| *vertex_id)
                    .collect::<Vec<_>>();
                let hydrate_started = std::time::Instant::now();
                // Absent vertices are dropped from the batch, so match records by
                // id: zipping by position would pair later candidates with the
                // wrong vertex's metadata.
                // A vertex can appear twice (a stale index entry beside its
                // current one), so look records up rather than take them.
                let metadata = self
                    .existing_vertex_metadata_batch_at(cell_id, &vertex_ids, read_epoch, budget)
                    .await?
                    .collect::<BTreeMap<_, _>>();
                hydrate_us = hydrate_us.saturating_add(
                    u64::try_from(hydrate_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                );
                for (encoded, vertex_id, value) in candidates {
                    let Some(metadata) = metadata.get(&vertex_id).cloned() else {
                        continue;
                    };
                    // A stale index entry must not surface a value the vertex
                    // no longer holds.
                    if metadata
                        .properties
                        .get(property)
                        .is_none_or(|value| encode_vertex_property_value_key(value) != encoded)
                    {
                        continue;
                    }
                    if !request
                        .labels
                        .iter()
                        .all(|label| metadata.labels.contains(label.as_str()))
                    {
                        continue;
                    }
                    // The same stop the inner loop applies, repeated after
                    // hydration: a batch may carry entries read before the
                    // window filled.
                    if accepted.len() >= required
                        && (request.index_order
                            || boundary.as_ref().is_none_or(|boundary| {
                                compare_property_values(&value, boundary).is_ne()
                            }))
                    {
                        stop = true;
                        break;
                    }
                    let candidate = Candidate {
                        value,
                        vertex_id,
                        ascending: request.ascending,
                        id_ascending: request.id_ascending,
                        metadata,
                    };
                    if accepted.len() < required {
                        self.ensure_query_intermediate_rows(
                            "experimental_ordered_vertex_property_rows",
                            accepted.len().saturating_add(1),
                        )?;
                        accepted.push(candidate);
                        // The window is now full, so everything from here on
                        // is the run of entries tying its worst row. Recording
                        // that value is what lets the walk stop at the end of
                        // the run rather than at the end of the index.
                        if accepted.len() == required {
                            boundary = accepted.peek().map(|worst| worst.value.clone());
                        }
                    } else if let Some(mut worst) = accepted.peek_mut() {
                        // Bounded: a better tie member replaces the worst held
                        // one, so retention never exceeds the window even when
                        // the run is the whole index.
                        if candidate < *worst {
                            *worst = candidate;
                        }
                    }
                }
            }
            if stop || exhausted {
                break;
            }
        }
        // The caller's operator span declares these; outside one they are
        // silently dropped, which is the right behavior for a bare call.
        let span = tracing::Span::current();
        span.record("hydradb.query.scan.index_entries", index_entries);
        span.record("hydradb.query.scan.hydrate_us", hydrate_us);
        let rows = accepted
            .into_sorted_vec()
            .into_iter()
            .map(|row| (row.vertex_id, row.metadata))
            .collect::<Vec<_>>();
        Ok(OrderedVertexPropertyPage {
            rows,
            last,
            exhausted,
            index_entries,
        })
    }
}

struct Entry {
    value: ScalarValue,
    vertex_id: VertexId,
    /// The value segment, type tag included, as stored in the key.
    encoded: String,
}

struct OpenStream {
    iter: slatedb::DbIterator,
    head: Option<Entry>,
}

impl OpenStream {
    async fn advance(
        &mut self,
        property: &str,
        budget: &QueryBudget,
        index_entries: &mut u64,
    ) -> Result<()> {
        loop {
            let Some(kv) = budget
                .read_only_io("experimental_ordered_vertex_index_next", async {
                    self.iter.next().await.map_err(GraphError::from)
                })
                .await?
            else {
                self.head = None;
                return Ok(());
            };
            *index_entries += 1;
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell, indexed_property, encoded, vertex_id) =
                parse_vertex_property_index_key(&key)?;
            // A property whose name extends this one shares the byte prefix.
            if indexed_property != property {
                continue;
            }
            self.head = Some(Entry {
                value: property_to_scalar(&decode_vertex_property_value_key(&encoded)?),
                vertex_id,
                encoded,
            });
            return Ok(());
        }
    }
}

/// The key range of `stream` that can hold a qualifying entry, or `None` when
/// none can. Ranges are relative to the stream prefix (property prefix plus
/// type tag).
/// `exact_resume` builds the key of the entry a continuation stops on, when
/// the caller can name it. Only the caller knows its own key layout -- the
/// vertex index stores one zero-padded vertex ID under a value, the
/// relationship indexes store endpoints -- so a caller that cannot name the
/// entry passes `None` and gets the value-level seek, then filters the run
/// itself.
pub(super) fn stream_range(
    stream: Stream,
    request: &OrderedPropertyScanRequest,
    resume: Option<&(ScalarValue, VertexId)>,
    exact_resume: &dyn Fn(&str, VertexId) -> Option<Vec<u8>>,
) -> Result<Option<KeyRange>> {
    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    match stream.zero {
        Some(ZeroSide::Negative) => upper = Bound::Excluded(POSITIVE_ZERO.as_bytes().to_vec()),
        Some(ZeroSide::Positive) => lower = Bound::Included(POSITIVE_ZERO.as_bytes().to_vec()),
        None => {}
    }
    let mut seeks = Vec::new();
    if let Some(prefix) = &request.prefix {
        if stream.tag != 's' {
            return Ok(None);
        }
        let start = segment(VertexPropertyValue::String(prefix.clone()));
        // Hex digits sort below `g`, and `/` below both, so every key whose
        // value starts with the prefix lies in [prefix, prefix + "g").
        upper = tighter_upper(upper, Bound::Excluded(format!("{start}g").into_bytes()));
        seeks.push(Seek::From(start.into_bytes()));
    }
    if let Some(bound) = &request.lower {
        seeks.push(seek(stream, &bound.value, true)?);
    }
    if let Some(bound) = &request.upper {
        seeks.push(seek(stream, &bound.value, false)?);
    }
    // A continuation starts at the cursor itself, not at its value. Seeking
    // to the value alone would re-read every tie member already returned, so
    // a run of n ties costs O(n^2) across its pages instead of O(n).
    if let Some((value, vertex_id)) = resume {
        match resume_entry_key(stream, value)?
            .and_then(|segment| exact_resume(&segment, *vertex_id).map(|key| (segment, key)))
        {
            // The cursor lives on this stream, so the walk can start strictly
            // past that one entry.
            Some((_, key)) => {
                if request.ascending {
                    lower = tighter_lower(lower, Bound::Excluded(key));
                } else {
                    upper = tighter_upper(upper, Bound::Excluded(key));
                }
            }
            // Another stream entirely: it sorts wholly before or after the
            // cursor, so the value-level answer is already exact.
            None => seeks.push(seek(stream, value, request.ascending)?),
        }
    }
    for restriction in seeks {
        match restriction {
            Seek::All => {}
            Seek::Empty => return Ok(None),
            Seek::From(key) => lower = tighter_lower(lower, Bound::Included(key)),
            Seek::To(key) => upper = tighter_upper(upper, Bound::Included(key)),
        }
    }
    let empty = match (&lower, &upper) {
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower >= upper,
        _ => false,
    };
    Ok((!empty).then_some((lower, upper)))
}

/// The key segment `value` occupies on `stream`, when it is exactly
/// representable there.
///
/// `seek` answers in value granularity and may clamp a number to the stream's
/// range, so a resume position can only be made exact when seeking from and
/// to the value lands on the same segment. Anything else -- another rank, a
/// clamped number -- returns `None`, and the caller keeps the value-level
/// bound, which for those streams is already all-or-nothing.
fn resume_entry_key(stream: Stream, value: &ScalarValue) -> Result<Option<String>> {
    let (Seek::From(from), Seek::To(to)) =
        (seek(stream, value, true)?, seek(stream, value, false)?)
    else {
        return Ok(None);
    };
    let Ok(from) = String::from_utf8(from) else {
        return Ok(None);
    };
    let Ok(to) = String::from_utf8(to) else {
        return Ok(None);
    };
    Ok((to == format!("{from}{ALL_IDS}")).then_some(from))
}

pub(super) fn tighter_lower(current: Bound<Vec<u8>>, candidate: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    let key = |bound: &Bound<Vec<u8>>| match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
        Bound::Unbounded => None,
    };
    match (key(&current), key(&candidate)) {
        (None, _) => candidate,
        (_, None) => current,
        (Some(left), Some(right)) => match left.cmp(&right) {
            Ordering::Less => candidate,
            Ordering::Greater => current,
            Ordering::Equal if matches!(candidate, Bound::Excluded(_)) => candidate,
            Ordering::Equal => current,
        },
    }
}

pub(super) fn tighter_upper(current: Bound<Vec<u8>>, candidate: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    let key = |bound: &Bound<Vec<u8>>| match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
        Bound::Unbounded => None,
    };
    match (key(&current), key(&candidate)) {
        (None, _) => candidate,
        (_, None) => current,
        (Some(left), Some(right)) => match left.cmp(&right) {
            Ordering::Greater => candidate,
            Ordering::Less => current,
            Ordering::Equal if matches!(candidate, Bound::Excluded(_)) => candidate,
            Ordering::Equal => current,
        },
    }
}

/// A sound seek on `stream` for entries at or after `value` (`from`) or at or
/// before it. Conservative wherever an encoding cannot express `value`
/// exactly: it may admit extra entries, which `accepts` then rejects, but
/// never excludes one that compares inside.
fn seek(stream: Stream, value: &ScalarValue, from: bool) -> Result<Seek> {
    let Some(value_rank) = rank(value) else {
        return Err(GraphError::UnsupportedQuery {
            dialect: "Cypher25",
            feature: "ordered property scan bound cannot be NULL".to_string(),
            reason: QueryFailureReason::OrderWindow,
        });
    };
    // Other ranks sort entirely before or after the bound.
    match (stream.rank.cmp(&value_rank), from) {
        (Ordering::Less, true) | (Ordering::Greater, false) => return Ok(Seek::Empty),
        (Ordering::Greater, true) | (Ordering::Less, false) => return Ok(Seek::All),
        (Ordering::Equal, _) => {}
    }
    let at = |segment: String| {
        if from {
            Seek::From(segment.into_bytes())
        } else {
            Seek::To(format!("{segment}{ALL_IDS}").into_bytes())
        }
    };
    let number = match value {
        ScalarValue::Boolean(value) => return Ok(at(segment(VertexPropertyValue::Bool(*value)))),
        ScalarValue::String(value) => {
            return Ok(at(segment(VertexPropertyValue::String(value.to_string()))))
        }
        ScalarValue::Integer(value) => Number::Integer(*value),
        ScalarValue::Float(text) => Number::Float(float_of(text)?),
        ScalarValue::Null => unreachable!("rank rejected NULL"),
    };
    Ok(match stream.tag {
        'i' => integer_seek(number, 0, i128::from(u64::MAX), from, |value| {
            segment(VertexPropertyValue::Integer(value as u64))
        })
        .map_or(Seek::All, |seek| seek.map_or(Seek::Empty, at)),
        'j' => integer_seek(
            number,
            i128::from(i64::MIN),
            i128::from(i64::MAX),
            from,
            |value| segment(VertexPropertyValue::SignedInteger(value as i64)),
        )
        .map_or(Seek::All, |seek| seek.map_or(Seek::Empty, at)),
        'n' => {
            let float = match number {
                Number::Float(value) if value.is_nan() => {
                    // Positive NaN is the largest number, negative the least.
                    return Ok(match (value.is_sign_negative(), from) {
                        (false, false) | (true, true) => Seek::All,
                        (false, true) => at(segment(float_value(f64::INFINITY))),
                        (true, false) => at(segment(float_value(f64::NEG_INFINITY))),
                    });
                }
                Number::Float(value) => value,
                Number::Integer(value) => {
                    let rounded = value as f64;
                    let ordering = compare_property_values(
                        &ScalarValue::Float(rounded.to_string().into()),
                        &ScalarValue::Integer(value),
                    );
                    match (ordering, from) {
                        (Ordering::Greater, true) => rounded.next_down(),
                        (Ordering::Less, false) => rounded.next_up(),
                        _ => rounded,
                    }
                }
            };
            // `-0.0` and `0.0` are one value on either side of the split.
            let float = if float == 0.0 {
                if from {
                    -0.0
                } else {
                    0.0
                }
            } else {
                float
            };
            at(segment(float_value(float)))
        }
        _ => unreachable!("only numeric streams share the numeric rank"),
    })
}

fn float_value(value: f64) -> VertexPropertyValue {
    VertexPropertyValue::Float(QueryFloat(value))
}

/// Seek within an integer stream holding `min..=max`. `None` means the whole
/// stream qualifies, `Some(None)` that none does, and `Some(Some(key))` the
/// segment to seek to.
fn integer_seek(
    number: Number,
    min: i128,
    max: i128,
    from: bool,
    encode: impl Fn(i128) -> String,
) -> Option<Option<String>> {
    // The integer nearest the bound on the side that keeps every qualifying
    // entry: the floor for a lower bound is conservative, and the floor is
    // also the largest integer at or below an upper bound.
    let target = match number {
        Number::Integer(value) => value,
        Number::Float(value) if value.is_nan() => {
            return if value.is_sign_negative() == from {
                None
            } else {
                Some(None)
            };
        }
        Number::Float(value) => {
            let floor = value.floor();
            if floor < min as f64 {
                return if from { None } else { Some(None) };
            }
            if floor > max as f64 {
                return if from { Some(None) } else { None };
            }
            floor as i128
        }
    };
    if from {
        if target <= min {
            return None;
        }
        if target > max {
            return Some(None);
        }
    } else {
        if target >= max {
            return None;
        }
        if target < min {
            return Some(None);
        }
    }
    Some(Some(encode(target)))
}
