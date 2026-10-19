use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use hydradb_cypher_engine::{
    EngineError, ListParameterMap, LogicalQuery, LowerTimings, LoweredTemplate, ParameterMap,
    PreparedQuery, ScalarValue, UnknownStatistics,
};

use crate::QueryFailureReason;
use crate::{GraphError, QueryColumn, Result, VertexPropertyValue};

pub(crate) struct ExperimentalCypherRequest {
    pub(crate) explain: bool,
    pub(crate) prepared: PreparedQuery,
}

pub(crate) fn prepare_experimental_cypher(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<ExperimentalCypherRequest> {
    let (explain, logical) = lower_experimental_cypher(query, parameters, lists)?;
    let prepared = logical
        .plan_with_statistics(&UnknownStatistics)
        .map_err(experimental_engine_error)?;
    Ok(ExperimentalCypherRequest { explain, prepared })
}

pub(crate) fn lower_experimental_cypher(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<(bool, LogicalQuery)> {
    let (explain, logical, _) = lower_experimental_cypher_cached(query, parameters, lists)?;
    Ok((explain, logical))
}

/// Like [`lower_experimental_cypher`], also reporting whether the parsed and
/// lowered template came from the process-wide cache.
pub(crate) fn lower_experimental_cypher_cached(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<(bool, LogicalQuery, bool)> {
    let (explain, logical, lowering) = lower_experimental_cypher_timed(query, parameters, lists)?;
    Ok((explain, logical, lowering.cache_hit))
}

/// How one request obtained its lowered template.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TemplateLowering {
    pub(crate) cache_hit: bool,
    /// Parser and lowering time. Both zero on a cache hit, which did neither.
    pub(crate) timings: LowerTimings,
}

/// Like [`lower_experimental_cypher_cached`], also reporting the parse and
/// lower split of a cache miss.
pub(crate) fn lower_experimental_cypher_timed(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<(bool, LogicalQuery, TemplateLowering)> {
    let (explain, query) = split_explain(query)?;
    let parameters = experimental_parameter_map(parameters)?;
    let lists = experimental_list_parameter_map(lists);
    let (template, lowering) = lowered_template(query)?;
    Ok((explain, template.bind(&parameters, &lists), lowering))
}

/// Upper bound on cached templates. Each holds one AST and one logical plan
/// for a distinct query text; a few thousand covers any real application's
/// prepared-statement vocabulary while bounding memory for an adversarial
/// stream of unique texts.
const LOWERED_TEMPLATE_CACHE_ENTRIES: usize = 4_096;
/// Upper bound on the estimated heap the cache retains across all entries.
/// A typical application query costs 2–6 KiB, so this holds thousands of
/// shapes; a stream of large texts is evicted by bytes long before it
/// reaches the entry cap.
const LOWERED_TEMPLATE_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
/// Texts longer than this are lowered but never cached: a giant one-off
/// query must not evict the working set.
const LOWERED_TEMPLATE_CACHE_MAX_QUERY_BYTES: usize = 16 * 1024;

/// Process-wide cache of parsed-and-lowered query templates, keyed by query
/// text after the `EXPLAIN` prefix is stripped.
///
/// Parsing is the single largest fixed cost of an experimental request
/// (75–120 µs for a shape with `ORDER BY`, against ~50 µs for the whole
/// legacy request), and the same text is parsed up to three times per
/// request across Bolt column discovery, client preparation and shard
/// execution. Templates depend on nothing but the text — parameters are bound
/// afterwards and statistics are consulted only at physical planning — so the
/// cache needs no invalidation short of a new binary.
///
/// Process-wide rather than per shard for the same reason: the template is a
/// pure function of the text, and the client transports that also lower have
/// no shard in hand.
struct LoweredTemplateEntry {
    template: LoweredTemplate,
    bytes: usize,
    last_used: u64,
}

/// A byte- and entry-bounded LRU. `entries` answers lookups; `by_use` orders
/// keys by their last-use tick so eviction is a pop of the smallest tick and
/// a hit is two O(log n) index updates. The tick is a monotonic counter, so
/// it is unique per touch and `by_use` never needs to resolve collisions.
/// Nothing here is O(n): a sustained stream of distinct texts costs each
/// request one parse plus a handful of map operations under the lock.
struct LoweredTemplateCache {
    entries: HashMap<String, LoweredTemplateEntry>,
    by_use: BTreeMap<u64, String>,
    max_entries: usize,
    max_bytes: usize,
    resident_bytes: usize,
    clock: u64,
}

impl LoweredTemplateCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            by_use: BTreeMap::new(),
            max_entries,
            max_bytes,
            resident_bytes: 0,
            clock: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn get(&mut self, query: &str) -> Option<LoweredTemplate> {
        let now = self.tick();
        let entry = self.entries.get_mut(query)?;
        self.by_use.remove(&entry.last_used);
        entry.last_used = now;
        self.by_use.insert(now, query.to_string());
        Some(entry.template.clone())
    }

    fn remove_entry(&mut self, query: &str) -> Option<LoweredTemplateEntry> {
        let entry = self.entries.remove(query)?;
        self.by_use.remove(&entry.last_used);
        self.resident_bytes = self.resident_bytes.saturating_sub(entry.bytes);
        Some(entry)
    }

    fn evict_least_recently_used(&mut self) -> bool {
        let Some((_, victim)) = self.by_use.pop_first() else {
            return false;
        };
        if let Some(entry) = self.entries.remove(&victim) {
            self.resident_bytes = self.resident_bytes.saturating_sub(entry.bytes);
        }
        true
    }

    fn insert(&mut self, query: &str, template: LoweredTemplate) {
        let bytes = query.len() + template.estimated_bytes();
        if bytes > self.max_bytes {
            return;
        }
        self.remove_entry(query);
        // Evict least recently used until both the entry and byte budgets
        // hold. Each eviction is one pop from the ordered index.
        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries
                || self.resident_bytes + bytes > self.max_bytes)
        {
            if !self.evict_least_recently_used() {
                break;
            }
        }
        let now = self.tick();
        self.resident_bytes += bytes;
        self.by_use.insert(now, query.to_string());
        self.entries.insert(
            query.to_string(),
            LoweredTemplateEntry {
                template,
                bytes,
                last_used: now,
            },
        );
    }
}

static LOWERED_TEMPLATE_CACHE: OnceLock<Mutex<LoweredTemplateCache>> = OnceLock::new();

fn lowered_template_cache() -> &'static Mutex<LoweredTemplateCache> {
    LOWERED_TEMPLATE_CACHE.get_or_init(|| {
        Mutex::new(LoweredTemplateCache::new(
            LOWERED_TEMPLATE_CACHE_ENTRIES,
            LOWERED_TEMPLATE_CACHE_MAX_BYTES,
        ))
    })
}

fn lowered_template(query: &str) -> Result<(LoweredTemplate, TemplateLowering)> {
    let cacheable = query.len() <= LOWERED_TEMPLATE_CACHE_MAX_QUERY_BYTES;
    if cacheable {
        let cached = lowered_template_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(query);
        if let Some(template) = cached {
            return Ok((
                template,
                TemplateLowering {
                    cache_hit: true,
                    timings: LowerTimings::default(),
                },
            ));
        }
    }
    let (template, timings) =
        LoweredTemplate::lower_timed(query).map_err(experimental_engine_error)?;
    if cacheable {
        lowered_template_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(query, template.clone());
    }
    Ok((
        template,
        TemplateLowering {
            cache_hit: false,
            timings,
        },
    ))
}

pub(crate) fn experimental_query_columns(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<Vec<QueryColumn>> {
    let request = prepare_experimental_cypher(query, parameters, lists)?;
    if request.explain {
        return Ok(vec![QueryColumn::new("plan")]);
    }
    Ok(request
        .prepared
        .column_names()
        .map_err(experimental_engine_error)?
        .into_iter()
        .map(QueryColumn::new)
        .collect())
}

pub(crate) fn is_experimental_explain(query: &str) -> bool {
    query
        .split_whitespace()
        .next()
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("EXPLAIN"))
}

pub(crate) fn experimental_parameter_map(
    parameters: &BTreeMap<String, VertexPropertyValue>,
) -> Result<ParameterMap> {
    parameters
        .iter()
        .map(|(name, value)| Ok((name.clone(), property_to_scalar(value))))
        .collect()
}

/// The list sidecar the engine's `IN` reads. Element conversion is the same
/// one scalars go through, so a list of strings binds exactly as fifty separate
/// string parameters would.
pub(crate) fn experimental_list_parameter_map(
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> ListParameterMap {
    lists
        .iter()
        .map(|(name, values)| {
            (
                name.clone(),
                values.iter().map(property_to_scalar).collect::<Vec<_>>(),
            )
        })
        .collect()
}

pub(crate) fn property_to_scalar(value: &VertexPropertyValue) -> ScalarValue {
    match value {
        VertexPropertyValue::Integer(value) => ScalarValue::Integer(i128::from(*value)),
        VertexPropertyValue::SignedInteger(value) => ScalarValue::Integer(i128::from(*value)),
        VertexPropertyValue::Bool(value) => ScalarValue::Boolean(*value),
        VertexPropertyValue::Float(value) => ScalarValue::Float(value.0.to_string().into()),
        VertexPropertyValue::String(value) => ScalarValue::String(value.clone().into()),
    }
}

pub(crate) fn experimental_engine_error(error: EngineError) -> GraphError {
    match error.failure_reason() {
        Some(reason) => experimental_query_error(reason, error),
        // A storage failure relayed as a string: its real class is lost, so
        // it keeps the wording clients always saw and is left uncounted.
        None => GraphError::UnclassifiedQuery {
            dialect: "Cypher25",
            feature: error.to_string(),
        },
    }
}

pub(crate) fn experimental_query_error(
    reason: QueryFailureReason,
    error: impl std::fmt::Display,
) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "Cypher25",
        feature: error.to_string(),
        reason,
    }
}

fn split_explain(query: &str) -> Result<(bool, &str)> {
    let query = query.trim();
    let Some(first) = query.split_whitespace().next() else {
        return Err(experimental_query_error(
            QueryFailureReason::ParseError,
            "query is empty",
        ));
    };
    if !is_experimental_explain(query) {
        return Ok((false, query));
    }
    let rest = query[first.len()..].trim_start();
    if rest.is_empty() {
        return Err(experimental_query_error(
            QueryFailureReason::ParseError,
            "EXPLAIN must be followed by a Cypher query",
        ));
    }
    Ok((true, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hydradb_cypher_engine::Symbol;

    /// A storage failure reaches the engine as a string, so a timeout while
    /// reading cannot be told from anything else. It keeps the wording clients
    /// always saw but carries no reason, so it is not counted as a query
    /// failure; a real planning gap still is.
    #[test]
    fn a_relayed_storage_error_is_not_filed_as_a_query_failure() {
        let storage = experimental_engine_error(EngineError::Storage(
            hydradb_cypher_engine::StorageError::new(
                "experimental_cypher_execute exceeded query timeout after 30000 ms",
            ),
        ));
        assert!(matches!(storage, GraphError::UnclassifiedQuery { .. }));
        assert_eq!(storage.failure_reason(), None);
        assert!(storage
            .to_string()
            .starts_with("Cypher25 query is not supported yet: graph storage error"));

        let unsupported = experimental_engine_error(EngineError::UnsupportedPlan(
            QueryFailureReason::Where,
            "not lowered".to_string(),
        ));
        assert_eq!(
            unsupported.failure_reason(),
            Some(QueryFailureReason::Where)
        );
    }

    /// The wiring this module exists for: a list bound by a client has to reach
    /// the engine, or `IN` is understood by something nothing can talk to.
    #[test]
    fn a_bound_list_reaches_the_engine_for_an_in_predicate() {
        let lists = BTreeMap::from([(
            "chunk_ids".to_string(),
            vec![
                VertexPropertyValue::String("chunk-a".to_string()),
                VertexPropertyValue::String("chunk-b".to_string()),
            ],
        )]);

        let request = prepare_experimental_cypher(
            "MATCH (c:Chunk) WHERE c.chunk_id IN $chunk_ids RETURN c.chunk_id AS id",
            &BTreeMap::new(),
            &lists,
        )
        .expect("an IN query prepares once its list is bound");

        assert!(!request.explain);
        assert!(
            request
                .prepared
                .explain()
                .contains("VertexPropertyMultiSeek"),
            "the bound list should reach the planner: {}",
            request.prepared.explain()
        );
    }

    /// Without the list the same query cannot be planned. This is the failure a
    /// client saw as a rejected parameter, one layer lower.
    #[test]
    fn the_same_query_without_its_list_is_rejected() {
        assert!(prepare_experimental_cypher(
            "MATCH (c:Chunk) WHERE c.chunk_id IN $chunk_ids RETURN c.chunk_id AS id",
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .is_err());
    }

    /// A repeated text is parsed once; different parameter values and the
    /// EXPLAIN prefix reuse the same template, and the bound plans still
    /// carry each request's own parameters.
    #[test]
    fn a_repeated_query_text_reuses_its_lowered_template() {
        // A text no other test lowers, so the first call is a guaranteed miss
        // even with the process-wide cache shared across the test binary.
        let query = "MATCH (e:Entity) WHERE e.entity_id = $id \
                     RETURN e.entity_id AS template_cache_probe \
                     ORDER BY template_cache_probe ASC LIMIT 5";
        let first = BTreeMap::from([(
            "id".to_string(),
            VertexPropertyValue::String("alpha".to_string()),
        )]);
        let second = BTreeMap::from([(
            "id".to_string(),
            VertexPropertyValue::String("beta".to_string()),
        )]);
        let (explain, cold, hit) =
            lower_experimental_cypher_cached(query, &first, &BTreeMap::new()).unwrap();
        assert!(!explain);
        assert!(!hit, "first sight of a text must lower it");
        let (_, warm, hit) =
            lower_experimental_cypher_cached(query, &second, &BTreeMap::new()).unwrap();
        assert!(hit, "second sight of the same text must reuse the template");
        let (explain, explained, hit) =
            lower_experimental_cypher_cached(&format!("EXPLAIN {query}"), &first, &BTreeMap::new())
                .unwrap();
        assert!(explain);
        assert!(hit, "EXPLAIN shares the template of the text it explains");

        // Process-wide counters are shared with every other test in this
        // binary, so identity of the shared template is the assertion, not a
        // hit/miss delta.
        assert!(std::sync::Arc::ptr_eq(&cold.logical, &warm.logical));
        assert!(std::sync::Arc::ptr_eq(&cold.logical, &explained.logical));
        assert_eq!(
            cold.planning_context().parameters.get(&Symbol::from("id")),
            Some(&ScalarValue::String("alpha".into()))
        );
        assert_eq!(
            warm.planning_context().parameters.get(&Symbol::from("id")),
            Some(&ScalarValue::String("beta".into()))
        );
    }

    /// The cache honors both budgets: the byte cap evicts least recently
    /// used templates before the entry cap is reached, a template larger than
    /// the whole budget is never admitted, and accounting stays exact across
    /// evictions and re-inserts.
    #[test]
    fn the_template_cache_evicts_by_bytes_and_entries() {
        let small = LoweredTemplate::lower("MATCH (a:Entity) RETURN a.id AS id").unwrap();
        let small_bytes = "MATCH (a:Entity) RETURN a.id AS id".len() + small.estimated_bytes();
        assert!(small_bytes > 0);

        // Room for two small templates by bytes, many by count.
        let mut cache = LoweredTemplateCache::new(64, small_bytes * 2 + small_bytes / 2);
        cache.insert("MATCH (a:Entity) RETURN a.id AS id", small.clone());
        cache.insert("MATCH (b:Entity) RETURN b.id AS id", small.clone());
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.resident_bytes <= cache.max_bytes);

        // Touch the first so the second is the least recently used.
        assert!(cache.get("MATCH (a:Entity) RETURN a.id AS id").is_some());
        cache.insert("MATCH (c:Entity) RETURN c.id AS id", small.clone());
        assert_eq!(cache.entries.len(), 2, "the byte cap forced one eviction");
        assert!(cache.get("MATCH (b:Entity) RETURN b.id AS id").is_none());
        assert!(cache.get("MATCH (a:Entity) RETURN a.id AS id").is_some());
        assert!(cache.get("MATCH (c:Entity) RETURN c.id AS id").is_some());
        assert!(cache.resident_bytes <= cache.max_bytes);

        // Re-inserting an existing key replaces, never double-counts, and the
        // recency index stays one-to-one with the entries.
        let before = cache.resident_bytes;
        cache.insert("MATCH (c:Entity) RETURN c.id AS id", small.clone());
        assert_eq!(cache.resident_bytes, before);
        assert_eq!(cache.by_use.len(), cache.entries.len());
        assert!(cache
            .by_use
            .values()
            .all(|key| cache.entries.contains_key(key)));

        // A template bigger than the whole budget is refused outright.
        let mut tiny = LoweredTemplateCache::new(64, small_bytes - 1);
        tiny.insert("MATCH (a:Entity) RETURN a.id AS id", small.clone());
        assert!(tiny.entries.is_empty());
        assert_eq!(tiny.resident_bytes, 0);

        // The entry cap still applies when bytes are plentiful.
        let mut counted = LoweredTemplateCache::new(1, usize::MAX);
        counted.insert("MATCH (a:Entity) RETURN a.id AS id", small.clone());
        counted.insert("MATCH (b:Entity) RETURN b.id AS id", small);
        assert_eq!(counted.entries.len(), 1);
        assert!(counted.get("MATCH (b:Entity) RETURN b.id AS id").is_some());
        assert_eq!(counted.by_use.len(), 1);

        // A long stream of distinct texts keeps both structures bounded and
        // in step; this is the churn case a linear eviction would serialize.
        let mut churn = LoweredTemplateCache::new(8, usize::MAX);
        for i in 0..64 {
            let text = format!("MATCH (n:Entity) RETURN n.id AS churn_{i}");
            churn.insert(&text, LoweredTemplate::lower(&text).unwrap());
            assert!(churn.entries.len() <= 8);
            assert_eq!(churn.by_use.len(), churn.entries.len());
        }
        assert!(churn
            .get("MATCH (n:Entity) RETURN n.id AS churn_63")
            .is_some());
        assert!(churn
            .get("MATCH (n:Entity) RETURN n.id AS churn_0")
            .is_none());
    }

    #[test]
    fn explain_uses_the_experimental_plan_and_one_transport_column() {
        let request = prepare_experimental_cypher(
            "explain MATCH (e:Entity) RETURN e",
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .expect("prepare experimental explain");
        assert!(request.explain);
        assert!(request.prepared.explain().contains("VertexLabelScan"));
        assert_eq!(
            experimental_query_columns(
                "EXPLAIN MATCH (e:Entity) RETURN e",
                &BTreeMap::new(),
                &BTreeMap::new()
            )
            .unwrap(),
            vec![QueryColumn::new("plan")]
        );
    }
}
