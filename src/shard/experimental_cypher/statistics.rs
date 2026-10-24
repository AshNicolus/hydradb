//! Persisted optimizer facts from the same storage generation as execution.
use std::sync::atomic::AtomicUsize;

use futures::{stream, StreamExt, TryStreamExt};
use hydradb_cypher_engine::{
    BloomFilterStatistics, GraphLogicalPlan, LogicalBinaryOperator, LogicalExpression,
    LogicalInList, PatternDirection, PhysicalPlanningContext, PropertyStatistics, ScalarValue,
    StatisticsProvider, Symbol,
};

use super::*;

const STATISTICS_READ_CONCURRENCY: usize = 8;
const MAX_EXACT_STATISTICS_VALUES: usize = 16;

#[derive(Default)]
pub(super) struct SnapshotStatistics {
    labels: BTreeMap<Vec<Symbol>, u64>,
    relationships: BTreeMap<Symbol, u64>,
    expansions: BTreeMap<(Vec<Symbol>, Symbol, PatternDirection), u64>,
    vertex_properties: BTreeMap<Symbol, QueryStatsRecord>,
    vertex_values: BTreeMap<(Symbol, ScalarValue), u64>,
    relationship_properties: BTreeMap<(Symbol, Symbol), QueryStatsRecord>,
    relationship_values: BTreeMap<(Symbol, Symbol, ScalarValue), u64>,
}

impl StatisticsProvider for SnapshotStatistics {
    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        (!labels.is_empty())
            .then(|| canonical_symbols(labels))
            .and_then(|labels| self.labels.get(&labels).copied())
    }

    fn property_statistics(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
    ) -> Option<PropertyStatistics> {
        self.vertex_properties
            .get(property)
            .map(property_statistics)
    }

    fn property_value_count(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<u64> {
        self.vertex_values
            .get(&(property.clone(), value.clone()))
            .copied()
    }

    fn bloom_may_contain(
        &self,
        _labels: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<bool> {
        let bloom = self.vertex_properties.get(property)?.bloom.as_ref()?;
        let keys = scalar_property_keys(value);
        (!keys.is_empty()).then(|| keys.iter().any(|key| bloom.may_contain_encoded(key)))
    }

    fn relationship_count(&self, relationship_types: &[Symbol]) -> Option<u64> {
        sum_complete(
            relationship_types
                .iter()
                .map(|relationship_type| self.relationships.get(relationship_type).copied()),
        )
    }

    fn relationship_expansion_count(
        &self,
        source_labels: &[Symbol],
        relationship_types: &[Symbol],
        direction: PatternDirection,
    ) -> Option<u64> {
        let source_labels = canonical_symbols(source_labels);
        sum_complete(relationship_types.iter().map(|relationship_type| {
            self.expansions
                .get(&(source_labels.clone(), relationship_type.clone(), direction))
                .copied()
        }))
    }

    fn relationship_property_statistics(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
    ) -> Option<PropertyStatistics> {
        let records = relationship_types
            .iter()
            .map(|relationship_type| {
                self.relationship_properties
                    .get(&(relationship_type.clone(), property.clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        let non_null_count = records
            .iter()
            .try_fold(0_u64, |total, record| total.checked_add(record.count))?;
        let distinct_count = records.iter().try_fold(0_u64, |total, record| {
            total.checked_add(record.distinct_values)
        })?;
        Some(PropertyStatistics {
            non_null_count: Some(non_null_count),
            distinct_count: Some(distinct_count),
            bloom: aggregate_bloom_statistics(records.into_iter()),
            ..PropertyStatistics::default()
        })
    }

    fn relationship_property_value_count(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<u64> {
        sum_complete(relationship_types.iter().map(|relationship_type| {
            self.relationship_values
                .get(&(relationship_type.clone(), property.clone(), value.clone()))
                .copied()
        }))
    }

    fn relationship_bloom_may_contain(
        &self,
        relationship_types: &[Symbol],
        property: &Symbol,
        value: &ScalarValue,
    ) -> Option<bool> {
        let keys = scalar_property_keys(value);
        if keys.is_empty() || relationship_types.is_empty() {
            return None;
        }
        let mut possibly_present = false;
        for relationship_type in relationship_types {
            let bloom = self
                .relationship_properties
                .get(&(relationship_type.clone(), property.clone()))?
                .bloom
                .as_ref()?;
            possibly_present |= keys.iter().any(|key| bloom.may_contain_encoded(key));
        }
        Some(possibly_present)
    }
}

fn sum_complete(mut values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut saw_value = false;
    let total = values.try_fold(0_u64, |total, value| {
        saw_value = true;
        total.checked_add(value?)
    })?;
    saw_value.then_some(total)
}

fn property_statistics(record: &QueryStatsRecord) -> PropertyStatistics {
    PropertyStatistics {
        non_null_count: Some(record.total_values),
        distinct_count: Some(record.distinct_values),
        bloom: record.bloom.as_ref().map(|bloom| BloomFilterStatistics {
            bit_count: bloom.bit_count,
            hash_count: bloom.hash_count,
            inserted_count: bloom.inserted_count,
            false_positive_rate_ppm: bloom.false_positive_rate_ppm,
        }),
        ..PropertyStatistics::default()
    }
}

fn aggregate_bloom_statistics<'a>(
    records: impl Iterator<Item = &'a QueryStatsRecord>,
) -> Option<BloomFilterStatistics> {
    records
        .map(|record| record.bloom.as_ref())
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .try_fold(
            BloomFilterStatistics {
                bit_count: 0,
                hash_count: 0,
                inserted_count: 0,
                false_positive_rate_ppm: 0,
            },
            |mut total, bloom| {
                total.bit_count = total.bit_count.checked_add(bloom.bit_count)?;
                total.hash_count = total.hash_count.max(bloom.hash_count);
                total.inserted_count = total.inserted_count.checked_add(bloom.inserted_count)?;
                total.false_positive_rate_ppm = total
                    .false_positive_rate_ppm
                    .max(bloom.false_positive_rate_ppm);
                Some(total)
            },
        )
}

impl GraphShard {
    /// [`GraphShard::query_stats_record`] behind a shard-wide memo keyed by
    /// the storage sequence being planned against. The per-snapshot memo that
    /// method already keeps dies with each request's snapshot, so before this
    /// every request re-read every fact it needed — on a store with no
    /// statistics at all, 40–100 µs of point reads per request to learn
    /// nothing. Keying by sequence keeps the ticket's rule that statistics
    /// never cross snapshots: a write advances the sequence and the next
    /// request reads fresh. Returns the record and whether it was memoized.
    async fn memoized_query_stats_record(
        &self,
        cell_id: &str,
        key: &str,
        read_epoch: StorageSequence,
    ) -> Result<(Option<QueryStatsRecord>, bool)> {
        let memo_key = (key.to_string(), read_epoch);
        if let Some(record) = self
            .experimental_statistics_memo
            .lock()
            .await
            .get(&memo_key)
        {
            return Ok((record, true));
        }
        let record = self.query_stats_record(key).await?;
        let resident_bytes = key.len()
            + std::mem::size_of::<Option<QueryStatsRecord>>()
            + record
                .as_ref()
                .and_then(|record| record.bloom.as_ref())
                .map_or(0, |bloom| bloom.bits.len());
        self.experimental_statistics_memo.lock().await.insert_sized(
            memo_key,
            record.clone(),
            cell_id.to_string(),
            false,
            resident_bytes,
            &self.cache_metrics,
        );
        Ok((record, false))
    }
}

impl SnapshotStatistics {
    /// Caller scopes this operation to the pinned query snapshot. Reads are
    /// point lookups only; neither missing stats nor an EXPLAIN scans graph data.
    pub(super) async fn load(
        shard: &GraphShard,
        cell_id: &str,
        read_epoch: StorageSequence,
        logical: &GraphLogicalPlan,
        context: &PhysicalPlanningContext,
        budget: &QueryBudget,
    ) -> Result<Self> {
        let needs = StatisticsNeeds::from_plan(logical, context);
        let requests = needs.requests(cell_id);
        tracing::Span::current().record("hydradb.query.stats.requested_records", requests.len());
        let now_ms = graph_now_millis();
        let memo_hits = AtomicUsize::new(0);
        let records: Vec<_> = stream::iter(requests)
            .map(|(fact, key)| {
                let memo_hits = &memo_hits;
                async move {
                    let (record, memoized) = budget
                        .read_only_io(
                            "experimental_cypher_statistics_read",
                            shard.memoized_query_stats_record(cell_id, &key, read_epoch),
                        )
                        .await?;
                    if memoized {
                        memo_hits.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok::<_, GraphError>((fact, record))
                }
            })
            .buffer_unordered(STATISTICS_READ_CONCURRENCY)
            .try_collect()
            .await?;
        tracing::Span::current().record(
            "hydradb.query.stats.memo_hits",
            memo_hits.load(Ordering::Relaxed),
        );
        let mut result = Self::default();
        let mut available_records = 0_usize;
        for (fact, record) in records {
            let Some(record) = record.filter(|record| !record.is_unusable_at(read_epoch, now_ms))
            else {
                continue;
            };
            available_records += 1;
            result.absorb(fact, record);
        }
        tracing::Span::current().record("hydradb.query.stats.available_records", available_records);
        Ok(result)
    }

    fn absorb(&mut self, fact: StatisticFact, record: QueryStatsRecord) {
        match fact {
            StatisticFact::VertexLabels(labels) => {
                self.labels.insert(labels, record.count);
            }
            StatisticFact::RelationshipType(relationship_type) => {
                self.relationships.insert(relationship_type, record.count);
            }
            StatisticFact::Expansion(source_labels, relationship_type, direction) => {
                self.expansions
                    .insert((source_labels, relationship_type, direction), record.count);
            }
            StatisticFact::VertexProperty(property) => {
                self.vertex_properties.insert(property, record);
            }
            StatisticFact::VertexValue(property, value) => {
                self.vertex_values
                    .entry((property, value))
                    .and_modify(|count| *count = count.saturating_add(record.count))
                    .or_insert(record.count);
            }
            StatisticFact::RelationshipProperty(relationship_type, property) => {
                self.relationship_properties
                    .insert((relationship_type, property), record);
            }
            StatisticFact::RelationshipValue(relationship_type, property, value) => {
                self.relationship_values
                    .entry((relationship_type, property, value))
                    .and_modify(|count| *count = count.saturating_add(record.count))
                    .or_insert(record.count);
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum StatisticFact {
    VertexLabels(Vec<Symbol>),
    RelationshipType(Symbol),
    Expansion(Vec<Symbol>, Symbol, PatternDirection),
    VertexProperty(Symbol),
    VertexValue(Symbol, ScalarValue),
    RelationshipProperty(Symbol, Symbol),
    RelationshipValue(Symbol, Symbol, ScalarValue),
}

#[derive(Clone, Debug)]
enum BindingStatistics {
    Vertex,
    Relationship(Vec<Symbol>),
    Ambiguous,
}

#[derive(Default)]
struct StatisticsNeeds {
    labels: BTreeSet<Vec<Symbol>>,
    relationship_types: BTreeSet<Symbol>,
    expansions: BTreeSet<(Vec<Symbol>, Symbol, PatternDirection)>,
    vertex_properties: BTreeSet<Symbol>,
    vertex_values: BTreeSet<(Symbol, ScalarValue)>,
    relationship_properties: BTreeSet<(Symbol, Symbol)>,
    relationship_values: BTreeSet<(Symbol, Symbol, ScalarValue)>,
}

impl StatisticsNeeds {
    fn from_plan(logical: &GraphLogicalPlan, context: &PhysicalPlanningContext) -> Self {
        let mut needs = Self::default();
        visit_plan(logical, &mut |plan| match plan {
            GraphLogicalPlan::NodeScan { labels, .. } => {
                needs.add_labels(labels);
            }
            GraphLogicalPlan::Expand {
                input,
                from,
                relationship_types,
                direction,
                target_labels,
                ..
            } => {
                needs.add_relationship_types(relationship_types);
                needs.add_labels(target_labels);
                needs.add_expansion(input, from, relationship_types, *direction);
            }
            GraphLogicalPlan::VariableExpand {
                input,
                from,
                relationship_types,
                direction,
                target_labels,
                ..
            } => {
                needs.add_relationship_types(relationship_types);
                needs.add_labels(target_labels);
                needs.add_expansion(input, from, relationship_types, *direction);
            }
            _ => {}
        });
        visit_plan(logical, &mut |plan| {
            if let GraphLogicalPlan::Filter { input, predicate } = plan {
                // Bindings are scoped to this filter's input. UNION arms may
                // legally reuse the same symbol for different entity kinds;
                // classifying the entire query at once would make both arms
                // ambiguous and suppress their property statistics.
                let bindings = binding_statistics(input);
                needs.add_expression(predicate, context, &bindings);
            }
        });
        needs
    }

    fn add_labels(&mut self, labels: &[Symbol]) {
        if labels.is_empty() {
            return;
        }
        let labels = canonical_symbols(labels);
        self.labels.insert(labels.clone());
        self.labels
            .extend(labels.into_iter().map(|label| vec![label]));
    }

    fn add_relationship_types(&mut self, relationship_types: &[Symbol]) {
        self.relationship_types
            .extend(relationship_types.iter().cloned());
    }

    fn add_expansion(
        &mut self,
        input: &GraphLogicalPlan,
        from: &Symbol,
        relationship_types: &[Symbol],
        direction: PatternDirection,
    ) {
        let Some(source_labels) = vertex_labels_for_binding(input, from) else {
            return;
        };
        if source_labels.is_empty() {
            return;
        }
        self.expansions.extend(
            relationship_types
                .iter()
                .cloned()
                .map(|relationship_type| (source_labels.clone(), relationship_type, direction)),
        );
    }

    fn add_expression(
        &mut self,
        expression: &LogicalExpression,
        context: &PhysicalPlanningContext,
        bindings: &BTreeMap<Symbol, BindingStatistics>,
    ) {
        if let LogicalExpression::Binary {
            left,
            operator: LogicalBinaryOperator::Equal,
            right,
        } = expression
        {
            self.add_property_values(left, constant_values(right, context), bindings);
            self.add_property_values(right, constant_values(left, context), bindings);
        }
        if let LogicalExpression::In { expression, list } = expression {
            self.add_property_values(expression, in_values(list, context), bindings);
        }
        if let LogicalExpression::Property { binding, property } = expression {
            self.add_property(binding, property, bindings);
        }
        match expression {
            LogicalExpression::Unary { expression, .. } => {
                self.add_expression(expression, context, bindings)
            }
            LogicalExpression::Function { arguments, .. } | LogicalExpression::List(arguments) => {
                for argument in arguments {
                    self.add_expression(argument, context, bindings);
                }
            }
            LogicalExpression::Case {
                operand,
                branches,
                default,
            } => {
                for part in operand.iter().chain(default.iter()) {
                    self.add_expression(part, context, bindings);
                }
                for (when, then) in branches {
                    self.add_expression(when, context, bindings);
                    self.add_expression(then, context, bindings);
                }
            }
            LogicalExpression::Aggregate { expression, .. } => {
                if let Some(expression) = expression {
                    self.add_expression(expression, context, bindings);
                }
            }
            LogicalExpression::Binary { left, right, .. } => {
                self.add_expression(left, context, bindings);
                self.add_expression(right, context, bindings);
            }
            LogicalExpression::In { expression, list } => {
                self.add_expression(expression, context, bindings);
                if let LogicalInList::Values(values) = list {
                    for value in values {
                        self.add_expression(value, context, bindings);
                    }
                }
            }
            LogicalExpression::Binding(_)
            | LogicalExpression::Identity(_)
            | LogicalExpression::Property { .. }
            | LogicalExpression::Parameter(_)
            | LogicalExpression::Literal(_) => {}
        }
    }

    fn add_property(
        &mut self,
        binding: &Symbol,
        property: &Symbol,
        bindings: &BTreeMap<Symbol, BindingStatistics>,
    ) {
        match bindings.get(binding) {
            Some(BindingStatistics::Vertex) => {
                self.vertex_properties.insert(property.clone());
            }
            Some(BindingStatistics::Relationship(types)) => {
                self.relationship_properties.extend(
                    types
                        .iter()
                        .cloned()
                        .map(|relationship_type| (relationship_type, property.clone())),
                );
            }
            Some(BindingStatistics::Ambiguous) | None => {}
        }
    }

    fn add_property_values(
        &mut self,
        expression: &LogicalExpression,
        values: Option<Vec<ScalarValue>>,
        bindings: &BTreeMap<Symbol, BindingStatistics>,
    ) {
        let (LogicalExpression::Property { binding, property }, Some(values)) =
            (expression, values)
        else {
            return;
        };
        self.add_property(binding, property, bindings);
        // The histogram still gives a bounded equality estimate for a large
        // IN list. Do not turn planning into hundreds of remote point reads.
        if values.len() > MAX_EXACT_STATISTICS_VALUES {
            return;
        }
        match bindings.get(binding) {
            Some(BindingStatistics::Vertex) => {
                self.vertex_values.extend(
                    values
                        .into_iter()
                        .filter(|value| !matches!(value, ScalarValue::Null))
                        .map(|value| (property.clone(), value)),
                );
            }
            Some(BindingStatistics::Relationship(types)) => {
                for relationship_type in types {
                    self.relationship_values.extend(
                        values
                            .iter()
                            .filter(|value| !matches!(value, ScalarValue::Null))
                            .cloned()
                            .map(|value| (relationship_type.clone(), property.clone(), value)),
                    );
                }
            }
            Some(BindingStatistics::Ambiguous) | None => {}
        }
    }

    fn requests(self, cell_id: &str) -> Vec<(StatisticFact, String)> {
        let mut requests = BTreeSet::new();
        for labels in self.labels {
            if labels
                .iter()
                .any(|label| validate_component("label", label.as_str()).is_err())
            {
                continue;
            }
            let key = if labels.len() == 1 {
                keys::query_stats_vertex_label(cell_id, labels[0].as_str())
            } else {
                keys::query_stats_vertex_label_intersection(
                    cell_id,
                    &labels.iter().map(Symbol::as_str).collect::<Vec<_>>(),
                )
            };
            requests.insert((StatisticFact::VertexLabels(labels), key));
        }
        for relationship_type in self.relationship_types {
            if validate_component("edge_type", relationship_type.as_str()).is_ok() {
                requests.insert((
                    StatisticFact::RelationshipType(relationship_type.clone()),
                    keys::query_stats_edge_type(cell_id, relationship_type.as_str()),
                ));
            }
        }
        for (source_labels, relationship_type, direction) in self.expansions {
            if validate_component("edge_type", relationship_type.as_str()).is_err()
                || source_labels
                    .iter()
                    .any(|label| validate_component("label", label.as_str()).is_err())
            {
                continue;
            }
            let labels = source_labels.iter().map(Symbol::as_str).collect::<Vec<_>>();
            let key = keys::query_stats_edge_expansion(
                cell_id,
                relationship_type.as_str(),
                pattern_direction_key(direction),
                &labels,
            );
            requests.insert((
                StatisticFact::Expansion(source_labels, relationship_type.clone(), direction),
                key,
            ));
        }
        for property in self.vertex_properties {
            if validate_component("property", property.as_str()).is_ok() {
                requests.insert((
                    StatisticFact::VertexProperty(property.clone()),
                    keys::query_stats_vertex_property_histogram(cell_id, property.as_str()),
                ));
            }
        }
        for (property, value) in self.vertex_values {
            if validate_component("property", property.as_str()).is_err() {
                continue;
            }
            for encoded in scalar_property_keys(&value) {
                requests.insert((
                    StatisticFact::VertexValue(property.clone(), value.clone()),
                    keys::query_stats_vertex_property(cell_id, property.as_str(), &encoded),
                ));
            }
        }
        for (relationship_type, property) in self.relationship_properties {
            if validate_component("edge_type", relationship_type.as_str()).is_ok()
                && validate_component("property", property.as_str()).is_ok()
            {
                requests.insert((
                    StatisticFact::RelationshipProperty(
                        relationship_type.clone(),
                        property.clone(),
                    ),
                    keys::query_stats_edge_property_histogram(
                        cell_id,
                        relationship_type.as_str(),
                        property.as_str(),
                    ),
                ));
            }
        }
        for (relationship_type, property, value) in self.relationship_values {
            if validate_component("edge_type", relationship_type.as_str()).is_err()
                || validate_component("property", property.as_str()).is_err()
            {
                continue;
            }
            for encoded in scalar_property_keys(&value) {
                requests.insert((
                    StatisticFact::RelationshipValue(
                        relationship_type.clone(),
                        property.clone(),
                        value.clone(),
                    ),
                    keys::query_stats_edge_property(
                        cell_id,
                        relationship_type.as_str(),
                        property.as_str(),
                        &encoded,
                    ),
                ));
            }
        }
        requests.into_iter().collect()
    }
}

fn vertex_labels_for_binding(plan: &GraphLogicalPlan, wanted: &Symbol) -> Option<Vec<Symbol>> {
    let mut matches = BTreeSet::new();
    visit_plan(plan, &mut |plan| match plan {
        GraphLogicalPlan::NodeScan { binding, labels } if binding == wanted => {
            matches.insert(canonical_symbols(labels));
        }
        GraphLogicalPlan::Expand {
            to, target_labels, ..
        }
        | GraphLogicalPlan::VariableExpand {
            to, target_labels, ..
        } if to == wanted => {
            matches.insert(canonical_symbols(target_labels));
        }
        _ => {}
    });
    (matches.len() == 1)
        .then(|| matches.into_iter().next())
        .flatten()
}

fn pattern_direction_key(direction: PatternDirection) -> &'static str {
    match direction {
        PatternDirection::Outgoing => "out",
        PatternDirection::Incoming => "in",
        PatternDirection::Undirected => "both",
    }
}

fn binding_statistics(plan: &GraphLogicalPlan) -> BTreeMap<Symbol, BindingStatistics> {
    let mut bindings = BTreeMap::new();
    visit_plan(plan, &mut |plan| match plan {
        GraphLogicalPlan::NodeScan { binding, .. } => {
            record_binding(&mut bindings, binding, BindingStatistics::Vertex);
        }
        GraphLogicalPlan::Expand {
            relationship,
            relationship_types,
            to,
            ..
        } => {
            record_binding(&mut bindings, to, BindingStatistics::Vertex);
            if let Some(relationship) = relationship {
                record_binding(
                    &mut bindings,
                    relationship,
                    BindingStatistics::Relationship(canonical_symbols(relationship_types)),
                );
            }
        }
        GraphLogicalPlan::VariableExpand { to, .. } => {
            record_binding(&mut bindings, to, BindingStatistics::Vertex);
        }
        _ => {}
    });
    bindings
}

fn record_binding(
    bindings: &mut BTreeMap<Symbol, BindingStatistics>,
    binding: &Symbol,
    kind: BindingStatistics,
) {
    use std::collections::btree_map::Entry;
    match bindings.entry(binding.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(kind);
        }
        Entry::Occupied(mut entry) => match (entry.get_mut(), kind) {
            (BindingStatistics::Vertex, BindingStatistics::Vertex) => {}
            (
                BindingStatistics::Relationship(existing),
                BindingStatistics::Relationship(additional),
            ) => {
                existing.extend(additional);
                existing.sort();
                existing.dedup();
            }
            (existing, _) => *existing = BindingStatistics::Ambiguous,
        },
    }
}

fn canonical_symbols(symbols: &[Symbol]) -> Vec<Symbol> {
    symbols
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn constant_values(
    expression: &LogicalExpression,
    context: &PhysicalPlanningContext,
) -> Option<Vec<ScalarValue>> {
    match expression {
        LogicalExpression::Literal(value) => Some(vec![value.clone()]),
        LogicalExpression::Parameter(parameter) => context
            .parameters
            .get(parameter)
            .cloned()
            .map(|value| vec![value]),
        _ => None,
    }
}

fn in_values(list: &LogicalInList, context: &PhysicalPlanningContext) -> Option<Vec<ScalarValue>> {
    match list {
        LogicalInList::Values(values) => values
            .iter()
            .map(|value| constant_values(value, context).and_then(|mut value| value.pop()))
            .collect(),
        LogicalInList::Parameter(parameter) => {
            context.list_parameters.get(parameter).cloned().or_else(|| {
                context
                    .parameters
                    .get(parameter)
                    .cloned()
                    .map(|value| vec![value])
            })
        }
    }
}

pub(super) fn scalar_property_keys(value: &ScalarValue) -> Vec<String> {
    let value = match value {
        ScalarValue::Null => return Vec::new(),
        ScalarValue::Boolean(value) => VertexPropertyValue::Bool(*value),
        ScalarValue::Integer(value) => match u64::try_from(*value) {
            Ok(value) => VertexPropertyValue::Integer(value),
            Err(_) => match i64::try_from(*value) {
                Ok(value) => VertexPropertyValue::SignedInteger(value),
                Err(_) => return Vec::new(),
            },
        },
        ScalarValue::Float(value) => match value.parse::<f64>() {
            Ok(value) => VertexPropertyValue::Float(QueryFloat(value)),
            Err(_) => return Vec::new(),
        },
        ScalarValue::String(value) => VertexPropertyValue::String(value.to_string()),
    };
    crate::shard::query::equivalent_property_index_keys(&value)
}

fn visit_plan(plan: &GraphLogicalPlan, visitor: &mut impl FnMut(&GraphLogicalPlan)) {
    visitor(plan);
    match plan {
        GraphLogicalPlan::Union { arms, .. } => {
            for arm in arms {
                visit_plan(arm, visitor);
            }
        }
        GraphLogicalPlan::NaturalJoin { left, right, .. } => {
            visit_plan(left, visitor);
            visit_plan(right, visitor);
        }
        GraphLogicalPlan::Unwind { input, .. }
        | GraphLogicalPlan::Expand { input, .. }
        | GraphLogicalPlan::VariableExpand { input, .. }
        | GraphLogicalPlan::Filter { input, .. }
        | GraphLogicalPlan::Sort { input, .. }
        | GraphLogicalPlan::Skip { input, .. }
        | GraphLogicalPlan::Limit { input, .. }
        | GraphLogicalPlan::Project { input, .. } => visit_plan(input, visitor),
        GraphLogicalPlan::SingleRow | GraphLogicalPlan::NodeScan { .. } => {}
    }
}
