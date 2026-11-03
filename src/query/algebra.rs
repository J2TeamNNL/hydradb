use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::QueryFailureReason;
use crate::{
    validate_component, CommitResult, EdgeMetadata, GraphError, GraphScope, QueryFloat,
    RelationshipId, Result, StorageSequence, VertexId, VertexMetadata, VertexPropertyValue,
};

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPathNode {
    pub id: VertexId,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPathRelationship {
    pub id: Option<RelationshipId>,
    pub edge_type: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: BTreeMap<String, VertexPropertyValue>,
}

impl QueryPathRelationship {
    pub fn element_id(&self) -> String {
        self.id.map_or_else(
            || format!("structural:{}:{}:{}", self.edge_type, self.src, self.dst),
            |id| id.to_string(),
        )
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryPath {
    pub nodes: Vec<QueryPathNode>,
    pub relationships: Vec<QueryPathRelationship>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryParameterValue {
    Scalar(VertexPropertyValue),
    List(Vec<QueryParameterValue>),
    Map(BTreeMap<String, QueryParameterValue>),
}

impl From<VertexPropertyValue> for QueryParameterValue {
    fn from(value: VertexPropertyValue) -> Self {
        Self::Scalar(value)
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryBatchEdge {
    pub src: VertexId,
    pub dst: VertexId,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchVertex {
    pub vertex: VertexId,
    pub metadata: VertexMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchIsolatedVertex {
    pub vertex: VertexId,
    pub path_node_constraints: VertexMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationship {
    pub src: VertexId,
    pub dst: VertexId,
    pub metadata: EdgeMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchRelationshipMerge {
    pub src: VertexId,
    pub dst: VertexId,
    pub relationship_id: RelationshipId,
    pub metadata: EdgeMetadata,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBatchMergePolicy {
    pub update_if_newer_by: String,
    pub create_only_properties: std::collections::BTreeSet<String>,
}

impl QueryBatchMergePolicy {
    pub fn validate(&self) -> Result<()> {
        validate_component("property", &self.update_if_newer_by)?;
        for property in &self.create_only_properties {
            validate_component("property", property)?;
        }
        if self
            .create_only_properties
            .contains(&self.update_if_newer_by)
        {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Unwind,
                dialect: "QueryBatch",
                feature: format!(
                    "update guard {} cannot also be create-only",
                    self.update_if_newer_by
                ),
            });
        }
        Ok(())
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryBatchOperation {
    OutNeighbors {
        edge_type: String,
        sources: Vec<VertexId>,
        source_column: QueryColumn,
        destination_column: QueryColumn,
    },
    CreateEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteEdges {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
    },
    DeleteVertices {
        vertices: Vec<VertexId>,
        detach: bool,
    },
    DeleteIsolatedVertices {
        candidates: Vec<QueryBatchIsolatedVertex>,
        deleted_column: QueryColumn,
    },
    DeleteVerticesAndIsolatedCandidates {
        detach_vertices: Vec<VertexId>,
        isolated_candidates: Vec<QueryBatchIsolatedVertex>,
        deleted_column: QueryColumn,
    },
    DeleteRelationshipsByProperty {
        edge_type: String,
        property: String,
        values: Vec<VertexPropertyValue>,
    },
    CreateEdgesBetweenLabeledVertices {
        edge_type: String,
        edges: Vec<QueryBatchEdge>,
        source_label: String,
        destination_label: String,
    },
    UpsertVertices {
        vertices: Vec<QueryBatchVertex>,
    },
    GuardedUpsertVertices {
        vertices: Vec<QueryBatchVertex>,
        merge_policy: QueryBatchMergePolicy,
    },
    CreateRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationship>,
        source_label: String,
        destination_label: String,
    },
    MergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationshipMerge>,
        source_label: String,
        destination_label: String,
    },
    GuardedMergeRelationshipsBetweenLabeledVertices {
        edge_type: String,
        relationships: Vec<QueryBatchRelationshipMerge>,
        source_label: String,
        destination_label: String,
        merge_policy: QueryBatchMergePolicy,
    },
}

impl QueryBatchOperation {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Self::CreateEdges { .. }
                | Self::CreateEdgesBetweenLabeledVertices { .. }
                | Self::DeleteEdges { .. }
                | Self::DeleteVertices { .. }
                | Self::DeleteIsolatedVertices { .. }
                | Self::DeleteVerticesAndIsolatedCandidates { .. }
                | Self::DeleteRelationshipsByProperty { .. }
                | Self::UpsertVertices { .. }
                | Self::GuardedUpsertVertices { .. }
                | Self::CreateRelationshipsBetweenLabeledVertices { .. }
                | Self::MergeRelationshipsBetweenLabeledVertices { .. }
                | Self::GuardedMergeRelationshipsBetweenLabeledVertices { .. }
        )
    }

    pub fn len(&self) -> usize {
        match self {
            Self::OutNeighbors { sources, .. } => sources.len(),
            Self::CreateEdges { edges, .. }
            | Self::CreateEdgesBetweenLabeledVertices { edges, .. }
            | Self::DeleteEdges { edges, .. } => edges.len(),
            Self::DeleteVertices { vertices, .. } => vertices.len(),
            Self::DeleteIsolatedVertices { candidates, .. } => candidates.len(),
            Self::DeleteVerticesAndIsolatedCandidates {
                detach_vertices,
                isolated_candidates,
                ..
            } => detach_vertices
                .len()
                .saturating_add(isolated_candidates.len()),
            Self::DeleteRelationshipsByProperty { values, .. } => values.len(),
            Self::UpsertVertices { vertices } | Self::GuardedUpsertVertices { vertices, .. } => {
                vertices.len()
            }
            Self::CreateRelationshipsBetweenLabeledVertices { relationships, .. } => {
                relationships.len()
            }
            Self::MergeRelationshipsBetweenLabeledVertices { relationships, .. }
            | Self::GuardedMergeRelationshipsBetweenLabeledVertices { relationships, .. } => {
                relationships.len()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[cfg_attr(feature = "query-transport", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CypherEngineMode {
    #[default]
    Legacy,
    Experimental,
}

impl CypherEngineMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Experimental => "experimental",
        }
    }

    #[cfg(feature = "query-transport")]
    fn is_legacy(&self) -> bool {
        *self == Self::Legacy
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContext {
    pub scope: GraphScope,
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<StorageSequence>,
    pub result_window: QueryWindow,
    pub parameters: BTreeMap<String, VertexPropertyValue>,
    /// List-valued parameters, kept beside `parameters` rather than in it:
    /// `VertexPropertyValue` is the stored-property type and must not grow a
    /// list variant. Only `IN` reads these. `serde(default)` is load-bearing
    /// during a rolling deploy, where a context serialized by an older node
    /// still has to decode here.
    #[cfg_attr(feature = "query-transport", serde(default))]
    pub list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
    pub max_runtime_ms: Option<u64>,
    pub max_result_bytes: Option<u64>,
    #[cfg_attr(
        feature = "query-transport",
        serde(default, skip_serializing_if = "CypherEngineMode::is_legacy")
    )]
    pub cypher_engine: CypherEngineMode,
    #[cfg_attr(feature = "query-transport", serde(default))]
    refreshed_reader: bool,
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    pub cancellation_token: Option<QueryCancellationToken>,
    #[cfg(feature = "opencypher")]
    #[cfg_attr(feature = "query-transport", serde(skip, default))]
    validated_read: Option<ValidatedQueryRead>,
}

impl QueryContext {
    pub fn new(cell_id: impl Into<String>, idempotency_key: impl Into<String>) -> Self {
        Self {
            scope: GraphScope::default(),
            cell_id: cell_id.into(),
            idempotency_key: idempotency_key.into(),
            read_epoch: None,
            result_window: QueryWindow::default(),
            parameters: BTreeMap::new(),
            list_parameters: BTreeMap::new(),
            max_runtime_ms: None,
            max_result_bytes: None,
            cypher_engine: CypherEngineMode::Legacy,
            refreshed_reader: false,
            cancellation_token: None,
            #[cfg(feature = "opencypher")]
            validated_read: None,
        }
    }

    pub fn in_scope(mut self, scope: GraphScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn at_epoch(mut self, read_epoch: StorageSequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_result_window(mut self, skip: u64, limit: Option<usize>) -> Self {
        self.result_window = QueryWindow { skip, limit };
        self
    }

    pub fn with_parameter(mut self, name: impl Into<String>, value: VertexPropertyValue) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    pub fn with_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (String, VertexPropertyValue)>,
    ) -> Self {
        self.parameters.extend(parameters);
        self
    }

    pub fn with_list_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (String, Vec<VertexPropertyValue>)>,
    ) -> Self {
        self.list_parameters.extend(parameters);
        self
    }

    pub fn with_timeout_ms(mut self, max_runtime_ms: u64) -> Self {
        self.max_runtime_ms = Some(max_runtime_ms);
        self
    }

    pub fn with_max_result_bytes(mut self, max_result_bytes: u64) -> Self {
        self.max_result_bytes = Some(max_result_bytes);
        self
    }

    pub fn with_cypher_engine(mut self, cypher_engine: CypherEngineMode) -> Self {
        self.cypher_engine = cypher_engine;
        self
    }

    #[cfg(any(feature = "client-api", test))]
    #[cfg(feature = "opencypher")]
    pub(crate) fn with_refreshed_reader(mut self) -> Self {
        self.refreshed_reader = true;
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn uses_refreshed_reader(&self) -> bool {
        self.refreshed_reader
    }

    pub fn with_cancellation_token(mut self, token: QueryCancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn with_validated_storage_read_epoch(
        mut self,
        read_epoch: StorageSequence,
        storage_sequence: crate::StorageSequence,
    ) -> Self {
        self.read_epoch = Some(read_epoch);
        self.validated_read = Some(ValidatedQueryRead {
            cell_id: self.cell_id.clone(),
            read_epoch,
            retention: ValidatedQueryReadRetention::StorageSnapshot(storage_sequence),
        });
        self
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn validated_read_epoch(&self) -> Option<StorageSequence> {
        self.validated_read.as_ref().and_then(|validated| {
            let _retention = &validated.retention;
            (validated.cell_id == self.cell_id && self.read_epoch == Some(validated.read_epoch))
                .then_some(validated.read_epoch)
        })
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn validated_storage_sequence(&self) -> Option<crate::StorageSequence> {
        self.validated_read.as_ref().and_then(|validated| {
            if validated.cell_id != self.cell_id || self.read_epoch != Some(validated.read_epoch) {
                return None;
            }
            match &validated.retention {
                ValidatedQueryReadRetention::StorageSnapshot(sequence) => Some(*sequence),
            }
        })
    }
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedQueryRead {
    cell_id: String,
    read_epoch: StorageSequence,
    retention: ValidatedQueryReadRetention,
}

#[cfg(feature = "opencypher")]
#[derive(Clone, Debug, Eq, PartialEq)]
enum ValidatedQueryReadRetention {
    StorageSnapshot(crate::StorageSequence),
}

#[derive(Clone, Debug, Default)]
pub struct QueryCancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

tokio::task_local! {
    static ACTIVE_QUERY_CANCELLATION: QueryCancellationToken;
}

impl QueryCancellationToken {
    #[cfg(any(feature = "query-transport", test))]
    pub(crate) async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        ACTIVE_QUERY_CANCELLATION.scope(self.clone(), future).await
    }

    pub(crate) fn current() -> Option<Self> {
        ACTIVE_QUERY_CANCELLATION.try_with(Clone::clone).ok()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl PartialEq for QueryCancellationToken {
    fn eq(&self, other: &Self) -> bool {
        self.is_cancelled() == other.is_cancelled()
    }
}

impl Eq for QueryCancellationToken {}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryWindow {
    pub skip: u64,
    pub limit: Option<usize>,
}

impl QueryWindow {
    pub fn is_default(self) -> bool {
        self.skip == 0 && self.limit.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCardinalityStatsKind {
    EdgeType {
        edge_type: String,
    },
    EdgeExpansion {
        edge_type: String,
        direction: QueryStatsDirection,
        source_labels: Vec<String>,
    },
    VertexLabel {
        label: String,
    },
    VertexLabelIntersection {
        labels: Vec<String>,
    },
    VertexProperty {
        property: String,
        value: VertexPropertyValue,
    },
    EdgeProperty {
        edge_type: String,
        property: String,
        value: VertexPropertyValue,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryStatsDirection {
    Outgoing,
    Incoming,
    Undirected,
}

impl QueryStatsDirection {
    #[cfg(feature = "opencypher")]
    pub(crate) fn as_key_component(self) -> &'static str {
        match self {
            Self::Outgoing => "out",
            Self::Incoming => "in",
            Self::Undirected => "both",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCardinalityStatsRefresh {
    pub cell_id: String,
    pub read_epoch: StorageSequence,
    pub kind: QueryCardinalityStatsKind,
    pub count: u64,
    pub stats: QueryStatsRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsRecord {
    pub count: u64,
    pub read_epoch: StorageSequence,
    pub refreshed_at_ms: u64,
    pub distinct_values: u64,
    pub total_values: u64,
    pub most_common_count: u64,
    pub bloom: Option<QueryStatsBloom>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsBloom {
    pub bit_count: u64,
    pub hash_count: u32,
    pub inserted_count: u64,
    pub false_positive_rate_ppm: u32,
    pub bits: Vec<u8>,
}

impl QueryStatsBloom {
    pub fn from_encoded_values<'a>(values: impl Iterator<Item = &'a str>) -> Self {
        const BITS_PER_VALUE: u64 = 10;
        const HASH_COUNT: u32 = 7;
        let values = values.collect::<Vec<_>>();
        let inserted_count = values.len() as u64;
        let requested_bits = inserted_count.saturating_mul(BITS_PER_VALUE).max(64);
        let bit_count = requested_bits.saturating_add(7) / 8 * 8;
        let mut bloom = Self {
            bit_count,
            hash_count: HASH_COUNT,
            inserted_count,
            false_positive_rate_ppm: bloom_false_positive_rate_ppm(
                bit_count,
                HASH_COUNT,
                inserted_count,
            ),
            bits: vec![
                0;
                usize::try_from(bit_count / 8)
                    .expect("bloom size derives from an in-memory value count")
            ],
        };
        for value in values {
            bloom.insert(value.as_bytes());
        }
        bloom
    }

    pub fn may_contain_encoded(&self, value: &str) -> bool {
        if self.bit_count == 0
            || self.bits.len() as u64 != self.bit_count / 8
            || !(1..=32).contains(&self.hash_count)
        {
            return true;
        }
        bloom_indexes(value.as_bytes(), self.bit_count, self.hash_count).all(|index| {
            let byte = usize::try_from(index / 8).expect("bloom index fits its backing vector");
            self.bits[byte] & (1 << (index % 8)) != 0
        })
    }

    fn insert(&mut self, value: &[u8]) {
        for index in bloom_indexes(value, self.bit_count, self.hash_count) {
            let byte = usize::try_from(index / 8).expect("bloom index fits its backing vector");
            self.bits[byte] |= 1 << (index % 8);
        }
    }
}

fn bloom_indexes(value: &[u8], bit_count: u64, hash_count: u32) -> impl Iterator<Item = u64> {
    let first = stable_bloom_hash(value, 0x517c_c1b7_2722_0a95);
    let second = stable_bloom_hash(value, 0x6c8e_9cf5_7093_2bd5) | 1;
    (0..hash_count)
        .map(move |index| first.wrapping_add(u64::from(index).wrapping_mul(second)) % bit_count)
}

fn stable_bloom_hash(value: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn bloom_false_positive_rate_ppm(bit_count: u64, hash_count: u32, values: u64) -> u32 {
    if values == 0 || bit_count == 0 {
        return 0;
    }
    let exponent = -(f64::from(hash_count) * values as f64 / bit_count as f64);
    let rate = (1.0 - exponent.exp()).powf(f64::from(hash_count));
    (rate * 1_000_000.0).round().clamp(0.0, 1_000_000.0) as u32
}

impl QueryStatsRecord {
    pub fn point_count(count: u64, read_epoch: StorageSequence, refreshed_at_ms: u64) -> Self {
        Self {
            count,
            read_epoch,
            refreshed_at_ms,
            distinct_values: 1,
            total_values: count,
            most_common_count: count,
            bloom: None,
        }
    }

    pub fn histogram(
        count: u64,
        read_epoch: StorageSequence,
        refreshed_at_ms: u64,
        distinct_values: u64,
        most_common_count: u64,
    ) -> Self {
        Self {
            count,
            read_epoch,
            refreshed_at_ms,
            distinct_values,
            total_values: count,
            most_common_count,
            bloom: None,
        }
    }

    /// Older optimizer stats remain useful estimates. Publishing stats itself
    /// advances SlateDB's sequence, so only future records are unusable.
    pub fn is_unusable_at(&self, current_epoch: StorageSequence, now_ms: u64) -> bool {
        self.read_epoch > current_epoch || self.refreshed_at_ms > now_ms
    }

    pub fn equality_estimate(&self) -> u64 {
        if self.distinct_values == 0 {
            return self.count.max(1);
        }
        self.count
            .checked_div(self.distinct_values)
            .unwrap_or(self.count)
            .max(1)
            .min(self.most_common_count.max(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsHistogramRefresh {
    pub cell_id: String,
    pub read_epoch: StorageSequence,
    pub property: String,
    pub edge_type: Option<String>,
    pub stats: QueryStatsRecord,
    pub buckets: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatsRefreshSpec {
    pub cell_id: String,
    pub kind: QueryStatsRefreshKind,
}

impl QueryStatsRefreshSpec {
    pub fn new(cell_id: impl Into<String>, kind: impl Into<QueryStatsRefreshKind>) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: kind.into(),
        }
    }

    pub fn vertex_property_histogram(
        cell_id: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: QueryStatsRefreshKind::VertexPropertyHistogram {
                property: property.into(),
            },
        }
    }

    pub fn edge_expansion<I, S>(
        cell_id: impl Into<String>,
        edge_type: impl Into<String>,
        direction: QueryStatsDirection,
        source_labels: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            cell_id: cell_id.into(),
            kind: QueryStatsRefreshKind::Cardinality(QueryCardinalityStatsKind::EdgeExpansion {
                edge_type: edge_type.into(),
                direction,
                source_labels: source_labels.into_iter().map(Into::into).collect(),
            }),
        }
    }

    pub fn edge_property_histogram(
        cell_id: impl Into<String>,
        edge_type: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            cell_id: cell_id.into(),
            kind: QueryStatsRefreshKind::EdgePropertyHistogram {
                edge_type: edge_type.into(),
                property: property.into(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatsRefreshKind {
    Cardinality(QueryCardinalityStatsKind),
    VertexPropertyHistogram { property: String },
    EdgePropertyHistogram { edge_type: String, property: String },
}

impl From<QueryCardinalityStatsKind> for QueryStatsRefreshKind {
    fn from(value: QueryCardinalityStatsKind) -> Self {
        Self::Cardinality(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatsRefreshResult {
    Cardinality(QueryCardinalityStatsRefresh),
    Histogram(QueryStatsHistogramRefresh),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutput {
    Write(CommitResult),
    Mutation(QueryMutationResult),
    Rows(QueryResultSet),
    Vertices(Vec<VertexId>),
    Count(u64),
    Bool(bool),
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

impl QueryColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryValue {
    Null,
    VertexId(VertexId),
    Count(u64),
    Bool(bool),
    Float(QueryFloat),
    Property(VertexPropertyValue),
    List(Vec<QueryValue>),
    Node(Box<QueryPathNode>),
    Relationship(Box<QueryPathRelationship>),
    Path(Box<QueryPath>),
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

impl QueryRow {
    pub fn new(values: Vec<QueryValue>) -> Self {
        Self { values }
    }

    #[cfg(feature = "opencypher")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        self.values
            .iter()
            .fold(std::mem::size_of::<Self>() as u64, |total, value| {
                total.saturating_add(value.estimated_resident_bytes())
            })
    }
}

impl QueryValue {
    #[cfg(feature = "opencypher")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        let inline = std::mem::size_of::<Self>() as u64;
        match self {
            Self::Property(VertexPropertyValue::String(value)) => {
                inline.saturating_add(value.len() as u64)
            }
            Self::List(values) => values.iter().fold(inline, |total, value| {
                total.saturating_add(value.estimated_resident_bytes())
            }),
            Self::Node(node) => inline.saturating_add(node.estimated_resident_bytes()),
            Self::Relationship(relationship) => {
                inline.saturating_add(relationship.estimated_resident_bytes())
            }
            Self::Path(path) => path.estimated_resident_bytes(),
            Self::Null
            | Self::VertexId(_)
            | Self::Count(_)
            | Self::Bool(_)
            | Self::Float(_)
            | Self::Property(_) => inline,
        }
    }
}

#[cfg(feature = "opencypher")]
impl QueryPath {
    fn estimated_resident_bytes(&self) -> u64 {
        let node_bytes = self.nodes.iter().fold(0_u64, |total, node| {
            total.saturating_add(node.estimated_resident_bytes())
        });
        self.relationships
            .iter()
            .fold(node_bytes, |total, relationship| {
                total.saturating_add(relationship.estimated_resident_bytes())
            })
    }
}

#[cfg(feature = "opencypher")]
impl QueryPathNode {
    fn estimated_resident_bytes(&self) -> u64 {
        self.labels
            .iter()
            .fold(std::mem::size_of::<Self>() as u64, |total, label| {
                total.saturating_add(label.len() as u64)
            })
            .saturating_add(property_map_resident_bytes(&self.properties))
    }
}

#[cfg(feature = "opencypher")]
impl QueryPathRelationship {
    fn estimated_resident_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64)
            .saturating_add(self.edge_type.len() as u64)
            .saturating_add(property_map_resident_bytes(&self.properties))
    }
}

#[cfg(feature = "opencypher")]
fn property_map_resident_bytes(properties: &BTreeMap<String, VertexPropertyValue>) -> u64 {
    properties.iter().fold(0_u64, |total, (key, value)| {
        total
            .saturating_add(key.len() as u64)
            .saturating_add(match value {
                VertexPropertyValue::String(value) => value.len() as u64,
                _ => std::mem::size_of::<VertexPropertyValue>() as u64,
            })
    })
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug)]
pub struct QueryResultSet {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub read_epoch: Option<StorageSequence>,
    pub storage_sequence: Option<crate::StorageSequence>,
}

impl PartialEq for QueryResultSet {
    fn eq(&self, other: &Self) -> bool {
        // A snapshot watermark describes execution, not the logical row set.
        self.columns == other.columns && self.rows == other.rows
    }
}

impl Eq for QueryResultSet {}

impl QueryResultSet {
    pub fn new(columns: Vec<QueryColumn>, rows: Vec<QueryRow>) -> Self {
        Self {
            columns,
            rows,
            read_epoch: None,
            storage_sequence: None,
        }
    }

    pub fn with_read_epoch(mut self, read_epoch: StorageSequence) -> Self {
        self.read_epoch = Some(read_epoch);
        self
    }

    pub fn with_storage_sequence(mut self, storage_sequence: crate::StorageSequence) -> Self {
        self.storage_sequence = Some(storage_sequence);
        self
    }

    // `client-api`, not the two wire protocols above it. The only caller is
    // `ClientQueryService::start_server_cursor`, which charges a materialised
    // result against `max_cursor_buffer_bytes` before it hands out a cursor —
    // server-side cursors are a `client-api` concept, and Bolt and HTTP inherit
    // them rather than owning them. Gating on `bolt-server`/`http-api` compiled
    // only because nothing built `client-api` on its own; `--features client-api`
    // alone failed E0599 here.
    #[cfg(feature = "client-api")]
    pub(crate) fn estimated_resident_bytes(&self) -> u64 {
        self.rows.iter().fold(0_u64, |total, row| {
            total.saturating_add(row.estimated_resident_bytes())
        })
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryCursorToken {
    pub offset: u64,
}

impl QueryCursorToken {
    pub fn new(offset: u64) -> Self {
        Self { offset }
    }
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResultPage {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
    pub next_cursor: Option<QueryCursorToken>,
}

impl QueryResultPage {
    pub fn new(
        columns: Vec<QueryColumn>,
        rows: Vec<QueryRow>,
        next_cursor: Option<QueryCursorToken>,
    ) -> Self {
        Self {
            columns,
            rows,
            next_cursor,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryMutationResult {
    pub matched_rows: u64,
    pub created_edges: u64,
    pub created_relationships: u64,
    pub deleted_edges: u64,
    pub updated_vertices: u64,
    pub updated_relationships: u64,
    pub noops: u64,
    pub topology_sequence: Option<crate::StorageSequence>,
    /// Rows produced by a trailing `RETURN count(<binding>)`, when the
    /// mutation had one. `None` is the historical shape: a mutation that
    /// reports only counters and yields an empty result set.
    pub returned_rows: Option<QueryResultSet>,
}

impl QueryMutationResult {
    /// The rows a row-shaped caller sees for this mutation.
    ///
    /// A mutation with a trailing `RETURN count(...)` yields that one row; one
    /// without keeps the historical empty result. Either way the topology
    /// sequence rides along, so a caller can still wait on its own write.
    pub fn into_result_set(self) -> QueryResultSet {
        let rows = self
            .returned_rows
            .unwrap_or_else(|| QueryResultSet::new(Vec::new(), Vec::new()));
        match self.topology_sequence {
            Some(sequence) => rows.with_storage_sequence(sequence),
            None => rows,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryStatement {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryStatement {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            QueryStatement::CreateEdge { .. }
                | QueryStatement::CreateEdgeWithMetadata { .. }
                | QueryStatement::CreateEdgeWithFullMetadata { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub cell_id: String,
    pub idempotency_key: String,
    pub read_epoch: Option<StorageSequence>,
    pub result_window: QueryWindow,
    pub max_runtime_ms: Option<u64>,
    pub logical: LogicalQueryPlan,
    pub physical: PhysicalQueryPlan,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlan {
    pub cell_id: String,
    pub read_epoch: StorageSequence,
    pub columns: Vec<QueryColumn>,
    pub groups: Vec<RowQueryPlanGroup>,
    pub union_all: bool,
    pub union_arms: Vec<RowQueryPlan>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanGroup {
    pub optional: bool,
    pub has_predicate: bool,
    pub estimated_cardinality: u64,
    pub patterns: Vec<RowQueryPlanPattern>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowQueryPlanPattern {
    pub original_index: usize,
    pub access: RowQueryAccess,
    pub estimated_cardinality: u64,
    pub bindings: Vec<String>,
    pub optimizer_passes: Vec<RowQueryOptimizerPass>,
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryAccess {
    VertexIdSeek,
    VertexPropertyIndex {
        property: String,
    },
    VertexLabelScan {
        label: String,
    },
    AllVertexScan,
    BoundOutExpand {
        edge_type: String,
    },
    BoundInExpand {
        edge_type: String,
    },
    ExpandInto {
        edge_type: String,
    },
    EdgePropertyIndex {
        edge_type: String,
        property: String,
    },
    FullEdgeScan {
        edge_type: String,
    },
    VariableLengthExpand {
        edge_type: String,
        min_hops: u8,
        max_hops: u8,
    },
}

#[cfg_attr(
    feature = "query-transport",
    derive(serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowQueryOptimizerPass {
    UtilizeVertexIndex,
    OrderedLimitPushdown,
    /// A WHERE equality was folded into a node pattern so the pattern could be
    /// anchored on the vertex property index instead of scanned by label.
    EqualityPredicatePushdown,
    UtilizeEdgeIndex,
    CostBasedLabelScan,
    ConnectivityOrder,
    JoinOrder,
    ExpandInto,
    GraphKernel,
    ReverseExpand,
    FullScanFallback,
    PreserveOptionalBoundary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalQueryPlan {
    CreateEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    CreateEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    CreateEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    MatchOut {
        edge_type: String,
        src: VertexId,
        return_count: bool,
    },
    MatchOutFiltered {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        return_count: bool,
    },
    MatchReachable {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalQueryPlan {
    WriteEdge {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    WriteEdgeWithMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
    },
    WriteEdgeWithFullMetadata {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
        src_metadata: VertexMetadata,
        dst_metadata: VertexMetadata,
        edge_metadata: EdgeMetadata,
    },
    OutDegreeCounter {
        edge_type: String,
        src: VertexId,
    },
    OutNeighbors {
        edge_type: String,
        src: VertexId,
    },
    EdgeExistsToCount {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToVertices {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    EdgeExistsToBool {
        edge_type: String,
        src: VertexId,
        dst: VertexId,
    },
    ReachableVertices {
        edge_type: String,
        src: VertexId,
        min_hops: u8,
        max_hops: u8,
        return_count: bool,
    },
}

impl QueryPlan {
    pub fn is_write(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::WriteEdge { .. }
                | PhysicalQueryPlan::WriteEdgeWithMetadata { .. }
                | PhysicalQueryPlan::WriteEdgeWithFullMetadata { .. }
        )
    }

    pub fn returns_vertices(&self) -> bool {
        matches!(
            self.physical,
            PhysicalQueryPlan::OutNeighbors { .. }
                | PhysicalQueryPlan::EdgeExistsToVertices { .. }
                | PhysicalQueryPlan::ReachableVertices {
                    return_count: false,
                    ..
                }
        )
    }

    pub fn validate_for_execution(&self) -> Result<()> {
        validate_component("cell_id", &self.cell_id)?;
        validate_logical_plan(&self.logical)?;

        let expected_physical = physical_plan(&self.logical);
        if self.physical != expected_physical {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::Other,
                dialect: "GraphQuery",
                feature: "physical query plan does not match logical query plan".to_string(),
            });
        }

        if self.is_write() {
            validate_component("idempotency_key", &self.idempotency_key)?;
            if self.read_epoch.is_some() {
                return Err(GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::InvalidRequest,
                    dialect: "GraphQuery",
                    feature: "snapshot epochs are only valid for read queries".to_string(),
                });
            }
        }
        if !self.result_window.is_default() && !self.returns_vertices() {
            return Err(GraphError::UnsupportedQuery {
                reason: QueryFailureReason::InvalidRequest,
                dialect: "GraphQuery",
                feature: "result windows are only valid for vertex-returning read queries"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(context: &QueryContext, statement: &QueryStatement) -> Result<QueryPlan> {
        validate_component("cell_id", &context.cell_id)?;

        let logical = logical_plan(statement)?;
        validate_logical_plan(&logical)?;
        let physical = physical_plan(&logical);
        let plan = QueryPlan {
            cell_id: context.cell_id.clone(),
            idempotency_key: context.idempotency_key.clone(),
            read_epoch: context.read_epoch,
            result_window: context.result_window,
            max_runtime_ms: context.max_runtime_ms,
            logical,
            physical,
        };
        plan.validate_for_execution()?;
        Ok(plan)
    }
}

fn logical_plan(statement: &QueryStatement) -> Result<LogicalQueryPlan> {
    Ok(match statement {
        QueryStatement::CreateEdge {
            edge_type,
            src,
            dst,
        } => LogicalQueryPlan::CreateEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        QueryStatement::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        QueryStatement::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        QueryStatement::MatchOut {
            edge_type,
            src,
            return_count,
        } => LogicalQueryPlan::MatchOut {
            edge_type: edge_type.clone(),
            src: *src,
            return_count: *return_count,
        },
        QueryStatement::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchOutFiltered {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchEdge {
            edge_type,
            src,
            dst,
            return_count,
        } => LogicalQueryPlan::MatchEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            return_count: *return_count,
        },
        QueryStatement::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => LogicalQueryPlan::MatchReachable {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    })
}

fn validate_logical_plan(plan: &LogicalQueryPlan) -> Result<()> {
    match plan {
        LogicalQueryPlan::CreateEdge { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithMetadata { edge_type, .. }
        | LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_type, .. }
        | LogicalQueryPlan::MatchOut { edge_type, .. }
        | LogicalQueryPlan::MatchOutFiltered { edge_type, .. }
        | LogicalQueryPlan::MatchEdge { edge_type, .. }
        | LogicalQueryPlan::MatchReachable { edge_type, .. } => {
            validate_component("edge_type", edge_type)?;
        }
    }
    if let LogicalQueryPlan::CreateEdgeWithMetadata {
        src_metadata,
        dst_metadata,
        ..
    }
    | LogicalQueryPlan::CreateEdgeWithFullMetadata {
        src_metadata,
        dst_metadata,
        ..
    } = plan
    {
        validate_query_vertex_metadata(src_metadata)?;
        validate_query_vertex_metadata(dst_metadata)?;
    }
    if let LogicalQueryPlan::CreateEdgeWithFullMetadata { edge_metadata, .. } = plan {
        validate_query_edge_metadata(edge_metadata)?;
    }
    Ok(())
}

fn physical_plan(logical: &LogicalQueryPlan) -> PhysicalQueryPlan {
    match logical {
        LogicalQueryPlan::CreateEdge {
            edge_type,
            src,
            dst,
        } => PhysicalQueryPlan::WriteEdge {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::CreateEdgeWithMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
        },
        LogicalQueryPlan::CreateEdgeWithFullMetadata {
            edge_type,
            src,
            dst,
            src_metadata,
            dst_metadata,
            edge_metadata,
        } => PhysicalQueryPlan::WriteEdgeWithFullMetadata {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
            src_metadata: src_metadata.clone(),
            dst_metadata: dst_metadata.clone(),
            edge_metadata: edge_metadata.clone(),
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: true,
        } => PhysicalQueryPlan::OutDegreeCounter {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOut {
            edge_type,
            src,
            return_count: false,
        } => PhysicalQueryPlan::OutNeighbors {
            edge_type: edge_type.clone(),
            src: *src,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: true,
        }
        | LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: true,
        } => PhysicalQueryPlan::EdgeExistsToCount {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchOutFiltered {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToVertices {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchEdge {
            edge_type,
            src,
            dst,
            return_count: false,
        } => PhysicalQueryPlan::EdgeExistsToBool {
            edge_type: edge_type.clone(),
            src: *src,
            dst: *dst,
        },
        LogicalQueryPlan::MatchReachable {
            edge_type,
            src,
            min_hops,
            max_hops,
            return_count,
        } => PhysicalQueryPlan::ReachableVertices {
            edge_type: edge_type.clone(),
            src: *src,
            min_hops: *min_hops,
            max_hops: *max_hops,
            return_count: *return_count,
        },
    }
}

fn validate_query_vertex_metadata(metadata: &VertexMetadata) -> Result<()> {
    for label in &metadata.labels {
        validate_component("label", label)?;
    }
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

fn validate_query_edge_metadata(metadata: &EdgeMetadata) -> Result<()> {
    for property in metadata.properties.keys() {
        validate_component("property", property)?;
    }
    Ok(())
}

#[cfg(all(test, feature = "opencypher"))]
mod entity_memory_tests {
    use super::*;

    #[test]
    fn entities_and_nested_lists_account_for_owned_payloads() {
        let node = QueryPathNode {
            id: 1,
            labels: vec!["Person".into()],
            properties: BTreeMap::from([(
                "name".into(),
                VertexPropertyValue::String("Ada".into()),
            )]),
        };
        let node_bytes =
            (std::mem::size_of::<QueryValue>() + std::mem::size_of::<QueryPathNode>() + 6 + 4 + 3)
                as u64;
        assert_eq!(
            QueryValue::Node(Box::new(node.clone())).estimated_resident_bytes(),
            node_bytes
        );
        let relation = QueryPathRelationship {
            id: None,
            edge_type: "KNOWS".into(),
            src: 1,
            dst: 2,
            properties: node.properties.clone(),
        };
        let relation_bytes = (std::mem::size_of::<QueryValue>()
            + std::mem::size_of::<QueryPathRelationship>()
            + 5
            + 4
            + 3) as u64;
        let value = QueryValue::List(vec![
            QueryValue::Node(Box::new(node)),
            QueryValue::Relationship(Box::new(relation)),
        ]);
        assert_eq!(
            value.estimated_resident_bytes(),
            std::mem::size_of::<QueryValue>() as u64 + node_bytes + relation_bytes
        );
    }
}
