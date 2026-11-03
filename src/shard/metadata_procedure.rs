//! Metadata reads use the pinned transactional snapshot, never planner statistics.
//! Category seeks skip label members and positive degree groups. Deleted types
//! can leave zero counters, so proving absence may read every source counter of
//! that type. That work is charged and fails at the scan cap, never truncated.
use super::*;
use crate::query::path_procedure::NativeMetadataProcedure;
use crate::{QueryResultSet, QueryRow, QueryValue};

impl GraphShard {
    pub(crate) async fn execute_native_metadata_rows_inner(
        &self,
        context: QueryContext,
        procedure: NativeMetadataProcedure,
    ) -> Result<QueryResultSet> {
        validate_component("cell_id", &context.cell_id)?;
        let budget = QueryBudget::new(
            context.max_runtime_ms.or(self.limits.max_query_runtime_ms),
            context.cancellation_token.clone(),
        )
        .with_max_result_bytes(context.max_result_bytes);
        budget.check("metadata_start")?;
        let snapshot = budget
            .read_only_io("metadata_snapshot", self.db.snapshot())
            .await?;
        let read_epoch =
            context
                .validated_read_epoch()
                .ok_or_else(|| GraphError::UnsupportedQuery {
                    reason: QueryFailureReason::InvalidRequest,
                    dialect: "OpenCypher",
                    feature: "metadata requires a validated storage snapshot".to_string(),
                })?;
        if snapshot.seq() != read_epoch {
            return Err(GraphError::SnapshotAhead {
                cell_id: context.cell_id,
                read_epoch,
                current_epoch: snapshot.seq(),
            });
        }
        let prefix = match procedure {
            NativeMetadataProcedure::Labels => keys::vertex_label_cell_prefix(&context.cell_id),
            NativeMetadataProcedure::RelationshipTypes => {
                format!("cell/{}/cnt/out/", context.cell_id)
            }
        };
        let mut start = Vec::<u8>::new();
        let mut rows = Vec::new();
        let mut scanned = 0_u64;
        loop {
            budget.check("metadata_seek")?;
            let mut iter = budget
                .read_only_io("metadata_seek", async {
                    Ok::<_, GraphError>(
                        snapshot
                            .scan_prefix_with_options(
                                prefix.as_bytes(),
                                start.clone()..,
                                &remote_scan_options(),
                            )
                            .await?,
                    )
                })
                .await?;
            let mut category = None::<String>;
            loop {
                let Some(kv) = budget
                    .read_only_io("metadata_next", async {
                        Ok::<_, GraphError>(iter.next().await?)
                    })
                    .await?
                else {
                    break;
                };
                scanned = scanned.saturating_add(1);
                ensure_limit(
                    "metadata_scan_records",
                    scanned,
                    self.limits.max_query_scan_edges,
                )?;
                let key =
                    std::str::from_utf8(&kv.key).map_err(|error| GraphError::CorruptValue {
                        key: prefix.clone(),
                        reason: error.to_string(),
                    })?;
                let suffix = key
                    .strip_prefix(&prefix)
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: key.to_string(),
                        reason: "metadata key outside cell prefix".to_string(),
                    })?;
                let name = metadata_category(suffix)?;
                if let Some(previous) = &category {
                    if previous != name {
                        break;
                    }
                } else {
                    category = Some(name.to_string());
                }
                let live = match procedure {
                    NativeMetadataProcedure::Labels => true,
                    NativeMetadataProcedure::RelationshipTypes => decode_u64(key, &kv.value)? > 0,
                };
                if live {
                    ensure_limit(
                        "metadata_result_rows",
                        rows.len() as u64 + 1,
                        self.limits.max_query_result_vertices as u64,
                    )?;
                    ensure_limit(
                        "metadata_intermediate_rows",
                        rows.len() as u64 + 1,
                        self.limits.max_query_intermediate_rows as u64,
                    )?;
                    let row = QueryRow::new(vec![QueryValue::Property(
                        VertexPropertyValue::String(name.to_string()),
                    )]);
                    budget.account_result_row(&row)?;
                    rows.push(row);
                    break;
                }
            }
            let Some(category) = category else {
                break;
            };
            // '/' sorts before '0'; this exclusive group successor skips every
            // member while preserving names that extend the category itself.
            start = format!("{category}0").into_bytes();
        }
        // The key delimiter sorts after '-' and '.', so key order alone is
        // not the lexical order of names such as A and A.B.
        rows.sort_by(|left, right| match (&left.values[0], &right.values[0]) {
            (
                QueryValue::Property(VertexPropertyValue::String(left)),
                QueryValue::Property(VertexPropertyValue::String(right)),
            ) => left.cmp(right),
            _ => unreachable!("metadata rows contain one name"),
        });
        budget.check("metadata_complete")?;
        let result = QueryResultSet::new(vec![QueryColumn::new(procedure.column())], rows)
            .with_read_epoch(read_epoch);
        Ok(match context.validated_storage_sequence() {
            Some(sequence) => result.with_storage_sequence(sequence),
            None => result,
        })
    }
}

fn metadata_category(suffix: &str) -> Result<&str> {
    let (name, vertex) = suffix
        .split_once('/')
        .ok_or_else(|| GraphError::CorruptValue {
            key: suffix.to_string(),
            reason: "metadata index key has no member".to_string(),
        })?;
    validate_component("metadata_name", name)?;
    if vertex.len() != 20 || !vertex.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(GraphError::CorruptValue {
            key: suffix.to_string(),
            reason: "metadata index member is not a vertex id".to_string(),
        });
    }
    vertex
        .parse::<u64>()
        .map_err(|error| GraphError::CorruptValue {
            key: suffix.to_string(),
            reason: error.to_string(),
        })?;
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn category_successor_skips_members_without_skipping_extended_names() {
        assert_eq!(metadata_category("A/00000000000000000001").unwrap(), "A");
        assert!("A/99999999999999999999" < "A0");
        assert!("A0" < "A0/00000000000000000001");
        assert!(metadata_category("A/18446744073709551616").is_err());
        assert!(metadata_category("A/1/2").is_err());
    }

    async fn local_shard() -> (tempfile::TempDir, GraphShard) {
        local_shard_with_policy(GraphIndexPolicy::Full).await
    }

    async fn local_shard_with_policy(
        index_policy: GraphIndexPolicy,
    ) -> (tempfile::TempDir, GraphShard) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(directory.path())
                .unwrap(),
        );
        let shard = GraphShard::open_standalone_writer_with_options(
            "metadata-tests",
            store,
            GraphOpenOptions {
                index_policy,
                ..GraphOpenOptions::default()
            },
        )
        .await
        .unwrap();
        (directory, shard)
    }

    fn labels(names: &[&str]) -> VertexMetadata {
        VertexMetadata {
            labels: names.iter().map(|name| (*name).to_string()).collect(),
            properties: BTreeMap::new(),
        }
    }

    fn edge(cell: &str, edge_type: &str, src: u64, dst: u64, key: &str) -> EdgeMutation {
        EdgeMutation {
            cell_id: cell.to_string(),
            edge_type: edge_type.to_string(),
            src,
            dst,
            idempotency_key: key.to_string(),
        }
    }

    async fn names(
        shard: &GraphShard,
        cell: &str,
        procedure: NativeMetadataProcedure,
    ) -> Vec<String> {
        let query = format!(
            "CALL {}() YIELD {} RETURN {} ORDER BY {}",
            procedure.name(),
            procedure.column(),
            procedure.column(),
            procedure.column()
        );
        let result = shard
            .execute_opencypher_rows(QueryContext::new(cell, "metadata-read"), &query)
            .await
            .unwrap();
        assert_eq!(result.columns, vec![QueryColumn::new(procedure.column())]);
        assert!(result.read_epoch.is_some());
        result
            .rows
            .into_iter()
            .map(|row| match row.values.into_iter().next().unwrap() {
                QueryValue::Property(VertexPropertyValue::String(name)) => name,
                value => panic!("expected metadata name, received {value:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn metadata_local_storage_labels_are_live_sorted_and_cell_local() {
        let (_directory, shard) = local_shard().await;
        assert!(names(&shard, "one", NativeMetadataProcedure::Labels)
            .await
            .is_empty());
        assert!(
            names(&shard, "one", NativeMetadataProcedure::RelationshipTypes)
                .await
                .is_empty()
        );
        shard
            .set_vertex_metadata("one", 1, labels(&["A", "A.B", "A-", "A0"]))
            .await
            .unwrap();
        shard
            .set_vertex_metadata("one", 2, labels(&["A", "Z"]))
            .await
            .unwrap();
        shard
            .set_vertex_metadata("two", 1, labels(&["Other"]))
            .await
            .unwrap();
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::Labels).await,
            ["A", "A-", "A.B", "A0", "Z"]
        );
        assert_eq!(
            names(&shard, "two", NativeMetadataProcedure::Labels).await,
            ["Other"]
        );
        shard
            .set_vertex_metadata("one", 1, labels(&[]))
            .await
            .unwrap();
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::Labels).await,
            ["A", "Z"]
        );
        shard
            .set_vertex_metadata("one", 2, labels(&[]))
            .await
            .unwrap();
        assert!(names(&shard, "one", NativeMetadataProcedure::Labels)
            .await
            .is_empty());
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn metadata_local_storage_types_cover_relationships_segments_and_zero_counters() {
        let (_directory, shard) = local_shard_with_policy(GraphIndexPolicy::OutboundOnly).await;
        shard
            .write_edge(edge("one", "DIRECT", 1, 2, "direct"))
            .await
            .unwrap();
        shard
            .create_relationship_with_vertex_metadata(
                edge("one", "IDENTIFIED", 3, 4, "identified"),
                labels(&[]),
                labels(&[]),
            )
            .await
            .unwrap();
        shard
            .bulk_append_out_adjacency_segment_trusted("one", "SEGMENT", 5, [6, 7], "segment")
            .await
            .unwrap();
        shard
            .write_edge(edge("two", "OTHER", 1, 2, "other"))
            .await
            .unwrap();
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::RelationshipTypes).await,
            ["DIRECT", "IDENTIFIED", "SEGMENT"]
        );
        assert_eq!(
            names(&shard, "two", NativeMetadataProcedure::RelationshipTypes).await,
            ["OTHER"]
        );
        shard
            .delete_edge(edge("one", "DIRECT", 1, 2, "delete-direct"))
            .await
            .unwrap();
        shard
            .delete_edge(edge("one", "IDENTIFIED", 3, 4, "delete-identified"))
            .await
            .unwrap();
        shard
            .delete_edge(edge("one", "SEGMENT", 5, 6, "delete-segment-6"))
            .await
            .unwrap();
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::RelationshipTypes).await,
            ["SEGMENT"]
        );
        shard
            .delete_edge(edge("one", "SEGMENT", 5, 7, "delete-segment-7"))
            .await
            .unwrap();
        // The transactional counter remains present with zero after deletion.
        let snapshot = shard.db.snapshot().await.unwrap();
        let key = keys::degree_out("one", "DIRECT", 1);
        let value = snapshot
            .get_with_options(key.as_bytes(), &remote_read_options())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode_u64(&key, &value).unwrap(), 0);
        drop(snapshot);
        assert!(
            names(&shard, "one", NativeMetadataProcedure::RelationshipTypes)
                .await
                .is_empty()
        );
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn metadata_category_seeks_skip_ten_thousand_members_without_losing_rare_labels() {
        let (_directory, mut shard) = local_shard().await;
        let updates = (0..10_000).map(|id| {
            let metadata = if id == 9_999 {
                labels(&["Common", "Rare"])
            } else {
                labels(&["Common"])
            };
            (id, metadata)
        });
        shard
            .set_vertex_metadata_batch("one", updates)
            .await
            .unwrap();
        // Only two live categories may be inspected, regardless of member count.
        shard.limits.max_query_scan_edges = 2;
        let started = std::time::Instant::now();
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::Labels).await,
            ["Common", "Rare"]
        );
        eprintln!("10,000-member metadata lookup: {:?}", started.elapsed());
        shard.close().await.unwrap();
    }

    #[tokio::test]
    async fn metadata_local_storage_caps_and_cancellation_fail_without_partial_results() {
        let (_directory, mut shard) = local_shard().await;
        shard
            .set_vertex_metadata("one", 1, labels(&["A", "B"]))
            .await
            .unwrap();
        shard.limits.max_query_scan_edges = 1;
        let result = shard
            .execute_opencypher_rows(QueryContext::new("one", "scan-cap"), "CALL db.labels()")
            .await;
        assert!(matches!(
            result,
            Err(GraphError::AdmissionRejected {
                operation: "metadata_scan_records",
                ..
            })
        ));
        shard.limits.max_query_scan_edges = 1_000;
        shard.limits.max_query_result_vertices = 1;
        let result = shard
            .execute_opencypher_rows(QueryContext::new("one", "row-cap"), "CALL db.labels()")
            .await;
        assert!(matches!(
            result,
            Err(GraphError::AdmissionRejected {
                operation: "metadata_result_rows",
                ..
            })
        ));
        shard.limits.max_query_result_vertices = 100;
        let result = shard
            .execute_opencypher_rows(
                QueryContext::new("one", "byte-cap").with_max_result_bytes(1),
                "CALL db.labels()",
            )
            .await;
        assert!(matches!(
            result,
            Err(GraphError::AdmissionRejected {
                operation: "client_cursor_buffer_bytes",
                ..
            })
        ));
        let token = crate::QueryCancellationToken::new();
        token.cancel();
        let result = shard
            .execute_opencypher_rows(
                QueryContext::new("one", "cancelled").with_cancellation_token(token),
                "CALL db.labels()",
            )
            .await;
        assert!(matches!(
            result,
            Err(GraphError::QueryTimeout {
                operation: "query_cancelled",
                ..
            })
        ));
        let result = shard
            .execute_opencypher_rows(
                QueryContext::new("one", "timed-out").with_timeout_ms(0),
                "CALL db.labels()",
            )
            .await;
        assert!(matches!(result, Err(GraphError::QueryTimeout { .. })));
        assert_eq!(
            names(&shard, "one", NativeMetadataProcedure::Labels).await,
            ["A", "B"]
        );
        shard.close().await.unwrap();
    }
}
