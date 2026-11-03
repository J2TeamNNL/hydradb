use boltr::error::BoltError;
use boltr::types::{
    BoltDict, BoltNode, BoltPath, BoltRelationship, BoltUnboundRelationship, BoltValue,
};

use super::{ClientBookmark, ClientQueryTarget};
use crate::{
    GraphError, QueryFloat, QueryParameterValue, QueryPath, QueryPathNode, QueryPathRelationship,
    QueryValue, VertexPropertyValue,
};

const MAX_QUERY_PARAMETER_DEPTH: usize = 16;

pub(super) fn highest_matching_bookmark(
    target: &ClientQueryTarget,
    bookmarks: Vec<String>,
) -> std::result::Result<Option<ClientBookmark>, BoltError> {
    let mut highest = None;
    for bookmark in bookmarks {
        let bookmark = ClientBookmark::parse(&bookmark).map_err(graph_error_to_bolt)?;
        if bookmark.target != *target {
            return Err(BoltError::Query {
                code: "Neo.ClientError.Transaction.InvalidBookmark".to_string(),
                message: "bookmark belongs to another graph scope or cell".to_string(),
            });
        }
        if highest
            .as_ref()
            .is_none_or(|current: &ClientBookmark| bookmark.epoch > current.epoch)
        {
            highest = Some(bookmark);
        }
    }
    Ok(highest)
}

pub(super) fn bolt_parameter_to_property(
    name: &str,
    value: &BoltValue,
) -> std::result::Result<VertexPropertyValue, BoltError> {
    match value {
        BoltValue::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        BoltValue::Integer(value) => Ok(VertexPropertyValue::from_i64(*value)),
        BoltValue::Float(value) if value.is_finite() => {
            Ok(VertexPropertyValue::Float(QueryFloat(*value)))
        }
        BoltValue::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        BoltValue::Float(_) => Err(invalid_bolt_parameter(name, "float must be finite")),
        _ => Err(invalid_bolt_parameter(
            name,
            "only boolean, signed integer, finite float, and string parameters are supported",
        )),
    }
}

pub(super) fn bolt_parameter_to_query_value(
    name: &str,
    value: &BoltValue,
) -> std::result::Result<QueryParameterValue, BoltError> {
    bolt_parameter_to_query_value_at_depth(name, value, 0)
}

fn bolt_parameter_to_query_value_at_depth(
    name: &str,
    value: &BoltValue,
    depth: usize,
) -> std::result::Result<QueryParameterValue, BoltError> {
    if depth > MAX_QUERY_PARAMETER_DEPTH {
        return Err(invalid_bolt_parameter(
            name,
            "nested parameter depth exceeds 16",
        ));
    }
    match value {
        BoltValue::List(values) => values
            .iter()
            .map(|value| bolt_parameter_to_query_value_at_depth(name, value, depth + 1))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(QueryParameterValue::List),
        BoltValue::Dict(values) => values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    bolt_parameter_to_query_value_at_depth(name, value, depth + 1)?,
                ))
            })
            .collect::<std::result::Result<std::collections::BTreeMap<_, _>, BoltError>>()
            .map(QueryParameterValue::Map),
        _ => bolt_parameter_to_property(name, value).map(QueryParameterValue::Scalar),
    }
}

fn invalid_bolt_parameter(name: &str, reason: &str) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: format!("invalid parameter ${name}: {reason}"),
    }
}

pub(super) fn query_value_to_bolt(value: &QueryValue) -> std::result::Result<BoltValue, BoltError> {
    match value {
        QueryValue::Null => Ok(BoltValue::Null),
        QueryValue::VertexId(value) | QueryValue::Count(value) => i64::try_from(*value)
            .map(BoltValue::Integer)
            .map_err(|_| bolt_integer_overflow(*value)),
        QueryValue::Bool(value) => Ok(BoltValue::Boolean(*value)),
        QueryValue::Float(value) => Ok(BoltValue::Float(value.0)),
        QueryValue::Property(value) => property_value_to_bolt(value),
        QueryValue::List(values) => values
            .iter()
            .map(query_value_to_bolt)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(BoltValue::List),
        QueryValue::Node(node) => query_node_to_bolt(node).map(BoltValue::Node),
        QueryValue::Relationship(relationship) => {
            query_relationship_to_bolt(relationship).map(BoltValue::Relationship)
        }
        QueryValue::Path(path) => query_path_to_bolt(path).map(BoltValue::Path),
    }
}

fn query_node_to_bolt(node: &QueryPathNode) -> std::result::Result<BoltNode, BoltError> {
    Ok(BoltNode {
        id: i64::try_from(node.id).map_err(|_| bolt_integer_overflow(node.id))?,
        labels: node.labels.clone(),
        properties: property_map_to_bolt(&node.properties)?,
        element_id: node.id.to_string(),
    })
}

fn query_relationship_to_bolt(
    relationship: &QueryPathRelationship,
) -> std::result::Result<BoltRelationship, BoltError> {
    Ok(BoltRelationship {
        // Structural edges have no supported legacy numeric ID. Bolt 5 clients
        // use the canonical element ID to distinguish these relationships.
        id: relationship.id.map_or(Ok(-1), |id| {
            i64::try_from(id).map_err(|_| bolt_integer_overflow(id))
        })?,
        start_node_id: i64::try_from(relationship.src)
            .map_err(|_| bolt_integer_overflow(relationship.src))?,
        end_node_id: i64::try_from(relationship.dst)
            .map_err(|_| bolt_integer_overflow(relationship.dst))?,
        rel_type: relationship.edge_type.clone(),
        properties: property_map_to_bolt(&relationship.properties)?,
        element_id: relationship.element_id(),
        start_element_id: relationship.src.to_string(),
        end_element_id: relationship.dst.to_string(),
    })
}

fn query_path_to_bolt(path: &QueryPath) -> std::result::Result<BoltPath, BoltError> {
    if path.nodes.len() != path.relationships.len().saturating_add(1) {
        return Err(BoltError::Backend(
            "native path node and relationship counts are inconsistent".to_string(),
        ));
    }
    let nodes = path
        .nodes
        .iter()
        .map(query_node_to_bolt)
        .collect::<std::result::Result<Vec<_>, BoltError>>()?;
    let relationships = path
        .relationships
        .iter()
        .enumerate()
        .map(|(index, relationship)| {
            let id = match relationship.id {
                Some(id) => i64::try_from(id).map_err(|_| bolt_integer_overflow(id))?,
                // Parenthesised deliberately: a method call binds tighter than
                // unary minus, so `-1_i64.saturating_sub(n)` reads as
                // `-(1 - n)`, which climbs into the positive ids that real
                // relationships occupy.
                None => (-1_i64).saturating_sub(index as i64),
            };
            Ok(BoltUnboundRelationship {
                id,
                rel_type: relationship.edge_type.clone(),
                properties: property_map_to_bolt(&relationship.properties)?,
                element_id: relationship.element_id(),
            })
        })
        .collect::<std::result::Result<Vec<_>, BoltError>>()?;
    let mut indices = Vec::with_capacity(path.relationships.len().saturating_mul(2));
    for (index, relationship) in path.relationships.iter().enumerate() {
        let from = path.nodes[index].id;
        let to = path.nodes[index + 1].id;
        let relationship_index = i64::try_from(index + 1).map_err(|_| {
            BoltError::Backend("native path contains too many relationships for Bolt".to_string())
        })?;
        let signed_index = if relationship.src == from && relationship.dst == to {
            relationship_index
        } else if relationship.src == to && relationship.dst == from {
            -relationship_index
        } else {
            return Err(BoltError::Backend(
                "native path relationship does not connect adjacent nodes".to_string(),
            ));
        };
        indices.push(signed_index);
        indices.push(i64::try_from(index + 1).map_err(|_| {
            BoltError::Backend("native path contains too many nodes for Bolt".to_string())
        })?);
    }
    Ok(BoltPath {
        nodes,
        rels: relationships,
        indices,
    })
}

fn property_map_to_bolt(
    properties: &std::collections::BTreeMap<String, VertexPropertyValue>,
) -> std::result::Result<BoltDict, BoltError> {
    properties
        .iter()
        .map(|(key, value)| Ok((key.clone(), property_value_to_bolt(value)?)))
        .collect()
}

fn property_value_to_bolt(
    value: &VertexPropertyValue,
) -> std::result::Result<BoltValue, BoltError> {
    match value {
        VertexPropertyValue::Integer(value) => i64::try_from(*value)
            .map(BoltValue::Integer)
            .map_err(|_| bolt_integer_overflow(*value)),
        VertexPropertyValue::SignedInteger(value) => Ok(BoltValue::Integer(*value)),
        VertexPropertyValue::Bool(value) => Ok(BoltValue::Boolean(*value)),
        VertexPropertyValue::Float(value) => Ok(BoltValue::Float(value.0)),
        VertexPropertyValue::String(value) => Ok(BoltValue::String(value.clone())),
    }
}

fn bolt_integer_overflow(value: u64) -> BoltError {
    BoltError::Query {
        code: "Neo.ClientError.Statement.TypeError".to_string(),
        message: format!("value {value} exceeds Bolt's signed 64-bit integer range"),
    }
}

pub(super) fn explicit_transactions_unsupported() -> BoltError {
    BoltError::Transaction(
        "explicit transactions are not supported; use auto-commit RUN queries".to_string(),
    )
}

pub(super) fn graph_error_to_bolt(error: GraphError) -> BoltError {
    match error {
        GraphError::GraphScopeAccessDenied { .. } => BoltError::Forbidden(error.to_string()),
        GraphError::AdmissionRejected { .. } => BoltError::ResourceExhausted(error.to_string()),
        GraphError::SnapshotAhead { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.BookmarkTimeout".to_string(),
            message: error.to_string(),
        },
        GraphError::SnapshotUnavailable { .. } => BoltError::Query {
            code: "Neo.DatabaseError.Statement.ExecutionFailed".to_string(),
            message: error.to_string(),
        },
        GraphError::InvalidKeyComponent { .. }
        | GraphError::MissingQueryParameter { .. }
        | GraphError::QueryParse { .. }
        | GraphError::UnsupportedQuery { .. }
        | GraphError::UnclassifiedQuery { .. } => BoltError::Query {
            code: "Neo.ClientError.Statement.InvalidSyntax".to_string(),
            message: error.to_string(),
        },
        // The query is fine; two of its rows set one property to different
        // values. A syntax code sends the caller to the wrong place.
        GraphError::BatchValueConflict { .. } => BoltError::Query {
            code: "Neo.ClientError.Statement.ArgumentError".to_string(),
            message: error.to_string(),
        },
        GraphError::QueryTimeout { .. } => BoltError::Query {
            code: "Neo.TransientError.Transaction.Terminated".to_string(),
            message: error.to_string(),
        },
        GraphError::IdempotencyConflict { .. } => BoltError::Query {
            code: "Neo.ClientError.Transaction.Invalid".to_string(),
            message: error.to_string(),
        },
        // Touch point (c). Drivers already know this code: discard the routing
        // table, re-route, retry. Without it a refused write arrives as an
        // opaque backend error and the driver retries into the same wrong node
        // until it gives up. The owner rides in the message, TiKV's
        // `NotLeader { leader_hint }` — and is absent, not guessed, when this
        // node has shed its view and has nothing truthful to point at.
        GraphError::NotCellWriter { .. } => BoltError::Query {
            code: "Neo.ClientError.Cluster.NotALeader".to_string(),
            message: error.to_string(),
        },
        // A routing refusal is transient and belongs to *this node*, so the
        // driver must move to the next router rather than fail the query. A
        // `ClientError` here would be read as "your request is wrong" and end
        // the attempt — which is the wrong answer for a node that has merely
        // shed its view of the fleet (decision 7) while every peer can still
        // answer. `TransientError` is what the drivers' retry logic is built
        // around, and `DatabaseUnavailable` is the honest reason: this node
        // cannot serve, ask another.
        GraphError::RoutingUnavailable { .. } => BoltError::Query {
            code: "Neo.TransientError.General.DatabaseUnavailable".to_string(),
            message: error.to_string(),
        },
        _ => {
            tracing::warn!(target: "hydradb", error = %error, "Bolt suppressed internal graph error");
            BoltError::Backend("internal query execution error".to_string())
        }
    }
}

#[cfg(test)]
mod entity_value_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn relationship(id: Option<u64>, dst: u64) -> QueryPathRelationship {
        QueryPathRelationship {
            id,
            edge_type: "KNOWS".into(),
            src: 9007199254740993,
            dst,
            properties: BTreeMap::from([(
                "weight".into(),
                VertexPropertyValue::Integer(9007199254740993),
            )]),
        }
    }

    #[test]
    fn standalone_entities_preserve_properties_and_nested_lists() {
        let node = QueryPathNode {
            id: 9007199254740993,
            labels: vec!["Person".into(), "Author".into()],
            properties: BTreeMap::from([(
                "name".into(),
                VertexPropertyValue::String("Ada".into()),
            )]),
        };
        let value = QueryValue::List(vec![QueryValue::List(vec![QueryValue::Node(Box::new(
            node.clone(),
        ))])]);
        let BoltValue::List(outer) = query_value_to_bolt(&value).unwrap() else {
            panic!("list")
        };
        let BoltValue::List(inner) = &outer[0] else {
            panic!("nested list")
        };
        let BoltValue::Node(encoded) = &inner[0] else {
            panic!("node")
        };
        assert_eq!(encoded.id, 9007199254740993);
        assert_eq!(encoded.element_id, "9007199254740993");
        assert_eq!(encoded.labels, node.labels);
        assert!(
            matches!(encoded.properties.get("name"), Some(BoltValue::String(value)) if value == "Ada")
        );
    }

    #[test]
    fn relationships_have_canonical_identity_and_exact_endpoints() {
        for id in [None, Some(41), Some(42)] {
            let relation = relationship(id, 7);
            let BoltValue::Relationship(encoded) =
                query_value_to_bolt(&QueryValue::Relationship(Box::new(relation.clone()))).unwrap()
            else {
                panic!("relationship")
            };
            assert_eq!(encoded.id, id.map_or(-1, |id| id as i64));
            assert_eq!(encoded.element_id, relation.element_id());
            assert_eq!(encoded.rel_type, "KNOWS");
            assert_eq!(encoded.start_node_id, 9007199254740993);
            assert_eq!(encoded.end_node_id, 7);
            assert_eq!(encoded.start_element_id, "9007199254740993");
            assert_eq!(encoded.end_element_id, "7");
            assert!(matches!(
                encoded.properties.get("weight"),
                Some(BoltValue::Integer(9007199254740993))
            ));
        }
        assert_ne!(
            relationship(None, 7).element_id(),
            relationship(None, 8).element_id()
        );
        assert_ne!(
            relationship(Some(41), 7).element_id(),
            relationship(Some(42), 7).element_id()
        );
    }

    #[test]
    fn entity_integer_overflow_is_an_explicit_error() {
        let mut relation = relationship(Some(u64::MAX), 7);
        assert!(query_relationship_to_bolt(&relation).is_err());
        relation.id = Some(1);
        relation.dst = u64::MAX;
        assert!(query_relationship_to_bolt(&relation).is_err());
        relation.dst = 7;
        relation
            .properties
            .insert("weight".into(), VertexPropertyValue::Integer(u64::MAX));
        assert!(query_relationship_to_bolt(&relation).is_err());
        let node = QueryPathNode {
            id: u64::MAX,
            labels: vec![],
            properties: BTreeMap::new(),
        };
        assert!(query_node_to_bolt(&node).is_err());
    }
}
