//! Named subgraphs over HTTP (#337): `POST` and `GET /api/subgraphs`,
//! `GET` and `DELETE /api/subgraphs/:name`, `GET /api/schema?subgraph=`, and
//! `POST /api/query` with `graph` set to a subgraph's name.
//!
//! A subgraph is a persisted view of the tenant graph, so every route here
//! authorizes against the tenant graph (`default`): a credential bound to it
//! can use its subgraphs, and defining or dropping one needs the write role.
//! See `docs/NAMED-SUBGRAPHS.md`.

use std::collections::BTreeMap;

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::graph::subgraph::{SubgraphDefinition, SubgraphScope};
use crate::graph::{GraphStore, PropertyValue};
use crate::http::handler::{
    accepts_ndjson, attach_agent_change, bound_params, merged_node_properties, ms,
    new_agent_change_id, read_only_refusal, render_query_result, validate_agent_attribution,
    QueryRequest, QueryStats, SCHEMA_PROPERTY_SAMPLE,
};
use crate::http::server::{AppState, AuditEnabled, AuditedChange, Subject};
use crate::query::executor::operator::SubgraphScopeRef;

/// The tenant graph a subgraph is a view of. This build serves one.
const TENANT: &str = "default";

fn error(status: StatusCode, message: impl Into<String>) -> axum::response::Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn authorize(
    subject: &Option<Extension<Subject>>,
    write: bool,
) -> Option<axum::response::Response> {
    let Extension(Subject(user)) = subject.as_ref()?;
    user.authorize_statement::<&str>(TENANT, Ok(write))
        .err()
        .map(|e| error(StatusCode::FORBIDDEN, e))
}

fn not_found(name: &str) -> axum::response::Response {
    error(StatusCode::NOT_FOUND, format!("no named subgraph '{name}'"))
}

#[derive(serde::Deserialize)]
pub struct CreateSubgraphRequest {
    #[serde(flatten)]
    definition: SubgraphDefinition,
    #[serde(default)]
    agent_id: Option<String>,
}

#[derive(serde::Deserialize, Default)]
pub struct AgentAttributionQuery {
    agent_id: Option<String>,
}

fn audit_subgraph_change(
    response: &mut axum::response::Response,
    audit: Option<&Extension<AuditEnabled>>,
    agent_id: Option<String>,
    statement: String,
    before: crate::index::catalog::IndexCatalog,
    after: crate::index::catalog::IndexCatalog,
) {
    let change_id = (agent_id.is_some() && before != after).then(new_agent_change_id);
    attach_agent_change(
        response,
        audit,
        AuditedChange {
            statement,
            agent_id,
            change_id,
            change_kind: Some("schema".to_string()),
            node_ids: Vec::new(),
            edge_ids: Vec::new(),
            before: serde_json::json!({ "catalog": before }),
            after: serde_json::json!({ "catalog": after }),
        },
    );
}

/// A subgraph's definition and the size of its view.
fn describe(store: &GraphStore, scope: &SubgraphScope) -> serde_json::Value {
    let schema = store.subgraph_schema(scope);
    let def = scope.definition();
    json!({
        "name": def.name,
        "labels": def.labels,
        "edge_types": def.edge_types,
        "where": def.predicates,
        "nodes": schema.total_nodes,
        "relationships": schema.total_edges,
    })
}

/// `POST /api/subgraphs`: define a subgraph. 201 with its description, 409
/// if the name is taken, 400 if the definition does not compile.
pub async fn create_subgraph_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    audit: Option<Extension<AuditEnabled>>,
    Json(request): Json<CreateSubgraphRequest>,
) -> axum::response::Response {
    let agent_id = match validate_agent_attribution(request.agent_id, audit.is_some()) {
        Ok(agent_id) => agent_id,
        Err(response) => return response,
    };
    let definition = request.definition;
    if let Some(refusal) = authorize(&subject, true) {
        return refusal;
    }
    if let Some(refusal) = state.writes_refused() {
        return error(StatusCode::SERVICE_UNAVAILABLE, refusal);
    }
    let definition_name = definition.name.clone();
    let exists = crate::store_lock::read(&state.store)
        .await
        .subgraph(&definition.name)
        .is_some();
    if exists {
        return error(
            StatusCode::CONFLICT,
            format!(
                "subgraph '{}' already exists; drop it first to redefine it",
                definition.name
            ),
        );
    }
    // `mutate` persists the index catalog, which now holds the definition.
    let (mut response, before, after) = state.mutate(TENANT, |store| {
        let before = store.index_catalog();
        let response = match store.create_subgraph(definition) {
            Ok(scope) => (StatusCode::CREATED, Json(describe(store, &scope))).into_response(),
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        };
        let after = store.index_catalog();
        (response, before, after)
    }).await;
    if response.status().is_success() {
        audit_subgraph_change(&mut response, audit.as_ref(), agent_id, format!("CREATE SUBGRAPH {definition_name}"), before, after);
    }
    response
}

/// `GET /api/subgraphs`: every subgraph of the tenant graph, by name.
pub async fn list_subgraphs_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
) -> impl IntoResponse {
    if let Some(refusal) = authorize(&subject, false) {
        return refusal;
    }
    let store = crate::store_lock::read(&state.store).await;
    let items: Vec<_> = store.subgraphs().map(|s| describe(&store, s)).collect();
    Json(json!({ "subgraphs": items, "count": items.len() })).into_response()
}

/// `GET /api/subgraphs/:name`.
pub async fn get_subgraph_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(refusal) = authorize(&subject, false) {
        return refusal;
    }
    let store = crate::store_lock::read(&state.store).await;
    match store.subgraph(&name) {
        Some(scope) => Json(describe(&store, &scope)).into_response(),
        None => not_found(&name),
    }
}

/// `DELETE /api/subgraphs/:name`. The data the view showed is untouched.
pub async fn delete_subgraph_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    audit: Option<Extension<AuditEnabled>>,
    Query(attribution): Query<AgentAttributionQuery>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let agent_id = match validate_agent_attribution(attribution.agent_id, audit.is_some()) {
        Ok(agent_id) => agent_id,
        Err(response) => return response,
    };
    if let Some(refusal) = authorize(&subject, true) {
        return refusal;
    }
    if let Some(refusal) = state.writes_refused() {
        return error(StatusCode::SERVICE_UNAVAILABLE, refusal);
    }
    let exists = crate::store_lock::read(&state.store)
        .await
        .subgraph(&name)
        .is_some();
    if !exists {
        return not_found(&name);
    }
    let name_for_mutation = name.clone();
    let (mut response, before, after) = state.mutate(TENANT, move |store| {
        let before = store.index_catalog();
        let response = match store.drop_subgraph(&name_for_mutation) {
            Some(_) => Json(json!({ "dropped": name_for_mutation })).into_response(),
            None => not_found(&name_for_mutation),
        };
        let after = store.index_catalog();
        (response, before, after)
    }).await;
    if response.status().is_success() {
        audit_subgraph_change(&mut response, audit.as_ref(), agent_id, format!("DROP SUBGRAPH {name}"), before, after);
    }
    response
}

/// `GET /api/schema?subgraph=name`: the schema response, restricted to the
/// view. Labels and relationship types outside it are not named; counts are
/// of the view's members.
pub(crate) fn subgraph_schema_response(store: &GraphStore, name: &str) -> axum::response::Response {
    let Some(scope) = store.subgraph(name) else {
        return not_found(name);
    };
    let schema = store.subgraph_schema(&scope);
    let node_types: Vec<_> = scope
        .labels()
        .iter()
        .map(|label| {
            // Property types from a bounded sample of the label's members.
            let sample: Vec<_> = store
                .node_ids_by_label(label, None)
                .into_iter()
                .filter(|id| scope.label_admits(store, label, *id))
                .take(SCHEMA_PROPERTY_SAMPLE)
                .collect();
            let mut properties = BTreeMap::new();
            for id in &sample {
                for (key, val) in &store.node_properties_merged(*id) {
                    let t = match val {
                        PropertyValue::String(_) => "String",
                        PropertyValue::Integer(_) => "Integer",
                        PropertyValue::Float(_) => "Float",
                        PropertyValue::Boolean(_) => "Boolean",
                        PropertyValue::Vector(_) => "Vector",
                        _ => "Unknown",
                    };
                    properties.insert(key.clone(), t.to_string());
                }
            }
            json!({
                "label": label.as_str(),
                "count": schema.labels.get(label.as_str()).copied().unwrap_or(0),
                "properties": properties,
                "properties_sampled_from": sample.len(),
            })
        })
        .collect();
    let edge_types: Vec<_> = schema
        .edge_types
        .iter()
        .map(|(t, (count, sources, targets))| {
            json!({
                "type": t,
                "count": count,
                "source_labels": sources.iter().collect::<Vec<_>>(),
                "target_labels": targets.iter().collect::<Vec<_>>(),
            })
        })
        .collect();
    let index_list = store.property_index.list_indexes();
    let indexes: Vec<_> = index_list
        .iter()
        .filter(|(l, _)| scope.has_label(l))
        .map(|(l, p)| json!({ "label": l.as_str(), "property": p, "type": "BTREE" }))
        .collect();
    let constraint_list = store.property_index.list_constraints();
    let constraints: Vec<_> = constraint_list
        .iter()
        .filter(|(l, _)| scope.has_label(l))
        .map(|(l, p)| json!({ "label": l.as_str(), "property": p, "type": "UNIQUE" }))
        .collect();
    let avg_out_degree = if schema.total_nodes > 0 {
        schema.total_edges as f64 / schema.total_nodes as f64
    } else {
        0.0
    };
    Json(json!({
        "subgraph": scope.name(),
        "node_types": node_types,
        "edge_types": edge_types,
        "indexes": indexes,
        "constraints": constraints,
        "statistics": {
            "total_nodes": schema.total_nodes,
            "total_edges": schema.total_edges,
            "avg_out_degree": avg_out_degree,
        }
    }))
    .into_response()
}

/// `POST /api/query` with `graph` naming a subgraph: the read runs as
/// written, confined to the view by the plan's operators. Writes, open
/// transactions and streamed results are refused.
pub(crate) async fn query_subgraph(
    state: &AppState,
    subject: Option<Extension<Subject>>,
    headers: &axum::http::HeaderMap,
    payload: &QueryRequest,
    scope: &SubgraphScopeRef,
) -> axum::response::Response {
    // `None` when the statement does not parse: it is authorized as a write
    // and fails with its parse error. Reduced to an `Option` here because the
    // parser's error is not `Send` and the store lock is awaited below.
    let is_write: Option<bool> = state.engine.statement_is_write(&payload.query).ok();
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement(TENANT, is_write.ok_or(())) {
            return error(StatusCode::FORBIDDEN, e);
        }
    }
    if let Some(refusal) = read_only_refusal(state, payload) {
        return refusal;
    }
    if is_write.unwrap_or(false) {
        return error(
            StatusCode::BAD_REQUEST,
            crate::graph::subgraph::WRITE_REFUSED,
        );
    }
    if payload.tx.is_some() {
        return error(
            StatusCode::BAD_REQUEST,
            "a named subgraph cannot be queried inside an HTTP transaction yet; \
             query it outside the transaction",
        );
    }
    if accepts_ndjson(headers) {
        return error(
            StatusCode::NOT_ACCEPTABLE,
            "streamed results are not available for named subgraphs yet; omit \
             `Accept: application/x-ndjson`",
        );
    }
    let params = match bound_params(payload) {
        Ok(p) => p,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    let time_limit = state.query_time_limit(TENANT);
    let asked = std::time::Instant::now();
    let store = crate::store_lock::read(&state.store).await;
    let mut stats = QueryStats {
        lock_wait_ms: ms(asked.elapsed()),
        ..QueryStats::default()
    };
    let started = std::time::Instant::now();
    let result =
        state
            .engine
            .execute_in_subgraph(&payload.query, &store, &params, time_limit, scope);
    stats.elapsed_ms = ms(started.elapsed());
    let props = result
        .as_ref()
        .map(|b| merged_node_properties(&b.records, &store))
        .unwrap_or_default();
    let version = store.current_version;
    let hash = payload
        .include_snapshot_hash
        .then(|| crate::snapshot::content_hash::cached_content_hash(&store));
    render_query_result(result, &props, false, version, hash, stats)
}
