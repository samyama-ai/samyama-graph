//! HTTP handlers for `OEH` hierarchy visual cues (`/api/hierarchies`) and structural manifold
//! conventions & 3D/2.5D visual analytics (`/api/structure`, `#2035`, `#2036`, `#2037`).

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use crate::graph::types::NodeId;
use crate::http::handler::reject_foreign_graph;
use crate::http::server::{AppState, Subject};
use crate::index::structure::{
    build_structural_visual_cues, ManifoldSpec, SgiIndex, StructuralConvention,
};

#[derive(Deserialize, Default)]
pub struct GraphQueryParam {
    #[serde(default = "default_graph")]
    pub graph: String,
}

fn default_graph() -> String {
    "default".to_string()
}

/// `GET /api/hierarchies` — list all declared `OEH` (`CREATE HIERARCHY INDEX`, `ADR-035`) indexes
/// and their structural probe / roll-up metadata (`#2035`).
pub async fn list_hierarchies_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Query(q): Query<GraphQueryParam>,
) -> impl IntoResponse {
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement::<&str>(&q.graph, Ok(false)) {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response();
        }
    }
    if let Some(resp) = reject_foreign_graph(&q.graph) {
        return resp;
    }

    let store = state.store.read().await;
    let items: Vec<_> = store
        .hierarchy_index
        .list()
        .into_iter()
        .map(|info| {
            json!({
                "name": info.name,
                "edge_types": info.edge_types,
                "encoding": info.encoding,
                "nodes": info.nodes,
                "edges": info.edges,
                "width": info.width,
                "measure": info.measure,
                "ops": info.ops,
                "bytes": info.bytes,
                "structural_bytes": info.structural_bytes,
                "rollup_bytes": info.rollup_bytes,
                "stale": info.stale,
                "declined": info.declined,
            })
        })
        .collect();
    Json(json!({
        "hierarchies": items,
        "count": items.len(),
    }))
    .into_response()
}

/// Request body for `POST /api/hierarchies/:name/cues`.
#[derive(Deserialize, Default)]
pub struct HierarchyCuesRequest {
    #[serde(default = "default_graph")]
    pub graph: String,
    /// Optional subset of `NodeId`s to compute cues for; `None` returns cues for the whole
    /// hierarchy (or up to `lod_budget`).
    #[serde(default)]
    pub node_ids: Option<Vec<u64>>,
    /// Optional pair of `NodeId`s `(a, b)` to compute whole-hierarchy Lowest Common Ancestors for.
    #[serde(default)]
    pub lca_pair: Option<(u64, u64)>,
    /// Optional Level-of-Detail (LoD) node budget (e.g. `250`) to return the top macro-subtrees.
    #[serde(default)]
    pub lod_budget: Option<usize>,
}

/// `POST /api/hierarchies/:name/cues` — authoritative whole-hierarchy `descendant_count`,
/// index-resident `rollups`, whole-poset `lca`, and deterministic 3D Cone-Tree + 2.5D Isometric
/// visual cues from `OehIndex` (`#2035`).
pub async fn hierarchy_cues_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Path(name): Path<String>,
    Json(payload): Json<HierarchyCuesRequest>,
) -> impl IntoResponse {
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement::<&str>(&payload.graph, Ok(false)) {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response();
        }
    }
    if let Some(resp) = reject_foreign_graph(&payload.graph) {
        return resp;
    }

    let store = state.store.read().await;
    let Some(entry_arc) = store.hierarchy_index.get(&name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no hierarchy index named '{name}'") })),
        )
            .into_response();
    };

    let entry = entry_arc.read().unwrap();
    if let Some(reason) = &entry.declined {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": reason,
                "name": entry.spec.name,
                "declined": true,
                "declined_width": entry.declined_width,
            })),
        )
            .into_response();
    }
    let Some(oeh) = &entry.index else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "error": format!("hierarchy index '{name}' is not built") })),
        )
            .into_response();
    };

    let filter: Option<Vec<NodeId>> = payload
        .node_ids
        .as_ref()
        .map(|ids| ids.iter().copied().map(NodeId).collect());
    let cues = oeh.visual_cues(filter.as_deref(), payload.lod_budget);

    let lca_node_ids: Option<Vec<u64>> = payload.lca_pair.and_then(|(a, b)| {
        oeh.lowest_common_ancestors_ids(NodeId(a), NodeId(b))
            .map(|v| v.into_iter().map(|id| id.as_u64()).collect())
    });

    Json(json!({
        "name": entry.spec.name,
        "encoding": oeh.encoding().name(),
        "stale": entry.stale,
        "total_poset_nodes": entry.nodes,
        "total_poset_edges": entry.edges,
        "measure": entry.spec.measure.as_ref().map(|m| &m.property),
        "lca_node_ids": lca_node_ids,
        "cues": cues,
    }))
    .into_response()
}

/// `GET /api/structure` — list explicit and auto-detected [`StructuralConvention`]s (`#2035`).
pub async fn get_structure_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Query(q): Query<GraphQueryParam>,
) -> impl IntoResponse {
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement::<&str>(&q.graph, Ok(false)) {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response();
        }
    }
    if let Some(resp) = reject_foreign_graph(&q.graph) {
        return resp;
    }

    let store = state.store.read().await;
    let explicit = store.structure_registry.list();
    let effective = store.structure_registry.effective_conventions(&store);
    Json(json!({
        "format": "samyama.structure/1",
        "explicit": explicit,
        "effective": effective,
    }))
    .into_response()
}

/// Request body for `PUT /api/structure`.
#[derive(Deserialize)]
pub struct PutStructureRequest {
    #[serde(default = "default_graph")]
    pub graph: String,
    pub structures: Vec<StructuralConvention>,
}

/// `PUT /api/structure` — replace the graph's explicit [`StructuralConvention`]s (`#2035`).
pub async fn put_structure_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Json(payload): Json<PutStructureRequest>,
) -> impl IntoResponse {
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement::<&str>(&payload.graph, Ok(true)) {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response();
        }
    }
    if let Some(resp) = reject_foreign_graph(&payload.graph) {
        return resp;
    }
    if payload.structures.len() > 64 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "at most 64 structural conventions may be registered per graph" })),
        )
            .into_response();
    }
    for conv in &payload.structures {
        if conv.name.trim().is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "structural convention name must not be empty" })),
            )
                .into_response();
        }
    }

    let store = state.store.read().await;
    store.structure_registry.replace(payload.structures);
    let explicit = store.structure_registry.list();
    let effective = store.structure_registry.effective_conventions(&store);
    Json(json!({
        "format": "samyama.structure/1",
        "explicit": explicit,
        "effective": effective,
    }))
    .into_response()
}

/// Request body for `POST /api/structure/cues`.
#[derive(Deserialize, Default)]
pub struct StructureCuesRequest {
    #[serde(default = "default_graph")]
    pub graph: String,
    /// Optional convention name; defaults to the first effective convention.
    #[serde(default)]
    pub convention_name: Option<String>,
    /// Optional subset of `NodeId`s to compute cues for.
    #[serde(default)]
    pub node_ids: Option<Vec<u64>>,
    /// Maximum number of nodes to return in `nodes` (`default: 2000`).
    #[serde(default)]
    pub max_nodes: Option<usize>,
    /// Optional override for implicit 3D contact radius (`contact_edges`).
    #[serde(default)]
    pub contact_radius: Option<f64>,
    /// Optional `(start_id, goal_id)` to compute a manifold-guided $A^*$ shortest path (`#2037`).
    #[serde(default)]
    pub astar_pair: Option<(u64, u64)>,
}

/// `POST /api/structure/cues` — compute 3D & non-WebGL 2.5D Isometric visual cues, ordered
/// backbone chains, implicit 3D contact edges with topological-geometric stress, index-resident
/// roll-ups, and optional manifold-guided $A^*$ shortest path (`#2035`, `#2036`, `#2037`).
pub async fn structure_cues_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Json(payload): Json<StructureCuesRequest>,
) -> impl IntoResponse {
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement::<&str>(&payload.graph, Ok(false)) {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response();
        }
    }
    if let Some(resp) = reject_foreign_graph(&payload.graph) {
        return resp;
    }

    let store = state.store.read().await;
    let effective = store.structure_registry.effective_conventions(&store);
    if effective.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "no structural convention declared or auto-detected on this graph"
            })),
        )
            .into_response();
    }

    let chosen = match &payload.convention_name {
        Some(name) => effective.into_iter().find(|c| &c.name == name),
        None => effective.into_iter().next(),
    };
    let Some(convention) = chosen else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!(
                    "no structural convention named '{}'",
                    payload.convention_name.as_deref().unwrap_or("")
                )
            })),
        )
            .into_response();
    };

    let filter: Option<Vec<NodeId>> = payload
        .node_ids
        .as_ref()
        .map(|ids| ids.iter().copied().map(NodeId).collect());

    if let ManifoldSpec::HierarchyPoset { index_name, .. } = &convention.manifold {
        let Some(entry_arc) = store.hierarchy_index.get(index_name) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("no hierarchy index named '{index_name}'") })),
            )
                .into_response();
        };
        let entry = entry_arc.read().unwrap();
        let Some(oeh) = &entry.index else {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": format!("hierarchy index '{index_name}' is not built") })),
            )
                .into_response();
        };
        let cues = oeh.visual_cues(filter.as_deref(), payload.max_nodes);
        return Json(json!({
            "convention": convention,
            "hierarchy_encoding": oeh.encoding().name(),
            "total_indexed_nodes": entry.nodes,
            "hierarchy_cues": cues,
        }))
        .into_response();
    }

    let visual_cues = build_structural_visual_cues(
        &store,
        &convention,
        filter.as_deref(),
        payload.max_nodes,
        payload.contact_radius,
    );

    let astar_path = payload.astar_pair.and_then(|(start, goal)| {
        let sgi = SgiIndex::build(&store, convention.clone());
        sgi.astar_shortest_path(&store, NodeId(start), NodeId(goal), None)
    });

    Json(json!({
        "convention": visual_cues.convention,
        "bbox": visual_cues.bbox,
        "total_indexed_nodes": visual_cues.total_indexed_nodes,
        "nodes": visual_cues.nodes,
        "backbone_chains": visual_cues.backbone_chains,
        "contact_edges": visual_cues.contact_edges,
        "whole_graph_rollups": visual_cues.whole_graph_rollups,
        "astar_path": astar_path,
    }))
    .into_response()
}
