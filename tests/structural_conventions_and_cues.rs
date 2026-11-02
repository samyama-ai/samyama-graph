//! Integration tests for `.sgsnap` structural conventions, `OEH` whole-hierarchy HTTP visual
//! cues, 3D Morton Z-order index-resident roll-ups, implicit 3D contact edges, manifold-guided
//! $A^*$ shortest paths, and 3D / non-WebGL 2.5D Isometric visual cues (`#2035`, `#2036`, `#2037`).

use std::io::Read;
use std::sync::Arc;

use axum::body::Body;
use http_body_util::BodyExt;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::http::HttpServer;
use samyama::index::structure::{ManifoldSpec, StructuralConvention};
use samyama::query::QueryEngine;
use samyama::snapshot::format::SnapshotHeader;

fn export_bytes(store: &GraphStore) -> Vec<u8> {
    let mut out = Vec::new();
    samyama::snapshot::export_tenant(store, &mut out).unwrap();
    out
}

fn header_raw_json(bytes: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    serde_json::from_str(text.lines().next().unwrap()).unwrap()
}

#[test]
fn sgsnap_header_omits_structures_when_empty_and_roundtrips_when_present() {
    let mut store = GraphStore::new();
    let n = store.create_node("Residue");
    store
        .set_node_property("default", n, "ca_x", PropertyValue::Float(1.0))
        .unwrap();

    // 1. No explicit structures registered -> header line has no `structures` key
    let empty_bytes = export_bytes(&store);
    assert!(!header_raw_json(&empty_bytes).contains_key("structures"));
    let parsed_empty: SnapshotHeader = samyama::snapshot::peek_header(&empty_bytes[..]).unwrap();
    assert!(parsed_empty.structures.is_empty());

    // 2. Register an explicit convention -> survives export + import + re-export
    let conv = StructuralConvention {
        name: "pdb_1crn".to_string(),
        label: Some("Residue".to_string()),
        manifold: ManifoldSpec::Euclidean3d {
            x_prop: "ca_x".to_string(),
            y_prop: "ca_y".to_string(),
            z_prop: "ca_z".to_string(),
            unit: "angstrom".to_string(),
            sequence_prop: Some("residue_seq".to_string()),
            group_by_prop: Some("chain_id".to_string()),
            contact_radius: Some(8.0),
        },
        visual_preset: Some("protein_ribbon_3d".to_string()),
        measure_prop: Some("b_factor".to_string()),
    };
    store.structure_registry.upsert(conv.clone());

    let with_struct_bytes = export_bytes(&store);
    assert!(header_raw_json(&with_struct_bytes).contains_key("structures"));
    let parsed_header: SnapshotHeader = samyama::snapshot::peek_header(&with_struct_bytes[..]).unwrap();
    assert_eq!(parsed_header.structures, vec![conv.clone()]);

    let mut restored = GraphStore::new();
    samyama::snapshot::import_tenant(&mut restored, &with_struct_bytes[..]).unwrap();
    assert_eq!(restored.structure_registry.list(), vec![conv]);
}

#[tokio::test]
async fn http_hierarchies_endpoint_serves_whole_poset_rollups_lca_and_3d_cone_cues() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Build a 3-level taxonomy: Root (0) <- BranchA (1), BranchB (2); BranchA <- Leaf1 (3), Leaf2 (4)
    let root = store.create_node("Taxon");
    let branch_a = store.create_node("Taxon");
    let branch_b = store.create_node("Taxon");
    let leaf_1 = store.create_node("Taxon");
    let leaf_2 = store.create_node("Taxon");

    for (node, units) in [
        (root, 10i64),
        (branch_a, 20),
        (branch_b, 30),
        (leaf_1, 40),
        (leaf_2, 50),
    ] {
        store
            .set_node_property("default", node, "units", PropertyValue::Integer(units))
            .unwrap();
    }

    store.create_edge(branch_a, root, "IS_A").unwrap();
    store.create_edge(branch_b, root, "IS_A").unwrap();
    store.create_edge(leaf_1, branch_a, "IS_A").unwrap();
    store.create_edge(leaf_2, branch_a, "IS_A").unwrap();

    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX tax ON ()-[:IS_A]->() MEASURE units AGGREGATE sum, min, max, count",
            &mut store,
            "default",
        )
        .unwrap();

    let shared_store = Arc::new(RwLock::new(store));
    let app = HttpServer::new(Arc::clone(&shared_store), 0).router();

    // 1. GET /api/hierarchies
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/api/hierarchies")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let list_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(list_json["count"], 1);
    assert_eq!(list_json["hierarchies"][0]["name"], "tax");
    assert_eq!(list_json["hierarchies"][0]["encoding"], "nested-set");
    assert_eq!(list_json["hierarchies"][0]["nodes"], 5);

    // 2. POST /api/hierarchies/tax/cues with lca_pair = (leaf_1, leaf_2)
    let cues_req_body = serde_json::json!({
        "lca_pair": [leaf_1.as_u64(), leaf_2.as_u64()],
    });
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/api/hierarchies/tax/cues")
        .header("content-type", "application/json")
        .body(Body::from(cues_req_body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let cues_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(
        cues_json["lca_node_ids"],
        serde_json::json!([branch_a.as_u64()])
    );
    let cues = cues_json["cues"].as_array().unwrap();
    assert_eq!(cues.len(), 5);

    // Verify Root's whole-hierarchy descendant_count (5) and index-resident SUM (10+20+30+40+50 = 150)
    let root_cue = cues
        .iter()
        .find(|c| c["id"] == root.as_u64())
        .expect("root cue");
    assert_eq!(root_cue["descendant_count"], 5);
    assert_eq!(root_cue["rollups"]["sum"], 150);
    assert_eq!(root_cue["rollups"]["max"], 50);
    assert_eq!(root_cue["depth"], 0);
    assert!(root_cue["iso_x"].as_f64().is_some());
    assert!(root_cue["opacity"].as_f64().unwrap() >= 0.3);

    // Verify BranchA's whole-hierarchy descendant_count (3: BranchA, Leaf1, Leaf2) and SUM (20+40+50 = 110)
    let branch_a_cue = cues
        .iter()
        .find(|c| c["id"] == branch_a.as_u64())
        .expect("branch_a cue");
    assert_eq!(branch_a_cue["descendant_count"], 3);
    assert_eq!(branch_a_cue["rollups"]["sum"], 110);

    // 3. Test LoD budget truncation (lod_budget = 2 returns Root + BranchA)
    let lod_req_body = serde_json::json!({ "lod_budget": 2 });
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/api/hierarchies/tax/cues")
        .header("content-type", "application/json")
        .body(Body::from(lod_req_body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let lod_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(lod_json["cues"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn http_structure_cues_serves_3d_protein_backbone_contacts_and_astar() {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..8 {
        let n = store.create_node("Residue");
        let (x, y, z) = if i == 7 {
            // Fold residue 7 close to residue 0 in 3D space (distance = 4.0 A)
            (4.0, 0.0, 0.0)
        } else {
            (i as f64 * 3.8, (i as f64) * 1.2, (i as f64) * 0.9)
        };
        store
            .set_node_property("default", n, "ca_x", PropertyValue::Float(x))
            .unwrap();
        store
            .set_node_property("default", n, "ca_y", PropertyValue::Float(y))
            .unwrap();
        store
            .set_node_property("default", n, "ca_z", PropertyValue::Float(z))
            .unwrap();
        store
            .set_node_property("default", n, "residue_seq", PropertyValue::Integer(i + 1))
            .unwrap();
        store
            .set_node_property(
                "default",
                n,
                "chain_id",
                PropertyValue::String("A".to_string()),
            )
            .unwrap();
        store
            .set_node_property(
                "default",
                n,
                "b_factor",
                PropertyValue::Float(10.0 * (i + 1) as f64),
            )
            .unwrap();
        ids.push(n);
    }
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "PEPTIDE_BOND").unwrap();
    }

    let shared_store = Arc::new(RwLock::new(store));
    let app = HttpServer::new(Arc::clone(&shared_store), 0).router();

    // 1. GET /api/structure auto-detects the 3D protein structure
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/api/structure")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let struct_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(struct_json["effective"].as_array().unwrap().len(), 1);
    assert_eq!(
        struct_json["effective"][0]["visual_preset"],
        "protein_ribbon_3d"
    );

    // 2. POST /api/structure/cues returns 3D + 2.5D coordinates, backbone chain, contact edges, and A*
    let cues_body = serde_json::json!({
        "astar_pair": [ids[0].as_u64(), ids[7].as_u64()],
    });
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/api/structure/cues")
        .header("content-type", "application/json")
        .body(Body::from(cues_body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let cues_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(cues_json["total_indexed_nodes"], 8);
    assert_eq!(cues_json["nodes"].as_array().unwrap().len(), 8);
    assert_eq!(cues_json["backbone_chains"].as_array().unwrap().len(), 1);
    assert_eq!(
        cues_json["backbone_chains"][0]["node_ids"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(cues_json["whole_graph_rollups"]["sum"], 360.0);
    assert!(!cues_json["contact_edges"].as_array().unwrap().is_empty());
    assert_eq!(cues_json["astar_path"]["hops"], 7);
}
