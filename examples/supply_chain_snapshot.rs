//! Paper 8 problem 3: build the samyama Supply Chain India KG and export it as
//! .sgsnap. Reads `p3_spec.json` (produced by `build_spec.py`) which already
//! contains the OSM-Dijkstra port->city distance matrix.
//!
//! Output schema:
//!   (:Port {id, name, lat, lon, throughput_mtpa})
//!   (:City {name, lat, lon, pop_million})
//!   (:Port)-[:ROAD_DISTANCE_TO {distance_km, disrupted}]->(:City)
//!
//! Usage:
//!   cargo run --release --example supply_chain_snapshot -- \
//!       --nodes india_nodes.json --spec p3_spec.json \
//!       --out /tmp/p8-snapshots/supplychain-india.sgsnap

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;

fn arg(argv: &[String], name: &str) -> Option<String> {
    argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1)).cloned()
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let nodes_path = PathBuf::from(arg(&argv, "--nodes").unwrap_or("india_nodes.json".into()));
    let spec_path = PathBuf::from(arg(&argv, "--spec").unwrap_or("p3_spec.json".into()));
    let out_path = PathBuf::from(
        arg(&argv, "--out").unwrap_or("/tmp/p8-snapshots/supplychain-india.sgsnap".into()),
    );

    eprintln!("nodes: {}", nodes_path.display());
    eprintln!("spec : {}", spec_path.display());

    let mut s = String::new();
    File::open(&nodes_path).expect("nodes").read_to_string(&mut s).unwrap();
    let nodes_json: serde_json::Value = serde_json::from_str(&s).expect("nodes json");
    s.clear();
    File::open(&spec_path).expect("spec").read_to_string(&mut s).unwrap();
    let spec: serde_json::Value = serde_json::from_str(&s).expect("spec json");

    let ports_json = nodes_json["ports"].as_array().expect("ports[]");
    let cities_json = nodes_json["cities"].as_array().expect("cities[]");
    let n_ports = ports_json.len();
    let n_cities = cities_json.len();
    let dist = spec["distance_km"].as_array().expect("distance_km");
    let disrupted: Vec<usize> = spec["disrupted_ports"].as_array().expect("disrupted_ports")
        .iter().filter_map(|v| v.as_u64().map(|n| n as usize)).collect();
    let used_osm = spec["used_osm"].as_bool().unwrap_or(false);
    eprintln!("ports={}, cities={}, used_osm={}, disrupted_ports={:?}",
        n_ports, n_cities, used_osm, disrupted);

    let t0 = Instant::now();
    let mut store = GraphStore::new();

    let mut port_ids: Vec<samyama::graph::NodeId> = Vec::with_capacity(n_ports);
    for (i, p) in ports_json.iter().enumerate() {
        let nid = store.create_node(Label::new("Port"));
        let m = store.get_node_mut(nid).unwrap();
        m.set_property("id", PropertyValue::String(p["id"].as_str().unwrap_or("").to_string()));
        m.set_property("name", PropertyValue::String(p["name"].as_str().unwrap_or("").to_string()));
        m.set_property("lat", PropertyValue::Float(p["lat"].as_f64().unwrap_or(0.0)));
        m.set_property("lon", PropertyValue::Float(p["lon"].as_f64().unwrap_or(0.0)));
        m.set_property("throughput_mtpa", PropertyValue::Float(p["throughput_mtpa"].as_f64().unwrap_or(0.0)));
        m.set_property("disrupted", PropertyValue::Boolean(disrupted.contains(&i)));
        port_ids.push(nid);
    }

    let mut city_ids: Vec<samyama::graph::NodeId> = Vec::with_capacity(n_cities);
    for c in cities_json.iter() {
        let nid = store.create_node(Label::new("City"));
        let m = store.get_node_mut(nid).unwrap();
        m.set_property("name", PropertyValue::String(c["name"].as_str().unwrap_or("").to_string()));
        m.set_property("lat", PropertyValue::Float(c["lat"].as_f64().unwrap_or(0.0)));
        m.set_property("lon", PropertyValue::Float(c["lon"].as_f64().unwrap_or(0.0)));
        m.set_property("pop_million", PropertyValue::Float(c["pop_million"].as_f64().unwrap_or(0.0)));
        city_ids.push(nid);
    }

    let mut edges_added = 0;
    for i in 0..n_ports {
        let row = dist[i].as_array().expect("dist row");
        for j in 0..n_cities {
            let d_km = row[j].as_f64().unwrap_or(0.0);
            let eid = store.create_edge(
                port_ids[i],
                city_ids[j],
                samyama::graph::EdgeType::new("ROAD_DISTANCE_TO"),
            ).expect("edge");
            store.set_edge_property(eid, "distance_km", PropertyValue::Float(d_km))
                .expect("set distance_km");
            store.set_edge_property(eid, "disrupted",
                PropertyValue::Boolean(disrupted.contains(&i)))
                .expect("set disrupted");
            edges_added += 1;
        }
    }

    let build_ms = t0.elapsed().as_millis();
    eprintln!("KG built: {} nodes, {} edges in {} ms",
        n_ports + n_cities, edges_added, build_ms);

    let engine = QueryEngine::new();
    let b = engine.execute(
        "MATCH (p:Port)-[r:ROAD_DISTANCE_TO]->(c:City) RETURN count(r) AS c", &store).expect("count");
    let c = match b.records.first().and_then(|r| r.get("c")) {
        Some(samyama::query::executor::record::Value::Property(PropertyValue::Integer(i))) => *i,
        _ => -1,
    };
    eprintln!("Cypher sanity: count(:Port-:ROAD_DISTANCE_TO->:City) = {}", c);

    std::fs::create_dir_all(out_path.parent().unwrap()).ok();
    let f = File::create(&out_path).expect("snapshot file");
    let stats = samyama::snapshot::export_tenant(&store, f).expect("export");
    eprintln!("snapshot -> {} ({} nodes, {} edges)",
        out_path.display(), stats.node_count, stats.edge_count);
}
