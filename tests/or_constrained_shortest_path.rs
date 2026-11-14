//! `algo.or.constrainedShortestPath`: the first graph-native optimisation
//! template (OPT-06, #1770).
//!
//! Driven through Cypher, as a caller would: the cheapest route whose total
//! travel time stays within a limit. The algorithm itself is checked against
//! brute force in `crates/samyama-graph-algorithms/src/constrained_path.rs`;
//! this file checks the procedure: its names, its arguments, which properties
//! it reads, what it refuses, and that it answers on both executors.

use samyama::graph::{EdgeType, GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::{BoundParams, QueryEngine};

const TENANT: &str = "default";

/// Four places. Three routes from `s` to `t`:
///   s-a-t: cost 2,  20 minutes (cheap, slow)
///   s-b-t: cost 10,  2 minutes
///   s-t:   cost 20,  1 minute  (dear, quick)
/// plus a `FERRY` from `s` to `t` that is cheapest and quickest of all, so a
/// call that does not filter by `edgeType` would take it.
struct Map {
    store: GraphStore,
    s: u64,
    a: u64,
    b: u64,
    t: u64,
}

fn map() -> Map {
    let mut store = GraphStore::new();
    let [s, a, b, t] = ["s", "a", "b", "t"].map(|name| {
        let id = store.create_node("Place");
        store.set_node_property(TENANT, id, "name", PropertyValue::String(name.into())).unwrap();
        id
    });
    let mut road = |from, to, cost: f64, minutes: f64, kind: &str| {
        let e = store.create_edge(from, to, EdgeType::new(kind)).unwrap();
        store.set_edge_property_sparse(e, "cost", PropertyValue::Float(cost));
        store.set_edge_property_sparse(e, "minutes", PropertyValue::Float(minutes));
    };
    road(s, a, 1.0, 10.0, "ROAD");
    road(a, t, 1.0, 10.0, "ROAD");
    road(s, b, 5.0, 1.0, "ROAD");
    road(b, t, 5.0, 1.0, "ROAD");
    road(s, t, 20.0, 1.0, "ROAD");
    road(s, t, 0.5, 0.5, "FERRY");
    Map { s: s.as_u64(), a: a.as_u64(), b: b.as_u64(), t: t.as_u64(), store }
}

/// `(path, cost, resource)` of the one row, or `None` for no row.
type Answer = Option<(Vec<u64>, f64, f64)>;

fn answer(m: &Map, config: &str) -> Result<Answer, String> {
    ask(m, "algo.or.constrainedShortestPath", config)
}

fn ask(m: &Map, name: &str, config: &str) -> Result<Answer, String> {
    let q = format!("CALL {name}({}, {}, {config}) YIELD path, cost, resource RETURN path, cost, resource", m.s, m.t);
    let batch = QueryEngine::new().execute(&q, &m.store).map_err(|e| e.to_string())?;
    assert!(batch.records.len() <= 1, "one row at most: {:?}", batch.records);
    Ok(batch.records.first().map(|r| {
        let path = match r.get("path") {
            Some(Value::Property(PropertyValue::Array(ids))) => ids
                .iter()
                .map(|v| match v {
                    PropertyValue::Integer(i) => *i as u64,
                    other => panic!("path holds {other:?}"),
                })
                .collect(),
            other => panic!("path is {other:?}"),
        };
        let num = |k: &str| match r.get(k) {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("{k} is {other:?}"),
        };
        (path, num("cost"), num("resource"))
    }))
}

const ROADS: &str = "resourceProperty: 'minutes', costProperty: 'cost', edgeType: 'ROAD'";

#[test]
fn the_time_limit_decides_which_route_is_cheapest() {
    let m = map();
    let at = |max: &str| answer(&m, &format!("{{{ROADS}, maxResource: {max}}}")).unwrap();
    let (s, a, b, t) = (m.s, m.a, m.b, m.t);
    assert_eq!(at("100"), Some((vec![s, a, t], 2.0, 20.0)), "no real limit: the cheapest road");
    assert_eq!(at("20"), Some((vec![s, a, t], 2.0, 20.0)), "a limit met exactly is within it");
    assert_eq!(at("19.5"), Some((vec![s, b, t], 10.0, 2.0)), "the cheap road is too slow");
    assert_eq!(at("1"), Some((vec![s, t], 20.0, 1.0)), "only the direct road fits");
    assert_eq!(at("0.5"), None, "no road fits: no row, not an error");
}

#[test]
fn without_an_edge_type_every_relationship_is_a_choice() {
    let m = map();
    let got = answer(&m, "{resourceProperty: 'minutes', costProperty: 'cost', maxResource: 100}").unwrap();
    assert_eq!(got, Some((vec![m.s, m.t], 0.5, 0.5)), "the ferry");
}

#[test]
fn without_a_cost_property_the_cost_is_the_number_of_hops() {
    let m = map();
    // Roads only. Fewest hops is the direct road (1), whatever it costs.
    let got = answer(&m, "{resourceProperty: 'minutes', edgeType: 'ROAD', maxResource: 100}").unwrap();
    assert_eq!(got, Some((vec![m.s, m.t], 1.0, 1.0)));
}

#[test]
fn both_names_answer_and_so_does_the_write_executor() {
    let mut m = map();
    let config = format!("{{{ROADS}, maxResource: 19.5}}");
    let expected = Some((vec![m.s, m.b, m.t], 10.0, 2.0));
    assert_eq!(ask(&m, "algo.or.constrainedShortestPath", &config).unwrap(), expected);
    assert_eq!(ask(&m, "algo.or.shortestPathWithConstraints", &config).unwrap(), expected);

    // An ordinary session runs on the write executor. A procedure added to
    // the read dispatcher alone answers "Unknown algorithm" there.
    let q = format!(
        "CALL algo.or.constrainedShortestPath({}, {}, {config}) YIELD cost RETURN cost",
        m.s, m.t
    );
    let batch = QueryEngine::new().execute_mut(&q, &mut m.store, TENANT).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].get("cost"),
        Some(&Value::Property(PropertyValue::Float(10.0)))
    );
}

/// Each refusal names what was wrong. Every one of these used to be
/// `Unknown algorithm`.
#[test]
fn bad_calls_are_refused_by_name() {
    let m = map();
    let refused = |config: &str, says: &str| {
        let err = answer(&m, config).unwrap_err();
        assert!(err.contains(says), "{config}: expected `{says}` in: {err}");
    };
    refused("{maxResource: 10}", "`resourceProperty` is required");
    refused("{resourceProperty: 'minutes'}", "`maxResource` is required");
    refused("{resourceProperty: 'minutes', maxResource: 'ten'}", "`maxResource` must be a number");
    refused("{resourceProperty: 'minutes', maxResource: -1}", "resource limit is -1");
    refused("{resourceProperty: 'minutes', maxResource: 10, maxResouce: 5}", "unknown config key `maxResouce`");
    refused("{resourceProperty: 'minutes', maxResource: 10, graph: 'g'}", "cannot run on a graph projection");

    let q = format!("CALL algo.or.constrainedShortestPath({}, 999999, {{resourceProperty: 'minutes', maxResource: 10}}) YIELD cost RETURN cost", m.s);
    let err = QueryEngine::new().execute(&q, &m.store).unwrap_err().to_string();
    assert!(err.contains("node 999999 is not in the graph"), "{err}");
}

#[test]
fn a_negative_resource_is_refused_not_used() {
    let mut store = GraphStore::new();
    let (x, y) = (store.create_node("Place"), store.create_node("Place"));
    let e = store.create_edge(x, y, EdgeType::new("ROAD")).unwrap();
    store.set_edge_property_sparse(e, "minutes", PropertyValue::Float(-5.0));
    let q = format!(
        "CALL algo.or.constrainedShortestPath({}, {}, {{resourceProperty: 'minutes', maxResource: 10}}) \
         YIELD cost RETURN cost",
        x.as_u64(),
        y.as_u64()
    );
    let err = QueryEngine::new().execute(&q, &store).unwrap_err().to_string();
    assert!(err.contains("`minutes` holds a negative value"), "{err}");
}

/// How an application calls it: ids and the config as parameters. A
/// parameter inside the map is not read, and the refusal says what to do.
#[test]
fn ids_and_the_whole_config_can_be_parameters() {
    let m = map();
    let mut config = std::collections::HashMap::new();
    config.insert("resourceProperty".to_string(), PropertyValue::String("minutes".into()));
    config.insert("costProperty".to_string(), PropertyValue::String("cost".into()));
    config.insert("edgeType".to_string(), PropertyValue::String("ROAD".into()));
    config.insert("maxResource".to_string(), PropertyValue::Float(19.5));
    let mut params = BoundParams::new();
    params.insert("source".into(), PropertyValue::Integer(m.s as i64));
    params.insert("target".into(), PropertyValue::Integer(m.t as i64));
    params.insert("config".into(), PropertyValue::Map(config));
    params.insert("limit".into(), PropertyValue::Float(19.5));
    let engine = QueryEngine::new();

    let q = "CALL algo.or.constrainedShortestPath($source, $target, $config) YIELD cost RETURN cost";
    let batch = engine.execute_with_params(q, &m.store, &params).unwrap();
    assert_eq!(batch.records[0].get("cost"), Some(&Value::Property(PropertyValue::Float(10.0))));

    let q = "CALL algo.or.constrainedShortestPath($source, $target, \
             {resourceProperty: 'minutes', maxResource: $limit}) YIELD cost RETURN cost";
    let err = engine.execute_with_params(q, &m.store, &params).unwrap_err().to_string();
    assert!(err.contains("pass the whole map as one parameter"), "{err}");
}
