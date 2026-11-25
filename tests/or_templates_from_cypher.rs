//! OPT-06 (#1770): the graph-native optimisation templates, called from
//! Cypher, each on a small instance whose answer is worked out by hand in
//! the comments. The solvers themselves are cross-checked against brute force
//! in `samyama_graph_algorithms::or_templates`; this file checks what a caller
//! sees: the problem read from the graph, one row back, the envelope, and the
//! refusals.

use samyama::graph::{EdgeType, GraphStore, NodeId, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;
use std::collections::HashMap;

const TENANT: &str = "default";

fn node(g: &mut GraphStore, label: &str, props: &[(&str, PropertyValue)]) -> NodeId {
    let id = g.create_node(label);
    for (k, v) in props {
        g.set_node_property(TENANT, id, *k, v.clone()).unwrap();
    }
    id
}

fn edge(g: &mut GraphStore, a: NodeId, b: NodeId, kind: &str, props: &[(&str, f64)]) {
    let e = g.create_edge(a, b, EdgeType::new(kind)).unwrap();
    for (k, v) in props {
        g.set_edge_property_sparse(e, *k, PropertyValue::Float(*v));
    }
}

fn f(v: f64) -> PropertyValue {
    PropertyValue::Float(v)
}

/// The single row a template returns, as column -> value.
fn call(
    g: &GraphStore,
    name: &str,
    config: &str,
    columns: &[&str],
) -> Result<HashMap<String, Value>, String> {
    let q = format!(
        "CALL {name}({config}) YIELD {cols} RETURN {cols}",
        cols = columns.join(", ")
    );
    let batch = QueryEngine::new()
        .execute(&q, g)
        .map_err(|e| e.to_string())?;
    assert_eq!(
        batch.records.len(),
        1,
        "one row, always: {:?}",
        batch.records
    );
    let r = &batch.records[0];
    Ok(columns
        .iter()
        .map(|c| (c.to_string(), r.get(c).cloned().unwrap_or(Value::Null)))
        .collect())
}

const ENVELOPE: [&str; 5] = [
    "objective",
    "feasible",
    "gap",
    "iterations",
    "total_time_ms",
];

fn cols(answer: &[&'static str]) -> Vec<&'static str> {
    answer.iter().copied().chain(ENVELOPE).collect()
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Property(PropertyValue::Float(x)) => Some(*x),
        Value::Property(PropertyValue::Integer(x)) => Some(*x as f64),
        _ => None,
    }
}

fn boolean(v: &Value) -> bool {
    matches!(v, Value::Property(PropertyValue::Boolean(true)))
}

fn ids(v: &Value) -> Vec<u64> {
    match v {
        Value::Property(PropertyValue::Array(a)) => a
            .iter()
            .map(|x| match x {
                PropertyValue::Integer(i) => *i as u64,
                other => panic!("an id list holds {other:?}"),
            })
            .collect(),
        other => panic!("not a list: {other:?}"),
    }
}

fn maps(v: &Value) -> Vec<HashMap<String, PropertyValue>> {
    match v {
        Value::Property(PropertyValue::Array(a)) => a
            .iter()
            .map(|x| match x {
                PropertyValue::Map(m) => m.clone(),
                other => panic!("not a map: {other:?}"),
            })
            .collect(),
        other => panic!("not a list: {other:?}"),
    }
}

fn int(m: &HashMap<String, PropertyValue>, k: &str) -> u64 {
    match m.get(k) {
        Some(PropertyValue::Integer(i)) => *i as u64,
        other => panic!("{k} is {other:?}"),
    }
}

/// Every row's envelope: feasible and proven optimal, objective as given.
fn proven(row: &HashMap<String, Value>, objective: f64) {
    assert!(boolean(&row["feasible"]), "{row:?}");
    assert_eq!(num(&row["objective"]), Some(objective), "{row:?}");
    assert_eq!(num(&row["gap"]), Some(0.0), "proven optimal: gap 0");
    assert!(num(&row["iterations"]).unwrap() >= 1.0);
    assert!(num(&row["total_time_ms"]).unwrap() >= 0.0);
}

// ─────────────────────────────────────────────────────── facility location

struct Region {
    g: GraphStore,
    north: NodeId,
    centre: NodeId,
    south: NodeId,
    n1: NodeId,
}

/// Three candidate warehouses and four stores:
///
/// | from \ to | n1 | n2 | s1 | s2 |
/// |---|---|---|---|---|
/// | north  | 1  | 2  | 10 | 11 |
/// | centre | 5  | 5  | 5  | 5  |
/// | south  | 10 | 11 | 1  | 2  |
///
/// `north -> n1` goes through a depot (0.5 + 0.5): distance is a shortest
/// path, not a single relationship.
fn region() -> Region {
    let mut g = GraphStore::new();
    let north = node(&mut g, "Warehouse", &[("name", "north".into())]);
    let centre = node(&mut g, "Warehouse", &[("name", "centre".into())]);
    let south = node(&mut g, "Warehouse", &[("name", "south".into())]);
    let depot = node(&mut g, "Depot", &[]);
    let n1 = node(&mut g, "Store", &[("demand", f(1.0))]);
    let n2 = node(&mut g, "Store", &[]);
    let s1 = node(&mut g, "Store", &[("demand", f(1.0))]);
    let s2 = node(&mut g, "Store", &[]);
    edge(&mut g, north, depot, "ROAD", &[("km", 0.5)]);
    edge(&mut g, depot, n1, "ROAD", &[("km", 0.5)]);
    for (w, row) in [
        (north, [None, Some(2.0), Some(10.0), Some(11.0)]),
        (centre, [Some(5.0); 4]),
        (south, [Some(10.0), Some(11.0), Some(1.0), Some(2.0)]),
    ] {
        for (store, km) in [n1, n2, s1, s2].into_iter().zip(row) {
            if let Some(km) = km {
                edge(&mut g, w, store, "ROAD", &[("km", km)]);
            }
        }
    }
    Region {
        g,
        north,
        centre,
        south,
        n1,
    }
}

const FL: [&str; 3] = ["facilities", "assignments", "unserved"];

#[test]
fn facility_location_opens_the_sites_a_hand_calculation_picks() {
    let r = region();
    let ask = |p: i64, extra: &str| {
        call(
            &r.g,
            "algo.or.facilityLocation",
            &format!("{{facilityLabel: 'Warehouse', customerLabel: 'Store', p: {p}, distanceProperty: 'km'{extra}}}"),
            &cols(&FL),
        )
        .unwrap()
    };
    // p = 1: centre costs 20; north and south 24 each.
    let one = ask(1, "");
    assert_eq!(ids(&one["facilities"]), vec![r.centre.as_u64()]);
    proven(&one, 20.0);
    // p = 2: north + south, 1 + 2 + 1 + 2 = 6.
    let two = ask(2, "");
    assert_eq!(
        ids(&two["facilities"]),
        vec![r.north.as_u64(), r.south.as_u64()]
    );
    proven(&two, 6.0);
    let a = maps(&two["assignments"]);
    assert_eq!(a.len(), 4);
    let n1 = a
        .iter()
        .find(|m| int(m, "customer") == r.n1.as_u64())
        .unwrap();
    assert_eq!(int(n1, "facility"), r.north.as_u64());
    assert_eq!(
        n1.get("distance"),
        Some(&f(1.0)),
        "through the depot: a path, not one relationship"
    );
    assert!(ids(&two["unserved"]).is_empty());
    // Demand weights: n1 wants 30. p = 1: north = 30 + 2 + 10 + 11 = 53,
    // centre = 150 + 15 = 165, south = 300 + 11 + 1 + 2 = 314.
    let mut heavy = region();
    heavy
        .g
        .set_node_property(TENANT, heavy.n1, "demand", f(30.0))
        .unwrap();
    let weighted = call(
        &heavy.g,
        "algo.or.pMedian",
        "{facilityLabel: 'Warehouse', customerLabel: 'Store', p: 1, distanceProperty: 'km', demandProperty: 'demand'}",
        &cols(&FL),
    )
    .unwrap();
    assert_eq!(ids(&weighted["facilities"]), vec![heavy.north.as_u64()]);
    proven(&weighted, 53.0);
}

#[test]
fn facility_location_says_when_a_customer_cannot_be_reached() {
    let mut r = region();
    let island = node(&mut r.g, "Store", &[]);
    let row = call(
        &r.g,
        "algo.or.facilityLocation",
        "{facilityLabel: 'Warehouse', customerLabel: 'Store', p: 1, distanceProperty: 'km'}",
        &cols(&FL),
    )
    .unwrap();
    assert!(!boolean(&row["feasible"]));
    assert_eq!(row["objective"], Value::Property(PropertyValue::Null));
    assert_eq!(row["gap"], Value::Property(PropertyValue::Null));
    assert_eq!(ids(&row["unserved"]), vec![island.as_u64()]);
    assert_eq!(ids(&row["facilities"]).len(), 1, "the rest is still served");
}

// ───────────────────────────────────────────────────────────── set cover

/// Patches and the vulnerabilities each fixes:
///
/// | patch | hours | fixes |
/// |---|---|---|
/// | big   | 2.5 | v1 v2 v3 |
/// | a     | 1   | v1 |
/// | b     | 1   | v2 |
/// | c     | 1   | v3 |
/// | other | 5   | v4 |
///
/// Cheapest cover: big + other = 7.5 (a + b + c + other = 8).
fn patches() -> (GraphStore, [NodeId; 5], [NodeId; 4]) {
    let mut g = GraphStore::new();
    let v: [NodeId; 4] = std::array::from_fn(|_| node(&mut g, "Vulnerability", &[]));
    let patch = |g: &mut GraphStore, hours: f64, fixes: &[usize]| {
        let p = node(g, "Patch", &[("hours", f(hours))]);
        for &i in fixes {
            edge(g, p, v[i], "FIXES", &[]);
        }
        p
    };
    let big = patch(&mut g, 2.5, &[0, 1, 2]);
    let a = patch(&mut g, 1.0, &[0]);
    let b = patch(&mut g, 1.0, &[1]);
    let c = patch(&mut g, 1.0, &[2]);
    let other = patch(&mut g, 5.0, &[3]);
    (g, [big, a, b, c, other], v)
}

const SC: [&str; 3] = ["chosen", "cost", "uncoverable"];

#[test]
fn set_cover_picks_the_cheapest_patches_that_fix_everything() {
    let (g, [big, _, _, _, other], _) = patches();
    let row = call(&g, "algo.or.setCover", "{setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours', edgeType: 'FIXES'}", &cols(&SC)).unwrap();
    assert_eq!(ids(&row["chosen"]), vec![big.as_u64(), other.as_u64()]);
    proven(&row, 7.5);
    // Without a cost, the fewest patches: still big + other, 2 sets.
    let row = call(
        &g,
        "algo.or.setCover",
        "{setLabel: 'Patch', elementLabel: 'Vulnerability'}",
        &cols(&SC),
    )
    .unwrap();
    proven(&row, 2.0);
}

#[test]
fn set_cover_names_what_nothing_can_fix() {
    let (mut g, _, _) = patches();
    let orphan = node(&mut g, "Vulnerability", &[]);
    let row = call(
        &g,
        "algo.or.setCover",
        "{setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours'}",
        &cols(&SC),
    )
    .unwrap();
    assert!(!boolean(&row["feasible"]));
    assert_eq!(row["objective"], Value::Property(PropertyValue::Null));
    assert_eq!(ids(&row["uncoverable"]), vec![orphan.as_u64()]);
    assert_eq!(
        num(&row["cost"]),
        Some(7.5),
        "the rest is still covered, at least cost"
    );
}

// ─────────────────────────────────────────────────────── max coverage

/// Towers and the villages each reaches:
///
/// | tower | cost | reaches |
/// |---|---|---|
/// | t1 | 2 | A (100), B (50) |
/// | t2 | 1 | C (80) |
/// | t3 | 1 | B (50), D (60) |
///
/// Budget 2: t2 + t3 = 190 (t1 alone 150). Budget 3: t1 + t2 = 230.
/// Budget 4: all, 290.
fn towers() -> (GraphStore, [NodeId; 3]) {
    let mut g = GraphStore::new();
    let [a, b, c, d] =
        [100.0, 50.0, 80.0, 60.0].map(|p| node(&mut g, "Village", &[("population", f(p))]));
    let t1 = node(&mut g, "Tower", &[("cost", f(2.0))]);
    let t2 = node(&mut g, "Tower", &[("cost", f(1.0))]);
    let t3 = node(&mut g, "Tower", &[("cost", f(1.0))]);
    for (t, vs) in [(t1, vec![a, b]), (t2, vec![c]), (t3, vec![b, d])] {
        for v in vs {
            edge(&mut g, t, v, "REACHES", &[]);
        }
    }
    (g, [t1, t2, t3])
}

#[test]
fn max_coverage_spends_the_budget_where_it_reaches_most() {
    let (g, [t1, t2, t3]) = towers();
    let ask = |budget: i64| {
        call(
            &g,
            "algo.or.maxCoverage",
            &format!("{{setLabel: 'Tower', elementLabel: 'Village', budget: {budget}, costProperty: 'cost', weightProperty: 'population'}}"),
            &cols(&["chosen", "covered", "cost"]),
        )
        .unwrap()
    };
    let two = ask(2);
    assert_eq!(ids(&two["chosen"]), vec![t2.as_u64(), t3.as_u64()]);
    assert_eq!(ids(&two["covered"]).len(), 3);
    proven(&two, 190.0);
    let three = ask(3);
    assert_eq!(ids(&three["chosen"]), vec![t1.as_u64(), t2.as_u64()]);
    proven(&three, 230.0);
    proven(&ask(4), 290.0);
    // Nothing affordable is still an answer: cover nothing, feasibly.
    let none = ask(0);
    assert!(ids(&none["chosen"]).is_empty());
    proven(&none, 0.0);
}

// ─────────────────────────────────────────────────────────── assignment

/// Hours each engineer needs for each ticket:
///
/// | | k1 | k2 | k3 |
/// |---|---|---|---|
/// | e1 | 4 | 1 | 3 |
/// | e2 | 2 | 0 | 5 |
/// | e3 | 3 | 2 | 2 |
///
/// Best: e1-k2 (1) + e2-k1 (2) + e3-k3 (2) = 5. Next best is 6.
fn tickets(skip: Option<(usize, usize)>) -> (GraphStore, [NodeId; 3], [NodeId; 3]) {
    let mut g = GraphStore::new();
    let e: [NodeId; 3] = std::array::from_fn(|_| node(&mut g, "Engineer", &[]));
    let k: [NodeId; 3] = std::array::from_fn(|_| node(&mut g, "Ticket", &[]));
    let hours = [[4.0, 1.0, 3.0], [2.0, 0.0, 5.0], [3.0, 2.0, 2.0]];
    for i in 0..3 {
        for j in 0..3 {
            if skip.is_some_and(|(a, b)| a == i && b != j) {
                continue;
            }
            edge(&mut g, e[i], k[j], "CAN_TAKE", &[("hours", hours[i][j])]);
        }
    }
    (g, e, k)
}

#[test]
fn assignment_matches_the_hand_worked_optimum() {
    let (g, e, k) = tickets(None);
    let row = call(
        &g,
        "algo.or.assignment",
        "{workerLabel: 'Engineer', taskLabel: 'Ticket', costProperty: 'hours'}",
        &cols(&["assignments", "cost"]),
    )
    .unwrap();
    proven(&row, 5.0);
    let pairs: Vec<(u64, u64)> = maps(&row["assignments"])
        .iter()
        .map(|m| (int(m, "worker"), int(m, "task")))
        .collect();
    assert_eq!(
        pairs,
        vec![
            (e[0].as_u64(), k[1].as_u64()),
            (e[1].as_u64(), k[0].as_u64()),
            (e[2].as_u64(), k[2].as_u64())
        ]
    );
}

#[test]
fn assignment_never_uses_a_pair_with_no_relationship() {
    // e1 can take only k2, and e2 only k2 as well: one of them goes without.
    let mut g = GraphStore::new();
    let e1 = node(&mut g, "Engineer", &[]);
    let e2 = node(&mut g, "Engineer", &[]);
    let k1 = node(&mut g, "Ticket", &[]);
    let k2 = node(&mut g, "Ticket", &[]);
    edge(&mut g, e1, k2, "CAN_TAKE", &[("hours", 1.0)]);
    edge(&mut g, e2, k2, "CAN_TAKE", &[("hours", 3.0)]);
    let _ = k1;
    let row = call(
        &g,
        "algo.or.assignment",
        "{workerLabel: 'Engineer', taskLabel: 'Ticket', costProperty: 'hours'}",
        &cols(&["assignments", "cost"]),
    )
    .unwrap();
    assert!(
        !boolean(&row["feasible"]),
        "two engineers, one ticket either can take"
    );
    let pairs = maps(&row["assignments"]);
    assert_eq!(pairs.len(), 1);
    assert_eq!(
        (int(&pairs[0], "worker"), int(&pairs[0], "task")),
        (e1.as_u64(), k2.as_u64()),
        "the cheaper of the two"
    );
    assert_eq!(row["objective"], Value::Property(PropertyValue::Null));
}

// ─────────────────────────────────────────────────────── min-cost flow

/// A factory supplies 4 to a shop: via `a` (1 + 1 per unit, at most 2) or
/// via `b` (3 + 3, at most 5). Cheapest: 2 via a (4) + 2 via b (12) = 16.
fn network(supply: f64) -> (GraphStore, [NodeId; 4]) {
    let mut g = GraphStore::new();
    let factory = node(&mut g, "Site", &[("supply", f(supply))]);
    let a = node(&mut g, "Site", &[]);
    let b = node(&mut g, "Site", &[]);
    let shop = node(&mut g, "Site", &[("supply", f(-supply))]);
    edge(
        &mut g,
        factory,
        a,
        "LANE",
        &[("capacity", 2.0), ("cost", 1.0)],
    );
    edge(&mut g, a, shop, "LANE", &[("capacity", 2.0), ("cost", 1.0)]);
    edge(
        &mut g,
        factory,
        b,
        "LANE",
        &[("capacity", 5.0), ("cost", 3.0)],
    );
    edge(&mut g, b, shop, "LANE", &[("capacity", 5.0), ("cost", 3.0)]);
    (g, [factory, a, b, shop])
}

const MCF: &str =
    "{supplyProperty: 'supply', capacityProperty: 'capacity', costProperty: 'cost', label: 'Site'}";

#[test]
fn min_cost_flow_routes_supply_at_least_cost() {
    let (g, [factory, a, _, _]) = network(4.0);
    let row = call(
        &g,
        "algo.or.minCostFlow",
        MCF,
        &cols(&["flows", "shipped", "cost"]),
    )
    .unwrap();
    proven(&row, 16.0);
    assert_eq!(num(&row["shipped"]), Some(4.0));
    let flows = maps(&row["flows"]);
    assert_eq!(flows.len(), 4);
    let fa = flows
        .iter()
        .find(|m| int(m, "source") == factory.as_u64() && int(m, "target") == a.as_u64())
        .unwrap();
    assert_eq!(
        fa.get("flow"),
        Some(&f(2.0)),
        "the cheap lane is filled to its capacity"
    );
}

#[test]
fn min_cost_flow_says_when_capacity_runs_out() {
    let (g, _) = network(9.0);
    let row = call(
        &g,
        "algo.or.minCostFlow",
        MCF,
        &cols(&["flows", "shipped", "cost"]),
    )
    .unwrap();
    assert!(!boolean(&row["feasible"]));
    assert_eq!(num(&row["shipped"]), Some(7.0), "everything that fits");
    assert_eq!(row["objective"], Value::Property(PropertyValue::Null));
}

// ─────────────────────────────────────────────────────────── refusals

#[test]
fn every_template_refuses_what_it_cannot_honour() {
    let (g, _, _) = patches();
    let err = |q: &str| {
        QueryEngine::new()
            .execute(q, &g)
            .map(|_| ())
            .unwrap_err()
            .to_string()
    };
    let e = err("CALL algo.or.setCover({setLabel: 'Patch', elementLabel: 'Vulnerability', costPropety: 'hours'}) YIELD cost RETURN cost");
    assert!(e.contains("unknown config key `costPropety`"), "{e}");
    let e = err("CALL algo.or.setCover({elementLabel: 'Vulnerability'}) YIELD cost RETURN cost");
    assert!(e.contains("`setLabel` is required"), "{e}");
    let e = err("CALL algo.or.setCover({setLabel: 'Nope', elementLabel: 'Vulnerability'}) YIELD cost RETURN cost");
    assert!(e.contains("no node has the label `Nope`"), "{e}");
    let e = err("CALL algo.or.setCover({setLabel: 'Patch', elementLabel: 'Vulnerability', graph: 'p'}) YIELD cost RETURN cost");
    assert!(e.contains("cannot run on a graph projection"), "{e}");
    let e = err("CALL algo.or.facilityLocation({facilityLabel: 'Patch', customerLabel: 'Vulnerability', p: 0}) YIELD objective RETURN objective");
    assert!(e.contains("`p` must be a whole number >= 1"), "{e}");
    let e = err("CALL algo.or.maxCoverage({setLabel: 'Patch', elementLabel: 'Vulnerability'}) YIELD objective RETURN objective");
    assert!(e.contains("`budget` is required"), "{e}");
    let e = err("CALL algo.or.setCover('Patch') YIELD cost RETURN cost");
    assert!(e.contains("takes one map of settings"), "{e}");

    // A value that is present and not a number is refused, naming the node.
    let (mut g2, [big, ..], _) = patches();
    g2.set_node_property(TENANT, big, "hours", PropertyValue::String("lots".into()))
        .unwrap();
    let e = QueryEngine::new()
        .execute("CALL algo.or.setCover({setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours'}) YIELD cost RETURN cost", &g2)
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(
        e.contains(&format!("node {} has `hours`", big.as_u64())) && e.contains("not a number"),
        "{e}"
    );
    // A negative cost is refused by the solver, by name.
    let (mut g3, [big, ..], _) = patches();
    g3.set_node_property(TENANT, big, "hours", f(-1.0)).unwrap();
    let e = QueryEngine::new()
        .execute("CALL algo.or.setCover({setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours'}) YIELD cost RETURN cost", &g3)
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(e.contains("set cost of -1"), "{e}");
}

#[test]
fn the_six_templates_are_in_the_catalog() {
    let e = QueryEngine::new()
        .execute("CALL algo.nope()", &GraphStore::new())
        .map(|_| ())
        .unwrap_err()
        .to_string();
    for name in [
        "or.constrainedShortestPath",
        "or.facilityLocation",
        "or.setCover",
        "or.maxCoverage",
        "or.assignment",
        "or.minCostFlow",
    ] {
        assert!(e.contains(name), "{name} missing from: {e}");
    }
}

#[test]
fn a_heuristic_answer_reports_an_unknown_gap_not_zero() {
    // 30 candidates, p = 8: 5.85 million choices, above the enumeration limit.
    let mut g = GraphStore::new();
    let sites: Vec<NodeId> = (0..30).map(|_| node(&mut g, "Site", &[])).collect();
    let towns: Vec<NodeId> = (0..30).map(|_| node(&mut g, "Town", &[])).collect();
    for (i, &s) in sites.iter().enumerate() {
        for (j, &t) in towns.iter().enumerate() {
            edge(&mut g, s, t, "ROAD", &[("km", (i as f64 - j as f64).abs())]);
        }
    }
    let row = call(
        &g,
        "algo.or.facilityLocation",
        "{facilityLabel: 'Site', customerLabel: 'Town', p: 8, distanceProperty: 'km'}",
        &cols(&FL),
    )
    .unwrap();
    assert!(boolean(&row["feasible"]));
    assert!(num(&row["objective"]).is_some());
    assert_eq!(
        row["gap"],
        Value::Property(PropertyValue::Null),
        "not proven optimal: the gap is unknown, not 0"
    );
    assert_eq!(ids(&row["facilities"]).len(), 8);
}

#[test]
fn edge_type_limits_which_relationships_count() {
    // Patch `a` also MENTIONS v4. Counted, `a` covers v1 and v4, and the
    // cheapest cover becomes a + b + c = 3 (big + a would be 3.5); as a
    // FIXES-only cover it stays big + other = 7.5.
    let (mut g, [big, a, b, c, other], v) = patches();
    edge(&mut g, a, v[3], "MENTIONS", &[]);
    let fixes = call(&g, "algo.or.setCover", "{setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours', edgeType: 'FIXES'}", &cols(&SC)).unwrap();
    assert_eq!(ids(&fixes["chosen"]), vec![big.as_u64(), other.as_u64()]);
    let any = call(
        &g,
        "algo.or.setCover",
        "{setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours'}",
        &cols(&SC),
    )
    .unwrap();
    assert_eq!(
        ids(&any["chosen"]),
        vec![a.as_u64(), b.as_u64(), c.as_u64()]
    );
    proven(&any, 3.0);
}

#[test]
fn of_parallel_relationships_the_cheapest_counts() {
    // One engineer, two tickets: k1 by a 1-hour or a 9-hour relationship,
    // k2 by a 5-hour one. The answer is k1 at 1 hour, whichever of the two
    // parallel relationships was made first.
    for cheap_first in [true, false] {
        let mut g = GraphStore::new();
        let e = node(&mut g, "Engineer", &[]);
        let k1 = node(&mut g, "Ticket", &[]);
        let k2 = node(&mut g, "Ticket", &[]);
        let hours = if cheap_first { [1.0, 9.0] } else { [9.0, 1.0] };
        for h in hours {
            edge(&mut g, e, k1, "CAN_TAKE", &[("hours", h)]);
        }
        edge(&mut g, e, k2, "CAN_TAKE", &[("hours", 5.0)]);
        let row = call(
            &g,
            "algo.or.assignment",
            "{workerLabel: 'Engineer', taskLabel: 'Ticket', costProperty: 'hours'}",
            &cols(&["assignments", "cost"]),
        )
        .unwrap();
        proven(&row, 1.0);
        assert_eq!(
            int(&maps(&row["assignments"])[0], "task"),
            k1.as_u64(),
            "cheap_first = {cheap_first}"
        );
    }
}
