//! Additional unit tests for `Value` equality/hashing, property resolution, the
//! property cursor, temporal components and Cypher orderability.

use super::*;
use crate::graph::EdgeType;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};

fn h(v: &Value) -> u64 {
    let mut s = DefaultHasher::new();
    v.hash(&mut s);
    s.finish()
}

fn p(v: PropertyValue) -> Value {
    Value::Property(v)
}

fn int(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

fn s(x: &str) -> PropertyValue {
    PropertyValue::String(x.to_string())
}

/// Two `Person` nodes and a `KNOWS` edge between them.
fn store_with_edge() -> (GraphStore, NodeId, NodeId, EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    (store, a, b, e)
}

fn edge_ref(store: &GraphStore, e: EdgeId) -> Value {
    let (src, tgt) = store.get_edge_endpoints(e).unwrap();
    Value::EdgeRef(e, src, tgt, EdgeType::new("KNOWS"))
}

// ---------------------------------------------------------------------------
// PartialEq / Hash
// ---------------------------------------------------------------------------

#[test]
fn edge_and_edge_ref_with_the_same_id_are_equal_and_hash_alike() {
    let (store, _, _, e) = store_with_edge();
    let full = Value::Edge(e, Box::new(store.get_edge(e).unwrap()));
    let lazy = edge_ref(&store, e);
    assert_eq!(full, lazy);
    assert_eq!(lazy, full);
    assert_eq!(full, full.clone());
    assert_eq!(h(&full), h(&lazy));
    assert_ne!(lazy, Value::NodeRef(NodeId::new(e.as_u64())));
    assert_eq!(lazy, lazy.clone());
    assert!(lazy.as_node().is_none());
    assert!(lazy.as_edge().is_none());
    assert!(full.as_edge().is_some());
}

#[test]
fn a_record_remembers_the_edges_it_traversed() {
    let mut r = Record::new();
    assert!(!r.edge_used(EdgeId::new(4)));
    r.mark_edge_used(EdgeId::new(4));
    assert!(r.edge_used(EdgeId::new(4)));
    assert!(!r.edge_used(EdgeId::new(5)));
    let copy = r.clone_with_capacity(2);
    assert!(copy.edge_used(EdgeId::new(4)));
}

#[test]
fn node_and_node_ref_compare_by_id() {
    let node = Node::new(NodeId::new(3), crate::graph::Label::new("X"));
    let full = Value::Node(NodeId::new(3), Box::new(node));
    assert_eq!(full, full.clone());
    assert_eq!(Value::NodeRef(NodeId::new(3)), full);
    assert_ne!(Value::NodeRef(NodeId::new(4)), full);
}

#[test]
fn paths_compare_by_nodes_and_edges() {
    let a = Value::Path {
        nodes: vec![NodeId::new(1), NodeId::new(2)],
        edges: vec![EdgeId::new(9)],
    };
    let b = a.clone();
    let c = Value::Path {
        nodes: vec![NodeId::new(1), NodeId::new(2)],
        edges: vec![EdgeId::new(8)],
    };
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_eq!(h(&a), h(&b));
    assert_eq!(Value::Null, Value::Null);
    assert_ne!(Value::Null, p(PropertyValue::Null));
}

#[test]
fn a_list_equals_the_array_spelling_of_itself_and_hashes_alike() {
    let list = Value::List(vec![p(int(1)), p(s("x"))]);
    let array = p(PropertyValue::Array(vec![int(1), s("x")]));
    assert_eq!(list, array);
    assert_eq!(array, list);
    assert_eq!(h(&list), h(&array));
    assert_ne!(list, p(PropertyValue::Array(vec![int(1)])));
    assert_ne!(list, p(PropertyValue::Array(vec![int(1), s("y")])));
    assert_eq!(Value::List(vec![]), Value::List(vec![]));
}

#[test]
fn a_map_equals_the_property_map_spelling_of_itself_and_hashes_alike() {
    let mut vm = BTreeMap::new();
    vm.insert("a".to_string(), p(int(1)));
    vm.insert("b".to_string(), p(s("z")));
    let vmap = Value::Map(vm.clone());
    let mut pm = HashMap::new();
    pm.insert("b".to_string(), s("z"));
    pm.insert("a".to_string(), int(1));
    let pmap = p(PropertyValue::Map(pm.clone()));
    assert_eq!(vmap, pmap);
    assert_eq!(pmap, vmap);
    assert_eq!(h(&vmap), h(&pmap));
    assert_eq!(vmap, Value::Map(vm));
    // A differing value or key breaks the equality.
    let mut other = pm.clone();
    other.insert("a".to_string(), int(2));
    assert_ne!(vmap, p(PropertyValue::Map(other)));
    let mut missing = pm;
    missing.remove("a");
    missing.insert("c".to_string(), int(1));
    assert_ne!(vmap, p(PropertyValue::Map(missing)));
}

#[test]
fn mismatched_kinds_are_never_equal() {
    assert_ne!(p(int(1)), Value::List(vec![p(int(1))]));
    assert_ne!(Value::Null, Value::NodeRef(NodeId::new(1)));
    assert_ne!(h(&Value::Null), h(&p(int(0))));
}

// ---------------------------------------------------------------------------
// resolve_property
// ---------------------------------------------------------------------------

#[test]
fn node_ref_falls_back_to_row_storage_and_missing_nodes_are_null() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    store
        .get_node_mut(n)
        .unwrap()
        .set_property("tags", PropertyValue::Array(vec![s("a")]));
    assert_eq!(
        Value::NodeRef(n).resolve_property("tags", &store),
        PropertyValue::Array(vec![s("a")])
    );
    assert_eq!(
        Value::NodeRef(n).resolve_property("absent", &store),
        PropertyValue::Null
    );
    assert_eq!(
        Value::NodeRef(NodeId::new(999)).resolve_property("tags", &store),
        PropertyValue::Null
    );
}

#[test]
fn materialized_edge_reads_column_then_its_own_map() {
    let (mut store, _, _, e) = store_with_edge();
    store.set_edge_property(e, "w", int(7)).unwrap();
    let mut edge = store.get_edge(e).unwrap();
    edge.properties.insert("only_here".to_string(), s("row"));
    let v = Value::Edge(e, Box::new(edge));
    assert_eq!(v.resolve_property("w", &store), int(7));
    assert_eq!(v.resolve_property("only_here", &store), s("row"));
    assert_eq!(v.resolve_property("nope", &store), PropertyValue::Null);
}

#[test]
fn edge_ref_reads_through_the_store() {
    let (mut store, _, _, e) = store_with_edge();
    store.set_edge_property(e, "since", int(2020)).unwrap();
    let v = edge_ref(&store, e);
    assert_eq!(v.resolve_property("since", &store), int(2020));
    assert_eq!(v.resolve_property("nope", &store), PropertyValue::Null);
}

#[test]
fn map_values_resolve_their_keys() {
    let mut m = HashMap::new();
    m.insert("a".to_string(), int(1));
    let v = p(PropertyValue::Map(m));
    let store = GraphStore::new();
    assert_eq!(v.resolve_property("a", &store), int(1));
    assert_eq!(v.resolve_property("b", &store), PropertyValue::Null);
}

#[test]
fn legacy_datetime_components() {
    let store = GraphStore::new();
    // 2021-03-04T05:06:07.089Z
    let millis = chrono::NaiveDate::from_ymd_opt(2021, 3, 4)
        .unwrap()
        .and_hms_milli_opt(5, 6, 7, 89)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let v = p(PropertyValue::DateTime(millis));
    let get = |k: &str| v.resolve_property(k, &store);
    assert_eq!(get("year"), int(2021));
    assert_eq!(get("month"), int(3));
    assert_eq!(get("day"), int(4));
    assert_eq!(get("hour"), int(5));
    assert_eq!(get("minute"), int(6));
    assert_eq!(get("second"), int(7));
    assert_eq!(get("millisecond"), int(89));
    assert_eq!(get("epochMillis"), int(millis));
    assert_eq!(get("week"), PropertyValue::Null);
    // Out of chrono's range: no components at all.
    assert_eq!(
        p(PropertyValue::DateTime(i64::MAX)).resolve_property("year", &store),
        PropertyValue::Null
    );
}

#[test]
fn duration_totals_and_remainders() {
    let store = GraphStore::new();
    // P1Y5M17DT1H1M1.5S
    let d = p(PropertyValue::Duration {
        months: 17,
        days: 17,
        seconds: 3661,
        nanos: 500_000_000,
    });
    let get = |k: &str| d.resolve_property(k, &store);
    assert_eq!(get("years"), int(1));
    assert_eq!(get("quarters"), int(5));
    assert_eq!(get("months"), int(17));
    assert_eq!(get("weeks"), int(2));
    assert_eq!(get("days"), int(17));
    assert_eq!(get("quartersOfYear"), int(1));
    assert_eq!(get("monthsOfQuarter"), int(2));
    assert_eq!(get("monthsOfYear"), int(5));
    assert_eq!(get("daysOfWeek"), int(3));
    assert_eq!(get("hours"), int(1));
    assert_eq!(get("minutes"), int(61));
    assert_eq!(get("seconds"), int(3661));
    assert_eq!(get("milliseconds"), int(3_661_500));
    assert_eq!(get("microseconds"), int(3_661_500_000));
    assert_eq!(get("nanoseconds"), int(3_661_500_000_000));
    assert_eq!(get("minutesOfHour"), int(1));
    assert_eq!(get("secondsOfMinute"), int(1));
    assert_eq!(get("millisecondsOfSecond"), int(500));
    assert_eq!(get("microsecondsOfSecond"), int(500_000));
    assert_eq!(get("nanosecondsOfSecond"), int(500_000_000));
    assert_eq!(get("fortnights"), PropertyValue::Null);
}

#[test]
fn negative_duration_normalises_the_sub_second_remainder() {
    let store = GraphStore::new();
    let d = p(PropertyValue::Duration {
        months: 0,
        days: 0,
        seconds: -86399,
        nanos: -900_000_000,
    });
    assert_eq!(d.resolve_property("seconds", &store), int(-86400));
    assert_eq!(
        d.resolve_property("nanosecondsOfSecond", &store),
        int(100_000_000)
    );
}

#[test]
fn non_entity_values_have_no_properties() {
    let store = GraphStore::new();
    assert_eq!(p(int(3)).resolve_property("x", &store), PropertyValue::Null);
    assert_eq!(
        Value::Null.resolve_property("x", &store),
        PropertyValue::Null
    );
    assert_eq!(
        Value::List(vec![]).resolve_property("x", &store),
        PropertyValue::Null
    );
}

// ---------------------------------------------------------------------------
// approx_heap_bytes / RecordBatch
// ---------------------------------------------------------------------------

#[test]
fn heap_bytes_walk_the_value() {
    let (store, a, _, e) = store_with_edge();
    assert_eq!(Value::NodeRef(a).approx_heap_bytes(), 0);
    assert_eq!(Value::Null.approx_heap_bytes(), 0);
    let node = Value::Node(a, Box::new(store.get_node(a).unwrap().clone()));
    assert!(node.approx_heap_bytes() >= std::mem::size_of::<Node>());
    let edge = Value::Edge(e, Box::new(store.get_edge(e).unwrap()));
    assert!(edge.approx_heap_bytes() >= std::mem::size_of::<Edge>());
    let path = Value::Path {
        nodes: Vec::with_capacity(2),
        edges: Vec::with_capacity(1),
    };
    assert_eq!(
        path.approx_heap_bytes(),
        2 * std::mem::size_of::<NodeId>() + std::mem::size_of::<EdgeId>()
    );
    let list = Value::List(vec![Value::Null, Value::Null]);
    assert!(list.approx_heap_bytes() >= 2 * std::mem::size_of::<Value>());
    let mut m = BTreeMap::new();
    m.insert("k".to_string(), Value::Null);
    assert!(Value::Map(m).approx_heap_bytes() >= std::mem::size_of::<Value>());
    assert!(p(s("hello")).approx_heap_bytes() >= 5);
}

#[test]
fn record_batch_helpers() {
    let mut batch = RecordBatch::new(vec!["x".to_string()]);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert!(batch.get(0).is_none());
    let empty_bytes = batch.approx_heap_bytes();
    let mut r = Record::new();
    r.bind("x", p(s("some text")));
    assert!(r.approx_heap_bytes() > 0);
    batch.push(r);
    assert_eq!(batch.len(), 1);
    assert!(!batch.is_empty());
    assert_eq!(batch.get(0).unwrap().get("x"), Some(&p(s("some text"))));
    assert!(batch.approx_heap_bytes() > empty_bytes);
    assert_eq!(batch.plan_hash, None);
}

// ---------------------------------------------------------------------------
// PropertyCursor
// ---------------------------------------------------------------------------

#[test]
fn cursor_reads_node_columns_then_row_storage() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    store.set_column_property(a, "name", s("alice"));
    store
        .get_node_mut(b)
        .unwrap()
        .set_property("name", s("row-bob"));
    let mut cur = PropertyCursor::new("n", "name");
    let mut ra = Record::new();
    ra.bind("n", Value::NodeRef(a));
    let mut rb = Record::new();
    rb.bind("n", Value::NodeRef(b));
    assert_eq!(cur.read(&ra, &store), s("alice"));
    // Column cached, but b has no column value: fall back to its row map.
    assert_eq!(cur.read(&rb, &store), s("row-bob"));
    let mut gone = Record::new();
    gone.bind("n", Value::NodeRef(NodeId::new(12345)));
    assert_eq!(cur.read(&gone, &store), PropertyValue::Null);
}

#[test]
fn cursor_reads_edge_columns_then_row_storage() {
    let (mut store, a, b, e1) = store_with_edge();
    let e2 = store.create_edge(b, a, "KNOWS").unwrap();
    store.set_edge_property(e1, "w", int(5)).unwrap();
    store
        .get_edge_properties_mut(e2)
        .unwrap()
        .insert("w".to_string(), int(9));
    let mut cur = PropertyCursor::new("r", "w");
    let mut r1 = Record::new();
    r1.bind("r", edge_ref(&store, e1));
    let mut r2 = Record::new();
    r2.bind("r", Value::Edge(e2, Box::new(store.get_edge(e2).unwrap())));
    assert_eq!(cur.read(&r1, &store), int(5));
    assert_eq!(cur.read(&r2, &store), int(9));
    // A relationship that does not exist reads as null.
    let mut gone = Record::new();
    gone.bind(
        "r",
        Value::EdgeRef(EdgeId::new(777), a, b, EdgeType::new("KNOWS")),
    );
    assert_eq!(cur.read(&gone, &store), PropertyValue::Null);
}

#[test]
fn cursor_without_a_column_reads_row_storage_for_edges() {
    let (mut store, _, _, e) = store_with_edge();
    store
        .get_edge_properties_mut(e)
        .unwrap()
        .insert("tag".to_string(), s("t"));
    let mut cur = PropertyCursor::new("r", "tag");
    let mut r = Record::new();
    r.bind("r", edge_ref(&store, e));
    assert_eq!(cur.read(&r, &store), s("t"));
}

#[test]
fn cursor_delegates_non_entities_and_unbound_variables() {
    let store = GraphStore::new();
    let mut m = HashMap::new();
    m.insert("k".to_string(), int(4));
    let mut r = Record::new();
    r.bind("m", p(PropertyValue::Map(m)));
    assert_eq!(PropertyCursor::new("m", "k").read(&r, &store), int(4));
    assert_eq!(
        PropertyCursor::new("unbound", "k").read(&r, &store),
        PropertyValue::Null
    );
}

#[test]
fn read_str_borrows_string_columns_only() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    store.set_column_property(a, "name", s("alice"));
    store.set_column_property(a, "age", int(30));
    let mut r = Record::new();
    r.bind("n", Value::NodeRef(a));

    let mut name = PropertyCursor::new("n", "name");
    assert_eq!(name.read_str(&r, &store), Some("alice"));
    // Second call uses the cached column and string-ness.
    assert_eq!(name.read_str(&r, &store), Some("alice"));

    let mut age = PropertyCursor::new("n", "age");
    assert_eq!(age.read_str(&r, &store), None);
    // Now known not to be a string column: short-circuits.
    assert_eq!(age.read_str(&r, &store), None);

    let mut missing = PropertyCursor::new("n", "nope");
    assert_eq!(missing.read_str(&r, &store), None);

    let mut not_entity = Record::new();
    not_entity.bind("n", p(s("x")));
    assert_eq!(
        PropertyCursor::new("n", "name").read_str(&not_entity, &store),
        None
    );
}

#[test]
fn read_str_on_edges() {
    let (mut store, _, _, e) = store_with_edge();
    store.set_edge_property(e, "label", s("friend")).unwrap();
    store.set_edge_property(e, "w", int(1)).unwrap();
    let mut r = Record::new();
    r.bind("r", edge_ref(&store, e));
    let mut label = PropertyCursor::new("r", "label");
    assert_eq!(label.read_str(&r, &store), Some("friend"));
    let mut w = PropertyCursor::new("r", "w");
    assert_eq!(w.read_str(&r, &store), None);
    let mut none = PropertyCursor::new("r", "absent");
    assert_eq!(none.read_str(&r, &store), None);
    let mut materialized = Record::new();
    materialized.bind("r", Value::Edge(e, Box::new(store.get_edge(e).unwrap())));
    assert_eq!(label.read_str(&materialized, &store), Some("friend"));
}

// ---------------------------------------------------------------------------
// temporal_property / temporal_component
// ---------------------------------------------------------------------------

fn days(y: i32, m: u32, d: u32) -> i64 {
    chrono::NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days()
}

#[test]
fn date_components() {
    // 2017-11-11 is a Saturday in ISO week 45.
    let d = PropertyValue::Date(days(2017, 11, 11) as i32);
    let get = |k: &str| temporal_property(&d, k).unwrap();
    assert_eq!(get("year"), int(2017));
    assert_eq!(get("month"), int(11));
    assert_eq!(get("day"), int(11));
    assert_eq!(get("quarter"), int(4));
    assert_eq!(get("dayOfQuarter"), int(42));
    assert_eq!(get("week"), int(45));
    assert_eq!(get("weekYear"), int(2017));
    assert_eq!(get("weekDay"), int(6));
    assert_eq!(get("dayOfWeek"), int(6));
    assert_eq!(get("ordinalDay"), int(315));
    assert_eq!(get("epochMillis"), int(days(2017, 11, 11) * 86_400_000));
    assert_eq!(get("epochSeconds"), int(days(2017, 11, 11) * 86_400));
    // A date has no clock or offset.
    assert_eq!(get("hour"), PropertyValue::Null);
    assert_eq!(get("offset"), PropertyValue::Null);
    assert_eq!(get("timezone"), PropertyValue::Null);
    assert_eq!(get("bogus"), PropertyValue::Null);
}

#[test]
fn time_components() {
    let nanos = (12 * 3600 + 31 * 60 + 14) * 1_000_000_000 + 645_876_123;
    let t = PropertyValue::Time {
        nanos,
        offset_seconds: -5400,
    };
    let get = |k: &str| temporal_property(&t, k).unwrap();
    assert_eq!(get("hour"), int(12));
    assert_eq!(get("minute"), int(31));
    assert_eq!(get("second"), int(14));
    assert_eq!(get("millisecond"), int(645));
    assert_eq!(get("microsecond"), int(645_876));
    assert_eq!(get("nanosecond"), int(645_876_123));
    assert_eq!(get("offsetSeconds"), int(-5400));
    assert_eq!(get("offsetMinutes"), int(-90));
    assert_eq!(get("offset"), s("-01:30"));
    assert_eq!(get("timezone"), s("-01:30"));
    assert_eq!(get("year"), PropertyValue::Null);

    let lt = PropertyValue::LocalTime(nanos);
    assert_eq!(temporal_property(&lt, "minute").unwrap(), int(31));
    assert_eq!(
        temporal_property(&lt, "offset").unwrap(),
        PropertyValue::Null
    );
}

#[test]
fn datetime_components_use_local_wall_clock() {
    // 2017-11-11T00:30+02:00[Europe/Stockholm] is 2017-11-10T22:30Z.
    let local = days(2017, 11, 11) * 86_400 + 30 * 60;
    let z = PropertyValue::ZonedDateTime {
        secs: local - 7200,
        nanos: 0,
        offset_seconds: 7200,
        zone: Some("Europe/Stockholm".into()),
    };
    let get = |k: &str| temporal_property(&z, k).unwrap();
    assert_eq!(get("day"), int(11));
    assert_eq!(get("hour"), int(0));
    assert_eq!(get("minute"), int(30));
    assert_eq!(get("timezone"), s("Europe/Stockholm"));
    assert_eq!(get("offset"), s("+02:00"));
    assert_eq!(get("epochSeconds"), int(local - 7200));

    let ldt = PropertyValue::LocalDateTime {
        secs: local,
        nanos: 5,
    };
    assert_eq!(temporal_property(&ldt, "day").unwrap(), int(11));
    assert_eq!(temporal_property(&ldt, "nanosecond").unwrap(), int(5));
    assert_eq!(
        temporal_property(&ldt, "offsetSeconds").unwrap(),
        PropertyValue::Null
    );
}

#[test]
fn legacy_datetime_reads_as_utc_zoned() {
    let ms = days(2020, 2, 29) * 86_400_000 + 3_723_004; // 01:02:03.004
    let v = PropertyValue::DateTime(ms);
    assert_eq!(temporal_property(&v, "day").unwrap(), int(29));
    assert_eq!(temporal_property(&v, "second").unwrap(), int(3));
    assert_eq!(temporal_property(&v, "millisecond").unwrap(), int(4));
    assert_eq!(temporal_property(&v, "offset").unwrap(), s("Z"));
}

#[test]
fn non_temporal_values_have_no_components() {
    assert_eq!(temporal_property(&int(1), "year"), None);
    assert_eq!(temporal_property(&PropertyValue::Null, "year"), None);
}

#[test]
fn out_of_range_date_components_are_null() {
    let d = PropertyValue::Date(i32::MAX);
    assert_eq!(temporal_property(&d, "year").unwrap(), PropertyValue::Null);
    assert_eq!(
        temporal_property(&d, "dayOfQuarter").unwrap(),
        PropertyValue::Null
    );
}

// ---------------------------------------------------------------------------
// cypher_order_rank / cypher_order_value
// ---------------------------------------------------------------------------

#[test]
fn ranks_follow_the_opencypher_cross_type_order() {
    let (store, a, _, e) = store_with_edge();
    let ordered = [
        Value::Map(BTreeMap::new()),
        Value::NodeRef(a),
        edge_ref(&store, e),
        Value::List(vec![]),
        Value::Path {
            nodes: vec![a],
            edges: vec![],
        },
        p(s("a")),
        p(PropertyValue::Boolean(false)),
        p(int(1)),
        p(PropertyValue::Float(f64::NAN)),
        Value::Null,
    ];
    for w in ordered.windows(2) {
        assert_eq!(
            cypher_order_value(&w[0], &w[1]),
            Ordering::Less,
            "{:?} should sort before {:?}",
            w[0],
            w[1]
        );
    }
    assert_eq!(cypher_order_rank(&p(PropertyValue::Map(HashMap::new()))), 0);
    assert_eq!(cypher_order_rank(&p(PropertyValue::Array(vec![]))), 3);
    assert_eq!(cypher_order_rank(&p(PropertyValue::Vector(vec![1.0]))), 3);
    assert_eq!(cypher_order_rank(&p(PropertyValue::Null)), 9);
}

#[test]
fn lists_order_element_wise_then_by_length() {
    let l = |xs: &[i64]| Value::List(xs.iter().map(|x| p(int(*x))).collect());
    assert_eq!(cypher_order_value(&l(&[1, 2]), &l(&[1, 3])), Ordering::Less);
    assert_eq!(cypher_order_value(&l(&[2]), &l(&[1, 9])), Ordering::Greater);
    assert_eq!(cypher_order_value(&l(&[1]), &l(&[1, 0])), Ordering::Less);
    assert_eq!(
        cypher_order_value(&l(&[1, 2]), &l(&[1, 2])),
        Ordering::Equal
    );
}

#[test]
fn maps_order_by_sorted_key_then_value_then_size() {
    let m = |pairs: &[(&str, i64)]| {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), p(int(*v))))
                .collect(),
        )
    };
    assert_eq!(
        cypher_order_value(&m(&[("a", 1)]), &m(&[("b", 1)])),
        Ordering::Less
    );
    assert_eq!(
        cypher_order_value(&m(&[("a", 2)]), &m(&[("a", 1)])),
        Ordering::Greater
    );
    assert_eq!(
        cypher_order_value(&m(&[("a", 1)]), &m(&[("a", 1), ("b", 0)])),
        Ordering::Less
    );
    assert_eq!(
        cypher_order_value(&m(&[("a", 1)]), &m(&[("a", 1)])),
        Ordering::Equal
    );
}

#[test]
fn entities_of_one_kind_order_by_id_and_paths_by_first_node_then_length() {
    let n = |i: u64| Value::NodeRef(NodeId::new(i));
    assert_eq!(cypher_order_value(&n(1), &n(2)), Ordering::Less);
    let full = Value::Node(
        NodeId::new(5),
        Box::new(Node::new(NodeId::new(5), crate::graph::Label::new("X"))),
    );
    assert_eq!(cypher_order_value(&full, &n(5)), Ordering::Equal);
    let path = |first: Option<u64>, edges: usize| Value::Path {
        nodes: first.into_iter().map(NodeId::new).collect(),
        edges: (0..edges as u64).map(EdgeId::new).collect(),
    };
    assert_eq!(
        cypher_order_value(&path(Some(1), 3), &path(Some(2), 0)),
        Ordering::Less
    );
    assert_eq!(
        cypher_order_value(&path(Some(1), 1), &path(Some(1), 2)),
        Ordering::Less
    );
    assert_eq!(
        cypher_order_value(&path(None, 0), &path(Some(1), 0)),
        Ordering::Less
    );
    let (store, _, _, e) = store_with_edge();
    let full_edge = Value::Edge(e, Box::new(store.get_edge(e).unwrap()));
    assert_eq!(
        cypher_order_value(&full_edge, &edge_ref(&store, e)),
        Ordering::Equal
    );
    assert_eq!(
        cypher_order_value(&Value::Null, &Value::Null),
        Ordering::Equal
    );
}

#[test]
fn properties_of_one_rank_defer_to_the_property_order() {
    assert_eq!(cypher_order_value(&p(int(1)), &p(int(2))), Ordering::Less);
    assert_eq!(
        cypher_order_value(&p(s("b")), &p(s("a"))),
        Ordering::Greater
    );
}
