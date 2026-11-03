//! Reading a property through a cached column id (#557).
//!
//! `PropertyCursor` exists to hoist one hash lookup out of a per-row loop, and
//! the whole value of it is that it is otherwise **indistinguishable** from
//! `Value::resolve_property`. So these tests are differential: same input, both
//! paths, same answer. A cursor that is merely fast is a bug.
//!
//! The cases that matter are the ones where the column is *not* the answer —
//! a property with no column yet, a value whose type has no typed column
//! representation and lives only in row storage (#545), a deleted row. Each is
//! a way for a cached column id to return the wrong thing confidently.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{PropertyCursor, QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;

/// Both paths, asserted equal, and the value returned for further checking.
fn both_ways(store: &GraphStore, record: &Record, var: &str, property: &str) -> PropertyValue {
    let direct = record
        .get(var)
        .map(|v| v.resolve_property(property, store))
        .unwrap_or(PropertyValue::Null);
    let mut cursor = PropertyCursor::new(var, property);
    let cached = cursor.read(record, store);
    assert_eq!(cached, direct, "cursor disagreed with resolve_property on {var}.{property}");
    // Read again: the second read comes from the memoised column id, which is
    // the path every row after the first takes.
    assert_eq!(cursor.read(record, store), direct, "the memoised read disagreed");
    direct
}

fn record_for(id: samyama::graph::NodeId) -> Record {
    let mut r = Record::new();
    r.bind("n", Value::NodeRef(id));
    r
}

#[test]
fn a_column_backed_property_reads_the_same_both_ways() {
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(42));
    assert_eq!(both_ways(&store, &record_for(id), "n", "v"), PropertyValue::Integer(42));
}

#[test]
fn a_property_that_does_not_exist_is_null_both_ways() {
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(1));
    assert_eq!(both_ways(&store, &record_for(id), "n", "absent"), PropertyValue::Null);
}

#[test]
fn a_column_created_after_the_cursor_first_missed_is_still_found() {
    // The reason a miss is not cached. A cursor that remembered "no column"
    // would keep returning null after a SET created one.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let record = record_for(id);

    let mut cursor = PropertyCursor::new("n", "later");
    assert_eq!(cursor.read(&record, &store), PropertyValue::Null);

    let _ = store.set_node_property("default", id, "later".to_string(), PropertyValue::Integer(7));
    assert_eq!(
        cursor.read(&record, &store),
        PropertyValue::Integer(7),
        "the cursor cached the absence of a column"
    );
}

#[test]
fn a_value_that_lives_only_in_row_storage_is_still_found() {
    // Complex values round-trip only through the per-node map (#545). A cursor
    // that trusted the column and stopped would return null for them.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    if let Some(node) = store.get_node_mut(id) {
        node.set_property(
            "tags",
            PropertyValue::Array(vec![
                PropertyValue::String("a".into()),
                PropertyValue::String("b".into()),
            ]),
        );
    }
    let got = both_ways(&store, &record_for(id), "n", "tags");
    assert!(matches!(got, PropertyValue::Array(ref a) if a.len() == 2), "{got:?}");
}

#[test]
fn a_row_with_no_value_in_a_column_that_exists_falls_back() {
    // The column exists because another node has the property. This node does
    // not, so the column returns null and row storage has to be consulted.
    let mut store = GraphStore::new();
    let other = store.create_node("N");
    let _ = store.set_node_property("default", other, "v".to_string(), PropertyValue::Integer(1));

    let id = store.create_node("N");
    if let Some(node) = store.get_node_mut(id) {
        node.set_property("v", PropertyValue::Integer(99));
    }
    assert_eq!(both_ways(&store, &record_for(id), "n", "v"), PropertyValue::Integer(99));
}

#[test]
fn an_edge_property_reads_the_same_both_ways() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let e = store.create_edge(a, b, "LINK").unwrap();
    let _ = store.set_edge_property(e, "weight", PropertyValue::Float(1.5));

    let mut record = Record::new();
    record.bind("r", Value::EdgeRef(e, a, b, samyama::graph::EdgeType::new("LINK")));
    assert_eq!(both_ways(&store, &record, "r", "weight"), PropertyValue::Float(1.5));
}

#[test]
fn an_unbound_variable_is_null_rather_than_a_panic() {
    let store = GraphStore::new();
    let mut cursor = PropertyCursor::new("missing", "v");
    assert_eq!(cursor.read(&Record::new(), &store), PropertyValue::Null);
}

#[test]
fn a_non_graph_value_falls_through_to_the_general_path() {
    // `duration.days`, `datetime.year` and friends resolve against the value
    // itself, not against any column.
    let store = GraphStore::new();
    let mut record = Record::new();
    record.bind("d", Value::Property(PropertyValue::DateTime(1_700_000_000_000)));
    let got = both_ways(&store, &record, "d", "year");
    assert!(matches!(got, PropertyValue::Integer(_)), "{got:?}");
}

#[test]
fn column_ids_survive_new_columns_being_added() {
    // The property that makes caching an id safe at all: ids are handed out on
    // append and never shift.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let _ = store.set_node_property("default", id, "first".to_string(), PropertyValue::Integer(1));

    let first = store.node_columns.column_id("first").expect("column exists");
    for k in 0..64 {
        let _ = store.set_node_property("default", id, format!("p{k}"), PropertyValue::Integer(k));
    }
    assert_eq!(store.node_columns.column_id("first"), Some(first), "the id moved");
    assert_eq!(
        store.node_columns.get_by_id(first, id.as_u64() as usize),
        PropertyValue::Integer(1)
    );
}

#[test]
fn a_recycled_row_does_not_read_the_previous_occupants_value() {
    // Node ids come off a free list (#364). Reading by cached column id must
    // see the cleared slot, not a stale one.
    let mut store = GraphStore::new();
    let victim = store.create_node("N");
    let _ = store.set_node_property("default", victim, "v".to_string(), PropertyValue::Integer(5));
    let column = store.node_columns.column_id("v").unwrap();

    let query = parse_query("MATCH (n:N) WHERE n.v = 5 DETACH DELETE n").unwrap();
    let mut mutating =
        samyama::query::executor::MutQueryExecutor::new(&mut store, "default".to_string());
    mutating.execute(&query).expect("delete should run");

    let fresh = store.create_node("Ghost");
    assert_eq!(
        store.node_columns.get_by_id(column, fresh.as_u64() as usize),
        PropertyValue::Null
    );
    let mut cursor = PropertyCursor::new("n", "v");
    let mut record = Record::new();
    record.bind("n", Value::NodeRef(fresh));
    assert_eq!(cursor.read(&record, &store), PropertyValue::Null);
}

#[test]
fn an_aggregate_over_many_rows_agrees_with_a_scan() {
    // End to end: the aggregate paths now read through cursors, so a whole-graph
    // fold has to match a hand-computed answer.
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    let mut expected_sum = 0i64;
    for i in 0..5000i64 {
        let n = store.create_node("N");
        let _ = store.set_node_property("default", n, "v".to_string(), PropertyValue::Integer(i));
        store.create_edge(n, hub, "IN").unwrap();
        expected_sum += i;
    }
    let query = parse_query("MATCH (h:Hub)<-[:IN]-(n:N) RETURN sum(n.v) AS s").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(
        batch.records[0].get("s"),
        Some(&Value::Property(PropertyValue::Integer(expected_sum)))
    );
}
