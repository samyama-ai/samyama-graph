use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[test]
fn test_grouped_aggregations() {
    let mut store = GraphStore::new();

    // Create test data: 3 Persons in 'HR' and 2 in 'Engineering'
    let hr = "HR".to_string();
    let eng = "Engineering".to_string();

    // HR Dept
    for age in [30, 40, 50] {
        let n = store.create_node("Person");
        let node = store.get_node_mut(n).unwrap();
        node.set_property("dept", hr.clone());
        node.set_property("age", age as i64);
    }

    // Engineering Dept
    for age in [20, 25] {
        let n = store.create_node("Person");
        let node = store.get_node_mut(n).unwrap();
        node.set_property("dept", eng.clone());
        node.set_property("age", age as i64);
    }

    // Query: GROUP BY dept, count(*), avg(age), max(age)
    let query_str = "MATCH (n:Person) RETURN n.dept AS dept, count(n) AS count, avg(n.age) AS avg_age, max(n.age) AS max_age ORDER BY dept";
    let query = parse_query(query_str).unwrap();
    let executor = QueryExecutor::new(&store);
    let result = executor.execute(&query).unwrap();

    assert_eq!(result.records.len(), 2);

    // Verify Engineering (comes first alphabetically)
    let eng_rec = &result.records[0];
    assert_eq!(eng_rec.get("dept").unwrap().as_property().unwrap().as_string().unwrap(), "Engineering");
    assert_eq!(eng_rec.get("count").unwrap().as_property().unwrap().as_integer().unwrap(), 2);
    assert_eq!(eng_rec.get("avg_age").unwrap().as_property().unwrap().as_float().unwrap(), 22.5);
    assert_eq!(eng_rec.get("max_age").unwrap().as_property().unwrap().as_integer().unwrap(), 25);

    // Verify HR
    let hr_rec = &result.records[1];
    assert_eq!(hr_rec.get("dept").unwrap().as_property().unwrap().as_string().unwrap(), "HR");
    assert_eq!(hr_rec.get("count").unwrap().as_property().unwrap().as_integer().unwrap(), 3);
    assert_eq!(hr_rec.get("avg_age").unwrap().as_property().unwrap().as_float().unwrap(), 40.0);
    assert_eq!(hr_rec.get("max_age").unwrap().as_property().unwrap().as_integer().unwrap(), 50);
}
