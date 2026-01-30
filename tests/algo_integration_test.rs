use samyama::graph::{GraphStore, PropertyValue, EdgeType};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[test]
fn test_max_flow_mst_integration() {
    let mut store = GraphStore::new();

    // Nodes 1, 2, 3, 4
    let n1 = store.create_node("Node");
    let n2 = store.create_node("Node");
    let n3 = store.create_node("Node");
    let n4 = store.create_node("Node");

    // Edges for Flow (Diamond)
    // 1->2 (10), 1->3 (10)
    // 2->3 (1)
    // 2->4 (10)
    // 3->4 (10)
    
    let et = EdgeType::new("LINK");
    
    let e1 = store.create_edge(n1, n2, et.clone()).unwrap();
    store.get_edge_mut(e1).unwrap().set_property("capacity".to_string(), PropertyValue::Float(10.0));
    
    let e2 = store.create_edge(n1, n3, et.clone()).unwrap();
    store.get_edge_mut(e2).unwrap().set_property("capacity".to_string(), PropertyValue::Float(10.0));
    
    let e3 = store.create_edge(n2, n3, et.clone()).unwrap();
    store.get_edge_mut(e3).unwrap().set_property("capacity".to_string(), PropertyValue::Float(1.0));

    let e4 = store.create_edge(n2, n4, et.clone()).unwrap();
    store.get_edge_mut(e4).unwrap().set_property("capacity".to_string(), PropertyValue::Float(10.0));

    let e5 = store.create_edge(n3, n4, et.clone()).unwrap();
    store.get_edge_mut(e5).unwrap().set_property("capacity".to_string(), PropertyValue::Float(10.0));

    // 1. Test Max Flow
    let query_str = format!("CALL algo.maxFlow({}, {}, 'capacity') YIELD max_flow", n1.as_u64(), n4.as_u64());
    let query = parse_query(&query_str).expect("Parse failed");
    let executor = QueryExecutor::new(&store);
    let result = executor.execute(&query).expect("Execute failed");
    
    assert_eq!(result.records.len(), 1);
    let max_flow = result.records[0].get("max_flow").unwrap().as_property().unwrap().as_float().unwrap();
    assert_eq!(max_flow, 20.0); // 10 top, 10 bottom. Middle edge 2->3 adds nothing? 
    // Path 1->2->4 (10)
    // Path 1->3->4 (10)
    // Total 20.
    
    // 2. Test MST (using 'capacity' as weight for simplicity)
    // MST should pick edges with minimum weight.
    // Weights: 10, 10, 1, 10, 10.
    // Sorted: 1 (2->3), then 10s.
    // MST edges: 2-3 (1), 1-2 (10), 3-4 (10) -> Total 21.
    // Or 1-3 (10) instead of 1-2.
    
    let query_str = "CALL algo.mst('capacity') YIELD source, target, weight, total_weight";
    let query = parse_query(query_str).expect("Parse failed");
    let result = executor.execute(&query).expect("Execute failed");
    
    // 1 summary record + 3 edge records = 4 records? 
    // Implementation pushed summary then edges.
    
    let mut total_weight = 0.0;
    let mut edge_count = 0;
    
    for rec in result.records {
        if rec.has("total_weight") {
            total_weight = rec.get("total_weight").unwrap().as_property().unwrap().as_float().unwrap();
        }
        if rec.has("source") {
            edge_count += 1;
        }
    }
    
    assert_eq!(total_weight, 21.0);
    assert_eq!(edge_count, 3);
}
