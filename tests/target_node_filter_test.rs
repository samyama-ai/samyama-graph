/// Tests for target node label and property filtering in MATCH patterns.
///
/// Regression tests for the bug where property filters on target nodes in
/// relationship patterns were ignored. E.g.:
///   MATCH (t:Trial)-[:STUDIES]->(d:Disease {name: 'Diabetes'})
/// would return ALL diseases, not just Diabetes.
use samyama::{GraphStore, QueryEngine, PropertyValue};

/// Set up a clinical trials graph with multiple diseases and trials
fn setup_trials_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Disease categories
    for disease in &["Diabetes", "Cancer", "Alzheimers", "Asthma"] {
        let q = format!("CREATE (d:DiseaseCategory {{name: '{}'}})", disease);
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    // Trials studying specific diseases
    let trials = vec![
        ("T001", "Phase3", "Diabetes"),
        ("T002", "Phase3", "Cancer"),
        ("T003", "Phase2", "Diabetes"),
        ("T004", "Phase3", "Alzheimers"),
        ("T005", "Phase1", "Asthma"),
        ("T006", "Phase3", "Diabetes"),
    ];

    for (trial_id, phase, disease) in &trials {
        let q = format!("CREATE (t:Trial {{trial_id: '{}', phase: '{}'}})", trial_id, phase);
        engine.execute_mut(&q, &mut store, "default").unwrap();

        let q = format!(
            "MATCH (t:Trial {{trial_id: '{}'}}), (d:DiseaseCategory {{name: '{}'}}) CREATE (t)-[:STUDIES]->(d)",
            trial_id, disease
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    store
}

#[test]
fn test_target_node_property_filter() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // This was the original failing query — should only return Diabetes trials
    let query = "MATCH (t:Trial {phase: 'Phase3'})-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) RETURN d.name, t.trial_id, t.phase";
    let result = engine.execute(query, &store).unwrap();

    // Phase3 trials studying Diabetes: T001, T006
    assert_eq!(result.len(), 2, "Expected 2 results but got {}", result.len());

    let disease_names: Vec<String> = result.records.iter()
        .map(|r| r.get("d.name").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();

    // All results must be Diabetes
    for name in &disease_names {
        assert_eq!(name, "Diabetes", "Expected Diabetes but got {}", name);
    }

    let trial_ids: Vec<String> = result.records.iter()
        .map(|r| r.get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();

    assert!(trial_ids.contains(&"T001".to_string()));
    assert!(trial_ids.contains(&"T006".to_string()));
}

#[test]
fn test_target_node_label_filter() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create nodes of different labels
    engine.execute_mut("CREATE (p:Person {name: 'Alice'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (c:Company {name: 'Acme'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (c:City {name: 'Boston'})", &mut store, "default").unwrap();

    // Alice works at Acme, Alice lives in Boston
    engine.execute_mut(
        "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)",
        &mut store, "default"
    ).unwrap();
    engine.execute_mut(
        "MATCH (p:Person {name: 'Alice'}), (c:City {name: 'Boston'}) CREATE (p)-[:LIVES_IN]->(c)",
        &mut store, "default"
    ).unwrap();

    // Query with target label Company — should only return Acme, not Boston
    let query = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name";
    let result = engine.execute(query, &store).unwrap();
    assert_eq!(result.len(), 1);
    let name = result.records[0].get("c.name").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(name, "Acme");
}

#[test]
fn test_target_property_filter_no_match() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // Query for a disease that no Phase3 trial studies
    let query = "MATCH (t:Trial {phase: 'Phase3'})-[:STUDIES]->(d:DiseaseCategory {name: 'Asthma'}) RETURN t.trial_id";
    let result = engine.execute(query, &store).unwrap();

    // Asthma only has a Phase1 trial (T005), no Phase3 trials
    assert_eq!(result.len(), 0, "Expected 0 results for Phase3+Asthma but got {}", result.len());
}

#[test]
fn test_target_property_filter_all_phases() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // Query Diabetes trials across all phases (no start node property filter)
    let query = "MATCH (t:Trial)-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) RETURN t.trial_id, t.phase";
    let result = engine.execute(query, &store).unwrap();

    // T001 (Phase3), T003 (Phase2), T006 (Phase3)
    assert_eq!(result.len(), 3, "Expected 3 Diabetes trials but got {}", result.len());

    let ids: Vec<String> = result.records.iter()
        .map(|r| r.get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"T001".to_string()));
    assert!(ids.contains(&"T003".to_string()));
    assert!(ids.contains(&"T006".to_string()));
}

#[test]
fn test_multi_hop_with_target_properties() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Build: Person -[:WORKS_AT]-> Company -[:IN_SECTOR]-> Sector
    engine.execute_mut("CREATE (p:Person {name: 'Alice'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (p:Person {name: 'Bob'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (c:Company {name: 'TechCorp', size: 'large'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (c:Company {name: 'SmallCo', size: 'small'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (s:Sector {name: 'Technology'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (s:Sector {name: 'Finance'})", &mut store, "default").unwrap();

    // Alice -> TechCorp -> Technology
    engine.execute_mut(
        "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) CREATE (p)-[:WORKS_AT]->(c)",
        &mut store, "default"
    ).unwrap();
    engine.execute_mut(
        "MATCH (c:Company {name: 'TechCorp'}), (s:Sector {name: 'Technology'}) CREATE (c)-[:IN_SECTOR]->(s)",
        &mut store, "default"
    ).unwrap();

    // Bob -> SmallCo -> Finance
    engine.execute_mut(
        "MATCH (p:Person {name: 'Bob'}), (c:Company {name: 'SmallCo'}) CREATE (p)-[:WORKS_AT]->(c)",
        &mut store, "default"
    ).unwrap();
    engine.execute_mut(
        "MATCH (c:Company {name: 'SmallCo'}), (s:Sector {name: 'Finance'}) CREATE (c)-[:IN_SECTOR]->(s)",
        &mut store, "default"
    ).unwrap();

    // Multi-hop: find people who work at large companies in Technology
    let query = "MATCH (p:Person)-[:WORKS_AT]->(c:Company {size: 'large'})-[:IN_SECTOR]->(s:Sector {name: 'Technology'}) RETURN p.name";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 1);
    let name = result.records[0].get("p.name").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(name, "Alice");
}

#[test]
fn test_target_property_with_where_clause() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // Combine inline property filter on target with WHERE clause
    let query = "MATCH (t:Trial)-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) WHERE t.phase = 'Phase3' RETURN t.trial_id";
    let result = engine.execute(query, &store).unwrap();

    // Same as test_target_node_property_filter but with phase filter in WHERE instead of inline
    assert_eq!(result.len(), 2);
    let ids: Vec<String> = result.records.iter()
        .map(|r| r.get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"T001".to_string()));
    assert!(ids.contains(&"T006".to_string()));
}

#[test]
fn test_target_multiple_properties() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create products with multiple properties
    engine.execute_mut("CREATE (s:Store {name: 'MainStreet'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (p:Product {name: 'Widget', color: 'red', size: 'large'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (p:Product {name: 'Gadget', color: 'red', size: 'small'})", &mut store, "default").unwrap();
    engine.execute_mut("CREATE (p:Product {name: 'Doohickey', color: 'blue', size: 'large'})", &mut store, "default").unwrap();

    // Store sells all products
    for product in &["Widget", "Gadget", "Doohickey"] {
        engine.execute_mut(
            &format!("MATCH (s:Store {{name: 'MainStreet'}}), (p:Product {{name: '{}'}}) CREATE (s)-[:SELLS]->(p)", product),
            &mut store, "default"
        ).unwrap();
    }

    // Filter target by TWO properties: red AND large
    let query = "MATCH (s:Store)-[:SELLS]->(p:Product {color: 'red', size: 'large'}) RETURN p.name";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 1);
    let name = result.records[0].get("p.name").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(name, "Widget");
}

#[test]
fn test_start_and_target_both_filtered() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // Both start and target have inline property filters
    let query = "MATCH (t:Trial {phase: 'Phase2'})-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) RETURN t.trial_id";
    let result = engine.execute(query, &store).unwrap();

    // Only T003 is Phase2 + Diabetes
    assert_eq!(result.len(), 1);
    let id = result.records[0].get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(id, "T003");
}

#[test]
fn test_target_property_with_count() {
    let store = setup_trials_graph();
    let engine = QueryEngine::new();

    // Count Phase3 Diabetes trials
    let query = "MATCH (t:Trial {phase: 'Phase3'})-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) RETURN count(t)";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 1);
    let count = result.records[0].get("count(t)").unwrap().as_property().unwrap();
    if let PropertyValue::Integer(c) = count {
        assert_eq!(*c, 2);
    } else {
        panic!("Expected integer, got {:?}", count);
    }
}

/// Set up a graph with trials, diseases, countries, and relationships for multi-path join tests
fn setup_multi_path_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Countries
    for country in &["India", "USA", "UK"] {
        engine.execute_mut(&format!("CREATE (c:Country {{name: '{}'}})", country), &mut store, "default").unwrap();
    }

    // Disease categories
    for disease in &["Diabetes", "Cancer", "Respiratory"] {
        engine.execute_mut(&format!("CREATE (d:DiseaseCategory {{name: '{}'}})", disease), &mut store, "default").unwrap();
    }

    // Trials with country + disease relationships
    let trials = vec![
        ("T001", "India", "Diabetes"),
        ("T002", "India", "Cancer"),
        ("T003", "USA", "Diabetes"),
        ("T004", "UK", "Respiratory"),
        ("T005", "India", "Respiratory"),
    ];

    for (tid, country, disease) in &trials {
        engine.execute_mut(&format!("CREATE (t:Trial {{trial_id: '{}'}})", tid), &mut store, "default").unwrap();
        engine.execute_mut(
            &format!("MATCH (t:Trial {{trial_id: '{}'}}), (c:Country {{name: '{}'}}) CREATE (t)-[:CONDUCTED_IN]->(c)", tid, country),
            &mut store, "default"
        ).unwrap();
        engine.execute_mut(
            &format!("MATCH (t:Trial {{trial_id: '{}'}}), (d:DiseaseCategory {{name: '{}'}}) CREATE (t)-[:STUDIES]->(d)", tid, disease),
            &mut store, "default"
        ).unwrap();
    }

    store
}

#[test]
fn test_multi_path_join_shared_variable() {
    let store = setup_multi_path_graph();
    let engine = QueryEngine::new();

    // Multi-path MATCH with shared variable t — should JOIN, not cross-product
    let query = "MATCH (t:Trial)-[:CONDUCTED_IN]->(c:Country {name: 'India'}), (t)-[:STUDIES]->(d:DiseaseCategory) RETURN d.name, t.trial_id LIMIT 20";
    let result = engine.execute(query, &store).unwrap();

    // India trials: T001 (Diabetes), T002 (Cancer), T005 (Respiratory)
    assert_eq!(result.len(), 3, "Expected 3 India trials but got {}", result.len());

    let mut trial_disease: Vec<(String, String)> = result.records.iter()
        .map(|r| {
            let tid = r.get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap().to_string();
            let disease = r.get("d.name").unwrap().as_property().unwrap().as_string().unwrap().to_string();
            (tid, disease)
        })
        .collect();
    trial_disease.sort();

    assert!(trial_disease.contains(&("T001".to_string(), "Diabetes".to_string())));
    assert!(trial_disease.contains(&("T002".to_string(), "Cancer".to_string())));
    assert!(trial_disease.contains(&("T005".to_string(), "Respiratory".to_string())));
}

#[test]
fn test_multi_path_join_with_both_target_filters() {
    let store = setup_multi_path_graph();
    let engine = QueryEngine::new();

    // Both paths have target property filters + shared variable t
    let query = "MATCH (t:Trial)-[:CONDUCTED_IN]->(c:Country {name: 'India'}), (t)-[:STUDIES]->(d:DiseaseCategory {name: 'Diabetes'}) RETURN t.trial_id";
    let result = engine.execute(query, &store).unwrap();

    // Only T001 is India + Diabetes
    assert_eq!(result.len(), 1, "Expected 1 result but got {}", result.len());
    let tid = result.records[0].get("t.trial_id").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(tid, "T001");
}

#[test]
fn test_multi_path_no_shared_variable() {
    let store = setup_multi_path_graph();
    let engine = QueryEngine::new();

    // Two independent paths with NO shared variable — should be CartesianProduct
    let query = "MATCH (c:Country {name: 'India'}), (d:DiseaseCategory {name: 'Diabetes'}) RETURN c.name, d.name";
    let result = engine.execute(query, &store).unwrap();

    // 1 country x 1 disease = 1 result
    assert_eq!(result.len(), 1);
    let country = result.records[0].get("c.name").unwrap().as_property().unwrap().as_string().unwrap();
    let disease = result.records[0].get("d.name").unwrap().as_property().unwrap().as_string().unwrap();
    assert_eq!(country, "India");
    assert_eq!(disease, "Diabetes");
}
