use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use std::time::Instant;
use rand::Rng;

fn main() {
    let mut store = GraphStore::new();
    let num_vectors = 10_000;
    let dimensions = 128;
    let k = 10;

    println!("Creating vector index ({} dimensions)...", dimensions);
    store.create_vector_index("Item", "embedding", dimensions, DistanceMetric::Cosine).unwrap();

    println!("Generating {} random vectors...", num_vectors);
    let mut rng = rand::thread_rng();
    let mut all_vectors = Vec::new();

    let start = Instant::now();
    for i in 0..num_vectors {
        let vec: Vec<f32> = (0..dimensions).map(|_| rng.gen::<f32>()).collect();
        all_vectors.push(vec.clone());
        
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("embedding".to_string(), PropertyValue::Vector(vec));
        
        store.create_node_with_properties("default", vec![Label::new("Item")], props);
        
        if (i + 1) % 2000 == 0 {
            println!("  Inserted {} vectors...", i + 1);
        }
    }
    println!("Indexing took: {:?}", start.elapsed());

    // Perform searches
    let num_searches = 100;
    println!("Performing {} searches (k={})...", num_searches, k);
    
    let mut total_duration = std::time::Duration::default();
    
    for _ in 0..num_searches {
        let query: Vec<f32> = (0..dimensions).map(|_| rng.gen::<f32>()).collect();
        
        let start_search = Instant::now();
        let results = store.vector_search("Item", "embedding", &query, k).unwrap();
        total_duration += start_search.elapsed();
        
        assert!(results.len() <= k);
    }

    println!("Average search latency: {:?}", total_duration / num_searches as u32);
    println!("Total search time: {:?}", total_duration);
}
