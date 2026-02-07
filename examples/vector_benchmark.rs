//! Vector Search Benchmark Suite
//!
//! Comprehensive benchmarks for Samyama's HNSW vector index:
//! 1. Index build time across dimensions
//! 2. Search latency by distance metric (Cosine, L2, Dot)
//! 3. Recall@k measurement
//! 4. Dimension scaling (64, 128, 384, 768)
//! 5. Dataset size scaling

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use std::collections::HashSet;
use std::time::Instant;
use rand::Rng;

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 { 1.0 } else { 1.0 - dot / denom }
}

fn benchmark_index_build(dimensions: &[usize], num_vectors: usize) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 1: Index Build Time ({} vectors)             │", format_number(num_vectors));
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut rng = rand::thread_rng();

    println!("  {:>6} {:>12} {:>14} {:>14}", "Dim", "Duration", "Vectors/sec", "MB est.");
    println!("  {:>6} {:>12} {:>14} {:>14}", "---", "--------", "-----------", "-------");

    for &dim in dimensions {
        let mut store = GraphStore::new();
        store.create_vector_index("Item", "embedding", dim, DistanceMetric::Cosine).unwrap();

        let start = Instant::now();
        for i in 0..num_vectors {
            let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), PropertyValue::Integer(i as i64));
            props.insert("embedding".to_string(), PropertyValue::Vector(vec));
            store.create_node_with_properties("default", vec![Label::new("Item")], props);
        }
        let duration = start.elapsed();

        let rate = num_vectors as f64 / duration.as_secs_f64();
        let mb = (num_vectors * dim * 4) as f64 / (1024.0 * 1024.0);

        println!("  {:>6} {:>10.2?} {:>12.0}/s {:>12.1}MB",
            dim, duration, rate, mb);
    }
    println!();
}

fn benchmark_distance_metrics(num_vectors: usize, dim: usize, k: usize) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 2: Distance Metrics ({} vectors, {} dim, k={})   │",
        format_number(num_vectors), dim, k);
    println!("└──────────────────────────────────────────────────────────────────┘");

    let metrics = [
        ("Cosine", DistanceMetric::Cosine),
        ("L2", DistanceMetric::L2),
        ("InnerProduct", DistanceMetric::InnerProduct),
    ];

    let mut rng = rand::thread_rng();
    let num_searches = 500;

    println!("  {:>12} {:>12} {:>14} {:>10}", "Metric", "Avg Latency", "QPS", "p99 est.");
    println!("  {:>12} {:>12} {:>14} {:>10}", "------", "-----------", "---", "--------");

    for (name, metric) in &metrics {
        let mut store = GraphStore::new();
        store.create_vector_index("Item", "embedding", dim, *metric).unwrap();

        for i in 0..num_vectors {
            let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), PropertyValue::Integer(i as i64));
            props.insert("embedding".to_string(), PropertyValue::Vector(vec));
            store.create_node_with_properties("default", vec![Label::new("Item")], props);
        }

        let mut latencies = Vec::with_capacity(num_searches);

        for _ in 0..num_searches {
            let query: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let t = Instant::now();
            let _ = store.vector_search("Item", "embedding", &query, k).unwrap();
            latencies.push(t.elapsed());
        }

        latencies.sort();
        let total: std::time::Duration = latencies.iter().sum();
        let avg = total / num_searches as u32;
        let p99 = latencies[(num_searches as f64 * 0.99) as usize];
        let qps = num_searches as f64 / total.as_secs_f64();

        println!("  {:>12} {:>10.2?} {:>12.0}/s {:>8.2?}",
            name, avg, qps, p99);
    }
    println!();
}

fn benchmark_recall(num_vectors: usize, dim: usize) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 3: Recall@k ({} vectors, {} dim)              │",
        format_number(num_vectors), dim);
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut rng = rand::thread_rng();
    let mut store = GraphStore::new();
    store.create_vector_index("Item", "embedding", dim, DistanceMetric::Cosine).unwrap();

    let mut all_vectors: Vec<Vec<f32>> = Vec::with_capacity(num_vectors);
    let mut node_ids = Vec::with_capacity(num_vectors);

    for i in 0..num_vectors {
        let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
        all_vectors.push(vec.clone());
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("embedding".to_string(), PropertyValue::Vector(vec));
        let id = store.create_node_with_properties("default", vec![Label::new("Item")], props);
        node_ids.push(id);
    }

    let k_values = [1, 5, 10, 20, 50];
    let num_queries = 100;

    println!("  {:>6} {:>10} {:>12} {:>12}", "k", "Recall@k", "Avg Latency", "Results");
    println!("  {:>6} {:>10} {:>12} {:>12}", "-", "--------", "-----------", "-------");

    for &k in &k_values {
        if k > num_vectors { continue; }

        let mut total_recall = 0.0;
        let mut total_time = std::time::Duration::default();

        for _ in 0..num_queries {
            let query: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();

            // Brute-force ground truth
            let mut distances: Vec<(usize, f32)> = all_vectors.iter()
                .enumerate()
                .map(|(i, v)| (i, cosine_distance(&query, v)))
                .collect();
            distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let ground_truth: HashSet<u64> = distances[..k].iter()
                .map(|(i, _)| node_ids[*i].as_u64())
                .collect();

            // HNSW search
            let t = Instant::now();
            let results = store.vector_search("Item", "embedding", &query, k).unwrap();
            total_time += t.elapsed();

            let found: HashSet<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
            let intersection = ground_truth.intersection(&found).count();
            total_recall += intersection as f64 / k as f64;
        }

        let avg_recall = total_recall / num_queries as f64;
        let avg_latency = total_time / num_queries as u32;

        println!("  {:>6} {:>9.1}% {:>10.2?} {:>12}",
            k, avg_recall * 100.0, avg_latency, k);
    }
    println!();
}

fn benchmark_dimension_scaling(num_vectors: usize, k: usize) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 4: Dimension Scaling ({} vectors, k={})        │",
        format_number(num_vectors), k);
    println!("└──────────────────────────────────────────────────────────────────┘");

    let dimensions = [32, 64, 128, 256, 384, 768];
    let mut rng = rand::thread_rng();
    let num_searches = 200;

    println!("  {:>6} {:>12} {:>12} {:>14} {:>10}", "Dim", "Build Time", "Search Avg", "Search QPS", "MB/index");
    println!("  {:>6} {:>12} {:>12} {:>14} {:>10}", "---", "----------", "----------", "----------", "--------");

    for &dim in &dimensions {
        let mut store = GraphStore::new();
        store.create_vector_index("Item", "embedding", dim, DistanceMetric::Cosine).unwrap();

        let build_start = Instant::now();
        for i in 0..num_vectors {
            let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), PropertyValue::Integer(i as i64));
            props.insert("embedding".to_string(), PropertyValue::Vector(vec));
            store.create_node_with_properties("default", vec![Label::new("Item")], props);
        }
        let build_time = build_start.elapsed();

        let mut total_search = std::time::Duration::default();
        for _ in 0..num_searches {
            let query: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let t = Instant::now();
            let _ = store.vector_search("Item", "embedding", &query, k).unwrap();
            total_search += t.elapsed();
        }

        let avg_search = total_search / num_searches as u32;
        let qps = num_searches as f64 / total_search.as_secs_f64();
        let mb = (num_vectors * dim * 4) as f64 / (1024.0 * 1024.0);

        println!("  {:>6} {:>10.2?} {:>10.2?} {:>12.0}/s {:>8.1}MB",
            dim, build_time, avg_search, qps, mb);
    }
    println!();
}

fn benchmark_dataset_scaling(dim: usize, k: usize) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 5: Dataset Size Scaling ({} dim, k={})            │", dim, k);
    println!("└──────────────────────────────────────────────────────────────────┘");

    let sizes = [1_000, 5_000, 10_000, 25_000, 50_000];
    let mut rng = rand::thread_rng();
    let num_searches = 100;

    println!("  {:>8} {:>12} {:>12} {:>14}", "Vectors", "Build Time", "Search Avg", "Search QPS");
    println!("  {:>8} {:>12} {:>12} {:>14}", "-------", "----------", "----------", "----------");

    for &n in &sizes {
        let mut store = GraphStore::new();
        store.create_vector_index("Item", "embedding", dim, DistanceMetric::Cosine).unwrap();

        let build_start = Instant::now();
        for i in 0..n {
            let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), PropertyValue::Integer(i as i64));
            props.insert("embedding".to_string(), PropertyValue::Vector(vec));
            store.create_node_with_properties("default", vec![Label::new("Item")], props);
        }
        let build_time = build_start.elapsed();

        let mut total_search = std::time::Duration::default();
        for _ in 0..num_searches {
            let query: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            let t = Instant::now();
            let _ = store.vector_search("Item", "embedding", &query, k).unwrap();
            total_search += t.elapsed();
        }

        let avg_search = total_search / num_searches as u32;
        let qps = num_searches as f64 / total_search.as_secs_f64();

        println!("  {:>8} {:>10.2?} {:>10.2?} {:>12.0}/s",
            format_number(n), build_time, avg_search, qps);
    }
    println!();
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   SAMYAMA Vector Search Benchmark Suite (HNSW)                  ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let total_start = Instant::now();

    let standard_dims = [64, 128, 384, 768];
    let standard_n = 10_000;
    let standard_k = 10;

    benchmark_index_build(&standard_dims, standard_n);
    benchmark_distance_metrics(standard_n, 128, standard_k);
    benchmark_recall(5_000, 128);
    benchmark_dimension_scaling(standard_n, standard_k);
    benchmark_dataset_scaling(128, standard_k);

    let total = total_start.elapsed();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   Benchmark Complete                                            ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Total duration: {:>10.2?}                                   ║", total);
    println!("║                                                                ║");
    println!("║  Configuration:                                                ║");
    println!("║  - HNSW index with default parameters                          ║");
    println!("║  - Random float32 vectors                                      ║");
    println!("║  - Metrics: Cosine, L2, InnerProduct                           ║");
    println!("║  - Dimensions: 64, 128, 384, 768                               ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
}
