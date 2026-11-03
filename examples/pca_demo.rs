//! PCA Demo: Research Paper Citation Network
//!
//! Demonstrates Samyama's PCA (Principal Component Analysis) capabilities:
//! - **Graph Model:** Research papers with 8 numeric properties
//! - **PCA:** Reduce 8-dimensional feature space to 3 principal components
//! - **Vector Search:** HNSW index on reduced PCA vectors for efficient similarity
//! - **Comparison:** Full-dimensional vs PCA-reduced similarity rankings
//!
//! No API keys required. All data is synthetic and deterministic.

use samyama_sdk::{
    EmbeddedClient, SamyamaClient, AlgorithmClient, VectorClient,
    NodeId, DistanceMetric, PcaConfig, PropertyValue,
};
use std::collections::HashMap;

/// Research paper fields (domain, title_seed)
const DOMAINS: &[&str] = &[
    "Machine Learning",
    "Computer Vision",
    "Natural Language Processing",
    "Robotics",
    "Systems",
    "Databases",
    "Security",
    "Networks",
    "Theory",
    "HCI",
];

/// Feature names for our paper nodes
const FEATURES: &[&str] = &[
    "citation_count",
    "year",
    "h_index",
    "impact_factor",
    "page_count",
    "reference_count",
    "download_count",
    "author_count",
];

/// Deterministic pseudo-random number from seed
fn pseudo_random(seed: usize, salt: usize) -> f64 {
    let h = seed.wrapping_mul(2654435761) ^ salt.wrapping_mul(40503);
    (h % 10000) as f64 / 10000.0
}

/// Generate synthetic paper properties based on domain and index
fn paper_properties(domain_idx: usize, paper_idx: usize) -> Vec<(String, f64)> {
    let seed = domain_idx * 1000 + paper_idx;

    // ML/CV/NLP papers tend to have higher citations and downloads
    let domain_boost = if domain_idx < 3 { 1.5 } else { 1.0 };

    let citation_count = (pseudo_random(seed, 1) * 500.0 * domain_boost + 5.0).round();
    let year = 2015.0 + (pseudo_random(seed, 2) * 11.0).round(); // 2015-2026
    let h_index = (pseudo_random(seed, 3) * 80.0 * domain_boost + 5.0).round();
    let impact_factor = pseudo_random(seed, 4) * 15.0 * domain_boost + 1.0;
    let page_count = (pseudo_random(seed, 5) * 20.0 + 4.0).round();
    let reference_count = (pseudo_random(seed, 6) * 60.0 + 10.0).round();
    let download_count = citation_count * (pseudo_random(seed, 7) * 20.0 + 5.0);
    let author_count = (pseudo_random(seed, 8) * 8.0 + 1.0).round();

    vec![
        ("citation_count".into(), citation_count),
        ("year".into(), year),
        ("h_index".into(), h_index),
        ("impact_factor".into(), impact_factor),
        ("page_count".into(), page_count),
        ("reference_count".into(), reference_count),
        ("download_count".into(), download_count),
        ("author_count".into(), author_count),
    ]
}

fn separator() {
    println!("{}", "-".repeat(90));
}

#[tokio::main]
async fn main() {
    println!();
    println!("============================================================================================");
    println!("              PCA Demo: Research Paper Citation Network");
    println!("              Powered by Samyama Graph Database");
    println!("============================================================================================");
    println!();

    let client = EmbeddedClient::new();

    // ====================================================================
    // Step 1: Build the Research Paper Graph
    // ====================================================================
    println!("Step 1: Building Research Paper Graph");
    separator();

    let papers_per_domain = 100;
    let total_papers = DOMAINS.len() * papers_per_domain;

    let mut paper_ids: Vec<u64> = Vec::with_capacity(total_papers);
    let mut paper_domains: HashMap<u64, String> = HashMap::new();
    let mut paper_titles: HashMap<u64, String> = HashMap::new();
    let mut edge_count = 0u64;

    {
        let mut store = client.store_write().await;

        // Create paper nodes
        for (d_idx, &domain) in DOMAINS.iter().enumerate() {
            for p_idx in 0..papers_per_domain {
                let node_id = store.create_node("Paper");
                let global_idx = d_idx * papers_per_domain + p_idx;
                let title = format!("{} Paper #{}", domain, p_idx + 1);

                if let Some(node) = store.get_node_mut(node_id) {
                    node.set_property("title", PropertyValue::String(title.clone()));
                    node.set_property("domain", PropertyValue::String(domain.to_string()));

                    let props = paper_properties(d_idx, p_idx);
                    for (key, val) in &props {
                        node.set_property(key, PropertyValue::Float(*val));
                    }
                }

                paper_ids.push(node_id.as_u64());
                paper_domains.insert(node_id.as_u64(), domain.to_string());
                paper_titles.insert(node_id.as_u64(), title);

                // Progress
                if global_idx % 200 == 0 && global_idx > 0 {
                    println!("  Created {} papers...", global_idx);
                }
            }
        }

        // Create CITES edges: papers cite earlier papers, with domain affinity
        for i in 0..total_papers {
            let src_domain = i / papers_per_domain;
            let num_refs = (pseudo_random(i, 100) * 8.0 + 2.0) as usize;

            for r in 0..num_refs {
                // 70% chance to cite within same domain, 30% cross-domain
                let target_domain = if pseudo_random(i * 100 + r, 200) < 0.7 {
                    src_domain
                } else {
                    (pseudo_random(i * 100 + r, 300) * DOMAINS.len() as f64) as usize % DOMAINS.len()
                };
                let target_paper = (pseudo_random(i * 100 + r, 400) * papers_per_domain as f64) as usize % papers_per_domain;
                let target_idx = target_domain * papers_per_domain + target_paper;

                if target_idx != i && target_idx < total_papers {
                    let src_nid = NodeId::new(paper_ids[i]);
                    let tgt_nid = NodeId::new(paper_ids[target_idx]);
                    if store.create_edge(src_nid, tgt_nid, "CITES").is_ok() {
                        edge_count += 1;
                    }
                }
            }
        }
    }

    println!("  Created {} papers across {} domains", total_papers, DOMAINS.len());
    println!("  Created {} CITES edges", edge_count);
    println!();

    // ====================================================================
    // Step 2: Run PCA on Paper Features
    // ====================================================================
    println!("Step 2: Running PCA (8D -> 3D)");
    separator();

    let n_components = 3;

    let pca_result = client.pca(
        Some("Paper"),
        &FEATURES.iter().map(|s| *s).collect::<Vec<_>>(),
        PcaConfig {
            n_components,
            max_iterations: 200,
            tolerance: 1e-8,
            center: true,
            scale: true, // Scale since features have very different ranges
            ..Default::default()
        },
    ).await;

    println!("  Samples: {}, Features: {}", pca_result.n_samples, pca_result.n_features);
    println!("  Components extracted: {}", pca_result.components.len());
    println!("  Converged in {} iterations", pca_result.iterations_used);
    println!();

    // Print explained variance
    println!("  Explained Variance:");
    let mut cumulative = 0.0;
    for (i, ratio) in pca_result.explained_variance_ratio.iter().enumerate() {
        cumulative += ratio;
        println!(
            "    PC{}: {:.4} ({:.1}% variance, cumulative {:.1}%)",
            i + 1,
            pca_result.explained_variance[i],
            ratio * 100.0,
            cumulative * 100.0,
        );
    }
    println!();

    // Print component loadings
    println!("  Component Loadings (which features contribute to each PC):");
    for (i, component) in pca_result.components.iter().enumerate() {
        println!("    PC{}:", i + 1);
        let mut loadings: Vec<(usize, f64)> = component.iter().enumerate().map(|(j, &v)| (j, v)).collect();
        loadings.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap());
        for (j, val) in loadings.iter().take(4) {
            let bar_len = (val.abs() * 30.0) as usize;
            let bar = if *val >= 0.0 {
                format!("+{}", "=".repeat(bar_len.min(30)))
            } else {
                format!("-{}", "=".repeat(bar_len.min(30)))
            };
            println!("      {:18} {:+.4}  {}", FEATURES[*j], val, bar);
        }
    }
    println!();

    // ====================================================================
    // Step 3: Project Papers into PCA Space
    // ====================================================================
    println!("Step 3: Projecting Papers into 3D PCA Space");
    separator();

    // Extract feature matrix again for projection
    let mut feature_matrix: Vec<Vec<f64>> = Vec::with_capacity(total_papers);
    {
        let store = client.store_read().await;
        for &pid in &paper_ids {
            let nid = NodeId::new(pid);
            if let Some(node) = store.get_node(nid) {
                let row: Vec<f64> = FEATURES.iter().map(|&f| {
                    match node.get_property(f) {
                        Some(samyama_sdk::PropertyValue::Float(v)) => *v,
                        Some(samyama_sdk::PropertyValue::Integer(v)) => *v as f64,
                        _ => 0.0,
                    }
                }).collect();
                feature_matrix.push(row);
            }
        }
    }

    let projected = pca_result.transform(&feature_matrix);

    // Show sample projections
    println!("  Sample projections (first 5 papers per domain):");
    for (d_idx, &domain) in DOMAINS.iter().enumerate().take(3) {
        println!("    {}:", domain);
        for p in 0..5 {
            let idx = d_idx * papers_per_domain + p;
            let coords = &projected[idx];
            println!(
                "      Paper #{}: ({:+.3}, {:+.3}, {:+.3})",
                p + 1,
                coords[0],
                coords[1],
                coords[2],
            );
        }
    }
    println!("    ... ({} domains total)", DOMAINS.len());
    println!();

    // ====================================================================
    // Step 4: Build HNSW Index with PCA Vectors
    // ====================================================================
    println!("Step 4: Building HNSW Index with 3D PCA Vectors");
    separator();

    client.create_vector_index("Paper", "pca_embedding", n_components, DistanceMetric::L2)
        .await
        .expect("Failed to create PCA vector index");

    for (i, &pid) in paper_ids.iter().enumerate() {
        let nid = NodeId::new(pid);
        let embedding: Vec<f32> = projected[i].iter().map(|&x| x as f32).collect();
        client.add_vector("Paper", "pca_embedding", nid, &embedding)
            .await
            .expect("Failed to add PCA vector");
    }

    println!("  Created HNSW index: dimension={}, metric=L2", n_components);
    println!("  Indexed {} papers with PCA vectors", total_papers);
    println!();

    // ====================================================================
    // Step 5: Similarity Search Using PCA Vectors
    // ====================================================================
    println!("Step 5: Similarity Search -- Finding Related Papers");
    separator();

    // Pick a query paper from Machine Learning domain
    let query_idx = 0; // First ML paper
    let query_id = paper_ids[query_idx];
    let query_title = paper_titles.get(&query_id).unwrap();
    let query_domain = paper_domains.get(&query_id).unwrap();

    println!("  Query paper: \"{}\" ({})", query_title, query_domain);
    println!();

    // Search using PCA vectors
    let query_vec: Vec<f32> = projected[query_idx].iter().map(|&x| x as f32).collect();
    let pca_neighbors = client.vector_search("Paper", "pca_embedding", &query_vec, 10)
        .await
        .expect("PCA vector search failed");

    println!("  Top 10 similar papers (PCA 3D Euclidean distance):");
    for (rank, (nid, distance)) in pca_neighbors.iter().enumerate() {
        let pid = nid.as_u64();
        let title = paper_titles.get(&pid).map(|s| s.as_str()).unwrap_or("?");
        let domain = paper_domains.get(&pid).map(|s| s.as_str()).unwrap_or("?");
        let marker = if domain == query_domain.as_str() { "*" } else { " " };
        println!(
            "    {:2}. [dist={:.4}] {} \"{}\" ({})",
            rank + 1,
            distance,
            marker,
            title,
            domain,
        );
    }
    println!("  (* = same domain as query)");
    println!();

    // ====================================================================
    // Step 6: Compare Full-Dimensional vs PCA-Reduced Similarity
    // ====================================================================
    println!("Step 6: Full-Dimensional vs PCA-Reduced Similarity Comparison");
    separator();

    // Build full-dimensional HNSW index for comparison
    client.create_vector_index("Paper", "full_embedding", FEATURES.len(), DistanceMetric::L2)
        .await
        .expect("Failed to create full vector index");

    for (i, &pid) in paper_ids.iter().enumerate() {
        let nid = NodeId::new(pid);
        // Standardize features for fair comparison (use PCA mean/std_dev)
        let embedding: Vec<f32> = feature_matrix[i]
            .iter()
            .enumerate()
            .map(|(j, &x)| {
                let centered = x - pca_result.mean[j];
                let scaled = if pca_result.std_dev[j] > 0.0 {
                    centered / pca_result.std_dev[j]
                } else {
                    centered
                };
                scaled as f32
            })
            .collect();
        client.add_vector("Paper", "full_embedding", nid, &embedding)
            .await
            .expect("Failed to add full vector");
    }

    // Search with full-dimensional vectors
    let query_full: Vec<f32> = feature_matrix[query_idx]
        .iter()
        .enumerate()
        .map(|(j, &x)| {
            let centered = x - pca_result.mean[j];
            let scaled = if pca_result.std_dev[j] > 0.0 {
                centered / pca_result.std_dev[j]
            } else {
                centered
            };
            scaled as f32
        })
        .collect();

    let full_neighbors = client.vector_search("Paper", "full_embedding", &query_full, 10)
        .await
        .expect("Full vector search failed");

    // Compute overlap
    let pca_set: std::collections::HashSet<u64> =
        pca_neighbors.iter().map(|(nid, _)| nid.as_u64()).collect();
    let full_set: std::collections::HashSet<u64> =
        full_neighbors.iter().map(|(nid, _)| nid.as_u64()).collect();
    let overlap = pca_set.intersection(&full_set).count();

    println!("  Full 8D neighbors vs PCA 3D neighbors:");
    println!("    Overlap in top-10: {}/10 ({:.0}%)", overlap, overlap as f64 * 10.0);
    println!();

    // Side-by-side comparison
    println!("  {:40}  |  {:40}", "Full 8D", "PCA 3D");
    println!("  {:40}  |  {:40}", "-".repeat(40), "-".repeat(40));
    for rank in 0..10 {
        let (f_nid, f_dist) = &full_neighbors[rank];
        let f_title = paper_titles.get(&f_nid.as_u64()).map(|s| s.as_str()).unwrap_or("?");
        let f_domain = paper_domains.get(&f_nid.as_u64()).map(|s| s.as_str()).unwrap_or("?");

        let (p_nid, p_dist) = &pca_neighbors[rank];
        let p_title = paper_titles.get(&p_nid.as_u64()).map(|s| s.as_str()).unwrap_or("?");
        let p_domain = paper_domains.get(&p_nid.as_u64()).map(|s| s.as_str()).unwrap_or("?");

        let f_label = format!("{}. {:.3} {}", rank + 1, f_dist, truncate(f_title, 20));
        let p_label = format!("{}. {:.3} {}", rank + 1, p_dist, truncate(p_title, 20));
        println!("  {:40}  |  {:40}", f_label, p_label);
    }
    println!();

    // ====================================================================
    // Step 7: Domain Clustering Analysis with PCA
    // ====================================================================
    println!("Step 7: Domain Clustering Analysis via PCA");
    separator();

    // Compute centroid per domain in PCA space
    let mut domain_centroids: HashMap<String, Vec<f64>> = HashMap::new();
    let mut domain_counts: HashMap<String, usize> = HashMap::new();

    for (i, &pid) in paper_ids.iter().enumerate() {
        let domain = paper_domains.get(&pid).unwrap().clone();
        let coords = &projected[i];

        let centroid = domain_centroids.entry(domain.clone()).or_insert_with(|| vec![0.0; n_components]);
        for (j, &c) in coords.iter().enumerate() {
            centroid[j] += c;
        }
        *domain_counts.entry(domain).or_insert(0) += 1;
    }

    // Normalize centroids
    for (domain, centroid) in &mut domain_centroids {
        let count = domain_counts[domain] as f64;
        for c in centroid.iter_mut() {
            *c /= count;
        }
    }

    // Print domain centroids sorted by PC1
    let mut sorted_domains: Vec<_> = domain_centroids.iter().collect();
    sorted_domains.sort_by(|a, b| a.1[0].partial_cmp(&b.1[0]).unwrap());

    println!("  Domain centroids in PCA space (sorted by PC1):");
    println!("  {:25} {:>10} {:>10} {:>10}", "Domain", "PC1", "PC2", "PC3");
    println!("  {:25} {:>10} {:>10} {:>10}", "-".repeat(25), "-".repeat(10), "-".repeat(10), "-".repeat(10));
    for (domain, centroid) in &sorted_domains {
        println!(
            "  {:25} {:>10.3} {:>10.3} {:>10.3}",
            domain,
            centroid[0],
            centroid[1],
            centroid[2],
        );
    }
    println!();

    // Compute inter-domain distances
    println!("  Inter-domain distances (Euclidean in PCA space):");
    let domains_vec: Vec<&String> = sorted_domains.iter().map(|(d, _)| *d).collect();
    println!("  Closest domain pairs:");
    let mut all_pairs: Vec<(f64, &str, &str)> = Vec::new();
    for i in 0..domains_vec.len() {
        for j in (i + 1)..domains_vec.len() {
            let c1 = &domain_centroids[domains_vec[i]];
            let c2 = &domain_centroids[domains_vec[j]];
            let dist: f64 = c1.iter().zip(c2.iter()).map(|(a, b)| (a - b).powi(2)).sum::<f64>().sqrt();
            all_pairs.push((dist, domains_vec[i], domains_vec[j]));
        }
    }
    all_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    for (dist, d1, d2) in all_pairs.iter().take(5) {
        println!("    {:.4}  {} <-> {}", dist, d1, d2);
    }
    println!("  Most distant domain pairs:");
    for (dist, d1, d2) in all_pairs.iter().rev().take(3) {
        println!("    {:.4}  {} <-> {}", dist, d1, d2);
    }
    println!();

    // ====================================================================
    // Step 8: Cypher Queries on the Paper Network
    // ====================================================================
    println!("Step 8: Cypher Queries on Paper Network");
    separator();

    // Top cited papers
    let result = client
        .query_readonly(
            "default",
            "MATCH (p:Paper) RETURN p.title, p.citation_count, p.domain ORDER BY p.citation_count DESC LIMIT 5",
        )
        .await
        .unwrap();
    println!("  Top 5 papers by citation count:");
    for row in &result.records {
        if row.len() >= 3 {
            let title = row[0].as_str().unwrap_or("?");
            let cites = &row[1];
            let domain = row[2].as_str().unwrap_or("?");
            println!("    {} (citations: {}, domain: {})", truncate(title, 40), cites, domain);
        }
    }
    println!();

    // Count papers per domain
    let result = client
        .query_readonly(
            "default",
            "MATCH (p:Paper) RETURN p.domain, count(p) AS paper_count ORDER BY paper_count DESC",
        )
        .await
        .unwrap();
    println!("  Papers per domain:");
    for row in &result.records {
        if row.len() >= 2 {
            let domain = row[0].as_str().unwrap_or("?");
            let count = &row[1];
            println!("    {:25} {}", domain, count);
        }
    }
    println!();

    // ====================================================================
    // Summary
    // ====================================================================
    println!("============================================================================================");
    println!("  Summary");
    println!("============================================================================================");
    println!("  Graph: {} papers, {} citations across {} domains", total_papers, edge_count, DOMAINS.len());
    println!("  PCA: Reduced {}D features -> {}D components", FEATURES.len(), n_components);
    let total_var: f64 = pca_result.explained_variance_ratio.iter().sum();
    println!(
        "  Explained variance: {:.1}% in {} components",
        total_var * 100.0,
        n_components,
    );
    println!("  Top-10 neighbor overlap (full vs PCA): {}/10", overlap);
    println!("  HNSW index: {}D vectors, {} papers indexed", n_components, total_papers);
    println!();
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
