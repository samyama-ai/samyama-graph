use samyama::{GraphStore, NodeId, QueryEngine, RespServer, ServerConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("Samyama Graph Database v{}", samyama::version());
    println!("==========================================");
    println!();

    demo_property_graph();
    demo_cypher_queries();

    println!("\n=== Starting RESP Server ===");
    println!("Connect with: redis-cli");
    println!("Example: GRAPH.QUERY sandbox \"MATCH (n:Disease) RETURN n.name LIMIT 10\"");
    println!();

    start_server().await;
}

fn demo_property_graph() {
    println!("=== Demo 1: Property Graph ===");
    let mut store = GraphStore::new();

    let alice = store.create_node("Person");
    if let Some(node) = store.get_node_mut(alice) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
        println!("Created Person: Alice");
    }

    let bob = store.create_node("Person");
    if let Some(node) = store.get_node_mut(bob) {
        node.set_property("name", "Bob");
        node.set_property("age", 25i64);
        println!("Created Person: Bob");
    }

    store.create_edge(alice, bob, "KNOWS").unwrap();
    println!("Created: Alice -[KNOWS]-> Bob");
    println!("Total nodes: {}, edges: {}", store.node_count(), store.edge_count());
}

fn demo_cypher_queries() {
    println!("\n=== Demo 2: OpenCypher Queries ===");
    let mut store = GraphStore::new();

    let alice = store.create_node("Person");
    if let Some(node) = store.get_node_mut(alice) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
    }

    let bob = store.create_node("Person");
    if let Some(node) = store.get_node_mut(bob) {
        node.set_property("name", "Bob");
        node.set_property("age", 25i64);
    }

    store.create_edge(alice, bob, "KNOWS").unwrap();

    let engine = QueryEngine::new();
    if let Ok(result) = engine.execute("MATCH (n:Person) RETURN n", &store) {
        println!("Query executed: Found {} persons", result.len());
    }
}

fn load_clinical_trials_data(store: &mut GraphStore) -> (HashMap<String, NodeId>, HashMap<String, NodeId>) {
    println!("Loading clinical trials data...");
    
    // Diseases
    let diseases = [
        "Diabetes Mellitus", "Hypertension", "Asthma", "Alzheimer Disease",
        "Parkinson Disease", "Breast Neoplasms", "Rheumatoid Arthritis",
        "Anxiety Disorders", "Epilepsy", "Migraine Disorders", "Osteoporosis",
        "Obesity", "Heart Failure", "Coronary Artery Disease", "Stroke",
        "Hepatitis C", "HIV Infections"
    ];
    
    let mut disease_ids: HashMap<String, NodeId> = HashMap::new();
    for name in &diseases {
        let id = store.create_node("Disease");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
        }
        disease_ids.insert(name.to_string(), id);
    }
    println!("  Created {} disease nodes", diseases.len());
    
    // Drugs
    let drugs = [
        "Insulin", "Aspirin", "Ibuprofen", "Acetaminophen", "Lisinopril",
        "Atorvastatin", "Metoprolol", "Amlodipine", "Omeprazole", "Gabapentin",
        "Sertraline", "Fluoxetine", "Prednisone", "Warfarin", "Clopidogrel",
        "Levothyroxine", "Albuterol", "Amoxicillin"
    ];
    
    let mut drug_ids: HashMap<String, NodeId> = HashMap::new();
    for name in &drugs {
        let id = store.create_node("Drug");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
        }
        drug_ids.insert(name.to_string(), id);
    }
    println!("  Created {} drug nodes", drugs.len());
    
    // Co-occurrence relationships
    let relationships: &[(&str, &str, i64)] = &[
        ("Diabetes Mellitus", "Insulin", 312),
        ("Diabetes Mellitus", "Aspirin", 6),
        ("Diabetes Mellitus", "Atorvastatin", 1),
        ("Diabetes Mellitus", "Gabapentin", 3),
        ("Diabetes Mellitus", "Clopidogrel", 4),
        ("Diabetes Mellitus", "Levothyroxine", 1),
        ("Hypertension", "Insulin", 6),
        ("Hypertension", "Aspirin", 8),
        ("Hypertension", "Ibuprofen", 2),
        ("Hypertension", "Acetaminophen", 2),
        ("Hypertension", "Lisinopril", 3),
        ("Hypertension", "Atorvastatin", 4),
        ("Hypertension", "Metoprolol", 6),
        ("Hypertension", "Amlodipine", 35),
        ("Hypertension", "Levothyroxine", 5),
        ("Asthma", "Aspirin", 19),
        ("Asthma", "Prednisone", 4),
        ("Asthma", "Albuterol", 67),
        ("Asthma", "Levothyroxine", 2),
        ("Alzheimer Disease", "Insulin", 20),
        ("Alzheimer Disease", "Aspirin", 1),
        ("Alzheimer Disease", "Atorvastatin", 1),
        ("Parkinson Disease", "Insulin", 7),
        ("Parkinson Disease", "Aspirin", 2),
        ("Parkinson Disease", "Sertraline", 1),
        ("Parkinson Disease", "Fluoxetine", 1),
        ("Breast Neoplasms", "Insulin", 4),
        ("Breast Neoplasms", "Aspirin", 6),
        ("Breast Neoplasms", "Atorvastatin", 3),
        ("Rheumatoid Arthritis", "Insulin", 5),
        ("Rheumatoid Arthritis", "Prednisone", 11),
        ("Rheumatoid Arthritis", "Levothyroxine", 3),
        ("Anxiety Disorders", "Gabapentin", 1),
        ("Anxiety Disorders", "Sertraline", 21),
        ("Anxiety Disorders", "Fluoxetine", 11),
        ("Epilepsy", "Gabapentin", 8),
        ("Epilepsy", "Atorvastatin", 2),
        ("Epilepsy", "Fluoxetine", 2),
        ("Migraine Disorders", "Aspirin", 4),
        ("Migraine Disorders", "Ibuprofen", 11),
        ("Migraine Disorders", "Acetaminophen", 13),
        ("Migraine Disorders", "Metoprolol", 2),
        ("Migraine Disorders", "Gabapentin", 2),
        ("Osteoporosis", "Insulin", 4),
        ("Osteoporosis", "Atorvastatin", 2),
        ("Osteoporosis", "Prednisone", 3),
        ("Osteoporosis", "Levothyroxine", 8),
        ("Obesity", "Insulin", 82),
        ("Obesity", "Aspirin", 3),
        ("Obesity", "Fluoxetine", 2),
        ("Obesity", "Levothyroxine", 6),
        ("Heart Failure", "Insulin", 7),
        ("Heart Failure", "Aspirin", 7),
        ("Heart Failure", "Metoprolol", 4),
        ("Heart Failure", "Warfarin", 5),
        ("Heart Failure", "Levothyroxine", 4),
        ("Coronary Artery Disease", "Aspirin", 67),
        ("Coronary Artery Disease", "Atorvastatin", 9),
        ("Coronary Artery Disease", "Clopidogrel", 51),
        ("Coronary Artery Disease", "Warfarin", 4),
        ("Stroke", "Aspirin", 44),
        ("Stroke", "Atorvastatin", 13),
        ("Stroke", "Warfarin", 22),
        ("Stroke", "Clopidogrel", 30),
        ("Hepatitis C", "Aspirin", 3),
        ("Hepatitis C", "Atorvastatin", 3),
        ("Hepatitis C", "Prednisone", 2),
        ("HIV Infections", "Insulin", 3),
        ("HIV Infections", "Aspirin", 1),
        ("HIV Infections", "Prednisone", 1),
    ];
    
    let mut edge_count = 0;
    for (disease_name, drug_name, count) in relationships {
        if let (Some(&d_id), Some(&dr_id)) = (disease_ids.get(*disease_name), drug_ids.get(*drug_name)) {
            if let Ok(edge_id) = store.create_edge(d_id, dr_id, "CO_OCCURS_WITH") {
                if let Some(edge) = store.get_edge_mut(edge_id) {
                    edge.set_property("count", *count);
                }
                edge_count += 1;
            }
        }
    }
    println!("  Created {} relationships", edge_count);
    println!("Clinical trials data loaded successfully!");
    
    (disease_ids, drug_ids)
}

fn load_hetionet_data(store: &mut GraphStore, disease_ids: &mut HashMap<String, NodeId>) {
    println!("\nLoading Hetionet data...");
    
    let nodes_path = "/tmp/clinical_nodes.tsv";
    let edges_path = "/tmp/clinical_edges.tsv";
    
    if !std::path::Path::new(nodes_path).exists() || !std::path::Path::new(edges_path).exists() {
        println!("  Hetionet data files not found at /tmp/, skipping...");
        return;
    }
    
    let mut compound_ids: HashMap<String, NodeId> = HashMap::new();
    let mut symptom_ids: HashMap<String, NodeId> = HashMap::new();
    let mut hetionet_disease_ids: HashMap<String, NodeId> = HashMap::new();
    
    let mut compound_count = 0;
    let mut symptom_count = 0;
    let mut disease_count = 0;
    
    if let Ok(file) = File::open(nodes_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 { continue; }
            
            let node_id = parts[0];
            let name = parts[1];
            let node_type = parts[2];
            
            match node_type {
                "Compound" => {
                    let id = store.create_node("Compound");
                    if let Some(node) = store.get_node_mut(id) {
                        node.set_property("name", name);
                        node.set_property("hetionet_id", node_id);
                    }
                    compound_ids.insert(node_id.to_string(), id);
                    compound_count += 1;
                },
                "Disease" => {
                    if let Some(&existing_id) = disease_ids.get(name) {
                        hetionet_disease_ids.insert(node_id.to_string(), existing_id);
                        if let Some(node) = store.get_node_mut(existing_id) {
                            node.set_property("hetionet_id", node_id);
                        }
                    } else {
                        let id = store.create_node("Disease");
                        if let Some(node) = store.get_node_mut(id) {
                            node.set_property("name", name);
                            node.set_property("hetionet_id", node_id);
                        }
                        hetionet_disease_ids.insert(node_id.to_string(), id);
                        disease_ids.insert(name.to_string(), id);
                    }
                    disease_count += 1;
                },
                "Symptom" => {
                    let id = store.create_node("Symptom");
                    if let Some(node) = store.get_node_mut(id) {
                        node.set_property("name", name);
                        node.set_property("hetionet_id", node_id);
                    }
                    symptom_ids.insert(node_id.to_string(), id);
                    symptom_count += 1;
                },
                _ => {}
            }
        }
    }
    println!("  Loaded {} compounds, {} diseases, {} symptoms", compound_count, disease_count, symptom_count);
    
    let mut treats_count = 0;
    let mut palliates_count = 0;
    let mut presents_count = 0;
    let mut resembles_count = 0;
    
    if let Ok(file) = File::open(edges_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 { continue; }
            
            let source_id = parts[0];
            let rel_type = parts[1];
            let target_id = parts[2];
            
            match rel_type {
                "CtD" => {
                    if let (Some(&c_id), Some(&d_id)) = (compound_ids.get(source_id), hetionet_disease_ids.get(target_id)) {
                        if store.create_edge(c_id, d_id, "TREATS").is_ok() {
                            treats_count += 1;
                        }
                    }
                },
                "CpD" => {
                    if let (Some(&c_id), Some(&d_id)) = (compound_ids.get(source_id), hetionet_disease_ids.get(target_id)) {
                        if store.create_edge(c_id, d_id, "PALLIATES").is_ok() {
                            palliates_count += 1;
                        }
                    }
                },
                "DpS" => {
                    if let (Some(&d_id), Some(&s_id)) = (hetionet_disease_ids.get(source_id), symptom_ids.get(target_id)) {
                        if store.create_edge(d_id, s_id, "PRESENTS").is_ok() {
                            presents_count += 1;
                        }
                    }
                },
                "DrD" => {
                    if let (Some(&d1_id), Some(&d2_id)) = (hetionet_disease_ids.get(source_id), hetionet_disease_ids.get(target_id)) {
                        if store.create_edge(d1_id, d2_id, "RESEMBLES").is_ok() {
                            resembles_count += 1;
                        }
                    }
                },
                _ => {}
            }
        }
    }
    println!("  Loaded {} TREATS, {} PALLIATES, {} PRESENTS, {} RESEMBLES edges", 
             treats_count, palliates_count, presents_count, resembles_count);
    println!("Hetionet data loaded successfully!");
}

fn load_phegeni_data(store: &mut GraphStore) {
    println!("\nLoading PheGenI data...");
    
    let phegeni_path = "/tmp/phegeni.tsv";
    
    if !std::path::Path::new(phegeni_path).exists() {
        println!("  PheGenI data file not found at /tmp/, skipping...");
        return;
    }
    
    let mut phenotype_ids: HashMap<String, NodeId> = HashMap::new();
    let mut gene_ids: HashMap<String, NodeId> = HashMap::new();
    let mut association_count = 0;
    
    if let Ok(file) = File::open(phegeni_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 4 { continue; }
            
            let phenotype = parts[0];
            let gene_symbol = parts[2];
            let gene_ncbi_id = parts[3];
            
            let p_id = *phenotype_ids.entry(phenotype.to_string()).or_insert_with(|| {
                let id = store.create_node("Phenotype");
                if let Some(node) = store.get_node_mut(id) {
                    node.set_property("name", phenotype);
                }
                id
            });
            
            let gene_key = format!("{}:{}", gene_symbol, gene_ncbi_id);
            let g_id = *gene_ids.entry(gene_key).or_insert_with(|| {
                let id = store.create_node("Gene");
                if let Some(node) = store.get_node_mut(id) {
                    node.set_property("symbol", gene_symbol);
                    node.set_property("ncbi_id", gene_ncbi_id);
                }
                id
            });
            
            if store.create_edge(p_id, g_id, "ASSOCIATED_WITH").is_ok() {
                association_count += 1;
            }
        }
    }
    
    println!("  Loaded {} phenotypes, {} genes, {} associations", 
             phenotype_ids.len(), gene_ids.len(), association_count);
    println!("PheGenI data loaded successfully!");
}

async fn start_server() {
    let (mut graph, rx) = GraphStore::with_async_indexing();

    {
        let alice = graph.create_node("Person");
        if let Some(node) = graph.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = graph.create_node("Person");
        if let Some(node) = graph.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        graph.create_edge(alice, bob, "KNOWS").unwrap();
        
        let (mut disease_ids, _drug_ids) = load_clinical_trials_data(&mut graph);
        load_hetionet_data(&mut graph, &mut disease_ids);
        load_phegeni_data(&mut graph);
        load_aact_data(&mut graph, &disease_ids);
        
        println!("\nGraph Statistics:");
        println!("  Total nodes: {}", graph.node_count());
        println!("  Total edges: {}", graph.edge_count());
    }

    let store = Arc::new(RwLock::new(graph));

    let mut config = ServerConfig::default();
    config.address = std::env::args().find(|a| a.starts_with("--host"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--host").nth(1))
        .unwrap_or_else(|| "127.0.0.1".to_string());
    config.port = std::env::args().find(|a| a.starts_with("--port"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--port").nth(1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);
    
    println!("\nServer starting on {}:{}", config.address, config.port);
    
    // Initialize persistence if path is configured
    let persistence = if let Some(path) = &config.data_path {
        let pm = Arc::new(samyama::PersistenceManager::new(path).expect("Failed to initialize persistence"));
        pm.start_indexer(&*store.read().await, rx);
        Some(pm)
    } else {
        None
    };

    let server = if let Some(pm) = persistence {
        RespServer::new_with_persistence(config, store, pm)
    } else {
        RespServer::new(config, store)
    };

    println!("Server ready. Press Ctrl+C to stop.\n");

    if let Err(e) = server.start().await {
        eprintln!("Server error: {}", e);
    }
}

fn load_aact_data(store: &mut GraphStore, disease_ids: &HashMap<String, NodeId>) {
    println!("\nLoading AACT Clinical Trials data...");

    let trials_path = "/tmp/aact_trials.tsv";
    let conditions_path = "/tmp/aact_conditions.tsv";
    let sponsors_path = "/tmp/aact_sponsors.tsv";
    let edges_studies_path = "/tmp/aact_edges_studies.tsv";
    let edges_sponsored_path = "/tmp/aact_edges_sponsored.tsv";
    let enriched_edges_path = "/tmp/enriched/enriched_trial_disease_edges.tsv";
    
    if !std::path::Path::new(trials_path).exists() {
        println!("  AACT data files not found, skipping...");
        return;
    }
    
    let mut trial_ids: HashMap<String, NodeId> = HashMap::new();
    let mut condition_ids: HashMap<String, NodeId> = HashMap::new();
    let mut sponsor_ids: HashMap<String, NodeId> = HashMap::new();
    
    // Load trials
    let mut trial_count = 0;
    if let Ok(file) = File::open(trials_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 { continue; }
            
            let trial_key = parts[0];
            let title = parts[1];
            let phase = if parts.len() > 3 { parts[3] } else { "Unknown" };
            
            let id = store.create_node("ClinicalTrial");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("trial_id", trial_key);
                node.set_property("title", title);
                node.set_property("phase", phase);
            }
            trial_ids.insert(trial_key.to_string(), id);
            trial_count += 1;
        }
    }
    println!("  Loaded {} clinical trials", trial_count);
    
    // Load conditions
    let mut condition_count = 0;
    if let Ok(file) = File::open(conditions_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 { continue; }
            
            let cond_key = parts[0];
            let name = parts[1];
            
            let id = store.create_node("MedicalCondition");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", name);
            }
            condition_ids.insert(cond_key.to_string(), id);
            condition_count += 1;
        }
    }
    println!("  Loaded {} medical conditions", condition_count);
    
    // Load sponsors
    let mut sponsor_count = 0;
    if let Ok(file) = File::open(sponsors_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 { continue; }
            
            let sponsor_key = parts[0];
            let name = parts[1];
            
            let id = store.create_node("Sponsor");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", name);
            }
            sponsor_ids.insert(sponsor_key.to_string(), id);
            sponsor_count += 1;
        }
    }
    println!("  Loaded {} sponsors", sponsor_count);
    
    // Load STUDIES edges (Trial -> Condition)
    let mut studies_count = 0;
    if let Ok(file) = File::open(edges_studies_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 { continue; }
            
            let trial_key = parts[0];
            let cond_key = parts[2];
            
            if let (Some(&t_id), Some(&c_id)) = (trial_ids.get(trial_key), condition_ids.get(cond_key)) {
                if store.create_edge(t_id, c_id, "STUDIES").is_ok() {
                    studies_count += 1;
                }
            }
        }
    }
    println!("  Loaded {} STUDIES relationships", studies_count);
    
    // Load SPONSORS edges (Sponsor -> Trial)
    let mut sponsored_count = 0;
    if let Ok(file) = File::open(edges_sponsored_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 { continue; }
            
            let sponsor_key = parts[0];
            let trial_key = parts[2];
            
            if let (Some(&s_id), Some(&t_id)) = (sponsor_ids.get(sponsor_key), trial_ids.get(trial_key)) {
                if store.create_edge(s_id, t_id, "SPONSORS").is_ok() {
                    sponsored_count += 1;
                }
            }
        }
    }
    println!("  Loaded {} SPONSORS relationships", sponsored_count);

    // Load enriched trial-disease edges (Trial -> Disease via TREATS)
    let mut enriched_count = 0;
    if let Ok(file) = File::open(enriched_edges_path) {
        let reader = BufReader::new(file);
        let mut first_line = true;
        for line in reader.lines().filter_map(|l| l.ok()) {
            // Skip header line
            if first_line {
                first_line = false;
                continue;
            }

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 4 { continue; }

            let trial_key = parts[0];
            let disease_name = parts[1];
            let confidence: f64 = parts[3].parse().unwrap_or(0.0);

            if let (Some(&t_id), Some(&d_id)) = (trial_ids.get(trial_key), disease_ids.get(disease_name)) {
                if let Ok(edge_id) = store.create_edge(t_id, d_id, "TREATS") {
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        edge.set_property("confidence", confidence.to_string().as_str());
                    }
                    enriched_count += 1;
                }
            }
        }
        println!("  Loaded {} enriched TREATS relationships (Trial->Disease)", enriched_count);
    } else {
        println!("  Enriched edges file not found, skipping enriched relationships");
    }

    println!("AACT data loaded successfully!");
}
