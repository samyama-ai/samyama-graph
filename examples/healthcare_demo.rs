//! Clinical Trials Optimization: Patient Matching & Site Selection
//!
//! Features:
//! - **Vector Search:** Match patient unstructured records to trial protocols.
//! - **Graph:** Knowledge Graph of Conditions and Drugs (Phase 5 data).
//! - **Optimization:** Multi-Objective Site Selection (Cost vs Recruitment).

use samyama::
{
    GraphStore,
    Label,
    EdgeType,
    PropertyValue,
    PersistenceManager,
    QueryEngine,
    persistence::tenant::{AutoEmbedConfig, LLMProvider},
};
use samyama_optimization::algorithms::NSGA2Solver;
use samyama_optimization::common::{MultiObjectiveProblem, SolverConfig};
use ndarray::Array1;
use std::collections::HashMap;
use std::sync::Arc;
use std::io::Write;

fn pause() {
    print!("\n👉 Press Enter to continue...");
    std::io::stdout().flush().unwrap();
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer).unwrap();
    println!();
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    println!("🏥 ClinicalTrialsAI: Patient Matching & Site Optimization");

    // 1. Setup
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    let tenant_id = "pharma_r_and_d";
    
    // Auto-Embed for Trial Protocols
    let embed_config = AutoEmbedConfig {
        provider: LLMProvider::Mock,
        embedding_model: "text-embedding-004".to_string(),
        api_key: Some("mock".to_string()),
        api_base_url: None,
        chunk_size: 200,
        chunk_overlap: 20,
        vector_dimension: 64,
        embedding_policies: HashMap::from([
            ("Trial".to_string(), vec!["criteria".to_string()])
        ]),
    };
    persistence.tenants().create_tenant(tenant_id.to_string(), "Clinical R&D".to_string(), None).unwrap();
    persistence.tenants().update_embed_config(tenant_id, Some(embed_config)).unwrap();

    let (graph, _rx) = GraphStore::with_async_indexing();
    let store = Arc::new(tokio::sync::RwLock::new(graph));

    // Create Vector Index
    {
        let g = store.read().await;
        g.create_vector_index("Trial", "criteria", 64, samyama::vector::DistanceMetric::Cosine).unwrap();
    }

    pause();

    // 2. Knowledge Graph Ingestion
    println!("\n[Step 1] Building Knowledge Graph...");
    {
        let mut g = store.write().await;
        let engine = QueryEngine::new();

        // Trials
        engine.execute_mut("CREATE (t:Trial {id: 'NCT01234567', title: 'Immunotherapy for NSCLC', criteria: 'Stage III/IV Non-Small Cell Lung Cancer, PD-L1 positive.'})", &mut g, tenant_id).unwrap();
        engine.execute_mut("CREATE (t:Trial {id: 'NCT09876543', title: 'Targeted Therapy for EGFR+', criteria: 'Advanced NSCLC with EGFR exon 19 deletion.'})", &mut g, tenant_id).unwrap();
        
        // Conditions & Drugs
        engine.execute_mut("CREATE (c:Condition {name: 'Non-Small Cell Lung Cancer'})", &mut g, tenant_id).unwrap();
        engine.execute_mut("CREATE (d:Drug {name: 'Pembrolizumab'})", &mut g, tenant_id).unwrap();
        
        // Relationships
        let trial = g.get_nodes_by_label(&Label::new("Trial"))[0].id;
        let cond = g.get_nodes_by_label(&Label::new("Condition"))[0].id;
        let drug = g.get_nodes_by_label(&Label::new("Drug"))[0].id;
        
        g.create_edge(trial, cond, EdgeType::new("STUDIES")).unwrap();
        g.create_edge(trial, drug, EdgeType::new("TESTS")).unwrap();

        println!("   ✓ Ingested Trials, Conditions, Drugs.");
    }

    pause();

    // 3. Patient Matching (Vector Search)
    println!("\n[Step 2] Matching Patient to Trials (Vector Search)...");
    println!("   Patient Profile: '55yo male, Stage IV Lung Cancer, PD-L1 high expression.'");
    
    {
        let g = store.read().await;
        // Mock query vector for patient profile
        let patient_vec = vec![0.1f32; 64]; 
        let results = g.vector_search("Trial", "criteria", &patient_vec, 2).unwrap();
        
        for (id, score) in results {
            let node = g.get_node(id).unwrap();
            let title = node.get_property("title").unwrap().as_string().unwrap();
            println!("   ✅ MATCH: {} (Score: {:.4})", title, score);
        }
    }

    pause();

    // 4. Site Selection Optimization
    println!("\n[Step 3] Optimizing Site Selection (Multi-Objective)...");
    println!("   Goal: Select 5 sites to Maximize Recruitment Rate AND Minimize Cost.");
    println!("   Constraint: Total Budget < $2M.");

    struct SiteSelectionProblem;
    impl MultiObjectiveProblem for SiteSelectionProblem {
        fn dim(&self) -> usize { 10 } // 10 potential sites
        fn num_objectives(&self) -> usize { 2 } // Cost, Recruitment
        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::from_elem(10, 0.0), Array1::from_elem(10, 1.0)) // Binary selection (relaxed to continuous 0-1)
        }
        
        fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
            let mut cost = 0.0;
            let mut recruitment = 0.0;
            
            // Mock data for 10 sites
            let site_costs = [50000., 60000., 45000., 80000., 70000., 55000., 90000., 40000., 65000., 75000.];
            let site_recruitment = [10., 15., 8., 20., 18., 12., 25., 5., 16., 19.];

            for i in 0..10 {
                // Soft decision: x[i] > 0.5 means selected
                if x[i] > 0.5 {
                    cost += site_costs[i];
                    recruitment += site_recruitment[i]; // Maximize recruitment = Minimize negative
                }
            }
            
            vec![cost, -recruitment]
        }

        fn penalties(&self, x: &Array1<f64>) -> Vec<f64> {
            // Constraint: Select exactly 5 sites (Soft constraint)
            let selected_count: f64 = x.iter().map(|&v| if v > 0.5 { 1.0 } else { 0.0 }).sum();
            if (selected_count - 5.0).abs() > 0.1 {
                vec![1000.0] // High penalty
            } else {
                vec![0.0]
            }
        }
    }

    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 50,
        max_iterations: 100,
    });
    
    let result = solver.solve(&SiteSelectionProblem);
    
    // Pick best trade-off from Pareto front
    if let Some(best) = result.pareto_front.first() {
        let cost = best.fitness[0];
        let recruitment = -best.fitness[1];
        println!("   🏆 Optimal Configuration Found:");
        println!("      Total Cost: ${:.0}", cost);
        println!("      Est. Recruitment: {:.0} patients/month", recruitment);
        println!("      Selected Sites Indices: {:?}", 
            best.variables.iter().enumerate()
                .filter(|(_, &v)| v > 0.5)
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        );
    } else {
        println!("   ⚠️ No feasible solution found.");
    }

    pause();
    println!("\n✅ DEMO COMPLETE.");
}