//! CyberSecurityGuardian: Threat Detection & Response
//!
//! Features:
//! - **Graph:** Network Topology (Servers, Users, Firewall)
//! - **Vector Search:** Threat Signature Matching (CVEs)
//! - **Agents:** Automated Log Analysis & Isolation
//! - **Raft:** HA State (Simulated)

use samyama::
{
    GraphStore, Label, EdgeType, PropertyValue, PersistenceManager, QueryEngine,
    agent::{AgentRuntime, tools::WebSearchTool},
    persistence::tenant::{AgentConfig, AutoEmbedConfig, LLMProvider},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
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
    println!("🛡️  CyberSecurityGuardian: Threat Detection System");
    println!("   Monitoring Network Traffic & Logs...");

    // 1. Setup
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    let tenant_id = "sec_ops";

    // Configure Agent for Threat Analysis
    let agent_config = AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "cyber-sentinel-v1".to_string(),
        api_key: Some("mock".to_string()),
        api_base_url: None,
        system_prompt: Some("You are a Tier 3 Security Analyst.".to_string()),
        tools: vec![],
        policies: HashMap::from([
            ("Alert".to_string(), "Correlate with Threat Intel and recommend action.".to_string())
        ]),
    };
    persistence.tenants().create_tenant(tenant_id.to_string(), "Security Operations".to_string(), None).unwrap();
    persistence.tenants().update_agent_config(tenant_id, Some(agent_config)).unwrap();

    let (graph, _rx) = GraphStore::with_async_indexing();
    let store = Arc::new(tokio::sync::RwLock::new(graph));

    // Create Vector Index for Threat Signatures
    {
        let g = store.read().await;
        g.create_vector_index("ThreatIntel", "description", 64, samyama::vector::DistanceMetric::Cosine).unwrap();
    }

    pause();

    // 2. Ingest Network Topology
    println!("\n[Step 1] Ingesting Network Topology...");
    {
        let mut g = store.write().await;
        let engine = QueryEngine::new();

        // Firewall
        engine.execute_mut("CREATE (fw:Firewall {name: 'Perimeter-FW-01', ip: '192.168.1.1'})", &mut g, tenant_id).unwrap();
        let fw_id = g.get_nodes_by_label(&Label::new("Firewall"))[0].id;

        // Servers
        engine.execute_mut("CREATE (s:Server {name: 'Web-Srv-01', ip: '192.168.1.10', os: 'Linux'})", &mut g, tenant_id).unwrap();
        engine.execute_mut("CREATE (s:Server {name: 'DB-Srv-01', ip: '192.168.1.20', os: 'Linux'})", &mut g, tenant_id).unwrap();
        
        let servers = g.get_nodes_by_label(&Label::new("Server"));
        for s in servers {
            g.create_edge(fw_id, s.id, EdgeType::new("PROTECTS")).unwrap();
        }

        // Users
        engine.execute_mut("CREATE (u:User {name: 'Alice', role: 'Admin'})", &mut g, tenant_id).unwrap();
        let user_id = g.get_nodes_by_label(&Label::new("User"))[0].id;
        
        // Logins
        let db_id = g.get_nodes_by_label(&Label::new("Server")).into_iter().find(|n| n.properties.get("name").unwrap().as_string().unwrap() == "DB-Srv-01").unwrap().id;
        g.create_edge(user_id, db_id, EdgeType::new("HAS_ACCESS")).unwrap();

        println!("   ✓ Network Graph Built: Firewall -> Servers <- Users");
    }

    pause();

    // 3. Ingest Threat Intelligence (Vectors)
    println!("\n[Step 2] Loading Threat Intelligence (Vectors)...");
    {
        let mut g = store.write().await;
        // Mock embeddings for CVEs
        // In real app, Auto-Embed would generate these from text
        let cve1 = vec![0.1; 64]; // "SQL Injection signature"
        let cve2 = vec![0.9; 64]; // "Ransomware payload"
        
        let cve1_id = g.create_node(Label::new("ThreatIntel"));
        if let Some(n) = g.get_node_mut(cve1_id) {
            n.set_property("cve", "CVE-2023-1234");
            n.set_property("description", "SQL Injection in login form");
            n.set_vector("description", cve1).unwrap();
        }

        let cve2_id = g.create_node(Label::new("ThreatIntel"));
        if let Some(n) = g.get_node_mut(cve2_id) {
            n.set_property("cve", "CVE-2024-5678");
            n.set_property("description", "WannaCry Ransomware variant");
            n.set_vector("description", cve2).unwrap();
        }
        println!("   ✓ Threat Intel Database Updated (Vector Index Active)");
    }

    pause();

    // 4. Simulate Attack & Detection
    println!("\n[Step 3] 🚨 ALERT: Suspicious Activity Detected on Web-Srv-01");
    println!("   Log: 'UNION SELECT * FROM users...'");
    
    // Create Alert Node
    let alert_id;
    {
        let mut g = store.write().await;
        alert_id = g.create_node(Label::new("Alert"));
        if let Some(n) = g.get_node_mut(alert_id) {
            n.set_property("severity", "High");
            n.set_property("payload", "UNION SELECT * FROM users");
            n.set_property("status", "New");
        }
        
        // Link to target
        let target = g.get_nodes_by_label(&Label::new("Server"))[0].id; // Web-Srv-01
        g.create_edge(alert_id, target, EdgeType::new("TARGETS")).unwrap();
    }

    // Vector Search to Identify Threat
    println!("   🔍 Analyzing payload signature against Threat Intel...");
    {
        let g = store.read().await;
        // Simulating embedding of the log payload
        let payload_vec = vec![0.12; 64]; // Close to SQLi
        let results = g.vector_search("ThreatIntel", "description", &payload_vec, 1).unwrap();
        
        if let Some((id, score)) = results.first() {
            let node = g.get_node(*id).unwrap();
            let cve = node.get_property("cve").unwrap().as_string().unwrap();
            let desc = node.get_property("description").unwrap().as_string().unwrap();
            println!("   ✅ MATCH FOUND: {} (Score: {:.4})", cve, score);
            println!("      Description: {}", desc);
            
            // Agent Action (Simulated)
            println!("   🤖 Agent: High confidence match for SQL Injection.");
            println!("   🤖 Agent: Initiating containment protocol.");
        }
    }

    pause();

    // 5. Automated Response
    println!("\n[Step 4] Automated Response (Graph Update)");
    {
        let mut g = store.write().await;
        let target = g.get_nodes_by_label(&Label::new("Server"))[0].id;
        
        // Isolate node
        if let Some(n) = g.get_node_mut(target) {
            n.set_property("status", "ISOLATED");
        }
        
        println!("   ✓ Web-Srv-01 status updated to ISOLATED");
        println!("   ✓ Firewall rules updated (Simulated)");
    }

    pause();
    println!("\n✅ DEMO COMPLETE: Threat neutralized.");
}
