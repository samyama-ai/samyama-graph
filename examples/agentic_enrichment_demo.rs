//! Agentic Enrichment Demo
//!
//! Demonstrates Generation-Augmented Knowledge (GAK): the database
//! actively uses Claude Code CLI to build its own knowledge graph.
//!
//! Instead of RAG (using a database to help LLMs answer questions),
//! GAK inverts the pattern — using LLMs to help the database build knowledge.
//!
//! This demo uses the full NLQ pipeline: TenantManager -> AgentRuntime -> NLQClient
//! with the ClaudeCode provider (shells out to `claude -p`).
//!
//! Requirements: `claude` CLI must be installed and authenticated.
//!
//! Run: cargo run --release --example agentic_enrichment_demo

use samyama_sdk::{
    EmbeddedClient, SamyamaClient,
    AgentConfig, LLMProvider, NLQConfig,
};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    println!("================================================================");
    println!("  Samyama Agentic Enrichment Demo");
    println!("  Generation-Augmented Knowledge (GAK) via NLQ Pipeline");
    println!("================================================================");
    println!();
    println!("The database becomes an active participant in building its own");
    println!("knowledge — the inverse of RAG.");
    println!();

    // Verify claude CLI is available
    if !is_claude_available() {
        eprintln!("Error: 'claude' CLI not found.");
        eprintln!("Install Claude Code: https://docs.anthropic.com/en/docs/claude-code");
        std::process::exit(1);
    }
    println!("[ok] Claude Code CLI detected");
    println!();

    // Setup: Tenant with ClaudeCode NLQ + Agent Config
    println!("--- Setup: Tenant with ClaudeCode NLQ + Agent Config ---");

    let client = EmbeddedClient::new();
    let tenant_mgr = client.tenant_manager();
    tenant_mgr
        .create_tenant(
            "pharma_research".to_string(),
            "Pharma Research".to_string(),
            None,
        )
        .unwrap();

    // NLQ config — translates natural language to read-only Cypher
    let nlq_config = NLQConfig {
        enabled: true,
        provider: LLMProvider::ClaudeCode,
        model: String::new(),
        api_key: None,
        api_base_url: None,
        system_prompt: Some(
            "You are a Cypher query expert for a pharmaceutical knowledge graph.".to_string(),
        ),
    };

    tenant_mgr
        .update_nlq_config("pharma_research", Some(nlq_config.clone()))
        .unwrap();

    // Agent config — generates enrichment CREATE statements
    let mut policies = HashMap::new();
    policies.insert(
        "Drug".to_string(),
        "When a Drug entity is missing, enrich it with indications, manufacturer, and clinical trials.".to_string(),
    );

    let agent_config = AgentConfig {
        enabled: true,
        provider: LLMProvider::ClaudeCode,
        model: String::new(),
        api_key: None,
        api_base_url: None,
        system_prompt: Some(
            "You are a pharmaceutical knowledge graph builder. Generate Cypher CREATE statements.".to_string(),
        ),
        tools: vec![],
        policies,
    };

    tenant_mgr
        .update_agent_config("pharma_research", Some(agent_config.clone()))
        .unwrap();

    println!("  Created tenant 'pharma_research'");
    println!("  NLQ config: ClaudeCode provider (natural language -> Cypher)");
    println!("  Agent config: ClaudeCode provider (enrichment CREATE statements)");
    println!("  Enrichment policy: Drug -> indications, manufacturer, trials");
    println!();

    // Phase 1: NLQ Translation + The Trigger
    println!("--- Phase 1: NLQ Translation + The Trigger ---");
    let user_query = "What are the indications and clinical trials for Semaglutide?";
    println!("User query: \"{}\"", user_query);
    println!();

    let schema_summary = "Node labels: Drug, Indication, Manufacturer, ClinicalTrial\n\
                          Edge types: TREATS (Drug->Indication), MADE_BY (Drug->Manufacturer), STUDIED_IN (Drug->ClinicalTrial)\n\
                          Properties: Drug(name, mechanism, drugClass, approvalYear), Indication(name), Manufacturer(name, headquarters), ClinicalTrial(name, phase, year)";

    let nlq_pipeline = client.nlq_pipeline(nlq_config).unwrap();
    println!("  NLQ pipeline: translating natural language to Cypher...");

    let cypher_query = match nlq_pipeline.text_to_cypher(user_query, schema_summary).await {
        Ok(cypher) => {
            println!("  Generated Cypher: {}", cypher);
            cypher
        }
        Err(e) => {
            println!("  NLQ translation failed: {} — falling back to default query", e);
            "MATCH (d:Drug) RETURN d.name".to_string()
        }
    };
    println!();

    // Execute the NLQ-generated Cypher
    let result_count = match client.query_readonly("default", &cypher_query).await {
        Ok(result) => result.len(),
        Err(e) => {
            println!("  Query execution error: {} — treating as empty result", e);
            0
        }
    };

    if result_count == 0 {
        println!("  No results — graph has no matching data.");
        println!("  Enrichment policy triggered: missing entity detected.");
    } else {
        println!("  Found {} result(s).", result_count);
    }
    println!();

    // Phase 2: Agentic Enrichment via NLQ Pipeline
    println!("--- Phase 2: Agentic Enrichment via NLQ Pipeline ---");
    println!("  Provider: ClaudeCode (claude -p CLI)");
    println!("  Pipeline: AgentRuntime -> NLQClient -> claude CLI");
    println!("  Waiting for Claude to generate knowledge subgraph...");
    println!();

    let tenant = tenant_mgr.get_tenant("pharma_research").unwrap();
    let config = tenant.agent_config.unwrap();
    let runtime = client.agent_runtime(config);

    let enrichment_prompt = build_enrichment_prompt("Semaglutide");
    let response = match runtime
        .process_trigger(&enrichment_prompt, "pharma_research")
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("Error from AgentRuntime: {}", e);
            std::process::exit(1);
        }
    };

    println!("Claude response:");
    for line in response.lines() {
        if !line.trim().is_empty() {
            println!("  | {}", line);
        }
    }
    println!();

    // Parse Cypher statements from response
    let cypher_statements: Vec<String> = response
        .lines()
        .map(|l| l.trim())
        .filter(|l| {
            let upper = l.to_uppercase();
            upper.starts_with("CREATE") || upper.starts_with("MATCH")
        })
        .map(|l| l.to_string())
        .collect();

    if cypher_statements.is_empty() {
        eprintln!("No Cypher statements found in Claude response");
        std::process::exit(1);
    }

    println!("Parsed {} Cypher statements from response.", cypher_statements.len());
    println!();

    // Phase 3: Knowledge Ingestion
    println!("--- Phase 3: Knowledge Ingestion ---");
    println!(
        "Executing {} Cypher statements against the graph...",
        cypher_statements.len()
    );
    println!();

    let mut success = 0;
    let mut failed = 0;

    let (creates, matches): (Vec<_>, Vec<_>) = cypher_statements
        .iter()
        .partition(|s| s.to_uppercase().starts_with("CREATE"));

    for stmt in creates.iter().chain(matches.iter()) {
        match client.query("default", stmt).await {
            Ok(_) => {
                success += 1;
                println!("  [ok] {}", truncate(stmt, 78));
            }
            Err(e) => {
                failed += 1;
                println!("  [!!] {}", truncate(stmt, 60));
                println!("        Error: {}", e);
            }
        }
    }

    println!();
    println!(
        "Ingestion complete: {} succeeded, {} failed",
        success, failed
    );
    println!();

    // Phase 4: Query the Enriched Graph
    println!("--- Phase 4: Query the Enriched Graph ---");
    println!();

    // Show drug info
    println!("Drug:");
    if let Ok(result) = client.query_readonly("default",
        "MATCH (d:Drug) RETURN d.name, d.mechanism, d.drugClass, d.approvalYear",
    ).await {
        for row in &result.records {
            for (i, col) in result.columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    if !val.is_null() {
                        let label = col.split('.').last().unwrap_or(col);
                        println!("  {}: {}", label, val);
                    }
                }
            }
        }
    }
    println!();

    // Show indications
    println!("Indications:");
    if let Ok(result) = client.query_readonly("default",
        "MATCH (d:Drug)-[:TREATS]->(i:Indication) RETURN i.name",
    ).await {
        if result.is_empty() {
            println!("  (none found — edge type may differ)");
        }
        for row in &result.records {
            if let Some(val) = row.first() {
                if !val.is_null() {
                    println!("  - {}", val);
                }
            }
        }
    }
    println!();

    // Show manufacturer
    println!("Manufacturer:");
    if let Ok(result) = client.query_readonly("default",
        "MATCH (d:Drug)-[:MADE_BY]->(m:Manufacturer) RETURN m.name, m.headquarters",
    ).await {
        if result.is_empty() {
            println!("  (none found — edge type may differ)");
        }
        for row in &result.records {
            let name = row.first().and_then(|v| v.as_str()).unwrap_or("");
            let hq = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if !name.is_empty() {
                println!("  - {} ({})", name, hq);
            }
        }
    }
    println!();

    // Show clinical trials
    println!("Clinical Trials:");
    if let Ok(result) = client.query_readonly("default",
        "MATCH (d:Drug)-[:STUDIED_IN]->(t:ClinicalTrial) RETURN t.name, t.phase, t.year",
    ).await {
        if result.is_empty() {
            println!("  (none found — edge type may differ)");
        }
        for row in &result.records {
            let name = row.first().map(|v| v.to_string()).unwrap_or("Unknown".into());
            let phase = row.get(1).map(|v| v.to_string()).unwrap_or_default();
            let year = row.get(2).map(|v| v.to_string()).unwrap_or_default();
            println!("  - {} (Phase {}, {})", name, phase, year);
        }
    }
    println!();

    // Graph stats
    println!("--- Graph Statistics ---");
    let status = client.status().await.unwrap();
    println!("  Nodes: {}", status.storage.nodes);
    println!("  Edges: {}", status.storage.edges);
    println!();

    println!("The database actively built its own knowledge using the NLQ pipeline.");
    println!("Provider: ClaudeCode (claude -p CLI) via AgentRuntime -> NLQClient.");
    println!("This is Generation-Augmented Knowledge (GAK) — the inverse of RAG.");
}

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn build_enrichment_prompt(drug_name: &str) -> String {
    format!(
        r#"Generate Cypher CREATE statements to build a knowledge subgraph about the drug "{drug_name}".

Create these nodes and relationships:
1. One Drug node with properties: name, mechanism, drugClass, manufacturer, approvalYear (integer)
2. Two or three Indication nodes each with property: name
3. One Manufacturer node with properties: name, headquarters
4. Two ClinicalTrial nodes each with properties: name, phase (integer), year (integer)
5. Edges: (Drug)-[:TREATS]->(Indication), (Drug)-[:MADE_BY]->(Manufacturer), (Drug)-[:STUDIED_IN]->(ClinicalTrial)

CRITICAL RULES — follow these exactly:
- Output ONLY Cypher statements, one per line
- NO markdown fences, NO comments, NO explanations, NO blank lines
- Use single quotes for ALL string values
- Use integers without quotes for numeric values like approvalYear: 2017
- First output all CREATE statements for individual nodes
- Then output MATCH...CREATE statements for edges
- For edges use exactly this format: MATCH (a:Label {{name: 'X'}}), (b:Label {{name: 'Y'}}) CREATE (a)-[:REL_TYPE]->(b)
- Variable names in MATCH clauses must be single lowercase letters (a, b, c, d)

Example output (for a DIFFERENT drug — do NOT copy these values):
CREATE (d:Drug {{name: 'Aspirin', mechanism: 'COX-1 and COX-2 inhibitor', drugClass: 'NSAID', manufacturer: 'Bayer', approvalYear: 1899}})
CREATE (i:Indication {{name: 'Pain'}})
CREATE (m:Manufacturer {{name: 'Bayer', headquarters: 'Leverkusen'}})
CREATE (t:ClinicalTrial {{name: 'ARRIVE', phase: 3, year: 2018}})
MATCH (a:Drug {{name: 'Aspirin'}}), (b:Indication {{name: 'Pain'}}) CREATE (a)-[:TREATS]->(b)
MATCH (a:Drug {{name: 'Aspirin'}}), (b:Manufacturer {{name: 'Bayer'}}) CREATE (a)-[:MADE_BY]->(b)
MATCH (a:Drug {{name: 'Aspirin'}}), (b:ClinicalTrial {{name: 'ARRIVE'}}) CREATE (a)-[:STUDIED_IN]->(b)"#,
        drug_name = drug_name
    )
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}
