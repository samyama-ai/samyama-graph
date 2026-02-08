//! Agentic Enrichment Demo
//!
//! Demonstrates Generation-Augmented Knowledge (GAK): the database
//! actively uses Claude Code CLI to build its own knowledge graph.
//!
//! Instead of RAG (using a database to help LLMs answer questions),
//! GAK inverts the pattern — using LLMs to help the database build knowledge.
//!
//! Requirements: `claude` CLI must be installed and authenticated.
//!
//! Run: cargo run --release --example agentic_enrichment_demo

use samyama::{GraphStore, PropertyValue, QueryEngine};
use std::process::Command;

fn main() {
    println!("================================================================");
    println!("  Samyama Agentic Enrichment Demo");
    println!("  Generation-Augmented Knowledge (GAK) via Claude Code CLI");
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

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // ━━━ Phase 1: The Trigger ━━━
    println!("--- Phase 1: The Trigger ---");
    let user_query = "What are the indications and clinical trials for Semaglutide?";
    println!("User query: \"{}\"", user_query);
    println!();

    // Check if we have information about this drug
    let result = engine
        .execute("MATCH (d:Drug) RETURN d.name", &store)
        .unwrap();

    if result.len() == 0 {
        println!("  Graph is empty — no Drug nodes found.");
        println!("  Enrichment policy triggered: missing entity detected.");
    } else {
        println!("  Found {} Drug node(s). Checking if Semaglutide exists...", result.len());
    }
    println!();

    // ━━━ Phase 2: Agentic Enrichment ━━━
    println!("--- Phase 2: Agentic Enrichment via Claude Code CLI ---");
    println!("Invoking: claude -p \"<enrichment prompt>\"");
    println!("Waiting for Claude to generate knowledge subgraph...");
    println!();

    let prompt = build_enrichment_prompt("Semaglutide");

    let cypher_statements = match invoke_claude(&prompt) {
        Ok(statements) => statements,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // ━━━ Phase 3: Knowledge Ingestion ━━━
    println!("--- Phase 3: Knowledge Ingestion ---");
    println!(
        "Executing {} Cypher statements against the graph...",
        cypher_statements.len()
    );
    println!();

    let mut success = 0;
    let mut failed = 0;

    // Execute CREATE statements first (nodes), then MATCH...CREATE (edges)
    let (creates, matches): (Vec<_>, Vec<_>) = cypher_statements
        .iter()
        .partition(|s| s.to_uppercase().starts_with("CREATE"));

    for stmt in creates.iter().chain(matches.iter()) {
        match engine.execute_mut(stmt, &mut store, "default") {
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

    // ━━━ Phase 4: Query the Enriched Graph ━━━
    println!("--- Phase 4: Query the Enriched Graph ---");
    println!();

    // Show drug info
    println!("Drug:");
    if let Ok(result) = engine.execute(
        "MATCH (d:Drug) RETURN d.name, d.mechanism, d.drugClass, d.approvalYear",
        &store,
    ) {
        for record in &result.records {
            print_field(record, "d.name", "  Name");
            print_field(record, "d.mechanism", "  Mechanism");
            print_field(record, "d.drugClass", "  Class");
            print_field(record, "d.approvalYear", "  Approved");
        }
    }
    println!();

    // Show indications
    println!("Indications:");
    if let Ok(result) = engine.execute(
        "MATCH (d:Drug)-[:TREATS]->(i:Indication) RETURN i.name",
        &store,
    ) {
        if result.len() == 0 {
            println!("  (none found — edge type may differ)");
        }
        for record in &result.records {
            if let Some(name) = get_string(record, "i.name") {
                println!("  - {}", name);
            }
        }
    }
    println!();

    // Show manufacturer
    println!("Manufacturer:");
    if let Ok(result) = engine.execute(
        "MATCH (d:Drug)-[:MADE_BY]->(m:Manufacturer) RETURN m.name, m.headquarters",
        &store,
    ) {
        if result.len() == 0 {
            println!("  (none found — edge type may differ)");
        }
        for record in &result.records {
            let name = get_string(record, "m.name").unwrap_or_default();
            let hq = get_string(record, "m.headquarters").unwrap_or_default();
            if !name.is_empty() {
                println!("  - {} ({})", name, hq);
            }
        }
    }
    println!();

    // Show clinical trials
    println!("Clinical Trials:");
    if let Ok(result) = engine.execute(
        "MATCH (d:Drug)-[:STUDIED_IN]->(t:ClinicalTrial) RETURN t.name, t.phase, t.year",
        &store,
    ) {
        if result.len() == 0 {
            println!("  (none found — edge type may differ)");
        }
        for record in &result.records {
            let name = get_string(record, "t.name").unwrap_or("Unknown".into());
            let phase = get_string(record, "t.phase").unwrap_or_default();
            let year = get_string(record, "t.year").unwrap_or_default();
            println!("  - {} (Phase {}, {})", name, phase, year);
        }
    }
    println!();

    // Graph stats
    println!("--- Graph Statistics ---");
    println!("  Nodes: {}", store.node_count());
    println!("  Edges: {}", store.edge_count());
    println!();

    println!("The database actively built its own knowledge using Claude Code CLI.");
    println!("This is Generation-Augmented Knowledge (GAK) — the inverse of RAG.");
}

fn is_claude_available() -> bool {
    Command::new("which")
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

fn invoke_claude(prompt: &str) -> Result<Vec<String>, String> {
    let output = Command::new("claude")
        .arg("-p")
        .arg(prompt)
        .output()
        .map_err(|e| format!("Failed to run claude CLI: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("claude CLI failed: {}", stderr));
    }

    let response = String::from_utf8_lossy(&output.stdout).to_string();

    println!("Claude response:");
    for line in response.lines() {
        if !line.trim().is_empty() {
            println!("  | {}", line);
        }
    }
    println!();

    // Extract lines that are Cypher statements
    let statements: Vec<String> = response
        .lines()
        .map(|l| l.trim())
        .filter(|l| {
            let upper = l.to_uppercase();
            upper.starts_with("CREATE") || upper.starts_with("MATCH")
        })
        .map(|l| l.to_string())
        .collect();

    if statements.is_empty() {
        return Err("No Cypher statements found in Claude response".to_string());
    }

    println!("Parsed {} Cypher statements from response.", statements.len());
    println!();

    Ok(statements)
}

fn get_string(record: &samyama::query::executor::Record, key: &str) -> Option<String> {
    let val = record.get(key)?;
    let pv = val.as_property()?;
    match pv {
        PropertyValue::String(s) => Some(s.clone()),
        PropertyValue::Integer(i) => Some(i.to_string()),
        PropertyValue::Float(f) => Some(f.to_string()),
        PropertyValue::Boolean(b) => Some(b.to_string()),
        PropertyValue::Null => None,
        _ => Some(format!("{:?}", pv)),
    }
}

fn print_field(record: &samyama::query::executor::Record, key: &str, label: &str) {
    if let Some(val) = get_string(record, key) {
        println!("{}: {}", label, val);
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}
