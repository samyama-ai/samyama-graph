//! Enterprise Banking Demo - Samyama Graph Database
//!
//! This example demonstrates enterprise-level banking data modeling with:
//! - Loading synthetic data from TSV files (customers, accounts, branches, transactions)
//! - Graph-based fraud detection patterns
//! - Money laundering pattern detection (structuring, rapid succession, circular transfers)
//! - OFAC/Sanctions screening simulation
//! - Customer relationship network analysis
//! - Multi-tenancy for different banking divisions
//! - Complex Cypher queries for business intelligence
//!
//! Prerequisites:
//!   1. Generate synthetic data first:
//!      cd docs/banking/generators
//!      python generate_all.py --size small  # or medium/large/enterprise
//!
//!   2. Run the demo:
//!      cargo run --example banking_demo
//!
//! Data files expected in docs/banking/data/:
//!   - branches.tsv
//!   - customers.tsv
//!   - accounts.tsv
//!   - transactions.tsv
//!   - owns_account.tsv
//!   - banks_at.tsv
//!   - transfer_to.tsv
//!   - knows.tsv
//!   - referred_by.tsv

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use samyama::{
    PersistenceManager, ResourceQuotas, QueryEngine, NLQPipeline,
    graph::{GraphStore, Label, NodeId},
    persistence::tenant::{LLMProvider, NLQConfig},
};

// ============================================================================
// TSV LOADER
// ============================================================================

/// Statistics about loaded data
#[derive(Debug, Default)]
struct LoadStats {
    branches: usize,
    customers: usize,
    accounts: usize,
    transactions: usize,
    relationships: usize,
}

/// ID mappings for relationship creation
struct IdMappings {
    branches: HashMap<String, NodeId>,
    customers: HashMap<String, NodeId>,
    accounts: HashMap<String, NodeId>,
    transactions: HashMap<String, NodeId>,
}

impl IdMappings {
    fn new() -> Self {
        Self {
            branches: HashMap::new(),
            customers: HashMap::new(),
            accounts: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    fn find(&self, id: &str) -> Option<NodeId> {
        self.customers.get(id)
            .or_else(|| self.accounts.get(id))
            .or_else(|| self.branches.get(id))
            .or_else(|| self.transactions.get(id))
            .copied()
    }
}

/// Load branches from TSV
fn load_branches(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &mut IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join("branches.tsv");
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('\t').collect();

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        let row: HashMap<&str, &str> = headers.iter().cloned()
            .zip(values.iter().cloned())
            .collect();

        let node_id = graph.create_node("Branch");

        // Set properties first (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            for &key in &["branch_id", "name", "code", "branch_type", "address",
                          "city", "state", "zip_code", "country", "phone", "status"] {
                if let Some(v) = row.get(key) { node.set_property(key, *v); }
            }

            if let Some(v) = row.get("employee_count") {
                if let Ok(n) = v.parse::<i64>() { node.set_property("employee_count", n); }
            }
            if let Some(v) = row.get("latitude") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("latitude", n); }
            }
            if let Some(v) = row.get("longitude") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("longitude", n); }
            }
        }

        // Add branch_type as label AFTER releasing mutable borrow
        // Using graph.add_label_to_node("default", ) ensures the label_index is updated,
        // making the node queryable via get_nodes_by_label() and Cypher MATCH
        if let Some(bt) = row.get("branch_type") {
            let _ = graph.add_label_to_node("default", node_id, bt.replace(" ", ""));
        }

        if let Some(id) = row.get("branch_id") {
            mappings.branches.insert(id.to_string(), node_id);
        }
        count += 1;
    }

    Ok(count)
}

/// Load customers from TSV
fn load_customers(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &mut IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join("customers.tsv");
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('\t').collect();

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        let row: HashMap<&str, &str> = headers.iter().cloned()
            .zip(values.iter().cloned())
            .collect();

        let node_id = graph.create_node("Customer");

        // Collect label info before mutable borrow
        let customer_type = row.get("customer_type").map(|s| s.to_string());
        let risk_label = row.get("risk_score").and_then(|risk| {
            risk.parse::<i64>().ok().map(|score| {
                if score >= 80 { "HighRisk" }
                else if score >= 50 { "MediumRisk" }
                else { "LowRisk" }
            })
        });

        // Set properties (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            // String properties
            for &key in &["customer_id", "customer_type", "first_name", "last_name",
                          "email", "phone", "address", "city", "state", "zip_code",
                          "country", "date_of_birth", "ssn_last4", "kyc_status",
                          "account_opened_date", "last_activity_date"] {
                if let Some(v) = row.get(key) {
                    if !v.is_empty() { node.set_property(key, *v); }
                }
            }

            // Optional string properties
            for &key in &["company_name", "occupation", "employer", "industry"] {
                if let Some(v) = row.get(key) {
                    if !v.is_empty() { node.set_property(key, *v); }
                }
            }

            // Numeric properties
            if let Some(v) = row.get("risk_score") {
                if let Ok(n) = v.parse::<i64>() { node.set_property("risk_score", n); }
            }
            if let Some(v) = row.get("annual_income") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("annual_income", n); }
            }
            if let Some(v) = row.get("credit_score") {
                if let Ok(n) = v.parse::<i64>() { node.set_property("credit_score", n); }
            }
        }

        // Add labels AFTER releasing mutable borrow
        // Using graph.add_label_to_node("default", ) ensures the label_index is updated,
        // making nodes queryable via get_nodes_by_label() and Cypher MATCH (c:Individual)
        if let Some(ct) = customer_type {
            let _ = graph.add_label_to_node("default", node_id, ct);
        }
        if let Some(risk) = risk_label {
            let _ = graph.add_label_to_node("default", node_id, risk);
        }

        if let Some(id) = row.get("customer_id") {
            mappings.customers.insert(id.to_string(), node_id);
        }
        count += 1;
    }

    Ok(count)
}

/// Load accounts from TSV
fn load_accounts(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &mut IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join("accounts.tsv");
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('\t').collect();

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        let row: HashMap<&str, &str> = headers.iter().cloned()
            .zip(values.iter().cloned())
            .collect();

        let node_id = graph.create_node("Account");

        // Collect label info before mutable borrow
        let account_type = row.get("account_type").map(|s| s.to_string());
        let status_label = row.get("status").and_then(|s| {
            if *s != "Active" { Some(s.replace(" ", "")) } else { None }
        });

        // Set properties (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            // String properties
            for &key in &["account_id", "account_number", "account_type", "customer_id",
                          "branch_id", "currency", "status", "opened_date"] {
                if let Some(v) = row.get(key) { node.set_property(key, *v); }
            }

            if let Some(v) = row.get("last_transaction_date") {
                if !v.is_empty() { node.set_property("last_transaction_date", *v); }
            }

            // Numeric properties
            for &key in &["balance", "interest_rate", "credit_limit", "minimum_balance",
                          "overdraft_limit", "original_amount"] {
                if let Some(v) = row.get(key) {
                    if let Ok(n) = v.parse::<f64>() { node.set_property(key, n); }
                }
            }
            if let Some(v) = row.get("term_months") {
                if let Ok(n) = v.parse::<i64>() { node.set_property("term_months", n); }
            }
        }

        // Add labels AFTER releasing mutable borrow
        // Using graph.add_label_to_node("default", ) ensures the label_index is updated,
        // making nodes queryable via get_nodes_by_label() and Cypher MATCH (a:Checking)
        if let Some(at) = account_type {
            let _ = graph.add_label_to_node("default", node_id, at);
        }
        if let Some(status) = status_label {
            let _ = graph.add_label_to_node("default", node_id, status);
        }

        if let Some(id) = row.get("account_id") {
            mappings.accounts.insert(id.to_string(), node_id);
        }
        count += 1;
    }

    Ok(count)
}

/// Load transactions from TSV
fn load_transactions(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &mut IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join("transactions.tsv");
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('\t').collect();

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        let row: HashMap<&str, &str> = headers.iter().cloned()
            .zip(values.iter().cloned())
            .collect();

        let node_id = graph.create_node("Transaction");

        // Collect label info before mutable borrow
        let transaction_type = row.get("transaction_type").map(|s| s.to_string());
        let is_fraud = row.get("fraud_flag").map(|ff| *ff == "True").unwrap_or(false);

        // Set properties (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            // String properties
            for &key in &["transaction_id", "account_id", "transaction_type", "timestamp",
                          "description", "status", "channel", "reference_number"] {
                if let Some(v) = row.get(key) { node.set_property(key, *v); }
            }

            // Optional string properties
            for &key in &["merchant_name", "merchant_category", "counterparty_account",
                          "location", "ip_address", "device_id"] {
                if let Some(v) = row.get(key) {
                    if !v.is_empty() { node.set_property(key, *v); }
                }
            }

            // Numeric properties
            if let Some(v) = row.get("amount") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("amount", n); }
            }
            if let Some(v) = row.get("balance_after") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("balance_after", n); }
            }
            if let Some(v) = row.get("mcc_code") {
                if let Ok(n) = v.parse::<i64>() { node.set_property("mcc_code", n); }
            }
            if let Some(v) = row.get("fraud_score") {
                if let Ok(n) = v.parse::<f64>() { node.set_property("fraud_score", n); }
            }
            if let Some(v) = row.get("fraud_flag") {
                node.set_property("fraud_flag", *v == "True");
            }
        }

        // Add labels AFTER releasing mutable borrow
        // Using graph.add_label_to_node("default", ) ensures the label_index is updated,
        // making nodes queryable via get_nodes_by_label() and Cypher MATCH (t:Transfer)
        if let Some(tt) = transaction_type {
            let _ = graph.add_label_to_node("default", node_id, tt);
        }
        if is_fraud {
            let _ = graph.add_label_to_node("default", node_id, "Flagged");
            let _ = graph.add_label_to_node("default", node_id, "Fraud");
        }

        if let Some(id) = row.get("transaction_id") {
            mappings.transactions.insert(id.to_string(), node_id);
        }
        count += 1;
    }

    Ok(count)
}

/// Load a relationship file
fn load_relationship_file(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &IdMappings,
    filename: &str,
    edge_type: &str,
    from_col: &str,
    to_col: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join(filename);
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('\t').collect();

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        let row: HashMap<&str, &str> = headers.iter().cloned()
            .zip(values.iter().cloned())
            .collect();

        let from_id = row.get(from_col).map(|s| s.to_string());
        let to_id = row.get(to_col).map(|s| s.to_string());

        if let (Some(from_id), Some(to_id)) = (from_id, to_id) {
            if let (Some(from_node), Some(to_node)) = (mappings.find(&from_id), mappings.find(&to_id)) {
                if let Ok(edge_id) = graph.create_edge(from_node, to_node, edge_type) {
                    if let Some(edge) = graph.get_edge_mut(edge_id) {
                        for (key, value) in &row {
                            if *key != from_col && *key != to_col && !value.is_empty() {
                                if let Ok(n) = value.parse::<f64>() {
                                    edge.set_property(*key, n);
                                } else if let Ok(n) = value.parse::<i64>() {
                                    edge.set_property(*key, n);
                                } else if *value == "True" || *value == "False" {
                                    edge.set_property(*key, *value == "True");
                                } else {
                                    edge.set_property(*key, *value);
                                }
                            }
                        }
                    }
                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Load all relationships
fn load_relationships(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total = 0;

    let rel_files = [
        ("owns_account.tsv", "OWNS", "customer_id", "account_id"),
        ("banks_at.tsv", "BANKS_AT", "customer_id", "branch_id"),
        ("account_at_branch.tsv", "LOCATED_AT", "account_id", "branch_id"),
        ("transfer_to.tsv", "TRANSFER_TO", "from_account_id", "to_account_id"),
        ("knows.tsv", "KNOWS", "customer1_id", "customer2_id"),
        ("referred_by.tsv", "REFERRED_BY", "customer_id", "referrer_id"),
        ("authorized_user.tsv", "AUTHORIZED_USER", "customer_id", "account_id"),
        ("employed_by.tsv", "EMPLOYED_BY", "customer_id", "employer_id"),
    ];

    for (filename, edge_type, from_col, to_col) in rel_files.iter() {
        let loaded = load_relationship_file(graph, data_dir, mappings, filename, edge_type, from_col, to_col)?;
        if loaded > 0 {
            println!("      {} {} edges", loaded, edge_type);
        }
        total += loaded;
    }

    // Create account -> transaction edges
    let tx_edges = create_account_transaction_edges(graph, data_dir, mappings)?;
    if tx_edges > 0 {
        println!("      {} HAS_TRANSACTION edges", tx_edges);
    }
    total += tx_edges;

    Ok(total)
}

/// Create edges from accounts to transactions
fn create_account_transaction_edges(
    graph: &mut GraphStore,
    data_dir: &Path,
    mappings: &IdMappings,
) -> Result<usize, Box<dyn std::error::Error>> {
    let path = data_dir.join("transactions.tsv");
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    lines.next(); // Skip header

    let mut count = 0;
    for line_result in lines {
        let line = line_result?;
        if line.trim().is_empty() { continue; }

        let values: Vec<&str> = line.split('\t').collect();
        if values.len() < 2 { continue; }

        let tx_id = values[0];
        let acc_id = values[1];

        if let (Some(&tx_node), Some(&acc_node)) =
            (mappings.transactions.get(tx_id), mappings.accounts.get(acc_id))
        {
            if graph.create_edge(acc_node, tx_node, "HAS_TRANSACTION").is_ok() {
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Load all data from TSV files
fn load_all_data(
    graph: &mut GraphStore,
    data_dir: &Path,
) -> Result<LoadStats, Box<dyn std::error::Error>> {
    let mut stats = LoadStats::default();
    let mut mappings = IdMappings::new();

    println!("    Loading branches...");
    stats.branches = load_branches(graph, data_dir, &mut mappings)?;
    println!("      ✓ {} branches", stats.branches);

    println!("    Loading customers...");
    stats.customers = load_customers(graph, data_dir, &mut mappings)?;
    println!("      ✓ {} customers", stats.customers);

    println!("    Loading accounts...");
    stats.accounts = load_accounts(graph, data_dir, &mut mappings)?;
    println!("      ✓ {} accounts", stats.accounts);

    println!("    Loading transactions...");
    stats.transactions = load_transactions(graph, data_dir, &mut mappings)?;
    println!("      ✓ {} transactions", stats.transactions);

    println!("    Loading relationships...");
    stats.relationships = load_relationships(graph, data_dir, &mappings)?;
    println!("      ✓ {} relationships", stats.relationships);

    Ok(stats)
}

// ============================================================================
// MAIN DEMO
// ============================================================================

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║     SAMYAMA GRAPH DATABASE - Enterprise Banking Demo                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    let start_time = Instant::now();

    // =========================================================================
    // 1. SETUP PERSISTENCE & MULTI-TENANCY
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 1: Setting up Banking Infrastructure                           │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    let persist_mgr = PersistenceManager::new("./banking_data")?;

    // Retail Banking Division
    let retail_quotas = ResourceQuotas {
        max_nodes: Some(10_000_000),
        max_edges: Some(50_000_000),
        max_memory_bytes: Some(4 * 1024 * 1024 * 1024),      // 4 GB
        max_storage_bytes: Some(20 * 1024 * 1024 * 1024),    // 20 GB
        max_connections: Some(500),
        max_query_time_ms: Some(60_000),
    };
    persist_mgr.tenants().create_tenant(
        "retail_banking".to_string(),
        "Retail Banking Division".to_string(),
        Some(retail_quotas),
    )?;
    println!("  ✓ Created 'retail_banking' tenant (10M nodes, 50M edges)");

    // Corporate Banking Division
    let corporate_quotas = ResourceQuotas {
        max_nodes: Some(1_000_000),
        max_edges: Some(10_000_000),
        max_memory_bytes: Some(8 * 1024 * 1024 * 1024),      // 8 GB
        max_storage_bytes: Some(50 * 1024 * 1024 * 1024),    // 50 GB
        max_connections: Some(100),
        max_query_time_ms: Some(120_000),
    };
    persist_mgr.tenants().create_tenant(
        "corporate_banking".to_string(),
        "Corporate Banking Division".to_string(),
        Some(corporate_quotas),
    )?;
    println!("  ✓ Created 'corporate_banking' tenant (1M nodes, 10M edges)");

    // Wealth Management Division
    let wealth_quotas = ResourceQuotas {
        max_nodes: Some(500_000),
        max_edges: Some(5_000_000),
        max_memory_bytes: Some(2 * 1024 * 1024 * 1024),      // 2 GB
        max_storage_bytes: Some(10 * 1024 * 1024 * 1024),    // 10 GB
        max_connections: Some(50),
        max_query_time_ms: Some(180_000),
    };
    persist_mgr.tenants().create_tenant(
        "wealth_management".to_string(),
        "Wealth Management Division".to_string(),
        Some(wealth_quotas),
    )?;
    println!("  ✓ Created 'wealth_management' tenant (500K nodes, 5M edges)");
    println!();

    // =========================================================================
    // 2. INITIALIZE GRAPH & LOAD DATA
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 2: Loading Enterprise Banking Data                             │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    let mut graph = GraphStore::new();
    let data_dir = Path::new("docs/banking/data");

    let stats = if data_dir.exists() {
        load_all_data(&mut graph, data_dir)?
    } else {
        println!("  ⚠ Data directory not found: {}", data_dir.display());
        println!("    Run the data generator first:");
        println!("    cd docs/banking/generators && python generate_all.py --size small");
        println!();
        println!("  Creating sample data inline...");
        create_sample_data(&mut graph)?
    };

    let load_time = start_time.elapsed();
    println!();
    println!("  Data loaded in {:.2}s", load_time.as_secs_f64());
    println!();

    // =========================================================================
    // 3. PERSIST DATA TO STORAGE
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 3: Persisting Data to Storage                                  │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    let persist_start = Instant::now();

    // Persist customers by type to appropriate tenants
    let individual_customers: Vec<_> = graph.get_nodes_by_label(&Label::new("Individual"))
        .into_iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == "Customer"))
        .collect();

    for node in &individual_customers {
        persist_mgr.persist_create_node("retail_banking", node)?;
    }
    println!("  ✓ Persisted {} individual customers to retail_banking", individual_customers.len());

    let corporate_customers: Vec<_> = graph.get_nodes_by_label(&Label::new("Corporate"))
        .into_iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == "Customer"))
        .collect();

    for node in &corporate_customers {
        persist_mgr.persist_create_node("corporate_banking", node)?;
    }
    println!("  ✓ Persisted {} corporate customers to corporate_banking", corporate_customers.len());

    let hnw_customers: Vec<_> = graph.get_nodes_by_label(&Label::new("HighNetWorth"))
        .into_iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == "Customer"))
        .collect();

    for node in &hnw_customers {
        persist_mgr.persist_create_node("wealth_management", node)?;
    }
    println!("  ✓ Persisted {} HNW customers to wealth_management", hnw_customers.len());

    // Persist edges (OWNS relationships) to appropriate tenants
    // This ensures tenant edge counts are accurate
    let mut retail_edges = 0;
    let mut corporate_edges = 0;
    let mut wealth_edges = 0;

    for node in &individual_customers {
        for edge in graph.get_outgoing_edges(node.id) {
            if edge.edge_type.as_str() == "OWNS" {
                persist_mgr.persist_create_edge("retail_banking", edge)?;
                retail_edges += 1;
            }
        }
    }
    println!("  ✓ Persisted {} edges to retail_banking", retail_edges);

    for node in &corporate_customers {
        for edge in graph.get_outgoing_edges(node.id) {
            if edge.edge_type.as_str() == "OWNS" {
                persist_mgr.persist_create_edge("corporate_banking", edge)?;
                corporate_edges += 1;
            }
        }
    }
    println!("  ✓ Persisted {} edges to corporate_banking", corporate_edges);

    for node in &hnw_customers {
        for edge in graph.get_outgoing_edges(node.id) {
            if edge.edge_type.as_str() == "OWNS" {
                persist_mgr.persist_create_edge("wealth_management", edge)?;
                wealth_edges += 1;
            }
        }
    }
    println!("  ✓ Persisted {} edges to wealth_management", wealth_edges);

    persist_mgr.checkpoint()?;
    println!("  ✓ Checkpoint created ({:.2}s)", persist_start.elapsed().as_secs_f64());
    println!();

    // =========================================================================
    // 4. RUN CYPHER QUERIES
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 4: Running Cypher Queries                                      │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    let engine = QueryEngine::new();

    // Query 1: All customers
    println!("\n  Query: MATCH (c:Customer) RETURN c LIMIT 5");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (c:Customer) RETURN c LIMIT 5", &graph)?;
    println!("  Found {} results ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);
    for record in &result.records {
        if let Some(value) = record.get("c") {
            println!("    {:?}", value);
        }
    }

    // Query 2: High-risk customers
    println!("\n  Query: MATCH (c:HighRisk) RETURN c LIMIT 10");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (c:HighRisk) RETURN c LIMIT 10", &graph)?;
    println!("  Found {} high-risk customers ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);

    // Query 3: Corporate customers
    println!("\n  Query: MATCH (c:Corporate) RETURN c LIMIT 5");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (c:Corporate) RETURN c LIMIT 5", &graph)?;
    println!("  Found {} corporate customers ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);

    // Query 4: Flagged transactions
    println!("\n  Query: MATCH (t:Flagged) RETURN t LIMIT 10");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (t:Flagged) RETURN t LIMIT 10", &graph)?;
    println!("  Found {} flagged transactions ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);

    // Query 5: Branches
    println!("\n  Query: MATCH (b:Branch) RETURN b LIMIT 5");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (b:Branch) RETURN b LIMIT 5", &graph)?;
    println!("  Found {} branches ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);

    // Query 6: Checking accounts
    println!("\n  Query: MATCH (a:Checking) RETURN a LIMIT 5");
    let q_start = Instant::now();
    let result = engine.execute("MATCH (a:Checking) RETURN a LIMIT 5", &graph)?;
    println!("  Found {} checking accounts ({:.3}ms)", result.len(), q_start.elapsed().as_secs_f64() * 1000.0);

    println!();

    // =========================================================================
    // 5. FRAUD DETECTION ANALYSIS
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 5A: Fraud Detection Analysis                                   │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    // High-risk customer connections (KNOWS relationships from high-risk customers)
    println!("\n  Analyzing high-risk customer network...");
    let high_risk = graph.get_nodes_by_label(&Label::new("HighRisk"));
    println!("    High-risk customers (risk_score >= 80): {}", high_risk.len());
    let mut risk_connections = 0;
    for hr_node in &high_risk {
        let edges = graph.get_outgoing_edges(hr_node.id);
        for edge in edges {
            if edge.edge_type.as_str() == "KNOWS" {
                risk_connections += 1;
            }
        }
    }
    println!("    KNOWS connections from high-risk customers: {}", risk_connections);

    // Flagged transactions analysis (transactions with fraud_flag = true)
    println!("\n  Analyzing flagged transactions...");
    let flagged = graph.get_nodes_by_label(&Label::new("Flagged"));
    println!("    Flagged transactions (fraud_flag = true): {}", flagged.len());
    let mut total_flagged_amount = 0.0;
    for tx_node in &flagged {
        if let Some(amount) = tx_node.get_property("amount") {
            if let Some(amt) = amount.as_float() {
                total_flagged_amount += amt;
            }
        }
    }
    println!("    Total flagged transaction amount: ${:.2}", total_flagged_amount);

    // Frozen accounts (accounts with status = "Frozen", ~2% of accounts)
    // Note: Frozen accounts are separate from flagged transactions
    // - Frozen = account status set during account creation
    // - Flagged = individual transactions marked suspicious
    println!("\n  Analyzing account status...");
    let under_review = graph.get_nodes_by_label(&Label::new("Frozen"));
    let total_accounts = graph.get_nodes_by_label(&Label::new("Account")).len();
    let frozen_pct = if total_accounts > 0 {
        (under_review.len() as f64 / total_accounts as f64) * 100.0
    } else {
        0.0
    };
    println!("    Accounts with Frozen status: {} ({:.1}% of {} accounts)",
        under_review.len(), frozen_pct, total_accounts);

    println!();

    // =========================================================================
    // STEP 5B: Money Laundering Pattern Detection
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 5B: Money Laundering Pattern Detection                         │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    // --- Structuring detection: transactions just under $10,000 (BSA threshold) ---
    println!("\n  [1] Structuring Detection (BSA $10,000 Threshold)");
    println!("      Searching for transactions between $9,000 and $10,000...");
    let all_transactions = graph.get_nodes_by_label(&Label::new("Transaction"));
    let mut structuring_suspects: Vec<(String, f64)> = Vec::new();
    for tx_node in &all_transactions {
        if let Some(amount) = tx_node.get_property("amount").and_then(|v| v.as_float()) {
            if amount >= 9000.0 && amount < 10000.0 {
                let tx_id = tx_node.get_property("transaction_id")
                    .and_then(|v| v.as_string())
                    .unwrap_or("unknown")
                    .to_string();
                structuring_suspects.push((tx_id, amount));
            }
        }
    }
    if structuring_suspects.is_empty() {
        println!("      No structuring patterns detected.");
    } else {
        println!("      ALERT: {} transactions just under BSA reporting threshold:",
            structuring_suspects.len());
        for (tx_id, amount) in &structuring_suspects {
            println!("        - {} : ${:.2}", tx_id, amount);
        }
    }

    // --- Rapid succession: accounts with multiple large transactions ---
    println!("\n  [2] Rapid Succession Detection (Multiple Large Transactions)");
    println!("      Searching for accounts with 2+ transactions over $5,000...");
    let all_accounts = graph.get_nodes_by_label(&Label::new("Account"));
    let mut rapid_succession_accounts: Vec<(String, usize, f64)> = Vec::new();
    for acc_node in &all_accounts {
        let acc_edges = graph.get_outgoing_edges(acc_node.id);
        let mut large_tx_count = 0usize;
        let mut large_tx_total = 0.0f64;
        for edge in &acc_edges {
            if edge.edge_type.as_str() == "HAS_TRANSACTION" {
                if let Some(tx_node) = graph.get_node(edge.target) {
                    if let Some(amount) = tx_node.get_property("amount").and_then(|v| v.as_float()) {
                        if amount > 5000.0 {
                            large_tx_count += 1;
                            large_tx_total += amount;
                        }
                    }
                }
            }
        }
        if large_tx_count >= 2 {
            let acc_id = acc_node.get_property("account_id")
                .and_then(|v| v.as_string())
                .unwrap_or("unknown")
                .to_string();
            rapid_succession_accounts.push((acc_id, large_tx_count, large_tx_total));
        }
    }
    if rapid_succession_accounts.is_empty() {
        println!("      No rapid succession patterns detected.");
    } else {
        println!("      ALERT: {} accounts with multiple large transactions:",
            rapid_succession_accounts.len());
        for (acc_id, count, total) in &rapid_succession_accounts {
            println!("        - {} : {} large txns totaling ${:.2}", acc_id, count, total);
        }
    }

    // --- Circular transfer detection: A -> B -> C -> A ---
    println!("\n  [3] Circular Transfer Detection (A -> B -> C -> A)");
    println!("      Scanning TRANSFER_TO edges for circular patterns...");
    let mut circular_patterns: Vec<(NodeId, NodeId, NodeId)> = Vec::new();
    // Build an adjacency list of TRANSFER_TO edges for efficient lookup
    let mut transfer_adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for acc_node in &all_accounts {
        let edges = graph.get_outgoing_edges(acc_node.id);
        for edge in &edges {
            if edge.edge_type.as_str() == "TRANSFER_TO" {
                transfer_adj.entry(acc_node.id).or_default().push(edge.target);
            }
        }
    }
    // For each node A with outgoing TRANSFER_TO, check A->B->C->A
    for (&node_a, targets_b) in &transfer_adj {
        for &node_b in targets_b {
            if let Some(targets_c) = transfer_adj.get(&node_b) {
                for &node_c in targets_c {
                    if node_c == node_a { continue; } // skip A->B->A (length 2)
                    if let Some(targets_from_c) = transfer_adj.get(&node_c) {
                        if targets_from_c.contains(&node_a) {
                            // Found cycle A -> B -> C -> A
                            // Normalize to avoid duplicates: smallest ID first
                            let cycle = [node_a, node_b, node_c];
                            let min_idx = cycle.iter().enumerate()
                                .min_by_key(|&(_, id)| *id)
                                .map(|(i, _)| i).unwrap();
                            let normalized = (
                                cycle[min_idx],
                                cycle[(min_idx + 1) % 3],
                                cycle[(min_idx + 2) % 3],
                            );
                            if !circular_patterns.contains(&normalized) {
                                circular_patterns.push(normalized);
                            }
                        }
                    }
                }
            }
        }
    }

    if circular_patterns.is_empty() {
        println!("      No circular transfer patterns detected.");
    } else {
        println!("      ALERT: {} circular transfer patterns detected:", circular_patterns.len());
        for (a, b, c) in &circular_patterns {
            let name_a = graph.get_node(*a)
                .and_then(|n| n.get_property("account_id"))
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{:?}", a));
            let name_b = graph.get_node(*b)
                .and_then(|n| n.get_property("account_id"))
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{:?}", b));
            let name_c = graph.get_node(*c)
                .and_then(|n| n.get_property("account_id"))
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{:?}", c));
            println!("        - {} -> {} -> {} -> {}", name_a, name_b, name_c, name_a);
        }
    }

    println!();

    // =========================================================================
    // STEP 5C: OFAC / Sanctions Screening
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 5C: OFAC / Sanctions Screening                                 │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    // Simulate OFAC screening with hardcoded watchlist names
    let watchlist = ["Meridian Holdings", "Global Trade Services"];
    println!("\n  Screening {} customers against OFAC watchlist ({} entries)...",
        stats.customers, watchlist.len());

    let all_customers = graph.get_nodes_by_label(&Label::new("Customer"));
    let mut ofac_matches: Vec<(String, String)> = Vec::new();

    for cust_node in &all_customers {
        // Check "name" property (used by inline sample data)
        let name = cust_node.get_property("name")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());
        // Also check "company_name" property (used by TSV-loaded corporate data)
        let company = cust_node.get_property("company_name")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let cust_id = cust_node.get_property("customer_id")
            .and_then(|v| v.as_string())
            .unwrap_or("unknown")
            .to_string();

        for &watchlist_name in &watchlist {
            let matched = if let Some(ref n) = name {
                n.contains(watchlist_name)
            } else {
                false
            };
            let matched_company = if let Some(ref c) = company {
                c.contains(watchlist_name)
            } else {
                false
            };

            if matched || matched_company {
                let display_name = name.clone()
                    .or_else(|| company.clone())
                    .unwrap_or_else(|| "N/A".to_string());
                ofac_matches.push((cust_id.clone(), display_name));
            }
        }
    }

    if ofac_matches.is_empty() {
        println!("  No OFAC watchlist matches found.");
    } else {
        println!("  ALERT: {} potential OFAC watchlist matches:", ofac_matches.len());
        for (cust_id, name) in &ofac_matches {
            println!("    - {} ({})", name, cust_id);
        }
    }
    println!();

    // =========================================================================
    // 6. STATISTICS SUMMARY
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 6: Graph Statistics                                            │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    let customers = graph.get_nodes_by_label(&Label::new("Customer")).len();
    let accounts = graph.get_nodes_by_label(&Label::new("Account")).len();
    let transactions = graph.get_nodes_by_label(&Label::new("Transaction")).len();
    let branches = graph.get_nodes_by_label(&Label::new("Branch")).len();

    println!();
    println!("  Entity Counts:");
    println!("  ┌─────────────────────┬──────────────┐");
    println!("  │ Entity Type         │        Count │");
    println!("  ├─────────────────────┼──────────────┤");
    println!("  │ Customers           │ {:>12} │", customers);
    println!("  │ Accounts            │ {:>12} │", accounts);
    println!("  │ Transactions        │ {:>12} │", transactions);
    println!("  │ Branches            │ {:>12} │", branches);
    println!("  └─────────────────────┴──────────────┘");

    // Customer breakdown
    let individual = graph.get_nodes_by_label(&Label::new("Individual")).len();
    let corporate = graph.get_nodes_by_label(&Label::new("Corporate")).len();
    let hnw = graph.get_nodes_by_label(&Label::new("HighNetWorth")).len();

    println!();
    println!("  Customer Breakdown:");
    println!("  ┌─────────────────────┬──────────────┐");
    println!("  │ Customer Type       │        Count │");
    println!("  ├─────────────────────┼──────────────┤");
    println!("  │ Individual          │ {:>12} │", individual);
    println!("  │ Corporate           │ {:>12} │", corporate);
    println!("  │ High Net Worth      │ {:>12} │", hnw);
    println!("  └─────────────────────┴──────────────┘");

    // Risk breakdown
    let high_risk_count = graph.get_nodes_by_label(&Label::new("HighRisk")).len();
    let medium_risk = graph.get_nodes_by_label(&Label::new("MediumRisk")).len();
    let low_risk = graph.get_nodes_by_label(&Label::new("LowRisk")).len();

    println!();
    println!("  Risk Distribution:");
    println!("  ┌─────────────────────┬──────────────┐");
    println!("  │ Risk Level          │        Count │");
    println!("  ├─────────────────────┼──────────────┤");
    println!("  │ High Risk           │ {:>12} │", high_risk_count);
    println!("  │ Medium Risk         │ {:>12} │", medium_risk);
    println!("  │ Low Risk            │ {:>12} │", low_risk);
    println!("  └─────────────────────┴──────────────┘");

    println!();

    // =========================================================================
    // 7. TENANT USAGE REPORT
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 7: Tenant Usage Report                                         │");
    println!("└──────────────────────────────────────────────────────────────────────┘");
    println!();

    let tenants = persist_mgr.tenants().list_tenants();
    for tenant in tenants {
        let usage = persist_mgr.tenants().get_usage(&tenant.id)?;
        let info = persist_mgr.tenants().get_tenant(&tenant.id)?;
        let max_nodes = info.quotas.max_nodes.unwrap_or(0);
        let pct = if max_nodes > 0 {
            (usage.node_count as f64 / max_nodes as f64) * 100.0
        } else {
            0.0
        };

        println!("  {} ({})", tenant.name, tenant.id);
        println!("    Nodes: {:>10} / {:>10} ({:.1}%)",
            usage.node_count,
            max_nodes,
            pct
        );
        println!("    Edges: {:>10} / {:>10}",
            usage.edge_count,
            info.quotas.max_edges.unwrap_or(0)
        );
        println!();
    }

    // =========================================================================
    // 8. FINALIZE
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 8: Finalizing                                                  │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    persist_mgr.flush()?;
    let total_time = start_time.elapsed();

    println!();
    println!("  ✓ All data flushed to disk");
    println!("  ✓ Total execution time: {:.2}s", total_time.as_secs_f64());
    println!();

    // =========================================================================
    // NLQ FRAUD INVESTIGATION (ClaudeCode)
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ NLQ Fraud Investigation (ClaudeCode)                                │");
    println!("└──────────────────────────────────────────────────────────────────────┘");

    if is_claude_available() {
        println!("  [ok] Claude Code CLI detected — running NLQ queries");
        println!();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: String::new(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some("You are a Cypher query expert for a banking fraud detection knowledge graph.".to_string()),
        };

        persist_mgr.tenants().update_nlq_config("retail_banking", Some(nlq_config.clone())).unwrap();

        let schema_summary = "Node labels: Branch, Customer, Account, Transaction\n\
                              Edge types: BANKS_AT, OWNS, HAS_TRANSACTION, TRANSFER_TO, KNOWS\n\
                              Properties: Customer(name, risk_score, customer_type, kyc_status), \
                              Account(account_number, account_type, balance), \
                              Transaction(amount, transaction_type, fraud_flag, description)";

        let nlq_pipeline = NLQPipeline::new(nlq_config).unwrap();

        let nlq_questions = vec![
            "Show high-risk customers with flagged transactions over $10,000",
            "Find customers with circular transfer patterns",
        ];

        for (i, question) in nlq_questions.iter().enumerate() {
            println!("  NLQ Query {}: \"{}\"", i + 1, question);
            match nlq_pipeline.text_to_cypher(question, schema_summary).await {
                Ok(cypher) => {
                    println!("  Generated Cypher: {}", cypher);
                    match engine.execute(&cypher, &graph) {
                        Ok(batch) => println!("  Results: {} records", batch.len()),
                        Err(e) => println!("  Execution error: {}", e),
                    }
                }
                Err(e) => println!("  NLQ translation error: {}", e),
            }
            println!();
        }
    } else {
        println!("  [skip] Claude Code CLI not found — skipping NLQ queries");
        println!("  Install: https://docs.anthropic.com/en/docs/claude-code");
    }
    println!();

    // =========================================================================
    // SUMMARY
    // =========================================================================
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║                    ENTERPRISE BANKING DEMO COMPLETE                  ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Graph Model:");
    println!();
    println!("    (Customer)─[:OWNS]─>(Account)─[:HAS_TRANSACTION]─>(Transaction)");
    println!("        │                   │");
    println!("        │                   └─[:LOCATED_AT]─>(Branch)");
    println!("        │");
    println!("        ├─[:BANKS_AT]─>(Branch)");
    println!("        ├─[:KNOWS]─>(Customer)");
    println!("        ├─[:REFERRED_BY]─>(Customer)");
    println!("        └─[:EMPLOYED_BY]─>(Customer:Corporate)");
    println!();
    println!("  Use Cases Demonstrated:");
    println!("    ✓ Multi-tenancy (Retail / Corporate / Wealth Management)");
    println!("    ✓ TSV data loading from synthetic data generators");
    println!("    ✓ Customer segmentation (Individual / Corporate / HNW)");
    println!("    ✓ Risk classification (Low / Medium / High)");
    println!("    ✓ Fraud detection patterns (flagged transactions)");
    println!("    ✓ Money laundering detection (structuring / rapid succession / circular)");
    println!("    ✓ OFAC / Sanctions screening");
    println!("    ✓ Network analysis (customer relationships)");
    println!("    ✓ Cypher query execution");
    println!("    ✓ Persistence with checkpointing");
    println!("    ✓ NLQ fraud investigation via ClaudeCode pipeline");
    println!();
    println!("  To generate different dataset sizes:");
    println!("    cd docs/banking/generators");
    println!("    python generate_all.py --size tiny      # ~100 customers");
    println!("    python generate_all.py --size small     # ~500 customers");
    println!("    python generate_all.py --size medium    # ~2,500 customers");
    println!("    python generate_all.py --size large     # ~10,000 customers");
    println!("    python generate_all.py --size enterprise # ~50,000 customers");
    println!();

    Ok(())
}

/// Create sample data when TSV files are not available
fn create_sample_data(graph: &mut GraphStore) -> Result<LoadStats, Box<dyn std::error::Error>> {
    let mut stats = LoadStats::default();

    // -----------------------------------------------------------------------
    // Branches (10)
    // -----------------------------------------------------------------------
    let branch_data: Vec<(&str, &str, &str, &str, &str)> = vec![
        ("Chase Manhattan",     "New York",      "NY", "10001", "245 Park Avenue"),
        ("Wells Fargo Center",  "San Francisco", "CA", "94105", "420 Montgomery St"),
        ("Bank of America Tower", "Charlotte",   "NC", "28255", "100 N Tryon St"),
        ("Citigroup Center",    "New York",      "NY", "10022", "601 Lexington Ave"),
        ("Goldman Sachs HQ",    "New York",      "NY", "10282", "200 West Street"),
        ("Morgan Stanley",      "New York",      "NY", "10036", "1585 Broadway"),
        ("US Bank Center",      "Minneapolis",   "MN", "55402", "800 Nicollet Mall"),
        ("PNC Financial",       "Pittsburgh",    "PA", "15222", "300 Fifth Avenue"),
        ("Truist Center",       "Charlotte",     "NC", "28280", "214 N Tryon St"),
        ("Capital One HQ",      "McLean",        "VA", "22102", "1680 Capital One Dr"),
    ];

    let mut branch_ids: Vec<NodeId> = Vec::new();
    for (i, (name, city, state, zip, address)) in branch_data.iter().enumerate() {
        let node_id = graph.create_node("Branch");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("branch_id", format!("BR-{:04}", i + 1));
            node.set_property("name", *name);
            node.set_property("city", *city);
            node.set_property("state", *state);
            node.set_property("zip_code", *zip);
            node.set_property("address", *address);
            node.set_property("status", "Active");
            node.set_property("employee_count", (50 + i * 15) as i64);
        }
        branch_ids.push(node_id);
        stats.branches += 1;
    }
    println!("    ✓ Created {} sample branches", stats.branches);

    // -----------------------------------------------------------------------
    // Customers (30 total: 20 Individual, 5 Corporate, 5 HighNetWorth)
    // -----------------------------------------------------------------------
    // (name, type, risk_score, kyc_status, email, occupation/company, city, state)
    #[allow(clippy::type_complexity)]
    let customers_data: Vec<(&str, &str, i64, &str, &str, &str, &str, &str)> = vec![
        // --- 20 Individual customers ---
        ("Alice Johnson",      "Individual",   25, "Verified",      "alice.johnson@email.com",    "Software Engineer",     "New York",      "NY"),
        ("Bob Smith",          "Individual",   35, "Verified",      "bob.smith@email.com",        "Accountant",            "San Francisco", "CA"),
        ("Carol Davis",        "Individual",   15, "Verified",      "carol.davis@email.com",      "Teacher",               "Charlotte",     "NC"),
        ("David Lee",          "Individual",   85, "PendingReview", "david.lee@email.com",        "Day Trader",            "New York",      "NY"),
        ("Elena Martinez",     "Individual",   42, "Verified",      "elena.martinez@email.com",   "Marketing Manager",     "Chicago",       "IL"),
        ("Frank Wilson",       "Individual",   58, "Verified",      "frank.wilson@email.com",     "Restaurant Owner",      "New York",      "NY"),
        ("Grace Kim",          "Individual",   22, "Verified",      "grace.kim@email.com",        "Physician",             "San Francisco", "CA"),
        ("Henry Brown",        "Individual",   91, "PendingReview", "henry.brown@email.com",      "Import/Export",         "Miami",         "FL"),
        ("Isabella Garcia",    "Individual",   30, "Verified",      "isabella.garcia@email.com",  "Nurse Practitioner",    "Charlotte",     "NC"),
        ("James Taylor",       "Individual",   18, "Verified",      "james.taylor@email.com",     "Civil Engineer",        "Pittsburgh",    "PA"),
        ("Karen White",        "Individual",   65, "Verified",      "karen.white@email.com",      "Real Estate Agent",     "McLean",        "VA"),
        ("Liam Anderson",      "Individual",   28, "Verified",      "liam.anderson@email.com",    "Pharmacist",            "Minneapolis",   "MN"),
        ("Maria Rodriguez",    "Individual",   45, "Verified",      "maria.rodriguez@email.com",  "Graphic Designer",      "New York",      "NY"),
        ("Nathan Thomas",      "Individual",   72, "Verified",      "nathan.thomas@email.com",    "Cryptocurrency Trader", "San Francisco", "CA"),
        ("Olivia Jackson",     "Individual",   12, "Verified",      "olivia.jackson@email.com",   "College Professor",     "Charlotte",     "NC"),
        ("Patrick Harris",     "Individual",   38, "Verified",      "patrick.harris@email.com",   "Architect",             "Pittsburgh",    "PA"),
        ("Quinn Murphy",       "Individual",   82, "PendingReview", "quinn.murphy@email.com",     "Cash Business Owner",   "New York",      "NY"),
        ("Rachel Clark",       "Individual",   20, "Verified",      "rachel.clark@email.com",     "Veterinarian",          "McLean",        "VA"),
        ("Steven Wright",      "Individual",   55, "Verified",      "steven.wright@email.com",    "Consultant",            "Minneapolis",   "MN"),
        ("Tiffany Lopez",      "Individual",   33, "Verified",      "tiffany.lopez@email.com",    "Dentist",               "San Francisco", "CA"),
        // --- 5 Corporate customers ---
        ("Meridian Holdings LLC",       "Corporate", 75, "Verified",   "contact@meridianholdings.com",    "Meridian Holdings LLC",       "New York",      "NY"),
        ("Apex Manufacturing Inc",      "Corporate", 40, "Verified",   "info@apexmfg.com",                "Apex Manufacturing Inc",      "Charlotte",     "NC"),
        ("Global Trade Services Corp",  "Corporate", 88, "Enhanced",   "compliance@globaltradesvcs.com",  "Global Trade Services Corp",  "San Francisco", "CA"),
        ("Pinnacle Investments Group",  "Corporate", 50, "Verified",   "admin@pinnacleinv.com",           "Pinnacle Investments Group",  "New York",      "NY"),
        ("Silverline Logistics Inc",    "Corporate", 30, "Verified",   "ops@silverlinelogistics.com",     "Silverline Logistics Inc",    "Pittsburgh",    "PA"),
        // --- 5 HighNetWorth customers ---
        ("Victoria Sterling",   "HighNetWorth", 10, "Premium", "v.sterling@private.com",      "Venture Capitalist",    "New York",      "NY"),
        ("William Rockford",    "HighNetWorth", 15, "Premium", "w.rockford@private.com",      "Hedge Fund Manager",    "San Francisco", "CA"),
        ("Diana Ashworth",      "HighNetWorth", 20, "Premium", "d.ashworth@private.com",      "Tech Entrepreneur",     "McLean",        "VA"),
        ("Sebastian Monroe",    "HighNetWorth", 60, "Premium", "s.monroe@private.com",        "Oil & Gas Executive",   "New York",      "NY"),
        ("Catherine Pemberton", "HighNetWorth", 25, "Premium", "c.pemberton@private.com",     "Inherited Wealth",      "Charlotte",     "NC"),
    ];

    let mut customer_ids: Vec<NodeId> = Vec::new();
    for (i, (name, ctype, risk, kyc, email, occupation, city, state)) in customers_data.iter().enumerate() {
        let node_id = graph.create_node("Customer");

        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("customer_id", format!("CUST-{:06}", i + 1));
            node.set_property("name", *name);
            node.set_property("customer_type", *ctype);
            node.set_property("risk_score", *risk);
            node.set_property("kyc_status", *kyc);
            node.set_property("email", *email);
            node.set_property("city", *city);
            node.set_property("state", *state);
            if *ctype == "Corporate" {
                node.set_property("company_name", *occupation);
            } else {
                node.set_property("occupation", *occupation);
            }
        }

        // Add type label
        let _ = graph.add_label_to_node("default", node_id, *ctype);
        // Add risk label
        let risk_label = if *risk >= 80 { "HighRisk" }
            else if *risk >= 50 { "MediumRisk" }
            else { "LowRisk" };
        let _ = graph.add_label_to_node("default", node_id, risk_label);

        customer_ids.push(node_id);
        stats.customers += 1;
    }
    println!("    ✓ Created {} sample customers (20 Individual, 5 Corporate, 5 HNW)", stats.customers);

    // -----------------------------------------------------------------------
    // Accounts (40)
    // Mix of Checking, Savings, CreditCard, Investment, Mortgage, BusinessChecking
    // -----------------------------------------------------------------------
    // (owner_customer_index, account_type, balance)
    #[allow(clippy::type_complexity)]
    let accounts_data: Vec<(usize, &str, f64)> = vec![
        // Individual accounts (customers 0-19 each get 1-2 accounts)
        ( 0, "Checking",         12_450.75),
        ( 0, "Savings",          45_200.00),
        ( 1, "Checking",          8_320.50),
        ( 1, "CreditCard",        2_150.00),
        ( 2, "Checking",          3_780.25),
        ( 3, "Checking",         15_900.00),
        ( 3, "Investment",       85_000.00),
        ( 4, "Checking",          6_540.30),
        ( 5, "Checking",         22_100.00),
        ( 5, "Savings",          18_750.00),
        ( 6, "Checking",         35_200.00),
        ( 6, "Savings",          92_400.00),
        ( 7, "Checking",         28_600.00),
        ( 8, "Checking",          4_250.00),
        ( 8, "Mortgage",        245_000.00),
        ( 9, "Checking",          7_830.00),
        (10, "Checking",         14_200.00),
        (10, "CreditCard",        5_800.00),
        (11, "Checking",          9_100.00),
        (12, "Checking",          5_670.00),
        (13, "Checking",         42_300.00),
        (13, "Investment",      125_000.00),
        (14, "Savings",          31_500.00),
        (15, "Checking",         11_400.00),
        (16, "Checking",         67_800.00),
        (17, "Savings",          22_900.00),
        (18, "Checking",          8_450.00),
        (19, "Checking",         15_300.00),
        // Corporate accounts (customers 20-24)
        (20, "BusinessChecking", 450_000.00),
        (20, "Investment",       750_000.00),
        (21, "BusinessChecking", 320_000.00),
        (22, "BusinessChecking", 890_000.00),
        (23, "BusinessChecking", 560_000.00),
        (24, "BusinessChecking", 210_000.00),
        // HNW accounts (customers 25-29)
        (25, "Investment",     2_500_000.00),
        (25, "Checking",         185_000.00),
        (26, "Investment",     1_800_000.00),
        (27, "Investment",     1_200_000.00),
        (28, "Checking",         350_000.00),
        (29, "Investment",       950_000.00),
    ];

    let mut account_ids: Vec<NodeId> = Vec::new();
    let mut account_owner: Vec<usize> = Vec::new(); // maps account index -> customer index
    for (i, (owner_idx, acc_type, balance)) in accounts_data.iter().enumerate() {
        let node_id = graph.create_node("Account");

        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("account_id", format!("ACC-{:08}", i + 1));
            node.set_property("account_number", format!("****{:04}", 1000 + i));
            node.set_property("account_type", *acc_type);
            node.set_property("balance", *balance);
            node.set_property("status", "Active");
            node.set_property("currency", "USD");
            node.set_property("customer_id", format!("CUST-{:06}", owner_idx + 1));
        }

        let _ = graph.add_label_to_node("default", node_id, *acc_type);

        account_ids.push(node_id);
        account_owner.push(*owner_idx);
        stats.accounts += 1;
    }
    println!("    ✓ Created {} sample accounts", stats.accounts);

    // -----------------------------------------------------------------------
    // Transactions (50)
    // Including 5 suspicious structuring transactions (just under $10,000)
    // -----------------------------------------------------------------------
    // (account_index, tx_type, amount, description, flagged)
    #[allow(clippy::type_complexity)]
    let transactions_data: Vec<(usize, &str, f64, &str, bool)> = vec![
        // Normal retail transactions
        ( 0, "Deposit",    5_000.00, "Payroll direct deposit",             false),
        ( 0, "Payment",      125.50, "Electric bill - ConEd",             false),
        ( 0, "Purchase",      67.82, "Whole Foods Market",                false),
        ( 1, "Deposit",    2_000.00, "Transfer from checking",            false),
        ( 2, "Withdrawal",   500.00, "ATM withdrawal",                    false),
        ( 2, "Purchase",     234.99, "Amazon.com",                        false),
        ( 3, "Payment",    1_200.00, "Credit card payment",               false),
        ( 4, "Deposit",    3_200.00, "Payroll direct deposit",            false),
        ( 5, "Purchase",      45.00, "Uber ride",                         false),
        ( 6, "Deposit",   12_000.00, "Wire transfer from client",         false),
        ( 7, "Purchase",     189.99, "Best Buy electronics",              false),
        ( 8, "Payment",      850.00, "Rent payment",                      false),
        ( 9, "Deposit",    4_500.00, "Payroll direct deposit",            false),
        (10, "Purchase",      92.50, "Shell gas station",                 false),
        (11, "Transfer",   3_000.00, "Transfer to savings",               false),
        (12, "Deposit",    7_500.00, "Consulting fee",                    false),
        (13, "Withdrawal", 2_000.00, "Cash withdrawal",                   false),
        (14, "Payment",    2_450.00, "Mortgage payment",                  false),
        (15, "Purchase",     156.00, "Target department store",           false),
        (16, "Deposit",    6_800.00, "Payroll direct deposit",            false),
        // Wire transfers and larger movements
        (17, "Transfer",  15_000.00, "Wire to investment account",        false),
        (18, "Deposit",   25_000.00, "Quarterly dividend",                false),
        (19, "Purchase",     320.00, "Delta Airlines ticket",             false),
        (20, "Transfer",  50_000.00, "Vendor payment - supplies",         false),
        (21, "Deposit",   75_000.00, "Client payment received",           false),
        // Suspicious structuring transactions (just under $10,000 - BSA threshold)
        ( 5, "Deposit",    9_900.00, "Cash deposit - business revenue",   true),
        ( 5, "Deposit",    9_850.00, "Cash deposit - weekend sales",      true),
        (12, "Deposit",    9_950.00, "Cash deposit",                      true),
        (12, "Deposit",    9_750.00, "Cash deposit - inventory sale",     true),
        (24, "Deposit",    9_999.00, "Cash deposit - consulting",         true),
        // More normal transactions
        (22, "Transfer", 100_000.00, "Quarterly investment rebalance",    false),
        (23, "Deposit",   35_000.00, "Monthly management fee",            false),
        (24, "Purchase",   1_250.00, "Office supplies",                   false),
        (25, "Deposit",  150_000.00, "Portfolio dividend payout",         false),
        (26, "Transfer",  75_000.00, "Fund reallocation",                 false),
        (27, "Deposit",   50_000.00, "Stock sale proceeds",               false),
        (28, "Purchase",   8_500.00, "Luxury watch - Rolex Boutique",     false),
        (29, "Deposit",   30_000.00, "Trust distribution",                false),
        // Additional retail volume
        ( 0, "Purchase",     312.45, "Costco wholesale",                  false),
        ( 2, "Purchase",      28.99, "Netflix subscription",              false),
        ( 4, "Purchase",      89.00, "Gym membership",                    false),
        ( 6, "Transfer",   5_000.00, "Transfer to savings",               false),
        ( 8, "Deposit",    3_800.00, "Payroll direct deposit",            false),
        (10, "Purchase",     445.00, "Home Depot supplies",               false),
        (15, "Deposit",    5_200.00, "Payroll direct deposit",            false),
        (16, "Purchase",      55.00, "Spotify + Apple Music",             false),
        (19, "Purchase",     175.00, "Nordstrom clothing",                false),
        (21, "Transfer",  45_000.00, "Supplier payment",                  false),
        (23, "Deposit",   80_000.00, "Real estate closing proceeds",      false),
    ];

    let mut transaction_ids: Vec<NodeId> = Vec::new();
    let mut transaction_account: Vec<usize> = Vec::new(); // maps tx index -> account index
    for (i, (acc_idx, tx_type, amount, description, flagged)) in transactions_data.iter().enumerate() {
        let node_id = graph.create_node("Transaction");

        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("transaction_id", format!("TXN-{:010}", i + 1));
            node.set_property("transaction_type", *tx_type);
            node.set_property("amount", *amount);
            node.set_property("description", *description);
            node.set_property("status", "Completed");
            node.set_property("account_id", format!("ACC-{:08}", acc_idx + 1));
            node.set_property("fraud_flag", *flagged);
            if *flagged {
                node.set_property("fraud_score", 0.85_f64);
            }
        }

        let _ = graph.add_label_to_node("default", node_id, *tx_type);
        if *flagged {
            let _ = graph.add_label_to_node("default", node_id, "Flagged");
            let _ = graph.add_label_to_node("default", node_id, "Fraud");
        }

        transaction_ids.push(node_id);
        transaction_account.push(*acc_idx);
        stats.transactions += 1;
    }
    println!("    ✓ Created {} sample transactions (5 flagged as suspicious)", stats.transactions);

    // -----------------------------------------------------------------------
    // Relationships
    // -----------------------------------------------------------------------

    // OWNS: customer -> account
    for (acc_idx, &owner_idx) in account_owner.iter().enumerate() {
        if owner_idx < customer_ids.len() && acc_idx < account_ids.len() {
            graph.create_edge(customer_ids[owner_idx], account_ids[acc_idx], "OWNS").unwrap();
            stats.relationships += 1;
        }
    }

    // BANKS_AT: customer -> branch (assign each customer to a branch)
    for (i, &cust_id) in customer_ids.iter().enumerate() {
        let branch_idx = i % branch_ids.len();
        graph.create_edge(cust_id, branch_ids[branch_idx], "BANKS_AT").unwrap();
        stats.relationships += 1;
    }

    // HAS_TRANSACTION: account -> transaction
    for (tx_idx, &acc_idx) in transaction_account.iter().enumerate() {
        if acc_idx < account_ids.len() && tx_idx < transaction_ids.len() {
            graph.create_edge(account_ids[acc_idx], transaction_ids[tx_idx], "HAS_TRANSACTION").unwrap();
            stats.relationships += 1;
        }
    }

    // KNOWS: social connections between customers
    let knows_pairs: Vec<(usize, usize)> = vec![
        (0, 1), (0, 4), (1, 2), (2, 8), (3, 7),   // Individual connections
        (4, 5), (5, 6), (6, 11), (9, 15), (10, 17),
        (12, 13), (14, 19), (16, 18),
        (3, 16),   // High-risk individuals know each other
        (7, 3),    // Henry Brown knows David Lee (both high risk)
        (20, 23),  // Corporate connections
    ];
    for (a, b) in &knows_pairs {
        if *a < customer_ids.len() && *b < customer_ids.len() {
            graph.create_edge(customer_ids[*a], customer_ids[*b], "KNOWS").unwrap();
            stats.relationships += 1;
        }
    }

    // TRANSFER_TO: account -> account (for circular detection testing)
    // Create a circular transfer pattern: ACC-06 -> ACC-13 -> ACC-25 -> ACC-06
    // (David Lee's account -> Henry Brown's account -> Quinn Murphy's account -> back)
    let transfer_pairs: Vec<(usize, usize)> = vec![
        (5, 12),   // ACC-06 -> ACC-13
        (12, 24),  // ACC-13 -> ACC-25
        (24, 5),   // ACC-25 -> ACC-06 (completes the circle)
        (0, 2),    // Normal transfers
        (6, 21),   // Investment account transfers
        (29, 34),  // Corporate to HNW
    ];
    for (from_idx, to_idx) in &transfer_pairs {
        if *from_idx < account_ids.len() && *to_idx < account_ids.len() {
            graph.create_edge(account_ids[*from_idx], account_ids[*to_idx], "TRANSFER_TO").unwrap();
            stats.relationships += 1;
        }
    }

    println!("    ✓ Created {} sample relationships (OWNS, BANKS_AT, HAS_TRANSACTION, KNOWS, TRANSFER_TO)",
        stats.relationships);

    Ok(stats)
}
