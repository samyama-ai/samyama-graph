//! Enterprise Banking Demo - Samyama Graph Database
//!
//! This example demonstrates enterprise-level banking data modeling with:
//! - Loading synthetic data from TSV files (customers, accounts, branches, transactions)
//! - Graph-based fraud detection patterns
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
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;

use samyama::{
    PersistenceManager, ResourceQuotas, QueryEngine,
    graph::{GraphStore, Label, NodeId},
};

fn pause() {
    print!("\n👉 Press Enter to continue...");
    std::io::stdout().flush().unwrap();
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer).unwrap();
    println!();
}

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║     SAMYAMA GRAPH DATABASE - Enterprise Banking Demo                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    pause();

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

    pause();

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

    pause();

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

    pause();

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

    pause();

    // =========================================================================
    // 5. FRAUD DETECTION ANALYSIS
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────────┐");
    println!("│ STEP 5: Fraud Detection Analysis                                    │");
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

    pause();

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

    pause();

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

    pause();

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
    println!("    ✓ Network analysis (customer relationships)");
    println!("    ✓ Cypher query execution");
    println!("    ✓ Persistence with checkpointing");
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

    // Create branches
    for (i, (name, city, state)) in [
        ("Main Branch", "New York", "NY"),
        ("West Coast HQ", "San Francisco", "CA"),
        ("Chicago Regional", "Chicago", "IL"),
    ].iter().enumerate() {
        let node_id = graph.create_node("Branch");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("branch_id", format!("BR-{:04}", i + 1));
            node.set_property("name", *name);
            node.set_property("city", *city);
            node.set_property("state", *state);
            node.set_property("status", "Active");
        }
        stats.branches += 1;
    }
    println!("    ✓ Created {} sample branches", stats.branches);

    // Create customers
    let customers_data = [
        ("Alice Johnson", "Individual", 25, "Verified"),
        ("Bob Smith", "Individual", 35, "Verified"),
        ("Acme Corp", "Corporate", 20, "Verified"),
        ("Carol Davis", "HighNetWorth", 15, "Premium"),
        ("David Lee", "Individual", 85, "PendingReview"),
    ];

    for (i, (name, ctype, risk, kyc)) in customers_data.iter().enumerate() {
        let node_id = graph.create_node("Customer");

        // Set properties first (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("customer_id", format!("CUST-{:06}", i + 1));
            node.set_property("name", *name);
            node.set_property("customer_type", *ctype);
            node.set_property("risk_score", *risk as i64);
            node.set_property("kyc_status", *kyc);
        }

        // Add labels AFTER releasing mutable borrow
        // Using graph.add_label_to_node("default", ) ensures the label_index is updated,
        // so MATCH (c:Individual) queries work correctly
        let _ = graph.add_label_to_node("default", node_id, *ctype);
        let risk_label = if *risk >= 80 { "HighRisk" }
            else if *risk >= 50 { "MediumRisk" }
            else { "LowRisk" };
        let _ = graph.add_label_to_node("default", node_id, risk_label);

        stats.customers += 1;
    }
    println!("    ✓ Created {} sample customers", stats.customers);

    // Create accounts
    for i in 0..8 {
        let node_id = graph.create_node("Account");
        let acc_type = match i % 4 {
            0 => "Checking",
            1 => "Savings",
            2 => "CreditCard",
            _ => "Investment",
        };

        // Set properties first (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("account_id", format!("ACC-{:08}", i + 1));
            node.set_property("account_type", acc_type);
            node.set_property("balance", 1000.0 + (i as f64 * 5000.0));
            node.set_property("status", "Active");
        }

        // Add label AFTER releasing mutable borrow
        let _ = graph.add_label_to_node("default", node_id, acc_type);

        stats.accounts += 1;
    }
    println!("    ✓ Created {} sample accounts", stats.accounts);

    // Create transactions
    for i in 0..20 {
        let node_id = graph.create_node("Transaction");
        let tx_type = match i % 5 {
            0 => "Deposit",
            1 => "Withdrawal",
            2 => "Transfer",
            3 => "Payment",
            _ => "Purchase",
        };
        let is_flagged = i == 15 || i == 18;

        // Set properties first (within mutable borrow scope)
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("transaction_id", format!("TXN-{:010}", i + 1));
            node.set_property("transaction_type", tx_type);
            node.set_property("amount", 50.0 + (i as f64 * 100.0));
            node.set_property("status", "Completed");
        }

        // Add labels AFTER releasing mutable borrow
        let _ = graph.add_label_to_node("default", node_id, tx_type);
        if is_flagged {
            let _ = graph.add_label_to_node("default", node_id, "Flagged");
        }

        stats.transactions += 1;
    }
    println!("    ✓ Created {} sample transactions", stats.transactions);

    Ok(stats)
}
