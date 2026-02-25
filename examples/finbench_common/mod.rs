//! Shared LDBC FinBench data loading and generation utilities.
//!
//! Used by both `finbench_loader` and `finbench_benchmark` examples.
//!
//! Since LDBC FinBench does not have a widely-available SF1 CSV download like
//! LDBC SNB, this module provides both:
//!   1. CSV file loading (when pre-generated data is available)
//!   2. Synthetic data generation that matches the FinBench schema

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::path::{Path, PathBuf};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// ID MAPPINGS
// ============================================================================

pub struct IdMaps {
    pub account: HashMap<i64, NodeId>,
    pub person: HashMap<i64, NodeId>,
    pub company: HashMap<i64, NodeId>,
    pub loan: HashMap<i64, NodeId>,
    pub medium: HashMap<i64, NodeId>,
}

impl IdMaps {
    pub fn new() -> Self {
        Self {
            account: HashMap::new(),
            person: HashMap::new(),
            company: HashMap::new(),
            loan: HashMap::new(),
            medium: HashMap::new(),
        }
    }
}

// ============================================================================
// GENERIC HELPERS
// ============================================================================

/// Load nodes from a pipe-delimited CSV file.
/// `parse_props` receives the header-keyed row and returns (key, PropertyValue) pairs.
pub fn load_nodes<F>(
    path: &Path,
    label: &str,
    graph: &mut GraphStore,
    id_map: &mut HashMap<i64, NodeId>,
    parse_props: F,
) -> Result<usize, Error>
where
    F: Fn(&[&str], &[&str]) -> Vec<(&'static str, PropertyValue)>,
{
    if !path.exists() {
        eprintln!("  WARNING: {} not found, skipping", path.display());
        return Ok(0);
    }

    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('|').collect();

    let id_col = headers.iter().position(|h| *h == "id")
        .ok_or_else(|| format!("No 'id' column in {}", path.display()))?;

    let mut count = 0usize;
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() { continue; }

        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() <= id_col { continue; }

        let csv_id: i64 = fields[id_col].parse()?;

        let node_id = graph.create_node(label);

        // Set properties
        let props = parse_props(&headers, &fields);
        if let Some(node) = graph.get_node_mut(node_id) {
            for (key, val) in props {
                node.set_property(key, val);
            }
            // Store the FinBench id as a property too
            node.set_property("id", csv_id);
        }

        id_map.insert(csv_id, node_id);
        count += 1;

        if count % 500_000 == 0 && io::stderr().is_terminal() {
            eprint!("\r  {:16} {:>12} nodes...          ", label, format_num(count));
        }
    }

    Ok(count)
}

/// Load edges from a pipe-delimited CSV file.
/// `parse_props` receives (headers, fields) and returns edge properties (may be empty).
pub fn load_edges<F>(
    path: &Path,
    edge_type: &str,
    graph: &mut GraphStore,
    src_map: &HashMap<i64, NodeId>,
    tgt_map: &HashMap<i64, NodeId>,
    parse_props: F,
) -> Result<usize, Error>
where
    F: Fn(&[&str], &[&str]) -> Vec<(&'static str, PropertyValue)>,
{
    if !path.exists() {
        eprintln!("  WARNING: {} not found, skipping", path.display());
        return Ok(0);
    }

    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('|').collect();

    let mut count = 0usize;
    let mut skipped = 0usize;
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() { continue; }

        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() < 2 { continue; }

        let src_id: i64 = match fields[0].parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };
        let tgt_id: i64 = match fields[1].parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };

        let src_node = match src_map.get(&src_id) {
            Some(&n) => n,
            None => { skipped += 1; continue; }
        };
        let tgt_node = match tgt_map.get(&tgt_id) {
            Some(&n) => n,
            None => { skipped += 1; continue; }
        };

        match graph.create_edge(src_node, tgt_node, edge_type) {
            Ok(edge_id) => {
                let props = parse_props(&headers, &fields);
                if !props.is_empty() {
                    if let Some(edge) = graph.get_edge_mut(edge_id) {
                        for (key, val) in props {
                            edge.set_property(key, val);
                        }
                    }
                }
                count += 1;
            }
            Err(_) => { skipped += 1; }
        }

        if count % 500_000 == 0 && count > 0 && io::stderr().is_terminal() {
            eprint!("\r  {:42} {:>12} edges...          ", edge_type, format_num(count));
        }
    }

    if skipped > 0 {
        eprintln!("  (skipped {} rows for {})", format_num(skipped), edge_type);
    }

    Ok(count)
}

// ============================================================================
// PROPERTY PARSERS — Field helpers
// ============================================================================

pub fn field_str(headers: &[&str], fields: &[&str], name: &str) -> Option<String> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

pub fn field_i64(headers: &[&str], fields: &[&str], name: &str) -> Option<i64> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .and_then(|v| v.parse().ok())
}

pub fn field_f64(headers: &[&str], fields: &[&str], name: &str) -> Option<f64> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .and_then(|v| v.parse().ok())
}

pub fn field_bool(headers: &[&str], fields: &[&str], name: &str) -> Option<bool> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .and_then(|v| match v.to_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        })
}

// ============================================================================
// PROPERTY PARSERS — Node types
// ============================================================================

pub fn props_account(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_i64(headers, fields, "createTime") {
        props.push(("createTime", PropertyValue::DateTime(v)));
    }
    if let Some(v) = field_bool(headers, fields, "isBlocked") {
        props.push(("isBlocked", PropertyValue::Boolean(v)));
    }
    if let Some(v) = field_str(headers, fields, "accountType") {
        props.push(("accountType", PropertyValue::String(v)));
    }
    props
}

pub fn props_person(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "name") {
        props.push(("name", PropertyValue::String(v)));
    }
    if let Some(v) = field_bool(headers, fields, "isBlocked") {
        props.push(("isBlocked", PropertyValue::Boolean(v)));
    }
    props
}

pub fn props_company(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "name") {
        props.push(("name", PropertyValue::String(v)));
    }
    if let Some(v) = field_bool(headers, fields, "isBlocked") {
        props.push(("isBlocked", PropertyValue::Boolean(v)));
    }
    props
}

pub fn props_loan(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_f64(headers, fields, "loanAmount") {
        props.push(("loanAmount", PropertyValue::Float(v)));
    }
    if let Some(v) = field_f64(headers, fields, "balance") {
        props.push(("balance", PropertyValue::Float(v)));
    }
    props
}

pub fn props_medium(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "mediumType") {
        props.push(("mediumType", PropertyValue::String(v)));
    }
    if let Some(v) = field_bool(headers, fields, "isBlocked") {
        props.push(("isBlocked", PropertyValue::Boolean(v)));
    }
    props
}

// ============================================================================
// PROPERTY PARSERS — Edge types
// ============================================================================

pub fn no_props(_headers: &[&str], _fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    Vec::new()
}

/// timestamp only (for OWN, SIGN_IN, APPLY, GUARANTEE)
pub fn props_timestamp(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_i64(headers, fields, "timestamp") {
        props.push(("timestamp", PropertyValue::DateTime(v)));
    }
    // Fallback: try column index 2
    if props.is_empty() && fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("timestamp", PropertyValue::DateTime(v)));
        }
    }
    props
}

/// timestamp + amount (for TRANSFER, WITHDRAW, DEPOSIT, REPAY)
pub fn props_timestamp_amount(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_i64(headers, fields, "timestamp") {
        props.push(("timestamp", PropertyValue::DateTime(v)));
    } else if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("timestamp", PropertyValue::DateTime(v)));
        }
    }
    if let Some(v) = field_f64(headers, fields, "amount") {
        props.push(("amount", PropertyValue::Float(v)));
    } else if fields.len() > 3 {
        if let Ok(v) = fields[3].parse::<f64>() {
            props.push(("amount", PropertyValue::Float(v)));
        }
    }
    props
}

/// timestamp + ratio (for INVEST)
pub fn props_timestamp_ratio(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_i64(headers, fields, "timestamp") {
        props.push(("timestamp", PropertyValue::DateTime(v)));
    } else if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("timestamp", PropertyValue::DateTime(v)));
        }
    }
    if let Some(v) = field_f64(headers, fields, "ratio") {
        props.push(("ratio", PropertyValue::Float(v)));
    } else if fields.len() > 3 {
        if let Ok(v) = fields[3].parse::<f64>() {
            props.push(("ratio", PropertyValue::Float(v)));
        }
    }
    props
}

// ============================================================================
// FORMATTING
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(ch);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else {
        format!("{:.1}s", secs)
    }
}

/// Print a final summary line, clearing any inline progress.
pub fn print_done(msg: &str) {
    eprintln!("\r{:80}", msg);
}

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
}

// ============================================================================
// CSV DATASET LOADER
// ============================================================================

/// Load the FinBench dataset from CSV files into the given GraphStore.
/// Expected directory structure:
///   data_dir/
///     account.csv, person.csv, company.csv, loan.csv, medium.csv
///     own.csv, transfer.csv, withdraw.csv, deposit.csv, repay.csv
///     signIn.csv, apply.csv, invest.csv, guarantee.csv
pub fn load_dataset(graph: &mut GraphStore, data_dir: &Path) -> Result<LoadResult, Error> {
    let mut ids = IdMaps::new();
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    // ====================================================================
    // PHASE 1: Load all nodes
    // ====================================================================
    eprintln!("=== Phase 1: Loading Nodes ===");

    let t = std::time::Instant::now();
    let n = load_nodes(&data_dir.join("account.csv"), "Account", graph, &mut ids.account, props_account)?;
    print_done(&format!("  Account:       {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&data_dir.join("person.csv"), "Person", graph, &mut ids.person, props_person)?;
    print_done(&format!("  Person:        {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&data_dir.join("company.csv"), "Company", graph, &mut ids.company, props_company)?;
    print_done(&format!("  Company:       {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&data_dir.join("loan.csv"), "Loan", graph, &mut ids.loan, props_loan)?;
    print_done(&format!("  Loan:          {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&data_dir.join("medium.csv"), "Medium", graph, &mut ids.medium, props_medium)?;
    print_done(&format!("  Medium:        {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    eprintln!();

    // ====================================================================
    // PHASE 2: Load all edges
    // ====================================================================
    eprintln!("=== Phase 2: Loading Edges ===");

    // OWN: Person/Company -> Account
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("personOwnAccount.csv"), "OWN", graph, &ids.person, &ids.account, props_timestamp)?;
    print_done(&format!("  OWN (Person->Account):                 {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("companyOwnAccount.csv"), "OWN", graph, &ids.company, &ids.account, props_timestamp)?;
    print_done(&format!("  OWN (Company->Account):                {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // TRANSFER: Account -> Account
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("transfer.csv"), "TRANSFER", graph, &ids.account, &ids.account, props_timestamp_amount)?;
    print_done(&format!("  TRANSFER (Account->Account):           {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // WITHDRAW: Account -> Account
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("withdraw.csv"), "WITHDRAW", graph, &ids.account, &ids.account, props_timestamp_amount)?;
    print_done(&format!("  WITHDRAW (Account->Account):           {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // DEPOSIT: Loan -> Account
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("deposit.csv"), "DEPOSIT", graph, &ids.loan, &ids.account, props_timestamp_amount)?;
    print_done(&format!("  DEPOSIT (Loan->Account):               {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // REPAY: Account -> Loan
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("repay.csv"), "REPAY", graph, &ids.account, &ids.loan, props_timestamp_amount)?;
    print_done(&format!("  REPAY (Account->Loan):                 {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // SIGN_IN: Account -> Medium
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("signIn.csv"), "SIGN_IN", graph, &ids.account, &ids.medium, props_timestamp)?;
    print_done(&format!("  SIGN_IN (Account->Medium):             {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // APPLY: Person/Company -> Loan
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("personApplyLoan.csv"), "APPLY", graph, &ids.person, &ids.loan, props_timestamp)?;
    print_done(&format!("  APPLY (Person->Loan):                  {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("companyApplyLoan.csv"), "APPLY", graph, &ids.company, &ids.loan, props_timestamp)?;
    print_done(&format!("  APPLY (Company->Loan):                 {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // INVEST: Company/Person -> Company
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("companyInvestCompany.csv"), "INVEST", graph, &ids.company, &ids.company, props_timestamp_ratio)?;
    print_done(&format!("  INVEST (Company->Company):             {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("personInvestCompany.csv"), "INVEST", graph, &ids.person, &ids.company, props_timestamp_ratio)?;
    print_done(&format!("  INVEST (Person->Company):              {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // GUARANTEE: Company/Person -> Company/Person
    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("companyGuaranteeCompany.csv"), "GUARANTEE", graph, &ids.company, &ids.company, props_timestamp)?;
    print_done(&format!("  GUARANTEE (Company->Company):          {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&data_dir.join("personGuaranteePerson.csv"), "GUARANTEE", graph, &ids.person, &ids.person, props_timestamp)?;
    print_done(&format!("  GUARANTEE (Person->Person):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    Ok(LoadResult { total_nodes, total_edges })
}

// ============================================================================
// SYNTHETIC DATA GENERATOR
// ============================================================================

/// Configuration for synthetic data generation.
pub struct GeneratorConfig {
    pub num_persons: usize,
    pub num_companies: usize,
    pub num_accounts: usize,
    pub num_loans: usize,
    pub num_mediums: usize,
    pub num_transfers: usize,
    pub num_withdrawals: usize,
    pub num_deposits: usize,
    pub num_repayments: usize,
    pub num_sign_ins: usize,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            num_persons: 1_000,
            num_companies: 500,
            num_accounts: 5_000,
            num_loans: 1_000,
            num_mediums: 200,
            num_transfers: 20_000,
            num_withdrawals: 5_000,
            num_deposits: 2_000,
            num_repayments: 3_000,
            num_sign_ins: 8_000,
        }
    }
}

/// Simple deterministic pseudo-random number generator (xorshift64).
/// Used instead of `rand` so this module has no extra dependencies.
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: if seed == 0 { 1 } else { seed } }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, max: usize) -> usize {
        (self.next_u64() % max as u64) as usize
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() & 0x001F_FFFF_FFFF_FFFF) as f64 / (1u64 << 53) as f64
    }

    fn next_bool(&mut self, prob: f64) -> bool {
        self.next_f64() < prob
    }
}

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nathan", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
    "Yuki", "Zara", "Amit", "Bala", "Chen", "Dev", "Elena", "Feng",
    "Gita", "Hans", "Ines", "Jun", "Kira", "Liam", "Mai", "Noor",
    "Omar", "Priya", "Raj", "Suki", "Tao", "Uri", "Vera", "Wei",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Kumar", "Singh", "Wang", "Li",
    "Zhang", "Chen", "Yang", "Huang", "Zhao", "Wu", "Patel", "Shah",
    "Gupta", "Muller", "Schmidt", "Fischer", "Weber", "Meyer", "Tanaka",
    "Suzuki", "Takahashi", "Watanabe", "Kim", "Park", "Lee", "Choi",
];

const COMPANY_SUFFIXES: &[&str] = &[
    "Corp", "Inc", "Ltd", "Group", "Holdings", "Partners", "Capital",
    "Technologies", "Financial", "Solutions", "Global", "Systems",
    "Ventures", "Dynamics", "Industries", "Consulting", "Networks",
    "Analytics", "Digital", "Services",
];

const COMPANY_PREFIXES: &[&str] = &[
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Theta",
    "Atlas", "Nova", "Apex", "Vertex", "Nexus", "Prism", "Quantum",
    "Stellar", "Fusion", "Orion", "Phoenix", "Summit", "Prime",
    "Azure", "Cobalt", "Emerald", "Falcon", "Glacier", "Harbor",
    "Ivory", "Jade", "Keystone", "Lunar", "Marble", "Neptune",
];

const ACCOUNT_TYPES: &[&str] = &["checking", "savings", "investment", "business"];
const MEDIUM_TYPES: &[&str] = &["phone", "tablet", "laptop", "desktop", "ATM"];

/// Generate a synthetic FinBench dataset directly into GraphStore.
pub fn generate_dataset(graph: &mut GraphStore, config: &GeneratorConfig) -> LoadResult {
    let mut rng = SimpleRng::new(42);
    let mut ids = IdMaps::new();
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    // Base timestamp: 2020-01-01 00:00:00 UTC (in milliseconds)
    let base_ts: i64 = 1_577_836_800_000;
    // Time range: 3 years in milliseconds
    let time_range: i64 = 3 * 365 * 24 * 3600 * 1000;

    let random_ts = |rng: &mut SimpleRng| -> i64 {
        base_ts + (rng.next_u64() % time_range as u64) as i64
    };

    // ====================================================================
    // PHASE 1: Generate nodes
    // ====================================================================
    eprintln!("=== Phase 1: Generating Nodes ===");

    // Persons
    let t = std::time::Instant::now();
    for i in 0..config.num_persons {
        let person_id = (i + 1) as i64;
        let node_id = graph.create_node("Person");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("id", person_id);
            let first = FIRST_NAMES[rng.next_usize(FIRST_NAMES.len())];
            let last = LAST_NAMES[rng.next_usize(LAST_NAMES.len())];
            node.set_property("name", PropertyValue::String(format!("{} {}", first, last)));
            node.set_property("isBlocked", PropertyValue::Boolean(rng.next_bool(0.02)));
        }
        ids.person.insert(person_id, node_id);
        total_nodes += 1;
    }
    print_done(&format!("  Person:        {:>12} nodes ({})", format_num(config.num_persons), format_duration(t.elapsed())));

    // Companies
    let t = std::time::Instant::now();
    for i in 0..config.num_companies {
        let company_id = (i + 1) as i64;
        let node_id = graph.create_node("Company");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("id", company_id);
            let prefix = COMPANY_PREFIXES[rng.next_usize(COMPANY_PREFIXES.len())];
            let suffix = COMPANY_SUFFIXES[rng.next_usize(COMPANY_SUFFIXES.len())];
            node.set_property("name", PropertyValue::String(format!("{} {}", prefix, suffix)));
            node.set_property("isBlocked", PropertyValue::Boolean(rng.next_bool(0.01)));
        }
        ids.company.insert(company_id, node_id);
        total_nodes += 1;
    }
    print_done(&format!("  Company:       {:>12} nodes ({})", format_num(config.num_companies), format_duration(t.elapsed())));

    // Accounts
    let t = std::time::Instant::now();
    for i in 0..config.num_accounts {
        let account_id = (i + 1) as i64;
        let node_id = graph.create_node("Account");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("id", account_id);
            node.set_property("createTime", PropertyValue::DateTime(random_ts(&mut rng)));
            node.set_property("isBlocked", PropertyValue::Boolean(rng.next_bool(0.03)));
            let acct_type = ACCOUNT_TYPES[rng.next_usize(ACCOUNT_TYPES.len())];
            node.set_property("accountType", PropertyValue::String(acct_type.to_string()));
        }
        ids.account.insert(account_id, node_id);
        total_nodes += 1;
    }
    print_done(&format!("  Account:       {:>12} nodes ({})", format_num(config.num_accounts), format_duration(t.elapsed())));

    // Loans
    let t = std::time::Instant::now();
    for i in 0..config.num_loans {
        let loan_id = (i + 1) as i64;
        let node_id = graph.create_node("Loan");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("id", loan_id);
            // Loan amounts between 1,000 and 500,000
            let amount = 1000.0 + rng.next_f64() * 499_000.0;
            node.set_property("loanAmount", PropertyValue::Float(amount));
            // Balance is some fraction of the original amount
            let balance = amount * rng.next_f64();
            node.set_property("balance", PropertyValue::Float(balance));
        }
        ids.loan.insert(loan_id, node_id);
        total_nodes += 1;
    }
    print_done(&format!("  Loan:          {:>12} nodes ({})", format_num(config.num_loans), format_duration(t.elapsed())));

    // Mediums
    let t = std::time::Instant::now();
    for i in 0..config.num_mediums {
        let medium_id = (i + 1) as i64;
        let node_id = graph.create_node("Medium");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("id", medium_id);
            let mtype = MEDIUM_TYPES[rng.next_usize(MEDIUM_TYPES.len())];
            node.set_property("mediumType", PropertyValue::String(mtype.to_string()));
            node.set_property("isBlocked", PropertyValue::Boolean(rng.next_bool(0.02)));
        }
        ids.medium.insert(medium_id, node_id);
        total_nodes += 1;
    }
    print_done(&format!("  Medium:        {:>12} nodes ({})", format_num(config.num_mediums), format_duration(t.elapsed())));

    eprintln!();

    // ====================================================================
    // PHASE 2: Generate edges
    // ====================================================================
    eprintln!("=== Phase 2: Generating Edges ===");

    // Collect IDs for random selection
    let person_ids: Vec<i64> = (1..=config.num_persons as i64).collect();
    let company_ids: Vec<i64> = (1..=config.num_companies as i64).collect();
    let account_ids: Vec<i64> = (1..=config.num_accounts as i64).collect();
    let loan_ids: Vec<i64> = (1..=config.num_loans as i64).collect();
    let medium_ids: Vec<i64> = (1..=config.num_mediums as i64).collect();

    // OWN: Each person owns 1-5 accounts, each company owns 1-3 accounts
    let t = std::time::Instant::now();
    let mut own_count = 0usize;
    let mut owned_accounts = std::collections::HashSet::new();

    for &pid in &person_ids {
        let num_accounts = 1 + rng.next_usize(5);
        for _ in 0..num_accounts {
            let aid = account_ids[rng.next_usize(account_ids.len())];
            if owned_accounts.contains(&aid) { continue; }
            let src = ids.person[&pid];
            let tgt = ids.account[&aid];
            if let Ok(eid) = graph.create_edge(src, tgt, "OWN") {
                if let Some(edge) = graph.get_edge_mut(eid) {
                    edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                }
                own_count += 1;
                owned_accounts.insert(aid);
            }
        }
    }
    for &cid in &company_ids {
        let num_accounts = 1 + rng.next_usize(3);
        for _ in 0..num_accounts {
            let aid = account_ids[rng.next_usize(account_ids.len())];
            if owned_accounts.contains(&aid) { continue; }
            let src = ids.company[&cid];
            let tgt = ids.account[&aid];
            if let Ok(eid) = graph.create_edge(src, tgt, "OWN") {
                if let Some(edge) = graph.get_edge_mut(eid) {
                    edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                }
                own_count += 1;
                owned_accounts.insert(aid);
            }
        }
    }
    print_done(&format!("  OWN:                                   {:>12} edges ({})", format_num(own_count), format_duration(t.elapsed())));
    total_edges += own_count;

    // TRANSFER: Account -> Account
    let t = std::time::Instant::now();
    let mut transfer_count = 0usize;
    for _ in 0..config.num_transfers {
        let src_aid = account_ids[rng.next_usize(account_ids.len())];
        let mut tgt_aid = account_ids[rng.next_usize(account_ids.len())];
        while tgt_aid == src_aid {
            tgt_aid = account_ids[rng.next_usize(account_ids.len())];
        }
        let src = ids.account[&src_aid];
        let tgt = ids.account[&tgt_aid];
        if let Ok(eid) = graph.create_edge(src, tgt, "TRANSFER") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                let amount = 10.0 + rng.next_f64() * 50_000.0;
                edge.set_property("amount", PropertyValue::Float(amount));
            }
            transfer_count += 1;
        }
    }
    print_done(&format!("  TRANSFER:                              {:>12} edges ({})", format_num(transfer_count), format_duration(t.elapsed())));
    total_edges += transfer_count;

    // WITHDRAW: Account -> Account
    let t = std::time::Instant::now();
    let mut withdraw_count = 0usize;
    for _ in 0..config.num_withdrawals {
        let src_aid = account_ids[rng.next_usize(account_ids.len())];
        let mut tgt_aid = account_ids[rng.next_usize(account_ids.len())];
        while tgt_aid == src_aid {
            tgt_aid = account_ids[rng.next_usize(account_ids.len())];
        }
        let src = ids.account[&src_aid];
        let tgt = ids.account[&tgt_aid];
        if let Ok(eid) = graph.create_edge(src, tgt, "WITHDRAW") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                let amount = 50.0 + rng.next_f64() * 20_000.0;
                edge.set_property("amount", PropertyValue::Float(amount));
            }
            withdraw_count += 1;
        }
    }
    print_done(&format!("  WITHDRAW:                              {:>12} edges ({})", format_num(withdraw_count), format_duration(t.elapsed())));
    total_edges += withdraw_count;

    // DEPOSIT: Loan -> Account
    let t = std::time::Instant::now();
    let mut deposit_count = 0usize;
    for _ in 0..config.num_deposits {
        let lid = loan_ids[rng.next_usize(loan_ids.len())];
        let aid = account_ids[rng.next_usize(account_ids.len())];
        let src = ids.loan[&lid];
        let tgt = ids.account[&aid];
        if let Ok(eid) = graph.create_edge(src, tgt, "DEPOSIT") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                let amount = 500.0 + rng.next_f64() * 100_000.0;
                edge.set_property("amount", PropertyValue::Float(amount));
            }
            deposit_count += 1;
        }
    }
    print_done(&format!("  DEPOSIT:                               {:>12} edges ({})", format_num(deposit_count), format_duration(t.elapsed())));
    total_edges += deposit_count;

    // REPAY: Account -> Loan
    let t = std::time::Instant::now();
    let mut repay_count = 0usize;
    for _ in 0..config.num_repayments {
        let aid = account_ids[rng.next_usize(account_ids.len())];
        let lid = loan_ids[rng.next_usize(loan_ids.len())];
        let src = ids.account[&aid];
        let tgt = ids.loan[&lid];
        if let Ok(eid) = graph.create_edge(src, tgt, "REPAY") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                let amount = 100.0 + rng.next_f64() * 50_000.0;
                edge.set_property("amount", PropertyValue::Float(amount));
            }
            repay_count += 1;
        }
    }
    print_done(&format!("  REPAY:                                 {:>12} edges ({})", format_num(repay_count), format_duration(t.elapsed())));
    total_edges += repay_count;

    // SIGN_IN: Account -> Medium
    let t = std::time::Instant::now();
    let mut signin_count = 0usize;
    for _ in 0..config.num_sign_ins {
        let aid = account_ids[rng.next_usize(account_ids.len())];
        let mid = medium_ids[rng.next_usize(medium_ids.len())];
        let src = ids.account[&aid];
        let tgt = ids.medium[&mid];
        if let Ok(eid) = graph.create_edge(src, tgt, "SIGN_IN") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
            }
            signin_count += 1;
        }
    }
    print_done(&format!("  SIGN_IN:                               {:>12} edges ({})", format_num(signin_count), format_duration(t.elapsed())));
    total_edges += signin_count;

    // APPLY: Person/Company -> Loan
    let t = std::time::Instant::now();
    let mut apply_count = 0usize;
    // Each loan has 1 applicant
    for &lid in &loan_ids {
        let src = if rng.next_bool(0.6) {
            let pid = person_ids[rng.next_usize(person_ids.len())];
            ids.person[&pid]
        } else {
            let cid = company_ids[rng.next_usize(company_ids.len())];
            ids.company[&cid]
        };
        let tgt = ids.loan[&lid];
        if let Ok(eid) = graph.create_edge(src, tgt, "APPLY") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
            }
            apply_count += 1;
        }
    }
    print_done(&format!("  APPLY:                                 {:>12} edges ({})", format_num(apply_count), format_duration(t.elapsed())));
    total_edges += apply_count;

    // INVEST: Company/Person -> Company
    let t = std::time::Instant::now();
    let mut invest_count = 0usize;
    let num_investments = config.num_companies / 2;
    for _ in 0..num_investments {
        let tgt_cid = company_ids[rng.next_usize(company_ids.len())];
        let src = if rng.next_bool(0.7) {
            let cid = company_ids[rng.next_usize(company_ids.len())];
            if cid == tgt_cid { continue; }
            ids.company[&cid]
        } else {
            let pid = person_ids[rng.next_usize(person_ids.len())];
            ids.person[&pid]
        };
        let tgt = ids.company[&tgt_cid];
        if let Ok(eid) = graph.create_edge(src, tgt, "INVEST") {
            if let Some(edge) = graph.get_edge_mut(eid) {
                edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                let ratio = rng.next_f64();
                edge.set_property("ratio", PropertyValue::Float(ratio));
            }
            invest_count += 1;
        }
    }
    print_done(&format!("  INVEST:                                {:>12} edges ({})", format_num(invest_count), format_duration(t.elapsed())));
    total_edges += invest_count;

    // GUARANTEE: Company/Person -> Company/Person
    let t = std::time::Instant::now();
    let mut guarantee_count = 0usize;
    let num_guarantees = config.num_persons / 5;
    for _ in 0..num_guarantees {
        // Mostly company-company guarantees, some person-person
        if rng.next_bool(0.6) {
            let src_cid = company_ids[rng.next_usize(company_ids.len())];
            let tgt_cid = company_ids[rng.next_usize(company_ids.len())];
            if src_cid == tgt_cid { continue; }
            let src = ids.company[&src_cid];
            let tgt = ids.company[&tgt_cid];
            if let Ok(eid) = graph.create_edge(src, tgt, "GUARANTEE") {
                if let Some(edge) = graph.get_edge_mut(eid) {
                    edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                }
                guarantee_count += 1;
            }
        } else {
            let src_pid = person_ids[rng.next_usize(person_ids.len())];
            let tgt_pid = person_ids[rng.next_usize(person_ids.len())];
            if src_pid == tgt_pid { continue; }
            let src = ids.person[&src_pid];
            let tgt = ids.person[&tgt_pid];
            if let Ok(eid) = graph.create_edge(src, tgt, "GUARANTEE") {
                if let Some(edge) = graph.get_edge_mut(eid) {
                    edge.set_property("timestamp", PropertyValue::DateTime(random_ts(&mut rng)));
                }
                guarantee_count += 1;
            }
        }
    }
    print_done(&format!("  GUARANTEE:                             {:>12} edges ({})", format_num(guarantee_count), format_duration(t.elapsed())));
    total_edges += guarantee_count;

    LoadResult { total_nodes, total_edges }
}

// ============================================================================
// CSV DATA GENERATOR (writes to disk)
// ============================================================================

/// Write generated CSV files to disk for the FinBench schema.
/// Uses the same synthetic generator logic but writes pipe-delimited CSVs.
pub fn write_csv_dataset(data_dir: &Path, config: &GeneratorConfig) -> Result<(), Error> {
    std::fs::create_dir_all(data_dir)?;
    let mut rng = SimpleRng::new(42);

    // Base timestamp: 2020-01-01 00:00:00 UTC (in milliseconds)
    let base_ts: i64 = 1_577_836_800_000;
    let time_range: i64 = 3 * 365 * 24 * 3600 * 1000;
    let random_ts = |rng: &mut SimpleRng| -> i64 {
        base_ts + (rng.next_u64() % time_range as u64) as i64
    };

    eprintln!("Writing FinBench CSV files to: {}", data_dir.display());

    // --- Person ---
    {
        let mut f = File::create(data_dir.join("person.csv"))?;
        writeln!(f, "id|name|isBlocked")?;
        for i in 0..config.num_persons {
            let first = FIRST_NAMES[rng.next_usize(FIRST_NAMES.len())];
            let last = LAST_NAMES[rng.next_usize(LAST_NAMES.len())];
            writeln!(f, "{}|{} {}|{}", i + 1, first, last, rng.next_bool(0.02))?;
        }
    }
    eprintln!("  person.csv             ({} rows)", format_num(config.num_persons));

    // --- Company ---
    {
        let mut f = File::create(data_dir.join("company.csv"))?;
        writeln!(f, "id|name|isBlocked")?;
        for i in 0..config.num_companies {
            let prefix = COMPANY_PREFIXES[rng.next_usize(COMPANY_PREFIXES.len())];
            let suffix = COMPANY_SUFFIXES[rng.next_usize(COMPANY_SUFFIXES.len())];
            writeln!(f, "{}|{} {}|{}", i + 1, prefix, suffix, rng.next_bool(0.01))?;
        }
    }
    eprintln!("  company.csv            ({} rows)", format_num(config.num_companies));

    // --- Account ---
    {
        let mut f = File::create(data_dir.join("account.csv"))?;
        writeln!(f, "id|createTime|isBlocked|accountType")?;
        for i in 0..config.num_accounts {
            let acct_type = ACCOUNT_TYPES[rng.next_usize(ACCOUNT_TYPES.len())];
            writeln!(f, "{}|{}|{}|{}", i + 1, random_ts(&mut rng), rng.next_bool(0.03), acct_type)?;
        }
    }
    eprintln!("  account.csv            ({} rows)", format_num(config.num_accounts));

    // --- Loan ---
    {
        let mut f = File::create(data_dir.join("loan.csv"))?;
        writeln!(f, "id|loanAmount|balance")?;
        for i in 0..config.num_loans {
            let amount = 1000.0 + rng.next_f64() * 499_000.0;
            let balance = amount * rng.next_f64();
            writeln!(f, "{}|{:.2}|{:.2}", i + 1, amount, balance)?;
        }
    }
    eprintln!("  loan.csv               ({} rows)", format_num(config.num_loans));

    // --- Medium ---
    {
        let mut f = File::create(data_dir.join("medium.csv"))?;
        writeln!(f, "id|mediumType|isBlocked")?;
        for i in 0..config.num_mediums {
            let mtype = MEDIUM_TYPES[rng.next_usize(MEDIUM_TYPES.len())];
            writeln!(f, "{}|{}|{}", i + 1, mtype, rng.next_bool(0.02))?;
        }
    }
    eprintln!("  medium.csv             ({} rows)", format_num(config.num_mediums));

    // --- Edge CSVs ---

    // personOwnAccount.csv
    let mut owned_accounts = std::collections::HashSet::new();
    {
        let mut f = File::create(data_dir.join("personOwnAccount.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp")?;
        let mut count = 0;
        for i in 0..config.num_persons {
            let pid = (i + 1) as i64;
            let num_accounts = 1 + rng.next_usize(5);
            for _ in 0..num_accounts {
                let aid = (1 + rng.next_usize(config.num_accounts)) as i64;
                if owned_accounts.contains(&aid) { continue; }
                writeln!(f, "{}|{}|{}", pid, aid, random_ts(&mut rng))?;
                owned_accounts.insert(aid);
                count += 1;
            }
        }
        eprintln!("  personOwnAccount.csv   ({} rows)", format_num(count));
    }

    // companyOwnAccount.csv
    {
        let mut f = File::create(data_dir.join("companyOwnAccount.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp")?;
        let mut count = 0;
        for i in 0..config.num_companies {
            let cid = (i + 1) as i64;
            let num_accounts = 1 + rng.next_usize(3);
            for _ in 0..num_accounts {
                let aid = (1 + rng.next_usize(config.num_accounts)) as i64;
                if owned_accounts.contains(&aid) { continue; }
                writeln!(f, "{}|{}|{}", cid, aid, random_ts(&mut rng))?;
                owned_accounts.insert(aid);
                count += 1;
            }
        }
        eprintln!("  companyOwnAccount.csv  ({} rows)", format_num(count));
    }

    // transfer.csv
    {
        let mut f = File::create(data_dir.join("transfer.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp|amount")?;
        for _ in 0..config.num_transfers {
            let src = (1 + rng.next_usize(config.num_accounts)) as i64;
            let mut tgt = (1 + rng.next_usize(config.num_accounts)) as i64;
            while tgt == src { tgt = (1 + rng.next_usize(config.num_accounts)) as i64; }
            let amount = 10.0 + rng.next_f64() * 50_000.0;
            writeln!(f, "{}|{}|{}|{:.2}", src, tgt, random_ts(&mut rng), amount)?;
        }
        eprintln!("  transfer.csv           ({} rows)", format_num(config.num_transfers));
    }

    // withdraw.csv
    {
        let mut f = File::create(data_dir.join("withdraw.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp|amount")?;
        for _ in 0..config.num_withdrawals {
            let src = (1 + rng.next_usize(config.num_accounts)) as i64;
            let mut tgt = (1 + rng.next_usize(config.num_accounts)) as i64;
            while tgt == src { tgt = (1 + rng.next_usize(config.num_accounts)) as i64; }
            let amount = 50.0 + rng.next_f64() * 20_000.0;
            writeln!(f, "{}|{}|{}|{:.2}", src, tgt, random_ts(&mut rng), amount)?;
        }
        eprintln!("  withdraw.csv           ({} rows)", format_num(config.num_withdrawals));
    }

    // deposit.csv
    {
        let mut f = File::create(data_dir.join("deposit.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp|amount")?;
        for _ in 0..config.num_deposits {
            let lid = (1 + rng.next_usize(config.num_loans)) as i64;
            let aid = (1 + rng.next_usize(config.num_accounts)) as i64;
            let amount = 500.0 + rng.next_f64() * 100_000.0;
            writeln!(f, "{}|{}|{}|{:.2}", lid, aid, random_ts(&mut rng), amount)?;
        }
        eprintln!("  deposit.csv            ({} rows)", format_num(config.num_deposits));
    }

    // repay.csv
    {
        let mut f = File::create(data_dir.join("repay.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp|amount")?;
        for _ in 0..config.num_repayments {
            let aid = (1 + rng.next_usize(config.num_accounts)) as i64;
            let lid = (1 + rng.next_usize(config.num_loans)) as i64;
            let amount = 100.0 + rng.next_f64() * 50_000.0;
            writeln!(f, "{}|{}|{}|{:.2}", aid, lid, random_ts(&mut rng), amount)?;
        }
        eprintln!("  repay.csv              ({} rows)", format_num(config.num_repayments));
    }

    // signIn.csv
    {
        let mut f = File::create(data_dir.join("signIn.csv"))?;
        writeln!(f, "srcId|tgtId|timestamp")?;
        for _ in 0..config.num_sign_ins {
            let aid = (1 + rng.next_usize(config.num_accounts)) as i64;
            let mid = (1 + rng.next_usize(config.num_mediums)) as i64;
            writeln!(f, "{}|{}|{}", aid, mid, random_ts(&mut rng))?;
        }
        eprintln!("  signIn.csv             ({} rows)", format_num(config.num_sign_ins));
    }

    // personApplyLoan.csv and companyApplyLoan.csv
    {
        let mut fp = File::create(data_dir.join("personApplyLoan.csv"))?;
        let mut fc = File::create(data_dir.join("companyApplyLoan.csv"))?;
        writeln!(fp, "srcId|tgtId|timestamp")?;
        writeln!(fc, "srcId|tgtId|timestamp")?;
        let mut pcount = 0;
        let mut ccount = 0;
        for i in 0..config.num_loans {
            let lid = (i + 1) as i64;
            if rng.next_bool(0.6) {
                let pid = (1 + rng.next_usize(config.num_persons)) as i64;
                writeln!(fp, "{}|{}|{}", pid, lid, random_ts(&mut rng))?;
                pcount += 1;
            } else {
                let cid = (1 + rng.next_usize(config.num_companies)) as i64;
                writeln!(fc, "{}|{}|{}", cid, lid, random_ts(&mut rng))?;
                ccount += 1;
            }
        }
        eprintln!("  personApplyLoan.csv    ({} rows)", format_num(pcount));
        eprintln!("  companyApplyLoan.csv   ({} rows)", format_num(ccount));
    }

    // companyInvestCompany.csv and personInvestCompany.csv
    {
        let mut fc = File::create(data_dir.join("companyInvestCompany.csv"))?;
        let mut fp = File::create(data_dir.join("personInvestCompany.csv"))?;
        writeln!(fc, "srcId|tgtId|timestamp|ratio")?;
        writeln!(fp, "srcId|tgtId|timestamp|ratio")?;
        let mut ccount = 0;
        let mut pcount = 0;
        let num_investments = config.num_companies / 2;
        for _ in 0..num_investments {
            let tgt_cid = (1 + rng.next_usize(config.num_companies)) as i64;
            if rng.next_bool(0.7) {
                let src_cid = (1 + rng.next_usize(config.num_companies)) as i64;
                if src_cid == tgt_cid { continue; }
                writeln!(fc, "{}|{}|{}|{:.4}", src_cid, tgt_cid, random_ts(&mut rng), rng.next_f64())?;
                ccount += 1;
            } else {
                let pid = (1 + rng.next_usize(config.num_persons)) as i64;
                writeln!(fp, "{}|{}|{}|{:.4}", pid, tgt_cid, random_ts(&mut rng), rng.next_f64())?;
                pcount += 1;
            }
        }
        eprintln!("  companyInvestCompany.csv ({} rows)", format_num(ccount));
        eprintln!("  personInvestCompany.csv  ({} rows)", format_num(pcount));
    }

    // companyGuaranteeCompany.csv and personGuaranteePerson.csv
    {
        let mut fc = File::create(data_dir.join("companyGuaranteeCompany.csv"))?;
        let mut fp = File::create(data_dir.join("personGuaranteePerson.csv"))?;
        writeln!(fc, "srcId|tgtId|timestamp")?;
        writeln!(fp, "srcId|tgtId|timestamp")?;
        let mut ccount = 0;
        let mut pcount = 0;
        let num_guarantees = config.num_persons / 5;
        for _ in 0..num_guarantees {
            if rng.next_bool(0.6) {
                let src = (1 + rng.next_usize(config.num_companies)) as i64;
                let tgt = (1 + rng.next_usize(config.num_companies)) as i64;
                if src == tgt { continue; }
                writeln!(fc, "{}|{}|{}", src, tgt, random_ts(&mut rng))?;
                ccount += 1;
            } else {
                let src = (1 + rng.next_usize(config.num_persons)) as i64;
                let tgt = (1 + rng.next_usize(config.num_persons)) as i64;
                if src == tgt { continue; }
                writeln!(fp, "{}|{}|{}", src, tgt, random_ts(&mut rng))?;
                pcount += 1;
            }
        }
        eprintln!("  companyGuaranteeCompany.csv ({} rows)", format_num(ccount));
        eprintln!("  personGuaranteePerson.csv  ({} rows)", format_num(pcount));
    }

    eprintln!();
    eprintln!("CSV generation complete.");

    Ok(())
}

/// Get the default data directory path.
pub fn default_data_dir() -> PathBuf {
    PathBuf::from("data/finbench-sf1")
}
