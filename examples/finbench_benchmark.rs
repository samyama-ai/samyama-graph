//! LDBC FinBench Query Benchmark — Samyama Graph Database
//!
//! Benchmarks Samyama's query engine against 40+ LDBC FinBench workload queries
//! covering Complex Reads (CR1-CR12), Simple Reads (SR1-SR6), Read-Writes (RW1-RW3),
//! and Write operations (W1-W19).
//!
//! The FinBench workload models financial transaction networks with accounts,
//! persons, companies, loans, and mediums connected by transfers, deposits,
//! repayments, sign-ins, investments, and guarantees.
//!
//! Usage:
//!   cargo run --release --example finbench_benchmark
//!   cargo run --release --example finbench_benchmark -- --runs 10
//!   cargo run --release --example finbench_benchmark -- --query CR-1
//!   cargo run --release --example finbench_benchmark -- --data-dir /path/to/data
//!   cargo run --release --example finbench_benchmark -- --writes   # include write operations

use std::path::PathBuf;
use std::time::{Duration, Instant};

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod finbench_common;
use finbench_common::{format_duration, format_num, GeneratorConfig};

type Error = Box<dyn std::error::Error>;

// ============================================================================
// QUERY DEFINITIONS
// ============================================================================

struct FinBenchQuery {
    id: &'static str,
    name: &'static str,
    cypher: &'static str,
    category: &'static str,
}

/// Build the 12 Complex Read queries (CR1-CR12).
///
/// Parameter choices (from synthetic dataset, deterministic seed=42):
///   accountId     = 1       (first account)
///   accountId2    = 100     (another account for path queries)
///   personId      = 1       (first person)
///   companyId     = 1       (first company)
///   loanId        = 1       (first loan)
///   mediumId      = 1       (first medium)
///   startTime     = 1577836800000   (2020-01-01)
///   endTime       = 1609459200000   (2021-01-01)
fn complex_reads() -> Vec<FinBenchQuery> {
    vec![
        FinBenchQuery {
            id: "CR-1",
            name: "Transfer In/Out Amounts",
            category: "complex",
            // Find total transfer-in and transfer-out amounts for an account
            cypher: "\
MATCH (src:Account)-[t:TRANSFER]->(a:Account {id: 1})
RETURN a.id, count(t) AS transferInCount, sum(t.amount) AS totalIn",
        },

        FinBenchQuery {
            id: "CR-2",
            name: "Blocked Account Transfers",
            category: "complex",
            // Find accounts that transferred money to a blocked account within time range
            cypher: "\
MATCH (src:Account)-[t:TRANSFER]->(dst:Account {isBlocked: true})
WHERE t.timestamp >= 1577836800000 AND t.timestamp < 1609459200000
RETURN src.id, dst.id, t.amount, t.timestamp
ORDER BY t.amount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-3",
            name: "Shortest Transfer Path",
            category: "complex",
            // Shortest path between two accounts via TRANSFER edges
            cypher: "\
MATCH p = shortestPath((a1:Account {id: 1})-[:TRANSFER*]-(a2:Account {id: 100}))
RETURN length(p) AS pathLength",
        },

        FinBenchQuery {
            id: "CR-4",
            name: "Transfer Cycle Detection",
            category: "complex",
            // 3-hop transfer cycle: A->B->C->A
            cypher: "\
MATCH (a:Account {id: 1})-[t1:TRANSFER]->(b:Account)-[t2:TRANSFER]->(c:Account)-[t3:TRANSFER]->(a)
WHERE b.id <> a.id AND c.id <> a.id AND b.id <> c.id
RETURN a.id, b.id, c.id, t1.amount, t2.amount, t3.amount
LIMIT 10",
        },

        FinBenchQuery {
            id: "CR-5",
            name: "Owner Account Transfer Patterns",
            category: "complex",
            // Persons/companies connected to an account and their other accounts' transfers
            cypher: "\
MATCH (owner)-[:OWN]->(a:Account {id: 1})
WITH owner
MATCH (owner)-[:OWN]->(otherAcct:Account)
MATCH (otherAcct)-[t:TRANSFER]->(dst:Account)
RETURN owner.name, otherAcct.id, count(t) AS transferCount, sum(t.amount) AS totalAmount
ORDER BY totalAmount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-6",
            name: "Loan Deposit Tracing",
            category: "complex",
            // Find accounts that got deposits from a loan and trace where money went
            cypher: "\
MATCH (l:Loan {id: 1})-[d:DEPOSIT]->(a:Account)-[t:TRANSFER]->(dst:Account)
RETURN a.id, d.amount AS depositAmount, dst.id AS transferTarget, t.amount AS transferAmount
ORDER BY d.amount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-7",
            name: "Transfer Chain Analysis",
            category: "complex",
            // 2-hop transfers in and out of an account
            cypher: "\
MATCH (upstream:Account)-[t1:TRANSFER]->(mid:Account)-[t2:TRANSFER]->(a:Account {id: 1})
RETURN upstream.id, mid.id, t1.amount AS upstreamAmount, t2.amount AS midAmount
ORDER BY t2.amount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-8",
            name: "Loan Deposit Distribution",
            category: "complex",
            // Where did loan deposits go — all deposits from all loans above a threshold
            cypher: "\
MATCH (l:Loan)-[d:DEPOSIT]->(a:Account)
WHERE d.amount > 10000.0
RETURN l.id, l.loanAmount, a.id AS targetAccount, d.amount AS depositAmount
ORDER BY d.amount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-9",
            name: "Guarantee Chain",
            category: "complex",
            // Find guarantee chains from a company (up to 3 hops)
            cypher: "\
MATCH (c:Company {id: 1})-[:GUARANTEE*1..3]->(guaranteed)
RETURN DISTINCT guaranteed.id, guaranteed.name
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-10",
            name: "Investment Network",
            category: "complex",
            // Companies connected by INVEST edges — count investors per company
            cypher: "\
MATCH (investor)-[inv:INVEST]->(target:Company)
RETURN target.id, target.name, count(investor) AS investorCount, sum(inv.ratio) AS totalRatio
ORDER BY investorCount DESC
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-11",
            name: "Shared Medium Sign-In",
            category: "complex",
            // Find accounts that signed in with the same medium as account 1
            cypher: "\
MATCH (a:Account {id: 1})-[:SIGN_IN]->(m:Medium)<-[:SIGN_IN]-(other:Account)
WHERE other.id <> 1
RETURN DISTINCT other.id, other.accountType, m.mediumType
ORDER BY other.id
LIMIT 20",
        },

        FinBenchQuery {
            id: "CR-12",
            name: "Person Account Transfer Stats",
            category: "complex",
            // Transfer amount statistics for accounts owned by a person
            cypher: "\
MATCH (p:Person {id: 1})-[:OWN]->(a:Account)-[t:TRANSFER]->(dst:Account)
RETURN a.id, count(t) AS transferCount, sum(t.amount) AS totalAmount
ORDER BY totalAmount DESC",
        },
    ]
}

/// Build the 6 Simple Read queries (SR1-SR6).
fn simple_reads() -> Vec<FinBenchQuery> {
    vec![
        FinBenchQuery {
            id: "SR-1",
            name: "Account by ID",
            category: "simple",
            cypher: "\
MATCH (a:Account {id: 1})
RETURN a.id, a.createTime, a.isBlocked, a.accountType",
        },

        FinBenchQuery {
            id: "SR-2",
            name: "Account Transfers in Window",
            category: "simple",
            // Get transfers from an account in a time window
            cypher: "\
MATCH (a:Account {id: 1})-[t:TRANSFER]->(dst:Account)
WHERE t.timestamp >= 1577836800000 AND t.timestamp < 1609459200000
RETURN dst.id, t.amount, t.timestamp
ORDER BY t.timestamp DESC
LIMIT 10",
        },

        FinBenchQuery {
            id: "SR-3",
            name: "Person's Accounts",
            category: "simple",
            // Get all accounts owned by a person
            cypher: "\
MATCH (p:Person {id: 1})-[:OWN]->(a:Account)
RETURN a.id, a.accountType, a.isBlocked, a.createTime
ORDER BY a.id",
        },

        FinBenchQuery {
            id: "SR-4",
            name: "Transfer-In Accounts",
            category: "simple",
            // Get transfer-in accounts for an account within time range
            cypher: "\
MATCH (src:Account)-[t:TRANSFER]->(a:Account {id: 1})
WHERE t.timestamp >= 1577836800000 AND t.timestamp < 1609459200000
RETURN src.id, t.amount, t.timestamp
ORDER BY t.timestamp DESC
LIMIT 10",
        },

        FinBenchQuery {
            id: "SR-5",
            name: "Transfer-Out Accounts",
            category: "simple",
            // Get transfer-out accounts for an account within time range
            cypher: "\
MATCH (a:Account {id: 1})-[t:TRANSFER]->(dst:Account)
WHERE t.timestamp >= 1577836800000 AND t.timestamp < 1609459200000
RETURN dst.id, t.amount, t.timestamp
ORDER BY t.timestamp DESC
LIMIT 10",
        },

        FinBenchQuery {
            id: "SR-6",
            name: "Loan by ID",
            category: "simple",
            cypher: "\
MATCH (l:Loan {id: 1})
RETURN l.id, l.loanAmount, l.balance",
        },
    ]
}

/// Build the 3 Read-Write queries (RW1-RW3).
fn read_writes() -> Vec<FinBenchQuery> {
    vec![
        FinBenchQuery {
            id: "RW-1",
            name: "Block Account + Read Transfers",
            category: "readwrite",
            // Block an account and then read its transfers
            cypher: "MATCH (a:Account {id: 2}) SET a.isBlocked = true RETURN a.id, a.isBlocked",
        },

        FinBenchQuery {
            id: "RW-2",
            name: "Block Medium + Find Accounts",
            category: "readwrite",
            // Block a medium then find connected accounts
            cypher: "MATCH (m:Medium {id: 2}) SET m.isBlocked = true RETURN m.id, m.isBlocked",
        },

        FinBenchQuery {
            id: "RW-3",
            name: "Block Person + Accounts",
            category: "readwrite",
            // Block a person (their accounts would need separate queries in real FinBench)
            cypher: "MATCH (p:Person {id: 2}) SET p.isBlocked = true RETURN p.id, p.name, p.isBlocked",
        },
    ]
}

/// Build the 19 Write queries (W1-W19) — CREATE operations for all node and edge types.
fn writes() -> Vec<FinBenchQuery> {
    vec![
        // --- Node creation ---
        FinBenchQuery {
            id: "W-1",
            name: "Create Person",
            category: "write",
            cypher: "\
CREATE (p:Person {id: 999001, name: \"Benchmark Person\", isBlocked: false})",
        },
        FinBenchQuery {
            id: "W-2",
            name: "Create Company",
            category: "write",
            cypher: "\
CREATE (c:Company {id: 999001, name: \"Benchmark Corp\", isBlocked: false})",
        },
        FinBenchQuery {
            id: "W-3",
            name: "Create Account",
            category: "write",
            cypher: "\
CREATE (a:Account {id: 999001, createTime: 1709251200000, isBlocked: false, accountType: \"checking\"})",
        },
        FinBenchQuery {
            id: "W-4",
            name: "Create Loan",
            category: "write",
            cypher: "\
CREATE (l:Loan {id: 999001, loanAmount: 50000.0, balance: 50000.0})",
        },
        FinBenchQuery {
            id: "W-5",
            name: "Create Medium",
            category: "write",
            cypher: "\
CREATE (m:Medium {id: 999001, mediumType: \"phone\", isBlocked: false})",
        },

        // --- Edge creation ---
        FinBenchQuery {
            id: "W-6",
            name: "Person Owns Account",
            category: "write",
            cypher: "MATCH (p:Person {id: 999001}), (a:Account {id: 999001}) CREATE (p)-[:OWN {timestamp: 1709251200000}]->(a)",
        },
        FinBenchQuery {
            id: "W-7",
            name: "Company Owns Account",
            category: "write",
            cypher: "MATCH (c:Company {id: 999001}), (a:Account {id: 1}) CREATE (c)-[:OWN {timestamp: 1709251200000}]->(a)",
        },
        FinBenchQuery {
            id: "W-8",
            name: "Transfer Between Accounts",
            category: "write",
            cypher: "MATCH (src:Account {id: 999001}), (dst:Account {id: 1}) CREATE (src)-[:TRANSFER {timestamp: 1709251200000, amount: 1500.0}]->(dst)",
        },
        FinBenchQuery {
            id: "W-9",
            name: "Withdraw Between Accounts",
            category: "write",
            cypher: "MATCH (src:Account {id: 999001}), (dst:Account {id: 2}) CREATE (src)-[:WITHDRAW {timestamp: 1709251200000, amount: 500.0}]->(dst)",
        },
        FinBenchQuery {
            id: "W-10",
            name: "Loan Deposit to Account",
            category: "write",
            cypher: "MATCH (l:Loan {id: 999001}), (a:Account {id: 999001}) CREATE (l)-[:DEPOSIT {timestamp: 1709251200000, amount: 50000.0}]->(a)",
        },
        FinBenchQuery {
            id: "W-11",
            name: "Account Repays Loan",
            category: "write",
            cypher: "MATCH (a:Account {id: 999001}), (l:Loan {id: 999001}) CREATE (a)-[:REPAY {timestamp: 1709251200000, amount: 5000.0}]->(l)",
        },
        FinBenchQuery {
            id: "W-12",
            name: "Account Sign-In Medium",
            category: "write",
            cypher: "MATCH (a:Account {id: 999001}), (m:Medium {id: 999001}) CREATE (a)-[:SIGN_IN {timestamp: 1709251200000}]->(m)",
        },
        FinBenchQuery {
            id: "W-13",
            name: "Person Applies for Loan",
            category: "write",
            cypher: "MATCH (p:Person {id: 999001}), (l:Loan {id: 999001}) CREATE (p)-[:APPLY {timestamp: 1709251200000}]->(l)",
        },
        FinBenchQuery {
            id: "W-14",
            name: "Company Applies for Loan",
            category: "write",
            cypher: "MATCH (c:Company {id: 999001}), (l:Loan {id: 1}) CREATE (c)-[:APPLY {timestamp: 1709251200000}]->(l)",
        },
        FinBenchQuery {
            id: "W-15",
            name: "Company Invests in Company",
            category: "write",
            cypher: "MATCH (c1:Company {id: 999001}), (c2:Company {id: 1}) CREATE (c1)-[:INVEST {timestamp: 1709251200000, ratio: 0.15}]->(c2)",
        },
        FinBenchQuery {
            id: "W-16",
            name: "Person Invests in Company",
            category: "write",
            cypher: "MATCH (p:Person {id: 999001}), (c:Company {id: 1}) CREATE (p)-[:INVEST {timestamp: 1709251200000, ratio: 0.05}]->(c)",
        },
        FinBenchQuery {
            id: "W-17",
            name: "Company Guarantees Company",
            category: "write",
            cypher: "MATCH (c1:Company {id: 999001}), (c2:Company {id: 2}) CREATE (c1)-[:GUARANTEE {timestamp: 1709251200000}]->(c2)",
        },
        FinBenchQuery {
            id: "W-18",
            name: "Person Guarantees Person",
            category: "write",
            cypher: "MATCH (p1:Person {id: 999001}), (p2:Person {id: 2}) CREATE (p1)-[:GUARANTEE {timestamp: 1709251200000}]->(p2)",
        },
        FinBenchQuery {
            id: "W-19",
            name: "Delete Account",
            category: "write",
            cypher: "MATCH (a:Account {id: 999001}) DELETE a",
        },
    ]
}

// ============================================================================
// BENCHMARK RUNNER
// ============================================================================

struct BenchResult {
    id: &'static str,
    name: &'static str,
    rows: usize,
    min: Duration,
    median: Duration,
    max: Duration,
    error: Option<String>,
}

fn format_ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.2}ms", ms)
    } else if ms < 100.0 {
        format!("{:.1}ms", ms)
    } else if ms < 10_000.0 {
        format!("{:.0}ms", ms)
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

async fn run_benchmark(
    client: &EmbeddedClient,
    query: &FinBenchQuery,
    runs: usize,
) -> BenchResult {
    let is_mutating = query.category == "write" || query.category == "readwrite";

    // Warm-up: 1 run, discard (skip for writes — they mutate state)
    let warmup = if is_mutating {
        client.query("default", query.cypher).await
    } else {
        client.query_readonly("default", query.cypher).await
    };
    if let Err(e) = &warmup {
        return BenchResult {
            id: query.id,
            name: query.name,
            rows: 0,
            min: Duration::ZERO,
            median: Duration::ZERO,
            max: Duration::ZERO,
            error: Some(e.to_string()),
        };
    }

    let mut timings = Vec::with_capacity(runs);
    let mut row_count = 0;

    let actual_runs = if is_mutating { 1 } else { runs }; // mutations run once
    for _ in 0..actual_runs {
        let start = Instant::now();
        let run_result = if is_mutating {
            client.query("default", query.cypher).await
        } else {
            client.query_readonly("default", query.cypher).await
        };
        match run_result {
            Ok(result) => {
                row_count = result.records.len();
                timings.push(start.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    id: query.id,
                    name: query.name,
                    rows: 0,
                    min: Duration::ZERO,
                    median: Duration::ZERO,
                    max: Duration::ZERO,
                    error: Some(e.to_string()),
                };
            }
        }
    }

    timings.sort();

    let len = timings.len();
    BenchResult {
        id: query.id,
        name: query.name,
        rows: row_count,
        min: timings[0],
        median: timings[len / 2],
        max: timings[len - 1],
        error: None,
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        Some(PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path argument")))
    } else {
        None
    };

    let runs: usize = if let Some(pos) = args.iter().position(|a| a == "--runs") {
        args.get(pos + 1).expect("--runs requires a number").parse().expect("--runs must be a positive integer")
    } else {
        5
    };

    let filter_query: Option<String> = if let Some(pos) = args.iter().position(|a| a == "--query") {
        Some(args.get(pos + 1).expect("--query requires a query ID (e.g. CR-1, SR-3)").to_uppercase())
    } else {
        None
    };

    let include_writes = args.iter().any(|a| a == "--writes");

    // ========================================================================
    // Load dataset
    // ========================================================================
    eprintln!("LDBC FinBench Benchmark — Samyama v0.5.8");
    eprintln!();

    let client = EmbeddedClient::new();
    let load_start = Instant::now();

    let load_result = if let Some(ref dir) = data_dir {
        if !dir.exists() {
            eprintln!("ERROR: Data directory not found: {}", dir.display());
            eprintln!("Run `scripts/download_finbench.sh` to generate data, or use synthetic mode (no --data-dir)");
            std::process::exit(1);
        }
        eprintln!("Loading FinBench dataset from CSV: {}", dir.display());
        eprintln!();
        let mut graph = client.store_write().await;
        finbench_common::load_dataset(&mut graph, dir)?
    } else {
        eprintln!("Generating synthetic FinBench dataset in-memory...");
        eprintln!();
        let config = GeneratorConfig::default();
        let mut graph = client.store_write().await;
        finbench_common::generate_dataset(&mut graph, &config)
    };

    let load_time = load_start.elapsed();

    eprintln!();
    eprintln!("Dataset: {} nodes, {} edges (loaded in {})",
        format_num(load_result.total_nodes),
        format_num(load_result.total_edges),
        format_duration(load_time));
    eprintln!("Runs per query: {}", runs);
    eprintln!();

    // ========================================================================
    // Build query list
    // ========================================================================
    let mut all_queries = Vec::new();
    all_queries.extend(complex_reads());
    all_queries.extend(simple_reads());
    all_queries.extend(read_writes());
    if include_writes {
        all_queries.extend(writes());
    }

    let queries: Vec<&FinBenchQuery> = if let Some(ref filter) = filter_query {
        all_queries.iter().filter(|q| q.id == filter.as_str()).collect()
    } else {
        all_queries.iter().collect()
    };

    if queries.is_empty() {
        eprintln!("ERROR: No matching query found for filter '{}'", filter_query.unwrap_or_default());
        eprintln!("Available: CR-1..CR-12, SR-1..SR-6, RW-1..RW-3, W-1..W-19 (with --writes)");
        std::process::exit(1);
    }

    // ========================================================================
    // Run benchmarks
    // ========================================================================

    // Print header
    println!("{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  {}",
        "ID", "Name", "Rows", "Min", "Median", "Max", "Status");
    println!("{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  {}",
        "------", "------------------------------------", "------", "----------", "----------", "----------", "------");

    let mut passed = 0usize;
    let mut errors = 0usize;
    let mut last_category = "";
    let bench_start = Instant::now();

    for query in &queries {
        // Print section separator when category changes
        if query.category != last_category {
            if !last_category.is_empty() { println!(); }
            let label = match query.category {
                "complex"   => "--- Complex Reads (CR) ---",
                "simple"    => "--- Simple Reads (SR) ---",
                "readwrite" => "--- Read-Write Operations (RW) ---",
                "write"     => "--- Write Operations (W) ---",
                other       => other,
            };
            println!("{}", label);
            last_category = query.category;
        }

        eprint!("  Running {}...\r", query.id);

        let result = run_benchmark(&client, query, runs).await;

        if let Some(ref err) = result.error {
            println!("{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  ERROR",
                result.id, result.name, "-", "-", "-", "-");
            eprintln!("       {}", err);
            errors += 1;
        } else {
            println!("{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  OK",
                result.id, result.name,
                result.rows,
                format_ms(result.min),
                format_ms(result.median),
                format_ms(result.max));
            passed += 1;
        }
    }

    let bench_time = bench_start.elapsed();

    // ========================================================================
    // Summary
    // ========================================================================
    println!();
    println!("Summary: {}/{} passed, {} errors (total benchmark time: {})",
        passed, queries.len(), errors, format_duration(bench_time));

    // Cache stats
    let stats = client.cache_stats();
    println!("AST cache: {} hits, {} misses", stats.hits(), stats.misses());

    if errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}
