// The shipped allocator (ADR-038): mimalloc unless built with --no-default-features.
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use samyama::{GraphStore, NodeId, PropertyValue, QueryEngine, RespServer, ServerConfig};
use samyama::http::HttpServer;
use samyama::persistence::{AutoEmbedConfig, LLMProvider};
use samyama::embed::EmbedPipeline;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Subcommands are handled before the demo and the server, so `verify` is a
    // tool that exits with a status rather than a database that starts up.
    let argv: Vec<String> = std::env::args().collect();
    match argv.get(1).map(|s| s.as_str()) {
        Some("verify") => std::process::exit(cmd_verify(&argv)),
        Some("catalog-build") => std::process::exit(cmd_catalog_build(&argv)),
        Some("catalog-gate") => std::process::exit(cmd_catalog_gate(&argv)),
        Some("auth-token") => std::process::exit(cmd_auth_token(&argv)),
        Some("snapshot-key") => std::process::exit(cmd_snapshot_key()),
        Some("schema") => std::process::exit(cmd_schema(&argv)),
        Some("auth-user") => std::process::exit(cmd_auth_user(&argv)),
        Some("pii-scan") => std::process::exit(cmd_pii_scan(&argv)),
        _ => {}
    }

    println!("Samyama Graph Database v{}", samyama::version());
    println!("==========================================");
    println!();

    demo_property_graph();
    demo_cypher_queries();

    println!("\n=== Starting RESP Server ===");
    println!("Connect with: redis-cli");
    println!("Try: GRAPH.QUERY default \"MATCH (n) RETURN labels(n), count(n)\"");
    println!();

    start_server().await;
}

/// `samyama pii-scan [--waivers <file>] <snapshot.sgsnap>...`
///
/// Scans each snapshot for personal identifiers and exits non-zero if any
/// un-waived ones are found (TRUST-10). Intended for CI over the artifacts
/// that are published.
///
/// Exits 2 -- not 0 -- when a file cannot be read. A scan that could not look
/// at the artifact must not report it clean; that is how a control passes on
/// something it never opened.
///
/// A waived finding is still **printed**. A waiver that hid its finding would
/// be indistinguishable from deleting the check, and the point of accepting
/// something is that the next reader can see what was accepted and why.
fn cmd_pii_scan(argv: &[String]) -> i32 {
    let waiver_path = argv
        .iter()
        .position(|a| a == "--waivers")
        .and_then(|i| argv.get(i + 1).cloned());
    let paths: Vec<&String> = argv
        .iter()
        .skip(2)
        .filter(|a| !a.starts_with('-'))
        .filter(|a| Some((*a).clone()) != waiver_path)
        .collect();
    if paths.is_empty() {
        eprintln!("usage: samyama pii-scan [--waivers <file>] <snapshot.sgsnap>...");
        return 2;
    }

    let waivers = match &waiver_path {
        None => Vec::new(),
        Some(p) => match samyama::pii::read_waivers(std::path::Path::new(p)) {
            Ok(w) => w,
            // A waiver file that cannot be parsed stops the scan rather than
            // running without it: running unwaived would flood the log and
            // running as if it were empty would be a different check.
            Err(e) => {
                eprintln!("ERROR {e}");
                return 2;
            }
        },
    };

    let mut worst = 0;
    // Which waivers fired, across every file. Tracked here and not per file,
    // because a waiver for one snapshot is legitimately unused while another
    // is being scanned.
    let mut used: std::collections::HashSet<(String, String)> = Default::default();

    for path in paths {
        match samyama::pii::scan_snapshot_path(std::path::Path::new(path)) {
            Err(e) => {
                eprintln!("ERROR {path}: {e}");
                worst = worst.max(2);
            }
            Ok(report) => {
                println!(
                    "{path}: {} nodes, {} edges, {} values scanned",
                    report.nodes_scanned, report.edges_scanned, report.values_scanned
                );
                let t = samyama::pii::triage(&report, &waivers);
                for (_, w) in &t.accepted {
                    used.insert((w.kind.clone(), w.location.clone()));
                }

                for (f, w) in &t.accepted {
                    println!(
                        "  accepted  {:<12} {:<34} {} of {} distinct -- {}",
                        f.kind, f.location, f.distinct, w.max_distinct, w.decided_in
                    );
                }
                for f in &t.unwaived {
                    println!(
                        "  FINDING   {:<12} {:<34} {} distinct, e.g. {}",
                        f.kind,
                        f.location,
                        f.distinct,
                        f.samples.join(", ")
                    );
                }
                if report.is_clean() {
                    println!("  clean -- no identifier patterns found");
                }
                if !t.unwaived.is_empty() {
                    worst = worst.max(1);
                }
            }
        }
    }

    // A waiver nobody needed is either a finding that went away or a waiver
    // aimed at the wrong place, and both are worth seeing. Not a failure: this
    // run may simply not have scanned the artifact it belongs to.
    let stale: Vec<&samyama::pii::Waiver> = waivers
        .iter()
        .filter(|w| !used.contains(&(w.kind.clone(), w.location.clone())))
        .collect();
    if !stale.is_empty() {
        println!();
        println!("{} waiver(s) matched nothing in this run:", stale.len());
        for w in stale {
            println!("  {:<12} {:<34} {}", w.kind, w.location, w.decided_in);
        }
        println!("  Either the finding is gone -- in which case delete the waiver --");
        println!("  or the scan did not cover the artifact it belongs to.");
    }
    worst
}

/// `samyama schema <snapshot.sgsnap> [--markdown]`
///
/// Prints the schema the snapshot actually contains, as a mermaid diagram or a
/// table (KG-02). Every relationship shown is one that occurs, with the number
/// of times it does.
fn cmd_schema(argv: &[String]) -> i32 {
    let markdown = argv.iter().any(|a| a == "--markdown");
    let Some(path) = argv.iter().skip(2).find(|a| !a.starts_with('-')) else {
        eprintln!("usage: samyama schema <snapshot.sgsnap> [--markdown]");
        return 2;
    };
    match samyama::schema_doc::derive_from_path(std::path::Path::new(path)) {
        Ok(s) => {
            eprintln!(
                "{path}: {} nodes, {} edges, {} labels, {} edge types, {} distinct relationships",
                s.nodes,
                s.edges,
                s.labels.len(),
                s.edge_types.len(),
                s.triples.len()
            );
            if s.dangling_edges > 0 {
                eprintln!(
                    "warning: {} edge(s) point at a node this snapshot does not contain",
                    s.dangling_edges
                );
            }
            print!("{}", if markdown { s.to_markdown() } else { s.to_mermaid() });
            0
        }
        Err(e) => {
            eprintln!("ERROR {path}: {e}");
            2
        }
    }
}

/// `samyama snapshot-key`
///
/// Prints a fresh 32-byte snapshot encryption key, as hex, from the OS random
/// source. Write it to a file and pass that file to `--snapshot-key`.
///
/// Hex rather than raw bytes so it can be pasted into a secret manager: a key
/// an operator can move is a key they can rotate, and REL-09 asks for rotation
/// without downtime.
fn cmd_snapshot_key() -> i32 {
    match samyama::snapshot::encryption::generate_key() {
        Ok(k) => {
            println!("# write this to a file and pass it to --snapshot-key");
            println!("{k}");
            0
        }
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

/// `samyama auth-user <name>` — reads a password from stdin, prints a credential line.
///
/// Stdin and not an argument: a password on the command line is visible in
/// `ps` to every user on the box, and lands in the shell history of the person
/// who typed it.
///
/// The output is an argon2id hash. That is the slow hash, and the reason the
/// two kinds of credential differ: against a stolen file the defence for a
/// human-chosen password is the cost of each guess, while a 32-byte random
/// token has nothing to guess and gets the fast one.
fn cmd_auth_user(argv: &[String]) -> i32 {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};

    let Some(name) = argv.get(2) else {
        eprintln!("usage: samyama auth-user <name>   (the password is read from stdin)");
        return 2;
    };
    if name.contains(':') {
        eprintln!("a credential name cannot contain `:` -- it separates the name from the hash");
        return 2;
    }

    let mut password = String::new();
    if std::io::stdin().read_line(&mut password).is_err() {
        eprintln!("could not read the password from stdin");
        return 1;
    }
    let password = password.trim_end_matches(['\n', '\r']);
    if password.is_empty() {
        eprintln!("refusing to hash an empty password");
        return 2;
    }

    let salt = SaltString::generate(&mut OsRng);
    match argon2::Argon2::default().hash_password(password.as_bytes(), &salt) {
        Ok(h) => {
            println!("# add this line to the file you pass to --auth-file");
            println!("{name}:{h}");
            0
        }
        Err(e) => {
            eprintln!("hashing failed: {e}");
            1
        }
    }
}

/// `samyama auth-token [name]`
///
/// Prints a credential line for `--auth-file`, and the token itself once.
///
/// It generates the token rather than taking one, because a token an operator
/// thinks of is a password, and a password is the thing a SHA-256 credential
/// file is *not* built for: a fast hash is the right choice against a stolen
/// file only when there is nothing to guess. 32 bytes from the OS random source
/// leaves nothing to guess.
///
/// The token is printed to stdout and never stored. What goes in the file is
/// the digest, so the file is not usable as a credential itself.
fn cmd_auth_token(argv: &[String]) -> i32 {
    use sha2::{Digest, Sha256};

    let name = argv.get(2).cloned().unwrap_or_else(|| "operator".to_string());
    if name.contains(':') {
        eprintln!("a credential name cannot contain `:` -- it separates the name from the digest");
        return 2;
    }

    // `getrandom` is already in the tree; reading /dev/urandom directly keeps
    // this to the standard library and makes the source of the entropy the
    // obvious thing rather than a crate feature.
    let mut raw = [0u8; 32];
    match std::fs::File::open("/dev/urandom").and_then(|mut f| {
        use std::io::Read;
        f.read_exact(&mut raw)
    }) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("cannot read /dev/urandom: {e}");
            return 1;
        }
    }
    let token: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let digest: String =
        Sha256::digest(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();

    println!("# add this line to the file you pass to --auth-file");
    println!("{name}:{digest}");
    println!();
    println!("# the token itself, shown once -- the server stores only the digest above");
    println!("{token}");
    0
}

/// `samyama verify <snapshot.sgsnap> --queries <catalog.json>`
///
/// Restores the snapshot into a fresh store and runs its shipped catalog. Exits
/// non-zero on any mismatch, naming the entries that failed and the probable
/// class -- not a diff, which tells the reader what changed rather than what
/// broke (#1157).
fn cmd_verify(argv: &[String]) -> i32 {
    let flag = |name: &str| argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1));
    let Some(snapshot) = argv.get(2).filter(|s| !s.starts_with("--")) else {
        eprintln!("usage: samyama verify <snapshot.sgsnap> --queries <catalog.json>");
        return 64;
    };
    let Some(catalog_path) = flag("--queries") else {
        eprintln!("verify needs --queries <catalog.json>");
        return 64;
    };

    let catalog: samyama::snapshot::verify::QueryCatalog = match std::fs::File::open(catalog_path)
        .map_err(|e| e.to_string())
        .and_then(|f| serde_json::from_reader(f).map_err(|e| e.to_string()))
    {
        Ok(c) => c,
        Err(e) => { eprintln!("could not read catalog {catalog_path}: {e}"); return 65; }
    };

    let mut store = GraphStore::new();
    let file = match std::fs::File::open(snapshot) {
        Ok(f) => f,
        Err(e) => { eprintln!("could not open {snapshot}: {e}"); return 66; }
    };
    if let Err(e) = samyama::snapshot::import_tenant(&mut store, file) {
        // An import that fails is already a failed restore; say so plainly
        // rather than going on to report every query as broken.
        eprintln!("restore failed before any query ran: {e}");
        return 1;
    }

    let report = match samyama::snapshot::verify::verify(&store, &catalog) {
        Ok(r) => r,
        Err(e) => { eprintln!("{e}"); return 65; }
    };

    let total = report.results.len();
    let failed: Vec<_> = report.failed().collect();
    println!("verify {snapshot}");
    println!("  catalog {catalog_path}: {total} entries");
    for r in &failed {
        let class = r.failure.expect("failed entries carry a class");
        println!("  FAIL {}  expected {} rows, got {}{}",
                 r.id, r.expected_rows, r.actual_rows,
                 if r.detail.is_empty() { String::new() } else { format!("  [{}]", r.detail) });
        println!("       {}", class.explain());
    }
    if report.everything_empty {
        println!("  FAIL every entry returned zero rows. That is what this catalog \
                  looks like run against an empty graph, so the run is not evidence \
                  of a good restore whatever the expectations say.");
    }
    if report.is_ok() {
        println!("  OK  {total} entries reproduced");
        0
    } else {
        println!("  {} of {total} entries failed", failed.len().max(1));
        1
    }
}

/// `samyama catalog-build <snapshot.sgsnap> --sql <queries.json> --out <catalog.json>`
///
/// Runs a list of queries against a snapshot and records what they returned, so
/// the snapshot can later prove it still returns it.
fn cmd_catalog_build(argv: &[String]) -> i32 {
    let flag = |name: &str| argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1));
    let Some(snapshot) = argv.get(2).filter(|s| !s.starts_with("--")) else {
        eprintln!("usage: samyama catalog-build <snapshot.sgsnap> --queries <queries.json> \
                   --out <catalog.json>");
        return 64;
    };
    let (Some(queries_path), Some(out)) = (flag("--queries"), flag("--out")) else {
        eprintln!("catalog-build needs --queries <queries.json> and --out <catalog.json>");
        return 64;
    };

    // Input shape: [{"id", "cypher", "unanswerable"?, "params"?}, ...]
    use samyama::snapshot::verify::QuerySpec;
    let queries: Vec<QuerySpec> = match std::fs::File::open(queries_path)
        .map_err(|e| e.to_string())
        .and_then(|f| serde_json::from_reader(f).map_err(|e| e.to_string()))
    {
        Ok(q) => q,
        Err(e) => { eprintln!("could not read {queries_path}: {e}"); return 65; }
    };

    let mut store = GraphStore::new();
    let file = match std::fs::File::open(snapshot) {
        Ok(f) => f,
        Err(e) => { eprintln!("could not open {snapshot}: {e}"); return 66; }
    };
    if let Err(e) = samyama::snapshot::import_tenant(&mut store, file) {
        eprintln!("could not restore {snapshot}: {e}");
        return 1;
    }

    match samyama::snapshot::verify::build_catalog(&store, &queries, &[]) {
        Err(e) => { eprintln!("{e}"); 1 }
        Ok(catalog) => {
            let json = serde_json::to_string_pretty(&catalog).expect("serialize");
            if let Err(e) = std::fs::write(out, json) {
                eprintln!("could not write {out}: {e}");
                return 74;
            }
            println!("wrote {out}: {} entries", catalog.entries.len());
            0
        }
    }
}

/// `samyama catalog-gate <catalog.json> [--allow-observed]`
///
/// Refuses to publish a catalog drawn from traffic without an explicit flag,
/// and scans every question and every parameter sample for personal data
/// (#1159). A parameter sample is a data excerpt: it exists so the build-time
/// execution gate has something to run, and on a KG holding personal data it is
/// a real value about to be published.
fn cmd_catalog_gate(argv: &[String]) -> i32 {
    use samyama::snapshot::publish_gate::gate;
    let Some(path) = argv.get(2).filter(|s| !s.starts_with("--")) else {
        eprintln!("usage: samyama catalog-gate <catalog.json> [--allow-observed]");
        return 64;
    };
    let allow_observed = argv.iter().any(|a| a == "--allow-observed");

    let catalog: samyama::snapshot::verify::QueryCatalog = match std::fs::File::open(path)
        .map_err(|e| e.to_string())
        .and_then(|f| serde_json::from_reader(f).map_err(|e| e.to_string()))
    {
        Ok(c) => c,
        Err(e) => { eprintln!("could not read {path}: {e}"); return 65; }
    };

    let mut texts: Vec<(String, String)> = Vec::new();
    for e in &catalog.entries {
        texts.push((e.id.clone(), e.cypher.clone()));
        for p in &e.params {
            texts.push((format!("{}.params.{}", e.id, p.name), p.sample.to_string()));
        }
    }

    let v = gate(catalog.provenance, &texts, allow_observed);
    println!("catalog-gate {path}");
    println!("  provenance: {:?}, {} entries", catalog.provenance, catalog.entries.len());
    println!("  digest: {}", samyama::snapshot::verify::catalog_digest(
        &serde_json::to_string(&catalog).unwrap_or_default()));

    // KG-08 conformance, reported always and enforced on request. Reported
    // always because a catalog that does not meet it is still worth publishing
    // and the gap should be visible; enforced on request because KG-08 is a
    // release requirement rather than a safety one.
    let kg08 = samyama::snapshot::verify::kg08_conformance(&catalog);
    for p in &kg08 {
        println!("  KG-08 {p}");
    }
    let require_kg08 = argv.iter().any(|a| a == "--kg08");
    if require_kg08 && !kg08.is_empty() {
        println!("  REFUSED {} KG-08 problem(s), and --kg08 was given", kg08.len());
        return 1;
    }
    for f in &v.findings {
        println!("  FINDING {:<16} in {}  {}", f.kind, f.where_, f.excerpt);
    }
    for r in &v.reasons {
        println!("  REFUSED {r}");
    }
    if v.publishable {
        println!("  OK  publishable");
        0
    } else {
        1
    }
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

/// Build a synthetic social network with rich schema.
///
/// Creates 6 node labels (Person, Company, City, Post, Comment, Tag) and
/// 8 edge types (KNOWS, WORKS_AT, LIVES_IN, WROTE, COMMENTED, REPLIED_TO,
/// LIKES, HAS_TAG) for a total of ~5,250 nodes and ~10,000 edges.
fn build_social_network(store: &mut GraphStore) {
    println!("Building synthetic social network...");

    // --- Reference data ---
    let first_names = [
        "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
        "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rosa", "Sam", "Tara", "Uma", "Vic", "Wendy", "Xander",
        "Yara", "Zane",
    ];
    let last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
        "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    ];
    let city_data = [
        ("New York", "US", 8_336_817i64), ("Los Angeles", "US", 3_979_576),
        ("Chicago", "US", 2_693_976), ("Houston", "US", 2_320_268),
        ("San Francisco", "US", 873_965), ("Seattle", "US", 737_015),
        ("Austin", "US", 978_908), ("Boston", "US", 675_647),
        ("Denver", "US", 715_522), ("Portland", "US", 652_503),
        ("London", "UK", 8_982_000), ("Manchester", "UK", 553_230),
        ("Edinburgh", "UK", 524_930), ("Berlin", "DE", 3_644_826),
        ("Munich", "DE", 1_471_508), ("Hamburg", "DE", 1_841_179),
        ("Paris", "FR", 2_161_000), ("Lyon", "FR", 513_275),
        ("Toronto", "CA", 2_794_356), ("Vancouver", "CA", 662_248),
        ("Sydney", "AU", 5_312_163), ("Melbourne", "AU", 5_078_193),
        ("Tokyo", "JP", 13_960_000), ("Singapore", "SG", 5_454_000),
        ("Bangalore", "IN", 8_443_675), ("Mumbai", "IN", 12_442_373),
        ("São Paulo", "BR", 12_325_232), ("Dublin", "IE", 544_107),
        ("Amsterdam", "NL", 872_680), ("Stockholm", "SE", 975_551),
    ];
    let company_data = [
        ("Acme Corp", "Technology", 5000i64), ("GlobalBank", "Finance", 12000),
        ("MediCare Plus", "Healthcare", 3500), ("EcoEnergy", "Energy", 2200),
        ("DataStream", "Technology", 800), ("CloudNine", "Technology", 1500),
        ("BioGenesis", "Healthcare", 4200), ("QuantumLeap", "Technology", 350),
        ("GreenField", "Agriculture", 6000), ("SkyRoute", "Logistics", 9000),
        ("NexGen AI", "Technology", 600), ("FinEdge", "Finance", 2800),
        ("UrbanBuild", "Construction", 7500), ("MediaPulse", "Media", 1200),
        ("AeroSpace X", "Aerospace", 4500), ("FoodChain", "Retail", 15000),
        ("CyberShield", "Security", 900), ("EduPath", "Education", 1800),
        ("TravelWise", "Travel", 3200), ("PharmaCore", "Healthcare", 5500),
    ];
    let tags = [
        "rust", "python", "javascript", "database", "graphdb", "ai",
        "machinelearning", "cloud", "devops", "kubernetes", "startup",
        "opensource", "performance", "security", "data", "engineering",
        "product", "design", "career", "remote",
    ];
    let post_topics = [
        "Just shipped a new feature for our graph database!",
        "Thoughts on knowledge graphs vs vector databases?",
        "Anyone using Cypher in production? Share your experience!",
        "The future of AI-native databases",
        "Graph algorithms that changed how we think about data",
        "Why property graphs beat RDF for most use cases",
        "Performance tuning tips for large-scale graph traversals",
        "How we reduced query latency by 10x with late materialization",
        "Building a real-time fraud detection system with graphs",
        "Open-source graph databases: a comparison",
        "The rise of multi-model databases",
        "Why every data engineer should learn graph theory",
        "Scaling graph analytics to billions of edges",
        "Our journey from PostgreSQL to a native graph database",
        "Community detection algorithms explained simply",
        "Vector search meets graph traversal: the best of both worlds",
        "What I learned building a distributed graph database in Rust",
        "Graph-powered recommendation engines",
        "The inverted LLM pattern: let graphs handle the data",
        "LDBC benchmark results: what they really mean",
    ];

    // --- Phase 1: Create City nodes ---
    let mut city_ids: Vec<NodeId> = Vec::with_capacity(30);
    for (name, country, population) in &city_data {
        let id = store.create_node("City");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
            node.set_property("country", *country);
            node.set_property("population", *population);
        }
        city_ids.push(id);
    }

    // --- Phase 2: Create Company nodes ---
    let mut company_ids: Vec<NodeId> = Vec::with_capacity(20);
    for (i, (name, industry, size)) in company_data.iter().enumerate() {
        let id = store.create_node("Company");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
            node.set_property("industry", *industry);
            node.set_property("employees", *size);
            node.set_property("founded", 1990i64 + (i as i64 * 3) % 35);
        }
        // LOCATED_IN edge: company → city
        let city = city_ids[i % city_ids.len()];
        let _ = store.create_edge(id, city, "LOCATED_IN");
        company_ids.push(id);
    }

    // --- Phase 3: Create Tag nodes ---
    let mut tag_ids: Vec<NodeId> = Vec::with_capacity(20);
    for tag in &tags {
        let id = store.create_node("Tag");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *tag);
        }
        tag_ids.push(id);
    }

    // --- Phase 4: Create Person nodes ---
    let num_persons = 200;
    let mut person_ids: Vec<NodeId> = Vec::with_capacity(num_persons);
    for i in 0..num_persons {
        let first = first_names[i % first_names.len()];
        let last = last_names[i / first_names.len() % last_names.len()];
        let id = store.create_node("Person");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", format!("{} {}", first, last));
            node.set_property("age", 22i64 + (i as i64 * 7) % 45);
            node.set_property("email", format!("{}.{}@example.com", first.to_lowercase(), last.to_lowercase()));
        }
        // LIVES_IN edge: person → city
        let city = city_ids[i % city_ids.len()];
        let _ = store.create_edge(id, city, "LIVES_IN");
        // WORKS_AT edge: person → company
        let company = company_ids[i % company_ids.len()];
        if let Ok(eid) = store.create_edge(id, company, "WORKS_AT") {
            store.set_edge_property_sparse(eid, "since", PropertyValue::Integer(2015i64 + (i as i64 * 3) % 11));
            store.set_edge_property_sparse(eid, "role", PropertyValue::String(match i % 5 {
                0 => "Engineer",
                1 => "Manager",
                2 => "Analyst",
                3 => "Designer",
                _ => "Director",
            }.to_string()));
        }
        person_ids.push(id);
    }

    // --- Phase 5: KNOWS edges (social connections) ---
    let mut knows_count = 0usize;
    for i in 0..num_persons {
        // Each person knows 5-8 others (deterministic spread)
        let degree = 5 + i % 4;
        for d in 0..degree {
            let j = (i + 1 + d * 7 + i * 3) % num_persons;
            if i != j {
                if store.create_edge(person_ids[i], person_ids[j], "KNOWS").is_ok() {
                    knows_count += 1;
                }
            }
        }
    }

    // --- Phase 6: Create Post nodes ---
    let num_posts = 2000;
    let mut post_ids: Vec<NodeId> = Vec::with_capacity(num_posts);
    for i in 0..num_posts {
        let id = store.create_node("Post");
        if let Some(node) = store.get_node_mut(id) {
            let topic = post_topics[i % post_topics.len()];
            let repeat = i / post_topics.len();
            let title = if repeat == 0 {
                topic.to_string()
            } else {
                format!("{} #{}", topic, repeat + 1)
            };
            node.set_property("title", title.as_str());
            node.set_property("content", format!("{}. This is post #{} with detailed thoughts on the topic.", topic, i));
            node.set_property("created_at", 1700000000000i64 + (i as i64 * 3_600_000));
            node.set_property("views", (i as i64 * 17) % 5000);
        }
        // WROTE edge: person → post
        let author = person_ids[i % num_persons];
        let _ = store.create_edge(author, id, "WROTE");
        // HAS_TAG edges: 1-3 tags per post
        let num_tags = 1 + i % 3;
        for t in 0..num_tags {
            let tag = tag_ids[(i + t * 7) % tag_ids.len()];
            let _ = store.create_edge(id, tag, "HAS_TAG");
        }
        post_ids.push(id);
    }

    // --- Phase 7: Create Comment nodes ---
    let num_comments = 3000;
    let mut comment_ids: Vec<NodeId> = Vec::with_capacity(num_comments);
    for i in 0..num_comments {
        let id = store.create_node("Comment");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("text", format!("Comment #{}: Great point! Here are my thoughts...", i));
            node.set_property("created_at", 1700100000000i64 + (i as i64 * 1_800_000));
        }
        // WROTE edge: person → comment
        let author = person_ids[i % num_persons];
        let _ = store.create_edge(author, id, "WROTE");
        // COMMENTED edge: comment → post (most comments reply to posts)
        if i % 3 != 0 || comment_ids.is_empty() {
            let post = post_ids[i % num_posts];
            let _ = store.create_edge(id, post, "COMMENTED");
        } else {
            // REPLIED_TO edge: comment → comment (threaded replies)
            let parent = comment_ids[(i * 7) % comment_ids.len()];
            let _ = store.create_edge(id, parent, "REPLIED_TO");
        }
        comment_ids.push(id);
    }

    // --- Phase 8: LIKES edges (person → post) ---
    let mut likes_count = 0usize;
    for i in 0..num_persons {
        // Each person likes 5-15 posts
        let num_likes = 5 + (i * 7) % 11;
        for l in 0..num_likes {
            let post = post_ids[(i * 13 + l * 37) % num_posts];
            if store.create_edge(person_ids[i], post, "LIKES").is_ok() {
                likes_count += 1;
            }
        }
    }

    let total_nodes = city_ids.len() + company_ids.len() + tag_ids.len()
        + person_ids.len() + post_ids.len() + comment_ids.len();
    println!("  Schema: 6 labels (Person, Company, City, Post, Comment, Tag)");
    println!("          8 edge types (KNOWS, WORKS_AT, LIVES_IN, LOCATED_IN, WROTE, COMMENTED, REPLIED_TO, LIKES, HAS_TAG)");
    println!("  Nodes:  {} Person, {} Company, {} City, {} Tag, {} Post, {} Comment",
             person_ids.len(), company_ids.len(), city_ids.len(),
             tag_ids.len(), post_ids.len(), comment_ids.len());
    println!("  Edges:  {} KNOWS, {} LIKES, {} Posts, {} Comments (total: {})",
             knows_count, likes_count, num_posts, num_comments,
             store.edge_count());
    println!("  Total:  {} nodes, {} edges", total_nodes, store.edge_count());
}

/// Load a single LDBC Graphalytics dataset into the graph store.
///
/// `max_vertices` caps the number of vertices loaded (edges are only created
/// when both endpoints are in the loaded set).
fn load_graphalytics_dataset(
    store: &mut GraphStore,
    dataset: &str,
    directed: bool,
    max_vertices: Option<usize>,
) -> bool {
    let data_dir = std::path::Path::new("data/graphalytics");

    // Try subdirectory first (XS layout), then flat (S-size tar extraction)
    let sub_v = data_dir.join(dataset).join(format!("{}.v", dataset));
    let sub_e = data_dir.join(dataset).join(format!("{}.e", dataset));
    let flat_v = data_dir.join(format!("{}.v", dataset));
    let flat_e = data_dir.join(format!("{}.e", dataset));

    let (v_path, e_path) = if sub_v.exists() && sub_e.exists() {
        (sub_v, sub_e)
    } else if flat_v.exists() && flat_e.exists() {
        (flat_v, flat_e)
    } else {
        println!("  Dataset '{}' not found in data/graphalytics/", dataset);
        println!("  Download with: ./scripts/download_graphalytics.sh --size S");
        return false;
    };

    let limit_str = match max_vertices {
        Some(n) => format!(" (limit: {} vertices)", n),
        None => String::new(),
    };
    println!("Loading LDBC Graphalytics: {}{}...", dataset, limit_str);

    // Phase 1: Read vertices
    let mut vid_to_node: HashMap<u64, NodeId> = HashMap::new();
    if let Ok(file) = File::open(&v_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            if let Some(cap) = max_vertices {
                if vid_to_node.len() >= cap {
                    break;
                }
            }
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Ok(vid) = trimmed.parse::<u64>() {
                let node_id = store.create_node("Vertex");
                if let Some(node) = store.get_node_mut(node_id) {
                    node.set_property("vid", vid as i64);
                    node.set_property("dataset", dataset);
                }
                vid_to_node.insert(vid, node_id);
            }
        }
    }

    // Phase 2: Read edges (only where both endpoints are loaded)
    let edge_type = if directed { "LINKS" } else { "CONNECTS" };
    let mut edge_count: usize = 0;
    if let Ok(file) = File::open(&e_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = if trimmed.contains('|') {
                trimmed.split('|').collect()
            } else {
                trimmed.split_whitespace().collect()
            };

            if parts.len() < 2 {
                continue;
            }

            let src = match parts[0].parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let tgt = match parts[1].parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let (Some(&s), Some(&t)) = (vid_to_node.get(&src), vid_to_node.get(&tgt)) {
                if let Ok(eid) = store.create_edge(s, t, edge_type) {
                    if parts.len() >= 3 {
                        if let Ok(w) = parts[2].parse::<f64>() {
                            store.set_edge_property_sparse(eid, "weight", PropertyValue::Float(w));
                        }
                    }
                    edge_count += 1;
                }
            }
        }
    }

    println!("  Loaded {} vertices, {} edges ({})",
             vid_to_node.len(), edge_count,
             if directed { "directed" } else { "undirected" });
    true
}

async fn start_server() {
    let (mut graph, rx) = GraphStore::with_async_indexing();

    let mut config = ServerConfig::default();
    config.address = std::env::args().find(|a| a.starts_with("--host"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--host").nth(1))
        .unwrap_or_else(|| "127.0.0.1".to_string());
    config.port = std::env::args().find(|a| a.starts_with("--port"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--port").nth(1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);

    // Parse --http-port (HTTP REST API port; default 8080).
    let http_port: u16 = std::env::args()
        .position(|a| a == "--http-port")
        .and_then(|pos| std::env::args().nth(pos + 1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    // Origins allowed to call the HTTP API from a browser, and the only origins
    // the Private Network Access opt-in is echoed to. Repeatable `--cors-origin`,
    // or `SAMYAMA_CORS_ORIGINS` as a comma-separated list. Empty by default:
    // before #1328 the server accepted every origin and echoed PNA to whoever
    // asked, so a page on the open web could drive `/api/query`.
    let mut cors_origins: Vec<String> = Vec::new();
    {
        let args: Vec<String> = std::env::args().collect();
        for (i, a) in args.iter().enumerate() {
            if a == "--cors-origin" {
                if let Some(v) = args.get(i + 1) {
                    cors_origins.push(v.clone());
                }
            }
        }
        if let Ok(env) = std::env::var("SAMYAMA_CORS_ORIGINS") {
            cors_origins.extend(
                env.split(',').map(|o| o.trim().to_string()).filter(|o| !o.is_empty()),
            );
        }
    }

    // TLS for the HTTP listener (REL-09). Paths, like the credential file:
    // a private key is a secret and belongs in a file with file permissions,
    // not in argv where `ps` shows it to every user on the box.
    let tls_pem: Option<(String, String)> = {
        let args: Vec<String> = std::env::args().collect();
        let pick = |flag: &str, env: &str| -> Option<String> {
            args.iter()
                .position(|a| a == flag)
                .and_then(|i| args.get(i + 1).cloned())
                .or_else(|| std::env::var(env).ok())
        };
        let cert = pick("--tls-cert", "SAMYAMA_TLS_CERT");
        let key = pick("--tls-key", "SAMYAMA_TLS_KEY");
        match (cert, key) {
            (None, None) => None,
            // One without the other is a misconfiguration, not a default. A
            // server that fell back to plain HTTP here would look like it had
            // TLS to the operator who asked for it.
            (Some(_), None) | (None, Some(_)) => {
                eprintln!("FATAL: --tls-cert and --tls-key must be given together");
                std::process::exit(1);
            }
            (Some(c), Some(k)) => {
                let read = |p: &str| match std::fs::read_to_string(p) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("FATAL: cannot read {p}: {e}");
                        std::process::exit(1);
                    }
                };
                Some((read(&c), read(&k)))
            }
        }
    };

    // Audit log for state-changing HTTP requests (REL-08).
    let audit_log: Option<std::sync::Arc<samyama::http::server::AuditLog>> = {
        let args: Vec<String> = std::env::args().collect();
        let path = args
            .iter()
            .position(|a| a == "--audit-log")
            .and_then(|i| args.get(i + 1).cloned())
            .or_else(|| std::env::var("SAMYAMA_AUDIT_LOG").ok());
        match path {
            None => None,
            // A configured audit log that cannot be opened stops the server.
            // Starting without it would run unaudited for an operator who
            // asked to be audited, which is the state the flag exists to leave.
            Some(p) => match samyama::http::server::AuditLog::open(&p) {
                Ok(l) => {
                    println!("HTTP API: auditing state-changing requests to {p}");
                    Some(std::sync::Arc::new(l))
                }
                Err(e) => {
                    eprintln!("FATAL: --audit-log {p}: {e}");
                    std::process::exit(1);
                }
            },
        }
    };

    // Snapshot encryption key (REL-09). A path: a key on the command line is
    // visible in `ps` to every user on the box.
    let snapshot_key: Option<std::sync::Arc<[u8; samyama::snapshot::encryption::KEY_BYTES]>> = {
        let args: Vec<String> = std::env::args().collect();
        let path = args
            .iter()
            .position(|a| a == "--snapshot-key")
            .and_then(|i| args.get(i + 1).cloned())
            .or_else(|| std::env::var("SAMYAMA_SNAPSHOT_KEY").ok());
        match path {
            None => None,
            // A configured key that cannot be read stops the server, rather
            // than exporting plaintext for an operator who asked for
            // encryption.
            Some(p) => match samyama::snapshot::encryption::read_key(std::path::Path::new(&p)) {
                Ok(k) => {
                    println!("HTTP API: snapshots exported encrypted, key from {p}");
                    Some(std::sync::Arc::new(k))
                }
                Err(e) => {
                    eprintln!("FATAL: --snapshot-key {p}: {e}");
                    std::process::exit(1);
                }
            },
        }
    };

    // Credentials for the HTTP API (REL-08, #1328). A path, not a token: a
    // secret passed on the command line is visible in `ps` to every user on the
    // box, and one in the environment is inherited by every child process.
    let credentials: Vec<samyama::http::server::Credential> = {
        let args: Vec<String> = std::env::args().collect();
        let path = args
            .iter()
            .position(|a| a == "--auth-file")
            .and_then(|i| args.get(i + 1).cloned())
            .or_else(|| std::env::var("SAMYAMA_AUTH_FILE").ok());
        match path {
            None => Vec::new(),
            // A configured file that cannot be read stops the server. Starting
            // anyway would publish an unauthenticated API to an operator who
            // had just asked for the opposite, and the log line saying so would
            // scroll past.
            Some(p) => match samyama::http::server::read_credentials(std::path::Path::new(&p)) {
                Ok(c) => {
                    println!("HTTP API: {} credential(s) loaded from {p}", c.len());
                    c
                }
                Err(e) => {
                    eprintln!("FATAL: --auth-file {p}: {e}");
                    std::process::exit(1);
                }
            },
        }
    };

    // Parse --data-path <dir> (snapshot/RocksDB persistence dir) and --ephemeral
    // (no persistence — guarantees an empty store, no CWD-relative ./samyama_data
    // recovery). --ephemeral wins if both are given.
    if std::env::args().any(|a| a == "--ephemeral") {
        config.data_path = None;
    } else if let Some(path) = std::env::args()
        .position(|a| a == "--data-path")
        .and_then(|pos| std::env::args().nth(pos + 1))
    {
        config.data_path = Some(path);
    }

    // Parse --import-dir <dir>: the directory `LOAD CSV` may read under (LANG-09).
    //
    // Absent by default, and absent means the clause is refused rather than reading
    // from the working directory. `LOAD CSV FROM 'file:///etc/passwd'` on a server
    // reachable over the network is an arbitrary local file read for anyone who can
    // send a query, so this is a gate that must be opened deliberately.
    if let Some(dir) = std::env::args()
        .position(|a| a == "--import-dir")
        .and_then(|pos| std::env::args().nth(pos + 1))
    {
        match samyama::query::csv_source::set_import_root(Some(std::path::Path::new(&dir))) {
            Ok(()) => println!("LOAD CSV enabled, reading under {}", dir),
            Err(e) => {
                eprintln!("--import-dir: {e}");
                std::process::exit(2);
            }
        }
    }

    // Parse --demo flag: social (rich schema) or large (scale stress test)
    let demo_mode: Option<String> = std::env::args()
        .position(|a| a == "--demo")
        .and_then(|pos| std::env::args().nth(pos + 1));

    // Initialize persistence FIRST (before loading data)
    let persistence = if let Some(path) = &config.data_path {
        match samyama::PersistenceManager::new(path) {
            Ok(pm) => Some(Arc::new(pm)),
            Err(e) => {
                eprintln!("Failed to initialize persistence: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Recover persisted data from RocksDB
    let mut recovered = false;
    if let Some(ref pm) = persistence {
        match pm.list_persisted_tenants() {
            Ok(tenants) if !tenants.is_empty() => {
                println!("Recovering data for {} tenant(s)...", tenants.len());
                for tenant in &tenants {
                    match pm.recover(tenant) {
                        Ok((nodes, edges)) => {
                            println!("  Tenant '{}': {} nodes, {} edges", tenant, nodes.len(), edges.len());
                            for node in nodes {
                                graph.insert_recovered_node(node);
                            }
                            for edge in edges {
                                if let Err(e) = graph.insert_recovered_edge(edge) {
                                    eprintln!("  Warning: edge recovery error: {}", e);
                                }
                            }
                            recovered = true;
                        }
                        Err(e) => eprintln!("  Error recovering tenant '{}': {}", tenant, e),
                    }
                }
                println!("Recovery complete. Total: {} nodes, {} edges in-memory", graph.node_count(), graph.edge_count());
            }
            Ok(_) => println!("No persisted tenants found."),
            Err(e) => eprintln!("Error listing persisted tenants: {}", e),
        }
    }

    // Load demo data based on --demo flag (skip if persisted data was recovered)
    if !recovered {
        match demo_mode.as_deref() {
            Some("social") => {
                // Rich schema: 6 labels, 9 edge types, ~5K nodes, ~10K edges
                build_social_network(&mut graph);
            }
            Some("large") => {
                // Full wiki-Talk dataset (2.4M vertices, 5M edges)
                load_graphalytics_dataset(&mut graph, "wiki-Talk", true, None);
            }
            Some(other) => {
                eprintln!("Unknown --demo mode '{}'. Use: --demo social  or  --demo large", other);
                eprintln!("  social  — synthetic social network (5K nodes, 6 labels, 9 edge types)");
                eprintln!("  large   — wiki-Talk (2.4M vertices, directed discussion graph)");
            }
            None => {
                // Default: empty graph
            }
        }
    }

    // HA-08: If no RocksDB recovery happened, replay the last committed .sgsnap
    // snapshot from <data_path>/snapshots/ so imports survive restart.
    if !recovered {
        if let Some(path) = &config.data_path {
            match samyama::snapshot::persist::restore_persisted_snapshots(path, &mut graph) {
                Ok(Some(stats)) => println!(
                    "[snapshot-persist] Restored {} nodes, {} edges from {}/snapshots",
                    stats.node_count, stats.edge_count, path
                ),
                Ok(None) => {}
                Err(e) => eprintln!("[snapshot-persist] Restore error: {}", e),
            }
        }
    }

    println!("\nGraph Statistics:");
    println!("  Total nodes: {}", graph.node_count());
    println!("  Total edges: {}", graph.edge_count());

    let store = Arc::new(RwLock::new(graph));
    let http_data_path = config.data_path.clone();

    // HA-09: one TenantManager shared between RESP and HTTP so a tenant
    // created via either path is visible to both.
    let shared_tenants: Arc<samyama::persistence::TenantManager> = persistence
        .as_ref()
        .map(|pm| pm.tenants_arc())
        .unwrap_or_else(|| Arc::new(samyama::persistence::TenantManager::new()));

    // The default tenant's quotas, from the command line.
    //
    // They were `ResourceQuotas::default()` and nothing could change them: no
    // flag, no environment variable, and `update_quotas` had no HTTP route. A
    // stock server therefore stopped accepting writes at 1M nodes on any
    // hardware, which is 1% of the node count PERF-12 is written against — so
    // three scale requirements were unmeasurable for a reason that had nothing
    // to do with the machine (#1483).
    //
    // Unset flags leave the shipped defaults exactly as they were. This adds a
    // way to raise the ceiling; it does not move it.
    if let Some(q) = quota_overrides_from_args() {
        match shared_tenants.update_quotas("default", q) {
            Ok(()) => tracing::info!("default tenant quotas overridden from the command line"),
            Err(e) => {
                eprintln!("error: could not apply quota overrides: {e}");
                std::process::exit(2);
            }
        }
    }

    let global_embed_pipeline: Option<Arc<EmbedPipeline>> =
        if std::env::var("EMBED_ENABLED").map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(false) {
            // Refused, not defaulted — and this is the path that matters most,
            // because embedding sends the property **value**, not the schema.
            // The old arm list did not even carry `azure`, so that spelling
            // meant OpenAI, and so did every typo.
            let provider = match LLMProvider::parse_named(
                "EMBED_PROVIDER",
                &std::env::var("EMBED_PROVIDER").unwrap_or_default(),
            ) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("EMBED_ENABLED is true but the provider is unusable: {e}");
                    std::process::exit(2);
                }
            };
            let model     = std::env::var("EMBED_MODEL").unwrap_or_else(|_| "text-embedding-3-small".to_string());
            let api_key   = std::env::var("EMBED_API_KEY").ok();
            let base_url  = std::env::var("EMBED_API_BASE_URL").ok();
            let dimension = std::env::var("EMBED_DIMENSION").ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1536);
            let chunk_size    = std::env::var("EMBED_CHUNK_SIZE").ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(512);
            let chunk_overlap = std::env::var("EMBED_CHUNK_OVERLAP").ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64);

            let embed_config = AutoEmbedConfig {
                provider,
                embedding_model: model.clone(),
                api_key,
                api_base_url: base_url,
                chunk_size,
                chunk_overlap,
                vector_dimension: dimension,
                embedding_policies: HashMap::new(),
                embedding_property: "embedding".to_string(),
            };

            match EmbedPipeline::new(embed_config) {
                Ok(pipeline) => {
                    println!(
                        "Global embed pipeline: model={} dimension={} chunk_size={} chunk_overlap={}",
                        model, dimension, chunk_size, chunk_overlap
                    );
                    Some(Arc::new(pipeline))
                }
                Err(e) => {
                    eprintln!("Warning: failed to build embed pipeline from EMBED_* vars: {}", e);
                    None
                }
            }
        } else {
            None
        };

    println!("\nServer starting on {}:{}", config.address, config.port);

    // Start background indexer now that store is wrapped in Arc
    if let Some(ref pm) = persistence {
        pm.start_indexer(Arc::clone(&store), rx);
    }

    // Start HTTP server for Visualizer API (port from --http-port, default 8080)
    let http_store = Arc::clone(&store);
    let http_tenants = Arc::clone(&shared_tenants);
    let http_persistence = persistence.clone();
    let http_bind_host = config.address.clone();
    let http_cors_origins = cors_origins.clone();
    let http_credentials = credentials.clone();
    let http_tls = tls_pem.clone();
    let http_audit = audit_log.clone();
    let http_snapshot_key = snapshot_key.clone();
    tokio::spawn(async move {
        let mut http_server = HttpServer::new(http_store, http_port)
            .with_data_path(http_data_path)
            // The same host as the RESP listener. The HTTP server used to bind
            // 0.0.0.0 unconditionally while RESP defaulted to loopback, so
            // `--host` said one thing and half the server did another (#1328).
            .with_bind_host(http_bind_host)
            .with_allowed_origins(http_cors_origins)
            .with_credentials(http_credentials)
            .with_tenant_manager(http_tenants);
        if let Some((cert, key)) = http_tls {
            http_server = http_server.with_tls(cert, key);
        }
        if let Some(log) = http_audit {
            http_server = http_server.with_audit_log(log);
        }
        if let Some(k) = http_snapshot_key {
            http_server = http_server.with_snapshot_key(k);
        }
        if let Some(pm) = http_persistence {
            http_server = http_server.with_persistence(pm);
        }
        if let Some(ref pipeline) = global_embed_pipeline {
            http_server = http_server.with_embed_pipeline(Arc::clone(pipeline));
        }
        println!("HTTP server starting on port {} (REST API; bundled visualizer deprecated — use https://graph.samyama.cloud)", http_port);
        if let Err(e) = http_server.start().await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    let server = RespServer::new_with_tenants(config, store, persistence, shared_tenants);

    println!("Server ready. Press Ctrl+C to stop.\n");

    if let Err(e) = server.start().await {
        eprintln!("Server error: {}", e);
    }
}

/// A `--max-*` value: a number, or `unlimited`/`none` for no limit.
///
/// Returns `None` when the flag is absent, so an unset flag is distinguishable
/// from one explicitly set to unlimited. `Some(None)` is "no ceiling".
fn quota_arg(name: &str) -> Option<Option<usize>> {
    let args: Vec<String> = std::env::args().collect();
    let i = args.iter().position(|a| a == name)?;
    // A following token that is itself a flag means the value was omitted.
    // Without this, `--max-nodes --http-port 8080` reports that `--http-port`
    // is not a number, which sends the reader after the wrong argument.
    let raw = match args.get(i + 1) {
        Some(v) if !v.starts_with("--") => v,
        _ => {
            eprintln!("error: {name} requires a value (a number, or `unlimited`)");
            std::process::exit(2);
        }
    };
    if raw.eq_ignore_ascii_case("unlimited") || raw.eq_ignore_ascii_case("none") {
        return Some(None);
    }
    match raw.replace('_', "").parse::<usize>() {
        Ok(v) => Some(Some(v)),
        Err(_) => {
            eprintln!("error: {name} expects a number or `unlimited`, got {raw:?}");
            std::process::exit(2);
        }
    }
}

/// The default tenant's quotas with any `--max-*` flag applied, or `None` when
/// no flag was given so the shipped defaults are left untouched.
fn quota_overrides_from_args() -> Option<samyama::persistence::tenant::ResourceQuotas> {
    let nodes = quota_arg("--max-nodes");
    let edges = quota_arg("--max-edges");
    let memory = quota_arg("--max-memory-bytes");
    let storage = quota_arg("--max-storage-bytes");
    if nodes.is_none() && edges.is_none() && memory.is_none() && storage.is_none() {
        return None;
    }
    let mut q = samyama::persistence::tenant::ResourceQuotas::default();
    if let Some(v) = nodes { q.max_nodes = v; }
    if let Some(v) = edges { q.max_edges = v; }
    if let Some(v) = memory { q.max_memory_bytes = v; }
    if let Some(v) = storage { q.max_storage_bytes = v; }
    Some(q)
}
