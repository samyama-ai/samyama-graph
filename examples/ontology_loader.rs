//! Load a real hierarchy from its published format and declare an OEH index over it.
//!
//! The HIER benchmark runs on generated data so it is reproducible from a clean checkout.
//! This loader is the other half: it takes the hierarchies as they actually ship, so the
//! structural probe's verdict is a fact about the real ontology rather than about a
//! generator. That matters because the verdicts differ — NCBI Taxonomy and ATC are trees,
//! Gene Ontology is a high-width DAG the index is expected to **decline**, and finding that
//! out on real data is the point.
//!
//! ```bash
//! cargo run --release --example ontology_loader -- --format taxdump --path nodes.dmp
//! cargo run --release --example ontology_loader -- --format obo --path mondo.obo
//! cargo run --release --example ontology_loader -- --format obo --path go-basic.obo   # declines
//! cargo run --release --example ontology_loader -- --format geonames --path hierarchy.txt
//! cargo run --release --example ontology_loader -- --format mesh --path mtrees2025.bin
//! cargo run --release --example ontology_loader -- --format prefix --path atc.csv --cuts 1,3,4,5,7
//! cargo run --release --example ontology_loader -- --format edgelist --path cwe.csv
//! cargo run --release --example ontology_loader -- --format calendar --from 2015 --to 2026
//! ```
//!
//! Attach to an existing KG by pointing `--label` and `--edge-type` at that KG's
//! conventions; the loader only ever *adds* the hierarchy backbone, so it is safe to run
//! against a store imported from a `.sgsnap`.
//!
//! ## Licensing
//!
//! Public-domain and open sources (Gene Ontology, MONDO, HPO, NCBI Taxonomy, GeoNames,
//! ATC codes, MITRE ATT&CK, CWE) load without ceremony, and no data is committed to this
//! repository — you supply the file. Restricted sources (UMLS, SNOMED CT) require a
//! licence and are gated behind `--i-have-a-licence <source>`, which does nothing but make
//! the acknowledgement explicit and audit-visible.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::index::hierarchy::{HierarchySpec, RollupOp};

/// Sources that may not be redistributed and must be acknowledged before loading.
const RESTRICTED: &[&str] = &["umls", "snomed", "snomedct", "mesh-restricted", "icd11"];

struct Args {
    format: String,
    path: Option<String>,
    label: String,
    edge_type: String,
    index_name: String,
    measure: Option<String>,
    cuts: Vec<usize>,
    from_year: i64,
    to_year: i64,
    licence: Option<String>,
    limit: Option<usize>,
    /// Term codes to drop before the structural probe runs.
    ///
    /// A cycle in a covering relation is a data defect and the build refuses it rather
    /// than condensing it, which is right -- but published ontologies do carry a handful
    /// of them, and 14 bad terms should not cost you the other 52,598. The refusal names
    /// the offending codes, so they can be fed straight back in here.
    exclude: HashSet<String>,
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let get = |flag: &str| -> Option<String> {
        argv.iter().position(|a| a == flag).and_then(|i| argv.get(i + 1)).cloned()
    };
    let args = Args {
        format: get("--format").unwrap_or_else(|| {
            eprintln!("{}", usage());
            std::process::exit(2)
        }),
        path: get("--path"),
        label: get("--label").unwrap_or_else(|| "Concept".to_string()),
        edge_type: get("--edge-type").unwrap_or_else(|| "IS_A".to_string()),
        index_name: get("--index").unwrap_or_else(|| "hier".to_string()),
        measure: get("--measure"),
        cuts: get("--cuts")
            .map(|c| c.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_default(),
        from_year: get("--from").and_then(|v| v.parse().ok()).unwrap_or(2015),
        to_year: get("--to").and_then(|v| v.parse().ok()).unwrap_or(2026),
        licence: get("--i-have-a-licence"),
        limit: get("--limit").and_then(|v| v.parse().ok()),
        exclude: {
            let mut set: HashSet<String> = get("--exclude")
                .map(|v| v.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect())
                .unwrap_or_default();
            if let Some(path) = get("--exclude-file") {
                match std::fs::read_to_string(&path) {
                    Ok(text) => set.extend(
                        text.lines()
                            .map(|l| l.split('#').next().unwrap_or("").trim().to_string())
                            .filter(|l| !l.is_empty()),
                    ),
                    Err(e) => {
                        eprintln!("Error: cannot read --exclude-file {path}: {e}");
                        std::process::exit(1);
                    }
                }
            }
            set
        },
    };

    if let Some(src) = &args.licence {
        eprintln!(
            "[ontology] licensed source acknowledged: {src}. \
             Data stays on your machine; nothing is written back to this repository."
        );
    }

    let mut store = GraphStore::new();
    let t0 = std::time::Instant::now();
    let (nodes, edges) = match args.format.as_str() {
        "obo" => load_obo(&mut store, &args),
        "taxdump" => load_taxdump(&mut store, &args),
        "geonames" => load_geonames(&mut store, &args),
        "mesh" => load_mesh(&mut store, &args),
        "prefix" => load_prefix(&mut store, &args),
        "edgelist" => load_edgelist(&mut store, &args),
        "calendar" => load_calendar(&mut store, &args),
        other => {
            eprintln!("unknown --format '{other}'\n\n{}", usage());
            std::process::exit(2);
        }
    };
    eprintln!(
        "[ontology] loaded {nodes} nodes, {edges} covering edges in {:.2}s",
        t0.elapsed().as_secs_f64()
    );

    // Drop excluded terms before the probe. Deleting the node takes its covering edges
    // with it, which is the point: excluding one member of a cycle breaks the cycle.
    // Done centrally rather than inside each format's loader so every format gets it.
    if !args.exclude.is_empty() {
        let mut dropped = 0usize;
        let mut missing: Vec<&String> = Vec::new();
        for code in &args.exclude {
            let hit = store
                .all_nodes()
                .iter()
                .find(|n| {
                    matches!(
                        store.node_columns.get_property(n.id.as_u64() as usize, "code"),
                        PropertyValue::String(ref c) if c == code
                    )
                })
                .map(|n| n.id);
            match hit {
                Some(id) => {
                    let _ = store.delete_node("default", id);
                    dropped += 1;
                }
                None => missing.push(code),
            }
        }
        eprintln!("[ontology] excluded {dropped} term(s) before building");
        if !missing.is_empty() {
            let shown: Vec<&str> = missing.iter().take(5).map(|s| s.as_str()).collect();
            eprintln!(
                "[ontology] warning: {} excluded code(s) were not present: {}{}",
                missing.len(),
                shown.join(", "),
                if missing.len() > 5 { ", ..." } else { "" }
            );
        }
    }

    // Declare the index and report the probe's verdict. A decline is a result, not a
    // failure: it says this poset belongs on a 2-hop index, which is the honest answer for
    // ontologies whose width approaches their leaf count.
    let mut spec = HierarchySpec::new(
        args.index_name.clone(),
        vec![samyama::graph::EdgeType::new(&args.edge_type)],
    );
    if let Some(m) = &args.measure {
        spec = spec.with_measure(
            None,
            m.clone(),
            vec![RollupOp::Sum, RollupOp::Min, RollupOp::Max, RollupOp::Count],
        );
    }
    let t1 = std::time::Instant::now();
    let mgr = std::sync::Arc::clone(&store.hierarchy_index);
    match mgr.create(&store, spec) {
        Ok(info) => {
            let build_ms = t1.elapsed().as_secs_f64() * 1000.0;
            println!();
            println!("index      : {}", info.name);
            println!("encoding   : {}", info.encoding.unwrap_or("declined"));
            println!("nodes      : {}", info.nodes);
            println!("edges      : {}", info.edges);
            match (info.width, info.declined.is_some()) {
                (Some(w), true) => println!("chain width: {w} (measured; over the cap)"),
                (Some(w), false) => println!("chain width: {w}"),
                (None, _) => println!("chain width: n/a (tree)"),
            }
            println!(
                "space      : {:.2} B/node order embedding, {} B roll-up structures",
                info.structural_bytes as f64 / info.nodes.max(1) as f64,
                info.rollup_bytes
            );
            println!("build      : {build_ms:.1} ms");
            if let Some(d) = &info.declined {
                println!();
                println!("DECLINED   : {d}");
                println!(
                    "This is the expected outcome for a high-width DAG (Gene Ontology is the\n\
                     canonical case). The planner keeps using variable-length expansion, and\n\
                     a 2-hop index is the right structure for this poset."
                );
            } else {
                println!();
                println!("Try it:");
                println!(
                    "  MATCH (d)-[:{}*0..]->(r:{} {{code: \"<code>\"}}) RETURN count(d) AS n",
                    args.edge_type, args.label
                );
                if let Some(measure) = &args.measure {
                    println!(
                        "  MATCH (d)-[:{}*0..]->(r:{} {{code: \"<code>\"}}) RETURN sum(d.{measure}) AS total",
                        args.edge_type, args.label
                    );
                }
            }
        }
        Err(samyama::index::hierarchy::HierarchyError::NotAcyclic {
            ordered,
            total,
            cycles,
        }) => {
            // Translate the internal node ids back into the identifiers the user supplied,
            // otherwise the diagnostic names nodes they have no way to look up.
            let code_of = |id: NodeId| -> String {
                match store.node_columns.get_property(id.as_u64() as usize, "code") {
                    PropertyValue::String(s) => s,
                    _ => format!("node:{}", id.as_u64()),
                }
            };
            eprintln!(
                "[ontology] index build failed: covering relation has a cycle — {} of {total} \
                 nodes could not be ordered",
                total - ordered
            );
            if !cycles.is_empty() {
                eprintln!("[ontology] {} cycle(s):", cycles.len());
                for cyc in cycles.iter().take(20) {
                    let path: Vec<String> = cyc.iter().map(|&i| code_of(i)).collect();
                    eprintln!("[ontology]   {}", path.join(" -> "));
                }
                if cycles.len() > 20 {
                    eprintln!("[ontology]   ... and {} more", cycles.len() - 20);
                }
                eprintln!(
                    "[ontology] a cycle in a subsumption relation is a data defect: it would \
                     make every roll-up over it wrong, so the build refuses rather than \
                     condensing it. Report them upstream, or exclude one term per cycle."
                );
                // One member of each cycle is enough to break it. Printed ready to paste so
                // the fix takes one more run rather than one run per cycle.
                let breakers: Vec<String> = cycles
                    .iter()
                    .filter_map(|c| c.first().map(|&i| code_of(i)))
                    .collect();
                if !breakers.is_empty() {
                    eprintln!("[ontology] to load anyway: --exclude {}", breakers.join(","));
                }
            }
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("[ontology] index build failed: {e}");
            std::process::exit(1);
        }
    }
}

fn usage() -> String {
    format!(
        "ontology_loader --format <obo|taxdump|geonames|mesh|prefix|edgelist|calendar> [--path FILE]\n\
         \n\
         Options:\n\
         \x20 --label L          node label to create           (default Concept)\n\
         \x20 --edge-type T      covering relation, child->parent (default IS_A)\n\
         \x20 --index NAME       hierarchy index name           (default hier)\n\
         \x20 --measure PROP     numeric property to roll up\n\
         \x20 --cuts a,b,c       prefix cut points, --format prefix (ATC: 1,3,4,5,7)\n\
         \x20 --from/--to YEAR   calendar range                 (default 2015..2026)\n\
         \x20 --limit N          stop after N covering edges (smoke tests)\n\
         \x20 --exclude A,B,C    drop these term codes before building (e.g. cycle members)\n\
         \x20 --exclude-file F   same, one code per line, # comments allowed\n\
         \x20 --i-have-a-licence SRC   acknowledge a restricted source: {}\n",
        RESTRICTED.join(", ")
    )
}

fn open(path: &Option<String>) -> BufReader<std::fs::File> {
    let p = path.as_ref().unwrap_or_else(|| {
        eprintln!("this format needs --path");
        std::process::exit(2)
    });
    BufReader::new(std::fs::File::open(p).unwrap_or_else(|e| {
        eprintln!("cannot open {p}: {e}");
        std::process::exit(1)
    }))
}

/// Intern a concept by its published identifier, creating the node on first sight.
struct Interner {
    ids: HashMap<String, NodeId>,
}

impl Interner {
    fn new() -> Self {
        Interner { ids: HashMap::new() }
    }

    fn get(&mut self, store: &mut GraphStore, label: &str, code: &str) -> NodeId {
        if let Some(&id) = self.ids.get(code) {
            return id;
        }
        let id = store.create_node(label);
        store.set_column_property(id, "code", PropertyValue::String(code.to_string()));
        self.ids.insert(code.to_string(), id);
        id
    }

    fn name(&self, store: &mut GraphStore, code: &str, name: &str) {
        if let Some(&id) = self.ids.get(code) {
            store.set_column_property(id, "name", PropertyValue::String(name.to_string()));
        }
    }
}

/// OBO flat file — Gene Ontology, MONDO, HPO, ChEBI, and most OBO Foundry ontologies.
///
/// Reads `is_a:` and `relationship: part_of` as covering edges, and skips `is_obsolete`
/// terms: an obsolete term with dangling parents would add spurious roots and inflate the
/// width, which is exactly the measurement the probe is making.
fn load_obo(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    let mut current: Option<String> = None;
    let mut current_name: Option<String> = None;
    let mut obsolete = false;
    let mut pending: Vec<(String, String)> = Vec::new();
    let mut in_term = false;

    let mut flush = |cur: &Option<String>,
                     name: &Option<String>,
                     obs: bool,
                     pending: &mut Vec<(String, String)>,
                     store: &mut GraphStore,
                     interner: &mut Interner,
                     edges: &mut usize| {
        if let (Some(code), false) = (cur, obs) {
            let child = interner.get(store, &args.label, code);
            if let Some(n) = name {
                interner.name(store, code, n);
            }
            for (_, parent) in pending.iter() {
                let p = interner.get(store, &args.label, parent);
                if store.create_edge(child, p, args.edge_type.as_str()).is_ok() {
                    *edges += 1;
                }
            }
        }
        pending.clear();
    };

    for line in reader.lines().map_while(Result::ok) {
        let line = line.trim().to_string();
        if line == "[Term]" {
            flush(&current, &current_name, obsolete, &mut pending, store, &mut interner, &mut edges);
            current = None;
            current_name = None;
            obsolete = false;
            in_term = true;
            continue;
        }
        if line.starts_with('[') {
            // [Typedef] and friends: stop collecting until the next [Term]
            flush(&current, &current_name, obsolete, &mut pending, store, &mut interner, &mut edges);
            current = None;
            in_term = false;
            continue;
        }
        if !in_term {
            continue;
        }
        if let Some(v) = line.strip_prefix("id: ") {
            current = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("name: ") {
            current_name = Some(v.trim().to_string());
        } else if line.starts_with("is_obsolete: true") {
            obsolete = true;
        } else if let Some(v) = line.strip_prefix("is_a: ") {
            let parent = v.split_whitespace().next().unwrap_or("").to_string();
            if !parent.is_empty() {
                pending.push(("is_a".to_string(), parent));
            }
        } else if let Some(v) = line.strip_prefix("relationship: part_of ") {
            let parent = v.split_whitespace().next().unwrap_or("").to_string();
            if !parent.is_empty() {
                pending.push(("part_of".to_string(), parent));
            }
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    flush(&current, &current_name, obsolete, &mut pending, store, &mut interner, &mut edges);
    (interner.ids.len(), edges)
}

/// NCBI Taxonomy `nodes.dmp`: `tax_id | parent_tax_id | rank | ...`, pipe-tab delimited.
///
/// The root (tax_id 1) is its own parent in the dump; that self-loop is dropped, because
/// the poset validator would correctly reject it as a cycle.
fn load_taxdump(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    for line in reader.lines().map_while(Result::ok) {
        let cols: Vec<&str> = line.split("\t|").map(|c| c.trim()).collect();
        if cols.len() < 3 {
            continue;
        }
        let (tax, parent, rank) = (cols[0], cols[1], cols[2]);
        let child = interner.get(store, &args.label, tax);
        store.set_column_property(child, "rank", PropertyValue::String(rank.to_string()));
        if tax == parent {
            continue; // the dump makes the root its own parent
        }
        let p = interner.get(store, &args.label, parent);
        if store.create_edge(child, p, args.edge_type.as_str()).is_ok() {
            edges += 1;
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    (interner.ids.len(), edges)
}

/// GeoNames `hierarchy.txt`: `parentId \t childId \t type`.
fn load_geonames(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    for line in reader.lines().map_while(Result::ok) {
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 2 {
            continue;
        }
        // Only the administrative hierarchy is a subsumption order; GeoNames also carries
        // non-containment relation types in the same file.
        if let Some(kind) = cols.get(2) {
            if !kind.is_empty() && *kind != "ADM" {
                continue;
            }
        }
        let parent = interner.get(store, &args.label, cols[0].trim());
        let child = interner.get(store, &args.label, cols[1].trim());
        if store.create_edge(child, parent, args.edge_type.as_str()).is_ok() {
            edges += 1;
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    (interner.ids.len(), edges)
}

/// Codes whose ancestry is encoded in the identifier itself.
///
/// ATC is the clean case (`--cuts 1,3,4,5,7`): `A10BA02` ⊑ `A10BA` ⊑ `A10B` ⊑ `A10` ⊑ `A`.
/// MITRE ATT&CK sub-techniques work the same way with a dot (`T1548.001` ⊑ `T1548`) — pass
/// no cuts and the loader splits on the last `.` instead.
///
/// Input: one code per line, optionally `code,name`.
fn load_prefix(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    let mut cuts = args.cuts.clone();
    cuts.sort_unstable();

    for line in reader.lines().map_while(Result::ok) {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (code, name) = match line.split_once(',') {
            Some((c, n)) => (c.trim(), Some(n.trim())),
            None => (line, None),
        };
        let id = interner.get(store, &args.label, code);
        if let Some(n) = name {
            store.set_column_property(id, "name", PropertyValue::String(n.to_string()));
        }

        let parent_code: Option<String> = if cuts.is_empty() {
            code.rsplit_once('.').map(|(head, _)| head.to_string())
        } else {
            // the largest cut strictly shorter than this code
            cuts.iter()
                .rev()
                .find(|&&c| c < code.len())
                .map(|&c| code[..c].to_string())
        };
        if let Some(pc) = parent_code {
            if pc != code {
                let p = interner.get(store, &args.label, &pc);
                if store.create_edge(id, p, args.edge_type.as_str()).is_ok() {
                    edges += 1;
                }
            }
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    (interner.ids.len(), edges)
}

/// NLM MeSH tree file (`mtrees<year>.bin`): `Descriptor Name;TreeNumber`, one line per
/// tree position.
///
/// The hierarchy is carried by the **tree number**, not the descriptor: `C01.252.400` sits
/// under `C01.252` under `C01`. Modelling tree numbers as the nodes matters — a descriptor
/// may occupy several positions in the tree ("Breast Neoplasms" is both a breast disease
/// and a neoplasm), so a descriptor-to-descriptor graph is a poly-hierarchy that the probe
/// would likely decline. Tree numbers give a strict tree, and the descriptor name rides
/// along as a property, so an article annotated with a descriptor can still be rolled up
/// through every position that descriptor occupies.
fn load_mesh(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    for line in reader.lines().map_while(Result::ok) {
        let Some((name, tree)) = line.split_once(';') else { continue };
        let (name, tree) = (name.trim(), tree.trim());
        if tree.is_empty() {
            continue;
        }
        let id = interner.get(store, &args.label, tree);
        store.set_column_property(id, "name", PropertyValue::String(name.to_string()));
        // Depth is free here and makes level-wise roll-up queries expressible.
        store.set_column_property(
            id,
            "level",
            PropertyValue::Integer(tree.matches('.').count() as i64),
        );
        if let Some((parent, _)) = tree.rsplit_once('.') {
            let p = interner.get(store, &args.label, parent);
            if store.create_edge(id, p, args.edge_type.as_str()).is_ok() {
                edges += 1;
            }
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    (interner.ids.len(), edges)
}

/// Generic `child,parent` CSV — CWE's `ChildOf` table, ICD chapter maps, anything already
/// reduced to a covering relation. A header line starting with `child` is skipped.
fn load_edgelist(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let reader = open(&args.path);
    let mut interner = Interner::new();
    let mut edges = 0usize;
    for (i, line) in reader.lines().map_while(Result::ok).enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if i == 0 && line.to_lowercase().starts_with("child") {
            continue;
        }
        let Some((c, p)) = line.split_once(',') else { continue };
        let child = interner.get(store, &args.label, c.trim());
        let parent = interner.get(store, &args.label, p.trim());
        if store.create_edge(child, parent, args.edge_type.as_str()).is_ok() {
            edges += 1;
        }
        if args.limit.is_some_and(|l| edges >= l) {
            break;
        }
    }
    (interner.ids.len(), edges)
}

/// Generate the calendar dimension: `day ⊑ month ⊑ quarter ⊑ year`.
///
/// No download, because a calendar is a function rather than a dataset. This is the axis
/// TimescaleDB continuous aggregates cover and a graph engine normally does not — the
/// point of the paper being that it is the same poset as the other two.
fn load_calendar(store: &mut GraphStore, args: &Args) -> (usize, usize) {
    let mut nodes = 0usize;
    let mut edges = 0usize;
    let days_in = |y: i64, m: i64| -> i64 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                    29
                } else {
                    28
                }
            }
        }
    };
    let mk = |store: &mut GraphStore, label: &str, code: String| -> NodeId {
        let id = store.create_node(label);
        store.set_column_property(id, "code", PropertyValue::String(code));
        id
    };
    for y in args.from_year..=args.to_year {
        let year = mk(store, "Year", format!("{y}"));
        nodes += 1;
        for q in 1..=4i64 {
            let quarter = mk(store, "Quarter", format!("{y}-Q{q}"));
            nodes += 1;
            store.create_edge(quarter, year, args.edge_type.as_str()).unwrap();
            edges += 1;
            for mo in 0..3i64 {
                let m = (q - 1) * 3 + mo + 1;
                let month = mk(store, "Month", format!("{y}-{m:02}"));
                nodes += 1;
                store.create_edge(month, quarter, args.edge_type.as_str()).unwrap();
                edges += 1;
                for d in 1..=days_in(y, m) {
                    let day = mk(store, "Day", format!("{y}-{m:02}-{d:02}"));
                    nodes += 1;
                    store.create_edge(day, month, args.edge_type.as_str()).unwrap();
                    edges += 1;
                }
            }
        }
    }
    (nodes, edges)
}
