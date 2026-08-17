//! Large-scale hierarchical roll-up over the **real MeSH tree**.
//!
//! The HIER corpus runs on a generated hierarchy, which is reproducible but leaves the
//! obvious question open: does the index still pay off on a real ontology at real fact
//! volume? This answers it with the ontology that matters most for the biomedical
//! federation — MeSH — and a literature-shaped fact table on top.
//!
//! ## Why MeSH, and why by tree number
//!
//! MeSH is the annotation vocabulary for all 66M PubMed articles, and in the mega-federation
//! (`samyama-graph-competitor-benchmarks/benchmarks/large-scale/`) `MeSHTerm` nodes are
//! **flat** — there is no hierarchy, so "articles about anything under Cardiovascular
//! Diseases" cannot be asked without enumerating descendants by hand.
//!
//! The hierarchy lives in the **tree number** (`C14.280.400` ⊑ `C14.280` ⊑ `C14`), not the
//! descriptor. That distinction is load-bearing: a descriptor may occupy several positions
//! at once — "Breast Neoplasms" is both a breast disease and a neoplasm — so a
//! descriptor-to-descriptor graph is a poly-hierarchy the structural probe would likely
//! decline, while tree numbers form a strict tree that gets the nested-set encoding.
//!
//! ```bash
//! cargo run --release --example mesh_scale_bench -- --mesh mtrees2025.bin --articles 2000000
//! cargo run --release --example mesh_scale_bench -- --mesh mtrees2025.bin --articles 500000 --export-csv /tmp/mesh-csv
//! ```

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::time::Instant;

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::index::hierarchy::{HierarchySpec, RollupOp};
use samyama::query::QueryEngine;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mesh_path = arg(&args, "--mesh").unwrap_or_else(|| {
        eprintln!("--mesh <mtrees.bin> is required (NLM MeSH tree file)");
        std::process::exit(2)
    });
    let n_articles: usize =
        arg(&args, "--articles").and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let terms_per_article: usize =
        arg(&args, "--terms").and_then(|v| v.parse().ok()).unwrap_or(5);
    let export = arg(&args, "--export-csv");
    let reps: usize = arg(&args, "--reps").and_then(|v| v.parse().ok()).unwrap_or(5);
    // Above this corpus size the unindexed fact scan takes minutes per query, so running
    // it at every scale would cost hours to re-measure something already established. The
    // indexed query still runs at every scale; the baseline is reported as not run rather
    // than quietly omitted.
    let baseline_max: usize = arg(&args, "--baseline-max-articles")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);

    // ---- load the real MeSH tree ------------------------------------------
    let t0 = Instant::now();
    let mut store = GraphStore::new();
    let file = std::fs::File::open(&mesh_path)
        .unwrap_or_else(|e| panic!("cannot open {mesh_path}: {e}"));
    let mut tree_ids: HashMap<String, NodeId> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Some((name, tree)) = line.split_once(';') else { continue };
        let (name, tree) = (name.trim().to_string(), tree.trim().to_string());
        if tree.is_empty() {
            continue;
        }
        let id = *tree_ids.entry(tree.clone()).or_insert_with(|| {
            let n = store.create_node("MeshTree");
            order.push(tree.clone());
            n
        });
        store.set_column_property(id, "code", PropertyValue::String(tree.clone()));
        store.set_column_property(id, "name", PropertyValue::String(name));
        store.set_column_property(
            id,
            "level",
            PropertyValue::Integer(tree.matches('.').count() as i64),
        );
    }
    // mtrees.bin starts at A01 — the single-letter categories ("C" = Diseases) are implied
    // but never listed, so top-level codes would otherwise be 16 disconnected roots. Adding
    // them makes the tree match MeSH as documented and gives the workload subtrees that
    // span three orders of magnitude instead of one.
    let categories: Vec<char> = {
        let mut c: Vec<char> = tree_ids
            .keys()
            .filter(|t| !t.contains('.'))
            .filter_map(|t| t.chars().next())
            .collect();
        c.sort_unstable();
        c.dedup();
        c
    };
    for cat in &categories {
        let code = cat.to_string();
        let id = *tree_ids.entry(code.clone()).or_insert_with(|| {
            let n = store.create_node("MeshTree");
            order.push(code.clone());
            n
        });
        store.set_column_property(id, "code", PropertyValue::String(code));
        store.set_column_property(id, "name", PropertyValue::String(format!("Category {cat}")));
        store.set_column_property(id, "level", PropertyValue::Integer(-1));
    }

    // parents, after every node exists
    let mut mesh_edges = 0usize;
    let all: Vec<String> = tree_ids.keys().cloned().collect();
    for tree in &all {
        let parent = match tree.rsplit_once('.') {
            Some((p, _)) => Some(p.to_string()),
            // "C14" hangs off category "C"; a bare category has no parent
            None if tree.len() > 1 => tree.chars().next().map(|c| c.to_string()),
            None => None,
        };
        if let Some(parent) = parent {
            if let (Some(&c), Some(&p)) = (tree_ids.get(tree), tree_ids.get(&parent)) {
                if store.create_edge(c, p, "IS_A").is_ok() {
                    mesh_edges += 1;
                }
            }
        }
    }
    let mesh_nodes = tree_ids.len();
    eprintln!(
        "[mesh] {mesh_nodes} tree numbers, {mesh_edges} IS_A edges in {:.2}s",
        t0.elapsed().as_secs_f64()
    );

    // ---- literature-shaped facts on top -----------------------------------
    // Deterministic annotation: a fixed multiplicative hash spreads articles over the tree
    // without an RNG, so the graph — and therefore every roll-up total — is identical on
    // every machine and every run.
    let t1 = Instant::now();
    let leaves: Vec<&String> = order.iter().collect();
    let mut fact_edges = 0usize;
    for a in 0..n_articles {
        let art = store.create_node("Article");
        store.set_column_property(art, "pmid", PropertyValue::Integer(a as i64 + 1));
        store.set_column_property(
            art,
            "citations",
            PropertyValue::Integer(((a as u64).wrapping_mul(2_654_435_761) >> 9 % 64) as i64 % 97),
        );
        for t in 0..terms_per_article {
            let h = (a as u64)
                .wrapping_mul(11_400_714_819_323_198_485)
                .wrapping_add((t as u64).wrapping_mul(1_442_695_040_888_963_407));
            let tree = leaves[(h >> 11) as usize % leaves.len()];
            let term = tree_ids[tree];
            if store.create_edge(art, term, "ANNOTATED_WITH").is_ok() {
                fact_edges += 1;
            }
        }
    }
    eprintln!(
        "[facts] {n_articles} articles, {fact_edges} ANNOTATED_WITH edges in {:.2}s",
        t1.elapsed().as_secs_f64()
    );
    eprintln!("[graph] {} nodes, {} edges", store.node_count(), mesh_edges + fact_edges);

    if let Some(dir) = &export {
        export_csv(&store, dir, mesh_nodes + n_articles, mesh_edges + fact_edges);
        return;
    }

    // ---- declare the hierarchy --------------------------------------------
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE INDEX ON :MeshTree(code)", &mut store, "default")
        .unwrap();
    let t2 = Instant::now();
    let mgr = std::sync::Arc::clone(&store.hierarchy_index);
    let info = mgr
        .create(
            &store,
            HierarchySpec::new("mesh", vec![samyama::graph::EdgeType::new("IS_A")]).with_measure(
                None,
                "level",
                vec![RollupOp::Sum, RollupOp::Count],
            ),
        )
        .expect("mesh hierarchy");
    eprintln!(
        "[index] {} — {} nodes, {:.2} B/node, build {:.0} ms",
        info.encoding.unwrap_or("declined"),
        info.nodes,
        info.structural_bytes as f64 / info.nodes.max(1) as f64,
        t2.elapsed().as_secs_f64() * 1000.0
    );

    // ---- the workload ------------------------------------------------------
    // Two questions, deliberately different in what they touch.
    //
    // TERM-LEVEL is a pure roll-up over the ontology: "how many MeSH terms sit under C?".
    // It is answered from the index and never reads the fact table, so its cost is
    // independent of how many articles exist — that is the property being demonstrated.
    //
    // ARTICLE-LEVEL is the question a literature search actually asks: "how many articles
    // are annotated with anything under Cardiovascular Diseases?". It has to reach the fact
    // table, so it is the one that scales with corpus size, and it is what the mega-
    // federation cannot ask today because its MeSHTerm nodes are flat.
    let roots = ["C", "C14", "C04", "C01", "D", "A", "E01", "C14.280", "G", "B"];
    println!();
    println!("TERM-LEVEL roll-up (index-resident; independent of corpus size)");
    println!("{:<10} {:>9} {:>12} {:>14} {:>10}", "subtree", "terms", "indexed ms", "traversal ms", "speedup");
    println!("{}", "-".repeat(60));
    let mut disagreements = 0usize;
    for root in roots {
        let indexed = format!(
            "MATCH (t:MeshTree {{code: \"{root}\"}}) RETURN hierarchy_rollup(t, \"count\") AS n"
        );
        // A label on the descendant side makes the planner decline the roll-up rewrite
        // (it filters the subtree the index would enumerate wholesale), so this really is
        // a variable-length traversal. Without the label the "baseline" is silently the
        // indexed plan and the comparison measures nothing.
        let traversal = format!(
            "MATCH (d:MeshTree)-[:IS_A*0..]->(r:MeshTree {{code: \"{root}\"}}) RETURN count(d) AS n"
        );
        let (ims, iv) = timed(&engine, &store, &indexed, reps);
        let (tms, tv) = timed(&engine, &store, &traversal, reps);
        let note = match (iv, tv) {
            (Some(a), Some(b)) if a == b => String::new(),
            (Some(a), Some(b)) => { disagreements += 1; format!("  <-- DISAGREE {a} vs {b}") }
            _ => "  <-- QUERY FAILED".to_string(),
        };
        println!(
            "{root:<10} {:>9} {ims:>12.4} {tms:>14.4} {:>9.1}x{note}",
            iv.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            if ims > 0.0 { tms / ims } else { 0.0 }
        );
    }

    println!();
    println!("ARTICLE-LEVEL count (reaches the fact table; scales with corpus size)");
    println!("{:<10} {:>11} {:>12} {:>14} {:>10}", "subtree", "articles", "indexed ms", "fact-scan ms", "speedup");
    println!("{}", "-".repeat(62));
    for root in ["C", "C14", "C01", "D", "C14.280"] {
        // Driven plan: enumerate the subtree from the index, walk ANNOTATED_WITH backwards.
        let indexed = format!(
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(t), (r:MeshTree {{code: \"{root}\"}}) \
             WHERE subsumes(t, r) RETURN count(a) AS n"
        );
        // What the same question costs without a hierarchy index: expand every article's
        // annotation up the tree until it either reaches the root or runs out.
        let scan = format!(
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(t:MeshTree)-[:IS_A*0..]->(r:MeshTree {{code: \"{root}\"}}) \
             RETURN count(a) AS n"
        );
        let (ims, iv) = timed(&engine, &store, &indexed, reps);
        let shown = iv.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        if n_articles > baseline_max {
            println!("{root:<10} {shown:>11} {ims:>12.4} {:>14} {:>10}", "not run", "-");
            continue;
        }
        // One rep for the baseline: it is minutes per query at this size and its variance
        // is irrelevant next to the ratio being measured.
        let (sms, sv) = timed(&engine, &store, &scan, 1);
        let note = match (iv, sv) {
            (Some(a), Some(b)) if a == b => String::new(),
            (Some(a), Some(b)) => { disagreements += 1; format!("  <-- DISAGREE {a} vs {b}") }
            _ => "  <-- QUERY FAILED".to_string(),
        };
        println!(
            "{root:<10} {shown:>11} {ims:>12.4} {sms:>14.4} {:>9.1}x{note}",
            if ims > 0.0 { sms / ims } else { 0.0 }
        );
    }
    println!("{}", "-".repeat(62));
    println!("{} disagreements", disagreements);
    if disagreements > 0 {
        std::process::exit(1);
    }
}

/// Median wall time and the scalar result.
///
/// The result is `Option` on purpose: a failed query and a query that returned a different
/// number are different events, and an earlier version of this collapsed both to `-1`,
/// which reported a timeout as though the two engines disagreed. A benchmark that cannot
/// tell "wrong" from "did not run" is worse than one that simply crashes.
fn timed(engine: &QueryEngine, store: &GraphStore, q: &str, reps: usize) -> (f64, Option<i64>) {
    let mut times = Vec::new();
    let mut val: Option<i64> = None;
    let mut failed = false;
    for _ in 0..reps.max(1) {
        let t = Instant::now();
        let r = engine.execute(q, store);
        times.push(t.elapsed().as_secs_f64() * 1000.0);
        match r {
            Ok(b) => {
                val = b.records.first().and_then(|rec| {
                    rec.values().next().and_then(|v| match v {
                        samyama::query::executor::record::Value::Property(
                            PropertyValue::Integer(i),
                        ) => Some(*i),
                        _ => None,
                    })
                });
            }
            Err(e) => {
                eprintln!("[query failed] {e}\n  {q}");
                failed = true;
                break;
            }
        }
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], if failed { None } else { val })
}

fn export_csv(store: &GraphStore, dir: &str, _n: usize, _e: usize) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut nodes = std::io::BufWriter::new(
        std::fs::File::create(format!("{dir}/nodes.csv")).expect("nodes.csv"),
    );
    writeln!(nodes, "id:ID,code,name,level:long,pmid:long,citations:long,:LABEL").unwrap();
    for node in store.all_nodes() {
        let i = node.id.as_u64() as usize;
        let g = |k: &str| match store.node_columns.get_property(i, k) {
            PropertyValue::String(s) => s.replace(',', " "),
            PropertyValue::Integer(v) => v.to_string(),
            _ => String::new(),
        };
        let label = node.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>().join(";");
        writeln!(
            nodes,
            "{},{},{},{},{},{},{}",
            node.id.as_u64(), g("code"), g("name"), g("level"), g("pmid"), g("citations"), label
        )
        .unwrap();
    }
    nodes.flush().unwrap();
    let mut rels = std::io::BufWriter::new(
        std::fs::File::create(format!("{dir}/rels.csv")).expect("rels.csv"),
    );
    writeln!(rels, ":START_ID,:END_ID,:TYPE").unwrap();
    let mut n = 0usize;
    for node in store.all_nodes() {
        for (_e, s, t, et) in store.get_outgoing_edge_targets_owned(node.id) {
            writeln!(rels, "{},{},{}", s.as_u64(), t.as_u64(), et.as_str()).unwrap();
            n += 1;
        }
    }
    rels.flush().unwrap();
    eprintln!("[export] wrote {dir}/nodes.csv and {dir}/rels.csv ({n} rels)");
}
