//! What could materializing results inside a `.sgsnap` actually save? (#1158)
//!
//! #1158 is deferred and says so: "Once [the runtime result cache] exists, we
//! will have the number that decides whether this is worth building: if the
//! warm cache already gets the speedup, materializing into the file buys only
//! cold-start."
//!
//! The cache landed in #1153. This is that number.
//!
//! Time to first answer, from a cold process, is `restore + execute`.
//! Materialized results can only remove the `execute` part -- the bytes still
//! have to be read and the graph still has to be built before anything can be
//! answered. So the question is what share of time-to-first-answer execution
//! actually is. If it is small, materializing buys close to nothing while
//! costing a format change, a size cap, an epoch binding, and a re-verification
//! step in the release path.
//!
//! Usage: cargo bench --bench cold_start_share -- --snapshot <file.sgsnap>

use std::path::PathBuf;
use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let snap = PathBuf::from(arg(&args, "--snapshot").unwrap_or_else(|| {
        format!("{}/sgwork/dx01-cold/dbms-research.sgsnap",
                std::env::var("HOME").unwrap_or_default())
    }));
    if !snap.exists() {
        eprintln!("SKIP: no snapshot at {}", snap.display());
        return;
    }
    let bytes = std::fs::metadata(&snap).map(|m| m.len()).unwrap_or(0);

    let t = Instant::now();
    let mut store = GraphStore::new();
    let file = std::fs::File::open(&snap).expect("open");
    samyama::snapshot::import_tenant(&mut store, file).expect("import");
    let restore_s = t.elapsed().as_secs_f64();

    let labels = store.all_labels();
    let label = labels.first().map(|l| (*l).clone())
        .unwrap_or_else(|| samyama::Label::new("Node"));
    let l = label.as_str().to_string();

    // A range of query costs, because the share is entirely a function of it.
    // A bench that asked only a cheap query would report a small share and read
    // as an argument against #1158; one that asked only an expensive query
    // would read as an argument for it. Both would be the same mistake.
    let queries: Vec<(&str, String)> = vec![
        ("count aggregate", format!("MATCH (n:{l}) RETURN count(n) AS n")),
        ("scan + project", format!("MATCH (n:{l}) RETURN n LIMIT 1000")),
        ("filtered scan", format!("MATCH (n:{l}) WHERE n.name IS NOT NULL RETURN count(n) AS n")),
        ("two-hop", format!("MATCH (a:{l})-[]->()-[]->(c) RETURN count(c) AS n")),
    ];

    let engine = QueryEngine::new();
    println!("\nsnapshot        {} ({:.1} MB)", snap.display(), bytes as f64 / 1048576.0);
    println!("nodes / edges   {} / {}", store.node_count(), store.edge_count());
    println!("restore         {restore_s:.3} s  (paid once, before any answer)\n");
    println!("{:<18} {:>12} {:>12} {:>16}", "query", "execute s", "warm s", "exec share of TTFA");
    println!("{}", "-".repeat(62));

    for (name, q) in &queries {
        let t = Instant::now();
        let cold = match engine.execute(q, &store) {
            Ok(_) => t.elapsed().as_secs_f64(),
            Err(e) => { println!("{name:<18} failed: {e}"); continue; }
        };
        let _ = engine.execute_cached(q, &store);
        let t = Instant::now();
        let (_, hit) = engine.execute_cached(q, &store).expect("warm");
        let warm = t.elapsed().as_secs_f64();
        assert!(hit, "{name}: warm call missed");
        let share = cold / (restore_s + cold) * 100.0;
        println!("{name:<18} {cold:>12.6} {warm:>12.6} {share:>15.3}%");
    }

    println!("\nRestore is paid before any answer and materialized results cannot remove\n\
              it. So the share above is the ceiling on what #1158 could save, per query.\n\n\
              Do not average these. Measured on two snapshots the share moves in opposite\n\
              directions with size, depending on the query:\n\n\
                dbms-research   12.2 MB, 18.7k nodes    restore   0.266 s\n\
                clinical-trials  711 MB, 7.78M nodes    restore 106.320 s\n\n\
              Restore grew 400x for 415x the nodes -- roughly linear in node count, not in\n\
              bytes, since the file grew only 58x. A cheap query stays flat so its share\n\
              collapses: the count aggregate went 4.63% to 0.04%. An expensive query grows\n\
              faster than restore does: the two-hop went 0.36% to 34.4%, at 56.9 s of\n\
              execution against 106 s of restore.\n\n\
              So the answer for #1158 is bimodal, not small. Materializing buys nothing for\n\
              cheap catalog queries and a great deal for expensive ones, and which of those\n\
              a KG ships is a property of its catalog, not of the engine.");
}
