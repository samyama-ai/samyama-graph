//! Where the rows in a graph came from, and what may leave (TRUST-03, ML-09, EVAL-10).
//!
//! ```bash
//! cargo run --release --example provenance_report -- --snapshot graph.sgsnap
//! cargo run --release --example provenance_report -- --policy require-permission --json r.json
//! ```
//!
//! `--policy` is one of:
//!
//! | Policy | Emits |
//! |---|---|
//! | `count-only` (default) | everything, and counts what is unmarked or forbidden |
//! | `withhold-forbidden` | drops rows marked non-redistributable; **unmarked rows still leave** |
//! | `require-permission` | drops anything not explicitly marked redistributable |
//!
//! Only the third is a guarantee. Under `withhold-forbidden` a row nobody
//! labelled goes out, which is most rows in most graphs — the report says how
//! many, so the gap between the policy you chose and the guarantee you wanted
//! is a number rather than an assumption.
//!
//! The default emits everything on purpose: turning this on must not silently
//! change what an existing export produces.

use samyama::graph::GraphStore;
use samyama::provenance::{self, derivation_path, screen, ExportPolicy};

fn demo_graph() -> GraphStore {
    // Every state the model distinguishes, so the report exercises each branch
    // rather than reporting one clean case and proving nothing.
    let mut g = GraphStore::new();
    let engine = samyama::query::QueryEngine::new();
    let q = "CREATE (d:Derived {name: 'median_income'}) \
             CREATE (s1:Source {name: 'census', \
                __source_uri: 'https://census.example/2026', \
                __source_version: 'release-2026-03', \
                __retrieved_at: '2026-04-01T00:00:00Z', \
                __license: 'CC0-1.0', __redistributable: true}) \
             CREATE (s2:Source {name: 'panel-survey', \
                __source_uri: 'https://panel.example', \
                __source_version: 'v3', \
                __license: 'proprietary', __redistributable: false}) \
             CREATE (s3:Source {name: 'scraped-table'}) \
             CREATE (d)-[:DERIVED_FROM]->(s1) \
             CREATE (d)-[:DERIVED_FROM]->(s2) \
             CREATE (s2)-[:DERIVED_FROM]->(s3)";
    engine
        .execute_mut(q, &mut g, "default")
        .expect("demo graph");
    g
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    let policy = match flag("--policy") {
        None => ExportPolicy::CountOnly,
        Some(s) => match ExportPolicy::parse(&s) {
            Some(p) => p,
            None => {
                // Not "unrecognised, so use the default": a misspelt
                // `require-permision` that quietly became "emit everything" is
                // how a licence guarantee is lost to a typo.
                eprintln!(
                    "unknown policy '{s}'; use count-only, withhold-forbidden or \
                     require-permission"
                );
                std::process::exit(2);
            }
        },
    };

    let store = match flag("--snapshot") {
        Some(path) => {
            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("cannot open snapshot {path}: {e}");
                    std::process::exit(2);
                }
            };
            let mut s = GraphStore::new();
            if let Err(e) = samyama::snapshot::import_tenant(&mut s, file) {
                eprintln!("cannot read snapshot {path}: {e}");
                std::process::exit(2);
            }
            s
        }
        None => demo_graph(),
    };

    let all: Vec<_> = store.all_nodes().iter().map(|n| n.id).collect();
    let (allowed, report) = screen(&store, &all, policy);

    println!("Provenance over {} node(s)", report.nodes_considered);
    println!(
        "  marked redistributable      {}",
        report.marked_redistributable
    );
    println!(
        "  marked NOT redistributable  {}",
        report.marked_not_redistributable
    );
    println!("  unmarked (unknown)          {}", report.unmarked);
    println!("  reproducible (uri+version)  {}", report.reproducible);
    println!(
        "  licences seen               {}",
        if report.licenses.is_empty() {
            "none".to_string()
        } else {
            report.licenses.join(", ")
        }
    );
    println!(
        "\n  Under {policy:?}: {} of {} node(s) may be exported, {} withheld",
        allowed.len(),
        report.nodes_considered,
        report.withheld
    );

    if report.unmarked > 0 && policy != ExportPolicy::RequirePermission {
        println!(
            "\n  {} node(s) carry no redistribution marking, and this policy lets them \
             out.\n  Absence is not permission: `--policy require-permission` is the only \
             one\n  of the three that is a guarantee.",
            report.unmarked
        );
    }

    // The derivation chain for the node furthest from a source, which is the
    // one a reader most wants explained. `max_by_key` over the path length
    // rather than a hardcoded node, so this says something about a real
    // snapshot too.
    let deepest = all
        .iter()
        .map(|id| (derivation_path(&store, *id), *id))
        .max_by_key(|(p, _)| p.len());
    if let Some((path, id)) = deepest {
        if path.len() > 1 {
            println!(
                "\n  Derivation of node {} ({} step(s)):",
                id.as_u64(),
                path.len()
            );
            for step in &path {
                let p = &step.provenance;
                println!(
                    "    depth {} node {:<4} {:<22} {} {}",
                    step.depth,
                    step.node,
                    step.labels.join(","),
                    p.source_uri.clone().unwrap_or_else(|| "(no source)".into()),
                    p.license
                        .clone()
                        .map(|l| format!("[{l}]"))
                        .unwrap_or_else(|| "[no licence]".into()),
                );
            }
        }
    }

    if let Some(out) = flag("--json") {
        let deepest_path = all
            .iter()
            .map(|id| derivation_path(&store, *id))
            .max_by_key(|p| p.len())
            .unwrap_or_default();
        let payload = serde_json::json!({
            "policy": format!("{policy:?}"),
            "report": report,
            "exportable": allowed.len(),
            "withheld": report.withheld,
            "reserved_keys": provenance::KEYS,
            "deepest_derivation": deepest_path,
            "derivation_depth": deepest_path.iter().map(|d| d.depth).max().unwrap_or(0),
        });
        if let Err(e) = std::fs::write(&out, serde_json::to_string_pretty(&payload).unwrap()) {
            eprintln!("cannot write {out}: {e}");
            std::process::exit(2);
        }
        println!("\n  Report written to {out}");
    }
}
