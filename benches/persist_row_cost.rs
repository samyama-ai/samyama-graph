//! What does `apply_mutations` cost per row, and does it matter? (#1109)
//!
//! #1109 proposes batching the existence probe, the per-row WAL mutex and the
//! per-row quota update. It also says, correctly, to measure first: the ingest
//! bottleneck is `&mut GraphStore` (#503), so a faster persist path may not move
//! end-to-end import time at all, and claiming a win without the baseline would
//! repeat the trap #1098 records for `LOAD PARQUET`.
//!
//! So this bench answers two questions in that order:
//!
//! 1. **What share of an import is persistence?** If it is small, the batching
//!    work in #1109 is bounded above by that share no matter how well it is done,
//!    and the issue should say so before anyone starts.
//! 2. **Within persistence, what do the parts #1109 names actually cost?** The
//!    existence probe, the serialize, the WAL append under its mutex, and the
//!    put — timed separately over the same rows.
//!
//! A per-row cost is reported as a median over rows rather than a mean, because
//! the storage engine's write path has occasional compaction spikes and a mean
//! reports those as if every row paid them.
//!
//! **Read the shares, not the microseconds, unless the host is quiet.** Measured
//! 2026-09-09: 3.61 us/row on an idle host and 8.5-10.8 us/row at load 13-15 on
//! the same tree, a 2.4x swing. The *proportions* moved far less over those same
//! runs -- the existence probe stayed 6.7-8.7% of the row -- so the question
//! #1109 actually asks survives a busy machine even though the absolute number
//! does not. The bench prints the load-sensitive figure too, but the decision
//! rests on the share.
//!
//! Usage: cargo bench --bench persist_row_cost -- [--rows N]

use std::time::Instant;

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::persistence::PersistenceManager;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(((v.len() - 1) as f64) * p).round() as usize]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = arg(&args, "--rows").and_then(|v| v.parse().ok()).unwrap_or(1_000_000);

    let dir = tempfile::tempdir().expect("tempdir");
    let pm = match PersistenceManager::new(dir.path()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP: cannot open a persistence manager here: {e}");
            return;
        }
    };
    let tenant = "default";

    eprintln!("rows: {rows}");

    // --- 1. build the graph in memory, which is the thing #1109 says dominates
    let mut store = GraphStore::new();
    let t_build = Instant::now();
    let mut ids = Vec::with_capacity(rows);
    for i in 0..rows {
        let mut props = PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("name".to_string(), PropertyValue::String(format!("row{i:08}")));
        props.insert("score".to_string(), PropertyValue::Float(i as f64 * 0.5));
        ids.push(store.create_node_with_properties(tenant, vec![Label::new("Row")], props));
    }
    let build_s = t_build.elapsed().as_secs_f64();

    // --- 2. persist them through the path #1109 is about
    let mutations: Vec<samyama::graph::event::Mutation> = ids
        .iter()
        .map(|id| samyama::graph::event::Mutation::NodeUpserted(*id))
        .collect();

    let t_persist = Instant::now();
    let written = pm.apply_mutations(tenant, &store, &mutations).expect("apply");
    let persist_s = t_persist.elapsed().as_secs_f64();
    assert_eq!(written, rows, "not every row was written");

    let total = build_s + persist_s;
    println!("\nwhere the time goes, {rows} rows");
    println!("{}", "-".repeat(64));
    println!("build in memory   {build_s:8.2} s   {:5.1}%", 100.0 * build_s / total);
    println!("apply_mutations   {persist_s:8.2} s   {:5.1}%", 100.0 * persist_s / total);
    println!("                  {:8.2} us/row persisted", persist_s * 1e6 / rows as f64);

    // --- 3. the parts #1109 names, timed over a fresh sample of the same rows
    //
    // A separate store and manager, because the rows above are now present and
    // the existence probe would take its "found" branch for all of them, which
    // is the cheaper one and would understate a bulk import of new ids.
    let dir2 = tempfile::tempdir().expect("tempdir2");
    let pm2 = PersistenceManager::new(dir2.path()).expect("pm2");
    let sample = rows.min(20_000);

    let mut probe_us = Vec::with_capacity(sample);
    let mut ser_us = Vec::with_capacity(sample);
    for &id in ids.iter().take(sample) {
        let node = store.node_materialized(id).expect("materialized");

        let t = Instant::now();
        let _ = pm2.storage().get_node(tenant, id.as_u64());
        probe_us.push(t.elapsed().as_secs_f64() * 1e6);

        let t = Instant::now();
        let _ = bincode::serialize(&node.properties);
        ser_us.push(t.elapsed().as_secs_f64() * 1e6);
    }

    // put_node, the storage engine's own write. Timed into a *fresh* store so
    // these are inserts rather than overwrites, which is what a bulk import does.
    let mut put_us = Vec::with_capacity(sample);
    for &id in ids.iter().take(sample) {
        let node = store.node_materialized(id).expect("materialized");
        let t = Instant::now();
        let _ = pm2.storage().put_node(tenant, &node);
        put_us.push(t.elapsed().as_secs_f64() * 1e6);
    }

    let probe_p50 = pct(&mut probe_us, 0.5);
    let ser_p50 = pct(&mut ser_us, 0.5);
    let put_p50 = pct(&mut put_us, 0.5);
    let per_row = persist_s * 1e6 / rows as f64;
    let rest = per_row - probe_p50 - ser_p50 - put_p50;

    println!("\nper-row parts (median of {sample}, us)");
    println!("{}", "-".repeat(64));
    println!("existence probe   {probe_p50:8.3}   {:5.1}% of the row",
             100.0 * probe_p50 / per_row);
    println!("serialize props   {ser_p50:8.3}   {:5.1}%", 100.0 * ser_p50 / per_row);
    println!("put_node          {put_p50:8.3}   {:5.1}%", 100.0 * put_p50 / per_row);
    println!("WAL append + rest {rest:8.3}   {:5.1}%  (by difference)",
             100.0 * rest / per_row);

    let probe_share = pct(&mut probe_us, 0.5) * rows as f64 / 1e6 / persist_s * 100.0;
    println!("\nThe existence probe is {probe_share:.1}% of apply_mutations, and \
              apply_mutations is {:.1}% of the import.", 100.0 * persist_s / total);
    println!("An upper bound on removing it entirely is therefore {:.2}% of the \
              import — before any of it is written.", probe_share * persist_s / total);
    println!("\n#1109 asks for this number before the work, not after.");
}
