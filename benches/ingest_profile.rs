//! Where ingestion time actually goes: reading, parsing, or mutating the store
//! (#503, PERF-14).
//!
//! Ingestion runs on exactly one core -- 99% of a single CPU while `PERF-14`
//! asks for ≥1M edges/s on sixteen. Before parallelising anything it is worth
//! knowing which part is the work, because the three parts have very different
//! parallelisation stories:
//!
//!   * **read** — trivially parallel, and probably not the bottleneck on a
//!     warm page cache;
//!   * **parse** — trivially parallel, no shared state;
//!   * **apply** — `create_node`/`create_edge` take `&mut GraphStore` and
//!     mutate shared vectors, so this is the part that resists.
//!
//! If parse dominates, "parse in parallel, apply serially" is most of the win
//! for a contained change. If apply dominates, that approach caps out early and
//! the store itself has to change.
//!
//!   cargo bench --bench ingest_profile
//!   cargo bench --bench ingest_profile -- --data-dir PATH

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use samyama::graph::GraphStore;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// Read every line of a file and discard it: the floor cost of touching the data.
fn read_only(path: &PathBuf) -> (usize, f64) {
    let t = Instant::now();
    let Ok(file) = File::open(path) else { return (0, 0.0) };
    let mut n = 0usize;
    let mut sink = 0usize;
    for line in BufReader::with_capacity(1 << 16, file).lines() {
        let Ok(line) = line else { continue };
        sink = sink.wrapping_add(line.len());
        n += 1;
    }
    std::hint::black_box(sink);
    (n, t.elapsed().as_secs_f64())
}

/// Read and split into fields, parsing the two endpoint ids -- everything an
/// edge load does except touching the store.
fn read_and_parse(path: &PathBuf) -> (usize, f64) {
    let t = Instant::now();
    let Ok(file) = File::open(path) else { return (0, 0.0) };
    let mut lines = BufReader::with_capacity(1 << 16, file).lines();
    let _header = lines.next();
    let mut n = 0usize;
    let mut sink = 0i64;
    for line in lines {
        let Ok(line) = line else { continue };
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() < 2 {
            continue;
        }
        // LDBC ids are sometimes float-formatted in relationship files
        // (`1236950581248.0`), which is why this is not a plain parse.
        let a = fields[0].split('.').next().and_then(|s| s.parse::<i64>().ok());
        let b = fields[1].split('.').next().and_then(|s| s.parse::<i64>().ok());
        if let (Some(a), Some(b)) = (a, b) {
            sink = sink.wrapping_add(a ^ b);
            n += 1;
        }
    }
    std::hint::black_box(sink);
    (n, t.elapsed().as_secs_f64())
}

fn main() {
    bench_setup::init();

    let args: Vec<String> = std::env::args().collect();
    let arg_total: Option<f64> = args
        .iter()
        .position(|a| a == "--total-load-secs")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok());
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from("data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter")
        });

    if !data_dir.exists() {
        println!("SKIP: LDBC dataset not present at {}", data_dir.display());
        println!("      Fetch it with scripts/download_ldbc_snb.sh, or pass --data-dir PATH.");
        println!("      Skipping rather than failing: the benchmark did not run, it did not break.");
        return;
    }

    // The largest relationship files, which dominate the edge phase.
    let candidates = [
        "dynamic/comment_hasTag_tag_0_0.csv",
        "dynamic/comment_hasCreator_person_0_0.csv",
        "dynamic/comment_isLocatedIn_place_0_0.csv",
        "dynamic/forum_hasMember_person_0_0.csv",
        "dynamic/person_likes_comment_0_0.csv",
    ];

    println!("Ingestion profile — where the time goes");
    println!("{}", "-".repeat(78));
    println!("{:<40} {:>10} {:>10} {:>10}", "file", "rows", "read s", "parse s");

    let mut tot_rows = 0usize;
    let mut tot_read = 0.0;
    let mut tot_parse = 0.0;
    for rel in &candidates {
        let p = data_dir.join(rel);
        if !p.exists() {
            continue;
        }
        let (rows, t_read) = read_only(&p);
        let (_, t_parse) = read_and_parse(&p);
        println!(
            "{:<40} {:>10} {:>10.2} {:>10.2}",
            rel.rsplit('/').next().unwrap_or(rel),
            rows,
            t_read,
            t_parse
        );
        tot_rows += rows;
        tot_read += t_read;
        tot_parse += t_parse;
    }

    if tot_rows == 0 {
        println!("no candidate files found under {}", data_dir.display());
        return;
    }

    println!("{}", "-".repeat(78));
    println!("{:<40} {:>10} {:>10.2} {:>10.2}", "TOTAL", tot_rows, tot_read, tot_parse);
    println!();

    // Deliberately NOT compared against a synthetic apply: measuring real files
    // against a synthetic insert loop compares two different workloads and
    // flatters whichever one happens to be cache-friendlier. What is defensible
    // is the absolute cost of read and parse against a real end-to-end load
    // time, which the caller supplies.
    let parse_only = (tot_parse - tot_read).max(0.0);
    println!("read  {:>6.2} s", tot_read);
    println!("parse {:>6.2} s   (parse cost above read)", parse_only);
    println!("both  {:>6.2} s   for {} rows across {} of the largest relationship files",
        tot_read + parse_only, tot_rows, candidates.len());
    println!();
    if let Some(total) = arg_total {
        let both = tot_read + parse_only;
        let share = 100.0 * both / total;
        println!("against a full load of {total:.1} s: read+parse is {share:.1}% of it");
        println!("so parallelising only those two caps out at {:.2}x (Amdahl)", 1.0 / (1.0 - share / 100.0));
    } else {
        println!("pass --total-load-secs N (a measured end-to-end load) to get the share");
        println!("and the Amdahl ceiling for parallelising read and parse alone.");
    }
    println!();
    println!("Read and parse have no shared state and parallelise freely. Everything");
    println!("else -- id-map lookups, node creation, property writes, and the adjacency");
    println!("mutation itself -- goes through &mut GraphStore and does not. That");
    println!("remainder is what decides whether a parse-in-parallel design is worth");
    println!("building.");
}
