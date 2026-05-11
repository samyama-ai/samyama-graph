//! CLI driver for the Paper-8 baseline benchmark suite.
//!
//! Usage:
//!   cargo run -p samyama-optimization --release --example run_baseline_suite -- \
//!       --out /tmp/p8 [--seeds 30] [--dim 30] [--pop 50] [--iters 500] \
//!       [--so-only|--mo-only]
//!
//! Writes:
//!   <out>/so_results.csv
//!   <out>/mo_results.csv
//!   <out>/manifest.json

use samyama_optimization::benchmarks::{
    moo_suite, run_mo_suite, run_so_suite, so_suite,
};
use samyama_optimization::benchmarks::runner::{
    mo_solver_names, so_solver_names, write_mo_csv, write_so_csv,
};
use samyama_optimization::common::SolverConfig;
use std::path::PathBuf;

#[derive(Debug)]
struct Args {
    out: PathBuf,
    seeds: usize,
    dim: usize,
    pop: usize,
    iters: usize,
    so: bool,
    mo: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self { out: PathBuf::from("/tmp/p8"), seeds: 5, dim: 30, pop: 50, iters: 200, so: true, mo: true }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--dim" => { a.dim = argv[i + 1].parse().unwrap(); i += 2; }
            "--pop" => { a.pop = argv[i + 1].parse().unwrap(); i += 2; }
            "--iters" => { a.iters = argv[i + 1].parse().unwrap(); i += 2; }
            "--so-only" => { a.mo = false; i += 1; }
            "--mo-only" => { a.so = false; i += 1; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();
    let cfg = SolverConfig { population_size: a.pop, max_iterations: a.iters };

    if a.so {
        let problems = so_suite(a.dim);
        let solvers = so_solver_names();
        let solver_refs: Vec<&str> = solvers.iter().copied().collect();
        eprintln!("[SO] {} solvers x {} problems x {} seeds = {} runs",
            solvers.len(), problems.len(), a.seeds,
            solvers.len() * problems.len() * a.seeds);
        let t = std::time::Instant::now();
        let records = run_so_suite(&solver_refs, &problems, &cfg, a.seeds);
        eprintln!("[SO] done in {:.1}s", t.elapsed().as_secs_f64());
        let path = a.out.join("so_results.csv");
        write_so_csv(&records, &path).unwrap();
        eprintln!("[SO] -> {}", path.display());
    }

    if a.mo {
        let problems = moo_suite(a.dim, a.dim, 3);
        let solvers = mo_solver_names();
        let solver_refs: Vec<&str> = solvers.iter().copied().collect();
        eprintln!("[MO] {} solvers x {} problems x {} seeds = {} runs",
            solvers.len(), problems.len(), a.seeds,
            solvers.len() * problems.len() * a.seeds);
        let t = std::time::Instant::now();
        let records = run_mo_suite(&solver_refs, &problems, &cfg, a.seeds);
        eprintln!("[MO] done in {:.1}s", t.elapsed().as_secs_f64());
        let path = a.out.join("mo_results.csv");
        write_mo_csv(&records, &path).unwrap();
        eprintln!("[MO] -> {}", path.display());
    }

    let git_sha = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"]).output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let manifest = format!(
        r#"{{
  "args": {{
    "out": "{}", "seeds": {}, "dim": {}, "pop": {}, "iters": {}, "so": {}, "mo": {}
  }},
  "git_sha": "{}",
  "timestamp_epoch": {},
  "host": "{}"
}}
"#,
        a.out.display(), a.seeds, a.dim, a.pop, a.iters, a.so, a.mo,
        git_sha, epoch, std::env::var("HOSTNAME").unwrap_or_default()
    );
    let mpath = a.out.join("manifest.json");
    std::fs::write(&mpath, manifest).unwrap();
    eprintln!("manifest -> {}", mpath.display());
}
