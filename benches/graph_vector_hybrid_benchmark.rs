//! Graph + vector hybrid benchmark (#1987): GraphRAG-shaped queries that
//! combine similarity, traversal and predicates in one Cypher statement.
//! See docs/benchmarks/graph-vector-hybrid.md.
//!
//! ```sh
//! cargo bench --bench graph_vector_hybrid_benchmark                       # synthetic, default sizes
//! cargo bench --bench graph_vector_hybrid_benchmark -- --papers 50000 --people 100000 --csv out.csv
//! cargo bench --bench graph_vector_hybrid_benchmark -- --snapshot vectors.sgsnap --embedding-property embedding
//! cargo bench --bench graph_vector_hybrid_benchmark -- --export data/hybrid   # for run_competitors.py
//! ```
//!
//! With no arguments it runs the synthetic graph at its default size, so the
//! nightly sweep (`scripts/verify-sweep.sh`, which passes none) measures
//! something rather than skipping.

#[path = "graph_vector_hybrid/mod.rs"]
mod graph_vector_hybrid;

use graph_vector_hybrid as hy;
use samyama::graph::GraphStore;
use samyama::query::{BoundParams, QueryEngine};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct Args {
    cfg: hy::Config,
    queries: usize,
    snapshot: Option<PathBuf>,
    embedding: String,
    export: Option<PathBuf>,
    csv: Option<PathBuf>,
}

fn positive(flag: &str, s: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .ok()
        .filter(|x| *x > 0)
        .ok_or_else(|| format!("{flag}: expected a positive integer, got {s}"))
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        cfg: hy::Config::default(),
        queries: 200,
        snapshot: None,
        embedding: "embedding".to_string(),
        export: None,
        csv: None,
    };
    let mut it = std::env::args().skip(1).filter(|x| x != "--bench");
    while let Some(flag) = it.next() {
        let v = it
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        let f = flag.as_str();
        match f {
            "--papers" => a.cfg.papers = positive(f, &v)?,
            "--people" => a.cfg.people = positive(f, &v)?,
            "--methods" => a.cfg.methods = v.parse().map_err(|_| format!("{f}: {v}"))?,
            "--problems" => a.cfg.problems = v.parse().map_err(|_| format!("{f}: {v}"))?,
            "--topics" => a.cfg.topics = positive(f, &v)?,
            "--dim" => a.cfg.dim = positive(f, &v)?,
            "--cites" => a.cfg.cites = v.parse().map_err(|_| format!("{f}: {v}"))?,
            "--seed" => a.cfg.seed = v.parse().map_err(|_| format!("{f}: {v}"))?,
            "--queries" => a.queries = positive(f, &v)?,
            "--snapshot" => a.snapshot = Some(v.into()),
            "--embedding-property" => a.embedding = v,
            "--export" => a.export = Some(v.into()),
            "--csv" => a.csv = Some(v.into()),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    Ok(a)
}

/// One template's measurement.
struct Row {
    template: &'static str,
    queries: usize,
    recall: f64,
    exact: usize,
    qps: f64,
    p50: Duration,
    p99: Duration,
    mean_rows: f64,
}

/// Run every instance once, after a short untimed warm-up, and score each
/// answer against its reference. A query error aborts the run: an error
/// counted as an empty answer would look like a slow, wrong engine rather
/// than a broken benchmark.
fn measure(
    template: &'static str,
    text: &str,
    engine: &QueryEngine,
    store: &GraphStore,
    instances: &[(BoundParams, Vec<u32>)],
) -> Result<Row, String> {
    for (params, _) in instances.iter().take(10) {
        hy::run_template(engine, store, text, params).map_err(|e| format!("{template}: {e}"))?;
    }
    let mut times = Vec::with_capacity(instances.len());
    let (mut recall, mut exact, mut rows) = (0.0, 0, 0usize);
    let started = Instant::now();
    for (params, want) in instances {
        let at = Instant::now();
        let got = hy::run_template(engine, store, text, params)
            .map_err(|e| format!("{template}: {e}"))?;
        times.push(at.elapsed());
        let (r, same) = hy::compare(&got, want);
        recall += r;
        exact += usize::from(same);
        rows += got.len();
    }
    let elapsed = started.elapsed().as_secs_f64();
    times.sort();
    let n = instances.len().max(1) as f64;
    Ok(Row {
        template,
        queries: instances.len(),
        recall: recall / n,
        exact,
        qps: instances.len() as f64 / elapsed.max(f64::MIN_POSITIVE),
        p50: hy::percentile(&times, 50.0),
        p99: hy::percentile(&times, 99.0),
        mean_rows: rows as f64 / n,
    })
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let started = Instant::now();
    let (g, source) = match &args.snapshot {
        Some(path) => {
            let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let mut store = GraphStore::new();
            samyama::snapshot::import_tenant(&mut store, std::io::BufReader::new(file))
                .map_err(|e| format!("{}: {e}", path.display()))?;
            (
                hy::from_store(&store, &args.embedding)?,
                path.display().to_string(),
            )
        }
        None => (
            hy::generate(&args.cfg)?,
            format!("synthetic(seed={})", args.cfg.seed),
        ),
    };
    let generated = started.elapsed();
    println!(
        "graph: {source}: {} papers, {} people, {} methods, {} problems, {} edges, dim {} ({:.1} s)",
        g.papers.len(),
        g.people,
        g.methods.len(),
        g.problems.len(),
        g.edge_count(),
        g.dim(),
        generated.as_secs_f64()
    );

    let work = hy::workload(&g, args.queries, args.cfg.seed ^ 0x51);
    let started = Instant::now();
    let reference = hy::Reference::new(&g);
    let expand: Vec<_> = work
        .expand
        .iter()
        .map(|q| (hy::params_expand(q), reference.seeded_expansion(q)))
        .collect();
    let filtered: Vec<_> = work
        .filtered
        .iter()
        .map(|(p, q)| {
            (
                hy::params_filtered(*p, q),
                reference.traversal_filtered(*p, q),
            )
        })
        .collect();
    let join: Vec<_> = work
        .join
        .iter()
        .map(|m| (hy::params_join(*m), reference.vector_join(*m)))
        .collect();
    println!(
        "reference answers: {:.1} s (brute force, untimed)",
        started.elapsed().as_secs_f64()
    );

    if let Some(dir) = &args.export {
        hy::export(dir, &g, &work, &reference)?;
        println!("exported to {}", dir.display());
    }

    let started = Instant::now();
    let store = hy::load_store(&g)?;
    println!(
        "load + index build: {:.1} s",
        started.elapsed().as_secs_f64()
    );

    // No result cache: each instance is a fresh query, as it is on Neo4j.
    let engine = QueryEngine::new().with_result_cache_budget(0);
    let mut rows = vec![
        measure(
            "seeded_expansion",
            hy::T1_SEEDED_EXPANSION,
            &engine,
            &store,
            &expand,
        )?,
        measure(
            "traversal_filtered",
            hy::T2_TRAVERSAL_FILTERED,
            &engine,
            &store,
            &filtered,
        )?,
    ];
    if g.has_join() {
        rows.push(measure(
            "vector_join",
            hy::T3_VECTOR_JOIN,
            &engine,
            &store,
            &join,
        )?);
    } else {
        println!("vector_join: skipped, the graph has no Method/Problem embeddings");
    }
    println!("hybrid_text: not run, needs db.hybrid.search (#1279, PR #2284)");

    println!(
        "\n{:<20} {:>7} {:>8} {:>8} {:>9} {:>9} {:>9} {:>6}",
        "template", "queries", "recall", "exact", "qps", "p50_ms", "p99_ms", "rows"
    );
    for r in &rows {
        println!(
            "{:<20} {:>7} {:>8.4} {:>8} {:>9.1} {:>9.3} {:>9.3} {:>6.1}",
            r.template,
            r.queries,
            r.recall,
            format!("{}/{}", r.exact, r.queries),
            r.qps,
            ms(r.p50),
            ms(r.p99),
            r.mean_rows
        );
    }
    if let Some(path) = &args.csv {
        let err = |e: std::io::Error| format!("{}: {e}", path.display());
        let mut w = std::io::BufWriter::new(std::fs::File::create(path).map_err(err)?);
        writeln!(w, "system,source,papers,people,dim,template,queries,recall,exact,qps,p50_ms,p99_ms,mean_rows")
            .map_err(err)?;
        for r in &rows {
            writeln!(
                w,
                "samyama,{source},{},{},{},{},{},{},{},{},{},{},{}",
                g.papers.len(),
                g.people,
                g.dim(),
                r.template,
                r.queries,
                r.recall,
                r.exact,
                r.qps,
                ms(r.p50),
                ms(r.p99),
                r.mean_rows
            )
            .map_err(err)?;
        }
        w.flush().map_err(err)?;
    }
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(2);
    }
}
