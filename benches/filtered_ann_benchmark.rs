//! In-process synthetic filtered-ANN sweep (#1985). See docs/benchmarks/filtered-ann.md.
#[path = "ann_common/mod.rs"]
mod ann_common;
#[path = "filtered_ann/mod.rs"]
mod filtered_ann;
use ann_common::{percentile, recall_at_k, synthetic};
use filtered_ann::{oracle, predicate, Correlation};
use samyama::vector::index::Quantization;
use samyama::{
    graph::NodeId,
    vector::{DistanceMetric, HnswParams, VectorIndexManager},
};
use std::{io::Write, time::Instant};

fn positive(s: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .ok()
        .filter(|x| *x > 0)
        .ok_or_else(|| format!("expected positive integer, got {s}"))
}
fn run() -> Result<(), String> {
    let (mut n, mut dim, mut nq) = (10_000, 32, 100);
    let mut efs = vec![32, 128, 512];
    let mut output = None;
    let mut args = std::env::args().skip(1).filter(|x| x != "--bench");
    while let Some(flag) = args.next() {
        let v = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--n" => n = positive(&v)?,
            "--dim" => dim = positive(&v)?,
            "--queries" => nq = positive(&v)?,
            "--ef" => efs = v.split(',').map(positive).collect::<Result<_, _>>()?,
            "--csv" => output = Some(v),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if n < 10_000 || n > u32::MAX as usize {
        return Err("--n must be 10000..=u32::MAX, so 0.1% contains k=10".into());
    }
    for ef in &efs {
        samyama::vector::index::check_ef(*ef)?;
    }
    let path = output.ok_or("--csv FILE is required")?;
    let base = synthetic(n, dim, 1985);
    let queries = synthetic(nq, dim, 1986);
    let manager = VectorIndexManager::new();
    manager
        .create_index_with_params(
            None,
            "V",
            "v",
            dim,
            DistanceMetric::L2,
            Quantization::None,
            HnswParams::default(),
        )
        .map_err(|e| e.to_string())?;
    for i in 0..n {
        manager
            .add_vector("V", "v", NodeId::new(i as u64), &base.row(i).to_vec())
            .map_err(|e| e.to_string())?;
    }
    let index = manager.get_index("V", "v").ok_or("missing index")?;
    let index = index.read().map_err(|e| e.to_string())?;
    let mut csv = std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
    writeln!(csv, "dataset,n,dim,queries,seed,selectivity,correlation,strategy,ef,k,recall,qps,p50_ms,p99_ms,short_results,filter_violations").map_err(|e| e.to_string())?;
    for permille in [1, 10, 100, 500] {
        let count = n * permille / 1000;
        for correlation in [
            Correlation::Positive,
            Correlation::None,
            Correlation::Negative,
        ] {
            let masks: Vec<_> = (0..nq)
                .map(|i| predicate(&base, queries.row(i), count, correlation, 1985 + i as u64))
                .collect();
            let truth: Vec<_> = (0..nq)
                .map(|i| oracle(&base, queries.row(i), &masks[i], 10))
                .collect();
            for strategy in ["auto", "exact", "post"] {
                for &ef in &efs {
                    let mut recall = 0.0;
                    let (mut short, mut violations) = (0, 0);
                    let mut times = Vec::new();
                    let started = Instant::now();
                    for qi in 0..nq {
                        let mask = &masks[qi];
                        let keep = |id: NodeId| mask[id.as_u64() as usize];
                        let at = Instant::now();
                        let got = match strategy {
                            "auto" => manager.search_filtered_ef(
                                "V",
                                "v",
                                queries.row(qi),
                                10,
                                Some(ef),
                                keep,
                            ),
                            "exact" => index.search_exact_where(queries.row(qi), 10, &keep),
                            _ => index
                                .search_ef(queries.row(qi), ef.max(10).min(n), Some(ef))
                                .map(|r| {
                                    r.into_iter().filter(|(id, _)| keep(*id)).take(10).collect()
                                }),
                        }
                        .map_err(|e| e.to_string())?;
                        times.push(at.elapsed());
                        let ids: Vec<u32> = got.iter().map(|(id, _)| id.as_u64() as u32).collect();
                        short += usize::from(ids.len() < 10);
                        violations += ids.iter().filter(|i| !mask[**i as usize]).count();
                        recall += recall_at_k(&ids, &truth[qi], 10);
                    }
                    let elapsed = started.elapsed().as_secs_f64();
                    times.sort();
                    writeln!(csv, "synthetic-l2,{n},{dim},{nq},1985,{},{},{strategy},{ef},10,{},{},{},{},{short},{violations}", count as f64/n as f64, correlation.name(), recall/nq as f64, nq as f64/elapsed, percentile(&times,50.0).as_secs_f64()*1000.0, percentile(&times,99.0).as_secs_f64()*1000.0).map_err(|e| e.to_string())?;
                }
            }
        }
    }
    csv.flush().map_err(|e| e.to_string())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(2);
    }
}
