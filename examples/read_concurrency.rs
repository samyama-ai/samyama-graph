//! Read throughput against client count, over `POST /api/query` (#1482).
//!
//! The workload CH-PERF-CONC runs: 200,000 `:P` nodes on a chain, an index on
//! `:P(id)`, and four reads -- an indexed lookup, one hop, two hops, and a range
//! aggregation -- cycled per client. Run against a server that is already up:
//!
//! ```bash
//! cargo run --release -- --http-port 18080 &
//! cargo run --release --example read_concurrency -- --port 18080 --load
//! cargo run --release --example read_concurrency -- --port 18080 --clients 1,16,64 --seconds 10
//! ```
//!
//! A Rust client rather than the harness's Python threads: the issue's own
//! caveat is that 64 Python threads may be measuring the client. This one is
//! one async task per client on a small runtime, and it reports its own CPU
//! time per phase so a client-bound phase says so.
//!
//! `--only N` runs just the N-th read (0-3), which separates "the mix
//! saturates" from "one query in it does".

// Measure what ships: the server's allocator, not the system default (ADR-038).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const READS: [&str; 4] = [
    "MATCH (p:P {id:$k}) RETURN p.id",
    "MATCH (p:P {id:$k})-[:R]->(b) RETURN b.id",
    "MATCH (p:P {id:$k})-[:R]->()-[:R]->(c) RETURN count(c)",
    "MATCH (p:P) WHERE p.id > $k AND p.id < $k + 50 RETURN count(p), avg(p.id)",
];
const NODES: i64 = 200_000;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[i]
}

/// CPU seconds a process has used, from `ps` (user + system). `None` when
/// `ps` cannot answer, which drops the column rather than failing the run.
fn cpu_seconds(pid: u32) -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8(out.stdout).ok()?;
    let t = text.trim();
    // [[dd-]hh:]mm:ss.ss
    let (days, rest) = match t.split_once('-') {
        Some((d, r)) => (d.parse::<f64>().ok()?, r),
        None => (0.0, t),
    };
    let mut secs = 0.0;
    for part in rest.split(':') {
        secs = secs * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86400.0 + secs)
}

async fn post(client: &reqwest::Client, url: &str, query: &str, k: Option<i64>) -> Result<(), String> {
    let body = match k {
        Some(k) => serde_json::json!({ "query": query, "params": { "k": k }, "cache": false }),
        None => serde_json::json!({ "query": query, "cache": false }),
    };
    let r = client.post(url).json(&body).send().await.map_err(|e| e.to_string())?;
    let status = r.status();
    let text = r.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("{status}: {}", &text[..text.len().min(300)]));
    }
    Ok(())
}

async fn load(client: &reqwest::Client, url: &str) {
    for q in [
        "CREATE INDEX ON :P(id)".to_string(),
        format!("UNWIND range(1,{NODES}) AS i CREATE (:P {{id:i, v:i*1.5}})"),
        format!(
            "UNWIND range(1,{}) AS i MATCH (a:P {{id:i}}), (b:P {{id:i+1}}) CREATE (a)-[:R]->(b)",
            NODES - 1
        ),
    ] {
        let t = Instant::now();
        post(client, url, &q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        eprintln!("loaded in {:.1}s: {}", t.elapsed().as_secs_f64(), &q[..q.len().min(60)]);
    }
}

async fn phase(url: Arc<str>, clients: usize, seconds: f64, only: Option<usize>, server: Option<u32>) {
    let stop = Arc::new(AtomicBool::new(false));
    let me = std::process::id();
    let cpu0 = cpu_seconds(me);
    let srv0 = server.and_then(cpu_seconds);
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let url = url.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            // One connection per client, as the harness has.
            let client = reqwest::Client::builder()
                .pool_max_idle_per_host(1)
                .build()
                .unwrap();
            let mut lat = Vec::new();
            let mut errors = 0usize;
            let mut n = c;
            let mut k: i64 = 1 + (c as i64 * 7919) % (NODES - 100);
            while !stop.load(Ordering::Relaxed) {
                let q = READS[only.unwrap_or(n % READS.len())];
                let t = Instant::now();
                match post(&client, &url, q, Some(k)).await {
                    Ok(()) => lat.push(t.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => {
                        if errors == 0 {
                            eprintln!("client {c}: {e}");
                        }
                        errors += 1;
                    }
                }
                n += 1;
                k = 1 + (k + 997) % (NODES - 100);
            }
            (lat, errors)
        }));
    }
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
    stop.store(true, Ordering::Relaxed);
    let mut lat = Vec::new();
    let mut errors = 0;
    for t in tasks {
        let (l, e) = t.await.unwrap();
        lat.extend(l);
        errors += e;
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let cores = |a: Option<f64>, b: Option<f64>| match (a, b) {
        (Some(a), Some(b)) => format!("{:.2}", (b - a) / elapsed),
        _ => "-".into(),
    };
    let client_cpu = cores(cpu0, cpu_seconds(me));
    let server_cpu = cores(srv0, server.and_then(cpu_seconds));
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{clients:>3} clients  {:>8.0} qps  p50 {:>8.3} ms  p95 {:>8.3}  p99 {:>8.3}  n={:<7} err={errors}  cores: server {server_cpu} client {client_cpu}",
        lat.len() as f64 / elapsed,
        pct(&lat, 50.0),
        pct(&lat, 95.0),
        pct(&lat, 99.0),
        lat.len(),
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port = arg(&args, "--port").unwrap_or_else(|| "18080".into());
    let seconds: f64 = arg(&args, "--seconds").and_then(|v| v.parse().ok()).unwrap_or(10.0);
    let clients: Vec<usize> = arg(&args, "--clients")
        .unwrap_or_else(|| "1,16,64".into())
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect();
    let only: Option<usize> = arg(&args, "--only").and_then(|v| v.parse().ok());
    let client_threads: usize =
        arg(&args, "--client-threads").and_then(|v| v.parse().ok()).unwrap_or(4);
    let url: Arc<str> = format!("http://127.0.0.1:{port}/api/query").into();
    // The server's pid, to report the cores it used per phase.
    let server: Option<u32> = arg(&args, "--server-pid").and_then(|v| v.parse().ok());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(client_threads)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        if args.iter().any(|a| a == "--load") {
            let client = reqwest::Client::new();
            load(&client, &url).await;
            return;
        }
        if let Some(q) = only {
            println!("only read {q}: {}", READS[q]);
        }
        // Warm the server: the first queries build caches and the type index.
        phase(url.clone(), 4, 2.0, only, server).await;
        for &c in &clients {
            phase(url.clone(), c, seconds, only, server).await;
        }
    });
}
