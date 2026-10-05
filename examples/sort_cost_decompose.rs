//! Decompose what an `ORDER BY` costs per row, by term (#1819).
//!
//! #1819 measures `ORDER BY` on an integer key at 130 ns/row over 142 rows and
//! 153 ns/row over 920, and asks where that goes: extracting the key per row,
//! comparing keys, or moving records. `perf record` does not work on vm-1
//! (`kernel.perf_event_paranoid` is 4), so this is the A/B method instead: the
//! same work, staged, each stage adding one term, every stage timed in the
//! same process against real LDBC data.
//!
//! The stages replicate `SortOperator::execute_all`'s unbounded branch:
//! `SortKey`/`KeyPart` have the same layout and the comparator calls the same
//! `cypher_order_value`, so a stage difference is that term's cost and not a
//! cost of a different data structure. `SortOperator` is private, so
//! replicating it is the only way to time its parts from outside the crate;
//! the `whole-query` line pins the replica against the engine's own number.
//!
//! Three key shapes, because the shape decides the answer:
//!
//! * `int1` — `ORDER BY f.id`, which is what #1819 measured;
//! * `str1` — one borrowed string key;
//! * `str2` — `ORDER BY f.firstName, f.lastName`, which is **the ORDER BY the
//!   LDBC bench's IS3 actually runs** (`benches/ldbc_benchmark.rs:198`). IS3
//!   does not order by an integer, so the integer figure is a proxy for its
//!   sort, not a measurement of it.
//!
//! ```bash
//! cargo run --release --example sort_cost_decompose -- \
//!     --data-dir /home/vm-1/sgwork/ldbc-data/social_network-sf1-CsvBasic-LongDateFormatter
//! ```

/// The allocator the server and the LDBC benchmarks install (ADR-038). A probe
/// on a different allocator measures a different product (#1818).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

#[path = "../benches/common/bench_setup.rs"]
mod bench_setup;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::cypher_order_value;
use samyama::query::executor::{MutQueryExecutor, PropertyCursor, QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;
use std::cmp::Ordering;
use std::path::PathBuf;
use std::time::Instant;

/// `SortOperator`'s `KeyPart`, same layout.
enum KeyPart<'s> {
    Value(Value),
    Str(&'s str),
}

/// `SortOperator`'s `SortKey`, same layout.
enum SortKey<'s> {
    Inline([KeyPart<'s>; 2], usize),
    #[allow(dead_code)]
    Heap(Vec<KeyPart<'s>>),
}

impl<'s> SortKey<'s> {
    fn as_slice(&self) -> &[KeyPart<'s>] {
        match self {
            SortKey::Inline(parts, n) => &parts[..*n],
            SortKey::Heap(parts) => parts,
        }
    }
}

fn cmp_part(x: &KeyPart<'_>, y: &KeyPart<'_>) -> Ordering {
    match (x, y) {
        (KeyPart::Value(a), KeyPart::Value(b)) => cypher_order_value(a, b),
        (KeyPart::Str(a), KeyPart::Str(b)) => a.cmp(b),
        // The mixed arms cost the same as the homogeneous ones and are not
        // reached by any key shape this probe measures.
        _ => Ordering::Equal,
    }
}

/// `cmp_part` with one arm added: two integers compared directly.
///
/// For two `PropertyValue::Integer`s the generic chain is
/// `cypher_order_value` -> `cypher_order_rank` twice -> `property::cypher_order`
/// -> `rank` twice -> `PropertyValue::cmp` -> `rank` twice -> `a.cmp(b)`. The
/// arm reaches the last step directly, and is the same answer by construction.
fn cmp_part_fast(x: &KeyPart<'_>, y: &KeyPart<'_>) -> Ordering {
    match (x, y) {
        (
            KeyPart::Value(Value::Property(PropertyValue::Integer(a))),
            KeyPart::Value(Value::Property(PropertyValue::Integer(b))),
        ) => a.cmp(b),
        _ => cmp_part(x, y),
    }
}

fn cmp_keys_fast(a: &[KeyPart<'_>], b: &[KeyPart<'_>], ascending: &[bool]) -> Ordering {
    for (i, asc) in ascending.iter().enumerate() {
        let (Some(x), Some(y)) = (a.get(i), b.get(i)) else {
            continue;
        };
        let ord = cmp_part_fast(x, y);
        if ord != Ordering::Equal {
            return if *asc { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

fn cmp_keys(a: &[KeyPart<'_>], b: &[KeyPart<'_>], ascending: &[bool]) -> Ordering {
    for (i, asc) in ascending.iter().enumerate() {
        let (Some(x), Some(y)) = (a.get(i), b.get(i)) else {
            continue;
        };
        let ord = cmp_part(x, y);
        if ord != Ordering::Equal {
            return if *asc { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// Median of `runs` timings of `f`, in nanoseconds.
fn median_ns(runs: usize, mut f: impl FnMut()) -> f64 {
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_nanos() as f64);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// One anchor: `rows` input records, each binding `p` and `f`, which is the
/// shape the `Sort` under `MATCH (p)-[:KNOWS]-(f) ... ORDER BY f.x` sees.
struct Anchor {
    person: i64,
    rows: Vec<Record>,
}

/// One key shape to measure: the properties of `f` it orders by, and whether
/// its values are integers or strings.
struct Shape {
    name: &'static str,
    props: &'static [&'static str],
    integer: bool,
}

fn cursors(props: &[&str]) -> Vec<PropertyCursor> {
    props.iter().map(|p| PropertyCursor::new("f", *p)).collect()
}

fn extract_keys<'s>(
    rows: &[Record],
    store: &'s GraphStore,
    readers: &mut [PropertyCursor],
) -> Vec<SortKey<'s>> {
    let n = readers.len();
    let mut keys = Vec::with_capacity(rows.len());
    for record in rows {
        let mut inline = [KeyPart::Value(Value::Null), KeyPart::Value(Value::Null)];
        for (i, c) in readers.iter_mut().enumerate() {
            inline[i] = match c.read_str(record, store) {
                Some(s) => KeyPart::Str(s),
                None => KeyPart::Value(Value::Property(c.read(record, store))),
            };
        }
        keys.push(SortKey::Inline(inline, n));
    }
    keys
}

/// Sort a u32 permutation through `keys`, exactly as the unbounded branch
/// does: `cmp_keys` then the row index as the tie-break.
fn generic_order(keys: &[SortKey<'_>], asc: &[bool]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..keys.len() as u32).collect();
    order.sort_unstable_by(|x, y| {
        cmp_keys(keys[*x as usize].as_slice(), keys[*y as usize].as_slice(), asc)
            .then(x.cmp(y))
    });
    order
}

/// The same permutation sort with only the comparator changed, so the
/// difference from `generic_order` is the comparator and nothing else.
fn fast_order(keys: &[SortKey<'_>], asc: &[bool]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..keys.len() as u32).collect();
    order.sort_unstable_by(|x, y| {
        cmp_keys_fast(keys[*x as usize].as_slice(), keys[*y as usize].as_slice(), asc)
            .then(x.cmp(y))
    });
    order
}

/// The typed integer arm: `(key, row)` pairs, 16 bytes, compared with
/// `i64::cmp`. Two keys use `[i64; 2]`, 24 bytes.
fn typed_int_order(
    rows: &[Record],
    store: &GraphStore,
    readers: &mut [PropertyCursor],
    asc: &[bool],
) -> Option<Vec<u32>> {
    let n = readers.len();
    let mut pairs: Vec<([i64; 2], u32)> = Vec::with_capacity(rows.len());
    for (i, record) in rows.iter().enumerate() {
        let mut k = [0i64; 2];
        for (j, c) in readers.iter_mut().enumerate() {
            match c.read(record, store) {
                PropertyValue::Integer(v) => k[j] = v,
                _ => return None,
            }
        }
        pairs.push((k, i as u32));
    }
    pairs.sort_unstable_by(|a, b| {
        for (j, up) in asc.iter().enumerate().take(n) {
            let o = a.0[j].cmp(&b.0[j]);
            if o != Ordering::Equal {
                return if *up { o } else { o.reverse() };
            }
        }
        a.1.cmp(&b.1)
    });
    Some(pairs.into_iter().map(|(_, i)| i).collect())
}

/// The typed string arm: borrowed `&str` keys in a compact vector, 2 or 3
/// words each plus the row index, compared with `str::cmp`.
fn typed_str_order<'s>(
    rows: &[Record],
    store: &'s GraphStore,
    readers: &mut [PropertyCursor],
    asc: &[bool],
) -> Option<Vec<u32>> {
    let n = readers.len();
    let mut pairs: Vec<([&'s str; 2], u32)> = Vec::with_capacity(rows.len());
    for (i, record) in rows.iter().enumerate() {
        let mut k = [""; 2];
        for (j, c) in readers.iter_mut().enumerate() {
            k[j] = c.read_str(record, store)?;
        }
        pairs.push((k, i as u32));
    }
    pairs.sort_unstable_by(|a, b| {
        for (j, up) in asc.iter().enumerate().take(n) {
            let o = a.0[j].cmp(b.0[j]);
            if o != Ordering::Equal {
                return if *up { o } else { o.reverse() };
            }
        }
        a.1.cmp(&b.1)
    });
    Some(pairs.into_iter().map(|(_, i)| i).collect())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench_setup::init();
    let _calibration = bench_setup::report_calibration();
    println!("allocator: {}", samyama::allocator::NAME);
    println!(
        "size_of: Value {}  KeyPart {}  SortKey {}  Record {}  (SortKey, Record) {}  \
         ([i64;2],u32) {}  ([&str;2],u32) {}",
        std::mem::size_of::<Value>(),
        std::mem::size_of::<KeyPart<'_>>(),
        std::mem::size_of::<SortKey<'_>>(),
        std::mem::size_of::<Record>(),
        std::mem::size_of::<(SortKey<'_>, Record)>(),
        std::mem::size_of::<([i64; 2], u32)>(),
        std::mem::size_of::<([&str; 2], u32)>(),
    );

    let args: Vec<String> = std::env::args().collect();
    let one = |flag: &str| args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1));
    let data_dir = one("--data-dir").map(PathBuf::from).unwrap_or_else(|| {
        eprintln!("--data-dir <path> is required");
        std::process::exit(2)
    });
    let runs: usize = one("--runs").map(|s| s.parse()).transpose()?.unwrap_or(301);

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = Instant::now();
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());
    for label in ["Person", "Post", "Comment", "Forum", "Place", "Organisation", "Tag"] {
        let q = parse_query(&format!("CREATE INDEX ON :{label}(id)"))?;
        MutQueryExecutor::new(&mut graph, "default".to_string()).execute(&q)?;
    }

    // Anchors found rather than written down, so the probe does not depend on
    // an id that a different extract renumbers.
    let degrees = {
        let q = parse_query(
            "MATCH (p:Person)-[:KNOWS]-(f) RETURN p.id AS id, count(f) AS n ORDER BY n",
        )?;
        let out = QueryExecutor::new(&graph).execute(&q)?;
        let mut v: Vec<(i64, i64)> = Vec::new();
        for r in &out.records {
            if let (
                Some(Value::Property(PropertyValue::Integer(id))),
                Some(Value::Property(PropertyValue::Integer(n))),
            ) = (r.get("id"), r.get("n"))
            {
                v.push((*id, *n));
            }
        }
        v
    };

    let mut anchors: Vec<Anchor> = Vec::new();
    for target in [5i64, 142, 920] {
        let Some((person, _)) = degrees.iter().min_by_key(|(_, n)| (n - target).abs()).copied()
        else {
            continue;
        };
        let q = parse_query(&format!(
            "MATCH (p:Person {{id: {person}}})-[:KNOWS]-(f) RETURN f"
        ))?;
        let out = QueryExecutor::new(&graph).execute(&q)?;
        let mut rows = Vec::with_capacity(out.records.len());
        for r in &out.records {
            let Some(f) = r.get("f") else { continue };
            let mut rec = Record::new();
            rec.bind("p", Value::Property(PropertyValue::Integer(person)));
            rec.bind("f", f.clone());
            rows.push(rec);
        }
        anchors.push(Anchor { person, rows });
    }

    let shapes = [
        Shape { name: "int1  ORDER BY f.id", props: &["id"], integer: true },
        Shape { name: "str1  ORDER BY f.firstName", props: &["firstName"], integer: false },
        Shape {
            name: "str2  IS3's own ORDER BY",
            props: &["firstName", "lastName"],
            integer: false,
        },
    ];

    for a in &anchors {
        let n = a.rows.len();
        if n == 0 {
            continue;
        }
        let nf = n as f64;
        println!("\n=== person {} , {} rows ===", a.person, n);
        println!(
            "{:<30} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "shape", "extract", "+sort", "+cheapcmp", "+gather", "typed", "ns/cmp"
        );

        for s in &shapes {
            let asc: Vec<bool> = s.props.iter().map(|_| true).collect();

            // Stages 1 and 2 do not consume the records, so they borrow them
            // and the only baseline they need is cursor construction. A clone
            // of the input rows costs 340-430 ns/row on its own, which swamped
            // a 30 ns/row term when every stage paid it.
            let base = median_ns(runs, || {
                let mut r = cursors(s.props);
                std::hint::black_box(&mut r);
            });
            let extract = median_ns(runs, || {
                let mut r = cursors(s.props);
                let keys = extract_keys(&a.rows, &graph, &mut r);
                std::hint::black_box(&keys);
            });
            let sorted = median_ns(runs, || {
                let mut r = cursors(s.props);
                let keys = extract_keys(&a.rows, &graph, &mut r);
                let order = generic_order(&keys, &asc);
                std::hint::black_box((&keys, &order));
            });

            let sorted_fast = median_ns(runs, || {
                let mut r = cursors(s.props);
                let keys = extract_keys(&a.rows, &graph, &mut r);
                let order = fast_order(&keys, &asc);
                std::hint::black_box((&keys, &order));
            });

            // The gather does consume the records, so it is measured against a
            // clone-only baseline of its own.
            let clone_base = median_ns(runs, || {
                let rows = a.rows.clone();
                std::hint::black_box(&rows);
            });
            let gathered = median_ns(runs, || {
                let mut rows = a.rows.clone();
                let mut r = cursors(s.props);
                let keys = extract_keys(&rows, &graph, &mut r);
                let order = generic_order(&keys, &asc);
                let out: Vec<Record> = order
                    .iter()
                    .map(|&i| std::mem::take(&mut rows[i as usize]))
                    .collect();
                std::hint::black_box(&out);
            });

            // The typed arm: extract into a compact (key, row) vector and sort
            // that, in place of the 120-byte `SortKey` vector and the generic
            // comparator.
            let typed = median_ns(runs, || {
                let mut r = cursors(s.props);
                let order = if s.integer {
                    typed_int_order(&a.rows, &graph, &mut r, &asc)
                } else {
                    typed_str_order(&a.rows, &graph, &mut r, &asc)
                };
                std::hint::black_box(&order.expect("typed arm applies to this shape"));
            });

            let mut comparisons = 0u64;
            {
                let mut r = cursors(s.props);
                let keys = extract_keys(&a.rows, &graph, &mut r);
                let mut order: Vec<u32> = (0..keys.len() as u32).collect();
                order.sort_unstable_by(|x, y| {
                    comparisons += 1;
                    cmp_keys(
                        keys[*x as usize].as_slice(),
                        keys[*y as usize].as_slice(),
                        &asc,
                    )
                    .then(x.cmp(y))
                });
            }

            println!(
                "{:<30} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.2} {:>9.2}",
                s.name,
                (extract - base) / nf,
                (sorted - extract) / nf,
                (sorted_fast - extract) / nf,
                (gathered - clone_base - (sorted - base)) / nf,
                (typed - base) / nf,
                (sorted - extract) / comparisons as f64,
            );
        }
        println!("    (clone-only baseline {:.1} ns/row, excluded from every column above)",
            median_ns(runs, || {
                let rows = a.rows.clone();
                std::hint::black_box(&rows);
            }) / nf);

        // The engine's own numbers for the same anchor, interleaved, so the
        // replica above is pinned against the product rather than trusted.
        let order_bys = [
            ("int1", "f.id"),
            ("str1", "f.firstName"),
            ("str2", "f.firstName, f.lastName"),
        ];
        let plain = parse_query(&format!(
            "MATCH (p:Person {{id: {}}})-[:KNOWS]-(f) RETURN f.id, f.firstName, f.lastName",
            a.person
        ))?;
        let mut queries = Vec::new();
        for (name, by) in order_bys {
            queries.push((
                name,
                parse_query(&format!(
                    "MATCH (p:Person {{id: {}}})-[:KNOWS]-(f) \
                     RETURN f.id, f.firstName, f.lastName ORDER BY {by}",
                    a.person
                ))?,
            ));
        }
        let mut plain_t = Vec::new();
        let mut sorted_t: Vec<Vec<f64>> = queries.iter().map(|_| Vec::new()).collect();
        for _ in 0..runs.min(401) {
            let t = Instant::now();
            let r = QueryExecutor::new(&graph).execute(&plain)?;
            plain_t.push(t.elapsed().as_nanos() as f64);
            std::hint::black_box(r.records.len());
            for (i, (_, q)) in queries.iter().enumerate() {
                let t = Instant::now();
                let r = QueryExecutor::new(&graph).execute(q)?;
                sorted_t[i].push(t.elapsed().as_nanos() as f64);
                std::hint::black_box(r.records.len());
            }
        }
        let p = median(plain_t);
        println!("    whole-query, no ORDER BY: {:.2} us", p / 1000.0);
        for (i, (name, _)) in queries.iter().enumerate() {
            let m = median(std::mem::take(&mut sorted_t[i]));
            println!(
                "    whole-query {name}: {:.2} us, sort costs {:.2} us = {:.1} ns/row",
                m / 1000.0,
                (m - p) / 1000.0,
                (m - p) / nf
            );
        }
    }
    Ok(())
}
