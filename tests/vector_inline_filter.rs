//! The WHERE predicate inside the HNSW walk (#1999).
//!
//! Post-filtering takes the k nearest and drops the ones that fail, so a
//! selective predicate comes back short; widening repeats the search until
//! k pass. The in-filter strategy hands the predicate to the walk itself,
//! together with the liveness test, so the beam keeps only passing, current
//! entries and one search returns k of them. These tests check that it
//! returns k rows, agrees with an exact filtered search, never returns a dead
//! entry, works at f16, and that the planner chooses it and EXPLAIN says so.
//! No test here measures time.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::index::Quantization;
use samyama::vector::strategy::{FilterPlan, FilterStrategy};
use samyama::vector::{DistanceMetric, HnswParams, VectorIndex};

/// Deterministic points in `[-1, 1)^dims`.
fn points(n: usize, dims: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ seed;
    (0..n)
        .map(|_| {
            (0..dims)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect()
        })
        .collect()
}

fn index(data: &[Vec<f32>], quantization: Quantization) -> VectorIndex {
    let mut index = VectorIndex::with_params(
        data[0].len(),
        DistanceMetric::L2,
        quantization,
        HnswParams::default(),
    );
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).unwrap();
    }
    index
}

/// Share of `want` found in `got`.
fn overlap(got: &[(NodeId, f32)], want: &[(NodeId, f32)]) -> (usize, usize) {
    let hits = got
        .iter()
        .filter(|(id, _)| want.iter().any(|(w, _)| w == id))
        .count();
    (hits, want.len())
}

/// The in-filter search against the exact filtered search: k rows whenever
/// k pass, every row passes, nearest first, the distances the exact search
/// reports for the same node, and recall@k of at least 0.9.
fn check_against_exact(
    index: &VectorIndex,
    queries: &[Vec<f32>],
    k: usize,
    per_cent: u64,
    alive: impl Fn(u64) -> bool,
) {
    let pass = |id: NodeId| id.as_u64() % 100 < per_cent && alive(id.as_u64());
    let (mut hits, mut total) = (0, 0);
    for q in queries {
        let got = index.search_inline_where(q, k, None, &pass).unwrap();
        let want = index.search_exact_where(q, k, &pass).unwrap();
        assert_eq!(got.len(), want.len(), "{per_cent}%: short result");
        assert!(
            got.iter().all(|(id, _)| pass(*id)),
            "{per_cent}%: a row fails the predicate"
        );
        assert!(
            got.windows(2).all(|w| w[0].1 <= w[1].1),
            "{per_cent}%: not nearest first: {got:?}"
        );
        for (id, d) in &got {
            if let Some((_, e)) = want.iter().find(|(w, _)| w == id) {
                assert!(
                    (d - e).abs() <= 1e-5 * e.abs().max(1.0),
                    "{per_cent}%: {id:?} at {d}, exact {e}"
                );
            }
        }
        let (h, t) = overlap(&got, &want);
        hits += h;
        total += t;
    }
    let recall = hits as f64 / total.max(1) as f64;
    assert!(recall >= 0.9, "{per_cent}%: recall@{k} {recall:.3}");
}

#[test]
fn the_cost_model_routes_moderate_selectivity_to_the_in_filter_search() {
    // k = 50 on a million 128-d vectors: a 20% predicate makes an exact scan
    // read and score 200k vectors, and widening repeat the search four
    // times; one walk that keeps only passing nodes is the cheapest.
    let moderate = FilterPlan::estimate(1_000_000, 128, 50, 100, 16, 0.2);
    assert_eq!(
        moderate.strategy,
        FilterStrategy::Inline,
        "{}",
        moderate.describe()
    );
    assert!(moderate.inline_work < moderate.widen_work.min(moderate.exact_work));
    assert!(
        moderate.describe().contains("strategy=in-filter"),
        "{}",
        moderate.describe()
    );
    assert!(
        moderate.describe().contains("inline_work="),
        "{}",
        moderate.describe()
    );

    // The ends are unchanged: a rare predicate is scanned exactly, and one
    // that passes everything is a plain search, which widening is.
    let rare = FilterPlan::estimate(1_000_000, 128, 50, 100, 16, 0.001);
    assert_eq!(rare.strategy, FilterStrategy::Exact, "{}", rare.describe());
    let all = FilterPlan::estimate(1_000_000, 128, 50, 100, 16, 1.0);
    assert_eq!(all.strategy, FilterStrategy::Widen, "{}", all.describe());

    // Under a hierarchy root the in-filter search is vector-first.
    let scoped = FilterPlan::estimate_enumerated(1_000_000, 128, 50, 100, 16, 200_000, 1.0);
    assert_eq!(
        scoped.strategy,
        FilterStrategy::Inline,
        "{}",
        scoped.describe()
    );
    assert!(
        scoped.describe().contains("order=vector-first"),
        "{}",
        scoped.describe()
    );

    for s in [0.0, 1e-9, 0.5, 1.0, f64::NAN] {
        for n in [0, 1, 10, 1_000_000] {
            let p = FilterPlan::estimate(n, 16, 10, 64, 16, s);
            assert!(
                p.inline_work.is_finite() && p.widen_work.is_finite(),
                "{n} {s}"
            );
        }
    }
}

#[test]
fn selective_predicates_return_k_where_post_filtering_comes_back_short() {
    let data = points(3_000, 8, 1);
    let index = index(&data, Quantization::None);
    let queries = points(20, 8, 2);
    let k = 10;
    let pass = |id: NodeId| id.as_u64().is_multiple_of(100);
    // Post-filtering the unfiltered top-k: a 1% predicate keeps almost none.
    let post: usize = queries
        .iter()
        .map(|q| {
            index
                .search(q, k)
                .unwrap()
                .into_iter()
                .filter(|(id, _)| pass(*id))
                .count()
        })
        .sum();
    assert!(
        post < k * queries.len(),
        "post-filtering was not short: {post}"
    );
    for q in &queries {
        let got = index.search_inline_where(q, k, None, &pass).unwrap();
        assert_eq!(got.len(), k);
        assert!(got.iter().all(|(id, _)| pass(*id)));
    }
}

#[test]
fn recall_against_the_exact_filtered_search() {
    let data = points(3_000, 8, 3);
    let index = index(&data, Quantization::None);
    let queries = points(20, 8, 4);
    for per_cent in [1, 5, 20, 100] {
        check_against_exact(&index, &queries, 10, per_cent, |_| true);
    }
    // Fewer passing nodes than k: all of them.
    let three = |id: NodeId| [7u64, 1_500, 2_999].contains(&id.as_u64());
    let got = index
        .search_inline_where(&queries[0], 10, None, &three)
        .unwrap();
    let mut ids: Vec<u64> = got.iter().map(|(id, _)| id.as_u64()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![7, 1_500, 2_999]);
    // None at all: nothing.
    assert!(index
        .search_inline_where(&queries[0], 10, None, &|_| false)
        .unwrap()
        .is_empty());
}

#[test]
fn the_f16_index_searches_with_the_predicate_too() {
    let data = points(3_000, 8, 5);
    let index = index(&data, Quantization::Fp16);
    assert_eq!(index.quantization(), Quantization::Fp16);
    let queries = points(20, 8, 6);
    for per_cent in [1, 5, 20] {
        check_against_exact(&index, &queries, 10, per_cent, |_| true);
    }
}

#[test]
fn dead_entries_are_still_excluded() {
    let data = points(3_000, 8, 7);
    let mut index = index(&data, Quantization::None);
    // Fewer than a fifth of the graph dies, so it is not compacted: the dead
    // entries stay in the walk. Every 10th node is deleted, and every 31st of
    // the rest is given a new vector, which leaves its old entry dead.
    let deleted = |id: u64| id % 10 == 3;
    for id in (0..3_000u64).filter(|&id| deleted(id)) {
        assert!(index.remove(NodeId::new(id)));
    }
    let moved = points(3_000, 8, 8);
    for id in (0..3_000u64).filter(|&id| !deleted(id) && id % 31 == 0) {
        index.add(NodeId::new(id), &moved[id as usize]).unwrap();
    }
    assert!(
        index.superseded() > 0,
        "the graph was compacted; the test needs dead entries"
    );

    let queries = points(20, 8, 9);
    // The predicate passes the deleted nodes too: the walk must still drop them.
    let pass = |id: NodeId| id.as_u64() % 100 < 5;
    for q in &queries {
        let got = index.search_inline_where(q, 10, None, &pass).unwrap();
        assert_eq!(got.len(), 10);
        assert!(
            got.iter().all(|(id, _)| !deleted(id.as_u64())),
            "a deleted node came back: {got:?}"
        );
    }
    check_against_exact(&index, &queries, 10, 5, |id| !deleted(id));

    // A moved node is at its new distance, never its old one.
    let q = &moved[62];
    let got = index
        .search_inline_where(q, 1, None, &|id| id.as_u64() == 62)
        .unwrap();
    assert_eq!(got.len(), 1);
    assert!(got[0].1.abs() < 1e-6, "{got:?}");
}

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// The search line of the plan EXPLAIN prints.
fn search_line(s: &GraphStore, q: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {q}")).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s).execute(&query).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    text.lines()
        .find(|l| l.contains("VectorSearch"))
        .unwrap_or_else(|| panic!("no vector search in\n{text}"))
        .to_string()
}

fn rows(s: &GraphStore, q: &str) -> Vec<(i64, f64)> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| {
            let id = match r.get("id") {
                Some(Value::Property(PropertyValue::Integer(i))) => *i,
                other => panic!("{other:?}"),
            };
            let d = match r.get("d") {
                Some(Value::Property(PropertyValue::Float(f))) => *f,
                other => panic!("{other:?}"),
            };
            (id, d)
        })
        .collect()
}

#[test]
fn the_planner_chooses_the_in_filter_search_and_explain_shows_it() {
    // 6,000 4-d nodes, `g = id % 4` passes a quarter, `b = id % 1000` one in
    // a thousand. With m = 8 and ef = 24 one filtered walk is cheaper than
    // scoring 1,500 nodes or widening three times.
    let n = 6_000;
    let data = points(n, 4, 10);
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_emb FOR (n:VDoc) ON (n.v) \
         OPTIONS {dimensions: 4, similarity: 'l2', m: 8, ef: 24}",
    );
    for chunk in (0..n).collect::<Vec<_>>().chunks(250) {
        let nodes: Vec<String> = chunk
            .iter()
            .map(|&i| {
                let p = &data[i];
                format!(
                    "(:VDoc {{id:{i}, g:{}, b:{}, v:[{:?},{:?},{:?},{:?}]}})",
                    i % 4,
                    i % 1000,
                    p[0],
                    p[1],
                    p[2],
                    p[3]
                )
            })
            .collect();
        write(&mut s, &format!("CREATE {}", nodes.join(", ")));
    }
    let query = |q: &[f32], pred: &str| {
        format!(
            "CALL db.index.vector.queryNodes('doc_emb', 10, [{:?},{:?},{:?},{:?}]) \
             YIELD node, distance WHERE {pred} RETURN node.id AS id, distance AS d",
            q[0], q[1], q[2], q[3]
        )
    };
    let queries = points(20, 4, 11);

    let line = search_line(&s, &query(&queries[0], "node.g = 1"));
    assert!(line.contains("strategy=in-filter"), "{line}");
    assert!(line.contains("inline_work="), "{line}");
    // A predicate that passes one node in a thousand is still scanned exactly.
    let rare = search_line(&s, &query(&queries[0], "node.b = 7"));
    assert!(rare.contains("strategy=pre-exact"), "{rare}");

    let l2 = |a: &[f32], b: &[f32]| -> f64 {
        a.iter()
            .zip(b)
            .map(|(x, y)| ((x - y) * (x - y)) as f64)
            .sum::<f64>()
            .sqrt()
    };
    let (mut hits, mut total) = (0, 0);
    for q in &queries {
        let got = rows(&s, &query(q, "node.g = 1"));
        assert_eq!(got.len(), 10, "{got:?}");
        assert!(got.iter().all(|(id, _)| id % 4 == 1), "{got:?}");
        assert!(
            got.windows(2).all(|w| w[0].1 <= w[1].1),
            "not nearest first: {got:?}"
        );
        for (id, d) in &got {
            let e = l2(q, &data[*id as usize]);
            assert!((d - e).abs() < 1e-4, "{id}: distance {d}, exact {e}");
        }
        let mut want: Vec<(usize, f64)> = (0..n)
            .filter(|i| i % 4 == 1)
            .map(|i| (i, l2(q, &data[i])))
            .collect();
        want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        want.truncate(10);
        hits += got
            .iter()
            .filter(|(id, _)| want.iter().any(|(w, _)| *w as i64 == *id))
            .count();
        total += want.len();
    }
    let recall = hits as f64 / total as f64;
    assert!(recall >= 0.9, "recall@10 {recall:.3}");
}
