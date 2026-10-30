//! A WHERE after `db.index.vector.queryNodes` decides which nodes count
//! towards k (#1969).
//!
//! It used to filter the k results the search had already chosen, so a
//! predicate that passes one node in ten, with k = 5, usually returned
//! nothing. The part of the WHERE that names only the yielded node is now
//! applied inside the search, which widens, and past a point scans exactly,
//! until it has k nodes that pass. These tests compare the answer with an
//! exact scan of the passing nodes at selectivities from 0.1% to 100%.

use samyama::graph::GraphStore;
use samyama::graph::PropertyValue;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

fn ids(s: &GraphStore, q: &str) -> Vec<i64> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Deterministic 4-dimensional points.
fn points(n: usize) -> Vec<[f32; 4]> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..n)
        .map(|_| {
            let mut p = [0f32; 4];
            for v in &mut p {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *v = ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0;
            }
            p
        })
        .collect()
}

/// `n` `:VDoc` nodes with `id`, `cat = id % 10`, `b = id % 1000` and `v`.
fn store(n: usize) -> (GraphStore, Vec<[f32; 4]>) {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_emb FOR (n:VDoc) ON (n.v) \
         OPTIONS {dimensions: 4, similarity: 'l2'}",
    );
    let data = points(n);
    for chunk in (0..n).collect::<Vec<_>>().chunks(250) {
        let nodes: Vec<String> = chunk
            .iter()
            .map(|&i| {
                let p = data[i];
                format!(
                    "(:VDoc {{id:{i}, cat:{}, b:{}, v:[{:?},{:?},{:?},{:?}]}})",
                    i % 10,
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
    (s, data)
}

fn l2(a: &[f32; 4], b: &[f32; 4]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// The exact `k` nearest to `q` among the ids `pass` accepts.
fn exact(data: &[[f32; 4]], q: &[f32; 4], k: usize, pass: impl Fn(usize) -> bool) -> Vec<i64> {
    let mut d: Vec<(usize, f32)> = (0..data.len())
        .filter(|&i| pass(i))
        .map(|i| (i, l2(q, &data[i])))
        .collect();
    d.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    d.iter().take(k).map(|p| p.0 as i64).collect()
}

/// The issue's reproduction: 1,000 nodes, one in ten passes, k = 5.
#[test]
fn a_selective_where_still_returns_k_rows() {
    let (s, data) = store(1_000);
    let q = [1.0, 2.0, 3.0, 1.0];
    let got = ids(
        &s,
        "CALL db.index.vector.queryNodes('doc_emb', 5, [1.0,2.0,3.0,1.0]) YIELD node, score \
         WHERE node.cat = 3 RETURN node.id AS id",
    );
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(got.iter().all(|id| id % 10 == 3), "{got:?}");
    let want = exact(&data, &q, 5, |i| i % 10 == 3);
    let mut sorted = got.clone();
    sorted.sort();
    let mut want_sorted = want.clone();
    want_sorted.sort();
    assert_eq!(sorted, want_sorted);
}

/// An aliased node, the label-and-property form of the call, and a conjunct
/// on the score, which only the WHERE above the search can apply.
#[test]
fn aliases_both_call_forms_and_score_conjuncts() {
    let (s, data) = store(1_000);
    let q = [0.5, -0.5, 0.25, 0.0];
    let want = exact(&data, &q, 5, |i| i % 10 == 7);

    let mut got = ids(
        &s,
        "CALL db.index.vector.queryNodes('VDoc', 'v', [0.5,-0.5,0.25,0.0], 5) \
         YIELD node AS d, score AS sc WHERE d.cat = 7 RETURN d.id AS id",
    );
    got.sort();
    let mut w = want.clone();
    w.sort();
    assert_eq!(got, w);

    // `sc < 1e9` passes everything; `d.cat = 7` is still pushed down.
    let got = ids(
        &s,
        "CALL db.index.vector.queryNodes('doc_emb', 5, [0.5,-0.5,0.25,0.0]) \
         YIELD node AS d, score AS sc WHERE d.cat = 7 AND sc < 1000000000.0 \
         RETURN d.id AS id",
    );
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|id| id % 10 == 7));
}

/// A filter that passes nothing returns nothing, and one that passes fewer
/// than k returns all of them.
#[test]
fn fewer_passing_nodes_than_k() {
    let (s, _) = store(1_000);
    let none = ids(
        &s,
        "CALL db.index.vector.queryNodes('doc_emb', 5, [0.0,0.0,0.0,0.0]) YIELD node \
         WHERE node.cat = 99 RETURN node.id AS id",
    );
    assert!(none.is_empty());
    let mut few = ids(
        &s,
        "CALL db.index.vector.queryNodes('doc_emb', 10, [0.0,0.0,0.0,0.0]) YIELD node \
         WHERE node.id IN [3, 500, 999] RETURN node.id AS id",
    );
    few.sort();
    assert_eq!(few, vec![3, 500, 999]);
}

/// The issue's acceptance: recall@10 >= 0.9 at every selectivity from 0.1%
/// to 100%.
#[test]
fn recall_at_every_selectivity() {
    let n = 5_000;
    let (s, data) = store(n);
    let queries = points(n + 20).split_off(n);
    // `b < x` over `b = id % 1000` passes x per mille.
    for per_mille in [1usize, 10, 50, 100, 500, 1000] {
        let mut hits = 0;
        let mut total = 0;
        for q in &queries {
            let got = ids(
                &s,
                &format!(
                    "CALL db.index.vector.queryNodes('doc_emb', 10, [{:?},{:?},{:?},{:?}]) \
                     YIELD node WHERE node.b < {per_mille} RETURN node.id AS id",
                    q[0], q[1], q[2], q[3]
                ),
            );
            let want = exact(&data, q, 10, |i| i % 1000 < per_mille);
            assert_eq!(got.len(), want.len(), "{per_mille}/1000: short result");
            assert!(
                got.iter().all(|id| (*id as usize) % 1000 < per_mille),
                "{per_mille}/1000: a row fails the WHERE"
            );
            hits += got.iter().filter(|id| want.contains(id)).count();
            total += want.len();
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.9, "{per_mille}/1000: recall@10 {recall:.2}");
    }
}
