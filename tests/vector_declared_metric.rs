//! A vector index searches with the metric it was declared with (#1967).
//!
//! `similarity: 'l2'` was accepted and stored, but both HNSW backends were
//! built with cosine distance whatever the DDL said, and so was the exact
//! path. Vectors collinear with the query came back at distance 0 under an
//! `l2` index. These tests pin each metric against an exact answer computed
//! here, both below the size where the index searches exactly and above it,
//! where the HNSW graph answers.

use samyama::graph::PropertyValue;
use samyama::graph::{GraphStore, NodeId};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::{DistanceMetric, VectorIndex};

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

/// The issue's own reproduction: 200 vectors along the query's direction and
/// 200 just off its tip. Under L2 the nearest are the query itself and the
/// ones a hundredth away, not the collinear ones far down the line.
#[test]
fn an_l2_index_answers_with_l2_distance() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX l2idx FOR (n:L2T) ON (n.v) OPTIONS {dimensions: 2, similarity: 'l2'}",
    );
    write(
        &mut s,
        "UNWIND range(1,200) AS i CREATE (:L2T {id:i, v:[toFloat(i), 0.0]})",
    );
    write(
        &mut s,
        "UNWIND range(1,200) AS i CREATE (:L2T {id:1000+i, v:[1.0, toFloat(i)/100.0]})",
    );
    let got = ids(
        &s,
        "CALL db.index.vector.queryNodes('L2T','v',[1.0,0.0],5) YIELD node, score \
         RETURN node.id AS id ORDER BY score, id",
    );
    assert_eq!(got, vec![1, 1001, 1002, 1003, 1004]);
}

#[test]
fn the_ddl_takes_every_metric_by_name() {
    for name in ["cosine", "l2", "euclidean", "inner_product", "dot"] {
        let mut s = GraphStore::new();
        write(
            &mut s,
            &format!(
                "CREATE VECTOR INDEX FOR (n:M) ON (n.v) OPTIONS {{dimensions: 2, similarity: '{name}'}}"
            ),
        );
    }
}

/// Deterministic points that are not normalised, so the three metrics
/// disagree about which are nearest.
fn points(n: usize) -> Vec<Vec<f32>> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..n)
        .map(|_| {
            (0..8)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    ((x >> 40) as f32 / (1u64 << 24) as f32) * 4.0 - 2.0
                })
                .collect()
        })
        .collect()
}

fn exact(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    match metric {
        DistanceMetric::L2 => a
            .iter()
            .zip(b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>()
            .sqrt(),
        DistanceMetric::InnerProduct => 1.0 - dot,
        DistanceMetric::Cosine => {
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            1.0 - dot / (na * nb)
        }
    }
}

/// The index's top-k agrees with an exact scan under the same metric, and
/// reports the distance the metric defines. Below 128 vectors the index scans
/// exactly; above it the graph answers, so the check there is recall rather
/// than equality.
#[test]
fn each_metric_ranks_and_scores_as_an_exact_scan_does() {
    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::L2,
        DistanceMetric::InnerProduct,
    ] {
        for n in [100usize, 2_000] {
            let data = points(n);
            let mut index = VectorIndex::new(8, metric);
            for (i, v) in data.iter().enumerate() {
                index.add(NodeId::new(i as u64), v).unwrap();
            }
            let mut hits = 0;
            let mut total = 0;
            for q in points(n + 20).into_iter().skip(n) {
                let mut want: Vec<(u64, f32)> = data
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (i as u64, exact(metric, &q, v)))
                    .collect();
                want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let want: Vec<u64> = want.iter().take(10).map(|p| p.0).collect();
                let got = index.search(&q, 10).unwrap();
                for (id, d) in &got {
                    let true_d = exact(metric, &q, &data[id.as_u64() as usize]);
                    assert!(
                        (d - true_d).abs() <= 1e-3 * true_d.abs().max(1.0),
                        "{metric:?}: reported {d}, the metric gives {true_d}"
                    );
                }
                let got: Vec<u64> = got.iter().map(|p| p.0.as_u64()).collect();
                if n <= 128 {
                    assert_eq!(got, want, "{metric:?} n={n}");
                }
                hits += got.iter().filter(|id| want.contains(id)).count();
                total += want.len();
            }
            let recall = hits as f64 / total as f64;
            assert!(recall >= 0.8, "{metric:?} n={n}: recall@10 {recall:.2}");
        }
    }
}
