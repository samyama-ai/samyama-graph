//! A vector index forgets what the graph no longer holds (#1968).
//!
//! HNSW has no delete. A deleted node's entry stayed in the graph and was
//! dropped by a label check after the search, so with enough of them the beam
//! came back full of dead entries and recall fell to zero. A reused node id
//! or a `SET` of a new vector left the old one answering as well. These tests
//! delete a share of the nodes and compare the index with an exact scan of
//! the survivors.

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

/// The issue's reproduction, after the delete.
///
/// 60 + 60 points, not the issue's 200 + 200: two lines in 2-D are a worst
/// case for HNSW, whose greedy search sometimes stays on the line it entered
/// by (it failed this way on arm CI). At 120 vectors the index searches
/// exactly, so this checks the delete and nothing else. The old code still
/// fails it: deleted node 1 sits on the query and takes a top-3 slot before
/// the label check drops it. Recall after deletes at HNSW sizes is checked
/// further down.
#[test]
fn deleted_nodes_do_not_crowd_out_the_survivors() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX l2idx FOR (n:L2T) ON (n.v) OPTIONS {dimensions: 2, similarity: 'l2'}",
    );
    write(
        &mut s,
        "UNWIND range(1,60) AS i CREATE (:L2T {id:i, v:[toFloat(i), 0.0]})",
    );
    write(
        &mut s,
        "UNWIND range(1,60) AS i CREATE (:L2T {id:1000+i, v:[1.0, toFloat(i)/100.0]})",
    );
    write(&mut s, "MATCH (n:L2T) WHERE n.id <= 40 DETACH DELETE n");
    let got = ids(
        &s,
        "CALL db.index.vector.queryNodes('L2T','v',[1.0,0.0],3) YIELD node, score \
         RETURN node.id AS id ORDER BY score, id",
    );
    assert_eq!(got, vec![1001, 1002, 1003]);
}

/// A node whose vector is replaced, removed, or whose label is removed, is
/// found only where it now is.
#[test]
fn set_remove_and_relabel_leave_no_stale_entry() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX FOR (n:P) ON (n.v) OPTIONS {dimensions: 2, similarity: 'l2'}",
    );
    write(
        &mut s,
        "UNWIND range(1,300) AS i CREATE (:P {id:i, v:[toFloat(i), 0.0]})",
    );
    let near_origin = "CALL db.index.vector.queryNodes('P','v',[0.0,0.0],1) YIELD node \
                       RETURN node.id AS id";
    assert_eq!(ids(&s, near_origin), vec![1]);

    write(&mut s, "MATCH (n:P {id:1}) SET n.v = [1000.0, 0.0]");
    assert_eq!(ids(&s, near_origin), vec![2]);

    write(&mut s, "MATCH (n:P {id:2}) REMOVE n.v");
    assert_eq!(ids(&s, near_origin), vec![3]);

    write(&mut s, "MATCH (n:P {id:3}) SET n.v = 'not a vector'");
    assert_eq!(ids(&s, near_origin), vec![4]);

    write(&mut s, "MATCH (n:P {id:4}) REMOVE n:P");
    assert_eq!(ids(&s, near_origin), vec![5]);
}

fn points(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            (0..8)
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

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// Delete 10%, 50% and 90% of an index, then check recall@10 against an exact
/// scan of the survivors, and that no deleted node is ever returned.
#[test]
fn recall_holds_after_deleting_a_share_of_the_index() {
    let n = 3_000;
    let data = points(n, 0x9E37_79B9_7F4A_7C15);
    let queries = points(30, 0xD1B5_4A32_D192_ED03);
    for share in [10usize, 50, 90] {
        let mut index = VectorIndex::new(8, DistanceMetric::L2);
        for (i, v) in data.iter().enumerate() {
            index.add(NodeId::new(i as u64), v).unwrap();
        }
        let deleted = |i: usize| i % 100 < share;
        for i in (0..n).filter(|&i| deleted(i)) {
            assert!(index.remove(NodeId::new(i as u64)));
        }
        let live: Vec<usize> = (0..n).filter(|&i| !deleted(i)).collect();
        assert_eq!(index.len(), live.len());
        // Dead entries never outnumber a fifth of what the graph holds.
        assert!(index.superseded() * 5 < index.len() + index.superseded());

        let mut hits = 0;
        let mut total = 0;
        for q in &queries {
            let mut want: Vec<(usize, f32)> = live.iter().map(|&i| (i, l2(q, &data[i]))).collect();
            want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let want: Vec<u64> = want.iter().take(10).map(|p| p.0 as u64).collect();
            let got: Vec<u64> = index
                .search(q, 10)
                .unwrap()
                .iter()
                .map(|p| p.0.as_u64())
                .collect();
            assert_eq!(got.len(), 10, "{share}% deleted: short result");
            for id in &got {
                assert!(
                    !deleted(*id as usize),
                    "{share}% deleted: returned deleted node {id}"
                );
            }
            hits += got.iter().filter(|id| want.contains(id)).count();
            total += want.len();
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.9, "{share}% deleted: recall@10 {recall:.2}");
    }
}

/// A reused id with a new vector answers with the new vector only.
#[test]
fn a_re_added_id_replaces_its_old_entry() {
    let mut index = VectorIndex::new(2, DistanceMetric::L2);
    for i in 0..500u64 {
        index.add(NodeId::new(i), &vec![i as f32, 0.0]).unwrap();
    }
    index.add(NodeId::new(0), &vec![10_000.0, 0.0]).unwrap();
    assert_eq!(index.len(), 500);
    let got = index.search(&[0.0, 0.0], 1).unwrap();
    assert_eq!(got[0].0.as_u64(), 1);
    let got = index.search(&[10_000.0, 0.0], 1).unwrap();
    assert_eq!(got[0].0.as_u64(), 0);
}
