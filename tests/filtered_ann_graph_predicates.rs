//! The graph-reachability workload of the filtered sweep (#1985): the k-NN
//! graph and reach sets it is built from, and the two Cypher statements the
//! benchmark times, checked on a corpus small enough to verify by hand.
//!
//! The benchmark compares strategies by recall against `oracle` over the
//! reach `two_hop` computes. That comparison is only meaningful if the
//! statements answer the question the oracle does, so they are run here
//! against the same oracle: `cypher_exact` must match it, `cypher_post` must
//! never return a node outside the reach.
#[path = "../benches/ann_common/mod.rs"]
mod ann_common;
#[path = "../benches/filtered_ann/mod.rs"]
mod filtered_ann;

use ann_common::{synthetic, Vectors};
use filtered_ann::graph::{anchor, cypher_exact, cypher_post, edge_type, knn_graph, two_hop};
use filtered_ann::{oracle, Correlation};
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::DistanceMetric;
use std::collections::HashMap;

fn line(n: usize) -> Vectors {
    Vectors {
        dim: 1,
        data: (0..n).map(|i| i as f32).collect(),
    }
}

#[test]
fn knn_graph_links_each_node_to_its_nearest_others() {
    let adj = knn_graph(&line(10), 2);
    assert_eq!(adj[0], vec![1, 2]);
    // Ties (4 and 6 are both one away from 5) go to the lower index.
    assert_eq!(adj[5], vec![4, 6]);
    assert_eq!(adj[9], vec![8, 7]);
    for (i, list) in adj.iter().enumerate() {
        assert!(!list.contains(&(i as u32)), "no self-loops");
    }
    // A prefix of a list is the graph of a smaller degree.
    let wide = knn_graph(&line(10), 4);
    for (narrow, wide) in adj.iter().zip(&wide) {
        assert_eq!(narrow[..], wide[..2]);
    }
}

#[test]
fn two_hop_is_out_and_out_of_out() {
    let adj = knn_graph(&line(10), 2);
    let reach = two_hop(&adj, 0, 2);
    let got: Vec<usize> = (0..10).filter(|&i| reach[i]).collect();
    // 0 -> {1, 2}; 1 -> {0, 2}; 2 -> {1, 3}. The anchor comes back via 1.
    assert_eq!(got, vec![0, 1, 2, 3]);
    let one = two_hop(&adj, 0, 1);
    assert_eq!((0..10).filter(|&i| one[i]).collect::<Vec<_>>(), vec![0, 1]);
}

#[test]
fn anchor_follows_the_correlation() {
    let base = line(10);
    assert_eq!(anchor(&base, &[2.2], Correlation::Positive, 0), 2);
    assert_eq!(anchor(&base, &[2.2], Correlation::Negative, 0), 9);
    let random = anchor(&base, &[2.2], Correlation::None, 7);
    assert_eq!(random, anchor(&base, &[2.2], Correlation::None, 7));
    assert!(random < 10);
}

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

fn ids(s: &GraphStore, q: &str) -> Vec<u32> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i as u32,
            other => panic!("{q}: {other:?}"),
        })
        .collect()
}

/// The store the benchmark builds, at a size this test can afford.
fn store(base: &Vectors, adj: &[Vec<u32>], degrees: &[usize]) -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE INDEX FOR (n:V) ON (n.id)");
    s.create_vector_index("V", "v", base.dim, DistanceMetric::L2)
        .unwrap();
    let mut node_of = Vec::new();
    for i in 0..base.len() {
        let mut props = HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("v".to_string(), PropertyValue::Vector(base.row(i).to_vec()));
        node_of.push(s.create_node_with_properties("default", vec![Label::new("V")], props));
    }
    for &degree in degrees {
        for (i, list) in adj.iter().enumerate() {
            for &j in list.iter().take(degree) {
                s.create_edge(node_of[i], node_of[j as usize], edge_type(degree).as_str())
                    .unwrap();
            }
        }
    }
    s
}

#[test]
fn the_benchmark_statements_answer_the_oracle_question() {
    let base = synthetic(400, 8, 1985);
    let queries = synthetic(12, 8, 1986);
    let degrees = [3, 6];
    let adj = knn_graph(&base, 6);
    let s = store(&base, &adj, &degrees);
    for degree in degrees {
        for correlation in [
            Correlation::Positive,
            Correlation::None,
            Correlation::Negative,
        ] {
            for qi in 0..queries.len() {
                let q = queries.row(qi);
                let x = anchor(&base, q, correlation, qi as u64);
                let reach = two_hop(&adj, x, degree);
                let truth = oracle(&base, q, &reach, 10);

                let exact = ids(&s, &cypher_exact(degree, x, q, 10));
                assert_eq!(
                    exact, truth,
                    "exact, degree {degree}, {correlation:?}, query {qi}"
                );

                let post = ids(&s, &cypher_post(degree, x, q, 10));
                assert!(post.len() <= 10);
                for id in &post {
                    assert!(reach[*id as usize], "post returned {id}, outside the reach");
                }
            }
        }
    }
}
