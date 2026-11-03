//! `ORDER BY count(*) … LIMIT k` over a degree aggregate selects k rows without
//! building a `Record` for every group (#304).
//!
//! PR #1809 made the Q19 shape read its group counts from the catalog's degree
//! maps and measured what was left: at 2M groups the query took 2.1 s, the same
//! query with the `ORDER BY` removed took 10.5 ms. The row source was 10 ms of
//! 2.1 s. The remaining 2.09 s was **materialising one `Record` per group**,
//! because `Sort` has to see every group to know which ten are the top ten.
//! Pushing the limit into the sort (also #1809) recovered 16% of it and no
//! more, because a bounded sort still receives every row.
//!
//! This replaces the `Sort` with a bounded selection *inside* the aggregate.
//! The operator pulls `(node, count)` pairs — 16 bytes each, straight out of
//! the catalog — keeps the best k in a k-element heap, and emits `Record`s for
//! those k only. The `Sort` is removed from the plan; `Skip` and `Limit` stay.
//!
//! ## Ties at the k boundary
//!
//! `SortOperator::trim_to` says it outright: "Cypher does not define a
//! tie-break for `ORDER BY … LIMIT`, so any k of a tied set is a valid answer,
//! but two runs may therefore disagree about *which*". It uses
//! `select_nth_unstable_by` and an unstable sort, and the rows arrive in the
//! iteration order of a hash map, so on `main` today **which** tied group
//! survives is unspecified and not reproducible between processes.
//!
//! This path is narrower, not wider: the heap tie-breaks on arrival order, so
//! for a given arrival order it returns exactly the rows a *stable* sort
//! followed by `LIMIT k` would. That is one of the answers the existing path
//! was already allowed to give, so nothing observable is taken away. The tests
//! below therefore assert on the **multiset of counts**, which is fully
//! determined, and on equality with the unbounded path's first k counts — not
//! on which tied group won, because neither path promises that.
//!
//! ## Shapes accepted
//!
//! The specialised plan is `plan_adjacency_count_aggregate`'s, so #1809's whole
//! shape set applies first. On top of it, streaming top-k needs:
//!
//! * exactly one `ORDER BY` item, and
//! * that item must resolve to the projected count alias — `ORDER BY c` or
//!   `ORDER BY count(*)`, either direction, and
//! * a `LIMIT`.
//!
//! `SKIP s LIMIT k` is accepted as k = s + k, since the rows are emitted in
//! order and the `Skip` above still trims the head.
//!
//! ## Shapes declined, each with a test that it declines *and* answers right
//!
//! | shape | why |
//! |---|---|
//! | no `LIMIT` | there is no k, so there is nothing to bound by |
//! | no `ORDER BY` | the aggregate must emit every group; `Limit` already stops the pull |
//! | `ORDER BY` on the group key | the key is a property of the surviving rows, not the count |
//! | `ORDER BY c DESC, t ASC` | the second key reorders ties, which the count alone cannot decide |
//! | `ORDER BY c + 1` | an expression over the count is not the count alias |
//!
//! A stub-loaded graph is **not** a decline for this: the completeness guard
//! from #1806/#1809 governs where the counts come from, and top-k is exact over
//! whichever source answers. The guard sends it to the adjacency walk, the walk
//! produces the same counts, and the heap selects over those. There is a test
//! for that too.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Five articles. a0 is cited three times, a1 once, a2/a3/a4 never.
fn citations() -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(format!("A{i}")),
        );
        ids.push(id);
    }
    store.create_edge(ids[1], ids[0], "CITES").unwrap();
    store.create_edge(ids[2], ids[0], "CITES").unwrap();
    store.create_edge(ids[3], ids[0], "CITES").unwrap();
    store.create_edge(ids[2], ids[1], "CITES").unwrap();
    store
}

/// `n` articles where article `i` is cited exactly `degrees[i]` times, by
/// dedicated citing articles so no citer is itself cited.
///
/// Ties are the point: `degrees` may repeat, and the fixtures below repeat it
/// across the k boundary.
fn graded(degrees: &[usize]) -> GraphStore {
    let mut store = GraphStore::new();
    let mut cited = Vec::new();
    for (i, _) in degrees.iter().enumerate() {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(format!("T{i}")),
        );
        cited.push(id);
    }
    for (i, d) in degrees.iter().enumerate() {
        for _ in 0..*d {
            let citer = store.create_node("Citer");
            store.create_edge(citer, cited[i], "CITES").unwrap();
        }
    }
    store
}

fn plan_unchecked(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// [`plan_unchecked`], refusing a plan tree that stops early.
///
/// `PhysicalOperator::describe` has a default printing `Unknown` with no
/// children, so an operator that does not implement it truncates the tree —
/// and an assertion that the plan does *not* contain something is then
/// satisfied by the missing half, whatever the planner did (#1826). Checked
/// here, where the text is read, so the hole fails loudly rather than passing.
fn plan(store: &GraphStore, cypher: &str) -> String {
    let text = plan_unchecked(store, cypher);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

/// The `c` column of every row, in the order the query returned them.
fn counts(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("c: {other:?}"),
        })
        .collect()
}

/// Rows as `(title, count)`, sorted, so a comparison does not depend on which
/// of a tied set survived.
fn titled(store: &GraphStore, cypher: &str) -> Vec<(String, i64)> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let t = match r.get("t") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("t: {other:?}"),
            };
            let c = match r.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("c: {other:?}"),
            };
            (t, c)
        })
        .collect();
    out.sort();
    out
}

const Q19: &str = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC LIMIT 2";

// ------------------------------------------------------------------- accepted

#[test]
fn the_q19_shape_streams_the_top_k_and_drops_the_sort() {
    let store = citations();
    let p = plan(&store, Q19);
    assert!(
        p.contains("topk=2 desc"),
        "the aggregate must do the bounded selection itself; plan was:\n{p}"
    );
    assert!(
        !p.contains("Sort ("),
        "and the Sort must be gone -- leaving it would materialise a Record per \
         group, which is the whole cost this removes; plan was:\n{p}"
    );
    assert_eq!(counts(&store, Q19), vec![3, 1]);
}

#[test]
fn ascending_streams_the_bottom_k() {
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c ASC LIMIT 2";
    let p = plan(&store, q);
    assert!(p.contains("topk=2 asc"), "plan was:\n{p}");
    assert!(!p.contains("Sort ("), "plan was:\n{p}");
    assert_eq!(counts(&store, q), vec![1, 3]);
}

#[test]
fn the_aggregate_spelling_of_the_sort_key_is_accepted_too() {
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY count(*) DESC LIMIT 2";
    let p = plan(&store, q);
    assert!(p.contains("topk=2 desc"), "plan was:\n{p}");
    assert_eq!(counts(&store, q), vec![3, 1]);
}

/// The oracle: the streaming answer is the unbounded answer's first k.
///
/// The no-`LIMIT` spelling declines streaming by construction (there is no k),
/// so it is the full `Sort` path on the same data in the same process — which
/// makes it the reference and not a second guess.
#[test]
fn the_streamed_counts_equal_the_sorted_paths_first_k() {
    // ties across the k boundary, degree-zero nodes, and k past the group count
    let fixtures: &[&[usize]] = &[
        &[3, 1, 0, 0, 0],
        &[5, 5, 5, 5, 5],
        &[4, 4, 2, 2, 2, 1],
        &[0, 0, 0, 1],
        &[7, 6, 5, 4, 3, 2, 1],
        &[2, 2],
        &[1],
    ];
    for degrees in fixtures {
        let store = graded(degrees);
        for desc in [true, false] {
            let dir = if desc { "DESC" } else { "ASC" };
            let full = counts(
                &store,
                &format!("MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c {dir}"),
            );
            for k in 1..=(degrees.len() + 3) {
                let q = format!(
                    "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c {dir} LIMIT {k}"
                );
                let p = plan(&store, &q);
                assert!(
                    p.contains(&format!("topk={k} {}", dir.to_lowercase())),
                    "{degrees:?} k={k}: plan was:\n{p}"
                );
                let got = counts(&store, &q);
                let want: Vec<i64> = full.iter().copied().take(k).collect();
                assert_eq!(got, want, "{degrees:?} {dir} LIMIT {k}");
            }
        }
    }
}

#[test]
fn skip_is_folded_into_k() {
    let store = graded(&[5, 4, 3, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC SKIP 1 LIMIT 2";
    let p = plan(&store, q);
    assert!(
        p.contains("topk=3 desc"),
        "k must be SKIP + LIMIT, or the skipped rows are the ones kept; plan was:\n{p}"
    );
    assert_eq!(counts(&store, q), vec![4, 3]);
}

/// A property group key is read while grouping, because the property value *is*
/// the group key -- two articles sharing a title are one group and only their
/// properties say so. So this shape cannot defer the read to the survivors; what
/// it does save is the `Record` per group and the `Sort` above it.
#[test]
fn a_property_group_key_streams_too_and_still_merges_groups() {
    let mut store = GraphStore::new();
    let mut cited = Vec::new();
    // two articles share the title "A0", so they are one group; a third has its own
    for t in ["A0", "A0", "A1"] {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(t.to_string()),
        );
        cited.push(id);
    }
    for (i, d) in [3usize, 1, 2].iter().enumerate() {
        for _ in 0..*d {
            let citer = store.create_node("Citer");
            store.create_edge(citer, cited[i], "CITES").unwrap();
        }
    }

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC LIMIT 1";
    let p = plan(&store, q);
    assert!(p.contains("topk=1 desc"), "plan was:\n{p}");
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 4)],
        "the two A0 articles are one group of 4, which must beat A1's 2"
    );
}

/// The #1806/#1809 completeness guard decides the *source*; it does not decide
/// whether the selection can be bounded. A stub load sends this to the
/// adjacency walk and the heap selects over the walk's counts.
#[test]
fn a_stub_loaded_graph_walks_adjacency_and_still_streams_the_top_k() {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(store.create_node_stub("Article"));
    }
    store.create_edge_stub(ids[1], ids[0], "CITES").unwrap();
    store.create_edge_stub(ids[2], ids[0], "CITES").unwrap();
    store.create_edge_stub(ids[3], ids[1], "CITES").unwrap();

    let p = plan(&store, Q19);
    assert!(
        p.contains("source=adjacency"),
        "the degree maps are short by every stub edge, so the catalog must be \
         declined; plan was:\n{p}"
    );
    assert!(
        p.contains("topk=2 desc"),
        "but the bounded selection is exact over the walk's counts too; plan \
         was:\n{p}"
    );
    assert_eq!(counts(&store, Q19), vec![2, 1]);
}

#[test]
fn count_distinct_streams_over_the_deduped_counts() {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(store.create_node("Article"));
    }
    // two parallel edges from ids[1] to ids[0]: degree 2, one distinct neighbour
    store.create_edge(ids[1], ids[0], "CITES").unwrap();
    store.create_edge(ids[1], ids[0], "CITES").unwrap();
    store.create_edge(ids[2], ids[1], "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-(b) RETURN a, count(DISTINCT b) AS c ORDER BY c DESC LIMIT 1";
    let p = plan(&store, q);
    assert!(p.contains("topk=1 desc"), "plan was:\n{p}");
    assert_eq!(counts(&store, q), vec![1]);
}

// ------------------------------------------------------------------- declined

#[test]
fn no_limit_declines_and_sorts_every_group() {
    let store = graded(&[3, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC";
    let p = plan(&store, q);
    assert!(!p.contains("topk="), "there is no k to bound by; plan was:\n{p}");
    assert!(p.contains("Sort ("), "plan was:\n{p}");
    assert_eq!(counts(&store, q), vec![3, 2, 1]);
}

#[test]
fn no_order_by_declines() {
    let store = graded(&[3, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c LIMIT 2";
    let p = plan(&store, q);
    assert!(
        !p.contains("topk="),
        "without an ORDER BY the k rows are not defined by the count, and the \
         Limit above already stops the pull; plan was:\n{p}"
    );
    let got = counts(&store, q);
    assert_eq!(got.len(), 2, "{got:?}");
}

#[test]
fn order_by_the_group_key_declines_and_answers_right() {
    let store = graded(&[3, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY t DESC LIMIT 2";
    let p = plan(&store, q);
    assert!(!p.contains("topk="), "plan was:\n{p}");
    assert!(p.contains("Sort ("), "plan was:\n{p}");
    assert_eq!(
        titled(&store, q),
        vec![("T1".to_string(), 2), ("T2".to_string(), 1)]
    );
}

#[test]
fn a_second_order_by_key_declines_and_answers_right() {
    let store = graded(&[2, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC, t ASC LIMIT 2";
    let p = plan(&store, q);
    assert!(
        !p.contains("topk="),
        "the second key reorders ties, which the count alone cannot decide; \
         plan was:\n{p}"
    );
    assert!(p.contains("Sort ("), "plan was:\n{p}");
    assert_eq!(
        titled(&store, q),
        vec![("T0".to_string(), 2), ("T1".to_string(), 2)]
    );
}

#[test]
fn an_expression_over_the_count_declines_and_answers_right() {
    let store = graded(&[3, 2, 1]);
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c + 1 DESC LIMIT 2";
    let p = plan(&store, q);
    assert!(!p.contains("topk="), "plan was:\n{p}");
    assert_eq!(counts(&store, q), vec![3, 2]);
}

// --------------------------------------------------------------------- #1810

/// `ORDER BY` on a projected aggregate alias in the clause-pipeline AST shape.
///
/// The pipeline's `Clause::Return` arm placed the `Sort` **below** the
/// projection unconditionally and resolved the key with
/// `SortPosition::BeforeProjection`, which substitutes the alias for the
/// expression it names. So `ORDER BY c` became `ORDER BY count(*)` and was
/// evaluated by the scalar expression evaluator, beneath the `Aggregate` that
/// implements `count` -- `RuntimeError("Unknown function: count")`.
///
/// This is the literal Q19 text, so the feature above was untestable in this
/// shape. An aggregating RETURN now sorts *above* the projection, resolved with
/// `SortPosition::AfterProjection`, which is what the by-kind path has always
/// done.
#[test]
fn order_by_an_aggregate_alias_works_in_the_clause_pipeline_shape() {
    use samyama::query::executor::MutQueryExecutor;
    let mut store = citations();
    let q = "CREATE (z:Tmp) WITH z MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC LIMIT 2";
    let parsed = parse_query(q).unwrap();
    assert!(
        !parsed.clauses.is_empty(),
        "this test is only meaningful in the pipeline shape; it parsed by-kind"
    );
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&parsed)
        .expect("ORDER BY on a projected aggregate alias must not re-evaluate the aggregate");
    let got: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("c: {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![3, 1]);
}

#[test]
fn order_by_an_aggregate_expression_works_in_the_clause_pipeline_shape() {
    use samyama::query::executor::MutQueryExecutor;
    let mut store = citations();
    let q = "CREATE (z:Tmp) WITH z MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY count(*) DESC LIMIT 2";
    let parsed = parse_query(q).unwrap();
    assert!(!parsed.clauses.is_empty(), "parsed by-kind");
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&parsed)
        .expect("the aggregate spelling must resolve to the projected alias too");
    let got: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("c: {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![3, 1]);
}

/// A non-aggregating RETURN must keep sorting below the projection: `WITH p,
/// count(q) AS rng RETURN p ORDER BY rng` sorts on a column the RETURN does not
/// carry, and moving that sort above the projection leaves the key unbound and
/// silently sorts nothing. CH-DETERM caught exactly that. The #1810 fix is
/// conditional on the RETURN aggregating, so this must still hold.
#[test]
fn a_non_aggregating_return_still_sorts_on_a_key_it_does_not_project() {
    use samyama::query::executor::MutQueryExecutor;
    let mut store = citations();
    let q = "CREATE (z:Tmp) WITH z MATCH (a:Article)<-[:CITES]-() WITH a, count(*) AS c RETURN a.title AS t ORDER BY c DESC";
    let parsed = parse_query(q).unwrap();
    assert!(!parsed.clauses.is_empty(), "parsed by-kind");
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&parsed)
        .unwrap();
    let got: Vec<String> = batch
        .records
        .iter()
        .map(|r| match r.get("t") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("t: {other:?}"),
        })
        .collect();
    assert_eq!(got, vec!["A0".to_string(), "A1".to_string()]);
}
