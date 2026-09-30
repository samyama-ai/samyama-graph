//! Additional unit tests for the hierarchy rewrite detector: the order-test
//! and hierarchy-driven shapes, and the roll-up rejections.

use super::*;
use crate::graph::EdgeType;
use crate::index::hierarchy::HierarchySpec;
use crate::query::parse_query;

struct Fixture {
    store: GraphStore,
    c0: NodeId,
}

/// `ROOT <- C0..C2 <- 3 Drugs each` over `IS_A`, with `units` on each drug
/// (Sum and Count built, Min not), plus a `Sale -[:OF]-> Drug` fact per drug.
fn fixture() -> Fixture {
    let mut store = GraphStore::new();
    let root = store.create_node("Class");
    store.set_column_property(root, "code", PropertyValue::String("ROOT".into()));
    let mut c0 = root;
    let mut n = 1i64;
    for c in 0..3 {
        let mid = store.create_node("Class");
        store.set_column_property(mid, "code", PropertyValue::String(format!("C{c}")));
        store.create_edge(mid, root, "IS_A").unwrap();
        if c == 0 {
            c0 = mid;
        }
        for _ in 0..3 {
            let leaf = store.create_node("Drug");
            store.create_edge(leaf, mid, "IS_A").unwrap();
            store.set_column_property(leaf, "units", PropertyValue::Integer(n));
            let sale = store.create_node("Sale");
            store.set_column_property(sale, "amount", PropertyValue::Integer(n * 10));
            store.create_edge(sale, leaf, "OF").unwrap();
            n += 1;
        }
    }
    let mgr = std::sync::Arc::clone(&store.hierarchy_index);
    mgr.create(
        &store,
        HierarchySpec::new("atc", vec![EdgeType::new("IS_A")]).with_measure(
            None,
            "units",
            vec![RollupOp::Sum, RollupOp::Count],
        ),
    )
    .unwrap();
    Fixture { store, c0 }
}

fn detect_str(q: &str, store: &GraphStore) -> Option<HierarchyRewrite> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}"));
    detect(&query, store)
}

// ---------------------------------------------------------------------------
// Order test
// ---------------------------------------------------------------------------

#[test]
fn order_test_count_is_detected() {
    let f = fixture();
    let r = detect_str(
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(d)",
        &f.store,
    );
    assert_eq!(
        r,
        Some(HierarchyRewrite::OrderTest {
            index_name: "atc".into(),
            root: f.c0,
            var: "d".into(),
            labels: vec![Label::new("Drug")],
            negated: false,
            output: OrderTestOutput::Count("count(d)".into()),
        })
    );
}

#[test]
fn negated_order_test_with_an_alias() {
    let f = fixture();
    match detect_str(
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE NOT subsumes(d, r) RETURN count(d) AS n",
        &f.store,
    ) {
        Some(HierarchyRewrite::OrderTest {
            negated, output, ..
        }) => {
            assert!(negated);
            assert_eq!(output, OrderTestOutput::Count("n".into()));
        }
        other => panic!("expected an order test, got {other:?}"),
    }
}

#[test]
fn order_test_returning_the_rows_themselves() {
    let f = fixture();
    match detect_str(
        "MATCH (r:Class {code: 'C0'}), (d) WHERE subsumes(d, r) RETURN d",
        &f.store,
    ) {
        Some(HierarchyRewrite::OrderTest { output, labels, .. }) => {
            assert_eq!(output, OrderTestOutput::Nodes);
            assert!(labels.is_empty());
        }
        other => panic!("expected an order test, got {other:?}"),
    }
}

#[test]
fn order_test_count_of_a_literal_counts_the_scan() {
    let f = fixture();
    assert!(matches!(
        detect_str(
            "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(1)",
            &f.store,
        ),
        Some(HierarchyRewrite::OrderTest {
            output: OrderTestOutput::Count(_),
            ..
        })
    ));
}

#[test]
fn order_test_rejections() {
    let f = fixture();
    for q in [
        // Counting the pinned side, not the scan.
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(r)",
        // An aliased row projection, and a property projection.
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN d AS x",
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN d.units",
        // Two items, and DISTINCT.
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN d, r",
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN DISTINCT d",
        // A property filter on the scanned side.
        "MATCH (d:Drug {units: 1}), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(d)",
        // A literal argument, and another function.
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, 1) RETURN count(d)",
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE exists(d.units) RETURN count(d)",
        // Row-count modifiers and extra clauses.
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN d LIMIT 1",
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN d ORDER BY d",
        "OPTIONAL MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(d)",
        // Three paths.
        "MATCH (d:Drug), (r:Class {code: 'C0'}), (z) WHERE subsumes(d, r) RETURN count(d)",
        // A pin that names nothing in the hierarchy.
        "MATCH (d:Drug), (r:Class {code: 'NOPE'}) WHERE subsumes(d, r) RETURN count(d)",
        // An unpinned root.
        "MATCH (d:Drug), (r:Class) WHERE subsumes(d, r) RETURN count(d)",
        // An empty pin, and counting a property rather than the scan.
        "MATCH (d:Drug), (r:Class {}) WHERE subsumes(d, r) RETURN count(d)",
        "MATCH (d:Drug), (r:Class {code: 'C0'}) WHERE subsumes(d, r) RETURN count(d.units)",
    ] {
        assert_eq!(detect_str(q, &f.store), None, "{q}");
    }
}

#[test]
fn order_test_rejects_a_pin_outside_every_usable_hierarchy() {
    let mut f = fixture();
    let loose = f.store.create_node("Class");
    f.store
        .set_column_property(loose, "code", PropertyValue::String("LOOSE".into()));
    assert_eq!(
        detect_str(
            "MATCH (d:Drug), (r:Class {code: 'LOOSE'}) WHERE subsumes(d, r) RETURN count(d)",
            &f.store
        ),
        None
    );
}

// ---------------------------------------------------------------------------
// Hierarchy-driven
// ---------------------------------------------------------------------------

#[test]
fn driven_count_is_detected_in_either_path_order() {
    let f = fixture();
    let expected = Some(HierarchyRewrite::HierarchyDriven {
        index_name: "atc".into(),
        root: f.c0,
        hier_var: "x".into(),
        fact_var: "s".into(),
        fact_labels: vec![Label::new("Sale")],
        edge_type: "OF".into(),
        to_fact: Direction::Incoming,
        output: DrivenOutput::Count {
            alias: "count(s)".into(),
            distinct: false,
        },
    });
    assert_eq!(
        detect_str(
            "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
            &f.store
        ),
        expected
    );
    assert_eq!(
        detect_str(
            "MATCH (r:Class {code: 'C0'}), (s:Sale)-[:OF]->(x) WHERE subsumes(x, r) RETURN count(s)",
            &f.store
        ),
        expected
    );
}

#[test]
fn driven_count_distinct_and_incoming_pattern() {
    let f = fixture();
    match detect_str(
        "MATCH (s)<-[:OF]-(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(DISTINCT s) AS c",
        &f.store,
    ) {
        Some(HierarchyRewrite::HierarchyDriven {
            to_fact,
            output,
            fact_labels,
            ..
        }) => {
            assert_eq!(to_fact, Direction::Outgoing);
            assert!(fact_labels.is_empty());
            assert_eq!(
                output,
                DrivenOutput::Count {
                    alias: "c".into(),
                    distinct: true
                }
            );
        }
        other => panic!("expected a driven rewrite, got {other:?}"),
    }
}

#[test]
fn driven_sum_with_and_without_alias() {
    let f = fixture();
    let out = |q: &str| match detect_str(q, &f.store) {
        Some(HierarchyRewrite::HierarchyDriven { output, .. }) => output,
        other => panic!("{q}: expected a driven rewrite, got {other:?}"),
    };
    assert_eq!(
        out("MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN sum(s.amount)"),
        DrivenOutput::Sum {
            alias: "sum(s.amount)".into(),
            property: "amount".into()
        }
    );
    assert_eq!(
        out("MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN sum(s.amount) AS total"),
        DrivenOutput::Sum {
            alias: "total".into(),
            property: "amount".into()
        }
    );
}

#[test]
fn driven_rejections() {
    let f = fixture();
    for q in [
        // Undirected, typed twice, variable-length, or with a named relationship.
        "MATCH (s:Sale)-[:OF]-(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        "MATCH (s:Sale)-[:OF|IS_A]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        "MATCH (s:Sale)-[:OF*1..2]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        "MATCH (s:Sale)-[e:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        // A constraint on either end of the fact edge.
        "MATCH (s:Sale)-[:OF]->(x:Drug), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        "MATCH (s:Sale {amount: 10})-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        // Anonymous fact.
        "MATCH ()-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(x)",
        // Negated, reversed arguments, literal arguments, another function.
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE NOT subsumes(x, r) RETURN count(s)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(r, x) RETURN count(s)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, 1) RETURN count(s)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE exists(x.units) RETURN count(s)",
        // Projections the rewrite does not produce.
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(x)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN sum(x.units)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN avg(s.amount)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN s",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s), sum(s.amount)",
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN DISTINCT count(s)",
        // Two edges, or none.
        "MATCH (s:Sale)-[:OF]->(x)-[:IS_A]->(y), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        // A root that is not in any usable hierarchy.
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'NOPE'}) WHERE subsumes(x, r) RETURN count(s)",
        // No WHERE at all.
        "MATCH (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) RETURN count(s)",
        // A named path.
        "MATCH p = (s:Sale)-[:OF]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
        // A property on the fact edge, which the walk back from the subtree would drop.
        "MATCH (s:Sale)-[:OF {w: 1}]->(x), (r:Class {code: 'C0'}) WHERE subsumes(x, r) RETURN count(s)",
    ] {
        assert_eq!(detect_str(q, &f.store), None, "{q}");
    }
}

// ---------------------------------------------------------------------------
// Cross-hierarchy driven (#350)
// ---------------------------------------------------------------------------

struct CrossFixture {
    store: GraphStore,
    c0: NodeId,
    r0: NodeId,
    z0: NodeId,
}

/// Two hierarchies over one fact table: `IS_A` (`ROOT <- C0, C1 <- 2 Drugs each`, index
/// `atc`) and `PART_OF` (`R <- R0 <- Z0..Z3`, `R <- R1 <- Z4, Z5`, index `geo`). Each
/// drug has a `Sale -[:OF]->` it, and each sale is `-[:IN]->` a zone. `BOTH` sits in
/// both hierarchies.
fn cross_fixture() -> CrossFixture {
    let mut store = GraphStore::new();
    let coded = |store: &mut GraphStore, label: &str, code: &str| {
        let id = store.create_node(label);
        store.set_column_property(id, "code", PropertyValue::String(code.into()));
        id
    };
    let root = coded(&mut store, "Class", "ROOT");
    let region = coded(&mut store, "Region", "R");
    let r0 = coded(&mut store, "Region", "R0");
    let r1 = coded(&mut store, "Region", "R1");
    store.create_edge(r0, region, "PART_OF").unwrap();
    store.create_edge(r1, region, "PART_OF").unwrap();
    let zones: Vec<NodeId> = (0..6)
        .map(|z| {
            let id = coded(&mut store, "Zone", &format!("Z{z}"));
            store
                .create_edge(id, if z < 4 { r0 } else { r1 }, "PART_OF")
                .unwrap();
            id
        })
        .collect();
    let both = coded(&mut store, "Class", "BOTH");
    store.create_edge(both, root, "IS_A").unwrap();
    store.create_edge(both, region, "PART_OF").unwrap();
    let mut c0 = root;
    let mut n = 0usize;
    for c in 0..2 {
        let mid = coded(&mut store, "Class", &format!("C{c}"));
        store.create_edge(mid, root, "IS_A").unwrap();
        if c == 0 {
            c0 = mid;
        }
        for _ in 0..2 {
            let drug = store.create_node("Drug");
            store.create_edge(drug, mid, "IS_A").unwrap();
            let sale = store.create_node("Sale");
            store.create_edge(sale, drug, "OF").unwrap();
            store
                .create_edge(sale, zones[n % zones.len()], "IN")
                .unwrap();
            n += 1;
        }
    }
    let mgr = std::sync::Arc::clone(&store.hierarchy_index);
    mgr.create(
        &store,
        HierarchySpec::new("atc", vec![EdgeType::new("IS_A")]),
    )
    .unwrap();
    mgr.create(
        &store,
        HierarchySpec::new("geo", vec![EdgeType::new("PART_OF")]),
    )
    .unwrap();
    CrossFixture {
        store,
        c0,
        r0,
        z0: zones[0],
    }
}

#[test]
fn cross_hierarchy_drives_from_the_smallest_subtree() {
    let f = cross_fixture();
    let axis = |index: &str, root, var: &str, edge: &str, size| DrivenAxis {
        index_name: index.into(),
        root,
        hier_var: var.into(),
        edge_type: edge.into(),
        from_fact: Direction::Outgoing,
        subtree_size: size,
    };
    // C0 holds 3 nodes, R0 holds 5: the ontology drives.
    let expected = Some(HierarchyRewrite::CrossHierarchyDriven {
        fact_var: "s".into(),
        fact_labels: vec![Label::new("Sale")],
        driving: axis("atc", f.c0, "x", "OF", 3),
        residual: vec![axis("geo", f.r0, "z", "IN", 5)],
        output: DrivenOutput::Count {
            alias: "n".into(),
            distinct: false,
        },
    });
    for q in [
        "MATCH (s:Sale)-[:OF]->(x), (s)-[:IN]->(z), (r:Class {code: 'C0'}), (g:Region {code: 'R0'}) \
         WHERE subsumes(x, r) AND subsumes(z, g) RETURN count(s) AS n",
        // Pattern and predicate order do not matter, nor which occurrence carries the label.
        "MATCH (g:Region {code: 'R0'}), (s)-[:IN]->(z), (r:Class {code: 'C0'}), (s:Sale)-[:OF]->(x) \
         WHERE subsumes(z, g) AND subsumes(x, r) RETURN count(s) AS n",
    ] {
        let mut got = detect_str(q, &f.store);
        // The second spelling lists the axes in the other order; compare as sets.
        if let Some(HierarchyRewrite::CrossHierarchyDriven { residual, .. }) = &mut got {
            residual.sort_by(|a, b| a.hier_var.cmp(&b.hier_var));
        }
        assert_eq!(got, expected, "{q}");
    }

    // A single zone holds 1 node: now the geography drives.
    match detect_str(
        "MATCH (s:Sale)-[:OF]->(x), (s)-[:IN]->(z), (r:Class {code: 'C0'}), (g:Zone {code: 'Z0'}) \
         WHERE subsumes(x, r) AND subsumes(z, g) RETURN sum(s.amount)",
        &f.store,
    ) {
        Some(HierarchyRewrite::CrossHierarchyDriven {
            driving,
            residual,
            output,
            ..
        }) => {
            assert_eq!(driving, axis("geo", f.z0, "z", "IN", 1));
            assert_eq!(residual, vec![axis("atc", f.c0, "x", "OF", 3)]);
            assert_eq!(
                output,
                DrivenOutput::Sum {
                    alias: "sum(s.amount)".into(),
                    property: "amount".into()
                }
            );
        }
        other => panic!("expected a cross-hierarchy rewrite, got {other:?}"),
    }
}

#[test]
fn cross_hierarchy_reads_each_axis_direction() {
    let f = cross_fixture();
    match detect_str(
        "MATCH (x)<-[:OF]-(s:Sale), (s)-[:IN]->(z), (r:Class {code: 'C0'}), (g:Region {code: 'R0'}) \
         WHERE subsumes(x, r) AND subsumes(z, g) RETURN count(DISTINCT s)",
        &f.store,
    ) {
        // `(x)<-[:OF]-(s)` starts at x, so s is the hierarchy side of that path and the
        // two paths do not share a fact variable: declined, never misread.
        None => {}
        other => panic!("expected no rewrite, got {other:?}"),
    }
    match detect_str(
        "MATCH (s:Sale)-[:OF]->(x), (s)<-[:HAS]-(z), (r:Class {code: 'C0'}), (g:Region {code: 'R0'}) \
         WHERE subsumes(x, r) AND subsumes(z, g) RETURN count(DISTINCT s)",
        &f.store,
    ) {
        Some(HierarchyRewrite::CrossHierarchyDriven { residual, output, .. }) => {
            assert_eq!(residual[0].from_fact, Direction::Incoming);
            assert_eq!(
                output,
                DrivenOutput::Count {
                    alias: "count(s)".into(),
                    distinct: true
                }
            );
        }
        other => panic!("expected a cross-hierarchy rewrite, got {other:?}"),
    }
}

#[test]
fn cross_hierarchy_rejections() {
    let f = cross_fixture();
    const M: &str = "MATCH (s:Sale)-[:OF]->(x), (s)-[:IN]->(z), (r:Class {code: 'C0'}), (g:Region {code: 'R0'})";
    const W: &str = "WHERE subsumes(x, r) AND subsumes(z, g)";
    assert!(
        detect_str(&format!("{M} {W} RETURN count(s)"), &f.store).is_some(),
        "the base shape must be accepted for the rejections below to mean anything"
    );
    let rejected = [
        // A missing, extra, negated, disjoined, repeated or reversed term.
        format!("{M} WHERE subsumes(x, r) RETURN count(s)"),
        format!("{M} {W} AND s.amount > 1 RETURN count(s)"),
        format!("{M} WHERE subsumes(x, r) AND NOT subsumes(z, g) RETURN count(s)"),
        format!("{M} WHERE subsumes(x, r) OR subsumes(z, g) RETURN count(s)"),
        format!("{M} WHERE subsumes(x, r) AND subsumes(x, g) RETURN count(s)"),
        format!("{M} WHERE subsumes(x, r) AND subsumes(z, r) RETURN count(s)"),
        format!("{M} WHERE subsumes(x, r) AND subsumes(g, z) RETURN count(s)"),
        // A pinned node no axis uses would multiply the count.
        format!("{M}, (q:Class {{code: 'C1'}}) {W} RETURN count(s)"),
        // Projections and clauses the plan does not produce.
        format!("{M} {W} RETURN count(x)"),
        format!("{M} {W} RETURN s"),
        format!("{M} {W} RETURN count(s) LIMIT 1"),
        format!("{M} {W} RETURN count(s) ORDER BY count(s)"),
        format!("OPTIONAL {M} {W} RETURN count(s)"),
        // Constraints on the fact, the hierarchy side or the relationship.
        format!("{} {W} RETURN count(s)", M.replace("(s)-[:IN]", "(s {amount: 1})-[:IN]")),
        format!("{} {W} RETURN count(s)", M.replace("->(z)", "->(z:Zone)")),
        format!("{} {W} RETURN count(s)", M.replace("-[:IN]->", "-[:IN {w: 1}]->")),
        format!("{} {W} RETURN count(s)", M.replace("-[:IN]->", "-[:IN]-")),
        format!("{} {W} RETURN count(s)", M.replace("-[:IN]->", "-[:IN*1..2]->")),
        format!("{} {W} RETURN count(s)", M.replace("-[:IN]->", "-[i:IN]->")),
        // Two facts, or a second hop.
        format!("{} {W} RETURN count(s)", M.replace("(s)-[:IN]", "(t)-[:IN]")),
        format!("{} {W} RETURN count(s)", M.replace("-[:IN]->(z)", "-[:IN]->(z)-[:PART_OF]->()")),
        // A pinned root that is also the fact.
        format!(
            "{} WHERE subsumes(x, r) AND subsumes(z, s) RETURN count(s)",
            M.replace("(g:Region", "(s:Region")
        ),
        // A root outside every hierarchy, or in two of them.
        format!("{} {W} RETURN count(s)", M.replace("'R0'", "'NOPE'")),
        format!("{} {W} RETURN count(s)", M.replace("(g:Region {code: 'R0'})", "(g:Class {code: 'BOTH'})")),
        // Both axes over one relationship type: the plan would not keep their edges apart.
        "MATCH (s:Sale)-[:OF]->(x), (s)-[:OF]->(y), (r:Class {code: 'C0'}), (q:Class {code: 'C1'}) \
         WHERE subsumes(x, r) AND subsumes(y, q) RETURN count(s)"
            .to_string(),
    ];
    for q in &rejected {
        assert!(
            !matches!(
                detect_str(q, &f.store),
                Some(HierarchyRewrite::CrossHierarchyDriven { .. })
            ),
            "{q}"
        );
    }
}

// ---------------------------------------------------------------------------
// Roll-up rejections and default aliases
// ---------------------------------------------------------------------------

#[test]
fn rollup_default_aliases_name_the_aggregate() {
    let f = fixture();
    let alias = |q: &str| match detect_str(q, &f.store) {
        Some(HierarchyRewrite::Rollup { alias, .. }) => alias,
        other => panic!("{q}: expected a roll-up, got {other:?}"),
    };
    assert_eq!(
        alias("MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN sum(d.units)"),
        "sum(d.units)"
    );
    assert_eq!(
        alias("MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(d)"),
        "count(d)"
    );
}

#[test]
fn rollup_rejections() {
    let f = fixture();
    for q in [
        // DISTINCT aggregate, unknown monoid, a non-descendant argument.
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(DISTINCT d)",
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN avg(d.units)",
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN sum(r.units)",
        // count of a property is a non-null count, not a structural one.
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(d.units)",
        // sum of the node itself has no measure.
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN sum(d)",
        // A monoid the index did not build.
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN min(d.units)",
        // Descendant scan disqualified by an alias, ORDER BY or DISTINCT.
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN d AS x",
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN d ORDER BY d",
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN DISTINCT d",
        // Undirected walk, named edge, bounded length, edge properties, path variable.
        "MATCH (d)-[:IS_A*0..]-(r:Class {code: 'C0'}) RETURN count(d)",
        "MATCH (d)-[e:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(d)",
        "MATCH (d)-[:IS_A*0..3]->(r:Class {code: 'C0'}) RETURN count(d)",
        "MATCH p = (d)-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(d)",
        // An anonymous descendant.
        "MATCH ()-[:IS_A*0..]->(r:Class {code: 'C0'}) RETURN count(*)",
    ] {
        assert_eq!(detect_str(q, &f.store), None, "{q}");
    }
}

// ---------------------------------------------------------------------------
// Pin resolution
// ---------------------------------------------------------------------------

#[test]
fn a_pin_on_a_row_stored_property_still_resolves() {
    let mut f = fixture();
    f.store
        .get_node_mut(f.c0)
        .unwrap()
        .set_property("tag", PropertyValue::String("first".into()));
    match detect_str(
        "MATCH (d)-[:IS_A*0..]->(r:Class {tag: 'first'}) RETURN count(d)",
        &f.store,
    ) {
        Some(HierarchyRewrite::Rollup { root, .. }) => assert_eq!(root, f.c0),
        other => panic!("expected a roll-up, got {other:?}"),
    }
}

#[test]
fn a_pin_with_a_mismatched_second_property_resolves_to_nothing() {
    let f = fixture();
    assert_eq!(
        detect_str(
            "MATCH (d)-[:IS_A*0..]->(r:Class {code: 'C0', missing: 1}) RETURN count(d)",
            &f.store,
        ),
        None
    );
}
