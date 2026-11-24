//! ML-14 (#1772): an evaluation set generated from a knowledge graph.
//!
//! What has to be true of a generated set for anyone to grade a model with it:
//! every gold answer is what the engine returns; the unanswerable items really
//! have no answer; difficulty labels are present and spread; the same graph and
//! seed give the same set; values are bound, never spliced into the query; and
//! the set survives a snapshot round trip, so `verify` can re-check it later.

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::eval_gen::{generate, EvalGenOptions, EvalSet, MAX_ANSWER_ROWS};
use samyama::snapshot::verify::{kg08_conformance, run_bound, verify};

/// A small drug-discovery graph: 40 drugs, 25 diseases, 30 genes, three
/// relationship types, numeric properties on two labels.
fn pharma() -> GraphStore {
    let mut g = GraphStore::new();
    let mut drugs = Vec::new();
    for i in 0..40i64 {
        let n = g.create_node("Drug");
        let node = g.get_node_mut(n).unwrap();
        node.set_property("name", format!("drug-{i:02}"));
        node.set_property("approved_year", 1980 + i);
        node.set_property("price", 10.0 + i as f64 * 1.5);
        drugs.push(n);
    }
    let mut diseases = Vec::new();
    for i in 0..25i64 {
        let n = g.create_node("Disease");
        let node = g.get_node_mut(n).unwrap();
        node.set_property("name", format!("disease-{i:02}"));
        node.set_property("prevalence", 100 + i * 7);
        diseases.push(n);
    }
    let mut genes = Vec::new();
    for i in 0..30i64 {
        let n = g.create_node("Gene");
        g.get_node_mut(n)
            .unwrap()
            .set_property("symbol", format!("G{i:03}"));
        genes.push(n);
    }
    for (i, d) in drugs.iter().enumerate() {
        for k in 0..(1 + i % 4) {
            g.create_edge(*d, diseases[(i * 3 + k) % diseases.len()], "TREATS")
                .unwrap();
        }
        for k in 0..(1 + i % 3) {
            g.create_edge(*d, genes[(i * 5 + k) % genes.len()], "TARGETS")
                .unwrap();
        }
    }
    for (i, gene) in genes.iter().enumerate() {
        g.create_edge(*gene, diseases[(i * 2) % diseases.len()], "ASSOCIATED_WITH")
            .unwrap();
    }
    g
}

fn gen(g: &GraphStore, count: usize, seed: u64) -> EvalSet {
    generate(g, &EvalGenOptions { count, seed }).expect("generate")
}

#[test]
fn every_gold_answer_is_what_the_engine_returns() {
    let g = pharma();
    let set = gen(&g, 120, 1);
    assert!(set.catalog.entries.len() >= 100, "{:?}", set.report);
    for e in &set.catalog.entries {
        let batch = run_bound(&g, &e.cypher, &e.params).unwrap();
        let answer = e
            .answer
            .as_ref()
            .unwrap_or_else(|| panic!("{}: no answer", e.id));
        let rows = answer["rows"].as_array().unwrap();
        assert_eq!(rows.len(), batch.records.len(), "{}", e.id);
        assert_eq!(rows.len(), e.rows, "{}", e.id);
        assert!(rows.len() <= MAX_ANSWER_ROWS, "{}", e.id);
        assert_eq!(
            answer["columns"].as_array().unwrap().len(),
            batch.columns.len(),
            "{}",
            e.id
        );
        if !e.unanswerable {
            assert!(
                rows.iter()
                    .any(|r| r.as_array().unwrap().iter().any(|v| !v.is_null())),
                "{}: null gold",
                e.id
            );
        }
    }
    let report = verify(&g, &set.catalog).unwrap();
    assert_eq!(report.failed().count(), 0);
}

#[test]
fn a_lookup_answer_holds_the_value_from_the_graph() {
    let g = pharma();
    let set = gen(&g, 200, 2);
    let lookup = set
        .catalog
        .entries
        .iter()
        .find(|e| {
            e.id.starts_with("easy-lookup")
                && e.cypher.contains("`approved_year`")
                && e.cypher.contains("`name` = $value")
        })
        .expect("a lookup of approved_year by name");
    let name = lookup.params[0].sample.as_str().unwrap();
    let i: i64 = name.trim_start_matches("drug-").parse().unwrap();
    assert_eq!(
        lookup.answer.as_ref().unwrap()["rows"][0][0],
        serde_json::json!(1980 + i)
    );
    assert!(lookup.question.contains(name), "{}", lookup.question);
}

#[test]
fn unanswerable_items_are_present_and_answer_nothing() {
    let g = pharma();
    let set = gen(&g, 100, 3);
    let unanswerable: Vec<_> = set
        .catalog
        .entries
        .iter()
        .filter(|e| e.unanswerable)
        .collect();
    assert_eq!(unanswerable.len(), 10, "10% of 100");
    let kinds: BTreeSet<&str> = unanswerable
        .iter()
        .map(|e| e.id.rsplit_once('-').unwrap().0)
        .collect();
    assert_eq!(kinds.len(), 2, "both kinds of unanswerable item: {kinds:?}");
    for e in unanswerable {
        assert_eq!(e.rows, 0, "{}", e.id);
        assert!(
            run_bound(&g, &e.cypher, &e.params)
                .unwrap()
                .records
                .is_empty(),
            "{}",
            e.id
        );
        assert_eq!(
            e.answer.as_ref().unwrap()["rows"],
            serde_json::json!([]),
            "{}",
            e.id
        );
    }
}

#[test]
fn difficulty_labels_follow_the_stated_mix() {
    let g = pharma();
    let set = gen(&g, 100, 4);
    assert!(
        set.report.shortfall.is_empty(),
        "{:?}",
        set.report.shortfall
    );
    assert_eq!(set.catalog.entries.len(), 100);
    // 90 answerable: 35% easy (31), 35% medium (31), the rest hard (28).
    let r = &set.report;
    assert_eq!(r.by_difficulty.get("easy"), Some(&31));
    assert_eq!(r.by_difficulty.get("medium"), Some(&31));
    assert_eq!(r.by_difficulty.get("hard"), Some(&28));
    assert_eq!(r.unanswerable, 10);
    for e in &set.catalog.entries {
        assert!(
            ["easy", "medium", "hard"].contains(&e.difficulty.as_str()),
            "{}",
            e.id
        );
        assert!(!e.question.trim().is_empty(), "{}", e.id);
    }
    // Every bucket uses more than one template: no single shape fills it.
    for prefix in ["easy-", "medium-", "hard-"] {
        let templates: BTreeSet<_> = r
            .by_template
            .keys()
            .filter(|t| t.starts_with(prefix))
            .collect();
        assert!(templates.len() >= 2, "{prefix}: {templates:?}");
    }
    assert!(
        kg08_conformance(&set.catalog).is_empty(),
        "{:?}",
        kg08_conformance(&set.catalog)
    );
}

#[test]
fn the_same_graph_and_seed_give_the_same_set() {
    // Two stores built the same way: their label sets iterate in different
    // orders (each HashSet has its own random state), so this also proves
    // the generator does not depend on that order.
    let a = gen(&pharma(), 80, 42);
    let b = gen(&pharma(), 80, 42);
    assert_eq!(
        serde_json::to_string(&a.catalog).unwrap(),
        serde_json::to_string(&b.catalog).unwrap()
    );
    assert!(
        a.catalog.generated_by.ends_with("seed=42"),
        "{}",
        a.catalog.generated_by
    );
    let c = gen(&pharma(), 80, 43);
    assert_ne!(
        serde_json::to_string(&a.catalog.entries).unwrap(),
        serde_json::to_string(&c.catalog.entries).unwrap(),
        "a different seed picks different questions"
    );
}

#[test]
fn values_are_bound_never_spliced_and_no_question_repeats() {
    let g = pharma();
    let set = gen(&g, 150, 5);
    let mut seen = BTreeSet::new();
    for e in &set.catalog.entries {
        for p in &e.params {
            let text = match &p.sample {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            assert!(
                !e.cypher.contains(&text),
                "{}: the value {text} is in the query text",
                e.id
            );
            assert!(e.cypher.contains(&format!("${}", p.name)), "{}", e.id);
        }
        let key = (e.cypher.clone(), serde_json::to_string(&e.params).unwrap());
        assert!(seen.insert(key), "{} repeats another question", e.id);
    }
    let ids: BTreeSet<_> = set.catalog.entries.iter().map(|e| &e.id).collect();
    assert_eq!(ids.len(), set.catalog.entries.len(), "ids are unique");
}

#[test]
fn the_set_survives_a_snapshot_round_trip() {
    let g = pharma();
    let set = gen(&g, 60, 6);
    let mut bytes = Vec::new();
    samyama::snapshot::export_tenant(&g, &mut bytes).unwrap();
    let mut restored = GraphStore::new();
    samyama::snapshot::import_tenant(&mut restored, &bytes[..]).unwrap();
    let report = verify(&restored, &set.catalog).unwrap();
    assert_eq!(
        report.failed().count(),
        0,
        "{:?}",
        report.failed().collect::<Vec<_>>()
    );
    assert!(!report.everything_empty);
}

#[test]
fn a_small_graph_yields_fewer_questions_and_says_why() {
    let mut g = GraphStore::new();
    let a = g.create_node("Thing");
    g.get_node_mut(a).unwrap().set_property("name", "only");
    let set = gen(&g, 500, 0);
    assert!(set.report.generated < 500);
    assert_eq!(set.report.generated, set.catalog.entries.len());
    assert!(!set.report.shortfall.is_empty());
    let distinct: BTreeSet<_> = set
        .catalog
        .entries
        .iter()
        .map(|e| (&e.cypher, serde_json::to_string(&e.params).unwrap()))
        .collect();
    assert_eq!(
        distinct.len(),
        set.catalog.entries.len(),
        "nothing repeated to reach the count"
    );
}

#[test]
fn names_that_need_quoting_work_and_unquotable_ones_are_skipped() {
    let mut g = GraphStore::new();
    for i in 0..12i64 {
        let n = g.create_node("Clinical Site");
        g.get_node_mut(n)
            .unwrap()
            .set_property("site name", format!("site {i}"));
        let bad = g.create_node("We`ird");
        g.get_node_mut(bad)
            .unwrap()
            .set_property("name", format!("w{i}"));
    }
    let set = gen(&g, 20, 0);
    assert!(set
        .catalog
        .entries
        .iter()
        .any(|e| e.cypher.contains("`Clinical Site`")));
    assert!(set
        .catalog
        .entries
        .iter()
        .all(|e| !e.cypher.contains("We`ird") && !e.question.contains("We`ird")));
    assert_eq!(verify(&g, &set.catalog).unwrap().failed().count(), 0);
}

#[test]
fn an_empty_graph_or_a_zero_count_is_an_error() {
    assert!(generate(&GraphStore::new(), &EvalGenOptions::default()).is_err());
    assert!(generate(&pharma(), &EvalGenOptions { count: 0, seed: 0 }).is_err());
}

#[test]
fn a_value_typed_as_an_integer_key_is_bound_as_an_integer() {
    let mut g = GraphStore::new();
    for i in 0..15i64 {
        let n = g.create_node("Ticket");
        let node = g.get_node_mut(n).unwrap();
        node.set_property("id", PropertyValue::Integer(1000 + i));
        node.set_property("status", if i % 2 == 0 { "open" } else { "closed" });
    }
    let set = gen(&g, 30, 0);
    let lookups: Vec<_> = set
        .catalog
        .entries
        .iter()
        .filter(|e| e.id.starts_with("easy-lookup"))
        .collect();
    assert!(!lookups.is_empty());
    for e in lookups {
        assert_eq!(e.params[0].kind, "int", "{}", e.id);
        assert!(e.params[0].sample.is_i64(), "{}", e.id);
    }
}

#[test]
fn a_key_must_be_unique_across_the_whole_label_not_just_the_sample() {
    // `name` ranks first as a key and is unique on all but two of 300
    // nodes; the 200-node sample may well miss the pair. `pid` is unique.
    let mut g = GraphStore::new();
    for i in 0..300i64 {
        let n = g.create_node("Person");
        let node = g.get_node_mut(n).unwrap();
        node.set_property(
            "name",
            if i == 299 {
                "p0".to_string()
            } else {
                format!("p{i}")
            },
        );
        node.set_property("pid", format!("ID-{i:04}"));
        node.set_property("city", format!("c{}", i % 7));
    }
    for seed in 0..5 {
        let set = gen(&g, 40, seed);
        for e in set.catalog.entries.iter().filter(|e| !e.params.is_empty()) {
            assert!(
                e.cypher.contains("`pid` = $value"),
                "seed {seed}, {}: {}",
                e.id,
                e.cypher
            );
        }
        for e in set
            .catalog
            .entries
            .iter()
            .filter(|e| e.id.starts_with("easy-lookup"))
        {
            assert_eq!(e.rows, 1, "a key names one node: {}", e.id);
        }
    }
}

#[test]
fn a_ranking_where_every_count_ties_is_not_kept() {
    // Every match is played at exactly one stadium: "the top 5 by stadiums"
    // would be the tie-break order and nothing else.
    let mut g = GraphStore::new();
    let stadium = g.create_node("Stadium");
    g.get_node_mut(stadium)
        .unwrap()
        .set_property("name", "only");
    for i in 0..20i64 {
        let m = g.create_node("Match");
        g.get_node_mut(m)
            .unwrap()
            .set_property("name", format!("m{i:02}"));
        g.create_edge(m, stadium, "PLAYED_AT").unwrap();
    }
    let set = gen(&g, 60, 0);
    assert!(
        !set.report.by_template.contains_key("hard-top"),
        "{:?}",
        set.report.by_template
    );
}

#[test]
fn unanswerable_lookups_are_near_misses_not_obvious_fakes() {
    let g = pharma();
    let set = gen(&g, 100, 0);
    let misses: Vec<_> = set
        .catalog
        .entries
        .iter()
        .filter(|e| e.id.starts_with("unanswerable-missing-node"))
        .collect();
    assert!(!misses.is_empty());
    for e in misses {
        let v = e.params[0].sample.as_str().unwrap_or_default().to_string();
        // Real keys look like `drug-07`, `disease-12`, `G004`.
        assert!(
            v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "{}: {v}",
            e.id
        );
        assert!(v.len() <= 16, "{}: {v}", e.id);
    }
}

#[test]
fn an_answer_longer_than_the_cap_or_null_is_not_kept() {
    // `flu` is treated by all 40 drugs: "which drugs treat flu" has 40 rows.
    // Disease d00 has no prevalence: its lookup and any average over it are null.
    let mut g = GraphStore::new();
    let mut diseases = Vec::new();
    for i in 0..20i64 {
        let d = g.create_node("Disease");
        let node = g.get_node_mut(d).unwrap();
        node.set_property(
            "name",
            if i == 19 {
                "flu".to_string()
            } else {
                format!("d{i:02}")
            },
        );
        if i != 0 {
            node.set_property("prevalence", 100 + i);
        }
        diseases.push(d);
    }
    for i in 0..40usize {
        let m = g.create_node("Drug");
        g.get_node_mut(m)
            .unwrap()
            .set_property("name", format!("m{i:02}"));
        g.create_edge(m, diseases[19], "TREATS").unwrap();
        if i < 19 {
            g.create_edge(m, diseases[i], "TREATS").unwrap();
        }
    }
    for seed in 0..4 {
        let set = gen(&g, 300, seed);
        for e in &set.catalog.entries {
            assert!(
                e.rows <= MAX_ANSWER_ROWS,
                "seed {seed}, {}: {} rows",
                e.id,
                e.rows
            );
            if !e.unanswerable {
                let rows = e.answer.as_ref().unwrap()["rows"]
                    .as_array()
                    .unwrap()
                    .clone();
                assert!(
                    rows.iter()
                        .flat_map(|r| r.as_array().unwrap().clone())
                        .any(|v| !v.is_null()),
                    "seed {seed}, {}: null gold",
                    e.id
                );
            }
        }
    }
}

#[test]
fn a_property_missing_on_some_nodes_is_not_a_key() {
    // `name` is unique wherever present but absent on one node of 1,000, which
    // the 200-node sample usually misses; `pid` is on every node.
    let mut g = GraphStore::new();
    for i in 0..1000i64 {
        let n = g.create_node("Person");
        let node = g.get_node_mut(n).unwrap();
        if i != 500 {
            node.set_property("name", format!("p{i}"));
        }
        node.set_property("pid", format!("ID-{i:04}"));
        node.set_property("city", format!("c{}", i % 7));
    }
    for seed in 0..3 {
        let set = gen(&g, 30, seed);
        assert!(
            set.catalog
                .entries
                .iter()
                .filter(|e| !e.params.is_empty())
                .all(|e| e.cypher.contains("`pid` = $value")),
            "seed {seed}"
        );
    }
}
