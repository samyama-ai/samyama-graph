//! Checks the shared code of the graph + vector hybrid benchmark (#1987):
//! the generator, the brute-force reference, the export, the snapshot reader,
//! and that the three Cypher templates return the reference's answers.
//!
//! No timings are asserted. The graphs are small enough for a debug build,
//! and the paper index holds fewer than 128 vectors, which the store searches
//! exactly, so engine and reference must agree exactly.

#[path = "../benches/graph_vector_hybrid/mod.rs"]
mod graph_vector_hybrid;

use graph_vector_hybrid as hy;
use hy::Vectors;
use samyama::query::QueryEngine;

fn tiny() -> hy::Config {
    hy::Config {
        papers: 100,
        people: 60,
        methods: 8,
        problems: 40,
        topics: 4,
        dim: 8,
        cites: 3,
        seed: 7,
    }
}

#[test]
fn the_generator_is_deterministic_and_well_formed() {
    let cfg = tiny();
    let a = hy::generate(&cfg).unwrap();
    assert_eq!(a, hy::generate(&cfg).unwrap(), "same seed, same graph");
    let other = hy::generate(&hy::Config { seed: 8, ..cfg }).unwrap();
    assert_ne!(a, other, "a different seed changes the graph");

    assert_eq!((a.papers.len(), a.papers.dim), (100, 8));
    assert_eq!((a.methods.len(), a.problems.len(), a.people), (8, 40, 60));
    assert!(
        a.cites.iter().all(|(s, t)| t < s),
        "a paper cites only earlier papers"
    );
    assert!(
        a.coauthor.iter().all(|(x, y)| x < y),
        "co-authorship is stored once"
    );
    assert!(a
        .authored
        .iter()
        .all(|(p, x)| (*p as usize) < 60 && (*x as usize) < 100));
    assert!(a
        .uses_method
        .iter()
        .all(|(p, m)| (*p as usize) < 100 && (*m as usize) < 8));
    assert!(a
        .addresses
        .iter()
        .all(|(p, q)| (*p as usize) < 100 && (*q as usize) < 40));
    let mut authored_papers: Vec<u32> = a.authored.iter().map(|e| e.1).collect();
    authored_papers.sort_unstable();
    authored_papers.dedup();
    assert_eq!(authored_papers.len(), 100, "every paper has an author");

    let w = hy::workload(&a, 20, 3);
    assert_eq!(w, hy::workload(&a, 20, 3));
    assert_eq!(
        (w.expand.len(), w.filtered.len(), w.join.len()),
        (20, 20, 20)
    );
    assert!(w.join.iter().all(|m| (*m as usize) < 8));
}

#[test]
fn reference_on_a_hand_built_graph() {
    // Five papers on a line of citations 0<-1<-2<-3<-4, embeddings along x.
    let g = hy::HybridGraph {
        papers: Vectors {
            dim: 2,
            data: vec![1.0, 0.0, 1.0, 0.1, 1.0, 0.5, 0.0, 1.0, 1.0, 0.05],
        },
        people: 3,
        methods: Vectors {
            dim: 2,
            data: vec![1.0, 0.0],
        },
        problems: Vectors {
            dim: 2,
            data: vec![1.0, 0.0, 0.9, 0.1, 0.0, 1.0],
        },
        cites: vec![(1, 0), (2, 1), (3, 2), (4, 3)],
        // person 0 wrote paper 3; 1 wrote 2; 2 wrote 0. 0-1 and 1-2 co-authored.
        authored: vec![(0, 3), (1, 2), (2, 0)],
        coauthor: vec![(0, 1), (1, 2)],
        // Paper 0 uses method 0 and addresses problem 0: that pair is linked.
        uses_method: vec![(0, 0)],
        addresses: vec![(0, 0)],
    };
    let r = hy::Reference::new(&g);

    assert_eq!(
        hy::within(&[vec![1], vec![0, 2], vec![1, 3], vec![2]], 0, 2),
        vec![0, 1, 2]
    );

    // Fewer papers than SEEDS: every paper is a seed, all five come back,
    // ranked by similarity to +x.
    assert_eq!(r.seeded_expansion(&[1.0, 0.0]), vec![0, 4, 1, 2, 3]);

    // Person 0 reaches 1 and 2 in two hops: papers 3, 2 and 0.
    assert_eq!(r.traversal_filtered(0, &[1.0, 0.0]), vec![0, 2, 3]);
    // Person 2 reaches 1 and 0 as well.
    assert_eq!(r.traversal_filtered(2, &[0.0, 1.0]), vec![3, 2, 0]);

    // Problem 0 is the most similar but already linked.
    assert_eq!(r.vector_join(0), vec![1, 2]);

    assert_eq!(hy::compare(&[1, 2], &[2, 1]), (1.0, true));
    assert_eq!(hy::compare(&[1, 3], &[1, 2]), (0.5, false));
    assert_eq!(hy::compare(&[], &[]), (1.0, true));
    assert_eq!(
        hy::top_k([3, 1, 2], 2, |_| 0.5),
        vec![1, 2],
        "ties go to the smaller uid"
    );
}

#[test]
fn the_cypher_templates_return_the_reference_answers() {
    let g = hy::generate(&tiny()).unwrap();
    let r = hy::Reference::new(&g);
    let w = hy::workload(&g, 12, 11);
    let store = hy::load_store(&g).unwrap();
    let engine = QueryEngine::new();

    for q in &w.expand {
        let got = hy::run_template(
            &engine,
            &store,
            hy::T1_SEEDED_EXPANSION,
            &hy::params_expand(q),
        )
        .unwrap();
        let want = r.seeded_expansion(q);
        assert_eq!(want.len(), hy::K_EXPAND);
        assert_eq!(
            hy::compare(&got, &want),
            (1.0, true),
            "seeded expansion: {got:?} vs {want:?}"
        );
    }
    let mut non_empty = 0;
    for (person, q) in &w.filtered {
        let got = hy::run_template(
            &engine,
            &store,
            hy::T2_TRAVERSAL_FILTERED,
            &hy::params_filtered(*person, q),
        )
        .unwrap();
        let want = r.traversal_filtered(*person, q);
        non_empty += usize::from(!want.is_empty());
        assert_eq!(
            hy::compare(&got, &want),
            (1.0, true),
            "person {person}: {got:?} vs {want:?}"
        );
    }
    assert!(
        non_empty > 0,
        "the workload must ask something with an answer"
    );
    for m in &w.join {
        let got =
            hy::run_template(&engine, &store, hy::T3_VECTOR_JOIN, &hy::params_join(*m)).unwrap();
        let want = r.vector_join(*m);
        assert_eq!(want.len(), hy::K_JOIN);
        assert_eq!(
            hy::compare(&got, &want),
            (1.0, true),
            "method {m}: {got:?} vs {want:?}"
        );
    }
}

#[test]
fn a_loaded_store_reads_back_as_the_same_graph() {
    let g = hy::generate(&tiny()).unwrap();
    let store = hy::load_store(&g).unwrap();
    assert_eq!(hy::from_store(&store, "emb").unwrap(), g);

    // Without COAUTHOR edges it is derived from shared authorship, which is
    // how the generator made it.
    let bare = hy::HybridGraph {
        coauthor: Vec::new(),
        ..g.clone()
    };
    let store = hy::load_store(&bare).unwrap();
    assert_eq!(hy::from_store(&store, "emb").unwrap().coauthor, g.coauthor);

    let err = hy::from_store(&store, "missing").unwrap_err();
    assert!(err.contains("no vector in `missing`"), "{err}");
}

#[test]
fn the_export_holds_the_graph_queries_and_answers() {
    let g = hy::generate(&tiny()).unwrap();
    let r = hy::Reference::new(&g);
    let w = hy::workload(&g, 5, 2);
    let dir = std::env::temp_dir().join(format!("gvh-export-{}", std::process::id()));
    hy::export(&dir, &g, &w, &r).unwrap();

    for f in hy::EXPORT_FILES {
        assert!(dir.join(f).is_file(), "{f} missing");
    }
    let papers = std::fs::read_to_string(dir.join("papers.csv")).unwrap();
    let lines: Vec<&str> = papers.lines().collect();
    assert_eq!(lines[0], "uid,emb");
    assert_eq!(lines.len(), 101);
    let first: Vec<f32> = lines[1]
        .split_once(',')
        .unwrap()
        .1
        .split(';')
        .map(|x| x.parse().unwrap())
        .collect();
    assert_eq!(
        first.as_slice(),
        g.papers.row(0),
        "vectors round-trip exactly"
    );
    let cites = std::fs::read_to_string(dir.join("cites.csv")).unwrap();
    assert_eq!(cites.lines().count(), g.cites.len() + 1);

    let q: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("queries.json")).unwrap()).unwrap();
    assert_eq!(q["seeded_expansion"]["cypher"], hy::T1_SEEDED_EXPANSION);
    assert_eq!(q["traversal_filtered"]["cypher"], hy::T2_TRAVERSAL_FILTERED);
    assert_eq!(q["vector_join"]["cypher"], hy::T3_VECTOR_JOIN);
    let t2 = &q["traversal_filtered"]["instances"];
    assert_eq!(t2.as_array().unwrap().len(), 5);
    assert_eq!(t2[0]["person"], w.filtered[0].0);
    let want = r.traversal_filtered(w.filtered[0].0, &w.filtered[0].1);
    assert_eq!(t2[0]["reference"], serde_json::json!(want));
    let meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["people"], 60);
    assert_eq!(meta["dim"], 8);
    std::fs::remove_dir_all(&dir).ok();
}
