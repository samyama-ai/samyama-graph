//! The biomedical vector benchmark's own logic (#1281).
//!
//! `benches/biomedical_vectors.rs` reports recall per type subset and the
//! recall a quantization costs against f32. A bench target cannot run its own
//! tests, so what derives those figures lives in
//! `benches/biomedical_vector_common` and is checked here: the synthetic
//! corpus is deterministic and biomedical-shaped, type subsets map back to
//! the right vectors, per-type answers merge correctly, the delta is taken
//! against the right baseline, and checkpoint bytes are counted by kind.
//!
//! Nothing here times anything.

#[path = "../benches/biomedical_vector_common/mod.rs"]
mod biomedical_vector_common;

use biomedical_vector_common::*;
use samyama::graph::NodeId;
use samyama::vector::index::Quantization;
use samyama::vector::{DistanceMetric, HnswParams, IndexKind, IvfParams, VectorIndexManager};

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

// ------------------------------------------------------------------ apportion

#[test]
fn apportion_sums_to_n_and_follows_the_weights() {
    let weights: Vec<u32> = CONCEPT_TYPES.iter().map(|t| t.1).collect();
    for n in [0, 1, 7, 99, 100, 1_000, 12_345] {
        let counts = apportion(n, &weights);
        assert_eq!(counts.iter().sum::<usize>(), n, "n={n}");
    }
    // Weights summing to 100 split 1,000 exactly.
    assert_eq!(
        apportion(1_000, &weights),
        vec![240, 200, 160, 120, 100, 80, 60, 40]
    );
    // Remainders go to the largest fractional share, ties to the earlier.
    assert_eq!(apportion(5, &[1, 1, 1]), vec![2, 2, 1]);
    assert_eq!(apportion(4, &[0, 0]), vec![0, 0]);
}

// ----------------------------------------------------------- synthetic corpus

#[test]
fn the_synthetic_corpus_is_a_function_of_its_seed() {
    let (a_base, a_queries) = synthetic(Level::Concept, 800, 20, 32, 7);
    let (b_base, b_queries) = synthetic(Level::Concept, 800, 20, 32, 7);
    assert_eq!(a_base, b_base);
    assert_eq!(a_queries, b_queries);
    let (c_base, _) = synthetic(Level::Concept, 800, 20, 32, 8);
    assert_ne!(a_base.data, c_base.data);
}

#[test]
fn concept_corpus_has_every_type_in_its_share() {
    let (base, queries) = synthetic(Level::Concept, 1_000, 50, 24, 1281);
    let names: Vec<&str> = CONCEPT_TYPES.iter().map(|t| t.0).collect();
    assert_eq!(base.type_names, names);
    let weights: Vec<u32> = CONCEPT_TYPES.iter().map(|t| t.1).collect();
    assert_eq!(base.type_counts(), apportion(1_000, &weights));
    assert_eq!(queries.type_counts(), apportion(50, &weights));
    for wanted in [
        "Drug",
        "Disease",
        "Gene",
        "Protein",
        "Target",
        "Pathway",
        "MeSHTerm",
        "ClinicalCondition",
    ] {
        assert!(base.type_id(wanted).is_some(), "{wanted}");
    }
    // Shuffled, not loaded one type after another.
    assert!(base.types.windows(2).any(|w| w[0] > w[1]));
}

#[test]
fn synthetic_vectors_are_unit_length_at_the_given_dimension() {
    for level in Level::ALL {
        let (base, queries) = synthetic(level, 300, 10, 48, 3);
        for c in [&base, &queries] {
            assert_eq!(c.dim, 48);
            assert_eq!(c.data.len(), c.len() * 48);
            for i in 0..c.len() {
                let norm = c.row(i).iter().map(|x| x * x).sum::<f32>().sqrt();
                assert!((norm - 1.0).abs() < 1e-4, "{level:?} row {i}: {norm}");
            }
        }
    }
}

#[test]
fn queries_are_new_points_not_copies_of_base_vectors() {
    let (base, queries) = synthetic(Level::Concept, 500, 40, 32, 11);
    for qi in 0..queries.len() {
        let q = queries.row(qi);
        assert!((0..base.len()).all(|bi| base.row(bi) != q), "query {qi}");
    }
}

#[test]
fn article_level_is_one_type() {
    let (base, _) = synthetic(Level::Article, 400, 5, 32, 2);
    assert_eq!(base.type_names, vec![ARTICLE_TYPE.to_string()]);
    assert_eq!(base.type_counts(), vec![400]);
}

#[test]
fn nearest_neighbours_mostly_share_a_type() {
    // What makes a type filter non-trivial and a type subset searchable:
    // vectors cluster by type. Chance agreement over these shares is about
    // 0.16; a corpus without type structure would sit there.
    let (c, _) = synthetic(Level::Concept, 1_500, 1, 48, 5);
    let mut same = 0;
    for i in 0..c.len() {
        let nearest = (0..c.len())
            .filter(|j| *j != i)
            .min_by(|a, b| l2(c.row(i), c.row(*a)).total_cmp(&l2(c.row(i), c.row(*b))))
            .unwrap();
        same += (c.types[i] == c.types[nearest]) as usize;
    }
    let rate = same as f64 / c.len() as f64;
    assert!(rate > 0.6, "1-NN same-type rate {rate}");
}

#[test]
fn related_types_sit_nearer_each_other_than_unrelated_ones() {
    // Gene and Protein share a family; Gene and Drug do not.
    let (c, _) = synthetic(Level::Concept, 2_000, 1, 48, 9);
    let of = |name: &str| positions_of(&c.types, c.type_id(name).unwrap());
    let mean = |a: &[u32], b: &[u32]| {
        let mut sum = 0.0f64;
        let mut n = 0usize;
        for x in a.iter().take(100) {
            for y in b.iter().take(100) {
                sum += l2(c.row(*x as usize), c.row(*y as usize)) as f64;
                n += 1;
            }
        }
        sum / n as f64
    };
    let (gene, protein, drug) = (of("Gene"), of("Protein"), of("Drug"));
    let related = mean(&gene, &protein);
    let unrelated = mean(&gene, &drug);
    assert!(
        related < unrelated,
        "Gene-Protein {related} vs Gene-Drug {unrelated}"
    );
}

// --------------------------------------------------------- prepared directory

#[test]
fn types_files_must_name_one_valid_type_per_vector() {
    assert_eq!(
        parse_types("Drug\nGene\nMeSHTerm\n", 3, "t").unwrap(),
        vec!["Drug", "Gene", "MeSHTerm"]
    );
    assert!(parse_types("Drug\nGene\n", 3, "t")
        .unwrap_err()
        .contains("2 types for 3 vectors"));
    // A blank line would shift every later type onto the wrong vector.
    assert!(parse_types("Drug\n\nGene\n", 3, "t").is_err());
    // A label becomes a checkpoint file name.
    assert!(parse_types("Drug/x\n", 1, "t").is_err());
    assert!(parse_types("9Drug\n", 1, "t").is_err());
    assert!(valid_type_name("Clinical_Condition2"));
}

#[test]
fn interned_type_ids_follow_sorted_names() {
    let names: Vec<String> = ["Gene", "Drug", "Gene", "Article"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let (ids, distinct) = intern(&names);
    assert_eq!(distinct, vec!["Article", "Drug", "Gene"]);
    assert_eq!(ids, vec![2, 1, 2, 0]);
}

// ------------------------------------------------------- subsets and results

#[test]
fn subset_ground_truth_maps_back_to_corpus_positions() {
    let types = vec![0u16, 1, 0, 2, 1, 0];
    let positions = positions_of(&types, 0);
    assert_eq!(positions, vec![0, 2, 5]);
    let data: Vec<f32> = (0..12).map(|v| v as f32).collect(); // 6 rows of 2
    assert_eq!(
        gather(&data, 2, &positions),
        vec![0.0, 1.0, 4.0, 5.0, 10.0, 11.0]
    );
    // Subset row 2 is corpus position 5.
    assert_eq!(remap(&[2, 0, 1], &positions), vec![5, 0, 2]);
}

#[test]
fn per_type_answers_merge_by_distance_then_id() {
    let lists = vec![
        vec![(10, 0.5), (11, 0.9)],
        vec![(20, 0.1), (21, 0.5)],
        vec![(30, 0.7), (10, 0.2)],
    ];
    // 10 is kept once, at its best distance; ties by id.
    assert_eq!(merge_top_k(&lists, 4), vec![20, 10, 21, 30]);
    assert_eq!(merge_top_k(&lists, 1), vec![20]);
    assert!(merge_top_k(&[], 3).is_empty());
}

// ------------------------------------------------------------ accuracy impact

fn row(group: &str, q: &str, width: usize, recall: f64) -> Measured {
    Measured {
        group: group.into(),
        quantization: q.into(),
        width,
        recall,
    }
}

#[test]
fn recall_delta_is_against_f32_at_the_same_group_and_width() {
    let rows = vec![
        row("concept/shared/hnsw/all", "f32", 64, 0.98),
        row("concept/shared/hnsw/all", "fp16", 64, 0.97),
        row("concept/shared/hnsw/all", "fp16", 128, 0.99),
        row("concept/shared/hnsw/all", "f32", 128, 0.995),
        row("concept/shared/hnsw/Drug", "fp16", 64, 0.90),
    ];
    let d = recall_deltas(&rows);
    assert_eq!(d[0], Some(0.0));
    assert!((d[1].unwrap() + 0.01).abs() < 1e-12);
    assert!((d[2].unwrap() + 0.005).abs() < 1e-12);
    assert_eq!(d[3], Some(0.0));
    // No f32 row for the Drug subset: unknown, not zero.
    assert_eq!(d[4], None);
}

#[test]
fn quantizations_are_selected_from_what_the_build_has() {
    let known: &[(&str, Quantization)] =
        &[("f32", Quantization::None), ("fp16", Quantization::Fp16)];
    let got = select(
        "fp16, f32,fp16",
        known,
        "--quantization",
        Quantization::parse,
    )
    .unwrap();
    assert_eq!(
        got,
        vec![("fp16", Quantization::Fp16), ("f32", Quantization::None)]
    );
    // An alias resolves to the row name.
    let got = select("half", known, "--quantization", Quantization::parse).unwrap();
    assert_eq!(got, vec![("fp16", Quantization::Fp16)]);
    // A quantization this build does not have names the ones it does.
    let err = select("f32,int8", known, "--quantization", Quantization::parse).unwrap_err();
    assert!(err.contains("`int8`") && err.contains("f32, fp16"), "{err}");
    assert!(select(" , ", known, "--quantization", Quantization::parse).is_err());
}

#[test]
fn per_vector_and_share_are_safe_at_zero() {
    assert_eq!(per_vector(1_000, 0), 0.0);
    assert_eq!(per_vector(1_000, 4), 250.0);
    assert_eq!(share(3, 0), 0.0);
    assert_eq!(share(1, 4), 0.25);
}

// ------------------------------------------------------------------ disk size

#[test]
fn checkpoint_files_are_counted_by_kind() {
    let mut b = CheckpointBytes::default();
    b.add("Drug_embedding.hnsw", 100);
    b.add("Drug_embedding.hnsw.topology.ids", 10);
    b.add("Drug_embedding.hnsw.topology.hnsw.graph", 20);
    b.add("Drug_embedding.hnsw.topology.hnsw.data", 30);
    b.add("Article_embedding.hnsw.partition.ivf", 5);
    b.add("metadata.json", 2);
    b.add("CURRENT", 1);
    assert_eq!(b.vector_lists, 100);
    assert_eq!(b.graphs, 60);
    assert_eq!(b.centroids, 5);
    assert_eq!(b.other, 3);
    assert_eq!(b.total(), 168);
}

/// A shared-layout manager over a small synthetic concept corpus.
fn shared_index(corpus: &Corpus, kind: IndexKind) -> VectorIndexManager {
    let manager = VectorIndexManager::new();
    manager
        .create_index_with_kind(
            None,
            Level::Concept.label(),
            "embedding",
            corpus.dim,
            DistanceMetric::Cosine,
            Quantization::None,
            HnswParams::default(),
            kind,
        )
        .unwrap();
    let index = manager
        .get_index(Level::Concept.label(), "embedding")
        .unwrap();
    let mut index = index.write().unwrap();
    for i in 0..corpus.len() {
        index
            .add_untrained(NodeId::new(i as u64), corpus.row(i))
            .unwrap();
    }
    index.train_if_due();
    drop(index);
    manager
}

#[test]
fn a_real_checkpoint_is_counted_by_kind() {
    let (corpus, _) = synthetic(Level::Concept, 400, 1, 16, 4);
    let floor = (corpus.len() * corpus.dim * 4) as u64;

    let dir = tempfile::tempdir().unwrap();
    let hnsw = dir.path().join("hnsw");
    shared_index(&corpus, IndexKind::Hnsw)
        .checkpoint_all(&hnsw)
        .unwrap();
    let b = checkpoint_bytes(&hnsw).unwrap();
    assert!(b.vector_lists >= floor, "{b:?}");
    assert!(b.graphs > 0, "{b:?}");
    assert_eq!(b.centroids, 0, "{b:?}");
    assert!(b.other > 0, "{b:?}");

    // 400 vectors is past IVF's training threshold, so centroids are written.
    let ivf = dir.path().join("ivf");
    let kind = IndexKind::Ivf(IvfParams {
        nlist: None,
        nprobe: None,
    });
    shared_index(&corpus, kind).checkpoint_all(&ivf).unwrap();
    let b = checkpoint_bytes(&ivf).unwrap();
    assert!(b.vector_lists >= floor, "{b:?}");
    assert!(b.centroids > 0, "{b:?}");
    assert_eq!(b.graphs, 0, "{b:?}");
}

#[test]
fn a_type_filtered_search_answers_only_that_type() {
    // The bench's shared layout: one index, the type applied as a filter.
    let (corpus, queries) = synthetic(Level::Concept, 600, 5, 16, 6);
    let drug = corpus.type_id("Drug").unwrap();
    for kind in [
        IndexKind::Hnsw,
        IndexKind::Ivf(IvfParams {
            nlist: None,
            nprobe: None,
        }),
    ] {
        let manager = shared_index(&corpus, kind);
        for qi in 0..queries.len() {
            let hits = manager
                .search_filtered_ef(
                    Level::Concept.label(),
                    "embedding",
                    queries.row(qi),
                    10,
                    Some(32),
                    |id| corpus.types[id.as_u64() as usize] == drug,
                )
                .unwrap();
            assert_eq!(hits.len(), 10, "{kind:?} query {qi}");
            for (id, _) in hits {
                assert_eq!(corpus.types[id.as_u64() as usize], drug, "{kind:?}");
            }
        }
    }
}
