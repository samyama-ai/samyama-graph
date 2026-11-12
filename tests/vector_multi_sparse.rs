//! Multi-vector (late-interaction) and sparse vector scoring (#1978).
//!
//! `vector.similarity.maxSim(query, doc[, metric])` is ColBERT's MaxSim: for
//! each query token, its best dot product (or cosine) against any document
//! token, summed over the query tokens. `vector.similarity.sparseDot(a, b)`
//! is the inner product of two sparse vectors, the score of learned sparse
//! retrievers such as SPLADE.
//!
//! Both are query-time functions over ordinary property values -- there is no
//! multi-vector or sparse index yet. Every expected number below is worked by
//! hand in the comment above its assertion.

use samyama::graph::GraphStore;
use samyama::graph::PropertyValue;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;

const TOL: f64 = 1e-6;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

fn rows(s: &GraphStore, q: &str) -> Vec<Record> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
        .records
}

fn error(q: &str) -> String {
    let s = GraphStore::new();
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match QueryExecutor::new(&s).execute(&query) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
    }
}

fn float(r: &Record, col: &str) -> f64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        other => panic!("{col}: {other:?}"),
    }
}

fn string(r: &Record, col: &str) -> String {
    match r.get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("{col}: {other:?}"),
    }
}

fn is_null(r: &Record, col: &str) -> bool {
    matches!(
        r.get(col),
        None | Some(Value::Null) | Some(Value::Property(PropertyValue::Null))
    )
}

/// `RETURN <expr> AS r`, as a float.
fn eval(expr: &str) -> f64 {
    let s = GraphStore::new();
    float(&rows(&s, &format!("RETURN {expr} AS r"))[0], "r")
}

fn assert_close(got: f64, want: f64, what: &str) {
    assert!(
        (got - want).abs() < TOL * want.abs().max(1.0),
        "{what}: got {got}, want {want}"
    );
}

fn assert_error(q: &str, needles: &[&str]) {
    let e = error(q);
    for n in needles {
        assert!(e.contains(n), "{q}: error {e:?} does not mention {n:?}");
    }
}

// ---------------------------------------------------------------------------
// vector.similarity.maxSim
// ---------------------------------------------------------------------------

const Q: &str = "[[1.0, 0.0], [0.0, 1.0]]";
const D: &str = "[[0.5, 0.5], [1.0, 0.0], [0.0, 2.0]]";

/// Dot products, the default and ColBERT's own (its tokens are normalised).
#[test]
fn maxsim_sums_each_query_tokens_best_dot_product() {
    // q1 = [1,0] against D: 0.5, 1, 0 -> 1.  q2 = [0,1]: 0.5, 0, 2 -> 2.
    assert_close(
        eval(&format!("vector.similarity.maxSim({Q}, {D})")),
        3.0,
        "dot",
    );
    // Named explicitly, any case.
    assert_close(
        eval(&format!("vector.similarity.maxSim({Q}, {D}, 'dot')")),
        3.0,
        "'dot'",
    );
    assert_close(
        eval(&format!("VECTOR.SIMILARITY.MAXSIM({Q}, {D}, 'DOT')")),
        3.0,
        "upper-case name",
    );
}

/// The sum runs over the *query* tokens, so swapping the arguments changes
/// the score.
#[test]
fn maxsim_is_not_symmetric() {
    // d1 = [.5,.5] against Q: .5, .5 -> .5.  d2 = [1,0]: 1, 0 -> 1.
    // d3 = [0,2]: 0, 2 -> 2.  Sum 3.5, against 3.0 the other way round.
    assert_close(
        eval(&format!("vector.similarity.maxSim({D}, {Q})")),
        3.5,
        "swapped",
    );
}

/// Raw cosine in [-1, 1] per token -- not the [0, 1] rescale of
/// `vector.similarity.cosine`.
#[test]
fn maxsim_with_cosine() {
    // q1: cos to [.5,.5] = 0.7071, to [1,0] = 1, to [0,2] = 0 -> 1.
    // q2: 0.7071, 0, 1 -> 1.  Sum 2.
    assert_close(
        eval(&format!("vector.similarity.maxSim({Q}, {D}, 'COSINE')")),
        2.0,
        "cosine",
    );
    // One token each: the plain cosine, 1/sqrt(2).
    assert_close(
        eval("vector.similarity.maxSim([[1.0, 0.0]], [[1.0, 1.0]], 'cosine')"),
        std::f64::consts::FRAC_1_SQRT_2,
        "single-token cosine",
    );
    // Opposite directions: -1, not 0.
    assert_close(
        eval("vector.similarity.maxSim([[1.0, 0.0]], [[-3.0, 0.0]], 'COSINE')"),
        -1.0,
        "negative cosine",
    );
}

/// One token against one token is the dot product; integers are numbers; a
/// best match that is negative stays negative.
#[test]
fn maxsim_edge_values() {
    // 1*4 + 2*5 + 3*6 = 32.
    assert_close(
        eval("vector.similarity.maxSim([[1, 2, 3]], [[4, 5, 6]])"),
        32.0,
        "single token",
    );
    // [-1,0] against [1,0] and [2,0]: -1 and -2, the best is -1.
    assert_close(
        eval("vector.similarity.maxSim([[-1.0, 0.0]], [[1.0, 0.0], [2.0, 0.0]])"),
        -1.0,
        "all negative",
    );
    // A zero token is fine for dot products: it contributes 0.
    assert_close(
        eval("vector.similarity.maxSim([[0.0, 0.0], [1.0, 1.0]], [[2.0, 3.0]])"),
        5.0,
        "zero token, dot",
    );
}

/// Multi-vectors stored as properties, ranked against a query.
#[test]
fn maxsim_over_stored_properties_ranks_documents() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE (:Doc {name: 'a', toks: [[1.0, 0.0], [0.0, 1.0]]}), \
                (:Doc {name: 'b', toks: [[0.6, 0.8]]}), \
                (:Doc {name: 'c', toks: [[-1.0, 0.0], [0.0, -1.0]]})",
    );
    let r = rows(
        &s,
        "MATCH (d:Doc) \
         RETURN d.name AS name, \
                vector.similarity.maxSim([[1.0, 0.0], [0.0, 1.0]], d.toks) AS score \
         ORDER BY score DESC",
    );
    // a: 1 + 1 = 2.  b: 0.6 + 0.8 = 1.4.  c: -1 + -1 = -2.
    let got: Vec<(String, f64)> = r
        .iter()
        .map(|r| (string(r, "name"), float(r, "score")))
        .collect();
    assert_eq!(got.len(), 3, "{got:?}");
    for ((name, score), (want_name, want)) in got.iter().zip([("a", 2.0), ("b", 1.4), ("c", -2.0)])
    {
        assert_eq!(name, want_name, "{got:?}");
        assert_close(*score, want, name);
    }
}

#[test]
fn maxsim_null_in_null_out() {
    let s = GraphStore::new();
    for q in [
        "RETURN vector.similarity.maxSim(null, [[1.0]]) AS r",
        "RETURN vector.similarity.maxSim([[1.0]], null) AS r",
        "RETURN vector.similarity.maxSim([[1.0]], [[1.0]], null) AS r",
        // A missing property is null, as for a document without tokens.
        "OPTIONAL MATCH (d:Doc) RETURN vector.similarity.maxSim([[1.0]], d.toks) AS r",
        // A zero token has no direction, so no cosine -- as
        // `vector.similarity.cosine` does for a zero vector.
        "RETURN vector.similarity.maxSim([[0.0, 0.0]], [[1.0, 0.0]], 'COSINE') AS r",
        "RETURN vector.similarity.maxSim([[1.0, 0.0]], [[1.0, 0.0], [0.0, 0.0]], 'COSINE') AS r",
    ] {
        assert!(is_null(&rows(&s, q)[0], "r"), "{q}");
    }
}

#[test]
fn maxsim_bad_arguments_are_errors() {
    // Query and document tokens of different dimensions.
    assert_error(
        "RETURN vector.similarity.maxSim([[1.0, 2.0]], [[1.0]]) AS r",
        &["same dimension", "2", "1"],
    );
    // Tokens of different dimensions within one multi-vector.
    assert_error(
        "RETURN vector.similarity.maxSim([[1.0, 2.0], [1.0]], [[1.0, 2.0]]) AS r",
        &["same dimension"],
    );
    // A single vector is not a multi-vector.
    assert_error(
        "RETURN vector.similarity.maxSim([1.0, 2.0], [[1.0, 2.0]]) AS r",
        &["multi-vectors"],
    );
    // Empty: no tokens, or a token with no components.
    assert_error(
        "RETURN vector.similarity.maxSim([], [[1.0]]) AS r",
        &["multi-vectors"],
    );
    assert_error(
        "RETURN vector.similarity.maxSim([[1.0]], [[]]) AS r",
        &["multi-vectors"],
    );
    // Not numbers.
    assert_error(
        "RETURN vector.similarity.maxSim([['a']], [[1.0]]) AS r",
        &["multi-vectors"],
    );
    assert_error(
        "RETURN vector.similarity.maxSim('a', [[1.0]]) AS r",
        &["multi-vectors"],
    );
    // A metric nobody defines, or one that is not a string.
    assert_error(
        "RETURN vector.similarity.maxSim([[1.0]], [[1.0]], 'L2') AS r",
        &["L2", "DOT", "COSINE"],
    );
    assert_error(
        "RETURN vector.similarity.maxSim([[1.0]], [[1.0]], 1) AS r",
        &["string"],
    );
    // Too few arguments.
    assert_error("RETURN vector.similarity.maxSim([[1.0]]) AS r", &["2"]);
}

// ---------------------------------------------------------------------------
// vector.similarity.sparseDot
// ---------------------------------------------------------------------------

/// Parallel lists, as Qdrant spells a sparse vector.
#[test]
fn sparse_dot_over_indices_and_values() {
    // Shared dimensions 3 and 7: 2.0*1.5 + 1.0*(-2.0) = 1.0. Dimensions 1
    // and 9 are on one side only and contribute nothing.
    let a = "{indices: [1, 3, 7], values: [0.5, 2.0, 1.0]}";
    let b = "{indices: [3, 7, 9], values: [1.5, -2.0, 4.0]}";
    assert_close(
        eval(&format!("vector.similarity.sparseDot({a}, {b})")),
        1.0,
        "parallel lists",
    );
    // Order of the entries does not matter.
    let a_shuffled = "{values: [1.0, 0.5, 2.0], indices: [7, 1, 3]}";
    assert_close(
        eval(&format!("vector.similarity.sparseDot({a_shuffled}, {b})")),
        1.0,
        "unsorted indices",
    );
    // Symmetric.
    assert_close(
        eval(&format!("vector.similarity.sparseDot({b}, {a})")),
        1.0,
        "swapped",
    );
}

/// A map from dimension to weight. Cypher map keys are strings.
#[test]
fn sparse_dot_over_dimension_maps() {
    let a = "{`1`: 0.5, `3`: 2.0, `7`: 1.0}";
    let b = "{`3`: 1.5, `7`: -2.0, `9`: 4.0}";
    assert_close(
        eval(&format!("vector.similarity.sparseDot({a}, {b})")),
        1.0,
        "dimension maps",
    );
    // Quoted keys are the same keys.
    assert_close(
        eval("vector.similarity.sparseDot({'3': 2.0}, {`3`: 4.0})"),
        8.0,
        "quoted keys",
    );
    // The two spellings mix.
    assert_close(
        eval(&format!(
            "vector.similarity.sparseDot({a}, {{indices: [3, 7, 9], values: [1.5, -2.0, 4.0]}})"
        )),
        1.0,
        "mixed spellings",
    );
}

#[test]
fn sparse_dot_edge_values() {
    // No shared dimension.
    assert_close(
        eval("vector.similarity.sparseDot({`1`: 1.0}, {`2`: 1.0})"),
        0.0,
        "disjoint",
    );
    // The empty sparse vector, in both spellings.
    assert_close(
        eval("vector.similarity.sparseDot({}, {`2`: 1.0})"),
        0.0,
        "empty map",
    );
    assert_close(
        eval("vector.similarity.sparseDot({indices: [], values: []}, {`2`: 1.0})"),
        0.0,
        "empty lists",
    );
    // Integer weights: 3 * 4 = 12. Large dimensions are fine (SPLADE's
    // vocabulary is ~30k; hashed features go far beyond).
    assert_close(
        eval("vector.similarity.sparseDot({indices: [0], values: [3]}, {`0`: 4})"),
        12.0,
        "integer weights",
    );
    assert_close(
        eval("vector.similarity.sparseDot({indices: [4000000000], values: [0.5]}, {`4000000000`: 0.5})"),
        0.25,
        "large dimension",
    );
}

/// Sparse vectors stored as map properties, ranked against a query.
#[test]
fn sparse_dot_over_stored_properties_ranks_documents() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE (:T {name: 'a', sv: {indices: [1, 3], values: [0.5, 2.0]}}), \
                (:T {name: 'b', sv: {`3`: 0.5, `8`: 1.0}}), \
                (:T {name: 'c', sv: {indices: [2], values: [9.0]}})",
    );
    let r = rows(
        &s,
        "MATCH (t:T) \
         RETURN t.name AS name, \
                vector.similarity.sparseDot(t.sv, {indices: [3, 8], values: [3.0, 1.0]}) AS score \
         ORDER BY score DESC",
    );
    // a: 2.0*3.0 = 6.  b: 0.5*3.0 + 1.0*1.0 = 2.5.  c: nothing shared, 0.
    let got: Vec<(String, f64)> = r
        .iter()
        .map(|r| (string(r, "name"), float(r, "score")))
        .collect();
    assert_eq!(got.len(), 3, "{got:?}");
    for ((name, score), (want_name, want)) in got.iter().zip([("a", 6.0), ("b", 2.5), ("c", 0.0)]) {
        assert_eq!(name, want_name, "{got:?}");
        assert_close(*score, want, name);
    }
}

#[test]
fn sparse_dot_null_in_null_out() {
    let s = GraphStore::new();
    for q in [
        "RETURN vector.similarity.sparseDot(null, {`1`: 1.0}) AS r",
        "RETURN vector.similarity.sparseDot({`1`: 1.0}, null) AS r",
        "OPTIONAL MATCH (t:T) RETURN vector.similarity.sparseDot(t.sv, {`1`: 1.0}) AS r",
    ] {
        assert!(is_null(&rows(&s, q)[0], "r"), "{q}");
    }
}

#[test]
fn sparse_dot_bad_arguments_are_errors() {
    // Not a map at all.
    assert_error(
        "RETURN vector.similarity.sparseDot([1.0, 2.0], {`1`: 1.0}) AS r",
        &["sparse vectors", "not a map"],
    );
    // A dimension twice.
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: [1, 1], values: [1.0, 2.0]}, {}) AS r",
        &["dimension 1 appears twice"],
    );
    // Parallel lists of different lengths.
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: [1, 2], values: [1.0]}, {}) AS r",
        &["2 entries", "`values` 1"],
    );
    // A negative or fractional index.
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: [-1], values: [1.0]}, {}) AS r",
        &["non-negative integer"],
    );
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: [1.5], values: [1.0]}, {}) AS r",
        &["non-negative integer"],
    );
    // A key that is not a dimension.
    assert_error(
        "RETURN vector.similarity.sparseDot({x: 1.0}, {}) AS r",
        &["'x'", "non-negative integer"],
    );
    assert_error(
        "RETURN vector.similarity.sparseDot({`-2`: 1.0}, {}) AS r",
        &["'-2'"],
    );
    // A weight that is not a number.
    assert_error(
        "RETURN vector.similarity.sparseDot({`1`: 'heavy'}, {}) AS r",
        &["weight of '1'", "finite number"],
    );
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: [1], values: ['a']}, {}) AS r",
        &["finite number"],
    );
    // `indices` that is not a list.
    assert_error(
        "RETURN vector.similarity.sparseDot({indices: 1, values: [1.0]}, {}) AS r",
        &["`indices` is not a list"],
    );
}
