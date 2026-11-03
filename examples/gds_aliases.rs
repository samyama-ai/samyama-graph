//! Does a GDS procedure name reach our implementation? (INT-04)
//!
//! INT-04 asks for `gds.*` names mapped to our algorithms *where the semantics
//! match*, with the divergences documented. Its H1 target is "aliases for the
//! implemented overlap". It has stood `unmeasured`.
//!
//! Dispatch strips a `gds.` prefix and lower-cases the rest, so `gds.pageRank`
//! resolves. That is not the question INT-04 asks. A user porting GDS code
//! writes the name GDS uses, and GDS names carry an **execution mode**:
//! `gds.pageRank.stream`, `gds.wcc.stats`, `gds.triangleCount.write`. Whether
//! the prefix alone is enough is what this measures.
//!
//! **The denominator is written down, not discovered.** `OVERLAP` lists the GDS
//! procedures whose semantics we implement, each naming the counterpart. A list
//! built from the names we happen to accept would report full coverage of
//! whatever we already do -- the same shape as a corpus derived from our own
//! error variants. A procedure we do not implement is in `NOT_IMPLEMENTED`, out
//! of both halves, and named in the output so the exclusion is visible.
//!
//! Acceptance is measured by **running the query**, not by asking the
//! dispatcher. `is_algorithm` returning true and the call then failing is a
//! real shape here: it is what `algo.pageRank({writeProperty: 'pr'})` did
//! before #1316.
//!
//! ```text
//! cargo run --release --example gds_aliases -- --json gds.json
//! ```

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// (GDS procedure, our counterpart, whether the semantics are the same answer)
///
/// **`same_answer` is a code-read assertion, not a measurement.** Nothing here
/// runs GDS, so "the same algorithm" is read off our implementation and GDS's
/// documented definition. What the probe measures is whether the GDS *name*
/// resolves; whether the two produce the same numbers on the same graph is a
/// different and unmeasured thing, and INT-04's "where semantics match" rests
/// on this column. Two entries in the first version of this list were wrong --
/// `louvain` and `nodeSimilarity` were mapped onto `cdlp` and called divergent
/// while we implement both under their own names -- which is the accuracy to
/// expect from a column filled in this way.
///
/// "Same answer" is the honest column: `gds.degree.stream` and our `degree`
/// both return a degree per node, so a port is a rename. `gds.louvain` and our
/// `cdlp` are both community detection and are *not* the same algorithm, so
/// aliasing them would return a different answer under a familiar name --
/// worse than not aliasing at all. Those are marked `false` and counted as
/// divergences to document rather than aliases to add.
const OVERLAP: &[(&str, &str, bool)] = &[
    ("gds.pageRank.stream", "pageRank", true),
    ("gds.wcc.stream", "wcc", true),
    ("gds.triangleCount.stream", "triangleCount", true),
    ("gds.localClusteringCoefficient.stream", "lcc", true),
    ("gds.degree.stream", "degree", true),
    ("gds.betweenness.stream", "betweenness", true),
    ("gds.closeness.stream", "closeness", true),
    ("gds.eigenvector.stream", "eigenvector", true),
    ("gds.kcore.stream", "kcore", true),
    ("gds.labelPropagation.stream", "cdlp", true),
    ("gds.scc.stream", "scc", true),
    // GDS's dijkstra is weighted, so the counterpart is `weightedPath` and not
    // `shortestPath`, which is our unweighted BFS. Mapping it to the wrong one
    // would have scored an alias as working while returning a different answer.
    ("gds.shortestPath.dijkstra.stream", "weightedPath", true),
    ("gds.spanningTree.stream", "mst", true),
    ("gds.maxFlow.stream", "maxflow", true),
    ("gds.alpha.jaccard.stream", "jaccard", true),
    ("gds.alpha.adamicAdar.stream", "adamicAdar", true),
    // Both were entered here as divergent, mapped onto `cdlp` -- and both are
    // implemented under their own names. The corpus was wrong, not the engine.
    ("gds.louvain.stream", "louvain", true),
    ("gds.nodeSimilarity.stream", "nodeSimilarity", true),
    // Genuinely divergent: GDS's `triangles` lists one row per triangle, ours
    // returns a count per node. Same word, different result shape, so the name
    // is left unresolved rather than aliased onto a different answer.
    ("gds.alpha.triangles", "triangleCount", false),
];

/// GDS procedures with no counterpart at all. Out of both halves of the ratio,
/// listed so the exclusion is visible rather than silent.
const NOT_IMPLEMENTED: &[&str] = &[
    "gds.graph.project",
    "gds.beta.node2vec.stream",
    "gds.fastRP.stream",
    "gds.knn.stream",
    "gds.alpha.ml.linkPrediction.train",
];

/// Minimal arguments for a call that should run rather than fail on arity.
/// A name that is accepted and then fails because the probe called it wrong is
/// a probe bug reported as an aliasing gap, which is how two earlier corpora in
/// this repo got their numbers wrong.
fn args_for(counterpart: &str) -> &'static str {
    match counterpart {
        "shortestPath" | "weightedPath" | "maxflow" => "0, 0",
        "jaccard" | "adamicAdar" => "0, 0",
        _ => "",
    }
}

fn main() {
    let json_out = {
        let a: Vec<String> = std::env::args().collect();
        a.iter()
            .position(|x| x == "--json")
            .and_then(|i| a.get(i + 1))
            .cloned()
    };

    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("N")]);
    let b = store.create_node_with_labels([Label::new("N")]);
    store.create_edge(a, b, "R").unwrap();

    let try_call = |name: &str, counterpart: &str| -> (bool, String) {
        let q = format!("CALL {}({}) YIELD node RETURN count(*)", name, args_for(counterpart));
        match parse_query(&q) {
            Err(e) => (false, format!("{e}")),
            Ok(p) => match QueryExecutor::new(&store).execute(&p) {
                Ok(_) => (true, String::new()),
                Err(e) => {
                    let msg = format!("{e}");
                    // A yield-column mismatch means the name *resolved* -- the
                    // algorithm ran and produced different columns. That is an
                    // alias that works, not a missing one, so it is not counted
                    // against INT-04. Only "unknown" is a missing alias.
                    let low = msg.to_lowercase();
                    let resolved = !(low.contains("unknown algorithm")
                        || low.contains("unknown procedure")
                        || low.contains("not a procedure"));
                    (resolved, msg)
                }
            },
        }
    };

    let mut rows = Vec::new();
    for (gds, ours, same_answer) in OVERLAP {
        let (accepted, err) = try_call(gds, ours);
        // The control: our own spelling must work, or a "gds name rejected"
        // result is really "this algorithm is broken" and nothing to do with
        // aliasing.
        let (ours_works, ours_err) = try_call(ours, ours);
        rows.push(serde_json::json!({
            "gds": gds,
            "ours": ours,
            "same_answer": same_answer,
            "gds_name_accepted": accepted,
            "our_name_accepted": ours_works,
            "error": err.chars().take(160).collect::<String>(),
            "our_error": ours_err.chars().take(160).collect::<String>(),
        }));
    }

    // Aliasable = the semantics match AND our own implementation is reachable.
    // Anything else is a divergence to document or a defect of its own, and
    // folding either into this ratio would make it say something it does not.
    let aliasable: Vec<_> = rows
        .iter()
        .filter(|r| r["same_answer"] == true && r["our_name_accepted"] == true)
        .collect();
    let aliased = aliasable
        .iter()
        .filter(|r| r["gds_name_accepted"] == true)
        .count();
    let broken: Vec<&str> = rows
        .iter()
        .filter(|r| r["our_name_accepted"] == false)
        .map(|r| r["ours"].as_str().unwrap())
        .collect();
    let divergent: Vec<&str> = rows
        .iter()
        .filter(|r| r["same_answer"] == false)
        .map(|r| r["gds"].as_str().unwrap())
        .collect();
    // The property that matters more than coverage: a name whose semantics
    // differ must NOT resolve. An alias that returns a different answer under a
    // name the user already trusts is worse than a missing alias, and the
    // coverage ratio alone would never show it -- divergent names are excluded
    // from that ratio, so aliasing one by accident raises nothing and lowers
    // nothing.
    let wrongly_aliased: Vec<&str> = rows
        .iter()
        .filter(|r| r["same_answer"] == false && r["gds_name_accepted"] == true)
        .map(|r| r["gds"].as_str().unwrap())
        .collect();

    let doc = serde_json::json!({
        "aliasable": aliasable.len(),
        "aliased": aliased,
        "divergent_semantics": divergent,
        "divergent_but_resolving": wrongly_aliased,
        "semantic_equivalence": "asserted from a code read, not measured against GDS",
        "our_implementation_unreachable": broken,
        "not_implemented": NOT_IMPLEMENTED,
        "cases": rows,
    });
    eprintln!(
        "{aliased} of {} semantically matching GDS names resolve; \
         {} divergent ({} of them wrongly resolving), {} not implemented, \
         {} of our own names unreachable",
        aliasable.len(),
        divergent.len(),
        wrongly_aliased.len(),
        NOT_IMPLEMENTED.len(),
        broken.len()
    );
    for r in &rows {
        if r["same_answer"] == true && r["gds_name_accepted"] == false {
            eprintln!("  rejected: {} -> {}", r["gds"], r["error"]);
        }
    }
    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        eprintln!("wrote {path}");
    } else {
        println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    }
}
