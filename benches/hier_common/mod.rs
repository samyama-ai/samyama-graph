//! Shared dataset builder for the HIER benchmark category (ADR-035).
//!
//! HIER exists because the suites Samyama already runs — LDBC SNB Interactive and BI,
//! FinBench, Graphalytics — contain essentially no subsumption or hierarchical roll-up.
//! They are social-network and financial-transaction workloads; the hierarchies in them are
//! shallow and incidental. That is precisely why a hierarchy index is invisible in those
//! numbers, and why a new category is needed rather than a new query in an old one.
//!
//! The dataset is **generated, deterministic and self-contained**: no download, no license
//! gate, identical on every machine, so a HIER result is reproducible from a clean
//! checkout. It carries the four hierarchy axes the paper unifies, deliberately shaped to
//! exercise different parts of the index:
//!
//! | Axis | Label chain | Covering edge | Shape | Encoding it forces |
//! |---|---|---|---|---|
//! | Time | Day ⊑ Month ⊑ Quarter ⊑ Year | `IN_PERIOD` | tree, depth 3 | nested-set |
//! | Geography | Zip ⊑ City ⊑ State ⊑ Country | `LOCATED_IN` | tree, depth 3 | nested-set |
//! | Ontology | Term ⊑ Term (5 levels, fanout 6) | `IS_A` | tree, depth 5 | nested-set |
//! | Threat | Technique ⊑ Technique | `MAPS_TO` | multi-parent DAG | chain |
//!
//! The ontology tree is deep and wide on purpose: its subtree sizes run 1, 6, 36, 216,
//! 1296, 7776, 9331 — four orders of magnitude, which is what makes an O(subtree) baseline
//! and an O(log n) index distinguishable rather than both "fast".
//!
//! `Event` nodes tie the axes together (`ON` a day, `AT` a zip, `ABOUT` a term, `USES` a
//! technique). Those are what the cross-hierarchy conjunction queries count, and they are
//! the reason this has to be a graph benchmark: no per-silo hierarchy index answers a
//! question that ranges over three hierarchies and a fact table at once.

use samyama::graph::{GraphStore, NodeId, PropertyValue};

/// Node counts and shape knobs, so a run can be scaled without editing the codes.
pub struct HierScale {
    /// Years in the calendar.
    pub years: usize,
    /// Days per month (a fixed 28 keeps codes and subtree sizes uniform).
    pub days_per_month: usize,
    /// Countries in the geography.
    pub countries: usize,
    /// Ontology tree depth (levels below the root).
    pub onto_depth: usize,
    /// Ontology fanout.
    pub onto_fanout: usize,
    /// Threat DAG layers.
    pub threat_layers: usize,
    /// Threat DAG width.
    pub threat_width: usize,
    /// Fact rows.
    pub events: usize,
}

impl Default for HierScale {
    fn default() -> Self {
        HierScale {
            years: 8,
            days_per_month: 28,
            countries: 4,
            onto_depth: 5,
            onto_fanout: 6,
            threat_layers: 3,
            threat_width: 12,
            events: 5_000,
        }
    }
}

/// A built HIER graph plus the handles a runner needs.
pub struct HierDataset {
    /// The store.
    pub store: GraphStore,
    /// Node count.
    pub nodes: usize,
    /// Edge count.
    pub edges: usize,
}

/// Deterministic pseudo-random measure in `1..=97`.
///
/// A fixed multiplicative hash rather than an RNG: the same graph on every machine and
/// every run, so a roll-up result is a stable expected value the corpus can assert against.
fn measure_for(seed: u64) -> i64 {
    ((seed.wrapping_mul(2_654_435_761) >> 7) % 97 + 1) as i64
}

/// Build the HIER dataset.
pub fn build(scale: &HierScale) -> HierDataset {
    let mut store = GraphStore::new();
    let mut edges = 0usize;

    let node = |store: &mut GraphStore, label: &str, code: String, units: Option<i64>| -> NodeId {
        let id = store.create_node(label);
        store.set_column_property(id, "code", PropertyValue::String(code));
        if let Some(u) = units {
            store.set_column_property(id, "units", PropertyValue::Integer(u));
        }
        id
    };

    // Extra scalar attributes the corpus filters on: `level` for the ontology and threat
    // axes (where depth is not encoded in the label), `y` for the calendar, `co` for the
    // geography. Without these, a level-wise roll-up query would have to be written as N
    // separate single-root queries, which is not the shape real OLAP asks.
    fn tag(store: &mut GraphStore, id: NodeId, key: &str, v: i64) {
        store.set_column_property(id, key, PropertyValue::Integer(v));
    }

    // ---- calendar: Day ⊑ Month ⊑ Quarter ⊑ Year ----------------------------
    let mut days: Vec<NodeId> = Vec::new();
    let mut months: Vec<NodeId> = Vec::new();
    let mut seed = 1u64;
    for y in 0..scale.years {
        let year = node(&mut store, "Year", format!("Y{}", 2019 + y), None);
        tag(&mut store, year, "y", (2019 + y) as i64);
        for q in 0..4 {
            let quarter = node(&mut store, "Quarter", format!("Y{}Q{}", 2019 + y, q + 1), None);
            tag(&mut store, quarter, "y", (2019 + y) as i64);
            store.create_edge(quarter, year, "IN_PERIOD").unwrap();
            edges += 1;
            for m in 0..3 {
                let mi = q * 3 + m + 1;
                let month = node(&mut store, "Month", format!("Y{}M{:02}", 2019 + y, mi), None);
                tag(&mut store, month, "y", (2019 + y) as i64);
                store.create_edge(month, quarter, "IN_PERIOD").unwrap();
                edges += 1;
                months.push(month);
                for d in 0..scale.days_per_month {
                    seed += 1;
                    let day = node(
                        &mut store,
                        "Day",
                        format!("Y{}M{:02}D{:02}", 2019 + y, mi, d + 1),
                        Some(measure_for(seed)),
                    );
                    tag(&mut store, day, "y", (2019 + y) as i64);
                    store.create_edge(day, month, "IN_PERIOD").unwrap();
                    edges += 1;
                    days.push(day);
                }
            }
        }
    }

    // ---- geography: Zip ⊑ City ⊑ State ⊑ Country ---------------------------
    let mut zips: Vec<NodeId> = Vec::new();
    for c in 0..scale.countries {
        let country = node(&mut store, "Country", format!("CO{c}"), None);
        tag(&mut store, country, "co", c as i64);
        for s in 0..5 {
            let state = node(&mut store, "State", format!("CO{c}S{s}"), None);
            tag(&mut store, state, "co", c as i64);
            store.create_edge(state, country, "LOCATED_IN").unwrap();
            edges += 1;
            for t in 0..8 {
                let city = node(&mut store, "City", format!("CO{c}S{s}T{t}"), None);
                tag(&mut store, city, "co", c as i64);
                store.create_edge(city, state, "LOCATED_IN").unwrap();
                edges += 1;
                for z in 0..10 {
                    seed += 1;
                    let zip = node(
                        &mut store,
                        "Zip",
                        format!("CO{c}S{s}T{t}Z{z}"),
                        Some(measure_for(seed)),
                    );
                    store.create_edge(zip, city, "LOCATED_IN").unwrap();
                    edges += 1;
                    zips.push(zip);
                }
            }
        }
    }

    // ---- ontology: a deep, wide IS_A tree ----------------------------------
    // Codes are the path from the root: "T", "T0", "T05", "T053"… so a query can name any
    // subtree root by construction, and the subtree size is fanout^(depth - level).
    let mut terms_by_level: Vec<Vec<NodeId>> = Vec::new();
    let root = node(&mut store, "Term", "T".to_string(), Some(measure_for(0)));
    tag(&mut store, root, "level", 0);
    terms_by_level.push(vec![root]);
    let mut codes: Vec<String> = vec!["T".to_string()];
    for level in 1..=scale.onto_depth {
        let parents = terms_by_level[level - 1].clone();
        let parent_codes = codes.clone();
        let mut this_level = Vec::with_capacity(parents.len() * scale.onto_fanout);
        let mut this_codes = Vec::with_capacity(parents.len() * scale.onto_fanout);
        for (pi, &parent) in parents.iter().enumerate() {
            for f in 0..scale.onto_fanout {
                seed += 1;
                let code = format!("{}{}", parent_codes[pi], f);
                let t = node(&mut store, "Term", code.clone(), Some(measure_for(seed)));
                tag(&mut store, t, "level", level as i64);
                // A small deterministic embedding so the corpus can ask the question a
                // time-series engine cannot: nearest neighbours *restricted to a subtree*.
                let e0 = (measure_for(seed) as f32) / 97.0;
                let e1 = (measure_for(seed + 1) as f32) / 97.0;
                store.set_column_property(
                    t,
                    "emb",
                    PropertyValue::Vector(vec![e0, e1, 1.0 - e0, 1.0 - e1]),
                );
                store.create_edge(t, parent, "IS_A").unwrap();
                edges += 1;
                this_level.push(t);
                this_codes.push(code);
            }
        }
        codes = this_codes;
        terms_by_level.push(this_level);
    }

    // ---- threat: a genuinely multi-parent DAG ------------------------------
    // Each node links to two parents in the layer above, so nodes are reachable along many
    // paths — the shape that makes a naive path-sum roll-up over-count and forces the
    // chain encoding to earn its keep.
    let mut techniques: Vec<Vec<NodeId>> = Vec::new();
    for layer in 0..scale.threat_layers {
        let mut this = Vec::with_capacity(scale.threat_width);
        for i in 0..scale.threat_width {
            seed += 1;
            let t = node(
                &mut store,
                "Technique",
                format!("K{layer}_{i}"),
                Some(measure_for(seed)),
            );
            tag(&mut store, t, "level", layer as i64);
            if layer > 0 {
                let above: &Vec<NodeId> = &techniques[layer - 1];
                store.create_edge(t, above[i], "MAPS_TO").unwrap();
                store
                    .create_edge(t, above[(i + 1) % scale.threat_width], "MAPS_TO")
                    .unwrap();
                edges += 2;
            }
            this.push(t);
        }
        techniques.push(this);
    }

    // ---- facts -------------------------------------------------------------
    let leaves = &terms_by_level[scale.onto_depth];
    let flat_techniques: Vec<NodeId> = techniques.iter().flatten().copied().collect();
    for e in 0..scale.events {
        seed += 1;
        let ev = node(&mut store, "Event", format!("E{e}"), Some(measure_for(seed)));
        store
            .create_edge(ev, days[(e * 7) % days.len()], "ON")
            .unwrap();
        store
            .create_edge(ev, zips[(e * 13) % zips.len()], "AT")
            .unwrap();
        store
            .create_edge(ev, leaves[(e * 31) % leaves.len()], "ABOUT")
            .unwrap();
        store
            .create_edge(ev, flat_techniques[(e * 5) % flat_techniques.len()], "USES")
            .unwrap();
        edges += 4;
    }

    let nodes = store.node_count();
    HierDataset {
        store,
        nodes,
        edges,
    }
}

/// Declarations applied to **both** stores — they are part of the fixture, not the thing
/// under test. The hierarchy-filtered vector queries need an ANN index on either side.
pub const SETUP_DECLARATIONS: &[&str] = &[
    "CREATE VECTOR INDEX termvec FOR (t:Term) ON (t.emb) OPTIONS {dimensions: 4}",
    // Property indexes on the pinned `code` columns. Both stores get them: locating the
    // subtree root is a lookup either side of the comparison, and leaving it as a label
    // scan would measure the scan rather than the hierarchy.
    "CREATE INDEX ON :Term(code)",
    "CREATE INDEX ON :Year(code)",
    "CREATE INDEX ON :Quarter(code)",
    "CREATE INDEX ON :Month(code)",
    "CREATE INDEX ON :Day(code)",
    "CREATE INDEX ON :Country(code)",
    "CREATE INDEX ON :State(code)",
    "CREATE INDEX ON :City(code)",
    "CREATE INDEX ON :Zip(code)",
    "CREATE INDEX ON :Technique(code)",
];

/// The four hierarchy declarations the benchmark measures.
pub const HIER_DECLARATIONS: &[&str] = &[
    "CREATE HIERARCHY INDEX cal ON ()-[:IN_PERIOD]->() MEASURE units AGGREGATE sum, min, max, count",
    "CREATE HIERARCHY INDEX geo ON ()-[:LOCATED_IN]->() MEASURE units AGGREGATE sum, min, max, count",
    "CREATE HIERARCHY INDEX onto ON ()-[:IS_A]->() MEASURE units AGGREGATE sum, min, max, count",
    "CREATE HIERARCHY INDEX threat ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum, min, max, count",
];
