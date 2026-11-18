//! The parts of the graph + vector hybrid benchmark (#1987) that do not time
//! anything: the synthetic research graph, the query workload, the exact
//! reference answers, the Cypher templates, loading a store, reading a
//! snapshot, and the export the competitor script reads.
//!
//! Shared by `benches/graph_vector_hybrid_benchmark.rs`, which runs and times
//! the templates, and `tests/graph_vector_hybrid_harness.rs`, which checks this
//! file in CI. A bench target cannot run its own tests, so the logic lives here
//! rather than in the bench.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::{BoundParams, QueryEngine, Value};

/// Row-major vectors of one dimension -- the shape of `ann_common::Vectors`,
/// kept here so this module stands alone.
#[derive(Clone, Debug, PartialEq)]
pub struct Vectors {
    pub dim: usize,
    pub data: Vec<f32>,
}

impl Vectors {
    pub fn len(&self) -> usize {
        self.data.len().checked_div(self.dim).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn row(&self, i: usize) -> &[f32] {
        &self.data[i * self.dim..(i + 1) * self.dim]
    }
}

/// Nearest-rank percentile of `sorted`, which must be sorted ascending.
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

// ------------------------------------------------------------------ the graph

/// A heterogeneous research graph: papers, people, methods and problems.
///
/// Every node is known by its position in its own label (`uid`). Edges are
/// `(source uid, target uid)`:
///
/// | edge | from | to | direction read by the templates |
/// |---|---|---|---|
/// | `cites` | Paper | Paper | either |
/// | `authored` | Person | Paper | outgoing |
/// | `coauthor` | Person | Person | either; stored once, smaller uid first |
/// | `uses_method` | Paper | Method | incoming at the method |
/// | `addresses` | Paper | Problem | outgoing from the paper |
///
/// `methods` and `problems` may be empty (a snapshot without them); template 3
/// is then skipped.
#[derive(Clone, Debug, PartialEq)]
pub struct HybridGraph {
    pub papers: Vectors,
    pub people: usize,
    pub methods: Vectors,
    pub problems: Vectors,
    pub cites: Vec<(u32, u32)>,
    pub authored: Vec<(u32, u32)>,
    pub coauthor: Vec<(u32, u32)>,
    pub uses_method: Vec<(u32, u32)>,
    pub addresses: Vec<(u32, u32)>,
}

impl HybridGraph {
    pub fn dim(&self) -> usize {
        self.papers.dim
    }

    pub fn has_join(&self) -> bool {
        !self.methods.is_empty() && !self.problems.is_empty()
    }

    pub fn edge_count(&self) -> usize {
        self.cites.len()
            + self.authored.len()
            + self.coauthor.len()
            + self.uses_method.len()
            + self.addresses.len()
    }
}

/// Sizes and seed of a synthetic graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub papers: usize,
    pub people: usize,
    pub methods: usize,
    pub problems: usize,
    pub topics: usize,
    pub dim: usize,
    /// Citations per paper, to earlier papers only (fewer for the first few).
    pub cites: usize,
    pub seed: u64,
}

impl Default for Config {
    /// About twice the research-kit `vectors.sgsnap` (2,667 papers, 5,799
    /// people): small enough for the no-argument run in the nightly sweep.
    fn default() -> Self {
        Self {
            papers: 5_000,
            people: 10_000,
            methods: 200,
            problems: 200,
            topics: 50,
            dim: 64,
            cites: 5,
            seed: 1987,
        }
    }
}

/// Sum of three uniforms: cheap, bell-shaped, no extra dependency. The same
/// noise `ann_common::synthetic` uses.
fn noisy(rng: &mut StdRng, centre: &[f32], spread: f32, out: &mut Vec<f32>) {
    for x in centre {
        let noise: f32 = (0..3).map(|_| rng.gen_range(-spread..spread)).sum();
        out.push(x + noise);
    }
}

/// Mostly from `local`, sometimes from anywhere in `0..all`: the topical
/// locality a real citation or authorship graph has, with some cross-talk.
fn pick(rng: &mut StdRng, local: &[u32], all: usize, p_local: f64) -> u32 {
    if !local.is_empty() && rng.gen_bool(p_local) {
        local[rng.gen_range(0..local.len())]
    } else {
        rng.gen_range(0..all as u32)
    }
}

fn dedup(mut v: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    v.sort_unstable();
    v.dedup();
    v
}

/// A seeded synthetic research graph. Each node belongs to one of
/// `cfg.topics` topics; embeddings are clustered around a centre per topic,
/// and edges prefer nodes of the same topic, so vector similarity and graph
/// proximity are correlated but not the same thing -- the case a hybrid query
/// exists for.
pub fn generate(cfg: &Config) -> Result<HybridGraph, String> {
    if cfg.papers == 0 || cfg.people == 0 || cfg.topics == 0 || cfg.dim == 0 {
        return Err("papers, people, topics and dim must all be positive".into());
    }
    if cfg.papers > u32::MAX as usize || cfg.people > u32::MAX as usize {
        return Err("papers and people must fit in u32".into());
    }
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let centres: Vec<Vec<f32>> = (0..cfg.topics)
        .map(|_| (0..cfg.dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect())
        .collect();

    let by_topic = |n: usize, spread: f32, rng: &mut StdRng| {
        let mut data = Vec::with_capacity(n * cfg.dim);
        let mut members = vec![Vec::new(); cfg.topics];
        let mut topic = Vec::with_capacity(n);
        for i in 0..n {
            let t = rng.gen_range(0..cfg.topics);
            noisy(rng, &centres[t], spread, &mut data);
            members[t].push(i as u32);
            topic.push(t);
        }
        (Vectors { dim: cfg.dim, data }, topic, members)
    };
    let (papers, paper_topic, _) = by_topic(cfg.papers, 0.1, &mut rng);
    let (methods, _, methods_of) = by_topic(cfg.methods, 0.2, &mut rng);
    let (problems, _, problems_of) = by_topic(cfg.problems, 0.2, &mut rng);

    let mut people_of = vec![Vec::new(); cfg.topics];
    for p in 0..cfg.people {
        people_of[rng.gen_range(0..cfg.topics)].push(p as u32);
    }

    let mut authored = Vec::new();
    let mut coauthor = Vec::new();
    let mut cites = Vec::new();
    let mut uses_method = Vec::new();
    let mut addresses = Vec::new();
    let mut earlier_of: Vec<Vec<u32>> = vec![Vec::new(); cfg.topics];
    for (i, &t) in paper_topic.iter().enumerate() {
        let paper = i as u32;
        let mut authors: Vec<u32> = (0..rng.gen_range(1..=4))
            .map(|_| pick(&mut rng, &people_of[t], cfg.people, 0.8))
            .collect();
        authors.sort_unstable();
        authors.dedup();
        for (j, &a) in authors.iter().enumerate() {
            authored.push((a, paper));
            for &b in &authors[j + 1..] {
                coauthor.push((a, b));
            }
        }
        for _ in 0..cfg.cites.min(i) {
            let target = if !earlier_of[t].is_empty() && rng.gen_bool(0.7) {
                earlier_of[t][rng.gen_range(0..earlier_of[t].len())]
            } else {
                rng.gen_range(0..paper)
            };
            cites.push((paper, target));
        }
        earlier_of[t].push(paper);
        if cfg.methods > 0 {
            for _ in 0..rng.gen_range(1..=2) {
                uses_method.push((paper, pick(&mut rng, &methods_of[t], cfg.methods, 0.8)));
            }
        }
        if cfg.problems > 0 {
            addresses.push((paper, pick(&mut rng, &problems_of[t], cfg.problems, 0.8)));
        }
    }
    Ok(HybridGraph {
        papers,
        people: cfg.people,
        methods,
        problems,
        cites: dedup(cites),
        authored: dedup(authored),
        coauthor: dedup(coauthor),
        uses_method: dedup(uses_method),
        addresses: dedup(addresses),
    })
}

// ---------------------------------------------------------------- the queries

/// Result sizes of the templates.
pub const SEEDS: usize = 10;
pub const K_EXPAND: usize = 10;
pub const K_FILTERED: usize = 10;
pub const K_JOIN: usize = 5;

/// The name the paper embedding index is created under, so the templates can
/// use Neo4j's `queryNodes(indexName, k, vector)` spelling and run unchanged
/// on Neo4j 5.
pub const PAPER_INDEX: &str = "paper_emb";

/// Template 1, seeded expansion: the `SEEDS` nearest papers to a text, every
/// paper within two CITES hops of them in either direction (the seeds
/// included), reranked by similarity to the text.
pub const T1_SEEDED_EXPANSION: &str = "CALL db.index.vector.queryNodes('paper_emb', 10, $q) \
     YIELD node AS seed \
     MATCH (seed)-[:CITES*0..2]-(c:Paper) \
     WITH DISTINCT c \
     RETURN c.uid AS uid, vector.similarity.cosine(c.emb, $q) AS score \
     ORDER BY score DESC, uid LIMIT 10";

/// Template 2, traversal-filtered k-NN: the nearest papers to a text among
/// those authored by someone within two COAUTHOR hops of person `$person`
/// (the person included). Exact over the candidates the traversal reaches.
pub const T2_TRAVERSAL_FILTERED: &str =
    "MATCH (x:Person {uid: $person})-[:COAUTHOR*0..2]-(a:Person)-[:AUTHORED]->(p:Paper) \
     WITH DISTINCT p \
     RETURN p.uid AS uid, vector.similarity.cosine(p.emb, $q) AS score \
     ORDER BY score DESC, uid LIMIT 10";

/// Template 3, vector join: for method `$method`, the five most similar
/// problems that no paper yet links to it (no paper both USES_METHOD it and
/// ADDRESSES the problem) -- the shape of a link-prediction query.
pub const T3_VECTOR_JOIN: &str = "MATCH (m:Method {uid: $method}), (p:Problem) \
     WHERE NOT EXISTS { MATCH (m)<-[:USES_METHOD]-(:Paper)-[:ADDRESSES]->(p) } \
     RETURN p.uid AS uid, vector.similarity.cosine(m.emb, p.emb) AS score \
     ORDER BY score DESC, uid LIMIT 5";

/// The template instances one run executes.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Template 1: a query vector each.
    pub expand: Vec<Vec<f32>>,
    /// Template 2: a person and a query vector each.
    pub filtered: Vec<(u32, Vec<f32>)>,
    /// Template 3: a method each.
    pub join: Vec<u32>,
}

/// `n` instances of each template, seeded. A query vector is a paper's
/// embedding with noise -- a "text" about that paper's topic. For template 2
/// the paper is one the person wrote, when they wrote any, so the query is
/// about what their neighbourhood works on. Template 3 takes methods in a
/// seeded order, cycling when `n` exceeds the method count.
pub fn workload(g: &HybridGraph, n: usize, seed: u64) -> Workload {
    let mut rng = StdRng::seed_from_u64(seed);
    let papers = g.papers.len();
    let near = |rng: &mut StdRng, paper: usize| {
        let mut q = Vec::with_capacity(g.dim());
        noisy(rng, g.papers.row(paper), 0.1, &mut q);
        q
    };
    let mut papers_of = vec![Vec::new(); g.people];
    for &(a, p) in &g.authored {
        papers_of[a as usize].push(p);
    }
    let expand = (0..n)
        .map(|_| {
            let p = rng.gen_range(0..papers);
            near(&mut rng, p)
        })
        .collect();
    let filtered = (0..n)
        .map(|_| {
            let person = rng.gen_range(0..g.people as u32);
            let own = &papers_of[person as usize];
            let p = if own.is_empty() {
                rng.gen_range(0..papers)
            } else {
                own[rng.gen_range(0..own.len())] as usize
            };
            (person, near(&mut rng, p))
        })
        .collect();
    let join = if g.has_join() {
        let mut order: Vec<u32> = (0..g.methods.len() as u32).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.gen_range(0..=i));
        }
        order.iter().copied().cycle().take(n).collect()
    } else {
        Vec::new()
    };
    Workload {
        expand,
        filtered,
        join,
    }
}

// ----------------------------------------------------------- the reference

/// Cosine similarity in f64. Ranking by it is ranking by Neo4j's
/// `vector.similarity.cosine`, which is `(1 + cos) / 2`.
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// The `k` best of `candidates` by `score`, highest first, ties to the
/// smaller uid -- the templates' `ORDER BY score DESC, uid LIMIT k`.
pub fn top_k(
    candidates: impl IntoIterator<Item = u32>,
    k: usize,
    score: impl Fn(u32) -> f64,
) -> Vec<u32> {
    let mut scored: Vec<(f64, u32)> = candidates.into_iter().map(|c| (score(c), c)).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.truncate(k);
    scored.into_iter().map(|(_, c)| c).collect()
}

fn undirected(n: usize, edges: &[(u32, u32)]) -> Vec<Vec<u32>> {
    let mut adj = vec![Vec::new(); n];
    for &(a, b) in edges {
        adj[a as usize].push(b);
        adj[b as usize].push(a);
    }
    adj
}

/// Every node within `hops` of `start` (included), by breadth-first search.
pub fn within(adj: &[Vec<u32>], start: u32, hops: usize) -> Vec<u32> {
    let mut seen = HashSet::from([start]);
    let mut out = vec![start];
    let mut frontier = VecDeque::from([(start, 0usize)]);
    while let Some((n, d)) = frontier.pop_front() {
        if d == hops {
            continue;
        }
        for &m in &adj[n as usize] {
            if seen.insert(m) {
                out.push(m);
                frontier.push_back((m, d + 1));
            }
        }
    }
    out
}

/// Exact answers to the templates, computed by brute force over the graph
/// without the engine: no index, no planner, no Cypher.
pub struct Reference<'a> {
    g: &'a HybridGraph,
    cites: Vec<Vec<u32>>,
    coauthor: Vec<Vec<u32>>,
    papers_of: Vec<Vec<u32>>,
    linked: HashSet<(u32, u32)>,
}

impl<'a> Reference<'a> {
    pub fn new(g: &'a HybridGraph) -> Self {
        let mut papers_of = vec![Vec::new(); g.people];
        for &(a, p) in &g.authored {
            papers_of[a as usize].push(p);
        }
        let mut methods_of: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(p, m) in &g.uses_method {
            methods_of.entry(p).or_default().push(m);
        }
        let mut linked = HashSet::new();
        for &(p, problem) in &g.addresses {
            for &m in methods_of.get(&p).map(Vec::as_slice).unwrap_or(&[]) {
                linked.insert((m, problem));
            }
        }
        Self {
            g,
            cites: undirected(g.papers.len(), &g.cites),
            coauthor: undirected(g.people, &g.coauthor),
            papers_of,
            linked,
        }
    }

    fn paper_score<'q>(&self, q: &'q [f32]) -> impl Fn(u32) -> f64 + 'q
    where
        'a: 'q,
    {
        let papers = &self.g.papers;
        move |p| cosine(papers.row(p as usize), q)
    }

    /// Template 1.
    pub fn seeded_expansion(&self, q: &[f32]) -> Vec<u32> {
        let seeds = top_k(0..self.g.papers.len() as u32, SEEDS, self.paper_score(q));
        let mut reached = HashSet::new();
        for s in seeds {
            reached.extend(within(&self.cites, s, 2));
        }
        top_k(reached, K_EXPAND, self.paper_score(q))
    }

    /// Template 2.
    pub fn traversal_filtered(&self, person: u32, q: &[f32]) -> Vec<u32> {
        let mut candidates = HashSet::new();
        for a in within(&self.coauthor, person, 2) {
            candidates.extend(self.papers_of[a as usize].iter().copied());
        }
        top_k(candidates, K_FILTERED, self.paper_score(q))
    }

    /// Template 3.
    pub fn vector_join(&self, method: u32) -> Vec<u32> {
        let m = self.g.methods.row(method as usize);
        let problems = &self.g.problems;
        top_k(
            (0..problems.len() as u32).filter(|p| !self.linked.contains(&(method, *p))),
            K_JOIN,
            |p| cosine(m, problems.row(p as usize)),
        )
    }
}

/// How an answer compares with the reference: the share of the reference it
/// contains (recall), and whether it is the same set. Set rather than list:
/// two nodes whose scores differ by less than the engine's f32 rounding may
/// swap places, which is not a wrong answer.
pub fn compare(got: &[u32], want: &[u32]) -> (f64, bool) {
    let want_set: HashSet<u32> = want.iter().copied().collect();
    let got_set: HashSet<u32> = got.iter().copied().collect();
    let recall = if want_set.is_empty() {
        if got_set.is_empty() {
            1.0
        } else {
            0.0
        }
    } else {
        got_set.intersection(&want_set).count() as f64 / want_set.len() as f64
    };
    (recall, got_set == want_set && got.len() == want.len())
}

// ----------------------------------------------------------------- the store

/// An edge type, its `(source uid, target uid)` list, and the node ids the
/// two uids index into.
type EdgeSpec<'a> = (&'a str, &'a [(u32, u32)], &'a [NodeId], &'a [NodeId]);

/// Load `g` into a store: a node per paper, person, method and problem with an
/// integer `uid` (and `emb` where there is one), the five edge types, a
/// property index on every `uid`, and the cosine vector index `paper_emb`.
///
/// Like `ann_common::load`, nodes first and the vector index built over them
/// afterwards, as `CREATE VECTOR INDEX` after a bulk load does.
pub fn load_store(g: &HybridGraph) -> Result<GraphStore, String> {
    let mut store = GraphStore::new();
    let add = |store: &mut GraphStore, label: &str, n: usize, emb: Option<&Vectors>| {
        (0..n)
            .map(|i| {
                let mut props = HashMap::new();
                props.insert("uid".to_string(), PropertyValue::Integer(i as i64));
                if let Some(v) = emb {
                    props.insert("emb".to_string(), PropertyValue::Vector(v.row(i).to_vec()));
                }
                store.create_node_with_properties("default", vec![Label::new(label)], props)
            })
            .collect::<Vec<NodeId>>()
    };
    let papers = add(&mut store, "Paper", g.papers.len(), Some(&g.papers));
    let people = add(&mut store, "Person", g.people, None);
    let methods = add(&mut store, "Method", g.methods.len(), Some(&g.methods));
    let problems = add(&mut store, "Problem", g.problems.len(), Some(&g.problems));
    let edges: [EdgeSpec; 5] = [
        ("CITES", &g.cites, &papers, &papers),
        ("AUTHORED", &g.authored, &people, &papers),
        ("COAUTHOR", &g.coauthor, &people, &people),
        ("USES_METHOD", &g.uses_method, &papers, &methods),
        ("ADDRESSES", &g.addresses, &papers, &problems),
    ];
    for (ty, list, from, to) in edges {
        for &(a, b) in list {
            store
                .create_edge(from[a as usize], to[b as usize], ty)
                .map_err(|e| format!("{ty} {a}->{b}: {e}"))?;
        }
    }
    let engine = QueryEngine::new();
    for label in ["Paper", "Person", "Method", "Problem"] {
        engine
            .execute_mut(
                &format!("CREATE INDEX ON :{label}(uid)"),
                &mut store,
                "default",
            )
            .map_err(|e| format!("index on {label}.uid: {e}"))?;
    }
    store
        .create_vector_index_with_params(
            Some(PAPER_INDEX),
            "Paper",
            "emb",
            g.dim(),
            samyama::vector::DistanceMetric::Cosine,
            samyama::vector::index::Quantization::None,
            samyama::vector::HnswParams::default(),
        )
        .map_err(|e| e.to_string())?;
    store.rebuild_vector_index();
    Ok(store)
}

fn vector_param(q: &[f32]) -> PropertyValue {
    PropertyValue::Vector(q.to_vec())
}

pub fn params_expand(q: &[f32]) -> BoundParams {
    BoundParams::from([("q".to_string(), vector_param(q))])
}

pub fn params_filtered(person: u32, q: &[f32]) -> BoundParams {
    BoundParams::from([
        ("person".to_string(), PropertyValue::Integer(person as i64)),
        ("q".to_string(), vector_param(q)),
    ])
}

pub fn params_join(method: u32) -> BoundParams {
    BoundParams::from([("method".to_string(), PropertyValue::Integer(method as i64))])
}

/// Run `text` and read its `uid` column, in row order.
pub fn run_template(
    engine: &QueryEngine,
    store: &GraphStore,
    text: &str,
    params: &BoundParams,
) -> Result<Vec<u32>, String> {
    let batch = engine
        .execute_with_params(text, store, params)
        .map_err(|e| e.to_string())?;
    batch
        .records
        .iter()
        .map(|r| match r.get("uid") {
            Some(Value::Property(PropertyValue::Integer(i))) => Ok(*i as u32),
            other => Err(format!("row without an integer `uid`: {other:?}")),
        })
        .collect()
}

// --------------------------------------------------------------- a snapshot

/// Read a research graph out of a store loaded from a `.sgsnap`.
///
/// The labels and edge types must be the ones `HybridGraph` names (`Paper`,
/// `Person`, `Method`, `Problem`; `CITES`, `AUTHORED`, `COAUTHOR`,
/// `USES_METHOD`, `ADDRESSES`). Papers must all carry a vector in
/// `embedding`; methods and problems are kept only when every one of them has
/// one, and template 3 is skipped otherwise. A snapshot without `COAUTHOR`
/// gets it derived from `AUTHORED`: two people are co-authors when they share
/// a paper. Edges whose ends are not of the expected labels are dropped.
///
/// Uids are assigned in node-id order, so the same snapshot always gives the
/// same graph. Other labels and properties are ignored.
pub fn from_store(store: &GraphStore, embedding: &str) -> Result<HybridGraph, String> {
    let collect = |label: &str| {
        let mut nodes: Vec<_> = store.get_nodes_by_label(&Label::new(label));
        nodes.sort_by_key(|n| n.id);
        nodes
    };
    let vectors =
        |label: &str, required: bool| -> Result<(Vectors, HashMap<NodeId, u32>), String> {
            let nodes = collect(label);
            let mut rows = Vec::with_capacity(nodes.len());
            for n in &nodes {
                match n.get_property(embedding).and_then(PropertyValue::to_vector) {
                    Some(v) => rows.push(v),
                    None if required => {
                        return Err(format!(
                            "{label} node {:?} has no vector in `{embedding}`; \
                         embed the snapshot first or pass --embedding-property",
                            n.id
                        ))
                    }
                    None => {
                        return Ok((
                            Vectors {
                                dim: 0,
                                data: Vec::new(),
                            },
                            HashMap::new(),
                        ))
                    }
                }
            }
            let dim = rows.first().map_or(0, Vec::len);
            if rows.iter().any(|r| r.len() != dim) {
                return Err(format!("{label}.{embedding} vectors differ in dimension"));
            }
            let ids = nodes
                .iter()
                .enumerate()
                .map(|(i, n)| (n.id, i as u32))
                .collect();
            Ok((
                Vectors {
                    dim,
                    data: rows.concat(),
                },
                ids,
            ))
        };
    let (papers, paper_ids) = vectors("Paper", true)?;
    if papers.is_empty() {
        return Err("the snapshot has no Paper nodes".into());
    }
    let (mut methods, mut method_ids) = vectors("Method", false)?;
    let (mut problems, mut problem_ids) = vectors("Problem", false)?;
    if methods.dim != papers.dim || problems.dim != papers.dim {
        methods = Vectors {
            dim: papers.dim,
            data: Vec::new(),
        };
        problems = Vectors {
            dim: papers.dim,
            data: Vec::new(),
        };
        method_ids.clear();
        problem_ids.clear();
    }
    let people_nodes = collect("Person");
    let person_ids: HashMap<NodeId, u32> = people_nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id, i as u32))
        .collect();
    let edges = |ty: &str, from: &HashMap<NodeId, u32>, to: &HashMap<NodeId, u32>| {
        dedup(
            store
                .get_edges_by_type(&ty.into())
                .iter()
                .filter_map(|e| Some((*from.get(&e.source)?, *to.get(&e.target)?)))
                .collect(),
        )
    };
    let authored = edges("AUTHORED", &person_ids, &paper_ids);
    let mut coauthor: Vec<(u32, u32)> = edges("COAUTHOR", &person_ids, &person_ids)
        .into_iter()
        .filter(|(a, b)| a != b)
        .map(|(a, b)| (a.min(b), a.max(b)))
        .collect();
    if coauthor.is_empty() {
        let mut authors_of: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(a, p) in &authored {
            authors_of.entry(p).or_default().push(a);
        }
        for authors in authors_of.values() {
            for (i, &a) in authors.iter().enumerate() {
                for &b in &authors[i + 1..] {
                    coauthor.push((a.min(b), a.max(b)));
                }
            }
        }
    }
    Ok(HybridGraph {
        cites: edges("CITES", &paper_ids, &paper_ids),
        uses_method: edges("USES_METHOD", &paper_ids, &method_ids),
        addresses: edges("ADDRESSES", &paper_ids, &problem_ids),
        coauthor: dedup(coauthor),
        authored,
        people: people_nodes.len(),
        papers,
        methods,
        problems,
    })
}

// ------------------------------------------------------------------ export

fn csv_vectors(path: &Path, v: &Vectors) -> Result<(), String> {
    let err = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).map_err(err)?);
    writeln!(w, "uid,emb").map_err(err)?;
    for i in 0..v.len() {
        let emb: Vec<String> = v.row(i).iter().map(|x| x.to_string()).collect();
        writeln!(w, "{i},{}", emb.join(";")).map_err(err)?;
    }
    w.flush().map_err(err)
}

fn csv_edges(path: &Path, edges: &[(u32, u32)]) -> Result<(), String> {
    let err = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).map_err(err)?);
    writeln!(w, "src,dst").map_err(err)?;
    for (a, b) in edges {
        writeln!(w, "{a},{b}").map_err(err)?;
    }
    w.flush().map_err(err)
}

/// The files `export` writes, in the order it writes them.
pub const EXPORT_FILES: [&str; 10] = [
    "papers.csv",
    "methods.csv",
    "problems.csv",
    "cites.csv",
    "authored.csv",
    "coauthor.csv",
    "uses_method.csv",
    "addresses.csv",
    "queries.json",
    "meta.json",
];

/// Write `g`, the workload and the reference answers to `dir`, for
/// `benches/graph_vector_hybrid/run_competitors.py`.
///
/// Node CSVs are `uid,emb` with the vector's components joined by `;`; edge
/// CSVs are `src,dst` in uids. People have no file: they are `0..people`
/// (in `meta.json`). `queries.json` holds each template's Cypher text,
/// exactly as this bench runs it, and every instance's parameters and
/// reference answer.
pub fn export(dir: &Path, g: &HybridGraph, w: &Workload, r: &Reference) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    csv_vectors(&dir.join("papers.csv"), &g.papers)?;
    csv_vectors(&dir.join("methods.csv"), &g.methods)?;
    csv_vectors(&dir.join("problems.csv"), &g.problems)?;
    csv_edges(&dir.join("cites.csv"), &g.cites)?;
    csv_edges(&dir.join("authored.csv"), &g.authored)?;
    csv_edges(&dir.join("coauthor.csv"), &g.coauthor)?;
    csv_edges(&dir.join("uses_method.csv"), &g.uses_method)?;
    csv_edges(&dir.join("addresses.csv"), &g.addresses)?;
    let queries = serde_json::json!({
        "seeded_expansion": {
            "cypher": T1_SEEDED_EXPANSION,
            "seeds": SEEDS,
            "k": K_EXPAND,
            "instances": w.expand.iter().map(|q| serde_json::json!({
                "q": q, "reference": r.seeded_expansion(q),
            })).collect::<Vec<_>>(),
        },
        "traversal_filtered": {
            "cypher": T2_TRAVERSAL_FILTERED,
            "k": K_FILTERED,
            "instances": w.filtered.iter().map(|(person, q)| serde_json::json!({
                "person": person, "q": q, "reference": r.traversal_filtered(*person, q),
            })).collect::<Vec<_>>(),
        },
        "vector_join": {
            "cypher": T3_VECTOR_JOIN,
            "k": K_JOIN,
            "instances": w.join.iter().map(|m| serde_json::json!({
                "method": m, "reference": r.vector_join(*m),
            })).collect::<Vec<_>>(),
        },
    });
    let meta = serde_json::json!({
        "dim": g.dim(),
        "similarity": "cosine",
        "papers": g.papers.len(),
        "people": g.people,
        "methods": g.methods.len(),
        "problems": g.problems.len(),
        "edges": g.edge_count(),
    });
    for (name, value) in [("queries.json", queries), ("meta.json", meta)] {
        let path = dir.join(name);
        let text = serde_json::to_string(&value).map_err(|e| e.to_string())?;
        std::fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(())
}
