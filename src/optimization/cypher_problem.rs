//! Cypher-driven optimization problem.

use crate::graph::{GraphStore, PropertyValue};
use crate::query::QueryEngine;
use crate::query::executor::record::Value;
use ndarray::Array1;
use samyama_optimization::common::{MultiObjectiveProblem, Problem};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Default, Clone)]
pub struct CypherProblemStats {
    pub hits: u64,
    pub misses: u64,
    pub total_eval_ms: u128,
    pub penalty_evals: u64,
}

/// A graph-grounded single-objective optimization problem.
///
/// `objective_template` is a Cypher string with `$x0`, `$x1`, ...
/// placeholders that are substituted with the decision-vector components
/// at each evaluation. The query must return a single scalar column on its
/// first record; that scalar (cast to f64) is the objective value. If the
/// query returns no records or a non-numeric value, the problem reports
/// `f64::INFINITY` and that evaluation is not cached.
///
/// `penalty_template` is optional and, if present, is evaluated the same
/// way; its return value is added to the objective via [`Problem::fitness`].
pub type CustomSubsFn = Box<dyn Fn(&Array1<f64>) -> Vec<(String, String)> + Send + Sync>;

pub struct CypherProblem {
    pub dim: usize,
    pub lower: Array1<f64>,
    pub upper: Array1<f64>,
    pub objective_template: String,
    pub penalty_template: Option<String>,
    pub graph: Arc<RwLock<GraphStore>>,
    pub engine: Arc<QueryEngine>,
    /// Quantization resolution for cache-key formation. Default 1e-10.
    pub quantize: f64,
    /// Optional user hook to compute custom (placeholder, value) substitutions
    /// from the decision vector. Applied BEFORE the standard `$x0..$xN`
    /// substitution, so callers can map decision vars to e.g. selected-item
    /// lists. Useful for discrete / mixed problems where embedding CASE
    /// expressions in `sum()` is awkward.
    pub custom_subs: Option<CustomSubsFn>,
    /// Memoization cache: quantized vector hash -> (objective, penalty). Either may be
    /// missing: a penalty evaluated first leaves the objective unset rather than caching a
    /// placeholder that `objective` would read back as a result (#1574).
    cache: Mutex<HashMap<u64, (Option<f64>, Option<f64>)>>,
    stats: Mutex<CypherProblemStats>,
}

impl CypherProblem {
    pub fn new(
        dim: usize,
        lower: Array1<f64>,
        upper: Array1<f64>,
        objective_template: impl Into<String>,
        graph: Arc<RwLock<GraphStore>>,
        engine: Arc<QueryEngine>,
    ) -> Self {
        Self {
            dim, lower, upper,
            objective_template: objective_template.into(),
            penalty_template: None,
            graph, engine,
            quantize: 1e-10,
            custom_subs: None,
            cache: Mutex::new(HashMap::new()),
            stats: Mutex::new(CypherProblemStats::default()),
        }
    }

    pub fn with_penalty(mut self, template: impl Into<String>) -> Self {
        self.penalty_template = Some(template.into());
        self
    }

    pub fn with_subs<F>(mut self, f: F) -> Self
    where F: Fn(&Array1<f64>) -> Vec<(String, String)> + Send + Sync + 'static {
        self.custom_subs = Some(Box::new(f));
        self
    }

    pub fn stats(&self) -> CypherProblemStats {
        self.stats.lock().unwrap().clone()
    }

    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

/// Substitute `$x0`, `$x1`, ..., `$xN` with f64 values formatted as full
/// decimals (no thousand separators, no scientific notation for typical
/// ranges, deterministic locale-independent).
fn substitute(template: &str, x: &Array1<f64>, custom: Option<&CustomSubsFn>) -> String {
    let mut out = template.to_string();
    if let Some(f) = custom {
        for (pat, val) in f(x) {
            out = out.replace(&pat, &val);
        }
    }
    // Reverse iteration so $x10 is replaced before $x1.
    for i in (0..x.len()).rev() {
        let pat = format!("$x{}", i);
        // Plain decimal (not scientific) — some Cypher parsers reject "0e0".
        // 17 fractional digits preserves f64 round-trip for typical ranges.
        let val = format!("{:.17}", x[i]);
        out = out.replace(&pat, &val);
    }
    out
}

fn hash_quantized(x: &Array1<f64>, q: f64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    for &v in x.iter() {
        let qv = (v / q).round() as i64;
        qv.hash(&mut h);
    }
    h.finish()
}

fn scalar_from_batch(batch: &crate::query::executor::record::RecordBatch) -> Option<f64> {
    let rec = batch.records.first()?;
    let col = batch.columns.first()?;
    let v = rec.get(col)?;
    match v {
        Value::Property(PropertyValue::Float(f)) => Some(*f),
        Value::Property(PropertyValue::Integer(i)) => Some(*i as f64),
        Value::Property(PropertyValue::Boolean(b)) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

impl Problem for CypherProblem {
    fn dim(&self) -> usize { self.dim }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) { (self.lower.clone(), self.upper.clone()) }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        let key = hash_quantized(variables, self.quantize);
        if let Some(&(Some(obj), _)) = self.cache.lock().unwrap().get(&key) {
            self.stats.lock().unwrap().hits += 1;
            return obj;
        }
        let t0 = std::time::Instant::now();
        let query = substitute(&self.objective_template, variables, self.custom_subs.as_ref());
        let store = self.graph.read().unwrap();
        let val = match self.engine.execute(&query, &store) {
            Ok(batch) => scalar_from_batch(&batch).unwrap_or(f64::INFINITY),
            Err(_) => f64::INFINITY,
        };
        let elapsed = t0.elapsed().as_millis();
        {
            let mut s = self.stats.lock().unwrap();
            s.misses += 1;
            s.total_eval_ms += elapsed;
        }
        if val.is_finite() {
            self.cache
                .lock()
                .unwrap()
                .entry(key)
                .or_insert((None, None))
                .0 = Some(val);
        }
        val
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        let Some(tmpl) = &self.penalty_template else { return 0.0; };
        let key = hash_quantized(variables, self.quantize);
        if let Some(&(_, Some(pen))) = self.cache.lock().unwrap().get(&key) {
            self.stats.lock().unwrap().hits += 1;
            return pen;
        }
        let query = substitute(tmpl, variables, self.custom_subs.as_ref());
        let store = self.graph.read().unwrap();
        let val = match self.engine.execute(&query, &store) {
            Ok(batch) => scalar_from_batch(&batch).unwrap_or(0.0),
            Err(_) => 0.0,
        };
        {
            let mut s = self.stats.lock().unwrap();
            s.penalty_evals += 1;
        }
        let mut cache = self.cache.lock().unwrap();
        let entry = cache.entry(key).or_insert((None, None));
        entry.1 = Some(val);
        val
    }
}

// Safety: GraphStore is wrapped in RwLock; QueryEngine is shared via Arc and uses
// internal Mutex for the AST cache.
unsafe impl Sync for CypherProblem {}
unsafe impl Send for CypherProblem {}

/// Multi-objective variant: each template returns a scalar, collected into a vector.
pub struct CypherMOProblem {
    pub dim: usize,
    pub lower: Array1<f64>,
    pub upper: Array1<f64>,
    pub objective_templates: Vec<String>,
    pub graph: Arc<RwLock<GraphStore>>,
    pub engine: Arc<QueryEngine>,
    pub quantize: f64,
    cache: Mutex<HashMap<u64, Vec<f64>>>,
    stats: Mutex<CypherProblemStats>,
}

impl CypherMOProblem {
    pub fn new(
        dim: usize,
        lower: Array1<f64>,
        upper: Array1<f64>,
        objective_templates: Vec<String>,
        graph: Arc<RwLock<GraphStore>>,
        engine: Arc<QueryEngine>,
    ) -> Self {
        Self {
            dim, lower, upper, objective_templates, graph, engine,
            quantize: 1e-10,
            cache: Mutex::new(HashMap::new()),
            stats: Mutex::new(CypherProblemStats::default()),
        }
    }

    pub fn stats(&self) -> CypherProblemStats { self.stats.lock().unwrap().clone() }
}

impl MultiObjectiveProblem for CypherMOProblem {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { self.objective_templates.len() }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) { (self.lower.clone(), self.upper.clone()) }

    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        let key = hash_quantized(variables, self.quantize);
        if let Some(v) = self.cache.lock().unwrap().get(&key) {
            self.stats.lock().unwrap().hits += 1;
            return v.clone();
        }
        let t0 = std::time::Instant::now();
        let store = self.graph.read().unwrap();
        let mut out = Vec::with_capacity(self.objective_templates.len());
        for tmpl in &self.objective_templates {
            let q = substitute(tmpl, variables, None);
            let v = match self.engine.execute(&q, &store) {
                Ok(b) => scalar_from_batch(&b).unwrap_or(f64::INFINITY),
                Err(_) => f64::INFINITY,
            };
            out.push(v);
        }
        let elapsed = t0.elapsed().as_millis();
        {
            let mut s = self.stats.lock().unwrap();
            s.misses += 1;
            s.total_eval_ms += elapsed;
        }
        if out.iter().all(|v| v.is_finite()) {
            self.cache.lock().unwrap().insert(key, out.clone());
        }
        out
    }
}

unsafe impl Sync for CypherMOProblem {}
unsafe impl Send for CypherMOProblem {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphStore, Label};

    fn build_test_graph() -> (Arc<RwLock<GraphStore>>, Arc<QueryEngine>) {
        let mut g = GraphStore::new();
        let n1 = g.create_node(Label::new("Item"));
        g.get_node_mut(n1).unwrap().set_property("weight", PropertyValue::Float(2.0));
        let n2 = g.create_node(Label::new("Item"));
        g.get_node_mut(n2).unwrap().set_property("weight", PropertyValue::Float(3.0));
        (Arc::new(RwLock::new(g)), Arc::new(QueryEngine::new()))
    }

    #[test]
    fn objective_evaluates_cypher() {
        let (g, e) = build_test_graph();
        // Objective: sum of squared decision vars (no graph access; sanity)
        let problem = CypherProblem::new(
            2,
            Array1::from(vec![-10.0, -10.0]),
            Array1::from(vec![10.0, 10.0]),
            "RETURN $x0 * $x0 + $x1 * $x1 AS f",
            g, e,
        );
        let v = problem.objective(&Array1::from(vec![3.0, 4.0]));
        assert!((v - 25.0).abs() < 1e-6, "got {}", v);
    }

    #[test]
    fn memoization_serves_repeat_calls() {
        let (g, e) = build_test_graph();
        let problem = CypherProblem::new(
            1, Array1::from(vec![0.0]), Array1::from(vec![10.0]),
            "RETURN $x0 AS f", g, e,
        );
        let x = Array1::from(vec![1.5]);
        for _ in 0..5 { problem.objective(&x); }
        let s = problem.stats();
        assert_eq!(s.misses, 1, "first call misses");
        assert_eq!(s.hits, 4, "next 4 hit cache");
    }

    #[test]
    fn graph_property_in_objective() {
        let (g, e) = build_test_graph();
        // Sum item weights weighted by decision var components
        let problem = CypherProblem::new(
            1, Array1::from(vec![0.0]), Array1::from(vec![10.0]),
            "MATCH (i:Item) RETURN sum(i.weight) * $x0 AS f",
            g, e,
        );
        let v = problem.objective(&Array1::from(vec![2.0]));
        // sum(weight) = 5.0; 5.0 * 2.0 = 10.0
        assert!((v - 10.0).abs() < 1e-6, "got {}", v);
    }

    fn one_dim(template: &str) -> CypherProblem {
        let (g, e) = build_test_graph();
        CypherProblem::new(1, Array1::from(vec![0.0]), Array1::from(vec![10.0]), template, g, e)
    }

    #[test]
    fn dim_and_bounds_are_reported() {
        let p = one_dim("RETURN 1");
        assert_eq!(Problem::dim(&p), 1);
        let (lo, hi) = Problem::bounds(&p);
        assert_eq!(lo.to_vec(), vec![0.0]);
        assert_eq!(hi.to_vec(), vec![10.0]);
        assert_eq!(p.quantize, 1e-10);
        assert!(p.penalty_template.is_none() && p.custom_subs.is_none());
    }

    #[test]
    fn substitute_replaces_high_indices_before_low_ones() {
        let x = Array1::from((0..11).map(|i| i as f64).collect::<Vec<_>>());
        let out = substitute("$x10 + $x1", &x, None);
        assert_eq!(out, format!("{:.17} + {:.17}", 10.0, 1.0));
    }

    #[test]
    fn substitute_applies_custom_subs_first() {
        let f: CustomSubsFn = Box::new(|x: &Array1<f64>| vec![("$picked".to_string(), format!("[{}]", x[0] as i64))]);
        let out = substitute("RETURN size($picked) + $x0", &Array1::from(vec![3.0]), Some(&f));
        assert_eq!(out, format!("RETURN size([3]) + {:.17}", 3.0));
    }

    #[test]
    fn hash_quantized_groups_values_within_resolution() {
        let a = hash_quantized(&Array1::from(vec![1.0]), 0.5);
        let b = hash_quantized(&Array1::from(vec![1.1]), 0.5);
        let c = hash_quantized(&Array1::from(vec![2.0]), 0.5);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn integer_and_boolean_results_are_numeric() {
        assert_eq!(one_dim("RETURN 7 AS f").objective(&Array1::from(vec![0.0])), 7.0);
        assert_eq!(one_dim("RETURN true AS f").objective(&Array1::from(vec![0.0])), 1.0);
        assert_eq!(one_dim("RETURN false AS f").objective(&Array1::from(vec![0.0])), 0.0);
    }

    #[test]
    fn non_numeric_empty_or_failing_objectives_are_infinite_and_not_cached() {
        for tmpl in [
            "RETURN 'text' AS f",
            "MATCH (n:Missing) RETURN n.w AS f",
            "THIS IS NOT CYPHER",
        ] {
            let p = one_dim(tmpl);
            let x = Array1::from(vec![1.0]);
            assert_eq!(p.objective(&x), f64::INFINITY, "{tmpl}");
            assert_eq!(p.objective(&x), f64::INFINITY, "{tmpl}");
            assert_eq!(p.cache_size(), 0, "{tmpl}");
            assert_eq!(p.stats().misses, 2, "{tmpl}");
            assert_eq!(p.stats().hits, 0, "{tmpl}");
        }
    }

    #[test]
    fn scalar_from_batch_needs_a_row_and_a_column() {
        use crate::query::executor::record::RecordBatch;
        assert_eq!(scalar_from_batch(&RecordBatch::new(vec!["f".into()])), None);
        let mut no_cols = RecordBatch::new(vec![]);
        no_cols.records.push(crate::query::executor::record::Record::new());
        assert_eq!(scalar_from_batch(&no_cols), None);
    }

    #[test]
    fn penalty_is_zero_without_template() {
        let p = one_dim("RETURN $x0 AS f");
        assert_eq!(p.penalty(&Array1::from(vec![4.0])), 0.0);
        assert_eq!(p.fitness(&Array1::from(vec![4.0])), 4.0);
        assert_eq!(p.stats().penalty_evals, 0);
    }

    #[test]
    fn penalty_is_added_to_fitness_and_memoized() {
        let p = one_dim("RETURN $x0 AS f").with_penalty("RETURN $x0 * 10 AS p");
        let x = Array1::from(vec![2.0]);
        assert!((p.fitness(&x) - 22.0).abs() < 1e-9);
        let s = p.stats();
        assert_eq!((s.misses, s.hits, s.penalty_evals), (1, 0, 1));
        // Second fitness: objective and penalty both come from the cache.
        assert!((p.fitness(&x) - 22.0).abs() < 1e-9);
        let s = p.stats();
        assert_eq!((s.misses, s.hits, s.penalty_evals), (1, 2, 1));
        assert_eq!(p.cache_size(), 1);
    }

    #[test]
    fn failing_or_non_numeric_penalty_counts_as_zero() {
        let x = Array1::from(vec![1.0]);
        let bad = one_dim("RETURN 1 AS f").with_penalty("NOT CYPHER");
        assert_eq!(bad.penalty(&x), 0.0);
        let text = one_dim("RETURN 1 AS f").with_penalty("RETURN 'x' AS p");
        assert_eq!(text.penalty(&x), 0.0);
        assert_eq!(text.stats().penalty_evals, 1);
    }

    #[test]
    fn with_subs_feeds_custom_placeholders_into_the_query() {
        let p = one_dim("RETURN size($items) AS f")
            .with_subs(|x| vec![("$items".into(), format!("range(1, {})", x[0] as i64))]);
        assert_eq!(p.objective(&Array1::from(vec![4.0])), 4.0);
    }

    #[test]
    fn penalty_first_does_not_poison_objective_cache() {
        let p = one_dim("RETURN $x0 AS f").with_penalty("RETURN 0 AS p");
        let x = Array1::from(vec![3.0]);
        assert_eq!(p.penalty(&x), 0.0);
        assert_eq!(p.objective(&x), 3.0);
    }

    fn mo(templates: &[&str]) -> CypherMOProblem {
        let (g, e) = build_test_graph();
        CypherMOProblem::new(
            1,
            Array1::from(vec![-1.0]),
            Array1::from(vec![1.0]),
            templates.iter().map(|s| s.to_string()).collect(),
            g,
            e,
        )
    }

    #[test]
    fn multi_objective_evaluates_each_template_and_memoizes() {
        let p = mo(&["RETURN $x0 AS a", "MATCH (i:Item) RETURN sum(i.weight) AS b"]);
        assert_eq!(p.num_objectives(), 2);
        assert_eq!(MultiObjectiveProblem::dim(&p), 1);
        let (lo, hi) = MultiObjectiveProblem::bounds(&p);
        assert_eq!((lo[0], hi[0]), (-1.0, 1.0));

        let x = Array1::from(vec![0.5]);
        assert_eq!(p.objectives(&x), vec![0.5, 5.0]);
        assert_eq!(p.objectives(&x), vec![0.5, 5.0]);
        let s = p.stats();
        assert_eq!((s.misses, s.hits), (1, 1));
    }

    #[test]
    fn multi_objective_with_a_failing_template_is_infinite_and_not_cached() {
        let p = mo(&["RETURN 1 AS a", "BROKEN"]);
        let x = Array1::from(vec![0.0]);
        assert_eq!(p.objectives(&x), vec![1.0, f64::INFINITY]);
        assert_eq!(p.objectives(&x), vec![1.0, f64::INFINITY]);
        let s = p.stats();
        assert_eq!((s.misses, s.hits), (2, 0));
    }
}
