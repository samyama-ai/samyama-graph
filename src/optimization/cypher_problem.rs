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
    /// Memoization cache: quantized vector hash -> (objective, optional penalty).
    cache: Mutex<HashMap<u64, (f64, Option<f64>)>>,
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
        if let Some(&(obj, _)) = self.cache.lock().unwrap().get(&key) {
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
            self.cache.lock().unwrap().insert(key, (val, None));
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
        let entry = cache.entry(key).or_insert((f64::INFINITY, None));
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
}
