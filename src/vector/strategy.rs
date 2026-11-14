//! Costed routing between the two implemented filtered-search strategies.
//! Work units are heuristic, not calibrated latency predictions (#1970).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    Exact,
    Widen,
}
#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub strategy: FilterStrategy,
    pub selectivity: f64,
    pub exact_work: f64,
    pub widen_work: f64,
    /// Size of a candidate set the exact strategy enumerates instead of
    /// scanning every vector: a hierarchy subtree (#445). `None` when the
    /// exact strategy scans the whole index.
    pub candidates: Option<usize>,
}
impl FilterPlan {
    pub fn estimate(
        n: usize,
        dimensions: usize,
        k: usize,
        ef: usize,
        m: usize,
        selectivity: f64,
    ) -> Self {
        let s = clamp_selectivity(selectivity);
        // An exact predicate scan reads all ids, but distances only passing ids.
        let exact_work = n as f64 * (1.0 + s * dimensions as f64);
        Self::priced(n, dimensions, k, ef, m, s, exact_work, None)
    }
    /// Price a search restricted to a candidate set the planner can enumerate
    /// -- the reflexive subtree under a hierarchy root, whose size the OEH
    /// index answers without walking it (#445). `filter_selectivity` is the
    /// share of those candidates any further predicate passes, 1.0 when
    /// there is none or it cannot be estimated.
    ///
    /// Exact (hierarchy-first) reads only the candidates and scores the ones
    /// the filter passes; widening (vector-first) searches the graph with the
    /// subtree test as its filter, at the combined selectivity.
    pub fn estimate_enumerated(
        n: usize,
        dimensions: usize,
        k: usize,
        ef: usize,
        m: usize,
        candidates: usize,
        filter_selectivity: f64,
    ) -> Self {
        let f = clamp_selectivity(filter_selectivity);
        let reachable = candidates.min(n);
        let s = if n == 0 { 0.0 } else { reachable as f64 / n as f64 * f };
        let exact_work = reachable as f64 * (1.0 + f * dimensions as f64);
        Self::priced(n, dimensions, k, ef, m, s, exact_work, Some(candidates))
    }
    #[allow(clippy::too_many_arguments)]
    fn priced(
        n: usize,
        dimensions: usize,
        k: usize,
        ef: usize,
        m: usize,
        s: f64,
        exact_work: f64,
        candidates: Option<usize>,
    ) -> Self {
        let fetch = if s == 0.0 {
            n as f64
        } else {
            (k as f64 / s).ceil().min(n as f64)
        };
        let width = (ef as f64).max(2.0 * fetch).min(n as f64);
        // Include layer-0 links, plus predicates on returned candidates.
        let widen_work = width * (2 * m) as f64 * dimensions as f64 + fetch;
        Self {
            strategy: if exact_work <= widen_work {
                FilterStrategy::Exact
            } else {
                FilterStrategy::Widen
            },
            selectivity: s,
            exact_work,
            widen_work,
            candidates,
        }
    }
    pub fn describe(&self) -> String {
        let order = match (self.candidates, self.strategy) {
            (Some(c), FilterStrategy::Exact) => format!(", order=hierarchy-first, subtree={c}"),
            (Some(c), FilterStrategy::Widen) => format!(", order=vector-first, subtree={c}"),
            (None, _) => String::new(),
        };
        format!("strategy={}{order}, selectivity={:.6}, exact_work={:.0}, widen_work={:.0}, reason=estimated predicate+distance work (uncalibrated)",
            if self.strategy == FilterStrategy::Exact { "pre-exact" } else { "post-widen" }, self.selectivity, self.exact_work, self.widen_work)
    }
}
fn clamp_selectivity(selectivity: f64) -> f64 {
    if selectivity.is_finite() {
        selectivity.clamp(0.0, 1.0)
    } else {
        1.0
    }
}
