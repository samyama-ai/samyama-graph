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
        let s = if selectivity.is_finite() {
            selectivity.clamp(0.0, 1.0)
        } else {
            1.0
        };
        let fetch = if s == 0.0 {
            n as f64
        } else {
            (k as f64 / s).ceil().min(n as f64)
        };
        let width = (ef as f64).max(2.0 * fetch).min(n as f64);
        // An exact predicate scan reads all ids, but distances only passing ids.
        let exact_work = n as f64 * (1.0 + s * dimensions as f64);
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
        }
    }
    pub fn describe(&self) -> String {
        format!("strategy={}, selectivity={:.6}, exact_work={:.0}, widen_work={:.0}, reason=estimated predicate+distance work (uncalibrated)",
            if self.strategy == FilterStrategy::Exact { "pre-exact" } else { "post-widen" }, self.selectivity, self.exact_work, self.widen_work)
    }
}
