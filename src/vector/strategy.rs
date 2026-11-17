//! Costed routing between the three implemented filtered-search strategies.
//! Work units are heuristic, not calibrated latency predictions (#1970).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Score every node the predicate passes, exactly.
    Exact,
    /// Search the graph without the predicate, filter what comes back, and
    /// widen the search until `k` pass.
    Widen,
    /// Search the graph with the predicate inside the walk, so the beam only
    /// keeps passing nodes and one search returns `k` of them (#1999).
    Inline,
}
#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub strategy: FilterStrategy,
    pub selectivity: f64,
    pub exact_work: f64,
    pub widen_work: f64,
    /// Work of one graph search with the predicate in the walk (#1999).
    pub inline_work: f64,
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
    /// the filter passes; widening and the in-filter search (vector-first)
    /// search the graph with the subtree test as their filter, at the
    /// combined selectivity.
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
    /// [`estimate`](Self::estimate) for an IVF index (#1975), which has no
    /// graph to widen: a filtered search scans its nearest `nprobe` lists
    /// with the predicate, and probes more until `k` pass.
    pub fn estimate_ivf(
        n: usize,
        dimensions: usize,
        k: usize,
        nlist: usize,
        nprobe: usize,
        selectivity: f64,
    ) -> Self {
        let s = clamp_selectivity(selectivity);
        let exact_work = n as f64 * (1.0 + s * dimensions as f64);
        Self::priced_ivf(n, dimensions, k, nlist, nprobe, s, exact_work, None)
    }
    /// [`estimate_enumerated`](Self::estimate_enumerated) for an IVF index.
    #[allow(clippy::too_many_arguments)]
    pub fn estimate_enumerated_ivf(
        n: usize,
        dimensions: usize,
        k: usize,
        nlist: usize,
        nprobe: usize,
        candidates: usize,
        filter_selectivity: f64,
    ) -> Self {
        let f = clamp_selectivity(filter_selectivity);
        let reachable = candidates.min(n);
        let s = if n == 0 { 0.0 } else { reachable as f64 / n as f64 * f };
        let exact_work = reachable as f64 * (1.0 + f * dimensions as f64);
        Self::priced_ivf(n, dimensions, k, nlist, nprobe, s, exact_work, Some(candidates))
    }
    /// Ranking the centroids, then reading the vectors of the probed lists
    /// -- `nprobe / nlist` of the index, or as many as it takes for `k` to
    /// pass -- and scoring the ones that pass.
    #[allow(clippy::too_many_arguments)]
    fn priced_ivf(
        n: usize,
        dimensions: usize,
        k: usize,
        nlist: usize,
        nprobe: usize,
        s: f64,
        exact_work: f64,
        candidates: Option<usize>,
    ) -> Self {
        let nlist = nlist.max(1);
        let fetch = if s == 0.0 { n as f64 } else { k as f64 / s };
        let probed = n as f64 * nprobe.min(nlist) as f64 / nlist as f64;
        let scanned = probed.max(fetch).min(n as f64);
        let widen_work = (nlist * dimensions) as f64 + scanned * (1.0 + s * dimensions as f64);
        // No graph, so no in-filter walk: infinite work never wins.
        Self::decide(s, exact_work, widen_work, f64::INFINITY, candidates)
    }
    fn decide(
        s: f64,
        exact_work: f64,
        widen_work: f64,
        inline_work: f64,
        candidates: Option<usize>,
    ) -> Self {
        let strategy = if exact_work <= widen_work.min(inline_work) {
            FilterStrategy::Exact
        } else if inline_work < widen_work {
            FilterStrategy::Inline
        } else {
            FilterStrategy::Widen
        };
        Self {
            strategy,
            selectivity: s,
            exact_work,
            widen_work,
            inline_work,
            candidates,
        }
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
        let nf = n as f64;
        let links = (2 * m) as f64;
        let d = dimensions as f64;
        let fetch = if s == 0.0 {
            nf
        } else {
            (k as f64 / s).ceil().min(nf)
        };
        // Widening repeats the search, doubling what it fetches from `k`
        // until `k` pass: each round is a search as wide as twice its fetch,
        // and asks the predicate about every result. A predicate that passes
        // one node in s needs about log2(1/s) rounds, priced here, not just
        // the last one.
        let mut widen_work = 0.0;
        let mut round = (k as f64).min(fetch).max(1.0);
        loop {
            let width = (ef as f64).max(2.0 * round).min(nf);
            // Include layer-0 links, plus predicates on returned candidates.
            widen_work += width * links * d + round;
            if round >= fetch {
                break;
            }
            round = (2.0 * round).min(fetch);
        }
        // The in-filter search walks once, but keeps only passing nodes in
        // its beam, so to fill a beam of `ef` it expands about `ef / s`
        // nodes, each reading its layer-0 links; the predicate is asked once
        // per node it reaches (#1999). Neither exceeds the index.
        let beam = (ef.max(k) as f64).min(nf);
        let expanded = if s == 0.0 { nf } else { (beam / s).min(nf) };
        let inline_work = (expanded * links).min(nf) * d + expanded;
        Self::decide(s, exact_work, widen_work, inline_work, candidates)
    }
    /// The name EXPLAIN prints for the strategy.
    pub fn strategy_name(&self) -> &'static str {
        match self.strategy {
            FilterStrategy::Exact => "pre-exact",
            FilterStrategy::Widen => "post-widen",
            FilterStrategy::Inline => "in-filter",
        }
    }
    pub fn describe(&self) -> String {
        let order = match (self.candidates, self.strategy) {
            (Some(c), FilterStrategy::Exact) => format!(", order=hierarchy-first, subtree={c}"),
            (Some(c), FilterStrategy::Widen | FilterStrategy::Inline) => {
                format!(", order=vector-first, subtree={c}")
            }
            (None, _) => String::new(),
        };
        // An IVF index has no in-filter walk to price (#1975).
        let inline = if self.inline_work.is_finite() {
            format!(", inline_work={:.0}", self.inline_work)
        } else {
            String::new()
        };
        format!(
            "strategy={}{order}, selectivity={:.6}, exact_work={:.0}, widen_work={:.0}{inline}, reason=estimated predicate+distance work (uncalibrated)",
            self.strategy_name(),
            self.selectivity,
            self.exact_work,
            self.widen_work,
        )
    }
}
fn clamp_selectivity(selectivity: f64) -> f64 {
    if selectivity.is_finite() {
        selectivity.clamp(0.0, 1.0)
    } else {
        1.0
    }
}
