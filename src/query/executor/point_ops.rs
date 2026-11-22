//! Operators over the point index (NDS-04, #1618): its DDL, the scan the
//! planner picks for a spatial predicate, and `db.index.point.nearest`.

use crate::graph::{GraphStore, Label, NodeId, PropertyValue};
use crate::index::point::{geographic_box, point_coords, Rect};
use crate::query::ast::{BinaryOp, Expression};
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// `CREATE POINT INDEX` / `DROP POINT INDEX`.
pub struct PointIndexDdlOperator {
    ddl: crate::query::ast::PointIndexDdl,
    executed: bool,
}

impl PointIndexDdlOperator {
    pub fn new(ddl: crate::query::ast::PointIndexDdl) -> Self {
        Self { ddl, executed: false }
    }
}

impl PhysicalOperator for PointIndexDdlOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError("point index DDL is a write; it needs next_mut".to_string()))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        use crate::query::ast::PointIndexDdl;
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        match &self.ddl {
            PointIndexDdl::Create { name, label, property } => {
                store.create_point_index(name, label.as_str(), property);
            }
            PointIndexDdl::Drop { name } => {
                if !store.drop_point_index(name) {
                    let known: Vec<String> = store.point_index.listing().into_iter().map(|(n, ..)| n).collect();
                    return Err(ExecutionError::RuntimeError(format!(
                        "no point index named '{name}'. Known point indexes: {}",
                        if known.is_empty() { "none".to_string() } else { known.join(", ") }
                    )));
                }
            }
        }
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        use crate::query::ast::PointIndexDdl;
        let (name, details) = match &self.ddl {
            PointIndexDdl::Create { name, label, property } => {
                ("CreatePointIndex", format!("{name} ON :{}({property})", label.as_str()))
            }
            PointIndexDdl::Drop { name } => ("DropPointIndex", name.clone()),
        };
        OperatorDescription { name: name.to_string(), details, children: Vec::new() }
    }
}

/// A region a spatial predicate confines `var.property` to, in the index's
/// coordinates, read from a predicate the planner found. Returns `None` for
/// anything it does not recognise, which leaves the scan to the planner's
/// other choices. The predicate stays in the filter either way.
pub fn spatial_region(var: &str, pred: &Expression, store: &GraphStore) -> Option<(String, Rect)> {
    let constant_point = |e: &Expression| -> Option<(f64, f64, bool)> {
        let mut vars = std::collections::HashSet::new();
        crate::query::executor::planner::QueryPlanner::collect_vars(e, &mut vars);
        if !vars.is_empty() {
            return None;
        }
        match eval_expression(e, &Record::new(), store).ok()? {
            Value::Property(v) => point_coords(&v),
            _ => None,
        }
    };
    let constant_number = |e: &Expression| -> Option<f64> {
        let mut vars = std::collections::HashSet::new();
        crate::query::executor::planner::QueryPlanner::collect_vars(e, &mut vars);
        if !vars.is_empty() {
            return None;
        }
        match eval_expression(e, &Record::new(), store).ok()? {
            Value::Property(PropertyValue::Float(f)) => Some(f),
            Value::Property(PropertyValue::Integer(i)) => Some(i as f64),
            _ => None,
        }
    };
    let property_of_var = |e: &Expression| -> Option<String> {
        match e {
            Expression::Property { variable, property } if variable == var => Some(property.clone()),
            _ => None,
        }
    };
    match pred {
        // point.withinBBox(n.p, lo, hi)
        Expression::Function { name, args, .. } if name.eq_ignore_ascii_case("point.withinBBox") && args.len() == 3 => {
            let prop = property_of_var(&args[0])?;
            let (lx, ly, lg) = constant_point(&args[1])?;
            let (hx, hy, hg) = constant_point(&args[2])?;
            // A box across the antimeridian (lo east of hi) is two boxes; the
            // scan declines it rather than answering half.
            if lg != hg || lx > hx || ly > hy {
                return None;
            }
            Some((prop, Rect { min_x: lx, min_y: ly, max_x: hx, max_y: hy }))
        }
        // point.distance(n.p, c) < r, <= r, and the mirrored r > / >= distance.
        Expression::Binary { left, op, right } => {
            let (dist, bound) = match op {
                BinaryOp::Lt | BinaryOp::Le => (left.as_ref(), right.as_ref()),
                BinaryOp::Gt | BinaryOp::Ge => (right.as_ref(), left.as_ref()),
                _ => return None,
            };
            let Expression::Function { name, args, .. } = dist else { return None };
            if !(name.eq_ignore_ascii_case("point.distance") || name.eq_ignore_ascii_case("distance")) || args.len() != 2 {
                return None;
            }
            let (prop, centre) = match (property_of_var(&args[0]), property_of_var(&args[1])) {
                (Some(p), None) => (p, &args[1]),
                (None, Some(p)) => (p, &args[0]),
                _ => return None,
            };
            let (cx, cy, geographic) = constant_point(centre)?;
            let r = constant_number(bound)?;
            // NaN or negative: no region to search.
            if r.is_nan() || r < 0.0 {
                return None;
            }
            let rect = if geographic {
                geographic_box(cx, cy, r)
            } else {
                Rect { min_x: cx - r, min_y: cy - r, max_x: cx + r, max_y: cy + r }
            };
            Some((prop, rect))
        }
        _ => None,
    }
}

/// Scan the nodes a point index places inside a region. A superset of the
/// answer: the predicate that produced the region filters above it.
pub struct PointIndexScanOperator {
    variable: String,
    label: Label,
    property: String,
    region: Rect,
    ids: Option<std::vec::IntoIter<NodeId>>,
}

impl PointIndexScanOperator {
    pub fn new(variable: String, label: Label, property: String, region: Rect) -> Self {
        Self { variable, label, property, region, ids: None }
    }
}

impl PhysicalOperator for PointIndexScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.ids.is_none() {
            let mut ids = store
                .point_index
                .with(self.label.as_str(), &self.property, |i| i.within(&self.region))
                .unwrap_or_default();
            // Ascending, like every other scan: downstream code that assumes a
            // scan's order (#1364) should not learn otherwise from this one.
            ids.sort();
            self.ids = Some(ids.into_iter());
        }
        for id in self.ids.as_mut().unwrap().by_ref() {
            if store.get_node(id).is_some_and(|n| n.labels.contains(&self.label)) {
                let mut r = Record::new();
                r.bind(self.variable.clone(), Value::NodeRef(id));
                return Ok(Some(r));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.ids = None;
    }

    fn estimate_rows(&self, store: &GraphStore, _children: &[Option<f64>]) -> Option<f64> {
        // The region's share of the index's own extent is not tracked, so the
        // estimate is the conventional range selectivity over the label.
        Some(store.label_node_count(&self.label) as f64 * 0.1)
    }

    fn describe(&self) -> OperatorDescription {
        let r = &self.region;
        OperatorDescription {
            name: "PointIndexScan".to_string(),
            details: format!(
                "var={}, {}.{} within [{}, {}]..[{}, {}]",
                self.variable,
                self.label.as_str(),
                self.property,
                r.min_x, r.min_y, r.max_x, r.max_y
            ),
            children: Vec::new(),
        }
    }
}

/// `CALL db.index.point.nearest(label, property, point, k) YIELD node, distance`.
///
/// Nearest by the distance `point.distance` reports: planar for a cartesian
/// index, great-circle metres for a geographic one. Both are exact k-NN; see
/// `PointIndex::nearest_geographic` for how the descent bounds metres.
pub struct PointNearestOperator {
    label: String,
    property: String,
    centre: PropertyValue,
    k: usize,
    node_var: String,
    distance_var: Option<String>,
    results: Option<std::vec::IntoIter<(NodeId, f64)>>,
}

impl PointNearestOperator {
    pub fn new(label: String, property: String, centre: PropertyValue, k: usize, node_var: String, distance_var: Option<String>) -> Self {
        Self { label, property, centre, k, node_var, distance_var, results: None }
    }
}

impl PhysicalOperator for PointNearestOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let (cx, cy, geographic) = point_coords(&self.centre).ok_or_else(|| {
                ExecutionError::TypeError("db.index.point.nearest: the third argument must be a point".to_string())
            })?;
            let ranked = store
                .point_index
                .with(&self.label, &self.property, |i| {
                    if geographic {
                        i.nearest_geographic(cx, cy, self.k)
                    } else {
                        i.nearest(cx, cy, self.k)
                    }
                })
                .ok_or_else(|| {
                    ExecutionError::RuntimeError(format!(
                        "no point index on :{}({}). CREATE POINT INDEX FOR (n:{}) ON (n.{}) creates one.",
                        self.label, self.property, self.label, self.property
                    ))
                })?;
            self.results = Some(ranked.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next().map(|(id, d)| {
            let mut r = Record::new();
            r.bind(self.node_var.clone(), Value::NodeRef(id));
            if let Some(v) = &self.distance_var {
                r.bind(v.clone(), Value::Property(PropertyValue::Float(d)));
            }
            r
        }))
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "PointNearest".to_string(),
            details: format!(":{}({}) k={}", self.label, self.property, self.k),
            children: Vec::new(),
        }
    }
}

/// The property and centre of a `point.distance(var.p, c)` sort key, either
/// operand order, when `c` is a constant point. `None` for any other key.
pub fn distance_sort_key(var: &str, key: &Expression, store: &GraphStore) -> Option<(String, (f64, f64, bool))> {
    let Expression::Function { name, args, .. } = key else { return None };
    if !(name.eq_ignore_ascii_case("point.distance") || name.eq_ignore_ascii_case("distance")) || args.len() != 2 {
        return None;
    }
    let property_of_var = |e: &Expression| match e {
        Expression::Property { variable, property } if variable == var => Some(property.clone()),
        _ => None,
    };
    let (property, centre) = match (property_of_var(&args[0]), property_of_var(&args[1])) {
        (Some(p), None) => (p, &args[1]),
        (None, Some(p)) => (p, &args[0]),
        _ => return None,
    };
    let mut vars = std::collections::HashSet::new();
    crate::query::executor::planner::QueryPlanner::collect_vars(centre, &mut vars);
    if !vars.is_empty() {
        return None;
    }
    let centre = match eval_expression(centre, &Record::new(), store).ok()? {
        Value::Property(v) => point_coords(&v)?,
        _ => return None,
    };
    if !(centre.0.is_finite() && centre.1.is_finite()) {
        return None;
    }
    Some((property, centre))
}

/// The nodes a point index ranks nearest a centre, as the scan under a
/// single-node `ORDER BY point.distance(n.p, c) LIMIT k` (#2255).
///
/// A superset of the answer, not the answer: the `Sort` and `Limit` the query
/// wrote stay above this, so the rows they keep are the ones they would keep
/// over the whole label. It emits the `k` nearest and every node tied with
/// the `k`-th, with a little slack for the last bit of a float, which is what
/// lets a second `ORDER BY` key still choose among a tie at the boundary.
pub struct PointNearestScanOperator {
    variable: String,
    label: Label,
    property: String,
    centre: (f64, f64, bool),
    k: usize,
    ids: Option<std::vec::IntoIter<NodeId>>,
}

impl PointNearestScanOperator {
    pub fn new(variable: String, label: Label, property: String, centre: (f64, f64, bool), k: usize) -> Self {
        Self { variable, label, property, centre, k, ids: None }
    }

    fn candidates(&self, store: &GraphStore) -> Vec<NodeId> {
        let (cx, cy, geographic) = self.centre;
        let ranked = |want: usize| {
            store
                .point_index
                .with(self.label.as_str(), &self.property, |i| {
                    if geographic {
                        i.nearest_geographic(cx, cy, want)
                    } else {
                        i.nearest(cx, cy, want)
                    }
                })
                .unwrap_or_default()
        };
        let mut want = self.k.max(1);
        let mut r = ranked(want);
        loop {
            if r.len() < self.k {
                // Fewer points than k: every one of them.
                break;
            }
            let kth = r[self.k - 1].1;
            let edge = kth + kth.abs() * 1e-9 + 1e-9;
            let last = r.last().map_or(f64::INFINITY, |(_, d)| *d);
            if r.len() < want || last > edge {
                r.retain(|(_, d)| *d <= edge);
                break;
            }
            // The last one fetched is still tied with the k-th; there may be more.
            want = want.saturating_mul(2);
            r = ranked(want);
        }
        r.into_iter().map(|(id, _)| id).collect()
    }
}

impl PhysicalOperator for PointNearestScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.ids.is_none() {
            self.ids = Some(self.candidates(store).into_iter());
        }
        for id in self.ids.as_mut().unwrap().by_ref() {
            if store.get_node(id).is_some_and(|n| n.labels.contains(&self.label)) {
                let mut r = Record::new();
                r.bind(self.variable.clone(), Value::NodeRef(id));
                return Ok(Some(r));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.ids = None;
    }

    fn estimate_rows(&self, _store: &GraphStore, _children: &[Option<f64>]) -> Option<f64> {
        Some(self.k as f64)
    }

    fn describe(&self) -> OperatorDescription {
        let (x, y, geographic) = self.centre;
        OperatorDescription {
            name: "PointIndexNearest".to_string(),
            details: format!(
                "var={}, {}.{} nearest ({x}, {y}){} k={}",
                self.variable,
                self.label.as_str(),
                self.property,
                if geographic { " wgs-84" } else { "" },
                self.k
            ),
            children: Vec::new(),
        }
    }
}
