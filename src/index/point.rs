//! Point index: an R-tree over a property holding points (NDS-04, #1618).
//!
//! A point here is a map -- `point({x, y})` or `point({latitude, longitude})`
//! -- so the index reads the coordinates out of the property value with the
//! same rules `point()` uses: a map with `latitude`/`longitude` (or a WGS-84
//! `crs` / `srid` 4326) is geographic and stored as `(longitude, latitude)`;
//! one with `x`/`y` is cartesian. A value that is not a point is not indexed,
//! which is what a B-tree does with a value of the wrong type for its range.
//!
//! It answers two questions without a scan:
//!
//! - **within a box** -- every point inside `[lo, hi]`, for
//!   `point.withinBBox(n.p, lo, hi)` and as the candidate set of
//!   `point.distance(n.p, c) < r` (a distance bound is a box around `c`);
//! - **nearest k** -- best-first descent ordered by a lower bound on the
//!   distance from the query point to each box, for `db.index.point.nearest`.
//!   Exact for both kinds: a geographic index bounds great-circle distance,
//!   not distance in degrees.
//!
//! Candidates are a superset: the box around a geographic distance bound is
//! conservative, and the predicate that asked stays in the filter above the
//! scan. The index can make a query cheaper, never change its answer.
//!
//! The tree is a plain R-tree: insert descends by least enlargement and splits
//! a full node by sorting its entries along the wider axis. Removal deletes
//! the entry and leaves the node underfull rather than re-inserting -- a
//! looser tree after many deletes, never a wrong one.

use crate::graph::{NodeId, PropertyValue};
use std::collections::{BinaryHeap, HashMap};

/// Entries per node before it splits.
const MAX_ENTRIES: usize = 16;

/// Mean earth radius, metres: the sphere `point.distance` measures on. The
/// index sizes its boxes and bounds its k-NN from this same constant, so the
/// candidate set and the predicate above it cannot disagree about where a
/// radius reaches.
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Metres per degree of latitude on that sphere (111,195.08 m).
pub const METRES_PER_DEGREE: f64 = std::f64::consts::TAU * EARTH_RADIUS_M / 360.0;

/// Great-circle distance in metres between two `(longitude, latitude)` points,
/// by haversine. The one `point.distance` reports for geographic points.
pub fn haversine_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lat1, lat2) = (a.1.to_radians(), b.1.to_radians());
    let dlat = (b.1 - a.1).to_radians();
    let dlon = (b.0 - a.0).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.min(1.0).sqrt().asin()
}

/// An axis-aligned box. A point is a box with no area.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rect {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl Rect {
    pub fn point(x: f64, y: f64) -> Self {
        Self { min_x: x, min_y: y, max_x: x, max_y: y }
    }

    fn union(&self, o: &Rect) -> Rect {
        Rect {
            min_x: self.min_x.min(o.min_x),
            min_y: self.min_y.min(o.min_y),
            max_x: self.max_x.max(o.max_x),
            max_y: self.max_y.max(o.max_y),
        }
    }

    fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    fn intersects(&self, o: &Rect) -> bool {
        self.min_x <= o.max_x && o.min_x <= self.max_x && self.min_y <= o.max_y && o.min_y <= self.max_y
    }

    fn contains_point(&self, x: f64, y: f64) -> bool {
        self.min_x <= x && x <= self.max_x && self.min_y <= y && y <= self.max_y
    }

    /// Planar distance from `(x, y)` to the nearest point of the box.
    fn min_dist(&self, x: f64, y: f64) -> f64 {
        let dx = if x < self.min_x { self.min_x - x } else if x > self.max_x { x - self.max_x } else { 0.0 };
        let dy = if y < self.min_y { self.min_y - y } else if y > self.max_y { y - self.max_y } else { 0.0 };
        (dx * dx + dy * dy).sqrt()
    }

    /// A lower bound, in metres, on the great-circle distance from
    /// `(lon, lat)` to any point of the box, which holds `(longitude,
    /// latitude)`.
    ///
    /// Haversine is `hav(d) = hav(dlat) + cos(lat1) cos(lat2) hav(dlon)`, and
    /// each term is bounded below on its own: `dlat` and `dlon` by the gap
    /// between the point and the box on that axis (longitude wrapping at the
    /// antimeridian), and `cos(lat2)` by its least value over the box's
    /// latitudes, which is at one of the two edges. Scaling degrees of
    /// longitude by the cosine of one latitude would not be a bound: the
    /// parallel is longer than the great circle between two points on it.
    fn min_haversine(&self, lon: f64, lat: f64) -> f64 {
        let dlat = if lat < self.min_y { self.min_y - lat } else if lat > self.max_y { lat - self.max_y } else { 0.0 };
        let dlon = if self.min_x <= lon && lon <= self.max_x {
            0.0
        } else {
            let wrap = |d: f64| {
                let d = d.abs() % 360.0;
                d.min(360.0 - d)
            };
            wrap(lon - self.min_x).min(wrap(lon - self.max_x))
        };
        let cos_box = self.min_y.to_radians().cos().min(self.max_y.to_radians().cos()).max(0.0);
        let h = (dlat.to_radians() / 2.0).sin().powi(2)
            + lat.to_radians().cos().max(0.0) * cos_box * (dlon.to_radians() / 2.0).sin().powi(2);
        // Rounding must not lift a bound above the exact distance of a point
        // inside the box, or the descent could return a farther one first.
        2.0 * EARTH_RADIUS_M * h.min(1.0).sqrt().asin() * (1.0 - 1e-12)
    }
}

#[derive(Debug, Clone)]
enum Node {
    Leaf(Vec<(Rect, NodeId)>),
    Inner(Vec<(Rect, Box<Node>)>),
}

impl Node {
    fn bounds(&self) -> Option<Rect> {
        match self {
            Node::Leaf(e) => e.iter().map(|(r, _)| *r).reduce(|a, b| a.union(&b)),
            Node::Inner(e) => e.iter().map(|(r, _)| *r).reduce(|a, b| a.union(&b)),
        }
    }
}

/// The coordinates of a point value: `(x, y, geographic)`, longitude first for
/// a geographic point. `None` for anything that is not a point.
pub fn point_coords(value: &PropertyValue) -> Option<(f64, f64, bool)> {
    let PropertyValue::Map(m) = value else { return None };
    let num = |k: &str| -> Option<f64> {
        m.get(k).and_then(|v| match v {
            PropertyValue::Float(f) => Some(*f),
            PropertyValue::Integer(i) => Some(*i as f64),
            _ => None,
        })
    };
    let geographic = match m.get("crs") {
        Some(PropertyValue::String(c)) => c.starts_with("wgs-84"),
        _ => match m.get("srid") {
            Some(PropertyValue::Integer(4326)) | Some(PropertyValue::Integer(4979)) => true,
            Some(PropertyValue::Integer(_)) => false,
            _ => num("latitude").is_some() && num("longitude").is_some(),
        },
    };
    if geographic {
        Some((num("longitude").or_else(|| num("x"))?, num("latitude").or_else(|| num("y"))?, true))
    } else {
        Some((num("x")?, num("y")?, false))
    }
}

/// An R-tree over one `(label, property)`.
#[derive(Debug, Clone)]
pub struct PointIndex {
    pub label: String,
    pub property: String,
    root: Node,
    /// Where each indexed node sits, so a removal or an update finds its entry.
    at: HashMap<NodeId, (f64, f64)>,
}

impl PointIndex {
    pub fn new(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self { label: label.into(), property: property.into(), root: Node::Leaf(Vec::new()), at: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.at.len()
    }

    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    /// Index `id` at `value`, replacing where it was. A value that is not a
    /// point removes it.
    pub fn set(&mut self, id: NodeId, value: &PropertyValue) {
        self.remove(id);
        if let Some((x, y, _)) = point_coords(value) {
            if x.is_finite() && y.is_finite() {
                self.insert(id, x, y);
            }
        }
    }

    pub fn remove(&mut self, id: NodeId) {
        if let Some((x, y)) = self.at.remove(&id) {
            fn rm(node: &mut Node, id: NodeId, x: f64, y: f64) -> bool {
                match node {
                    Node::Leaf(e) => {
                        let before = e.len();
                        e.retain(|(_, n)| *n != id);
                        e.len() != before
                    }
                    Node::Inner(children) => {
                        for (r, child) in children.iter_mut() {
                            if r.contains_point(x, y) && rm(child, id, x, y) {
                                if let Some(b) = child.bounds() {
                                    *r = b;
                                }
                                return true;
                            }
                        }
                        false
                    }
                }
            }
            rm(&mut self.root, id, x, y);
            if let Node::Inner(children) = &mut self.root {
                children.retain(|(_, c)| c.bounds().is_some());
                if children.is_empty() {
                    self.root = Node::Leaf(Vec::new());
                }
            }
        }
    }

    fn insert(&mut self, id: NodeId, x: f64, y: f64) {
        self.at.insert(id, (x, y));
        let entry = Rect::point(x, y);
        // Insert, splitting on the way back up; a split of the root grows the tree.
        fn ins(node: &mut Node, entry: Rect, id: NodeId) -> Option<Node> {
            match node {
                Node::Leaf(e) => {
                    e.push((entry, id));
                    (e.len() > MAX_ENTRIES).then(|| {
                        let mut all = std::mem::take(e);
                        let right = split(&mut all, |r| *r);
                        *e = all;
                        Node::Leaf(right)
                    })
                }
                Node::Inner(children) => {
                    let best = children
                        .iter()
                        .enumerate()
                        .min_by(|(_, (a, _)), (_, (b, _))| {
                            let ea = a.union(&entry).area() - a.area();
                            let eb = b.union(&entry).area() - b.area();
                            ea.partial_cmp(&eb).unwrap_or(std::cmp::Ordering::Equal)
                                .then(a.area().partial_cmp(&b.area()).unwrap_or(std::cmp::Ordering::Equal))
                        })
                        .map(|(i, _)| i)
                        .expect("an inner node has children");
                    let (r, child) = &mut children[best];
                    *r = r.union(&entry);
                    if let Some(sibling) = ins(child, entry, id) {
                        *r = child.bounds().expect("a split keeps entries on both sides");
                        let sr = sibling.bounds().expect("a split keeps entries on both sides");
                        children.push((sr, Box::new(sibling)));
                    }
                    (children.len() > MAX_ENTRIES).then(|| {
                        let mut all = std::mem::take(children);
                        let right = split(&mut all, |r| *r);
                        *children = all;
                        Node::Inner(right)
                    })
                }
            }
        }
        /// Sort along the wider axis of the entries' extent and cut in half.
        fn split<T>(entries: &mut Vec<(Rect, T)>, rect: impl Fn(&Rect) -> Rect) -> Vec<(Rect, T)> {
            let b = entries.iter().map(|(r, _)| rect(r)).reduce(|a, c| a.union(&c)).expect("non-empty");
            let by_x = (b.max_x - b.min_x) >= (b.max_y - b.min_y);
            entries.sort_by(|(a, _), (c, _)| {
                let (ka, kc) = if by_x { (a.min_x + a.max_x, c.min_x + c.max_x) } else { (a.min_y + a.max_y, c.min_y + c.max_y) };
                ka.partial_cmp(&kc).unwrap_or(std::cmp::Ordering::Equal)
            });
            entries.split_off(entries.len() / 2)
        }
        if let Some(sibling) = ins(&mut self.root, entry, id) {
            let old = std::mem::replace(&mut self.root, Node::Leaf(Vec::new()));
            let (a, b) = (old.bounds().expect("non-empty"), sibling.bounds().expect("non-empty"));
            self.root = Node::Inner(vec![(a, Box::new(old)), (b, Box::new(sibling))]);
        }
    }

    /// Every indexed node whose point lies in `area`.
    pub fn within(&self, area: &Rect) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut stack = vec![&self.root];
        while let Some(node) = stack.pop() {
            match node {
                Node::Leaf(e) => out.extend(e.iter().filter(|(r, _)| r.intersects(area)).map(|(_, n)| *n)),
                Node::Inner(c) => stack.extend(c.iter().filter(|(r, _)| r.intersects(area)).map(|(_, n)| n.as_ref())),
            }
        }
        out
    }

    /// The `k` nodes nearest `(x, y)` by planar distance in the index's own
    /// coordinates, nearest first, with that distance. For a cartesian index.
    pub fn nearest(&self, x: f64, y: f64, k: usize) -> Vec<(NodeId, f64)> {
        self.nearest_by(k, |r| r.min_dist(x, y), |px, py| ((px - x).powi(2) + (py - y).powi(2)).sqrt())
    }

    /// The `k` nodes nearest `(lon, lat)` by great-circle distance, nearest
    /// first, in metres -- the distance `point.distance` reports. For a
    /// geographic index. Exact, not a planar order re-ranked: the descent is
    /// ordered by a lower bound on metres (`Rect::min_haversine`), so a point
    /// east or west of the centre at high latitude is reached in its turn.
    pub fn nearest_geographic(&self, lon: f64, lat: f64, k: usize) -> Vec<(NodeId, f64)> {
        self.nearest_by(k, |r| r.min_haversine(lon, lat), |px, py| haversine_m((lon, lat), (px, py)))
    }

    /// Best-first descent: a subtree is keyed by `bound`, a lower bound on the
    /// distance to anything in it, and an entry by its exact `distance`. An
    /// entry popped from the heap is therefore no farther than anything still
    /// in it, so the first `k` popped are the `k` nearest.
    fn nearest_by(&self, k: usize, bound: impl Fn(&Rect) -> f64, distance: impl Fn(f64, f64) -> f64) -> Vec<(NodeId, f64)> {
        struct Item<'a>(f64, Option<NodeId>, Option<&'a Node>);
        impl PartialEq for Item<'_> {
            fn eq(&self, o: &Self) -> bool {
                self.0 == o.0
            }
        }
        impl Eq for Item<'_> {}
        impl PartialOrd for Item<'_> {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for Item<'_> {
            // Min-heap on distance.
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                o.0.partial_cmp(&self.0).unwrap_or(std::cmp::Ordering::Equal)
            }
        }
        let mut heap = BinaryHeap::new();
        heap.push(Item(0.0, None, Some(&self.root)));
        let mut out = Vec::with_capacity(k);
        while let Some(Item(d, id, node)) = heap.pop() {
            if out.len() >= k {
                break;
            }
            match (id, node) {
                (Some(id), _) => out.push((id, d)),
                (None, Some(Node::Leaf(e))) => {
                    for (r, n) in e {
                        heap.push(Item(distance(r.min_x, r.min_y), Some(*n), None));
                    }
                }
                (None, Some(Node::Inner(c))) => {
                    for (r, n) in c {
                        heap.push(Item(bound(r), None, Some(n.as_ref())));
                    }
                }
                (None, None) => {}
            }
        }
        out
    }

    /// Resident bytes, estimated from the structure (NDS-14's convention).
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        fn walk(n: &Node) -> usize {
            match n {
                Node::Leaf(e) => e.capacity() * size_of::<(Rect, NodeId)>(),
                Node::Inner(c) => {
                    c.capacity() * size_of::<(Rect, Box<Node>)>()
                        + c.iter().map(|(_, n)| size_of::<Node>() + walk(n)).sum::<usize>()
                }
            }
        }
        walk(&self.root) + self.at.capacity() * (size_of::<NodeId>() + size_of::<(f64, f64)>() + 1)
    }
}

/// A box that contains every point within `metres` of `(lon, lat)`, measured
/// as `point.distance` measures it.
///
/// Conservative: degrees are converted on the same sphere `haversine_m` uses
/// and then widened by a millionth, so rounding at the boundary cannot clip a
/// point the predicate admits; degrees of longitude shrink towards the poles,
/// so the box is sized at the latitude farther from the equator; and a box
/// reaching a pole or crossing the antimeridian spans every longitude, since
/// the tree holds longitudes in [-180, 180] and a box past either end would
/// miss the rows just across the line. The distance predicate filters the
/// candidates, so widening costs only candidates it then drops.
pub fn geographic_box(lon: f64, lat: f64, metres: f64) -> Rect {
    let metres = metres * (1.0 + 1e-6);
    let dlat = metres / METRES_PER_DEGREE;
    let (lo_lat, hi_lat) = (lat - dlat, lat + dlat);
    if lo_lat <= -90.0 || hi_lat >= 90.0 {
        return Rect { min_x: -180.0, min_y: lo_lat.max(-90.0), max_x: 180.0, max_y: hi_lat.min(90.0) };
    }
    let worst = lo_lat.abs().max(hi_lat.abs()).to_radians().cos();
    let dlon = metres / (METRES_PER_DEGREE * worst);
    if dlon >= 180.0 || lon - dlon < -180.0 || lon + dlon > 180.0 {
        return Rect { min_x: -180.0, min_y: lo_lat, max_x: 180.0, max_y: hi_lat };
    }
    Rect { min_x: lon - dlon, min_y: lo_lat, max_x: lon + dlon, max_y: hi_lat }
}

/// Every point index in a store, by name.
#[derive(Debug, Default)]
pub struct PointIndexes {
    by_name: std::sync::RwLock<HashMap<String, PointIndex>>,
}

impl PointIndexes {
    pub fn create(&self, name: impl Into<String>, label: &str, property: &str) {
        self.by_name.write().unwrap().insert(name.into(), PointIndex::new(label, property));
    }

    pub fn drop_index(&self, name: &str) -> bool {
        self.by_name.write().unwrap().remove(name).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.read().unwrap().is_empty()
    }

    /// `(name, label, property)` for every index, sorted by name.
    pub fn listing(&self) -> Vec<(String, String, String)> {
        let g = self.by_name.read().unwrap();
        let mut v: Vec<_> = g.iter().map(|(n, i)| (n.clone(), i.label.clone(), i.property.clone())).collect();
        v.sort();
        v
    }

    /// The name of the index on `(label, property)`, if one exists.
    pub fn name_for(&self, label: &str, property: &str) -> Option<String> {
        let g = self.by_name.read().unwrap();
        g.iter().find(|(_, i)| i.label == label && i.property == property).map(|(n, _)| n.clone())
    }

    /// A property of a node with `label` changed to `value` (`None`: removed).
    pub fn on_property_set(&self, label: &str, key: &str, id: NodeId, value: Option<&PropertyValue>) {
        let mut g = self.by_name.write().unwrap();
        for idx in g.values_mut().filter(|i| i.label == label && i.property == key) {
            match value {
                Some(v) => idx.set(id, v),
                None => idx.remove(id),
            }
        }
    }

    /// A node lost `label`, or was deleted (`label` `None`: every index).
    pub fn on_node_removed(&self, label: Option<&str>, id: NodeId) {
        let mut g = self.by_name.write().unwrap();
        for idx in g.values_mut().filter(|i| label.is_none_or(|l| i.label == l)) {
            idx.remove(id);
        }
    }

    /// Replace an index's contents with `points` (a build or a rebuild).
    pub fn rebuild(&self, name: &str, points: impl IntoIterator<Item = (NodeId, PropertyValue)>) -> usize {
        let mut g = self.by_name.write().unwrap();
        let Some(idx) = g.get_mut(name) else { return 0 };
        *idx = PointIndex::new(idx.label.clone(), idx.property.clone());
        for (id, v) in points {
            idx.set(id, &v);
        }
        idx.len()
    }

    /// Run `f` against the index on `(label, property)`.
    pub fn with<R>(&self, label: &str, property: &str, f: impl FnOnce(&PointIndex) -> R) -> Option<R> {
        let g = self.by_name.read().unwrap();
        g.values().find(|i| i.label == label && i.property == property).map(f)
    }

    /// Run `f` against the index named `name`.
    pub fn with_name<R>(&self, name: &str, f: impl FnOnce(&PointIndex) -> R) -> Option<R> {
        self.by_name.read().unwrap().get(name).map(f)
    }

    pub fn index_memory(&self) -> Vec<(String, String, String, usize, usize)> {
        let g = self.by_name.read().unwrap();
        g.iter().map(|(n, i)| (n.clone(), i.label.clone(), i.property.clone(), i.heap_bytes(), i.len())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(x: f64, y: f64) -> PropertyValue {
        let mut m = HashMap::new();
        m.insert("x".to_string(), PropertyValue::Float(x));
        m.insert("y".to_string(), PropertyValue::Float(y));
        PropertyValue::Map(m)
    }

    fn grid() -> (PointIndex, Vec<(NodeId, f64, f64)>) {
        let mut idx = PointIndex::new("P", "loc");
        let mut all = Vec::new();
        for i in 0..40u64 {
            for j in 0..40u64 {
                let id = NodeId::new(i * 40 + j);
                let (x, y) = (i as f64 * 1.5, j as f64 * 0.75);
                idx.set(id, &pt(x, y));
                all.push((id, x, y));
            }
        }
        (idx, all)
    }

    #[test]
    fn within_matches_a_scan() {
        let (idx, all) = grid();
        for area in [
            Rect { min_x: 3.0, min_y: 2.0, max_x: 12.0, max_y: 9.5 },
            Rect { min_x: -5.0, min_y: -5.0, max_x: 0.1, max_y: 0.1 },
            Rect { min_x: 100.0, min_y: 100.0, max_x: 200.0, max_y: 200.0 },
        ] {
            let mut got = idx.within(&area);
            got.sort();
            let mut want: Vec<NodeId> =
                all.iter().filter(|(_, x, y)| area.contains_point(*x, *y)).map(|(n, _, _)| *n).collect();
            want.sort();
            assert_eq!(got, want, "{area:?}");
        }
    }

    #[test]
    fn nearest_matches_a_sort() {
        let (idx, all) = grid();
        let (qx, qy) = (17.3, 11.1);
        let got: Vec<f64> = idx.nearest(qx, qy, 25).into_iter().map(|(_, d)| d).collect();
        let mut want: Vec<f64> = all.iter().map(|(_, x, y)| ((x - qx).powi(2) + (y - qy).powi(2)).sqrt()).collect();
        want.sort_by(|a, b| a.partial_cmp(b).unwrap());
        want.truncate(25);
        for (g, w) in got.iter().zip(&want) {
            assert!((g - w).abs() < 1e-9, "{got:?} vs {want:?}");
        }
    }

    #[test]
    fn removal_and_update_are_seen() {
        let (mut idx, _) = grid();
        let area = Rect { min_x: 0.0, min_y: 0.0, max_x: 0.0, max_y: 0.0 };
        assert_eq!(idx.within(&area), vec![NodeId::new(0)]);
        idx.remove(NodeId::new(0));
        assert!(idx.within(&area).is_empty());
        idx.set(NodeId::new(5), &pt(0.0, 0.0));
        assert_eq!(idx.within(&area), vec![NodeId::new(5)]);
        assert_eq!(idx.len(), 1599);
        idx.set(NodeId::new(5), &PropertyValue::String("not a point".into()));
        assert!(idx.within(&area).is_empty());
    }

    /// A tiny deterministic generator, so the property tests below need no
    /// seed plumbing and fail the same way every time.
    fn lcg(state: &mut u64) -> f64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*state >> 11) as f64 / (1u64 << 53) as f64
    }

    /// The point `metres` from `(lon, lat)` on `bearing` (radians), on the
    /// sphere `haversine_m` measures on.
    fn destination(lon: f64, lat: f64, bearing: f64, metres: f64) -> (f64, f64) {
        let d = metres / EARTH_RADIUS_M;
        let (p1, l1) = (lat.to_radians(), lon.to_radians());
        let p2 = (p1.sin() * d.cos() + p1.cos() * d.sin() * bearing.cos()).asin();
        let l2 = l1 + (bearing.sin() * d.sin() * p1.cos()).atan2(d.cos() - p1.sin() * p2.sin());
        (l2.to_degrees(), p2.to_degrees())
    }

    #[test]
    fn a_geographic_box_holds_every_point_the_distance_admits() {
        // The review's counterexample: due north of (0, 0) at r = 100 km, a
        // point at 99.94 km fell outside a box sized at 111,320 m per degree.
        let r = 100_000.0;
        let probe_lat = r / METRES_PER_DEGREE * (1.0 - 1e-4);
        assert!(haversine_m((0.0, 0.0), (0.0, probe_lat)) < r);
        assert!(geographic_box(0.0, 0.0, r).contains_point(0.0, probe_lat));
        assert!(probe_lat > r / 111_320.0, "the probe sits in the band the old constant clipped");

        // Every bearing, every longitude including either side of the
        // antimeridian, latitudes up to 85, radii from metres to 500 km, at
        // 99.99% of the radius.
        let mut st = 7u64;
        for _ in 0..20_000 {
            let lon = lcg(&mut st) * 360.0 - 180.0;
            let lat = lcg(&mut st) * 170.0 - 85.0;
            let r = 10f64.powf(lcg(&mut st) * 4.7 + 1.0);
            let bearing = lcg(&mut st) * std::f64::consts::TAU;
            let (plon, plat) = destination(lon, lat, bearing, r * 0.9999);
            let plon = (plon + 540.0).rem_euclid(360.0) - 180.0;
            assert!(haversine_m((lon, lat), (plon, plat)) < r);
            let b = geographic_box(lon, lat, r);
            assert!(b.contains_point(plon, plat), "centre ({lon}, {lat}) r={r} bearing={bearing}: ({plon}, {plat}) outside {b:?}");
        }
    }

    #[test]
    fn geographic_nearest_is_exact_at_high_latitude() {
        // Rows clustered east-west of a centre at 80 degrees, where a degree
        // of longitude is 0.17 of a degree of latitude: the planar order puts
        // the north-south rows first and is wrong. Compared with a full sort
        // by the distance `point.distance` reports.
        let mut idx = PointIndex::new("C", "at");
        let mut all = Vec::new();
        let mut st = 11u64;
        for i in 0..3000u64 {
            let lon = 10.0 + (lcg(&mut st) - 0.5) * 20.0;
            let lat = 80.0 + (lcg(&mut st) - 0.5) * 4.0;
            let mut m = HashMap::new();
            m.insert("longitude".to_string(), PropertyValue::Float(lon));
            m.insert("latitude".to_string(), PropertyValue::Float(lat));
            idx.set(NodeId::new(i), &PropertyValue::Map(m));
            all.push((NodeId::new(i), lon, lat));
        }
        for (qlon, qlat, k) in [(10.0, 80.0, 1), (10.0, 80.0, 40), (0.5, 81.5, 25), (-170.0, 79.0, 10), (10.0, 80.0, 3000)] {
            let got = idx.nearest_geographic(qlon, qlat, k);
            let mut want: Vec<f64> = all.iter().map(|(_, x, y)| haversine_m((qlon, qlat), (*x, *y))).collect();
            want.sort_by(|a, b| a.partial_cmp(b).unwrap());
            want.truncate(k);
            assert_eq!(got.len(), want.len());
            for ((_, g), w) in got.iter().zip(&want) {
                assert!((g - w).abs() < 1e-6, "({qlon}, {qlat}) k={k}: {g} vs {w}");
            }
        }
    }

    #[test]
    fn a_geographic_box_contains_its_radius() {
        let b = geographic_box(77.6, 12.9, 5_000.0);
        assert!(b.contains_point(77.6, 12.9 + 0.044) && b.contains_point(77.6 + 0.045, 12.9));
        assert!(!b.contains_point(77.6, 12.9 + 0.06));
        let polar = geographic_box(0.0, 89.99, 5_000.0);
        assert_eq!((polar.min_x, polar.max_x), (-180.0, 180.0));
        let dateline = geographic_box(179.99, 10.0, 5_000.0);
        assert_eq!((dateline.min_x, dateline.max_x), (-180.0, 180.0));
    }
}
