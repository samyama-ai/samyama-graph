//! Structure-aware path algorithms: graphs whose nodes have positions (#2037).
//!
//! A `STRUCTURE INDEX` (or an auto-detected convention) says that a label's
//! nodes sit in a space -- 3D coordinates, or latitude and longitude. Two
//! questions use that space as well as the relationships:
//!
//! * [`astar`]: the shortest path between two nodes, guided by the straight
//!   line to the target. The same answer as an unguided search, from fewer
//!   expanded nodes; both counts are reported, so the saving is measured, not
//!   claimed.
//! * [`stress`]: pairs that are close in space but far apart in the graph --
//!   the stretch of the shortest path through the graph against the straight
//!   line. A protein folding contact, a missing fiber link, a detour.
//!
//! **Both walk only nodes with a position under the structure.** The guide is
//! a lower bound only where every step has a length; a node with no position
//! could be a shortcut the bound cannot see, and the path would no longer be
//! guaranteed shortest. So a relationship is walkable when both its ends are
//! positioned, and answers are about that subgraph.
//!
//! Distances are the structure's own: Euclidean in its units for 3D, the
//! great-circle distance in metres (with elevation) for geographic positions.
//! Hierarchy and space-time structures have no such distance and are refused.

use std::collections::{HashMap, HashSet};

use samyama_graph_algorithms::{guided_shortest_path, GraphView};

use crate::graph::{GraphStore, NodeId};
use crate::index::point::EARTH_RADIUS_M;
use crate::index::structure::{ManifoldSpec, SgiIndex, StructuralConvention};

/// What [`astar`] minimises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathCost {
    /// Fewest relationships. Guided by the straight-line distance divided by
    /// the longest relationship: no relationship covers more, so no path can
    /// take fewer hops than that.
    Hops,
    /// Shortest total length, each relationship as long as the straight line
    /// between its ends. Guided by the straight-line distance to the target.
    Length,
}

impl PathCost {
    pub const NAMES: [&'static str; 2] = ["hops", "length"];

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "hops" => Some(Self::Hops),
            "length" => Some(Self::Length),
            _ => None,
        }
    }
}

/// The most close pairs [`stress`] will examine. Each costs a path search; a
/// radius that finds more than this is almost always a radius in the wrong
/// unit.
pub const MAX_STRESS_PAIRS: usize = 100_000;

/// A structure with a distance, and the positions of its nodes.
pub struct Structure {
    pub convention: StructuralConvention,
    sgi: SgiIndex,
}

impl Structure {
    /// The structure named `name`, or the only one with a distance when no
    /// name is given.
    pub fn resolve(store: &GraphStore, name: Option<&str>) -> Result<Self, String> {
        let all = store.structure_registry.effective_conventions(store);
        let measurable = |c: &&StructuralConvention| {
            matches!(c.manifold, ManifoldSpec::Euclidean3d { .. } | ManifoldSpec::GeoSpherical { .. })
        };
        let convention = match name {
            Some(n) => {
                let c = all.iter().find(|c| c.name == n).ok_or_else(|| {
                    let names: Vec<&str> = all.iter().map(|c| c.name.as_str()).collect();
                    format!("no structure named `{n}`; this graph has: {}", list_or_none(&names))
                })?;
                if !measurable(&c) {
                    return Err(format!(
                        "structure `{n}` is a {} structure, which has no distance; these \
                         algorithms need 3D or geographic positions",
                        kind(c)
                    ));
                }
                c.clone()
            }
            None => {
                let usable: Vec<&StructuralConvention> = all.iter().filter(measurable).collect();
                match usable.as_slice() {
                    [one] => (*one).clone(),
                    [] => {
                        return Err("this graph has no 3D or geographic structure; declare one with \
                                    CREATE STRUCTURE INDEX name ON :Label(x, y, z)"
                            .to_string())
                    }
                    many => {
                        let names: Vec<&str> = many.iter().map(|c| c.name.as_str()).collect();
                        return Err(format!(
                            "this graph has several structures; name one with `structure`: {}",
                            names.join(", ")
                        ));
                    }
                }
            }
        };
        let sgi = SgiIndex::build(store, convention.clone());
        Ok(Self { convention, sgi })
    }

    /// Straight-line distance between two positioned nodes.
    pub fn distance(&self, a: NodeId, b: NodeId) -> Option<f64> {
        self.sgi.manifold_distance(a, b)
    }

    /// Every positioned node and a point in a space where straight-line
    /// distance never exceeds the structure's distance -- the coordinates
    /// themselves in 3D, Earth-centred metres for geographic positions (a
    /// chord is never longer than its arc). Used to find close pairs.
    fn spatial_points(&self) -> Vec<(NodeId, [f64; 3])> {
        let geo = matches!(self.convention.manifold, ManifoldSpec::GeoSpherical { .. });
        self.sgi
            .points
            .iter()
            .map(|p| {
                if !geo {
                    return (p.node_id, p.coords);
                }
                let [lon, lat, alt] = p.coords;
                let (lat, lon) = (lat.to_radians(), lon.to_radians());
                let r = EARTH_RADIUS_M + alt;
                (p.node_id, [r * lat.cos() * lon.cos(), r * lat.cos() * lon.sin(), r * lat.sin()])
            })
            .collect()
    }

    /// The radius the structure was declared with, if any.
    pub fn contact_radius(&self) -> Option<f64> {
        match &self.convention.manifold {
            ManifoldSpec::Euclidean3d { contact_radius, .. } => *contact_radius,
            ManifoldSpec::GeoSpherical { contact_radius_m, .. } => *contact_radius_m,
            _ => None,
        }
    }
}

fn list_or_none(names: &[&str]) -> String {
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

fn kind(c: &StructuralConvention) -> &'static str {
    match c.manifold {
        ManifoldSpec::Euclidean3d { .. } => "3D",
        ManifoldSpec::GeoSpherical { .. } => "geographic",
        ManifoldSpec::HierarchyPoset { .. } => "hierarchy",
        ManifoldSpec::CausalSpacetime { .. } => "space-time",
    }
}

/// Node ids of a view, by dense index.
fn id_at(view: &GraphView, i: usize) -> NodeId {
    NodeId::new(view.index_to_node[i])
}

/// The answer of [`astar`].
#[derive(Debug, Clone, PartialEq)]
pub struct GuidedRoute {
    pub path: Vec<NodeId>,
    pub hops: usize,
    /// Total straight-line length of the path's relationships.
    pub length: f64,
    /// Straight-line distance from source to target.
    pub direct: f64,
    /// `length / direct`; `None` when source and target coincide.
    pub stretch: Option<f64>,
    /// Nodes the guided search expanded.
    pub expanded: usize,
    /// Nodes the same search expanded with no guide.
    pub expanded_unguided: usize,
}

/// The shortest path from `source` to `target` by `cost`, over the
/// positioned nodes of `view`, or `Ok(None)` when none joins them.
pub fn astar(
    structure: &Structure,
    view: &GraphView,
    source: NodeId,
    target: NodeId,
    cost: PathCost,
    undirected: bool,
) -> Result<Option<GuidedRoute>, String> {
    let index = |id: NodeId, which: &str| -> Result<usize, String> {
        let i = view
            .node_to_index
            .get(&id.as_u64())
            .copied()
            .ok_or_else(|| format!("the {which} node {} is not in the graph searched", id.as_u64()))?;
        structure.distance(id, id).ok_or_else(|| {
            format!(
                "the {which} node {} has no position under structure `{}`",
                id.as_u64(),
                structure.convention.name
            )
        })?;
        Ok(i)
    };
    let (s, t) = (index(source, "source")?, index(target, "target")?);
    let length = |u: usize, w: usize| structure.distance(id_at(view, u), id_at(view, w));
    let to_target = |w: usize| structure.distance(id_at(view, w), target).unwrap_or(0.0);
    let route = match cost {
        PathCost::Length => {
            let guided = guided_shortest_path(view, s, t, undirected, length, to_target);
            let blind = guided_shortest_path(view, s, t, undirected, length, |_| 0.0);
            guided.zip(blind)
        }
        PathCost::Hops => {
            let hop = |u: usize, w: usize| length(u, w).map(|_| 1.0);
            let longest = (0..view.node_count)
                .flat_map(|u| view.successors(u).iter().map(move |&w| (u, w)))
                .filter_map(|(u, w)| length(u, w))
                .fold(0.0, f64::max);
            let bound = |w: usize| if longest > 0.0 { to_target(w) / longest } else { 0.0 };
            let guided = guided_shortest_path(view, s, t, undirected, hop, bound);
            let blind = guided_shortest_path(view, s, t, undirected, hop, |_| 0.0);
            guided.zip(blind)
        }
    };
    let Some((found, blind)) = route else { return Ok(None) };
    let direct = structure.distance(source, target).unwrap_or(0.0);
    let length: f64 = found.path.windows(2).filter_map(|p| length(p[0], p[1])).sum();
    Ok(Some(GuidedRoute {
        hops: found.path.len() - 1,
        path: found.path.iter().map(|&i| id_at(view, i)).collect(),
        length,
        direct,
        stretch: (direct > 0.0).then(|| length / direct),
        expanded: found.expanded,
        expanded_unguided: blind.expanded,
    }))
}

/// Two nodes close in space, and how far apart the graph keeps them.
#[derive(Debug, Clone, PartialEq)]
pub struct StressPair {
    /// The smaller node id first.
    pub a: NodeId,
    pub b: NodeId,
    /// Straight-line distance.
    pub distance: f64,
    /// Length of the shortest path through the graph; `None` when none joins
    /// them.
    pub path_length: Option<f64>,
    /// Relationships on that path.
    pub hops: Option<usize>,
    /// `path_length / distance`, at least 1; `None` when unreachable.
    pub stretch: Option<f64>,
}

/// Every pair of positioned nodes within `radius` of each other that no
/// relationship joins directly, with the stretch of the shortest path
/// between them; unreachable pairs first, then the highest stretch.
///
/// Read undirected: closeness in space is symmetric, and a path either way
/// counts. Coincident nodes are skipped -- the straight line has no length
/// to divide by.
pub fn stress(structure: &Structure, view: &GraphView, radius: f64) -> Result<Vec<StressPair>, String> {
    if !(radius > 0.0 && radius.is_finite()) {
        return Err(format!("`radius` must be a positive distance, got {radius}"));
    }
    let points: Vec<(NodeId, [f64; 3])> = structure
        .spatial_points()
        .into_iter()
        .filter(|(id, _)| view.node_to_index.contains_key(&id.as_u64()))
        .collect();
    let cell = |p: [f64; 3]| -> [i64; 3] { p.map(|c| (c / radius).floor() as i64) };
    let mut grid: HashMap<[i64; 3], Vec<usize>> = HashMap::new();
    for (i, &(_, p)) in points.iter().enumerate() {
        grid.entry(cell(p)).or_default().push(i);
    }
    let joined: HashSet<(usize, usize)> = (0..view.node_count)
        .flat_map(|u| view.successors(u).iter().map(move |&w| (u.min(w), u.max(w))))
        .collect();

    let mut close = Vec::new();
    for (i, &(a, pa)) in points.iter().enumerate() {
        let [cx, cy, cz] = cell(pa);
        for dx in -1..=1 {
            for dy in -1..=1 {
                for dz in -1..=1 {
                    let Some(bucket) = grid.get(&[cx + dx, cy + dy, cz + dz]) else { continue };
                    for &j in bucket.iter().filter(|&&j| j > i) {
                        let b = points[j].0;
                        let Some(d) = structure.distance(a, b) else { continue };
                        let (ia, ib) = (view.node_to_index[&a.as_u64()], view.node_to_index[&b.as_u64()]);
                        if d > 0.0 && d <= radius && !joined.contains(&(ia.min(ib), ia.max(ib))) {
                            close.push((a.min(b), a.max(b), d));
                            if close.len() > MAX_STRESS_PAIRS {
                                return Err(format!(
                                    "more than {MAX_STRESS_PAIRS} pairs lie within {radius} of each \
                                     other; use a smaller `radius` (in the structure's units: \
                                     metres for geographic positions)"
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    let length = |u: usize, w: usize| structure.distance(id_at(view, u), id_at(view, w));
    let mut out: Vec<StressPair> = close
        .into_iter()
        .map(|(a, b, distance)| {
            let (s, t) = (view.node_to_index[&a.as_u64()], view.node_to_index[&b.as_u64()]);
            let to_b = |w: usize| structure.distance(id_at(view, w), b).unwrap_or(0.0);
            let found = guided_shortest_path(view, s, t, true, length, to_b);
            StressPair {
                a,
                b,
                distance,
                path_length: found.as_ref().map(|f| f.cost),
                hops: found.as_ref().map(|f| f.path.len() - 1),
                stretch: found.map(|f| f.cost / distance),
            }
        })
        .collect();
    out.sort_by(|x, y| {
        let rank = |p: &StressPair| p.stretch.unwrap_or(f64::INFINITY);
        rank(y).total_cmp(&rank(x)).then(x.a.cmp(&y.a)).then(x.b.cmp(&y.b))
    });
    Ok(out)
}
