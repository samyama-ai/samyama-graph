//! Structural & Manifold Indexing (`SGI`) and Visual Analytic Cues (`#2035`, `#2036`, `#2037`).
//!
//! Real-world graphs are embedded in an ambient structural manifold:
//! - **3D Euclidean (`euclidean_3d`):** Protein / molecular structures (`(x, y, z)` in Ångströms
//!   with `residue_seq` backbone order and 8 Å contact radius), 3D CAD meshes, brain connectomes.
//! - **Geospatial Spherical (`geo_spherical`):** Telecom towers, power grids, logistics networks
//!   (`(lat, lon, elevation)` on WGS-84).
//! - **Subsumption Posets (`hierarchy_poset`):** Ontologies, taxonomies, and calendars indexed by
//!   `OEH` (`ADR-035`, [`crate::index::hierarchy`]).
//! - **Causal Space-Time (`causal_spacetime`):** Event DAGs and provenance graphs (`(t, x, y)`).
//!
//! This module provides:
//! 1. **Declarative & Auto-Detected Structural Conventions ([`StructuralConvention`]):**
//!    Persisted in the `.sgsnap` header (`structures`) and served over `/api/structure`.
//! 2. **64-bit 3D Morton Code (Z-Order) Structural-Geometric Index ([`SgiIndex`]):**
//!    Linearizes 3D coordinates into a Z-order curve and attaches the same [`Fenwick`] and
//!    [`SegmentTree`] monoid structures used by `OEH` (`ADR-035`) for $O(\log n)$ index-resident
//!    Morton-interval roll-ups and fast bounding-box / radius queries.
//! 3. **Implicit Geometric Contact Graph & Topological–Geometric Stress:**
//!    Extracts spatial proximity contacts (`contact_radius`) without materializing $O(nk)$ edges
//!    in CSR, and computes per-pair stretch $\sigma(u,v) = d_{\text{graph}}(u,v) / d_{\text{manifold}}(u,v)$
//!    to spotlight allosteric protein contacts or routing detours.
//! 4. **Manifold-Guided $A^*$ Shortest Path ([`SgiIndex::astar_shortest_path`]):**
//!    Uses the ambient metric lower bound to guide shortest-path search, reporting both the
//!    geodesic path and the explored-node reduction relative to blind BFS.
//! 5. **Server-Computed 3D & Non-WebGL 2.5D Isometric Visual Cues ([`StructuralVisualCues`]):**
//!    Supplies `graph.samyama.cloud` / `samyama-insight` with zero-simulation 3D coordinates,
//!    ordered backbone polylines, and painter's-algorithm depth-sorted 2.5D isometric projections.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::graph::types::{EdgeType, NodeId};
use crate::graph::{GraphStore, PropertyValue};
use crate::index::hierarchy::monoid::{Fenwick, RollupOp, RollupValue, SegmentTree};
use crate::index::point::haversine_m;

/// Declarative structural convention for a node label or hierarchy in a graph (`samyama.structure/1`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuralConvention {
    /// Unique name for this structural convention (e.g. `"pdb_residue_3d"`, `"tower_geo"`).
    pub name: String,
    /// Optional target node label (e.g. `Some("Residue")`); `None` applies across all labels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Ambient manifold specification.
    pub manifold: ManifoldSpec,
    /// Optional visual preset hint for `graph.samyama.cloud` (`"protein_ribbon_3d"`, `"globe_3d"`,
    /// `"cone_tree_3d"`, `"isometric_25d"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visual_preset: Option<String>,
    /// Optional numeric property to aggregate in index-resident spatial roll-ups (e.g. `"b_factor"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measure_prop: Option<String>,
}

/// Ambient coordinate manifold and structural parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManifoldSpec {
    /// 3D Euclidean coordinates `(x, y, z)` (e.g., protein structures in Ångströms, 3D meshes).
    Euclidean3d {
        x_prop: String,
        y_prop: String,
        z_prop: String,
        #[serde(default = "default_unit_angstrom")]
        unit: String,
        /// Optional sequential ordering property along a chain/polymer (e.g. `"residue_seq"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_prop: Option<String>,
        /// Optional grouping property for separate chains/fibers (e.g. `"chain_id"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        group_by_prop: Option<String>,
        /// Optional implicit contact radius in `unit` (e.g. `8.0` Å for $\text{C}_\alpha$ contacts).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        contact_radius: Option<f64>,
    },
    /// Geographic spherical coordinates `(lat, lon, optional elevation)` on WGS-84.
    GeoSpherical {
        lat_prop: String,
        lon_prop: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alt_prop: Option<String>,
        #[serde(default = "default_crs_wgs84")]
        crs: String,
        /// Optional proximity radius in metres for implicit line-of-sight / neighbour links.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        contact_radius_m: Option<f64>,
    },
    /// Subsumption poset backed by an `OEH` (`CREATE HIERARCHY INDEX`) index (`ADR-035`).
    HierarchyPoset {
        index_name: String,
        #[serde(default = "default_hier_layout")]
        layout: String,
    },
    /// Causal space-time manifold `(time_prop, x_prop, y_prop)`.
    CausalSpacetime {
        time_prop: String,
        x_prop: String,
        y_prop: String,
    },
}

fn default_unit_angstrom() -> String {
    "unit".to_string()
}

fn default_crs_wgs84() -> String {
    "EPSG:4326".to_string()
}

fn default_hier_layout() -> String {
    "cone_3d".to_string()
}

/// Thread-safe registry of explicit [`StructuralConvention`]s attached to a [`GraphStore`].
#[derive(Debug, Default)]
pub struct StructureRegistry {
    conventions: RwLock<Vec<StructuralConvention>>,
}

impl StructureRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self {
            conventions: RwLock::new(Vec::new()),
        }
    }

    /// Explicitly declared conventions, in declaration order.
    pub fn list(&self) -> Vec<StructuralConvention> {
        self.conventions.read().unwrap().clone()
    }

    /// Replace all explicit conventions.
    pub fn replace(&self, items: Vec<StructuralConvention>) {
        *self.conventions.write().unwrap() = items;
    }

    /// Add or update a convention by `name`.
    pub fn upsert(&self, item: StructuralConvention) {
        let mut guard = self.conventions.write().unwrap();
        if let Some(existing) = guard.iter_mut().find(|c| c.name == item.name) {
            *existing = item;
        } else {
            guard.push(item);
        }
    }

    /// Clear all explicit conventions.
    /// Drop one convention by name, reporting whether it was there.
    ///
    /// `DROP STRUCTURE INDEX` needs to tell "removed" from "no such index",
    /// because silently succeeding on a name nobody declared is how a typo
    /// reads as a successful drop.
    pub fn remove(&self, name: &str) -> bool {
        let mut items = self.conventions.write().unwrap();
        let before = items.len();
        items.retain(|c| c.name != name);
        items.len() != before
    }

    /// One convention by name.
    pub fn get(&self, name: &str) -> Option<StructuralConvention> {
        self.conventions
            .read()
            .unwrap()
            .iter()
            .find(|c| c.name == name)
            .cloned()
    }

    pub fn clear(&self) {
        self.conventions.write().unwrap().clear();
    }

    /// Return all explicit conventions plus any auto-detected conventions from node properties
    /// and built `OEH` hierarchy indexes (`#2035`).
    pub fn effective_conventions(&self, store: &GraphStore) -> Vec<StructuralConvention> {
        let mut out = self.list();
        for detected in detect_conventions(store) {
            let already_covered = out.iter().any(|c| {
                c.name == detected.name
                    || (c.label == detected.label
                        && std::mem::discriminant(&c.manifold)
                            == std::mem::discriminant(&detected.manifold))
            });
            if !already_covered {
                out.push(detected);
            }
        }
        out
    }
}

/// Read a numeric property (`f64`) for `node_id` from either the columnar store (`node_columns`)
/// or the in-memory property map.
pub fn node_f64(store: &GraphStore, node_id: NodeId, prop: &str) -> Option<f64> {
    let col_val = store.node_columns.get_property(node_id.as_u64() as usize, prop);
    match col_val {
        PropertyValue::Float(f) if f.is_finite() => return Some(f),
        PropertyValue::Integer(i) => return Some(i as f64),
        _ => {}
    }
    store
        .get_node(node_id)
        .and_then(|n| n.get_property(prop))
        .and_then(|v| match v {
            PropertyValue::Float(f) if f.is_finite() => Some(*f),
            PropertyValue::Integer(i) => Some(*i as f64),
            _ => None,
        })
}

/// Read a string or integer grouping property as a `String`.
pub fn node_group_string(store: &GraphStore, node_id: NodeId, prop: &str) -> Option<String> {
    let col_val = store.node_columns.get_property(node_id.as_u64() as usize, prop);
    match col_val {
        PropertyValue::String(s) if !s.is_empty() => return Some(s),
        PropertyValue::Integer(i) => return Some(i.to_string()),
        _ => {}
    }
    store
        .get_node(node_id)
        .and_then(|n| n.get_property(prop))
        .and_then(|v| match v {
            PropertyValue::String(s) if !s.is_empty() => Some(s.clone()),
            PropertyValue::Integer(i) => Some(i.to_string()),
            _ => None,
        })
}

/// Auto-detect structural conventions across labels and built `OEH` hierarchy indexes (`#2035`).
pub fn detect_conventions(store: &GraphStore) -> Vec<StructuralConvention> {
    let mut out = Vec::new();

    // 1. Every usable OEH hierarchy index is a first-class `hierarchy_poset` structure.
    for info in store.hierarchy_index.list() {
        if info.declined.is_none() && info.nodes > 0 {
            out.push(StructuralConvention {
                name: format!("hier_{}", info.name),
                label: None,
                manifold: ManifoldSpec::HierarchyPoset {
                    index_name: info.name.clone(),
                    layout: "cone_3d".to_string(),
                },
                visual_preset: Some("cone_tree_3d".to_string()),
                measure_prop: info.measure.clone(),
            });
        }
    }

    // 2. Probe up to 32 nodes per label for 3D Euclidean `(x,y,z)` / `(ca_x,ca_y,ca_z)`
    //    or Geospatial `(lat,lon)` / `(latitude,longitude)` properties.
    let mut labels: Vec<String> = store
        .all_labels()
        .into_iter()
        .map(|l| l.as_str().to_string())
        .collect();
    labels.sort();

    for label in labels {
        let sample: Vec<NodeId> = store
            .get_nodes_by_label(&crate::graph::types::Label::new(&label))
            .into_iter()
            .take(32)
            .map(|n| n.id)
            .collect();
        if sample.is_empty() {
            continue;
        }

        // Check 3D Euclidean candidate triplets
        for (xp, yp, zp, preset, unit, default_radius) in [
            ("ca_x", "ca_y", "ca_z", "protein_ribbon_3d", "angstrom", Some(8.0)),
            ("x", "y", "z", "euclidean_3d", "unit", None),
            ("pos_x", "pos_y", "pos_z", "euclidean_3d", "unit", None),
        ] {
            let hits = sample
                .iter()
                .filter(|&&id| {
                    node_f64(store, id, xp).is_some()
                        && node_f64(store, id, yp).is_some()
                        && node_f64(store, id, zp).is_some()
                })
                .count();
            if hits * 2 >= sample.len() {
                let seq_prop = ["residue_seq", "seq_id", "res_num", "seq", "order"]
                    .into_iter()
                    .find(|&p| sample.iter().any(|&id| node_f64(store, id, p).is_some()))
                    .map(str::to_string);
                let group_prop = ["chain_id", "chain", "model_id", "segment"]
                    .into_iter()
                    .find(|&p| sample.iter().any(|&id| node_group_string(store, id, p).is_some()))
                    .map(str::to_string);
                let measure_prop = ["b_factor", "plddt", "weight", "score", "units"]
                    .into_iter()
                    .find(|&p| sample.iter().any(|&id| node_f64(store, id, p).is_some()))
                    .map(str::to_string);
                let is_protein = xp == "ca_x" || seq_prop.as_deref() == Some("residue_seq");
                out.push(StructuralConvention {
                    name: format!("auto_3d_{}", label.to_lowercase()),
                    label: Some(label.clone()),
                    manifold: ManifoldSpec::Euclidean3d {
                        x_prop: xp.to_string(),
                        y_prop: yp.to_string(),
                        z_prop: zp.to_string(),
                        unit: if is_protein {
                            "angstrom".to_string()
                        } else {
                            unit.to_string()
                        },
                        sequence_prop: seq_prop,
                        group_by_prop: group_prop,
                        contact_radius: if is_protein { Some(8.0) } else { default_radius },
                    },
                    visual_preset: Some(
                        if is_protein {
                            "protein_ribbon_3d"
                        } else {
                            preset
                        }
                        .to_string(),
                    ),
                    measure_prop,
                });
                break;
            }
        }

        // Check GeoSpherical candidate pairs
        for (lat_p, lon_p) in [("lat", "lon"), ("latitude", "longitude"), ("lat", "lng")] {
            let hits = sample
                .iter()
                .filter(|&&id| {
                    match (node_f64(store, id, lat_p), node_f64(store, id, lon_p)) {
                        (Some(lat), Some(lon)) => {
                            (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon)
                        }
                        _ => false,
                    }
                })
                .count();
            if hits * 2 >= sample.len() {
                let alt_prop = ["elevation", "alt", "altitude", "height"]
                    .into_iter()
                    .find(|&p| sample.iter().any(|&id| node_f64(store, id, p).is_some()))
                    .map(str::to_string);
                out.push(StructuralConvention {
                    name: format!("auto_geo_{}", label.to_lowercase()),
                    label: Some(label.clone()),
                    manifold: ManifoldSpec::GeoSpherical {
                        lat_prop: lat_p.to_string(),
                        lon_prop: lon_p.to_string(),
                        alt_prop,
                        crs: "EPSG:4326".to_string(),
                        contact_radius_m: None,
                    },
                    visual_preset: Some("globe_3d".to_string()),
                    measure_prop: None,
                });
                break;
            }
        }
    }

    out
}

// ============================================================================
// 64-Bit 3D Morton Code (Z-Order Curve) & Structural-Geometric Index (SGI)
// ============================================================================

/// Spread the lowest 21 bits of `v` so there are two zero bits between each bit:
/// bit `i` moves to bit `3*i`.
#[inline]
fn part1by2_21(v: u32) -> u64 {
    let mut x = (v & 0x1f_ffff) as u64;
    x = (x | x << 32) & 0x001f_0000_0000_ffff;
    x = (x | x << 16) & 0x001f_0000_ff00_00ff;
    x = (x | x << 8) & 0x100f_00f0_0f00_f00f;
    x = (x | x << 4) & 0x10c3_0c30_c30c_30c3;
    x = (x | x << 2) & 0x1249_2492_4924_9249;
    x
}

/// Compact every third bit of `x` back into the lowest 21 bits.
#[inline]
fn compact1by2_21(mut x: u64) -> u32 {
    x &= 0x1249_2492_4924_9249;
    x = (x ^ (x >> 2)) & 0x10c3_0c30_c30c_30c3;
    x = (x ^ (x >> 4)) & 0x100f_00f0_0f00_f00f;
    x = (x ^ (x >> 8)) & 0x001f_0000_ff00_00ff;
    x = (x ^ (x >> 16)) & 0x001f_0000_0000_ffff;
    x = (x ^ (x >> 32)) & 0x0000_0000_001f_ffff;
    x as u32
}

/// Interleave three 21-bit quantized coordinates `(qx, qy, qz)` into a 63-bit 3D Morton
/// Z-order code.
#[inline]
pub fn morton_encode_3d(qx: u32, qy: u32, qz: u32) -> u64 {
    part1by2_21(qx) | (part1by2_21(qy) << 1) | (part1by2_21(qz) << 2)
}

/// Decode a 63-bit 3D Morton Z-order code back into `(qx, qy, qz)`.
#[inline]
pub fn morton_decode_3d(code: u64) -> (u32, u32, u32) {
    (
        compact1by2_21(code),
        compact1by2_21(code >> 1),
        compact1by2_21(code >> 2),
    )
}

/// 3D axis-aligned bounding box.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct BoundingBox3d {
    pub min: [f64; 3],
    pub max: [f64; 3],
}

impl BoundingBox3d {
    /// Center of the 3D bounding box.
    pub fn center(&self) -> [f64; 3] {
        [
            0.5 * (self.min[0] + self.max[0]),
            0.5 * (self.min[1] + self.max[1]),
            0.5 * (self.min[2] + self.max[2]),
        ]
    }

    /// Maximum extent along any axis (at least `1e-9` to prevent division by zero).
    pub fn max_span(&self) -> f64 {
        (self.max[0] - self.min[0])
            .max(self.max[1] - self.min[1])
            .max(self.max[2] - self.min[2])
            .max(1e-9)
    }

    /// Does this box contain `(x, y, z)`?
    pub fn contains(&self, x: f64, y: f64, z: f64) -> bool {
        self.min[0] <= x
            && x <= self.max[0]
            && self.min[1] <= y
            && y <= self.max[1]
            && self.min[2] <= z
            && z <= self.max[2]
    }
}

/// One indexed point in [`SgiIndex`], ordered by 63-bit 3D Morton code.
#[derive(Debug, Clone)]
pub struct SgiPoint {
    pub node_id: NodeId,
    pub morton: u64,
    /// Metric coordinates `(x, y, z)` (for `GeoSpherical`, `(lon, lat, alt_m)`).
    pub coords: [f64; 3],
    /// Cartesian 3D rendering coordinates in metric space.
    pub cartesian: [f64; 3],
    pub measure: Option<RollupValue>,
}

/// An [`SgiPoint`] before its Morton code is known: `(node_id, coords, cartesian, measure)`.
///
/// The code quantizes against the bounding box, and the box is not known until
/// every point has been read -- so [`SgiIndex::build`] collects these on a first
/// pass and attaches the code on a second.
type UnrankedPoint = (NodeId, [f64; 3], [f64; 3], Option<RollupValue>);

/// Structural-Geometric Index (`SGI`, `#2036`): a 3D Morton Z-order index with index-resident
/// [`Fenwick`] (`SUM`/`COUNT`) and [`SegmentTree`] (`MIN`/`MAX`) monoid roll-ups.
#[derive(Debug, Clone)]
pub struct SgiIndex {
    pub convention: StructuralConvention,
    pub bbox: BoundingBox3d,
    /// Points sorted in ascending `morton` order.
    pub points: Vec<SgiPoint>,
    /// Fast lookup from `NodeId` to rank in `points`.
    rank_of: HashMap<NodeId, usize>,
    /// Index-resident Fenwick tree over Morton ranks for `SUM`.
    fenwick_sum: Option<Fenwick>,
    /// Index-resident SegmentTree over Morton ranks for `MIN`.
    seg_min: Option<SegmentTree>,
    /// Index-resident SegmentTree over Morton ranks for `MAX`.
    seg_max: Option<SegmentTree>,
}

const MORTON_MAX_QUANT: f64 = ((1u32 << 21) - 1) as f64;

impl SgiIndex {
    /// Build an [`SgiIndex`] from `store` for the given [`StructuralConvention`].
    pub fn build(store: &GraphStore, convention: StructuralConvention) -> Self {
        let candidate_nodes: Vec<NodeId> = match &convention.label {
            Some(lbl) => store
                .get_nodes_by_label(&crate::graph::types::Label::new(lbl))
                .into_iter()
                .map(|n| n.id)
                .collect(),
            None => store.all_nodes().into_iter().map(|n| n.id).collect(),
        };

        let mut raw: Vec<UnrankedPoint> = Vec::new();
        let mut min = [f64::INFINITY; 3];
        let mut max = [f64::NEG_INFINITY; 3];

        for id in candidate_nodes {
            let extracted = match &convention.manifold {
                ManifoldSpec::Euclidean3d {
                    x_prop,
                    y_prop,
                    z_prop,
                    ..
                } => match (
                    node_f64(store, id, x_prop),
                    node_f64(store, id, y_prop),
                    node_f64(store, id, z_prop),
                ) {
                    (Some(x), Some(y), Some(z)) => Some(([x, y, z], [x, y, z])),
                    _ => None,
                },
                ManifoldSpec::GeoSpherical {
                    lat_prop,
                    lon_prop,
                    alt_prop,
                    ..
                } => match (node_f64(store, id, lat_prop), node_f64(store, id, lon_prop)) {
                    (Some(lat), Some(lon)) => {
                        let alt = alt_prop
                            .as_deref()
                            .and_then(|p| node_f64(store, id, p))
                            .unwrap_or(0.0);
                        // Project (lat, lon, alt) to 3D spherical / ECEF-like unit sphere scaled by meters
                        let lat_r = lat.to_radians();
                        let lon_r = lon.to_radians();
                        let r = 100.0 + alt * 0.001;
                        let cx = r * lat_r.cos() * lon_r.cos();
                        let cy = r * lat_r.cos() * lon_r.sin();
                        let cz = r * lat_r.sin();
                        Some(([lon, lat, alt], [cx, cy, cz]))
                    }
                    _ => None,
                },
                ManifoldSpec::CausalSpacetime {
                    time_prop,
                    x_prop,
                    y_prop,
                } => match (
                    node_f64(store, id, x_prop),
                    node_f64(store, id, y_prop),
                    node_f64(store, id, time_prop),
                ) {
                    (Some(x), Some(y), Some(t)) => Some(([x, y, t], [x, y, t])),
                    _ => None,
                },
                ManifoldSpec::HierarchyPoset { .. } => None,
            };

            if let Some((coords, cart)) = extracted {
                for axis in 0..3 {
                    min[axis] = min[axis].min(coords[axis]);
                    max[axis] = max[axis].max(coords[axis]);
                }
                let mval = convention
                    .measure_prop
                    .as_deref()
                    .and_then(|p| node_f64(store, id, p))
                    .map(RollupValue::Float);
                raw.push((id, coords, cart, mval));
            }
        }

        if raw.is_empty() {
            min = [0.0; 3];
            max = [0.0; 3];
        }
        let bbox = BoundingBox3d { min, max };

        let mut points: Vec<SgiPoint> = raw
            .into_iter()
            .map(|(node_id, coords, cartesian, measure)| {
                let morton = Self::quantize_morton(&bbox, coords);
                SgiPoint {
                    node_id,
                    morton,
                    coords,
                    cartesian,
                    measure,
                }
            })
            .collect();

        points.sort_by(|a, b| {
            a.morton
                .cmp(&b.morton)
                .then_with(|| a.node_id.as_u64().cmp(&b.node_id.as_u64()))
        });

        let mut rank_of = HashMap::with_capacity(points.len());
        for (idx, pt) in points.iter().enumerate() {
            rank_of.insert(pt.node_id, idx);
        }

        let (fenwick_sum, seg_min, seg_max) = if convention.measure_prop.is_some() && !points.is_empty() {
            let sum_vals: Vec<RollupValue> = points
                .iter()
                .map(|p| p.measure.unwrap_or(RollupValue::Float(0.0)))
                .collect();
            let null_vals: Vec<RollupValue> = points
                .iter()
                .map(|p| p.measure.unwrap_or(RollupValue::Null))
                .collect();
            (
                Some(Fenwick::build(&sum_vals)),
                Some(SegmentTree::build(&null_vals, RollupOp::Min)),
                Some(SegmentTree::build(&null_vals, RollupOp::Max)),
            )
        } else {
            (None, None, None)
        };

        Self {
            convention,
            bbox,
            points,
            rank_of,
            fenwick_sum,
            seg_min,
            seg_max,
        }
    }

    /// Quantize `(x, y, z)` within `bbox` into a 63-bit 3D Morton code.
    pub fn quantize_morton(bbox: &BoundingBox3d, coords: [f64; 3]) -> u64 {
        let q = |axis: usize| -> u32 {
            let span = (bbox.max[axis] - bbox.min[axis]).max(1e-12);
            let norm = ((coords[axis] - bbox.min[axis]) / span).clamp(0.0, 1.0);
            (norm * MORTON_MAX_QUANT).round() as u32
        };
        morton_encode_3d(q(0), q(1), q(2))
    }

    /// Index-resident monoid roll-up over a contiguous Morton code interval `[morton_lo, morton_hi]`
    /// in $O(\log n)$ time (`#2036`).
    ///
    /// Mirrors `OehIndex::rollup` (`ADR-035`): `COUNT` is a rank subtraction, `SUM` is a
    /// Fenwick prefix difference, and `MIN`/`MAX` are `SegmentTree` range queries — with zero
    /// point scanning.
    pub fn rollup_morton_range(&self, morton_lo: u64, morton_hi: u64, op: RollupOp) -> RollupValue {
        if self.points.is_empty() || morton_lo > morton_hi {
            return if op == RollupOp::Count {
                RollupValue::Int(0)
            } else {
                RollupValue::Null
            };
        }
        let lo = self.points.partition_point(|p| p.morton < morton_lo);
        let hi_excl = self.points.partition_point(|p| p.morton <= morton_hi);
        if lo >= hi_excl {
            return if op == RollupOp::Count {
                RollupValue::Int(0)
            } else {
                RollupValue::Null
            };
        }
        let hi = hi_excl - 1;
        match op {
            RollupOp::Count => RollupValue::Int((hi - lo + 1) as i128),
            RollupOp::Sum => self
                .fenwick_sum
                .as_ref()
                .map(|f| f.range(lo, hi))
                .unwrap_or(RollupValue::Null),
            RollupOp::Min => self
                .seg_min
                .as_ref()
                .map(|s| s.range(lo, hi))
                .unwrap_or(RollupValue::Null),
            RollupOp::Max => self
                .seg_max
                .as_ref()
                .map(|s| s.range(lo, hi))
                .unwrap_or(RollupValue::Null),
        }
    }

    /// Exact 3D bounding-box roll-up narrowed by the Z-order Morton range `[morton(min), morton(max)]`.
    pub fn rollup_bbox(&self, query_box: &BoundingBox3d, op: RollupOp) -> RollupValue {
        if self.points.is_empty() {
            return if op == RollupOp::Count {
                RollupValue::Int(0)
            } else {
                RollupValue::Null
            };
        }
        let clamped_min = [
            query_box.min[0].max(self.bbox.min[0]),
            query_box.min[1].max(self.bbox.min[1]),
            query_box.min[2].max(self.bbox.min[2]),
        ];
        let clamped_max = [
            query_box.max[0].min(self.bbox.max[0]),
            query_box.max[1].min(self.bbox.max[1]),
            query_box.max[2].min(self.bbox.max[2]),
        ];
        if clamped_min[0] > clamped_max[0]
            || clamped_min[1] > clamped_max[1]
            || clamped_min[2] > clamped_max[2]
        {
            return if op == RollupOp::Count {
                RollupValue::Int(0)
            } else {
                RollupValue::Null
            };
        }
        let morton_lo = Self::quantize_morton(&self.bbox, clamped_min);
        let morton_hi = Self::quantize_morton(&self.bbox, clamped_max);
        let lo = self.points.partition_point(|p| p.morton < morton_lo);
        let hi_excl = self.points.partition_point(|p| p.morton <= morton_hi);

        let mut count = 0i128;
        let mut acc = op.identity();
        for pt in &self.points[lo..hi_excl] {
            if query_box.contains(pt.coords[0], pt.coords[1], pt.coords[2]) {
                count += 1;
                if op != RollupOp::Count {
                    if let Some(v) = pt.measure {
                        acc = op.combine(acc, v);
                    }
                }
            }
        }
        if op == RollupOp::Count {
            RollupValue::Int(count)
        } else {
            acc
        }
    }

    /// Ambient manifold distance between two indexed nodes (`Å` / units for `Euclidean3d`,
    /// metres for `GeoSpherical`).
    pub fn manifold_distance(&self, a: NodeId, b: NodeId) -> Option<f64> {
        let pa = &self.points[*self.rank_of.get(&a)?];
        let pb = &self.points[*self.rank_of.get(&b)?];
        Some(self.distance_coords(pa.coords, pb.coords))
    }

    fn distance_coords(&self, a: [f64; 3], b: [f64; 3]) -> f64 {
        match &self.convention.manifold {
            ManifoldSpec::GeoSpherical { .. } => {
                let horiz = haversine_m((a[0], a[1]), (b[0], b[1]));
                let dz = a[2] - b[2];
                (horiz * horiz + dz * dz).sqrt()
            }
            _ => {
                let dx = a[0] - b[0];
                let dy = a[1] - b[1];
                let dz = a[2] - b[2];
                (dx * dx + dy * dy + dz * dz).sqrt()
            }
        }
    }

    /// Extract implicit 3D proximity / contact edges within `radius` using a 3D spatial voxel
    /// hash grid (`#2035`, `#2036`, `#2037`), together with graph hop distance and
    /// topological–geometric stress $\sigma(u,v) = d_{\text{graph}}(u,v) / d_{\text{manifold}}(u,v)$.
    pub fn contact_edges(
        &self,
        store: &GraphStore,
        radius: f64,
        max_contacts: usize,
    ) -> Vec<ContactEdgeCue> {
        if radius <= 0.0 || self.points.len() < 2 {
            return Vec::new();
        }
        let cell_size = radius.max(1e-6);
        let mut grid: HashMap<(i64, i64, i64), Vec<usize>> = HashMap::new();
        for (idx, pt) in self.points.iter().enumerate() {
            let cell = (
                (pt.cartesian[0] / cell_size).floor() as i64,
                (pt.cartesian[1] / cell_size).floor() as i64,
                (pt.cartesian[2] / cell_size).floor() as i64,
            );
            grid.entry(cell).or_default().push(idx);
        }

        // Direct 1-hop adjacency set so we can separate non-bonded spatial contacts from
        // explicit covalent/network edges.
        let mut direct_edges: HashSet<(u64, u64)> = HashSet::new();
        for pt in &self.points {
            let u = pt.node_id;
            for e in store.get_outgoing_edges(u) {
                let (a, b) = if u.as_u64() <= e.target.as_u64() {
                    (u.as_u64(), e.target.as_u64())
                } else {
                    (e.target.as_u64(), u.as_u64())
                };
                direct_edges.insert((a, b));
            }
        }

        let mut contacts = Vec::new();
        for (idx_a, pa) in self.points.iter().enumerate() {
            let base = (
                (pa.cartesian[0] / cell_size).floor() as i64,
                (pa.cartesian[1] / cell_size).floor() as i64,
                (pa.cartesian[2] / cell_size).floor() as i64,
            );
            for dx in -1..=1 {
                for dy in -1..=1 {
                    for dz in -1..=1 {
                        let Some(bucket) = grid.get(&(base.0 + dx, base.1 + dy, base.2 + dz))
                        else {
                            continue;
                        };
                        for &idx_b in bucket {
                            if idx_b <= idx_a {
                                continue;
                            }
                            let pb = &self.points[idx_b];
                            let dist = self.distance_coords(pa.coords, pb.coords);
                            if dist <= radius && dist > 1e-9 {
                                let (u, v) = if pa.node_id.as_u64() <= pb.node_id.as_u64() {
                                    (pa.node_id, pb.node_id)
                                } else {
                                    (pb.node_id, pa.node_id)
                                };
                                let is_explicit = direct_edges.contains(&(u.as_u64(), v.as_u64()));
                                if !is_explicit {
                                    let hops = bounded_hop_distance(store, u, v, 12);
                                    let stress = hops.unwrap_or(12) as f64 / dist.max(0.1);
                                    contacts.push(ContactEdgeCue {
                                        source: u.as_u64(),
                                        target: v.as_u64(),
                                        distance: dist,
                                        graph_hops: hops,
                                        structural_stress: stress,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Order highest structural stress first (long-range folding contacts / allosteric bridges)
        contacts.sort_by(|a, b| {
            b.structural_stress
                .partial_cmp(&a.structural_stress)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.source.cmp(&b.source))
                .then_with(|| a.target.cmp(&b.target))
        });
        contacts.truncate(max_contacts);
        contacts
    }

    /// Manifold-guided $A^*$ shortest path (`#2037`), returning both the optimal path and the
    /// explored-state reduction relative to blind Dijkstra/BFS.
    pub fn astar_shortest_path(
        &self,
        store: &GraphStore,
        start: NodeId,
        goal: NodeId,
        edge_types: Option<&[EdgeType]>,
    ) -> Option<AstarPathResult> {
        let goal_pt = &self.points[*self.rank_of.get(&goal)?];
        let start_pt = &self.points[*self.rank_of.get(&start)?];
        let direct_distance = self.distance_coords(start_pt.coords, goal_pt.coords);

        #[derive(Clone, Copy, PartialEq)]
        struct State {
            f_score: f64,
            g_score: f64,
            node: NodeId,
        }
        impl Eq for State {}
        impl Ord for State {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .f_score
                    .partial_cmp(&self.f_score)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| other.node.as_u64().cmp(&self.node.as_u64()))
            }
        }
        impl PartialOrd for State {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let neighbors = |u: NodeId| -> Vec<NodeId> {
            let mut nbrs = Vec::new();
            for e in store.get_outgoing_edges(u) {
                if edge_types.is_none_or(|ets| ets.contains(&e.edge_type)) {
                    nbrs.push(e.target);
                }
            }
            for e in store.get_incoming_edges(u) {
                if edge_types.is_none_or(|ets| ets.contains(&e.edge_type)) {
                    nbrs.push(e.source);
                }
            }
            nbrs.sort_by_key(|n| n.as_u64());
            nbrs.dedup();
            nbrs
        };

        let mut open = BinaryHeap::new();
        let mut g_best: HashMap<NodeId, f64> = HashMap::new();
        let mut parent: HashMap<NodeId, (NodeId, usize)> = HashMap::new();
        let mut explored_astar = 0usize;

        g_best.insert(start, 0.0);
        open.push(State {
            f_score: direct_distance,
            g_score: 0.0,
            node: start,
        });

        let mut found_cost = None;
        while let Some(State { g_score, node, .. }) = open.pop() {
            if g_score > *g_best.get(&node).unwrap_or(&f64::INFINITY) {
                continue;
            }
            explored_astar += 1;
            if node == goal {
                found_cost = Some(g_score);
                break;
            }
            let u_coords = self
                .rank_of
                .get(&node)
                .map(|&r| self.points[r].coords);
            for v in neighbors(node) {
                let step = match (u_coords, self.rank_of.get(&v)) {
                    (Some(uc), Some(&vr)) => {
                        self.distance_coords(uc, self.points[vr].coords).max(1e-6)
                    }
                    _ => 1.0,
                };
                let tentative_g = g_score + step;
                if tentative_g < *g_best.get(&v).unwrap_or(&f64::INFINITY) {
                    let hops = parent.get(&node).map(|(_, h)| h + 1).unwrap_or(1);
                    g_best.insert(v, tentative_g);
                    parent.insert(v, (node, hops));
                    let h = self
                        .rank_of
                        .get(&v)
                        .map(|&vr| self.distance_coords(self.points[vr].coords, goal_pt.coords))
                        .unwrap_or(0.0);
                    open.push(State {
                        f_score: tentative_g + h,
                        g_score: tentative_g,
                        node: v,
                    });
                }
            }
        }

        let path_length = found_cost?;
        let mut path = vec![goal.as_u64()];
        let mut cur = goal;
        while cur != start {
            let &(prev, _) = parent.get(&cur)?;
            path.push(prev.as_u64());
            cur = prev;
        }
        path.reverse();

        // Measure unguided Dijkstra/BFS explored states for exact comparison.
        let mut open_dijkstra = BinaryHeap::new();
        let mut d_best: HashMap<NodeId, f64> = HashMap::new();
        let mut explored_dijkstra = 0usize;
        d_best.insert(start, 0.0);
        open_dijkstra.push(State {
            f_score: 0.0,
            g_score: 0.0,
            node: start,
        });
        while let Some(State { g_score, node, .. }) = open_dijkstra.pop() {
            if g_score > *d_best.get(&node).unwrap_or(&f64::INFINITY) {
                continue;
            }
            explored_dijkstra += 1;
            if node == goal {
                break;
            }
            let u_coords = self
                .rank_of
                .get(&node)
                .map(|&r| self.points[r].coords);
            for v in neighbors(node) {
                let step = match (u_coords, self.rank_of.get(&v)) {
                    (Some(uc), Some(&vr)) => {
                        self.distance_coords(uc, self.points[vr].coords).max(1e-6)
                    }
                    _ => 1.0,
                };
                let tg = g_score + step;
                if tg < *d_best.get(&v).unwrap_or(&f64::INFINITY) {
                    d_best.insert(v, tg);
                    open_dijkstra.push(State {
                        f_score: tg,
                        g_score: tg,
                        node: v,
                    });
                }
            }
        }

        let stretch_ratio = if direct_distance > 1e-9 {
            path_length / direct_distance
        } else {
            1.0
        };

        Some(AstarPathResult {
            path_node_ids: path.clone(),
            hops: path.len().saturating_sub(1),
            manifold_path_length: path_length,
            direct_manifold_distance: direct_distance,
            stretch_ratio,
            explored_astar,
            explored_uninformed: explored_dijkstra,
        })
    }
}

/// Unweighted BFS hop distance up to `max_hops`.
fn bounded_hop_distance(
    store: &GraphStore,
    start: NodeId,
    goal: NodeId,
    max_hops: usize,
) -> Option<usize> {
    if start == goal {
        return Some(0);
    }
    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut queue: VecDeque<(NodeId, usize)> = VecDeque::new();
    visited.insert(start);
    queue.push_back((start, 0));
    while let Some((u, d)) = queue.pop_front() {
        if d >= max_hops {
            continue;
        }
        for e in store.get_outgoing_edges(u) {
            if e.target == goal {
                return Some(d + 1);
            }
            if visited.insert(e.target) {
                queue.push_back((e.target, d + 1));
            }
        }
        for e in store.get_incoming_edges(u) {
            if e.source == goal {
                return Some(d + 1);
            }
            if visited.insert(e.source) {
                queue.push_back((e.source, d + 1));
            }
        }
    }
    None
}

/// Implicit 3D proximity / contact edge and its topological-geometric stress (`#2035`, `#2037`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContactEdgeCue {
    pub source: u64,
    pub target: u64,
    pub distance: f64,
    pub graph_hops: Option<usize>,
    /// Stretch ratio `graph_hops / distance` (high values flag long-range folding contacts).
    pub structural_stress: f64,
}

/// Result of a manifold-guided $A^*$ shortest-path query (`#2037`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AstarPathResult {
    pub path_node_ids: Vec<u64>,
    pub hops: usize,
    pub manifold_path_length: f64,
    pub direct_manifold_distance: f64,
    pub stretch_ratio: f64,
    pub explored_astar: usize,
    pub explored_uninformed: usize,
}

/// Server-computed 3D & non-WebGL 2.5D coordinates for one node (`#2035`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuralNodeCue {
    pub id: u64,
    pub morton: u64,
    /// Raw metric coordinates `(x, y, z)` or `(lon, lat, alt)`.
    pub x: f64,
    pub y: f64,
    pub z: f64,
    /// Normalized 3D coordinates in `[-1.0, 1.0]` centered on the bounding box for WebGL cameras.
    pub norm_x: f64,
    pub norm_y: f64,
    pub norm_z: f64,
    /// Non-WebGL 2.5D isometric projected X coordinate.
    pub iso_x: f64,
    /// Non-WebGL 2.5D isometric projected Y coordinate.
    pub iso_y: f64,
    /// Normalized camera depth in `[0.0, 1.0]` (`0.0` = closest to viewer, `1.0` = farthest).
    pub depth_norm: f64,
    /// Back-to-front painter's-algorithm draw rank (`0` = draw first / backmost).
    pub draw_order_rank: usize,
    /// Suggested depth-fog opacity in `[0.3, 1.0]` for non-WebGL 2D canvas rendering.
    pub opacity: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_order: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measure: Option<f64>,
}

/// Ordered polymer / sequence backbone polyline for rendering 3D ribbons or transit lines.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackboneChainCue {
    pub chain_id: String,
    pub node_ids: Vec<u64>,
}

/// Complete visual analytic cues response for a structural convention (`#2035`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuralVisualCues {
    pub convention: StructuralConvention,
    pub bbox: BoundingBox3d,
    pub total_indexed_nodes: usize,
    pub nodes: Vec<StructuralNodeCue>,
    pub backbone_chains: Vec<BackboneChainCue>,
    pub contact_edges: Vec<ContactEdgeCue>,
    pub whole_graph_rollups: BTreeMap<String, serde_json::Value>,
}

/// Build [`StructuralVisualCues`] for `convention` over `store` (`#2035`).
pub fn build_structural_visual_cues(
    store: &GraphStore,
    convention: &StructuralConvention,
    filter_nodes: Option<&[NodeId]>,
    max_nodes: Option<usize>,
    contact_radius_override: Option<f64>,
) -> StructuralVisualCues {
    let sgi = SgiIndex::build(store, convention.clone());
    let total_indexed_nodes = sgi.points.len();
    let center = sgi.bbox.center();
    let half_span = (0.5 * sgi.bbox.max_span()).max(1e-9);

    let filter_set: Option<HashSet<NodeId>> =
        filter_nodes.map(|ids| ids.iter().copied().collect());

    let cos_az = std::f64::consts::FRAC_PI_4.cos();
    let sin_el = (30.0_f64).to_radians().sin();
    let cos_el = (30.0_f64).to_radians().cos();

    let (seq_prop, group_prop, default_contact_r) = match &convention.manifold {
        ManifoldSpec::Euclidean3d {
            sequence_prop,
            group_by_prop,
            contact_radius,
            ..
        } => (
            sequence_prop.as_deref(),
            group_by_prop.as_deref(),
            *contact_radius,
        ),
        ManifoldSpec::GeoSpherical {
            contact_radius_m, ..
        } => (None, None, *contact_radius_m),
        _ => (None, None, None),
    };

    let mut selected_pts: Vec<&SgiPoint> = sgi
        .points
        .iter()
        .filter(|pt| filter_set.as_ref().is_none_or(|s| s.contains(&pt.node_id)))
        .collect();
    if let Some(limit) = max_nodes {
        if selected_pts.len() > limit && limit > 0 {
            selected_pts.truncate(limit);
        }
    }

    let mut cues: Vec<StructuralNodeCue> = Vec::with_capacity(selected_pts.len());
    let mut raw_depths: Vec<(usize, f64)> = Vec::with_capacity(selected_pts.len());

    for (idx, pt) in selected_pts.iter().enumerate() {
        let nx = ((pt.cartesian[0] - center[0]) / half_span).clamp(-1.0, 1.0);
        let ny = ((pt.cartesian[1] - center[1]) / half_span).clamp(-1.0, 1.0);
        let nz = ((pt.cartesian[2] - center[2]) / half_span).clamp(-1.0, 1.0);

        let iso_x = (nx - ny) * cos_az;
        let iso_y = (nx + ny) * sin_el - nz * cos_el;
        let cam_depth = (nx + ny) * cos_el + nz * sin_el;
        let depth_norm = ((cam_depth + 2.0) / 4.0).clamp(0.0, 1.0);
        let opacity = (1.0 - 0.6 * depth_norm).clamp(0.3, 1.0);

        let chain_group = group_prop
            .and_then(|p| node_group_string(store, pt.node_id, p));
        let sequence_order = seq_prop.and_then(|p| node_f64(store, pt.node_id, p));

        raw_depths.push((idx, depth_norm));
        cues.push(StructuralNodeCue {
            id: pt.node_id.as_u64(),
            morton: pt.morton,
            x: pt.coords[0],
            y: pt.coords[1],
            z: pt.coords[2],
            norm_x: nx,
            norm_y: ny,
            norm_z: nz,
            iso_x,
            iso_y,
            depth_norm,
            draw_order_rank: 0,
            opacity,
            chain_group,
            sequence_order,
            measure: pt.measure.and_then(|v| v.as_f64()),
        });
    }

    // Back-to-front painter's-algorithm rank: largest depth_norm (farthest) gets rank 0.
    raw_depths.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    for (rank, (cue_idx, _)) in raw_depths.into_iter().enumerate() {
        cues[cue_idx].draw_order_rank = rank;
    }

    // Build ordered backbone chains when `sequence_prop` is available
    let mut backbone_chains = Vec::new();
    if seq_prop.is_some() {
        let mut by_chain: BTreeMap<String, Vec<(f64, u64)>> = BTreeMap::new();
        for c in &cues {
            if let Some(seq) = c.sequence_order {
                let chain = c
                    .chain_group
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                by_chain.entry(chain).or_default().push((seq, c.id));
            }
        }
        for (chain_id, mut members) in by_chain {
            members.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
            if members.len() >= 2 {
                backbone_chains.push(BackboneChainCue {
                    chain_id,
                    node_ids: members.into_iter().map(|(_, id)| id).collect(),
                });
            }
        }
    }

    let contact_r = contact_radius_override.or(default_contact_r).unwrap_or(0.0);
    let contact_edges = if contact_r > 0.0 {
        sgi.contact_edges(store, contact_r, 512)
    } else {
        Vec::new()
    };

    let mut whole_graph_rollups = BTreeMap::new();
    whole_graph_rollups.insert(
        "count".to_string(),
        serde_json::json!(total_indexed_nodes),
    );
    if convention.measure_prop.is_some() && total_indexed_nodes > 0 {
        for op in [RollupOp::Sum, RollupOp::Min, RollupOp::Max] {
            let val = sgi.rollup_morton_range(0, u64::MAX, op);
            if let Some(f) = val.as_f64() {
                whole_graph_rollups.insert(op.name().to_string(), serde_json::json!(f));
            }
        }
    }

    StructuralVisualCues {
        convention: convention.clone(),
        bbox: sgi.bbox,
        total_indexed_nodes,
        nodes: cues,
        backbone_chains,
        contact_edges,
        whole_graph_rollups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn morton_3d_roundtrip_preserves_21_bit_coordinates() {
        for &(x, y, z) in &[
            (0, 0, 0),
            (1, 2, 3),
            (1024, 2048, 4096),
            ((1 << 21) - 1, (1 << 20) + 7, 42),
        ] {
            let code = morton_encode_3d(x, y, z);
            assert_eq!(morton_decode_3d(code), (x, y, z));
        }
    }

    #[test]
    fn sgi_index_resident_rollup_and_contacts_on_protein_helix() {
        let mut store = GraphStore::new();
        // Create an alpha-helix-like 3D chain of 12 residues where residue 0 and residue 10
        // fold back within 6.0 Angstroms of each other (an allosteric tertiary contact!).
        let mut ids = Vec::new();
        for i in 0..12 {
            let n = store.create_node("Residue");
            let (x, y, z) = if i == 10 {
                // Folded back close to residue 0 (0.0, 0.0, 0.0)
                (3.5, 2.0, 1.5)
            } else {
                (i as f64 * 3.8, (i as f64).sin() * 2.5, i as f64 * 1.5)
            };
            store
                .set_node_property("default", n, "ca_x", PropertyValue::Float(x))
                .unwrap();
            store
                .set_node_property("default", n, "ca_y", PropertyValue::Float(y))
                .unwrap();
            store
                .set_node_property("default", n, "ca_z", PropertyValue::Float(z))
                .unwrap();
            store
                .set_node_property("default", n, "residue_seq", PropertyValue::Integer(i + 1))
                .unwrap();
            store
                .set_node_property(
                    "default",
                    n,
                    "chain_id",
                    PropertyValue::String("A".to_string()),
                )
                .unwrap();
            store
                .set_node_property(
                    "default",
                    n,
                    "b_factor",
                    PropertyValue::Float((i + 1) as f64 * 10.0),
                )
                .unwrap();
            ids.push(n);
        }
        // Covalent peptide backbone edges (i -> i+1)
        for w in ids.windows(2) {
            store.create_edge(w[0], w[1], "PEPTIDE_BOND").unwrap();
        }

        let detected = detect_conventions(&store);
        assert_eq!(detected.len(), 1);
        assert_eq!(
            detected[0].visual_preset.as_deref(),
            Some("protein_ribbon_3d")
        );

        let sgi = SgiIndex::build(&store, detected[0].clone());
        assert_eq!(sgi.points.len(), 12);

        // Index-resident SUM of b_factor = 10 * (1..=12) = 780.0
        let sum = sgi.rollup_morton_range(0, u64::MAX, RollupOp::Sum);
        assert_eq!(sum.as_f64(), Some(780.0));
        let max = sgi.rollup_morton_range(0, u64::MAX, RollupOp::Max);
        assert_eq!(max.as_f64(), Some(120.0));

        // Implicit 8.0 Angstrom contact detection must find the tertiary contact (0, 10)
        // with graph_hops = 10 and high structural stress!
        let contacts = sgi.contact_edges(&store, 8.0, 20);
        let tertiary = contacts
            .iter()
            .find(|c| {
                (c.source == ids[0].as_u64() && c.target == ids[10].as_u64())
                    || (c.source == ids[10].as_u64() && c.target == ids[0].as_u64())
            })
            .expect("tertiary contact (0, 10) within 8.0 A must be found");
        assert_eq!(tertiary.graph_hops, Some(10));
        assert!(tertiary.distance < 6.0);
        assert!(tertiary.structural_stress > 1.5);

        // Manifold-guided A* along the backbone
        let astar = sgi
            .astar_shortest_path(&store, ids[0], ids[11], None)
            .expect("path exists");
        assert_eq!(astar.hops, 11);
        assert_eq!(astar.path_node_ids.len(), 12);
    }
}
