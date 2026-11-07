//! `CREATE / SHOW / DROP / REBUILD STRUCTURE INDEX` (SGI, ADR-041 / #2036).
//!
//! The index itself already exists: [`SgiIndex`] interleaves quantized
//! coordinates into a 63-bit 3D Morton code and answers index-resident
//! roll-ups over Morton ranges in $O(\log n)$. What it had no way to be was
//! *declared* -- it was reachable only from HTTP, so a graph's structure was
//! something a caller asserted per request rather than something the database
//! knew. These operators give it the same DDL surface
//! `CREATE HIERARCHY INDEX` has, and store the declaration in the
//! [`StructureRegistry`] that `GraphStore` already owns.
//!
//! Query-time use of the declaration -- rewriting `distance3d(a, b) <= r` and
//! the virtual `:CONTACT` pattern into a structure-aware operator -- is the
//! next slice of #2036 and deliberately not here.

use std::sync::Arc;

use crate::graph::{GraphStore, PropertyValue};
use crate::index::structure::{ManifoldSpec, SgiIndex, StructuralConvention};
use crate::query::ast::CreateStructureIndexClause;

use super::operator::{OperatorDescription, PhysicalOperator};
use super::record::{Record, Value};
use super::{ExecutionError, ExecutionResult};

/// Turn a parsed declaration into the convention the index is built from.
///
/// The arity of the coordinate list decides the manifold when `METRIC` is
/// absent: two properties are a geographic pair, three a 3D position. An
/// explicit `METRIC geo` with three coordinates means the third is elevation.
pub fn convention_from_clause(clause: &CreateStructureIndexClause) -> StructuralConvention {
    let geo = match clause.metric.as_deref() {
        Some("geo") | Some("geospherical") | Some("spherical") => true,
        Some(_) => false,
        None => clause.coords.len() == 2,
    };
    let manifold = if geo {
        ManifoldSpec::GeoSpherical {
            lat_prop: clause.coords[0].clone(),
            lon_prop: clause.coords[1].clone(),
            alt_prop: clause.coords.get(2).cloned(),
            crs: "EPSG:4326".to_string(),
            contact_radius_m: clause.contact_radius,
        }
    } else {
        ManifoldSpec::Euclidean3d {
            x_prop: clause.coords[0].clone(),
            y_prop: clause.coords[1].clone(),
            // The grammar admits two coordinates with an explicit euclidean
            // metric only via a parse error, so this is a defensive default
            // rather than a reachable one.
            z_prop: clause.coords.get(2).cloned().unwrap_or_default(),
            unit: "unit".to_string(),
            sequence_prop: clause.sequence_prop.clone(),
            group_by_prop: clause.group_by_prop.clone(),
            contact_radius: clause.contact_radius,
        }
    };
    StructuralConvention {
        name: clause.name.clone(),
        label: Some(clause.label.clone()),
        manifold,
        visual_preset: None,
        measure_prop: clause.measure_prop.clone(),
    }
}

fn text(value: &str) -> Value {
    Value::Property(PropertyValue::String(value.to_string()))
}

/// One row describing a declared convention, as `SHOW` and `CREATE` report it.
fn convention_record(c: &StructuralConvention, points: Option<usize>) -> Record {
    let mut record = Record::new();
    record.bind("name".to_string(), text(&c.name));
    record.bind(
        "label".to_string(),
        c.label.as_deref().map(text).unwrap_or(Value::Null),
    );
    let (kind, coords) = match &c.manifold {
        ManifoldSpec::Euclidean3d {
            x_prop,
            y_prop,
            z_prop,
            ..
        } => ("euclidean3d", format!("{x_prop}, {y_prop}, {z_prop}")),
        ManifoldSpec::GeoSpherical {
            lat_prop,
            lon_prop,
            alt_prop,
            ..
        } => (
            "geospherical",
            match alt_prop {
                Some(a) => format!("{lat_prop}, {lon_prop}, {a}"),
                None => format!("{lat_prop}, {lon_prop}"),
            },
        ),
        ManifoldSpec::HierarchyPoset { index_name, .. } => {
            ("hierarchy_poset", index_name.clone())
        }
        ManifoldSpec::CausalSpacetime {
            time_prop,
            x_prop,
            y_prop,
        } => ("causal_spacetime", format!("{time_prop}, {x_prop}, {y_prop}")),
    };
    record.bind("manifold".to_string(), text(kind));
    record.bind("coordinates".to_string(), text(&coords));
    let radius = match &c.manifold {
        ManifoldSpec::Euclidean3d { contact_radius, .. } => *contact_radius,
        ManifoldSpec::GeoSpherical {
            contact_radius_m, ..
        } => *contact_radius_m,
        _ => None,
    };
    record.bind(
        "contact_radius".to_string(),
        radius
            .map(|r| Value::Property(PropertyValue::Float(r)))
            .unwrap_or(Value::Null),
    );
    record.bind(
        "measure".to_string(),
        c.measure_prop.as_deref().map(text).unwrap_or(Value::Null),
    );
    // Absent for SHOW, which reads declarations and does not build: reporting a
    // point count there would mean scanning the store on a listing.
    record.bind(
        "points".to_string(),
        points
            .map(|n| Value::Property(PropertyValue::Integer(n as i64)))
            .unwrap_or(Value::Null),
    );
    record
}

/// Columns of a structure-index description, in the order the record binds them.
pub fn structure_info_columns() -> Vec<String> {
    [
        "name",
        "label",
        "manifold",
        "coordinates",
        "contact_radius",
        "measure",
        "points",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Columns of a drop acknowledgement.
pub fn structure_drop_columns() -> Vec<String> {
    ["name", "dropped"].iter().map(|s| s.to_string()).collect()
}

/// `CREATE STRUCTURE INDEX <name> ON :Label(x, y, z) ...`
pub struct CreateStructureIndexOperator {
    clause: CreateStructureIndexClause,
    executed: bool,
}

impl CreateStructureIndexOperator {
    pub fn new(clause: CreateStructureIndexClause) -> Self {
        Self {
            clause,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateStructureIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateStructureIndexOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let convention = convention_from_clause(&self.clause);
        // Built once here, so `CREATE` fails loudly on a label whose nodes do
        // not carry the coordinates rather than registering a declaration that
        // every later query finds empty.
        let index = SgiIndex::build(store, convention.clone());
        let points = index.points.len();
        if points == 0 {
            return Err(ExecutionError::RuntimeError(format!(
                "CREATE STRUCTURE INDEX {}: no node labelled {} carries all of ({}) \
                 as numbers, so the index would be empty",
                self.clause.name,
                self.clause.label,
                self.clause.coords.join(", ")
            )));
        }
        let registry = Arc::clone(&store.structure_registry);
        registry.upsert(convention.clone());
        Ok(Some(convention_record(&convention, Some(points))))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateStructureIndex".to_string(),
            details: format!(
                "{} on :{}({})",
                self.clause.name,
                self.clause.label,
                self.clause.coords.join(", ")
            ),
            children: Vec::new(),
        }
    }
}

/// `SHOW STRUCTURE INDEXES`
pub struct ShowStructureIndexesOperator {
    rows: Option<std::vec::IntoIter<Record>>,
}

impl ShowStructureIndexesOperator {
    pub fn new() -> Self {
        Self { rows: None }
    }

    fn rows_for(store: &GraphStore) -> std::vec::IntoIter<Record> {
        // `effective_conventions` adds the auto-detected ones, which is what a
        // caller asking what structure this graph has wants to see -- the
        // declared ones alone would hide a convention the engine is already
        // using.
        store
            .structure_registry
            .effective_conventions(store)
            .iter()
            .map(|c| convention_record(c, None))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl Default for ShowStructureIndexesOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOperator for ShowStructureIndexesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        let rows = self.rows.get_or_insert_with(|| Self::rows_for(store));
        Ok(rows.next())
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant: &str,
    ) -> ExecutionResult<Option<Record>> {
        let rows = match self.rows.as_mut() {
            Some(rows) => rows,
            None => {
                let built = Self::rows_for(store);
                self.rows = Some(built);
                self.rows.as_mut().expect("just set")
            }
        };
        Ok(rows.next())
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowStructureIndexes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// `DROP STRUCTURE INDEX <name>`
pub struct DropStructureIndexOperator {
    name: String,
    executed: bool,
}

impl DropStructureIndexOperator {
    pub fn new(name: String) -> Self {
        Self {
            name,
            executed: false,
        }
    }
}

impl PhysicalOperator for DropStructureIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "DropStructureIndexOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        if !store.structure_registry.remove(&self.name) {
            return Err(ExecutionError::RuntimeError(format!(
                "DROP STRUCTURE INDEX {}: no such structure index",
                self.name
            )));
        }
        let mut record = Record::new();
        record.bind("name".to_string(), text(&self.name));
        record.bind("dropped".to_string(), Value::Property(PropertyValue::Boolean(true)));
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "DropStructureIndex".to_string(),
            details: self.name.clone(),
            children: Vec::new(),
        }
    }
}

/// `REBUILD STRUCTURE INDEX <name>`
pub struct RebuildStructureIndexOperator {
    name: String,
    executed: bool,
}

impl RebuildStructureIndexOperator {
    pub fn new(name: String) -> Self {
        Self {
            name,
            executed: false,
        }
    }
}

impl PhysicalOperator for RebuildStructureIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "RebuildStructureIndexOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let convention = store.structure_registry.get(&self.name).ok_or_else(|| {
            ExecutionError::RuntimeError(format!(
                "REBUILD STRUCTURE INDEX {}: no such structure index",
                self.name
            ))
        })?;
        let index = SgiIndex::build(store, convention.clone());
        Ok(Some(convention_record(&convention, Some(index.points.len()))))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "RebuildStructureIndex".to_string(),
            details: self.name.clone(),
            children: Vec::new(),
        }
    }
}
