//! Physical operators for the OEH hierarchy index (ADR-035 §8).
//!
//! Two groups:
//!
//! - **DDL** — `CreateHierarchyIndex`, `DropHierarchyIndex`, `RebuildHierarchyIndex`,
//!   `ShowHierarchyIndexes`. `CREATE` returns a row describing the outcome, including the
//!   selected encoding and, when the structural probe declines, the diagnostic explaining
//!   why. A decline is reported as a row, not raised as an error: the user asked a
//!   reasonable question about a poset that happens to be out of regime, and the answer is
//!   "use a 2-hop index", not a failure.
//! - **Query** — `HierarchyOrderTest` (O(1) subsumption predicate), `HierarchyRollup`
//!   (index-resident monoid fold), `HierarchyDescendantScan` (enumerate a subtree from the
//!   index instead of expanding edges).
//!
//! Every query operator carries the index name in its `EXPLAIN` details, so a plan makes
//! it obvious *which* hierarchy answered the query — and the planner's fallback path
//! reports why an index was not used when one exists but is stale or declined.

use crate::graph::{GraphStore, NodeId, PropertyValue};
use crate::index::hierarchy::{RollupOp, RollupValue};

use super::operator::{OperatorBox, OperatorDescription, PhysicalOperator};
use super::record::{Record, Value};
use super::{ExecutionError, ExecutionResult};

/// Convert a roll-up result into a property value for the result set.
///
/// An `i128` sum that does not fit an `i64` is surfaced as a float rather than silently
/// truncated — losing precision loudly beats reporting a wrapped number as exact.
pub fn rollup_to_property(v: RollupValue) -> PropertyValue {
    match v {
        RollupValue::Int(i) => match i64::try_from(i) {
            Ok(x) => PropertyValue::Integer(x),
            Err(_) => PropertyValue::Float(i as f64),
        },
        RollupValue::Float(f) => PropertyValue::Float(f),
        RollupValue::Null => PropertyValue::Null,
    }
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// `CREATE HIERARCHY INDEX <name> ON ()-[:T]->() [MEASURE p] [AGGREGATE ops]`
pub struct CreateHierarchyIndexOperator {
    spec: crate::index::hierarchy::HierarchySpec,
    executed: bool,
}

impl CreateHierarchyIndexOperator {
    /// Build from a parsed declaration.
    pub fn new(spec: crate::index::hierarchy::HierarchySpec) -> Self {
        Self {
            spec,
            executed: false,
        }
    }
}

fn info_record(info: &crate::index::hierarchy::HierarchyInfo) -> Record {
    let mut record = Record::new();
    record.bind(
        "name".to_string(),
        Value::Property(PropertyValue::String(info.name.clone())),
    );
    record.bind(
        "encoding".to_string(),
        Value::Property(PropertyValue::String(
            info.encoding.unwrap_or("declined").to_string(),
        )),
    );
    record.bind(
        "nodes".to_string(),
        Value::Property(PropertyValue::Integer(info.nodes as i64)),
    );
    record.bind(
        "edges".to_string(),
        Value::Property(PropertyValue::Integer(info.edges as i64)),
    );
    record.bind(
        "width".to_string(),
        Value::Property(match info.width {
            Some(w) => PropertyValue::Integer(w as i64),
            None => PropertyValue::Null,
        }),
    );
    record.bind(
        "measure".to_string(),
        Value::Property(match &info.measure {
            Some(m) => PropertyValue::String(m.clone()),
            None => PropertyValue::Null,
        }),
    );
    record.bind(
        "aggregates".to_string(),
        Value::Property(PropertyValue::String(info.ops.join(","))),
    );
    record.bind(
        "bytes".to_string(),
        Value::Property(PropertyValue::Integer(info.bytes as i64)),
    );
    record.bind(
        "structural_bytes".to_string(),
        Value::Property(PropertyValue::Integer(info.structural_bytes as i64)),
    );
    record.bind(
        "rollup_bytes".to_string(),
        Value::Property(PropertyValue::Integer(info.rollup_bytes as i64)),
    );
    record.bind(
        "stale".to_string(),
        Value::Property(PropertyValue::Boolean(info.stale)),
    );
    record.bind(
        "status".to_string(),
        Value::Property(PropertyValue::String(match &info.declined {
            Some(d) => d.clone(),
            None => "ok".to_string(),
        })),
    );
    record
}

/// Columns produced by every hierarchy DDL statement that reports an index.
pub fn hierarchy_info_columns() -> Vec<String> {
    vec![
        "name".to_string(),
        "encoding".to_string(),
        "nodes".to_string(),
        "edges".to_string(),
        "width".to_string(),
        "measure".to_string(),
        "aggregates".to_string(),
        "bytes".to_string(),
        "structural_bytes".to_string(),
        "rollup_bytes".to_string(),
        "stale".to_string(),
        "status".to_string(),
    ]
}

impl PhysicalOperator for CreateHierarchyIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateHierarchyIndexOperator requires mutable store access".to_string(),
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
        let mgr = std::sync::Arc::clone(&store.hierarchy_index);
        let info = mgr
            .create(store, self.spec.clone())
            .map_err(|e| ExecutionError::RuntimeError(e.to_string()))?;
        Ok(Some(info_record(&info)))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateHierarchyIndex".to_string(),
            details: format!(
                "{} on {}",
                self.spec.name,
                self.spec
                    .edge_types
                    .iter()
                    .map(|t| t.as_str().to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            ),
            children: Vec::new(),
        }
    }
}

/// `DROP HIERARCHY INDEX <name>`
pub struct DropHierarchyIndexOperator {
    name: String,
    executed: bool,
}

impl DropHierarchyIndexOperator {
    /// Drop by name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            executed: false,
        }
    }
}

impl PhysicalOperator for DropHierarchyIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "DropHierarchyIndexOperator requires mutable store access".to_string(),
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
        store
            .hierarchy_index
            .drop_index(&self.name)
            .map_err(|e| ExecutionError::RuntimeError(e.to_string()))?;
        Ok(None)
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "DropHierarchyIndex".to_string(),
            details: self.name.clone(),
            children: Vec::new(),
        }
    }
}

/// `REBUILD HIERARCHY INDEX <name>` — the supported answer to staleness.
pub struct RebuildHierarchyIndexOperator {
    name: String,
    executed: bool,
}

impl RebuildHierarchyIndexOperator {
    /// Rebuild by name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            executed: false,
        }
    }
}

impl PhysicalOperator for RebuildHierarchyIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "RebuildHierarchyIndexOperator requires mutable store access".to_string(),
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
        let mgr = std::sync::Arc::clone(&store.hierarchy_index);
        let info = mgr
            .rebuild(store, &self.name)
            .map_err(|e| ExecutionError::RuntimeError(e.to_string()))?;
        Ok(Some(info_record(&info)))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "RebuildHierarchyIndex".to_string(),
            details: self.name.clone(),
            children: Vec::new(),
        }
    }
}

/// `SHOW HIERARCHY INDEXES`
pub struct ShowHierarchyIndexesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowHierarchyIndexesOperator {
    /// List every declared hierarchy, declined ones included.
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl Default for ShowHierarchyIndexesOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOperator for ShowHierarchyIndexesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let records: Vec<Record> = store
                .hierarchy_index
                .list()
                .iter()
                .map(info_record)
                .collect();
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowHierarchyIndexes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Query operators
// ---------------------------------------------------------------------------

/// Filter rows by an O(1) subsumption test against a hierarchy index.
///
/// Replaces a variable-length expansion used as an existence test. The saving is per row:
/// the expansion costs O(ancestors(x)) each time, this costs two integer comparisons.
pub struct HierarchyOrderTestOperator {
    input: OperatorBox,
    index_name: String,
    /// Variable holding the candidate (`x` in `x ⊑ y`).
    child_var: String,
    /// The fixed ancestor to test against.
    ancestor: NodeId,
    /// When false the operator keeps rows that are *not* under `ancestor`.
    negated: bool,
}

impl HierarchyOrderTestOperator {
    /// Build the predicate operator.
    pub fn new(
        input: OperatorBox,
        index_name: String,
        child_var: String,
        ancestor: NodeId,
        negated: bool,
    ) -> Self {
        Self {
            input,
            index_name,
            child_var,
            ancestor,
            negated,
        }
    }
}

fn record_node_id(record: &Record, var: &str) -> Option<NodeId> {
    match record.get(var) {
        Some(Value::Node(id, _)) => Some(*id),
        Some(Value::NodeRef(id)) => Some(*id),
        _ => None,
    }
}

impl PhysicalOperator for HierarchyOrderTestOperator {
    fn children_mut(&mut self) -> Vec<&mut crate::query::executor::operator::OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        let entry = store.hierarchy_index.get(&self.index_name).ok_or_else(|| {
            ExecutionError::RuntimeError(format!(
                "hierarchy index '{}' disappeared mid-query",
                self.index_name
            ))
        })?;
        let guard = entry.read().unwrap();
        let index = guard.index.as_ref().ok_or_else(|| {
            ExecutionError::RuntimeError(format!(
                "hierarchy index '{}' is not built",
                self.index_name
            ))
        })?;

        while let Some(record) = self.input.next(store)? {
            let Some(node) = record_node_id(&record, &self.child_var) else {
                continue;
            };
            // A node outside the hierarchy is not under `ancestor` — but it is also not a
            // failure, so it simply does not pass the positive test.
            let inside = index.subsumes_ids(node, self.ancestor).unwrap_or(false);
            if inside != self.negated {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HierarchyOrderTest".to_string(),
            details: format!(
                "{}{} ⊑ {} via {}",
                if self.negated { "NOT " } else { "" },
                self.child_var,
                self.ancestor.as_u64(),
                self.index_name
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// Answer a subtree aggregate from the index, in one step, with no scan beneath it.
///
/// This is the operator that makes the roll-up *index-resident*: it has no input, does no
/// expansion, and produces exactly one row. An engine aggregation over the same subtree is
/// O(subtree); this is O(log n) in nested-set mode and O(width) in chain mode.
pub struct HierarchyRollupOperator {
    index_name: String,
    root: NodeId,
    op: RollupOp,
    alias: String,
    executed: bool,
}

impl HierarchyRollupOperator {
    /// Build the roll-up.
    pub fn new(index_name: String, root: NodeId, op: RollupOp, alias: String) -> Self {
        Self {
            index_name,
            root,
            op,
            alias,
            executed: false,
        }
    }
}

impl PhysicalOperator for HierarchyRollupOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let entry = store.hierarchy_index.get(&self.index_name).ok_or_else(|| {
            ExecutionError::RuntimeError(format!(
                "hierarchy index '{}' disappeared mid-query",
                self.index_name
            ))
        })?;
        let guard = entry.read().unwrap();
        let index = guard.index.as_ref().ok_or_else(|| {
            ExecutionError::RuntimeError(format!(
                "hierarchy index '{}' is not built",
                self.index_name
            ))
        })?;
        let value = index
            .rollup_id(self.root, self.op)
            .unwrap_or(RollupValue::Null);
        let mut record = Record::new();
        record.bind(
            self.alias.clone(),
            Value::Property(rollup_to_property(value)),
        );
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HierarchyRollup".to_string(),
            details: format!(
                "{}(measure) under {} via {}",
                self.op.name(),
                self.root.as_u64(),
                self.index_name
            ),
            children: Vec::new(),
        }
    }
}

/// Enumerate `{root} ∪ descendants(root)` straight from the index.
///
/// The alternative is a variable-length expansion, which on a DAG must also carry a
/// visited-set to avoid emitting a node once per path. Reading the descendant set out of
/// the index is duplicate-free by construction.
pub struct HierarchyDescendantScanOperator {
    index_name: String,
    root: NodeId,
    var: String,
    results: Option<std::vec::IntoIter<NodeId>>,
}

impl HierarchyDescendantScanOperator {
    /// Scan the subtree of `root`, binding each node to `var`.
    pub fn new(index_name: String, root: NodeId, var: String) -> Self {
        Self {
            index_name,
            root,
            var,
            results: None,
        }
    }
}

impl PhysicalOperator for HierarchyDescendantScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let entry = store.hierarchy_index.get(&self.index_name).ok_or_else(|| {
                ExecutionError::RuntimeError(format!(
                    "hierarchy index '{}' disappeared mid-query",
                    self.index_name
                ))
            })?;
            let guard = entry.read().unwrap();
            let index = guard.index.as_ref().ok_or_else(|| {
                ExecutionError::RuntimeError(format!(
                    "hierarchy index '{}' is not built",
                    self.index_name
                ))
            })?;
            let ids: Vec<NodeId> = match index.poset().idx(self.root) {
                Some(r) => index
                    .descendants(r)
                    .into_iter()
                    .map(|i| index.poset().node_at(i))
                    .collect(),
                None => Vec::new(),
            };
            self.results = Some(ids.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next().map(|id| {
            let mut record = Record::new();
            record.bind(self.var.clone(), Value::NodeRef(id));
            record
        }))
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HierarchyDescendantScan".to_string(),
            details: format!(
                "{} under {} via {}",
                self.var,
                self.root.as_u64(),
                self.index_name
            ),
            children: Vec::new(),
        }
    }
}
