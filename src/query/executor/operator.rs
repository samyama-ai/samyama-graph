//! Physical operators for query execution (Volcano iterator model)
//!
//! Implements ADR-007 (Volcano Iterator Model)

use crate::graph::{GraphStore, Label, NodeId, EdgeType};
use crate::query::ast::{Expression, BinaryOp, Direction};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::graph::PropertyValue;
use std::collections::{HashMap, HashSet};

/// Physical operator trait - all operators implement this
pub trait PhysicalOperator: Send {
    /// Get the next record from this operator (read-only operations)
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>>;

    /// Get the next record from this operator (write operations that mutate the store)
    fn next_mut(&mut self, store: &mut GraphStore) -> ExecutionResult<Option<Record>> {
        // Default implementation calls the read-only version
        self.next(store)
    }

    /// Reset the operator to start from the beginning
    fn reset(&mut self);

    /// Returns true if this operator mutates the graph store
    fn is_mutating(&self) -> bool {
        false
    }
}

/// Type alias for boxed operators
pub type OperatorBox = Box<dyn PhysicalOperator>;

/// Node scan operator: MATCH (n:Person)
pub struct NodeScanOperator {
    /// Variable name to bind nodes to
    variable: String,
    /// Labels to filter by
    labels: Vec<Label>,
    /// Current position in iteration
    node_ids: Vec<NodeId>,
    /// Current index
    current: usize,
}

impl NodeScanOperator {
    /// Create a new node scan operator
    pub fn new(variable: String, labels: Vec<Label>) -> Self {
        Self {
            variable,
            labels,
            node_ids: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) {
        if !self.node_ids.is_empty() {
            return;
        }

        // Get all nodes matching the labels
        if self.labels.is_empty() {
            // No labels - scan all nodes
            self.node_ids = store.all_nodes().into_iter().map(|n| n.id).collect();
        } else {
            // Get nodes for each label
            let mut node_set = HashSet::new();
            for label in &self.labels {
                let nodes = store.get_nodes_by_label(label);
                for node in nodes {
                    node_set.insert(node.id);
                }
            }

            // Convert to sorted vec for consistent ordering
            let mut nodes: Vec<_> = node_set.into_iter().collect();
            nodes.sort_by_key(|id| id.as_u64());
            self.node_ids = nodes;
        }
    }
}

impl PhysicalOperator for NodeScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        let node_id = self.node_ids[self.current];
        self.current += 1;

        let node = store.get_node(node_id)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("Node {:?} not found", node_id)))?;

        let mut record = Record::new();
        record.bind(self.variable.clone(), Value::Node(node_id, node.clone()));

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
    }
}

/// Filter operator: WHERE n.age > 30
pub struct FilterOperator {
    /// Input operator
    input: OperatorBox,
    /// Predicate expression
    predicate: Expression,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: OperatorBox, predicate: Expression) -> Self {
        Self { input, predicate }
    }

    fn evaluate_predicate(&self, record: &Record, _store: &GraphStore) -> ExecutionResult<bool> {
        let result = self.evaluate_expression(&self.predicate, record, _store)?;

        match result {
            Value::Property(PropertyValue::Boolean(b)) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(ExecutionError::TypeError("Predicate must evaluate to boolean".to_string())),
        }
    }

    fn evaluate_expression(&self, expr: &Expression, record: &Record, _store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => {
                record.get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Property { variable, property } => {
                let val = record.get(variable)
                    .ok_or_else(|| ExecutionError::VariableNotFound(variable.clone()))?;

                match val {
                    Value::Node(_, node) => {
                        let prop = node.get_property(property)
                            .cloned()
                            .unwrap_or(PropertyValue::Null);
                        Ok(Value::Property(prop))
                    }
                    Value::Edge(_, edge) => {
                        let prop = edge.get_property(property)
                            .cloned()
                            .unwrap_or(PropertyValue::Null);
                        Ok(Value::Property(prop))
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = self.evaluate_expression(left, record, _store)?;
                let right_val = self.evaluate_expression(right, record, _store)?;
                self.evaluate_binary_op(op, left_val, right_val)
            }
            Expression::Function { name, args } => {
                self.evaluate_function(name, args, record, _store)
            }
            _ => Err(ExecutionError::RuntimeError("Unsupported expression type".to_string())),
        }
    }

    fn evaluate_binary_op(&self, op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
        // Extract property values
        let left_prop = match left {
            Value::Property(p) => p,
            Value::Null => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Binary op requires property values".to_string())),
        };

        let right_prop = match right {
            Value::Property(p) => p,
            Value::Null => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Binary op requires property values".to_string())),
        };

        let result = match op {
            BinaryOp::Eq => PropertyValue::Boolean(left_prop == right_prop),
            BinaryOp::Ne => PropertyValue::Boolean(left_prop != right_prop),
            BinaryOp::Lt => self.compare_lt(&left_prop, &right_prop)?,
            BinaryOp::Le => self.compare_le(&left_prop, &right_prop)?,
            BinaryOp::Gt => self.compare_gt(&left_prop, &right_prop)?,
            BinaryOp::Ge => self.compare_ge(&left_prop, &right_prop)?,
            BinaryOp::And => self.logical_and(&left_prop, &right_prop)?,
            BinaryOp::Or => self.logical_or(&left_prop, &right_prop)?,
            _ => return Err(ExecutionError::RuntimeError(format!("Unsupported operator: {:?}", op))),
        };

        Ok(Value::Property(result))
    }

    fn compare_lt(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Boolean(l < r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l < r)),
            (PropertyValue::String(l), PropertyValue::String(r)) => Ok(PropertyValue::Boolean(l < r)),
            _ => Err(ExecutionError::TypeError("Cannot compare these types".to_string())),
        }
    }

    fn compare_le(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Boolean(l <= r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l <= r)),
            (PropertyValue::String(l), PropertyValue::String(r)) => Ok(PropertyValue::Boolean(l <= r)),
            _ => Err(ExecutionError::TypeError("Cannot compare these types".to_string())),
        }
    }

    fn compare_gt(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Boolean(l > r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l > r)),
            (PropertyValue::String(l), PropertyValue::String(r)) => Ok(PropertyValue::Boolean(l > r)),
            _ => Err(ExecutionError::TypeError("Cannot compare these types".to_string())),
        }
    }

    fn compare_ge(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Boolean(l >= r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l >= r)),
            (PropertyValue::String(l), PropertyValue::String(r)) => Ok(PropertyValue::Boolean(l >= r)),
            _ => Err(ExecutionError::TypeError("Cannot compare these types".to_string())),
        }
    }

    fn logical_and(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => Ok(PropertyValue::Boolean(*l && *r)),
            _ => Err(ExecutionError::TypeError("AND requires boolean operands".to_string())),
        }
    }

    fn logical_or(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => Ok(PropertyValue::Boolean(*l || *r)),
            _ => Err(ExecutionError::TypeError("OR requires boolean operands".to_string())),
        }
    }

    fn evaluate_function(&self, name: &str, _args: &[Expression], _record: &Record, _store: &GraphStore) -> ExecutionResult<Value> {
        match name.to_lowercase().as_str() {
            "count" => {
                // Simple count - just return 1 for now (should be aggregated)
                Ok(Value::Property(PropertyValue::Integer(1)))
            }
            _ => Err(ExecutionError::RuntimeError(format!("Unknown function: {}", name))),
        }
    }
}

impl PhysicalOperator for FilterOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next(store)? {
            if self.evaluate_predicate(&record, store)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.input.reset();
    }
}

/// Expand operator: -[:KNOWS]->
pub struct ExpandOperator {
    /// Input operator
    input: OperatorBox,
    /// Source variable
    source_var: String,
    /// Target variable
    target_var: String,
    /// Edge variable (optional)
    edge_var: Option<String>,
    /// Edge types to expand (empty = all types)
    edge_types: Vec<String>,
    /// Direction
    direction: Direction,
    /// Current input record
    current_record: Option<Record>,
    /// Current edges being iterated
    current_edges: Vec<(crate::graph::EdgeId, crate::graph::Edge)>,
    /// Current edge index
    edge_index: usize,
}

impl ExpandOperator {
    /// Create a new expand operator
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        edge_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            edge_var,
            edge_types,
            direction,
            current_record: None,
            current_edges: Vec::new(),
            edge_index: 0,
        }
    }

    fn load_edges(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
        let source_val = record.get(&self.source_var)
            .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.clone()))?;

        let (node_id, _) = source_val.as_node()
            .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", self.source_var)))?;

        // Get edges based on direction
        let edges = match self.direction {
            Direction::Outgoing => store.get_outgoing_edges(node_id),
            Direction::Incoming => store.get_incoming_edges(node_id),
            Direction::Both => {
                let mut all = store.get_outgoing_edges(node_id);
                all.extend(store.get_incoming_edges(node_id));
                all
            }
        };

        // Filter by edge type if specified
        self.current_edges = if self.edge_types.is_empty() {
            edges.into_iter().map(|e| (e.id, e.clone())).collect()
        } else {
            edges.into_iter()
                .filter(|e| self.edge_types.iter().any(|t| e.edge_type.as_str() == t))
                .map(|e| (e.id, e.clone()))
                .collect()
        };

        self.edge_index = 0;
        Ok(())
    }
}

impl PhysicalOperator for ExpandOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            // If we have edges from current record, return them
            if self.edge_index < self.current_edges.len() {
                let (edge_id, edge) = &self.current_edges[self.edge_index];
                self.edge_index += 1;

                let mut new_record = self.current_record.as_ref().unwrap().clone();

                // Determine target node based on direction
                let target_id = match self.direction {
                    Direction::Outgoing => edge.target,
                    Direction::Incoming => edge.source,
                    Direction::Both => {
                        // For both, target is the "other" node
                        let source_val = new_record.get(&self.source_var).unwrap();
                        let (source_id, _) = source_val.as_node().unwrap();
                        if edge.source == source_id {
                            edge.target
                        } else {
                            edge.source
                        }
                    }
                };

                let target_node = store.get_node(target_id)
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Target node {:?} not found", target_id)))?;

                new_record.bind(self.target_var.clone(), Value::Node(target_id, target_node.clone()));

                if let Some(edge_var) = &self.edge_var {
                    new_record.bind(edge_var.clone(), Value::Edge(*edge_id, edge.clone()));
                }

                return Ok(Some(new_record));
            }

            // Need new input record
            if let Some(record) = self.input.next(store)? {
                self.current_record = Some(record.clone());
                self.load_edges(&record, store)?;
            } else {
                return Ok(None);
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.current_record = None;
        self.current_edges.clear();
        self.edge_index = 0;
    }
}

/// Project operator: RETURN n.name, n.age
pub struct ProjectOperator {
    /// Input operator
    input: OperatorBox,
    /// Expressions to project
    projections: Vec<(Expression, String)>, // (expr, alias)
}

impl ProjectOperator {
    /// Create a new project operator
    pub fn new(input: OperatorBox, projections: Vec<(Expression, String)>) -> Self {
        Self { input, projections }
    }

    fn evaluate_expression(&self, expr: &Expression, record: &Record, _store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => {
                record.get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Property { variable, property } => {
                let val = record.get(variable)
                    .ok_or_else(|| ExecutionError::VariableNotFound(variable.clone()))?;

                match val {
                    Value::Node(_, node) => {
                        let prop = node.get_property(property)
                            .cloned()
                            .unwrap_or(PropertyValue::Null);
                        Ok(Value::Property(prop))
                    }
                    Value::Edge(_, edge) => {
                        let prop = edge.get_property(property)
                            .cloned()
                            .unwrap_or(PropertyValue::Null);
                        Ok(Value::Property(prop))
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            _ => Err(ExecutionError::RuntimeError("Unsupported projection expression".to_string())),
        }
    }
}

impl PhysicalOperator for ProjectOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next(store)? {
            let mut new_record = Record::new();

            for (expr, alias) in &self.projections {
                let value = self.evaluate_expression(expr, &record, store)?;
                new_record.bind(alias.clone(), value);
            }

            Ok(Some(new_record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }
}

/// Limit operator: LIMIT 10
pub struct LimitOperator {
    /// Input operator
    input: OperatorBox,
    /// Maximum number of records
    limit: usize,
    /// Current count
    count: usize,
}

impl LimitOperator {
    /// Create a new limit operator
    pub fn new(input: OperatorBox, limit: usize) -> Self {
        Self { input, limit, count: 0 }
    }
}

impl PhysicalOperator for LimitOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        if let Some(record) = self.input.next(store)? {
            self.count += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.count = 0;
    }
}

/// Create node operator: CREATE (n:Person {name: "Alice"})
pub struct CreateNodeOperator {
    /// Nodes to create (label, properties, variable)
    nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)>,
    /// Created node IDs (for returning)
    created_nodes: Vec<(NodeId, Option<String>)>,
    /// Current index for iteration
    current: usize,
    /// Whether creation has been executed
    executed: bool,
}

impl CreateNodeOperator {
    /// Create a new CreateNodeOperator
    pub fn new(nodes: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)>) -> Self {
        Self {
            nodes_to_create: nodes,
            created_nodes: Vec::new(),
            current: 0,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateNodeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        // Read-only version cannot create nodes
        Err(ExecutionError::RuntimeError(
            "CreateNodeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore) -> ExecutionResult<Option<Record>> {
        // First call: create all nodes
        if !self.executed {
            for (labels, properties, variable) in &self.nodes_to_create {
                // Use first label as primary, or empty string if none
                let primary_label = labels.first()
                    .map(|l| l.clone())
                    .unwrap_or_else(|| Label::new(""));

                let node_id = store.create_node(primary_label);

                // Set properties on the created node
                if let Some(node) = store.get_node_mut(node_id) {
                    // Add additional labels
                    for label in labels.iter().skip(1) {
                        node.add_label(label.clone());
                    }
                    // Set properties
                    for (key, value) in properties {
                        node.properties.set(key.clone(), value.clone());
                    }
                }

                self.created_nodes.push((node_id, variable.clone()));
            }
            self.executed = true;
        }

        // Return created nodes one by one
        if self.current >= self.created_nodes.len() {
            return Ok(None);
        }

        let (node_id, variable) = &self.created_nodes[self.current];
        self.current += 1;

        let node = store.get_node(*node_id)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("Created node {:?} not found", node_id)))?;

        let mut record = Record::new();
        if let Some(var) = variable {
            record.bind(var.clone(), Value::Node(*node_id, node.clone()));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
        // Note: We don't reset executed flag - nodes are already created
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create edge operator: CREATE (a)-[:KNOWS]->(b)
pub struct CreateEdgeOperator {
    /// Input operator (provides source/target nodes from MATCH)
    input: Option<OperatorBox>,
    /// Edge pattern to create: (source_var, target_var, edge_type, properties, edge_var)
    edge_pattern: (String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>),
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, Option<String>)>,
    /// Current index
    current: usize,
    /// Whether we've processed input
    processed: bool,
}

impl CreateEdgeOperator {
    /// Create a new CreateEdgeOperator
    pub fn new(
        input: Option<OperatorBox>,
        source_var: String,
        target_var: String,
        edge_type: EdgeType,
        properties: HashMap<String, PropertyValue>,
        edge_var: Option<String>,
    ) -> Self {
        Self {
            input,
            edge_pattern: (source_var, target_var, edge_type, properties, edge_var),
            created_edges: Vec::new(),
            current: 0,
            processed: false,
        }
    }
}

impl PhysicalOperator for CreateEdgeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateEdgeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore) -> ExecutionResult<Option<Record>> {
        let (source_var, target_var, edge_type, properties, edge_var) = &self.edge_pattern;

        // Process input records and create edges
        if !self.processed {
            if let Some(ref mut input) = self.input {
                // Create edge for each input record
                while let Some(record) = input.next_mut(store)? {
                    let source_val = record.get(source_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                    let target_val = record.get(target_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                    let (source_id, _) = source_val.as_node()
                        .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", source_var)))?;
                    let (target_id, _) = target_val.as_node()
                        .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", target_var)))?;

                    let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set properties on edge
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        for (key, value) in properties {
                            edge.properties.set(key.clone(), value.clone());
                        }
                    }

                    self.created_edges.push((edge_id, edge_var.clone()));
                }
            }
            self.processed = true;
        }

        // Return created edges one by one
        if self.current >= self.created_edges.len() {
            return Ok(None);
        }

        let (edge_id, variable) = &self.created_edges[self.current];
        self.current += 1;

        let edge = store.get_edge(*edge_id)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("Created edge {:?} not found", edge_id)))?;

        let mut record = Record::new();
        if let Some(var) = variable {
            record.bind(var.clone(), Value::Edge(*edge_id, edge.clone()));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        if let Some(ref mut input) = self.input {
            input.reset();
        }
        self.current = 0;
        self.processed = false;
        self.created_edges.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Label;

    #[test]
    fn test_node_scan_operator() {
        let mut store = GraphStore::new();
        let _alice = store.create_node("Person");
        let _bob = store.create_node("Person");

        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);

        let mut count = 0;
        while let Ok(Some(_record)) = op.next(&store) {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_filter_operator() {
        let mut store = GraphStore::new();
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("age", 25i64);
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let predicate = Expression::Binary {
            left: Box::new(Expression::Property {
                variable: "n".to_string(),
                property: "age".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expression::Literal(PropertyValue::Integer(28))),
        };

        let mut filter = FilterOperator::new(Box::new(scan), predicate);

        let mut count = 0;
        while let Ok(Some(_record)) = filter.next(&store) {
            count += 1;
        }

        assert_eq!(count, 1); // Only Alice (age 30) passes the filter
    }

    #[test]
    fn test_limit_operator() {
        let mut store = GraphStore::new();
        for _ in 0..10 {
            store.create_node("Person");
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let mut limit = LimitOperator::new(Box::new(scan), 3);

        let mut count = 0;
        while let Ok(Some(_record)) = limit.next(&store) {
            count += 1;
        }

        assert_eq!(count, 3);
    }
}
