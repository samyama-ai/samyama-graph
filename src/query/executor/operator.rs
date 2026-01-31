//! Physical operators for query execution (Volcano iterator model)
//!
//! Implements ADR-007 (Volcano Iterator Model)

use crate::graph::{GraphStore, Label, NodeId, EdgeType};
use crate::query::ast::{Expression, BinaryOp, Direction};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::graph::PropertyValue;
use std::collections::{HashMap, HashSet};
use samyama_optimization::common::{Problem, SolverConfig, MultiObjectiveProblem};
use samyama_optimization::algorithms::{JayaSolver, RaoSolver, RaoVariant, TLBOSolver, FireflySolver, CuckooSolver, GWOSolver, GASolver, SASolver, BatSolver, ABCSolver, GSASolver, NSGA2Solver, MOTLBOSolver, HSSolver, FPASolver};
use ndarray::Array1;

/// Optimization problem wrapper for GraphStore
struct GraphOptimizationProblem {
    /// Static cost coefficients (e.g. price per unit) for single objective
    costs: Vec<f64>,
    /// Multiple cost coefficient vectors for multi-objective
    multi_costs: Vec<Vec<f64>>,
    /// Budget constraint (optional)
    budget: Option<f64>,
    /// Minimum total sum constraint (optional)
    min_total: Option<f64>,
    /// Dimensions
    dim: usize,
    /// Bounds
    lower: f64,
    upper: f64,
}

impl Problem for GraphOptimizationProblem {
    fn dim(&self) -> usize {
        self.dim
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper)
        )
    }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        // Minimize sum(variable * cost)
        let mut sum = 0.0;
        for i in 0..self.dim {
            sum += variables[i] * self.costs[i];
        }
        sum
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        let mut penalty = 0.0;
        
        // 1. Budget Constraint: sum(x * cost) <= budget
        if let Some(budget) = self.budget {
            let mut total_cost = 0.0;
            for i in 0..self.dim {
                total_cost += variables[i] * self.costs[i];
            }
            if total_cost > budget {
                penalty += (total_cost - budget).powi(2);
            }
        }

        // 2. Minimum Total Constraint: sum(x) >= min_total
        if let Some(min_total) = self.min_total {
            let mut total_val = 0.0;
            for i in 0..self.dim {
                total_val += variables[i];
            }
            if total_val < min_total {
                penalty += (min_total - total_val).powi(2) * 100.0; // High weight for feasibility
            }
        }

        penalty
    }
}

impl MultiObjectiveProblem for GraphOptimizationProblem {
    fn num_objectives(&self) -> usize {
        self.multi_costs.len()
    }

    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        let mut results = Vec::with_capacity(self.multi_costs.len());
        for costs in &self.multi_costs {
            let mut sum = 0.0;
            for i in 0..self.dim {
                sum += variables[i] * costs[i];
            }
            results.push(sum);
        }
        results
    }

    fn dim(&self) -> usize { self.dim }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper)
        )
    }
}

/// Physical operator trait - all operators implement this
pub trait PhysicalOperator: Send {
    /// Get the next record from this operator (read-only operations)
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>>;

    /// Get the next record from this operator (write operations that mutate the store)
    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
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

/// Aggregation type
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Aggregation function definition
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub func: AggregateType,
    pub expr: Expression,
    pub alias: String,
}

/// Internal state for an aggregator
#[derive(Debug, Clone)]
enum AggregatorState {
    Count(i64),
    Sum(f64),
    Avg { sum: f64, count: i64 },
    Min(Option<PropertyValue>),
    Max(Option<PropertyValue>),
}

impl AggregatorState {
    fn new(func: &AggregateType) -> Self {
        match func {
            AggregateType::Count => AggregatorState::Count(0),
            AggregateType::Sum => AggregatorState::Sum(0.0),
            AggregateType::Avg => AggregatorState::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregatorState::Min(None),
            AggregateType::Max => AggregatorState::Max(None),
        }
    }

    fn update(&mut self, value: &Value) {
        match self {
            AggregatorState::Count(c) => {
                if !value.is_null() {
                    *c += 1;
                }
            }
            AggregatorState::Sum(s) => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() { *s += f; }
                    else if let Some(i) = prop.as_integer() { *s += i as f64; }
                }
            }
            AggregatorState::Avg { sum, count } => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() { *sum += f; *count += 1; }
                    else if let Some(i) = prop.as_integer() { *sum += i as f64; *count += 1; }
                }
            }
            AggregatorState::Min(curr) => {
                if let Some(prop) = value.as_property() {
                    if curr.is_none() || prop < curr.as_ref().unwrap() {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Max(curr) => {
                if let Some(prop) = value.as_property() {
                    if curr.is_none() || prop > curr.as_ref().unwrap() {
                        *curr = Some(prop.clone());
                    }
                }
            }
        }
    }

    fn result(&self) -> Value {
        match self {
            AggregatorState::Count(c) => Value::Property(PropertyValue::Integer(*c)),
            AggregatorState::Sum(s) => Value::Property(PropertyValue::Float(*s)),
            AggregatorState::Avg { sum, count } => {
                if *count == 0 { Value::Null }
                else { Value::Property(PropertyValue::Float(*sum / *count as f64)) }
            }
            AggregatorState::Min(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
            AggregatorState::Max(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
        }
    }
}

/// Aggregate operator: GROUP BY + Aggregations
pub struct AggregateOperator {
    input: OperatorBox,
    group_by: Vec<(Expression, String)>, // (expr, alias)
    aggregates: Vec<AggregateFunction>,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl AggregateOperator {
    pub fn new(
        input: OperatorBox, 
        group_by: Vec<(Expression, String)>, 
        aggregates: Vec<AggregateFunction>
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }

    fn evaluate_expression(expr: &Expression, record: &Record) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => {
                Ok(record.get(var).cloned().unwrap_or(Value::Null))
            }
            Expression::Property { variable, property } => {
                let val = record.get(variable).cloned().unwrap_or(Value::Null);
                match val {
                    Value::Node(_, node) => Ok(Value::Property(node.get_property(property).cloned().unwrap_or(PropertyValue::Null))),
                    Value::Edge(_, edge) => Ok(Value::Property(edge.get_property(property).cloned().unwrap_or(PropertyValue::Null))),
                    _ => Ok(Value::Null),
                }
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            _ => Err(ExecutionError::RuntimeError("Unsupported expression in aggregation".to_string())),
        }
    }
}

impl PhysicalOperator for AggregateOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();
            
            // 1. Consume all input
            while let Some(record) = self.input.next(store)? {
                // Evaluate grouping keys
                let mut key = Vec::new();
                for (expr, _) in &self.group_by {
                    key.push(Self::evaluate_expression(expr, &record)?);
                }

                // Initialize state if new group
                let states = groups.entry(key).or_insert_with(|| {
                    self.aggregates.iter().map(|agg| AggregatorState::new(&agg.func)).collect()
                });

                // Update state
                for (i, agg) in self.aggregates.iter().enumerate() {
                    let val = Self::evaluate_expression(&agg.expr, &record)?;
                    states[i].update(&val);
                }
            }

            // 2. Generate results
            let mut output_records = Vec::new();
            for (key, states) in groups {
                let mut record = Record::new();
                
                // Add grouping keys
                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }

                // Add aggregation results
                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }
                
                output_records.push(record);
            }

            self.results = output_records.into_iter();
            self.executed = true;
        }

        Ok(self.results.next())
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
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

/// Sort operator: ORDER BY n.age ASC
pub struct SortOperator {
    input: OperatorBox,
    sort_items: Vec<(Expression, bool)>, // (expr, ascending)
    records: Vec<Record>,
    current: usize,
    executed: bool,
}

impl SortOperator {
    pub fn new(input: OperatorBox, sort_items: Vec<(Expression, bool)>) -> Self {
        Self {
            input,
            sort_items,
            records: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn evaluate_expression(expr: &Expression, record: &Record) -> ExecutionResult<Value> {
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
            _ => Err(ExecutionError::RuntimeError("Unsupported sort expression".to_string())),
        }
    }
}

impl PhysicalOperator for SortOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            // Materialize all records
            while let Some(record) = self.input.next(store)? {
                self.records.push(record);
            }

            // Sort
            // We need to handle errors during sort comparison which is tricky with sort_by
            // So we'll unwrap/panic or use a custom sort that pre-calculates keys?
            // For simplicity, we assume evaluation succeeds or panic (not ideal but ok for prototype)
            // Better: use sort_by with a closure that captures errors? sort_by expects Ordering.
            
            let sort_items = &self.sort_items;
            self.records.sort_by(|a, b| {
                for (expr, ascending) in sort_items {
                    // Evaluate expr for a and b
                    // TODO: Handle evaluation errors gracefully?
                    let val_a = Self::evaluate_expression(expr, a).unwrap_or(Value::Null);
                    let val_b = Self::evaluate_expression(expr, b).unwrap_or(Value::Null);

                    // Extract PropertyValue
                    let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                    let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);

                    let ord = prop_a.cmp(prop_b);
                    if ord != std::cmp::Ordering::Equal {
                        return if *ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });

            self.executed = true;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.records.clear();
        self.current = 0;
        self.executed = false;
    }
}

/// Index scan operator: MATCH (n:Person) WHERE n.id = 1
pub struct IndexScanOperator {
    variable: String,
    label: Label,
    property: String,
    op: BinaryOp,
    value: PropertyValue,
    node_ids: Vec<NodeId>,
    current: usize,
}

impl IndexScanOperator {
    pub fn new(variable: String, label: Label, property: String, op: BinaryOp, value: PropertyValue) -> Self {
        Self {
            variable,
            label,
            property,
            op,
            value,
            node_ids: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) {
        if !self.node_ids.is_empty() {
            return;
        }

        if let Some(index_lock) = store.property_index.get_index(&self.label, &self.property) {
            let index = index_lock.read().unwrap();
            self.node_ids = match self.op {
                BinaryOp::Eq => index.get(&self.value),
                BinaryOp::Gt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Excluded(self.value.clone()), Unbounded))
                },
                BinaryOp::Ge => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Included(self.value.clone()), Unbounded))
                },
                BinaryOp::Lt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Excluded(self.value.clone())))
                },
                BinaryOp::Le => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Included(self.value.clone())))
                },
                _ => Vec::new(),
            };
        }
    }
}

impl PhysicalOperator for IndexScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        while self.current < self.node_ids.len() {
            let node_id = self.node_ids[self.current];
            self.current += 1;

            if let Some(node) = store.get_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::Node(node_id, node.clone()));
                return Ok(Some(record));
            }
        }
        
        Ok(None)
    }

    fn reset(&mut self) {
        self.current = 0;
    }
}

/// Vector search operator: CALL db.index.vector.queryNodes(...)
pub struct VectorSearchOperator {
    /// Label to search in
    label: String,
    /// Property key to search in
    property_key: String,
    /// Query vector
    query_vector: Vec<f32>,
    /// Number of neighbors to return
    k: usize,
    /// Variable name for matched nodes
    node_var: String,
    /// Variable name for similarity scores (optional)
    score_var: Option<String>,
    /// Search results
    results: Vec<(NodeId, f32)>,
    /// Current index in results
    current: usize,
}

impl VectorSearchOperator {
    pub fn new(
        label: String,
        property_key: String,
        query_vector: Vec<f32>,
        k: usize,
        node_var: String,
        score_var: Option<String>,
    ) -> Self {
        Self {
            label,
            property_key,
            query_vector,
            k,
            node_var,
            score_var,
            results: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if !self.results.is_empty() || self.current > 0 {
            return Ok(());
        }

        self.results = store.vector_search(
            &self.label,
            &self.property_key,
            &self.query_vector,
            self.k,
        ).map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        Ok(())
    }
}

impl PhysicalOperator for VectorSearchOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store)?;

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let (node_id, score) = &self.results[self.current];
        self.current += 1;

        let node = store.get_node(*node_id)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("Node {:?} not found", node_id)))?;

        let mut record = Record::new();
        record.bind(self.node_var.clone(), Value::Node(*node_id, node.clone()));
        
        if let Some(score_var) = &self.score_var {
            record.bind(score_var.clone(), Value::Property(PropertyValue::Float(*score as f64)));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
    }
}

/// Cartesian product operator: MATCH (a:X), (b:Y)
/// Produces all combinations of records from left and right inputs
pub struct CartesianProductOperator {
    left: OperatorBox,
    right: OperatorBox,
    left_records: Vec<Record>,
    left_index: usize,
    current_right: Option<Record>,
    left_materialized: bool,
}

impl CartesianProductOperator {
    pub fn new(left: OperatorBox, right: OperatorBox) -> Self {
        Self {
            left,
            right,
            left_records: Vec::new(),
            left_index: 0,
            current_right: None,
            left_materialized: false,
        }
    }

    fn materialize_left(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.left_materialized {
            return Ok(());
        }
        while let Some(record) = self.left.next(store)? {
            self.left_records.push(record);
        }
        self.left_materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for CartesianProductOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize_left(store)?;
        if self.left_records.is_empty() {
            return Ok(None);
        }
        loop {
            if self.current_right.is_none() {
                self.current_right = self.right.next(store)?;
                self.left_index = 0;
                if self.current_right.is_none() {
                    return Ok(None);
                }
            }
            if self.left_index < self.left_records.len() {
                let left_record = &self.left_records[self.left_index];
                let right_record = self.current_right.as_ref().unwrap();
                let mut merged = left_record.clone();
                for (key, value) in right_record.bindings() {
                    merged.bind(key.clone(), value.clone());
                }
                self.left_index += 1;
                return Ok(Some(merged));
            } else {
                self.current_right = None;
            }
        }
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.left_index = 0;
        self.current_right = None;
        self.left_materialized = false;
    }
}

/// Join operator: Joins two inputs on a shared variable
pub struct JoinOperator {
    left: OperatorBox,
    right: OperatorBox,
    join_var: String,
    left_records: HashMap<Value, Vec<Record>>,
    right_records: Vec<Record>,
    current_right_index: usize,
    current_left_list_index: usize,
    materialized: bool,
}

impl JoinOperator {
    pub fn new(left: OperatorBox, right: OperatorBox, join_var: String) -> Self {
        Self {
            left,
            right,
            join_var,
            left_records: HashMap::new(),
            right_records: Vec::new(),
            current_right_index: 0,
            current_left_list_index: 0,
            materialized: false,
        }
    }

    fn materialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.materialized {
            return Ok(());
        }

        // Materialize left into a hash map
        while let Some(record) = self.left.next(store)? {
            if let Some(val) = record.get(&self.join_var) {
                self.left_records.entry(val.clone()).or_default().push(record);
            }
        }

        // Materialize right into a list
        while let Some(record) = self.right.next(store)? {
            self.right_records.push(record);
        }

        self.materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for JoinOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize(store)?;

        while self.current_right_index < self.right_records.len() {
            let right_record = &self.right_records[self.current_right_index];
            if let Some(join_val) = right_record.get(&self.join_var) {
                if let Some(left_list) = self.left_records.get(join_val) {
                    if self.current_left_list_index < left_list.len() {
                        let left_record = &left_list[self.current_left_list_index];
                        self.current_left_list_index += 1;

                        // Merge records
                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }
                        return Ok(Some(merged));
                    }
                }
            }
            
            // Move to next right record
            self.current_right_index += 1;
            self.current_left_list_index = 0;
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.right_records.clear();
        self.current_right_index = 0;
        self.current_left_list_index = 0;
        self.materialized = false;
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

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // First call: create all nodes
        if !self.executed {
            for (labels, properties, variable) in &self.nodes_to_create {
                // Use first label as primary, or empty string if none
                let primary_label = labels.first()
                    .map(|l| l.clone())
                    .unwrap_or_else(|| Label::new(""));

                let node_id = store.create_node(primary_label);

                // Add additional labels
                for label in labels.iter().skip(1) {
                    let _ = store.add_label_to_node(tenant_id, node_id, label.clone());
                }

                // Set properties using store.set_node_property to trigger indexing
                for (key, value) in properties {
                    let _ = store.set_node_property(tenant_id, node_id, key.clone(), value.clone());
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

/// Create property index operator: CREATE INDEX ON :Person(id)
pub struct CreateIndexOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl CreateIndexOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self { label, property, executed: false }
    }
}

impl PhysicalOperator for CreateIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateIndexOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        store.property_index.create_index(self.label.clone(), self.property.clone());

        // Backfill index
        // Since we have mutable access to store, we can get nodes
        // But we need to avoid borrowing store while mutating property_index if we accessed it differently
        // Here we use get_nodes_by_label which borrows store.
        // property_index is inside store. 
        // IndexManager uses RwLock internally so it handles its own mutability.
        
        // We collect entries to release the borrow on nodes
        let mut entries = Vec::new();
        let nodes = store.get_nodes_by_label(&self.label);
        
        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                entries.push((node.id, val.clone()));
            }
        }
        
        for (node_id, val) in entries {
            store.property_index.index_insert(&self.label, &self.property, val, node_id);
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create vector index operator: CREATE VECTOR INDEX ...
pub struct CreateVectorIndexOperator {
    label: Label,
    property_key: String,
    dimensions: usize,
    similarity: String,
    executed: bool,
}

impl CreateVectorIndexOperator {
    pub fn new(label: Label, property_key: String, dimensions: usize, similarity: String) -> Self {
        Self {
            label,
            property_key,
            dimensions,
            similarity,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateVectorIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateVectorIndexOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        let metric = match self.similarity.to_lowercase().as_str() {
            "cosine" => crate::vector::DistanceMetric::Cosine,
            "l2" => crate::vector::DistanceMetric::L2,
            _ => return Err(ExecutionError::RuntimeError(format!("Unsupported similarity metric: {}", self.similarity))),
        };

        store.create_vector_index(self.label.as_str(), &self.property_key, self.dimensions, metric)
            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        self.executed = true;
        
        // Return an empty record or a success record
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
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

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        let (source_var, target_var, edge_type, properties, edge_var) = &self.edge_pattern;

        // Process input records and create edges
        if !self.processed {
            if let Some(ref mut input) = self.input {
                // Create edge for each input record
                while let Some(record) = input.next_mut(store, tenant_id)? {
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

                    // Set properties on edge using Edge's set_property method
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        for (key, value) in properties {
                            edge.set_property(key.clone(), value.clone());
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

/// Combined operator for CREATE patterns with both nodes and edges
/// Example: CREATE (a:Person)-[:KNOWS]->(b:Person)
/// This operator first creates all nodes, then creates edges between them
pub struct CreateNodesAndEdgesOperator {
    /// Node creation operator
    node_operator: OperatorBox,
    /// Edges to create: (source_var, target_var, edge_type, properties, edge_var)
    edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    /// Variable to NodeId mapping (built during node creation)
    var_to_node_id: HashMap<String, NodeId>,
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, crate::graph::Edge, Option<String>)>,
    /// Current phase: 0 = creating nodes, 1 = creating edges, 2 = returning results
    phase: usize,
    /// Current index for returning results
    result_index: usize,
    /// All results to return (nodes first, then edges)
    results: Vec<(Option<String>, Value)>,
}

impl CreateNodesAndEdgesOperator {
    /// Create a new CreateNodesAndEdgesOperator
    pub fn new(
        node_operator: OperatorBox,
        edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    ) -> Self {
        Self {
            node_operator,
            edges_to_create,
            var_to_node_id: HashMap::new(),
            created_edges: Vec::new(),
            phase: 0,
            result_index: 0,
            results: Vec::new(),
        }
    }
}

impl PhysicalOperator for CreateNodesAndEdgesOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateNodesAndEdgesOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // Phase 0: Create all nodes and collect their IDs
        if self.phase == 0 {
            while let Some(record) = self.node_operator.next_mut(store, tenant_id)? {
                // Extract variable and node from record
                for (var, value) in record.bindings().iter() {
                    if let Value::Node(node_id, node) = value {
                        self.var_to_node_id.insert(var.clone(), *node_id);
                        self.results.push((Some(var.clone()), Value::Node(*node_id, node.clone())));
                    }
                }
            }
            self.phase = 1;
        }

        // Phase 1: Create all edges
        if self.phase == 1 {
            for (source_var, target_var, edge_type, properties, edge_var) in &self.edges_to_create {
                let source_id = self.var_to_node_id.get(source_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                let target_id = self.var_to_node_id.get(target_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                let edge_id = store.create_edge(*source_id, *target_id, edge_type.clone())
                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                // Set properties on edge
                if let Some(edge) = store.get_edge_mut(edge_id) {
                    for (key, value) in properties {
                        edge.set_property(key.clone(), value.clone());
                    }
                }

                // Get the created edge for returning
                if let Some(edge) = store.get_edge(edge_id) {
                    self.created_edges.push((edge_id, edge.clone(), edge_var.clone()));
                    if edge_var.is_some() {
                        self.results.push((edge_var.clone(), Value::Edge(edge_id, edge.clone())));
                    }
                }
            }
            self.phase = 2;
        }

        // Phase 2: Return results one by one
        if self.result_index >= self.results.len() {
            return Ok(None);
        }

        let (var, value) = &self.results[self.result_index];
        self.result_index += 1;

        let mut record = Record::new();
        if let Some(v) = var {
            record.bind(v.clone(), value.clone());
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.node_operator.reset();
        self.var_to_node_id.clear();
        self.created_edges.clear();
        self.phase = 0;
        self.result_index = 0;
        self.results.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Operator for MATCH...CREATE queries
/// Example: MATCH (a:Trial {id: 'NCT001'}), (b:Condition {mesh_id: 'D001'}) CREATE (a)-[:STUDIES]->(b)
/// This operator takes matched nodes and creates edges between them
pub struct MatchCreateEdgeOperator {
    /// Input operator (MATCH results)
    input: OperatorBox,
    /// Edges to create: (source_var, target_var, edge_type, properties, edge_var)
    edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    /// Whether edges have been created for current batch
    done: bool,
    /// Results to return
    results: Vec<Record>,
    /// Current result index
    result_index: usize,
}

impl MatchCreateEdgeOperator {
    /// Create a new MatchCreateEdgeOperator
    pub fn new(
        input: OperatorBox,
        edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    ) -> Self {
        Self {
            input,
            edges_to_create,
            done: false,
            results: Vec::new(),
            result_index: 0,
        }
    }
}

impl PhysicalOperator for MatchCreateEdgeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "MatchCreateEdgeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // First pass: process all matched records and create edges
        if !self.done {
            while let Some(record) = self.input.next_mut(store, tenant_id)? {
                // For each matched record, create the specified edges
                for (source_var, target_var, edge_type, properties, _edge_var) in &self.edges_to_create {
                    // Get source node ID from record bindings
                    let source_id = match record.get(source_var) {
                        Some(Value::Node(id, _)) => *id,
                        _ => continue, // Skip if source not found
                    };

                    // Get target node ID from record bindings
                    let target_id = match record.get(target_var) {
                        Some(Value::Node(id, _)) => *id,
                        _ => continue, // Skip if target not found
                    };

                    // Create the edge
                    let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set properties on edge
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        for (key, value) in properties {
                            edge.set_property(key.clone(), value.clone());
                        }
                    }

                    // Build result record with the created edge
                    let mut result_record = record.clone();
                    if let Some(edge) = store.get_edge(edge_id) {
                        result_record.bind("_edge".to_string(), Value::Edge(edge_id, edge.clone()));
                    }
                    self.results.push(result_record);
                }
            }
            self.done = true;
        }

        // Return results one by one
        if self.result_index >= self.results.len() {
            return Ok(None);
        }

        let result = self.results[self.result_index].clone();
        self.result_index += 1;
        Ok(Some(result))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.done = false;
        self.results.clear();
        self.result_index = 0;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Algorithm operator: CALL algo.pageRank(...)
pub struct AlgorithmOperator {
    /// Procedure name
    name: String,
    /// Arguments
    args: Vec<crate::query::ast::Expression>,
    /// Result records
    results: Vec<Record>,
    /// Current index
    current: usize,
    /// Whether algorithm has run
    executed: bool,
}

impl AlgorithmOperator {
    pub fn new(name: String, args: Vec<crate::query::ast::Expression>) -> Self {
        Self {
            name,
            args,
            results: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn execute_pagerank(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?, config_map?)
        let mut label = None;
        let mut edge_type = None;
        let mut config = crate::algo::PageRankConfig::default();

        if self.args.len() > 0 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }
        
        // Parse optional config map
        for arg in &self.args {
            if let Expression::Literal(PropertyValue::Map(m)) = arg {
                if let Some(PropertyValue::Integer(i)) = m.get("iterations") {
                    config.iterations = *i as usize;
                }
                if let Some(PropertyValue::Float(f)) = m.get("damping") {
                    config.damping_factor = *f;
                }
            }
        }

        // Build view and run
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let scores = crate::algo::page_rank(&view, config);

        // Convert to records
        for (algo_id, score) in scores {
            let node_id = NodeId::new(algo_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(node_id) {
                record.bind("node".to_string(), Value::Node(node_id, node.clone()));
                record.bind("score".to_string(), Value::Property(PropertyValue::Float(score)));
                self.results.push(record);
            }
        }
        
        // Sort by score descending
        self.results.sort_by(|a, b| {
            let score_a = a.get("score").unwrap().as_property().unwrap().as_float().unwrap();
            let score_b = b.get("score").unwrap().as_property().unwrap().as_float().unwrap();
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(())
    }

    fn execute_shortest_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node, target_node, config?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError("shortestPath requires source and target".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Target must be integer ID".to_string())),
        };

        let mut weight_prop = None;
        if self.args.len() > 2 {
            if let Expression::Literal(PropertyValue::Map(m)) = &self.args[2] {
                if let Some(PropertyValue::String(s)) = m.get("weight_property") {
                    weight_prop = Some(s.clone());
                }
            }
        }
        
        // Build view
        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());
        
        // Run Algorithm
        let result = if weight_prop.is_some() {
            crate::algo::dijkstra(&view, source_id, target_id)
        } else {
            crate::algo::bfs(&view, source_id, target_id)
        };

        if let Some(result) = result {
             let mut record = Record::new();
             record.bind("cost".to_string(), Value::Property(PropertyValue::Float(result.cost)));
             
             // Construct path list
             let mut path_nodes = Vec::new();
             for nid_u64 in result.path {
                 path_nodes.push(PropertyValue::Integer(nid_u64 as i64));
             }
             record.bind("path".to_string(), Value::Property(PropertyValue::Array(path_nodes)));
             
             self.results.push(record);
        }

        Ok(())
    }

    fn execute_wcc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?)
        let mut label = None;
        let mut edge_type = None;

        if self.args.len() > 0 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }

        // Build view and run WCC
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let result = crate::algo::weakly_connected_components(&view);

        // Convert to records
        // For WCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, node.clone()));
                record.bind("componentId".to_string(), Value::Property(PropertyValue::Integer(component_id as i64)));
                self.results.push(record);
            }
        }
        
        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            let cid_b = b.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }

    fn execute_weighted_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node_id, target_node_id, weight_property)
        if self.args.len() < 3 {
            return Err(ExecutionError::RuntimeError("weightedPath requires source, target, and weight property".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Target must be integer ID".to_string())),
        };
        
        let weight_prop = match &self.args[2] {
            Expression::Literal(PropertyValue::String(s)) => s.clone(),
            _ => return Err(ExecutionError::TypeError("Weight property must be a string".to_string())),
        };

        // Build view with weights
        let view = crate::algo::build_view(store, None, None, Some(&weight_prop));
        
        if let Some(result) = crate::algo::dijkstra(&view, source_id, target_id) {
             let mut record = Record::new();
             record.bind("cost".to_string(), Value::Property(PropertyValue::Float(result.cost)));
             
             // Construct path list
             let mut path_nodes = Vec::new();
             for nid_u64 in result.path {
                 let nid = NodeId::new(nid_u64);
                 // We add just IDs for now, or could fetch full nodes if needed
                 path_nodes.push(PropertyValue::Integer(nid.as_u64() as i64));
             }
             record.bind("path".to_string(), Value::Property(PropertyValue::Array(path_nodes)));
             
             self.results.push(record);
        }

        Ok(())
    }
    fn execute_or_solve(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        if self.args.is_empty() {
             return Err(ExecutionError::RuntimeError("algo.or.solve requires a config map".to_string()));
        }

        let config_map = match &self.args[0] {
            Expression::Literal(PropertyValue::Map(m)) => m,
            _ => return Err(ExecutionError::TypeError("First argument must be a map".to_string())),
        };

        // Extract parameters
        let algorithm = config_map.get("algorithm").and_then(|v| v.as_string()).unwrap_or("Jaya");
        let label_str = config_map.get("label").and_then(|v| v.as_string())
            .ok_or_else(|| ExecutionError::RuntimeError("Missing 'label' in config".to_string()))?;
        let property = config_map.get("property").and_then(|v| v.as_string())
            .ok_or_else(|| ExecutionError::RuntimeError("Missing 'property' in config".to_string()))?;
        
        let min_val = config_map.get("min").and_then(|v| v.as_float()).unwrap_or(0.0);
        let max_val = config_map.get("max").and_then(|v| v.as_float()).unwrap_or(100.0);
        
        // Objective: minimize sum(variable * cost_property)
        let cost_prop = config_map.get("cost_property").and_then(|v| v.as_string());
        
        // Support multiple objectives
        let mut cost_props: Vec<String> = Vec::new();
        if let Some(cp) = cost_prop {
            cost_props.push(cp.to_string());
        } else if let Some(PropertyValue::Array(arr)) = config_map.get("cost_properties") {
            for v in arr {
                if let Some(s) = v.as_string() { cost_props.push(s.to_string()); }
            }
        }

        let budget = config_map.get("budget").and_then(|v| v.as_float());
        let min_total = config_map.get("min_total").and_then(|v| v.as_float());
        
        let pop_size = config_map.get("population_size").and_then(|v| v.as_integer()).unwrap_or(50) as usize;
        let max_iter = config_map.get("max_iterations").and_then(|v| v.as_integer()).unwrap_or(100) as usize;

        // 1. Gather nodes and costs
        let label = Label::new(label_str);
        
        let mut node_ids = Vec::new();
        let mut single_costs = Vec::new();
        let mut multi_costs = vec![Vec::new(); cost_props.len()];
        
        {
            let nodes = store.get_nodes_by_label(&label);
            for node in nodes {
                node_ids.push(node.id);
                
                // Single cost (for single objective solvers)
                if cost_props.len() == 1 {
                    let cost = node.get_property(&cost_props[0]).and_then(|v| v.as_float()).unwrap_or(1.0);
                    single_costs.push(cost);
                } else if !cost_props.is_empty() {
                    for (i, cp) in cost_props.iter().enumerate() {
                        let cost = node.get_property(cp).and_then(|v| v.as_float()).unwrap_or(1.0);
                        multi_costs[i].push(cost);
                    }
                } else {
                    single_costs.push(1.0);
                }
            }
        }

        if node_ids.is_empty() {
             return Ok(());
        }

        // 2. Setup Problem
        let problem = GraphOptimizationProblem {
            costs: single_costs,
            multi_costs,
            budget,
            min_total,
            dim: node_ids.len(),
            lower: min_val,
            upper: max_val,
        };

        let solver_config = SolverConfig {
            population_size: pop_size,
            max_iterations: max_iter,
        };

        // 3. Run Solver
        if algorithm == "NSGA2" || algorithm == "MOTLBO" || cost_props.len() > 1 {
            let res = match algorithm {
                "MOTLBO" => MOTLBOSolver::new(solver_config).solve(&problem),
                _ => NSGA2Solver::new(solver_config).solve(&problem), // Default multi
            };

            // Write back first individual from Pareto Front
            if let Some(best) = res.pareto_front.first() {
                for (i, &val) in best.variables.iter().enumerate() {
                    let node_id = node_ids[i];
                    let _ = store.set_node_property(tenant_id, node_id, property.to_string(), PropertyValue::Float(val));
                }
            }

            let mut record = Record::new();
            if let Some(best) = res.pareto_front.first() {
                let fitness_props: Vec<PropertyValue> = best.fitness.iter().map(|&f| PropertyValue::Float(f)).collect();
                record.bind("fitness".to_string(), Value::Property(PropertyValue::Array(fitness_props)));
            }
            record.bind("algorithm".to_string(), Value::Property(PropertyValue::String(algorithm.to_string())));
            record.bind("front_size".to_string(), Value::Property(PropertyValue::Integer(res.pareto_front.len() as i64)));
            self.results.push(record);

        } else {
            let result = match algorithm {
                "Rao1" => RaoSolver::new(solver_config, RaoVariant::Rao1).solve(&problem),
                "Rao2" => RaoSolver::new(solver_config, RaoVariant::Rao2).solve(&problem),
                "Rao3" => RaoSolver::new(solver_config, RaoVariant::Rao3).solve(&problem),
                "TLBO" => TLBOSolver::new(solver_config).solve(&problem),
                "Firefly" => FireflySolver::new(solver_config).solve(&problem),
                "Cuckoo" => CuckooSolver::new(solver_config).solve(&problem),
                "GWO" => GWOSolver::new(solver_config).solve(&problem),
                "GA" => GASolver::new(solver_config).solve(&problem),
                "SA" => SASolver::new(solver_config).solve(&problem),
                "Bat" => BatSolver::new(solver_config).solve(&problem),
                "ABC" => ABCSolver::new(solver_config).solve(&problem),
                "GSA" => GSASolver::new(solver_config).solve(&problem),
                "HS" => HSSolver::new(solver_config).solve(&problem),
                "FPA" => FPASolver::new(solver_config).solve(&problem),
                _ => JayaSolver::new(solver_config).solve(&problem), // Default to Jaya
            };

            // 4. Write back results
            for (i, &val) in result.best_variables.iter().enumerate() {
                let node_id = node_ids[i];
                let _ = store.set_node_property(tenant_id, node_id, property.to_string(), PropertyValue::Float(val));
            }

            // 5. Return result record
            let mut record = Record::new();
            record.bind("fitness".to_string(), Value::Property(PropertyValue::Float(result.best_fitness)));
            record.bind("algorithm".to_string(), Value::Property(PropertyValue::String(algorithm.to_string())));
            record.bind("iterations".to_string(), Value::Property(PropertyValue::Integer(max_iter as i64)));
            
            // Yield history as an array for plotting
            let history_props: Vec<PropertyValue> = result.history.into_iter().map(PropertyValue::Float).collect();
            record.bind("history".to_string(), Value::Property(PropertyValue::Array(history_props)));
            
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_max_flow(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source, sink, capacity_property?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError("maxFlow requires source and sink".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Sink must be integer ID".to_string())),
        };

        let cap_prop = if self.args.len() > 2 {
            match &self.args[2] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        // Build view
        let view = crate::algo::build_view(store, None, None, cap_prop.as_deref());
        
        // edmonds_karp expects u64 (AlgoNodeId), not crate::graph::NodeId
        if let Some(result) = crate::algo::edmonds_karp(&view, source_id, target_id) {
            let mut record = Record::new();
            record.bind("max_flow".to_string(), Value::Property(PropertyValue::Float(result.max_flow)));
            self.results.push(record);
        } else {
             // No flow found or invalid nodes
             let mut record = Record::new();
             record.bind("max_flow".to_string(), Value::Property(PropertyValue::Float(0.0)));
             self.results.push(record);
        }

        Ok(())
    }

    fn execute_mst(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (weight_property?)
        let weight_prop = if self.args.len() > 0 {
            match &self.args[0] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());
        let result = crate::algo::prim_mst(&view);

        // Return total weight
        let mut summary = Record::new();
        summary.bind("total_weight".to_string(), Value::Property(PropertyValue::Float(result.total_weight)));
        self.results.push(summary);

        // Return edges
        for (u_u64, v_u64, w) in result.edges {
            let u = NodeId::new(u_u64);
            let v = NodeId::new(v_u64);
            
            let mut record = Record::new();
            if let Some(node_u) = store.get_node(u) {
                record.bind("source".to_string(), Value::Node(u, node_u.clone()));
            }
            if let Some(node_v) = store.get_node(v) {
                record.bind("target".to_string(), Value::Node(v, node_v.clone()));
            }
            record.bind("weight".to_string(), Value::Property(PropertyValue::Float(w)));
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_triangle_count(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view (undirected treatment is handled in the algorithm)
        let view = crate::algo::build_view(store, None, None, None);
        let count = crate::algo::count_triangles(&view);

        let mut record = Record::new();
        record.bind("triangles".to_string(), Value::Property(PropertyValue::Integer(count as i64)));
        self.results.push(record);

        Ok(())
    }

    fn execute_scc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view and run SCC
        let view = crate::algo::build_view(store, None, None, None);
        let result = crate::algo::strongly_connected_components(&view);

        // For SCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, node.clone()));
                record.bind("componentId".to_string(), Value::Property(PropertyValue::Integer(component_id as i64)));
                self.results.push(record);
            }
        }
        
        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            let cid_b = b.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }
}

impl PhysicalOperator for AlgorithmOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            match self.name.as_str() {
                "algo.pageRank" => self.execute_pagerank(store)?,
                "algo.shortestPath" => self.execute_shortest_path(store)?,
                "algo.wcc" => self.execute_wcc(store)?,
                "algo.scc" => self.execute_scc(store)?,
                "algo.weightedPath" => self.execute_weighted_path(store)?,
                "algo.maxFlow" => self.execute_max_flow(store)?,
                "algo.mst" => self.execute_mst(store)?,
                "algo.triangleCount" => self.execute_triangle_count(store)?,
                "algo.or.solve" => return Err(ExecutionError::RuntimeError("algo.or.solve requires write access (MutQueryExecutor)".to_string())),
                _ => return Err(ExecutionError::RuntimeError(format!("Unknown algorithm: {}", self.name))),
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
         if !self.executed {
            match self.name.as_str() {
                "algo.or.solve" => self.execute_or_solve(store, tenant_id)?,
                // For read-only algos, we can just call the immutable implementations
                // But we need to borrow store immutably.
                // Since we have &mut store, we can reborrow as &store
                "algo.pageRank" => self.execute_pagerank(store)?,
                "algo.shortestPath" => self.execute_shortest_path(store)?,
                "algo.wcc" => self.execute_wcc(store)?,
                "algo.scc" => self.execute_scc(store)?,
                "algo.weightedPath" => self.execute_weighted_path(store)?,
                "algo.maxFlow" => self.execute_max_flow(store)?,
                "algo.mst" => self.execute_mst(store)?,
                "algo.triangleCount" => self.execute_triangle_count(store)?,
                _ => return Err(ExecutionError::RuntimeError(format!("Unknown algorithm: {}", self.name))),
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn is_mutating(&self) -> bool {
        self.name == "algo.or.solve"
    }

    fn reset(&mut self) {
        self.current = 0;
        self.executed = false;
        self.results.clear();
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