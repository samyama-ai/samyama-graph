//! # Records: The Data Unit of Query Execution
//!
//! A [`Record`] is a set of **variable bindings** -- a mapping from variable names (like
//! `n`, `r`, `m` in `MATCH (n)-[r]->(m)`) to [`Value`]s. It is the graph-database analog
//! of a "row" in a relational database, except that columns are named variables rather
//! than positional indices. Records flow through the Volcano iterator pipeline one at a
//! time, accumulating bindings as they pass through operators (e.g., `ExpandOperator` adds
//! the target node binding to an existing record that already contains the source node).
//!
//! ## Late Materialization (ADR-012)
//!
//! The most important optimization in this module is **late materialization**. Instead of
//! cloning an entire `Node` (with all its properties, labels, and metadata) when a scan
//! produces a result, we store only a `Value::NodeRef(id)` -- a 64-bit integer. The full
//! node data is resolved **on demand** via [`Value::resolve_property(prop, store)`] only
//! when a property is actually needed (e.g., in a WHERE filter or RETURN projection).
//!
//! This matters enormously for traversal queries. Consider `MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)`:
//! the ExpandOperator traverses through `b` nodes, but if the query only returns `c.name`,
//! the `b` nodes never need their properties loaded. Late materialization turns what would
//! be O(n * avg_properties) memory into O(n * 8 bytes).
//!
//! ## Semantic Equality: `NodeRef(id) == Node(id, _)`
//!
//! The [`Value`] enum has both lazy (`NodeRef`) and materialized (`Node`) variants for the
//! same logical entity. This creates a subtle correctness requirement: the `JoinOperator`
//! uses hash-based lookups to match records from two sides of a join. If the left side
//! produces `NodeRef(42)` and the right side produces `Node(42, <data>)`, they must be
//! considered **equal** and must produce the **same hash** -- otherwise the join silently
//! drops valid matches.
//!
//! This is why [`PartialEq`] and [`Hash`] are implemented **manually** instead of derived.
//! The derive macro would compare all fields (including the `Node` data), breaking the
//! semantic equivalence. The manual implementation compares only the identity (the `NodeId`),
//! and the hash function uses a discriminant tag (0 for nodes, 1 for edges) plus the ID,
//! ensuring the **hash consistency invariant**: if `a == b`, then `hash(a) == hash(b)`.
//!
//! [`RecordBatch`] is the final output container -- a vector of [`Record`]s plus column
//! names, returned to the caller after query execution completes.

use crate::graph::{Edge, Node, NodeId, EdgeId, EdgeType, PropertyValue, GraphStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::hash::{Hash, Hasher};

/// A single record flowing through the query pipeline
#[derive(Debug, Clone)]
pub struct Record {
    /// Variable bindings, in the order they were bound.
    ///
    /// A flat vector, not a hash map. A query plan binds a handful of
    /// variables — three or four is typical, a dozen is a lot — and at that
    /// size a linear scan comparing short strings beats hashing one.
    ///
    /// What a `HashMap<String, Value>` cost, per row:
    ///
    /// * a **table allocation** for every record;
    /// * a **`String` allocation per binding** on every clone, and operators
    ///   clone a record per output row (`ExpandOperator::next` does exactly
    ///   that);
    /// * a **`SipHash` over a variable name** on every read, several times per
    ///   row — once in the expand, once per property access inside
    ///   `evaluate_expression`.
    ///
    /// Measured on LDBC IC5, whose `Aggregate` consumes 1,678,980 records at
    /// ~1,275 ns each while its group key accounts for perhaps 200 ns of that
    /// (#546).
    ///
    /// `Arc<str>` rather than `String` is the other half: cloning a record now
    /// copies a vector of pointer pairs and bumps refcounts, where before it
    /// allocated a fresh `String` for every variable name on every row.
    bindings: Vec<(Arc<str>, Value)>,
    /// Relationships already traversed by the MATCH pattern being matched.
    ///
    /// openCypher uses **relationship isomorphism**: one edge may not appear
    /// twice in a single pattern. Without this, `MATCH (a)-[:R]-(b)-[:R]-(c)`
    /// over a three-node chain returned 6 rows where Cypher gives 2 — every
    /// two-hop undirected pattern was inflated by walking an edge back on
    /// itself (#684).
    ///
    /// A `Vec` rather than a set: patterns bind a handful of relationships, so
    /// a linear scan over a few `u64`s beats hashing, and an **empty `Vec`
    /// does not allocate** — single-hop patterns, which cannot violate the
    /// rule, pay only the 24 bytes in the struct and a null clone.
    ///
    /// Scoped to one clause. `MATCH (a)-[:R]-(b) MATCH (b)-[:R]-(c)` *may*
    /// reuse the edge (Neo4j agrees), so a clause boundary clears this.
    used_edges: Vec<crate::graph::EdgeId>,
}

/// Value types that can be bound to variables in a query record.
///
/// The key design choice here is the **late materialization hierarchy**:
///
/// - **`NodeRef(id)`** -- a lazy reference. Stores only the 64-bit `NodeId`. Produced by
///   scan and expand operators. Extremely cheap to create (no heap allocation, no cloning).
///   Properties are resolved on demand via `resolve_property(prop, store)`.
///
/// - **`Node(id, node)`** -- a fully materialized node. Contains a clone of the `Node`
///   struct with all labels and properties. Produced by `ProjectOperator` when the RETURN
///   clause requests `RETURN n` (the entire node), triggering full materialization.
///
/// The same lazy/eager split exists for edges: `EdgeRef(id, src, tgt, type)` carries the
/// structural data (endpoints and type) without property clones, while `Edge(id, edge)`
/// is fully materialized.
///
/// `Property(PropertyValue)` wraps scalar values (strings, integers, floats, booleans,
/// datetimes, arrays, maps) that result from property access (`n.name`) or literal
/// expressions. `Path` stores ordered sequences of node/edge IDs for named path patterns
/// like `p = (a)-[]->(b)`. `Null` represents the absence of a value, following Cypher's
/// three-valued logic (true/false/null).
#[derive(Debug, Clone)]
pub enum Value {
    /// A fully materialized node.
    ///
    /// Boxed. `Node` is 128 bytes -- a `HashSet<Label>` and a `PropertyMap`
    /// held inline, plus id, version and two timestamps -- and carrying it
    /// inline made `Value` 144 bytes. `Value` is the executor's universal cell,
    /// so that width was paid by every binding in every record, every hash
    /// entry holding one, and every sort key.
    ///
    /// Late materialization (ADR-012) exists so scans produce `NodeRef` and
    /// this variant stays rare, which is exactly what makes the indirection
    /// cheap and the saving broad (#570).
    Node(NodeId, Box<Node>),
    /// A lazy node reference (no property clone)
    NodeRef(NodeId),
    /// A fully materialized edge. Boxed, for the same reason as `Node`.
    Edge(EdgeId, Box<Edge>),
    /// A lazy edge reference (structural data only, no property clone)
    EdgeRef(EdgeId, NodeId, NodeId, EdgeType),
    /// A property value
    Property(PropertyValue),
    /// A path (ordered sequence of node/edge IDs)
    Path {
        nodes: Vec<NodeId>,
        edges: Vec<EdgeId>,
    },
    /// A list whose elements are themselves `Value`s.
    ///
    /// `PropertyValue::Array` cannot hold a node or a relationship, so a list
    /// of them had nowhere to live: `relationships(p)` degraded its edges to
    /// integer ids and a variable-length relationship variable was not bound
    /// at all.
    List(Vec<Value>),
    /// A map whose values are themselves `Value`s.
    ///
    /// `PropertyValue::Map` cannot hold an entity, and `{key: u}` over a node
    /// is how the TCK's Delete5 scenarios reach what they delete (#654).
    Map(std::collections::BTreeMap<String, Value>),
    /// Null
    Null,
}

// NodeRef(id) == Node(id, _) — compare by ID only for nodes/edges
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Node variants compare by ID
            (Value::Node(id1, _), Value::Node(id2, _)) => id1 == id2,
            (Value::NodeRef(id1), Value::NodeRef(id2)) => id1 == id2,
            (Value::Node(id1, _), Value::NodeRef(id2)) | (Value::NodeRef(id2), Value::Node(id1, _)) => id1 == id2,
            // Edge variants compare by ID
            (Value::Edge(id1, _), Value::Edge(id2, _)) => id1 == id2,
            (Value::EdgeRef(id1, ..), Value::EdgeRef(id2, ..)) => id1 == id2,
            (Value::Edge(id1, _), Value::EdgeRef(id2, ..)) | (Value::EdgeRef(id2, ..), Value::Edge(id1, _)) => id1 == id2,
            // Property and Null
            (Value::Property(p1), Value::Property(p2)) => p1 == p2,
            // Path
            (Value::Path { nodes: n1, edges: e1 }, Value::Path { nodes: n2, edges: e2 }) => n1 == n2 && e1 == e2,
            (Value::Null, Value::Null) => true,
            // Lists and maps had no arm at all, so they fell to `_ => false`
            // and **no two lists were ever equal** -- `[] == []` included, and
            // `a == a` with it, for a type that also implements `Eq` (#925).
            //
            // Nothing errored. `GROUP BY` a list simply never merged two
            // groups and `DISTINCT` never removed a duplicate, so a query
            // returned one row too many with every row individually right.
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            // The two spellings of one list. `PropertyValue::Array` cannot
            // hold a node, so a list of relationships has to be a
            // `Value::List` while a list read from a property is an `Array` --
            // and a query that groups over both is mixing spellings, not
            // types. Same for maps.
            (Value::List(a), Value::Property(PropertyValue::Array(b)))
            | (Value::Property(PropertyValue::Array(b)), Value::List(a)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(x, y)| *x == Value::Property(y.clone()))
            }
            (Value::Map(a), Value::Property(PropertyValue::Map(b)))
            | (Value::Property(PropertyValue::Map(b)), Value::Map(a)) => {
                a.len() == b.len()
                    && a.iter().all(|(k, v)| {
                        b.get(k).is_some_and(|w| *v == Value::Property(w.clone()))
                    })
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    /// Hash **canonically**, not per variant.
    ///
    /// `Eq` treats `Value::List` and `Value::Property(Array)` as the same
    /// list, and `Value::Map` and `Value::Property(Map)` as the same map, so
    /// the two spellings have to reach the same hash or the `Hash`/`Eq`
    /// contract is broken and a HashMap loses entries it holds.
    ///
    /// That is why a list is hashed here rather than delegating to its
    /// elements' `Hash`: the elements are `PropertyValue` on one side and
    /// `Value` on the other, and only hashing each element *as the `Value` it
    /// is equal to* makes the two agree.
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use semantic tags so NodeRef and Node hash the same
        match self {
            Value::Node(id, _) | Value::NodeRef(id) => { 0u8.hash(state); id.hash(state); }
            Value::Edge(id, _) | Value::EdgeRef(id, ..) => { 1u8.hash(state); id.hash(state); }
            // A property that is a list or a map hashes as the list or map it
            // is, not as "a property".
            Value::Property(PropertyValue::Array(items)) => {
                5u8.hash(state);
                items.len().hash(state);
                for item in items { Value::Property(item.clone()).hash(state); }
            }
            Value::Property(PropertyValue::Map(entries)) => {
                hash_map_entries(state, entries.iter().map(|(k, v)| (k, Value::Property(v.clone()))));
            }
            Value::Property(p) => { 2u8.hash(state); p.hash(state); }
            Value::Path { nodes, edges } => { 3u8.hash(state); nodes.hash(state); edges.hash(state); }
            Value::List(items) => {
                5u8.hash(state);
                items.len().hash(state);
                for item in items { item.hash(state); }
            }
            Value::Map(entries) => {
                hash_map_entries(state, entries.iter().map(|(k, v)| (k, v.clone())));
            }
            Value::Null => { 4u8.hash(state); }
        }
    }
}

/// Hash a map's entries in key order.
///
/// `Value::Map` is a `BTreeMap` and `PropertyValue::Map` is a `HashMap`, whose
/// iteration order is not stable between runs let alone between the two types.
/// Sorting by key is what makes one hash of the other possible at all.
fn hash_map_entries<'a, H: Hasher>(
    state: &mut H,
    entries: impl Iterator<Item = (&'a String, Value)>,
) {
    6u8.hash(state);
    let mut pairs: Vec<(&String, Value)> = entries.collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs.len().hash(state);
    for (k, v) in pairs { k.hash(state); v.hash(state); }
}

impl Record {
    /// Create a new empty record
    pub fn new() -> Self {
        Self {
            bindings: Vec::new(),
            used_edges: Vec::new(),
        }
    }

    /// Clone this record leaving room for `extra` further bindings.
    ///
    /// `Vec::clone` allocates *exact* capacity, so a cloned record has
    /// `len == cap` and the very next `bind` reallocates: allocate, memcpy,
    /// free. Clone-then-bind is how nearly every operator derives an output row
    /// from an input row, so nearly every row in every query was paying for
    /// that. Measured on a 3-binding record: 79.8 ns to clone, and 175.7 ns to
    /// clone and bind a fourth -- 95.9 ns of the difference being the
    /// reallocation alone (#562).
    ///
    /// Callers that bind a known number of variables should say how many.
    pub fn clone_with_capacity(&self, extra: usize) -> Record {
        let mut bindings = Vec::with_capacity(self.bindings.len() + extra);
        bindings.extend(self.bindings.iter().cloned());
        Record { bindings, used_edges: self.used_edges.clone() }
    }

    /// Has this relationship already been traversed by the current pattern?
    pub fn edge_used(&self, edge: crate::graph::EdgeId) -> bool {
        self.used_edges.contains(&edge)
    }

    /// Record a relationship as traversed by the current pattern.
    pub fn mark_edge_used(&mut self, edge: crate::graph::EdgeId) {
        self.used_edges.push(edge);
    }

    /// The relationships already traversed, for a caller that filters
    /// candidates in a hot loop and wants no allocation.
    pub fn used_edge_slice(&self) -> &[crate::graph::EdgeId] {
        &self.used_edges
    }

    /// Forget the traversed relationships — a new MATCH clause starts fresh,
    /// because relationship isomorphism is scoped to one clause.
    pub fn clear_used_edges(&mut self) {
        self.used_edges.clear();
    }

    /// Bind a variable to a value, replacing any previous binding.
    ///
    /// Accepts anything that converts to `Arc<str>`, so an operator holding
    /// its variable name as an `Arc<str>` binds with a refcount bump; passing
    /// a `String` copies once, as inserting into the old map did.
    pub fn bind(&mut self, variable: impl Into<Arc<str>>, value: Value) {
        let variable = variable.into();
        match self.bindings.iter_mut().find(|(name, _)| *name == variable) {
            Some(slot) => slot.1 = value,
            None => self.bindings.push((variable, value)),
        }
    }

    /// Get a bound value
    pub fn get(&self, variable: &str) -> Option<&Value> {
        self.bindings
            .iter()
            .find(|(name, _)| &**name == variable)
            .map(|(_, value)| value)
    }

    /// All bindings, in binding order.
    /// Drop every binding whose name the predicate rejects.
    ///
    /// Used to strip the private bindings a `WITH ... ORDER BY` carries for
    /// the sort: they must not become columns, and must not re-enter scope for
    /// the next clause under a name it never projected (#970).
    pub fn retain_bindings(&mut self, keep: impl Fn(&str) -> bool) {
        self.bindings.retain(|(k, _)| keep(k));
    }

    pub fn bindings(&self) -> &[(Arc<str>, Value)] {
        &self.bindings
    }

    /// The bound values, without their names.
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.bindings.iter().map(|(_, value)| value)
    }

    /// Check if a variable is bound
    pub fn has(&self, variable: &str) -> bool {
        self.get(variable).is_some()
    }

    /// Merge another record into this one, `other` winning on a clash.
    pub fn merge(&mut self, other: Record) {
        for (name, value) in other.bindings {
            self.bind(name, value);
        }
    }

    /// Clone with only specified variables
    pub fn project(&self, variables: &[String]) -> Record {
        let mut new_record = Record::new();
        for var in variables {
            if let Some((name, value)) = self.bindings.iter().find(|(n, _)| &**n == var.as_str()) {
                new_record.bind(name.clone(), value.clone());
            }
        }
        new_record
    }

    /// A deterministic key for deduplication: bindings sorted by name.
    ///
    /// Sorted because binding *order* is an artefact of how a plan was built,
    /// not part of a row's identity — two records binding the same values in a
    /// different order are the same row and must collide.
    pub fn dedup_key(&self) -> Vec<(Arc<str>, Value)> {
        let mut key = self.bindings.clone();
        key.sort_by(|a, b| a.0.cmp(&b.0));
        key
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

impl Value {
    /// Get as node if this is a fully materialized node value
    pub fn as_node(&self) -> Option<(NodeId, &Node)> {
        match self {
            Value::Node(id, node) => Some((*id, node)),
            _ => None,
        }
    }

    /// Get as edge if this is a fully materialized edge value
    pub fn as_edge(&self) -> Option<(EdgeId, &Edge)> {
        match self {
            Value::Edge(id, edge) => Some((*id, edge)),
            _ => None,
        }
    }

    /// Get as property if this is a property value
    pub fn as_property(&self) -> Option<&PropertyValue> {
        match self {
            Value::Property(prop) => Some(prop),
            _ => None,
        }
    }

    /// Check if this is null
    /// Is this value null?
    ///
    /// Null has two representations here — the `Value::Null` variant, and a
    /// `Value::Property(PropertyValue::Null)` produced when a node simply does not carry
    /// the property being read. They mean the same thing, and treating only the first as
    /// null is what made `count(x.prop)` count rows rather than non-null values (#358):
    /// a missing property arrives as the second form and slipped through the check.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null | Value::Property(PropertyValue::Null))
    }

    /// Extract NodeId from any node variant (Node or NodeRef)
    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            Value::Node(id, _) | Value::NodeRef(id) => Some(*id),
            _ => None,
        }
    }

    /// Extract EdgeId from any edge variant (Edge or EdgeRef)
    pub fn edge_id(&self) -> Option<EdgeId> {
        match self {
            Value::Edge(id, _) => Some(*id),
            Value::EdgeRef(id, ..) => Some(*id),
            _ => None,
        }
    }

    /// Extract edge endpoints from any edge variant
    pub fn edge_endpoints(&self) -> Option<(NodeId, NodeId)> {
        match self {
            Value::Edge(_, edge) => Some((edge.source, edge.target)),
            Value::EdgeRef(_, src, tgt, _) => Some((*src, *tgt)),
            _ => None,
        }
    }

    /// Extract edge type from any edge variant
    pub fn edge_type(&self) -> Option<&EdgeType> {
        match self {
            Value::Edge(_, edge) => Some(&edge.edge_type),
            Value::EdgeRef(_, _, _, et) => Some(et),
            _ => None,
        }
    }

    /// Check if this represents a node (Node or NodeRef)
    pub fn is_node(&self) -> bool {
        matches!(self, Value::Node(..) | Value::NodeRef(..))
    }

    /// Check if this represents an edge (Edge or EdgeRef)
    pub fn is_edge(&self) -> bool {
        matches!(self, Value::Edge(..) | Value::EdgeRef(..))
    }

    /// Materialize a NodeRef into a full Node by looking it up in the store.
    /// Returns self unchanged if already materialized or not a node variant.
    pub fn materialize_node(self, store: &GraphStore) -> Self {
        match self {
            Value::NodeRef(id) => {
                if let Some(node) = store.get_node(id) {
                    Value::Node(id, Box::new(node.clone()))
                } else {
                    Value::Null
                }
            }
            other => other,
        }
    }

    /// Materialize an EdgeRef into a full Edge by looking it up in the store.
    /// Returns self unchanged if already materialized or not an edge variant.
    pub fn materialize_edge(self, store: &GraphStore) -> Self {
        match self {
            Value::EdgeRef(id, ..) => {
                if let Some(edge) = store.get_edge(id) {
                    Value::Edge(id, Box::new(edge.clone()))
                } else {
                    Value::Null
                }
            }
            other => other,
        }
    }

    /// Resolve a property from this value, using columnar store first, then
    /// falling back to materialized node/edge properties or store lookup for refs.
    pub fn resolve_property(&self, property: &str, store: &GraphStore) -> PropertyValue {
        match self {
            Value::Node(id, node) => {
                let prop = store.node_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else {
                    node.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                }
            }
            Value::NodeRef(id) => {
                let prop = store.node_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else if let Some(node) = store.get_node(*id) {
                    node.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                } else {
                    PropertyValue::Null
                }
            }
            Value::Edge(id, edge) => {
                let prop = store.edge_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else {
                    edge.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                }
            }
            Value::EdgeRef(id, ..) => {
                let prop = store.edge_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else if let Some(edge) = store.get_edge(*id) {
                    edge.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                } else {
                    PropertyValue::Null
                }
            }
            // Map property access: `m.a` where `m` is a map, from a literal, an
            // `UNWIND` over a list of maps, or a map-valued node property.
            //
            // Without this arm `m.a` fell through to `Null` -- and it did so
            // *silently*, so a query over map values returned confidently wrong
            // answers rather than failing. Grouping was the sharpest case:
            // distinct keys collapsed into one `Null` group while the row count
            // stayed plausible (#571).
            //
            // An absent key is still `Null`, which is Cypher's answer for it.
            Value::Property(PropertyValue::Map(entries)) => entries
                .get(property)
                .cloned()
                .unwrap_or(PropertyValue::Null),
            // Component access on the five temporal types (#689).
            //
            // Routed through one function so a `Date` and a `LocalDateTime`
            // cannot disagree about what `.year` means. Before this, only the
            // legacy `DateTime` had accessors, so teaching the constructors to
            // return real types would have silently removed `dt.year` — which
            // the suite caught, and which is Cypher-required behaviour rather
            // than a test encoding the old shape.
            Value::Property(p @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. })) => temporal_component(p, property),
            // Temporal component access: dt.year, dt.month, dur.days, etc.
            Value::Property(PropertyValue::DateTime(millis)) => {
                use chrono::{Datelike, Timelike, TimeZone};
                match chrono::Utc.timestamp_millis_opt(*millis).single() {
                    Some(dt) => match property {
                        "year" => PropertyValue::Integer(dt.year() as i64),
                        "month" => PropertyValue::Integer(dt.month() as i64),
                        "day" => PropertyValue::Integer(dt.day() as i64),
                        "hour" => PropertyValue::Integer(dt.hour() as i64),
                        "minute" => PropertyValue::Integer(dt.minute() as i64),
                        "second" => PropertyValue::Integer(dt.second() as i64),
                        "millisecond" => PropertyValue::Integer(dt.timestamp_subsec_millis() as i64),
                        "epochMillis" => PropertyValue::Integer(*millis),
                        _ => PropertyValue::Null,
                    },
                    None => PropertyValue::Null,
                }
            }
            Value::Property(PropertyValue::Duration { months, days, seconds, nanos }) => {
                // Duration accessors come in two families, and the difference
                // is the whole design (#819):
                //
                //   * **Totals** -- `minutes` is the entire time part in
                //     minutes (61 for PT1H1M1S), `nanoseconds` is the entire
                //     time part in nanoseconds.
                //   * **Remainders**, spelled `<unit>Of<Unit>` -- `minutesOfHour`
                //     is the same value modulo the next unit up (1).
                //
                // `minutes` was returning the remainder and `nanoseconds` was
                // returning only the sub-second field, so both read as the
                // wrong family. Nine more accessors were absent and returned
                // null, which is indistinguishable from a legitimate zero.
                //
                // Time and date parts never mix: a month is not a fixed number
                // of days, so `days` cannot be derived from `months`, and the
                // two families are computed independently.
                const NPS: i64 = 1_000_000_000;
                // Total sub-day time, normalized so the nanosecond remainder is
                // **non-negative** -- `duration.between` of -86399.9s reports
                // `seconds = -86400` with `nanosecondsOfSecond = +100000000`,
                // not -86399 with -900000000.
                //
                // This is a presentation split and does not contradict the
                // sign-consistency invariant (#806), which governs the stored
                // components: those stay -86399/-900000000 so the duration
                // still renders as `PT-23H-59M-59.9S`.
                let total_nanos = *seconds as i128 * NPS as i128 + *nanos as i128;
                let sec = total_nanos.div_euclid(NPS as i128) as i64;
                let nos = total_nanos.rem_euclid(NPS as i128) as i64;
                let int = |x: i64| PropertyValue::Integer(x);
                match property {
                    // Date part: totals, then remainders.
                    "years" => int(*months / 12),
                    "quarters" => int(*months / 3),
                    "months" => int(*months),
                    "weeks" => int(*days / 7),
                    "days" => int(*days),
                    "quartersOfYear" => int(*months / 3 % 4),
                    "monthsOfQuarter" => int(*months % 3),
                    "monthsOfYear" => int(*months % 12),
                    "daysOfWeek" => int(*days % 7),
                    // Time part: totals, then remainders.
                    "hours" => int(sec / 3600),
                    "minutes" => int(sec / 60),
                    "seconds" => int(sec),
                    "milliseconds" => int((total_nanos / 1_000_000) as i64),
                    "microseconds" => int((total_nanos / 1_000) as i64),
                    "nanoseconds" => int(total_nanos as i64),
                    "minutesOfHour" => int(sec % 3600 / 60),
                    "secondsOfMinute" => int(sec % 60),
                    "millisecondsOfSecond" => int(nos / 1_000_000),
                    "microsecondsOfSecond" => int(nos / 1_000),
                    "nanosecondsOfSecond" => int(nos),
                    _ => PropertyValue::Null,
                }
            }
            _ => PropertyValue::Null,
        }
    }
}

/// A batch of records (result set)
#[derive(Debug)]
pub struct RecordBatch {
    /// All records in the batch
    pub records: Vec<Record>,
    /// Column names for the result
    pub columns: Vec<String>,
}

impl RecordBatch {
    /// Create a new empty batch
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            records: Vec::new(),
            columns,
        }
    }

    /// Get number of records
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Add a record
    pub fn push(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Get a record by index
    pub fn get(&self, index: usize) -> Option<&Record> {
        self.records.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Label;

    #[test]
    fn test_record_creation() {
        let record = Record::new();
        assert_eq!(record.bindings().len(), 0);
    }

    #[test]
    fn test_record_binding() {
        let mut record = Record::new();
        let node = Node::new(NodeId::new(1), Label::new("Person"));

        record.bind("n".to_string(), Value::Node(NodeId::new(1), Box::new(node)));

        assert!(record.has("n"));
        assert!(record.get("n").is_some());
    }

    #[test]
    fn test_record_merge() {
        let mut record1 = Record::new();
        let mut record2 = Record::new();

        record1.bind("a".to_string(), Value::Property(PropertyValue::Integer(1)));
        record2.bind("b".to_string(), Value::Property(PropertyValue::Integer(2)));

        record1.merge(record2);

        assert!(record1.has("a"));
        assert!(record1.has("b"));
    }

    #[test]
    fn test_record_project() {
        let mut record = Record::new();
        record.bind("a".to_string(), Value::Property(PropertyValue::Integer(1)));
        record.bind("b".to_string(), Value::Property(PropertyValue::Integer(2)));
        record.bind("c".to_string(), Value::Property(PropertyValue::Integer(3)));

        let projected = record.project(&vec!["a".to_string(), "c".to_string()]);

        assert!(projected.has("a"));
        assert!(!projected.has("b"));
        assert!(projected.has("c"));
    }

    #[test]
    fn test_value_types() {
        let node_val = Value::Node(NodeId::new(1), Box::new(Node::new(NodeId::new(1), Label::new("Test"))));
        assert!(node_val.as_node().is_some());
        assert!(node_val.as_edge().is_none());

        let prop_val = Value::Property(PropertyValue::String("test".to_string()));
        assert!(prop_val.as_property().is_some());

        let null_val = Value::Null;
        assert!(null_val.is_null());
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new(vec!["n".to_string(), "m".to_string()]);
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());

        batch.push(Record::new());
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    // ========== Batch 5: Additional Record Tests ==========

    #[test]
    fn test_as_edge() {
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("KNOWS"),
        );
        let val = Value::Edge(EdgeId::new(1), Box::new(edge));
        let (eid, e) = val.as_edge().unwrap();
        assert_eq!(eid, EdgeId::new(1));
        assert_eq!(e.source, NodeId::new(10));
        assert_eq!(e.target, NodeId::new(20));

        // Non-edge variants return None
        assert!(Value::Null.as_edge().is_none());
        assert!(Value::NodeRef(NodeId::new(1)).as_edge().is_none());
    }

    #[test]
    fn test_node_id() {
        // From Node
        let node = Node::new(NodeId::new(5), Label::new("Person"));
        let val = Value::Node(NodeId::new(5), Box::new(node));
        assert_eq!(val.node_id(), Some(NodeId::new(5)));

        // From NodeRef
        let val = Value::NodeRef(NodeId::new(7));
        assert_eq!(val.node_id(), Some(NodeId::new(7)));

        // Non-node variants
        assert!(Value::Null.node_id().is_none());
        assert!(Value::Property(PropertyValue::Integer(42)).node_id().is_none());
    }

    #[test]
    fn test_edge_id() {
        // From Edge
        let edge = crate::graph::Edge::new(
            EdgeId::new(3),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let val = Value::Edge(EdgeId::new(3), Box::new(edge));
        assert_eq!(val.edge_id(), Some(EdgeId::new(3)));

        // From EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(4),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(val.edge_id(), Some(EdgeId::new(4)));

        // Non-edge
        assert!(Value::Null.edge_id().is_none());
    }

    #[test]
    fn test_edge_endpoints() {
        // From Edge
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("E"),
        );
        let val = Value::Edge(EdgeId::new(1), Box::new(edge));
        assert_eq!(val.edge_endpoints(), Some((NodeId::new(10), NodeId::new(20))));

        // From EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(30),
            NodeId::new(40),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(val.edge_endpoints(), Some((NodeId::new(30), NodeId::new(40))));

        // Non-edge
        assert!(Value::Null.edge_endpoints().is_none());
    }

    #[test]
    fn test_edge_type_accessor() {
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("KNOWS"),
        );
        let val = Value::Edge(EdgeId::new(1), Box::new(edge));
        assert_eq!(val.edge_type().unwrap().as_str(), "KNOWS");

        let val = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("LIKES"),
        );
        assert_eq!(val.edge_type().unwrap().as_str(), "LIKES");

        assert!(Value::Null.edge_type().is_none());
    }

    #[test]
    fn test_is_node_is_edge() {
        let node = Node::new(NodeId::new(1), Label::new("A"));
        assert!(Value::Node(NodeId::new(1), Box::new(node)).is_node());
        assert!(Value::NodeRef(NodeId::new(1)).is_node());
        assert!(!Value::Null.is_node());
        assert!(!Value::Property(PropertyValue::Integer(1)).is_node());

        let edge = crate::graph::Edge::new(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert!(Value::Edge(EdgeId::new(1), Box::new(edge)).is_edge());
        assert!(Value::EdgeRef(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        ).is_edge());
        assert!(!Value::Null.is_edge());
    }

    #[test]
    fn test_materialize_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );

        // NodeRef materializes to Node
        let val = Value::NodeRef(id).materialize_node(&store);
        match &val {
            Value::Node(nid, node) => {
                assert_eq!(*nid, id);
                assert!(node.labels.contains(&Label::new("Person")));
            }
            _ => panic!("Expected Value::Node after materialization"),
        }

        // Already materialized stays the same
        let node = store.get_node(id).unwrap().clone();
        let val = Value::Node(id, Box::new(node)).materialize_node(&store);
        assert!(matches!(val, Value::Node(..)));

        // Non-existent NodeRef becomes Null
        let val = Value::NodeRef(NodeId::new(9999)).materialize_node(&store);
        assert!(val.is_null());

        // Non-node value is returned unchanged
        let val = Value::Property(PropertyValue::Integer(42)).materialize_node(&store);
        assert!(matches!(val, Value::Property(..)));
    }

    #[test]
    fn test_materialize_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let eid = store.create_edge(a, b, "KNOWS").unwrap();

        // EdgeRef materializes to Edge
        let val = Value::EdgeRef(
            eid, a, b, crate::graph::EdgeType::new("KNOWS"),
        ).materialize_edge(&store);
        match &val {
            Value::Edge(id, edge) => {
                assert_eq!(*id, eid);
                assert_eq!(edge.source, a);
                assert_eq!(edge.target, b);
            }
            _ => panic!("Expected Value::Edge after materialization"),
        }

        // Non-existent EdgeRef becomes Null
        let val = Value::EdgeRef(
            EdgeId::new(9999), a, b, crate::graph::EdgeType::new("X"),
        ).materialize_edge(&store);
        assert!(val.is_null());

        // Non-edge value is returned unchanged
        let val = Value::Null.materialize_edge(&store);
        assert!(val.is_null());
    }

    #[test]
    fn test_resolve_property_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );

        // Resolve from Node (materialized)
        let node = store.get_node(id).unwrap().clone();
        let val = Value::Node(id, Box::new(node));
        let prop = val.resolve_property("name", &store);
        assert_eq!(prop, PropertyValue::String("Alice".to_string()));

        // Missing property returns Null
        let prop = val.resolve_property("missing", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_noderef() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "age".to_string(),
            PropertyValue::Integer(30),
        );

        let val = Value::NodeRef(id);
        let prop = val.resolve_property("age", &store);
        assert_eq!(prop, PropertyValue::Integer(30));

        // Non-existent NodeRef
        let val = Value::NodeRef(NodeId::new(9999));
        let prop = val.resolve_property("age", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");

        let mut props = std::collections::HashMap::new();
        props.insert("since".to_string(), PropertyValue::Integer(2020));
        let eid = store.create_edge_with_properties(a, b, "KNOWS", props).unwrap();

        // From Edge
        let edge = store.get_edge(eid).unwrap();
        let val = Value::Edge(eid, Box::new(edge));
        let prop = val.resolve_property("since", &store);
        assert_eq!(prop, PropertyValue::Integer(2020));
    }

    #[test]
    fn test_resolve_property_edgeref() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");

        let mut props = std::collections::HashMap::new();
        props.insert("weight".to_string(), PropertyValue::Float(0.5));
        let eid = store.create_edge_with_properties(a, b, "KNOWS", props).unwrap();

        let val = Value::EdgeRef(eid, a, b, crate::graph::EdgeType::new("KNOWS"));
        let prop = val.resolve_property("weight", &store);
        assert_eq!(prop, PropertyValue::Float(0.5));

        // Non-existent EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(9999), a, b, crate::graph::EdgeType::new("X"),
        );
        let prop = val.resolve_property("weight", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_non_node_edge() {
        let store = GraphStore::new();
        let val = Value::Null;
        assert_eq!(val.resolve_property("anything", &store), PropertyValue::Null);

        let val = Value::Property(PropertyValue::Integer(42));
        assert_eq!(val.resolve_property("x", &store), PropertyValue::Null);
    }

    #[test]
    fn test_record_batch_get() {
        let mut batch = RecordBatch::new(vec!["n".to_string()]);
        let mut r1 = Record::new();
        r1.bind("n".to_string(), Value::Property(PropertyValue::Integer(1)));
        let mut r2 = Record::new();
        r2.bind("n".to_string(), Value::Property(PropertyValue::Integer(2)));
        batch.push(r1);
        batch.push(r2);

        assert!(batch.get(0).is_some());
        assert!(batch.get(1).is_some());
        assert!(batch.get(2).is_none()); // out of bounds

        let r = batch.get(0).unwrap();
        assert_eq!(
            r.get("n").unwrap().as_property(),
            Some(&PropertyValue::Integer(1))
        );
    }

    #[test]
    fn test_record_bindings() {
        let mut r = Record::new();
        r.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));
        r.bind("y".to_string(), Value::Null);

        let bindings = r.bindings();
        assert_eq!(bindings.len(), 2);
        assert!(r.has("x"));
        assert!(r.has("y"));
        // Bindings keep insertion order now, which `dedup_key` normalises
        // away for identity. Order is observable here and deliberately not
        // relied on anywhere else.
        assert_eq!(&*bindings[0].0, "x");
        assert_eq!(&*bindings[1].0, "y");
    }

    #[test]
    fn rebinding_a_variable_replaces_it_rather_than_duplicating() {
        let mut r = Record::new();
        r.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));
        r.bind("x".to_string(), Value::Property(PropertyValue::Integer(2)));
        assert_eq!(r.bindings().len(), 1, "a flat vector must not accumulate duplicates");
        assert_eq!(r.get("x"), Some(&Value::Property(PropertyValue::Integer(2))));
    }

    #[test]
    fn the_dedup_key_ignores_binding_order() {
        // Two records with the same bindings in a different order are the same
        // row. The previous key was `format!("{:?}", bindings)` over a hash
        // map, so it depended on iteration order for identity.
        let mut a = Record::new();
        a.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));
        a.bind("y".to_string(), Value::Property(PropertyValue::Integer(2)));

        let mut b = Record::new();
        b.bind("y".to_string(), Value::Property(PropertyValue::Integer(2)));
        b.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));

        assert_eq!(a.dedup_key(), b.dedup_key());
    }

    #[test]
    fn merge_lets_the_incoming_record_win() {
        let mut a = Record::new();
        a.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));
        a.bind("y".to_string(), Value::Property(PropertyValue::Integer(9)));

        let mut b = Record::new();
        b.bind("x".to_string(), Value::Property(PropertyValue::Integer(2)));

        a.merge(b);
        assert_eq!(a.bindings().len(), 2);
        assert_eq!(a.get("x"), Some(&Value::Property(PropertyValue::Integer(2))));
        assert_eq!(a.get("y"), Some(&Value::Property(PropertyValue::Integer(9))));
    }

    #[test]
    fn test_record_default() {
        let r = Record::default();
        assert_eq!(r.bindings().len(), 0);
    }

    #[test]
    fn test_value_partial_eq_cross_variant() {
        // Node == NodeRef with same ID
        let node = Node::new(NodeId::new(5), Label::new("A"));
        let v1 = Value::Node(NodeId::new(5), Box::new(node.clone()));
        let v2 = Value::NodeRef(NodeId::new(5));
        assert_eq!(v1, v2);
        assert_eq!(v2, v1);

        // Different IDs
        let v3 = Value::NodeRef(NodeId::new(6));
        assert_ne!(v1, v3);

        // Edge == EdgeRef with same ID
        let edge = crate::graph::Edge::new(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let ev1 = Value::Edge(EdgeId::new(1), Box::new(edge));
        let ev2 = Value::EdgeRef(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(ev1, ev2);
        assert_eq!(ev2, ev1);

        // Different types don't equal
        assert_ne!(v1, ev1);
        assert_ne!(Value::Null, v1);

        // Path equality
        let p1 = Value::Path { nodes: vec![NodeId::new(1)], edges: vec![EdgeId::new(1)] };
        let p2 = Value::Path { nodes: vec![NodeId::new(1)], edges: vec![EdgeId::new(1)] };
        let p3 = Value::Path { nodes: vec![NodeId::new(2)], edges: vec![EdgeId::new(1)] };
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_value_hash_cross_variant() {
        use std::collections::hash_map::DefaultHasher;

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Node and NodeRef with same ID should hash the same
        let node = Node::new(NodeId::new(5), Label::new("A"));
        let v1 = Value::Node(NodeId::new(5), Box::new(node));
        let v2 = Value::NodeRef(NodeId::new(5));
        assert_eq!(hash_value(&v1), hash_value(&v2));

        // Edge and EdgeRef with same ID should hash the same
        let edge = crate::graph::Edge::new(
            EdgeId::new(3), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let ev1 = Value::Edge(EdgeId::new(3), Box::new(edge));
        let ev2 = Value::EdgeRef(
            EdgeId::new(3), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(hash_value(&ev1), hash_value(&ev2));

        // Different variant types should have different hashes
        assert_ne!(hash_value(&v1), hash_value(&ev1));
        assert_ne!(hash_value(&Value::Null), hash_value(&v1));
    }
}

/// One `x.prop` read, with the column located once instead of once per row.
///
/// `Value::resolve_property` takes the property name as a `&str` and hashes it
/// against the store's column index on every call. For an operator looping over
/// a million rows evaluating the same expression, that is a million hashes to
/// reach the same column: 37.6 ns per read in scattered order against 22.6 ns
/// when the column is hoisted out of the loop (#557).
///
/// The name is fixed at plan time, so the operator holds one of these per
/// property expression and the lookup happens once.
///
/// Two things it does **not** do, both deliberate:
///
/// * it caches only a *found* column. `None` means no column exists **yet** —
///   a `MERGE` or `SET` later in the same query may create one — so a miss
///   re-resolves rather than being remembered as absent;
/// * it keeps the row-storage fallback. A property whose value has no typed
///   column representation is readable only from the per-node map (#545), and
///   dropping that path would silently return null for complex types.
#[derive(Debug, Clone)]
pub struct PropertyCursor {
    variable: Arc<str>,
    property: Arc<str>,
    node_column: Option<crate::graph::storage::columnar::ColumnId>,
    edge_column: Option<crate::graph::storage::columnar::ColumnId>,
}

impl PropertyCursor {
    pub fn new(variable: impl Into<Arc<str>>, property: impl Into<Arc<str>>) -> Self {
        Self {
            variable: variable.into(),
            property: property.into(),
            node_column: None,
            edge_column: None,
        }
    }

    /// The value of `variable.property` in `record`.
    ///
    /// Equivalent to `record.get(variable).resolve_property(property, store)`,
    /// which is what it falls back to for anything that is not a node or edge.
    pub fn read(&mut self, record: &Record, store: &GraphStore) -> PropertyValue {
        match record.get(&self.variable) {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => {
                let idx = id.as_u64() as usize;
                let column = match self.node_column {
                    Some(id) => Some(id),
                    None => {
                        let found = store.node_columns.column_id(&self.property);
                        self.node_column = found;
                        found
                    }
                };
                if let Some(column) = column {
                    let value = store.node_columns.get_by_id(column, idx);
                    if !value.is_null() {
                        return value;
                    }
                }
                // The column has no value here. Row storage may still.
                match store.get_node(*id) {
                    Some(node) => node.get_property(&self.property).cloned().unwrap_or(PropertyValue::Null),
                    None => PropertyValue::Null,
                }
            }
            Some(Value::EdgeRef(id, ..)) | Some(Value::Edge(id, _)) => {
                let idx = id.as_u64() as usize;
                let column = match self.edge_column {
                    Some(id) => Some(id),
                    None => {
                        let found = store.edge_columns.column_id(&self.property);
                        self.edge_column = found;
                        found
                    }
                };
                if let Some(column) = column {
                    let value = store.edge_columns.get_by_id(column, idx);
                    if !value.is_null() {
                        return value;
                    }
                }
                match store.get_edge(*id) {
                    Some(edge) => edge.get_property(&self.property).cloned().unwrap_or(PropertyValue::Null),
                    None => PropertyValue::Null,
                }
            }
            Some(other) => other.resolve_property(&self.property, store),
            None => PropertyValue::Null,
        }
    }
}

/// One component of a temporal value: `.year`, `.hour`, `.offsetSeconds`, ...
///
/// Absent components are `Null`, which is Cypher's answer — `date.hour` has no
/// meaning and is null rather than zero. Returning zero would read as midnight.
fn temporal_component(v: &PropertyValue, property: &str) -> PropertyValue {
    use chrono::{Datelike, Timelike};
    const DAY_NS: i64 = 86_400 * 1_000_000_000;

    // Split into the date part (days since epoch) and the time part (nanos
    // since midnight), each optional, and answer from those.
    let (days, tod, offset, zone) = match v {
        PropertyValue::Date(d) => (Some(*d as i64), None, None, None),
        PropertyValue::LocalTime(n) => (None, Some(*n), None, None),
        PropertyValue::Time { nanos, offset_seconds } => {
            (None, Some(*nanos), Some(*offset_seconds), None)
        }
        PropertyValue::LocalDateTime { secs, nanos } => (
            Some(secs.div_euclid(86_400)),
            Some(secs.rem_euclid(86_400) * 1_000_000_000 + *nanos as i64),
            None,
            None,
        ),
        PropertyValue::ZonedDateTime { secs, nanos, offset_seconds, zone } => {
            let local = secs + *offset_seconds as i64;
            (
                Some(local.div_euclid(86_400)),
                Some(local.rem_euclid(86_400) * 1_000_000_000 + *nanos as i64),
                Some(*offset_seconds),
                zone.clone(),
            )
        }
        _ => return PropertyValue::Null,
    };

    let date = days.and_then(|d| {
        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::days(d))
    });
    let int = |x: i64| PropertyValue::Integer(x);

    match property {
        "year" => date.map_or(PropertyValue::Null, |d| int(d.year() as i64)),
        "month" => date.map_or(PropertyValue::Null, |d| int(d.month() as i64)),
        "day" => date.map_or(PropertyValue::Null, |d| int(d.day() as i64)),
        "quarter" => date.map_or(PropertyValue::Null, |d| int(((d.month() - 1) / 3 + 1) as i64)),
        "dayOfQuarter" => date.map_or(PropertyValue::Null, |d| {
            let qm = (d.month() - 1) / 3 * 3 + 1;
            let start = chrono::NaiveDate::from_ymd_opt(d.year(), qm, 1);
            start.map_or(PropertyValue::Null, |s| {
                int(d.signed_duration_since(s).num_days() + 1)
            })
        }),
        "week" => date.map_or(PropertyValue::Null, |d| int(d.iso_week().week() as i64)),
        "weekYear" => date.map_or(PropertyValue::Null, |d| int(d.iso_week().year() as i64)),
        // Cypher spells the **accessor** `weekDay` and the **constructor key**
        // `dayOfWeek`, for the same quantity. Only the constructor spelling was
        // here, so `d.weekDay` returned null -- indistinguishable from a
        // component the value does not have, which is what a date's `hour`
        // legitimately returns (#862).
        "weekDay" | "dayOfWeek" => date.map_or(PropertyValue::Null, |d| {
            int(d.weekday().number_from_monday() as i64)
        }),
        "ordinalDay" => date.map_or(PropertyValue::Null, |d| int(d.ordinal() as i64)),

        "hour" => tod.map_or(PropertyValue::Null, |n| int(n / 3_600_000_000_000)),
        "minute" => tod.map_or(PropertyValue::Null, |n| int(n / 60_000_000_000 % 60)),
        "second" => tod.map_or(PropertyValue::Null, |n| int(n / 1_000_000_000 % 60)),
        "millisecond" => tod.map_or(PropertyValue::Null, |n| int(n % 1_000_000_000 / 1_000_000)),
        "microsecond" => tod.map_or(PropertyValue::Null, |n| int(n % 1_000_000_000 / 1_000)),
        "nanosecond" => tod.map_or(PropertyValue::Null, |n| int(n % 1_000_000_000)),

        "offsetSeconds" => offset.map_or(PropertyValue::Null, |o| int(o as i64)),
        "offsetMinutes" => offset.map_or(PropertyValue::Null, |o| int(o as i64 / 60)),
        "offset" => offset.map_or(PropertyValue::Null, |o| {
            PropertyValue::String(crate::graph::property::fmt_offset(o))
        }),
        "timezone" => match zone {
            Some(z) => PropertyValue::String(z),
            None => offset.map_or(PropertyValue::Null, |o| {
                PropertyValue::String(crate::graph::property::fmt_offset(o))
            }),
        },
        "epochMillis" => v.as_epoch_millis().map_or(PropertyValue::Null, int),
        "epochSeconds" => v
            .as_epoch_millis()
            .map_or(PropertyValue::Null, |ms| int(ms.div_euclid(1000))),
        _ => {
            let _ = DAY_NS;
            PropertyValue::Null
        }
    }
}

/// Cypher's orderability over `Value`, for `ORDER BY`.
///
/// openCypher defines one total order across types, ascending:
///
/// ```text
/// Map < Node < Relationship < List < Path < String < Boolean < Number < NaN < null
/// ```
///
/// [`crate::graph::property::cypher_order`] already implements the part of
/// that order a `PropertyValue` can express. It cannot express the rest: a
/// `PropertyValue` holds no node, relationship or path, so sorting through
/// `as_property()` folded every entity to `Null` (#917). The rows still came
/// back — the right rows, in fact — with every node, relationship and path
/// bunched at the null end and indistinguishable from each other and from a
/// missing value. `ORDER BY` reported success and answered wrongly.
///
/// So the comparison has to happen at `Value`, above the property type, and
/// this function is where the three entity ranks live. Everything it can hand
/// to `cypher_order` it hands over, so there is still one implementation of
/// the property half rather than two that drift.
pub fn cypher_order_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Ranks are the ascending order above. `Value::List`/`Value::Map` and
    // their `PropertyValue` spellings are the same type to a query and must
    // rank the same, or `[1]` and a list of nodes sort into different places.
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Map(_) => 0,
            Value::Node(..) | Value::NodeRef(_) => 1,
            Value::Edge(..) | Value::EdgeRef(..) => 2,
            Value::List(_) => 3,
            Value::Path { .. } => 4,
            Value::Null => 9,
            Value::Property(p) => match p {
                PropertyValue::Map(_) => 0,
                PropertyValue::Array(_) | PropertyValue::Vector(_) => 3,
                PropertyValue::String(_) => 5,
                PropertyValue::Boolean(_) => 6,
                PropertyValue::Float(f) if f.is_nan() => 8,
                PropertyValue::Null => 9,
                _ => 7,
            },
        }
    }

    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }

    match (a, b) {
        // Both are properties of the same rank: the existing order decides,
        // including NaN and the element-wise list comparison.
        (Value::Property(x), Value::Property(y)) => crate::graph::property::cypher_order(x, y),
        // Element-wise, then by length — the same rule `cypher_order` applies
        // to `PropertyValue::Array`, restated here because the elements are
        // `Value`s and may be entities.
        (Value::List(x), Value::List(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let c = cypher_order_value(xi, yi);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        // A `Value::Map` against a `PropertyValue::Map`, or a list against an
        // array: same rank, different spelling. Compare by sorted key, then by
        // the value under it, so the two spellings interleave.
        (Value::Map(x), Value::Map(y)) => {
            let (mut xk, mut yk): (Vec<_>, Vec<_>) =
                (x.keys().collect(), y.keys().collect());
            xk.sort();
            yk.sort();
            for (kx, ky) in xk.iter().zip(yk.iter()) {
                let c = kx.cmp(ky);
                if c != Ordering::Equal {
                    return c;
                }
                let c = cypher_order_value(&x[*kx], &y[*ky]);
                if c != Ordering::Equal {
                    return c;
                }
            }
            xk.len().cmp(&yk.len())
        }
        // Entities of the same kind. openCypher leaves the order among them
        // undefined; element id is stable and total, which is what a sort
        // needs. Two runs over the same graph therefore agree.
        _ => entity_id(a).cmp(&entity_id(b)),
    }
}

fn entity_id(v: &Value) -> (u64, usize) {
    match v {
        Value::Node(id, _) | Value::NodeRef(id) => (id.as_u64(), 0),
        Value::Edge(id, _) | Value::EdgeRef(id, ..) => (id.as_u64(), 0),
        // A path has no id; length then first node orders it deterministically.
        Value::Path { nodes, edges } => (
            nodes.first().map(|n| n.as_u64()).unwrap_or(0),
            edges.len(),
        ),
        _ => (0, 0),
    }
}
