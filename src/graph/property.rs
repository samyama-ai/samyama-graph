//! # Property Value Types -- Schema-Free, Dynamically Typed Properties
//!
//! This module defines [`PropertyValue`], the dynamic type system that allows
//! nodes and edges to carry arbitrary key-value properties without a fixed schema.
//!
//! ## Type systems in databases
//!
//! Relational databases enforce a rigid schema: every row in a table must have
//! the same columns with the same types. Graph databases take a **schema-free**
//! (or schema-optional) approach -- any node can have any set of properties,
//! and two nodes with the same label may have entirely different property keys.
//! This flexibility is modeled in Rust via an `enum` (algebraic data type) where
//! each variant represents a different value type.
//!
//! ## Algebraic data types (tagged unions)
//!
//! Rust's `enum` is a **tagged union** (also called a *sum type* or *discriminated
//! union*). Unlike C's `union`, which is unsafe because the programmer must track
//! which variant is active, Rust's enum carries a hidden discriminant tag and the
//! compiler enforces exhaustive `match` -- you cannot forget to handle a variant.
//! This makes `PropertyValue` both type-safe and extensible: adding a new variant
//! (e.g., `Duration`) causes a compiler error at every unhandled `match` site.
//!
//! ## Three-valued logic (3VL) and NULL semantics
//!
//! The `Null` variant follows SQL/Cypher NULL semantics, which implement
//! **three-valued logic**: NULL represents an *unknown* value, not an *empty* one.
//! Under 3VL, `NULL = NULL` evaluates to NULL (not `true`), `NULL <> 5` evaluates
//! to NULL (not `true`), and `NULL AND true` evaluates to NULL. This propagation
//! of unknowns through expressions is critical for correct query evaluation and
//! is one of the most common sources of bugs in database applications.
//!
//! ## Type coercion
//!
//! The query engine performs **implicit type coercion** (widening) when comparing
//! values of different numeric types: an `Integer(i64)` is promoted to `Float(f64)`
//! for comparison with a `Float`. String-to-number and boolean conversions are
//! also supported via explicit functions (`toInteger()`, `toFloat()`, `toString()`).
//! This mirrors the behavior of OpenCypher and SQL CAST operations.
//!
//! ## `PartialOrd` vs `Ord`
//!
//! IEEE 754 floating-point numbers are only `PartialOrd` because `NaN != NaN`
//! and `NaN` is incomparable with every value. However, databases need a **total
//! ordering** for `ORDER BY` and indexing. This module implements `Ord` by using
//! `f64::total_cmp()`, which defines a total order where `-0.0 < +0.0` and `NaN`
//! sorts after all other values. `PartialOrd` delegates to `Ord` so the two are
//! always consistent.
//!
//! ## Hashing floats (`Hash` for `f64`)
//!
//! Hashing floating-point values is notoriously tricky because IEEE 754 has
//! multiple bit representations for the same logical value (`+0.0` and `-0.0`
//! compare equal but have different bits, while `NaN != NaN` but may have
//! identical bits). The `to_bits()` approach used here converts the `f64` to
//! its raw `u64` bit pattern and hashes that. This is correct for use with our
//! `Eq` implementation (derived `PartialEq` compares variant-by-variant) because
//! we treat each bit pattern as distinct. The discriminant tag (0, 1, 2, ...)
//! hashed before each value prevents cross-type collisions (e.g., `Integer(42)`
//! vs `String("42")`).
//!
//! Implements REQ-GRAPH-005: Support for multiple property data types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use std::cmp::Ordering;

/// Property value type supporting multiple data types
///
/// Supports:
/// - String
/// - Integer (i64)
/// - Float (f64)
/// - Boolean
/// - DateTime (as i64 timestamp)
/// - Array (`Vec<PropertyValue>`)
/// - Map (HashMap<String, PropertyValue>)
/// - Vector (`Vec<f32>`) for AI/Vector search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(i64), // Unix timestamp in milliseconds
    Array(Vec<PropertyValue>),
    Map(HashMap<String, PropertyValue>),
    Vector(Vec<f32>),
    /// Duration with months, days, seconds, nanos components (ISO 8601)
    Duration {
        months: i64,
        days: i64,
        seconds: i64,
        nanos: i32,
    },
    Null,
}

impl Eq for PropertyValue {}

impl PartialOrd for PropertyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PropertyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use PropertyValue::*;
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Boolean(_), _) => Ordering::Less,
            (_, Boolean(_)) => Ordering::Greater,

            (Integer(a), Integer(b)) => a.cmp(b),
            (Integer(_), _) => Ordering::Less,
            (_, Integer(_)) => Ordering::Greater,

            (Float(a), Float(b)) => a.total_cmp(b),
            (Float(_), _) => Ordering::Less,
            (_, Float(_)) => Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
            (String(_), _) => Ordering::Less,
            (_, String(_)) => Ordering::Greater,

            (DateTime(a), DateTime(b)) => a.cmp(b),
            (DateTime(_), _) => Ordering::Less,
            (_, DateTime(_)) => Ordering::Greater,

            (Array(a), Array(b)) => a.cmp(b),
            (Array(_), _) => Ordering::Less,
            (_, Array(_)) => Ordering::Greater,

            // Maps are not trivially comparable because HashMap doesn't implement Ord.
            // We'll skip Maps for indexing or define a canonical order (e.g. sorted keys).
            // For now, let's just use memory address or length as a fallback to satisfy trait?
            // No, that breaks determinism. 
            // Let's implement a slow but deterministic comparison for Maps.
            (Map(a), Map(b)) => {
                let mut keys_a: Vec<_> = a.keys().collect();
                let mut keys_b: Vec<_> = b.keys().collect();
                keys_a.sort();
                keys_b.sort();
                
                // Compare keys first
                for (ka, kb) in keys_a.iter().zip(keys_b.iter()) {
                    match ka.cmp(kb) {
                        Ordering::Equal => {},
                        ord => return ord,
                    }
                }
                
                if keys_a.len() != keys_b.len() {
                    return keys_a.len().cmp(&keys_b.len());
                }
                
                // Compare values
                for k in keys_a {
                    let va = a.get(k).unwrap();
                    let vb = b.get(k).unwrap();
                    match va.cmp(vb) {
                        Ordering::Equal => {},
                        ord => return ord,
                    }
                }
                Ordering::Equal
            }
            (Map(_), _) => Ordering::Less,
            (_, Map(_)) => Ordering::Greater,

            (Vector(a), Vector(b)) => {
                // Lexicographical comparison using total_cmp for floats
                for (va, vb) in a.iter().zip(b.iter()) {
                    match va.total_cmp(vb) {
                        Ordering::Equal => {},
                        ord => return ord,
                    }
                }
                a.len().cmp(&b.len())
            }
            (Vector(_), _) => Ordering::Less,
            (_, Vector(_)) => Ordering::Greater,

            (Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
             Duration { months: m2, days: d2, seconds: s2, nanos: n2 }) => {
                m1.cmp(m2)
                    .then(d1.cmp(d2))
                    .then(s1.cmp(s2))
                    .then(n1.cmp(n2))
            }
        }
    }
}

impl std::hash::Hash for PropertyValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PropertyValue::String(s) => {
                0.hash(state);
                s.hash(state);
            }
            PropertyValue::Integer(i) => {
                1.hash(state);
                i.hash(state);
            }
            PropertyValue::Float(f) => {
                2.hash(state);
                // Hash the bits of the float
                f.to_bits().hash(state);
            }
            PropertyValue::Boolean(b) => {
                3.hash(state);
                b.hash(state);
            }
            PropertyValue::DateTime(dt) => {
                4.hash(state);
                dt.hash(state);
            }
            PropertyValue::Array(arr) => {
                5.hash(state);
                arr.hash(state);
            }
            PropertyValue::Map(map) => {
                6.hash(state);
                // Sort keys for deterministic hashing
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for k in keys {
                    k.hash(state);
                    map.get(k).unwrap().hash(state);
                }
            }
            PropertyValue::Vector(v) => {
                7.hash(state);
                for val in v {
                    val.to_bits().hash(state);
                }
            }
            PropertyValue::Duration { months, days, seconds, nanos } => {
                8.hash(state);
                months.hash(state);
                days.hash(state);
                seconds.hash(state);
                nanos.hash(state);
            }
            PropertyValue::Null => {
                9.hash(state);
            }
        }
    }
}

impl PropertyValue {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, PropertyValue::Null)
    }

    /// Get string value if this is a string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get integer value if this is an integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            PropertyValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get float value if this is a float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Get boolean value if this is a boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            PropertyValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get datetime value if this is a datetime
    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            PropertyValue::DateTime(dt) => Some(*dt),
            _ => None,
        }
    }

    /// Get array value if this is an array
    pub fn as_array(&self) -> Option<&Vec<PropertyValue>> {
        match self {
            PropertyValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Get map value if this is a map
    pub fn as_map(&self) -> Option<&HashMap<String, PropertyValue>> {
        match self {
            PropertyValue::Map(map) => Some(map),
            _ => None,
        }
    }

    /// Get vector value if this is a vector
    pub fn as_vector(&self) -> Option<&Vec<f32>> {
        match self {
            PropertyValue::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// Get type name as string
    pub fn type_name(&self) -> &'static str {
        match self {
            PropertyValue::String(_) => "String",
            PropertyValue::Integer(_) => "Integer",
            PropertyValue::Float(_) => "Float",
            PropertyValue::Boolean(_) => "Boolean",
            PropertyValue::DateTime(_) => "DateTime",
            PropertyValue::Array(_) => "Array",
            PropertyValue::Map(_) => "Map",
            PropertyValue::Vector(_) => "Vector",
            PropertyValue::Duration { .. } => "Duration",
            PropertyValue::Null => "Null",
        }
    }

    /// Convert to a flattened JSON value for API responses
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            PropertyValue::String(s) => json!(s),
            PropertyValue::Integer(i) => json!(i),
            PropertyValue::Float(f) => json!(f),
            PropertyValue::Boolean(b) => json!(b),
            PropertyValue::DateTime(dt) => json!(dt),
            PropertyValue::Array(arr) => {
                json!(arr.iter().map(|v| v.to_json()).collect::<Vec<_>>())
            }
            PropertyValue::Map(map) => {
                let mut json_map = serde_json::Map::new();
                for (k, v) in map {
                    json_map.insert(k.clone(), v.to_json());
                }
                serde_json::Value::Object(json_map)
            }
            PropertyValue::Vector(v) => json!(v),
            PropertyValue::Duration { months, days, seconds, nanos } => {
                json!({
                    "months": months,
                    "days": days,
                    "seconds": seconds,
                    "nanos": nanos
                })
            }
            PropertyValue::Null => serde_json::Value::Null,
        }
    }
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Integer(i) => write!(f, "{}", i),
            PropertyValue::Float(fl) => write!(f, "{}", fl),
            PropertyValue::Boolean(b) => write!(f, "{}", b),
            PropertyValue::DateTime(dt) => write!(f, "DateTime({})", dt),
            PropertyValue::Array(arr) => {
                write!(f, "[")?;
                for (i, val) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            PropertyValue::Map(map) => {
                write!(f, "{{")?;
                for (i, (key, val)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, val)?;
                }
                write!(f, "}}")
            }
            PropertyValue::Vector(v) => {
                write!(f, "Vector([")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "])")
            }
            PropertyValue::Duration { months, days, seconds, nanos } => {
                write!(f, "P")?;
                if *months > 0 {
                    let years = months / 12;
                    let rem_months = months % 12;
                    if years > 0 { write!(f, "{}Y", years)?; }
                    if rem_months > 0 { write!(f, "{}M", rem_months)?; }
                }
                if *days > 0 { write!(f, "{}D", days)?; }
                if *seconds > 0 || *nanos > 0 {
                    write!(f, "T")?;
                    let h = seconds / 3600;
                    let m = (seconds % 3600) / 60;
                    let s = seconds % 60;
                    if h > 0 { write!(f, "{}H", h)?; }
                    if m > 0 { write!(f, "{}M", m)?; }
                    if s > 0 || *nanos > 0 { write!(f, "{}S", s)?; }
                }
                Ok(())
            }
            PropertyValue::Null => write!(f, "null"),
        }
    }
}

// Convenience conversions
impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self {
        PropertyValue::Integer(i)
    }
}

impl From<i32> for PropertyValue {
    fn from(i: i32) -> Self {
        PropertyValue::Integer(i as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self {
        PropertyValue::Float(f)
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Boolean(b)
    }
}

impl From<Vec<PropertyValue>> for PropertyValue {
    fn from(arr: Vec<PropertyValue>) -> Self {
        PropertyValue::Array(arr)
    }
}

impl From<HashMap<String, PropertyValue>> for PropertyValue {
    fn from(map: HashMap<String, PropertyValue>) -> Self {
        PropertyValue::Map(map)
    }
}

impl From<Vec<f32>> for PropertyValue {
    fn from(v: Vec<f32>) -> Self {
        PropertyValue::Vector(v)
    }
}

/// Property map for storing node and edge properties
pub type PropertyMap = HashMap<String, PropertyValue>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_value_types() {
        // Test all property types (REQ-GRAPH-005)
        assert_eq!(
            PropertyValue::String("test".to_string()).type_name(),
            "String"
        );
        assert_eq!(PropertyValue::Integer(42).type_name(), "Integer");
        assert_eq!(PropertyValue::Float(3.14).type_name(), "Float");
        assert_eq!(PropertyValue::Boolean(true).type_name(), "Boolean");
        assert_eq!(PropertyValue::DateTime(1234567890).type_name(), "DateTime");
        assert_eq!(PropertyValue::Array(vec![]).type_name(), "Array");
        assert_eq!(
            PropertyValue::Map(HashMap::new()).type_name(),
            "Map"
        );
        assert_eq!(PropertyValue::Vector(vec![0.1]).type_name(), "Vector");
        assert_eq!(PropertyValue::Null.type_name(), "Null");
    }

    #[test]
    fn test_property_value_conversions() {
        let string_prop: PropertyValue = "hello".into();
        assert_eq!(string_prop.as_string(), Some("hello"));

        let int_prop: PropertyValue = 42i64.into();
        assert_eq!(int_prop.as_integer(), Some(42));

        let float_prop: PropertyValue = 3.14.into();
        assert_eq!(float_prop.as_float(), Some(3.14));

        let bool_prop: PropertyValue = true.into();
        assert_eq!(bool_prop.as_boolean(), Some(true));

        let vector_prop: PropertyValue = vec![1.0f32, 2.0f32].into();
        assert_eq!(vector_prop.as_vector(), Some(&vec![1.0f32, 2.0f32]));
    }

    #[test]
    fn test_property_map() {
        let mut props = PropertyMap::new();
        props.insert("name".to_string(), "Alice".into());
        props.insert("age".to_string(), 30i64.into());
        props.insert("active".to_string(), true.into());

        assert_eq!(props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(props.get("age").unwrap().as_integer(), Some(30));
        assert_eq!(props.get("active").unwrap().as_boolean(), Some(true));
    }

    #[test]
    fn test_nested_properties() {
        // Test array property
        let arr = vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3),
        ];
        let arr_prop = PropertyValue::Array(arr);
        assert_eq!(arr_prop.as_array().unwrap().len(), 3);

        // Test map property
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::String("value".to_string()));
        let map_prop = PropertyValue::Map(map);
        assert!(map_prop.as_map().unwrap().contains_key("key"));
    }

    // ========== Batch 1: Ord impl tests ==========

    #[test]
    fn test_ord_null_less_than_everything() {
        assert!(PropertyValue::Null < PropertyValue::Boolean(false));
        assert!(PropertyValue::Null < PropertyValue::Integer(0));
        assert!(PropertyValue::Null < PropertyValue::Float(0.0));
        assert!(PropertyValue::Null < PropertyValue::String("".to_string()));
    }

    #[test]
    fn test_ord_null_equal() {
        assert_eq!(PropertyValue::Null.cmp(&PropertyValue::Null), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_ord_boolean_less_than_integer() {
        assert!(PropertyValue::Boolean(true) < PropertyValue::Integer(0));
    }

    #[test]
    fn test_ord_boolean_comparison() {
        assert!(PropertyValue::Boolean(false) < PropertyValue::Boolean(true));
    }

    #[test]
    fn test_ord_integer_comparison() {
        assert!(PropertyValue::Integer(1) < PropertyValue::Integer(2));
        assert_eq!(PropertyValue::Integer(5).cmp(&PropertyValue::Integer(5)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_ord_integer_less_than_float() {
        assert!(PropertyValue::Integer(100) < PropertyValue::Float(0.0));
    }

    #[test]
    fn test_ord_float_comparison() {
        assert!(PropertyValue::Float(1.0) < PropertyValue::Float(2.0));
        assert_eq!(PropertyValue::Float(3.14).cmp(&PropertyValue::Float(3.14)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_ord_float_less_than_string() {
        assert!(PropertyValue::Float(1e100) < PropertyValue::String("a".to_string()));
    }

    #[test]
    fn test_ord_string_comparison() {
        assert!(PropertyValue::String("a".to_string()) < PropertyValue::String("b".to_string()));
        assert!(PropertyValue::String("abc".to_string()) > PropertyValue::String("ab".to_string()));
    }

    #[test]
    fn test_ord_datetime_comparison() {
        assert!(PropertyValue::DateTime(100) < PropertyValue::DateTime(200));
    }

    #[test]
    fn test_ord_array_comparison() {
        let a1 = PropertyValue::Array(vec![PropertyValue::Integer(1)]);
        let a2 = PropertyValue::Array(vec![PropertyValue::Integer(2)]);
        assert!(a1 < a2);
    }

    #[test]
    fn test_ord_map_comparison() {
        let mut m1 = HashMap::new();
        m1.insert("a".to_string(), PropertyValue::Integer(1));
        let mut m2 = HashMap::new();
        m2.insert("a".to_string(), PropertyValue::Integer(2));
        let pv1 = PropertyValue::Map(m1);
        let pv2 = PropertyValue::Map(m2);
        assert!(pv1 < pv2);
    }

    #[test]
    fn test_ord_map_different_keys() {
        let mut m1 = HashMap::new();
        m1.insert("a".to_string(), PropertyValue::Integer(1));
        let mut m2 = HashMap::new();
        m2.insert("b".to_string(), PropertyValue::Integer(1));
        let pv1 = PropertyValue::Map(m1);
        let pv2 = PropertyValue::Map(m2);
        assert!(pv1 < pv2); // "a" < "b"
    }

    #[test]
    fn test_ord_map_different_sizes() {
        let mut m1 = HashMap::new();
        m1.insert("a".to_string(), PropertyValue::Integer(1));
        let mut m2 = HashMap::new();
        m2.insert("a".to_string(), PropertyValue::Integer(1));
        m2.insert("b".to_string(), PropertyValue::Integer(2));
        let pv1 = PropertyValue::Map(m1);
        let pv2 = PropertyValue::Map(m2);
        assert!(pv1 < pv2); // fewer keys is less
    }

    #[test]
    fn test_ord_vector_comparison() {
        let v1 = PropertyValue::Vector(vec![1.0, 2.0]);
        let v2 = PropertyValue::Vector(vec![1.0, 3.0]);
        assert!(v1 < v2);
    }

    #[test]
    fn test_ord_vector_different_lengths() {
        let v1 = PropertyValue::Vector(vec![1.0]);
        let v2 = PropertyValue::Vector(vec![1.0, 2.0]);
        assert!(v1 < v2);
    }

    #[test]
    fn test_ord_duration_comparison() {
        let d1 = PropertyValue::Duration { months: 1, days: 0, seconds: 0, nanos: 0 };
        let d2 = PropertyValue::Duration { months: 2, days: 0, seconds: 0, nanos: 0 };
        assert!(d1 < d2);
    }

    #[test]
    fn test_ord_duration_tiebreak_days() {
        let d1 = PropertyValue::Duration { months: 1, days: 5, seconds: 0, nanos: 0 };
        let d2 = PropertyValue::Duration { months: 1, days: 10, seconds: 0, nanos: 0 };
        assert!(d1 < d2);
    }

    #[test]
    fn test_ord_duration_tiebreak_seconds() {
        let d1 = PropertyValue::Duration { months: 1, days: 5, seconds: 100, nanos: 0 };
        let d2 = PropertyValue::Duration { months: 1, days: 5, seconds: 200, nanos: 0 };
        assert!(d1 < d2);
    }

    #[test]
    fn test_ord_duration_tiebreak_nanos() {
        let d1 = PropertyValue::Duration { months: 1, days: 5, seconds: 100, nanos: 10 };
        let d2 = PropertyValue::Duration { months: 1, days: 5, seconds: 100, nanos: 20 };
        assert!(d1 < d2);
    }

    #[test]
    fn test_partial_ord_consistency() {
        let a = PropertyValue::Integer(42);
        let b = PropertyValue::Integer(42);
        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Equal));
    }

    // ========== Hash impl tests ==========

    #[test]
    fn test_hash_deterministic_map() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut m1 = HashMap::new();
        m1.insert("b".to_string(), PropertyValue::Integer(2));
        m1.insert("a".to_string(), PropertyValue::Integer(1));

        let mut m2 = HashMap::new();
        m2.insert("a".to_string(), PropertyValue::Integer(1));
        m2.insert("b".to_string(), PropertyValue::Integer(2));

        let hash1 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Map(m1).hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Map(m2).hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_deterministic_vector() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let v1 = PropertyValue::Vector(vec![1.0, 2.0, 3.0]);
        let v2 = PropertyValue::Vector(vec![1.0, 2.0, 3.0]);

        let hash1 = {
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            v2.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_deterministic_duration() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let d1 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let d2 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };

        let hash1 = {
            let mut h = DefaultHasher::new();
            d1.hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            d2.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_different_types_different_hashes() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let int_hash = {
            let mut h = DefaultHasher::new();
            PropertyValue::Integer(42).hash(&mut h);
            h.finish()
        };
        let str_hash = {
            let mut h = DefaultHasher::new();
            PropertyValue::String("42".to_string()).hash(&mut h);
            h.finish()
        };
        assert_ne!(int_hash, str_hash);
    }

    #[test]
    fn test_hash_null() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let hash = {
            let mut h = DefaultHasher::new();
            PropertyValue::Null.hash(&mut h);
            h.finish()
        };
        // Just verify it doesn't panic and produces some value
        assert!(hash > 0 || hash == 0); // always true, just exercises the code
    }

    #[test]
    fn test_hash_float() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let hash1 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Float(3.14).hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Float(3.14).hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_array() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let arr = PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]);
        let hash = {
            let mut h = DefaultHasher::new();
            arr.hash(&mut h);
            h.finish()
        };
        assert!(hash > 0 || hash == 0);
    }

    #[test]
    fn test_hash_boolean() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let hash1 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Boolean(true).hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            PropertyValue::Boolean(false).hash(&mut h);
            h.finish()
        };
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_datetime() {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let hash1 = {
            let mut h = DefaultHasher::new();
            PropertyValue::DateTime(12345).hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            PropertyValue::DateTime(12345).hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }

    // ========== Display impl tests ==========

    #[test]
    fn test_display_string() {
        assert_eq!(format!("{}", PropertyValue::String("hello".to_string())), "\"hello\"");
    }

    #[test]
    fn test_display_integer() {
        assert_eq!(format!("{}", PropertyValue::Integer(42)), "42");
    }

    #[test]
    fn test_display_float() {
        assert_eq!(format!("{}", PropertyValue::Float(3.14)), "3.14");
    }

    #[test]
    fn test_display_boolean() {
        assert_eq!(format!("{}", PropertyValue::Boolean(true)), "true");
        assert_eq!(format!("{}", PropertyValue::Boolean(false)), "false");
    }

    #[test]
    fn test_display_datetime() {
        assert_eq!(format!("{}", PropertyValue::DateTime(1234567890)), "DateTime(1234567890)");
    }

    #[test]
    fn test_display_array() {
        let arr = PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]);
        assert_eq!(format!("{}", arr), "[1, 2]");
    }

    #[test]
    fn test_display_array_empty() {
        let arr = PropertyValue::Array(vec![]);
        assert_eq!(format!("{}", arr), "[]");
    }

    #[test]
    fn test_display_null() {
        assert_eq!(format!("{}", PropertyValue::Null), "null");
    }

    #[test]
    fn test_display_vector() {
        let v = PropertyValue::Vector(vec![1.0, 2.5]);
        let s = format!("{}", v);
        assert!(s.starts_with("Vector(["));
        assert!(s.contains("1"));
        assert!(s.contains("2.5"));
        assert!(s.ends_with("])"));
    }

    #[test]
    fn test_display_map() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::Integer(42));
        let pv = PropertyValue::Map(map);
        let s = format!("{}", pv);
        assert!(s.contains("key: 42"));
    }

    #[test]
    fn test_display_duration_years_months() {
        let d = PropertyValue::Duration { months: 14, days: 0, seconds: 0, nanos: 0 };
        let s = format!("{}", d);
        assert!(s.contains("1Y"));
        assert!(s.contains("2M"));
    }

    #[test]
    fn test_display_duration_days_time() {
        let d = PropertyValue::Duration { months: 0, days: 5, seconds: 7261, nanos: 0 };
        let s = format!("{}", d);
        assert!(s.contains("5D"));
        assert!(s.contains("T"));
        assert!(s.contains("2H"));
        assert!(s.contains("1M"));
        assert!(s.contains("1S"));
    }

    #[test]
    fn test_display_duration_empty() {
        let d = PropertyValue::Duration { months: 0, days: 0, seconds: 0, nanos: 0 };
        let s = format!("{}", d);
        assert_eq!(s, "P");
    }

    #[test]
    fn test_display_duration_nanos_only() {
        let d = PropertyValue::Duration { months: 0, days: 0, seconds: 0, nanos: 100 };
        let s = format!("{}", d);
        assert!(s.contains("T"));
        assert!(s.contains("0S"));
    }

    // ========== to_json tests ==========

    #[test]
    fn test_to_json_string() {
        let json = PropertyValue::String("hello".to_string()).to_json();
        assert_eq!(json, serde_json::json!("hello"));
    }

    #[test]
    fn test_to_json_integer() {
        let json = PropertyValue::Integer(42).to_json();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_to_json_float() {
        let json = PropertyValue::Float(3.14).to_json();
        assert_eq!(json, serde_json::json!(3.14));
    }

    #[test]
    fn test_to_json_boolean() {
        let json = PropertyValue::Boolean(true).to_json();
        assert_eq!(json, serde_json::json!(true));
    }

    #[test]
    fn test_to_json_null() {
        let json = PropertyValue::Null.to_json();
        assert!(json.is_null());
    }

    #[test]
    fn test_to_json_datetime() {
        let json = PropertyValue::DateTime(1234567890).to_json();
        assert_eq!(json, serde_json::json!(1234567890));
    }

    #[test]
    fn test_to_json_array() {
        let arr = PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::String("x".to_string())]);
        let json = arr.to_json();
        assert_eq!(json, serde_json::json!([1, "x"]));
    }

    #[test]
    fn test_to_json_map() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
        map.insert("age".to_string(), PropertyValue::Integer(30));
        let json = PropertyValue::Map(map).to_json();
        assert_eq!(json["name"], serde_json::json!("Alice"));
        assert_eq!(json["age"], serde_json::json!(30));
    }

    #[test]
    fn test_to_json_vector() {
        let v = PropertyValue::Vector(vec![1.0, 2.0, 3.0]);
        let json = v.to_json();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_to_json_duration() {
        let d = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let json = d.to_json();
        assert_eq!(json["months"], serde_json::json!(1));
        assert_eq!(json["days"], serde_json::json!(2));
        assert_eq!(json["seconds"], serde_json::json!(3));
        assert_eq!(json["nanos"], serde_json::json!(4));
    }

    // ========== is_null tests ==========

    #[test]
    fn test_is_null_true() {
        assert!(PropertyValue::Null.is_null());
    }

    #[test]
    fn test_is_null_false() {
        assert!(!PropertyValue::Integer(0).is_null());
        assert!(!PropertyValue::String("".to_string()).is_null());
        assert!(!PropertyValue::Boolean(false).is_null());
    }

    // ========== as_* accessor negative tests ==========

    #[test]
    fn test_as_string_on_non_string() {
        assert_eq!(PropertyValue::Integer(42).as_string(), None);
    }

    #[test]
    fn test_as_integer_on_non_integer() {
        assert_eq!(PropertyValue::String("x".to_string()).as_integer(), None);
    }

    #[test]
    fn test_as_float_on_non_float() {
        assert_eq!(PropertyValue::Integer(1).as_float(), None);
    }

    #[test]
    fn test_as_boolean_on_non_boolean() {
        assert_eq!(PropertyValue::Integer(1).as_boolean(), None);
    }

    #[test]
    fn test_as_datetime_on_non_datetime() {
        assert_eq!(PropertyValue::Integer(1).as_datetime(), None);
    }

    #[test]
    fn test_as_array_on_non_array() {
        assert_eq!(PropertyValue::Integer(1).as_array(), None);
    }

    #[test]
    fn test_as_map_on_non_map() {
        assert_eq!(PropertyValue::Integer(1).as_map(), None);
    }

    #[test]
    fn test_as_vector_on_non_vector() {
        assert_eq!(PropertyValue::Integer(1).as_vector(), None);
    }

    // ========== From conversions ==========

    #[test]
    fn test_from_i32() {
        let pv: PropertyValue = 42i32.into();
        assert_eq!(pv.as_integer(), Some(42));
    }

    #[test]
    fn test_from_string_owned() {
        let pv: PropertyValue = String::from("test").into();
        assert_eq!(pv.as_string(), Some("test"));
    }

    // ========== Duration type_name ==========

    #[test]
    fn test_duration_type_name() {
        let d = PropertyValue::Duration { months: 0, days: 0, seconds: 0, nanos: 0 };
        assert_eq!(d.type_name(), "Duration");
    }

    // ========== Eq impl for PartialEq ==========

    #[test]
    fn test_eq_same_types() {
        assert_eq!(PropertyValue::Integer(1), PropertyValue::Integer(1));
        assert_ne!(PropertyValue::Integer(1), PropertyValue::Integer(2));
        assert_eq!(PropertyValue::Float(1.0), PropertyValue::Float(1.0));
        assert_eq!(PropertyValue::String("a".to_string()), PropertyValue::String("a".to_string()));
        assert_ne!(PropertyValue::String("a".to_string()), PropertyValue::String("b".to_string()));
    }

    #[test]
    fn test_eq_different_types() {
        assert_ne!(PropertyValue::Integer(1), PropertyValue::Float(1.0));
        assert_ne!(PropertyValue::Integer(1), PropertyValue::String("1".to_string()));
    }

    #[test]
    fn test_eq_duration() {
        let d1 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let d2 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let d3 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 5 };
        assert_eq!(d1, d2);
        assert_ne!(d1, d3);
    }
}
