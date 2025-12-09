//! Property value types for graph nodes and edges
//!
//! Implements REQ-GRAPH-005: Support for multiple property data types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Property value type supporting multiple data types
///
/// Supports:
/// - String
/// - Integer (i64)
/// - Float (f64)
/// - Boolean
/// - DateTime (as i64 timestamp)
/// - Array (Vec<PropertyValue>)
/// - Map (HashMap<String, PropertyValue>)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(i64), // Unix timestamp in milliseconds
    Array(Vec<PropertyValue>),
    Map(HashMap<String, PropertyValue>),
    Null,
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
            PropertyValue::Null => "Null",
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
}
