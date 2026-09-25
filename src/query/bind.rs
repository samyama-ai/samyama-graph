//! Turning a caller's parameter values into bound `PropertyValue`s (#1463).
//!
//! Both remote surfaces reach the engine through
//! [`QueryEngine::execute_with_params`](crate::query::QueryEngine::execute_with_params),
//! which hands the values to the executor as bindings. **Nothing here ever
//! builds query text from a value.** That is the whole property the feature
//! exists for: a value of `1 RETURN 1` or `' OR 1=1 --` is a string, not
//! syntax, and it cannot become syntax on a path that never concatenates.
//!
//! The two surfaces differ only in how a value arrives — JSON over HTTP,
//! bytes over RESP — so the typing rules live here once rather than twice.

use crate::graph::PropertyValue;
use std::collections::HashMap;

/// A JSON value as a bound parameter.
///
/// | JSON | `PropertyValue` |
/// |---|---|
/// | integer in `i64` | `Integer` |
/// | other number | `Float` |
/// | `true`/`false` | `Boolean` |
/// | string | `String` — never re-read as Cypher, never as a date |
/// | `null` | `Null` |
/// | array | `Array`, element-wise by this table |
/// | object | `Map`, value-wise by this table |
///
/// A number that is neither an `i64` nor an exactly-typed float — `2^64 - 1`,
/// say — is **refused**. `serde_json` would hand back an `f64` for it, which
/// is a *different number*: binding it would answer a question the caller did
/// not ask, and the caller would have no way to tell. An error naming the
/// parameter is the only outcome that keeps the value the caller sent.
///
/// A string stays a string. No value is sniffed for a date, a number or a
/// boolean: a caller that wants a temporal value passes it to `date()` or
/// `datetime()` in the query, where the conversion is visible in the text.
pub fn property_from_json(v: &serde_json::Value) -> Result<PropertyValue, String> {
    Ok(match v {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if n.is_f64() {
                // `is_f64` is asked before `as_f64` on purpose: `as_f64`
                // succeeds for an out-of-range *integer* too, lossily.
                PropertyValue::Float(n.as_f64().ok_or("number is not representable")?)
            } else {
                return Err(format!(
                    "the number {n} has no exact i64 or f64 representation; \
                     binding it would change the value. Send it as a string \
                     and convert it in the query."
                ));
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Array(items) => PropertyValue::Array(
            items
                .iter()
                .map(property_from_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(fields) => {
            let mut map = HashMap::with_capacity(fields.len());
            for (k, v) in fields {
                map.insert(k.clone(), property_from_json(v)?);
            }
            PropertyValue::Map(map)
        }
    })
}

/// A whole JSON parameter object, with the failing key named in any error.
pub fn properties_from_json(
    params: &HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, PropertyValue>, String> {
    let mut out = HashMap::with_capacity(params.len());
    for (name, value) in params {
        let bound = property_from_json(value).map_err(|e| format!("parameter ${name}: {e}"))?;
        out.insert(name.clone(), bound);
    }
    Ok(out)
}

/// A RESP argument as a bound parameter.
///
/// RESP has no types: every argument is bytes. The value is read as JSON when
/// it parses as JSON — so `1` is an integer, `true` a boolean, `[1,2]` a list,
/// `"12"` the *string* `12` — and as a plain string when it does not, which is
/// what makes `x alice` work without quoting.
///
/// The consequence, stated rather than hidden: `x 12` binds the integer 12,
/// not the string. A caller who wants the string sends `x "12"`. The fallback
/// is what makes the common case ergonomic; the quoting rule is what makes the
/// uncommon case expressible. Either way the value is a value — the text is
/// never spliced into the query, so `x "1 RETURN 1"` and `x 1 RETURN 1` both
/// bind a string and neither adds a clause.
pub fn property_from_resp_text(s: &str) -> PropertyValue {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => property_from_json(&v).unwrap_or_else(|_| PropertyValue::String(s.to_string())),
        Err(_) => PropertyValue::String(s.to_string()),
    }
}

/// Refuse a parameter the query never mentions.
///
/// A key the query does not use is almost always a typo, and a typo is
/// invisible twice over if it is ignored: the value is dropped, and the
/// failure that follows is `Unresolved parameter: $name` — which points at the
/// query, the one part of the request that is right. Naming the unused key
/// puts the error where the mistake is.
///
/// The check is conservative by construction. `referenced_params` scans the
/// text for `$name`, so a `$name` inside a string literal counts as a
/// reference and the supplied value is *accepted*. Over-accepting keeps a
/// valid query running; under-accepting would reject one. It never misses a
/// real reference, because a reference can only be written as `$name`.
///
/// The other direction — referenced but not supplied — is deliberately not
/// checked here. The executor already raises `Unresolved parameter: $name`,
/// which names the parameter and carries a span into the query text.
pub fn reject_unused(query: &str, params: &HashMap<String, PropertyValue>) -> Result<(), String> {
    if params.is_empty() {
        return Ok(());
    }
    let referenced = crate::snapshot::verify::referenced_params(query);
    let mut unused: Vec<&str> = params
        .keys()
        .filter(|k| !referenced.contains(k))
        .map(|k| k.as_str())
        .collect();
    if unused.is_empty() {
        return Ok(());
    }
    // Sorted: a HashMap's order is seeded per process, and an error message
    // that changes between runs is one nobody can test against.
    unused.sort_unstable();
    Err(format!(
        "parameter{} {} {} supplied but the query never uses {}. A key the query \
         does not mention is usually a typo, and ignoring it would drop the value \
         silently.",
        if unused.len() == 1 { "" } else { "s" },
        unused
            .iter()
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(", "),
        if unused.len() == 1 { "was" } else { "were" },
        if unused.len() == 1 { "it" } else { "them" },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_scalars_map_by_type() {
        assert_eq!(
            property_from_json(&json!(7)).unwrap(),
            PropertyValue::Integer(7)
        );
        assert_eq!(
            property_from_json(&json!(-1.5)).unwrap(),
            PropertyValue::Float(-1.5)
        );
        assert_eq!(
            property_from_json(&json!(true)).unwrap(),
            PropertyValue::Boolean(true)
        );
        assert_eq!(
            property_from_json(&json!("s")).unwrap(),
            PropertyValue::String("s".to_string())
        );
        assert_eq!(
            property_from_json(&json!(null)).unwrap(),
            PropertyValue::Null
        );
    }

    #[test]
    fn a_json_integer_stays_an_integer() {
        // 1.0 is a float in JSON and must not be rounded into an Integer:
        // `toString($v)` and integer division both see the difference.
        assert_eq!(
            property_from_json(&json!(1.0)).unwrap(),
            PropertyValue::Float(1.0)
        );
        assert_eq!(
            property_from_json(&json!(i64::MIN)).unwrap(),
            PropertyValue::Integer(i64::MIN)
        );
    }

    #[test]
    fn a_number_outside_i64_is_refused_not_rounded() {
        let v: serde_json::Value = serde_json::from_str("18446744073709551615").unwrap();
        let err = property_from_json(&v).expect_err("2^64-1 was accepted");
        assert!(err.contains("exact"), "{err}");
    }

    #[test]
    fn containers_map_element_wise() {
        assert_eq!(
            property_from_json(&json!([1, "a", null])).unwrap(),
            PropertyValue::Array(vec![
                PropertyValue::Integer(1),
                PropertyValue::String("a".to_string()),
                PropertyValue::Null,
            ])
        );
        let nested = property_from_json(&json!({"outer": {"inner": 42}})).unwrap();
        let PropertyValue::Map(outer) = nested else {
            panic!("not a map");
        };
        let PropertyValue::Map(inner) = &outer["outer"] else {
            panic!("nesting lost");
        };
        assert_eq!(inner["inner"], PropertyValue::Integer(42));
    }

    #[test]
    fn resp_text_is_json_when_it_parses_and_a_string_when_it_does_not() {
        assert_eq!(property_from_resp_text("1"), PropertyValue::Integer(1));
        assert_eq!(
            property_from_resp_text("true"),
            PropertyValue::Boolean(true)
        );
        assert_eq!(
            property_from_resp_text("alice"),
            PropertyValue::String("alice".to_string())
        );
        assert_eq!(
            property_from_resp_text("\"12\""),
            PropertyValue::String("12".to_string())
        );
        assert_eq!(
            property_from_resp_text("1 RETURN 1"),
            PropertyValue::String("1 RETURN 1".to_string())
        );
        // Out of i64 range: the JSON reading fails, and the text is bound as
        // the string it is rather than as a different number.
        assert_eq!(
            property_from_resp_text("18446744073709551615"),
            PropertyValue::String("18446744073709551615".to_string())
        );
    }

    #[test]
    fn an_unused_parameter_is_named() {
        let mut p = HashMap::new();
        p.insert("nmae".to_string(), PropertyValue::Integer(1));
        let err = reject_unused("RETURN $name", &p).expect_err("accepted");
        assert!(err.contains("$nmae"), "{err}");
    }

    #[test]
    fn a_used_parameter_passes() {
        let mut p = HashMap::new();
        p.insert("name".to_string(), PropertyValue::Integer(1));
        assert!(reject_unused("RETURN $name", &p).is_ok());
    }
}
