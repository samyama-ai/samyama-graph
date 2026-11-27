//! LANG-10: register a function and a procedure from Rust, then call them
//! from Cypher (#1674, ADR-044).
//!
//! The names are the ones `CH-UDF` probes a server with -- `RETURN myfn(1)`
//! and `CALL myfn()` -- registered the only way H1 asks for: from a Rust
//! program that embeds the engine. Each call is judged by its answer, not by
//! whether it parses, and the names are also checked to be unknown before
//! they are registered, so a pass cannot come from a name that was always
//! there.
//!
//! ```text
//! cargo run --release --example udf_registration -- --json udf.json
//! ```

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::Value;
use samyama::query::QueryEngine;

struct Call {
    query: &'static str,
    /// The value of the first column of the only row a working engine returns.
    expect: Value,
}

fn first_value(engine: &QueryEngine, store: &GraphStore, query: &str) -> Result<Value, String> {
    let batch = engine.execute(query, store).map_err(|e| e.to_string())?;
    let column = batch.columns.first().ok_or("no columns")?.clone();
    match batch.records.as_slice() {
        [row] => row
            .get(&column)
            .cloned()
            .ok_or_else(|| format!("no `{column}`")),
        rows => Err(format!("{} rows, expected 1", rows.len())),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "CREATE (:P {name: 'a'}), (:P {name: 'b'}), (:Q {name: 'c'})",
            &mut store,
            "default",
        )
        .expect("fixture");

    let calls = [
        Call {
            query: "RETURN myfn(1) AS x",
            expect: Value::Property(PropertyValue::Integer(2)),
        },
        Call {
            query: "CALL myfn() YIELD nodes RETURN nodes",
            expect: Value::Property(PropertyValue::Integer(3)),
        },
    ];

    // Before: neither name means anything.
    let before: Vec<bool> = calls
        .iter()
        .map(|c| first_value(&engine, &store, c.query).is_err())
        .collect();

    samyama::udf::register_function("myfn", 1..=1, |args| match &args[0] {
        PropertyValue::Integer(n) => Ok(PropertyValue::Integer(n + 1)),
        other => Err(format!("expected an integer, got {other}")),
    })
    .expect("register the function");
    samyama::udf::register_procedure("myfn", 0..=0, &["nodes"], |store, _| {
        Ok(vec![vec![
            PropertyValue::Integer(store.node_count() as i64),
        ]])
    })
    .expect("register the procedure");

    let mut results = Vec::new();
    let mut working = 0;
    for (call, unknown_before) in calls.iter().zip(&before) {
        let got = first_value(&engine, &store, call.query);
        let works = *unknown_before && got.as_ref() == Ok(&call.expect);
        working += usize::from(works);
        println!(
            "{:<5} {:<40} unknown before: {unknown_before}; got {}",
            if works { "works" } else { "FAILS" },
            call.query,
            match &got {
                Ok(v) => format!("{v:?}"),
                Err(e) => e.lines().next().unwrap_or("").to_string(),
            }
        );
        results.push(serde_json::json!({
            "query": call.query,
            "unknown_before_registration": unknown_before,
            "works": works,
            "got": match &got { Ok(v) => format!("{v:?}"), Err(e) => e.clone() },
        }));
    }
    println!("{working} of {} registered calls work", calls.len());

    if let Some(path) = json_out {
        let doc = serde_json::json!({
            "requirement": "LANG-10",
            "registration": "samyama::udf::register_function / register_procedure (Rust)",
            "functions": samyama::udf::function_names(),
            "procedures": samyama::udf::procedure_names(),
            "calls": results,
            "working": working,
            "total": calls.len(),
        });
        std::fs::write(&path, serde_json::to_string_pretty(&doc).expect("json"))
            .expect("write the report");
    }
    if working != calls.len() {
        std::process::exit(1);
    }
}
