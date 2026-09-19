//! The Python type stubs must describe the extension that ships (API-12).
//!
//! `sdk/python` is a PyO3 extension: its surface is defined in Rust and
//! compiled, so a type checker importing `samyama` sees an opaque module.
//! `CH-SDK-RT` measured `python_ships_type_information: False` for that reason,
//! and `sdk/python/samyama-stubs/__init__.pyi` is the answer.
//!
//! A hand-written stub drifts. It drifts silently, and the failure is worse
//! than having none: a caller is told a method exists that does not, or omits
//! an argument and gets a default the stub invented. So the stub is compared
//! against the Rust source here.
//!
//! **This test lives in the main crate on purpose.** `sdk/python` is
//! `exclude`d from the workspace, so `cargo test --workspace` -- which is what
//! CI runs -- does not reach it. A check that cannot run is the thing this
//! repository keeps finding; putting it here means it runs on every PR.
//!
//! It reads source rather than importing the built module, so it needs no
//! maturin, no Python and no compiled extension.

use std::collections::{BTreeMap, BTreeSet};

const LIB: &str = include_str!("../sdk/python/src/lib.rs");
const STUB: &str = include_str!("../sdk/python/samyama-stubs/__init__.pyi");

/// Methods PyO3 exposes, with the defaults declared in `#[pyo3(signature)]`.
///
/// Dunders and private helpers are skipped: `__len__` and `__repr__` are in
/// the stub for the reader's benefit and carry no signature worth comparing.
fn rust_methods() -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut pending_sig: Option<String> = None;
    let mut in_pymethods = false;

    for line in LIB.lines() {
        let t = line.trim();
        if t.starts_with("#[pymethods]") {
            in_pymethods = true;
            continue;
        }
        // A `#[pymethods]` block ends at the impl's closing brace in column 0.
        // Without this the two free helpers after it -- `convert_query_result`
        // and `json_to_py` -- were read as exposed methods, and the test
        // demanded stubs for functions Python never sees.
        if line == "}" {
            in_pymethods = false;
            continue;
        }
        if !in_pymethods {
            continue;
        }
        if let Some(rest) = t.strip_prefix("#[pyo3(signature = (") {
            pending_sig = Some(rest.trim_end_matches("))]").to_string());
            continue;
        }
        if let Some(rest) = t.strip_prefix("fn ") {
            let name = rest.split('(').next().unwrap_or("").trim().to_string();
            let sig = pending_sig.take().unwrap_or_default();
            if name.starts_with("__") || name == "require_embedded" {
                continue;
            }
            let mut defaults = BTreeMap::new();
            for part in sig.split(',') {
                let part = part.trim();
                if let Some((k, v)) = part.split_once('=') {
                    defaults.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
            out.insert(name, defaults);
        }
    }
    out
}

/// Struct fields PyO3 exposes as read-only properties via `#[pyo3(get)]`.
///
/// They reach Python as attributes rather than methods, and the stub declares
/// them with `@property`, so they have to be compared too -- otherwise the
/// stub could promise a `version` attribute the extension does not have.
fn rust_properties() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut pending = false;
    for line in LIB.lines() {
        let t = line.trim();
        if t.starts_with("#[pyo3(get)]") {
            pending = true;
            continue;
        }
        if pending {
            if let Some(name) = t.split(':').next() {
                let name = name.trim().trim_start_matches("pub ").trim();
                if !name.is_empty() {
                    out.insert(name.to_string());
                }
            }
            pending = false;
        }
    }
    out
}

/// Methods the stub declares, with their defaults.
fn stub_methods() -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    // `def name(` may wrap across lines, so join the whole file and split on
    // `def `: a signature that spans five lines is the common case here.
    for chunk in STUB.split("    def ").skip(1) {
        let name = chunk.split('(').next().unwrap_or("").trim().to_string();
        if name.starts_with("__") {
            continue;
        }
        let args: String = chunk
            .chars()
            .skip_while(|c| *c != '(')
            .take_while(|c| *c != ')')
            .collect();
        let mut defaults = BTreeMap::new();
        for part in args.trim_start_matches('(').split(',') {
            let part = part.trim();
            if let Some((lhs, v)) = part.split_once('=') {
                // `damping: float = 0.85` -> key `damping`
                let k = lhs.split(':').next().unwrap_or("").trim();
                if !k.is_empty() {
                    defaults.insert(k.to_string(), v.trim().to_string());
                }
            }
        }
        out.insert(name, defaults);
    }
    out
}

/// Python and Rust spell the same default differently; these are the pairs
/// that mean the same value. Anything not listed has to match literally.
fn same_default(rust: &str, py: &str) -> bool {
    let norm = |s: &str| -> String {
        match s.trim().trim_matches('"') {
            "None" => "none".into(),
            "true" | "True" => "true".into(),
            "false" | "False" => "false".into(),
            other => other.trim_start_matches('"').trim_end_matches('"').to_lowercase(),
        }
    };
    let (a, b) = (norm(rust), norm(py));
    if a == b {
        return true;
    }
    // 1e-6 vs 0.000001, 20 vs 20.0
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => (x - y).abs() < f64::EPSILON * x.abs().max(1.0),
        _ => false,
    }
}

#[test]
fn every_exposed_method_is_in_the_stub() {
    let rust = rust_methods();
    let stub = stub_methods();
    assert!(!rust.is_empty(), "parsed no methods out of lib.rs");

    let missing: BTreeSet<_> = rust.keys().filter(|k| !stub.contains_key(*k)).collect();
    assert!(
        missing.is_empty(),
        "the extension exposes methods the stub does not declare, so a type \
         checker sees them as absent: {missing:?}"
    );
}

#[test]
fn the_stub_declares_nothing_the_extension_does_not_have() {
    // The direction that actively misleads: a stub promising a method that is
    // not there type-checks a call that fails at run time.
    let rust = rust_methods();
    let stub = stub_methods();
    let props = rust_properties();
    let invented: BTreeSet<_> = stub
        .keys()
        .filter(|k| !rust.contains_key(*k) && !props.contains(*k))
        .collect();
    assert!(
        invented.is_empty(),
        "the stub declares methods the extension does not expose: {invented:?}"
    );
}

#[test]
fn the_defaults_agree() {
    // A default that disagrees is worse than a missing stub: the caller omits
    // an argument, gets one value, and is told another.
    let rust = rust_methods();
    let stub = stub_methods();
    let mut wrong = Vec::new();
    for (name, rdefs) in &rust {
        let Some(sdefs) = stub.get(name) else { continue };
        for (arg, rv) in rdefs {
            match sdefs.get(arg) {
                None => wrong.push(format!("{name}({arg}): Rust defaults to {rv}, stub has no default")),
                Some(sv) if !same_default(rv, sv) => {
                    wrong.push(format!("{name}({arg}): Rust {rv} vs stub {sv}"))
                }
                _ => {}
            }
        }
    }
    assert!(wrong.is_empty(), "defaults disagree:\n  {}", wrong.join("\n  "));
}

#[test]
fn the_stub_ships_with_a_py_typed_marker() {
    // Without it a type checker ignores the stub entirely, which is the same
    // as not having written it (PEP 561).
    assert!(
        std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/sdk/python/samyama-stubs/py.typed"
        ))
        .exists(),
        "samyama-stubs/py.typed is missing; PEP 561 says the stubs are then ignored"
    );
}
