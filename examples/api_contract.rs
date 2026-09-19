//! The OpenAPI document and the HTTP server describe the same API (API-01).
//!
//! A documented endpoint the server does not serve is a broken client. A
//! served endpoint nobody documented is an undocumented surface. Neither was
//! checked by anything until #613 found one phantom endpoint and five
//! undocumented ones — by hand, once.
//!
//! This is the same check, as a per-commit gate, so the two cannot drift
//! apart again. It is deliberately static: it reads the document and greps the
//! server source, and does **not** exercise a running server, which is what
//! API-01 ultimately asks for.
//!
//!     cargo run --release --example api_contract [-- --json out.json]
//!
//! Exits non-zero when the two disagree, so CI fails on drift.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The shipped surfaces. Their absence is a packaging failure, not a drift.
const SURFACES: &[(&str, &str)] = &[
    ("rust_sdk", "crates/samyama-sdk/src/lib.rs"),
    ("python_sdk", "sdk/python/pyproject.toml"),
    ("typescript_sdk", "sdk/typescript/package.json"),
    ("cli", "cli/Cargo.toml"),
    ("openapi", "api/openapi.yaml"),
];

/// Collapse path parameters so the two spellings compare equal.
///
/// The document writes `/api/tenants/{id}`; the server registers
/// `/api/tenants/:id`. Comparing them literally reports endpoints that are
/// served perfectly well as missing — a false alarm, which is the failure mode
/// that gets a check switched off.
fn normalise(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                for c in chars.by_ref() {
                    if c == '}' {
                        break;
                    }
                }
                out.push_str("{}");
            }
            ':' => {
                while chars.peek().is_some_and(|c| c.is_alphanumeric() || *c == '_') {
                    chars.next();
                }
                out.push_str("{}");
            }
            _ => out.push(c),
        }
    }
    out
}

/// Endpoint paths declared under the document's `paths:` block.
fn documented_paths(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_paths = false;
    for line in text.lines() {
        if line.starts_with("paths:") {
            in_paths = true;
            continue;
        }
        if in_paths {
            // A new top-level key ends the block.
            if !line.is_empty() && !line.starts_with(char::is_whitespace) {
                break;
            }
            if let Some(rest) = line.strip_prefix("  /") {
                if let Some(path) = rest.split(':').next() {
                    out.insert(format!("/{path}"));
                }
            }
        }
    }
    out
}

/// Endpoints the server registers, read out of the source.
fn served_paths(src: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut stack = vec![src.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let Ok(text) = std::fs::read_to_string(&path) else { continue };
                // `:` and `{}` belong in the character set: the server
                // registers path parameters as `/api/tenants/:id`, and leaving
                // `:` out makes every parameterised route look unserved.
                // Every `route("/...")`, not only the ones under `/api/`.
                // Scanning for the `/api/` prefix made any route outside it
                // invisible in both directions: `/metrics` read as documented
                // but not served, and an *undocumented* route outside `/api/`
                // would never have been reported at all. The check is about
                // the surface, and the surface is not defined by a prefix.
                let mut rest = text.as_str();
                while let Some(i) = rest.find("route(") {
                    rest = &rest[i + "route(".len()..];
                    // The path may sit on the next line: `rustfmt` wraps
                    // `.route(\n    "/api/tenants/:id",\n    ...)`. Looking for
                    // `route("` on one span missed exactly that one, and it is
                    // the only parameterised route in the server.
                    let head = &rest[..rest.len().min(120)];
                    let Some(q) = head.find('"') else { continue };
                    if head[..q].chars().any(|c| !c.is_whitespace()) {
                        continue; // something other than the path came first
                    }
                    let after = &rest[q + 1..];
                    let end = after.find('"').unwrap_or(0);
                    let candidate = &after[..end];
                    if candidate.starts_with('/')
                        && candidate
                            .chars()
                            .all(|c| c.is_alphanumeric() || "/_-:{}.".contains(c))
                    {
                        out.insert(candidate.to_string());
                    }
                }
            }
        }
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut failures: Vec<String> = Vec::new();

    println!("API contract");
    for (name, rel) in SURFACES {
        let present = repo.join(rel).exists();
        println!("  {:<16} {:<48} {}", name, rel, if present { "present" } else { "MISSING" });
        if !present {
            failures.push(format!("surface {name} missing at {rel}"));
        }
    }

    let openapi = repo.join("api/openapi.yaml");
    let documented: BTreeSet<String> = std::fs::read_to_string(&openapi)
        .map(|t| documented_paths(&t))
        .unwrap_or_default();
    let served = served_paths(&repo.join("src"));

    let documented_n: BTreeSet<String> = documented.iter().map(|p| normalise(p)).collect();
    let served_n: BTreeSet<String> = served.iter().map(|p| normalise(p)).collect();
    let unserved: Vec<&String> = documented_n.difference(&served_n).collect();
    let undocumented: Vec<&String> = served_n.difference(&documented_n).collect();

    println!("\n  documented endpoints  {}", documented_n.len());
    println!("  served endpoints      {}", served_n.len());
    println!("  agreeing              {}", documented_n.intersection(&served_n).count());

    if !unserved.is_empty() {
        println!("\n  documented but not served: {unserved:?}");
        failures.push(format!("{} documented endpoint(s) the server does not register", unserved.len()));
    }
    if !undocumented.is_empty() {
        println!("  served but undocumented:   {undocumented:?}");
        failures.push(format!("{} served endpoint(s) missing from the document", undocumented.len()));
    }

    if let Some(path) = json_out {
        let json = serde_json::json!({
            "surfaces_present": SURFACES.iter().filter(|(_, r)| repo.join(r).exists()).count(),
            "surfaces_total": SURFACES.len(),
            "documented_endpoints": documented_n.len(),
            "served_endpoints": served_n.len(),
            "agreeing": documented_n.intersection(&served_n).count(),
            "documented_not_served": unserved,
            "served_not_documented": undocumented,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();
    }

    if failures.is_empty() {
        println!("\nthe document and the server agree");
    } else {
        println!("\nAPI contract broken:");
        for f in &failures {
            println!("  - {f}");
        }
        std::process::exit(1);
    }
}
