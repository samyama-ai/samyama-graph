//! Parse queries from stdin, one per line; print `ok` or `fail` per line.
//!
//! A pipe rather than a flag because the useful question is rarely "does this
//! one query parse" — it is "which of these two hundred variants parse", asked
//! by a script that is bisecting a construct. Starting a process per query
//! makes that unbearably slow.
//!
//!   printf '%s\n' 'MATCH (n) RETURN n' 'CREATE ()' | cargo run --release --example parse_check

use samyama::query::parser::parse_query;
use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            let _ = writeln!(out, "skip");
            continue;
        }
        let verdict = match parse_query(&line) {
            Ok(_) => "ok",
            Err(_) => "fail",
        };
        let _ = writeln!(out, "{verdict}");
    }
}
