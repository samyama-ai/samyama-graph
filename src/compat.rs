//! Which of a corpus of queries this engine accepts, and why not (INT-11).
//!
//! `docs/CYPHER_COMPATIBILITY.md` answers "which features exist". A user
//! deciding whether to migrate is asking something else: will *my* queries
//! run. A matrix of 78 supported features says nothing about the twelve
//! queries in their application that use the two that are missing.
//!
//! The logic lives here rather than in `examples/compatibility_report.rs` so
//! the report and the test that pins it cannot drift apart — two copies of a
//! classifier is how a report starts disagreeing with the suite that checks it.
//!
//! # What "accepted" means
//!
//! The query parses and a plan can be built for it. That is a statement about
//! the language, not about the data and not about the answer: an accepted
//! query can still return the wrong rows, and nothing here would know.
//!
//! Planning runs against whatever store the caller passes, so a refusal that
//! depends on the schema — a declared index, a registered procedure — belongs
//! to that store rather than to the engine. Pass the real one when there is
//! one.

use crate::graph::GraphStore;

/// One query's verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    /// The query, as written.
    pub query: String,
    /// `None` when the query was accepted.
    pub refusal: Option<Refusal>,
}

impl Verdict {
    /// Was the query accepted?
    pub fn accepted(&self) -> bool {
        self.refusal.is_none()
    }
}

/// Why a query was refused.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    /// The engine's error code, e.g. `Samyama.ClientError.Statement.SyntaxError`.
    pub code: String,
    /// The part of the message a reader can act on.
    pub detail: String,
}

impl Refusal {
    /// The last segment of the code, for a table that has to fit on a line.
    pub fn short_code(&self) -> &str {
        self.code.rsplit('.').next().unwrap_or(&self.code)
    }

    /// A grouping key: the detail with quoted specifics removed.
    ///
    /// `Unknown function 'foo'` and `Unknown function 'bar'` are one cause with
    /// two instances, not two causes. Without this the commonest problem in a
    /// corpus reads as hundreds of singletons, and the report is a list rather
    /// than a plan.
    pub fn cause(&self) -> String {
        let mut out = String::new();
        let mut in_quote = false;
        for c in self.detail.chars() {
            match c {
                '\'' | '"' | '`' => {
                    if !in_quote {
                        out.push_str("'…'");
                    }
                    in_quote = !in_quote;
                }
                _ if in_quote => {}
                _ => out.push(c),
            }
        }
        out.split(" -->").next().unwrap_or(&out).trim().to_string()
    }
}

/// Judge one query without executing it.
pub fn judge(query: &str, store: &GraphStore) -> Verdict {
    let refusal = match crate::query::parse_query(query) {
        Err(e) => Some(refusal_from(&e.to_string())),
        Ok(parsed) => {
            let planner = crate::query::executor::planner::QueryPlanner::new();
            match planner.plan(&parsed, store) {
                Ok(_) => None,
                Err(e) => Some(refusal_from(&e.to_string())),
            }
        }
    };
    Verdict {
        query: query.to_string(),
        refusal,
    }
}

/// Judge a whole corpus.
pub fn judge_all(queries: &[String], store: &GraphStore) -> Vec<Verdict> {
    queries.iter().map(|q| judge(q, store)).collect()
}

fn refusal_from(message: &str) -> Refusal {
    let code = message
        .strip_prefix('[')
        .and_then(|r| r.split_once(']'))
        .map(|(code, _)| code.to_string())
        .unwrap_or_else(|| "Unclassified".to_string());
    Refusal {
        code,
        detail: actionable(message),
    }
}

/// The part of an error a reader can act on.
///
/// A parse failure's first line is "Parse error:" and a position; the line that
/// says what was wanted is further down. Grouping on the first line put every
/// syntax error in a corpus into one bucket called "Parse error", and
/// `expected label` and `expected expression` are different migration problems.
fn actionable(message: &str) -> String {
    if let Some(expected) = message
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("= expected"))
    {
        return format!("Parse error: {}", expected.trim_start_matches("= "));
    }
    let head = message.lines().next().unwrap_or("");
    head.split_once("] ")
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| head.to_string())
}

/// Split a corpus file into queries.
///
/// Separated by a `;` at the end of a line, a line containing only `;`, or a
/// blank line. A line starting with `//`, `#` or `--` is a comment.
pub fn split_queries(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") || trimmed.starts_with('#') || trimmed.starts_with("--") {
            continue;
        }
        if trimmed == ";" || trimmed.is_empty() {
            push(&mut out, &mut current);
            continue;
        }
        if let Some(head) = trimmed.strip_suffix(';') {
            current.push_str(head);
            current.push(' ');
            push(&mut out, &mut current);
            continue;
        }
        current.push_str(trimmed);
        current.push(' ');
    }
    push(&mut out, &mut current);
    out
}

fn push(out: &mut Vec<String>, current: &mut String) {
    let q = current.trim();
    if !q.is_empty() {
        out.push(q.to_string());
    }
    current.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_that_parses_and_plans_is_accepted() {
        let store = GraphStore::new();
        assert!(judge("MATCH (p:Person) RETURN p.name", &store).accepted());
    }

    #[test]
    fn a_syntax_error_carries_what_was_expected() {
        let store = GraphStore::new();
        let v = judge("MATCH (p:Person RETURN p", &store);
        let r = v.refusal.expect("must be refused");
        assert!(r.code.contains("SyntaxError"), "{r:?}");
        assert!(
            r.detail.contains("expected"),
            "the detail must say what was wanted, not just that parsing failed: {r:?}"
        );
    }

    #[test]
    fn two_unknown_functions_are_one_cause() {
        // The grouping this exists for. Without it a corpus using ten APOC
        // functions reports ten problems instead of one.
        let a = Refusal { code: "X".into(), detail: "Unknown function 'foo'".into() };
        let b = Refusal { code: "X".into(), detail: "Unknown function 'bar'".into() };
        assert_eq!(a.cause(), b.cause());
        assert!(a.cause().contains("Unknown function"), "{}", a.cause());
    }

    #[test]
    fn the_splitter_handles_the_shapes_a_corpus_comes_in() {
        let text = "// a comment\nMATCH (n) RETURN n;\n\nCREATE (:N)\n;\n# another\nRETURN 1;\n";
        assert_eq!(
            split_queries(text),
            vec![
                "MATCH (n) RETURN n".to_string(),
                "CREATE (:N)".to_string(),
                "RETURN 1".to_string()
            ]
        );
    }

    #[test]
    fn a_multi_line_query_stays_one_query() {
        let text = "MATCH (n:Person)\nWHERE n.age > 30\nRETURN n;\n";
        let qs = split_queries(text);
        assert_eq!(qs.len(), 1, "{qs:?}");
        assert!(qs[0].contains("WHERE"), "{qs:?}");
    }
}
