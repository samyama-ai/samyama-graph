//! Point an error at the place in the query that caused it (LANG-12).
//!
//! LANG-12 asks for "codes **and** spans on 100%". Codes were at 100% and spans
//! at 3 of 16 -- and all three were grammar errors, where `pest` produces the
//! caret diagram for us. Every semantic and runtime error named the offending
//! variable or function in prose and gave no offset, so a caller with a
//! 400-character query got told that `m` is not bound and had to find `m`.
//!
//! **The span is attached where the query text is in scope.** That is
//! `QueryEngine::execute`/`execute_mut`, which every surface goes through --
//! HTTP, RESP and the CLI. The alternative is threading a span through the AST
//! and every error construction site, which is the right long-term answer and
//! is not what this is: this makes the error useful now, from the token the
//! message already names.
//!
//! **It annotates only when it finds the token.** An error that names nothing
//! locatable is left alone rather than given a position that would be a guess.
//! A span on every error is easy to fake and helps nobody; the number this
//! moves is supposed to mean "the caller can see where".

/// Where a token sits in the query text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// 1-based line.
    pub line: usize,
    /// 1-based column, in characters.
    pub column: usize,
    /// Byte offset from the start of the query.
    pub offset: usize,
    /// Length of the token in bytes.
    pub len: usize,
}

/// The token an error message is about, if it names one.
///
/// Messages name it in one of a few shapes, and each is a deliberate entry
/// rather than a general "find any word": a general extractor pulls English
/// words out of prose and points at whichever one happens to appear in the
/// query, which is worse than no span because it looks authoritative.
///
/// - `` `m` is not bound `` — backticked, the common case
/// - `Unknown procedure: nosuch.procedure` — after a colon
/// - `range() step cannot be 0` — a call, named with its parentheses
pub fn offending_token(message: &str) -> Option<&str> {
    // Candidates with the position they were found at, because the **earliest**
    // one wins. "Unknown algorithm: algo.nope. Available: pageRank(), wcc()"
    // matches the call shape at `pageRank` and the colon shape at `algo.nope`;
    // taking whichever rule ran first pointed the caret at a suggestion.
    let mut candidates: Vec<(usize, &str)> = Vec::new();

    // `` `m` `` -- backticked, the common case.
    if let Some(open) = message.find('`') {
        let rest = &message[open + 1..];
        if let Some(len) = rest.find('`') {
            candidates.push((open, &rest[..len]));
        }
    }

    // `name()` -- a call, named with its parentheses.
    if let Some(idx) = message.find("()") {
        let before = &message[..idx];
        let start = before
            .rfind(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
            .map(|i| i + 1)
            .unwrap_or(0);
        let name = &before[start..];
        if name.chars().next().is_some_and(|c| c.is_alphabetic()) {
            candidates.push((start, name));
        }
    }

    // `Unknown procedure: nosuch.procedure` -- after a colon. Every `: `
    // boundary is tried, not only the last: a message often names the fault
    // after the first colon and offers suggestions after a later one.
    for (i, _) in message.match_indices(": ") {
        let after = &message[i + 2..];
        // Stop at the end of the sentence or the first space, whichever comes
        // first, so a suggestion list is not swept in.
        let end = after
            .find(|c: char| c == ' ' || c == ',')
            .unwrap_or(after.len());
        let t = after[..end].trim_end_matches('.');
        // A *qualified* name only -- it must contain a dot or an underscore.
        // Without that guard the rule takes the first English word after any
        // colon: "Type error: Add requires numeric" offered `Add`, which is a
        // Rust variant name. It happened to be harmless because `Add` is not in
        // the query, and that is exactly the wrong reason for a check to be
        // safe -- a message reading "Type error: Integer expected" against a
        // query with a property called `Integer` would have pointed the caret
        // at it with full confidence.
        if !t.is_empty()
            && (t.contains('.') || t.contains('_'))
            && t.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
            && t.chars().next().is_some_and(|c| c.is_alphabetic())
        {
            candidates.push((i + 2, t));
        }
    }

    candidates
        .into_iter()
        .filter(|(_, t)| !t.is_empty())
        .min_by_key(|(pos, _)| *pos)
        .map(|(_, t)| t)
}

/// Find `needle` in `query` as a whole token.
///
/// Whole-token, so looking for `n` in `MATCH (n) RETURN name` does not land on
/// the `n` of `name`. A token boundary is anything that cannot appear in an
/// identifier -- dots included for `nosuch.procedure`, which is one name.
pub fn locate(query: &str, needle: &str) -> Option<Span> {
    if needle.is_empty() {
        return None;
    }
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    let mut from = 0usize;
    while let Some(rel) = query[from..].find(needle) {
        let start = from + rel;
        let end = start + needle.len();
        let before_ok = start == 0
            || !query[..start].chars().next_back().is_some_and(is_ident);
        let after_ok = end == query.len()
            || !query[end..].chars().next().is_some_and(is_ident);
        if before_ok && after_ok {
            let line = query[..start].matches('\n').count() + 1;
            let line_start = query[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
            let column = query[line_start..start].chars().count() + 1;
            return Some(Span { line, column, offset: start, len: needle.len() });
        }
        from = end;
    }
    None
}

/// The caret diagram, in the shape `pest` already produces for grammar errors,
/// so a caller parsing our errors sees one format rather than two.
///
/// ```text
///  --> 1:18
///   |
/// 1 | MATCH (n) RETURN m.name
///   |                  ^
/// ```
pub fn render(query: &str, span: Span) -> String {
    let line_text = query.lines().nth(span.line - 1).unwrap_or("");
    let gutter = span.line.to_string().len();
    let pad = " ".repeat(gutter);
    let caret_pad = " ".repeat(span.column.saturating_sub(1));
    let carets = "^".repeat(query[span.offset..]
        .chars()
        .take(span.len)
        .count()
        .max(1));
    format!(
        " --> {}:{}\n{pad} |\n{} | {line_text}\n{pad} | {caret_pad}{carets}",
        span.line, span.column, span.line
    )
}

/// Append a span to `message` when the token it names can be found.
///
/// Returns `None` when there is nothing to point at, so the caller can leave
/// the original error untouched.
pub fn annotate(message: &str, query: &str) -> Option<String> {
    // A message that already carries one is left alone: `pest` grammar errors
    // arrive with a diagram, and a second one would be both wrong and noisy.
    if message.contains(" --> ") {
        return None;
    }
    let token = offending_token(message)?;
    let span = locate(query, token)?;
    Some(format!("{message}\n{}", render(query, span)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backticked_token_is_the_one_named() {
        assert_eq!(offending_token("`m` is not bound: nothing"), Some("m"));
    }

    #[test]
    fn a_call_is_named_by_its_parentheses() {
        assert_eq!(offending_token("range() step cannot be 0"), Some("range"));
        assert_eq!(
            offending_token("substring() requires at least 2 arguments"),
            Some("substring")
        );
    }

    #[test]
    fn a_name_after_a_colon_is_taken_when_it_is_one_word() {
        assert_eq!(
            offending_token("Unknown procedure: nosuch.procedure"),
            Some("nosuch.procedure")
        );
    }

    #[test]
    fn a_name_before_a_suggestion_list_is_still_found() {
        // The last colon introduces the suggestions, not the name.
        assert_eq!(
            offending_token("Unknown algorithm: algo.nope. Available: pageRank(), wcc()"),
            Some("algo.nope")
        );
    }

    #[test]
    fn prose_after_a_colon_is_not_a_token() {
        // The guard that stops this pointing at whichever English word happens
        // to appear in the query. A bare word after a colon is prose; only a
        // qualified name is taken.
        assert_eq!(offending_token("Type error: Add requires numeric"), None);
        assert_eq!(offending_token("Type error: Integer expected"), None);
        assert_eq!(offending_token("range() step cannot be 0"), Some("range"));
    }

    #[test]
    fn the_earliest_candidate_wins() {
        // Two rules match this message. The call shape finds `pageRank` in the
        // suggestion list and the colon shape finds the name at fault; the
        // caret must land on the fault, not on the advice.
        assert_eq!(
            offending_token("Unknown algorithm: algo.nope. Available: pageRank(), wcc()"),
            Some("algo.nope")
        );
    }

    #[test]
    fn a_token_is_matched_whole() {
        // `n` must not land inside `name`.
        let q = "MATCH (n) RETURN name";
        let s = locate(q, "n").expect("n");
        assert_eq!(s.offset, 7, "landed at {:?} in {q}", s);
    }

    #[test]
    fn a_token_that_is_not_there_has_no_span() {
        assert!(locate("RETURN 1", "nosuch").is_none());
    }

    #[test]
    fn the_second_line_is_numbered_and_columns_restart() {
        let q = "MATCH (n)\nRETURN m";
        let s = locate(q, "m").expect("m");
        assert_eq!((s.line, s.column), (2, 8));
    }

    #[test]
    fn an_annotated_message_keeps_the_original_and_points_at_the_token() {
        let out = annotate("`m` is not bound", "MATCH (n) RETURN m.name").expect("annotated");
        assert!(out.starts_with("`m` is not bound"));
        assert!(out.contains(" --> 1:18"), "{out}");
        assert!(out.contains("^"), "{out}");
    }

    #[test]
    fn a_message_with_a_span_already_is_left_alone() {
        assert!(annotate("Parse error:  --> 1:10\n  |", "MATCH (n").is_none());
    }

    #[test]
    fn a_message_naming_nothing_locatable_is_left_alone() {
        // Not given a position it cannot justify. This is why the measurement
        // this moves is not 100%: some errors genuinely name no token, and the
        // fix for those is a better message, not a guessed span.
        assert!(annotate("Type error: Add requires numeric or string", "RETURN 1 + {a: 1}").is_none());
    }
}
