//! Reading a parsed statement under one language's defaults or the other's.
//!
//! Two readings differ between openCypher and ISO GQL, and neither default can be
//! changed unilaterally without changing the answer to queries users already have:
//!
//! * **The path mode of an unprefixed variable-length pattern.** openCypher specifies
//!   relationship uniqueness, which is TRAIL. ISO/IEC 39075 says that with no
//!   restrictor the matched object is an unrestricted path -- a WALK (#1642).
//! * **What a bare `*` abbreviates.** openCypher reads `{1,}`, GQL reads `{0,}`, so a
//!   ported query silently gains or loses the zero-length path: one row per starting
//!   node, small enough to look like a rounding difference and large enough to change
//!   a COUNT (#1644).
//!
//! So the reading is selected per statement and applied here, after parsing, as a
//! rewrite of the defaults the parser filled in. Anything written explicitly is left
//! alone in both directions: `MATCH TRAIL` stays TRAIL under GQL, `MATCH WALK` stays
//! WALK under Cypher, and `*0..` and `*1..` never move because they are unambiguous in
//! both languages.
//!
//! **Both AST shapes are walked.** `ast::Query` carries by-kind fields for the common
//! case and a `clauses` pipeline for shapes the by-kind grammar cannot express; a pass
//! written against one does nothing for queries that parse into the other, silently.

use crate::query::ast::{Clause, Dialect, PathPattern, PathRestrictor, Query};

/// Rewrite the defaults a parse filled in to the ones `dialect` specifies.
///
/// `Dialect::Cypher` is a no-op by construction: the parser already fills in the
/// openCypher defaults, so nothing is rewritten and no existing caller changes
/// behaviour.
pub fn apply(query: &mut Query, dialect: Dialect) -> Result<(), crate::query::validate::ValidationError> {
    if dialect == Dialect::Cypher {
        return Ok(());
    }
    for mc in &mut query.match_clauses {
        for path in &mut mc.pattern.paths {
            gql_defaults(path);
        }
    }
    for clause in &mut query.clauses {
        if let Clause::Match(mc) = clause {
            for path in &mut mc.pattern.paths {
                gql_defaults(path);
            }
        }
    }
    // Validate *again*, because this pass can make a statement ill-formed that was
    // well-formed when the parser checked it.
    //
    // The parser refuses an unbounded quantifier under WALK (#1141): on a graph with a
    // cycle the candidate set is infinite, and this engine enumerates candidates before
    // a selector gets a turn. `MATCH (x)-[:E*]->(y)` passes that check under openCypher
    // because its restrictor is TRAIL, which is bounded by the edge count -- and then
    // this pass turns it into WALK. Without re-validating, the rewrite smuggles a query
    // past the one check that exists to stop it, and the server hangs rather than
    // answering or refusing (#1648).
    crate::query::validate::validate(query)
}

fn gql_defaults(path: &mut PathPattern) {
    // The restrictor the parser defaulted to is openCypher's. Written out loud it is
    // the user's choice and stands.
    if !path.restrictor_explicit {
        path.restrictor = PathRestrictor::Walk;
    }
    for seg in &mut path.segments {
        if let Some(len) = seg.edge.length.as_mut() {
            if len.bare_star {
                len.min = Some(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_query;

    fn parsed(q: &str) -> Query {
        parse_query(q).unwrap()
    }

    fn first_path(q: &Query) -> &PathPattern {
        &q.match_clauses[0].pattern.paths[0]
    }

    #[test]
    fn cypher_changes_nothing() {
        let before = parsed("MATCH (a)-[:E*]->(b) RETURN a");
        let mut after = before.clone();
        apply(&mut after, Dialect::Cypher).unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn gql_makes_an_unprefixed_pattern_a_walk() {
        let mut q = parsed("MATCH (a)-[:E*1..3]->(b) RETURN a");
        assert_eq!(first_path(&q).restrictor, PathRestrictor::Trail);
        apply(&mut q, Dialect::Gql).unwrap();
        assert_eq!(first_path(&q).restrictor, PathRestrictor::Walk);
    }

    #[test]
    fn gql_leaves_an_explicit_restrictor_alone() {
        for (text, want) in [
            ("MATCH TRAIL (a)-[:E*1..3]->(b) RETURN a", PathRestrictor::Trail),
            ("MATCH ACYCLIC (a)-[:E*1..3]->(b) RETURN a", PathRestrictor::Acyclic),
            ("MATCH SIMPLE (a)-[:E*1..3]->(b) RETURN a", PathRestrictor::Simple),
        ] {
            let mut q = parsed(text);
            apply(&mut q, Dialect::Gql).unwrap();
            assert_eq!(first_path(&q).restrictor, want, "{text}");
        }
    }

    #[test]
    fn gql_moves_only_a_bare_star() {
        let mut bare = parsed("MATCH TRAIL (a)-[:E*]->(b) RETURN a");
        apply(&mut bare, Dialect::Gql).unwrap();
        let seg = &first_path(&bare).segments[0];
        assert_eq!(seg.edge.length.as_ref().unwrap().min, Some(0));

        for text in [
            "MATCH TRAIL (a)-[:E*1..]->(b) RETURN a",
            "MATCH TRAIL (a)-[:E*1..3]->(b) RETURN a",
        ] {
            let mut q = parsed(text);
            apply(&mut q, Dialect::Gql).unwrap();
            let seg = &first_path(&q).segments[0];
            assert_eq!(
                seg.edge.length.as_ref().unwrap().min,
                Some(1),
                "an explicit lower bound is unambiguous in both languages: {text}"
            );
        }
    }

    #[test]
    fn a_fixed_length_pattern_has_no_quantifier_to_move() {
        let mut q = parsed("MATCH (a)-[:E]->(b) RETURN a");
        apply(&mut q, Dialect::Gql).unwrap();
        assert!(first_path(&q).segments[0].edge.length.is_none());
    }
}
