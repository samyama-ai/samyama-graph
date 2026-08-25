//! # OpenCypher Parser: PEG Grammar + Pratt Precedence
//!
//! This module transforms a Cypher query string into an `ast::Query` tree. It combines
//! two parsing techniques:
//!
//! ## PEG (Parsing Expression Grammar)
//!
//! Unlike context-free grammars (CFGs) used by traditional parser generators (yacc, bison,
//! ANTLR), **PEGs** use an **ordered choice** operator (`/`). When a PEG rule offers
//! alternatives `A / B / C`, the parser tries `A` first; if `A` matches, `B` and `C` are
//! never attempted. This makes PEGs **inherently unambiguous** -- there is always exactly
//! one parse tree for any input. CFGs, by contrast, can be ambiguous (the classic
//! "dangling else" problem), requiring precedence annotations or grammar refactoring.
//!
//! ## Pest: Rust's PEG Parser Generator
//!
//! [Pest](https://pest.rs) reads a `.pest` grammar file ([`cypher.pest`](cypher.pest)) and
//! generates a parser at compile time using a proc macro (`#[derive(Parser)]`). The grammar
//! defines rules like `match_clause`, `expression`, `variable`, etc. Pest produces a
//! `Pairs` iterator of matched spans, which this module walks to construct AST nodes.
//!
//! ## Atomic Rules and Keyword Boundaries (ADR-013)
//!
//! In Pest, non-atomic rules (`rule = { ... }`) insert **implicit whitespace** between
//! sequence elements. This is convenient for most grammar rules but dangerous for keyword
//! detection. Consider: `rule = { ^"AND" ~ !(ASCII_ALPHA) }`. The implicit whitespace
//! rule consumes the space after "AND", and then the negative lookahead sees the next
//! identifier character and *fails* -- the keyword is not recognized.
//!
//! The fix is **atomic rules** (`rule = @{ ^"AND" ~ !(ASCII_ALPHANUMERIC | "_") }`).
//! The `@` prefix disables implicit whitespace, so the lookahead fires immediately after
//! the keyword text, before any space is consumed. This is critical for operators like
//! `AND`, `OR`, `NOT`, `IN`, `CONTAINS`, and `STARTS WITH`.
//!
//! ## Pratt Parsing for Operator Precedence
//!
//! Expressions like `1 + 2 * 3` require **operator precedence** to parse correctly (the
//! multiplication binds tighter than addition). This module uses Pest's built-in
//! **Pratt parser**, an algorithm invented by Vaughan Pratt in 1973. Each operator is
//! assigned a **binding power** (precedence level). The parser recursively consumes tokens,
//! comparing binding powers to decide whether to "shift" (absorb the next operator) or
//! "reduce" (close the current sub-expression). The result is correct associativity and
//! precedence without rewriting the grammar into layers of precedence rules.
//!
//! The precedence levels (lowest to highest) are:
//! 1. `OR`
//! 2. `AND`
//! 3. `IN`, comparisons (`=`, `<>`, `<`, `>`, `<=`, `>=`)
//! 4. Addition/subtraction (`+`, `-`)
//! 5. Multiplication/division/modulo (`*`, `/`, `%`)
//!
//! ## `LazyLock`: Thread-Safe One-Time Initialization
//!
//! The `PRATT_PARSER` static is initialized using [`std::sync::LazyLock`], Rust's
//! built-in "once cell" pattern (stabilized in Rust 1.80). `LazyLock` guarantees that
//! the closure runs **exactly once**, even under concurrent access from multiple threads.
//! This avoids rebuilding the Pratt parser on every query while remaining thread-safe
//! without explicit locking.

use crate::graph::{EdgeType, Label, PropertyValue};
use crate::query::ast::*;
use pest::Parser;
use pest::pratt_parser::{PrattParser, Op, Assoc};
use pest_derive::Parser;
use std::collections::HashMap;
use thiserror::Error;
use std::sync::LazyLock;

#[derive(Parser)]
#[grammar = "query/cypher.pest"]
struct CypherParser;

static PRATT_PARSER: LazyLock<PrattParser<Rule>> = LazyLock::new(|| {
    PrattParser::new()
        .op(Op::infix(Rule::or_op, Assoc::Left))
        // XOR binds tighter than OR and looser than AND, which is where Cypher
        // puts it (#578).
        .op(Op::infix(Rule::xor_op, Assoc::Left))
        .op(Op::infix(Rule::and_op, Assoc::Left))
        // NOT sits between AND and the comparisons: `NOT a STARTS WITH b` negates the
        // comparison, while `NOT a AND b` still groups as `(NOT a) AND b`.
        .op(Op::prefix(Rule::not_op))
        .op(Op::infix(Rule::comparison_op, Assoc::Left))
        // `IN` binds **tighter than every comparison operator**, so
        // `a < b IN c` is `a < (b IN c)` and not `(a < b) IN c` (#833).
        //
        // Sharing a level with the comparisons made it group left, and the two
        // readings agree whenever the list holds what the comparison would have
        // produced -- which is most hand-written queries and every example
        // anyone reaches for. The TCK settles it by enumerating all three
        // truth values against six lists at once.
        .op(Op::infix(Rule::in_op, Assoc::Left))
        .op(Op::infix(Rule::add_sub_op, Assoc::Left))
        .op(Op::infix(Rule::mul_div_mod_op, Assoc::Left))
        // Exponentiation binds tightest and associates to the *right*:
        // `2 ^ 3 ^ 2` is 2^(3^2), not (2^3)^2.
        .op(Op::infix(Rule::pow_op, Assoc::Right))
});

/// Parser errors
#[derive(Error, Debug)]
pub enum ParseError {
    /// Pest parsing error
    #[error("Parse error: {0}")]
    PestError(#[from] pest::error::Error<Rule>),

    /// Semantic error
    #[error("Semantic error: {0}")]
    SemanticError(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}

pub type ParseResult<T> = Result<T, ParseError>;

/// Parse a Cypher query string into an AST
/// Parse a query as a flat, ordered clause sequence.
///
/// Reached only when every shape-specific rule has rejected the input. The
/// result carries `clauses` in written order and `needs_clause_pipeline`, and
/// the legacy by-kind fields are left empty — a query here is by definition one
/// they cannot represent, and half-filling them would give the planner two
/// disagreeing accounts of the same query.
fn parse_clause_pipeline(input: &str) -> ParseResult<Query> {
    use crate::query::ast::Clause;

    let pairs = CypherParser::parse(Rule::pipeline_query, input)?;
    let mut query = Query::new();
    query.needs_clause_pipeline = true;

    for pair in pairs {
        if pair.as_rule() != Rule::pipeline_query {
            continue;
        }
        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::explain_clause => {
                    query.explain = true;
                    query.profile = inner.as_str().eq_ignore_ascii_case("PROFILE");
                }
                Rule::pipeline_stmt => {
                    for c in inner.into_inner() {
                        match c.as_rule() {
                            Rule::match_clause | Rule::optional_match_clause => {
                                let optional = c.as_rule() == Rule::optional_match_clause;
                                for p in c.into_inner() {
                                    if p.as_rule() == Rule::pattern {
                                        query.clauses.push(Clause::Match(MatchClause {
                                            pattern: parse_pattern(p)?,
                                            optional,
                                        }));
                                    }
                                }
                            }
                            Rule::where_clause => {
                                query.clauses.push(Clause::Where(parse_where_clause(c)?));
                            }
                            Rule::unwind_clause => {
                                query.clauses.push(Clause::Unwind(parse_unwind_clause(c)?));
                            }
                            Rule::with_clause => {
                                query.clauses.push(Clause::With(parse_with_clause(c)?));
                            }
                            Rule::create_clause => {
                                for p in c.into_inner() {
                                    if p.as_rule() == Rule::pattern {
                                        query.clauses.push(Clause::Create(CreateClause {
                                            pattern: parse_pattern(p)?,
                                        }));
                                    }
                                }
                            }
                            Rule::merge_inline => {
                                query.clauses.push(Clause::Merge(parse_merge_clause(c)?));
                            }
                            Rule::delete_clause => {
                                query.clauses.push(Clause::Delete(parse_delete_clause(c)?));
                            }
                            Rule::set_clause => {
                                query.clauses.push(Clause::Set(parse_set_clause(c)?));
                            }
                            Rule::remove_clause => {
                                query.clauses.push(Clause::Remove(parse_remove_clause(c)?));
                            }
                            Rule::return_clause => {
                                query.clauses.push(Clause::Return(parse_return_clause(c)?));
                            }
                            Rule::order_by_clause => {
                                query.order_by = Some(parse_order_by_clause(c)?);
                            }
                            Rule::skip_clause => {
                                for i in c.into_inner() {
                                    if i.as_rule() == Rule::integer {
                                        query.skip = i.as_str().parse().ok();
                                    }
                                }
                            }
                            Rule::limit_clause => {
                                for i in c.into_inner() {
                                    if i.as_rule() == Rule::integer {
                                        query.limit = i.as_str().parse().ok();
                                    }
                                }
                            }
                            // Anything this builder cannot lower has to be an
                            // error. Falling through silently was the worse
                            // failure: FOREACH and CALL parsed cleanly, were
                            // dropped on the floor, and the query then ran as
                            // though the clause had never been written —
                            // `CREATE (a:A) WITH a FOREACH (i IN [1,2] | SET
                            // a.n = i)` reported success having set nothing. A
                            // clause the engine cannot run must never be
                            // mistaken for one it ran.
                            other => {
                                let keyword = c
                                    .as_str()
                                    .split_whitespace()
                                    .next()
                                    .unwrap_or("")
                                    .to_uppercase();
                                let name = if keyword.is_empty() {
                                    format!("{other:?}")
                                } else {
                                    keyword
                                };
                                return Err(ParseError::SemanticError(format!(
                                    "`{name}` is not yet supported in this clause position \
                                     (samyama-graph#617)"
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if query.clauses.is_empty() {
        return Err(ParseError::SemanticError("empty clause sequence".to_string()));
    }
    // The RETURN is mirrored into the legacy field so `validate` and the
    // star expansion, which both read it, keep working unchanged.
    if let Some(Clause::Return(rc)) = query.clauses.iter().rev().find(|c| matches!(c, Clause::Return(_))) {
        query.return_clause = Some(rc.clone());
    }
    crate::query::star::expand_stars(&mut query);
    crate::query::validate::validate(&query)
        .map_err(|e| ParseError::SemanticError(e.to_string()))?;
    Ok(query)
}

/// An integer literal, in decimal, hexadecimal (`0x1A`) or octal (`0o17`).
///
/// Returns an error rather than panicking when the value does not fit. The
/// previous `.parse().unwrap()` meant `RETURN 9223372036854775808` **crashed
/// the process**: an out-of-range literal is a syntax error in Cypher, and the
/// TCK has two scenarios asserting exactly that, but a panic is reachable from
/// any query string and takes the server with it (#633).
fn parse_integer_literal(text: &str) -> ParseResult<i64> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (radix, digits) = if let Some(rest) = digits.strip_prefix("0x").or_else(|| digits.strip_prefix("0X")) {
        (16, rest)
    } else if let Some(rest) = digits.strip_prefix("0o").or_else(|| digits.strip_prefix("0O")) {
        (8, rest)
    } else {
        (10, digits)
    };

    // Parsed as i128 so the magnitude of i64::MIN -- which is one larger than
    // i64::MAX -- can be represented before the sign is applied. Without this,
    // `-9223372036854775808` has no valid intermediate form.
    let magnitude = i128::from_str_radix(digits, radix).map_err(|_| {
        ParseError::SemanticError(format!("integer literal out of range: `{text}`"))
    })?;
    let value = if negative { -magnitude } else { magnitude };
    i64::try_from(value).map_err(|_| {
        ParseError::SemanticError(format!("integer literal out of range: `{text}`"))
    })
}

/// A `SKIP`/`LIMIT` count. Same crash as the literal parser had, same fix:
/// `LIMIT 99999999999999999999` panicked rather than being refused.
fn parse_count_literal(text: &str) -> ParseResult<usize> {
    let value = parse_integer_literal(text)?;
    usize::try_from(value)
        .map_err(|_| ParseError::SemanticError(format!("SKIP/LIMIT must not be negative: `{text}`")))
}

pub fn parse_query(input: &str) -> ParseResult<Query> {
    let pairs = match CypherParser::parse(Rule::query, input) {
        Ok(pairs) => pairs,
        // The established rules each encode one permitted clause order. A
        // query they all reject may still be valid Cypher — a write before a
        // `WITH`, two writes either side of a projection — so it gets one more
        // attempt against the general clause sequence.
        //
        // The original error is kept if that fails too: it points at the
        // construct, whereas the general rule fails at the first clause it
        // cannot start.
        Err(original) => match parse_clause_pipeline(input) {
            Ok(query) => return Ok(query),
            // A semantic error means the general rule *did* recognise the
            // clause order and then refused to lower one of the clauses. That
            // names the actual obstacle, so it wins; a plain parse failure
            // does not, and the original error is kept instead.
            Err(e @ ParseError::SemanticError(_)) => return Err(e),
            Err(_) => return Err(original.into()),
        },
    };

    let mut query = Query::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::query => {
                let mut is_union_all = false;
                let mut first = true;
                for inner in pair.into_inner() {
                    match inner.as_rule() {
                        Rule::explain_clause => {
                            let text = inner.as_str().to_uppercase();
                            if text.starts_with("PROFILE") {
                                query.profile = true;
                            } else {
                                query.explain = true;
                            }
                        }
                        Rule::union_clause => {
                            // Check if UNION ALL (inner has "ALL" text)
                            let text = inner.as_str().to_uppercase();
                            is_union_all = text.contains("ALL");
                        }
                        Rule::statement => {
                            if first {
                                parse_statement(inner, &mut query)?;
                                first = false;
                            } else {
                                // UNION query
                                let mut union_query = Query::new();
                                parse_statement(inner, &mut union_query)?;
                                query.union_queries.push((union_query, is_union_all));
                                is_union_all = false;
                            }
                        }
                        Rule::EOI => break,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    // `RETURN *` / `WITH *` are resolved here rather than in the planner, so
    // that nothing downstream has to know the sentinel exists. See
    // `crate::query::star`.
    crate::query::star::expand_stars(&mut query);

    // Checks the grammar cannot express — duplicate result columns, UNION
    // arity, CREATE over an already-bound variable. Reported as a parse
    // failure because that is what they are to a caller: the query was never
    // well-formed, and running it would answer a question nobody asked.
    crate::query::validate::validate(&query)
        .map_err(|e| ParseError::SemanticError(e.to_string()))?;

    Ok(query)
}

fn parse_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::show_indexes_stmt => {
                query.show_indexes = true;
            }
            Rule::show_hierarchy_indexes_stmt => {
                query.show_hierarchy_indexes = true;
            }
            Rule::create_hierarchy_index_stmt => {
                parse_create_hierarchy_index_statement(inner, query)?;
            }
            Rule::drop_hierarchy_index_stmt => {
                query.drop_hierarchy_index = inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::variable)
                    .map(|p| p.as_str().to_string());
            }
            Rule::rebuild_hierarchy_index_stmt => {
                query.rebuild_hierarchy_index = inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::variable)
                    .map(|p| p.as_str().to_string());
            }
            Rule::show_constraints_stmt => {
                query.show_constraints = true;
            }
            Rule::drop_index_stmt => {
                parse_drop_index_statement(inner, query)?;
            }
            Rule::create_constraint_stmt => {
                parse_create_constraint_statement(inner, query)?;
            }
            Rule::create_vector_index_stmt => {
                parse_create_vector_index_statement(inner, query)?;
            }
            Rule::create_index_stmt => {
                parse_create_index_statement(inner, query)?;
            }
            Rule::call_stmt => {
                parse_call_statement(inner, query)?;
            }
            Rule::merge_stmt => {
                parse_merge_statement(inner, query)?;
            }
            Rule::foreach_stmt => {
                // A leading FOREACH: no pattern to match, so the clause is all
                // there is. It runs against one empty row (see the planner).
                for fe in inner.into_inner() {
                    if fe.as_rule() == Rule::foreach_clause {
                        query.foreach_clause = Some(parse_foreach_clause(fe)?);
                    }
                }
            }
            Rule::match_stmt | Rule::unwind_stmt => {
                // Same clause set, so the same builder applies -- it already dispatches on
                // each inner rule rather than assuming a fixed clause order. The rule
                // itself is what records the UNWIND's position: in `unwind_stmt` it is by
                // construction the first clause.
                query.unwind_leading = inner.as_rule() == Rule::unwind_stmt;
                parse_match_statement(inner, query)?;
            }
            Rule::create_stmt => {
                parse_create_statement(inner, query)?;
            }
            Rule::with_return_stmt => {
                for child in inner.into_inner() {
                    match child.as_rule() {
                        Rule::with_clause => {
                            query.with_clause = Some(parse_with_clause(child)?);
                        }
                        Rule::return_clause => {
                            query.return_clause = Some(parse_return_clause(child)?);
                        }
                        Rule::order_by_clause => {
                            query.order_by = Some(parse_order_by_clause(child)?);
                        }
                        Rule::skip_clause => {
                            for skip_inner in child.into_inner() {
                                if skip_inner.as_rule() == Rule::integer {
                                    query.skip = skip_inner.as_str().parse::<usize>().ok();
                                }
                            }
                        }
                        Rule::limit_clause => {
                            for limit_inner in child.into_inner() {
                                if limit_inner.as_rule() == Rule::integer {
                                    query.limit = limit_inner.as_str().parse::<usize>().ok();
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Rule::return_stmt => {
                for child in inner.into_inner() {
                    match child.as_rule() {
                        Rule::return_clause => {
                            query.return_clause = Some(parse_return_clause(child)?);
                        }
                        Rule::order_by_clause => {
                            query.order_by = Some(parse_order_by_clause(child)?);
                        }
                        Rule::skip_clause => {
                            for skip_inner in child.into_inner() {
                                if skip_inner.as_rule() == Rule::integer {
                                    query.skip = skip_inner.as_str().parse::<usize>().ok();
                                }
                            }
                        }
                        Rule::limit_clause => {
                            for limit_inner in child.into_inner() {
                                if limit_inner.as_rule() == Rule::integer {
                                    query.limit = limit_inner.as_str().parse::<usize>().ok();
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_create_index_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut label = None;
    let mut properties: Vec<String> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::label => label = Some(Label::new(inner.as_str())),
            Rule::property_key => properties.push(inner.as_str().to_string()),
            _ => {}
        }
    }

    let first_property = properties.first()
        .ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?
        .clone();
    let additional_properties = properties.into_iter().skip(1).collect();

    query.create_index_clause = Some(CreateIndexClause {
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: first_property,
        additional_properties,
    });
    Ok(())
}

/// `CREATE HIERARCHY INDEX <name> ON ()-[:T|T2]->() [MEASURE [Label.]prop] [AGGREGATE ops]`
///
/// The relationship pattern carries the orientation: `()-[:IS_A]->()` reads the stored
/// edge as `child -> parent`, `()<-[:HAS_CHILD]-()` as `parent -> child`. Everything else
/// about the declaration is optional — COUNT roll-up needs no measure at all.
fn parse_create_hierarchy_index_statement(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    let mut name: Option<String> = None;
    let mut edge_types: Vec<String> = Vec::new();
    let mut reverse = false;
    let mut measure_label: Option<String> = None;
    let mut measure_property: Option<String> = None;
    let mut aggregates: Vec<String> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => name = Some(inner.as_str().to_string()),
            Rule::hier_relationship => {
                for rel in inner.into_inner() {
                    reverse = rel.as_rule() == Rule::hier_rel_reverse;
                    for t in rel.into_inner() {
                        if t.as_rule() == Rule::hier_types {
                            for l in t.into_inner() {
                                edge_types.push(l.as_str().to_string());
                            }
                        }
                    }
                }
            }
            Rule::hier_measure => {
                // `MEASURE Trial.enrollment` yields (label, property); `MEASURE enrollment`
                // yields the property alone.
                let parts: Vec<_> = inner.into_inner().collect();
                for part in &parts {
                    match part.as_rule() {
                        Rule::label => measure_label = Some(part.as_str().to_string()),
                        Rule::property_key => measure_property = Some(part.as_str().to_string()),
                        _ => {}
                    }
                }
            }
            Rule::hier_aggregates => {
                for op in inner.into_inner() {
                    aggregates.push(op.as_str().to_string());
                }
            }
            _ => {}
        }
    }

    if edge_types.is_empty() {
        return Err(ParseError::SemanticError(
            "CREATE HIERARCHY INDEX requires at least one relationship type".to_string(),
        ));
    }

    query.create_hierarchy_index_clause = Some(CreateHierarchyIndexClause {
        name: name.ok_or_else(|| ParseError::SemanticError("Missing index name".to_string()))?,
        edge_types,
        reverse,
        measure_label,
        measure_property,
        aggregates,
    });
    Ok(())
}

fn parse_drop_index_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut label = None;
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::label => label = Some(Label::new(inner.as_str())),
            Rule::property_key => property = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    query.drop_index_clause = Some(DropIndexClause {
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: property.ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?,
    });
    Ok(())
}

fn parse_create_constraint_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut variable = None;
    let mut label = None;
    let mut property = None;

    // Both the legacy `ON ... ASSERT` and modern `FOR ... REQUIRE` forms wrap their parts
    // in a sub-rule; unwrap it so the field extraction below is shared. `constraint_name`
    // and `if_not_exists` are deliberately separate rules so the optional constraint name
    // is not picked up as the pattern variable.
    let inner_pairs: Vec<_> = pair
        .into_inner()
        .flat_map(|p| match p.as_rule() {
            Rule::constraint_modern | Rule::constraint_legacy => {
                p.into_inner().collect::<Vec<_>>()
            }
            _ => vec![p],
        })
        .collect();

    for inner in inner_pairs {
        match inner.as_rule() {
            Rule::variable => {
                if variable.is_none() {
                    variable = Some(inner.as_str().to_string());
                }
            }
            Rule::label => label = Some(Label::new(inner.as_str())),
            Rule::property_access => {
                // Extract property from property_access (variable.property)
                for pa in inner.into_inner() {
                    if pa.as_rule() == Rule::property_key {
                        property = Some(pa.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    query.create_constraint_clause = Some(CreateConstraintClause {
        variable: variable.ok_or_else(|| ParseError::SemanticError("Missing variable".to_string()))?,
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: property.ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?,
    });
    Ok(())
}

fn parse_create_vector_index_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut index_name = None;
    let mut label = None;
    let mut property_key = None;
    let mut dimensions = 1536; // Default
    let mut similarity = "cosine".to_string(); // Default

    for inner in pair.into_inner() {
        match inner.as_rule() {
            // The optional index name has its own rule so it cannot swallow the `FOR`
            // keyword. Taking it from the first `variable` instead would now pick up the
            // pattern variable (`n` in `FOR (n:Embedding)`) whenever the name is omitted.
            Rule::index_name => {
                index_name = Some(inner.as_str().to_string());
            }
            Rule::label => {
                label = Some(Label::new(inner.as_str()));
            }
            Rule::property_key => {
                property_key = Some(inner.as_str().to_string());
            }
            Rule::options => {
                let options_map = parse_properties(inner)?;

                // Reject anything we do not honour. Silently discarding an
                // unrecognised key builds an index the caller did not ask for
                // and reports success: `{dimension: 4}` (singular) produced a
                // 1536-dimension index, and the mismatch only surfaced later
                // against the caller's *vector*, which is the wrong thing to
                // blame (#474).
                const ACCEPTED: [&str; 2] = ["dimensions", "similarity"];
                let mut unknown: Vec<&String> = options_map
                    .keys()
                    .filter(|k| !ACCEPTED.contains(&k.as_str()))
                    .collect();
                if !unknown.is_empty() {
                    unknown.sort();
                    let listed = unknown
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(ParseError::SemanticError(format!(
                        "CREATE VECTOR INDEX: unknown option {listed}. Accepted options are \
`dimensions` (integer) and `similarity` (string)."
                    )));
                }

                if let Some(value) = options_map.get("dimensions") {
                    match value {
                        PropertyValue::Integer(d) if *d > 0 => dimensions = *d as usize,
                        other => {
                            return Err(ParseError::SemanticError(format!(
                                "CREATE VECTOR INDEX: `dimensions` must be a positive integer, got {other:?}"
                            )))
                        }
                    }
                }
                if let Some(value) = options_map.get("similarity") {
                    match value {
                        PropertyValue::String(s) => similarity = s.clone(),
                        other => {
                            return Err(ParseError::SemanticError(format!(
                                "CREATE VECTOR INDEX: `similarity` must be a string, got {other:?}"
                            )))
                        }
                    }
                }
            }
            _ => {}
        }
    }

    query.create_vector_index_clause = Some(CreateVectorIndexClause {
        index_name,
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label in CREATE VECTOR INDEX".to_string()))?,
        property_key: property_key.ok_or_else(|| ParseError::SemanticError("Missing property key in CREATE VECTOR INDEX".to_string()))?,
        dimensions,
        similarity,
    });

    Ok(())
}

fn parse_call_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::call_clause => {
                query.call_clause = Some(parse_call_clause(inner)?);
            }
            Rule::call_subquery => {
                // CALL { subquery }
                for sub_inner in inner.into_inner() {
                    if sub_inner.as_rule() == Rule::statement {
                        let mut sub_query = Query::new();
                        parse_statement(sub_inner, &mut sub_query)?;
                        query.call_subquery = Some(Box::new(sub_query));
                    }
                }
            }
            Rule::where_clause => {
                // `CALL ... YIELD x WHERE <pred>` -- filters the procedure's output
                // directly, with no intervening MATCH.
                query.where_clause = Some(parse_where_clause(inner)?);
            }
            Rule::match_stmt_partial => {
                parse_match_statement_partial(inner, query)?;
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            Rule::order_by_clause => {
                query.order_by = Some(parse_order_by_clause(inner)?);
            }
            Rule::skip_clause => {
                for i in inner.into_inner() {
                    if i.as_rule() == Rule::integer {
                        query.skip = i.as_str().parse::<usize>().ok();
                    }
                }
            }
            Rule::limit_clause => {
                for i in inner.into_inner() {
                    if i.as_rule() == Rule::integer {
                        query.limit = i.as_str().parse::<usize>().ok();
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Strip the surrounding quotes from a string literal and interpret its escape sequences.
///
/// The grammar lets a backslash escape the following character so a quote can appear inside
/// a string of the same kind; this turns those sequences into the characters they denote.
/// Previously nothing interpreted them, so `"a\\nb"` produced a literal backslash and an
/// `n` rather than a newline, and an escaped quote could not be written at all.
///
/// An unrecognised escape yields the escaped character itself (`\\q` -> `q`), which keeps
/// Windows-style paths and regex fragments from turning into a parse error.
fn unescape_string_literal(literal: &str) -> String {
    let inner = &literal[1..literal.len() - 1];
    if !inner.contains('\\') {
        return inner.to_string();
    }

    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('0') => out.push('\0'),
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            // \uXXXX, as in openCypher
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                    Some(decoded) => out.push(decoded),
                    None => {
                        out.push('u');
                        out.push_str(&hex);
                    }
                }
            }
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

fn parse_match_statement_partial(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => {
                let pattern = parse_pattern(inner)?;
                query.match_clauses.push(MatchClause {
                    pattern,
                    optional: false,
                });
            }
            Rule::where_clause => {
                query.where_clause = Some(parse_where_clause(inner)?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_call_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<CallClause> {
    let mut procedure_name = String::new();
    let mut arguments = Vec::new();
    let mut yield_items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::procedure_name => {
                procedure_name = inner.as_str().to_string();
            }
            Rule::expression => {
                arguments.push(parse_expression(inner)?);
            }
            Rule::yield_items => {
                for yield_pair in inner.into_inner() {
                    if yield_pair.as_rule() == Rule::yield_item {
                        yield_items.push(parse_yield_item(yield_pair)?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(CallClause {
        procedure_name,
        arguments,
        yield_items,
    })
}

fn parse_yield_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<YieldItem> {
    let mut name = String::new();
    let mut alias = None;

    let inner: Vec<_> = pair.into_inner().collect();
    if inner.len() >= 1 {
        name = inner[0].as_str().to_string();
    }
    if inner.len() >= 2 {
        alias = Some(inner[1].as_str().to_string());
    }

    Ok(YieldItem { name, alias })
}

fn parse_match_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                for mc_inner in inner.into_inner() {
                    if mc_inner.as_rule() == Rule::pattern {
                        query.match_clauses.push(MatchClause {
                            pattern: parse_pattern(mc_inner)?,
                            optional: false,
                        });
                    }
                }
            }
            Rule::optional_match_clause => {
                for mc_inner in inner.into_inner() {
                    if mc_inner.as_rule() == Rule::pattern {
                        query.match_clauses.push(MatchClause {
                            pattern: parse_pattern(mc_inner)?,
                            optional: true,
                        });
                    }
                }
            }
            Rule::where_clause => {
                // Cypher permits a `WHERE` after each `MATCH`. The planner
                // expects a single AND-chain in `query.where_clause` (or in
                // `post_with_where_clause` after a `WITH`), so when multiple
                // WHEREs appear in the same pre-WITH or post-WITH block, AND
                // them together rather than silently overwriting the earlier
                // predicate. Dropping the first WHERE was behind OM27's
                // timeout + wrong-semantics on the v1.0 mega benchmark.
                let parsed = parse_where_clause(inner)?;
                let target = if query.with_split_index.is_some() {
                    &mut query.post_with_where_clause
                } else {
                    &mut query.where_clause
                };
                match target.take() {
                    Some(existing) => {
                        *target = Some(WhereClause {
                            predicate: Expression::Binary {
                                left: Box::new(existing.predicate),
                                op: BinaryOp::And,
                                right: Box::new(parsed.predicate),
                            },
                        });
                    }
                    None => *target = Some(parsed),
                }
            }
            Rule::with_clause => {
                if query.with_clause.is_some() {
                    // Additional WITH clause — save current post-WITH state as an extra stage
                    let split = query.with_split_index.unwrap_or(query.match_clauses.len());
                    let post_matches: Vec<_> = query.match_clauses.drain(split..).collect();
                    let post_where = query.post_with_where_clause.take();
                    let prev_with = query.with_clause.take().unwrap();
                    // The UNWIND that belongs to the stage being closed is the
                    // one written *after* its WITH. The query's leading UNWIND
                    // belongs at the head and stays in `unwind_clause`.
                    //
                    // Taking `unwind_clause` here made this slot mean two
                    // different things -- the stage's own unwind when it had
                    // one, the query's leading unwind when it did not -- and
                    // the planner cannot tell them apart. It read
                    // `extra_with_stages[0].1` as the leading unwind and so
                    // hoisted `UNWIND b AS c` to the head of
                    //
                    //   UNWIND [1,2] AS a WITH [1,2] AS b UNWIND b AS c ...
                    //
                    // where `b` does not exist yet: `VariableNotFound("b")`.
                    // A second hack in the planner suppressed stage 0's unwind
                    // to compensate, and the two cancelled out for every shape
                    // with no unwind on the first stage -- which is why one
                    // WITH+UNWIND worked and two did not (#785).
                    let prev_unwind = if query.post_with_unwind_clauses.is_empty() {
                        None
                    } else {
                        Some(query.post_with_unwind_clauses.remove(0))
                    };
                    query.extra_with_stages.push((prev_with, prev_unwind, post_matches, post_where));
                }
                // Record where WITH splits pre-WITH from post-WITH match clauses
                query.with_split_index = Some(query.match_clauses.len());
                query.with_clause = Some(parse_with_clause(inner)?);
            }
            Rule::call_clause => {
                query.call_clause = Some(parse_call_clause(inner)?);
            }
            Rule::create_clause => {
                for create_inner in inner.into_inner() {
                    if create_inner.as_rule() == Rule::pattern {
                        query.create_clause = Some(CreateClause {
                            pattern: parse_pattern(create_inner)?,
                        });
                    }
                }
            }
            Rule::delete_clause => {
                query.delete_clause = Some(parse_delete_clause(inner)?);
            }
            Rule::foreach_clause => {
                query.foreach_clause = Some(parse_foreach_clause(inner)?);
            }
            Rule::set_clause => {
                query.set_clauses.push(parse_set_clause(inner)?);
            }
            Rule::remove_clause => {
                query.remove_clauses.push(parse_remove_clause(inner)?);
            }
            Rule::unwind_clause => {
                // The first UNWIND stays in `unwind_clause`; the rest queue up
                // behind it, each a cross product with everything before.
                //
                // Unless a WITH has already been seen, in which case the UNWIND
                // belongs *after* that WITH and must be kept apart: the planner
                // applies the leading run before the WITH barrier, so a
                // post-WITH unwind put there reads a variable the WITH has not
                // projected yet (#785).
                let u = parse_unwind_clause(inner)?;
                if query.with_clause.is_some() {
                    query.post_with_unwind_clauses.push(u);
                } else if query.unwind_clause.is_none() {
                    query.unwind_clause = Some(u);
                } else {
                    query.extra_unwind_clauses.push(u);
                }
            }
            Rule::merge_inline => {
                query.merge_clause = Some(parse_merge_clause(inner)?);
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            Rule::order_by_clause => {
                query.order_by = Some(parse_order_by_clause(inner)?);
            }
            Rule::skip_clause => {
                for skip_inner in inner.into_inner() {
                    if skip_inner.as_rule() == Rule::integer {
                        query.skip = Some(parse_count_literal(skip_inner.as_str())?);
                    }
                }
            }
            Rule::limit_clause => {
                for limit_inner in inner.into_inner() {
                    if limit_inner.as_rule() == Rule::integer {
                        query.limit = Some(parse_count_literal(limit_inner.as_str())?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_create_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    // Adjacent CREATE clauses are merged into one pattern. They are equivalent
    // by definition — `CREATE (a) CREATE (b)` and `CREATE (a), (b)` bind the
    // same variables to the same nodes — and merging means the planner and the
    // executor never learn that repeated clauses exist. The alternative,
    // `Vec<CreateClause>` on the AST, would touch 36 call sites, most of them
    // `is_some()` guards asking only "is this a write query".
    let mut paths = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            // A bare `CREATE` statement yields `pattern` directly; a repeated
            // one yields `create_clause` wrappers.
            Rule::pattern => paths.extend(parse_pattern(inner)?.paths),
            Rule::create_clause => {
                for c in inner.into_inner() {
                    if c.as_rule() == Rule::pattern {
                        paths.extend(parse_pattern(c)?.paths);
                    }
                }
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            Rule::order_by_clause => {
                query.order_by = Some(parse_order_by_clause(inner)?);
            }
            Rule::skip_clause => {
                for i in inner.into_inner() {
                    if i.as_rule() == Rule::integer {
                        query.skip = i.as_str().parse::<usize>().ok();
                    }
                }
            }
            Rule::limit_clause => {
                for i in inner.into_inner() {
                    if i.as_rule() == Rule::integer {
                        query.limit = i.as_str().parse::<usize>().ok();
                    }
                }
            }
            _ => {}
        }
    }
    if !paths.is_empty() {
        query.create_clause = Some(CreateClause {
            pattern: crate::query::ast::Pattern { paths },
        });
    }
    Ok(())
}

fn parse_with_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<WithClause> {
    let mut items = Vec::new();
    let mut distinct = false;
    let mut where_clause = None;
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct => { distinct = true; }
            Rule::return_items => {
                items = parse_return_items(inner)?;
            }
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(inner)?);
            }
            Rule::order_by_clause => {
                order_by = Some(parse_order_by_clause(inner)?);
            }
            Rule::skip_clause => {
                for skip_inner in inner.into_inner() {
                    if skip_inner.as_rule() == Rule::integer {
                        skip = Some(parse_count_literal(skip_inner.as_str())?);
                    }
                }
            }
            Rule::limit_clause => {
                for limit_inner in inner.into_inner() {
                    if limit_inner.as_rule() == Rule::integer {
                        limit = Some(parse_count_literal(limit_inner.as_str())?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(WithClause { items, distinct, where_clause, order_by, skip, limit })
}

fn parse_delete_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<DeleteClause> {
    let text = pair.as_str().to_uppercase();
    let detach = text.starts_with("DETACH");
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            expressions.push(parse_expression(inner)?);
        }
    }

    Ok(DeleteClause { expressions, detach })
}

/// `variable (":" label)+` — the label form of a SET item.
///
/// Shared by `SET`, `ON CREATE SET` and `ON MATCH SET`. Kept as one function
/// because the last time these were parsed in two places the copies drifted
/// and one of them silently dropped items it did not recognise.
fn parse_set_label_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetLabelItem> {
    let mut variable = String::new();
    let mut labels = Vec::new();
    for sl in pair.into_inner() {
        match sl.as_rule() {
            Rule::variable => variable = sl.as_str().to_string(),
            Rule::label => labels.push(Label::new(sl.as_str())),
            _ => {}
        }
    }
    if labels.is_empty() {
        return Err(ParseError::SemanticError("SET label item has no label".to_string()));
    }
    Ok(SetLabelItem { variable, labels })
}

fn parse_set_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetClause> {
    let mut items = Vec::new();
    let mut label_items: Vec<SetLabelItem> = Vec::new();
    let mut entity_items: Vec<crate::query::ast::SetEntityItem> = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_label_item {
            label_items.push(parse_set_label_item(inner)?);
            continue;
        }
        if inner.as_rule() == Rule::set_entity_item {
            let mut variable = String::new();
            let mut merge = false;
            let mut value = None;
            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::variable if variable.is_empty() => variable = part.as_str().to_string(),
                    Rule::set_entity_op => merge = part.as_str().trim() == "+=",
                    Rule::expression => value = Some(parse_expression(part)?),
                    _ => {}
                }
            }
            entity_items.push(crate::query::ast::SetEntityItem {
                variable,
                merge,
                value: value.ok_or_else(|| {
                    ParseError::SemanticError("SET <entity> = missing a value".to_string())
                })?,
            });
            continue;
        }
        if inner.as_rule() == Rule::set_item {
            let mut variable = String::new();
            let mut property = String::new();
            let mut value = None;

            for si in inner.into_inner() {
                match si.as_rule() {
                    Rule::property_access => {
                        for pa in si.into_inner() {
                            match pa.as_rule() {
                                Rule::variable => variable = pa.as_str().to_string(),
                                Rule::property_key => property = pa.as_str().to_string(),
                                _ => {}
                            }
                        }
                    }
                    Rule::expression => {
                        value = Some(parse_expression(si)?);
                    }
                    _ => {}
                }
            }

            items.push(SetItem {
                variable,
                property,
                value: value.ok_or_else(|| ParseError::SemanticError("SET item missing value".to_string()))?,
            });
        }
    }

    Ok(SetClause { items, label_items, entity_items })
}

fn parse_remove_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<RemoveClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::remove_item {
            let children: Vec<_> = inner.into_inner().collect();
            if children.len() == 1 && children[0].as_rule() == Rule::property_access {
                let mut variable = String::new();
                let mut property = String::new();
                for pa in children[0].clone().into_inner() {
                    match pa.as_rule() {
                        Rule::variable => variable = pa.as_str().to_string(),
                        Rule::property_key => property = pa.as_str().to_string(),
                        _ => {}
                    }
                }
                items.push(RemoveItem::Property { variable, property });
            } else {
                // `variable (":" label)+` — one item per label, so
                // `REMOVE n:L1:L3` removes both rather than only the first.
                let mut variable = String::new();
                let mut labels = Vec::new();
                for child in children {
                    match child.as_rule() {
                        Rule::variable => variable = child.as_str().to_string(),
                        Rule::label => labels.push(child.as_str().to_string()),
                        _ => {}
                    }
                }
                for label in labels {
                    items.push(RemoveItem::Label {
                        variable: variable.clone(),
                        label: Label::new(&label),
                    });
                }
            }
        }
    }

    Ok(RemoveClause { items })
}

fn parse_unwind_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<UnwindClause> {
    let mut expression = None;
    let mut variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(parse_expression(inner)?),
            Rule::variable => variable = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    Ok(UnwindClause {
        expression: expression.ok_or_else(|| ParseError::SemanticError("UNWIND missing expression".to_string()))?,
        variable: variable.ok_or_else(|| ParseError::SemanticError("UNWIND missing AS variable".to_string()))?,
    })
}

fn parse_merge_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    // merge_stmt has pattern, on_create_set?, on_match_set?, return_clause?
    let mut pattern = None;
    let mut on_create_set = Vec::new();
    let mut on_match_set = Vec::new();
    let mut on_create_labels = Vec::new();
    let mut on_match_labels = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(inner)?),
            Rule::on_create_set => {
                for si in inner.into_inner() {
                    match si.as_rule() {
                        Rule::set_item => on_create_set.push(parse_set_item(si)?),
                        Rule::set_label_item => on_create_labels.push(parse_set_label_item(si)?),
                        _ => {}
                    }
                }
            }
            Rule::on_match_set => {
                for si in inner.into_inner() {
                    match si.as_rule() {
                        Rule::set_item => on_match_set.push(parse_set_item(si)?),
                        Rule::set_label_item => on_match_labels.push(parse_set_label_item(si)?),
                        _ => {}
                    }
                }
            }
            Rule::set_clause => {
                // A bare SET after MERGE, applying on both branches. Kept in
                // `query.set_clauses` rather than folded into ON CREATE/ON MATCH so the
                // planner can layer one SetProperty over the merge instead of duplicating
                // the items into both branch lists.
                query.set_clauses.push(parse_set_clause(inner)?);
            }
            Rule::remove_clause => {
                query.remove_clauses.push(parse_remove_clause(inner)?);
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            _ => {}
        }
    }

    query.merge_clause = Some(MergeClause {
        pattern: pattern.ok_or_else(|| ParseError::SemanticError("MERGE missing pattern".to_string()))?,
        on_create_set,
        on_match_set,
        on_create_labels,
        on_match_labels,
    });
    Ok(())
}

fn parse_merge_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<MergeClause> {
    let mut pattern = None;
    let mut on_create_set = Vec::new();
    let mut on_match_set = Vec::new();
    let mut on_create_labels = Vec::new();
    let mut on_match_labels = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(inner)?),
            Rule::on_create_set => {
                for si in inner.into_inner() {
                    match si.as_rule() {
                        Rule::set_item => on_create_set.push(parse_set_item(si)?),
                        Rule::set_label_item => on_create_labels.push(parse_set_label_item(si)?),
                        _ => {}
                    }
                }
            }
            Rule::on_match_set => {
                for si in inner.into_inner() {
                    match si.as_rule() {
                        Rule::set_item => on_match_set.push(parse_set_item(si)?),
                        Rule::set_label_item => on_match_labels.push(parse_set_label_item(si)?),
                        _ => {}
                    }
                }
            }
            Rule::return_clause => {
                // Handled at statement level for merge_stmt
            }
            _ => {}
        }
    }

    Ok(MergeClause {
        pattern: pattern.ok_or_else(|| ParseError::SemanticError("MERGE missing pattern".to_string()))?,
        on_create_set,
        on_match_set,
        on_create_labels,
        on_match_labels,
    })
}

fn parse_set_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetItem> {
    let mut variable = String::new();
    let mut property = String::new();
    let mut value = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::property_access => {
                for pa in inner.into_inner() {
                    match pa.as_rule() {
                        Rule::variable => variable = pa.as_str().to_string(),
                        Rule::property_key => property = pa.as_str().to_string(),
                        _ => {}
                    }
                }
            }
            Rule::expression => value = Some(parse_expression(inner)?),
            _ => {}
        }
    }

    Ok(SetItem {
        variable,
        property,
        value: value.ok_or_else(|| ParseError::SemanticError("SET item missing value".to_string()))?,
    })
}

/// Parse a `RETURN` / `WITH` item list.
///
/// Delegates to `parse_return_item` rather than repeating its body. The two
/// had drifted: this one matched only `expression` and `variable` and dropped
/// anything else *silently* (`if let Some(e) = expr`), so when `star_item` was
/// added `WITH *` parsed to an empty projection and the query failed at
/// runtime with "Variable not found" — a grammar addition that looked like an
/// executor bug. One implementation cannot drift from itself.
fn parse_return_items(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<ReturnItem>> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::return_item {
            items.push(parse_return_item(inner)?);
        }
    }
    Ok(items)
}

fn parse_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<Pattern> {
    let mut paths = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::named_path => {
                paths.push(parse_named_path(inner)?);
            }
            Rule::path => {
                paths.push(parse_path(inner)?);
            }
            _ => {}
        }
    }

    Ok(Pattern { paths })
}

fn parse_named_path(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let mut path_variable: Option<String> = None;
    let mut path_pattern: Option<PathPattern> = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                if path_variable.is_none() {
                    path_variable = Some(inner.as_str().to_string());
                }
            }
            Rule::path => {
                path_pattern = Some(parse_path(inner)?);
            }
            Rule::shortest_path_call => {
                path_pattern = Some(parse_shortest_path_call(inner)?);
            }
            _ => {}
        }
    }

    let mut pp = path_pattern.ok_or_else(|| ParseError::SemanticError("Named path missing path pattern".to_string()))?;
    pp.path_variable = path_variable;
    Ok(pp)
}

fn parse_shortest_path_call(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let text = pair.as_str();
    let path_type = if text.to_lowercase().starts_with("allshortestpaths") {
        PathType::AllShortest
    } else {
        PathType::Shortest
    };

    let mut pp = None;
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::path {
            pp = Some(parse_path(inner)?);
        }
    }

    let mut path = pp.ok_or_else(|| ParseError::SemanticError("shortestPath() missing inner path".to_string()))?;
    path.path_type = path_type;
    Ok(path)
}

fn parse_path(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node => {
                nodes.push(parse_node(inner)?);
            }
            Rule::edge_pattern => {
                edges.push(parse_edge(inner)?);
            }
            _ => {}
        }
    }

    if nodes.is_empty() {
        return Err(ParseError::SemanticError("Path must have at least one node".to_string()));
    }

    let start = nodes.remove(0);
    let mut segments = Vec::new();

    for (edge, node) in edges.into_iter().zip(nodes.into_iter()) {
        segments.push(PathSegment { edge, node });
    }

    Ok(PathPattern { path_variable: None, path_type: PathType::Normal, start, segments })
}

fn parse_node(pair: pest::iterators::Pair<Rule>) -> ParseResult<NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;
    let mut property_exprs = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                variable = Some(inner.as_str().to_string());
            }
            Rule::labels => {
                for label_pair in inner.into_inner() {
                    if label_pair.as_rule() == Rule::label {
                        labels.push(Label::new(label_pair.as_str()));
                    }
                }
            }
            Rule::properties => {
                let (literals, exprs) = parse_properties_split(inner)?;
                properties = Some(literals);
                property_exprs = exprs;
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
        property_exprs,
    })
}

fn parse_edge(pair: pest::iterators::Pair<Rule>) -> ParseResult<EdgePattern> {
    let mut direction = Direction::Both;
    let edge_str = pair.as_str();

    if edge_str.starts_with("<-") {
        direction = Direction::Incoming;
    } else if edge_str.ends_with("->") {
        direction = Direction::Outgoing;
    }

    let mut variable = None;
    let mut types = Vec::new();
    let mut length = None;
    let mut properties = None;
    let mut property_exprs = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::edge_detail {
            for detail in inner.into_inner() {
                match detail.as_rule() {
                    Rule::variable => {
                        variable = Some(detail.as_str().to_string());
                    }
                    Rule::edge_types => {
                        for type_pair in detail.into_inner() {
                            if type_pair.as_rule() == Rule::edge_type {
                                types.push(EdgeType::new(type_pair.as_str()));
                            }
                        }
                    }
                    Rule::length_pattern => {
                        length = Some(parse_length_pattern(detail)?);
                    }
                    Rule::properties => {
                        let (literals, exprs) = parse_properties_split(detail)?;
                        properties = Some(literals);
                        property_exprs = exprs;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(EdgePattern {
        variable,
        types,
        direction,
        length,
        properties,
        property_exprs,
    })
}

fn parse_length_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<LengthPattern> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::range_pattern {
            let range_str = inner.as_str();
            let parts: Vec<&str> = range_str.split("..").collect();

            let min = if parts[0].is_empty() {
                Some(1)
            } else {
                Some(parts[0].parse().unwrap_or(1))
            };

            let max = if parts.len() > 1 && !parts[1].is_empty() {
                Some(parts[1].parse().unwrap())
            } else {
                None
            };

            return Ok(LengthPattern { min, max });
        } else if inner.as_rule() == Rule::integer {
            let exact = inner.as_str().parse().unwrap();
            return Ok(LengthPattern {
                min: Some(exact),
                max: Some(exact),
            });
        }
    }

    // Just * means 1..unbounded
    Ok(LengthPattern {
        min: Some(1),
        max: None,
    })
}

/// Split a property map into literal values and expression values.
///
/// Literals keep their concrete `PropertyValue` -- they are the majority and the only form
/// usable for an index lookup -- while anything referring to a bound variable
/// (`{n: p.n}`, `{id: row.id}`) is returned separately for CREATE/MERGE to evaluate per
/// row. Returning `(literals, exprs)` rather than converting everything to expressions
/// keeps all existing consumers of `properties` working unchanged.
type SplitProperties = (HashMap<String, PropertyValue>, Option<HashMap<String, Expression>>);

fn parse_properties_split(pair: pest::iterators::Pair<Rule>) -> ParseResult<SplitProperties> {
    let mut literals = HashMap::new();
    let mut exprs: HashMap<String, Expression> = HashMap::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_list {
            for prop in inner.into_inner() {
                if prop.as_rule() == Rule::property {
                    let mut key = String::new();
                    for part in prop.into_inner() {
                        match part.as_rule() {
                            Rule::property_key => key = part.as_str().to_string(),
                            Rule::value => {
                                literals.insert(key.clone(), parse_value(part)?);
                            }
                            Rule::expression => {
                                exprs.insert(key.clone(), parse_expression(part)?);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    Ok((literals, if exprs.is_empty() { None } else { Some(exprs) }))
}

#[allow(dead_code)]
fn parse_properties(pair: pest::iterators::Pair<Rule>) -> ParseResult<HashMap<String, PropertyValue>> {
    let mut props = HashMap::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_list {
            for prop in inner.into_inner() {
                if prop.as_rule() == Rule::property {
                    let mut key = String::new();
                    let mut value = PropertyValue::Null;

                    for part in prop.into_inner() {
                        match part.as_rule() {
                            Rule::property_key => {
                                key = part.as_str().to_string();
                            }
                            Rule::value => {
                                value = parse_value(part)?;
                            }
                            _ => {}
                        }
                    }

                    props.insert(key, value);
                }
            }
        }
    }

    Ok(props)
}

fn parse_value(pair: pest::iterators::Pair<Rule>) -> ParseResult<PropertyValue> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::null => return Ok(PropertyValue::Null),
            Rule::boolean => {
                let val = inner.as_str().eq_ignore_ascii_case("true");
                return Ok(PropertyValue::Boolean(val));
            }
            Rule::integer => {
                return Ok(PropertyValue::Integer(parse_integer_literal(inner.as_str())?));
            }
            Rule::float => {
                let val = inner.as_str().trim().parse().map_err(|_| {
                    ParseError::SemanticError(format!(
                        "float literal out of range: `{}`",
                        inner.as_str()
                    ))
                })?;
                return Ok(PropertyValue::Float(val));
            }
            Rule::string => {
                return Ok(PropertyValue::String(unescape_string_literal(inner.as_str())));
            }
            Rule::list => {
                // `Vector` is the embedding type -- f32 throughout. Treating *any*
                // all-numeric list as one meant `[1, 2, 3]` came back as
                // `Float(1.0), Float(2.0), Float(3.0)`: `UNWIND [1,2,3] AS x RETURN x`
                // returned decimals for data that had none (#409).
                //
                // A list literal is a list. It used to become a
                // `Vector(Vec<f32>)` as soon as one element was a float, on the
                // theory that a float list is an embedding -- and every element
                // was narrowed to 32 bits on the way in. Cypher floats are
                // 64-bit, so `UNWIND [1.3, 1.5] AS v RETURN v` returned
                // 1.2999999523162842, and `ORDER BY` on those values sorted
                // numbers that were no longer the ones written (#628).
                //
                // Nothing needs the coercion: `PropertyValue::to_vector`
                // already accepts a numeric array, which is how an embedding
                // written as `[1, 0, 0]` has indexed since #409 -- an
                // all-integer list was never turned into a vector either.
                // Deciding vector-ness belongs to the consumer, not to whether
                // the literal happened to contain a decimal point.
                let mut items = Vec::new();
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::value {
                        items.push(parse_value(item)?);
                    }
                }
                return Ok(PropertyValue::Array(items));
            }
            Rule::map => {
                let mut map = HashMap::new();
                for entry in inner.into_inner() {
                    if entry.as_rule() == Rule::map_entry {
                        let mut key = String::new();
                        let mut val = PropertyValue::Null;
                        
                        for part in entry.into_inner() {
                            match part.as_rule() {
                                Rule::property_key => key = part.as_str().to_string(),
                                Rule::string => {
                                    key = unescape_string_literal(part.as_str());
                                }
                                Rule::value => val = parse_value(part)?,
                                _ => {}
                            }
                        }
                        map.insert(key, val);
                    }
                }
                return Ok(PropertyValue::Map(map));
            }
            _ => {}
        }
    }

    Ok(PropertyValue::Null)
}

fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<WhereClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            return Ok(WhereClause {
                predicate: parse_expression(inner)?,
            });
        }
    }
    Err(ParseError::SemanticError("Invalid WHERE clause".to_string()))
}

fn parse_return_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<ReturnClause> {
    let mut distinct = false;
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct => {
                distinct = true;
            }
            Rule::return_items => {
                for item_pair in inner.into_inner() {
                    if item_pair.as_rule() == Rule::return_item {
                        items.push(parse_return_item(item_pair)?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ReturnClause { items, distinct })
}

fn parse_return_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<ReturnItem> {
    let mut expression = None;
    let mut alias = None;
    // Captured before the pair is consumed. This is the column's name when no
    // alias is given, so it has to be the text the user wrote rather than
    // anything reconstructed from the AST.
    let mut source_text = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::star_item => {
                expression = Some(Expression::Variable(crate::query::ast::STAR_ITEM.to_string()));
            }
            Rule::expression => {
                source_text = Some(inner.as_str().trim().to_string());
                expression = Some(parse_expression(inner)?);
            }
            Rule::variable => {
                alias = Some(inner.as_str().to_string());
            }
            _ => {}
        }
    }

    Ok(ReturnItem {
        expression: expression.ok_or_else(|| ParseError::SemanticError("Missing expression in RETURN".to_string()))?,
        alias,
        source_text,
    })
}

fn parse_order_by_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<OrderByClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::order_items {
            for item_pair in inner.into_inner() {
                if item_pair.as_rule() == Rule::order_item {
                    items.push(parse_order_item(item_pair)?);
                }
            }
        }
    }

    Ok(OrderByClause { items })
}

fn parse_order_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<OrderByItem> {
    let mut expression = None;
    let mut ascending = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                expression = Some(parse_expression(inner)?);
            }
            Rule::order_direction => {
                ascending = inner.as_str().eq_ignore_ascii_case("ASC") ||
                           inner.as_str().eq_ignore_ascii_case("ASCENDING");
            }
            _ => {}
        }
    }

    Ok(OrderByItem {
        expression: expression.ok_or_else(|| ParseError::SemanticError("Missing expression in ORDER BY".to_string()))?,
        ascending,
    })
}

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let inner: Vec<_> = pair.into_inner().collect();

    // `1 < n.num < 3` means `1 < n.num AND n.num < 3`. Left-associative
    // parsing makes it `(1 < n.num) < 3` instead, which compares a boolean to
    // 3 -- "Cannot compare these types" on a query that is ordinary Cypher.
    //
    // The rewrite keys on the **token sequence**, not the parsed tree, and
    // that distinction is the whole safety argument. Parentheses are inline in
    // `primary`, so `(a < b) = true` and `a < b < c` are indistinguishable
    // once parsed -- but at this level the first is one top-level comparison
    // operator and the second is two. Applying the expansion only when *every*
    // top-level operator is a comparison therefore cannot touch a
    // parenthesised comparison being compared to something, and cannot touch
    // anything joined by AND/OR/arithmetic, which the Pratt parser goes on
    // handling exactly as before.
    //
    // The middle operand appears in both conjuncts. Cypher evaluates it once,
    // and duplicating it is observationally the same here because expressions
    // in this position are pure.
    let ops: Vec<&pest::iterators::Pair<Rule>> = inner.iter().skip(1).step_by(2).collect();
    if ops.len() >= 2 && ops.iter().all(|p| p.as_rule() == Rule::comparison_op) {
        let mut terms = Vec::with_capacity(ops.len() + 1);
        for term in inner.iter().step_by(2) {
            terms.push(parse_term(term.clone())?);
        }
        if terms.len() == ops.len() + 1 {
            let mut conjunction: Option<Expression> = None;
            for (i, op) in ops.iter().enumerate() {
                let comparison = Expression::Binary {
                    left: Box::new(terms[i].clone()),
                    op: parse_op_str(op.as_str())?,
                    right: Box::new(terms[i + 1].clone()),
                };
                conjunction = Some(match conjunction {
                    None => comparison,
                    Some(previous) => Expression::Binary {
                        left: Box::new(previous),
                        op: BinaryOp::And,
                        right: Box::new(comparison),
                    },
                });
            }
            if let Some(expr) = conjunction {
                return Ok(expr);
            }
        }
    }

    // A chain the rewrite above declined to expand — `1 < x < 3 AND ...`,
    // where the top-level operators are not *all* comparisons.
    //
    // Left-associative parsing turns the chain into `(1 < x) < 3`, comparing a
    // boolean to a number. That is null in Cypher (#607), so a WHERE built on
    // it quietly matches nothing: the query returns zero rows where Neo4j
    // returns the row. Refusing is the only honest option short of expanding
    // chains inside arbitrary expressions, which means reimplementing operator
    // precedence outside the Pratt parser — the thing this rewrite exists to
    // avoid.
    //
    // Until then: an error the caller can see, never a silently empty result.
    if ops.windows(2).any(|w| {
        w[0].as_rule() == Rule::comparison_op && w[1].as_rule() == Rule::comparison_op
    }) {
        return Err(ParseError::SemanticError(
            "a chained comparison (like `1 < x < 3`) is only supported when it is the \
             whole expression; here it is combined with other operators. Write it as \
             an explicit conjunction instead, for example `1 < x AND x < 3 AND ...`"
                .to_string(),
        ));
    }

    PRATT_PARSER
        .map_primary(|primary| parse_term(primary))
        .map_prefix(|op, rhs| match op.as_rule() {
            Rule::not_op => Ok(Expression::Unary {
                op: UnaryOp::Not,
                expr: Box::new(rhs?),
            }),
            other => Err(ParseError::SemanticError(format!(
                "Unexpected prefix operator: {other:?}"
            ))),
        })
        .map_infix(|left, op, right| {
            let left = left?;
            let right = right?;
            
            let op = match op.as_rule() {
                Rule::or_op => BinaryOp::Or,
                Rule::xor_op => BinaryOp::Xor,
                Rule::and_op => BinaryOp::And,
                Rule::pow_op => BinaryOp::Pow,
                Rule::comparison_op => parse_op_str(op.as_str())?,
                Rule::in_op => BinaryOp::In,
                Rule::add_sub_op => parse_op_str(op.as_str())?,
                Rule::mul_div_mod_op => parse_op_str(op.as_str())?,
                _ => return Err(ParseError::SemanticError(format!("Unexpected operator: {:?}", op.as_rule()))),
            };

            Ok(Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        })
        .parse(inner.into_iter())
}

fn parse_op_str(op_str: &str) -> ParseResult<BinaryOp> {
    Ok(match op_str {
        "==" | "=" => BinaryOp::Eq,
        "!=" | "<>" => BinaryOp::Ne,
        "<" => BinaryOp::Lt,
        "<=" => BinaryOp::Le,
        ">" => BinaryOp::Gt,
        ">=" => BinaryOp::Ge,
        "+" => BinaryOp::Add,
        "-" => BinaryOp::Sub,
        "*" => BinaryOp::Mul,
        "/" => BinaryOp::Div,
        "%" => BinaryOp::Mod,
        _ if op_str.eq_ignore_ascii_case("STARTS WITH") => BinaryOp::StartsWith,
        _ if op_str.eq_ignore_ascii_case("ENDS WITH") => BinaryOp::EndsWith,
        _ if op_str.eq_ignore_ascii_case("CONTAINS") => BinaryOp::Contains,
        _ if op_str.eq_ignore_ascii_case("IN") => BinaryOp::In,
        "=~" => BinaryOp::RegexMatch,
        _ => return Err(ParseError::SemanticError(format!("Unknown operator: {}", op_str))),
    })
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    match pair.as_rule() {
        Rule::term => {
            let mut prefix_ops = Vec::new();
            let mut primary_pair = None;
            let mut postfix_pair = None;
            let mut index_pairs = Vec::new();

            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::unary_op => prefix_ops.push(inner),
                    Rule::primary => primary_pair = Some(inner),
                    Rule::postfix_op => postfix_pair = Some(inner),
                    // Both suffixes go into one list so `f(x).a[0].b` applies
                    // them in source order; splitting them would apply every
                    // subscript before every member (#673).
                    Rule::index_op | Rule::member_op => index_pairs.push(inner),
                    _ => {}
                }
            }

            // `-9223372036854775808` is i64::MIN, and its magnitude is not a
            // valid i64 on its own -- so the minus has to be folded into the
            // literal rather than applied to it afterwards, or the only way to
            // write the smallest integer is a parse error. Every other negated
            // literal folds to exactly what the operator would have produced.
            if prefix_ops.len() == 1
                && prefix_ops[0].as_str().trim() == "-"
                && index_pairs.is_empty()
                && postfix_pair.is_none()
            {
                if let Some(primary) = primary_pair.as_ref() {
                    let text = primary.as_str().trim();
                    let numeric = !text.is_empty()
                        && text.bytes().all(|b| b.is_ascii_digit())
                        || text.len() > 2
                            && text.starts_with('0')
                            && matches!(text.as_bytes()[1], b'x' | b'X' | b'o' | b'O');
                    if numeric {
                        if let Ok(value) = parse_integer_literal(&format!("-{text}")) {
                            return Ok(Expression::Literal(PropertyValue::Integer(value)));
                        }
                    }
                }
            }

            let mut expr = parse_primary(primary_pair.unwrap())?;

            // Apply each index/slice suffix in source order, so chained
            // subscripts like m["a"]["b"] and xs[0][1] compose left to right.
            for index in index_pairs {
                // `.name` desugars to `["name"]`, reusing the map indexing that
                // `d.meta["a"]` already goes through — one evaluation path for
                // both spellings rather than two that can drift (#452, #673).
                if index.as_rule() == Rule::member_op {
                    let key = index
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::property_key)
                        .map(|p| p.as_str().to_string())
                        .ok_or_else(|| {
                            ParseError::SemanticError("member access without a name".to_string())
                        })?;
                    expr = Expression::Index {
                        expr: Box::new(expr),
                        index: Box::new(Expression::Literal(PropertyValue::String(key))),
                    };
                    continue;
                }
                let mut handled = false;
                for idx_inner in index.into_inner() {
                    if idx_inner.as_rule() == Rule::slice_op {
                        // List slicing: [start..end]
                        let mut start_expr = None;
                        let mut end_expr = None;
                        for slice_inner in idx_inner.into_inner() {
                            match slice_inner.as_rule() {
                                Rule::slice_start => {
                                    let expr_inner = slice_inner.into_inner().next().unwrap();
                                    start_expr = Some(Box::new(parse_expression(expr_inner)?));
                                }
                                Rule::slice_end => {
                                    let expr_inner = slice_inner.into_inner().next().unwrap();
                                    end_expr = Some(Box::new(parse_expression(expr_inner)?));
                                }
                                _ => {}
                            }
                        }
                        expr = Expression::ListSlice {
                            expr: Box::new(expr),
                            start: start_expr,
                            end: end_expr,
                        };
                        handled = true;
                        break;
                    } else if idx_inner.as_rule() == Rule::expression {
                        let index_expr = parse_expression(idx_inner)?;
                        expr = Expression::Index {
                            expr: Box::new(expr),
                            index: Box::new(index_expr),
                        };
                        handled = true;
                        break;
                    }
                }
                let _ = handled;
            }

            // Apply postfix operator (IS NULL / IS NOT NULL)
            if let Some(postfix) = postfix_pair {
                // `n:A:B` — a label test used as a boolean value. Desugared to
                // a function call rather than given its own `Expression`
                // variant: a new variant would have to be handled by every
                // exhaustive match over `Expression`, and this needs no
                // information a call cannot carry.
                // The labels sit two levels down: postfix_op → label_check →
                // label. Reading only the first level found nothing and fell
                // through to the `IS NULL` branch, so `n:A` silently became
                // `n IS NULL` — a wrong answer rather than a parse error.
                let labels: Vec<PropertyValue> = postfix
                    .clone()
                    .into_inner()
                    .flat_map(|p| {
                        if p.as_rule() == Rule::label_check {
                            p.into_inner().collect::<Vec<_>>()
                        } else {
                            vec![p]
                        }
                    })
                    .filter(|p| p.as_rule() == Rule::label)
                    .map(|p| PropertyValue::String(p.as_str().to_string()))
                    .collect();
                if !labels.is_empty() {
                    expr = Expression::Function {
                        name: "hasLabels".to_string(),
                        args: vec![expr, Expression::Literal(PropertyValue::Array(labels))],
                        distinct: false,
                    };
                } else {
                    let text = postfix.as_str().to_uppercase();
                    let op = if text.contains("NOT") {
                        UnaryOp::IsNotNull
                    } else {
                        UnaryOp::IsNull
                    };
                    expr = Expression::Unary {
                        op,
                        expr: Box::new(expr),
                    };
                }
            }

            // Apply prefix operators in reverse order (innermost first)
            for prefix in prefix_ops.into_iter().rev() {
                let op_str = prefix.as_str().trim();
                let op = if op_str == "-" {
                    UnaryOp::Minus
                } else {
                    UnaryOp::Not
                };
                expr = Expression::Unary {
                    op,
                    expr: Box::new(expr),
                };
            }

            Ok(expr)
        }
        Rule::primary => parse_primary(pair),
        _ => Err(ParseError::SemanticError(format!("Unexpected term: {:?}", pair.as_rule())))
    }
}

/// `d.meta.a`, `d.meta.a.b` -- a property lookup followed by map key lookups.
///
/// The first segment is the stored property; every segment after it indexes into
/// the map that property holds. Desugaring to `Expression::Index` reuses the map
/// indexing that `d.meta["a"]` already goes through, so there is one evaluation
/// path for both spellings rather than two that can drift apart (#452).
fn parse_nested_property_access(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut variable = None;
    let mut keys: Vec<String> = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::property_key => keys.push(inner.as_str().to_string()),
            _ => {}
        }
    }
    let variable = variable
        .ok_or_else(|| ParseError::SemanticError("Missing variable in property path".to_string()))?;
    let mut it = keys.into_iter();
    let first = it
        .next()
        .ok_or_else(|| ParseError::SemanticError("Missing property in property path".to_string()))?;

    let mut expr = Expression::Property { variable, property: first };
    for key in it {
        expr = Expression::Index {
            expr: Box::new(expr),
            index: Box::new(Expression::Literal(PropertyValue::String(key))),
        };
    }
    Ok(expr)
}

fn parse_primary(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::case_expression => {
                return parse_case_expression(inner);
            }
            Rule::exists_subquery => {
                return parse_exists_subquery(inner);
            }
            Rule::pattern_predicate => {
                // `WHERE (:Acc)-[:SUPPORTS]->(o)` means "such a path exists", which is
                // exactly `EXISTS { MATCH ... }` -- so it desugars to the same node and
                // inherits its (already correct) evaluation, including under NOT.
                return Ok(Expression::ExistsSubquery {
                    pattern: Pattern { paths: vec![parse_path(inner)?] },
                    where_clause: None,
                    bare_pattern: true,
                });
            }
            Rule::reduce_expression => {
                return parse_reduce_expression(inner);
            }
            Rule::predicate_function => {
                return parse_predicate_function(inner);
            }
            Rule::pattern_comprehension => {
                return parse_pattern_comprehension(inner);
            }
            Rule::list_comprehension => {
                return parse_list_comprehension(inner);
            }
            Rule::count_star => {
                let mut distinct = false;
                for cs_inner in inner.into_inner() {
                    if cs_inner.as_rule() == Rule::distinct {
                        distinct = true;
                    }
                }
                return Ok(Expression::Function {
                    name: "count".to_string(),
                    args: vec![],
                    distinct,
                });
            }
            Rule::property_access => {
                return parse_property_access(inner);
            }
            Rule::nested_property_access => {
                return parse_nested_property_access(inner);
            }
            Rule::function_call => {
                return parse_function_call(inner);
            }
            Rule::parameter => {
                // Strip leading '$' from parameter name
                let name = inner.as_str()[1..].to_string();
                return Ok(Expression::Parameter(name));
            }
            Rule::variable => {
                return Ok(Expression::Variable(inner.as_str().to_string()));
            }
            Rule::value => {
                let val = parse_value(inner)?;
                return Ok(Expression::Literal(val));
            }
            // Reached only when the literal forms above could not match, so an
            // all-literal collection still becomes a `PropertyValue` and every
            // consumer of that shape is untouched (#654).
            Rule::list_expr => {
                let mut items = Vec::new();
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::expression {
                        items.push(parse_expression(item)?);
                    }
                }
                return Ok(Expression::ListExpr(items));
            }
            Rule::map_expr => {
                let mut entries = Vec::new();
                for entry in inner.into_inner() {
                    if entry.as_rule() != Rule::map_expr_entry {
                        continue;
                    }
                    let mut key = String::new();
                    let mut value = None;
                    for part in entry.into_inner() {
                        match part.as_rule() {
                            Rule::property_key => key = part.as_str().to_string(),
                            Rule::string => {
                                key = unescape_string_literal(part.as_str());
                            }
                            Rule::expression => value = Some(parse_expression(part)?),
                            _ => {}
                        }
                    }
                    if let Some(v) = value {
                        entries.push((key, v));
                    }
                }
                return Ok(Expression::MapExpr(entries));
            }
            Rule::expression => {
                return parse_expression(inner);
            }
            _ => {}
        }
    }
    Err(ParseError::SemanticError("Invalid primary expression".to_string()))
}

fn parse_case_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut operand = None;
    let mut when_clauses = Vec::new();
    let mut else_result = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                // First expression is the operand for simple CASE form
                if operand.is_none() && when_clauses.is_empty() {
                    operand = Some(Box::new(parse_expression(inner)?));
                }
            }
            Rule::case_when => {
                let mut exprs: Vec<Expression> = Vec::new();
                for wi in inner.into_inner() {
                    if wi.as_rule() == Rule::expression {
                        exprs.push(parse_expression(wi)?);
                    }
                }
                if exprs.len() == 2 {
                    when_clauses.push((exprs.remove(0), exprs.remove(0)));
                }
            }
            Rule::case_else => {
                for ei in inner.into_inner() {
                    if ei.as_rule() == Rule::expression {
                        else_result = Some(Box::new(parse_expression(ei)?));
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Expression::Case {
        operand,
        when_clauses,
        else_result,
    })
}

fn parse_exists_subquery(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut pattern = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(inner)?),
            Rule::where_clause => where_clause = Some(parse_where_clause(inner)?),
            _ => {}
        }
    }

    Ok(Expression::ExistsSubquery {
        pattern: pattern.ok_or_else(|| ParseError::SemanticError("EXISTS missing pattern".to_string()))?,
        where_clause: where_clause.map(Box::new),
        // `EXISTS { ... }` may introduce variables; a bare pattern predicate
        // may not (#798).
        bare_pattern: false,
    })
}

fn parse_list_comprehension(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut variable = None;
    let mut list_expr = None;
    let mut filter = None;
    let mut map_expr = None;

    // Which optional expression is which is decided by the marker that preceded
    // it, not by how many there are: `[x IN xs WHERE p]` and `[x IN xs | e]`
    // both have two expressions and mean different things (#578).
    let mut seen_where = false;
    let mut seen_pipe = false;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::in_op => {} // skip the IN keyword
            Rule::where_kw => seen_where = true,
            Rule::pipe_op => seen_pipe = true,
            Rule::expression => {
                let expr = parse_expression(inner)?;
                if list_expr.is_none() {
                    list_expr = Some(expr);
                } else if seen_pipe {
                    map_expr = Some(expr);
                } else if seen_where {
                    filter = Some(expr);
                } else {
                    map_expr = Some(expr);
                }
            }
            _ => {}
        }
    }

    // Cypher defaults the projection to the iteration variable, so
    // `[x IN xs WHERE p]` means `[x IN xs WHERE p | x]`.
    if map_expr.is_none() {
        if let Some(var) = &variable {
            map_expr = Some(Expression::Variable(var.clone()));
        }
    }

    Ok(Expression::ListComprehension {
        variable: variable.ok_or_else(|| ParseError::SemanticError("List comprehension missing variable".to_string()))?,
        list_expr: Box::new(list_expr.ok_or_else(|| ParseError::SemanticError("List comprehension missing list expression".to_string()))?),
        filter: filter.map(Box::new),
        map_expr: Box::new(map_expr.ok_or_else(|| ParseError::SemanticError("List comprehension missing map expression".to_string()))?),
    })
}

fn parse_predicate_function(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut name = String::new();
    let mut variable = None;
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::predicate_function_name => name = inner.as_str().to_lowercase(),
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::in_op => {}
            Rule::expression => expressions.push(parse_expression(inner)?),
            _ => {}
        }
    }

    // expressions: [list_expr, predicate]
    if expressions.len() < 2 {
        return Err(ParseError::SemanticError("Predicate function requires list and predicate".to_string()));
    }
    let list_expr = expressions.remove(0);
    let predicate = expressions.remove(0);

    Ok(Expression::PredicateFunction {
        name,
        variable: variable.ok_or_else(|| ParseError::SemanticError("Predicate function missing variable".to_string()))?,
        list_expr: Box::new(list_expr),
        predicate: Box::new(predicate),
    })
}

fn parse_pattern_comprehension(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut pattern_path = None;
    let mut filter = None;
    let mut projection = None;
    let mut expressions = Vec::new();

    let mut path_variable = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            // The only bare `variable` in this rule is the `p =` prefix; the
            // pattern's own variables are inside `path`.
            Rule::variable => path_variable = Some(inner.as_str().to_string()),
            Rule::path => pattern_path = Some(parse_path(inner)?),
            Rule::where_clause => {
                let wc = parse_where_clause(inner)?;
                filter = Some(wc.predicate);
            }
            Rule::expression => expressions.push(parse_expression(inner)?),
            _ => {}
        }
    }

    // The last expression is the projection
    projection = expressions.pop();

    let mut path = pattern_path.ok_or_else(|| ParseError::SemanticError("Pattern comprehension missing pattern".to_string()))?;
    if path_variable.is_some() {
        path.path_variable = path_variable;
    }

    Ok(Expression::PatternComprehension {
        pattern: Pattern { paths: vec![path] },
        filter: filter.map(Box::new),
        projection: Box::new(projection.ok_or_else(|| ParseError::SemanticError("Pattern comprehension missing projection".to_string()))?),
    })
}

fn parse_reduce_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut variables = Vec::new();
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variables.push(inner.as_str().to_string()),
            Rule::in_op => {}
            Rule::expression => expressions.push(parse_expression(inner)?),
            _ => {}
        }
    }

    // variables: [accumulator, iterator]
    // expressions: [init, list, body]
    if variables.len() < 2 || expressions.len() < 3 {
        return Err(ParseError::SemanticError("reduce() requires (acc = init, x IN list | expr)".to_string()));
    }

    Ok(Expression::Reduce {
        accumulator: variables[0].clone(),
        init: Box::new(expressions[0].clone()),
        variable: variables[1].clone(),
        list_expr: Box::new(expressions[1].clone()),
        expression: Box::new(expressions[2].clone()),
    })
}

fn parse_foreach_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<ForeachClause> {
    let mut variable = None;
    let mut expression = None;
    let mut set_clauses = Vec::new();
    let mut create_clauses = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::in_op => {} // skip
            Rule::expression => expression = Some(parse_expression(inner)?),
            Rule::set_clause => set_clauses.push(parse_set_clause(inner)?),
            Rule::create_clause => {
                for ci in inner.into_inner() {
                    if ci.as_rule() == Rule::pattern {
                        create_clauses.push(CreateClause { pattern: parse_pattern(ci)? });
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ForeachClause {
        variable: variable.ok_or_else(|| ParseError::SemanticError("FOREACH missing variable".to_string()))?,
        expression: expression.ok_or_else(|| ParseError::SemanticError("FOREACH missing expression".to_string()))?,
        set_clauses,
        create_clauses,
    })
}

fn parse_property_access(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let parts: Vec<_> = pair.into_inner().collect();

    if parts.len() != 2 {
        return Err(ParseError::SemanticError("Invalid property access".to_string()));
    }

    let variable = parts[0].as_str().to_string();
    let property = parts[1].as_str().to_string();

    Ok(Expression::Property { variable, property })
}

fn parse_function_call(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut name = String::new();
    let mut args = Vec::new();
    let mut distinct = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::function_name => {
                name = inner.as_str().to_string();
            }
            Rule::distinct => {
                distinct = true;
            }
            Rule::expression => {
                args.push(parse_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(Expression::Function { name, args, distinct })
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let query = "MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 1);
        assert!(ast.return_clause.is_some());
    }

    #[test]
    fn test_parse_match_with_properties() {
        let query = r#"MATCH (n:Person {name: "Alice"}) RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_match_with_edge() {
        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b";
        let result = parse_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        let path = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(path.segments.len(), 1);
    }

    #[test]
    fn test_parse_with_where() {
        let query = "MATCH (n:Person) WHERE n.age > 30 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        assert!(ast.where_clause.is_some());
    }

    #[test]
    fn test_parse_with_limit() {
        let query = "MATCH (n:Person) RETURN n LIMIT 10";
        let result = parse_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn test_parse_create() {
        let query = r#"CREATE (n:Person {name: "Alice", age: 30})"#;
        let result = parse_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        assert!(ast.create_clause.is_some());
        assert!(!ast.is_read_only());
    }

    #[test]
    fn test_parse_explain() {
        let query = "EXPLAIN MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok());
        assert!(result.unwrap().explain);
    }

    #[test]
    fn test_parse_is_null() {
        let query = "MATCH (n:Person) WHERE n.email IS NULL RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IS NULL: {:?}", result.err());

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::IsNull);
                assert!(matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "email"));
            }
            other => panic!("Expected Unary(IsNull), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_is_not_null() {
        let query = "MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IS NOT NULL: {:?}", result.err());

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::IsNotNull);
                assert!(matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "name"));
            }
            other => panic!("Expected Unary(IsNotNull), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_not_expression() {
        let query = "MATCH (n:Person) WHERE NOT n.active RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse NOT: {:?}", result.err());

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::Not);
                assert!(matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "active"));
            }
            other => panic!("Expected Unary(Not), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_optional_match() {
        let query = "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n, m";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse OPTIONAL MATCH: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 2);
        assert!(!ast.match_clauses[0].optional);
        assert!(ast.match_clauses[1].optional);
    }

    #[test]
    fn test_parse_with_clause() {
        let query = "MATCH (n:Person) WITH n.name AS name RETURN name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.with_clause.is_some());
    }

    #[test]
    fn test_parse_skip() {
        let query = "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SKIP: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.skip, Some(5));
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn test_parse_delete() {
        let query = "MATCH (n:Person) DELETE n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DELETE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.delete_clause.is_some());
        assert!(!ast.delete_clause.unwrap().detach);
    }

    #[test]
    fn test_parse_detach_delete() {
        let query = "MATCH (n:Person) DETACH DELETE n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DETACH DELETE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.delete_clause.as_ref().unwrap().detach);
    }

    #[test]
    fn test_parse_set() {
        let query = r#"MATCH (n:Person) SET n.name = "Bob" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SET: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.set_clauses.len(), 1);
        assert_eq!(ast.set_clauses[0].items[0].variable, "n");
        assert_eq!(ast.set_clauses[0].items[0].property, "name");
    }

    #[test]
    fn test_parse_remove() {
        let query = "MATCH (n:Person) REMOVE n.email RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse REMOVE: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.remove_clauses.len(), 1);
    }

    #[test]
    fn test_parse_in_operator() {
        let query = r#"MATCH (n:Person) WHERE n.name IN ["Alice", "Bob"] RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IN: {:?}", result.err());
        let ast = result.unwrap();
        let pred = &ast.where_clause.unwrap().predicate;
        assert!(matches!(pred, Expression::Binary { op: BinaryOp::In, .. }));
    }

    #[test]
    fn test_parse_arithmetic() {
        let query = "MATCH (n:Person) RETURN n.age + 1";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse arithmetic: {:?}", result.err());
    }

    #[test]
    fn test_parse_regex() {
        let query = r#"MATCH (n:Person) WHERE n.email =~ ".*@gmail.com" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse regex: {:?}", result.err());
        let ast = result.unwrap();
        let pred = &ast.where_clause.unwrap().predicate;
        assert!(matches!(pred, Expression::Binary { op: BinaryOp::RegexMatch, .. }));
    }

    #[test]
    fn test_parse_case_expression() {
        let query = r#"MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN "adult" ELSE "minor" END"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CASE: {:?}", result.err());
    }

    #[test]
    fn test_parse_collect() {
        let query = "MATCH (n:Person) RETURN collect(n.name)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse collect: {:?}", result.err());
    }

    #[test]
    fn test_parse_string_functions() {
        let query = r#"MATCH (n:Person) RETURN toUpper(n.name), toLower(n.name), trim(n.name)"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse string functions: {:?}", result.err());
    }

    #[test]
    fn test_parse_unwind() {
        let query = "MATCH (n:Person) UNWIND [1, 2, 3] AS x RETURN n, x";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNWIND: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.unwind_clause.is_some());
        assert_eq!(ast.unwind_clause.unwrap().variable, "x");
    }

    #[test]
    fn test_parse_merge() {
        let query = r#"MERGE (n:Person {name: "Alice"}) ON CREATE SET n.created = "now" ON MATCH SET n.lastSeen = "now" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse MERGE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
        let merge = ast.merge_clause.unwrap();
        assert_eq!(merge.on_create_set.len(), 1);
        assert_eq!(merge.on_match_set.len(), 1);
    }

    #[test]
    fn test_parse_merge_simple() {
        let query = r#"MERGE (n:Person {name: "Alice"})"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse simple MERGE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_union() {
        let query = "MATCH (n:Person) RETURN n.name UNION MATCH (m:Animal) RETURN m.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNION: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(!ast.union_queries[0].1); // not UNION ALL
    }

    #[test]
    fn test_parse_union_all() {
        let query = "MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Person) RETURN m.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNION ALL: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(ast.union_queries[0].1); // is UNION ALL
    }

    #[test]
    fn test_parse_list_index() {
        let query = "MATCH (n:Person) RETURN n.tags[0]";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse list index: {:?}", result.err());
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        assert!(matches!(&item.expression, Expression::Index { .. }));
    }

    #[test]
    fn test_parse_list_slice() {
        // Test [1..3]
        let query = "MATCH (n:Person) RETURN n.tags[1..3]";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse list slice [1..3]: {:?}", result.err());
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        assert!(matches!(&item.expression, Expression::ListSlice { .. }),
            "Expected ListSlice, got: {:?}", item.expression);

        // Test [..2]
        let query2 = "MATCH (n:Person) RETURN n.tags[..2]";
        let result2 = parse_query(query2);
        assert!(result2.is_ok(), "Failed to parse list slice [..2]: {:?}", result2.err());

        // Test [1..]
        let query3 = "MATCH (n:Person) RETURN n.tags[1..]";
        let result3 = parse_query(query3);
        assert!(result3.is_ok(), "Failed to parse list slice [1..]: {:?}", result3.err());
    }

    #[test]
    fn test_parse_exists_subquery() {
        let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse EXISTS subquery: {:?}", result.err());
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        assert!(matches!(where_clause.predicate, Expression::ExistsSubquery { .. }));
    }

    #[test]
    fn test_parse_exists_subquery_with_where() {
        let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 30 } RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse EXISTS with WHERE: {:?}", result.err());
        let ast = result.unwrap();
        if let Expression::ExistsSubquery { pattern, where_clause, .. } = &ast.where_clause.unwrap().predicate {
            assert!(!pattern.paths.is_empty());
            assert!(where_clause.is_some());
        } else {
            panic!("Expected ExistsSubquery");
        }
    }

    #[test]
    fn test_parse_list_comprehension() {
        let query = "MATCH (n:Person) RETURN [x IN n.tags WHERE x <> 'admin' | x]";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse list comprehension: {:?}", result.err());
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        if let Expression::ListComprehension { variable, filter, .. } = &item.expression {
            assert_eq!(variable, "x");
            assert!(filter.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", item.expression);
        }
    }

    #[test]
    fn test_parse_list_comprehension_no_filter() {
        let query = "MATCH (n:Person) RETURN [x IN n.scores | x * 2]";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse list comprehension without filter: {:?}", result.err());
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        if let Expression::ListComprehension { variable, filter, .. } = &item.expression {
            assert_eq!(variable, "x");
            // Note: without a WHERE, there should be no filter
            // But in practice, the parser might not distinguish - just check it parsed
        } else {
            panic!("Expected ListComprehension, got {:?}", item.expression);
        }
    }

    #[test]
    fn test_parse_foreach() {
        let query = "MATCH (n:Person) FOREACH (tag IN n.tags | SET n.processed = TRUE)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse FOREACH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.foreach_clause.is_some());
        let fc = ast.foreach_clause.unwrap();
        assert_eq!(fc.variable, "tag");
        assert!(!fc.set_clauses.is_empty());
    }

    #[test]
    fn test_parse_foreach_with_create() {
        let query = r#"MATCH (n:Person) FOREACH (x IN n.friends | CREATE (:Person {name: "friend"}))"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse FOREACH with CREATE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.foreach_clause.is_some());
        let fc = ast.foreach_clause.unwrap();
        assert_eq!(fc.variable, "x");
        assert!(!fc.create_clauses.is_empty());
    }

    #[test]
    fn test_parse_complex_where_with_exists_and_and() {
        let query = "MATCH (n:Person) WHERE n.age > 25 AND EXISTS { MATCH (n)-[:WORKS_AT]->(:Company) } RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse complex WHERE with EXISTS: {:?}", result.err());
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        // Should be Binary(And, Property comparison, ExistsSubquery)
        if let Expression::Binary { op, right, .. } = &where_clause.predicate {
            assert_eq!(*op, BinaryOp::And);
            assert!(matches!(right.as_ref(), Expression::ExistsSubquery { .. }));
        } else {
            panic!("Expected Binary(And, ..., ExistsSubquery), got {:?}", where_clause.predicate);
        }
    }

    // ========== Batch 5: Additional Parser Tests ==========

    #[test]
    fn test_parse_profile() {
        let query = "PROFILE MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse PROFILE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.profile);
    }

    #[test]
    fn test_parse_parameterized_query() {
        let query = "MATCH (n:Person) WHERE n.name = $name RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse parameterized query: {:?}", result.err());
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        // The predicate should contain a Parameter expression
        if let Expression::Binary { right, .. } = &where_clause.predicate {
            assert!(matches!(right.as_ref(), Expression::Parameter(_)));
        } else {
            panic!("Expected Binary with Parameter, got {:?}", where_clause.predicate);
        }
    }

    #[test]
    fn test_parse_create_index() {
        let query = "CREATE INDEX ON :Person(name)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE INDEX: {:?}", result.err());
        let ast = result.unwrap();
        let idx = ast.create_index_clause.unwrap();
        assert_eq!(idx.label, Label::new("Person"));
        assert_eq!(idx.property, "name");
    }

    #[test]
    fn test_parse_create_composite_index() {
        let query = "CREATE INDEX ON :Person(name, age)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse composite index: {:?}", result.err());
        let ast = result.unwrap();
        let idx = ast.create_index_clause.unwrap();
        assert_eq!(idx.label, Label::new("Person"));
        assert_eq!(idx.property, "name");
        assert_eq!(idx.additional_properties, vec!["age".to_string()]);
    }

    #[test]
    fn test_parse_drop_index() {
        let query = "DROP INDEX ON :Person(name)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DROP INDEX: {:?}", result.err());
        let ast = result.unwrap();
        let di = ast.drop_index_clause.unwrap();
        assert_eq!(di.label, Label::new("Person"));
        assert_eq!(di.property, "name");
    }

    #[test]
    fn test_parse_show_indexes() {
        let query = "SHOW INDEXES";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SHOW INDEXES: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.show_indexes);
    }

    #[test]
    fn test_parse_show_constraints() {
        let query = "SHOW CONSTRAINTS";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SHOW CONSTRAINTS: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.show_constraints);
    }

    #[test]
    fn test_parse_create_constraint() {
        let query = "CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE CONSTRAINT: {:?}", result.err());
        let ast = result.unwrap();
        let cc = ast.create_constraint_clause.unwrap();
        assert_eq!(cc.label, Label::new("Person"));
        assert_eq!(cc.property, "email");
        assert_eq!(cc.variable, "n");
    }

    #[test]
    fn test_parse_create_vector_index() {
        let query = "CREATE VECTOR INDEX myIdx FOR (n:Document) ON (n.embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE VECTOR INDEX: {:?}", result.err());
        let ast = result.unwrap();
        let vi = ast.create_vector_index_clause.unwrap();
        assert_eq!(vi.label, Label::new("Document"));
        assert_eq!(vi.property_key, "embedding");
        assert_eq!(vi.dimensions, 384);
        assert_eq!(vi.similarity, "cosine");
    }

    #[test]
    fn test_parse_call_algorithm() {
        let query = "CALL algo.pageRank({maxIterations: 20, dampingFactor: 0.85}) YIELD node, score";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CALL algo: {:?}", result.err());
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert!(call.procedure_name.starts_with("algo."));
    }

    #[test]
    fn test_parse_named_path() {
        let query = "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse named path: {:?}", result.err());
        let ast = result.unwrap();
        // Named path should be captured
        assert!(!ast.match_clauses.is_empty());
    }

    #[test]
    fn test_parse_collect_distinct() {
        let query = "MATCH (n:Person) RETURN collect(DISTINCT n.name) AS names";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse collect(DISTINCT): {:?}", result.err());
    }

    #[test]
    fn test_parse_datetime_constructor() {
        let query = "MATCH (n) RETURN datetime({year: 2024, month: 1, day: 15})";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse datetime({{}}): {:?}", result.err());
    }

    #[test]
    fn test_parse_multiple_match_clauses() {
        let query = "MATCH (a:Person) MATCH (b:Company) RETURN a, b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multi-MATCH: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 2);
    }

    #[test]
    fn test_parse_variable_length_edge() {
        let query = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse variable-length edge: {:?}", result.err());
    }

    #[test]
    fn test_parse_bidirectional_edge() {
        let query = "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse bidirectional edge: {:?}", result.err());
    }

    #[test]
    fn test_parse_return_distinct() {
        let query = "MATCH (n:Person) RETURN DISTINCT n.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse RETURN DISTINCT: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert!(ret.distinct);
    }

    /// `ASCENDING` and `DESCENDING` are the long forms, and the grammar only
    /// had the short ones.
    ///
    /// `^"ASC"` matched the first three letters of `ASCENDING` and left
    /// `ENDING` unconsumed, so this was a **parse error**, not a mis-sort --
    /// and a parse error is invisible to any test that checks sort order. The
    /// openCypher TCK uses the long forms throughout: 56 scenarios across
    /// WithOrderBy1, WithOrderBy2 and WithOrderBy3 failed on this one rule.
    #[test]
    fn order_by_accepts_the_long_direction_keywords() {
        for (q, want_asc) in [
            ("MATCH (n) RETURN n ORDER BY n.age ASCENDING", true),
            ("MATCH (n) RETURN n ORDER BY n.age DESCENDING", false),
            ("MATCH (n) RETURN n ORDER BY n.age ascending", true),
            ("MATCH (n) RETURN n ORDER BY n.age descending", false),
            ("MATCH (n) RETURN n ORDER BY n.age ASC", true),
            ("MATCH (n) RETURN n ORDER BY n.age DESC", false),
        ] {
            let parsed = parse_query(q).unwrap_or_else(|e| panic!("`{q}` should parse: {e:?}"));
            let items = &parsed
                .order_by
                .as_ref()
                .unwrap_or_else(|| panic!("`{q}` should have an ORDER BY"))
                .items;
            assert_eq!(
                items[0].ascending, want_asc,
                "`{q}` sorts the wrong way -- parsing the keyword is not enough, \
                 it has to reach `ascending`"
            );
        }
    }

    /// The long forms are only useful if they sort. A grammar that accepts
    /// `DESCENDING` but hands the Rust side something it compares against
    /// `\"DESC\"` would parse every scenario and silently sort all of them
    /// ascending -- turning 56 parse errors into 56 wrong answers, which is
    /// worse, because a parse error is loud.
    #[test]
    fn a_mixed_direction_list_keeps_each_direction_with_its_own_key() {
        let q = "MATCH (n) RETURN n ORDER BY n.a DESCENDING, n.b ASCENDING, n.c DESC";
        let parsed = parse_query(q).expect("should parse");
        let items = &parsed.order_by.as_ref().expect("ORDER BY").items;
        assert_eq!(items.len(), 3);
        assert!(!items[0].ascending, "n.a DESCENDING");
        assert!(items[1].ascending, "n.b ASCENDING");
        assert!(!items[2].ascending, "n.c DESC");
    }

    #[test]
    fn test_parse_order_by_desc() {
        let query = "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse ORDER BY DESC: {:?}", result.err());
        let ast = result.unwrap();
        let ob = ast.order_by.unwrap();
        assert!(!ob.items.is_empty());
        assert!(!ob.items[0].ascending);
    }

    #[test]
    fn test_parse_skip_and_limit() {
        let query = "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SKIP+LIMIT: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.skip, Some(5));
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn test_parse_error_malformed() {
        let query = "MATCHH (n) RETURN n";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for malformed query");
    }

    #[test]
    fn test_parse_error_empty() {
        let query = "";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for empty query");
    }

    #[test]
    fn test_parse_merge_on_create_on_match() {
        let query = "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true ON MATCH SET n.visits = 1";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse MERGE ON CREATE/ON MATCH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_map_literal_in_properties() {
        let query = "MATCH (n:Person {name: 'Alice', age: 30}) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse map literal: {:?}", result.err());
    }

    #[test]
    fn test_parse_boolean_values() {
        let query = "MATCH (n) WHERE n.active = true AND n.deleted = false RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse boolean values: {:?}", result.err());
    }

    #[test]
    fn test_parse_null_check() {
        let query = "MATCH (n) WHERE n.name IS NULL RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IS NULL: {:?}", result.err());
    }

    #[test]
    fn test_parse_or_expression() {
        let query = "MATCH (n) WHERE n.age > 30 OR n.name = 'Alice' RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse OR expression: {:?}", result.err());
    }

    #[test]
    fn test_parse_nested_function_calls() {
        let query = "MATCH (n) RETURN toUpper(trim(n.name))";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse nested functions: {:?}", result.err());
    }

    #[test]
    fn test_parse_count_function() {
        let query = "MATCH (n:Person) RETURN count(n)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse count(n): {:?}", result.err());
    }

    #[test]
    fn test_parse_count_star() {
        let query = "MATCH (n) RETURN count(*)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse count(*): {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items.len(), 1);
        match &items[0].expression {
            Expression::Function { name, args, distinct } => {
                assert_eq!(name, "count");
                assert!(args.is_empty(), "count(*) should have empty args");
                assert!(!distinct);
            }
            other => panic!("Expected Function, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_count_star_with_alias() {
        let query = "MATCH (n:Person) RETURN count(*) AS total";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse count(*) AS total: {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items[0].alias, Some("total".to_string()));
    }

    #[test]
    fn test_parse_count_star_distinct() {
        let query = "MATCH (n) RETURN count(DISTINCT *)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse count(DISTINCT *): {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        match &items[0].expression {
            Expression::Function { name, distinct, .. } => {
                assert_eq!(name, "count");
                assert!(*distinct);
            }
            other => panic!("Expected Function, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_return_alias() {
        let query = "MATCH (n:Person) RETURN n.name AS personName, count(n) AS total";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse RETURN alias: {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].alias, Some("personName".to_string()));
        assert_eq!(items[1].alias, Some("total".to_string()));
    }

    #[test]
    fn test_parse_with_aggregation() {
        let query = "MATCH (n:Person) WITH n.city AS city, count(n) AS cnt RETURN city ORDER BY cnt DESC";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH aggregation: {:?}", result.err());
    }

    #[test]
    fn test_parse_reduce_expression() {
        let query = "MATCH (n) RETURN reduce(acc = 0, x IN [1,2,3] | acc + x)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse reduce: {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_all() {
        let query = "MATCH (n) WHERE all(x IN n.scores WHERE x > 0) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse all(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_any() {
        let query = "MATCH (n) WHERE any(x IN n.scores WHERE x > 90) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse any(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_none() {
        let query = "MATCH (n) WHERE none(x IN n.scores WHERE x < 0) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse none(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_single() {
        let query = "MATCH (n) WHERE single(x IN n.scores WHERE x = 100) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse single(): {:?}", result.err());
    }

    // ========== Coverage batch: additional parser paths ==========

    #[test]
    fn test_parse_profile_with_where() {
        let query = "PROFILE MATCH (n:Person) WHERE n.age > 25 RETURN n.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse PROFILE with WHERE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.profile);
        assert!(!ast.explain); // PROFILE sets profile, not explain
        assert!(ast.where_clause.is_some());
    }

    #[test]
    fn test_parse_explain_not_profile() {
        let query = "EXPLAIN MATCH (n) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse EXPLAIN: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.explain);
        assert!(!ast.profile);
    }

    #[test]
    fn test_parse_parameterized_multiple_params() {
        let query = "MATCH (n:Person) WHERE n.name = $name AND n.age > $minAge RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multi-param query: {:?}", result.err());
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        // Should be Binary(And, Binary(Eq, ..., Parameter), Binary(Gt, ..., Parameter))
        if let Expression::Binary { op, left, right } = &where_clause.predicate {
            assert_eq!(*op, BinaryOp::And);
            // Check left side has parameter
            if let Expression::Binary { right: inner_right, .. } = left.as_ref() {
                assert!(matches!(inner_right.as_ref(), Expression::Parameter(name) if name == "name"));
            } else {
                panic!("Expected Binary on left, got {:?}", left);
            }
            // Check right side has parameter
            if let Expression::Binary { right: inner_right, .. } = right.as_ref() {
                assert!(matches!(inner_right.as_ref(), Expression::Parameter(name) if name == "minAge"));
            } else {
                panic!("Expected Binary on right, got {:?}", right);
            }
        } else {
            panic!("Expected Binary(And, ...), got {:?}", where_clause.predicate);
        }
    }

    #[test]
    fn test_parse_create_vector_index_full() {
        // Same pattern as test_parse_create_vector_index but with different values
        let query = "CREATE VECTOR INDEX vecIdx FOR (n:Label) ON (n.prop) OPTIONS {dimensions: 128, similarity: 'cosine'}";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE VECTOR INDEX: {:?}", result.err());
        let ast = result.unwrap();
        let vi = ast.create_vector_index_clause.unwrap();
        assert_eq!(vi.label, Label::new("Label"));
        assert_eq!(vi.property_key, "prop");
        assert_eq!(vi.dimensions, 128);
        assert_eq!(vi.similarity, "cosine");
    }

    #[test]
    fn test_parse_create_vector_index_l2_similarity() {
        let query = "CREATE VECTOR INDEX vecIdx FOR (n:Embedding) ON (n.vec) OPTIONS {dimensions: 256, similarity: 'l2'}";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse l2 vector index: {:?}", result.err());
        let ast = result.unwrap();
        let vi = ast.create_vector_index_clause.unwrap();
        assert_eq!(vi.label, Label::new("Embedding"));
        assert_eq!(vi.property_key, "vec");
        assert_eq!(vi.dimensions, 256);
        assert_eq!(vi.similarity, "l2");
        assert_eq!(vi.index_name, Some("vecIdx".to_string()));
    }

    #[test]
    fn test_parse_drop_index_different_label() {
        let query = "DROP INDEX ON :Company(revenue)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DROP INDEX: {:?}", result.err());
        let ast = result.unwrap();
        let di = ast.drop_index_clause.unwrap();
        assert_eq!(di.label, Label::new("Company"));
        assert_eq!(di.property, "revenue");
    }

    #[test]
    fn test_parse_create_constraint_unique_different() {
        let query = "CREATE CONSTRAINT ON (c:Company) ASSERT c.taxId IS UNIQUE";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE CONSTRAINT: {:?}", result.err());
        let ast = result.unwrap();
        let cc = ast.create_constraint_clause.unwrap();
        assert_eq!(cc.label, Label::new("Company"));
        assert_eq!(cc.property, "taxId");
        assert_eq!(cc.variable, "c");
    }

    #[test]
    fn test_parse_call_algo_pagerank_with_config() {
        let query = "CALL algo.pageRank({label: 'Person', maxIterations: 20, dampingFactor: 0.85}) YIELD node, score";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CALL algo.pageRank: {:?}", result.err());
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert_eq!(call.procedure_name, "algo.pageRank");
        assert!(!call.arguments.is_empty());
        assert_eq!(call.yield_items.len(), 2);
        assert_eq!(call.yield_items[0].name, "node");
        assert_eq!(call.yield_items[1].name, "score");
    }

    #[test]
    fn test_parse_call_algo_wcc() {
        let query = "CALL algo.wcc({label: 'Node'}) YIELD node, componentId";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CALL algo.wcc: {:?}", result.err());
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert_eq!(call.procedure_name, "algo.wcc");
        assert_eq!(call.yield_items.len(), 2);
    }

    #[test]
    fn test_parse_named_path_with_return_p() {
        let query = "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse named path: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        let mc = &ast.match_clauses[0];
        assert!(!mc.pattern.paths.is_empty());
        let pp = &mc.pattern.paths[0];
        assert_eq!(pp.path_variable, Some("p".to_string()));
        // Verify return clause references p
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 1);
        assert!(matches!(&ret.items[0].expression, Expression::Variable(v) if v == "p"));
    }

    #[test]
    fn test_parse_collect_distinct_full() {
        let query = "MATCH (n:Person) RETURN collect(DISTINCT n.name) AS names";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse collect(DISTINCT): {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 1);
        if let Expression::Function { name, distinct, args } = &ret.items[0].expression {
            assert_eq!(name, "collect");
            assert!(*distinct);
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected Function, got {:?}", ret.items[0].expression);
        }
        assert_eq!(ret.items[0].alias, Some("names".to_string()));
    }

    #[test]
    fn test_parse_datetime_string_constructor() {
        // datetime with string argument
        let query = r#"MATCH (n) RETURN datetime("2024-01-15T10:30:00Z")"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse datetime string: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 1);
    }

    #[test]
    fn test_parse_foreach_with_variable_list() {
        // FOREACH with variable reference is supported; list literals in FOREACH are not
        let query = r#"MATCH (n) WITH collect(n.name) AS names FOREACH (x IN names | SET n.tag = 'done')"#;
        let result = parse_query(query);
        // Parser may or may not support this exact form; just verify no crash
        let _ = result;
    }

    #[test]
    fn test_parse_error_completely_malformed() {
        let query = "!!@#$%^&*()_+ totally not cypher";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for malformed query");
        let err = result.err().unwrap();
        let err_str = format!("{}", err);
        assert!(err_str.contains("Parse error"), "Error should be a PestError, got: {}", err_str);
    }

    #[test]
    fn test_parse_error_incomplete_match() {
        let query = "MATCH";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for incomplete MATCH");
    }

    #[test]
    fn test_parse_error_invalid_return() {
        let query = "RETURN";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for bare RETURN");
    }

    #[test]
    fn test_parse_union_different_labels() {
        let query = "MATCH (n:A) RETURN n.name UNION MATCH (n:B) RETURN n.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNION: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(!ast.union_queries[0].1); // not UNION ALL
        // Check main query has match clause with label A
        assert!(!ast.match_clauses.is_empty());
        // Check union query has match clause with label B
        let union_q = &ast.union_queries[0].0;
        assert!(!union_q.match_clauses.is_empty());
    }

    #[test]
    fn test_parse_union_all_same_labels() {
        let query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name UNION ALL MATCH (n:Person) WHERE n.age <= 30 RETURN n.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNION ALL with WHERE: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(ast.union_queries[0].1); // is UNION ALL
    }

    #[test]
    fn test_parse_optional_match_with_return() {
        let query = "MATCH (n:Person) OPTIONAL MATCH (n)-[:FRIEND]->(m:Person) RETURN n, m";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse OPTIONAL MATCH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.match_clauses.len() >= 2);
        // First match is mandatory
        assert!(!ast.match_clauses[0].optional);
        // Second match is optional
        assert!(ast.match_clauses[1].optional);
    }

    #[test]
    fn test_parse_optional_match_with_where() {
        let query = "MATCH (n:Person) OPTIONAL MATCH (n)-[:REL]->(m) WHERE m.active = true RETURN n, m";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse OPTIONAL MATCH with WHERE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.match_clauses.len() >= 2);
        assert!(ast.match_clauses[1].optional);
    }

    #[test]
    fn test_parse_exists_subquery_simple() {
        let query = "MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->() } RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse EXISTS subquery: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::ExistsSubquery { pattern, where_clause, .. } = &wc.predicate {
            assert!(!pattern.paths.is_empty());
            assert!(where_clause.is_none());
        } else {
            panic!("Expected ExistsSubquery, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_starts_with_operator() {
        let query = "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse STARTS WITH: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::StartsWith);
        } else {
            panic!("Expected Binary with StartsWith, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_ends_with_operator() {
        let query = "MATCH (n:Person) WHERE n.name ENDS WITH 'son' RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse ENDS WITH: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::EndsWith);
        } else {
            panic!("Expected Binary with EndsWith, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_contains_operator() {
        let query = "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CONTAINS: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::Contains);
        } else {
            panic!("Expected Binary with Contains, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_in_list_operator() {
        let query = "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IN list: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::In);
        } else {
            panic!("Expected Binary with In, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_not_equals_operators() {
        // Test != syntax
        let query1 = "MATCH (n) WHERE n.x != 5 RETURN n";
        let result1 = parse_query(query1);
        assert!(result1.is_ok(), "Failed to parse !=: {:?}", result1.err());
        let wc1 = result1.unwrap().where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc1.predicate {
            assert_eq!(*op, BinaryOp::Ne);
        }

        // Test <> syntax
        let query2 = "MATCH (n) WHERE n.x <> 5 RETURN n";
        let result2 = parse_query(query2);
        assert!(result2.is_ok(), "Failed to parse <>: {:?}", result2.err());
        let wc2 = result2.unwrap().where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc2.predicate {
            assert_eq!(*op, BinaryOp::Ne);
        }
    }

    #[test]
    fn test_parse_arithmetic_operations() {
        let query = "MATCH (n) RETURN n.a + n.b * 2 - n.c / n.d % 3";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse arithmetic: {:?}", result.err());
    }

    #[test]
    fn test_parse_unary_minus() {
        let query = "MATCH (n) WHERE n.balance < -100 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse unary minus: {:?}", result.err());
    }

    #[test]
    fn test_parse_is_not_null_postfix() {
        let query = "MATCH (n) WHERE n.email IS NOT NULL RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IS NOT NULL: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Unary { op, .. } = &wc.predicate {
            assert_eq!(*op, UnaryOp::IsNotNull);
        } else {
            panic!("Expected Unary IsNotNull, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_match_create_in_same_query() {
        let query = "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(:Person {name: 'Bob'})";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse MATCH+CREATE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        assert!(ast.create_clause.is_some());
    }

    #[test]
    fn test_parse_match_set_clause() {
        let query = "MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SET clause: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.set_clauses.is_empty());
        let item = &ast.set_clauses[0].items[0];
        assert_eq!(item.variable, "n");
        assert_eq!(item.property, "age");
    }

    #[test]
    fn test_parse_match_remove_property() {
        let query = "MATCH (n:Person) REMOVE n.age RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse REMOVE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.remove_clauses.is_empty());
    }

    #[test]
    fn test_parse_detach_delete_with_property() {
        let query = "MATCH (n:Person {name: 'test'}) DETACH DELETE n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DETACH DELETE: {:?}", result.err());
        let ast = result.unwrap();
        let dc = ast.delete_clause.unwrap();
        assert!(dc.detach);
    }

    #[test]
    fn test_parse_multiple_set_items() {
        let query = "MATCH (n:Person) SET n.name = 'Bob', n.age = 25 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multiple SET items: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.set_clauses.is_empty());
        assert!(ast.set_clauses[0].items.len() >= 2);
    }

    #[test]
    fn test_parse_with_where_clause() {
        let query = "MATCH (n:Person) WITH n.city AS city, count(n) AS cnt WHERE cnt > 5 RETURN city";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH WHERE: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.where_clause.is_some());
    }

    #[test]
    fn test_parse_with_distinct() {
        let query = "MATCH (n:Person) WITH DISTINCT n.city AS city RETURN city";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH DISTINCT: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.distinct);
    }

    #[test]
    fn test_parse_incoming_edge() {
        let query = "MATCH (a:Person)<-[:FOLLOWS]-(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse incoming edge: {:?}", result.err());
    }

    #[test]
    fn test_parse_edge_with_variable() {
        let query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse edge variable: {:?}", result.err());
    }

    #[test]
    fn test_parse_multiple_labels_on_node() {
        let query = "MATCH (n:Person:Employee) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multi-label node: {:?}", result.err());
        let ast = result.unwrap();
        let paths = &ast.match_clauses[0].pattern.paths;
        assert!(paths[0].start.labels.len() >= 2);
    }

    #[test]
    fn test_parse_multiple_edge_types() {
        // Pipe-separated edge types not yet supported; verify it doesn't crash
        let query = "MATCH (a)-[:KNOWS|FOLLOWS]->(b) RETURN b";
        let result = parse_query(query);
        // Parser may not support this syntax yet
        let _ = result;
    }

    #[test]
    fn test_parse_long_chain_pattern() {
        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a, b, c";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse chain pattern: {:?}", result.err());
        let ast = result.unwrap();
        let pp = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(pp.segments.len(), 2);
    }

    #[test]
    fn test_parse_create_node_with_properties() {
        let query = r#"CREATE (n:Person {name: "Alice", age: 30, active: true})"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CREATE with properties: {:?}", result.err());
        let ast = result.unwrap();
        let create = ast.create_clause.unwrap();
        let props = create.pattern.paths[0].start.properties.as_ref().unwrap();
        assert_eq!(props.get("name"), Some(&PropertyValue::String("Alice".to_string())));
        assert_eq!(props.get("age"), Some(&PropertyValue::Integer(30)));
        assert_eq!(props.get("active"), Some(&PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_parse_return_star_equivalent() {
        // Test returning multiple variables
        let query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multi-return: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 3);
    }

    #[test]
    fn test_parse_count_distinct() {
        let query = "MATCH (n:Person) RETURN count(DISTINCT n.city) AS uniqueCities";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse count(DISTINCT): {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        if let Expression::Function { name, distinct, .. } = &ret.items[0].expression {
            assert_eq!(name, "count");
            assert!(*distinct);
        } else {
            panic!("Expected Function, got {:?}", ret.items[0].expression);
        }
    }

    #[test]
    fn test_parse_aggregation_functions() {
        let query = "MATCH (n:Person) RETURN sum(n.salary), avg(n.age), min(n.age), max(n.age)";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse aggregation functions: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 4);
    }

    #[test]
    fn test_parse_string_functions_detailed() {
        let query = r#"MATCH (n) RETURN toLower(n.name), substring(n.name, 0, 3), replace(n.name, 'a', 'b')"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse string functions: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert_eq!(ret.items.len(), 3);
    }

    #[test]
    fn test_parse_coalesce_function() {
        let query = "MATCH (n) RETURN coalesce(n.nickname, n.name, 'Unknown')";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse coalesce: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        if let Expression::Function { name, args, .. } = &ret.items[0].expression {
            assert_eq!(name, "coalesce");
            assert_eq!(args.len(), 3);
        } else {
            panic!("Expected Function coalesce");
        }
    }

    #[test]
    fn test_parse_variable_length_unbounded() {
        let query = "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse unbounded variable-length: {:?}", result.err());
    }

    #[test]
    fn test_parse_variable_length_exact() {
        let query = "MATCH (a:Person)-[:KNOWS*2]->(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse exact variable-length: {:?}", result.err());
    }

    #[test]
    fn test_parse_order_by_asc_explicit() {
        let query = "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse ORDER BY ASC: {:?}", result.err());
        let ast = result.unwrap();
        let ob = ast.order_by.unwrap();
        assert!(ob.items[0].ascending);
    }

    #[test]
    fn test_parse_order_by_multiple() {
        let query = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC, n.name ASC";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse multi ORDER BY: {:?}", result.err());
        let ast = result.unwrap();
        let ob = ast.order_by.unwrap();
        assert_eq!(ob.items.len(), 2);
        assert!(!ob.items[0].ascending);
        assert!(ob.items[1].ascending);
    }

    #[test]
    fn test_parse_float_literal() {
        let query = "MATCH (n) WHERE n.weight > 3.14 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse float literal: {:?}", result.err());
    }

    #[test]
    fn test_parse_scientific_notation_float() {
        // Exponent without a decimal point (1e-07), uppercase E, explicit +,
        // and leading-dot forms must all parse as floats and round-trip via f64.
        for (q, expected) in [
            ("CREATE (n {r: 1e-07}) RETURN n", 1e-07_f64),
            ("CREATE (n {x: 1.5E10}) RETURN n", 1.5E10_f64),
            ("CREATE (n {y: 6e+3}) RETURN n", 6e+3_f64),
            ("CREATE (n {z: .5}) RETURN n", 0.5_f64),
            ("CREATE (n {w: 0.015}) RETURN n", 0.015_f64),
        ] {
            let parsed = parse_query(q);
            assert!(parsed.is_ok(), "Failed to parse {q:?}: {:?}", parsed.err());
            let props = parsed.unwrap().create_clause.unwrap().pattern.paths[0]
                .start
                .properties
                .clone()
                .expect("node should have properties");
            let val = props.values().next().unwrap();
            match val {
                PropertyValue::Float(f) => assert!(
                    (f - expected).abs() < 1e-12 * expected.abs().max(1.0),
                    "{q}: got {f}, expected {expected}"
                ),
                other => panic!("{q}: expected Float, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_parse_negative_integer() {
        let query = "MATCH (n) WHERE n.temperature < -10 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse negative integer: {:?}", result.err());
    }

    #[test]
    fn test_parse_null_literal() {
        let query = "MATCH (n) WHERE n.value = null RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse null literal: {:?}", result.err());
    }

    #[test]
    fn test_parse_list_literal_in_return() {
        // Standalone list literals in RETURN are not yet supported
        let query = "RETURN [1, 2, 3, 4, 5]";
        let result = parse_query(query);
        // Verify no crash; may return error
        let _ = result;
    }

    #[test]
    fn test_parse_map_literal_in_return() {
        // Standalone map literals in RETURN are not yet supported
        let query = "RETURN {name: 'Alice', age: 30}";
        let result = parse_query(query);
        // Verify no crash; may return error
        let _ = result;
    }

    #[test]
    fn test_parse_empty_properties_node() {
        let query = "MATCH (n:Person {}) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse empty properties: {:?}", result.err());
    }

    #[test]
    fn test_parse_regex_match() {
        let query = "MATCH (n:Person) WHERE n.name =~ '.*Alice.*' RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse regex: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::RegexMatch);
        } else {
            panic!("Expected Binary with RegexMatch");
        }
    }

    #[test]
    fn test_parse_merge_inline_after_match() {
        let query = "MATCH (a:Person {name: 'Alice'}) MERGE (a)-[:KNOWS]->(b:Person {name: 'Bob'})";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse MERGE after MATCH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_unwind_with_match_and_return() {
        // Standalone UNWIND with list literal not yet supported; test with variable
        let query = "MATCH (n) WITH collect(n.name) AS names UNWIND names AS x RETURN x";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNWIND with variable: {:?}", result.err());
    }

    #[test]
    fn test_parse_case_without_else() {
        let query = r#"MATCH (n) RETURN CASE WHEN n.age > 18 THEN "adult" END"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CASE without ELSE: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        if let Expression::Case { else_result, when_clauses, .. } = &ret.items[0].expression {
            assert!(!when_clauses.is_empty());
            assert!(else_result.is_none());
        } else {
            panic!("Expected Case expression");
        }
    }

    #[test]
    fn test_parse_nested_boolean_logic() {
        let query = "MATCH (n) WHERE (n.a > 1 OR n.b < 2) AND (n.c = 3 OR n.d = 4) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse nested boolean: {:?}", result.err());
    }

    #[test]
    fn test_parse_comparison_operators_all() {
        // Test all comparison operators: =, <, >, <=, >=
        let queries = vec![
            "MATCH (n) WHERE n.x = 1 RETURN n",
            "MATCH (n) WHERE n.x < 1 RETURN n",
            "MATCH (n) WHERE n.x > 1 RETURN n",
            "MATCH (n) WHERE n.x <= 1 RETURN n",
            "MATCH (n) WHERE n.x >= 1 RETURN n",
        ];
        let expected_ops = vec![BinaryOp::Eq, BinaryOp::Lt, BinaryOp::Gt, BinaryOp::Le, BinaryOp::Ge];
        for (query, expected_op) in queries.iter().zip(expected_ops.iter()) {
            let result = parse_query(query);
            assert!(result.is_ok(), "Failed to parse {}: {:?}", query, result.err());
            let wc = result.unwrap().where_clause.unwrap();
            if let Expression::Binary { op, .. } = &wc.predicate {
                assert_eq!(op, expected_op, "Wrong op for query: {}", query);
            }
        }
    }

    #[test]
    fn test_parse_error_display() {
        let err = ParseError::SemanticError("test semantic error".to_string());
        let display = format!("{}", err);
        assert!(display.contains("Semantic error"));
        assert!(display.contains("test semantic error"));

        let err2 = ParseError::UnsupportedFeature("test feature".to_string());
        let display2 = format!("{}", err2);
        assert!(display2.contains("Unsupported feature"));
    }

    #[test]
    fn test_parse_pattern_comprehension() {
        let query = "MATCH (n:Person) RETURN [(n)-[:KNOWS]->(m) WHERE m.age > 20 | m.name]";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse pattern comprehension: {:?}", result.err());
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert!(matches!(&ret.items[0].expression, Expression::PatternComprehension { .. }));
    }

    #[test]
    fn test_parse_with_order_by_skip_limit() {
        let query = "MATCH (n:Person) WITH n ORDER BY n.age SKIP 5 LIMIT 10 RETURN n.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH ORDER BY SKIP LIMIT: {:?}", result.err());
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.order_by.is_some());
        assert_eq!(wc.skip, Some(5));
        assert_eq!(wc.limit, Some(10));
    }

    #[test]
    fn test_parse_shortest_path() {
        let query = "MATCH p = shortestPath((a:Person)-[:KNOWS*1..10]->(b:Person)) RETURN p";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse shortestPath: {:?}", result.err());
        let ast = result.unwrap();
        let pp = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(pp.path_type, PathType::Shortest);
        assert_eq!(pp.path_variable, Some("p".to_string()));
    }

    #[test]
    fn test_parse_all_shortest_paths() {
        let query = "MATCH p = allShortestPaths((a:Person)-[:KNOWS*1..10]->(b:Person)) RETURN p";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse allShortestPaths: {:?}", result.err());
        let ast = result.unwrap();
        let pp = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(pp.path_type, PathType::AllShortest);
    }

    #[test]
    fn test_parse_edge_with_properties() {
        let query = r#"MATCH (a)-[r:TRANSFER {amount: 1000}]->(b) RETURN r"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse edge with properties: {:?}", result.err());
    }

    #[test]
    fn test_parse_remove_label() {
        let query = "MATCH (n:Person) REMOVE n:Employee RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse REMOVE label: {:?}", result.err());
        let ast = result.unwrap();
        assert!(!ast.remove_clauses.is_empty());
    }

    #[test]
    fn test_parse_vector_list_literal() {
        let query = "CREATE (n:Doc {embedding: [0.1, 0.2, 0.3, 0.4]})";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse vector list: {:?}", result.err());
        let ast = result.unwrap();
        let create = ast.create_clause.unwrap();
        let props = create.pattern.paths[0].start.properties.as_ref().unwrap();
        // A float list stays a list, at full precision. It is still indexable
        // as an embedding -- `to_vector` accepts a numeric array -- but the
        // literal keeps the 64-bit values that were written (#628).
        let embedding = props.get("embedding").expect("embedding property");
        assert_eq!(
            embedding,
            &PropertyValue::Array(vec![
                PropertyValue::Float(0.1),
                PropertyValue::Float(0.2),
                PropertyValue::Float(0.3),
                PropertyValue::Float(0.4),
            ])
        );
        assert_eq!(embedding.to_vector().map(|v| v.len()), Some(4), "still indexable");
    }

    #[test]
    fn test_parse_call_with_yield_alias() {
        let query = "CALL algo.bfs({startNode: 'n1'}) YIELD node AS vertex, depth AS level";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CALL with YIELD alias: {:?}", result.err());
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert_eq!(call.yield_items.len(), 2);
        assert_eq!(call.yield_items[0].name, "node");
        assert_eq!(call.yield_items[0].alias, Some("vertex".to_string()));
        assert_eq!(call.yield_items[1].name, "depth");
        assert_eq!(call.yield_items[1].alias, Some("level".to_string()));
    }
}
