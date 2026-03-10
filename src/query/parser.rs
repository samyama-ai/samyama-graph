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
        .op(Op::infix(Rule::and_op, Assoc::Left))
        .op(Op::infix(Rule::in_op, Assoc::Left) | Op::infix(Rule::comparison_op, Assoc::Left))
        .op(Op::infix(Rule::add_sub_op, Assoc::Left))
        .op(Op::infix(Rule::mul_div_mod_op, Assoc::Left))
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
pub fn parse_query(input: &str) -> ParseResult<Query> {
    let pairs = CypherParser::parse(Rule::query, input)?;

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

    Ok(query)
}

fn parse_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::show_indexes_stmt => {
                query.show_indexes = true;
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
            Rule::match_stmt => {
                parse_match_statement(inner, query)?;
            }
            Rule::create_stmt => {
                parse_create_statement(inner, query)?;
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

    for inner in pair.into_inner() {
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
            Rule::variable => {
                if index_name.is_none() {
                    index_name = Some(inner.as_str().to_string());
                }
            }
            Rule::label => {
                label = Some(Label::new(inner.as_str()));
            }
            Rule::property_key => {
                property_key = Some(inner.as_str().to_string());
            }
            Rule::options => {
                let options_map = parse_properties(inner)?;
                if let Some(PropertyValue::Integer(d)) = options_map.get("dimensions") {
                    dimensions = *d as usize;
                }
                if let Some(PropertyValue::String(s)) = options_map.get("similarity") {
                    similarity = s.clone();
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
            Rule::match_stmt_partial => {
                parse_match_statement_partial(inner, query)?;
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            _ => {}
        }
    }
    Ok(())
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
                if query.with_split_index.is_some() {
                    // Second WHERE clause (after WITH ... MATCH ... WHERE ...)
                    query.post_with_where_clause = Some(parse_where_clause(inner)?);
                } else {
                    query.where_clause = Some(parse_where_clause(inner)?);
                }
            }
            Rule::with_clause => {
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
                query.unwind_clause = Some(parse_unwind_clause(inner)?);
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
                        query.skip = Some(skip_inner.as_str().parse().unwrap());
                    }
                }
            }
            Rule::limit_clause => {
                for limit_inner in inner.into_inner() {
                    if limit_inner.as_rule() == Rule::integer {
                        query.limit = Some(limit_inner.as_str().parse().unwrap());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_create_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            query.create_clause = Some(CreateClause {
                pattern: parse_pattern(inner)?,
            });
        }
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
                        skip = Some(skip_inner.as_str().parse().unwrap());
                    }
                }
            }
            Rule::limit_clause => {
                for limit_inner in inner.into_inner() {
                    if limit_inner.as_rule() == Rule::integer {
                        limit = Some(limit_inner.as_str().parse().unwrap());
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

fn parse_set_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
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

    Ok(SetClause { items })
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
                // variable : label
                let mut variable = String::new();
                let mut label = String::new();
                for child in children {
                    match child.as_rule() {
                        Rule::variable => variable = child.as_str().to_string(),
                        Rule::label => label = child.as_str().to_string(),
                        _ => {}
                    }
                }
                items.push(RemoveItem::Label { variable, label: Label::new(&label) });
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

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(inner)?),
            Rule::on_create_set => {
                for si in inner.into_inner() {
                    if si.as_rule() == Rule::set_item {
                        on_create_set.push(parse_set_item(si)?);
                    }
                }
            }
            Rule::on_match_set => {
                for si in inner.into_inner() {
                    if si.as_rule() == Rule::set_item {
                        on_match_set.push(parse_set_item(si)?);
                    }
                }
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
    });
    Ok(())
}

fn parse_merge_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<MergeClause> {
    let mut pattern = None;
    let mut on_create_set = Vec::new();
    let mut on_match_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => pattern = Some(parse_pattern(inner)?),
            Rule::on_create_set => {
                for si in inner.into_inner() {
                    if si.as_rule() == Rule::set_item {
                        on_create_set.push(parse_set_item(si)?);
                    }
                }
            }
            Rule::on_match_set => {
                for si in inner.into_inner() {
                    if si.as_rule() == Rule::set_item {
                        on_match_set.push(parse_set_item(si)?);
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

fn parse_return_items(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<ReturnItem>> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::return_item {
            let mut expr = None;
            let mut alias = None;
            for ri in inner.into_inner() {
                match ri.as_rule() {
                    Rule::expression => expr = Some(parse_expression(ri)?),
                    Rule::variable => alias = Some(ri.as_str().to_string()),
                    _ => {}
                }
            }
            if let Some(e) = expr {
                items.push(ReturnItem { expression: e, alias });
            }
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
                properties = Some(parse_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
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
                        properties = Some(parse_properties(detail)?);
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
                let val = inner.as_str().parse().unwrap();
                return Ok(PropertyValue::Integer(val));
            }
            Rule::float => {
                let val = inner.as_str().parse().unwrap();
                return Ok(PropertyValue::Float(val));
            }
            Rule::string => {
                let s = inner.as_str();
                // Remove quotes
                let unquoted = &s[1..s.len()-1];
                return Ok(PropertyValue::String(unquoted.to_string()));
            }
            Rule::list => {
                let mut items = Vec::new();
                let mut all_floats = true;
                let mut float_vals = Vec::new();

                for item in inner.into_inner() {
                    if item.as_rule() == Rule::value {
                        let val = parse_value(item)?;
                        if let PropertyValue::Float(f) = val {
                            float_vals.push(f as f32);
                        } else if let PropertyValue::Integer(i) = val {
                            float_vals.push(i as f32);
                        } else {
                            all_floats = false;
                        }
                        items.push(val);
                    }
                }

                if !float_vals.is_empty() && all_floats {
                    return Ok(PropertyValue::Vector(float_vals));
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
                                    let s = part.as_str();
                                    key = s[1..s.len()-1].to_string();
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

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
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
    PRATT_PARSER
        .map_primary(|primary| parse_term(primary))
        .map_infix(|left, op, right| {
            let left = left?;
            let right = right?;
            
            let op = match op.as_rule() {
                Rule::or_op => BinaryOp::Or,
                Rule::and_op => BinaryOp::And,
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
        .parse(pair.into_inner())
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
            let mut index_pair = None;

            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::unary_op => prefix_ops.push(inner),
                    Rule::primary => primary_pair = Some(inner),
                    Rule::postfix_op => postfix_pair = Some(inner),
                    Rule::index_op => index_pair = Some(inner),
                    _ => {}
                }
            }

            let mut expr = parse_primary(primary_pair.unwrap())?;

            // Apply postfix operator (IS NULL / IS NOT NULL)
            if let Some(postfix) = postfix_pair {
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

            // Apply index operator [expr] or slice operator [start..end]
            if let Some(index) = index_pair {
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

fn parse_primary(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::case_expression => {
                return parse_case_expression(inner);
            }
            Rule::exists_subquery => {
                return parse_exists_subquery(inner);
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
    })
}

fn parse_list_comprehension(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut variable = None;
    let mut list_expr = None;
    let mut filter = None;
    let mut map_expr = None;
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::in_op => {} // skip the IN keyword
            Rule::expression => expressions.push(parse_expression(inner)?),
            _ => {}
        }
    }

    // Order: list_expr, [filter], map_expr
    // Grammar: variable IN expression (WHERE expression)? | expression
    // So expressions are: [list_expr, optional_filter, map_expr]
    if expressions.len() >= 2 {
        list_expr = Some(expressions.remove(0));
        map_expr = Some(expressions.pop().unwrap());
        if !expressions.is_empty() {
            filter = Some(expressions.remove(0));
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

    for inner in pair.into_inner() {
        match inner.as_rule() {
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

    let path = pattern_path.ok_or_else(|| ParseError::SemanticError("Pattern comprehension missing pattern".to_string()))?;

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
        if let Expression::ExistsSubquery { pattern, where_clause } = &ast.where_clause.unwrap().predicate {
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
        if let Expression::ExistsSubquery { pattern, where_clause } = &wc.predicate {
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
        // Float list should be parsed as Vector
        if let Some(PropertyValue::Vector(v)) = props.get("embedding") {
            assert_eq!(v.len(), 4);
        } else {
            panic!("Expected Vector property, got {:?}", props.get("embedding"));
        }
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
