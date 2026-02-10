//! OpenCypher query parser using Pest
//!
//! Implements REQ-CYPHER-001, REQ-CYPHER-002

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
                for inner in pair.into_inner() {
                    match inner.as_rule() {
                        Rule::explain_clause => {
                            query.explain = true;
                        }
                        Rule::statement => {
                            parse_statement(inner, &mut query)?;
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
            Rule::create_vector_index_stmt => {
                parse_create_vector_index_statement(inner, query)?;
            }
            Rule::create_index_stmt => {
                parse_create_index_statement(inner, query)?;
            }
            Rule::call_stmt => {
                parse_call_statement(inner, query)?;
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
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::label => label = Some(Label::new(inner.as_str())),
            Rule::property_key => property = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    query.create_index_clause = Some(CreateIndexClause {
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
                query.where_clause = Some(parse_where_clause(inner)?);
            }
            Rule::with_clause => {
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
            Rule::set_clause => {
                query.set_clauses.push(parse_set_clause(inner)?);
            }
            Rule::remove_clause => {
                query.remove_clauses.push(parse_remove_clause(inner)?);
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
        if inner.as_rule() == Rule::path {
            paths.push(parse_path(inner)?);
        }
    }

    Ok(Pattern { paths })
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

    Ok(PathPattern { start, segments })
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

            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::unary_op => prefix_ops.push(inner),
                    Rule::primary => primary_pair = Some(inner),
                    Rule::postfix_op => postfix_pair = Some(inner),
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
            Rule::property_access => {
                return parse_property_access(inner);
            }
            Rule::function_call => {
                return parse_function_call(inner);
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

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::function_name => {
                name = inner.as_str().to_string();
            }
            Rule::expression => {
                args.push(parse_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(Expression::Function { name, args })
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
}
