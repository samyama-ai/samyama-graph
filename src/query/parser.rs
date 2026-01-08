//! OpenCypher query parser using Pest
//!
//! Implements REQ-CYPHER-001, REQ-CYPHER-002

use crate::graph::{EdgeType, Label, PropertyValue};
use crate::query::ast::*;
use pest::Parser;
use pest_derive::Parser;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "query/cypher.pest"]
struct CypherParser;

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
                        Rule::explain => {
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
    let mut pattern = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => {
                pattern = Some(parse_pattern(inner)?);
            }
            Rule::where_clause => {
                query.where_clause = Some(parse_where_clause(inner)?);
            }
            Rule::call_clause => {
                query.call_clause = Some(parse_call_clause(inner)?);
            }
            Rule::create_clause => {
                // Handle CREATE clause inside MATCH (for MATCH...CREATE pattern)
                for create_inner in inner.into_inner() {
                    if create_inner.as_rule() == Rule::pattern {
                        query.create_clause = Some(CreateClause {
                            pattern: parse_pattern(create_inner)?,
                        });
                    }
                }
            }
            Rule::return_clause => {
                query.return_clause = Some(parse_return_clause(inner)?);
            }
            Rule::order_by_clause => {
                query.order_by = Some(parse_order_by_clause(inner)?);
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

    if let Some(p) = pattern {
        query.match_clauses.push(MatchClause {
            pattern: p,
            optional: false,
        });
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
    let mut expr_parts: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if expr_parts.is_empty() {
        return Err(ParseError::SemanticError("Empty expression".to_string()));
    }

    // Simple case: just one term
    if expr_parts.len() == 1 {
        return parse_term(expr_parts.remove(0));
    }

    // Parse binary operations left-to-right (simplified)
    let mut result = parse_term(expr_parts.remove(0))?;

    while !expr_parts.is_empty() {
        if expr_parts.len() < 2 {
            break;
        }

        let op_pair = expr_parts.remove(0);
        let right_pair = expr_parts.remove(0);

        let op = parse_binary_op(op_pair)?;
        let right = parse_term(right_pair)?;

        result = Expression::Binary {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }

    Ok(result)
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    match pair.as_rule() {
        Rule::term => {
            let inner: Vec<_> = pair.into_inner().collect();
            if inner.len() == 1 {
                parse_term(inner[0].clone())
            } else {
                // Has unary operators
                parse_primary(inner.last().unwrap().clone())
            }
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

fn parse_binary_op(pair: pest::iterators::Pair<Rule>) -> ParseResult<BinaryOp> {
    let op_str = pair.as_str();

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
        _ if op_str.eq_ignore_ascii_case("AND") => BinaryOp::And,
        _ if op_str.eq_ignore_ascii_case("OR") => BinaryOp::Or,
        _ => return Err(ParseError::SemanticError(format!("Unknown operator: {}", op_str))),
    })
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
}
