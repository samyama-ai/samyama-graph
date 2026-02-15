# ADR-013: PEG Grammar with Atomic Keyword Rules

## Status
**Accepted**

## Date
2025-12-20

## Context

Samyama uses the [Pest](https://pest.rs/) PEG (Parsing Expression Grammar) parser for OpenCypher query parsing. The grammar is defined in `src/query/cypher.pest` and covers keywords like `MATCH`, `WHERE`, `RETURN`, `AND`, `OR`, `ORDER BY`, `NOT`, `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, and others.

During development, we encountered critical parsing bugs caused by how Pest handles whitespace in non-atomic rules:

### Problem 1: Keyword Prefix Matching

In PEG, `^"OR"` matches the literal string "OR" (case-insensitive) but has **no word boundary check**. This means it also matches the "OR" prefix of "ORDER":

```cypher
-- Intended: ORDER BY n.name
-- Parsed as: OR (keyword) + DER (identifier) + BY (identifier) + n.name
MATCH (n:Person) RETURN n ORDER BY n.name
```

### Problem 2: Implicit Whitespace Consumption

Non-atomic rules in Pest (`rule = { ... }`) insert implicit `WHITESPACE` matches between every element. This caused a subtle bug with word boundary lookaheads:

```pest
// Non-atomic rule - BROKEN
and_op = { ^"AND" ~ !(ALPHA) }
// What actually happens:
// 1. Match "AND"
// 2. Implicit WHITESPACE consumed (eats the space)
// 3. !(ALPHA) checks next char after the space -- sees identifier char, FAILS
```

For the query `WHERE n.age > 30 AND n.name = "Alice"`, the parser would:
1. Match "AND"
2. Consume the space (implicit WHITESPACE)
3. Check `!(ALPHA)` against "n" (from `n.name`) -- fail
4. Reject "AND" as a keyword

### Problem 3: Operator Ordering Ambiguity

PEG tries alternatives left-to-right with no backtracking once a match succeeds. This caused issues with operators that share prefixes:

```pest
// BROKEN: < matches before <> gets a chance
comparison_op = { "<" | "<>" | "<=" | ">" | ">=" | "=" }
```

## Decision

**We will use atomic rules for keyword operators and enforce strict PEG ordering for ambiguous alternatives.**

### Atomic Keyword Rules

All keyword operators use atomic rules (`@{ }`) which prevent implicit WHITESPACE insertion:

```pest
// Atomic rule - CORRECT
and_op = @{ ^"AND" ~ !(ASCII_ALPHANUMERIC | "_") }
or_op  = @{ ^"OR"  ~ !(ASCII_ALPHANUMERIC | "_") }
not_op = @{ ^"NOT" ~ !(ASCII_ALPHANUMERIC | "_") }
in_op  = @{ ^"IN"  ~ !(ASCII_ALPHANUMERIC | "_") }
```

With atomic rules:
1. Match "AND"
2. **No implicit WHITESPACE** -- go directly to lookahead
3. `!(ASCII_ALPHANUMERIC | "_")` checks the very next character
4. If next char is a space or end-of-input, the keyword matches
5. If next char is alphanumeric (like "ORDER" after "OR"), the keyword does NOT match

### Reserved Word Protection

The `variable` and `function_name` rules use a `!reserved` negative lookahead to prevent keywords from being parsed as identifiers:

```pest
reserved = @{
    (^"MATCH" | ^"WHERE" | ^"RETURN" | ^"CREATE" | ^"DELETE" | ^"SET"
     | ^"REMOVE" | ^"ORDER" | ^"LIMIT" | ^"SKIP" | ^"AND" | ^"OR"
     | ^"NOT" | ^"IN" | ^"AS" | ^"BY" | ^"TRUE" | ^"FALSE" | ^"NULL"
     | ^"IS" | ^"CONTAINS" | ^"STARTS" | ^"ENDS" | ^"WITH"
     | ^"OPTIONAL" | ^"DETACH" | ^"DESC" | ^"ASC" | ^"DISTINCT"
     | ^"EXISTS" | ^"CASE" | ^"WHEN" | ^"THEN" | ^"ELSE" | ^"END"
     | ^"UNION" | ^"ALL" | ^"UNWIND" | ^"EXPLAIN" | ^"PROFILE")
    ~ !(ASCII_ALPHANUMERIC | "_")
}

variable = @{ !reserved ~ (ASCII_ALPHA | "_") ~ (ASCII_ALPHANUMERIC | "_")* }
```

### PEG Ordering Rules

Alternatives are ordered longest-match-first to prevent prefix ambiguity:

```pest
// CORRECT: Longer operators first
comparison_op = { "<>" | "<=" | ">=" | "<" | ">" | "=" }

// CORRECT: Literal values before variables (TRUE/FALSE/NULL parsed as values, not identifiers)
primary = { function_call | literal | parameter | parenthesized | variable }
```

Key ordering principles:
- In `comparison_op`: `<>` must come **before** `<`, and `<=` before `<`
- In `primary`: `literal` (which includes `TRUE`/`FALSE`/`NULL`) must come **before** `variable`
- In `boolean_expr`: `and_op` alternatives are tried before falling through to `comparison_expr`

## Consequences

### Positive

- Keywords correctly disambiguated from identifiers in all cases
- `ORDER BY` no longer parsed as `OR` + `DER BY`
- `AND`/`OR`/`NOT`/`IN` work correctly adjacent to identifiers
- `TRUE`, `FALSE`, `NULL` always parse as literal values, never as variable names
- Grammar is self-documenting -- atomic rules make word boundary handling explicit

### Negative

- Atomic rules require explicit whitespace handling in some compound keywords (e.g., `STARTS WITH` needs explicit `~ WHITESPACE+` between words)
- Adding new keywords requires updating the `reserved` rule
- PEG ordering rules are a subtle correctness requirement that must be documented and maintained

### Neutral

- No runtime performance impact -- PEG parsing is linear time regardless of atomic vs non-atomic rules
- Pest grammar file grows slightly larger due to explicit word boundary patterns

## Alternatives Considered

### Alternative 1: Lexer-Based Tokenization

Use a separate lexer pass to convert keywords to tokens before parsing.

**Rejected because**:
- Breaks PEG simplicity -- Pest is designed as a single-pass parser
- Requires a separate tokenization stage and token type definitions
- Adds complexity without significant benefit for our grammar size

### Alternative 2: Post-Parse Keyword Validation

Parse permissively and validate keyword usage in a semantic pass after parsing.

**Rejected because**:
- Error messages would point to wrong locations (semantic pass vs parse failure)
- Ambiguous parses could propagate silently and cause incorrect query plans
- Harder to reason about correctness than fixing it at the grammar level

### Alternative 3: Use a Different Parser Generator

Switch to a parser that handles keyword boundaries natively (e.g., LALR parser like lalrpop).

**Rejected because**:
- Pest is well-integrated and performant for our needs
- Migration cost would be significant (~2,000 lines of grammar + tests)
- The atomic rule solution fully addresses the issue within Pest

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) - Query execution depends on correct parsing
- [ADR-011: Cypher CRUD Operations](./ADR-011-cypher-crud-operations.md) - CRUD keywords (DELETE, SET, REMOVE) follow the same atomic keyword pattern

## References

- [Pest Book: Atomic Rules](https://pest.rs/book/grammars/syntax.html#atomic)
- [PEG Parsing: Ordered Choice](https://en.wikipedia.org/wiki/Parsing_expression_grammar#Ordered_choice)
- [OpenCypher Grammar Specification](https://opencypher.org/resources/)

---

**Last Updated**: 2025-12-20
**Status**: Accepted and Implemented
