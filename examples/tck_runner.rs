//! Run the openCypher TCK and report a real pass rate (#434).
//!
//! The spec's H1 gate for `LANG-01` is `CH-TCK >= 85%`, and until now the TCK
//! had never been run — a "~90% OpenCypher coverage" claim was withdrawn as
//! unmeasured in #437. This produces the measurement.
//!
//! `Scenario Outline:` blocks are expanded against their `Examples:` rows
//! (#756). They were skipped until then, which was 274 of the corpus's 1,615
//! scenarios — more than every other skip reason combined, and ~2,280
//! concrete cases once expanded. The pass rate quoted before that expansion,
//! 87.1%, was measured over the 1,244 scenarios that remained.
//!
//! ## What it does and does not attempt
//!
//! The TCK is Gherkin. This implements the step vocabulary that covers the
//! corpus — `Given an empty graph`, `And having executed:`, `When executing
//! query:`, `Then the result should be[, in order]`, `Then the result should be
//! empty`, and the `Then a <Error> should be raised` forms — and reports
//! anything else as **skipped, with the reason**, rather than silently passing
//! it.
//!
//! That distinction is the point. A harness that counts unimplemented steps as
//! passes produces a number that flatters; one that counts them as failures
//! produces a number that misleads in the other direction. Both are reported
//! separately so the headline can be read honestly:
//!
//!   * **pass rate over evaluated scenarios** — what the engine gets right
//!     among the scenarios this harness can actually judge;
//!   * **coverage** — how many of the corpus's scenarios it can judge at all.
//!
//! Both belong in any quote of the result.
//!
//! ## Comparison
//!
//! Expected values use the TCK's own literal syntax — `(:A {name: 'b'})` for a
//! node, `[:R]` for a relationship, `'str'`, numbers, `null`, lists, maps. Both
//! sides are parsed into one shape and rendered canonically before comparison,
//! with labels and map keys sorted, so an ordering difference that Cypher does
//! not specify is not counted as a failure.
//!
//!   cargo run --release --example tck_runner -- --features /path/to/tck/features
//!   cargo run --release --example tck_runner -- --features PATH --json /tmp/tck.json
//!   cargo run --release --example tck_runner -- --features PATH --failures-manifest /tmp/f.tsv
//!   cargo run --release --example tck_runner -- --features PATH --failures-detail /tmp/d.tsv

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

// ---------------------------------------------------------------- values

/// A value in the shape both sides are compared in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Tck {
    Null,
    Bool(bool),
    Int(i64),
    /// Rendered to a fixed precision, so 1.0 and 1.00 compare equal.
    Float(String),
    Str(String),
    List(Vec<Tck>),
    Map(BTreeMap<String, Tck>),
    /// `(:A:B {k: v})` — labels sorted, since Cypher does not order them.
    Node(Vec<String>, BTreeMap<String, Tck>),
    /// `[:TYPE {k: v}]`
    Rel(String, BTreeMap<String, Tck>),
    /// `<(:A)-[:T]->(:B)<-[:T]-(:C)>` — the nodes of a path and, between each
    /// adjacent pair, the relationship and whether it points forwards along
    /// the walk. Direction is the whole point: three Match6 scenarios assert
    /// that a path which exists in one direction does not match in the other,
    /// and a renderer that dropped it would pass them for the wrong reason.
    Path { nodes: Vec<Tck>, rels: Vec<(Tck, bool)> },
    /// Anything this harness does not model; carries the raw text so a
    /// mismatch report is still readable.
    Opaque(String),
}

impl Tck {
    fn render(&self) -> String {
        match self {
            Tck::Null => "null".into(),
            Tck::Bool(b) => b.to_string(),
            Tck::Int(i) => i.to_string(),
            Tck::Float(f) => f.clone(),
            Tck::Str(s) => format!("'{s}'"),
            Tck::List(items) => {
                let inner: Vec<String> = items.iter().map(|i| i.render()).collect();
                format!("[{}]", inner.join(", "))
            }
            Tck::Map(m) => render_props(m),
            Tck::Node(labels, props) => {
                let mut s = String::from("(");
                for l in labels {
                    let _ = write!(s, ":{l}");
                }
                if !props.is_empty() {
                    let _ = write!(s, " {}", render_props(props));
                }
                s.push(')');
                s
            }
            Tck::Rel(t, props) => {
                let mut s = format!("[:{t}");
                if !props.is_empty() {
                    let _ = write!(s, " {}", render_props(props));
                }
                s.push(']');
                s
            }
            Tck::Path { nodes, rels } => {
                let mut out = String::from("<");
                for (i, node) in nodes.iter().enumerate() {
                    if i > 0 {
                        let (rel, forward) = &rels[i - 1];
                        let rel = rel.render();
                        if *forward {
                            let _ = write!(out, "-{rel}->");
                        } else {
                            let _ = write!(out, "<-{rel}-");
                        }
                    }
                    out.push_str(&node.render());
                }
                out.push('>');
                out
            }
            Tck::Opaque(raw) => raw.clone(),
        }
    }

    /// `render`, with the elements of every list sorted.
    ///
    /// Used only for scenarios that say `(ignoring element order for lists)`.
    /// Sorting the *rendered* elements rather than the values is deliberate:
    /// it needs a total order over mixed-type lists, and the rendering
    /// already has one.
    fn render_sorted_lists(&self) -> String {
        match self {
            Tck::List(items) => {
                let mut inner: Vec<String> = items.iter().map(|i| i.render_sorted_lists()).collect();
                inner.sort();
                format!("[{}]", inner.join(", "))
            }
            Tck::Map(m) => {
                let inner: Vec<String> = m
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", v.render_sorted_lists()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
            other => other.render(),
        }
    }
}

fn render_props(m: &BTreeMap<String, Tck>) -> String {
    let inner: Vec<String> = m.iter().map(|(k, v)| format!("{k}: {}", v.render())).collect();
    format!("{{{}}}", inner.join(", "))
}

fn float_repr(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Inf".into() } else { "-Inf".into() };
    }
    // Negative zero renders as `0`.
    //
    // `-0.0 == 0.0` is true, and this function turns a float into a string for
    // a **string** comparison -- so rendering them differently makes the
    // comparator disagree with the equality it is standing in for. `-0.0`
    // arrived as `-0.000000000`, which trims to `-0` rather than to the empty
    // string the guard below was written for, and `RETURN -0.0` was scored
    // wrong against an expected `0.0` (#883).
    //
    // This is a fix to the ruler, not to the engine's score: it is only
    // defensible because the two values are equal, and it applies to the
    // expected side as much as the actual one.
    if f == 0.0 {
        return "0".into();
    }
    // Enough digits to distinguish, few enough that 1.0 and 1.0000000001 do not
    // both appear as distinct-but-equal-looking.
    let s = format!("{f:.9}");
    let s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    if s.is_empty() || s == "-" { "0".into() } else { s }
}

// ------------------------------------------------- parsing a TCK literal

/// Characters, not bytes.
///
/// This indexed `&[u8]` and cast each byte to `char`, which splits every
/// multi-byte character into mojibake -- the reason the TCK's UTF-8 literal
/// scenarios could not be scored correctly for any engine.
struct Cursor {
    s: Vec<char>,
    i: usize,
}

impl Cursor {
    fn new(s: &str) -> Self {
        Self { s: s.chars().collect(), i: 0 }
    }
    fn skip_ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_whitespace() {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<char> {
        self.s.get(self.i).map(|b| *b as char)
    }
    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn rest(&self) -> String {
        self.s[self.i..].iter().collect::<String>()
    }

    fn parse(&mut self) -> Tck {
        self.skip_ws();
        match self.peek() {
            None => Tck::Null,
            Some('\'') => self.parse_quoted('\''),
            Some('"') => self.parse_quoted('"'),
            Some('[') => {
                // Either a relationship `[:T ...]` or a list.
                let save = self.i;
                self.i += 1;
                self.skip_ws();
                if self.peek() == Some(':') {
                    self.i += 1;
                    let t = self.take_ident();
                    let props = self.parse_optional_props();
                    self.eat(']');
                    return Tck::Rel(t, props);
                }
                self.i = save;
                self.parse_list()
            }
            Some('(') => self.parse_node(),
            Some('{') => Tck::Map(self.parse_props()),
            Some('<') => self.parse_path(),
            _ => {
                let word = self.take_while(|c| !matches!(c, ',' | ']' | '}' | ')' | ' '));
                // A parser may decline to understand its input. It may not
                // decline to *advance*.
                //
                // `(` is not a stop character, so `relativedelta(seconds=+13)`
                // is consumed as far as the `)` and the `(` is swallowed into
                // the word untracked. The scan then sits on an orphaned `)`,
                // which is no collection's terminator, and every caller loop
                // asks for another value at the same offset forever --
                // `parse_list` pushing an empty item each time, which is
                // 2.5 GB/s and an OOM-killed machine (#761).
                if word.is_empty() {
                    let c = self.peek();
                    self.i += 1;
                    return Tck::Opaque(c.map(String::from).unwrap_or_default());
                }
                match word.as_str() {
                    "null" => Tck::Null,
                    "true" => Tck::Bool(true),
                    "false" => Tck::Bool(false),
                    other => {
                        if let Ok(i) = other.parse::<i64>() {
                            Tck::Int(i)
                        } else if let Ok(f) = other.parse::<f64>() {
                            Tck::Float(float_repr(f))
                        } else {
                            Tck::Opaque(other.to_string())
                        }
                    }
                }
            }
        }
    }

    fn parse_quoted(&mut self, q: char) -> Tck {
        self.i += 1;
        let mut out = String::new();
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if c == '\\' && self.i + 1 < self.s.len() {
                // Interpreted, not merely un-backslashed. Dropping the
                // backslash turned `'Foo\nFoo'` into `FoonFoo` on the expected
                // side, so a correct engine returning a real newline was
                // scored wrong -- for every engine, including Neo4j, which is
                // how it was found.
                let esc = self.s[self.i + 1];
                self.i += 2;
                match esc {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    '0' => out.push('\0'),
                    'u' => {
                        let hex: String =
                            (0..4).filter_map(|k| self.s.get(self.i + k).copied()).collect();
                        match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                            Some(ch) if hex.len() == 4 => {
                                out.push(ch);
                                self.i += 4;
                            }
                            // Not a valid escape; the TCK has scenarios that
                            // assert exactly that, so keep it literal.
                            _ => out.push('u'),
                        }
                    }
                    other => out.push(other),
                }
                continue;
            }
            self.i += 1;
            if c == q {
                break;
            }
            out.push(c);
        }
        Tck::Str(out)
    }

    fn parse_list(&mut self) -> Tck {
        self.eat('[');
        let mut items = Vec::new();
        loop {
            let before = self.i;
            self.skip_ws();
            if self.eat(']') || self.peek().is_none() {
                break;
            }
            items.push(self.parse());
            self.skip_ws();
            let _ = self.eat(',');
            // Belt and braces over the progress guarantee in `parse`. This
            // parser is a test *oracle*: it is fed whatever four engines
            // choose to render, including things no one has seen yet. An
            // unrecognised rendering must become a wrong answer, never a
            // hang (#761).
            if self.i == before {
                break;
            }
        }
        Tck::List(items)
    }

    /// `<(:A {k: 1})-[:T]->(:B)<-[:T]-(:C)>`, or `<()>` for a zero-length path.
    ///
    /// Parsed into the same shape the engine's paths are converted into, so
    /// the two are compared through one renderer rather than by matching the
    /// feature file's exact spacing.
    fn parse_path(&mut self) -> Tck {
        let start = self.i;
        self.eat('<');
        let mut nodes = Vec::new();
        let mut rels = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() != Some('(') {
                // Not a shape this harness models. Rewind and keep the raw text
                // so the mismatch report still shows what was expected.
                self.i = start;
                return self.consume_opaque_path();
            }
            nodes.push(self.parse_node());
            self.skip_ws();
            match self.peek() {
                Some('>') => {
                    self.i += 1;
                    break;
                }
                Some('<') => {
                    // `<-[:T]-`
                    self.i += 1;
                    self.eat('-');
                    let rel = self.parse_rel();
                    self.eat('-');
                    rels.push((rel, false));
                }
                Some('-') => {
                    // `-[:T]->`
                    self.i += 1;
                    let rel = self.parse_rel();
                    self.eat('-');
                    self.eat('>');
                    rels.push((rel, true));
                }
                _ => {
                    self.i = start;
                    return self.consume_opaque_path();
                }
            }
        }
        Tck::Path { nodes, rels }
    }

    /// The old behaviour: swallow a `<...>` whole and keep it as raw text.
    fn consume_opaque_path(&mut self) -> Tck {
        let start = self.i;
        let mut depth = 0usize;
        while self.i < self.s.len() {
            match self.s[self.i] {
                '<' => depth += 1,
                '>' => {
                    // Saturating rather than `-= 1`. Unreachable today: this
                    // function is only entered sitting on a `<`, so depth is
                    // at least 1 before any `>` is seen. Hardened anyway
                    // because a caller that stopped guaranteeing that would
                    // wrap the counter in release and panic in debug, and the
                    // guarantee is three call sites away. Deliberately without
                    // a test -- there is no input that reaches it, and a test
                    // that cannot fail is worse than none.
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        self.i += 1;
                        break;
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
        Tck::Opaque(self.s[start..self.i].iter().collect::<String>())
    }

    fn parse_rel(&mut self) -> Tck {
        self.eat('[');
        self.skip_ws();
        if self.peek() == Some(':') {
            self.i += 1;
        }
        let t = self.take_ident();
        let props = self.parse_optional_props();
        self.eat(']');
        Tck::Rel(t, props)
    }

    fn parse_node(&mut self) -> Tck {
        self.eat('(');
        let mut labels = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(':') {
                self.i += 1;
                labels.push(self.take_ident());
            } else {
                break;
            }
        }
        let props = self.parse_optional_props();
        self.eat(')');
        labels.sort();
        Tck::Node(labels, props)
    }

    fn parse_optional_props(&mut self) -> BTreeMap<String, Tck> {
        self.skip_ws();
        if self.peek() == Some('{') {
            self.parse_props()
        } else {
            BTreeMap::new()
        }
    }

    fn parse_props(&mut self) -> BTreeMap<String, Tck> {
        let mut m = BTreeMap::new();
        self.eat('{');
        loop {
            let before = self.i;
            self.skip_ws();
            if self.eat('}') || self.peek().is_none() {
                break;
            }
            let key = if self.peek() == Some('\'') || self.peek() == Some('"') {
                match self.parse() {
                    Tck::Str(s) => s,
                    other => other.render(),
                }
            } else {
                self.take_ident()
            };
            self.skip_ws();
            let _ = self.eat(':');
            let val = self.parse();
            m.insert(key, val);
            self.skip_ws();
            let _ = self.eat(',');
            // See `parse_list`. This loop is the one that hung without growing
            // memory -- it re-inserts under the same empty key, so the map
            // stays one entry wide while the process spins (#761).
            if self.i == before {
                break;
            }
        }
        m
    }

    fn take_ident(&mut self) -> String {
        self.skip_ws();
        self.take_while(|c| c.is_alphanumeric() || c == '_' || c == '`')
            .trim_matches('`')
            .to_string()
    }

    fn take_while(&mut self, f: impl Fn(char) -> bool) -> String {
        let start = self.i;
        while self.i < self.s.len() && f(self.s[self.i]) {
            self.i += 1;
        }
        self.s[start..self.i].iter().collect::<String>()
    }
}

/// Split one Gherkin table row into cells, undoing the table's own escaping.
///
/// A cell is not raw text. Gherkin escapes `|` as `\|` inside a cell -- it has
/// to, since `|` is the delimiter -- and therefore escapes the backslash
/// itself as `\\`. Both have to be undone before the cell is read as Cypher.
///
/// Splitting on a bare `|` and handing the cell straight to `parse_quoted`
/// skipped that step, with two consequences. A cell containing `\|` was torn
/// into two cells. And a `\\`, which the table writes for one backslash,
/// survived as two -- so `Literals6[5]`, whose expected value is
/// `a\bcn5t'"\//\"'`, was compared against a string with every backslash
/// doubled and **no engine could pass it**. Neo4j fails it too, which is how
/// this was found: when every engine loses the same scenario, suspect the
/// ruler.
///
/// Only `\|` and `\\` are handled here, deliberately. `\n` and friends are
/// Cypher escapes inside the quoted literal and belong to `parse_quoted`;
/// interpreting them at this layer as well would decode them twice.
fn split_gherkin_row(line: &str) -> Vec<String> {
    let inner = line.trim().trim_matches('|');
    let mut cells = Vec::new();
    let mut cur = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.peek() {
                Some('|') => {
                    cur.push('|');
                    chars.next();
                }
                Some('\\') => {
                    cur.push('\\');
                    chars.next();
                }
                // Left intact for the Cypher layer.
                _ => cur.push('\\'),
            },
            '|' => {
                cells.push(cur.trim().to_string());
                cur = String::new();
            }
            other => cur.push(other),
        }
    }
    cells.push(cur.trim().to_string());
    cells
}


fn parse_expected(text: &str) -> Tck {
    let t = text.trim();
    if t.is_empty() {
        return Tck::Null;
    }
    let mut c = Cursor::new(t);
    let v = c.parse();
    c.skip_ws();
    // Trailing text means the literal was not fully understood; say so rather
    // than compare half of it.
    if c.i < c.s.len() {
        return Tck::Opaque(format!("{}{}", v.render(), c.rest()));
    }
    v
}

// ------------------------------------------------- converting our values

fn prop_to_tck(p: &PropertyValue) -> Tck {
    match p {
        PropertyValue::Null => Tck::Null,
        PropertyValue::Boolean(b) => Tck::Bool(*b),
        PropertyValue::Integer(i) => Tck::Int(*i),
        PropertyValue::Float(f) => Tck::Float(float_repr(*f)),
        PropertyValue::String(s) => Tck::Str(s.clone()),
        PropertyValue::Array(items) => Tck::List(items.iter().map(prop_to_tck).collect()),
        PropertyValue::Vector(v) => {
            Tck::List(v.iter().map(|f| Tck::Float(float_repr(*f as f64))).collect())
        }
        PropertyValue::Map(m) => {
            Tck::Map(m.iter().map(|(k, v)| (k.clone(), prop_to_tck(v))).collect())
        }
        // Temporal values render as the TCK writes them -- a quoted ISO-8601
        // string -- via the engine's own `to_cypher_string`, so the harness and
        // `toString()` cannot disagree (#689).
        //
        // These previously fell to the `Debug` arm below and produced
        // `DateTime(-1882656000000)`, which is not a TCK literal, is not
        // anything, and is what fed #761 an input its value parser could not
        // consume.
        // A Duration is an ISO-8601 string too, and fell to the same `Debug`
        // arm -- `Duration { months: 0, days: 0, ... }` where `'PT6H'` belongs.
        PropertyValue::Duration { .. }
        | PropertyValue::Date(_)
        | PropertyValue::LocalTime(_)
        | PropertyValue::Time { .. }
        | PropertyValue::LocalDateTime { .. }
        | PropertyValue::ZonedDateTime { .. } => Tck::Str(p.to_cypher_string()),
        other => Tck::Opaque(format!("{other:?}")),
    }
}

fn value_to_tck(v: &Value, store: &GraphStore) -> Tck {
    match v {
        Value::Null => Tck::Null,
        Value::Property(p) => prop_to_tck(p),
        Value::NodeRef(id) | Value::Node(id, _) => {
            let mut labels: Vec<String> = store
                .get_node(*id)
                .map(|n| n.labels.iter().map(|l| l.as_str().to_string()).collect())
                .unwrap_or_default();
            labels.sort();
            let props: BTreeMap<String, Tck> = store
                .node_properties_merged(*id)
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), prop_to_tck(v)))
                .collect();
            Tck::Node(labels, props)
        }
        Value::EdgeRef(id, _, _, t) => {
            let props: BTreeMap<String, Tck> = store
                .get_edge(*id)
                .map(|e| {
                    e.properties
                        .iter()
                        .filter(|(_, v)| !v.is_null())
                        .map(|(k, v)| (k.clone(), prop_to_tck(v)))
                        .collect()
                })
                .unwrap_or_default();
            Tck::Rel(t.as_str().to_string(), props)
        }
        Value::Edge(id, e) => {
            let props: BTreeMap<String, Tck> = e
                .properties
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), prop_to_tck(v)))
                .collect();
            let _ = id;
            Tck::Rel(e.edge_type.as_str().to_string(), props)
        }
        Value::List(items) => Tck::List(items.iter().map(|i| value_to_tck(i, store)).collect()),
        Value::Map(entries) => Tck::Map(
            entries.iter().map(|(k, v)| (k.clone(), value_to_tck(v, store))).collect(),
        ),
        Value::Path { nodes, edges } => {
            // An edge is stored with its own source and target, which need not
            // run the same way as the walk: `MATCH (b)<-[r]-(a)` traverses `r`
            // backwards. Comparing the stored endpoints against the node the
            // walk arrived from is what recovers the arrow the TCK prints.
            let rels: Vec<(Tck, bool)> = edges
                .iter()
                .enumerate()
                .map(|(i, eid)| {
                    let edge = store.get_edge(*eid);
                    let props: BTreeMap<String, Tck> = edge
                        .as_ref()
                        .map(|e| {
                            e.properties
                                .iter()
                                .filter(|(_, v)| !v.is_null())
                                .map(|(k, v)| (k.clone(), prop_to_tck(v)))
                                .collect()
                        })
                        .unwrap_or_default();
                    let ty = edge
                        .as_ref()
                        .map(|e| e.edge_type.as_str().to_string())
                        .unwrap_or_default();
                    let forward = match (edge.as_ref(), nodes.get(i)) {
                        (Some(e), Some(from)) => e.source == *from,
                        _ => true,
                    };
                    (Tck::Rel(ty, props), forward)
                })
                .collect();
            let nodes: Vec<Tck> = nodes
                .iter()
                .map(|id| value_to_tck(&Value::NodeRef(*id), store))
                .collect();
            Tck::Path { nodes, rels }
        }
    }
}

// ------------------------------------------------------------- scenarios

#[derive(Debug, Clone)]
enum Expect {
    Rows {
        header: Vec<String>,
        rows: Vec<Vec<String>>,
        ordered: bool,
        /// Set by `the result should be (ignoring element order for lists)`.
        ///
        /// That phrase relaxes the order of elements *inside a list value*,
        /// not the order of rows — `labels(n)` may answer `['L','B']` or
        /// `['B','L']` and both satisfy the scenario. Treating it as a
        /// row-order relaxation only, which is what this runner did, reports
        /// a correct engine as wrong.
        list_order_insensitive: bool,
    },
    Empty,
    Error(String),
}

#[derive(Debug, Clone)]
struct Scenario {
    feature: String,
    name: String,
    setup: Vec<String>,
    query: Option<String>,
    /// `When executing control query` — a *verification* query that runs
    /// **after** the main one, and whose result is what the scenario's final
    /// expectation describes.
    ///
    /// This was being appended to `setup`, so it ran first, returned nothing,
    /// and the main query was then scored against **its** expectation. The
    /// main query in these scenarios is a write whose own result is empty, so
    /// every engine failed all 27 of them — Neo4j included, which is how it
    /// was noticed.
    control_query: Option<String>,
    expect: Option<Expect>,
    /// A step this harness does not implement; the scenario is skipped and this
    /// says which step, so the gap is legible.
    unsupported: Option<String>,
    /// Declared node and relationship deltas from `And the side effects should
    /// be`, as `(+nodes, -nodes, +relationships, -relationships)`.
    ///
    /// Side effects used to be parsed and thrown away. A scenario asserting
    /// *both* an empty result and a non-zero side effect therefore passed on
    /// the empty result alone -- **83 scenarios** in the corpus, each scored
    /// green while the write it exists to test went unchecked. That is how
    /// `DELETE nodes.key` was found deleting nothing (#888).
    ///
    /// Only nodes and relationships. `+properties` and `+labels` count *writes
    /// performed*, not the net change, so a `SET` that overwrites an existing
    /// property is `+properties 1` with a delta of zero; a before/after count
    /// cannot see it, and checking it that way would fail correct engines.
    /// Those two stay unchecked, and say so.
    side_effects: Option<(i64, i64, i64, i64)>,
    /// The `Examples:` blocks of a `Scenario Outline:`, each a header and its
    /// rows. Empty for an ordinary scenario.
    ///
    /// An outline is a template: its steps carry `<placeholder>` tokens and
    /// each example row is one concrete scenario. Skipping them cost 274 of
    /// the TCK's 1,615 scenarios — the single largest reason the harness could
    /// not judge a scenario, and larger than every other skip reason combined.
    /// A block per entry rather than one flat table because an outline may
    /// carry several, with different headers.
    examples: Vec<(Vec<String>, Vec<Vec<String>>)>,
    /// This scenario was declared `Scenario Outline:`.
    ///
    /// A separate flag rather than a preset `unsupported` marker. Marking it
    /// unsupported at the header line -- before its steps are read -- makes
    /// every later step handler see a scenario that is already skipped: the
    /// generic `step:` fallback is guarded on `unsupported.is_none()` and so
    /// records nothing, and expansion then has no way to tell an outline that
    /// is merely an outline from one whose steps it cannot run. That is how 13
    /// `Call5` scenarios went from "skipped: user-defined procedure" to
    /// "errored: Unknown procedure" on the first version of this change.
    is_outline: bool,
}

/// Substitute `<key>` for its example value throughout one scenario.
///
/// Plain textual replacement, because that is what the placeholder is: Gherkin
/// interpolates before the step is read, so `<n>` inside a query, inside an
/// expected cell, and inside an expected error kind are the same substitution.
fn subst(text: &str, header: &[String], row: &[String]) -> String {
    let mut out = text.to_string();
    for (k, v) in header.iter().zip(row) {
        out = out.replace(&format!("<{k}>"), v);
    }
    out
}

/// Expand an outline into one scenario per example row.
///
/// Returns the scenario unchanged when it is not an outline, so the caller does
/// not branch. An outline whose `Examples:` block never arrived keeps its
/// `unsupported` marker and stays a single skipped scenario — silently dropping
/// it would make the coverage number claim work the harness did not do.
fn expand_outline(mut s: Scenario) -> Vec<Scenario> {
    if s.examples.is_empty() {
        if s.is_outline && s.unsupported.is_none() {
            s.unsupported = Some("Scenario Outline without Examples".into());
        }
        return vec![s];
    }
    let mut out = Vec::new();
    for (header, rows) in &s.examples {
        for row in rows {
            if row.len() != header.len() {
                continue;
            }
            let mut c = s.clone();
            c.examples = Vec::new();
            c.is_outline = false;
            c.name = format!("{} [{}]", s.name, row.join(", "));
            c.setup = s.setup.iter().map(|x| subst(x, header, row)).collect();
            c.query = s.query.as_ref().map(|x| subst(x, header, row));
            c.control_query = s.control_query.as_ref().map(|x| subst(x, header, row));
            c.expect = s.expect.as_ref().map(|e| match e {
                Expect::Rows { header: h, rows: r, ordered, list_order_insensitive } => Expect::Rows {
                    header: h.iter().map(|x| subst(x, header, row)).collect(),
                    rows: r
                        .iter()
                        .map(|cells| cells.iter().map(|x| subst(x, header, row)).collect())
                        .collect(),
                    ordered: *ordered,
                    list_order_insensitive: *list_order_insensitive,
                },
                Expect::Empty => Expect::Empty,
                Expect::Error(k) => Expect::Error(subst(k, header, row)),
            });
            out.push(c);
        }
    }
    if out.is_empty() { vec![s] } else { out }
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Pass,
    WrongResult,
    Errored,
    Skipped,
}

fn parse_feature(path: &Path, text: &str) -> Vec<Scenario> {
    let feature = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut cur: Option<Scenario> = None;
    let mut i = 0usize;

    // A `Background:` block's steps belong to *every* scenario in the file.
    // Collected by pointing `cur` at a scratch scenario and moving its setup
    // here when the first real `Scenario:` arrives. Without this the block was
    // dropped on the floor -- the loop below skips any line before the first
    // scenario -- and every scenario in the file ran against an empty graph.
    // One feature uses it (Match5), and 26 of its 29 scenarios were being
    // scored as wrong answers for returning nothing from a graph that had
    // never been built.
    let mut background: Vec<String> = Vec::new();
    let mut in_background = false;

    // The step a docstring or table belongs to.
    #[derive(PartialEq, Clone, Copy)]
    enum Pending { None, Setup, Query, Control, Result(bool), Params, Examples, SideEffects }
    let mut pending = Pending::None;

    while i < lines.len() {
        let raw = lines[i];
        let line = raw.trim();
        i += 1;

        if line.starts_with("Background:") {
            cur = Some(Scenario {
                feature: feature.clone(),
                name: "Background".to_string(),
                setup: Vec::new(),
                query: None,
                control_query: None,
                expect: None,
                unsupported: None,
                side_effects: None,
                examples: Vec::new(),
                is_outline: false,
            });
            in_background = true;
            pending = Pending::None;
            continue;
        }

        if line.starts_with("Examples:") {
            if let Some(s) = cur.as_mut() {
                s.examples.push((Vec::new(), Vec::new()));
            }
            pending = Pending::Examples;
            continue;
        }

        if line.starts_with("Scenario:") || line.starts_with("Scenario Outline:") {
            if let Some(s) = cur.take() {
                if in_background {
                    background = s.setup;
                    in_background = false;
                } else {
                    out.extend(expand_outline(s));
                }
            }
            let mut s = Scenario {
                feature: feature.clone(),
                name: line.trim_start_matches("Scenario Outline:").trim_start_matches("Scenario:").trim().to_string(),
                setup: background.clone(),
                query: None,
                control_query: None,
                expect: None,
                unsupported: None,
                side_effects: None,
                examples: Vec::new(),
                is_outline: line.starts_with("Scenario Outline:"),
            };
            cur = Some(s);
            pending = Pending::None;
            continue;
        }
        let Some(s) = cur.as_mut() else { continue };

        if line.starts_with("Given ") || line.starts_with("And ") || line.starts_with("When ")
            || line.starts_with("Then ") || line.starts_with("But ")
        {
            let body = line.splitn(2, ' ').nth(1).unwrap_or("").trim();
            pending = Pending::None;
            if body.starts_with("an empty graph") || body.starts_with("any graph") {
                // Both are a fresh store here: "any graph" means the scenario
                // does not depend on contents.
            } else if body.starts_with("having executed") {
                pending = Pending::Setup;
            } else if body.starts_with("parameters are") {
                pending = Pending::Params;
            } else if body.starts_with("executing query") {
                pending = Pending::Query;
            } else if body.starts_with("executing control query") {
                pending = Pending::Control;
            } else if body.starts_with("the result should be, in any order")
                || body.starts_with("the result should be (ignoring element order for lists)")
            {
                let list_order_insensitive =
                    body.starts_with("the result should be (ignoring element order for lists)");
                s.expect = Some(Expect::Rows {
                    header: vec![],
                    rows: vec![],
                    ordered: false,
                    list_order_insensitive,
                });
                pending = Pending::Result(false);
            } else if body.starts_with("the result should be, in order") {
                s.expect = Some(Expect::Rows {
                    header: vec![],
                    rows: vec![],
                    ordered: true,
                    list_order_insensitive: false,
                });
                pending = Pending::Result(true);
            } else if body.starts_with("the result should be empty") {
                s.expect = Some(Expect::Empty);
            } else if body.contains("should be raised") {
                let kind = body.split_whitespace().nth(1).unwrap_or("Error").to_string();
                s.expect = Some(Expect::Error(kind));
            } else if body.starts_with("no side effects") {
                s.side_effects = Some((0, 0, 0, 0));
                if s.expect.is_none() {
                    s.unsupported = Some("side-effect assertion only".into());
                }
            } else if body.starts_with("the side effects should be") {
                // The table rows follow; `pending` collects them.
                s.side_effects = Some((0, 0, 0, 0));
                pending = Pending::SideEffects;
                if s.expect.is_none() {
                    s.unsupported = Some("side-effect assertion only".into());
                }
            } else if body.starts_with("there exists a procedure") {
                s.unsupported = Some("user-defined procedure".into());
            } else if body.starts_with("the binary-tree") {
                s.unsupported = Some("named fixture graph".into());
            } else if s.unsupported.is_none() {
                s.unsupported = Some(format!("step: {}", body.chars().take(48).collect::<String>()));
            }
            continue;
        }

        // Docstring
        if line == "\"\"\"" {
            let mut buf = String::new();
            while i < lines.len() && lines[i].trim() != "\"\"\"" {
                buf.push_str(lines[i].trim());
                buf.push('\n');
                i += 1;
            }
            i += 1;
            match pending {
                Pending::Setup => s.setup.push(buf.trim().to_string()),
                Pending::Control => s.control_query = Some(buf.trim().to_string()),
                Pending::Query => s.query = Some(buf.trim().to_string()),
                _ => {}
            }
            pending = Pending::None;
            continue;
        }

        // Table row
        if line.starts_with('|') {
            let cells: Vec<String> = split_gherkin_row(line);
            if pending == Pending::Examples {
                if let Some((header, rows)) = s.examples.last_mut() {
                    if header.is_empty() {
                        *header = cells;
                    } else {
                        rows.push(cells);
                    }
                }
                continue;
            }
            if pending == Pending::SideEffects {
                // Rows look like `| +nodes | 1 |`.
                if cells.len() >= 2 {
                    if let (Some(key), Ok(n)) = (cells.first(), cells[1].parse::<i64>()) {
                        if let Some(se) = s.side_effects.as_mut() {
                            match key.as_str() {
                                "+nodes" => se.0 = n,
                                "-nodes" => se.1 = n,
                                "+relationships" => se.2 = n,
                                "-relationships" => se.3 = n,
                                // `+properties` / `+labels` count writes
                                // performed rather than the net change, so a
                                // before/after delta cannot check them.
                                _ => {}
                            }
                        }
                    }
                }
                continue;
            }
            match (&mut s.expect, pending) {
                (Some(Expect::Rows { header, rows, .. }), Pending::Result(_)) => {
                    if header.is_empty() {
                        *header = cells;
                    } else {
                        rows.push(cells);
                    }
                }
                (_, Pending::Params) => {
                    if s.unsupported.is_none() {
                        s.unsupported = Some("parameters".into());
                    }
                }
                _ => {}
            }
            continue;
        }
    }
    if let Some(s) = cur.take() {
        if !in_background {
            out.extend(expand_outline(s));
        }
    }
    out
}

/// Run a scenario and render its rows the way a competitor driver would.
///
/// Shares `run_scenario`'s setup and control-query handling by construction --
/// it is the same function body up to the point where the rows are produced --
/// so the only difference between this and a competitor is the engine.
fn run_scenario_rows(s: &Scenario, header: &[String]) -> Result<Vec<Vec<String>>, String> {
    let query = s.query.as_ref().ok_or("no query")?;
    let mut store = GraphStore::new();
    for stmt in &s.setup {
        let q = parse_query(stmt).map_err(|_| format!("setup did not parse: {stmt}"))?;
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        m.execute(&q).map_err(|e| format!("setup did not run: {e}"))?;
    }
    let query: &String = if let Some(control) = &s.control_query {
        let q = parse_query(query).map_err(|e| format!("parse: {e}"))?;
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        m.execute(&q).map_err(|e| format!("{e}"))?;
        control
    } else {
        query
    };
    let parsed = parse_query(query).map_err(|e| format!("parse: {e}"))?;
    // Node and relationship counts either side of the query, so a declared
    // side effect can be checked rather than parsed and discarded (#888).
    let before = (store.node_count() as i64, store.edge_count() as i64);
    let batch = {
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        match m.execute(&parsed) {
            Ok(b) => b,
            Err(_) => QueryExecutor::new(&store)
                .execute(&parsed)
                .map_err(|e| format!("exec: {e}"))?,
        }
    };
    if let Some((pn, mn, pr, mr)) = s.side_effects {
        // Only when the query under test is the *main* one. With a control
        // query the write already happened above and this snapshot spans the
        // control query instead, which changes nothing by design.
        if s.control_query.is_none() {
            let after = (store.node_count() as i64, store.edge_count() as i64);
            let (want_nodes, want_rels) = (pn - mn, pr - mr);
            let (got_nodes, got_rels) = (after.0 - before.0, after.1 - before.1);
            if got_nodes != want_nodes || got_rels != want_rels {
                return Err(format!(
                    "side effects: nodes {got_nodes:+} want {want_nodes:+}, \
                     relationships {got_rels:+} want {want_rels:+}"
                ));
            }
        }
    }
    let mut rows = Vec::new();
    for rec in &batch.records {
        rows.push(
            header
                .iter()
                .map(|c| rec.get(c).map(|v| value_to_tck(v, &store)).unwrap_or(Tck::Null).render())
                .collect(),
        );
    }
    Ok(rows)
}

/// Compare a scenario's declared node/relationship deltas against what happened.
///
/// Side effects used to be parsed and thrown away, so a scenario asserting
/// *both* an empty result and a non-zero side effect passed on the empty result
/// alone -- **83 scenarios** in the corpus, each green while the write it exists
/// to test went unchecked. `DELETE nodes.key` was found deleting nothing that
/// way (#888).
///
/// Only nodes and relationships. `+properties` and `+labels` count *writes
/// performed*, not the net change: a `SET` overwriting an existing property is
/// `+properties 1` with a delta of zero, so a before/after count would fail a
/// correct engine. Those stay unchecked.
///
/// Skipped when the scenario has a control query, since the write then happened
/// before this snapshot and the snapshot spans the control query instead.
fn side_effect_mismatch(
    s: &Scenario,
    before: (i64, i64),
    store: &GraphStore,
) -> Option<String> {
    let (pn, mn, pr, mr) = s.side_effects?;
    if s.control_query.is_some() {
        return None;
    }
    let (want_nodes, want_rels) = (pn - mn, pr - mr);
    let got_nodes = store.node_count() as i64 - before.0;
    let got_rels = store.edge_count() as i64 - before.1;
    if got_nodes == want_nodes && got_rels == want_rels {
        return None;
    }
    Some(format!(
        "side effects: nodes {got_nodes:+} want {want_nodes:+}, relationships {got_rels:+} want {want_rels:+}"
    ))
}

fn run_scenario(s: &Scenario) -> (Outcome, String) {
    if let Some(why) = &s.unsupported {
        return (Outcome::Skipped, why.clone());
    }
    let Some(query) = &s.query else {
        return (Outcome::Skipped, "no query".into());
    };
    let Some(expect) = &s.expect else {
        return (Outcome::Skipped, "no result assertion".into());
    };

    let mut store = GraphStore::new();
    for stmt in &s.setup {
        let Ok(q) = parse_query(stmt) else {
            return (
                Outcome::Skipped,
                format!("setup did not parse: {}", stmt.replace('\n', " ")),
            );
        };
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        if m.execute(&q).is_err() {
            return (Outcome::Skipped, "setup did not run".into());
        }
    }

    // A control query runs *after* the main one and is what the scenario's
    // final expectation describes. The main query in these scenarios is a
    // write whose own result is empty, so its rows are deliberately discarded
    // here -- what is being asserted is that the write happened, which only
    // the control query can see.
    let query: &String = if let Some(control) = &s.control_query {
        if let Ok(q) = parse_query(query) {
            let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
            if m.execute(&q).is_err() {
                return (Outcome::Errored, "the query before the control query failed".into());
            }
        } else {
            return (Outcome::Errored, format!("parse: {}", query.replace('\n', " ")));
        }
        control
    } else {
        query
    };

    // A write query has to go through the mutating executor; try that first and
    // fall back, since the harness does not know which it is.
    let parsed = match parse_query(query) {
        Ok(q) => q,
        Err(e) => {
            return match expect {
                Expect::Error(_) => (Outcome::Pass, String::new()),
                _ => (
                    Outcome::Errored,
                    // The pest error lists the grammar rules that *could* have
                    // matched, which does not say what syntax the engine is
                    // missing. The query does.
                    format!("parse: {}", query.replace('\n', " ")),
                ),
            }
        }
    };

    let is_write = {
        let u = query.to_uppercase();
        ["CREATE", "MERGE", "DELETE", "SET", "REMOVE", "FOREACH"]
            .iter()
            .any(|k| u.contains(k))
    };

    // Node and relationship counts either side of the query, so a declared side
    // effect is checked rather than parsed and discarded (#888).
    let before_counts = (store.node_count() as i64, store.edge_count() as i64);

    let batch = if is_write {
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        m.execute(&parsed)
    } else {
        QueryExecutor::new(&store).execute(&parsed)
    };

    let batch = match batch {
        Ok(b) => b,
        Err(e) => {
            return match expect {
                Expect::Error(_) => (Outcome::Pass, String::new()),
                _ => (Outcome::Errored, format!("exec: {}", short(&format!("{e:?}")))),
            }
        }
    };

    match expect {
        Expect::Error(kind) => (
            Outcome::WrongResult,
            format!("expected {kind}, query succeeded with {} rows", batch.records.len()),
        ),
        Expect::Empty => {
            if !batch.records.is_empty() {
                (Outcome::WrongResult, format!("expected empty, got {} rows", batch.records.len()))
            } else if let Some(why) = side_effect_mismatch(s, before_counts, &store) {
                (Outcome::WrongResult, why)
            } else {
                (Outcome::Pass, String::new())
            }
        }
        Expect::Rows { header, rows: _, ordered: _, list_order_insensitive } => {
            if header.is_empty() {
                return (Outcome::Skipped, "no header".into());
            }
            let mut actual: Vec<Vec<String>> = Vec::new();
            for rec in &batch.records {
                let mut row = Vec::new();
                for col in header {
                    let v = rec.get(col).map(|v| value_to_tck(v, &store)).unwrap_or(Tck::Null);
                    row.push(if *list_order_insensitive {
                        v.render_sorted_lists()
                    } else {
                        v.render()
                    });
                }
                actual.push(row);
            }
            compare_rendered_rows(expect, actual)
        }
    }
}

/// Score already-rendered rows against a scenario's expectation.
///
/// Split out so that **every** engine is judged by the same code. A competitor
/// is run by a thin driver that renders each cell in TCK literal syntax; that
/// text is parsed and re-rendered through `Tck` here, exactly as the expected
/// side is. If the comparison lived in the driver instead, a difference
/// between engines could be a difference between two comparators, and the
/// number would say nothing about either engine.
fn compare_rendered_rows(expect: &Expect, mut actual: Vec<Vec<String>>) -> (Outcome, String) {
    match expect {
        Expect::Rows { rows, ordered, list_order_insensitive, .. } => {
            let mut expected: Vec<Vec<String>> = rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|c| {
                            let v = parse_expected(c);
                            if *list_order_insensitive {
                                v.render_sorted_lists()
                            } else {
                                v.render()
                            }
                        })
                        .collect()
                })
                .collect();

            if !*ordered {
                actual.sort();
                expected.sort();
            }
            if actual == expected {
                (Outcome::Pass, String::new())
            } else {
                (
                    Outcome::WrongResult,
                    format!(
                        "expected {} rows {}, got {} rows {}",
                        expected.len(),
                        short(&format!("{expected:?}")),
                        actual.len(),
                        short(&format!("{actual:?}"))
                    ),
                )
            }
        }
        Expect::Empty => {
            if actual.is_empty() {
                (Outcome::Pass, String::new())
            } else {
                (Outcome::WrongResult, format!("expected empty, got {} rows", actual.len()))
            }
        }
        Expect::Error(kind) => (
            Outcome::WrongResult,
            format!("expected {kind}, query succeeded with {} rows", actual.len()),
        ),
    }
}

fn short(s: &str) -> String {
    s.chars().take(110).collect()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().map(|x| x == "feature").unwrap_or(false) {
            out.push(p);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
    };
    let features = arg("--features").unwrap_or_else(|| {
        eprintln!("usage: tck_runner --features <path to tck/features> [--json out.json]");
        std::process::exit(64);
    });

    let mut files = Vec::new();
    walk(Path::new(&features), &mut files);
    files.sort();
    if files.is_empty() {
        eprintln!("no .feature files under {features}");
        std::process::exit(66);
    }

    let mut scenarios = Vec::new();
    for f in &files {
        if let Ok(text) = std::fs::read_to_string(f) {
            scenarios.extend(parse_feature(f, &text));
        }
    }

    // Hand the scenario list to another engine, or score what one sent back.
    //
    // Both exist so a competitor is measured by *this* code: same scenario
    // selection, same comparison. A separate harness per engine would make
    // every difference ambiguous between the engines and the harnesses -- and
    // the harness is the thing under most suspicion here, having been wrong
    // about `Background:` blocks and paths already.
    if let Some(path) = arg("--emit-scenarios") {
        let mut out = Vec::new();
        for s in &scenarios {
            if s.unsupported.is_some() || s.query.is_none() || s.expect.is_none() {
                continue;
            }
            let expect = s.expect.as_ref().unwrap();
            let (kind, header, ordered, loose) = match expect {
                Expect::Rows { header, ordered, list_order_insensitive, .. } =>
                    ("rows", header.clone(), *ordered, *list_order_insensitive),
                Expect::Empty => ("empty", Vec::new(), false, false),
                Expect::Error(_) => ("error", Vec::new(), false, false),
            };
            out.push(serde_json::json!({
                "feature": s.feature,
                "name": s.name,
                "setup": s.setup,
                "query": s.query.as_ref().unwrap(),
                "control_query": s.control_query,
                "expect_kind": kind,
                "header": header,
                "ordered": ordered,
                "list_order_insensitive": loose,
            }));
        }
        let n = out.len();
        std::fs::write(&path, serde_json::to_string_pretty(&out).unwrap()).unwrap();
        println!("wrote {n} evaluable scenarios to {path}");
        return;
    }

    // Samyama's own results, in the shape the competitor drivers emit.
    //
    // Without this our number and theirs sit on slightly different
    // denominators -- the native path skips a few scenarios at run time that
    // the emitted list still contains -- and a comparison across two
    // denominators is not a comparison. It doubles as a check on the judge:
    // scoring these should reproduce the native run.
    if let Some(path) = arg("--emit-actuals") {
        let mut out = Vec::new();
        for sc in &scenarios {
            if sc.unsupported.is_some() || sc.query.is_none() || sc.expect.is_none() {
                continue;
            }
            let mut rec = serde_json::json!({ "feature": sc.feature, "name": sc.name });
            let header: Vec<String> = match sc.expect.as_ref().unwrap() {
                Expect::Rows { header, .. } => header.clone(),
                _ => Vec::new(),
            };
            let produced = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_scenario_rows(sc, &header)
            }))
            .unwrap_or_else(|_| Err("panicked".to_string()));
            match produced {
                Ok(rows) => {
                    rec["status"] = serde_json::json!("ok");
                    rec["rows"] = serde_json::json!(rows);
                }
                Err(e) => {
                    rec["status"] = serde_json::json!("error");
                    rec["error"] = serde_json::json!(e);
                }
            }
            out.push(rec);
        }
        let n = out.len();
        std::fs::write(&path, serde_json::to_string(&out).unwrap()).unwrap();
        println!("wrote {n} actuals to {path}");
        return;
    }

    if let Some(path) = arg("--judge") {
        let engine = arg("--engine").unwrap_or_else(|| "competitor".into());
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("cannot read {path}: {e}");
            std::process::exit(66);
        });
        let actuals: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap_or_else(|e| {
            eprintln!("{path} is not the expected JSON: {e}");
            std::process::exit(65);
        });
        let by_key: BTreeMap<(String, String), &serde_json::Value> = actuals
            .iter()
            .map(|a| {
                (
                    (
                        a["feature"].as_str().unwrap_or("").to_string(),
                        a["name"].as_str().unwrap_or("").to_string(),
                    ),
                    a,
                )
            })
            .collect();

        let (mut pass, mut wrong, mut err, mut missing) = (0usize, 0usize, 0usize, 0usize);
        let mut failures: Vec<(String, String, &'static str, String)> = Vec::new();
        let mut per_feature: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        let mut evaluated = 0usize;
        for s in &scenarios {
            if s.unsupported.is_some() || s.query.is_none() || s.expect.is_none() {
                continue;
            }
            evaluated += 1;
            let entry = per_feature.entry(s.feature.clone()).or_insert((0, 0));
            entry.1 += 1;
            let expect = s.expect.as_ref().unwrap();
            let Some(actual) = by_key.get(&(s.feature.clone(), s.name.clone())) else {
                missing += 1;
                err += 1;
                failures.push((s.feature.clone(), s.name.clone(), "errored", "no result reported".into()));
                continue;
            };
            let status = actual["status"].as_str().unwrap_or("error");
            if status != "ok" {
                let detail = actual["error"].as_str().unwrap_or("error").to_string();
                // A scenario asserting an error is *satisfied* by one.
                if matches!(expect, Expect::Error(_)) {
                    pass += 1;
                    entry.0 += 1;
                } else {
                    err += 1;
                    failures.push((s.feature.clone(), s.name.clone(), "errored", short(&detail)));
                }
                continue;
            }
            if matches!(expect, Expect::Error(_)) {
                wrong += 1;
                failures.push((
                    s.feature.clone(),
                    s.name.clone(),
                    "wrong_result",
                    "expected an error, query succeeded".into(),
                ));
                continue;
            }
            // Each cell arrives as TCK literal text and is put through the same
            // parse-and-render as the expected side.
            let loose = matches!(expect, Expect::Rows { list_order_insensitive: true, .. });
            let rows: Vec<Vec<String>> = actual["rows"]
                .as_array()
                .map(|rs| {
                    rs.iter()
                        .map(|r| {
                            r.as_array()
                                .map(|cs| {
                                    cs.iter()
                                        .map(|c| {
                                            let v = parse_expected(c.as_str().unwrap_or("null"));
                                            if loose { v.render_sorted_lists() } else { v.render() }
                                        })
                                        .collect()
                                })
                                .unwrap_or_default()
                        })
                        .collect()
                })
                .unwrap_or_default();
            match compare_rendered_rows(expect, rows) {
                (Outcome::Pass, _) => { pass += 1; entry.0 += 1; }
                (_, detail) => {
                    wrong += 1;
                    failures.push((s.feature.clone(), s.name.clone(), "wrong_result", detail));
                }
            }
        }
        println!("openCypher TCK — {engine}");
        println!("  evaluated          {evaluated}");
        println!("    pass             {pass}");
        println!("    wrong result     {wrong}");
        println!("    errored          {err}   (of which {missing} reported nothing)");
        let rate = if evaluated > 0 { pass as f64 / evaluated as f64 * 100.0 } else { 0.0 };
        println!("\n  PASS RATE          {rate:.1}%  of evaluated scenarios");
        if let Some(m) = arg("--failures-manifest") {
            let mut lines: Vec<String> = failures
                .iter()
                .map(|(f, n, o, _)| format!("{o}\t{f}\t{n}"))
                .collect();
            lines.sort();
            std::fs::write(m, lines.join("\n")).ok();
        }
        if let Some(d) = arg("--failures-detail") {
            let mut lines: Vec<String> = failures
                .iter()
                .map(|(f, n, o, det)| format!("{o}\t{f}\t{n}\t{det}"))
                .collect();
            lines.sort();
            std::fs::write(d, lines.join("\n")).ok();
        }
        return;
    }

    let (mut pass, mut wrong, mut err, mut skip) = (0usize, 0usize, 0usize, 0usize);
    let mut skip_reasons: BTreeMap<String, usize> = BTreeMap::new();
    // (feature, scenario, outcome, detail). The outcome is carried so a
    // manifest can distinguish a wrong answer from an error — they are
    // different bugs and only one of them is dangerous.
    let mut failures: Vec<(String, String, &'static str, String)> = Vec::new();
    let mut per_feature: BTreeMap<String, (usize, usize)> = BTreeMap::new();

    for s in &scenarios {
        let (outcome, detail) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(s)))
            .unwrap_or((Outcome::Errored, "panicked".into()));
        let entry = per_feature.entry(s.feature.clone()).or_insert((0, 0));
        match outcome {
            Outcome::Pass => { pass += 1; entry.0 += 1; entry.1 += 1; }
            Outcome::WrongResult => { wrong += 1; entry.1 += 1;
                failures.push((s.feature.clone(), s.name.clone(), "wrong_result", detail)); }
            Outcome::Errored => { err += 1; entry.1 += 1;
                failures.push((s.feature.clone(), s.name.clone(), "errored", detail)); }
            Outcome::Skipped => {
                skip += 1;
                // Group on the category, not the whole detail. "setup did not
                // parse" now carries the offending query so it can be fixed,
                // and counting those verbatim would make every row unique and
                // the histogram useless.
                let category = detail.split_once(": ").map_or(detail.as_str(), |(head, _)| head).to_string();
                *skip_reasons.entry(category).or_insert(0) += 1;
            }
        }
    }

    let total = scenarios.len();
    let evaluated = pass + wrong + err;
    let pass_rate = if evaluated == 0 { 0.0 } else { pass as f64 * 100.0 / evaluated as f64 };
    let coverage = if total == 0 { 0.0 } else { evaluated as f64 * 100.0 / total as f64 };

    println!("openCypher TCK");
    println!("  feature files      {}", files.len());
    println!("  scenarios          {total}");
    println!();
    println!("  evaluated          {evaluated}  ({coverage:.1}% of scenarios)");
    println!("    pass             {pass}");
    println!("    wrong result     {wrong}");
    println!("    errored          {err}");
    println!("  skipped            {skip}");
    println!();
    println!("  PASS RATE          {pass_rate:.1}%  of evaluated scenarios");
    println!("  gate CH-TCK >= 85% of evaluated: {}", if pass_rate >= 85.0 { "MET" } else { "NOT MET" });
    println!();
    println!("  The rate is over every scenario this harness can judge, outlines");
    println!("  included. Before outlines were expanded it read 87.1% over 1,244");
    println!("  scenarios; the same engine reads 55.9% over 3,761. Nothing regressed --");
    println!("  the earlier figure was measured on a sample that left out the 274");
    println!("  outlines, which are ~2,280 concrete cases and the harder ones. Quote the");
    println!("  pair, never the rate alone.");
    println!();
    println!("Both numbers matter. The pass rate says what the engine gets right among");
    println!("scenarios this harness can judge; the coverage says how many it can judge at");
    println!("all. Quoting either alone misleads.");

    // Ratchet (#436). CI passes the count the engine is known to reach; a run
    // below it fails the build.
    //
    // The *count* rather than the rate, because the rate has a denominator that
    // can move: a scenario the harness learns to judge shifts a scenario from
    // "skipped" into "evaluated", which changes the percentage without the
    // engine changing at all. The count of passing scenarios only moves when
    // the engine does — provided the scenario set is pinned, which is what
    // `TCK_REF` in the harness is for.
    //
    // The proviso has now been used once. Expanding outlines (#756) changed the
    // scenario set, so the floor moved 1,083 -> 2,103 without the engine
    // changing. That is a **re-baseline, not a gain**: a floor that jumps by a
    // thousand is only honest if the commit that moves it says which of the two
    // it is, so that the next reader does not read it as progress.
    if let Some(min) = arg("--min-pass").and_then(|v| v.parse::<usize>().ok()) {
        println!();
        if pass < min {
            println!(
                "RATCHET FAILED: {pass} scenarios pass, floor is {min}. \
                 {} fewer than the recorded baseline.",
                min - pass
            );
            println!(
                "  A drop here means a change took working behaviour away. If it is \
                 deliberate, lower the floor in the same commit and say why."
            );
            std::process::exit(1);
        }
        println!("RATCHET OK: {pass} pass, floor {min}{}.", if pass > min {
            format!(" (+{} — raise the floor)", pass - min)
        } else {
            String::new()
        });
    }

    println!("\nTop skip reasons:");
    let mut reasons: Vec<_> = skip_reasons.into_iter().collect();
    reasons.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (why, n) in reasons.iter().take(12) {
        println!("  {n:>5}  {why}");
    }

    println!("\nWeakest features (evaluated >= 5):");
    let mut worst: Vec<_> = per_feature
        .iter()
        .filter(|(_, (_, ev))| *ev >= 5)
        .map(|(f, (p, ev))| (f.clone(), *p, *ev, *p as f64 / *ev as f64))
        .collect();
    worst.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap());
    for (f, p, ev, rate) in worst.iter().take(15) {
        println!("  {:>5.0}%  {p:>3}/{ev:<3}  {f}", rate * 100.0);
    }

    println!("\nWhere the failures are (top 10 features by scenarios not passing):");
    let mut deficit: Vec<_> = per_feature
        .iter()
        .map(|(f, (p, ev))| (f.clone(), ev - p, *ev))
        .filter(|(_, miss, _)| *miss > 0)
        .collect();
    deficit.sort_by_key(|(_, miss, _)| std::cmp::Reverse(*miss));
    let total_missed: usize = deficit.iter().map(|(_, m, _)| m).sum();
    for (f, miss, ev) in deficit.iter().take(10) {
        println!(
            "  {miss:>5} of {ev:<5} ({:>4.1}% of all failures)  {f}",
            *miss as f64 * 100.0 / total_missed as f64
        );
    }
    println!(
        "  {:>5} across {} features in total",
        total_missed,
        deficit.len()
    );

    if let Some(path) = arg("--json") {
        let mut j = String::from("{\n");
        let _ = writeln!(j, "  \"scenarios\": {total},");
        let _ = writeln!(j, "  \"evaluated\": {evaluated},");
        let _ = writeln!(j, "  \"pass\": {pass},");
        let _ = writeln!(j, "  \"wrong_result\": {wrong},");
        let _ = writeln!(j, "  \"errored\": {err},");
        let _ = writeln!(j, "  \"skipped\": {skip},");
        let _ = writeln!(j, "  \"pass_rate_of_evaluated\": {pass_rate:.2},");
        let _ = writeln!(j, "  \"coverage_of_scenarios\": {coverage:.2}");
        j.push_str("}\n");
        let _ = std::fs::write(&path, j);
        println!("\nwrote {path}");
    }

    // A sorted, one-line-per-scenario manifest of everything that did not
    // pass. Two of these can be diffed, which is how the pass count was shown
    // to vary by up to 3 scenarios between processes at a fixed commit: the
    // totals moved while `errored` did not, so the drift was between pass and
    // wrong-answer and no summary number could localise it.
    if let Some(path) = arg("--failures-manifest") {
        let mut lines: Vec<String> = failures
            .iter()
            .map(|(f, n, o, _)| format!("{o}\t{f}\t{n}"))
            .collect();
        lines.sort();
        let _ = std::fs::write(&path, lines.join("\n") + "\n");
        println!("wrote failure manifest ({} scenarios): {path}", lines.len());
    }

    // Full detail, one scenario per line, for grouping failures by cause.
    // Kept separate from `--failures-manifest` on purpose: the detail text
    // embeds row dumps, which differ between runs for unordered results, and
    // CH-DETERM compares manifests as sets. Mixing them would make the
    // determinism suite flag its own diagnostics as nondeterminism.
    if let Some(path) = arg("--failures-detail") {
        let mut lines: Vec<String> = failures
            .iter()
            .map(|(f, n, o, d)| {
                let one_line = d.replace('\n', " ");
                format!("{o}\t{f}\t{n}\t{one_line}")
            })
            .collect();
        lines.sort();
        let _ = std::fs::write(&path, lines.join("\n") + "\n");
        println!("wrote failure detail ({} scenarios): {path}", lines.len());
    }

    if arg("--show-failures").is_some() {
        println!("\nFailures:");
        for (f, n, o, d) in failures.iter().take(60) {
            println!("  [{f}] {n} ({o})\n      {d}");
        }
        if failures.len() > 60 {
            println!("  ... and {} more; use --failures-manifest for the full list",
                     failures.len() - 60);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The parser must terminate on input no one anticipated.
    ///
    /// Both strings below are real engine output, recorded during the
    /// cross-engine run. Both are perfectly *balanced* -- which is why a
    /// bracket-matching check finds nothing wrong with them. The imbalance is
    /// in the scanner: `(` is not a stop character for a bare word, so
    /// `relativedelta(seconds=+13` is swallowed whole and the parser is left
    /// sitting on an orphaned `)` that terminates no collection.
    ///
    /// Before the fix these did not fail, they **hung**: `parse_list` pushed an
    /// empty item per iteration at ~2.5 GB/s until systemd-oomd killed the
    /// machine, and `parse_props` spun at 87% CPU for 27 minutes without
    /// growing, because it re-inserts under the same empty key (#761).
    ///
    /// A `#[timeout]` attribute would be the natural way to pin this; Rust's
    /// test harness has none, so these assert on the *value* and rely on the
    /// suite's own wall-clock to catch a regression. That is weaker than I
    /// would like and is the honest state of it.
    #[test]
    fn an_unmatched_paren_does_not_hang_the_parser() {
        // FalkorDB, Temporal4 [12] — a Python repr leaking through the driver.
        let v = parse_expected("[relativedelta(seconds=+13)]");
        match v {
            Tck::List(items) => assert!(
                items.len() < 8,
                "a two-token list must not expand without bound, got {} items",
                items.len()
            ),
            other => panic!("expected a list, got {other:?}"),
        }

        // Samyama, WithOrderBy1 [33] — our own Debug output where a TCK
        // temporal literal belongs (#689).
        let v = parse_expected("(:A {date: DateTime(-1882656000000)})");
        match v {
            Tck::Node(labels, props) => {
                assert_eq!(labels, vec!["A".to_string()]);
                assert!(props.len() < 8, "got {} props", props.len());
            }
            other => panic!("expected a node, got {other:?}"),
        }
    }

    /// Every bare `parse` advances, whatever it is handed.
    ///
    /// This is the invariant the two hangs violated, stated directly rather
    /// than through a caller. A character the scanner has no rule for is
    /// consumed and reported as `Opaque` — declining to understand is fine,
    /// declining to move is not.
    #[test]
    fn parse_always_consumes_at_least_one_character() {
        for input in [")", "}", "]", ",", ")x", "}}}", "%", "\u{1f600}"] {
            let mut p = Cursor::new(input);
            let before = p.i;
            let _ = p.parse();
            assert!(
                p.i > before,
                "`{input}` left the cursor at {before}; every caller loop then \
                 asks again at the same offset, forever"
            );
        }
    }


    /// An outline is a template; each `Examples:` row is one concrete scenario.
    ///
    /// Skipping them was the harness's single largest blind spot: 274 of the
    /// TCK's 1,615 scenarios, more than every other skip reason combined, and
    /// they expand to ~2,280 concrete cases the engine had never been judged
    /// on. Coverage went 77.0% -> 96.5% when they started running, and the
    /// pass rate went 87.1% -> 55.9%, because the 87.1% was measured over a
    /// sample that excluded them.
    #[test]
    fn an_outline_expands_to_one_scenario_per_example_row() {
        let text = "\
Feature: Demo

  Scenario Outline: [1] adds
    Given an empty graph
    When executing query:
      \"\"\"
      RETURN <a> + <b> AS r
      \"\"\"
    Then the result should be, in any order:
      | r |
      | <sum> |

    Examples:
      | a | b | sum |
      | 1 | 2 | 3   |
      | 4 | 5 | 9   |
";
        let out = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(out.len(), 2, "two example rows, two scenarios");
        assert_eq!(out[0].query.as_deref(), Some("RETURN 1 + 2 AS r"));
        assert_eq!(out[1].query.as_deref(), Some("RETURN 4 + 5 AS r"));
        assert!(out.iter().all(|s| s.unsupported.is_none()), "expanded rows must run");

        // The placeholder is substituted in the *expectation* too. A harness
        // that interpolated only the query would score every row against the
        // literal text `<sum>` and report a correct engine as wrong.
        for (s, want) in out.iter().zip(["3", "9"]) {
            match s.expect.as_ref().expect("expectation") {
                Expect::Rows { rows, .. } => assert_eq!(rows, &vec![vec![want.to_string()]]),
                other => panic!("expected rows, got {other:?}"),
            }
        }
    }

    /// The `Examples:` table must not be read as more expected rows.
    ///
    /// The two tables are adjacent and identical in syntax; the only thing
    /// separating them is the `Examples:` keyword resetting what the parser is
    /// collecting. Without that reset the example rows append to the
    /// expectation, and a scenario that should assert one row asserts three.
    #[test]
    fn example_rows_do_not_land_in_the_expected_table() {
        let text = "\
Feature: Demo

  Scenario Outline: [1] one row only
    Given an empty graph
    When executing query:
      \"\"\"
      RETURN <a> AS r
      \"\"\"
    Then the result should be, in any order:
      | r   |
      | <a> |

    Examples:
      | a |
      | 7 |
";
        let out = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(out.len(), 1);
        match out[0].expect.as_ref().expect("expectation") {
            Expect::Rows { rows, header, .. } => {
                assert_eq!(header, &vec!["r".to_string()]);
                assert_eq!(rows, &vec![vec!["7".to_string()]], "one row, not the examples too");
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// An expanded row keeps a step the harness cannot run.
    ///
    /// This is the regression the first version of the change shipped: it
    /// cleared `unsupported` on every clone, so 13 `Call5` scenarios stopped
    /// being "skipped: user-defined procedure" and became "errored: Unknown
    /// procedure". Expansion changed a *reported gap* into a *reported defect*
    /// — the harness accusing the engine of the harness's own limitation.
    #[test]
    fn expansion_does_not_erase_a_reason_the_scenario_cannot_run() {
        let text = "\
Feature: Demo

  Scenario Outline: [1] calls
    Given an empty graph
    And there exists a procedure test.my.proc() :: (a :: INTEGER?)
    When executing query:
      \"\"\"
      CALL test.my.proc() YIELD <y> RETURN <y>
      \"\"\"
    Then the result should be, in any order:
      | <y> |
      | 1   |

    Examples:
      | y |
      | a |
";
        let out = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].unsupported.as_deref(), Some("user-defined procedure"));
    }

    /// An outline whose `Examples:` block never arrived stays one skipped
    /// scenario, and says so. Dropping it would shrink the denominator and
    /// raise the coverage figure for work the harness did not do.
    #[test]
    fn an_outline_without_examples_is_skipped_not_dropped() {
        let text = "\
Feature: Demo

  Scenario Outline: [1] no examples
    Given an empty graph
    When executing query:
      \"\"\"
      RETURN <a>
      \"\"\"
    Then the result should be empty
";
        let out = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].unsupported.as_deref(), Some("Scenario Outline without Examples"));
    }

    /// A single outline may carry several `Examples:` blocks, with different
    /// headers. Flattening them into one table would misalign every row of the
    /// second against the first block's header.
    #[test]
    fn several_examples_blocks_each_expand_against_their_own_header() {
        let text = "\
Feature: Demo

  Scenario Outline: [1] two blocks
    Given an empty graph
    When executing query:
      \"\"\"
      RETURN <x>
      \"\"\"
    Then the result should be empty

    Examples:
      | x |
      | 1 |

    Examples:
      | x |
      | 2 |
      | 3 |
";
        let out = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(out.len(), 3);
        let qs: Vec<_> = out.iter().filter_map(|s| s.query.clone()).collect();
        assert_eq!(qs, vec!["RETURN 1", "RETURN 2", "RETURN 3"]);
    }

    /// A `Background:` block applies to every scenario in its file.
    ///
    /// This harness dropped it: the parse loop skips any line appearing before
    /// the first `Scenario:`, so the block was read and discarded. One feature
    /// in the TCK uses it — Match5, 29 scenarios — and all of them ran against
    /// an empty graph. They did not error; they returned no rows and were
    /// scored as **wrong answers**, so the engine was charged with 26 defects
    /// it did not have and the published pass rate was two points low.
    ///
    /// A harness that silently tests nothing looks exactly like an engine that
    /// silently answers nothing.
    #[test]
    fn a_background_block_reaches_every_scenario_in_the_file() {
        let text = "\
Feature: Demo

  Background:
    Given an empty graph
    And having executed:
      \"\"\"
      CREATE (:A {name: 'a'})
      \"\"\"

  Scenario: [1] first
    When executing query:
      \"\"\"
      MATCH (n:A) RETURN n.name
      \"\"\"
    Then the result should be, in any order:
      | n.name |
      | 'a'    |

  Scenario: [2] second
    And having executed:
      \"\"\"
      CREATE (:B)
      \"\"\"
    When executing query:
      \"\"\"
      MATCH (n) RETURN n
      \"\"\"
    Then the result should be, in any order:
      | n |
";
        let scenarios = parse_feature(Path::new("Demo.feature"), text);

        assert_eq!(scenarios.len(), 2, "the Background must not become a scenario");
        assert_eq!(scenarios[0].name, "[1] first");
        assert_eq!(
            scenarios[0].setup,
            vec!["CREATE (:A {name: 'a'})".to_string()],
            "a scenario with no setup of its own still gets the Background's"
        );
        assert_eq!(
            scenarios[1].setup,
            vec!["CREATE (:A {name: 'a'})".to_string(), "CREATE (:B)".to_string()],
            "a scenario's own setup runs after the Background's, not instead of it"
        );
    }

    /// The expected side and the engine side must meet in one renderer.
    ///
    /// Paths used to be compared as raw text on the expected side and rendered
    /// as `<path 2 1>` on the engine side, so **every** path scenario was a
    /// mismatch — 14 of Match6's 19 failures were the harness declining to look
    /// at the value it had been given.
    #[test]
    fn a_path_round_trips_through_the_renderer() {
        for text in [
            "<(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})>",
            "<(:B)<-[:T]-(:A)>",
            "<(:C)-[:T]->(:B)-[:T]->(:A)>",
            "<(:Label1)<-[:T1]-(:Label2)-[:T2]->(:Label3)>",
            "<()>",
            "<( {id: 0})-[:R {num: 1}]->( {id: 1})>",
        ] {
            let parsed = parse_expected(text);
            assert!(
                matches!(parsed, Tck::Path { .. }),
                "`{text}` should parse as a path, got {parsed:?}"
            );
            assert_eq!(parsed.render(), text, "rendering must reproduce the input");
        }
    }

    /// Direction is not decoration. Three Match6 scenarios assert that a path
    /// existing one way does not match the other, and they would pass for the
    /// wrong reason against a renderer that printed every arrow forwards.
    #[test]
    fn a_reversed_path_does_not_render_the_same_as_a_forward_one() {
        let forward = parse_expected("<(:A)-[:T]->(:B)>");
        let backward = parse_expected("<(:A)<-[:T]-(:B)>");
        assert_ne!(forward.render(), backward.render());
    }

    /// The ordinary case: no `Background:`, no setup invented for anyone.
    #[test]
    fn a_file_without_a_background_is_unaffected() {
        let text = "\
Feature: Demo

  Scenario: [1] only
    Given any graph
    When executing query:
      \"\"\"
      RETURN 1 AS n
      \"\"\"
    Then the result should be, in any order:
      | n |
      | 1 |
";
        let scenarios = parse_feature(Path::new("Demo.feature"), text);
        assert_eq!(scenarios.len(), 1);
        assert!(scenarios[0].setup.is_empty());
    }
}
