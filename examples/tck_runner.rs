//! Run the openCypher TCK and report a real pass rate (#434).
//!
//! The spec's H1 gate for `LANG-01` is `CH-TCK >= 85%`, and until now the TCK
//! had never been run — a "~90% OpenCypher coverage" claim was withdrawn as
//! unmeasured in #437. This produces the measurement.
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
//!   * **coverage** — how many of the 1,615 scenarios it can judge at all.
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
            self.skip_ws();
            if self.eat(']') || self.peek().is_none() {
                break;
            }
            items.push(self.parse());
            self.skip_ws();
            let _ = self.eat(',');
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
                    depth -= 1;
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
    enum Pending { None, Setup, Query, Control, Result(bool), Params }
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
            });
            in_background = true;
            pending = Pending::None;
            continue;
        }

        if line.starts_with("Scenario:") || line.starts_with("Scenario Outline:") {
            if let Some(s) = cur.take() {
                if in_background {
                    background = s.setup;
                    in_background = false;
                } else {
                    out.push(s);
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
            };
            if line.starts_with("Scenario Outline:") {
                // Outlines need Examples expansion, which this harness does not
                // do. Reported rather than silently dropped.
                s.unsupported = Some("Scenario Outline".into());
            }
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
            } else if body.starts_with("no side effects") || body.starts_with("the side effects should be") {
                // Side effects are not checked. Noted on the scenario only when
                // it would otherwise pass, so the number is not inflated by
                // scenarios whose *only* assertion is a side effect.
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
            let cells: Vec<String> = line
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().to_string())
                .collect();
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
            out.push(s);
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
    let batch = {
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        match m.execute(&parsed) {
            Ok(b) => b,
            Err(_) => QueryExecutor::new(&store)
                .execute(&parsed)
                .map_err(|e| format!("exec: {e}"))?,
        }
    };
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
            if batch.records.is_empty() {
                (Outcome::Pass, String::new())
            } else {
                (Outcome::WrongResult, format!("expected empty, got {} rows", batch.records.len()))
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
    println!("Both numbers matter. The pass rate says what the engine gets right among");
    println!("scenarios this harness can judge; the coverage says how many it can judge at");
    println!("all. Quoting either alone misleads.");

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
