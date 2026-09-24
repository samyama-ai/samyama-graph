//! The reference sketch's frame is one the server actually accepts (API-15).
//!
//! API-15 asks for a documented pattern and a reference sketch for
//! microcontroller-class clients. A sketch nobody can run is easy to get
//! subtly wrong -- an off-by-one in a bulk-string length, a bare LF, the
//! argument count disagreeing with the arguments -- and every one of those
//! fails at the device, which is the most expensive place to find out.
//!
//! So this reads `examples/esp32/samyama_esp32.ino`, extracts the format
//! string the sketch sends, rebuilds the frame exactly as `snprintf` would,
//! and pushes the bytes through the server's own RESP decoder and then its
//! query engine.
//!
//! **Read from the file on purpose.** A copy of the format string in this test
//! would verify the copy. Reading the sketch means editing the sketch without
//! editing the protocol fails here rather than in the field.
//!
//! What this does not do: run on an ESP32. It says the bytes are right, not
//! that the board fits, links, or has the RAM. API-15's H2 asks for a real
//! device and that remains unmeasured.

use bytes::BytesMut;
use samyama::graph::GraphStore;
use samyama::protocol::resp::RespValue;
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

const SKETCH: &str = include_str!("../examples/esp32/samyama_esp32.ino");

/// The `SAMYAMA_RESP_FMT` macro body from the sketch, with C string-literal
/// concatenation and escapes resolved into the bytes `snprintf` would emit.
fn format_string_from_sketch() -> String {
    let start = SKETCH
        .find("#define SAMYAMA_RESP_FMT")
        .expect("sketch no longer defines SAMYAMA_RESP_FMT");
    // The macro continues while lines end in a backslash.
    let mut body = String::new();
    for line in SKETCH[start..].lines().skip(1) {
        body.push_str(line.trim_end().trim_end_matches('\\').trim());
        if !line.trim_end().ends_with('\\') {
            break;
        }
    }
    // Join adjacent C string literals and resolve the escapes we use.
    let mut out = String::new();
    let mut in_str = false;
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => in_str = !in_str,
            '\\' if in_str => match chars.next() {
                Some('r') => out.push('\r'),
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => {}
            },
            _ if in_str => out.push(c),
            _ => {}
        }
    }
    assert!(!out.is_empty(), "extracted an empty format string");
    out
}

/// What the sketch's `snprintf(out, cap, FMT, strlen(g), g, strlen(c), c)` writes.
fn build_frame(graph: &str, cypher: &str) -> Vec<u8> {
    let fmt = format_string_from_sketch();
    // Substitute the four conversions in order: %u, %s, %u, %s.
    let mut out = String::new();
    let mut rest = fmt.as_str();
    let subs = [
        graph.len().to_string(),
        graph.to_string(),
        cypher.len().to_string(),
        cypher.to_string(),
    ];
    let mut i = 0;
    while let Some(pos) = rest.find('%') {
        out.push_str(&rest[..pos]);
        let spec = &rest[pos..];
        let consumed = if spec.starts_with("%u") || spec.starts_with("%s") {
            out.push_str(&subs[i]);
            i += 1;
            2
        } else {
            out.push('%');
            1
        };
        rest = &spec[consumed..];
    }
    out.push_str(rest);
    assert_eq!(i, 4, "format string no longer takes exactly four arguments");
    out.into_bytes()
}

#[test]
fn the_sketch_frame_decodes_to_a_graph_query() {
    let cypher = "CREATE (:Reading {sensor:'esp32-01', celsius:21.50})";
    let mut buf = BytesMut::from(&build_frame("default", cypher)[..]);

    let decoded = RespValue::decode(&mut buf)
        .expect("the server's decoder rejected the sketch's frame")
        .expect("the frame was incomplete");

    let args = match decoded {
        RespValue::Array(a) => a,
        other => panic!("expected an array of bulk strings, got {other:?}"),
    };
    assert_eq!(args.len(), 3, "GRAPH.QUERY takes a graph and a statement");

    let text = |v: &RespValue| match v {
        RespValue::BulkString(Some(b)) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("expected a bulk string, got {other:?}"),
    };
    assert_eq!(text(&args[0]), "GRAPH.QUERY");
    assert_eq!(text(&args[1]), "default");
    assert_eq!(text(&args[2]), cypher);

    // Nothing may be left over. A length that overstates by one would leave a
    // trailing byte here and desynchronise the next command on a real socket.
    assert!(buf.is_empty(), "{} bytes left after the frame", buf.len());
}

#[test]
fn the_statement_the_sketch_sends_runs() {
    // Decoding proves the framing. It says nothing about whether the Cypher is
    // valid, and a sketch that frames an unparseable statement perfectly is
    // still a broken reference.
    let cypher = "CREATE (:Reading {sensor:'esp32-01', celsius:21.50})";
    let write = parse_query(cypher).expect("the sketch's statement does not parse");

    let mut store = GraphStore::new();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&write)
        .expect("the sketch's statement did not execute");

    let read = parse_query("MATCH (r:Reading) RETURN r.sensor AS s").expect("read parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&read)
        .expect("read back failed");
    assert_eq!(rows.records.len(), 1, "the write did not land");
}

#[test]
fn a_wrong_bulk_length_would_be_caught() {
    // The guard above is only worth having if it can fail. Overstate the
    // statement's length by one byte and the decoder must not hand back a
    // clean, fully-consumed frame.
    let cypher = "RETURN 1";
    let frame = String::from_utf8(build_frame("default", cypher)).unwrap();
    let broken = frame.replacen(
        &format!("${}\r\n{}", cypher.len(), cypher),
        &format!("${}\r\n{}", cypher.len() + 1, cypher),
        1,
    );
    let mut buf = BytesMut::from(broken.as_bytes());
    let decoded = RespValue::decode(&mut buf);
    let clean = matches!(decoded, Ok(Some(_))) && buf.is_empty();
    assert!(!clean, "a one-byte length error decoded as a complete frame");
}
