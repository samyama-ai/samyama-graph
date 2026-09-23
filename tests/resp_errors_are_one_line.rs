//! A RESP error must not put the client out of step with its own replies.
//!
//! `+` and `-` are line types: CRLF terminates them and may not appear inside.
//! Our parse errors are multi-line — `pest` draws a caret under the offending
//! token — and they went into a simple error verbatim. A client scanning for
//! CRLF survived, so the defect was invisible from a test that reads one buffer
//! and checks the text. A client scanning for LF (the common case) read the
//! first line as the error and then returned `  |` as the reply to the *next*
//! command, one fragment behind for the rest of the session.
//!
//! So the test is written the way the break shows up: encode an error, split on
//! LF, and require exactly one line. Asserting `contains("Parse error")` would
//! pass on the broken encoding, which is why the old tests did.

use bytes::BytesMut;
use samyama::protocol::resp::RespValue;

fn encoded(v: &RespValue) -> Vec<u8> {
    let mut buf = Vec::new();
    v.encode(&mut buf).unwrap();
    buf
}

#[test]
fn an_error_occupies_exactly_one_line() {
    let multi = "[Samyama.ClientError.Statement.SyntaxError] Parse error:  --> 1:10\n  |\n1 | MATCH (n RETURN n\n  |          ^---\n";
    let buf = encoded(&RespValue::Error(multi.to_string()));
    let body = &buf[..buf.len() - 2]; // drop the CRLF terminator
    assert_eq!(
        body.iter().filter(|b| **b == b'\n' || **b == b'\r').count(),
        0,
        "the payload still carries CR or LF: {:?}",
        String::from_utf8_lossy(body)
    );
    assert!(buf.ends_with(b"\r\n"));
}

#[test]
fn the_span_survives_the_escaping() {
    // Truncating at the first newline would also make the error one line, and
    // would throw away the line/column and the caret -- the half of the message
    // LANG-12 is about. Escaping keeps it.
    let buf = encoded(&RespValue::Error(
        "Parse error:  --> 1:10\n  |\n  |          ^---".to_string(),
    ));
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("1:10"), "line and column lost: {s}");
    assert!(s.contains("^---"), "caret lost: {s}");
    assert!(s.contains("\\n"), "newlines were dropped, not escaped: {s}");
}

#[test]
fn a_simple_string_is_one_line_too() {
    let buf = encoded(&RespValue::SimpleString("a\nb".to_string()));
    assert_eq!(buf, b"+a\\nb\r\n");
}

#[test]
fn an_error_without_newlines_is_untouched() {
    // The escape must not change the wire format of every error that was
    // already correct -- clients match on these strings.
    let buf = encoded(&RespValue::Error("ERR unknown command".to_string()));
    assert_eq!(buf, b"-ERR unknown command\r\n");
}

#[test]
fn the_next_reply_is_the_next_reply() {
    // The bug as a client sees it: read an error, then read again, and the
    // second value must be the second reply -- not a fragment of the first.
    //
    // Read the way the *broken* client reads. Our own decoder scans for CRLF,
    // so it stays in sync across a bare-LF payload and this test passed on the
    // broken encoding when written against it -- a check that could not fail.
    // The clients that desync are the ones that treat LF as the terminator,
    // which is what `readline` does in most standard libraries, so that is the
    // reader modelled here.
    fn lf_lines(buf: &[u8]) -> Vec<String> {
        String::from_utf8_lossy(buf)
            .split('\n')
            .map(|l| l.trim_end_matches('\r').to_string())
            .filter(|l| !l.is_empty())
            .collect()
    }

    let mut buf = Vec::new();
    RespValue::Error("Parse error:\n  |\n  ^".to_string())
        .encode(&mut buf)
        .unwrap();
    RespValue::SimpleString("PONG".to_string())
        .encode(&mut buf)
        .unwrap();

    let lines = lf_lines(&buf);
    assert_eq!(
        lines.len(),
        2,
        "an LF-reading client sees {} replies where the server sent 2: {:?}",
        lines.len(),
        lines
    );
    assert!(lines[0].starts_with('-'), "first reply is the error");
    assert_eq!(lines[1], "+PONG", "second reply is the second reply");

    // And the CRLF-reading client still decodes both, unchanged.
    let mut stream = BytesMut::from(&buf[..]);
    assert!(matches!(
        RespValue::decode(&mut stream).unwrap().unwrap(),
        RespValue::Error(_)
    ));
    assert_eq!(
        RespValue::decode(&mut stream).unwrap().unwrap(),
        RespValue::SimpleString("PONG".to_string())
    );
}
