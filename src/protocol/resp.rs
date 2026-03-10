//! # RESP3 Encoder/Decoder
//!
//! ## RESP3 encoding and decoding
//!
//! The protocol uses `\r\n` (CRLF) as line terminators. Each message starts with a type
//! byte (`+` for simple strings, `-` for errors, `:` for integers, `$` for bulk strings,
//! `*` for arrays, `_` for null). The decoder reads the type byte, then parses the
//! remainder according to the type-specific format.
//!
//! ## State machine parsing
//!
//! Messages arrive as byte streams over TCP. A single logical message may be split across
//! multiple TCP packets (fragmentation), or multiple messages may arrive in one packet
//! (pipelining). The `decode()` function handles this by returning:
//! - `Ok(Some(value))` — a complete message was parsed and consumed from the buffer
//! - `Ok(None)` — not enough data yet; the caller should buffer more bytes and retry
//! - `Err(...)` — the data is malformed
//!
//! This pattern is standard in async network programming and integrates with Tokio's
//! codec framework.
//!
//! ## `BytesMut` from the `bytes` crate
//!
//! `BytesMut` is a mutable byte buffer optimized for network I/O. Unlike `Vec<u8>`, it
//! supports zero-copy slicing (`split_to()` returns the consumed bytes without copying)
//! and efficient prepending. It is the standard buffer type in the Tokio ecosystem for
//! reading from and writing to sockets.
//!
//! ## Rust concept: the `Buf` trait
//!
//! The `Buf` trait (from the `bytes` crate) abstracts over "a buffer with a read cursor."
//! Methods like `chunk()` return the readable bytes, and `advance(n)` moves the cursor
//! forward without copying data. This enables efficient sequential reads through a buffer,
//! which is exactly what a protocol parser needs.

use bytes::{Buf, BytesMut};
use std::io::{self, Write};
use thiserror::Error;

/// RESP protocol errors
#[derive(Error, Debug)]
pub enum RespError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Protocol parsing error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Incomplete data
    #[error("Incomplete data")]
    Incomplete,

    /// Invalid encoding
    #[error("Invalid encoding: {0}")]
    InvalidEncoding(String),
}

pub type RespResult<T> = Result<T, RespError>;

/// RESP value types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(String),
    /// Error: -ERR message\r\n
    Error(String),
    /// Integer: :1000\r\n
    Integer(i64),
    /// Bulk string: $6\r\nfoobar\r\n (or $-1\r\n for null)
    BulkString(Option<Vec<u8>>),
    /// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Vec<RespValue>),
    /// Null: _\r\n (RESP3)
    Null,
}

impl RespValue {
    /// Encode RESP value to bytes
    pub fn encode(&self, buf: &mut Vec<u8>) -> io::Result<()> {
        match self {
            RespValue::SimpleString(s) => {
                write!(buf, "+{}\r\n", s)?;
            }
            RespValue::Error(e) => {
                write!(buf, "-{}\r\n", e)?;
            }
            RespValue::Integer(i) => {
                write!(buf, ":{}\r\n", i)?;
            }
            RespValue::BulkString(None) => {
                write!(buf, "$-1\r\n")?;
            }
            RespValue::BulkString(Some(data)) => {
                write!(buf, "${}\r\n", data.len())?;
                buf.extend_from_slice(data);
                write!(buf, "\r\n")?;
            }
            RespValue::Array(items) => {
                write!(buf, "*{}\r\n", items.len())?;
                for item in items {
                    item.encode(buf)?;
                }
            }
            RespValue::Null => {
                write!(buf, "_\r\n")?;
            }
        }
        Ok(())
    }

    /// Parse RESP value from buffer
    pub fn decode(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if buf.is_empty() {
            return Ok(None);
        }

        let first = buf[0];

        match first {
            b'+' => Self::decode_simple_string(buf),
            b'-' => Self::decode_error(buf),
            b':' => Self::decode_integer(buf),
            b'$' => Self::decode_bulk_string(buf),
            b'*' => Self::decode_array(buf),
            b'_' => Self::decode_null(buf),
            // Handle inline commands (plain text commands not in RESP format)
            // Redis protocol supports inline commands for simple clients like telnet
            _ => Self::decode_inline_command(buf),
        }
    }

    fn decode_simple_string(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if let Some(line) = Self::read_line(buf)? {
            let s = String::from_utf8(line[1..].to_vec())
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
            Ok(Some(RespValue::SimpleString(s)))
        } else {
            Ok(None)
        }
    }

    fn decode_error(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if let Some(line) = Self::read_line(buf)? {
            let s = String::from_utf8(line[1..].to_vec())
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
            Ok(Some(RespValue::Error(s)))
        } else {
            Ok(None)
        }
    }

    fn decode_integer(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if let Some(line) = Self::read_line(buf)? {
            let s = String::from_utf8(line[1..].to_vec())
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
            let i = s.parse::<i64>()
                .map_err(|e| RespError::Protocol(format!("Invalid integer: {}", e)))?;
            Ok(Some(RespValue::Integer(i)))
        } else {
            Ok(None)
        }
    }

    fn decode_bulk_string(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        // First, read the length line
        if let Some(len_line) = Self::read_line(buf)? {
            let len_str = String::from_utf8(len_line[1..].to_vec())
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
            let len = len_str.parse::<i64>()
                .map_err(|e| RespError::Protocol(format!("Invalid bulk string length: {}", e)))?;

            if len == -1 {
                return Ok(Some(RespValue::BulkString(None)));
            }

            let len = len as usize;

            // Check if we have enough data for the bulk string + \r\n
            if buf.len() < len + 2 {
                // Put the length line back and wait for more data
                return Err(RespError::Incomplete);
            }

            // Read the bulk string data
            let data = buf[..len].to_vec();
            buf.advance(len);

            // Verify and consume \r\n
            if buf.len() < 2 || &buf[..2] != b"\r\n" {
                return Err(RespError::Protocol("Missing \\r\\n after bulk string".to_string()));
            }
            buf.advance(2);

            Ok(Some(RespValue::BulkString(Some(data))))
        } else {
            Ok(None)
        }
    }

    fn decode_array(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        // Read array length
        if let Some(len_line) = Self::read_line(buf)? {
            let len_str = String::from_utf8(len_line[1..].to_vec())
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
            let len = len_str.parse::<usize>()
                .map_err(|e| RespError::Protocol(format!("Invalid array length: {}", e)))?;

            // Read array elements
            let mut elements = Vec::with_capacity(len);
            for _ in 0..len {
                match Self::decode(buf)? {
                    Some(val) => elements.push(val),
                    None => return Err(RespError::Incomplete),
                }
            }

            Ok(Some(RespValue::Array(elements)))
        } else {
            Ok(None)
        }
    }

    fn decode_null(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if let Some(line) = Self::read_line(buf)? {
            if line.len() == 1 && line[0] == b'_' {
                Ok(Some(RespValue::Null))
            } else {
                Err(RespError::Protocol("Invalid null value".to_string()))
            }
        } else {
            Ok(None)
        }
    }

    /// Decode inline command (plain text, not RESP formatted)
    /// Example: GRAPH.QUERY graphname "CREATE (n:Person {name: 'Alice'})"
    /// Converts to Array of BulkStrings for uniform handling
    fn decode_inline_command(buf: &mut BytesMut) -> RespResult<Option<RespValue>> {
        if let Some(line) = Self::read_line(buf)? {
            let line_str = String::from_utf8(line)
                .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;

            // Parse the inline command into tokens, respecting quotes
            let tokens = Self::parse_inline_tokens(&line_str)?;

            if tokens.is_empty() {
                return Err(RespError::Protocol("Empty inline command".to_string()));
            }

            // Convert tokens to array of bulk strings
            let elements: Vec<RespValue> = tokens
                .into_iter()
                .map(|t| RespValue::BulkString(Some(t.into_bytes())))
                .collect();

            Ok(Some(RespValue::Array(elements)))
        } else {
            Ok(None)
        }
    }

    /// Parse inline command tokens, respecting quoted strings
    /// "GRAPH.QUERY graph \"CREATE (n)\"" -> ["GRAPH.QUERY", "graph", "CREATE (n)"]
    fn parse_inline_tokens(line: &str) -> RespResult<Vec<String>> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                }
                ' ' | '\t' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                }
                '\\' if in_quotes => {
                    // Handle escape sequences
                    if let Some(next) = chars.next() {
                        match next {
                            'n' => current.push('\n'),
                            't' => current.push('\t'),
                            'r' => current.push('\r'),
                            '"' => current.push('"'),
                            '\\' => current.push('\\'),
                            _ => {
                                current.push('\\');
                                current.push(next);
                            }
                        }
                    }
                }
                _ => {
                    current.push(c);
                }
            }
        }

        if !current.is_empty() {
            tokens.push(current);
        }

        if in_quotes {
            return Err(RespError::Protocol("Unclosed quote in inline command".to_string()));
        }

        Ok(tokens)
    }

    /// Read a CRLF-terminated line from the buffer
    fn read_line(buf: &mut BytesMut) -> RespResult<Option<Vec<u8>>> {
        // Find \r\n
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf[..pos].to_vec();
            buf.advance(pos + 2);
            Ok(Some(line))
        } else {
            Ok(None)
        }
    }

    /// Convert to array or error
    pub fn as_array(&self) -> RespResult<&[RespValue]> {
        match self {
            RespValue::Array(arr) => Ok(arr),
            _ => Err(RespError::Protocol("Expected array".to_string())),
        }
    }

    /// Convert to bulk string or error
    pub fn as_bulk_string(&self) -> RespResult<Option<&[u8]>> {
        match self {
            RespValue::BulkString(Some(data)) => Ok(Some(data)),
            RespValue::BulkString(None) => Ok(None),
            _ => Err(RespError::Protocol("Expected bulk string".to_string())),
        }
    }

    /// Convert bulk string to UTF-8 string
    pub fn as_string(&self) -> RespResult<Option<String>> {
        match self.as_bulk_string()? {
            Some(bytes) => {
                let s = String::from_utf8(bytes.to_vec())
                    .map_err(|e| RespError::InvalidEncoding(e.to_string()))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let val = RespValue::SimpleString("OK".to_string());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let val = RespValue::Error("ERR unknown command".to_string());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let val = RespValue::Integer(1000);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b":1000\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let val = RespValue::BulkString(Some(b"foobar".to_vec()));
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"$6\r\nfoobar\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let val = RespValue::BulkString(None);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let val = RespValue::Array(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_decode_simple_string() {
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut buf = BytesMut::from(&b"$6\r\nfoobar\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"foobar".to_vec())));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_array() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec())),
            ])
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_incomplete() {
        let mut buf = BytesMut::from(&b"$6\r\nfoo"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(matches!(result, Err(RespError::Incomplete)));
    }

    #[test]
    fn test_decode_inline_command() {
        let mut buf = BytesMut::from(&b"PING\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"PING".to_vec())),
            ])
        );
    }

    #[test]
    fn test_decode_inline_command_with_args() {
        let mut buf = BytesMut::from(&b"GRAPH.QUERY mygraph \"MATCH (n) RETURN n\"\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
                RespValue::BulkString(Some(b"mygraph".to_vec())),
                RespValue::BulkString(Some(b"MATCH (n) RETURN n".to_vec())),
            ])
        );
    }

    #[test]
    fn test_parse_inline_tokens() {
        let tokens = RespValue::parse_inline_tokens("SET key \"hello world\"").unwrap();
        assert_eq!(tokens, vec!["SET", "key", "hello world"]);
    }

    // ========== Batch 6: Additional RESP Tests ==========

    #[test]
    fn test_encode_null() {
        let val = RespValue::Null;
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        // RESP3 null type
        assert_eq!(&buf, b"_\r\n");
    }

    #[test]
    fn test_encode_nested_array() {
        let val = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
            ]),
            RespValue::SimpleString("ok".to_string()),
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_decode_integer() {
        let mut buf = BytesMut::from(&b":42\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(42));
    }

    #[test]
    fn test_decode_negative_integer() {
        let mut buf = BytesMut::from(&b":-10\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(-10));
    }

    #[test]
    fn test_decode_error() {
        let mut buf = BytesMut::from(&b"-ERR unknown command\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_decode_null_bulk_string() {
        let mut buf = BytesMut::from(&b"$-1\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        // $-1 decodes as BulkString(None), not Null
        assert_eq!(val, RespValue::BulkString(None));
    }

    #[test]
    fn test_as_array() {
        let val = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)]);
        let arr = val.as_array().unwrap();
        assert_eq!(arr.len(), 2);

        let val2 = RespValue::Integer(42);
        assert!(val2.as_array().is_err());
    }

    #[test]
    fn test_as_bulk_string() {
        let val = RespValue::BulkString(Some(b"hello".to_vec()));
        let bs = val.as_bulk_string().unwrap();
        assert_eq!(bs, Some(&b"hello"[..]));

        let null_val = RespValue::BulkString(None);
        let bs_null = null_val.as_bulk_string().unwrap();
        assert!(bs_null.is_none());

        let int_val = RespValue::Integer(1);
        assert!(int_val.as_bulk_string().is_err());
    }

    #[test]
    fn test_as_string() {
        let val = RespValue::BulkString(Some(b"hello".to_vec()));
        let s = val.as_string().unwrap();
        assert_eq!(s, Some("hello".to_string()));

        let null_val = RespValue::BulkString(None);
        let s_null = null_val.as_string().unwrap();
        assert!(s_null.is_none());
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(Some(b"MATCH (n) RETURN n".to_vec())),
        ]);
        let mut encoded = Vec::new();
        original.encode(&mut encoded).unwrap();

        let mut buf = BytesMut::from(&encoded[..]);
        let decoded = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
    }

    // ========== Additional RESP Coverage Tests ==========

    #[test]
    fn test_decode_empty_buffer() {
        let mut buf = BytesMut::new();
        let result = RespValue::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_null_resp3() {
        let mut buf = BytesMut::from(&b"_\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Null);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_invalid_null() {
        // Null marker followed by extra characters before CRLF
        let mut buf = BytesMut::from(&b"_extra\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_empty_bulk_string() {
        let val = RespValue::BulkString(Some(b"".to_vec()));
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"$0\r\n\r\n");
    }

    #[test]
    fn test_decode_empty_bulk_string() {
        let mut buf = BytesMut::from(&b"$0\r\n\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"".to_vec())));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_large_integer() {
        let val = RespValue::Integer(i64::MAX);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        let expected = format!(":{}\r\n", i64::MAX);
        assert_eq!(buf, expected.as_bytes());
    }

    #[test]
    fn test_encode_negative_integer() {
        let val = RespValue::Integer(i64::MIN);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        let expected = format!(":{}\r\n", i64::MIN);
        assert_eq!(buf, expected.as_bytes());
    }

    #[test]
    fn test_encode_zero_integer() {
        let val = RespValue::Integer(0);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b":0\r\n");
    }

    #[test]
    fn test_decode_zero_integer() {
        let mut buf = BytesMut::from(&b":0\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(0));
    }

    #[test]
    fn test_encode_empty_array() {
        let val = RespValue::Array(vec![]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"*0\r\n");
    }

    #[test]
    fn test_decode_empty_array() {
        let mut buf = BytesMut::from(&b"*0\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Array(vec![]));
    }

    #[test]
    fn test_encode_large_array() {
        let items: Vec<RespValue> = (0..100)
            .map(|i| RespValue::Integer(i))
            .collect();
        let val = RespValue::Array(items);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        if let RespValue::Array(arr) = decoded {
            assert_eq!(arr.len(), 100);
            assert_eq!(arr[0], RespValue::Integer(0));
            assert_eq!(arr[99], RespValue::Integer(99));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_decode_nested_array() {
        // *2\r\n *2\r\n :1\r\n :2\r\n *1\r\n :3\r\n
        let mut buf = BytesMut::from(&b"*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(1),
                    RespValue::Integer(2),
                ]),
                RespValue::Array(vec![
                    RespValue::Integer(3),
                ]),
            ])
        );
    }

    #[test]
    fn test_encode_array_of_mixed_types() {
        let val = RespValue::Array(vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(b"hello".to_vec())),
            RespValue::BulkString(None),
            RespValue::Error("ERR test".to_string()),
            RespValue::Null,
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_decode_simple_string_incomplete() {
        let mut buf = BytesMut::from(&b"+OK"[..]); // Missing \r\n
        let result = RespValue::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_error_incomplete() {
        let mut buf = BytesMut::from(&b"-ERR"[..]); // Missing \r\n
        let result = RespValue::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_integer_incomplete() {
        let mut buf = BytesMut::from(&b":123"[..]); // Missing \r\n
        let result = RespValue::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_integer_invalid() {
        let mut buf = BytesMut::from(&b":abc\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_bulk_string_length_line_incomplete() {
        let mut buf = BytesMut::from(&b"$6"[..]); // Missing \r\n after length
        let result = RespValue::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_array_incomplete_elements() {
        // Array of 2 elements but only 1 provided
        let mut buf = BytesMut::from(&b"*2\r\n:1\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(result.is_err()); // Incomplete
    }

    #[test]
    fn test_encode_bulk_string_with_binary_data() {
        let data = vec![0x00, 0x01, 0xFF, 0xFE];
        let val = RespValue::BulkString(Some(data.clone()));
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, RespValue::BulkString(Some(data)));
    }

    #[test]
    fn test_encode_simple_string_with_special_chars() {
        let val = RespValue::SimpleString("hello world!@#$%".to_string());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"+hello world!@#$%\r\n");
    }

    #[test]
    fn test_encode_error_message_with_details() {
        let val = RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind".to_string());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"-WRONGTYPE Operation against a key holding the wrong kind\r\n");
    }

    #[test]
    fn test_decode_long_simple_string() {
        let long_str = "x".repeat(10000);
        let input = format!("+{}\r\n", long_str);
        let mut buf = BytesMut::from(input.as_bytes());
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString(long_str));
    }

    #[test]
    fn test_decode_long_bulk_string() {
        let data = vec![b'A'; 5000];
        let header = format!("${}\r\n", data.len());
        let mut input = Vec::new();
        input.extend_from_slice(header.as_bytes());
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");

        let mut buf = BytesMut::from(&input[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(Some(data)));
    }

    #[test]
    fn test_inline_command_with_escape_sequences() {
        let mut buf = BytesMut::from(&b"SET key \"hello\\nworld\"\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        if let RespValue::Array(arr) = &val {
            assert_eq!(arr.len(), 3);
            // The value should have a newline in it
            if let RespValue::BulkString(Some(data)) = &arr[2] {
                let s = String::from_utf8(data.clone()).unwrap();
                assert!(s.contains('\n'));
            }
        } else {
            panic!("Expected Array from inline command");
        }
    }

    #[test]
    fn test_inline_command_with_tab_escape() {
        let mut buf = BytesMut::from(&b"SET key \"col1\\tcol2\"\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        if let RespValue::Array(arr) = &val {
            if let RespValue::BulkString(Some(data)) = &arr[2] {
                let s = String::from_utf8(data.clone()).unwrap();
                assert!(s.contains('\t'));
            }
        }
    }

    #[test]
    fn test_inline_command_with_backslash_escape() {
        let mut buf = BytesMut::from(&b"SET key \"path\\\\file\"\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        if let RespValue::Array(arr) = &val {
            if let RespValue::BulkString(Some(data)) = &arr[2] {
                let s = String::from_utf8(data.clone()).unwrap();
                assert_eq!(s, "path\\file");
            }
        }
    }

    #[test]
    fn test_inline_command_with_escaped_quote() {
        let mut buf = BytesMut::from(&b"SET key \"say \\\"hi\\\"\"\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        if let RespValue::Array(arr) = &val {
            if let RespValue::BulkString(Some(data)) = &arr[2] {
                let s = String::from_utf8(data.clone()).unwrap();
                assert_eq!(s, "say \"hi\"");
            }
        }
    }

    #[test]
    fn test_inline_command_with_unknown_escape() {
        // Unknown escape like \z should produce literal \z
        let tokens = RespValue::parse_inline_tokens(r#""hello\zworld""#).unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], "hello\\zworld");
    }

    #[test]
    fn test_inline_command_unclosed_quote() {
        let result = RespValue::parse_inline_tokens(r#"SET key "unclosed"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_inline_command_tab_separator() {
        // Tabs should also act as whitespace separators
        let tokens = RespValue::parse_inline_tokens("SET\tkey\tvalue").unwrap();
        assert_eq!(tokens, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_inline_command_multiple_spaces() {
        let tokens = RespValue::parse_inline_tokens("SET   key   value").unwrap();
        assert_eq!(tokens, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_inline_command_empty_string() {
        let mut buf = BytesMut::from(&b"\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        // Empty inline command should produce an error
        assert!(result.is_err());
    }

    #[test]
    fn test_as_string_non_utf8() {
        let val = RespValue::BulkString(Some(vec![0xFF, 0xFE]));
        let result = val.as_string();
        assert!(result.is_err());
    }

    #[test]
    fn test_as_string_from_non_bulk_string() {
        let val = RespValue::SimpleString("hello".to_string());
        let result = val.as_string();
        // SimpleString is not BulkString, should error
        assert!(result.is_err());
    }

    #[test]
    fn test_as_array_on_null() {
        let val = RespValue::Null;
        assert!(val.as_array().is_err());
    }

    #[test]
    fn test_as_bulk_string_on_null() {
        let val = RespValue::Null;
        assert!(val.as_bulk_string().is_err());
    }

    #[test]
    fn test_encode_decode_roundtrip_all_types() {
        // Test roundtrip for every single variant
        let values = vec![
            RespValue::SimpleString("test".to_string()),
            RespValue::Error("ERR something".to_string()),
            RespValue::Integer(-999),
            RespValue::BulkString(Some(b"data".to_vec())),
            RespValue::BulkString(None),
            RespValue::Array(vec![]),
            RespValue::Null,
        ];

        for original in &values {
            let mut encoded = Vec::new();
            original.encode(&mut encoded).unwrap();
            let mut buf = BytesMut::from(&encoded[..]);
            let decoded = RespValue::decode(&mut buf).unwrap().unwrap();
            assert_eq!(&decoded, original, "Roundtrip failed for {:?}", original);
        }
    }

    #[test]
    fn test_decode_multiple_commands_in_buffer() {
        // Buffer with two commands
        let mut buf = BytesMut::from(&b"+OK\r\n:42\r\n"[..]);

        let val1 = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val1, RespValue::SimpleString("OK".to_string()));

        let val2 = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val2, RespValue::Integer(42));

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_bulk_string_invalid_length() {
        let mut buf = BytesMut::from(&b"$abc\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_array_invalid_length() {
        let mut buf = BytesMut::from(&b"*xyz\r\n"[..]);
        let result = RespValue::decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_decode_deeply_nested() {
        let val = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(42),
                ]),
            ]),
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_parse_inline_tokens_empty_string() {
        let tokens = RespValue::parse_inline_tokens("").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_parse_inline_tokens_only_whitespace() {
        let tokens = RespValue::parse_inline_tokens("   \t  ").unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_inline_carriage_return_escape() {
        let tokens = RespValue::parse_inline_tokens(r#""hello\rworld""#).unwrap();
        assert_eq!(tokens.len(), 1);
        assert!(tokens[0].contains('\r'));
    }

    // ========== Additional RESP Coverage Tests ==========

    #[test]
    fn test_encode_decode_single_element_array() {
        let val = RespValue::Array(vec![RespValue::Integer(42)]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"*1\r\n:42\r\n");

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_encode_array_with_null_and_bulk_strings() {
        let val = RespValue::Array(vec![
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(None),
            RespValue::Null,
            RespValue::BulkString(Some(b"val".to_vec())),
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_decode_large_integer() {
        let max_str = format!(":{}\r\n", i64::MAX);
        let mut buf = BytesMut::from(max_str.as_bytes());
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(i64::MAX));
    }

    #[test]
    fn test_decode_min_integer() {
        let min_str = format!(":{}\r\n", i64::MIN);
        let mut buf = BytesMut::from(min_str.as_bytes());
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(i64::MIN));
    }

    #[test]
    fn test_encode_error_with_prefix() {
        let val = RespValue::Error("MOVED 3999 127.0.0.1:6380".to_string());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"-MOVED 3999 127.0.0.1:6380\r\n");
    }

    #[test]
    fn test_encode_decode_large_bulk_string() {
        let data = vec![b'X'; 100_000];
        let val = RespValue::BulkString(Some(data.clone()));
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, RespValue::BulkString(Some(data)));
    }

    #[test]
    fn test_resp_error_display() {
        let err = RespError::Protocol("test protocol error".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Protocol error"));

        let err2 = RespError::Incomplete;
        let msg2 = format!("{}", err2);
        assert!(msg2.contains("Incomplete"));

        let err3 = RespError::InvalidEncoding("bad utf8".to_string());
        let msg3 = format!("{}", err3);
        assert!(msg3.contains("Invalid encoding"));
    }

    #[test]
    fn test_inline_command_with_single_token() {
        let tokens = RespValue::parse_inline_tokens("QUIT").unwrap();
        assert_eq!(tokens, vec!["QUIT"]);
    }

    #[test]
    fn test_inline_command_leading_trailing_spaces() {
        let tokens = RespValue::parse_inline_tokens("  PING  ").unwrap();
        assert_eq!(tokens, vec!["PING"]);
    }

    #[test]
    fn test_encode_simple_string_empty() {
        let val = RespValue::SimpleString(String::new());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"+\r\n");
    }

    #[test]
    fn test_decode_simple_string_empty() {
        let mut buf = BytesMut::from(&b"+\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString(String::new()));
    }

    #[test]
    fn test_encode_error_empty() {
        let val = RespValue::Error(String::new());
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, b"-\r\n");
    }

    #[test]
    fn test_decode_error_empty() {
        let mut buf = BytesMut::from(&b"-\r\n"[..]);
        let val = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Error(String::new()));
    }

    #[test]
    fn test_encode_decode_array_of_arrays() {
        let val = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"val1".to_vec())),
            ]),
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"key2".to_vec())),
                RespValue::BulkString(Some(b"val2".to_vec())),
            ]),
        ]);
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();

        let mut decode_buf = BytesMut::from(&buf[..]);
        let decoded = RespValue::decode(&mut decode_buf).unwrap().unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_partial_then_complete_decode() {
        // Simulate receiving data in two chunks
        let full_data = b"+Hello\r\n:42\r\n";

        // First chunk: just the simple string
        let mut buf = BytesMut::from(&full_data[..8]); // "+Hello\r\n"
        let val1 = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val1, RespValue::SimpleString("Hello".to_string()));

        // Add remaining data
        buf.extend_from_slice(&full_data[8..]);
        let val2 = RespValue::decode(&mut buf).unwrap().unwrap();
        assert_eq!(val2, RespValue::Integer(42));
    }
}
