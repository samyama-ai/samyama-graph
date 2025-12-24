//! RESP (Redis Serialization Protocol) implementation
//!
//! Implements REQ-REDIS-001 (RESP protocol support)
//! Based on RESP3 specification: https://redis.io/docs/reference/protocol-spec/

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
}
