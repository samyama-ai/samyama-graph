//! Test-only one-shot HTTP server on 127.0.0.1 for exercising the LLM and
//! embedding clients without touching a real provider.
//!
//! Each [`MockHttp`] serves a fixed list of canned responses, one per
//! connection, in order, and records every request it received so a test can
//! assert on the method, path, headers and JSON body the client built.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// One request as the server saw it.
#[derive(Debug, Clone, Default)]
pub struct Captured {
    pub method: String,
    /// Path including the query string.
    pub path: String,
    /// Header names lowercased.
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Captured {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).expect("request body is JSON")
    }
}

pub struct MockHttp {
    pub base_url: String,
    captured: Arc<Mutex<Vec<Captured>>>,
    handle: Option<JoinHandle<()>>,
}

impl MockHttp {
    /// Serve `responses` (status, body), one per incoming connection.
    pub fn serve(responses: Vec<(u16, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1");
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let cap = captured.clone();
        let handle = std::thread::spawn(move || {
            for (status, body) in responses {
                let (stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req = Captured::default();
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut parts = line.split_whitespace();
                req.method = parts.next().unwrap_or_default().to_string();
                req.path = parts.next().unwrap_or_default().to_string();
                let mut content_length = 0usize;
                loop {
                    let mut h = String::new();
                    if reader.read_line(&mut h).unwrap() == 0 {
                        break;
                    }
                    let h = h.trim_end();
                    if h.is_empty() {
                        break;
                    }
                    if let Some((k, v)) = h.split_once(':') {
                        let k = k.trim().to_ascii_lowercase();
                        let v = v.trim().to_string();
                        if k == "content-length" {
                            content_length = v.parse().unwrap_or(0);
                        }
                        req.headers.push((k, v));
                    }
                }
                let mut req_body = vec![0u8; content_length];
                reader.read_exact(&mut req_body).unwrap();
                req.body = String::from_utf8_lossy(&req_body).into_owned();
                cap.lock().unwrap().push(req);

                let mut stream = stream;
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            captured,
            handle: Some(handle),
        }
    }

    /// Wait for the server to finish serving every response and return what
    /// it received.
    pub fn requests(mut self) -> Vec<Captured> {
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
        self.captured.lock().unwrap().clone()
    }
}

/// A base URL on which nothing is listening: the port was bound and released.
pub fn dead_base_url() -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    format!("http://{addr}")
}
