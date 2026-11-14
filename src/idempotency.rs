//! Idempotency keys for writes (#2176).
//!
//! A client whose reply is lost -- a timeout, a dropped connection -- cannot
//! tell whether its write committed. Without a key it can only send the write
//! again and risk a duplicate, or give up and risk losing it. With a key, the
//! server remembers the reply of every keyed write for [`ttl`], and a request
//! carrying a key it has already seen gets that reply back without the write
//! running a second time.
//!
//! **What is remembered.** Only writes that succeeded. A statement that fails
//! is undone, so running it again on a retry is correct, and a key whose write
//! failed is free to be used again.
//!
//! **Same key, different request.** Refused. A key names one request; a
//! client that reuses one for something else has a bug, and replaying the
//! first reply to the second request would hide it.
//!
//! **Concurrency.** The log lives in the [`GraphStore`](crate::graph::GraphStore)
//! and is read and written under the store's write lock, in the same critical
//! section as the write itself. Two requests with the same key are therefore
//! serialised: the second finds the first's record and replays it.
//!
//! **Durability.** With persistence on, a record is written in the same
//! RocksDB commit as the last rows of its statement, so after a crash the
//! write and its key are on disk together or neither is. Records are reloaded
//! on recovery; expired ones are dropped then.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The request header a client sends the key in.
pub const HEADER: &str = "idempotency-key";

/// The response header set on a reply that was replayed rather than produced.
pub const REPLAYED_HEADER: &str = "idempotent-replayed";

/// Longest key accepted, in bytes. A UUID is 36.
pub const MAX_KEY_LEN: usize = 255;

/// How long a key is remembered: `SAMYAMA_IDEMPOTENCY_TTL_SECS`, default 24 h.
///
/// A retry after this is treated as a new request. Read per call so an
/// operator or a test can change it without a restart.
pub fn ttl() -> Duration {
    Duration::from_secs(
        std::env::var("SAMYAMA_IDEMPOTENCY_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(24 * 60 * 60),
    )
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Check a key a client sent. Visible ASCII only, 1 to [`MAX_KEY_LEN`] bytes.
pub fn validate_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("the Idempotency-Key header is empty".to_string());
    }
    if key.len() > MAX_KEY_LEN {
        return Err(format!(
            "the Idempotency-Key header is {} bytes; the limit is {MAX_KEY_LEN}",
            key.len()
        ));
    }
    if !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("the Idempotency-Key header must be visible ASCII, with no spaces".to_string());
    }
    Ok(())
}

/// What the server answered to a keyed write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// A hash of the request the key was first used for. A later request with
    /// the same key and a different fingerprint is refused.
    pub fingerprint: String,
    /// The reply's status code.
    pub status: u16,
    /// The reply's body, exactly as sent.
    pub body: Vec<u8>,
    /// The store version the write produced.
    pub version: u64,
    /// When the write committed, in milliseconds since the Unix epoch.
    pub created_ms: u64,
}

impl Record {
    fn expired(&self, now_ms: u64, ttl: Duration) -> bool {
        now_ms.saturating_sub(self.created_ms) >= ttl.as_millis() as u64
    }
}

/// The answer to "has this key been used?".
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Not used, or used so long ago that it has been forgotten.
    Miss,
    /// Used for this same request: here is what it got.
    Replay(Record),
    /// Used for a different request.
    Conflict,
}

/// A tenant and a key.
type Slot = (String, String);

/// Writes the persistence layer has to make for the log. See
/// [`IdempotencyLog::take_pending`].
#[derive(Debug, Default)]
pub struct PendingWrites {
    /// Records to write.
    pub puts: Vec<(String, String, Record)>,
    /// Expired records to delete, as `(tenant, key)`.
    pub deletes: Vec<(String, String)>,
}

impl PendingWrites {
    /// Whether there is nothing to write.
    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty()
    }
}

/// Every key the store remembers, per tenant.
#[derive(Debug, Default)]
pub struct IdempotencyLog {
    entries: HashMap<Slot, Record>,
    /// Slots in the order they were recorded, oldest first, so expiry is a
    /// walk from the front and not a scan of every entry on every write.
    order: VecDeque<(u64, Slot)>,
    pending: PendingWrites,
}

impl IdempotencyLog {
    /// Has `key` been used in `tenant`, and for this request?
    pub fn lookup(
        &self,
        tenant: &str,
        key: &str,
        fingerprint: &str,
        now_ms: u64,
        ttl: Duration,
    ) -> Lookup {
        match self.get(tenant, key, now_ms, ttl) {
            None => Lookup::Miss,
            Some(r) if r.fingerprint == fingerprint => Lookup::Replay(r.clone()),
            Some(_) => Lookup::Conflict,
        }
    }

    /// The record for `key`, unless there is none or it has expired.
    pub fn get(&self, tenant: &str, key: &str, now_ms: u64, ttl: Duration) -> Option<&Record> {
        self.entries
            .get(&(tenant.to_string(), key.to_string()))
            .filter(|r| !r.expired(now_ms, ttl))
    }

    /// Remember the reply to a write that succeeded, and forget what expired.
    ///
    /// The record is also queued for the persistence layer, which writes it in
    /// the same commit as the statement's rows.
    pub fn record(&mut self, tenant: &str, key: &str, record: Record, ttl: Duration) {
        let now = record.created_ms;
        self.expire(now, ttl);
        let slot = (tenant.to_string(), key.to_string());
        self.order.push_back((record.created_ms, slot.clone()));
        self.pending
            .puts
            .push((slot.0.clone(), slot.1.clone(), record.clone()));
        self.entries.insert(slot, record);
    }

    /// Put back a record read from disk at start-up. Not queued for writing:
    /// it is already there.
    pub fn restore(&mut self, tenant: &str, key: &str, record: Record) {
        let slot = (tenant.to_string(), key.to_string());
        self.order.push_back((record.created_ms, slot.clone()));
        self.entries.insert(slot, record);
    }

    /// Restore sorts nothing, so recovery calls this once after the last
    /// [`Self::restore`]: expiry walks `order` from the front and stops at the
    /// first live entry.
    pub fn finish_restore(&mut self) {
        self.order.make_contiguous().sort_by_key(|(t, _)| *t);
    }

    /// The writes queued since the last call, for the persistence layer.
    pub fn take_pending(&mut self) -> PendingWrites {
        std::mem::take(&mut self.pending)
    }

    /// Keys remembered, live or not yet swept.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no key is remembered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn expire(&mut self, now_ms: u64, ttl: Duration) {
        while let Some((created, slot)) = self.order.front() {
            if now_ms.saturating_sub(*created) < ttl.as_millis() as u64 {
                break;
            }
            let (created, slot) = (*created, slot.clone());
            self.order.pop_front();
            // A key recorded again after expiring has a newer entry in `order`;
            // this stale one must not remove it.
            if self
                .entries
                .get(&slot)
                .is_some_and(|r| r.created_ms == created)
            {
                self.entries.remove(&slot);
                self.pending.deletes.push(slot);
            }
        }
    }
}

/// A hash of what makes two requests the same: the statement, the graph, the
/// dialect and the parameters.
///
/// Parameters are hashed in key order, so the same parameters sent in a
/// different order are the same request.
pub fn fingerprint(
    query: &str,
    graph: &str,
    dialect: Option<&str>,
    params: &HashMap<String, serde_json::Value>,
) -> String {
    use sha2::{Digest, Sha256};
    let params: std::collections::BTreeMap<_, _> = params.iter().collect();
    let canonical = serde_json::json!({
        "query": query,
        "graph": graph,
        "dialect": dialect,
        "params": params,
    });
    Sha256::digest(canonical.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(fp: &str, at: u64) -> Record {
        Record {
            fingerprint: fp.into(),
            status: 200,
            body: b"{}".to_vec(),
            version: 1,
            created_ms: at,
        }
    }

    const DAY: Duration = Duration::from_secs(86_400);

    #[test]
    fn a_recorded_key_replays_for_the_same_request_and_conflicts_for_another() {
        let mut log = IdempotencyLog::default();
        assert_eq!(log.lookup("t", "k", "a", 0, DAY), Lookup::Miss);
        log.record("t", "k", rec("a", 0), DAY);
        assert_eq!(
            log.lookup("t", "k", "a", 1, DAY),
            Lookup::Replay(rec("a", 0))
        );
        assert_eq!(log.lookup("t", "k", "b", 1, DAY), Lookup::Conflict);
    }

    #[test]
    fn keys_are_per_tenant() {
        let mut log = IdempotencyLog::default();
        log.record("t1", "k", rec("a", 0), DAY);
        assert_eq!(log.lookup("t2", "k", "b", 1, DAY), Lookup::Miss);
    }

    #[test]
    fn an_expired_key_is_forgotten_and_its_deletion_queued() {
        let ttl = Duration::from_millis(10);
        let mut log = IdempotencyLog::default();
        log.record("t", "old", rec("a", 0), ttl);
        assert_eq!(log.lookup("t", "old", "a", 10, ttl), Lookup::Miss);
        let _ = log.take_pending();
        log.record("t", "new", rec("b", 10), ttl);
        assert_eq!(log.len(), 1);
        let pending = log.take_pending();
        assert_eq!(pending.deletes, vec![("t".to_string(), "old".to_string())]);
        assert_eq!(pending.puts.len(), 1);
    }

    #[test]
    fn a_key_reused_after_expiry_is_not_removed_by_its_old_entry() {
        let ttl = Duration::from_millis(10);
        let mut log = IdempotencyLog::default();
        log.record("t", "k", rec("a", 0), ttl);
        log.record("t", "k", rec("b", 10), ttl);
        log.record("t", "other", rec("c", 15), ttl);
        assert_eq!(
            log.lookup("t", "k", "b", 15, ttl),
            Lookup::Replay(rec("b", 10))
        );
    }

    #[test]
    fn fingerprints_ignore_parameter_order_and_see_parameter_values() {
        let mut p1 = HashMap::new();
        p1.insert("a".to_string(), serde_json::json!(1));
        p1.insert("b".to_string(), serde_json::json!(2));
        let mut p2 = HashMap::new();
        p2.insert("b".to_string(), serde_json::json!(2));
        p2.insert("a".to_string(), serde_json::json!(1));
        assert_eq!(
            fingerprint("q", "g", None, &p1),
            fingerprint("q", "g", None, &p2)
        );
        p2.insert("a".to_string(), serde_json::json!(3));
        assert_ne!(
            fingerprint("q", "g", None, &p1),
            fingerprint("q", "g", None, &p2)
        );
        assert_ne!(
            fingerprint("q", "g", None, &p1),
            fingerprint("q2", "g", None, &p1)
        );
    }

    #[test]
    fn keys_are_validated() {
        assert!(validate_key("3f2b-uuid").is_ok());
        assert!(validate_key("").is_err());
        assert!(validate_key("has space").is_err());
        assert!(validate_key(&"x".repeat(MAX_KEY_LEN + 1)).is_err());
    }
}
