//! The slow-query ring stays in id order when queries finish together (#2079).
//!
//! Its own binary: the ring is process-wide, and `tests/slowlog.rs` counts
//! what is in it.

use samyama::query::slowlog;

#[test]
fn concurrent_records_keep_newest_first_in_id_order() {
    let threads: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                for i in 0..2_000 {
                    slowlog::record(&format!("q{t}-{i}"), 10, 1, 0, false);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let recent = slowlog::recent(slowlog::max_len());
    assert_eq!(recent.len(), slowlog::max_len());
    for pair in recent.windows(2) {
        assert!(
            pair[0].id > pair[1].id,
            "newest first must be in id order: {} then {}",
            pair[0].id,
            pair[1].id
        );
        assert!(
            pair[0].timestamp_ms >= pair[1].timestamp_ms,
            "a newer entry is not older"
        );
    }
    // Every record got an id and none was handed out twice.
    assert_eq!(recent[0].id, 16_000);
}
