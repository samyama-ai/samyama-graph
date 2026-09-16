//! A column promoted from sparse to dense moves its values; it does not copy
//! them (#1269).
//!
//! Promotion used to clone every value into the dense array and then drop the
//! map, which freed every original. For a string column that is one small
//! allocation and one small free per entry, all at once. On glibc the frees
//! land scattered among the copies: loading LDBC SF1's Comment nodes, the
//! free-chunk count went from 6 to 1,775,224 between rows 1.8M and 1.9M, the
//! step where a string column crosses 2^21 entries. Those chunks are what made
//! BI-9/BI-12/BI-14 1.5-1.7x slower for embedded users on glibc once MVCC step
//! 5b stopped refilling them.
//!
//! Counted with an allocator local to this test binary, so the assertion is on
//! the number of allocations the promoting insert makes, which a copy makes one
//! per entry and a move does not.

use samyama::graph::{ColumnStore, PropertyValue};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn value(i: usize) -> PropertyValue {
    PropertyValue::String(format!("comment content number {i:08}"))
}

#[test]
fn promoting_a_string_column_moves_its_values_instead_of_copying_them() {
    // Promotion is considered when the map reaches a power of two and the dense
    // layout would be smaller. Contiguous rows promote at the first check
    // (1,024), so, as with a label whose rows arrive with gaps, the even rows
    // come first -- 2,048 entries over 4,096 slots stays sparse at 1,024 and
    // 2,048 -- and the odd rows fill in until the 4,096th insert promotes.
    const N: usize = 4096;
    let mut store = ColumnStore::new();
    for i in (0..N).step_by(2) {
        store.set_property(i, "content", value(i));
    }
    for i in (1..N - 1).step_by(2) {
        store.set_property(i, "content", value(i));
    }
    assert!(!store.get_column("content").expect("column").is_dense(), "promoted before the threshold");

    let last = value(N - 1);
    let before = ALLOCS.load(Ordering::Relaxed);
    store.set_property(N - 1, "content", last);
    let during = ALLOCS.load(Ordering::Relaxed) - before;

    assert!(store.get_column("content").expect("column").is_dense(), "the column was not promoted at {N} entries");
    assert!(
        during < 64,
        "promoting {N} strings made {during} allocations: the values were copied, not moved"
    );
    for i in [0, 1, N / 2, N - 2, N - 1] {
        assert_eq!(store.get_property(i, "content"), value(i), "row {i} changed across promotion");
    }
}
