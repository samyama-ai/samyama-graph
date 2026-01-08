# Phase 9: Async Ingestion Pipeline - Implementation Plan

## Executive Summary

**Goal**: Fix the node ingestion bottleneck (184 nodes/sec) caused by synchronous HNSW/BTree indexing.

**Solution**: Decouple the write path. `GraphStore` will write to memory/disk and immediately acknowledge the client, while pushing indexing tasks to a background channel.

**Target**: >1,000 nodes/sec ingestion rate.

## Architecture

### 1. Index Events
New enum `IndexEvent` to capture changes:
```rust
enum IndexEvent {
    NodeCreated { id: NodeId, labels: Vec<Label>, properties: PropertyMap },
    PropertyUpdated { id: NodeId, labels: Vec<Label>, key: String, old: Option<PropertyValue>, new: PropertyValue },
    // ...
}
```

### 2. GraphStore Updates
*   Add `index_queue: Sender<IndexEvent>` to `GraphStore`.
*   In `create_node` / `set_property`: Send event to queue instead of calling `index.insert`.
*   This makes the write O(1) + Channel Send (very fast).

### 3. Background Worker
*   A loop that consumes `IndexEvent`s.
*   Calls `vector_index.add_vector` and `property_index.index_insert`.
*   Handles errors (logging) without crashing the server.

## Implementation Steps

### Step 1: Event System
*   Define `IndexEvent`.
*   Add crossbeam/tokio channel to `GraphStore`.

### Step 2: Refactor Write Path
*   Update `create_node_with_properties`.
*   Update `set_node_property`.
*   Update `add_label_to_node`.
*   Remove direct calls to `vector_index` and `property_index` from these methods.

### Step 3: Indexer Loop
*   Implement `run_indexer_loop(receiver, vector_mgr, property_mgr)`.
*   Integrate with `GraphStore::new()` (spawn thread/task).

### Step 4: Verification
*   Update `full_benchmark.rs` to measure ingestion time (which should now be fast).
*   Add a "wait for indexing" mechanism for the Search part of the benchmark (eventual consistency).

## Risks
*   **Consistency**: Search results might be stale for a few milliseconds. This is acceptable for "Eventual Consistency" models.
*   **Queue Overflow**: If writes are too fast, memory usage grows. We'll use a bounded channel or handle backpressure later.

---
**Status**: Planned
**Version**: 1.0
