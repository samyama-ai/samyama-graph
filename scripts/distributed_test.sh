#!/bin/bash
set -e

# Build release binary
echo "Building Samyama..."
cargo build --release

BIN="./target/release/samyama"

# Kill existing
pkill -f "samyama" || true

# Start Node 1
echo "Starting Node 1 (6379 / 8080)..."
RESP_PORT=6379 HTTP_PORT=8080 $BIN > node1.log 2>&1 &
PID1=$!

# Start Node 2
echo "Starting Node 2 (6380 / 8081)..."
RESP_PORT=6380 HTTP_PORT=8081 $BIN > node2.log 2>&1 &
PID2=$!

echo "Waiting for nodes to start..."
sleep 5

# Run Client Test
echo "Running Distributed Client Test..."
python3 tests/distributed_client.py

# Cleanup
echo "Stopping nodes..."
kill $PID1 $PID2
wait $PID1 $PID2
echo "Distributed Test Complete"
