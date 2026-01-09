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

echo "Waiting 10s for nodes to start..."
sleep 10

# Check if nodes are running
if ! ps -p $PID1 > /dev/null; then
    echo "Node 1 failed to start! Log:"
    cat node1.log
    exit 1
fi
if ! ps -p $PID2 > /dev/null; then
    echo "Node 2 failed to start! Log:"
    cat node2.log
    kill $PID1 || true
    exit 1
fi

# Run Client Test
echo "Running Distributed Client Test..."
python3 tests/distributed_client.py
TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo "Test Failed! Node 1 Log:"
    cat node1.log
    echo "Node 2 Log:"
    cat node2.log
fi

# Cleanup
echo "Stopping nodes..."
kill $PID1 $PID2
wait $PID1 $PID2
echo "Distributed Test Complete"
exit $TEST_EXIT_CODE
