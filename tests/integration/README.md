# Integration Tests for Samyama Graph Database

This directory contains integration tests for the RESP protocol server and OpenCypher query engine.

## Prerequisites

1. **Python 3.6+** installed
2. **Samyama server** running on port 6379

## Running the Tests

### 1. Start the Samyama Server

```bash
# From the project root
cargo run --release
```

The server will start on `127.0.0.1:6379` and display:
```
✅ Server ready. Press Ctrl+C to stop.
```

### 2. Run Tests (in another terminal)

#### Basic RESP Protocol Tests

Tests basic Redis commands and OpenCypher queries:

```bash
cd tests/integration
python3 test_resp_basic.py
```

**Expected Output:**
```
🔗 Connecting to Samyama server at 127.0.0.1:6379...
✅ Connected!

Test 1: PING
Response: +PONG
...
✅ All tests completed!
```

#### Visual RESP Protocol Demonstration

Shows detailed RESP protocol encoding/decoding:

```bash
python3 test_resp_visual.py
```

**Expected Output:**
```
================================================================================
SAMYAMA GRAPH DATABASE - RESP PROTOCOL DEMONSTRATION
================================================================================
...
📤 Sending (RESP format):
  '*1\r\n$4\r\nPING\r\n'

📥 Received (RESP format):
  '+PONG\r\n'
...
```

## Test Coverage

### test_resp_basic.py

Tests the following functionality:

1. **Basic Redis Commands**
   - `PING` - Server health check
   - `PING message` - Echo message back
   - `ECHO message` - Echo command
   - `INFO` - Server information

2. **Graph Management Commands**
   - `GRAPH.LIST` - List available graphs

3. **OpenCypher Queries**
   - `GRAPH.QUERY` with simple MATCH
   - `GRAPH.QUERY` with WHERE filtering
   - `GRAPH.QUERY` with edge traversal

### test_resp_visual.py

Demonstrates:
- RESP protocol encoding (how commands are sent)
- RESP protocol decoding (how responses are received)
- Visual inspection of protocol messages
- Detailed query execution flow

## Test Data

The server initializes with the following test data:

```
Nodes:
- Alice (Person, age: 30)
- Bob (Person, age: 25)

Edges:
- Alice -[KNOWS]-> Bob
```

## Expected Test Results

All tests should pass with:
- ✅ PING returns PONG
- ✅ ECHO returns the message
- ✅ INFO returns server metadata
- ✅ GRAPH.LIST returns ["default"]
- ✅ GRAPH.QUERY executes OpenCypher queries correctly
- ✅ Query results match expected data

## Troubleshooting

### Connection Refused

**Error:** `Connection refused to 127.0.0.1:6379`

**Solution:** Make sure the Samyama server is running:
```bash
cargo run --release
```

### Import Errors

**Error:** `ModuleNotFoundError: No module named '...'`

**Solution:** These tests use only Python standard library. Ensure you're using Python 3.6+:
```bash
python3 --version
```

### Timeout Errors

**Error:** `socket.timeout: timed out`

**Solution:**
- Check if server is responsive: try restarting it
- Increase timeout in test files if needed (default: 5 seconds)

## Using with redis-cli

If you have `redis-cli` installed, you can also test manually:

```bash
# Install redis-cli
brew install redis  # macOS
apt install redis-tools  # Ubuntu/Debian

# Connect
redis-cli

# Try commands
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> GRAPH.QUERY mygraph "MATCH (n:Person) RETURN n"
...

127.0.0.1:6379> INFO
# Server
samyama_version:0.1.0
...
```

## Using with Python Redis Library

```python
import redis

# Connect
r = redis.Redis(host='localhost', port=6379)

# Test connection
print(r.ping())  # True

# Execute query
result = r.execute_command(
    'GRAPH.QUERY', 'mygraph',
    'MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age'
)

# Parse results
headers = result[0]
for row in result[1:]:
    print(dict(zip(headers, row)))
```

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Start Samyama Server
  run: cargo run --release &

- name: Wait for Server
  run: sleep 2

- name: Run Integration Tests
  run: |
    cd tests/integration
    python3 test_resp_basic.py
    python3 test_resp_visual.py
```

## Adding New Tests

To add new integration tests:

1. Create a new Python file in this directory
2. Follow the pattern in existing test files:
   - Connect to server via socket
   - Send RESP-formatted commands
   - Parse RESP responses
   - Assert expected results

3. Document the new test in this README

## Test Results

See `docs/test-results/PHASE2_RESP_TESTS.md` for detailed test results from Phase 2 implementation.

---

**Last Updated:** 2025-10-15
**Python Version:** 3.6+
**Dependencies:** None (uses standard library only)
