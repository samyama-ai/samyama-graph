"""
Samyama Graph Database - Python RESP Client Demo

Demonstrates connecting to the Samyama RESP server and running queries.
Requires a running Samyama server: cargo run -- --port 6379

Features:
- RESP protocol encoding/decoding
- GRAPH.QUERY command for Cypher queries
- Batch query execution
- Error handling and connection retry
"""

import socket
import time
import sys


class SamyamaClient:
    """Simple RESP client for Samyama Graph Database."""

    def __init__(self, host="127.0.0.1", port=6379, timeout=5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None

    def connect(self):
        """Connect to the server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

    def close(self):
        """Close the connection."""
        if self.sock:
            self.sock.close()
            self.sock = None

    def _send_command(self, *args):
        """Encode and send a RESP command, return raw response."""
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            s = str(arg)
            cmd += f"${len(s)}\r\n{s}\r\n"
        self.sock.sendall(cmd.encode("utf-8"))
        return self._read_response()

    def _read_response(self):
        """Read and parse a RESP response."""
        data = b""
        while True:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                # Simple heuristic: if we got data and no more is coming
                if len(chunk) < 4096:
                    break
            except socket.timeout:
                break
        return data.decode("utf-8", errors="replace")

    def ping(self):
        """Send PING command."""
        return self._send_command("PING")

    def graph_query(self, graph_name, cypher):
        """Execute a Cypher query on a named graph."""
        return self._send_command("GRAPH.QUERY", graph_name, cypher)

    def graph_list(self):
        """List all graphs."""
        return self._send_command("GRAPH.LIST")


def connect_with_retry(host, port, max_retries=3, delay=2.0):
    """Connect to server with retry logic."""
    client = SamyamaClient(host, port)
    for attempt in range(1, max_retries + 1):
        try:
            client.connect()
            return client
        except (ConnectionRefusedError, socket.timeout) as e:
            if attempt < max_retries:
                print(f"  Connection attempt {attempt}/{max_retries} failed: {e}")
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"  All {max_retries} connection attempts failed.")
                raise
    return None


def parse_resp_response(raw):
    """Parse a raw RESP response into a readable format."""
    lines = raw.strip().split("\r\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("+"):
            result.append(("simple", line[1:]))
        elif line.startswith("-"):
            result.append(("error", line[1:]))
        elif line.startswith(":"):
            result.append(("integer", int(line[1:])))
        elif line.startswith("$"):
            length = int(line[1:])
            if length == -1:
                result.append(("null", None))
            elif i + 1 < len(lines):
                result.append(("bulk", lines[i + 1]))
                i += 1
        elif line.startswith("*"):
            result.append(("array_header", int(line[1:])))
        i += 1
    return result


def main():
    print("=" * 60)
    print("  Samyama Graph Database - Python RESP Client Demo")
    print("=" * 60)

    host = "127.0.0.1"
    port = 6379

    print(f"\n  Server: {host}:{port}")
    print()

    # 1. Connect with retry
    print("[1] Connecting to Samyama...")
    try:
        client = connect_with_retry(host, port)
        print("  Connected successfully.")
    except Exception as e:
        print(f"  Failed to connect: {e}")
        print("  Make sure the server is running: cargo run -- --port 6379")
        sys.exit(1)

    # 2. Ping
    print("\n[2] PING")
    resp = client.ping()
    print(f"  Response: {resp.strip()}")
    client.close()

    # 3. Create nodes via Cypher
    print("\n[3] Creating Graph Data (Cypher)")
    queries = [
        ("CREATE (n:Company {name: 'Samyama AI', founded: 2024, domain: 'Graph Database'})",
         "Create company node"),
        ("CREATE (n:Engineer {name: 'Alice Chen', role: 'Principal Engineer', team: 'Core'})",
         "Create engineer Alice"),
        ("CREATE (n:Engineer {name: 'Bob Kumar', role: 'Staff Engineer', team: 'Query Engine'})",
         "Create engineer Bob"),
        ("CREATE (n:Engineer {name: 'Carol Zhang', role: 'Senior Engineer', team: 'Vector Search'})",
         "Create engineer Carol"),
        ("CREATE (n:Product {name: 'SamyamaDB', version: '0.5.0', language: 'Rust'})",
         "Create product node"),
    ]

    for cypher, description in queries:
        client = SamyamaClient(host, port)
        try:
            client.connect()
            resp = client.graph_query("demo", cypher)
            parsed = parse_resp_response(resp)
            status = "OK" if any(t == "simple" and v == "OK" for t, v in parsed) else resp.strip()[:40]
            print(f"  {description}: {status}")
        except Exception as e:
            print(f"  {description}: Error - {e}")
        finally:
            client.close()

    # 4. Query data
    print("\n[4] Querying Graph Data")
    read_queries = [
        ("MATCH (n:Company) RETURN n.name, n.domain",
         "Find all companies"),
        ("MATCH (n:Engineer) RETURN n.name, n.role, n.team",
         "Find all engineers"),
        ("MATCH (n:Product) RETURN n.name, n.version",
         "Find all products"),
        ("MATCH (n:Engineer) WHERE n.team = 'Core' RETURN n.name",
         "Find Core team members"),
    ]

    for cypher, description in read_queries:
        client = SamyamaClient(host, port)
        try:
            client.connect()
            start = time.time()
            resp = client.graph_query("demo", cypher)
            elapsed = (time.time() - start) * 1000
            print(f"\n  Query: {description}")
            print(f"  Cypher: {cypher}")
            print(f"  Response ({elapsed:.1f}ms): {resp.strip()[:120]}")
        except Exception as e:
            print(f"  {description}: Error - {e}")
        finally:
            client.close()

    # 5. Batch query execution
    print("\n\n[5] Batch Query Execution")
    batch_queries = [
        "MATCH (n:Engineer) RETURN n.name",
        "MATCH (n:Company) RETURN n.name",
        "MATCH (n:Product) RETURN n.version",
    ]

    print(f"  Executing {len(batch_queries)} queries in sequence...")
    total_time = 0
    for i, cypher in enumerate(batch_queries, 1):
        client = SamyamaClient(host, port)
        try:
            client.connect()
            start = time.time()
            resp = client.graph_query("demo", cypher)
            elapsed = (time.time() - start) * 1000
            total_time += elapsed
            print(f"  Query {i}: {elapsed:.1f}ms")
        except Exception as e:
            print(f"  Query {i}: Error - {e}")
        finally:
            client.close()
    print(f"  Total: {total_time:.1f}ms (avg: {total_time/len(batch_queries):.1f}ms)")

    # 6. Graph list
    print("\n[6] Listing Graphs")
    client = SamyamaClient(host, port)
    try:
        client.connect()
        resp = client.graph_list()
        print(f"  Response: {resp.strip()}")
    except Exception as e:
        print(f"  Error: {e}")
    finally:
        client.close()

    print("\n" + "=" * 60)
    print("  Demo Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
