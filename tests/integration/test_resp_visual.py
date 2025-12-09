#!/usr/bin/env python3
"""Visual demonstration of RESP protocol communication"""

import socket
import time

def send_and_show(sock, *args):
    """Send command and show both request and response"""
    # Build RESP command
    command = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode('utf-8')
        command += f"${len(arg_bytes)}\r\n{arg_bytes.decode()}\r\n"

    # Show what we're sending
    print("📤 Sending (RESP format):")
    print("  " + repr(command))
    print()

    # Send command
    sock.sendall(command.encode('utf-8'))
    time.sleep(0.1)

    # Read response
    response = b""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if response.endswith(b"\r\n"):
                break
        except socket.timeout:
            break

    # Show response
    print("📥 Received (RESP format):")
    print("  " + repr(response.decode('utf-8', errors='ignore')))
    print()

    # Parse and show human-readable
    parsed = parse_resp(response.decode('utf-8', errors='ignore'))
    print("🔍 Parsed response:")
    print(f"  {parsed}")
    print()
    print("=" * 80)
    print()

    return response

def parse_resp(resp):
    """Simple RESP parser for display"""
    if not resp:
        return "(empty)"

    first = resp[0]
    if first == '+':
        return f"Simple String: {resp[1:].strip()}"
    elif first == '-':
        return f"Error: {resp[1:].strip()}"
    elif first == ':':
        return f"Integer: {resp[1:].strip()}"
    elif first == '$':
        lines = resp.split('\r\n')
        if len(lines) >= 2:
            return f"Bulk String: {lines[1]}"
        return "Bulk String: (parsing error)"
    elif first == '*':
        lines = resp.split('\r\n')
        count = lines[0][1:]
        return f"Array with {count} elements"
    else:
        return f"Unknown type: {first}"

def main():
    print("=" * 80)
    print("SAMYAMA GRAPH DATABASE - RESP PROTOCOL DEMONSTRATION")
    print("=" * 80)
    print()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)

    try:
        sock.connect(('127.0.0.1', 6379))
        print("✅ Connected to Samyama at 127.0.0.1:6379")
        print()
        print("=" * 80)
        print()

        # Test 1
        print("TEST 1: Basic PING command")
        print("-" * 40)
        send_and_show(sock, "PING")

        # Test 2
        print("TEST 2: Query for all Person nodes")
        print("-" * 40)
        send_and_show(sock, "GRAPH.QUERY", "mygraph", "MATCH (n:Person) RETURN n")

        # Test 3
        print("TEST 3: Filtered query with WHERE clause")
        print("-" * 40)
        send_and_show(sock, "GRAPH.QUERY", "mygraph",
                     "MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age")

        # Test 4
        print("TEST 4: Edge traversal query")
        print("-" * 40)
        send_and_show(sock, "GRAPH.QUERY", "mygraph",
                     "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

        print("✅ All demonstrations completed!")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
