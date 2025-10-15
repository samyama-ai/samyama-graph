#!/usr/bin/env python3
"""Test Samyama RESP server"""

import socket
import time

def send_command(sock, *args):
    """Send a RESP command"""
    command = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode('utf-8')
        command += f"${len(arg_bytes)}\r\n{arg_bytes.decode()}\r\n"

    sock.sendall(command.encode('utf-8'))

def read_response(sock):
    """Read RESP response"""
    response = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
        # Simple check if we got a complete response
        if response.endswith(b"\r\n"):
            break
    return response.decode('utf-8', errors='ignore')

def test_server():
    """Run tests against Samyama server"""
    print("🔗 Connecting to Samyama server at 127.0.0.1:6379...")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)

    try:
        sock.connect(('127.0.0.1', 6379))
        print("✅ Connected!\n")

        # Test 1: PING
        print("Test 1: PING")
        send_command(sock, "PING")
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 2: PING with message
        print("Test 2: PING Hello")
        send_command(sock, "PING", "Hello")
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 3: ECHO
        print("Test 3: ECHO 'Hello Samyama'")
        send_command(sock, "ECHO", "Hello Samyama")
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 4: INFO
        print("Test 4: INFO")
        send_command(sock, "INFO")
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 5: GRAPH.LIST
        print("Test 5: GRAPH.LIST")
        send_command(sock, "GRAPH.LIST")
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 6: GRAPH.QUERY - Simple match
        print("Test 6: GRAPH.QUERY mygraph 'MATCH (n:Person) RETURN n'")
        send_command(sock, "GRAPH.QUERY", "mygraph", "MATCH (n:Person) RETURN n")
        time.sleep(0.1)  # Give time for query to execute
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 7: GRAPH.QUERY - With WHERE clause
        print("Test 7: GRAPH.QUERY mygraph 'MATCH (n:Person) WHERE n.age > 25 RETURN n.name'")
        send_command(sock, "GRAPH.QUERY", "mygraph", "MATCH (n:Person) WHERE n.age > 25 RETURN n.name")
        time.sleep(0.1)
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        # Test 8: GRAPH.QUERY - Edge traversal
        print("Test 8: GRAPH.QUERY mygraph 'MATCH (a)-[:KNOWS]->(b) RETURN a, b'")
        send_command(sock, "GRAPH.QUERY", "mygraph", "MATCH (a)-[:KNOWS]->(b) RETURN a, b")
        time.sleep(0.1)
        response = read_response(sock)
        print(f"Response: {response.strip()}")
        print()

        print("✅ All tests completed!")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        sock.close()

if __name__ == "__main__":
    test_server()
