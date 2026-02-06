import socket
import time

def send_command(host, port, *args):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        
        # RESP serialization
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            cmd += f"${len(str(arg))}\r\n{arg}\r\n"
        
        s.sendall(cmd.encode('utf-8'))
        
        # Simple response reading (not a full RESP parser, just for demo)
        response = s.recv(4096).decode('utf-8', errors='ignore')
        s.close()
        return response
    except Exception as e:
        return f"Error: {e}"

def main():
    print("--- Samyama Python Client Demo ---")
    host = "127.0.0.1"
    port = 6379
    
    print(f"Connecting to {host}:{port}...")
    
    # 1. Ping
    print("\n[1] PING")
    res = send_command(host, port, "PING")
    print(f"Response: {res.strip()}")
    
    # 2. Create Data via OpenCypher
    print("\n[2] Creating Nodes (Cypher)")
    query_create = "CREATE (n:User {name: 'ClientUser', active: true})"
    print(f"Query: {query_create}")
    # GRAPH.QUERY is the command, "default" is the key/graph name, then the query
    res = send_command(host, port, "GRAPH.QUERY", "default", query_create)
    print(f"Response: {res.strip()}")
    
    # 3. Query Data
    print("\n[3] Querying Nodes")
    query_match = "MATCH (n:User) RETURN n.name, n.active"
    print(f"Query: {query_match}")
    res = send_command(host, port, "GRAPH.QUERY", "default", query_match)
    print(f"Response: {res.strip()}")

if __name__ == "__main__":
    main()
