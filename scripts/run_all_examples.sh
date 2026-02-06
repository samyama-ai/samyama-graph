#!/bin/bash

# Configuration
SERVER_BIN="./target/release/samyama"
SERVER_PORT=6379
SERVER_HOST="127.0.0.1"
SERVER_PID=""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

function build_project() {
    if [ -f "$SERVER_BIN" ]; then
        read -p "Build found. Rebuild project? [y/N]: " REBUILD
        if [[ ! "$REBUILD" =~ ^[Yy]$ ]]; then
            echo -e "${GREEN}Skipping build.${NC}"
            return
        fi
    fi

    echo -e "${BLUE}🔨 Building Samyama Graph (Release)...${NC}"
    # Build the server binary
    cargo build --release --bin samyama
    # Build examples
    cargo build --release --examples
}

function start_server() {
    if [ -n "$SERVER_PID" ]; then
        echo -e "${YELLOW}⚠️  Server is already running (PID: $SERVER_PID)${NC}"
        return
    fi

    echo -e "${BLUE}🚀 Starting Samyama Graph Server...${NC}"
    # Ensure binary exists
    if [ ! -f "$SERVER_BIN" ]; then
        echo -e "${RED}❌ Server binary not found at $SERVER_BIN${NC}"
        exit 1
    fi

    $SERVER_BIN --port $SERVER_PORT > server.log 2>&1 &
    SERVER_PID=$!
    echo "   Server PID: $SERVER_PID"
    echo "   Waiting for server to be ready..."
    sleep 3
}

function stop_server() {
    if [ -n "$SERVER_PID" ]; then
        echo -e "${YELLOW}🛑 Stopping Server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID
        wait $SERVER_PID 2>/dev/null
        SERVER_PID=""
        echo "   Server stopped."
    fi
}

function run_rust_example() {
    EXAMPLE_NAME=$1
    DESCRIPTION=$2
    echo -e "${BLUE}▶️  Running $DESCRIPTION ($EXAMPLE_NAME)...${NC}"
    
    # Check if data prerequisites exist for specific demos
    if [[ "$EXAMPLE_NAME" == "banking_demo" && ! -d "docs/banking/data" ]]; then
        echo -e "${YELLOW}⚠️  Banking data not generated. Running with in-memory sample.${NC}"
    fi

    ./target/release/examples/$EXAMPLE_NAME
    read -p "Press Enter to return to menu..."
}

function run_python_client() {
    echo -e "${BLUE}🐍 Running Python Client Demo...${NC}"
    if command -v python3 &>/dev/null; then
        python3 examples/simple_client_demo.py
    else
        echo -e "${RED}❌ python3 not found.${NC}"
    fi
    read -p "Press Enter to return to menu..."
}

function cleanup() {
    stop_server
    exit 0
}

# Trap Ctrl+C
trap cleanup SIGINT

# Main Execution
build_project
start_server

while true; do
    clear
    echo -e "${GREEN}==============================================${NC}"
    echo -e "${GREEN}   Samyama Graph - Examples Runner${NC}"
    echo -e "${GREEN}==============================================${NC}"
    echo -e "${YELLOW}--- Core Demos ---${NC}"
    echo "1. Banking Demo (Multi-tenant, Fraud Detection)"
    echo "2. Supply Chain Demo (Agents, Optimization, Visualizer)"
    echo "3. Healthcare Demo (Clinical Trials, Knowledge Graph)"
    echo "4. Graph RAG Demo (Vector Search + Graph)"
    echo -e "${YELLOW}--- AI & Agents ---${NC}"
    echo "5. Agent Demo (LLM Tools)"
    echo "6. NLQ Demo (Natural Language Query)"
    echo "7. Auto-Embed Demo (Vector Embeddings)"
    echo -e "${YELLOW}--- Infrastructure ---${NC}"
    echo "8. Cluster Demo (Raft Consensus)"
    echo "9. Persistence Demo (Storage Engine)"
    echo "10. Optimization Demo (Solvers)"
    echo "11. Visualizer Demo (Standalone)"
    echo -e "${YELLOW}--- Benchmarks ---${NC}"
    echo "12. Full Benchmark Suite"
    echo "13. Vector Search Benchmark"
    echo "14. MVCC Benchmark"
    echo "15. Graph Optimization Benchmark"
    echo -e "${YELLOW}--- Clients ---${NC}"
    echo "16. Python Client Demo (Connects to Server)"
    echo -e "${YELLOW}--- Server ---${NC}"
    echo "17. View Server Logs"
    echo "q. Quit"
    echo -e "${GREEN}==============================================${NC}"
    read -p "Select an option: " choice

    case $choice in
        1) run_rust_example "banking_demo" "Banking Demo" ;;
        2) run_rust_example "supply_chain_demo" "Supply Chain Demo" ;;
        3) run_rust_example "healthcare_demo" "Healthcare Demo" ;;
        4) run_rust_example "graph_rag_demo" "Graph RAG Demo" ;;
        5) run_rust_example "agent_demo" "Agent Demo" ;;
        6) run_rust_example "nlq_demo" "NLQ Demo" ;;
        7) run_rust_example "auto_embed_demo" "Auto-Embed Demo" ;;
        8) run_rust_example "cluster_demo" "Cluster Demo" ;;
        9) run_rust_example "persistence_demo" "Persistence Demo" ;;
        10) run_rust_example "optimization_demo" "Optimization Demo" ;;
        11) run_rust_example "visualize_demo" "Visualizer Demo" ;;
        12) run_rust_example "full_benchmark" "Full Benchmark" ;;
        13) run_rust_example "vector_benchmark" "Vector Benchmark" ;;
        14) run_rust_example "mvcc_benchmark" "MVCC Benchmark" ;;
        15) run_rust_example "graph_optimization_benchmark" "Graph Optimization Benchmark" ;;
        16) run_python_client ;;
        17) echo "--- Last 20 lines of server.log ---"; tail -n 20 server.log; read -p "Press Enter..." ;;
        q) cleanup ;;
        *) echo "Invalid option"; sleep 1 ;;
    esac
done