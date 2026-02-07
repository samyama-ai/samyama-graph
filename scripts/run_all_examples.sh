#!/bin/bash

# Configuration
SERVER_BIN="./target/release/samyama"
SERVER_PORT=6379
SERVER_HOST="127.0.0.1"
SERVER_PID=""
BATCH_MODE=false
TOTAL_TIME=0
EXAMPLE_RESULTS=()

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --batch) BATCH_MODE=true ;;
        --port) SERVER_PORT="$2"; shift ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --batch    Run all examples non-interactively (for CI/CD)"
            echo "  --port N   Use port N for the server (default: 6379)"
            echo "  --help     Show this help message"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

function build_project() {
    if [ "$BATCH_MODE" = true ]; then
        echo -e "${BLUE}Building Samyama Graph (Release)...${NC}"
        cargo build --release --bin samyama 2>&1
        cargo build --release --examples 2>&1
        return
    fi

    if [ -f "$SERVER_BIN" ]; then
        read -p "Build found. Rebuild project? [y/N]: " REBUILD
        if [[ "$REBUILD" =~ ^[Yy]$ ]]; then
            echo -e "${BLUE}Building Samyama Graph (Release)...${NC}"
            cargo build --release --bin samyama
            cargo build --release --examples
        else
            echo -e "${GREEN}Skipping build.${NC}"
        fi
    else
        echo -e "${BLUE}Building Samyama Graph (Release)...${NC}"
        cargo build --release --bin samyama
        cargo build --release --examples
    fi
}

function cleanup_data() {
    rm -rf banking_data demo_data supply_chain_data persistence_data 2>/dev/null
}

function start_server() {
    if [ -n "$SERVER_PID" ]; then
        if ps -p $SERVER_PID > /dev/null 2>&1; then
            return
        else
            SERVER_PID=""
        fi
    fi

    echo -e "${BLUE}Starting Samyama Graph Server...${NC}"
    if [ ! -f "$SERVER_BIN" ]; then
        echo -e "${RED}Server binary not found at $SERVER_BIN${NC}"
        exit 1
    fi

    $SERVER_BIN --port $SERVER_PORT > server.log 2>&1 &
    SERVER_PID=$!
    echo "   Server PID: $SERVER_PID"
    sleep 3
}

function stop_server() {
    if [ -n "$SERVER_PID" ]; then
        echo -e "${YELLOW}Stopping Server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null
        wait $SERVER_PID 2>/dev/null
        SERVER_PID=""
    fi
}

function reset_environment() {
    stop_server
    cleanup_data
    start_server
}

function run_rust_example() {
    EXAMPLE_NAME=$1
    DESCRIPTION=$2

    cleanup_data

    echo -e "${BLUE}Running $DESCRIPTION ($EXAMPLE_NAME)...${NC}"

    if [[ "$EXAMPLE_NAME" == "banking_demo" && ! -d "docs/banking/data" ]]; then
        echo -e "${YELLOW}Banking data not generated. Running with inline sample data.${NC}"
    fi

    local START_TIME=$SECONDS
    ./target/release/examples/$EXAMPLE_NAME
    local EXIT_CODE=$?
    local ELAPSED=$(( SECONDS - START_TIME ))

    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}$DESCRIPTION completed in ${ELAPSED}s${NC}"
        EXAMPLE_RESULTS+=("PASS  ${ELAPSED}s  $DESCRIPTION")
    else
        echo -e "${RED}$DESCRIPTION FAILED (exit code: $EXIT_CODE) after ${ELAPSED}s${NC}"
        EXAMPLE_RESULTS+=("FAIL  ${ELAPSED}s  $DESCRIPTION")
    fi

    TOTAL_TIME=$(( TOTAL_TIME + ELAPSED ))
    cleanup_data

    if [ "$BATCH_MODE" = false ]; then
        read -p "Press Enter to return to menu..."
    fi
}

function run_python_client() {
    echo -e "${BLUE}Running Python Client Demo...${NC}"
    if command -v python3 &>/dev/null; then
        start_server

        local START_TIME=$SECONDS
        python3 examples/simple_client_demo.py
        local EXIT_CODE=$?
        local ELAPSED=$(( SECONDS - START_TIME ))

        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}Python Client Demo completed in ${ELAPSED}s${NC}"
            EXAMPLE_RESULTS+=("PASS  ${ELAPSED}s  Python Client Demo")
        else
            echo -e "${RED}Python Client Demo FAILED after ${ELAPSED}s${NC}"
            EXAMPLE_RESULTS+=("FAIL  ${ELAPSED}s  Python Client Demo")
        fi
        TOTAL_TIME=$(( TOTAL_TIME + ELAPSED ))
    else
        echo -e "${RED}python3 not found, skipping.${NC}"
        EXAMPLE_RESULTS+=("SKIP  0s    Python Client Demo (python3 not found)")
    fi

    if [ "$BATCH_MODE" = false ]; then
        read -p "Press Enter to return to menu..."
    fi
}

function print_summary() {
    echo ""
    echo -e "${GREEN}=============================================${NC}"
    echo -e "${GREEN}   Example Run Summary${NC}"
    echo -e "${GREEN}=============================================${NC}"
    echo ""
    printf "  %-6s %-6s %s\n" "Status" "Time" "Example"
    printf "  %-6s %-6s %s\n" "------" "----" "-------"
    for result in "${EXAMPLE_RESULTS[@]}"; do
        local status=$(echo "$result" | awk '{print $1}')
        local time=$(echo "$result" | awk '{print $2}')
        local name=$(echo "$result" | cut -d' ' -f3-)
        if [[ "$status" == "PASS" ]]; then
            printf "  ${GREEN}%-6s${NC} %-6s %s\n" "$status" "$time" "$name"
        elif [[ "$status" == "FAIL" ]]; then
            printf "  ${RED}%-6s${NC} %-6s %s\n" "$status" "$time" "$name"
        else
            printf "  ${YELLOW}%-6s${NC} %-6s %s\n" "$status" "$time" "$name"
        fi
    done
    echo ""
    echo -e "  Total time: ${CYAN}${TOTAL_TIME}s${NC}"
    echo -e "${GREEN}=============================================${NC}"
}

function run_all_batch() {
    echo -e "${GREEN}=============================================${NC}"
    echo -e "${GREEN}   Samyama Graph - Batch Example Runner${NC}"
    echo -e "${GREEN}=============================================${NC}"
    echo ""

    # Enterprise Scenarios
    echo -e "${YELLOW}--- Enterprise Scenarios ---${NC}"
    run_rust_example "enterprise_soc_demo" "Enterprise SOC Demo"
    run_rust_example "clinical_trials_demo" "Clinical Trials Demo"
    run_rust_example "banking_demo" "Banking Demo"
    run_rust_example "supply_chain_demo" "Supply Chain Demo"

    # Knowledge & AI
    echo -e "${YELLOW}--- Knowledge & AI ---${NC}"
    run_rust_example "knowledge_graph_demo" "Knowledge Graph Demo"
    run_rust_example "social_network_demo" "Social Network Demo"

    # Core Infrastructure
    echo -e "${YELLOW}--- Core Infrastructure ---${NC}"
    run_rust_example "cluster_demo" "Cluster Demo"
    run_rust_example "persistence_demo" "Persistence Demo"

    # Benchmarks
    echo -e "${YELLOW}--- Benchmarks ---${NC}"
    run_rust_example "full_benchmark" "Full Benchmark Suite"
    run_rust_example "vector_benchmark" "Vector Search Benchmark"
    run_rust_example "mvcc_benchmark" "MVCC Benchmark"
    run_rust_example "graph_optimization_benchmark" "Graph Optimization Benchmark"

    # Python Client (requires server)
    echo -e "${YELLOW}--- Connectivity ---${NC}"
    run_python_client

    print_summary
}

function cleanup_exit() {
    stop_server
    cleanup_data
    if [ ${#EXAMPLE_RESULTS[@]} -gt 0 ]; then
        print_summary
    fi
    exit 0
}

# Trap Ctrl+C
trap cleanup_exit SIGINT

# Main Execution
build_project

if [ "$BATCH_MODE" = true ]; then
    start_server
    run_all_batch
    cleanup_exit
fi

# Interactive menu mode
start_server

while true; do
    clear
    echo -e "${GREEN}==============================================${NC}"
    echo -e "${GREEN}   Samyama Graph - Examples Runner${NC}"
    echo -e "${GREEN}==============================================${NC}"
    echo ""
    echo -e "${YELLOW}--- Enterprise Scenarios ---${NC}"
    echo "1.  Enterprise SOC Demo (APT Investigation)"
    echo "2.  Clinical Trials Demo (Pharma R&D)"
    echo "3.  Banking Demo (Multi-tenant Fraud Detection)"
    echo "4.  Supply Chain Demo (Global Logistics)"
    echo ""
    echo -e "${YELLOW}--- Knowledge & AI ---${NC}"
    echo "5.  Knowledge Graph Demo (Enterprise RAG)"
    echo "6.  Social Network Demo (Community Analysis)"
    echo ""
    echo -e "${YELLOW}--- Core Infrastructure ---${NC}"
    echo "7.  Cluster Demo (Raft HA)"
    echo "8.  Persistence Demo (Multi-tenant Storage)"
    echo ""
    echo -e "${YELLOW}--- Benchmarks ---${NC}"
    echo "9.  Full Benchmark Suite"
    echo "10. Vector Search Benchmark"
    echo "11. MVCC Benchmark"
    echo "12. Graph Optimization Benchmark"
    echo ""
    echo -e "${YELLOW}--- Connectivity ---${NC}"
    echo "13. Python Client Demo"
    echo ""
    echo -e "${YELLOW}--- System ---${NC}"
    echo "14. Reset Database (Restart Server & Clean Data)"
    echo "15. View Server Logs"
    echo "a.  Run All Examples (batch)"
    echo "q.  Quit"
    echo -e "${GREEN}==============================================${NC}"
    read -p "Select an option: " choice

    case $choice in
        1) run_rust_example "enterprise_soc_demo" "Enterprise SOC Demo" ;;
        2) run_rust_example "clinical_trials_demo" "Clinical Trials Demo" ;;
        3) run_rust_example "banking_demo" "Banking Demo" ;;
        4) run_rust_example "supply_chain_demo" "Supply Chain Demo" ;;
        5) run_rust_example "knowledge_graph_demo" "Knowledge Graph Demo" ;;
        6) run_rust_example "social_network_demo" "Social Network Demo" ;;
        7) run_rust_example "cluster_demo" "Cluster Demo" ;;
        8) run_rust_example "persistence_demo" "Persistence Demo" ;;
        9) run_rust_example "full_benchmark" "Full Benchmark Suite" ;;
        10) run_rust_example "vector_benchmark" "Vector Search Benchmark" ;;
        11) run_rust_example "mvcc_benchmark" "MVCC Benchmark" ;;
        12) run_rust_example "graph_optimization_benchmark" "Graph Optimization Benchmark" ;;
        13) run_python_client ;;
        14) reset_environment; read -p "Environment Reset. Press Enter..." ;;
        15) echo "--- Last 30 lines of server.log ---"; tail -n 30 server.log; read -p "Press Enter..." ;;
        a) run_all_batch ;;
        q) cleanup_exit ;;
        *) echo "Invalid option"; sleep 1 ;;
    esac
done
