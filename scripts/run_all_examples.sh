#!/bin/bash

# Configuration
SERVER_BIN="./target/release/samyama"
SERVER_PORT=6379
SERVER_HOST="127.0.0.1"
SERVER_PID=""

function build_project() {
    echo "🔨 Building Samyama Graph (Release)..."
    # Build the server binary
    cargo build --release --bin samyama
    # Build examples
    cargo build --release --examples
}

function start_server() {
    if [ -n "$SERVER_PID" ]; then
        echo "⚠️  Server is already running (PID: $SERVER_PID)"
        return
    fi

    echo "🚀 Starting Samyama Graph Server..."
    # Ensure binary exists
    if [ ! -f "$SERVER_BIN" ]; then
        echo "❌ Server binary not found at $SERVER_BIN"
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
        echo "🛑 Stopping Server (PID: $SERVER_PID)..."
        kill $SERVER_PID
        wait $SERVER_PID 2>/dev/null
        SERVER_PID=""
        echo "   Server stopped."
    fi
}

function run_banking_demo() {
    echo "🏦 Running Banking Demo (Embedded Mode)..."
    # Check if data exists, if not warn
    if [ ! -d "docs/banking/data" ]; then
        echo "⚠️  Banking data not generated. The demo will run with small in-memory sample."
    fi
    ./target/release/examples/banking_demo
    read -p "Press Enter to continue..."
}

function run_supply_chain_demo() {
    echo "🏭 Running Supply Chain Demo (Embedded Mode)..."
    ./target/release/examples/supply_chain_demo
    read -p "Press Enter to continue..."
}

function run_client_demo() {
    echo "🐍 Running Python Client Demo..."
    # Check for python3
    if command -v python3 &>/dev/null; then
        python3 examples/simple_client_demo.py
    else
        echo "❌ python3 not found."
    fi
    read -p "Press Enter to continue..."
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
    echo "=============================================="
    echo "   Samyama Graph - Examples Runner"
    echo "=============================================="
    echo "1. Banking Demo (Embedded)"
    echo "2. Supply Chain Demo (Embedded)"
    echo "3. Python Client Demo (Connects to Server)"
    echo "4. View Server Logs"
    echo "q. Quit"
    echo "=============================================="
    read -p "Select an option: " choice

    case $choice in
        1) run_banking_demo ;;
        2) run_supply_chain_demo ;;
        3) run_client_demo ;;
        4) echo "--- Last 20 lines of server.log ---"; tail -n 20 server.log; read -p "Press Enter..." ;;
        q) cleanup ;;
        *) echo "Invalid option"; sleep 1 ;;
    esac
done
