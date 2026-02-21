# Build stage
FROM rust:1.83-bookworm AS builder

# Install build dependencies for RocksDB
RUN apt-get update && apt-get install -y \
    clang \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all source files
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY benches ./benches
COPY examples ./examples

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/samyama /usr/local/bin/samyama

# Create data directory for persistence
RUN mkdir -p /data

# Expose RESP protocol port
EXPOSE 6379
# Expose Web Visualizer port
EXPOSE 8080

# Set environment variables
ENV RUST_LOG=info

# Healthcheck via RESP PING
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD echo "PING" | nc -w1 localhost 6379 | grep -q PONG || exit 1

# Run the server, binding to 0.0.0.0 so it's reachable from outside the container
CMD ["samyama", "--host", "0.0.0.0"]
