# syntax=docker/dockerfile:1
# Build stage
FROM rust:1.85-bookworm AS builder

# Install build dependencies for RocksDB
RUN apt-get update && apt-get install -y \
    clang \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all workspace source files
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY cli ./cli
COPY benches ./benches
COPY examples ./examples

# Build the application — cache mounts keep the Cargo registry and build
# artifacts across rebuilds so only changed crates recompile (~2 min vs ~15 min).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/samyama /samyama-bin

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder (extracted outside the cache-mounted target dir)
COPY --from=builder /samyama-bin /usr/local/bin/samyama

# Create data directory for persistence
RUN mkdir -p /data

# Expose RESP protocol port
EXPOSE 6379
# Expose Web Visualizer port
EXPOSE 8080

# Set environment variables
ENV RUST_LOG=info

# Healthcheck via HTTP status endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8080/api/status || exit 1

# Run the server, binding to 0.0.0.0 so it's reachable from outside the container
CMD ["samyama", "--host", "0.0.0.0"]
