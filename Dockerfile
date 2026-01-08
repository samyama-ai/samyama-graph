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
ENV BIND_ADDRESS=0.0.0.0

# Run the server
CMD ["samyama"]
