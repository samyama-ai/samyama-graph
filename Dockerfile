# Build stage
FROM rust:1.75-bookworm AS builder

# Install build dependencies for RocksDB
RUN apt-get update && apt-get install -y \
    clang \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files first for caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "pub fn version() -> &'static str { \"0.1.0\" }" > src/lib.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release && rm -rf src

# Copy actual source code
COPY src ./src

# Touch main.rs to ensure it gets rebuilt
RUN touch src/main.rs

# Build the actual application
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

# Set environment variables
ENV RUST_LOG=info

# Run the server
CMD ["samyama"]
