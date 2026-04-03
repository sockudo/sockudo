# =============================================================================
# Sockudo WebSocket Server - Multi-stage Docker Build
# =============================================================================

# -----------------------------------------------------------------------------
# Build Stage: Compile the Rust application
# -----------------------------------------------------------------------------
FROM rust:1.93-bookworm AS builder

# Install system dependencies required for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libpq-dev \
    libmariadb-dev \
    cmake \
    protobuf-compiler \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Create app user and directory
RUN groupadd -r sockudo && useradd -r -g sockudo sockudo

# Set the working directory
WORKDIR /app

# Preserve repo rustflags such as `--cfg tokio_unstable` inside Docker builds.
COPY .cargo/ ./.cargo/

# Copy workspace Cargo.toml and lockfile first for dependency caching
COPY Cargo.toml ./
COPY Cargo.loc[k] ./

# Copy all crate Cargo.toml files to establish workspace structure
COPY crates/sockudo-protocol/Cargo.toml crates/sockudo-protocol/Cargo.toml
COPY crates/sockudo-filter/Cargo.toml crates/sockudo-filter/Cargo.toml
COPY crates/sockudo-core/Cargo.toml crates/sockudo-core/Cargo.toml
COPY crates/sockudo-app/Cargo.toml crates/sockudo-app/Cargo.toml
COPY crates/sockudo-cache/Cargo.toml crates/sockudo-cache/Cargo.toml
COPY crates/sockudo-queue/Cargo.toml crates/sockudo-queue/Cargo.toml
COPY crates/sockudo-rate-limiter/Cargo.toml crates/sockudo-rate-limiter/Cargo.toml
COPY crates/sockudo-metrics/Cargo.toml crates/sockudo-metrics/Cargo.toml
COPY crates/sockudo-webhook/Cargo.toml crates/sockudo-webhook/Cargo.toml
COPY crates/sockudo-delta/Cargo.toml crates/sockudo-delta/Cargo.toml
COPY crates/sockudo-adapter/Cargo.toml crates/sockudo-adapter/Cargo.toml
COPY crates/sockudo-server/Cargo.toml crates/sockudo-server/Cargo.toml

# Create dummy lib.rs / main.rs for each crate so cargo can resolve the workspace
# and build dependencies without the real source code
RUN for dir in protocol filter core app cache queue rate-limiter metrics webhook delta adapter; do \
        mkdir -p crates/sockudo-$dir/src && \
        echo "// dummy" > crates/sockudo-$dir/src/lib.rs; \
    done && \
    mkdir -p crates/sockudo-server/src && \
    echo "fn main() {}" > crates/sockudo-server/src/main.rs

# Generate lockfile if missing
RUN test -f Cargo.lock || cargo generate-lockfile

# Build dependencies only (this layer is cached unless Cargo.toml files change)
RUN cargo build -p sockudo --release --features full || true
# Clean up dummy sources but keep compiled deps
RUN for dir in protocol filter core app cache queue rate-limiter metrics webhook delta adapter server; do \
        rm -rf crates/sockudo-$dir/src; \
    done && \
    rm -f target/release/deps/sockudo* target/release/deps/libsockudo* target/release/sockudo

# Copy actual source code
COPY crates/ ./crates/

# Build the real binary
RUN cargo build -p sockudo --release --features full

# Strip the binary to reduce size
RUN strip target/release/sockudo

# -----------------------------------------------------------------------------
# Runtime Stage: Create minimal runtime image
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create app user and directories
RUN groupadd -r sockudo && useradd -r -g sockudo sockudo \
    && mkdir -p /app/config /app/logs /app/ssl \
    && chown -R sockudo:sockudo /app

# Set working directory
WORKDIR /app

# Copy the compiled binary from builder stage
COPY --from=builder /app/target/release/sockudo /usr/local/bin/sockudo
RUN chmod +x /usr/local/bin/sockudo

# Copy configuration files (if they exist)
COPY --chown=sockudo:sockudo config/ ./config/

# Create default configuration if it doesn't exist
RUN if [ ! -f ./config/config.json ]; then \
    echo '{}' > ./config/config.json && \
    chown sockudo:sockudo ./config/config.json; \
    fi

# Switch to non-root user
USER sockudo

# Set environment variables with defaults
ENV CONFIG_FILE=/app/config/config.json
ENV HOST=0.0.0.0
ENV PORT=6001
ENV METRICS_PORT=9601
# Path for health check, you can change this to match your app's health endpoint e.g. "up/my-app-id"
# Defaults to "up" which is a common health check endpoint
ENV HEALTHCHECK_PATH=up

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${PORT}/${HEALTHCHECK_PATH} || exit 1

# Expose ports
EXPOSE 6001 9601

# Default command
CMD ["sockudo", "--config", "/app/config/config.json"]
