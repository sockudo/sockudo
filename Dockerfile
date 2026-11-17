# =============================================================================
# Sockudo WebSocket Server - Multi-stage Docker Build
#
# Operator dashboard images are built separately from dashboard/Dockerfile.
# =============================================================================

# -----------------------------------------------------------------------------
# Build Stage: Compile the Rust application
# -----------------------------------------------------------------------------
FROM rust:1.99-bookworm AS builder

# Install system dependencies required for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libpq-dev \
    libmariadb-dev \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Create app user and directory
RUN groupadd -r sockudo && useradd -r -g sockudo sockudo

# Set the working directory
WORKDIR /app

# Preserve repo rustflags such as `--cfg tokio_unstable` inside Docker builds.
COPY .cargo/ ./.cargo/

# Copy the workspace and the real sources. Every workspace member lives under
# crates/ or benches/ai (see the root Cargo.toml).
COPY Cargo.toml ./
COPY Cargo.loc[k] ./
COPY crates/ ./crates/
COPY benches/ai/ ./benches/ai/

# The release image runs the in-process push pipeline workers. Custom worker/API
# images can still override this build argument with a narrower feature set.
ARG SOCKUDO_FEATURES=full,monolith
# The full feature graph exceeds the standard ARM64 runner's memory with fat LTO.
# Thin LTO keeps cross-architecture images optimized while bounding link memory.
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=8
ARG CARGO_BUILD_JOBS=2
ENV CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO}
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${CARGO_PROFILE_RELEASE_CODEGEN_UNITS}
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

# Set jemalloc's page size to 4KB (use 16 for 64KB-page hosts)
ARG JEMALLOC_SYS_WITH_LG_PAGE=12
ENV JEMALLOC_SYS_WITH_LG_PAGE=${JEMALLOC_SYS_WITH_LG_PAGE}

# Build the release binary from the real sources in one pass. There is no stub
# source or dependency warm-up layer, so cargo always compiles the actual code and
# the image cannot ship a placeholder binary.
RUN cargo build -p sockudo --release --features "${SOCKUDO_FEATURES}"

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
