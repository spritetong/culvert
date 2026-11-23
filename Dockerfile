# syntax=docker/dockerfile:1

# ==============================================================================
# Culvert Multi-Arch Dockerfile
#
# Supported build modes:
#   1. 'source'   - Compiles binary from source code using official Rust image.
#                   (Default for local `docker build -t culvert .`)
#   2. 'prebuilt' - Ingests pre-compiled cross-target binaries from bin/${TARGETARCH}.
#                   (Used in CI for blazing-fast multi-arch manifests without QEMU)
# ==============================================================================

ARG BUILD_MODE=source

################################################################################
# Stage 1a: Compile binary from source (Local Build)
################################################################################
FROM rust:bookworm AS builder-source

WORKDIR /build
COPY . .

# Build culvert-cli in release mode
RUN cargo build --release -p culvert-cli && \
    mkdir -p /out && \
    cp target/release/culvert /out/culvert

################################################################################
# Stage 1b: Ingest pre-compiled binary (CI Build)
################################################################################
FROM debian:bookworm-slim AS builder-prebuilt

ARG TARGETARCH
WORKDIR /out

# Prebuilt binaries are placed in bin/amd64/culvert or bin/arm64/culvert
COPY bin/${TARGETARCH}/culvert /out/culvert

################################################################################
# Stage 2: Binary Provider Selector
################################################################################
FROM builder-${BUILD_MODE} AS binary-provider

################################################################################
# Stage 3: Minimal Production Runtime Image
################################################################################
FROM debian:bookworm-slim AS final

# Install dumb-init for clean PID 1 signal forwarding and zombie process reaping,
# ca-certificates for TLS verification, and libcap2-bin for privileged port binding.
RUN groupadd -g 1000 culvert && \
    useradd -u 1000 -g culvert -m -s /bin/bash culvert && \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates dumb-init libcap2-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /home/culvert

# Copy compiled binary from provider stage
COPY --from=binary-provider /out/culvert /usr/local/bin/culvert

# Grant CAP_NET_BIND_SERVICE to allow binding low ports (<1024) as unprivileged user
RUN chmod +x /usr/local/bin/culvert && \
    (setcap 'cap_net_bind_service=+ep' /usr/local/bin/culvert || true)

# Prepare config directory with culvert user ownership
RUN mkdir -p /etc/culvert && chown -R culvert:culvert /etc/culvert /home/culvert

ENV RUST_LOG="info"
VOLUME ["/etc/culvert"]

USER culvert

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/culvert"]
CMD ["--config", "/etc/culvert/config.yaml"]
