# syntax=docker/dockerfile:1

# ==============================================================================
# Culvert Multi-Arch Dockerfile (Alpine-based, ultra-lightweight)
#
# Supported build modes:
#   1. 'source'   - Compiles binary from source code using official rust:alpine.
#                   (Default for local `docker build -t culvert .`)
#   2. 'prebuilt' - Ingests pre-compiled cross-target musl binaries from bin/${TARGETARCH}.
#                   (Used in CI for blazing-fast multi-arch manifests without QEMU)
# ==============================================================================

ARG BUILD_MODE=source

################################################################################
# Stage 1a: Compile binary from source (Local Build)
################################################################################
FROM rust:alpine AS builder-source

WORKDIR /build
COPY . .

# Install build dependencies for C-binding crates (ring, mimalloc)
RUN apk add --no-cache build-base && \
    cargo build --release -p culvert-cli && \
    mkdir -p /out && \
    cp target/release/culvert /out/culvert

################################################################################
# Stage 1b: Ingest pre-compiled binary (CI Build)
################################################################################
FROM alpine:latest AS builder-prebuilt

ARG TARGETARCH
WORKDIR /out

# Prebuilt musl binaries are placed in bin/amd64/culvert or bin/arm64/culvert
COPY bin/${TARGETARCH}/culvert /out/culvert

################################################################################
# Stage 2: Binary Provider Selector
################################################################################
FROM builder-${BUILD_MODE} AS binary-provider

################################################################################
# Stage 3: Minimal Production Runtime Image (Alpine Linux)
################################################################################
FROM alpine:latest AS final

# 1. Create unprivileged culvert user (UID/GID 1000)
# 2. Install dumb-init, ca-certificates, tzdata, and libcap
# 3. Pre-create /etc/timezone and /etc/localtime as regular files so that host
#    bind-mounts (-v /etc/timezone:/etc/timezone) never create directories
RUN addgroup -g 1000 culvert && \
    adduser -u 1000 -G culvert -s /bin/sh -D culvert && \
    apk add --no-cache ca-certificates dumb-init tzdata libcap && \
    mkdir -p /etc/culvert /home/culvert && \
    chown -R culvert:culvert /etc/culvert /home/culvert

WORKDIR /etc/culvert

# Copy compiled binary from provider stage
COPY --from=binary-provider /out/culvert /usr/local/bin/culvert

# Grant CAP_NET_BIND_SERVICE to allow binding low ports (<1024) as unprivileged user
RUN chmod +x /usr/local/bin/culvert && \
    (setcap 'cap_net_bind_service=+ep' /usr/local/bin/culvert || true)

VOLUME ["/etc/culvert"]

USER culvert

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/culvert"]
CMD ["--config", "config.yaml"]
