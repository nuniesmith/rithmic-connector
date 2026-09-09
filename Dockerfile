# =============================================================================
# FKS Rithmic Connector — Docker image (fks-state home)
#
# Self-contained multi-stage build for the `rithmic-connector` binary — the
# credential-gated, READ-ONLY Rithmic futures market-data sidecar. Moved here
# from the fks repo (crates/rithmic-connector + the Dockerfile that lived at
# infrastructure/docker/services/rithmic-connector/) — it is becoming the
# funded-side heart (ORB supervision, copy-trade fill source) and stays private.
#
# DOCTRINE: read-only. The image contains no order-entry path. At runtime the
# connector no-ops unless RITHMIC_ENABLED=true AND credentials are injected.
#
# Build context = the FKS-STATE REPO ROOT:
#
#   docker build -f crates/rithmic-connector/Dockerfile -t fks-rithmic-connector:latest .
#
# (Compose wiring is a later stage — this file only defines the image.)
#
# Builds against the crate's own nested [workspace] manifest
# (crates/rithmic-connector/Cargo.toml), mirroring the spawner pattern.
# =============================================================================

ARG RUST_VERSION=1.94.1
ARG BUILDER_BASE=debian:bookworm-slim
ARG RUNTIME_BASE=debian:bookworm-slim

FROM ${BUILDER_BASE} AS base

ARG RUST_VERSION=1.94.1

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# libssl-dev is required: rithmic-rs speaks TLS via native-tls (openssl).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates pkg-config libssl-dev g++ make git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --no-modify-path --profile minimal \
              --default-toolchain ${RUST_VERSION} && \
    chmod -R a+w ${RUSTUP_HOME} ${CARGO_HOME}

WORKDIR /build

# The build context is this repository, which contains only the connector, so
# the crate sits at the root. It used to live at crates/rithmic-connector/ in
# the private fks-state repo and was copied from there; extracting it to its own
# public repository flattened the path.
COPY . .

ENV CARGO_TARGET_DIR=/build/target

# The connector is a self-contained crate with its own nested [workspace]
# (crates/rithmic-connector/Cargo.toml) — NOT a member of any parent
# workspace. Point cargo at the crate's own manifest, mirroring spawner.
RUN cargo build --release --locked --bin rithmic-connector && \
    strip /build/target/release/rithmic-connector

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM ${RUNTIME_BASE}

ARG APP_USER=appuser
ARG SERVICE_PORT=9091

# libssl3 for the native-tls runtime linkage.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    ( apt-get install -y --no-install-recommends libssl3 2>/dev/null || \
      apt-get install -y --no-install-recommends libssl3t64 ) && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -u 1000 -m -s /bin/bash ${APP_USER}

WORKDIR /app

COPY --from=base /build/target/release/rithmic-connector /app/rithmic-connector

RUN chown ${APP_USER}:${APP_USER} /app

# /state holds the durable kill-switch sentinel and is a NAMED VOLUME at
# runtime. Create it (owned by the app user) in the IMAGE: Docker seeds an
# EMPTY named volume from the image path, ownership included, so the container
# can write there as uid 1000. Without this the volume is created root-owned
# and POST /kill fails with EACCES — the kill switch would appear present and
# then refuse to work, which is the worst possible state for a control the
# operator relies on to free their trading session.
RUN mkdir -p /state && chown ${APP_USER}:${APP_USER} /state

USER ${APP_USER}

# Read-only + gated OFF by default. The spawner secret store injects
# RITHMIC_USER / RITHMIC_PASSWORD / RITHMIC_SYSTEM_NAME at deploy time.
ENV RUST_LOG=info,rithmic_connector=debug \
    RITHMIC_ENABLED=false \
    RITHMIC_ENV=demo \
    RITHMIC_INSTRUMENT=MES \
    RITHMIC_EXCHANGE=CME \
    QUESTDB_ILP_HOST=fks_questdb \
    QUESTDB_ILP_PORT=9009 \
    RITHMIC_HEALTH_HOST=0.0.0.0 \
    RITHMIC_HEALTH_PORT=${SERVICE_PORT}

EXPOSE ${SERVICE_PORT}

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:${RITHMIC_HEALTH_PORT:-9091}/health || exit 1

CMD ["/app/rithmic-connector"]
