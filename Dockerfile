FROM ubuntu:24.04 AS chef

ARG RUST_VERSION=1.98.0
ARG CARGO_CHEF_VERSION=0.1.77
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential ca-certificates clang cmake curl libclang-dev \
      libcypher-parser-dev libffi-dev libgraphblas-dev pkg-config && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" && \
    cargo install --locked --version "${CARGO_CHEF_VERSION}" cargo-chef && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /workspace/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json \
      --features server-runtime,indexer-runtime,experimental-cypher-engine,otlp \
      --bin graph-node --bin graph-indexer

COPY . .

# The commit is passed in rather than read from the tree: `.dockerignore`
# excludes `.git`, so `crates/telemetry/build.rs` has no repository to
# interrogate here and would stamp `unknown` into every image. CI supplies both
# from `container.yml`/`release.yml`; a local `docker build` without them
# produces an image that says so on its first log line.
#
# Keep these below the dependency-only `cargo chef cook` layer: the build stamp
# changes every commit, but it must invalidate only HydraDB's own compilation,
# not the locked third-party dependency graph.
ARG GIT_SHA=""
ARG GIT_BRANCH=""
ENV GIT_SHA=${GIT_SHA} \
    GIT_BRANCH=${GIT_BRANCH}

# `otlp` is off by default in Cargo.toml, and every OTLP path — the trace
# bridge, the log appender, the observable-counter registration — compiles to an
# inert stub without it. Omitting it here produced an image whose OTLP export
# was absent rather than misconfigured: no error, no endpoint, nothing on the
# wire. It must stay on this line for OTEL_EXPORTER_OTLP_ENDPOINT to mean
# anything at runtime.
RUN cargo build --locked --release \
      --features server-runtime,indexer-runtime,experimental-cypher-engine,otlp \
      --bin graph-node --bin graph-indexer && \
    strip target/release/graph-node target/release/graph-indexer

FROM ubuntu:24.04 AS runtime-base

ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libcypher-parser-dev libgraphblas-dev && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 graph && \
    useradd --uid 10001 --gid graph --no-create-home --shell /usr/sbin/nologin graph && \
    mkdir -p /var/cache/slatedb /tmp/graph && \
    chown -R graph:graph /var/cache/slatedb /tmp/graph

FROM runtime-base AS runtime

# The same value the binaries carry, in the place `docker inspect` and every
# registry UI already look for it. Redundant on purpose: a label answers
# "what is in this image" without running it, which is the only question you
# can ask about an image that will not start.
ARG GIT_SHA=""
LABEL org.opencontainers.image.revision="${GIT_SHA}"

COPY --from=builder /workspace/target/release/graph-node /usr/local/bin/graph-node
COPY --from=builder /workspace/target/release/graph-indexer /usr/local/bin/graph-indexer

USER 10001:10001
EXPOSE 7687 8443 9090 9443
ENTRYPOINT ["/usr/local/bin/graph-node"]
