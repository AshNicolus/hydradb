set shell := ["bash", "-eu", "-c"]

# The build environment every recipe below needs, so that `just <recipe>` works
# on a clean checkout without anyone rediscovering these three by bisecting a
# link error. An already-exported value always wins.
#
# Homebrew's prefix is macOS only: `opencypher` (and therefore `server-runtime`)
# runs bindgen against libcypher-parser, and `graphblas` links libgraphblas.
# Neither is on the default search path there.
brew_prefix := if os() == "macos" { shell("brew --prefix 2>/dev/null || echo /opt/homebrew") } else { "" }
export BINDGEN_EXTRA_CLANG_ARGS := env_var_or_default("BINDGEN_EXTRA_CLANG_ARGS", if os() == "macos" { "-I" + brew_prefix + "/include" } else { "" })
export LIBRARY_PATH := env_var_or_default("LIBRARY_PATH", if os() == "macos" { brew_prefix + "/lib" } else { "" })

# OpenCypher's async query futures exceed the 2 MiB default thread stack. Keep
# enough headroom across compiler profiles and platforms so examples and the
# binaries run rather than aborting with SIGABRT.
export RUST_MIN_STACK := env_var_or_default("RUST_MIN_STACK", "33554432")

# Show available recipes.
default:
    @just --list

# Show available recipes.
help:
    @just --list

# Format Rust code.
fmt:
    cargo fmt --all

# Check Rust formatting.
fmt-check:
    cargo fmt --all --check

# The root package is linted once per shipped feature combination, every one
# `--all-targets ... -D warnings`. None subsumes another: a lint only fires
# inside the cfg arms its feature set compiles, so the `cfg(not(feature = ...))`
# arms are visible only to the default run and the Bolt and HTTP paths only to
# the last three. `--all-targets` covers the examples too, so each clippy recipe
# strictly subsumes the same-feature `check`/`check-examples*` recipe below.
# Lint the default feature set.
clippy:
    cargo clippy --locked --all-targets -- -D warnings

# Lint the feature-gated hard-fence chaos harness.
clippy-chaos:
    cargo clippy --locked --all-targets --features chaos-harness -- -D warnings

# Lint the native OpenCypher parser feature set.
clippy-opencypher:
    cargo clippy --locked --all-targets --features opencypher -- -D warnings

# Lint every native feature at once.
clippy-native:
    cargo clippy --locked --all-targets --features opencypher,query-transport,query-transport-tls,query-service-discovery,public-client-protocols -- -D warnings

# Lint the public Bolt and HTTP client protocols.
clippy-client-protocols:
    cargo clippy --locked --all-targets --features public-client-protocols -- -D warnings

# Lint the production node and indexer runtimes.
clippy-runtime:
    cargo clippy --locked --all-targets --features server-runtime,indexer-runtime -- -D warnings

# Check all default-feature targets.
check:
    cargo check --locked --all-targets

# The widest compile surface in one line, so a feature that only this recipe
# reaches cannot rot unnoticed. `--all-features` rather than an enumerated list
# on purpose: an enumerated list silently stops covering the next feature added
# to Cargo.toml.
# Check every target with every feature enabled.
check-all-features:
    cargo check --locked --all-targets --all-features

# `client-api` is the shared client stack that `bolt-server` and `http-api` both
# imply. Every path that reaches it otherwise drags a wire protocol in too, so an
# item gated `#[cfg(any(feature = "bolt-server", feature = "http-api"))]` could be
# called from `client-api` code and still compile everywhere else anyone looked.
# `--lib` keeps this to the configuration this recipe exists to catch.
# Check the shared client stack with no wire protocol above it.
check-client-api:
    cargo check --locked --features client-api --lib

# check-all-features cannot stand in for this: with `http-api` also on, anything
# this configuration is missing gets supplied by the other protocol and the gap
# stays hidden.
# Check the standalone Bolt server feature set.
check-bolt-server:
    cargo check --locked --all-targets --features bolt-server

# Check default-feature examples.
check-examples:
    cargo check --locked --examples

# Check examples with the native OpenCypher parser enabled.
check-examples-native:
    cargo check --locked --examples --features opencypher

# Check the feature-gated hard-fence chaos harness.
check-examples-chaos:
    cargo check --locked --examples --features chaos-harness

# Every bare recipe selects the root package only; a workspace member needs its
# own explicit `-p` line or it is never linted.
# Lint the publication protocol crate.
clippy-object-publication:
    cargo clippy --locked --all-targets -p hydradb-object-publication -- -D warnings

# Lint the placement crate.
clippy-placement:
    cargo clippy --locked --all-targets -p hydradb-placement -- -D warnings

# The telemetry crate's OTLP-only modules are behind an off-by-default feature,
# so `--features otlp` is needed to even compile the sampler.
# Lint the telemetry crate with OTLP export enabled.
clippy-telemetry:
    cargo clippy --locked --all-targets -p hydradb-telemetry --features otlp -- -D warnings

# Lint the shared Cypher AST frontend facade.
clippy-cypher-ast:
    cargo clippy --locked --all-targets -p hydradb-cypher-ast -- -D warnings

# Lint the Neo4j grammar parser crate (after its shared AST).
clippy-cypher: clippy-cypher-ast
    cargo clippy --locked --all-targets -p hydradb-cypher-parser-antlr -- -D warnings

# Lint the semantic graph IR and KV planner crate.
clippy-graph-plan:
    cargo clippy --locked --all-targets -p hydradb-graph-plan -- -D warnings

# Lint the isolated Cypher engine crate.
clippy-cypher-engine:
    cargo clippy --locked --all-targets -p hydradb-cypher-engine -- -D warnings

# Lint the opt-in experimental parser/planner/GraphShard route.
clippy-experimental-cypher:
    cargo clippy --locked --all-targets --features server-runtime,experimental-cypher-engine -- -D warnings

# Build the two-phase ordered-boundary-tie memory probe used by experiment 006.
build-ordered-tie-memory-probe:
    cargo build --locked --release --features experimental-cypher-engine --example ordered_tie_memory_probe

# Verify native libraries required by Rust FFI crates.
native-check:
    #!/usr/bin/env bash
    set -euo pipefail
    pkg-config --exists cypher-parser
    if command -v ldconfig >/dev/null 2>&1; then
      ldconfig -p | grep -qi libgraphblas
    elif command -v brew >/dev/null 2>&1; then
      test -f "$(brew --prefix suite-sparse)/lib/libgraphblas.dylib"
    else
      test -f /opt/homebrew/lib/libgraphblas.dylib || test -f /usr/local/lib/libgraphblas.dylib
    fi

# Format check, then every lint over every shipped feature combination, then the
# compile-only configurations. Lints first because they are the cheapest way to
# fail; they are `--all-targets`, so they subsume the same-feature check and
# check-examples recipes feature-for-feature.
# Run the local verification set.
ci: native-check fmt-check clippy clippy-chaos clippy-opencypher clippy-native clippy-client-protocols clippy-runtime clippy-placement clippy-telemetry clippy-cypher clippy-graph-plan clippy-cypher-engine clippy-object-publication check-all-features check-client-api check-bolt-server

# Run the local object-store smoke test.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    store_root="$(mktemp -d)"
    trap 'rm -rf "$store_root"' EXIT
    CLOUD_PROVIDER=local LOCAL_PATH="$store_root" \
      cargo run --locked --example object_store_smoke

# SuiteSparse is the default kernel, so this differs from `smoke` only by pinning
# it, which is the point: `smoke` inherits whatever GRAPH_MATRIX_KERNEL the
# caller's shell already exports. `examples/object_store_smoke.rs` also accepts
# `compact` and `rust`.
# Run the local object-store smoke test pinned to the SuiteSparse kernel.
smoke-graphblas:
    #!/usr/bin/env bash
    set -euo pipefail
    store_root="$(mktemp -d)"
    trap 'rm -rf "$store_root"' EXIT
    CLOUD_PROVIDER=local LOCAL_PATH="$store_root" GRAPH_MATRIX_KERNEL=graphblas \
      cargo run --locked --example object_store_smoke

# Run local multiprocess stress against the local filesystem object store.
stress:
    bash scripts/multiprocess_stress.sh

# Run hard write-fence takeover proof against the local filesystem object store.
fence:
    bash scripts/fence_takeover.sh

# Run MinIO smoke test. Requires Docker.
minio-smoke:
    bash scripts/minio_smoke.sh

# Run Query engine exact query correctness benchmark.
query-correctness:
    bash scripts/query_correctness.sh

# Build one node containing both routes, seed through legacy Bolt, then compare
# the two routes through real Bolt and HTTP against the same immutable store.
# Requires a Python interpreter with the neo4j package (set PYTHON if needed).
cypher-engine-ab:
    bash scripts/cypher_engine_ab.sh

# Bounded protocol benchmark: 10 iterations per corpus query/transport/route.
# Results are timestamped below ignored bench-results/cypher_engine_ab/.
cypher-engine-ab-bench:
    GRAPH_CYPHER_ENGINE_AB_MODE=benchmark GRAPH_CYPHER_ENGINE_AB_ITERATIONS=10 bash scripts/cypher_engine_ab.sh

# Fast offline coverage for the normalization and fixed supported corpus.
test-cypher-engine-ab:
    python3 scripts/cypher_engine_ab_client.py self-test

# Run Query engine exact query correctness benchmark against MinIO. Requires Docker.
minio-query-correctness:
    bash scripts/minio_query_correctness.sh

# Run MinIO chaos test. Requires Docker.
minio-chaos:
    bash scripts/minio_chaos.sh

# Run hard write-fence takeover proof against MinIO. Requires Docker.
minio-fence:
    bash scripts/minio_fence_takeover.sh

# Replay all six Quint Connect adapters against isolated MinIO paths. Requires Docker.
minio-mbt:
    bash scripts/minio_mbt.sh

# Refresh the pinned SlateDB Git dependency in Cargo.lock.
update-slatedb:
    cargo update -p slatedb

# Decode-only validation for the FalkorDB DUMP decoder.
check-falkordb-rdb:
    cargo check --locked -p falkordb-rdb

clippy-falkordb-rdb:
    cargo clippy --locked -p falkordb-rdb --all-targets -- -D warnings

refresh-falkordb-rdb-lock:
    cargo check -p falkordb-rdb

inspect-falkordb-rdb dump:
    cargo run --locked -p falkordb-rdb --bin inspect -- "{{dump}}"

scan-falkordb-rdb root workers="":
    cargo run --locked --release -p falkordb-rdb --bin scan -- "{{root}}" {{workers}}

check-falkor-dump-import:
    cargo check --locked -p falkor-dump-import

clippy-falkor-dump-import:
    cargo clippy --locked -p falkor-dump-import --all-targets -- -D warnings

refresh-falkor-dump-import-lock:
    cargo check -p falkor-dump-import

plan-falkor-dump-import root *args:
    cargo run --locked --release -p falkor-dump-import --bin plan -- "{{root}}" {{args}}

build-falkor-dump-import:
    cargo build --locked --release -p falkor-dump-import --bin import

import-falkor-dump snapshot *args:
    cargo run --locked --release -p falkor-dump-import --bin import -- --snapshot-root "{{snapshot}}" {{args}}
