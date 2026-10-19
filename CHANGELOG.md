# Changelog

All notable changes to HydraDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-28

Initial public release of HydraDB, an object-store-native distributed graph
database written in Rust. S3-compatible object storage is the durable source of
truth; query nodes and indexers hold only disposable local state, so they can be
replaced or scaled without moving the graph.

### Added

- **Object-store durability.** Graph records, WALs, manifests, and immutable
  traversal indexes persist to S3-compatible storage on SlateDB.
- **Snapshot-consistent reads.** Every query runs against one pinned SlateDB
  snapshot; indexed traversal combines a compiled CSC generation with its
  visible WAL overlay.
- **OpenCypher queries.** A practical OpenCypher subset for graph reads and
  mutations, plus native path procedures.
- **Graph-native execution.** Planner support for property indexes, reverse
  adjacency, sparse traversal, and SuiteSparse GraphBLAS where appropriate.
- **Familiar clients.** Neo4j-compatible connectivity over Bolt 5.x and a typed
  JSON / streaming NDJSON HTTPS query API.
- **Safe writer handoff.** Object-store CAS leases select the active writer per
  cell; SlateDB writer epochs fence stale writers.
- **Independent compute.** Query nodes and indexers scale separately and rebuild
  their local caches from durable state.
- **Bounded server runtime.** Authentication, authorization, deadlines, result
  limits, backpressure, cancellation, cache budgets, metrics, and traces.
- **Deployment.** Dockerfile for the `graph-node` and `graph-indexer` images and
  a Helm chart for Kubernetes; MinIO-based local harnesses.
- **Docs.** `README.md`, `USING-HYDRADB.md`, and `architecture.md`.

[Unreleased]: https://github.com/hydra-db/hydradb/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/hydra-db/hydradb/releases/tag/v0.2.0
