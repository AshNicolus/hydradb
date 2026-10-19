# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Build and test through the justfile

Run builds and tests as `just <recipe>` (`just --list`), not bare `cargo`. The
justfile exports the three environment variables the FFI features need —
`BINDGEN_EXTRA_CLANG_ARGS` and `LIBRARY_PATH` for Homebrew on macOS, and
`RUST_MIN_STACK` on every platform — without which `opencypher`,
`server-runtime` and `graphblas` fail to link or abort on a stack overflow. CI
is Linux and sets them elsewhere, so a bare `cargo` line that works there fails
here.
