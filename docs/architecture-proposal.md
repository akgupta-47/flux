# Flux architecture proposal

## Repository inspection

The requested destination `/Users/akshatgupta/Developer/flux` did not exist, so this is a new binary crate. There is no legacy implementation or API to preserve.

## MVP scope

Phase 1 delivers the executable `flux` with:

- `listen tcp <address>`: accept TCP clients and log lifecycle and byte totals.
- `connect tcp <address>`: connect and relay stdin/stdout interactively.
- `proxy tcp <listen> <target>` and equivalent `forward tcp --listen ... --target ...`: bounded-concurrency bidirectional TCP proxying.
- `relay udp <listen> <target>`: relay datagrams with per-client upstream sockets and return traffic to the originating client.
- Ctrl-C graceful shutdown, structured tracing, and integration coverage for basic transfer and bidirectional proxying.

TLS, metrics endpoint, connection listing, benchmarks, config files, io_uring, splice, and advanced resource controls remain later phases. Listener mode accepts connections but does not offer a generic arbitrary protocol handler.

## Architecture

Keep CLI parsing in `cli.rs`, runtime dispatch in `main.rs`, and transport behavior in `tcp.rs` and `udp.rs`. Tokio is the sole MVP runtime. TCP proxy tasks use Tokio's `copy_bidirectional`, which propagates write pressure through the two streams and handles half-close semantics. A semaphore bounds accepted proxy sessions; excess sessions are closed immediately. UDP uses a bounded client-session table with a dedicated connected upstream socket per client, preserving reply mapping; table capacity and idle expiry prevent unbounded state.

The shutdown signal is shared across accept/receive loops and connection tasks. Tracing is initialized once at startup with environment-driven filtering and human-readable output. No unsafe code or runtime abstraction is needed for this phase.

## Dependency and compatibility notes

Use stable Rust 2024, Tokio full networking/signal/io features, clap derive, tracing plus tracing-subscriber env-filter/fmt, anyhow, and thiserror only if a distinct domain error type becomes useful. Keep `bytes`, serde, rustls, socket2, metrics, criterion, and io_uring out until a concrete MVP need exists. Tokio's portable TCP/UDP APIs cover macOS, Linux, and Windows; platform-specific backends are deferred behind later feature work.

## Implementation sequence

1. Establish this proposal and document Phase 1 boundaries.
2. Implement CLI and portable Tokio TCP/UDP flows with clean shutdown and bounded resources.
3. Add integration coverage for listener/connect/proxy/UDP data paths and shutdown behavior where practical.
4. Format, run Clippy, run tests, and record any environmental limits.


## Current implementation status (2026-11-17)

Implemented: portable Tokio TCP listen/connect/proxy/forward and UDP listen/connect/relay; bounded TCP proxy and UDP client state; configurable proxy buffers, connection limits, keepalive, and idle expiry; structured logs; YAML proxy defaults; rustls TLS termination; Prometheus text metrics and a live connection registry; sustained socat comparison; Linux/macOS/Windows CI; Docker build and tagged target build workflow.

The TCP proxy can optionally use the Linux-only `io-uring-backend` Cargo feature and `--backend io-uring`. It runs on an isolated tokio-uring runtime; the portable Tokio path remains the default. Socket send/receive buffers can be tuned from CLI or YAML. This backend is experimental and has not been shown to outperform Tokio on the project's Linux benchmark workload. Linux splice remains deferred because its zero-copy behavior has not yet been validated against the current bidirectional shutdown, timeout, and metrics semantics. Advanced routing rules, UDP IPv6 session sockets, a full TLS integration matrix, and Homebrew release publication also remain.
