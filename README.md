# Flux

Flux is a cross-platform async networking utility written in Rust. The current MVP includes TCP listen/connect/proxy/forward and a UDP relay.

## Build

Requires stable Rust with edition 2024 support.

```sh
cargo build --release
```

## Commands

```sh
# Accept TCP clients and consume their input while logging received bytes
cargo run -- listen tcp :8080

# Connect interactively; Ctrl-D closes the sending half
cargo run -- connect tcp example.com:443

# TCP proxy
cargo run -- proxy tcp :8080 localhost:3000

# Equivalent forwarding syntax
cargo run -- forward tcp --listen :8080 --target localhost:3000

# UDP relay, retaining a connected upstream socket for each client
cargo run -- relay udp :5353 localhost:5354
```

Set `RUST_LOG=debug` to adjust structured log verbosity. Ctrl-C stops the listener loops. TCP proxying is capped at 1024 concurrent sessions; excess clients are dropped. UDP relay state is capped at 1024 client addresses and expires after two minutes of inactivity.

## MVP boundaries

TCP listen mode consumes input and reports byte totals; it is not an echo server. UDP listener, UDP connect, TLS, metrics endpoint, connection inspection, YAML config, benchmarking, and io_uring are not implemented yet. The UDP relay uses IPv4 wildcard-bound upstream sockets and is intended for IPv4 targets in this MVP. See [the architecture proposal](docs/architecture-proposal.md).

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Proxy buffer tuning and benchmark

TCP proxy/forward defaults to a 32 KiB copy buffer per direction and enables `TCP_NODELAY` on both proxy sockets. Set `--buffer-size` to test a value from 1 byte to 1 MiB, for example:

```sh
cargo run --release -- proxy tcp 127.0.0.1:8080 127.0.0.1:3000 --buffer-size 262144
```

Run the sustained local comparison against socat after building:

```sh
cargo build --release
python3 benches/relay_bench.py
```

The script compares 8, 32, 64, and 256 KiB Flux buffers against socat across three rounds, with eight concurrent clients sending 32 MiB each. It runs a local Python echo server and verifies returned payload bytes. Set `FLUX_BENCH_ROUNDS=5` to increase repetitions. The loopback result describes this machine and workload; it does not predict throughput over a physical network. The Python echo process may become the bottleneck.
