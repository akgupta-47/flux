# Flux

<p align="center">
  <img src="assets/flux-otter.png" alt="Flux mascot: an otter riding a network current" width="280">
</p>

Flux is a cross-platform async networking utility written in Rust. Flux currently includes TCP and UDP listen/connect modes, TCP proxy/forward and TLS termination, a UDP relay, YAML proxy defaults, a Prometheus text endpoint, and live TCP connection inspection.

## Build

Requires stable Rust with edition 2024 support.

```sh
cargo build --release
```

## Commands

```sh
# Accept TCP clients and consume their input while logging received bytes
cargo run -- listen tcp :8080

# Receive UDP datagrams and log their source and size
cargo run -- listen udp :5353

# Send each input line as one UDP datagram
cargo run -- connect udp 127.0.0.1:5353

# Connect interactively; Ctrl-D closes the sending half
cargo run -- connect tcp example.com:443

# TCP proxy
cargo run -- proxy tcp :8080 localhost:3000 --idle-timeout-secs 300

# Equivalent forwarding syntax
cargo run -- forward tcp --listen :8080 --target localhost:3000

# UDP relay, retaining a connected upstream socket for each client
cargo run -- relay udp :5353 localhost:5354

# TLS termination and TCP forwarding
cargo run -- proxy tls :443 localhost:8080 --cert cert.pem --key key.pem

# TCP proxy with Prometheus metrics and active connection inspection
cargo run -- proxy tcp :8080 localhost:3000 --metrics 127.0.0.1:9090
curl http://127.0.0.1:9090/metrics
cargo run -- connections --endpoint 127.0.0.1:9090
```

Set `RUST_LOG=debug` to adjust structured log verbosity. TCP proxy idle timeout defaults to 300 seconds and can be changed with `--idle-timeout-secs`. Ctrl-C stops listeners and active relay sessions. TCP proxying defaults to 1024 concurrent sessions; configure this with `--max-connections`. TCP keepalive defaults to 60 seconds and is configurable with `--keepalive-secs`. Excess TCP clients are dropped. UDP relay state is capped at 1024 client addresses and expires after two minutes of inactivity.

## Current boundaries

TCP listen mode consumes input and reports byte totals; it is not an echo server. Advanced forwarding rules, Linux io_uring, and Linux splice are not implemented yet. TLS termination is available with `proxy tls`; TCP metrics and connection inspection are available through `--metrics`. Proxy defaults can also be loaded from YAML with `--config`. The UDP relay uses IPv4 wildcard-bound upstream sockets and is intended for IPv4 targets. See [the architecture proposal](docs/architecture-proposal.md) and [benchmark notes](docs/benchmarks.md).

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Proxy buffer tuning and benchmark

TCP proxy/forward defaults to a 32 KiB copy buffer per direction and enables `TCP_NODELAY` and TCP keepalive on both proxy sockets. Set `--buffer-size` to test a value from 1 byte to 1 MiB; set `--max-connections` and `--keepalive-secs` for connection limits and keepalive tuning, for example:

```sh
cargo run --release -- proxy tcp 127.0.0.1:8080 127.0.0.1:3000 --buffer-size 262144 --max-connections 2048 --keepalive-secs 60 --idle-timeout-secs 300
```

Run the sustained local comparison against socat after building:

```sh
cargo build --release
python3 benches/relay_bench.py
```

The script compares 8, 32, 64, and 256 KiB Flux buffers against socat across three rounds, with eight concurrent clients sending 32 MiB each. It runs a local Python echo server and verifies returned payload bytes. Set `FLUX_BENCH_ROUNDS=5` to increase repetitions. If macOS reports socket buffer exhaustion during longer sweeps, lower pressure with `FLUX_BENCH_CONCURRENCY=4 FLUX_BENCH_MIB_PER_CLIENT=16` and/or increase cleanup delay with `FLUX_BENCH_COOLDOWN=1`. `FLUX_BENCH_CHUNK_KIB` controls client write size (64 KiB by default). The loopback result describes this machine and workload; it does not predict throughput over a physical network. The Python echo process may become the bottleneck.

## TLS proxy

`flux proxy tls <listen> <target> --cert cert.pem --key key.pem` accepts TLS connections, decrypts them with rustls, and relays the plaintext stream to the TCP target. Use a certificate chain in PEM format and a PEM private key. Protect the key file with appropriate filesystem permissions.

## Packaging

A multi-stage Dockerfile is provided. Build with `docker build -t flux .`, then pass a Flux command after the image name. Tagged builds produce CI artifacts for macOS arm64/x86_64, Linux x86_64/arm64, and Windows x86_64. A Homebrew formula template is in `packaging/homebrew/flux.rb`; publishing requires a matching GitHub tag.
