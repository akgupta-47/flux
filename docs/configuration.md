# Configuration

The MVP has no config file. Configure log filtering with `RUST_LOG` (for example `RUST_LOG=debug flux proxy tcp :8080 localhost:3000`). The TCP proxy and TCP listener currently allow at most 1024 accepted sessions. UDP relay keeps at most 1024 client sessions and expires each after 120 seconds idle.

All addresses are passed to Tokio's socket bind/connect and name resolution APIs. Use explicit loopback addresses such as `127.0.0.1:8080` when only local access is intended. The UDP relay currently binds per-client upstream sockets on IPv4 wildcard `0.0.0.0`, so IPv6-only UDP targets are not supported yet.
