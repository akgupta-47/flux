# Configuration

Proxy and forward commands can load optional YAML defaults with `--config`. Configure log filtering with `RUST_LOG` (for example `RUST_LOG=debug flux proxy tcp :8080 localhost:3000`). The TCP proxy and TCP listener currently allow at most 1024 accepted sessions. UDP relay keeps at most 1024 client sessions and expires each after 120 seconds idle.

All addresses are passed to Tokio's socket bind/connect and name resolution APIs. Use explicit loopback addresses such as `127.0.0.1:8080` when only local access is intended. The UDP relay currently binds per-client upstream sockets on IPv4 wildcard `0.0.0.0`, so IPv6-only UDP targets are not supported yet.


## TCP proxy resource controls

The proxy and forward commands accept `--buffer-size` (1 to 1,048,576 bytes, default 32,768), `--max-connections` (default 1,024), and `--keepalive-secs` (default 60). TCP_NODELAY is enabled on both sides. When the connection limit is reached, new clients are closed immediately. Keepalive is configured on accepted and upstream sockets. The application idle timeout defaults to 300 seconds and resets when data is read or written on either socket.


## Metrics and connection inspection

Add `--metrics 127.0.0.1:9090` to a TCP proxy or forward command to expose `/metrics` (Prometheus text format) and `/connections` (active sessions as JSON). `flux connections --endpoint 127.0.0.1:9090` prints the JSON body from the connections endpoint. Bind this endpoint only to a trusted interface: authentication and TLS for the metrics listener are not implemented. Metrics avoid client-address labels; the connection inspection response includes peer addresses by design.

A YAML file can provide defaults for proxy/forward settings; explicit CLI options override file values. Address and target remain positional CLI arguments:

```yaml
buffer_size: 32768
max_connections: 1024
keepalive_secs: 60
idle_timeout_secs: 300
metrics: 127.0.0.1:9090
```

Pass it with `--config flux.yaml`. The idle timeout closes a TCP proxy session when neither side transfers data for the configured number of seconds.
