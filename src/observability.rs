use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
};
use tracing::warn;

#[derive(Default)]
pub struct Metrics {
    active: AtomicUsize,
    accepted: AtomicU64,
    rejected: AtomicU64,
    errors: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    connections: Mutex<BTreeMap<u64, (String, Instant)>>,
    next_id: AtomicU64,
}

pub struct ConnectionGuard {
    metrics: Arc<Metrics>,
    id: u64,
}

impl Metrics {
    pub fn accept(self: &Arc<Self>, peer: String) -> ConnectionGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        self.accepted.fetch_add(1, Ordering::Relaxed);
        self.connections
            .lock()
            .expect("connection registry poisoned")
            .insert(id, (peer, Instant::now()));
        ConnectionGuard {
            metrics: self.clone(),
            id,
        }
    }
    pub fn reject(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> String {
        format!(
            "# HELP flux_connections_active Active TCP connections\n# TYPE flux_connections_active gauge\nflux_connections_active {}\n# HELP flux_connections_accepted_total Accepted TCP connections\n# TYPE flux_connections_accepted_total counter\nflux_connections_accepted_total {}\n# HELP flux_connections_rejected_total Connections rejected at the limit\n# TYPE flux_connections_rejected_total counter\nflux_connections_rejected_total {}\n# HELP flux_connection_errors_total TCP connection errors\n# TYPE flux_connection_errors_total counter\nflux_connection_errors_total {}\n# HELP flux_bytes_up_total Bytes sent from clients to upstreams\n# TYPE flux_bytes_up_total counter\nflux_bytes_up_total {}\n# HELP flux_bytes_down_total Bytes sent from upstreams to clients\n# TYPE flux_bytes_down_total counter\nflux_bytes_down_total {}\n",
            self.active.load(Ordering::Relaxed),
            self.accepted.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.bytes_up.load(Ordering::Relaxed),
            self.bytes_down.load(Ordering::Relaxed)
        )
    }
    pub fn connections_json(&self) -> String {
        let rows = self
            .connections
            .lock()
            .expect("connection registry poisoned");
        let items = rows.iter().map(|(id, (peer, started))| format!("{{\"id\":{id},\"peer\":\"{}\",\"duration_seconds\":{:.3},\"state\":\"relaying\"}}", peer.replace('\\', "\\\\").replace('"', "\\\""), started.elapsed().as_secs_f64())).collect::<Vec<_>>().join(",");
        format!("[{items}]")
    }
}
impl ConnectionGuard {
    pub fn complete(self, up: u64, down: u64) {
        self.metrics.bytes_up.fetch_add(up, Ordering::Relaxed);
        self.metrics.bytes_down.fetch_add(down, Ordering::Relaxed);
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .connections
            .lock()
            .expect("connection registry poisoned")
            .remove(&self.id);
    }
}

pub async fn serve(
    address: &str,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind metrics endpoint at {address}"))?;
    tracing::info!(address = %listener.local_addr()?, "metrics endpoint ready");
    let permits = Arc::new(Semaphore::new(32));
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let permit = match permits.clone().try_acquire_owned() { Ok(permit) => permit, Err(_) => continue };
                let metrics = metrics.clone();
                tokio::spawn(async move { let _permit = permit; if let Err(error) = respond(stream, metrics).await { warn!(%error, "metrics request failed"); } });
            }
        }
    }
    Ok(())
}
async fn respond(stream: TcpStream, metrics: Arc<Metrics>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut request_bytes = Vec::with_capacity(128);
    while request_bytes.len() < 2048 {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            break;
        }
        let limit = (2048 - request_bytes.len()).min(available.len());
        let end = available[..limit]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|n| n + 1)
            .unwrap_or(limit);
        request_bytes.extend_from_slice(&available[..end]);
        reader.consume(end);
        if request_bytes.last() == Some(&b'\n') {
            break;
        }
    }
    let request = String::from_utf8_lossy(&request_bytes);
    let path = request.split_whitespace().nth(1).unwrap_or("/");
    let (content_type, body) = match path {
        "/metrics" => ("text/plain; version=0.0.4", metrics.snapshot()),
        "/connections" => ("application/json", metrics.connections_json()),
        _ => ("text/plain", "not found\n".to_owned()),
    };
    let status = if path == "/metrics" || path == "/connections" {
        "200 OK"
    } else {
        "404 Not Found"
    };
    write.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metrics_track_lifecycle_and_bytes_without_client_labels() {
        let stats = Arc::new(Metrics::default());
        let guard = stats.accept("127.0.0.1:1234".to_owned());
        assert!(stats.snapshot().contains("flux_connections_active 1"));
        assert!(stats.connections_json().contains("127.0.0.1:1234"));
        guard.complete(120, 80);
        let snapshot = stats.snapshot();
        assert!(snapshot.contains("flux_bytes_up_total 120"));
        assert!(snapshot.contains("flux_bytes_down_total 80"));
        assert!(snapshot.contains("flux_connections_active 0"));
        assert!(!snapshot.contains("127.0.0.1"));
    }
}
