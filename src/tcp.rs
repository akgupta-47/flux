use crate::observability::Metrics;
use crate::timeout_io::IdleTimeoutIo;
use anyhow::{Context, Result};
use socket2::{SockRef, TcpKeepalive};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
};
use tracing::{info, warn};

const MAX_CONNECTIONS: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub struct ProxyOptions {
    pub buffer_size: usize,
    pub max_connections: usize,
    pub keepalive_secs: u64,
    pub idle_timeout_secs: u64,
    pub send_buffer_size: Option<usize>,
    pub recv_buffer_size: Option<usize>,
}
impl Default for ProxyOptions {
    fn default() -> Self {
        Self {
            buffer_size: 32768,
            max_connections: MAX_CONNECTIONS,
            keepalive_secs: 60,
            idle_timeout_secs: 300,
            send_buffer_size: None,
            recv_buffer_size: None,
        }
    }
}

pub async fn listen(address: &str, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind TCP listener at {address}"))?;
    let permits = std::sync::Arc::new(Semaphore::new(MAX_CONNECTIONS));
    info!(address = %listener.local_addr()?, "TCP listener ready");
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            accepted = listener.accept() => {
                let (mut stream, peer) = accepted?;
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => { warn!(%peer, "connection limit reached; dropping client"); continue; }
                };
                let mut connection_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut sink = tokio::io::sink();
                    tokio::select! {
                        result = io::copy(&mut stream, &mut sink) => match result {
                            Ok(bytes) => info!(%peer, bytes, "listener connection closed"),
                            Err(error) => warn!(%peer, %error, "listener connection failed"),
                        },
                        _ = connection_shutdown.changed() => info!(%peer, "listener connection stopped during shutdown"),
                    }
                });
            }
        }
    }
    Ok(())
}

pub async fn connect(address: &str) -> Result<()> {
    let mut stream = TcpStream::connect(address)
        .await
        .with_context(|| format!("connect to {address}"))?;
    info!(address, "connected; press Ctrl-D to finish sending");
    let (mut reader, mut writer) = stream.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let send = async {
        io::copy(&mut stdin, &mut writer).await?;
        writer.shutdown().await
    };
    let receive = async {
        io::copy(&mut reader, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::try_join!(send, receive)?;
    Ok(())
}

pub async fn proxy(
    address: &str,
    target: &str,
    buffer_size: usize,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    proxy_with_options(
        address,
        target,
        ProxyOptions {
            buffer_size,
            ..ProxyOptions::default()
        },
        shutdown,
    )
    .await
}

pub async fn proxy_with_options(
    address: &str,
    target: &str,
    options: ProxyOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    proxy_with_metrics(
        address,
        target,
        options,
        Arc::new(Metrics::default()),
        shutdown,
    )
    .await
}

pub async fn proxy_with_metrics(
    address: &str,
    target: &str,
    options: ProxyOptions,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let ProxyOptions {
        buffer_size,
        max_connections,
        keepalive_secs,
        idle_timeout_secs,
        send_buffer_size,
        recv_buffer_size,
    } = options;
    anyhow::ensure!(
        buffer_size > 0 && buffer_size <= 1_048_576,
        "buffer size must be between 1 and 1048576 bytes"
    );
    anyhow::ensure!(
        max_connections > 0,
        "maximum connections must be greater than zero"
    );
    anyhow::ensure!(
        keepalive_secs > 0,
        "keepalive interval must be greater than zero"
    );
    anyhow::ensure!(
        idle_timeout_secs > 0,
        "idle timeout must be greater than zero"
    );
    validate_socket_buffer_sizes(send_buffer_size, recv_buffer_size)?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind TCP proxy at {address}"))?;
    let target = target.to_owned();
    let permits = std::sync::Arc::new(Semaphore::new(max_connections));
    info!(listen = %listener.local_addr()?, %target, max_connections, buffer_size, keepalive_secs, "TCP proxy ready");
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            accepted = listener.accept() => {
                let (inbound, peer) = accepted?;
                inbound.set_nodelay(true)?;
                configure_keepalive(&inbound, keepalive_secs)?;
                configure_socket_buffers(&inbound, send_buffer_size, recv_buffer_size)?;
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => { metrics.reject(); warn!(%peer, "connection limit reached; dropping client"); continue; }
                };
                let target = target.clone();
                let metrics = metrics.clone();
                let idle_timeout = Duration::from_secs(idle_timeout_secs);
                let mut connection_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let connection = metrics.accept(peer.to_string());
                    match TcpStream::connect(&target).await {
                        Ok(outbound) => {
                            if let Err(error) = outbound.set_nodelay(true) {
                                warn!(%peer, %error, "failed to set TCP_NODELAY on upstream");
                                return;
                            }
                            if let Err(error) = configure_keepalive(&outbound, keepalive_secs) {
                                warn!(%peer, %error, "failed to configure TCP keepalive on upstream");
                                return;
                            }
                            if let Err(error) = configure_socket_buffers(&outbound, send_buffer_size, recv_buffer_size) {
                                warn!(%peer, %error, "failed to configure upstream socket buffers");
                                return;
                            }
                            let mut inbound = IdleTimeoutIo::new(inbound, idle_timeout);
                            let mut outbound = IdleTimeoutIo::new(outbound, idle_timeout);
                            tokio::select! {
                                result = io::copy_bidirectional_with_sizes(&mut inbound, &mut outbound, buffer_size, buffer_size) => match result {
                                    Ok((up, down)) => { connection.complete(up, down); info!(%peer, bytes_up = up, bytes_down = down, "proxy connection closed"); }
                                    Err(error) => { metrics.error(); warn!(%peer, %error, "proxy connection failed"); }
                                },
                                _ = connection_shutdown.changed() => {
                                    info!(%peer, "proxy connection stopped during shutdown");
                                    let _ = inbound.shutdown().await;
                                    let _ = outbound.shutdown().await;
                                },
                            }
                        }
                        Err(error) => { metrics.error(); warn!(%peer, %target, %error, "upstream connection failed"); },
                    }
                });
            }
        }
    }
    Ok(())
}

fn configure_keepalive(stream: &TcpStream, secs: u64) -> Result<()> {
    let keepalive = TcpKeepalive::new().with_time(Duration::from_secs(secs));
    SockRef::from(stream)
        .set_tcp_keepalive(&keepalive)
        .context("configure TCP keepalive")
}

fn validate_socket_buffer_sizes(send: Option<usize>, recv: Option<usize>) -> Result<()> {
    for (name, size) in [("send", send), ("receive", recv)] {
        if let Some(size) = size {
            anyhow::ensure!(
                size > 0 && size <= 16 * 1024 * 1024,
                "TCP {name} buffer size must be between 1 and 16777216 bytes"
            );
        }
    }
    Ok(())
}

fn configure_socket_buffers(
    stream: &TcpStream,
    send: Option<usize>,
    recv: Option<usize>,
) -> Result<()> {
    let socket = SockRef::from(stream);
    if let Some(size) = send {
        socket
            .set_send_buffer_size(size)
            .context("configure TCP send buffer")?;
    }
    if let Some(size) = recv {
        socket
            .set_recv_buffer_size(size)
            .context("configure TCP receive buffer")?;
    }
    Ok(())
}

pub async fn tls_proxy(
    address: &str,
    target: &str,
    cert_path: &str,
    key_path: &str,
    options: ProxyOptions,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    use tokio_rustls::TlsAcceptor;
    let ProxyOptions {
        buffer_size,
        max_connections,
        keepalive_secs,
        idle_timeout_secs,
        send_buffer_size,
        recv_buffer_size,
    } = options;
    anyhow::ensure!(
        buffer_size > 0 && buffer_size <= 1_048_576,
        "buffer size must be between 1 and 1048576 bytes"
    );
    anyhow::ensure!(
        max_connections > 0,
        "maximum connections must be greater than zero"
    );
    anyhow::ensure!(
        keepalive_secs > 0 && idle_timeout_secs > 0,
        "keepalive and idle timeout values must be greater than zero"
    );
    validate_socket_buffer_sizes(send_buffer_size, recv_buffer_size)?;
    let cert_bytes = tokio::fs::read(cert_path)
        .await
        .with_context(|| format!("read certificate {cert_path}"))?;
    let key_bytes = tokio::fs::read(key_path)
        .await
        .with_context(|| format!("read private key {key_path}"))?;
    let certs = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    anyhow::ensure!(
        !certs.is_empty(),
        "certificate file contains no certificates"
    );
    let key = rustls_pemfile::private_key(&mut key_bytes.as_slice())?
        .context("private key file contains no supported key")?;
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("configure TLS certificate and key")?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind TLS proxy at {address}"))?;
    let target = target.to_owned();
    let permits = Arc::new(Semaphore::new(max_connections));
    info!(listen = %listener.local_addr()?, %target, max_connections, buffer_size, "TLS proxy ready");
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            accepted = listener.accept() => {
                let (inbound, peer) = accepted?;
                inbound.set_nodelay(true)?;
                configure_keepalive(&inbound, keepalive_secs)?;
                configure_socket_buffers(&inbound, send_buffer_size, recv_buffer_size)?;
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => { warn!(%peer, "connection limit reached; dropping TLS client"); continue; }
                };
                let target = target.clone();
                let acceptor = acceptor.clone();
                let mut connection_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    tokio::select! {
                        _ = connection_shutdown.changed() => info!(%peer, "TLS connection stopped during shutdown"),
                        _ = async {
                            match acceptor.accept(inbound).await {
                                Ok(tls_stream) => match TcpStream::connect(&target).await {
                                    Ok(upstream) => {
                                        if let Err(error) = upstream.set_nodelay(true) { warn!(%peer, %error, "failed to configure TLS upstream socket"); return; }
                                        if let Err(error) = configure_keepalive(&upstream, keepalive_secs) { warn!(%peer, %error, "failed to configure TLS upstream socket"); return; }
                                        if let Err(error) = configure_socket_buffers(&upstream, send_buffer_size, recv_buffer_size) { warn!(%peer, %error, "failed to configure TLS upstream socket buffers"); return; }
                                        let mut tls_stream = IdleTimeoutIo::new(tls_stream, Duration::from_secs(idle_timeout_secs));
                                        let mut upstream = IdleTimeoutIo::new(upstream, Duration::from_secs(idle_timeout_secs));
                                        match io::copy_bidirectional_with_sizes(&mut tls_stream, &mut upstream, buffer_size, buffer_size).await {
                                        Ok((up, down)) => info!(%peer, bytes_up = up, bytes_down = down, "TLS proxy connection closed"),
                                        Err(error) => warn!(%peer, %error, "TLS proxy connection failed"),
                                        }
                                    }
                                    Err(error) => warn!(%peer, %target, %error, "TLS upstream connection failed"),
                                },
                                Err(error) => warn!(%peer, %error, "TLS handshake failed"),
                            }
                        } => {}
                    }
                });
            }
        }
    }
    Ok(())
}
