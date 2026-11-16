use anyhow::{Context, Result};
use socket2::{SockRef, TcpKeepalive};
use std::time::Duration;
use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
};
use tracing::{info, warn};

const MAX_CONNECTIONS: usize = 1024;

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
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut sink = tokio::io::sink();
                    match io::copy(&mut stream, &mut sink).await {
                        Ok(bytes) => info!(%peer, bytes, "listener connection closed"),
                        Err(error) => warn!(%peer, %error, "listener connection failed"),
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
    proxy_with_options(address, target, buffer_size, MAX_CONNECTIONS, 60, shutdown).await
}

pub async fn proxy_with_options(
    address: &str,
    target: &str,
    buffer_size: usize,
    max_connections: usize,
    keepalive_secs: u64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
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
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => { warn!(%peer, "connection limit reached; dropping client"); continue; }
                };
                let target = target.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match TcpStream::connect(&target).await {
                        Ok(mut outbound) => {
                            if let Err(error) = outbound.set_nodelay(true) {
                                warn!(%peer, %error, "failed to set TCP_NODELAY on upstream");
                                return;
                            }
                            if let Err(error) = configure_keepalive(&outbound, keepalive_secs) {
                                warn!(%peer, %error, "failed to configure TCP keepalive on upstream");
                                return;
                            }
                            let mut inbound = inbound;
                            match io::copy_bidirectional_with_sizes(&mut inbound, &mut outbound, buffer_size, buffer_size).await {
                                Ok((up, down)) => info!(%peer, bytes_up = up, bytes_down = down, "proxy connection closed"),
                                Err(error) => warn!(%peer, %error, "proxy connection failed"),
                            }
                        }
                        Err(error) => warn!(%peer, %target, %error, "upstream connection failed"),
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
