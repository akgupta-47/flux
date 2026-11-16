use anyhow::{Context, Result};
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
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    anyhow::ensure!(
        buffer_size > 0 && buffer_size <= 1_048_576,
        "buffer size must be between 1 and 1048576 bytes"
    );
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind TCP proxy at {address}"))?;
    let target = target.to_owned();
    let permits = std::sync::Arc::new(Semaphore::new(MAX_CONNECTIONS));
    info!(listen = %listener.local_addr()?, %target, max_connections = MAX_CONNECTIONS, buffer_size, "TCP proxy ready");
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            accepted = listener.accept() => {
                let (inbound, peer) = accepted?;
                inbound.set_nodelay(true)?;
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
