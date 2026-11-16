use anyhow::{Context, Result};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt},
    net::UdpSocket,
    sync::{Mutex, watch},
};
use tracing::{info, warn};

const MAX_CLIENTS: usize = 1024;
const IDLE_SECS: u64 = 120;
type SessionMap = Arc<Mutex<HashMap<SocketAddr, (Arc<UdpSocket>, tokio::time::Instant)>>>;

pub async fn relay(address: &str, target: &str, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let target_addr = tokio::net::lookup_host(target)
        .await
        .with_context(|| format!("resolve UDP target {target}"))?
        .next()
        .context("UDP target resolved to no addresses")?;
    let listener = Arc::new(
        UdpSocket::bind(address)
            .await
            .with_context(|| format!("bind UDP relay at {address}"))?,
    );
    let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
    let mut buf = vec![0u8; 65536];
    info!(listen = %listener.local_addr()?, target = %target_addr, max_clients = MAX_CLIENTS, "UDP relay ready");
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            received = listener.recv_from(&mut buf) => {
                let (len, client) = received?;
                let upstream = {
                    let mut map = sessions.lock().await;
                    map.retain(|_, (_, last)| last.elapsed() < Duration::from_secs(IDLE_SECS));
                    if !map.contains_key(&client) && map.len() >= MAX_CLIENTS {
                        warn!(%client, "UDP client limit reached; dropping datagram");
                        continue;
                    }
                    if let Some((socket, last)) = map.get_mut(&client) {
                        *last = tokio::time::Instant::now();
                        socket.clone()
                    } else {
                        let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
                        socket.connect(target_addr).await?;
                        map.insert(client, (socket.clone(), tokio::time::Instant::now()));
                        let downstream = listener.clone();
                        let map_ref = sessions.clone();
                        let upstream_ref = socket.clone();
                        let mut session_shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            let mut reply = vec![0u8; 65536];
                            loop {
                                tokio::select! {
                                    changed = session_shutdown.changed() => { if changed.is_err() || *session_shutdown.borrow() { break; } }
                                    result = tokio::time::timeout(Duration::from_secs(IDLE_SECS), upstream_ref.recv(&mut reply)) => match result {
                                        Ok(Ok(size)) => {
                                            if downstream.send_to(&reply[..size], client).await.is_err() { break; }
                                            if let Some((_, last)) = map_ref.lock().await.get_mut(&client) { *last = tokio::time::Instant::now(); }
                                        }
                                        Ok(Err(error)) => { warn!(%client, %error, "UDP upstream receive failed"); break; }
                                        Err(_) => break,
                                    }
                                }
                            }
                        });
                        socket
                    }
                };
                if let Err(error) = upstream.send(&buf[..len]).await { warn!(%client, %error, "UDP upstream send failed"); }
            }
        }
    }
    Ok(())
}

pub async fn listen(address: &str, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let socket = UdpSocket::bind(address)
        .await
        .with_context(|| format!("bind UDP listener at {address}"))?;
    info!(address = %socket.local_addr()?, "UDP listener ready; datagrams are consumed and logged");
    let mut buffer = vec![0u8; 65536];
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            received = socket.recv_from(&mut buffer) => {
                let (bytes, peer) = received?;
                info!(%peer, bytes, "UDP datagram received");
            }
        }
    }
    Ok(())
}

pub async fn connect(address: &str, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let target = tokio::net::lookup_host(address)
        .await
        .with_context(|| format!("resolve UDP target {address}"))?
        .next()
        .context("UDP target resolved to no addresses")?;
    let bind_addr = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(target).await?;
    info!(%target, "connected UDP; each input line is sent as one datagram");
    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut line = Vec::with_capacity(2048);
    let mut reply = vec![0u8; 65536];
    loop {
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            read = read_datagram_line(&mut input, &mut line) => {
                if !read? { break; }
                socket.send(&line).await?;
                line.clear();
            }
            received = socket.recv(&mut reply) => {
                let size = received?;
                stdout.write_all(&reply[..size]).await?;
                stdout.flush().await?;
            }
        }
    }
    Ok(())
}

async fn read_datagram_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> Result<bool> {
    const MAX_DATAGRAM: usize = 65_507;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let remaining = MAX_DATAGRAM.saturating_sub(line.len());
        if remaining == 0 {
            anyhow::bail!("input line exceeds maximum UDP payload size");
        }
        let available_len = available.len();
        let count = available_len.min(remaining);
        let newline = available[..count].iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(count, |index| index + 1);
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(true);
        }
        if take < available_len || line.len() == MAX_DATAGRAM {
            anyhow::bail!("input line exceeds maximum UDP payload size");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn udp_input_line_is_bounded_to_maximum_datagram_size() {
        let (mut writer, reader) = tokio::io::duplex(70_000);
        tokio::spawn(async move {
            writer.write_all(&vec![b'x'; 65_508]).await.unwrap();
            writer.write_all(b"\\n").await.unwrap();
        });
        let mut reader = tokio::io::BufReader::new(reader);
        let mut line = Vec::new();
        assert!(read_datagram_line(&mut reader, &mut line).await.is_err());
        assert!(line.len() <= 65_507);
    }
}
