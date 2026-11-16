use anyhow::{Context, Result};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
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
                        tokio::spawn(async move {
                            let mut reply = vec![0u8; 65536];
                            loop {
                                match tokio::time::timeout(Duration::from_secs(IDLE_SECS), upstream_ref.recv(&mut reply)).await {
                                    Ok(Ok(size)) => {
                                        if downstream.send_to(&reply[..size], client).await.is_err() { break; }
                                        if let Some((_, last)) = map_ref.lock().await.get_mut(&client) { *last = tokio::time::Instant::now(); }
                                    }
                                    Ok(Err(error)) => { warn!(%client, %error, "UDP upstream receive failed"); break; }
                                    Err(_) => break,
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
