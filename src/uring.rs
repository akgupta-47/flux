//! Experimental Linux TCP proxy backend built on a dedicated tokio-uring runtime.
//!
//! The backend is deliberately opt-in. It owns a separate current-thread runtime because
//! tokio-uring sockets and buffers are not interchangeable with Tokio's I/O types.

use crate::{observability::Metrics, tcp::ProxyOptions};
use anyhow::{Context, Result};
use socket2::{SockRef, TcpKeepalive};
use std::{
    net::ToSocketAddrs,
    os::fd::{AsFd, AsRawFd, BorrowedFd},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Semaphore, watch};
use tokio_uring::{
    Runtime,
    buf::BoundedBuf,
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

pub async fn proxy(
    address: String,
    target: String,
    options: ProxyOptions,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || run(address, target, options, metrics, shutdown))
        .await
        .context("io_uring proxy worker panicked")?
}

fn run(
    address: String,
    target: String,
    options: ProxyOptions,
    metrics: Arc<Metrics>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    anyhow::ensure!(
        options.buffer_size > 0 && options.buffer_size <= 1_048_576,
        "buffer size must be between 1 and 1048576 bytes"
    );
    anyhow::ensure!(
        options.max_connections > 0,
        "maximum connections must be greater than zero"
    );
    anyhow::ensure!(
        options.idle_timeout_secs > 0,
        "idle timeout must be greater than zero"
    );
    anyhow::ensure!(
        options.keepalive_secs > 0,
        "keepalive interval must be greater than zero"
    );
    for (name, size) in [
        ("send", options.send_buffer_size),
        ("receive", options.recv_buffer_size),
    ] {
        if let Some(size) = size {
            anyhow::ensure!(
                size > 0 && size <= 16 * 1024 * 1024,
                "TCP {name} buffer size must be between 1 and 16777216 bytes"
            );
        }
    }
    let listen_addr = address
        .to_socket_addrs()
        .context("resolve listen address")?
        .next()
        .context("listen address resolved to no socket addresses")?;
    let target_addrs = target
        .to_socket_addrs()
        .context("resolve upstream address")?
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !target_addrs.is_empty(),
        "upstream address resolved to no socket addresses"
    );
    let runtime = Runtime::new(&tokio_uring::builder())
        .context("initialize io_uring; check kernel support and container restrictions")?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(listen_addr).context("bind io_uring TCP proxy")?;
        let permits = Arc::new(Semaphore::new(options.max_connections));
        let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut shutdown = shutdown;
        info!(listen = %listener.local_addr()?, target = %target, max_connections = options.max_connections, buffer_size = options.buffer_size, "io_uring TCP proxy ready");
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                accepted = listener.accept() => {
                    tasks.retain(|task| !task.is_finished());
                    let (inbound, peer) = accepted?;
                    inbound.set_nodelay(true)?;
                    configure_keepalive(&inbound, options.keepalive_secs)?;
                    configure_buffers(&inbound, options.send_buffer_size, options.recv_buffer_size)?;
                    let permit = match permits.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => { metrics.reject(); warn!(%peer, "connection limit reached; dropping client"); continue; }
                    };
                    let metrics = metrics.clone();
                    let mut connection_shutdown = shutdown.clone();
                    let target_addrs = target_addrs.clone();
                    tasks.push(tokio_uring::spawn(async move {
                        let _permit = permit;
                        let connection = metrics.accept(peer.to_string());
                        let result = async {
                            let outbound = connect_upstream(&target_addrs).await?;
                            outbound.set_nodelay(true)?;
                            configure_keepalive(&outbound, options.keepalive_secs)?;
                            configure_buffers(&outbound, options.send_buffer_size, options.recv_buffer_size)?;
                            let (up, down) = tokio::select! {
                                result = copy_bidirectional(&inbound, &outbound, options.buffer_size, Duration::from_secs(options.idle_timeout_secs)) => result?,
                                _ = connection_shutdown.changed() => {
                                    let _ = inbound.shutdown(std::net::Shutdown::Both);
                                    let _ = outbound.shutdown(std::net::Shutdown::Both);
                                    return Ok::<(u64, u64), anyhow::Error>((0, 0));
                                }
                            };
                            Ok::<(u64, u64), anyhow::Error>((up, down))
                        }.await;
                        match result {
                            Ok((up, down)) => connection.complete(up, down),
                            Err(error) => { metrics.error(); warn!(%peer, %error, "io_uring proxy connection failed"); }
                        }
                    }));
                }
            }
        }
        for task in tasks {
            if let Err(error) = task.await { warn!(%error, "io_uring connection task failed"); }
        }
        Ok::<(), anyhow::Error>(())
    })
}

fn configure_keepalive(stream: &TcpStream, secs: u64) -> Result<()> {
    let keepalive = TcpKeepalive::new().with_time(Duration::from_secs(secs));
    SockRef::from(&FdRef(stream))
        .set_tcp_keepalive(&keepalive)
        .context("configure TCP keepalive")
}

async fn connect_upstream(addresses: &[std::net::SocketAddr]) -> std::io::Result<TcpStream> {
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect(*address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "no upstream addresses",
        )
    }))
}

fn configure_buffers(stream: &TcpStream, send: Option<usize>, recv: Option<usize>) -> Result<()> {
    let fd_ref = FdRef(stream);
    let socket = SockRef::from(&fd_ref);
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

struct FdRef<'a>(&'a TcpStream);

impl AsFd for FdRef<'_> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: The borrowed tokio-uring stream owns this descriptor and outlives this borrow.
        unsafe { BorrowedFd::borrow_raw(self.0.as_raw_fd()) }
    }
}

async fn copy_bidirectional(
    left: &TcpStream,
    right: &TcpStream,
    buffer_size: usize,
    idle_timeout: Duration,
) -> std::io::Result<(u64, u64)> {
    tokio::try_join!(
        copy(left, right, buffer_size, idle_timeout),
        copy(right, left, buffer_size, idle_timeout),
    )
}

async fn copy(
    source: &TcpStream,
    destination: &TcpStream,
    buffer_size: usize,
    idle_timeout: Duration,
) -> std::io::Result<u64> {
    let mut buffer = vec![0; buffer_size];
    let mut transferred = 0_u64;
    loop {
        let (read_result, next_buffer) = tokio::time::timeout(idle_timeout, source.read(buffer))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy idle timeout"))?;
        buffer = next_buffer;
        let count = read_result?;
        if count == 0 {
            destination.shutdown(std::net::Shutdown::Write)?;
            return Ok(transferred);
        }
        let (write_result, returned) =
            tokio::time::timeout(idle_timeout, destination.write_all(buffer.slice(..count)))
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy idle timeout")
                })?;
        write_result?;
        buffer = returned.into_inner();
        transferred += count as u64;
    }
}
