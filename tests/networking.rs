use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};

async fn start_tcp_echo() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    (addr, task)
}

#[tokio::test]
async fn tcp_proxy_forwards_bidirectional_payload() {
    let (target, _echo_task) = start_tcp_echo().await;
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = probe.local_addr().unwrap();
    drop(probe);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let proxy_task = tokio::spawn(async move {
        flux::tcp::proxy(&proxy_addr.to_string(), &target, 32768, shutdown_rx).await
    });
    let mut client = loop {
        match TcpStream::connect(proxy_addr).await {
            Ok(stream) => break stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    client.write_all(b"bidirectional").await.unwrap();
    let mut response = [0; 13];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"bidirectional");
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}

#[tokio::test]
async fn tcp_echo_fixture_accepts_data() {
    let (address, _task) = start_tcp_echo().await;
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(b"flux").await.unwrap();
    let mut response = [0; 4];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"flux");
}

#[tokio::test]
async fn udp_relay_preserves_client_reply_mapping() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target.local_addr().unwrap().to_string();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0; 128];
        while let Ok((size, peer)) = target.recv_from(&mut buffer).await {
            if target.send_to(&buffer[..size], peer).await.is_err() {
                break;
            }
        }
    });
    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = probe.local_addr().unwrap();
    drop(probe);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let relay_task = tokio::spawn(async move {
        flux::udp::relay(&relay_addr.to_string(), &target_addr, shutdown_rx).await
    });
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"udp-flux", relay_addr).await.unwrap();
    let mut response = [0; 32];
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut response))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&response[..size], b"udp-flux");
    assert_eq!(source, relay_addr);
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), relay_task).await;
    echo_task.abort();
}
