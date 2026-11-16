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

#[tokio::test]
async fn tcp_proxy_transfers_large_payload() {
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
    let size = 8 * 1024 * 1024;
    let payload = vec![0xA5; size];
    client.write_all(&payload).await.unwrap();
    let mut response = vec![0; size];
    tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload, response);
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}

#[tokio::test]
async fn proxy_shutdown_closes_idle_active_session() {
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
    client.write_all(b"x").await.unwrap();
    let mut echo = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut echo))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echo, b"x");
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), proxy_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn tcp_proxy_closes_idle_connection_after_configured_timeout() {
    let (target, _echo_task) = start_tcp_echo().await;
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = probe.local_addr().unwrap();
    drop(probe);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let proxy_task = tokio::spawn(async move {
        flux::tcp::proxy_with_options(
            &proxy_addr.to_string(),
            &target,
            flux::tcp::ProxyOptions {
                idle_timeout_secs: 1,
                ..flux::tcp::ProxyOptions::default()
            },
            shutdown_rx,
        )
        .await
    });
    let mut client = loop {
        match TcpStream::connect(proxy_addr).await {
            Ok(stream) => break stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), client.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}

#[tokio::test]
async fn tls_proxy_terminates_tls_and_relays_payload() {
    use std::sync::Arc;
    use tokio_rustls::rustls::{
        ClientConfig, RootCertStore,
        pki_types::{CertificateDer, ServerName},
    };

    let (target, _echo_task) = start_tcp_echo().await;
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = probe.local_addr().unwrap();
    drop(probe);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let proxy_task = tokio::spawn(async move {
        flux::tcp::tls_proxy(
            &proxy_addr.to_string(),
            &target,
            "tests/fixtures/test-cert.pem",
            "tests/fixtures/test-key.pem",
            flux::tcp::ProxyOptions {
                max_connections: 32,
                ..flux::tcp::ProxyOptions::default()
            },
            shutdown_rx,
        )
        .await
    });
    let tcp = loop {
        match TcpStream::connect(proxy_addr).await {
            Ok(stream) => break stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    let pem = include_bytes!("fixtures/test-cert.pem");
    let certificates = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let certificate: CertificateDer<'static> = certificates.into_iter().next().unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let tls_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let name = ServerName::try_from("localhost").unwrap().to_owned();
    let mut tls = connector.connect(name, tcp).await.unwrap();
    tls.write_all(b"tls-flux").await.unwrap();
    let mut response = [0; 8];
    tokio::time::timeout(Duration::from_secs(2), tls.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"tls-flux");
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}

#[tokio::test]
async fn tcp_proxy_survives_slow_consumer_backpressure() {
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
    let payload = vec![0x5A; 4 * 1024 * 1024];
    client.write_all(&payload).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut response = vec![0; payload.len()];
    tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, payload);
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}

#[tokio::test]
async fn tcp_proxy_handles_many_concurrent_clients() {
    let (target, _echo_task) = start_tcp_echo().await;
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = probe.local_addr().unwrap();
    drop(probe);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let proxy_task = tokio::spawn(async move {
        flux::tcp::proxy(&proxy_addr.to_string(), &target, 32768, shutdown_rx).await
    });
    let mut clients = Vec::new();
    for _ in 0..64 {
        let stream = loop {
            match TcpStream::connect(proxy_addr).await {
                Ok(stream) => break stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        clients.push(tokio::spawn(async move {
            let mut stream = stream;
            stream.write_all(b"concurrent").await.unwrap();
            let mut response = [0; 10];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"concurrent");
        }));
    }
    for client in clients {
        client.await.unwrap();
    }
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), proxy_task).await;
}
