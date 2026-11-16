mod cli;
use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use flux::{
    config::{self, ProxySettings},
    observability::{self, Metrics},
    tcp::{self, ProxyOptions},
    udp,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
    let cli = Cli::parse();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for shutdown signal");
        }
        let _ = shutdown_tx.send(true);
    });
    match cli.command {
        Command::Listen {
            protocol: cli::TcpCommand::Tcp { address },
        } => tcp::listen(&address, shutdown_rx).await?,
        Command::Listen {
            protocol: cli::TcpCommand::Udp { address },
        } => udp::listen(&address, shutdown_rx).await?,
        Command::Connect {
            protocol: cli::TcpCommand::Tcp { address },
        } => tcp::connect(&address).await?,
        Command::Connect {
            protocol: cli::TcpCommand::Udp { address },
        } => udp::connect(&address, shutdown_rx).await?,
        Command::Proxy {
            protocol:
                cli::ProxyProtocol::Tls {
                    address,
                    target,
                    cert,
                    key,
                    buffer_size,
                    max_connections,
                    keepalive_secs,
                    idle_timeout_secs,
                },
        } => {
            tcp::tls_proxy(
                &address,
                &target,
                &cert,
                &key,
                ProxyOptions {
                    buffer_size,
                    max_connections,
                    keepalive_secs,
                    idle_timeout_secs,
                },
                shutdown_rx,
            )
            .await?;
        }
        Command::Proxy {
            protocol:
                cli::ProxyProtocol::Tcp {
                    address,
                    target,
                    buffer_size,
                    max_connections,
                    keepalive_secs,
                    idle_timeout_secs,
                    metrics,
                    config: config_path,
                },
        } => {
            run_tcp_proxy(
                &address,
                &target,
                ProxySettings {
                    buffer_size,
                    max_connections,
                    keepalive_secs,
                    idle_timeout_secs,
                    metrics,
                },
                config_path,
                shutdown_rx,
            )
            .await?
        }
        Command::Forward {
            protocol: cli::ForwardProtocol::Tcp(args),
        } => {
            run_tcp_proxy(
                &args.listen,
                &args.target,
                ProxySettings {
                    buffer_size: args.buffer_size,
                    max_connections: args.max_connections,
                    keepalive_secs: args.keepalive_secs,
                    idle_timeout_secs: args.idle_timeout_secs,
                    metrics: args.metrics,
                },
                args.config,
                shutdown_rx,
            )
            .await?
        }
        Command::Relay {
            protocol: cli::RelayProtocol::Udp { address, target },
        } => udp::relay(&address, &target, shutdown_rx).await?,
        Command::Connections { endpoint } => {
            let mut stream = TcpStream::connect(&endpoint).await?;
            stream
                .write_all(b"GET /connections HTTP/1.1\r\nHost: flux\r\nConnection: close\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            let response = String::from_utf8_lossy(&response);
            print!(
                "{}",
                response
                    .split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or(&response)
            );
        }
    }
    Ok(())
}

async fn run_tcp_proxy(
    address: &str,
    target: &str,
    cli_settings: ProxySettings,
    config_path: Option<String>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let file_settings = match config_path {
        Some(path) => config::load_proxy_settings(&path).await?,
        None => ProxySettings::default(),
    };
    let options = ProxyOptions {
        buffer_size: cli_settings
            .buffer_size
            .or(file_settings.buffer_size)
            .unwrap_or(32768),
        max_connections: cli_settings
            .max_connections
            .or(file_settings.max_connections)
            .unwrap_or(1024),
        keepalive_secs: cli_settings
            .keepalive_secs
            .or(file_settings.keepalive_secs)
            .unwrap_or(60),
        idle_timeout_secs: cli_settings
            .idle_timeout_secs
            .or(file_settings.idle_timeout_secs)
            .unwrap_or(300),
    };
    if let Some(metrics_address) = cli_settings.metrics.or(file_settings.metrics) {
        let stats = Arc::new(Metrics::default());
        let endpoint_stats = stats.clone();
        let endpoint_shutdown = shutdown.clone();
        tokio::spawn(async move {
            observability::serve(&metrics_address, endpoint_stats, endpoint_shutdown).await
        });
        tcp::proxy_with_metrics(address, target, options, stats, shutdown).await
    } else {
        tcp::proxy_with_options(address, target, options, shutdown).await
    }
}
