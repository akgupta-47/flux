mod cli;
use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use flux::{tcp, udp};
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
        Command::Connect {
            protocol: cli::TcpCommand::Tcp { address },
        } => tcp::connect(&address).await?,
        Command::Proxy {
            protocol:
                cli::ProxyProtocol::Tcp {
                    address,
                    target,
                    buffer_size,
                },
        } => tcp::proxy(&address, &target, buffer_size, shutdown_rx).await?,
        Command::Forward {
            protocol: cli::ForwardProtocol::Tcp(args),
        } => tcp::proxy(&args.listen, &args.target, args.buffer_size, shutdown_rx).await?,
        Command::Relay {
            protocol: cli::RelayProtocol::Udp { address, target },
        } => udp::relay(&address, &target, shutdown_rx).await?,
    }
    Ok(())
}
