use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "flux",
    version,
    about = "A high-performance networking utility"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Listen {
        #[command(subcommand)]
        protocol: TcpCommand,
    },
    Connect {
        #[command(subcommand)]
        protocol: TcpCommand,
    },
    Proxy {
        #[command(subcommand)]
        protocol: ProxyProtocol,
    },
    Forward {
        #[command(subcommand)]
        protocol: ForwardProtocol,
    },
    Relay {
        #[command(subcommand)]
        protocol: RelayProtocol,
    },
}

#[derive(Subcommand)]
pub enum TcpCommand {
    Tcp { address: String },
}
#[derive(Subcommand)]
pub enum ProxyProtocol {
    Tcp {
        address: String,
        target: String,
        #[arg(long, default_value_t = 32768)]
        buffer_size: usize,
    },
}
#[derive(Subcommand)]
pub enum RelayProtocol {
    Udp { address: String, target: String },
}
#[derive(Subcommand)]
pub enum ForwardProtocol {
    Tcp(ForwardArgs),
}
#[derive(Args)]
pub struct ForwardArgs {
    #[arg(long)]
    pub listen: String,
    #[arg(long)]
    pub target: String,
    #[arg(long, default_value_t = 32768)]
    pub buffer_size: usize,
}
