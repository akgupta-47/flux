use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ProxyBackend {
    #[default]
    Tokio,
    IoUring,
}

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
    Connections {
        #[arg(long, default_value = "127.0.0.1:9090")]
        endpoint: String,
    },
}

#[derive(Subcommand)]
pub enum TcpCommand {
    Tcp { address: String },
    Udp { address: String },
}
#[derive(Subcommand)]
pub enum ProxyProtocol {
    Tls {
        address: String,
        target: String,
        #[arg(long)]
        cert: String,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 32768)]
        buffer_size: usize,
        #[arg(long, default_value_t = 1024)]
        max_connections: usize,
        #[arg(long, default_value_t = 60)]
        keepalive_secs: u64,
        #[arg(long, default_value_t = 300)]
        idle_timeout_secs: u64,
    },
    Tcp {
        address: String,
        target: String,
        #[arg(long)]
        buffer_size: Option<usize>,
        #[arg(long)]
        max_connections: Option<usize>,
        #[arg(long)]
        keepalive_secs: Option<u64>,
        #[arg(long)]
        idle_timeout_secs: Option<u64>,
        #[arg(long)]
        metrics: Option<String>,
        #[arg(long)]
        config: Option<String>,
        #[arg(long, value_enum, default_value = "tokio")]
        backend: ProxyBackend,
        #[arg(long)]
        send_buffer_size: Option<usize>,
        #[arg(long)]
        recv_buffer_size: Option<usize>,
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
    #[arg(long)]
    pub buffer_size: Option<usize>,
    #[arg(long)]
    pub max_connections: Option<usize>,
    #[arg(long)]
    pub keepalive_secs: Option<u64>,
    #[arg(long)]
    pub idle_timeout_secs: Option<u64>,
    #[arg(long)]
    pub metrics: Option<String>,
    #[arg(long)]
    pub config: Option<String>,
    #[arg(long, value_enum, default_value = "tokio")]
    pub backend: ProxyBackend,
    #[arg(long)]
    pub send_buffer_size: Option<usize>,
    #[arg(long)]
    pub recv_buffer_size: Option<usize>,
}
