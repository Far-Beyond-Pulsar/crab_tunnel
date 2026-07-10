use std::net::SocketAddr;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "crab-tunnel-server")]
#[command(about = "UDP hole-punch rendezvous server")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:3478")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    info!("Starting crab-tunnel-server on {}", args.bind);

    let server = crab_tunnel_core::RendezvousServer::bind(args.bind).await?;
    server.run().await?;

    Ok(())
}
