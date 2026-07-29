//! p2psync — peer-to-peer file synchronization with streaming CRDT sync.

use p2psync::engine;

use anyhow::Result;
use clap::Parser;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "p2psync", about = "Peer-to-peer file sync with streaming CRDT merge")]
struct Args {
    /// Directory to keep in sync.
    dir: PathBuf,

    /// Address to listen on.
    #[arg(short, long, default_value = "0.0.0.0:7878")]
    listen: String,

    /// Peer to connect to (repeatable), e.g. 192.168.1.20:7878.
    #[arg(short, long = "peer")]
    peers: Vec<String>,

    /// Human-readable name for this peer.
    #[arg(short, long)]
    name: Option<String>,

    /// Encrypt peer links with TLS. All peers must agree.
    #[arg(long)]
    tls: bool,

    /// Find peers on the local network via mDNS.
    #[arg(long)]
    discover: bool,

    /// Log merges and snapshots, not just edits.
    #[arg(short, long)]
    verbose: bool,
}

/// Stable per-directory identity, so a restarted peer keeps its id — and with
/// it, ownership of the character ids already in flight.
fn peer_id_for(dir: &Path, name: &str) -> Result<u64> {
    let path = dir.join(".p2psync").join("peer_id");
    if let Ok(s) = std::fs::read_to_string(&path) {
        if let Ok(id) = s.trim().parse::<u64>() {
            return Ok(id);
        }
    }
    // Mix randomness with the name so two peers can't collide.
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    rand::random::<u64>().hash(&mut hasher);
    let id = hasher.finish();
    std::fs::create_dir_all(dir.join(".p2psync"))?;
    std::fs::write(&path, id.to_string())?;
    Ok(id)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.dir)?;
    let name = args.name.clone().unwrap_or_else(|| {
        args.dir
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "peer".into())
    });
    let peer_id = peer_id_for(&args.dir, &name)?;

    let cfg = engine::Config {
        root: args.dir,
        peer_id,
        name,
        listen: args.listen,
        peers: args.peers,
        tls: args.tls,
        discovery: args.discover,
        verbose: args.verbose,
    };
    engine::Engine::run(cfg).await
}
