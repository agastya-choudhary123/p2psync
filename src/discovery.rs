//! Peer discovery over mDNS (Bonjour on macOS).
//!
//! Each peer advertises `_p2psync._tcp.local.` with its peer id in a TXT
//! record. Discovered addresses come back on a channel; the engine decides
//! what to dial.

use crate::crdt::PeerId;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashSet;
use tokio::sync::mpsc;

const SERVICE: &str = "_p2psync._tcp.local.";

/// Register our service and start browsing. The returned daemon must be kept
/// alive; dropping it withdraws the advertisement.
pub fn start(
    peer_id: PeerId,
    port: u16,
    name: String,
) -> anyhow::Result<(ServiceDaemon, mpsc::UnboundedReceiver<String>)> {
    let daemon = ServiceDaemon::new()?;
    let instance = format!("p2psync-{:08x}", peer_id & 0xffff_ffff);
    let host = format!("{instance}.local.");
    let props = [("peer_id", peer_id.to_string()), ("name", name)];
    let info = ServiceInfo::new(SERVICE, &instance, &host, (), port, &props[..])?.enable_addr_auto();
    daemon.register(info)?;
    println!("[mdns] advertising {instance} on port {port}");

    let receiver = daemon.browse(SERVICE)?;
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        let mut seen: HashSet<String> = HashSet::new();
        while let Ok(event) = receiver.recv() {
            if let ServiceEvent::ServiceResolved(info) = event {
                let their_id: Option<u64> = info
                    .get_property_val_str("peer_id")
                    .and_then(|v| v.parse().ok());
                if their_id == Some(peer_id) {
                    continue; // ourselves
                }
                for addr in info.get_addresses() {
                    if addr.is_ipv6() {
                        continue;
                    }
                    let target = format!("{}:{}", addr, info.get_port());
                    if seen.insert(target.clone()) {
                        println!("[mdns] discovered peer at {target}");
                        if tx.send(target).is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });
    Ok((daemon, rx))
}
