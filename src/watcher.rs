//! Filesystem watching. `notify` gives us FSEvents on macOS and inotify on
//! Linux; this module turns its raw event stream into debounced, per-path
//! change notifications on a tokio channel.

use anyhow::Result;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Editors save in bursts (write temp file, rename, touch); coalesce those.
///
/// This is the floor on end-to-end sync latency, so it's tunable via
/// `P2PSYNC_DEBOUNCE_MS` for benchmarking the transport on its own.
pub fn debounce() -> Duration {
    match std::env::var("P2PSYNC_DEBOUNCE_MS").ok().and_then(|v| v.parse().ok()) {
        Some(ms) => Duration::from_millis(ms),
        None => Duration::from_millis(40),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsEvent {
    /// File created or modified — read it and diff.
    Touched(PathBuf),
    Removed(PathBuf),
}

/// Should this path participate in sync?
pub fn ignored(rel: &Path) -> bool {
    rel.components().any(|c| {
        let s = c.as_os_str().to_string_lossy();
        s == ".p2psync" || s == ".git" || s == ".DS_Store" || s.ends_with(".swp") || s.starts_with(".#")
    })
}

/// Spawn a watcher thread over `root`. Returns the receiver of debounced events
/// and the live watcher (dropping it stops watching).
pub fn spawn(root: PathBuf) -> Result<(mpsc::UnboundedReceiver<FsEvent>, RecommendedWatcher)> {
    let (raw_tx, raw_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher = RecommendedWatcher::new(raw_tx, Config::default())?;
    watcher.watch(&root, RecursiveMode::Recursive)?;

    let (tx, rx) = mpsc::unbounded_channel::<FsEvent>();
    let debounce = debounce();
    std::thread::spawn(move || {
        // path -> (latest event, first time we saw it in this burst)
        let mut pending: HashMap<PathBuf, (FsEvent, Instant)> = HashMap::new();
        loop {
            // Wait only as long as the soonest pending item needs.
            let timeout = pending
                .values()
                .map(|(_, t)| debounce.saturating_sub(t.elapsed()))
                .min()
                .unwrap_or(Duration::from_millis(200));

            match raw_rx.recv_timeout(timeout) {
                Ok(Ok(event)) => {
                    let kind = match event.kind {
                        EventKind::Remove(_) => Some(false),
                        EventKind::Create(_) | EventKind::Modify(_) => Some(true),
                        // FSEvents coalesces; `Any` shows up for renames too.
                        EventKind::Any | EventKind::Other => Some(true),
                        EventKind::Access(_) => None,
                    };
                    let Some(exists_hint) = kind else { continue };
                    for path in event.paths {
                        if path.is_dir() {
                            continue;
                        }
                        // Trust the filesystem over the event kind: FSEvents
                        // reports coalesced create+delete as a modify.
                        let ev = if path.exists() && exists_hint {
                            FsEvent::Touched(path.clone())
                        } else if !path.exists() {
                            FsEvent::Removed(path.clone())
                        } else {
                            FsEvent::Touched(path.clone())
                        };
                        pending.insert(path, (ev, Instant::now()));
                    }
                }
                Ok(Err(e)) => eprintln!("[watch] error: {e}"),
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }

            let ready: Vec<PathBuf> = pending
                .iter()
                .filter(|(_, (_, t))| t.elapsed() >= debounce)
                .map(|(p, _)| p.clone())
                .collect();
            for p in ready {
                if let Some((ev, _)) = pending.remove(&p) {
                    if tx.send(ev).is_err() {
                        return;
                    }
                }
            }
        }
    });

    Ok((rx, watcher))
}
