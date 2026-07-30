//! The sync engine.
//!
//! All mutable state lives in a single task, so there are no locks around the
//! CRDT documents: filesystem events, peer messages, and timer ticks all
//! funnel into one `Event` channel and are handled sequentially.

use crate::auth::{self, Role};
use crate::binary;
use crate::crdt::{self, Doc, Op, PeerId};
use crate::diff;
use crate::ignore::Ignore;
use crate::net::{self, Stream, Tls};
use crate::watcher::{self, FsEvent};
use crate::wire::{self, read_msg, write_msg, BinDigest, DeltaOp, Msg, TextDigest, BLOCK_SIZE};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use rand::Rng;

/// How long a delete waits before being broadcast, so that a delete+create
/// pair can be recognized as a rename instead.
fn rename_window() -> Duration {
    // Must exceed the watcher's debounce, or a rename's create half arrives
    // after we have already broadcast the delete.
    (watcher::debounce() * 3).max(Duration::from_millis(60))
}

pub struct Config {
    pub root: PathBuf,
    pub peer_id: PeerId,
    pub name: String,
    pub listen: String,
    pub peers: Vec<String>,
    pub tls: bool,
    pub discovery: bool,
    pub verbose: bool,
    /// Pre-shared key. When set, peers must prove knowledge of it to sync.
    pub secret: Option<Vec<u8>>,
}

/// Compact a document once it is this bloated with tombstones, and no sooner.
/// Overridable so the test suite doesn't have to generate 4096 deletions.
fn compaction_threshold() -> usize {
    std::env::var("P2PSYNC_COMPACT_AFTER")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4096)
}

/// How often to consider compacting. Overridable for the same reason.
fn compaction_interval() -> Duration {
    match std::env::var("P2PSYNC_COMPACT_EVERY_MS").ok().and_then(|v| v.parse().ok()) {
        Some(ms) => Duration::from_millis(ms),
        None => Duration::from_secs(30),
    }
}

/// A text file's CRDT document plus the lineage of its id space.
#[derive(Clone, Serialize, Deserialize)]
struct TextEntry {
    doc: Doc,
    /// Peer whose id space this document's base came from. Two documents can
    /// only be merged as CRDTs if their bases agree; see `on_snapshot`.
    base: PeerId,
}

#[derive(Clone, Serialize, Deserialize)]
struct BinEntry {
    hash: String,
    len: u64,
    /// Lamport version for last-writer-wins, immune to clock skew.
    version: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    text: HashMap<String, TextEntry>,
    bin: HashMap<String, BinEntry>,
    /// Every peer we have ever talked to. Compaction waits for all of them,
    /// because changing a document's lineage while a peer is away would
    /// demote that peer's offline edits to last-writer-wins on return.
    #[serde(default)]
    known_peers: std::collections::HashSet<PeerId>,
}

struct PeerConn {
    name: String,
    tx: mpsc::UnboundedSender<Msg>,
    /// Latest (hash, base) this peer reported per text path, from its manifest.
    /// Compaction only proceeds when every connected peer agrees with us.
    digests: HashMap<String, (String, PeerId)>,
}

enum Event {
    Fs(FsEvent),
    Connected {
        peer_id: PeerId,
        name: String,
        tx: mpsc::UnboundedSender<Msg>,
        addr: String,
    },
    Msg {
        from: PeerId,
        msg: Msg,
    },
    Disconnected(PeerId),
    Tick,
}

pub struct Engine {
    cfg: Config,
    state: State,
    peers: HashMap<PeerId, PeerConn>,
    /// Content hashes we just wrote ourselves, per path, so the resulting
    /// filesystem event doesn't get diffed back into "user edits".
    self_writes: HashMap<PathBuf, Vec<(String, Instant)>>,
    /// Deletes waiting out `rename_window()`: (path, content hash, when).
    pending_deletes: Vec<(String, Option<String>, Instant)>,
    /// Addresses with a live connection, shared with the dialer tasks.
    live_addrs: Arc<Mutex<std::collections::HashSet<String>>>,
    dirty: bool,
    ops_sent: u64,
    ops_recv: u64,
    /// When we last printed a throughput line, and the counts at that point.
    last_stats: Instant,
    reported: (u64, u64),
    ignore: Ignore,
    last_compaction_check: Instant,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn mtime_ms(path: &Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(now_ms)
}

impl Engine {
    pub async fn run(cfg: Config) -> Result<()> {
        std::fs::create_dir_all(&cfg.root)?;
        std::fs::create_dir_all(cfg.root.join(".p2psync"))?;
        let root = cfg.root.canonicalize()?;
        let cfg = Config { root, ..cfg };

        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Event>();
        let tls = if cfg.tls { Some(Tls::self_signed()?) } else { None };

        let mut engine = Engine {
            state: State::default(),
            peers: HashMap::new(),
            self_writes: HashMap::new(),
            pending_deletes: Vec::new(),
            live_addrs: Arc::new(Mutex::new(Default::default())),
            dirty: false,
            ops_sent: 0,
            ops_recv: 0,
            last_stats: Instant::now(),
            reported: (0, 0),
            ignore: Ignore::load(&cfg.root),
            last_compaction_check: Instant::now(),
            cfg,
        };
        engine.load_state();
        engine.scan_root()?;

        // Filesystem watcher (kept alive for the process lifetime).
        let (mut fs_rx, _watcher) = watcher::spawn(engine.cfg.root.clone())?;
        {
            let tx = ev_tx.clone();
            tokio::spawn(async move {
                while let Some(ev) = fs_rx.recv().await {
                    if tx.send(Event::Fs(ev)).is_err() {
                        break;
                    }
                }
            });
        }

        // Listener.
        let listener = tokio::net::TcpListener::bind(&engine.cfg.listen)
            .await
            .with_context(|| format!("binding {}", engine.cfg.listen))?;
        let bound = listener.local_addr()?;
        println!(
            "[p2psync] peer {} \"{}\" listening on {} ({})",
            format_id(engine.cfg.peer_id),
            engine.cfg.name,
            bound,
            if engine.cfg.tls { "TLS" } else { "plaintext" }
        );
        println!("[p2psync] syncing {}", engine.cfg.root.display());
        {
            let tx = ev_tx.clone();
            let tls = tls.clone();
            let me = (engine.cfg.peer_id, engine.cfg.name.clone());
            let listen_secret = engine.cfg.secret.clone();
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((tcp, peer_addr)) => {
                            let tx = tx.clone();
                            let tls = tls.clone();
                            let me = me.clone();
                            let secret = listen_secret.clone();
                            tokio::spawn(async move {
                                match net::accept(tcp, tls.as_ref()).await {
                                    Ok(s) => {
                                        if let Err(e) = pump(
                                            s,
                                            me,
                                            tx,
                                            peer_addr.to_string(),
                                            Role::Responder,
                                            secret,
                                        )
                                        .await
                                        {
                                            eprintln!("[net] {peer_addr}: {e:#}");
                                        }
                                    }
                                    Err(e) => eprintln!("[net] accept failed: {e}"),
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("[net] listener error: {e}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            });
        }

        // Outbound dialers: one retry loop per configured address.
        let me = (engine.cfg.peer_id, engine.cfg.name.clone());
        for addr in engine.cfg.peers.clone() {
            spawn_dialer(
                addr,
                me.clone(),
                tls.clone(),
                ev_tx.clone(),
                engine.live_addrs.clone(),
                engine.cfg.secret.clone(),
            );
        }

        let mut _mdns = None;
        if engine.cfg.discovery {
            match crate::discovery::start(engine.cfg.peer_id, bound.port(), engine.cfg.name.clone()) {
                Ok((daemon, mut addr_rx)) => {
                    _mdns = Some(daemon);
                    let me = (engine.cfg.peer_id, engine.cfg.name.clone());
                    let live = engine.live_addrs.clone();
                    let tls = tls.clone();
                    let ev = ev_tx.clone();
                    let secret = engine.cfg.secret.clone();
                    tokio::spawn(async move {
                        while let Some(addr) = addr_rx.recv().await {
                            spawn_dialer(
                                addr,
                                me.clone(),
                                tls.clone(),
                                ev.clone(),
                                live.clone(),
                                secret.clone(),
                            );
                        }
                    });
                }
                Err(e) => eprintln!("[mdns] discovery unavailable: {e}"),
            }
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let tick_tx = ev_tx.clone();
        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                if tick_tx.send(Event::Tick).is_err() {
                    break;
                }
            }
        });

        let mut last_save = Instant::now();
        while let Some(ev) = ev_rx.recv().await {
            if let Err(e) = engine.handle(ev).await {
                eprintln!("[engine] {e:#}");
            }
            if engine.dirty && last_save.elapsed() > Duration::from_secs(2) {
                engine.save_state();
                last_save = Instant::now();
            }
        }
        engine.save_state();
        Ok(())
    }

    // ---- event dispatch ----------------------------------------------

    async fn handle(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Fs(FsEvent::Touched(path)) => self.on_touched(&path),
            Event::Fs(FsEvent::Removed(path)) => {
                self.on_removed(&path);
                Ok(())
            }
            Event::Connected { peer_id, name, tx, addr } => {
                if peer_id == self.cfg.peer_id {
                    return Ok(()); // dialed ourselves
                }
                if self.peers.contains_key(&peer_id) {
                    return Ok(()); // duplicate link (both sides dialed)
                }
                println!("[peer] + {} \"{}\" via {}", format_id(peer_id), name, addr);
                if self.state.known_peers.insert(peer_id) {
                    self.dirty = true;
                }
                self.peers.insert(
                    peer_id,
                    PeerConn {
                        name,
                        tx,
                        digests: HashMap::new(),
                    },
                );
                self.send_catchup(peer_id);
                Ok(())
            }
            Event::Disconnected(peer_id) => {
                if let Some(p) = self.peers.remove(&peer_id) {
                    println!("[peer] - {} \"{}\"", format_id(peer_id), p.name);
                }
                Ok(())
            }
            Event::Msg { from, msg } => self.on_msg(from, msg),
            Event::Tick => {
                self.flush_pending_deletes();
                self.prune_self_writes();
                self.report_stats();
                self.maybe_compact();
                Ok(())
            }
        }
    }

    /// Periodic throughput line, but only when something actually happened.
    fn report_stats(&mut self) {
        if self.last_stats.elapsed() < Duration::from_secs(30) {
            return;
        }
        self.last_stats = Instant::now();
        let now = (self.ops_sent, self.ops_recv);
        if now == self.reported {
            return;
        }
        self.reported = now;
        println!(
            "[stats] {} ops sent, {} ops received, {} text file(s), {} binary file(s), {} peer(s)",
            now.0,
            now.1,
            self.state.text.len(),
            self.state.bin.len(),
            self.peers.len()
        );
    }

    // ---- local filesystem changes ------------------------------------

    fn rel(&self, path: &Path) -> Option<String> {
        let rel = path.strip_prefix(&self.cfg.root).ok()?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        if rel.is_empty() || self.ignore.is_ignored(&rel) {
            return None;
        }
        Some(rel)
    }

    fn on_touched(&mut self, path: &Path) -> Result<()> {
        let Some(rel) = self.rel(path) else { return Ok(()) };
        let Ok(bytes) = std::fs::read(path) else { return Ok(()) };
        let hash = binary::sha256_hex(&bytes);

        // Echo suppression: this is a file we just wrote from a remote op.
        if let Some(list) = self.self_writes.get_mut(path) {
            if let Some(i) = list.iter().position(|(h, _)| *h == hash) {
                list.remove(i);
                return Ok(());
            }
        }

        if binary::is_binary(&bytes) {
            return self.on_local_binary(rel, bytes, hash);
        }
        let text = String::from_utf8_lossy(&bytes).to_string();

        // A create whose content matches a just-deleted file is a rename.
        if !self.state.text.contains_key(&rel) {
            if let Some(i) = self
                .pending_deletes
                .iter()
                .position(|(_, h, _)| h.as_deref() == Some(hash.as_str()))
            {
                let (old_rel, _, _) = self.pending_deletes.remove(i);
                if let Some(entry) = self.state.text.remove(&old_rel) {
                    self.state.text.insert(rel.clone(), entry);
                    self.dirty = true;
                    println!("[sync] rename {old_rel} -> {rel}");
                    self.broadcast(
                        Msg::FileRename {
                            from: old_rel,
                            to: rel,
                        },
                        None,
                    );
                    return Ok(());
                }
            }
        }

        match self.state.text.get_mut(&rel) {
            None => {
                // New text file. Derive the character ids from the content
                // itself, so a peer that already holds these exact bytes builds
                // an identical document and the two merge as a real CRDT
                // instead of colliding as unrelated histories.
                let (mut doc, base) = Doc::from_shared_content_owned(&rel, text);
                doc.set_peer(self.cfg.peer_id);
                let entry = TextEntry { doc, base };
                let snap = Msg::Snapshot {
                    path: rel.clone(),
                    elems: entry.doc.snapshot(),
                    text_hash: hash,
                    base: entry.base,
                    mtime_ms: mtime_ms(path),
                };
                self.state.text.insert(rel.clone(), entry);
                self.dirty = true;
                if self.cfg.verbose {
                    println!(
                        "[sync] new text file {rel} ({} chars)",
                        self.state.text[&rel].doc.len_visible()
                    );
                }
                self.broadcast(Msg::FileCreate { path: rel, binary: false }, None);
                self.broadcast(snap, None);
            }
            Some(entry) => {
                let ops = diff::detect(&mut entry.doc, &text);
                if ops.is_empty() {
                    return Ok(());
                }
                let (ins, del) = count_ops(&ops);
                let base = entry.base;
                self.dirty = true;
                self.ops_sent += ops.len() as u64;
                println!("[sync] {rel} +{ins} -{del} → {} peer(s)", self.peers.len());
                self.broadcast(
                    Msg::Ops {
                        path: rel,
                        ops: wire::compress(&ops),
                        base,
                    },
                    None,
                );
            }
        }
        Ok(())
    }

    fn on_local_binary(&mut self, rel: String, bytes: Vec<u8>, hash: String) -> Result<()> {
        if self.state.bin.get(&rel).map(|b| b.hash.as_str()) == Some(hash.as_str()) {
            return Ok(());
        }
        let is_new = !self.state.bin.contains_key(&rel);
        // Bump the logical clock past anything we've seen for this path.
        let version = self.state.bin.get(&rel).map(|b| b.version).unwrap_or(0) + 1;
        let entry = BinEntry {
            hash: hash.clone(),
            len: bytes.len() as u64,
            version,
        };
        println!("[sync] binary {rel} ({} bytes, {})", entry.len, &hash[..8]);
        if is_new {
            self.broadcast(Msg::FileCreate { path: rel.clone(), binary: true }, None);
        }
        let meta = Msg::BinaryMeta {
            path: rel.clone(),
            hash,
            len: entry.len,
            version: entry.version,
        };
        self.state.bin.insert(rel, entry);
        self.dirty = true;
        self.broadcast(meta, None);
        Ok(())
    }

    fn on_removed(&mut self, path: &Path) {
        let Some(rel) = self.rel(path) else { return };
        let text_hash = self
            .state
            .text
            .get(&rel)
            .map(|e| binary::sha256_hex(e.doc.text().as_bytes()))
            .or_else(|| self.state.bin.get(&rel).map(|b| b.hash.clone()));
        if text_hash.is_none() {
            return;
        }
        // Hold briefly: an editor's atomic-save or a rename shows up as a
        // delete followed immediately by a create.
        self.pending_deletes.push((rel, text_hash, Instant::now()));
    }

    fn flush_pending_deletes(&mut self) {
        let ready: Vec<usize> = self
            .pending_deletes
            .iter()
            .enumerate()
            .filter(|(_, (_, _, t))| t.elapsed() >= rename_window())
            .map(|(i, _)| i)
            .collect();
        for i in ready.into_iter().rev() {
            let (rel, _, _) = self.pending_deletes.remove(i);
            // If it came back (atomic save), the create path already handled it.
            if self.cfg.root.join(&rel).exists() {
                continue;
            }
            self.state.text.remove(&rel);
            self.state.bin.remove(&rel);
            self.dirty = true;
            println!("[sync] delete {rel}");
            self.broadcast(Msg::FileDelete { path: rel }, None);
        }
    }

    // ---- remote messages ---------------------------------------------

    fn on_msg(&mut self, from: PeerId, msg: Msg) -> Result<()> {
        match msg {
            Msg::Hello { .. } | Msg::Auth { .. } => Ok(()), // handled during handshake
            Msg::Manifest { text, bin } => self.on_manifest(from, text, bin),
            Msg::SnapshotRequest { paths } => self.on_snapshot_request(from, paths),
            Msg::Checkpoint { path, elems, base, text_hash } => {
                self.on_checkpoint(from, path, elems, base, text_hash)
            }
            Msg::FileCreate { .. } => Ok(()), // the Snapshot/BinaryMeta carries the content
            Msg::Ops { path, ops, base } => self.on_ops(from, path, ops, base),
            Msg::Snapshot { path, elems, text_hash, base, mtime_ms } => {
                self.on_snapshot(from, path, elems, text_hash, base, mtime_ms)
            }
            Msg::FileDelete { path } => {
                let known = self.state.text.remove(&path).is_some() | self.state.bin.remove(&path).is_some();
                let full = self.cfg.root.join(&path);
                if full.exists() {
                    let _ = std::fs::remove_file(&full);
                    println!("[recv] delete {path}");
                }
                if known {
                    self.dirty = true;
                }
                self.broadcast(Msg::FileDelete { path }, Some(from));
                Ok(())
            }
            Msg::FileRename { from: old, to } => {
                if let Some(entry) = self.state.text.remove(&old) {
                    self.state.text.insert(to.clone(), entry);
                }
                if let Some(entry) = self.state.bin.remove(&old) {
                    self.state.bin.insert(to.clone(), entry);
                }
                let (a, b) = (self.cfg.root.join(&old), self.cfg.root.join(&to));
                if a.exists() {
                    if let Some(p) = b.parent() {
                        let _ = std::fs::create_dir_all(p);
                    }
                    let _ = std::fs::rename(&a, &b);
                    println!("[recv] rename {old} -> {to}");
                }
                self.dirty = true;
                self.broadcast(Msg::FileRename { from: old, to }, Some(from));
                Ok(())
            }
            Msg::BinaryMeta { path, hash, len, version } => {
                self.on_binary_meta(from, path, hash, len, version)
            }
            Msg::BinarySignatures { path, block_size, sigs } => {
                self.on_binary_signatures(from, path, block_size, sigs)
            }
            Msg::BinaryDelta { path, hash, version, ops } => {
                self.on_binary_delta(from, path, hash, version, ops)
            }
        }
    }

    /// Pull any un-detected on-disk edits into the CRDT *before* a remote
    /// change overwrites them.
    ///
    /// Without this there is a lost-update window the width of the watcher's
    /// debounce: the user saves, and before the filesystem event fires we write
    /// merged remote content over the top. The user's edit would then be read
    /// back as "already ours" and vanish.
    fn absorb_local_edits(&mut self, rel: &str) -> Result<()> {
        let full = self.cfg.root.join(rel);
        let Ok(bytes) = std::fs::read(&full) else { return Ok(()) };
        if binary::is_binary(&bytes) {
            return Ok(());
        }
        // Content we wrote ourselves is not a user edit.
        let hash = binary::sha256_hex(&bytes);
        if self
            .self_writes
            .get(&full)
            .is_some_and(|l| l.iter().any(|(h, _)| *h == hash))
        {
            return Ok(());
        }
        let Some(entry) = self.state.text.get_mut(rel) else { return Ok(()) };
        let text = String::from_utf8_lossy(&bytes).to_string();
        if text == entry.doc.text() {
            return Ok(());
        }
        let ops = diff::detect(&mut entry.doc, &text);
        if ops.is_empty() {
            return Ok(());
        }
        let (ins, del) = count_ops(&ops);
        let base = entry.base;
        self.ops_sent += ops.len() as u64;
        self.dirty = true;
        println!("[sync] {rel} +{ins} -{del} (local edit, merged before remote)");
        self.broadcast(
            Msg::Ops {
                path: rel.to_string(),
                ops: wire::compress(&ops),
                base,
            },
            None,
        );
        Ok(())
    }

    fn on_ops(&mut self, from: PeerId, path: String, wire_ops: Vec<wire::WireOp>, base: PeerId) -> Result<()> {
        let ops = wire::expand(&wire_ops);
        self.state.text.entry(path.clone()).or_insert_with(|| TextEntry {
            doc: Doc::new(self.cfg.peer_id),
            base,
        });
        self.absorb_local_edits(&path)?;
        let entry = self.state.text.get_mut(&path).expect("just inserted");
        if entry.base != base {
            // Different id lineages: the snapshot exchange will reconcile the
            // bases, and these ops will be resent afterwards.
            if self.cfg.verbose {
                println!("[recv] {path}: dropping ops from a different base");
            }
            return Ok(());
        }
        let fresh = entry.doc.apply_all(ops);
        if fresh.is_empty() {
            return Ok(());
        }
        self.ops_recv += fresh.len() as u64;
        let (ins, del) = count_ops(&fresh);
        let text = entry.doc.text();
        self.write_file(&path, text.as_bytes())?;
        self.dirty = true;
        println!("[recv] {path} +{ins} -{del} from {}", format_id(from));
        // Relay so partial meshes still converge; duplicates are dropped by
        // `Doc::apply`, which is what stops this from looping forever.
        self.broadcast(
            Msg::Ops {
                path,
                ops: wire::compress(&fresh),
                base,
            },
            Some(from),
        );
        Ok(())
    }

    fn on_snapshot(
        &mut self,
        from: PeerId,
        path: String,
        elems: Vec<crate::crdt::Elem>,
        text_hash: String,
        base: PeerId,
        remote_mtime: u64,
    ) -> Result<()> {
        if self.state.text.contains_key(&path) {
            self.absorb_local_edits(&path)?;
        }
        match self.state.text.get_mut(&path) {
            // Never seen this file: take the remote document as-is.
            None => {
                let mut doc = Doc::new(self.cfg.peer_id);
                doc.adopt(elems);
                let text = doc.text();
                self.state.text.insert(path.clone(), TextEntry { doc, base });
                self.write_file(&path, text.as_bytes())?;
                self.dirty = true;
                println!("[recv] new file {path} from {} ({} chars)", format_id(from), text.chars().count());
                // Relay this new file to our other peers so it propagates through
                // the network. Snapshot straight out of the map: cloning the
                // `TextEntry` here would duplicate the whole arena and slot index.
                self.broadcast(
                    Msg::Snapshot {
                        path: path.clone(),
                        elems: self.state.text[&path].doc.snapshot(),
                        text_hash: text_hash.clone(),
                        base,
                        mtime_ms: mtime_ms(&self.cfg.root.join(&path)),
                    },
                    Some(from),
                );
            }
            // Same lineage: a real CRDT merge.
            Some(entry) if entry.base == base => {
                let old_text = entry.doc.text();
                entry.doc.merge_snapshot(&elems);
                let text = entry.doc.text();
                let text_changed = text != old_text;
                // Save snapshot before write_file to avoid borrow conflicts.
                let entry_snapshot = if text_changed {
                    Some(entry.doc.snapshot())
                } else {
                    None
                };
                self.write_file(&path, text.as_bytes())?;
                self.dirty = true;
                if self.cfg.verbose {
                    println!("[recv] merged snapshot {path} from {}", format_id(from));
                }
                // Relay the merged snapshot only if the visible text actually changed as a result.
                if let Some(snapshot) = entry_snapshot {
                    let new_hash = binary::sha256_hex(text.as_bytes());
                    self.broadcast(
                        Msg::Snapshot {
                            path: path.clone(),
                            elems: snapshot,
                            text_hash: new_hash,
                            base,
                            mtime_ms: mtime_ms(&self.cfg.root.join(&path)),
                        },
                        Some(from),
                    );
                }
            }
            // Two independent histories for the same path. Merging disjoint id
            // spaces would duplicate the text, so the lower base id wins the
            // lineage and the loser rebases onto it. Which *content* survives
            // is then last-writer-wins on mtime.
            Some(entry) => {
                if base > entry.base {
                    return Ok(()); // we win the lineage; the peer will rebase
                }
                let local_text = entry.doc.text();
                let local_hash = binary::sha256_hex(local_text.as_bytes());
                let local_mtime = mtime_ms(&self.cfg.root.join(&path));
                entry.doc.adopt(elems);
                entry.base = base;
                self.dirty = true;

                if local_hash == text_hash {
                    // Same content, different ids — adopting is enough.
                    return Ok(());
                }
                let local_wins = (local_mtime, self.cfg.peer_id) > (remote_mtime, from);
                if local_wins {
                    println!("[recv] {path}: rebased onto {}'s history, keeping local content", format_id(from));
                    let entry = self.state.text.get_mut(&path).unwrap();
                    let ops = diff::detect(&mut entry.doc, &local_text);
                    let base = entry.base;
                    self.broadcast(
                        Msg::Ops {
                            path,
                            ops: wire::compress(&ops),
                            base,
                        },
                        None,
                    );
                } else {
                    let remote_text = self.state.text.get(&path).unwrap().doc.text();
                    // Get snapshot before write_file to avoid borrow conflicts.
                    let entry_snapshot = self.state.text.get(&path).unwrap().doc.snapshot();
                    let remote_hash = binary::sha256_hex(remote_text.as_bytes());
                    let conflict = format!("{path}.conflict-{}", format_id(self.cfg.peer_id));
                    println!("[recv] {path}: remote is newer; local copy saved as {conflict}");
                    self.write_file(&conflict, local_text.as_bytes())?;
                    self.write_file(&path, remote_text.as_bytes())?;
                    // Relay the remote snapshot to our other peers so they also adopt it.
                    self.broadcast(
                        Msg::Snapshot {
                            path: path.clone(),
                            elems: entry_snapshot,
                            text_hash: remote_hash,
                            base,
                            mtime_ms: mtime_ms(&self.cfg.root.join(&path)),
                        },
                        Some(from),
                    );
                }
            }
        }
        Ok(())
    }

    fn on_binary_meta(&mut self, from: PeerId, path: String, hash: String, _len: u64, version: u64) -> Result<()> {
        if let Some(local) = self.state.bin.get(&path) {
            if local.hash == hash {
                // Same bytes: just keep the logical clock monotonic.
                if version > local.version {
                    let mut e = local.clone();
                    e.version = version;
                    self.state.bin.insert(path, e);
                    self.dirty = true;
                }
                return Ok(());
            }
            // Last-writer-wins on the logical clock, peer id as the tiebreak.
            if (local.version, self.cfg.peer_id) > (version, from) {
                return Ok(());
            }
        }
        // Ask for a delta against whatever we currently have.
        let current = std::fs::read(self.cfg.root.join(&path)).unwrap_or_default();
        let sigs = binary::signatures(&current);
        if self.cfg.verbose {
            println!("[recv] {path}: requesting delta ({} local blocks)", sigs.len());
        }
        self.send_to(
            from,
            Msg::BinarySignatures {
                path,
                block_size: BLOCK_SIZE as u32,
                sigs,
            },
        );
        Ok(())
    }

    fn on_binary_signatures(
        &mut self,
        from: PeerId,
        path: String,
        block_size: u32,
        sigs: Vec<(u32, [u8; 8])>,
    ) -> Result<()> {
        if block_size as usize != BLOCK_SIZE {
            anyhow::bail!("peer asked for block size {block_size}, we only speak {BLOCK_SIZE}");
        }
        let full = self.cfg.root.join(&path);
        let Ok(data) = std::fs::read(&full) else { return Ok(()) };
        let ops = binary::delta(&data, &sigs);
        let literal: usize = ops
            .iter()
            .map(|o| match o {
                DeltaOp::Literal(b) => b.len(),
                DeltaOp::CopyBlock(_) => 0,
            })
            .sum();
        println!(
            "[sync] {path}: delta to {} — {} literal bytes of {} ({} block reuses)",
            format_id(from),
            literal,
            data.len(),
            ops.iter().filter(|o| matches!(o, DeltaOp::CopyBlock(_))).count()
        );
        let version = self.state.bin.get(&path).map(|b| b.version).unwrap_or(1);
        self.send_to(
            from,
            Msg::BinaryDelta {
                path,
                hash: binary::sha256_hex(&data),
                version,
                ops,
            },
        );
        Ok(())
    }

    fn on_binary_delta(
        &mut self,
        from: PeerId,
        path: String,
        hash: String,
        version: u64,
        ops: Vec<DeltaOp>,
    ) -> Result<()> {
        let base = std::fs::read(self.cfg.root.join(&path)).unwrap_or_default();
        let rebuilt = binary::apply_delta(&base, &ops);
        let got = binary::sha256_hex(&rebuilt);
        if got != hash {
            anyhow::bail!("{path}: delta from {} reconstructed to {} but peer said {hash}", format_id(from), got);
        }
        self.write_file(&path, &rebuilt)?;
        let version = version.max(self.state.bin.get(&path).map(|b| b.version).unwrap_or(0));

        // Check if the hash changed from what we had before.
        let hash_changed = self.state.bin.get(&path).map(|b| &b.hash) != Some(&hash);

        self.state.bin.insert(
            path.clone(),
            BinEntry {
                hash: hash.clone(),
                len: rebuilt.len() as u64,
                version,
            },
        );
        self.dirty = true;
        println!("[recv] binary {path} ({} bytes) from {}", rebuilt.len(), format_id(from));

        // Relay the binary metadata to our other peers if the content actually changed.
        // This allows peers not directly connected to the sender to discover and fetch the file.
        if hash_changed {
            self.broadcast(
                Msg::BinaryMeta {
                    path: path.clone(),
                    hash,
                    len: rebuilt.len() as u64,
                    version,
                },
                Some(from),
            );
        }
        Ok(())
    }

    // ---- outbound ----------------------------------------------------

    fn broadcast(&mut self, msg: Msg, except: Option<PeerId>) {
        let dead: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|(id, _)| Some(**id) != except)
            .filter(|(_, p)| p.tx.send(msg.clone()).is_err())
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            self.peers.remove(&id);
        }
    }

    fn send_to(&mut self, peer: PeerId, msg: Msg) {
        if let Some(p) = self.peers.get(&peer) {
            if p.tx.send(msg).is_err() {
                self.peers.remove(&peer);
            }
        }
    }

    /// Bring a freshly connected peer up to date.
    ///
    /// Sends a *manifest* of hashes rather than a snapshot per file: the common
    /// reconnect case is that most files already agree, and a full CRDT
    /// snapshot per file is far heavier than a hash. The peer asks for only
    /// what actually differs. This doubles as the reconnect-after-offline
    /// path — a snapshot subsumes any ops missed while the link was down.
    fn send_catchup(&mut self, peer: PeerId) {
        let manifest = self.manifest();
        if let Msg::Manifest { text, bin } = &manifest {
            if !text.is_empty() || !bin.is_empty() {
                println!(
                    "[peer] offering {} text + {} binary file(s) to {}",
                    text.len(),
                    bin.len(),
                    format_id(peer)
                );
            }
        }
        self.send_to(peer, manifest);
    }

    fn manifest(&self) -> Msg {
        Msg::Manifest {
            text: self
                .state
                .text
                .iter()
                .map(|(path, e)| TextDigest {
                    path: path.clone(),
                    hash: binary::sha256_hex(e.doc.text().as_bytes()),
                    base: e.base,
                    tombstones: e.doc.tombstones() as u64,
                })
                .collect(),
            bin: self
                .state
                .bin
                .iter()
                .map(|(path, b)| BinDigest {
                    path: path.clone(),
                    hash: b.hash.clone(),
                    len: b.len,
                    version: b.version,
                })
                .collect(),
        }
    }

    /// Compare a peer's manifest against ours and ask only for what differs.
    fn on_manifest(&mut self, from: PeerId, text: Vec<TextDigest>, bin: Vec<BinDigest>) -> Result<()> {
        let mut want: Vec<String> = Vec::new();
        let mut agreed = 0usize;
        for d in &text {
            match self.state.text.get(&d.path) {
                // Already identical, same lineage: nothing to exchange. This is
                // the case the manifest exists to make free.
                Some(local) if local.base == d.base && binary::sha256_hex(local.doc.text().as_bytes()) == d.hash => {
                    agreed += 1;
                }
                _ => want.push(d.path.clone()),
            }
        }
        // Record what the peer holds, so compaction can require unanimity.
        if let Some(p) = self.peers.get_mut(&from) {
            p.digests = text.into_iter().map(|d| (d.path, (d.hash, d.base))).collect();
        }

        if self.cfg.verbose || !want.is_empty() {
            println!(
                "[peer] {}: {agreed} file(s) already in sync, requesting {}",
                format_id(from),
                want.len()
            );
        }
        if !want.is_empty() {
            self.send_to(from, Msg::SnapshotRequest { paths: want });
        }
        // Binary files are compared by hash already, so reuse that path.
        for d in bin {
            self.on_binary_meta(from, d.path, d.hash, d.len, d.version)?;
        }
        Ok(())
    }

    fn on_snapshot_request(&mut self, from: PeerId, paths: Vec<String>) -> Result<()> {
        for path in paths {
            let Some(entry) = self.state.text.get(&path) else { continue };
            let msg = Msg::Snapshot {
                path: path.clone(),
                elems: entry.doc.snapshot(),
                text_hash: binary::sha256_hex(entry.doc.text().as_bytes()),
                base: entry.base,
                mtime_ms: mtime_ms(&self.cfg.root.join(&path)),
            };
            self.send_to(from, msg);
        }
        Ok(())
    }

    /// Drop tombstones once every connected peer agrees on the visible text.
    ///
    /// Safe tombstone collection needs causal stability — proof that every
    /// replica has seen the deletions. Unanimous agreement on the text hash is
    /// that proof for the peers we can see. The compacted document gets a fresh
    /// content-derived lineage, so peers rebuild identical id spaces; a peer
    /// that was offline for the checkpoint comes back on an older lineage and
    /// reconciles through the normal snapshot path.
    fn maybe_compact(&mut self) {
        if self.last_compaction_check.elapsed() < compaction_interval() {
            return;
        }
        self.last_compaction_check = Instant::now();
        if self.peers.is_empty() {
            return; // no proof anyone else has the deletions
        }

        // Re-advertise what we hold. Digests captured at connect time go stale
        // the moment anyone edits, so without this refresh the unanimity check
        // below could never be satisfied and compaction would never run. It
        // doubles as anti-entropy: a peer that somehow drifted will notice the
        // hash mismatch and ask for a snapshot.
        let manifest = self.manifest();
        self.broadcast(manifest, None);

        let threshold = compaction_threshold();
        // Only the lowest-numbered peer issues checkpoints, so two peers don't
        // race to compact the same document into different lineages.
        if self.peers.keys().any(|p| *p < self.cfg.peer_id) {
            return;
        }
        // Every peer we have ever seen must be present. A checkpoint rewrites
        // the id space, so an absent peer would come back on a dead lineage and
        // have its offline edits resolved by last-writer-wins instead of merged.
        if !self.state.known_peers.iter().all(|p| self.peers.contains_key(p)) {
            if self.cfg.verbose {
                println!(
                    "[compact] deferring: {} of {} known peer(s) connected",
                    self.peers.len(),
                    self.state.known_peers.len()
                );
            }
            return;
        }

        let candidates: Vec<(String, String)> = self
            .state
            .text
            .iter()
            .filter(|(_, e)| e.doc.tombstones() >= threshold && e.doc.tombstones() * 2 >= e.doc.len_raw())
            .map(|(p, e)| (p.clone(), binary::sha256_hex(e.doc.text().as_bytes())))
            .collect();

        for (path, hash) in candidates {
            let base = self.state.text[&path].base;
            let unanimous = self
                .peers
                .values()
                .all(|p| p.digests.get(&path) == Some(&(hash.clone(), base)));
            if !unanimous {
                continue;
            }
            let text = self.state.text[&path].doc.text();
            let (mut doc, new_base) = Doc::from_shared_content_owned(&path, text);
            doc.set_peer(self.cfg.peer_id);
            if new_base == base {
                continue; // already compact under this lineage
            }
            let before = self.state.text[&path].doc.len_raw();
            let elems = doc.snapshot();
            println!(
                "[compact] {path}: {before} elements -> {} (dropped {} tombstones)",
                elems.len(),
                before - elems.len()
            );
            self.state.text.insert(path.clone(), TextEntry { doc, base: new_base });
            self.dirty = true;
            self.broadcast(
                Msg::Checkpoint {
                    path,
                    elems,
                    base: new_base,
                    text_hash: hash,
                },
                None,
            );
        }
    }

    /// Adopt a compacted document, but only if our visible text already matches
    /// it — otherwise we would silently discard local edits.
    fn on_checkpoint(
        &mut self,
        from: PeerId,
        path: String,
        elems: Vec<crdt::Elem>,
        base: PeerId,
        text_hash: String,
    ) -> Result<()> {
        self.absorb_local_edits(&path)?;
        let Some(entry) = self.state.text.get_mut(&path) else { return Ok(()) };
        let local_hash = binary::sha256_hex(entry.doc.text().as_bytes());
        if local_hash != text_hash {
            if self.cfg.verbose {
                println!("[recv] {path}: declining checkpoint from {} (text differs)", format_id(from));
            }
            return Ok(());
        }
        if entry.base == base {
            return Ok(());
        }
        let before = entry.doc.len_raw();
        entry.doc.adopt(elems);
        entry.doc.set_peer(self.cfg.peer_id);
        entry.base = base;
        self.dirty = true;
        println!(
            "[recv] {path}: compacted {before} -> {} elements per checkpoint from {}",
            self.state.text[&path].doc.len_raw(),
            format_id(from)
        );
        Ok(())
    }

    // ---- disk --------------------------------------------------------

    /// Write a file on behalf of a remote change, remembering the content hash
    /// so the resulting filesystem event is recognized as our own.
    fn write_file(&mut self, rel: &str, content: &[u8]) -> Result<()> {
        let full = self.cfg.root.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let hash = binary::sha256_hex(content);
        let list = self.self_writes.entry(full.clone()).or_default();
        list.push((hash, Instant::now()));
        if list.len() > 16 {
            list.remove(0);
        }
        // Write to a temp file and rename, so a reader never sees a half file.
        let tmp = full.with_extension(format!(
            "p2ptmp{}",
            std::process::id()
        ));
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, &full)?;
        Ok(())
    }

    fn prune_self_writes(&mut self) {
        for list in self.self_writes.values_mut() {
            list.retain(|(_, t)| t.elapsed() < Duration::from_secs(10));
        }
        self.self_writes.retain(|_, l| !l.is_empty());
    }

    fn state_path(&self) -> PathBuf {
        self.cfg.root.join(".p2psync").join("state.msgpack")
    }

    fn load_state(&mut self) {
        // Read through a buffer rather than slurping the whole file: the state
        // is far larger than the documents it describes.
        let Ok(file) = std::fs::File::open(self.state_path()) else { return };
        let rd = std::io::BufReader::new(file);
        match rmp_serde::from_read::<_, State>(rd) {
            Ok(mut s) => {
                // Documents keep their persisted ids; only the local minting
                // identity is rebound to this process's peer id.
                for entry in s.text.values_mut() {
                    entry.doc.set_peer(self.cfg.peer_id);
                }
                println!(
                    "[state] restored {} text file(s), {} binary file(s)",
                    s.text.len(),
                    s.bin.len()
                );
                self.state = s;
            }
            Err(e) => eprintln!("[state] ignoring unreadable state file: {e}"),
        }
    }

    /// Persist to a temp file and rename, so a crash mid-write can't truncate
    /// the real state file.
    ///
    /// Serialization streams into the file rather than building the whole
    /// encoding in memory first: this state is tens of megabytes for a document
    /// of a few hundred KB, and it is rewritten every two seconds while the user
    /// types. `to_vec_named` held all of it as one `Vec<u8>`.
    fn save_state(&mut self) {
        use serde::Serialize;
        use std::io::Write;

        let p = self.state_path();
        let tmp = p.with_extension("tmp");
        let Ok(file) = std::fs::File::create(&tmp) else { return };
        let mut wr = std::io::BufWriter::new(file);
        // `with_struct_map` keeps the field-name encoding `to_vec_named`
        // produced, so state files written by older builds still load.
        let mut ser = rmp_serde::Serializer::new(&mut wr).with_struct_map();
        if self.state.serialize(&mut ser).is_ok() && wr.flush().is_ok() {
            drop(wr);
            let _ = std::fs::rename(&tmp, &p);
        }
        self.dirty = false;
    }

    /// Walk the root at startup so pre-existing files are under management.
    fn scan_root(&mut self) -> Result<()> {
        let mut stack = vec![self.cfg.root.clone()];
        let mut files = Vec::new();
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let Some(_) = self.rel(&path) else { continue };
                if path.is_dir() {
                    stack.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        for path in files {
            let Some(rel) = self.rel(&path) else { continue };
            let Ok(bytes) = std::fs::read(&path) else { continue };
            let hash = binary::sha256_hex(&bytes);
            if binary::is_binary(&bytes) {
                if self.state.bin.get(&rel).map(|b| b.hash.as_str()) != Some(hash.as_str()) {
                    self.state.bin.insert(
                        rel,
                        BinEntry {
                            hash,
                            len: bytes.len() as u64,
                            version: 1,
                        },
                    );
                    self.dirty = true;
                }
                continue;
            }
            let text = String::from_utf8_lossy(&bytes).to_string();
            match self.state.text.get_mut(&rel) {
                // Restored state, but the file changed while we were down.
                Some(entry) => {
                    let ops = diff::detect(&mut entry.doc, &text);
                    if !ops.is_empty() {
                        self.dirty = true;
                    }
                }
                None => {
                    // Content-derived lineage: two machines that were handed
                    // the same folder converge without a conflict copy.
                    let (mut doc, base) = Doc::from_shared_content_owned(&rel, text);
                    doc.set_peer(self.cfg.peer_id);
                    self.state.text.insert(rel, TextEntry { doc, base });
                    self.dirty = true;
                }
            }
        }
        Ok(())
    }
}

fn count_ops(ops: &[Op]) -> (usize, usize) {
    let ins = ops.iter().filter(|o| matches!(o, Op::Insert { .. })).count();
    (ins, ops.len() - ins)
}

pub fn format_id(id: PeerId) -> String {
    format!("{:08x}", id & 0xffff_ffff)
}

/// Per-address backoff and error tracking state.
struct DialerState {
    /// Current backoff delay in milliseconds.
    delay_ms: u64,
    /// Number of consecutive failures.
    consecutive_failures: u64,
    /// Last error message we logged (to suppress repeats).
    last_error: Option<String>,
    /// Number of identical errors suppressed since the last log.
    suppressed_count: u64,
}

impl DialerState {
    fn new() -> Self {
        DialerState {
            delay_ms: 1000,
            consecutive_failures: 0,
            last_error: None,
            suppressed_count: 0,
        }
    }

    /// Reset backoff on successful connection.
    fn on_success(&mut self) {
        self.delay_ms = 1000;
        self.consecutive_failures = 0;
        // Don't reset last_error/suppressed_count so we can detect transitions.
    }

    /// Update delay on failure, with exponential backoff capped at 30s.
    fn on_failure(&mut self) {
        self.consecutive_failures += 1;
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s, 30s...
        self.delay_ms = (self.delay_ms * 2).min(30_000);
    }

    /// Get the sleep duration with jitter (+/- 10-20%).
    fn get_delay_with_jitter(&self) -> Duration {
        let mut rng = rand::thread_rng();
        // Jitter: +/- 15% (10-20% range)
        let jitter_pct = rng.gen_range(85..=115) as f64 / 100.0;
        let jittered_ms = (self.delay_ms as f64 * jitter_pct) as u64;
        Duration::from_millis(jittered_ms)
    }

    /// Log the error, suppressing repeats.
    fn log_error(&mut self, addr: &str, error: &str) {
        let error_str = error.to_string();
        if self.last_error.as_ref() == Some(&error_str) {
            // Same error as before; suppress and count.
            self.suppressed_count += 1;
        } else {
            // Different error or first time; emit any suppression summary and log the new one.
            if let Some(prev) = self.last_error.take() {
                if self.suppressed_count > 0 {
                    eprintln!(
                        "[net] {}: still unreachable ({} attempts suppressed, error: {})",
                        addr, self.suppressed_count, prev
                    );
                }
            }
            eprintln!("[net] {}: {}", addr, error_str);
            self.last_error = Some(error_str);
            self.suppressed_count = 0;
        }
    }

    /// Emit a periodic summary of suppressed errors.
    fn log_suppression_summary(&mut self, addr: &str) {
        if let Some(error) = &self.last_error {
            if self.suppressed_count > 0 {
                eprintln!(
                    "[net] {}: still unreachable ({} attempts suppressed, error: {})",
                    addr, self.suppressed_count, error
                );
                self.suppressed_count = 0;
            }
        }
    }
}

/// Keep one outbound connection to `addr` alive, retrying after it drops.
///
/// This is what makes reconnect-after-offline work: the dialer keeps knocking,
/// and `send_catchup` replays state once the link is back.
fn spawn_dialer(
    addr: String,
    me: (PeerId, String),
    tls: Option<Tls>,
    ev_tx: mpsc::UnboundedSender<Event>,
    live: Arc<Mutex<std::collections::HashSet<String>>>,
    secret: Option<Vec<u8>>,
) {
    tokio::spawn(async move {
        let backoff_states = Arc::new(Mutex::new(HashMap::<String, DialerState>::new()));
        loop {
            let already = live.lock().await.contains(&addr);
            if !already {
                if let Ok(s) = net::connect(&addr, tls.as_ref()).await {
                    live.lock().await.insert(addr.clone());
                    // Time the link. Reaching the far end's TCP port is *not*
                    // success on its own: a peer with the wrong secret accepts
                    // the connection and then rejects the handshake in
                    // milliseconds. Resetting the backoff there would leave a
                    // misconfigured pair hammering each other forever, which is
                    // the exact behavior this backoff exists to stop.
                    let started = std::time::Instant::now();
                    let outcome = pump(
                        s,
                        me.clone(),
                        ev_tx.clone(),
                        addr.clone(),
                        Role::Initiator,
                        secret.clone(),
                    )
                    .await;
                    let lasted = started.elapsed();
                    {
                        let mut states = backoff_states.lock().await;
                        let state = states.entry(addr.clone()).or_insert_with(DialerState::new);
                        match outcome {
                            Ok(()) => state.on_success(),
                            Err(e) => {
                                state.log_error(&addr, &format!("{e:#}"));
                                // A link that carried traffic for a while and
                                // then dropped is a healthy peer going away —
                                // redial it promptly. One that dies immediately
                                // is a misconfiguration; back off.
                                if lasted >= Duration::from_secs(5) {
                                    state.on_success();
                                } else {
                                    state.on_failure();
                                }
                            }
                        }
                    }
                    live.lock().await.remove(&addr);
                } else {
                    // Connection failed to establish.
                    let error_msg = "connection refused";
                    let mut states = backoff_states.lock().await;
                    let state = states.entry(addr.clone()).or_insert_with(DialerState::new);
                    state.log_error(&addr, &error_msg);
                    state.on_failure();
                }
            }

            // Get the backoff delay with jitter.
            let delay = {
                let mut states = backoff_states.lock().await;
                let state = states.entry(addr.clone()).or_insert_with(DialerState::new);
                state.get_delay_with_jitter()
            };

            // Periodically emit suppression summaries (roughly every 30+ seconds).
            tokio::time::sleep(delay).await;

            // After the delay, emit a summary if many errors were suppressed.
            {
                let mut states = backoff_states.lock().await;
                if let Some(state) = states.get_mut(&addr) {
                    if state.suppressed_count > 10 {
                        state.log_suppression_summary(&addr);
                    }
                }
            }
        }
    });
}

/// Handshake, then shuttle messages between the socket and the engine until the
/// connection drops.
#[allow(clippy::too_many_arguments)]
async fn pump(
    stream: Stream,
    me: (PeerId, String),
    ev_tx: mpsc::UnboundedSender<Event>,
    addr: String,
    role: Role,
    secret: Option<Vec<u8>>,
) -> Result<()> {
    // Bind authentication to this TLS certificate before consuming the stream.
    let binding = stream.channel_binding();
    let (mut rd, mut wr) = stream.split();

    let our_nonce = auth::random_nonce();
    write_msg(
        &mut wr,
        &Msg::Hello {
            peer_id: me.0,
            name: me.1.clone(),
            nonce: our_nonce.to_vec(),
            authenticated: secret.is_some(),
        },
    )
    .await?;
    let (peer_id, name, their_nonce, they_authenticate) = match read_msg(&mut rd).await? {
        Msg::Hello {
            peer_id,
            name,
            nonce,
            authenticated,
        } => {
            let n: auth::Nonce = nonce
                .try_into()
                .map_err(|_| anyhow::anyhow!("peer sent a malformed nonce"))?;
            (peer_id, name, n, authenticated)
        }
        other => anyhow::bail!("expected Hello, got {other:?}"),
    };
    if peer_id == me.0 {
        return Ok(()); // that's us on the other end
    }

    // Refuse to sync in the clear with a peer that expects authentication, or
    // vice versa — a silent downgrade is worse than a visible failure.
    match (&secret, they_authenticate) {
        (Some(secret), true) => {
            let (init_nonce, resp_nonce) = match role {
                Role::Initiator => (our_nonce, their_nonce),
                Role::Responder => (their_nonce, our_nonce),
            };
            let proof = auth::mac(secret, role, &init_nonce, &resp_nonce, binding.as_ref());
            write_msg(&mut wr, &Msg::Auth { mac: proof }).await?;
            let presented = match read_msg(&mut rd).await? {
                Msg::Auth { mac } => mac,
                other => anyhow::bail!("expected Auth, got {other:?}"),
            };
            auth::verify(
                secret,
                role.peer(),
                &init_nonce,
                &resp_nonce,
                binding.as_ref(),
                &presented,
            )?;
            if binding.is_none() {
                eprintln!(
                    "[auth] {} authenticated over plaintext: authorized, but not protected \
                     against an active man-in-the-middle. Add --tls.",
                    format_id(peer_id)
                );
            }
        }
        (None, false) => {}
        (Some(_), false) => anyhow::bail!(
            "peer {} does not authenticate but we require a shared secret",
            format_id(peer_id)
        ),
        (None, true) => anyhow::bail!(
            "peer {} requires a shared secret; start this peer with --secret",
            format_id(peer_id)
        ),
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
    ev_tx.send(Event::Connected {
        peer_id,
        name,
        tx,
        addr,
    })?;

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write_msg(&mut wr, &msg).await.is_err() {
                break;
            }
        }
    });

    // Read until the peer goes away or the engine shuts down.
    while let Ok(msg) = read_msg(&mut rd).await {
        if ev_tx.send(Event::Msg { from: peer_id, msg }).is_err() {
            break;
        }
    }
    writer.abort();
    let _ = ev_tx.send(Event::Disconnected(peer_id));
    Ok(())
}
