//! Wire protocol: length-prefixed MessagePack frames.
//!
//! ```text
//! [4 bytes big-endian: payload length][MessagePack-encoded Msg]
//! ```

use crate::crdt::{CharId, Elem, Op, PeerId};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Refuse frames larger than this so a bad peer can't make us allocate wildly.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Block size for the rsync-style binary delta transfer.
pub const BLOCK_SIZE: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Msg {
    /// First frame on every connection. The nonce feeds the authentication
    /// handshake; `authenticated` lets a peer say it expects one, so a
    /// misconfigured pair fails loudly instead of syncing unprotected.
    Hello {
        peer_id: PeerId,
        name: String,
        #[serde(with = "serde_bytes")]
        nonce: Vec<u8>,
        authenticated: bool,
    },

    /// Proof of the shared secret; see `crate::auth`.
    Auth {
        #[serde(with = "serde_bytes")]
        mac: Vec<u8>,
    },

    /// What we have, so the peer can ask only for what it's missing. Sent on
    /// connect in place of a full snapshot per file.
    Manifest {
        text: Vec<TextDigest>,
        bin: Vec<BinDigest>,
    },

    /// "Send me the full CRDT state for these paths."
    SnapshotRequest { paths: Vec<String> },

    /// A compacted replacement document: same visible text, tombstones dropped,
    /// new content-derived lineage. Adopted only by peers whose text already
    /// matches `text_hash`.
    Checkpoint {
        path: String,
        elems: Vec<Elem>,
        base: PeerId,
        text_hash: String,
    },

    /// Streaming CRDT operations for one text file.
    Ops {
        path: String,
        ops: Vec<WireOp>,
        /// Lineage of the sender's document (see `engine::TextEntry::base`).
        base: PeerId,
    },

    /// Full CRDT state for one text file, sent on connect for catch-up.
    Snapshot {
        path: String,
        elems: Vec<Elem>,
        /// Hash of the visible text, so the receiver can skip identical docs.
        text_hash: String,
        base: PeerId,
        /// The file's mtime, for last-writer-wins across unrelated histories.
        mtime_ms: u64,
    },

    FileCreate { path: String, binary: bool },
    FileDelete { path: String },
    FileRename { from: String, to: String },

    /// Binary file changed; `hash` is SHA-256 of the whole file.
    BinaryMeta {
        path: String,
        hash: String,
        len: u64,
        /// Logical (Lamport) version, not a timestamp: last-writer-wins on a
        /// wall clock picks the wrong winner whenever machine clocks disagree.
        version: u64,
    },

    /// "I have a different version; here are my block signatures — send me a
    /// delta against them." (rsync, receiver-driven.)
    BinarySignatures {
        path: String,
        block_size: u32,
        /// (rolling checksum, strong hash) per block, in order.
        sigs: Vec<(u32, [u8; 8])>,
    },

    /// Reconstruction instructions against the requester's own blocks.
    BinaryDelta {
        path: String,
        hash: String,
        version: u64,
        ops: Vec<DeltaOp>,
    },
}

/// One text file's identity in a `Manifest`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextDigest {
    pub path: String,
    /// SHA-256 of the visible text.
    pub hash: String,
    pub base: PeerId,
    /// Stored elements vs. visible characters, so peers can agree on when a
    /// document is worth compacting.
    pub tombstones: u64,
}

/// One binary file's identity in a `Manifest`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinDigest {
    pub path: String,
    pub hash: String,
    pub len: u64,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaOp {
    /// Reuse the receiver's own block at this index.
    CopyBlock(u32),
    /// Literal bytes the receiver did not have. `serde_bytes` keeps this in
    /// MessagePack's compact bin format instead of an array of ints.
    Literal(#[serde(with = "serde_bytes")] Vec<u8>),
}

/// Wire-level encoding of CRDT operations.
///
/// The CRDT itself is character-granular, but sending one `Insert` per typed
/// character is wasteful: in a run of typing, each op's origin is just the
/// previous op's id and the clocks are consecutive. `Run` collapses such a
/// chain into a single frame carrying the text, which is the difference between
/// ~40 bytes per character and ~1.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireOp {
    Ins {
        id: CharId,
        origin: Option<CharId>,
        ch: char,
    },
    /// `text` inserted after `origin`, ids running consecutively from `first`,
    /// each character anchored to the one before it.
    Run {
        first: CharId,
        origin: Option<CharId>,
        text: String,
    },
    /// A batch of tombstones, sharing one variant tag.
    Dels(Vec<CharId>),
}

/// Collapse an op sequence into the compact wire form.
pub fn compress(ops: &[Op]) -> Vec<WireOp> {
    let mut out: Vec<WireOp> = Vec::new();
    let mut i = 0;
    while i < ops.len() {
        match &ops[i] {
            Op::Delete { id } => {
                // Absorb the whole consecutive run of deletes.
                let mut ids = vec![*id];
                while let Some(Op::Delete { id }) = ops.get(i + 1) {
                    ids.push(*id);
                    i += 1;
                }
                out.push(WireOp::Dels(ids));
            }
            Op::Insert { id, origin, ch } => {
                let first = *id;
                let origin = *origin;
                let mut text = String::new();
                text.push(*ch);
                let mut prev = first;
                // Extend while the next insert continues this exact chain.
                while let Some(Op::Insert {
                    id: nid,
                    origin: norigin,
                    ch: nch,
                }) = ops.get(i + 1)
                {
                    if *norigin == Some(prev) && nid.peer == prev.peer && nid.clock == prev.clock + 1 {
                        text.push(*nch);
                        prev = *nid;
                        i += 1;
                    } else {
                        break;
                    }
                }
                if text.chars().count() == 1 {
                    out.push(WireOp::Ins {
                        id: first,
                        origin,
                        ch: text.chars().next().unwrap(),
                    });
                } else {
                    out.push(WireOp::Run { first, origin, text });
                }
            }
        }
        i += 1;
    }
    out
}

/// Expand the wire form back into individual CRDT operations.
pub fn expand(wire: &[WireOp]) -> Vec<Op> {
    let mut out = Vec::new();
    for w in wire {
        match w {
            WireOp::Ins { id, origin, ch } => out.push(Op::Insert {
                id: *id,
                origin: *origin,
                ch: *ch,
            }),
            WireOp::Run { first, origin, text } => {
                let mut origin = *origin;
                for (clock, ch) in (first.clock..).zip(text.chars()) {
                    let id = CharId::new(clock, first.peer);
                    out.push(Op::Insert { id, origin, ch });
                    origin = Some(id);
                }
            }
            WireOp::Dels(ids) => out.extend(ids.iter().map(|id| Op::Delete { id: *id })),
        }
    }
    out
}

pub fn encode(msg: &Msg) -> Result<Vec<u8>> {
    // Compact (positional) MessagePack: field names would otherwise dominate
    // the payload for small operations.
    let body = rmp_serde::to_vec(msg)?;
    if body.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", body.len());
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, msg: &Msg) -> Result<()> {
    let frame = encode(msg)?;
    w.write_all(&frame).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Msg> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("peer announced oversized frame: {len}");
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok(rmp_serde::from_slice(&body)?)
}
