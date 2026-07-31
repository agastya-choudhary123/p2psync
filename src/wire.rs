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
        #[serde(with = "crate::elemcodec")]
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
        #[serde(with = "crate::elemcodec")]
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

    /// One chunk of reconstruction instructions against the requester's own
    /// blocks. A delta for a large or wholly-new file can run past
    /// `MAX_FRAME` in one message — a first sync of any file is entirely
    /// `Literal` bytes, so this was a hard failure for any file bigger than
    /// the frame cap. Splitting into `seq`/`total`-numbered chunks removes the
    /// size limit; see `engine`'s streaming receive path, which writes each
    /// chunk straight to a temp file rather than collecting them first.
    BinaryDeltaChunk {
        path: String,
        hash: String,
        version: u64,
        seq: u32,
        total: u32,
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
    /// Literal bytes, DEFLATE-compressed — used when compression actually
    /// shrank the payload (see `binary::maybe_compress`); an already-compressed
    /// format like jpg or mp4 just uses `Literal` since compressing it again
    /// buys nothing.
    LiteralZ(#[serde(with = "serde_bytes")] Vec<u8>),
}

/// Bound on one chunk's serialized size, well under `MAX_FRAME` so a chunk
/// leaves headroom for MessagePack framing overhead and never needs to be
/// split further.
pub const DELTA_CHUNK_TARGET: usize = 4 << 20; // 4 MB

/// Group `ops` into chunks no larger than `DELTA_CHUNK_TARGET`, compressing
/// each literal payload that's worth compressing along the way.
///
/// A single `CopyBlock` is 5 bytes and never needs splitting. A `Literal` can
/// be arbitrarily large — a first sync of any file (nothing on the far side to
/// diff against yet) is exactly one `Literal` the size of the *whole file*,
/// not one per block, since `delta_with_block_size` only starts a new op when
/// it finds a match to interrupt the literal run. An earlier version of this
/// function only cut chunk boundaries *between* ops and left an oversized
/// single literal whole, on the reasoning that it was rare and bounded; it
/// was neither — it's the common case for any new file, and it reproduced the
/// exact `MAX_FRAME` failure this whole mechanism exists to remove, just
/// pushed one level down. So a literal is now split into
/// `DELTA_CHUNK_TARGET`-sized pieces *before* anything else, each piece
/// compressed independently, and chunking only ever groups pieces that are
/// already at most one chunk's worth.
pub fn chunk_ops(ops: Vec<DeltaOp>) -> Vec<Vec<DeltaOp>> {
    if ops.is_empty() {
        return vec![Vec::new()];
    }
    let mut pieces: Vec<DeltaOp> = Vec::new();
    for op in ops {
        match op {
            DeltaOp::Literal(bytes) => {
                for piece in bytes.chunks(DELTA_CHUNK_TARGET) {
                    pieces.push(match crate::binary::maybe_compress(piece) {
                        Some(z) => DeltaOp::LiteralZ(z),
                        None => DeltaOp::Literal(piece.to_vec()),
                    });
                }
            }
            other => pieces.push(other),
        }
    }

    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0usize;
    for op in pieces {
        let size = match &op {
            DeltaOp::Literal(b) | DeltaOp::LiteralZ(b) => b.len(),
            DeltaOp::CopyBlock(_) => 5,
        };
        if current_size + size > DELTA_CHUNK_TARGET && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += size;
        current.push(op);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
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

#[cfg(test)]
mod tests {
    use super::*;

    fn total_literal_bytes(chunks: &[Vec<DeltaOp>]) -> usize {
        chunks
            .iter()
            .flatten()
            .map(|op| match op {
                DeltaOp::Literal(b) | DeltaOp::LiteralZ(b) => b.len(),
                DeltaOp::CopyBlock(_) => 0,
            })
            .sum()
    }

    #[test]
    fn empty_ops_still_produce_one_chunk() {
        // A delta that reused every block (no changes) is still a valid
        // "here is your file" message: the receiver must see at least one
        // chunk to know the transfer is complete.
        let chunks = chunk_ops(vec![]);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_empty());
    }

    #[test]
    fn small_ops_fit_in_one_chunk() {
        let ops = vec![DeltaOp::CopyBlock(0), DeltaOp::Literal(vec![1, 2, 3])];
        let chunks = chunk_ops(ops);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn large_literal_run_splits_into_multiple_chunks() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(9);
        let mut ops = Vec::new();
        for _ in 0..20u32 {
            // Genuinely random bytes: incompressible, so the pre-chunk size is
            // exactly what drives the split, exercising the chunk-size
            // accounting rather than DEFLATE's.
            let mut bytes = vec![0u8; DELTA_CHUNK_TARGET / 3];
            rng.fill_bytes(&mut bytes);
            ops.push(DeltaOp::Literal(bytes));
        }
        let total_in: usize = ops
            .iter()
            .map(|o| match o {
                DeltaOp::Literal(b) => b.len(),
                _ => 0,
            })
            .sum();
        let chunks = chunk_ops(ops);
        assert!(chunks.len() > 1, "should have split into multiple chunks");
        assert_eq!(total_literal_bytes(&chunks), total_in, "no bytes lost, none duplicated");
    }

    #[test]
    fn compressible_literal_becomes_literal_z() {
        let text = "the quick brown fox ".repeat(500).into_bytes();
        let chunks = chunk_ops(vec![DeltaOp::Literal(text)]);
        assert!(matches!(chunks[0][0], DeltaOp::LiteralZ(_)), "compressible text should compress");
    }

    #[test]
    fn a_single_oversized_literal_splits_and_fits_frames() {
        // The real shape a first sync of a large file produces: not many
        // separate ops, but exactly ONE Literal covering the entire file,
        // because there is nothing on the other side to interrupt it with a
        // CopyBlock match. This is the case an earlier version of chunk_ops
        // got wrong — grouping only cuts *between* ops, so one enormous
        // literal sailed through whole and blew MAX_FRAME on send.
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(99);
        let mut whole_file = vec![0u8; 20 << 20]; // 20 MB, one Literal
        rng.fill_bytes(&mut whole_file);

        let chunks = chunk_ops(vec![DeltaOp::Literal(whole_file.clone())]);
        assert!(chunks.len() > 1, "a 20 MB single literal must split");

        let mut reassembled = Vec::new();
        for (i, ops) in chunks.iter().enumerate() {
            let msg = Msg::BinaryDeltaChunk {
                path: "movie.mp4".into(),
                hash: "irrelevant".into(),
                version: 1,
                seq: i as u32,
                total: chunks.len() as u32,
                ops: ops.clone(),
            };
            let frame = encode(&msg).expect("every chunk must fit in one frame");
            assert!(frame.len() < MAX_FRAME, "chunk {i} is {} bytes", frame.len());
            for op in ops {
                match op {
                    DeltaOp::Literal(b) => reassembled.extend_from_slice(b),
                    DeltaOp::LiteralZ(z) => reassembled.extend(crate::binary::decompress(z).unwrap()),
                    DeltaOp::CopyBlock(_) => unreachable!(),
                }
            }
        }
        assert_eq!(reassembled, whole_file, "splitting and reassembling must be lossless");
    }

    #[test]
    fn chunks_never_exceed_the_wire_frame_cap() {
        // A single delta bigger than MAX_FRAME used to fail outright —
        // `encode` bailed with "frame too large" and the sync silently gave
        // up, which is exactly what a first sync of any file over 64 MB did
        // (a first sync has nothing to diff against, so it's one giant
        // `Literal`). Build a delta comfortably past that cap and check every
        // resulting frame actually fits.
        let mut x = 0x2545F4914F6CDD1Du64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut ops = Vec::new();
        let mut remaining = 70usize << 20; // 70 MB, past the 64 MB cap
        while remaining > 0 {
            let n = remaining.min(1 << 20);
            let mut bytes = vec![0u8; n];
            for chunk in bytes.chunks_mut(8) {
                chunk.copy_from_slice(&rnd().to_le_bytes()[..chunk.len()]);
            }
            ops.push(DeltaOp::Literal(bytes));
            remaining -= n;
        }

        let chunks = chunk_ops(ops);
        assert!(chunks.len() > 1, "70 MB of incompressible data must not fit in one chunk");
        for (i, ops) in chunks.into_iter().enumerate() {
            let msg = Msg::BinaryDeltaChunk {
                path: "big.bin".into(),
                hash: "deadbeef".into(),
                version: 1,
                seq: i as u32,
                total: 99,
                ops,
            };
            let frame = encode(&msg).expect("every chunk must fit in one frame");
            assert!(frame.len() < MAX_FRAME, "chunk {i} is {} bytes, still over MAX_FRAME", frame.len());
        }
    }

    #[test]
    fn incompressible_literal_stays_literal() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(10);
        let mut random = vec![0u8; 4096];
        rng.fill_bytes(&mut random);
        let chunks = chunk_ops(vec![DeltaOp::Literal(random)]);
        assert!(matches!(chunks[0][0], DeltaOp::Literal(_)), "incompressible data should stay raw");
    }
}
