//! Binary files: content hashing, rsync-style rolling checksums, and delta
//! reconstruction. Non-text files get last-writer-wins semantics rather than
//! character-level merging.
//!
//! # Large files
//!
//! Three things scale with file size and needed separate fixes:
//!
//! - **Detecting and hashing a locally changed file** used to read the whole
//!   file into memory just to sniff whether it's text and compute a hash.
//!   [`sniff_file`] and [`sha256_file`] instead read a bounded prefix and a
//!   streamed pass respectively, so touching a multi-GB video no longer costs
//!   a multi-GB allocation on the watcher's hot path.
//! - **The block signature list** was one `(u32, [u8;8])` pair per 4 KB block
//!   unconditionally, which is 3 MB of signatures for a 1 GB file over a
//!   connection that might reuse none of it. [`adaptive_block_size`] grows the
//!   block with the file so the signature list stays bounded regardless of
//!   file size.
//! - **Applying a delta** used to build the entire reconstructed file in one
//!   `Vec<u8>` before writing it out. [`apply_ops_streaming`] instead seeks
//!   into the base file block by block and writes straight to the output,
//!   hashing as it goes — the caller (`engine`) uses this to stream a delta
//!   to a temp file as chunks arrive off the wire, so reconstructing a file
//!   costs O(chunk size), not O(file size), on the receiving side.

use crate::wire::{DeltaOp, BLOCK_SIZE};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// How much of a file to read before deciding whether it's text or binary.
/// Large enough to catch the common case of a text header before binary
/// payload (id3, exif); small enough that sniffing a multi-GB file is cheap.
const SNIFF_BYTES: usize = 256 * 1024;

/// Is this file text or binary, without reading more of it than necessary.
///
/// A prefix is strictly less accurate than looking at the whole file — binary
/// content that only appears after `SNIFF_BYTES` would be missed — but reading
/// gigabytes just to answer "is this a video" is worse. The existing
/// [`is_binary`] heuristic is already imprecise (NUL-or-invalid-UTF8), so this
/// trades a little more of the same imprecision for boundedness.
pub fn sniff_file(path: &Path) -> io::Result<bool> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = vec![0u8; SNIFF_BYTES];
    let mut filled = 0;
    loop {
        let n = f.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
        if filled == buf.len() {
            break;
        }
    }
    buf.truncate(filled);
    Ok(is_binary(&buf))
}

/// SHA-256 and length of a file, streamed through a fixed-size buffer rather
/// than reading the whole file into memory first.
pub fn sha256_file(path: &Path) -> io::Result<(String, u64)> {
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut len = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        len += n as u64;
    }
    Ok((hex::encode(h.finalize()), len))
}

/// Rsync block size for a file of this length.
///
/// A fixed 4 KB block is fine for a document but produces an absurd signature
/// list for a multi-GB file — 1 GB / 4 KB is 262,144 blocks, ~3 MB of
/// signatures for a connection that might not reuse any of them. Scaling the
/// block with the file keeps the signature list within a few hundred KB
/// regardless of file size, at the cost of coarser reuse on a large file with
/// small scattered edits (a single changed byte now costs a bigger block).
pub fn adaptive_block_size(len: u64) -> usize {
    const TARGET_BLOCKS: u64 = 32_000;
    const MIN: usize = BLOCK_SIZE;
    const MAX: usize = 4 << 20; // 4 MB
    if len <= TARGET_BLOCKS * MIN as u64 {
        return MIN;
    }
    let raw = (len / TARGET_BLOCKS).next_power_of_two();
    raw.clamp(MIN as u64, MAX as u64) as usize
}

/// Try to shrink `data` with a fast DEFLATE pass. `None` if it isn't worth
/// sending compressed — already-compressed formats (jpg, mp4, zip) are the
/// common binary-file case, and paying the framing overhead to save nothing
/// is worse than sending the bytes as they are.
pub fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 256 {
        return None; // deflate's own overhead dominates below this
    }
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    let mut enc = DeflateEncoder::new(Vec::with_capacity(data.len() / 2), Compression::fast());
    enc.write_all(data).ok()?;
    let out = enc.finish().ok()?;
    // Require a real saving, not just "technically smaller": decompression
    // costs something on the other end too.
    (out.len() * 10 < data.len() * 9).then_some(out)
}

pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    use flate2::read::DeflateDecoder;
    let mut dec = DeflateDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)?;
    Ok(out)
}

fn strong8(data: &[u8]) -> [u8; 8] {
    let mut h = Sha256::new();
    h.update(data);
    let d = h.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&d[..8]);
    out
}

/// rsync's rolling checksum. Sliding the window one byte costs two adds, so
/// scanning a whole file for block matches is linear rather than quadratic.
///
/// For a window `x[k..k+L]`:
/// ```text
/// a(k) = Σ x[i]                    mod M
/// b(k) = Σ (k + L - i) * x[i]      mod M
/// ```
/// which gives the recurrence `a' = a - x[k] + x[k+L]`, `b' = b - L*x[k] + a'`.
#[derive(Clone, Copy)]
pub struct Rolling {
    a: u32,
    b: u32,
    len: u32,
}

const M: u32 = 65_521;

impl Rolling {
    pub fn new(window: &[u8]) -> Self {
        let l = window.len() as u32;
        let mut a: u32 = 0;
        let mut b: u32 = 0;
        for (i, &byte) in window.iter().enumerate() {
            a = (a + byte as u32) % M;
            b = (b + (l - i as u32) * byte as u32) % M;
        }
        Self { a, b, len: l }
    }

    pub fn digest(&self) -> u32 {
        (self.b << 16) | (self.a & 0xffff)
    }

    /// Slide the window forward one byte: `out` leaves, `inb` enters.
    pub fn roll(&mut self, out: u8, inb: u8) {
        let l = self.len % M;
        self.a = (self.a + M - (out as u32 % M) + inb as u32) % M;
        self.b = (self.b + M * 2 - (l * (out as u32 % M)) % M + self.a) % M;
    }
}

/// Block signatures over the receiver's current copy of the file, at the
/// default (small-file) block size.
pub fn signatures(data: &[u8]) -> Vec<(u32, [u8; 8])> {
    signatures_with_block_size(data, BLOCK_SIZE)
}

/// Block signatures at an explicit block size. See [`adaptive_block_size`].
pub fn signatures_with_block_size(data: &[u8], block_size: usize) -> Vec<(u32, [u8; 8])> {
    data.chunks(block_size)
        .map(|c| (Rolling::new(c).digest(), strong8(c)))
        .collect()
}

/// [`signatures_with_block_size`] streamed from a reader instead of a byte
/// slice — unlike delta generation (which needs the whole file resident for
/// its sliding window), building a signature list is one block at a time with
/// no lookback, so it never needs more than one block of the source file in
/// memory. Used to fingerprint the receiver's own (possibly huge) file without
/// reading the whole thing in.
pub fn signatures_from_reader<R: Read>(mut r: R, block_size: usize) -> io::Result<Vec<(u32, [u8; 8])>> {
    let mut buf = vec![0u8; block_size];
    let mut out = Vec::new();
    loop {
        let mut filled = 0;
        while filled < buf.len() {
            let n = r.read(&mut buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            break;
        }
        let c = &buf[..filled];
        out.push((Rolling::new(c).digest(), strong8(c)));
        if filled < buf.len() {
            break; // that was the last (short) block
        }
    }
    Ok(out)
}

/// Build reconstruction instructions that turn the *receiver's* file (described
/// by `sigs`) into `data`, reusing whole blocks wherever they still match, at
/// the default (small-file) block size.
pub fn delta(data: &[u8], sigs: &[(u32, [u8; 8])]) -> Vec<DeltaOp> {
    delta_with_block_size(data, sigs, BLOCK_SIZE)
}

/// [`delta`] at an explicit block size, which must match the size `sigs` was
/// built with.
///
/// # A quadratic tail, and why it only showed up once block sizes grew
///
/// The scan below only rolls the checksum window while a *full-size* window
/// still fits. Once fewer than `block_size` bytes remain, an earlier version
/// of this function kept recomputing the weak checksum from scratch at every
/// byte position — `Rolling::new` is O(window length), and the window is
/// still close to `block_size` for most of that stretch, so the tail alone
/// cost O(block_size²). At the original fixed 4 KB block that was ~17M byte
/// ops per file: slow enough to notice under a profiler, invisible under a
/// stopwatch. [`adaptive_block_size`] raises the block to as much as 4 MB for
/// a large file, and 4 MB² is 256× that — several seconds of CPU per binary
/// sync, which for a video would look exactly like a hung daemon. A fuzz test
/// at large block sizes (`streaming_apply_matches_in_memory_apply_delta_under_fuzzing`)
/// caught it.
///
/// The fix: never re-derive the checksum byte by byte once a full window
/// stops fitting. A short window can only ever legitimately match a file's
/// own short *trailing* block (block boundaries are fixed, so nothing else is
/// the right length) — so the tail gets exactly one match attempt, not one
/// per byte.
pub fn delta_with_block_size(data: &[u8], sigs: &[(u32, [u8; 8])], block_size: usize) -> Vec<DeltaOp> {
    // weak checksum -> candidate block indices
    let mut table: HashMap<u32, Vec<u32>> = HashMap::new();
    for (i, (weak, _)) in sigs.iter().enumerate() {
        table.entry(*weak).or_default().push(i as u32);
    }
    let find = |window: &[u8], table: &HashMap<u32, Vec<u32>>| -> Option<u32> {
        let weak = Rolling::new(window).digest();
        let strong = strong8(window);
        table.get(&weak)?.iter().copied().find(|&i| sigs[i as usize].1 == strong)
    };

    let mut ops: Vec<DeltaOp> = Vec::new();
    let mut literal: Vec<u8> = Vec::new();
    let mut pos = 0usize;

    let flush = |ops: &mut Vec<DeltaOp>, literal: &mut Vec<u8>| {
        if !literal.is_empty() {
            ops.push(DeltaOp::Literal(std::mem::take(literal)));
        }
    };

    // Main scan: a full-size window slides one byte at a time via `roll`,
    // O(1) each step. `roll` is only ever rebuilt from scratch right after a
    // match, where the window jumps to an unrelated position and there is
    // nothing to sensibly roll from.
    let mut roll: Option<Rolling> = None;
    while pos + block_size <= data.len() {
        let window = &data[pos..pos + block_size];
        let weak = match &roll {
            Some(r) => r.digest(),
            None => {
                let r = Rolling::new(window);
                let d = r.digest();
                roll = Some(r);
                d
            }
        };
        let matched = table
            .get(&weak)
            .and_then(|cands| cands.iter().copied().find(|&i| sigs[i as usize].1 == strong8(window)));
        match matched {
            Some(i) => {
                flush(&mut ops, &mut literal);
                ops.push(DeltaOp::CopyBlock(i));
                pos += block_size;
                roll = None;
            }
            None => {
                literal.push(data[pos]);
                // Only slide the window if another full-size window still
                // fits after this step; on the last eligible position there
                // is no `data[pos + block_size]` to bring in, and the loop
                // condition will end the scan on the next check anyway.
                if pos + block_size < data.len() {
                    if let Some(r) = &mut roll {
                        r.roll(data[pos], data[pos + block_size]);
                    }
                } else {
                    roll = None;
                }
                pos += 1;
            }
        }
    }

    // Tail: fewer than `block_size` bytes remain, so no further full-size
    // window exists to roll — one match attempt against the whole remainder,
    // not a byte-by-byte scan.
    if pos < data.len() {
        let tail = &data[pos..];
        match find(tail, &table) {
            Some(i) => {
                flush(&mut ops, &mut literal);
                ops.push(DeltaOp::CopyBlock(i));
            }
            None => literal.extend_from_slice(tail),
        }
    }

    flush(&mut ops, &mut literal);
    ops
}

/// Apply a delta against `base` (the receiver's current file bytes), at the
/// default (small-file) block size.
pub fn apply_delta(base: &[u8], ops: &[DeltaOp]) -> Vec<u8> {
    let mut cursor = io::Cursor::new(base);
    let mut out = Vec::new();
    let mut hasher = Sha256::new();
    apply_ops_streaming(&mut cursor, BLOCK_SIZE, ops, &mut out, &mut hasher)
        .expect("Vec<u8> writes and Cursor reads over a slice never fail");
    out
}

/// Apply reconstruction ops against a random-access base and a writer,
/// hashing the output as it goes.
///
/// This is the shared core behind both [`apply_delta`] (small files, base and
/// output both in memory) and the streaming receive path in `engine`, which
/// calls this once per chunk of ops as they arrive off the wire against a
/// `File` base and a temp-file writer — so reconstructing an N-byte file costs
/// O(chunk size) of memory on the receiving side, not O(N).
pub fn apply_ops_streaming<B: Read + Seek, W: Write>(
    base: &mut B,
    block_size: usize,
    ops: &[DeltaOp],
    out: &mut W,
    hasher: &mut Sha256,
) -> io::Result<()> {
    let mut buf = vec![0u8; block_size];
    for op in ops {
        match op {
            DeltaOp::CopyBlock(i) => {
                base.seek(SeekFrom::Start((*i as u64) * block_size as u64))?;
                // The last block of a file is usually short; read until the
                // base runs out rather than assuming a full block.
                let mut filled = 0;
                while filled < buf.len() {
                    let n = base.read(&mut buf[filled..])?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                out.write_all(&buf[..filled])?;
                hasher.update(&buf[..filled]);
            }
            DeltaOp::Literal(bytes) => {
                out.write_all(bytes)?;
                hasher.update(bytes);
            }
            DeltaOp::LiteralZ(compressed) => {
                let bytes = decompress(compressed)?;
                out.write_all(&bytes)?;
                hasher.update(&bytes);
            }
        }
    }
    Ok(())
}

/// Heuristic: treat a file as binary if it isn't valid UTF-8 or contains NULs.
pub fn is_binary(data: &[u8]) -> bool {
    if data.contains(&0) {
        return true;
    }
    std::str::from_utf8(data).is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::io::Cursor;

    #[test]
    fn adaptive_block_size_stays_small_for_small_files() {
        assert_eq!(adaptive_block_size(0), BLOCK_SIZE);
        assert_eq!(adaptive_block_size(1_000_000), BLOCK_SIZE);
    }

    #[test]
    fn adaptive_block_size_bounds_signature_count_for_large_files() {
        for len in [10_000_000u64, 1 << 30, 50 << 30] {
            let bs = adaptive_block_size(len);
            let blocks = len.div_ceil(bs as u64);
            assert!(
                blocks < 40_000,
                "len={len} block_size={bs} gives {blocks} blocks, expected a bounded count"
            );
            assert!(bs.is_power_of_two());
            assert!(bs <= 4 << 20, "block size {bs} exceeds the cap");
        }
    }

    #[test]
    fn compress_round_trips_and_skips_incompressible() {
        let compressible = "the quick brown fox jumps over the lazy dog. ".repeat(200);
        let compressed = maybe_compress(compressible.as_bytes()).expect("should compress well");
        assert!(compressed.len() < compressible.len());
        assert_eq!(decompress(&compressed).unwrap(), compressible.as_bytes());

        // Random bytes don't compress; maybe_compress must say so rather than
        // pay framing overhead for nothing.
        let mut rng = StdRng::seed_from_u64(42);
        let mut random = vec![0u8; 4096];
        rng.fill_bytes(&mut random);
        assert!(maybe_compress(&random).is_none());

        assert!(maybe_compress(b"short").is_none(), "below the worth-it floor");
    }

    #[test]
    fn sniff_and_hash_match_in_memory_computation() {
        let dir = tempfile::tempdir().unwrap();
        for (name, content) in [
            ("text.txt", "hello world\n".repeat(1000).into_bytes()),
            ("bin.dat", vec![0u8, 1, 2, 3, 255, 254]),
            ("empty.txt", Vec::new()),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, &content).unwrap();
            assert_eq!(sniff_file(&path).unwrap(), is_binary(&content), "{name}");
            let (hash, len) = sha256_file(&path).unwrap();
            assert_eq!(hash, sha256_hex(&content), "{name}");
            assert_eq!(len, content.len() as u64, "{name}");
        }
    }

    #[test]
    fn sniff_reads_only_a_bounded_prefix_of_a_huge_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin");
        let f = std::fs::File::create(&path).unwrap();
        // Sparse file: looks huge, costs no real disk or memory to sniff.
        f.set_len(50 * 1024 * 1024).unwrap();
        // A zero-filled prefix is valid UTF-8 (NUL bytes make it binary
        // either way) — the point of this test is that sniffing finishes at
        // all rather than reading 50 MB.
        let is_bin = sniff_file(&path).unwrap();
        assert!(is_bin, "NUL-filled sparse file must sniff as binary");
    }

    #[test]
    fn streaming_apply_matches_in_memory_apply_delta_under_fuzzing() {
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..80 {
            let bs = [512usize, 4096, 65536][rng.gen_range(0..3)];
            let len = rng.gen_range(0..bs * 6);
            let mut old = vec![0u8; len];
            rng.fill_bytes(&mut old);
            let mut new = old.clone();
            for _ in 0..rng.gen_range(0..10) {
                if new.is_empty() {
                    break;
                }
                let at = rng.gen_range(0..new.len());
                new[at] = rng.gen();
            }

            let sigs = signatures_with_block_size(&old, bs);
            let ops = delta_with_block_size(&new, &sigs, bs);

            let mut base = Cursor::new(old.clone());
            let mut out = Vec::new();
            let mut hasher = Sha256::new();
            apply_ops_streaming(&mut base, bs, &ops, &mut out, &mut hasher).unwrap();

            assert_eq!(out, new, "streaming apply mismatch at block_size={bs}");
            assert_eq!(hex::encode(hasher.finalize()), sha256_hex(&new));
        }
    }

    #[test]
    fn reader_signatures_match_in_memory_signatures() {
        let mut rng = StdRng::seed_from_u64(11);
        for bs in [64usize, 4096, 65537] {
            for len in [0usize, 1, bs - 1, bs, bs + 1, bs * 5 + 17] {
                let mut data = vec![0u8; len];
                rng.fill_bytes(&mut data);
                let in_memory = signatures_with_block_size(&data, bs);
                let streamed = signatures_from_reader(Cursor::new(&data), bs).unwrap();
                assert_eq!(streamed, in_memory, "block_size={bs} len={len}");
            }
        }
    }

    #[test]
    fn streaming_apply_handles_compressed_literals() {
        let compressible = "abcdefgh".repeat(500).into_bytes();
        let compressed = maybe_compress(&compressible).unwrap();
        let ops = vec![DeltaOp::LiteralZ(compressed)];
        let mut base = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut hasher = Sha256::new();
        apply_ops_streaming(&mut base, BLOCK_SIZE, &ops, &mut out, &mut hasher).unwrap();
        assert_eq!(out, compressible);
        assert_eq!(hex::encode(hasher.finalize()), sha256_hex(&compressible));
    }
}
