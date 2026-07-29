//! Binary files: content hashing, rsync-style rolling checksums, and delta
//! reconstruction. Non-text files get last-writer-wins semantics rather than
//! character-level merging.

use crate::wire::{DeltaOp, BLOCK_SIZE};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
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

/// Block signatures over the receiver's current copy of the file.
pub fn signatures(data: &[u8]) -> Vec<(u32, [u8; 8])> {
    data.chunks(BLOCK_SIZE)
        .map(|c| (Rolling::new(c).digest(), strong8(c)))
        .collect()
}

/// Build reconstruction instructions that turn the *receiver's* file (described
/// by `sigs`) into `data`, reusing whole blocks wherever they still match.
pub fn delta(data: &[u8], sigs: &[(u32, [u8; 8])]) -> Vec<DeltaOp> {
    // weak checksum -> candidate block indices
    let mut table: HashMap<u32, Vec<u32>> = HashMap::new();
    for (i, (weak, _)) in sigs.iter().enumerate() {
        table.entry(*weak).or_default().push(i as u32);
    }

    let mut ops: Vec<DeltaOp> = Vec::new();
    let mut literal: Vec<u8> = Vec::new();
    let mut pos = 0usize;

    let flush = |ops: &mut Vec<DeltaOp>, literal: &mut Vec<u8>| {
        if !literal.is_empty() {
            ops.push(DeltaOp::Literal(std::mem::take(literal)));
        }
    };

    // Rolling window state, valid while a full-size window fits.
    let mut roll: Option<Rolling> = None;
    while pos < data.len() {
        let end = (pos + BLOCK_SIZE).min(data.len());
        let window = &data[pos..end];
        let weak = match (&mut roll, window.len() == BLOCK_SIZE) {
            (Some(r), true) => r.digest(),
            _ => {
                let r = Rolling::new(window);
                let d = r.digest();
                if window.len() == BLOCK_SIZE {
                    roll = Some(r);
                }
                d
            }
        };
        let mut matched = None;
        if let Some(cands) = table.get(&weak) {
            let strong = strong8(window);
            for &i in cands {
                if sigs[i as usize].1 == strong {
                    matched = Some(i);
                    break;
                }
            }
        }
        match matched {
            Some(i) => {
                flush(&mut ops, &mut literal);
                ops.push(DeltaOp::CopyBlock(i));
                pos = end;
                // Window jumped; rebuild it at the new offset.
                roll = None;
            }
            None => {
                // No match here: this byte is literal, slide one forward.
                literal.push(data[pos]);
                if let Some(r) = &mut roll {
                    if pos + BLOCK_SIZE < data.len() {
                        r.roll(data[pos], data[pos + BLOCK_SIZE]);
                    } else {
                        roll = None;
                    }
                }
                pos += 1;
            }
        }
    }
    flush(&mut ops, &mut literal);
    ops
}

/// Apply a delta against `base` (the receiver's current file bytes).
pub fn apply_delta(base: &[u8], ops: &[DeltaOp]) -> Vec<u8> {
    let mut out = Vec::new();
    for op in ops {
        match op {
            DeltaOp::CopyBlock(i) => {
                let start = (*i as usize) * BLOCK_SIZE;
                let end = (start + BLOCK_SIZE).min(base.len());
                if start < base.len() {
                    out.extend_from_slice(&base[start..end]);
                }
            }
            DeltaOp::Literal(bytes) => out.extend_from_slice(bytes),
        }
    }
    out
}

/// Heuristic: treat a file as binary if it isn't valid UTF-8 or contains NULs.
pub fn is_binary(data: &[u8]) -> bool {
    if data.contains(&0) {
        return true;
    }
    std::str::from_utf8(data).is_err()
}
