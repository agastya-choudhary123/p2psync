//! Compact encoding for runs of CRDT elements.
//!
//! A `Vec<Elem>` is the single largest thing this program moves around: it is
//! what a `Snapshot` puts on the wire and what the state file stores. Encoded
//! as plain MessagePack it costs roughly 45 bytes per character on the wire and
//! 77 bytes in the state file, because every character carries two full
//! `CharId`s — its own and its origin's — and each of those is two `u64`s.
//! A 108 KB file cost 3.4 MB to transfer.
//!
//! Almost all of that is predictable. Elements are encoded in document order,
//! and in text that someone typed:
//!
//! - an element's origin is the element just before it,
//! - its clock is the previous clock plus one,
//! - its peer is the same peer as the one before it.
//!
//! So each element becomes a flags byte plus the UTF-8 of its character — two
//! bytes for the common case — and anything that breaks the pattern (a merge, a
//! concurrent insert, a peer switch) falls back to explicit varints. Nothing is
//! lossy: `decode(encode(x)) == x` for any element sequence.
//!
//! ```text
//! varint  peer_count
//!   varint  peer_id          (repeated; a table, since a document has few peers)
//! varint  elem_count
//!   u8      flags            (repeated, one per element)
//!   varint  peer_index       if !SAME_PEER
//!   varint  zigzag clock     delta from the previous element's clock, if !CLOCK_PLUS_1
//!   varint  origin_peer      if the origin is explicit
//!   varint  zigzag origin    delta from this element's own clock, if the origin is explicit
//!   1-4     UTF-8 bytes of the character
//! ```

use crate::crdt::{CharId, Elem, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

const F_DELETED: u8 = 0x01;
/// The origin is the element immediately before this one in document order.
const F_ORIGIN_PREV: u8 = 0x02;
/// The origin is the start of the document.
const F_ORIGIN_NONE: u8 = 0x04;
/// Same peer as the previous element.
const F_SAME_PEER: u8 = 0x08;
/// Clock is the previous element's clock plus one.
const F_CLOCK_PLUS_1: u8 = 0x10;

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_uvarint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Difference between two clocks, as a signed delta. Clocks are Lamport
/// counters and stay far away from the ends of the range in practice.
fn delta(to: u64, from: u64) -> i64 {
    (to as i64).wrapping_sub(from as i64)
}

pub fn encode(elems: &[Elem]) -> Vec<u8> {
    encode_streaming(|| elems.iter().cloned())
}

/// Encode without ever holding a `Vec<Elem>`.
///
/// `make_iter` is called twice — once to build the peer table, once for the
/// body — so a `Doc` can serialize straight out of its arena. That matters:
/// materializing the element list for a 660 KB document costs 32 MB, and this
/// runs every time the state is persisted.
pub fn encode_streaming<F, I>(make_iter: F) -> Vec<u8>
where
    F: Fn() -> I,
    I: Iterator<Item = Elem>,
{
    // Peer table: a document is written by a handful of peers, so an index into
    // a table beats repeating a u64 per character.
    let mut peer_ids: Vec<PeerId> = Vec::new();
    let mut peer_index: HashMap<PeerId, u64> = HashMap::new();
    let intern = |p: PeerId, ids: &mut Vec<PeerId>, idx: &mut HashMap<PeerId, u64>| {
        idx.entry(p).or_insert_with(|| {
            ids.push(p);
            (ids.len() - 1) as u64
        });
    };
    let mut count: u64 = 0;
    for e in make_iter() {
        count += 1;
        intern(e.id.peer, &mut peer_ids, &mut peer_index);
        if let Some(o) = e.origin {
            intern(o.peer, &mut peer_ids, &mut peer_index);
        }
    }

    let mut out = Vec::with_capacity(count as usize * 2 + peer_ids.len() * 9 + 16);
    put_uvarint(&mut out, peer_ids.len() as u64);
    for p in &peer_ids {
        put_uvarint(&mut out, *p);
    }
    put_uvarint(&mut out, count);

    let mut prev_clock: u64 = 0;
    let mut prev_peer: Option<u64> = None;
    let mut prev_id: Option<CharId> = None;

    for e in make_iter() {
        let peer_idx = peer_index[&e.id.peer];
        let mut flags = 0u8;
        if e.deleted {
            flags |= F_DELETED;
        }
        match e.origin {
            None => flags |= F_ORIGIN_NONE,
            Some(o) if Some(o) == prev_id => flags |= F_ORIGIN_PREV,
            Some(_) => {}
        }
        if prev_peer == Some(peer_idx) {
            flags |= F_SAME_PEER;
        }
        if e.id.clock == prev_clock.wrapping_add(1) {
            flags |= F_CLOCK_PLUS_1;
        }
        out.push(flags);

        if flags & F_SAME_PEER == 0 {
            put_uvarint(&mut out, peer_idx);
        }
        if flags & F_CLOCK_PLUS_1 == 0 {
            put_uvarint(&mut out, zigzag(delta(e.id.clock, prev_clock)));
        }
        if flags & (F_ORIGIN_PREV | F_ORIGIN_NONE) == 0 {
            let o = e.origin.expect("explicit origin");
            put_uvarint(&mut out, peer_index[&o.peer]);
            put_uvarint(&mut out, zigzag(delta(o.clock, e.id.clock)));
        }

        let mut buf = [0u8; 4];
        out.extend_from_slice(e.ch.encode_utf8(&mut buf).as_bytes());

        prev_clock = e.id.clock;
        prev_peer = Some(peer_idx);
        prev_id = Some(e.id);
    }
    out
}

pub fn decode(buf: &[u8]) -> Option<Vec<Elem>> {
    let mut pos = 0usize;
    let n_peers = get_uvarint(buf, &mut pos)? as usize;
    let mut peers = Vec::with_capacity(n_peers.min(4096));
    for _ in 0..n_peers {
        peers.push(get_uvarint(buf, &mut pos)?);
    }
    let n_elems = get_uvarint(buf, &mut pos)? as usize;
    // Each element is at least two bytes, so a count larger than what remains
    // is a corrupt or hostile frame and must not drive a huge allocation.
    if n_elems > buf.len().saturating_sub(pos) {
        return None;
    }
    let mut elems = Vec::with_capacity(n_elems);

    let mut prev_clock: u64 = 0;
    let mut prev_peer: Option<u64> = None;
    let mut prev_id: Option<CharId> = None;

    for _ in 0..n_elems {
        let flags = *buf.get(pos)?;
        pos += 1;

        let peer_idx = if flags & F_SAME_PEER != 0 {
            prev_peer?
        } else {
            get_uvarint(buf, &mut pos)?
        };
        let peer = *peers.get(peer_idx as usize)?;

        let clock = if flags & F_CLOCK_PLUS_1 != 0 {
            prev_clock.wrapping_add(1)
        } else {
            let d = unzigzag(get_uvarint(buf, &mut pos)?);
            (prev_clock as i64).wrapping_add(d) as u64
        };
        let id = CharId { clock, peer };

        let origin = if flags & F_ORIGIN_NONE != 0 {
            None
        } else if flags & F_ORIGIN_PREV != 0 {
            Some(prev_id?)
        } else {
            let op = *peers.get(get_uvarint(buf, &mut pos)? as usize)?;
            let d = unzigzag(get_uvarint(buf, &mut pos)?);
            Some(CharId {
                clock: (clock as i64).wrapping_add(d) as u64,
                peer: op,
            })
        };

        // UTF-8 is self-delimiting: the leading byte gives the length.
        let lead = *buf.get(pos)?;
        let len = if lead < 0x80 {
            1
        } else if lead >> 5 == 0b110 {
            2
        } else if lead >> 4 == 0b1110 {
            3
        } else if lead >> 3 == 0b11110 {
            4
        } else {
            return None;
        };
        let ch = std::str::from_utf8(buf.get(pos..pos + len)?).ok()?.chars().next()?;
        pos += len;

        elems.push(Elem {
            id,
            origin,
            ch,
            deleted: flags & F_DELETED != 0,
        });
        prev_clock = clock;
        prev_peer = Some(peer_idx);
        prev_id = Some(id);
    }
    Some(elems)
}

/// `#[serde(with = "crate::elemcodec")]` hooks: the sequence rides as one
/// MessagePack binary blob rather than a list of structs.
pub fn serialize<S: Serializer>(elems: &Vec<Elem>, s: S) -> Result<S::Ok, S::Error> {
    serde_bytes::ByteBuf::from(encode(elems)).serialize(s)
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Elem>, D::Error> {
    let raw = serde_bytes::ByteBuf::deserialize(d)?;
    decode(&raw).ok_or_else(|| serde::de::Error::custom("malformed element block"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn el(clock: u64, peer: u64, origin: Option<(u64, u64)>, ch: char, deleted: bool) -> Elem {
        Elem {
            id: CharId { clock, peer },
            origin: origin.map(|(c, p)| CharId { clock: c, peer: p }),
            ch,
            deleted,
        }
    }

    #[test]
    fn empty_round_trips() {
        assert_eq!(decode(&encode(&[])).unwrap(), Vec::<Elem>::new());
    }

    #[test]
    fn sequential_text_round_trips() {
        // The shape produced by someone typing: one peer, consecutive clocks,
        // each origin the character before it.
        let mut elems = vec![el(1, 7, None, 'h', false)];
        for (i, ch) in "ello world".chars().enumerate() {
            let clock = i as u64 + 2;
            elems.push(el(clock, 7, Some((clock - 1, 7)), ch, false));
        }
        assert_eq!(decode(&encode(&elems)).unwrap(), elems);
    }

    #[test]
    fn sequential_text_costs_two_bytes_per_character() {
        let mut elems = vec![el(1, 7, None, 'a', false)];
        for i in 1..1000u64 {
            elems.push(el(i + 1, 7, Some((i, 7)), 'a', false));
        }
        let n = encode(&elems).len();
        // Peer table plus two bytes each; the old encoding was ~45x this.
        assert!(n < 1000 * 3, "expected under 3 bytes per char, got {}", n as f64 / 1000.0);
    }

    #[test]
    fn multi_peer_and_tombstones_round_trip() {
        let elems = vec![
            el(1, 100, None, 'x', false),
            el(9, 200, Some((1, 100)), 'y', true),
            el(2, 100, Some((9, 200)), 'z', false),
            el(50, 300, Some((1, 100)), 'q', true),
            el(51, 300, Some((50, 300)), 'r', false),
        ];
        assert_eq!(decode(&encode(&elems)).unwrap(), elems);
    }

    #[test]
    fn non_ascii_round_trips() {
        let elems: Vec<Elem> = "héllo → 世界 🌍"
            .chars()
            .enumerate()
            .map(|(i, ch)| {
                let c = i as u64 + 1;
                el(c, 5, if i == 0 { None } else { Some((c - 1, 5)) }, ch, false)
            })
            .collect();
        assert_eq!(decode(&encode(&elems)).unwrap(), elems);
    }

    #[test]
    fn backward_clocks_and_wide_gaps_round_trip() {
        // Merges produce origins that point far backwards and clocks that jump.
        let elems = vec![
            el(u32::MAX as u64, 1, None, 'a', false),
            el(5, 2, Some((u32::MAX as u64, 1)), 'b', false),
            el(u32::MAX as u64 + 1000, 1, Some((5, 2)), 'c', true),
            el(1, 3, Some((u32::MAX as u64 + 1000, 1)), 'd', false),
        ];
        assert_eq!(decode(&encode(&elems)).unwrap(), elems);
    }

    #[test]
    fn truncated_input_is_rejected_not_panicked() {
        let elems = vec![el(1, 7, None, 'a', false), el(2, 7, Some((1, 7)), 'b', false)];
        let full = encode(&elems);
        for cut in 0..full.len() {
            // Must return None rather than panic or allocate wildly.
            let _ = decode(&full[..cut]);
        }
        assert!(decode(&full[..full.len() - 1]).is_none() || full.len() == 1);
    }

    #[test]
    fn garbage_does_not_panic() {
        for seed in 0..2000u64 {
            let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let n = (x % 64) as usize;
            let bytes: Vec<u8> = (0..n)
                .map(|_| {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    (x >> 33) as u8
                })
                .collect();
            let _ = decode(&bytes);
        }
    }

    #[test]
    fn pseudorandom_sequences_round_trip() {
        let mut x = 0x2545F4914F6CDD1Du64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200 {
            let n = (rnd() % 80) as usize;
            let mut elems: Vec<Elem> = Vec::new();
            for _ in 0..n {
                let origin = if elems.is_empty() || rnd() % 4 == 0 {
                    None
                } else {
                    let pick = (rnd() as usize) % elems.len();
                    Some((elems[pick].id.clock, elems[pick].id.peer))
                };
                let ch = char::from_u32((rnd() % 0x2000) as u32 + 32).unwrap_or('?');
                elems.push(el(rnd() % 100_000, rnd() % 5, origin, ch, rnd() % 3 == 0));
            }
            assert_eq!(decode(&encode(&elems)).unwrap(), elems, "round trip failed");
        }
    }
}
