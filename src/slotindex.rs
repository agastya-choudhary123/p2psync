//! `CharId -> arena slot` lookup, stored as runs instead of one entry per
//! character.
//!
//! This index used to be a `HashMap<CharId, u32>`, and it was the single
//! largest allocation in the process — 41 MB for a 660 KB document, larger than
//! the 26 MB arena it indexes. A `CharId` is two `u64`s, so every character paid
//! 24 bytes of key and value plus hash-table slack, for a document whose text
//! was 660 KB.
//!
//! The redundancy is the same one the wire codec exploits: characters typed in
//! sequence get consecutive clocks from one peer and land in consecutive arena
//! slots. A run records `(clock_start, slot_start, len)` and answers any id
//! inside it by arithmetic, so a file typed start to finish needs *one* run per
//! peer rather than one entry per character.
//!
//! Degenerate input degrades gracefully: in the worst case every character is
//! its own run at 16 bytes, still smaller than the hash map's per-entry cost.

use crate::crdt::{CharId, PeerId};
use std::collections::HashMap;

/// `len` consecutive clocks starting at `clock_start`, occupying `len`
/// consecutive arena slots starting at `slot_start`.
#[derive(Clone, Copy, Debug)]
struct Run {
    clock_start: u64,
    slot_start: u32,
    len: u32,
}

impl Run {
    #[inline]
    fn end_clock(&self) -> u64 {
        self.clock_start + self.len as u64
    }
    #[inline]
    fn contains(&self, clock: u64) -> bool {
        clock >= self.clock_start && clock < self.end_clock()
    }
    #[inline]
    fn slot_of(&self, clock: u64) -> u32 {
        self.slot_start + (clock - self.clock_start) as u32
    }
}

#[derive(Clone, Debug, Default)]
pub struct SlotIndex {
    /// Per peer, runs sorted by `clock_start` and never overlapping.
    runs: HashMap<PeerId, Vec<Run>>,
}

impl SlotIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.runs.clear();
    }

    pub fn shrink_to_fit(&mut self) {
        for v in self.runs.values_mut() {
            v.shrink_to_fit();
        }
        self.runs.shrink_to_fit();
    }

    /// Index of the run that could contain `clock`: the last one starting at or
    /// before it.
    #[inline]
    fn seek(v: &[Run], clock: u64) -> Option<usize> {
        let i = v.partition_point(|r| r.clock_start <= clock);
        if i == 0 {
            None
        } else {
            Some(i - 1)
        }
    }

    pub fn get(&self, id: &CharId) -> Option<u32> {
        let v = self.runs.get(&id.peer)?;
        let i = Self::seek(v, id.clock)?;
        let r = &v[i];
        r.contains(id.clock).then(|| r.slot_of(id.clock))
    }

    pub fn contains_key(&self, id: &CharId) -> bool {
        self.get(id).is_some()
    }

    pub fn insert(&mut self, id: CharId, slot: u32) {
        let v = self.runs.entry(id.peer).or_default();

        // Fast path: extending the newest run, which is what sequential typing
        // and in-order rebuilds do.
        if let Some(last) = v.last_mut() {
            if last.end_clock() == id.clock && last.slot_start + last.len == slot {
                last.len += 1;
                return;
            }
        }

        let at = v.partition_point(|r| r.clock_start <= id.clock);
        if at > 0 {
            let prev = &mut v[at - 1];
            if prev.contains(id.clock) {
                return; // already indexed; ids are unique
            }
            if prev.end_clock() == id.clock && prev.slot_start + prev.len == slot {
                prev.len += 1;
                // The new element may now bridge this run and the next one.
                if let Some(next) = v.get(at).copied() {
                    let prev = &mut v[at - 1];
                    if prev.end_clock() == next.clock_start && prev.slot_start + prev.len == next.slot_start {
                        prev.len += next.len;
                        v.remove(at);
                    }
                }
                return;
            }
        }
        // Prepending onto the run that starts just after us.
        if let Some(next) = v.get_mut(at) {
            if id.clock + 1 == next.clock_start && slot + 1 == next.slot_start {
                next.clock_start = id.clock;
                next.slot_start = slot;
                next.len += 1;
                return;
            }
        }
        v.insert(
            at,
            Run {
                clock_start: id.clock,
                slot_start: slot,
                len: 1,
            },
        );
    }

    /// Build the whole index at once from `(id, slot)` pairs in any order.
    ///
    /// `adopt` rebuilds the index for an entire document, and feeding that
    /// through `insert` one at a time can memmove the run vector on every
    /// out-of-order id. Sorting once and coalescing is O(n log n) with no
    /// quadratic edge.
    pub fn build<I: Iterator<Item = (CharId, u32)>>(&mut self, pairs: I) {
        self.runs.clear();
        let mut by_peer: HashMap<PeerId, Vec<(u64, u32)>> = HashMap::new();
        for (id, slot) in pairs {
            by_peer.entry(id.peer).or_default().push((id.clock, slot));
        }
        for (peer, mut list) in by_peer {
            list.sort_unstable();
            let mut runs: Vec<Run> = Vec::new();
            for (clock, slot) in list {
                match runs.last_mut() {
                    Some(r) if r.end_clock() == clock && r.slot_start + r.len == slot => r.len += 1,
                    // Duplicate id: keep the first, matching HashMap-with-unique-keys behavior.
                    Some(r) if r.contains(clock) => {}
                    _ => runs.push(Run {
                        clock_start: clock,
                        slot_start: slot,
                        len: 1,
                    }),
                }
            }
            runs.shrink_to_fit();
            self.runs.insert(peer, runs);
        }
        self.runs.shrink_to_fit();
    }

    /// Number of runs across all peers — the thing that actually costs memory.
    #[cfg(test)]
    fn run_count(&self) -> usize {
        self.runs.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    fn cid(clock: u64, peer: u64) -> CharId {
        CharId { clock, peer }
    }

    #[test]
    fn sequential_inserts_collapse_to_one_run() {
        let mut ix = SlotIndex::new();
        for i in 0..10_000u32 {
            ix.insert(cid(i as u64 + 1, 42), i);
        }
        assert_eq!(ix.run_count(), 1, "sequential typing must be a single run");
        for i in 0..10_000u32 {
            assert_eq!(ix.get(&cid(i as u64 + 1, 42)), Some(i));
        }
        assert_eq!(ix.get(&cid(10_001, 42)), None);
        assert_eq!(ix.get(&cid(1, 43)), None);
    }

    #[test]
    fn bridging_two_runs_merges_them() {
        let mut ix = SlotIndex::new();
        ix.insert(cid(1, 1), 0);
        ix.insert(cid(3, 1), 2); // gap at clock 2
        assert_eq!(ix.run_count(), 2);
        ix.insert(cid(2, 1), 1); // fills it
        assert_eq!(ix.run_count(), 1);
        for (c, s) in [(1u64, 0u32), (2, 1), (3, 2)] {
            assert_eq!(ix.get(&cid(c, 1)), Some(s));
        }
    }

    #[test]
    fn prepending_extends_the_following_run() {
        let mut ix = SlotIndex::new();
        ix.insert(cid(5, 1), 5);
        ix.insert(cid(4, 1), 4);
        assert_eq!(ix.run_count(), 1);
        assert_eq!(ix.get(&cid(4, 1)), Some(4));
        assert_eq!(ix.get(&cid(5, 1)), Some(5));
    }

    #[test]
    fn matches_a_hashmap_under_random_insertion() {
        let mut x = 0x9E3779B97F4A7C15u64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200 {
            let mut ix = SlotIndex::new();
            let mut reference: Map<CharId, u32> = Map::new();
            for slot in 0..300u32 {
                let id = cid(rnd() % 400, rnd() % 3);
                // Mirror the CRDT's own contract: an id is inserted once.
                if reference.contains_key(&id) {
                    continue;
                }
                reference.insert(id, slot);
                ix.insert(id, slot);
            }
            for (id, slot) in &reference {
                assert_eq!(ix.get(id), Some(*slot), "mismatch for {id:?}");
            }
            for _ in 0..300 {
                let id = cid(rnd() % 500, rnd() % 4);
                assert_eq!(ix.get(&id), reference.get(&id).copied(), "mismatch for {id:?}");
            }
        }
    }

    #[test]
    fn build_matches_incremental_insert() {
        let mut x = 0x2545F4914F6CDD1Du64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200 {
            let mut pairs: Vec<(CharId, u32)> = Vec::new();
            let mut seen: Map<CharId, u32> = Map::new();
            for slot in 0..200u32 {
                let id = cid(rnd() % 300, rnd() % 3);
                if seen.contains_key(&id) {
                    continue;
                }
                seen.insert(id, slot);
                pairs.push((id, slot));
            }
            let mut bulk = SlotIndex::new();
            bulk.build(pairs.iter().copied());
            let mut incremental = SlotIndex::new();
            for (id, slot) in &pairs {
                incremental.insert(*id, *slot);
            }
            for (id, slot) in &seen {
                assert_eq!(bulk.get(id), Some(*slot));
                assert_eq!(incremental.get(id), Some(*slot));
            }
        }
    }

    #[test]
    fn clear_and_shrink_behave() {
        let mut ix = SlotIndex::new();
        for i in 0..100u32 {
            ix.insert(cid(i as u64, 1), i);
        }
        ix.shrink_to_fit();
        assert_eq!(ix.get(&cid(50, 1)), Some(50));
        ix.clear();
        assert_eq!(ix.get(&cid(50, 1)), None);
    }
}
