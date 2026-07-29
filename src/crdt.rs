//! RGA (Replicated Growable Array) — a sequence CRDT over `char`s.
//!
//! Every character gets a globally unique `CharId` = (lamport clock, peer id).
//! Inserts name the element they follow (`origin`), deletes are tombstones.
//! Both operations are commutative and idempotent, so a set of operations
//! converges to the same visible string on every replica regardless of the
//! order in which they arrive.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type PeerId = u64;

/// Unique identity of one character.
///
/// The `Ord` impl is the CRDT's total order: clock first, peer id as the
/// deterministic tiebreak for concurrent inserts. Derived lexicographic
/// ordering over (clock, peer) is exactly what we want, so the field order
/// here is load-bearing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CharId {
    pub clock: u64,
    pub peer: PeerId,
}

impl CharId {
    pub fn new(clock: u64, peer: PeerId) -> Self {
        Self { clock, peer }
    }
}

impl std::fmt::Debug for CharId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{:x}", self.clock, self.peer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Insert `ch` immediately after `origin` (`None` = start of document).
    Insert {
        id: CharId,
        origin: Option<CharId>,
        ch: char,
    },
    /// Tombstone the character `id`.
    Delete { id: CharId },
}

impl Op {
    pub fn id(&self) -> CharId {
        match self {
            Op::Insert { id, .. } => *id,
            Op::Delete { id } => *id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Elem {
    pub id: CharId,
    pub origin: Option<CharId>,
    pub ch: char,
    pub deleted: bool,
}

/// One CRDT document — the state of a single text file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Doc {
    /// Linearized RGA sequence, including tombstones.
    elems: Vec<Elem>,
    /// Lamport clock for locally generated ids.
    clock: u64,
    /// This replica's id (used when minting new character ids).
    peer: PeerId,
    /// Ids we have already integrated, for idempotent apply.
    seen: HashMap<CharId, ()>,
    /// Inserts whose `origin` has not arrived yet, keyed by the missing origin.
    pending: HashMap<CharId, Vec<Op>>,
    /// Deletes for characters we have not seen yet.
    pending_deletes: Vec<CharId>,
}

impl Doc {
    pub fn new(peer: PeerId) -> Self {
        Self {
            elems: Vec::new(),
            clock: 0,
            peer,
            seen: HashMap::new(),
            pending: HashMap::new(),
            pending_deletes: Vec::new(),
        }
    }

    pub fn peer(&self) -> PeerId {
        self.peer
    }

    /// Rebind this document to a new local peer id (used after loading
    /// persisted state under a different identity).
    pub fn set_peer(&mut self, peer: PeerId) {
        self.peer = peer;
    }

    fn next_id(&mut self) -> CharId {
        self.clock += 1;
        CharId::new(self.clock, self.peer)
    }

    fn observe_clock(&mut self, id: CharId) {
        if id.clock > self.clock {
            self.clock = id.clock;
        }
    }

    fn index_of(&self, id: CharId) -> Option<usize> {
        self.elems.iter().position(|e| e.id == id)
    }

    /// Visible (non-tombstoned) characters as a string.
    pub fn text(&self) -> String {
        self.elems.iter().filter(|e| !e.deleted).map(|e| e.ch).collect()
    }

    /// Ids of the visible characters, in order. Used by the change detector to
    /// map diff positions back onto CRDT elements.
    pub fn visible_ids(&self) -> Vec<CharId> {
        self.elems.iter().filter(|e| !e.deleted).map(|e| e.id).collect()
    }

    pub fn len_visible(&self) -> usize {
        self.elems.iter().filter(|e| !e.deleted).count()
    }

    pub fn snapshot(&self) -> Vec<Elem> {
        self.elems.clone()
    }

    /// Number of stored elements, tombstones included.
    pub fn len_raw(&self) -> usize {
        self.elems.len()
    }

    // ---- local edits -------------------------------------------------

    /// Generate (and apply) an insert of `ch` after `origin`.
    pub fn local_insert(&mut self, origin: Option<CharId>, ch: char) -> Op {
        let id = self.next_id();
        let op = Op::Insert { id, origin, ch };
        self.apply(op.clone());
        op
    }

    /// Generate (and apply) a delete of `id`.
    pub fn local_delete(&mut self, id: CharId) -> Op {
        let op = Op::Delete { id };
        self.apply(op.clone());
        op
    }

    // ---- remote integration ------------------------------------------

    /// Integrate an operation. Safe to call with duplicates or with ops whose
    /// causal dependencies have not arrived yet (they are buffered).
    ///
    /// Returns `false` if the operation was already known. Callers relaying ops
    /// through a mesh use this to stop forwarding loops.
    pub fn apply(&mut self, op: Op) -> bool {
        match op {
            Op::Insert { id, origin, ch } => {
                if self.seen.contains_key(&id) {
                    return false;
                }
                // Buffer until the element we are anchored to exists.
                if let Some(o) = origin {
                    if self.index_of(o).is_none() {
                        let slot = self.pending.entry(o).or_default();
                        if slot.iter().any(|p| p.id() == id) {
                            return false;
                        }
                        slot.push(Op::Insert { id, origin, ch });
                        return true;
                    }
                }
                self.observe_clock(id);
                self.integrate_insert(id, origin, ch);
                self.seen.insert(id, ());
                self.drain_pending(id);
                true
            }
            Op::Delete { id } => match self.index_of(id) {
                Some(i) => {
                    let was = self.elems[i].deleted;
                    self.elems[i].deleted = true;
                    !was
                }
                // Delete arrived before the insert it refers to.
                None => {
                    if self.pending_deletes.contains(&id) {
                        false
                    } else {
                        self.pending_deletes.push(id);
                        true
                    }
                }
            },
        }
    }

    /// Apply many ops, returning only those that were new to this replica.
    pub fn apply_all<I: IntoIterator<Item = Op>>(&mut self, ops: I) -> Vec<Op> {
        let mut fresh = Vec::new();
        for op in ops {
            if self.apply(op.clone()) {
                fresh.push(op);
            }
        }
        fresh
    }

    /// Merge a remote snapshot by replaying it as insert/delete operations.
    pub fn merge_snapshot(&mut self, elems: &[Elem]) {
        for e in elems {
            self.apply(Op::Insert {
                id: e.id,
                origin: e.origin,
                ch: e.ch,
            });
        }
        for e in elems {
            if e.deleted {
                self.apply(Op::Delete { id: e.id });
            }
        }
    }

    /// Replace local state wholesale with a remote snapshot.
    ///
    /// Used on first contact when both replicas independently built documents
    /// for the same file: merging two disjoint id spaces would duplicate the
    /// text, so the peer with the higher id adopts the other's document as the
    /// base and re-derives its own divergence as ops (see `engine::rebase`).
    pub fn adopt(&mut self, elems: Vec<Elem>) {
        self.seen = elems.iter().map(|e| (e.id, ())).collect();
        self.clock = elems.iter().map(|e| e.id.clock).max().unwrap_or(0);
        self.elems = elems;
        self.pending.clear();
        self.pending_deletes.clear();
    }

    /// RGA integration: walk forward from `origin`, skipping elements that
    /// concurrent-and-later replicas would have placed ahead of us. The first
    /// element with a *smaller* id than ours is our insertion point.
    fn integrate_insert(&mut self, id: CharId, origin: Option<CharId>, ch: char) {
        let start = match origin {
            None => 0,
            Some(o) => self.index_of(o).expect("origin checked by caller") + 1,
        };
        let mut i = start;
        while i < self.elems.len() {
            let cur = &self.elems[i];
            if cur.id < id {
                break;
            }
            // Elements anchored to something at-or-before our origin are
            // siblings we order against; anything anchored deeper belongs to a
            // subtree of a sibling and is skipped wholesale.
            i += 1;
        }
        self.elems.insert(
            i,
            Elem {
                id,
                origin,
                ch,
                deleted: false,
            },
        );
        // A tombstone may have been waiting for this element.
        if let Some(pos) = self.pending_deletes.iter().position(|d| *d == id) {
            self.pending_deletes.swap_remove(pos);
            let idx = self.index_of(id).unwrap();
            self.elems[idx].deleted = true;
        }
    }

    /// Re-attempt inserts that were waiting on `id`.
    fn drain_pending(&mut self, id: CharId) {
        let mut queue = vec![id];
        while let Some(anchor) = queue.pop() {
            if let Some(waiting) = self.pending.remove(&anchor) {
                // Deterministic order so replicas that buffer the same set
                // integrate it identically.
                let mut waiting = waiting;
                waiting.sort_by_key(|op| op.id());
                for op in waiting {
                    if let Op::Insert { id, origin, ch } = op {
                        if self.seen.contains_key(&id) {
                            continue;
                        }
                        self.observe_clock(id);
                        self.integrate_insert(id, origin, ch);
                        self.seen.insert(id, ());
                        queue.push(id);
                    }
                }
            }
        }
    }

    /// Build a document from scratch out of plain text, as local inserts.
    pub fn from_text(peer: PeerId, text: &str) -> (Self, Vec<Op>) {
        let mut doc = Doc::new(peer);
        let mut ops = Vec::new();
        let mut prev = None;
        for ch in text.chars() {
            let op = doc.local_insert(prev, ch);
            prev = Some(op.id());
            ops.push(op);
        }
        (doc, ops)
    }
}
