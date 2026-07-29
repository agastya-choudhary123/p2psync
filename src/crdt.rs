//! RGA (Replicated Growable Array) — a sequence CRDT over `char`s.
//!
//! Every character gets a globally unique `CharId` = (lamport clock, peer id).
//! Inserts name the element they follow (`origin`), deletes are tombstones.
//! Both operations are commutative and idempotent, so a set of operations
//! converges to the same visible string on every replica regardless of the
//! order in which they arrive.
//!
//! # Representation
//!
//! Elements live in an append-only arena and are threaded into a doubly linked
//! list, with a hash map from `CharId` to arena slot. That combination is what
//! makes integration cheap: finding an operation's origin is O(1), splicing is
//! O(1), and the only forward scan is across the handful of *concurrent*
//! siblings competing for the same position.
//!
//! A flat `Vec` with linear search is the obvious first implementation and it is
//! quadratic in the document length — loading a 220 KB file took ~18 seconds
//! before this change, because every one of 220,000 inserts scanned and
//! memmoved the whole vector. Walking the list to produce the text is still
//! O(n), but that is inherent to writing the file out.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

#[derive(Clone, Debug)]
struct Node {
    elem: Elem,
    next: Option<u32>,
    prev: Option<u32>,
}

/// Serialized form: just the elements in document order. Links and indexes are
/// rebuilt on load, so the on-disk format doesn't depend on arena layout.
#[derive(Serialize, Deserialize)]
struct DocRepr {
    elems: Vec<Elem>,
    clock: u64,
    peer: PeerId,
}

/// One CRDT document — the state of a single text file.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "DocRepr", into = "DocRepr")]
pub struct Doc {
    arena: Vec<Node>,
    head: Option<u32>,
    tail: Option<u32>,
    /// id -> arena slot. Doubles as the "have we seen this id" set.
    slot: HashMap<CharId, u32>,
    /// Lamport clock for locally generated ids.
    clock: u64,
    /// This replica's id (used when minting new character ids).
    peer: PeerId,
    /// Inserts whose `origin` has not arrived yet, keyed by the missing origin.
    pending: HashMap<CharId, Vec<Op>>,
    /// Deletes for characters we have not seen yet.
    pending_deletes: HashSet<CharId>,
}

impl From<DocRepr> for Doc {
    fn from(r: DocRepr) -> Self {
        let mut doc = Doc::new(r.peer);
        doc.adopt(r.elems);
        doc.clock = doc.clock.max(r.clock);
        doc
    }
}

impl From<Doc> for DocRepr {
    fn from(d: Doc) -> Self {
        DocRepr {
            elems: d.snapshot(),
            clock: d.clock,
            peer: d.peer,
        }
    }
}

impl Doc {
    pub fn new(peer: PeerId) -> Self {
        Self {
            arena: Vec::new(),
            head: None,
            tail: None,
            slot: HashMap::new(),
            clock: 0,
            peer,
            pending: HashMap::new(),
            pending_deletes: HashSet::new(),
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

    /// Walk the list in document order.
    fn iter(&self) -> impl Iterator<Item = &Elem> {
        let mut cur = self.head;
        std::iter::from_fn(move || {
            let i = cur?;
            let node = &self.arena[i as usize];
            cur = node.next;
            Some(&node.elem)
        })
    }

    /// Visible (non-tombstoned) characters as a string.
    pub fn text(&self) -> String {
        self.iter().filter(|e| !e.deleted).map(|e| e.ch).collect()
    }

    /// Ids of the visible characters, in order. Used by the change detector to
    /// map diff positions back onto CRDT elements.
    pub fn visible_ids(&self) -> Vec<CharId> {
        self.iter().filter(|e| !e.deleted).map(|e| e.id).collect()
    }

    pub fn len_visible(&self) -> usize {
        self.iter().filter(|e| !e.deleted).count()
    }

    /// Elements in document order, tombstones included.
    pub fn snapshot(&self) -> Vec<Elem> {
        self.iter().cloned().collect()
    }

    /// Number of stored elements, tombstones included.
    pub fn len_raw(&self) -> usize {
        self.arena.len()
    }

    /// How many stored elements are tombstones. Drives compaction decisions.
    pub fn tombstones(&self) -> usize {
        self.arena.iter().filter(|n| n.elem.deleted).count()
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
                if self.slot.contains_key(&id) {
                    return false;
                }
                // Buffer until the element we are anchored to exists.
                if let Some(o) = origin {
                    if !self.slot.contains_key(&o) {
                        let waiting = self.pending.entry(o).or_default();
                        if waiting.iter().any(|p| p.id() == id) {
                            return false;
                        }
                        waiting.push(Op::Insert { id, origin, ch });
                        return true;
                    }
                }
                self.observe_clock(id);
                self.integrate_insert(id, origin, ch);
                self.drain_pending(id);
                true
            }
            Op::Delete { id } => match self.slot.get(&id) {
                Some(&i) => {
                    let was = self.arena[i as usize].elem.deleted;
                    self.arena[i as usize].elem.deleted = true;
                    !was
                }
                // Delete arrived before the insert it refers to.
                None => self.pending_deletes.insert(id),
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

    /// Replace local state wholesale with a document-ordered element list.
    ///
    /// Used on first contact when both replicas independently built documents
    /// for the same file: merging two disjoint id spaces would duplicate the
    /// text, so one side adopts the other's document as the base and re-derives
    /// its own divergence as ops (see `engine::on_snapshot`).
    pub fn adopt(&mut self, elems: Vec<Elem>) {
        let n = elems.len();
        self.arena = Vec::with_capacity(n);
        self.slot = HashMap::with_capacity(n);
        self.clock = 0;
        for (i, elem) in elems.into_iter().enumerate() {
            let i = i as u32;
            self.clock = self.clock.max(elem.id.clock);
            self.slot.insert(elem.id, i);
            self.arena.push(Node {
                elem,
                prev: if i == 0 { None } else { Some(i - 1) },
                next: if i as usize + 1 == n { None } else { Some(i + 1) },
            });
        }
        self.head = if n == 0 { None } else { Some(0) };
        self.tail = if n == 0 { None } else { Some(n as u32 - 1) };
        self.pending.clear();
        self.pending_deletes.clear();
    }

    /// RGA integration: walk forward from `origin`, skipping elements that
    /// concurrent-and-later replicas would have placed ahead of us. The first
    /// element with a *smaller* id than ours is our insertion point.
    fn integrate_insert(&mut self, id: CharId, origin: Option<CharId>, ch: char) {
        let mut prev = origin.map(|o| self.slot[&o]);
        let mut next = match prev {
            Some(p) => self.arena[p as usize].next,
            None => self.head,
        };
        // Only concurrent siblings are ever scanned here, so this stays short.
        while let Some(n) = next {
            if self.arena[n as usize].elem.id < id {
                break;
            }
            prev = Some(n);
            next = self.arena[n as usize].next;
        }

        // A tombstone may have been waiting for this element.
        let deleted = self.pending_deletes.remove(&id);
        let me = self.arena.len() as u32;
        self.arena.push(Node {
            elem: Elem {
                id,
                origin,
                ch,
                deleted,
            },
            prev,
            next,
        });
        self.slot.insert(id, me);
        match prev {
            Some(p) => self.arena[p as usize].next = Some(me),
            None => self.head = Some(me),
        }
        match next {
            Some(n) => self.arena[n as usize].prev = Some(me),
            None => self.tail = Some(me),
        }
    }

    /// Re-attempt inserts that were waiting on `id`.
    fn drain_pending(&mut self, id: CharId) {
        let mut queue = vec![id];
        while let Some(anchor) = queue.pop() {
            if let Some(mut waiting) = self.pending.remove(&anchor) {
                // Deterministic order so replicas that buffer the same set
                // integrate it identically.
                waiting.sort_by_key(|op| op.id());
                for op in waiting {
                    if let Op::Insert { id, origin, ch } = op {
                        if self.slot.contains_key(&id) {
                            continue;
                        }
                        self.observe_clock(id);
                        self.integrate_insert(id, origin, ch);
                        queue.push(id);
                    }
                }
            }
        }
    }

    /// Build a document from scratch out of plain text, as local inserts.
    pub fn from_text(peer: PeerId, text: &str) -> (Self, Vec<Op>) {
        let mut doc = Doc::new(peer);
        let mut ops = Vec::with_capacity(text.len());
        let mut prev = None;
        for ch in text.chars() {
            let op = doc.local_insert(prev, ch);
            prev = Some(op.id());
            ops.push(op);
        }
        (doc, ops)
    }

    /// Build a document whose character ids are derived from the content
    /// itself, so two replicas that independently start from the *same* bytes
    /// produce byte-identical documents and can merge as a true CRDT.
    ///
    /// The peer component is a hash of the path and content, which keeps ids
    /// from colliding when the content differs — two different characters must
    /// never share an id.
    pub fn from_shared_content(path: &str, text: &str) -> (Self, PeerId) {
        let lineage = content_lineage(path, text);
        let mut doc = Doc::new(lineage);
        let mut prev = None;
        for ch in text.chars() {
            let op = doc.local_insert(prev, ch);
            prev = Some(op.id());
        }
        (doc, lineage)
    }
}

/// Deterministic lineage id for a (path, content) pair.
pub fn content_lineage(path: &str, text: &str) -> PeerId {
    use std::hash::{Hash, Hasher};
    // Not cryptographic — this only needs to be stable across peers and
    // collision-resistant enough that distinct content gets distinct ids.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut h);
    text.hash(&mut h);
    // Keep it out of the way of real peer ids' low bits.
    h.finish() | 1
}
