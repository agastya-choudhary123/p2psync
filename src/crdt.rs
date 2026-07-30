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
//!
//! # Memory Optimization
//!
//! The in-memory `Node` structure packs element data efficiently:
//! - `origin` is stored as a u32 slot index with u32::MAX as "start of document",
//!   instead of an `Option<CharId>` (saves 20 bytes per node).
//! - `next` and `prev` use u32::MAX sentinel instead of `Option<u32>`
//!   (saves 8 bytes per node).
//! The serialized `Elem` format remains unchanged for wire and disk compatibility.
//! Reconstruction happens on demand via `Node::to_elem()` during serialization.

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

const NONE_SLOT: u32 = u32::MAX;

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

/// In-memory node: compact version of Elem that stores origin as a slot index.
/// Actual CharId values are reconstructed on demand during serialization.
#[derive(Clone, Debug)]
struct Node {
    id: CharId,
    ch: char,
    deleted: bool,
    // u32::MAX means "start of document" (None)
    origin_slot: u32,
    // u32::MAX means None
    next: u32,
    // u32::MAX means None
    prev: u32,
}

impl Node {
    /// Convert to the serializable Elem form by reconstructing origin from slot.
    fn to_elem(&self, arena: &[Node]) -> Elem {
        let origin = if self.origin_slot == NONE_SLOT {
            None
        } else {
            Some(arena[self.origin_slot as usize].id)
        };
        Elem {
            id: self.id,
            origin,
            ch: self.ch,
            deleted: self.deleted,
        }
    }

    /// Create a node from an Elem, looking up the origin slot by id.
    /// Used when adopting or integrating.
    fn from_elem(elem: Elem, origin_slot: u32, next: u32, prev: u32) -> Self {
        Node {
            id: elem.id,
            ch: elem.ch,
            deleted: elem.deleted,
            origin_slot,
            next,
            prev,
        }
    }
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
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "DocRepr")]
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

/// Serialize straight out of the arena, emitting exactly the `DocRepr` shape.
///
/// The obvious `#[serde(into = "DocRepr")]` is what this replaces, and it was
/// ruinous: serde's `into` clones the whole `Doc` — arena *and* slot index —
/// and then `DocRepr` materializes a second copy as `Vec<Elem>`, all before a
/// byte is written. For a 660 KB file that was over 100 MB of allocation every
/// time the state was persisted, which happens every two seconds while the user
/// is typing. Streaming the elements costs one `Elem` at a time.
impl Serialize for Doc {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("DocRepr", 3)?;
        st.serialize_field("elems", &ElemSeq(self))?;
        st.serialize_field("clock", &self.clock)?;
        st.serialize_field("peer", &self.peer)?;
        st.end()
    }
}

/// The `elems` field of a `DocRepr`, rendered without ever holding the whole
/// `Vec<Elem>` in memory.
struct ElemSeq<'a>(&'a Doc);

impl Serialize for ElemSeq<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.0.arena.len()))?;
        for node in self.0.iter_nodes() {
            seq.serialize_element(&node.to_elem(&self.0.arena))?;
        }
        seq.end()
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

    /// Walk the list in document order, borrowing the nodes.
    ///
    /// This is the hot path — `text()` runs on every edit and every write — so
    /// it deliberately does *not* rebuild an `Elem`. Reconstructing the origin
    /// costs a random arena lookup per character, which is pure waste for the
    /// callers that only want `ch` or `id`.
    fn iter_nodes(&self) -> impl Iterator<Item = &Node> {
        let mut cur = self.head;
        let arena = &self.arena;
        std::iter::from_fn(move || {
            let i = cur?;
            let node = &arena[i as usize];
            cur = if node.next == NONE_SLOT { None } else { Some(node.next) };
            Some(node)
        })
    }

    /// Walk the list in document order as serializable `Elem`s, rebuilding each
    /// element's origin id from its slot. Only serialization needs this.
    fn iter(&self) -> impl Iterator<Item = Elem> + '_ {
        self.iter_nodes().map(|n| n.to_elem(&self.arena))
    }

    /// Visible (non-tombstoned) characters as a string.
    pub fn text(&self) -> String {
        self.iter_nodes().filter(|n| !n.deleted).map(|n| n.ch).collect()
    }

    /// Ids of the visible characters, in order. Used by the change detector to
    /// map diff positions back onto CRDT elements.
    pub fn visible_ids(&self) -> Vec<CharId> {
        self.iter_nodes().filter(|n| !n.deleted).map(|n| n.id).collect()
    }

    pub fn len_visible(&self) -> usize {
        self.iter_nodes().filter(|n| !n.deleted).count()
    }

    /// Elements in document order, tombstones included.
    pub fn snapshot(&self) -> Vec<Elem> {
        self.iter().collect()
    }

    /// Number of stored elements, tombstones included.
    pub fn len_raw(&self) -> usize {
        self.arena.len()
    }

    /// How many stored elements are tombstones. Drives compaction decisions.
    pub fn tombstones(&self) -> usize {
        self.arena.iter().filter(|n| n.deleted).count()
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
                    let was = self.arena[i as usize].deleted;
                    self.arena[i as usize].deleted = true;
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

            // Resolve the origin to a slot. An origin always precedes its
            // element in document order, so it is already in `slot` — but this
            // document arrives over the network, and indexing a missing key
            // would let any peer panic the daemon with a dangling origin.
            // Falling back to "start of document" keeps a malformed snapshot
            // from taking the process down; ordering of a corrupt element was
            // undefined anyway.
            let origin_slot = elem
                .origin
                .and_then(|o| self.slot.get(&o).copied())
                .unwrap_or(NONE_SLOT);

            let prev = if i == 0 { NONE_SLOT } else { i - 1 };
            let next = if i as usize + 1 == n { NONE_SLOT } else { i + 1 };

            self.arena.push(Node::from_elem(elem, origin_slot, next, prev));
        }
        self.head = if n == 0 { None } else { Some(0) };
        self.tail = if n == 0 { None } else { Some(n as u32 - 1) };
        self.pending.clear();
        self.pending_deletes.clear();

        // Reduce memory footprint after loading
        self.arena.shrink_to_fit();
        self.slot.shrink_to_fit();
    }

    /// RGA integration: walk forward from `origin`, skipping elements that
    /// concurrent-and-later replicas would have placed ahead of us. The first
    /// element with a *smaller* id than ours is our insertion point.
    fn integrate_insert(&mut self, id: CharId, origin: Option<CharId>, ch: char) {
        let origin_slot = origin.map(|o| self.slot[&o]).unwrap_or(NONE_SLOT);
        let mut prev = origin.map(|o| self.slot[&o]);
        let mut next = match prev {
            Some(p) => {
                let p_next = self.arena[p as usize].next;
                if p_next == NONE_SLOT { None } else { Some(p_next) }
            }
            None => self.head,
        };
        // Only concurrent siblings are ever scanned here, so this stays short.
        while let Some(n) = next {
            if self.arena[n as usize].id < id {
                break;
            }
            prev = Some(n);
            next = {
                let n_next = self.arena[n as usize].next;
                if n_next == NONE_SLOT { None } else { Some(n_next) }
            };
        }

        // A tombstone may have been waiting for this element.
        let deleted = self.pending_deletes.remove(&id);
        let me = self.arena.len() as u32;
        let prev_u32 = prev.unwrap_or(NONE_SLOT);
        let next_u32 = next.unwrap_or(NONE_SLOT);

        self.arena.push(Node {
            id,
            ch,
            deleted,
            origin_slot,
            prev: prev_u32,
            next: next_u32,
        });
        self.slot.insert(id, me);
        match prev {
            Some(p) => self.arena[p as usize].next = me,
            None => self.head = Some(me),
        }
        match next {
            Some(n) => self.arena[n as usize].prev = me,
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
        let char_count = text.chars().count();
        let mut doc = Doc::new(peer);
        let mut ops = Vec::with_capacity(char_count);
        let mut prev = None;
        for ch in text.chars() {
            let op = doc.local_insert(prev, ch);
            prev = Some(op.id());
            ops.push(op);
        }
        doc.arena.shrink_to_fit();
        doc.slot.shrink_to_fit();
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
        doc.arena.shrink_to_fit();
        doc.slot.shrink_to_fit();
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
