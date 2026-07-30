//! RGA (Replicated Growable Array) — a sequence CRDT over `char`s.
//!
//! Every character gets a globally unique `CharId` = (lamport clock, peer id).
//! Inserts name the element they follow (`origin`), deletes are tombstones.
//! Both operations are commutative and idempotent, so a set of operations
//! converges to the same visible string on every replica regardless of the
//! order in which they arrive.
//!
//! # Representation: one record per *run*, not per character
//!
//! The obvious implementation gives every character its own record. That is
//! what this did originally, and the arithmetic is brutal: a `CharId` is two
//! `u64`s, and a character needs its own id, its origin, a tombstone flag and
//! list links. Even packed hard that is 40 bytes to store one letter, so a
//! 660 KB file cost 26 MB of arena and another 41 MB of `CharId -> slot` hash
//! map — over 100x the text.
//!
//! Almost all of it is redundant, because of how the ids are handed out. When
//! someone types, each character's origin is the character before it, its clock
//! is the previous clock plus one, and the peer never changes. A whole typed
//! file is one arithmetic progression.
//!
//! So a `Block` stores a maximal run: a first id, a first origin, and a slice of
//! text. Character `i` of a block has clock `clock + i` and origin "character
//! `i - 1`", both computed rather than stored. Characters live in one flat
//! `Vec<char>` that blocks point into, so splitting a block moves no text — it
//! just hands out two ranges over the same buffer.
//!
//! A file typed start to finish is a *single* block. Editing splits blocks where
//! the edits land, so the block count tracks the number of distinct edited
//! regions rather than the document length, and a checkpoint (see
//! `engine::maybe_compact`) rebuilds the document back down to one.
//!
//! Blocks are threaded into a doubly linked list for document order, and
//! `BlockIndex` maps an id to the block holding it. Both are sized by the number
//! of blocks, not the number of characters.

use crate::elemcodec;
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

/// "No block" for the linked list and for `head`/`tail`.
const NONE: u32 = u32::MAX;

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

/// One character, as it appears on the wire and on disk. The in-memory form is
/// a `Block`; this is what a block expands into.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Elem {
    pub id: CharId,
    pub origin: Option<CharId>,
    pub ch: char,
    pub deleted: bool,
}

/// A maximal run of characters that share a peer, occupy consecutive clocks,
/// each follow the one before it, and are either all live or all tombstoned.
#[derive(Clone, Debug)]
struct Block {
    /// Clock of the first character; character `i` has clock `clock + i`.
    clock: u64,
    /// Peer of every character in the block.
    peer: PeerId,
    /// Origin of the *first* character. Every later character's origin is the
    /// character before it, which is computed, not stored.
    origin: Option<CharId>,
    /// Offset of this block's text in `Doc::chars`.
    at: u32,
    len: u32,
    deleted: bool,
    next: u32,
    prev: u32,
}

impl Block {
    #[inline]
    fn id_at(&self, off: u32) -> CharId {
        CharId {
            clock: self.clock + off as u64,
            peer: self.peer,
        }
    }
    #[inline]
    fn end_clock(&self) -> u64 {
        self.clock + self.len as u64
    }
    /// Origin of character `off`: the block's own origin for the first, the
    /// preceding character for the rest.
    #[inline]
    fn origin_at(&self, off: u32) -> Option<CharId> {
        if off == 0 {
            self.origin
        } else {
            Some(self.id_at(off - 1))
        }
    }
    #[inline]
    fn last_id(&self) -> CharId {
        self.id_at(self.len - 1)
    }
}

/// `CharId -> (block, offset)`.
///
/// One entry per block per peer, not one per character. Entries for a peer are
/// sorted by clock and never overlap, so a lookup is a binary search over the
/// blocks that peer contributed.
#[derive(Clone, Copy, Debug)]
struct Span {
    clock: u64,
    len: u32,
    block: u32,
}

#[derive(Clone, Debug, Default)]
struct BlockIndex {
    spans: HashMap<PeerId, Vec<Span>>,
}

impl BlockIndex {
    fn clear(&mut self) {
        self.spans.clear();
    }

    fn shrink_to_fit(&mut self) {
        for v in self.spans.values_mut() {
            v.shrink_to_fit();
        }
        self.spans.shrink_to_fit();
    }

    fn locate(&self, id: &CharId) -> Option<(u32, u32)> {
        let v = self.spans.get(&id.peer)?;
        let i = v.partition_point(|s| s.clock <= id.clock);
        if i == 0 {
            return None;
        }
        let s = &v[i - 1];
        (id.clock < s.clock + s.len as u64).then(|| (s.block, (id.clock - s.clock) as u32))
    }

    fn add(&mut self, peer: PeerId, clock: u64, len: u32, block: u32) {
        let v = self.spans.entry(peer).or_default();
        let at = v.partition_point(|s| s.clock < clock);
        v.insert(at, Span { clock, len, block });
    }

    /// The span starting at `clock` gained one character at its end.
    fn grow(&mut self, peer: PeerId, clock: u64) {
        if let Some(v) = self.spans.get_mut(&peer) {
            if let Ok(i) = v.binary_search_by(|s| s.clock.cmp(&clock)) {
                v[i].len += 1;
            }
        }
    }

    /// The span starting at `clock` was split at offset `at`, and its tail is
    /// now held by `block`.
    fn split(&mut self, peer: PeerId, clock: u64, at: u32, block: u32) {
        if let Some(v) = self.spans.get_mut(&peer) {
            if let Ok(i) = v.binary_search_by(|s| s.clock.cmp(&clock)) {
                let tail = v[i].len - at;
                v[i].len = at;
                v.insert(
                    i + 1,
                    Span {
                        clock: clock + at as u64,
                        len: tail,
                        block,
                    },
                );
            }
        }
    }
}

/// Ids of the visible characters, in document order.
///
/// Backed by runs rather than one id per character: the change detector walks
/// this for every edit, and materializing 16 bytes per character was 10 MB of
/// allocation per keystroke on a large file.
#[derive(Clone, Debug, Default)]
pub struct VisibleIds {
    /// (first id of the run, number of characters, index of the run's first
    /// character within the visible sequence)
    runs: Vec<(CharId, u32, u32)>,
    len: usize,
}

impl VisibleIds {
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn iter(&self) -> impl Iterator<Item = CharId> + '_ {
        self.runs.iter().flat_map(|&(first, n, _)| {
            (0..n).map(move |k| CharId {
                clock: first.clock + k as u64,
                peer: first.peer,
            })
        })
    }
    pub fn get(&self, i: usize) -> Option<CharId> {
        if i >= self.len {
            return None;
        }
        let r = self.runs.partition_point(|&(_, _, start)| (start as usize) <= i);
        let (first, _, start) = self.runs[r - 1];
        Some(CharId {
            clock: first.clock + (i - start as usize) as u64,
            peer: first.peer,
        })
    }
}

/// Serialized form: just the elements in document order. Links and indexes are
/// rebuilt on load, so the on-disk format doesn't depend on the block layout.
#[derive(Serialize, Deserialize)]
struct DocRepr {
    #[serde(with = "crate::elemcodec")]
    elems: Vec<Elem>,
    clock: u64,
    peer: PeerId,
}

/// One CRDT document — the state of a single text file.
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "DocRepr")]
pub struct Doc {
    /// Every character ever inserted, in insertion order. Blocks reference
    /// contiguous slices of this; tombstoned text stays until a checkpoint
    /// rebuilds the document.
    chars: Vec<char>,
    blocks: Vec<Block>,
    head: u32,
    tail: u32,
    index: BlockIndex,
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

/// Serialize straight out of the blocks, emitting exactly the `DocRepr` shape.
///
/// The obvious `#[serde(into = "DocRepr")]` is what this replaces, and it was
/// ruinous: serde's `into` clones the whole `Doc` and then materializes every
/// character as an `Elem`, all before a byte is written — over 100 MB for a
/// 660 KB file, every two seconds while the user is typing.
impl Serialize for Doc {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let blob = elemcodec::encode_streaming(|| self.iter());
        let mut st = s.serialize_struct("DocRepr", 3)?;
        st.serialize_field("elems", &serde_bytes::ByteBuf::from(blob))?;
        st.serialize_field("clock", &self.clock)?;
        st.serialize_field("peer", &self.peer)?;
        st.end()
    }
}

impl Doc {
    pub fn new(peer: PeerId) -> Self {
        Self {
            chars: Vec::new(),
            blocks: Vec::new(),
            head: NONE,
            tail: NONE,
            index: BlockIndex::default(),
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

    // ---- reading -----------------------------------------------------

    /// Walk blocks in document order.
    fn iter_blocks(&self) -> impl Iterator<Item = &Block> {
        let mut cur = self.head;
        let blocks = &self.blocks;
        std::iter::from_fn(move || {
            if cur == NONE {
                return None;
            }
            let b = &blocks[cur as usize];
            cur = b.next;
            Some(b)
        })
    }

    /// Expand to one `Elem` per character, in document order. Only
    /// serialization and snapshots need this.
    fn iter(&self) -> impl Iterator<Item = Elem> + '_ {
        let mut cur = self.head;
        let mut off = 0u32;
        std::iter::from_fn(move || loop {
            if cur == NONE {
                return None;
            }
            let b = &self.blocks[cur as usize];
            if off >= b.len {
                cur = b.next;
                off = 0;
                continue;
            }
            let e = Elem {
                id: b.id_at(off),
                origin: b.origin_at(off),
                ch: self.chars[(b.at + off) as usize],
                deleted: b.deleted,
            };
            off += 1;
            return Some(e);
        })
    }

    /// Visible (non-tombstoned) characters as a string.
    pub fn text(&self) -> String {
        let mut s = String::with_capacity(self.len_visible());
        for b in self.iter_blocks() {
            if !b.deleted {
                s.extend(self.chars[b.at as usize..(b.at + b.len) as usize].iter());
            }
        }
        s
    }

    /// Ids of the visible characters, in order. Used by the change detector to
    /// map diff positions back onto CRDT elements.
    pub fn visible_ids(&self) -> VisibleIds {
        let mut runs = Vec::new();
        let mut n = 0u32;
        for b in self.iter_blocks() {
            if !b.deleted {
                runs.push((b.id_at(0), b.len, n));
                n += b.len;
            }
        }
        VisibleIds {
            runs,
            len: n as usize,
        }
    }

    pub fn len_visible(&self) -> usize {
        self.iter_blocks().filter(|b| !b.deleted).map(|b| b.len as usize).sum()
    }

    /// Elements in document order, tombstones included.
    pub fn snapshot(&self) -> Vec<Elem> {
        self.iter().collect()
    }

    /// Number of stored characters, tombstones included.
    pub fn len_raw(&self) -> usize {
        self.iter_blocks().map(|b| b.len as usize).sum()
    }

    /// How many stored characters are tombstones. Drives compaction decisions.
    pub fn tombstones(&self) -> usize {
        self.iter_blocks().filter(|b| b.deleted).map(|b| b.len as usize).sum()
    }

    /// How many blocks the document occupies. The point of the representation,
    /// and what the memory cost actually scales with.
    pub fn block_count(&self) -> usize {
        self.iter_blocks().count()
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
                if self.index.locate(&id).is_some() {
                    return false;
                }
                // Buffer until the element we are anchored to exists.
                if let Some(o) = origin {
                    if self.index.locate(&o).is_none() {
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
            Op::Delete { id } => match self.index.locate(&id) {
                Some((b, off)) => {
                    if self.blocks[b as usize].deleted {
                        return false;
                    }
                    // Tombstones are per block, so isolate the character into
                    // its own block before marking it.
                    let target = self.isolate(b, off);
                    self.blocks[target as usize].deleted = true;
                    true
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
    ///
    /// This is also the path that recoalesces a document: elements arrive in
    /// order, so a file that is one run again becomes one block again.
    pub fn adopt(&mut self, elems: Vec<Elem>) {
        self.chars = Vec::with_capacity(elems.len());
        self.blocks = Vec::new();
        self.index.clear();
        self.head = NONE;
        self.tail = NONE;
        self.clock = 0;
        self.pending.clear();
        self.pending_deletes.clear();

        for e in elems {
            // Defensive: this list arrives over the network, and a repeated id
            // would put two characters in one slot of the index.
            if self.index.locate(&e.id).is_some() {
                continue;
            }
            self.clock = self.clock.max(e.id.clock);

            let extends = match self.blocks.last() {
                Some(b) => {
                    b.peer == e.id.peer
                        && b.end_clock() == e.id.clock
                        && b.deleted == e.deleted
                        && e.origin == Some(b.last_id())
                        && (b.at + b.len) as usize == self.chars.len()
                }
                None => false,
            };
            self.chars.push(e.ch);
            if extends {
                let last = self.blocks.last_mut().expect("checked above");
                last.len += 1;
                let (peer, clock) = (last.peer, last.clock);
                self.index.grow(peer, clock);
            } else {
                let idx = self.blocks.len() as u32;
                let prev = self.tail;
                self.blocks.push(Block {
                    clock: e.id.clock,
                    peer: e.id.peer,
                    origin: e.origin,
                    at: (self.chars.len() - 1) as u32,
                    len: 1,
                    deleted: e.deleted,
                    next: NONE,
                    prev,
                });
                if prev == NONE {
                    self.head = idx;
                } else {
                    self.blocks[prev as usize].next = idx;
                }
                self.tail = idx;
                self.index.add(e.id.peer, e.id.clock, 1, idx);
            }
        }
        self.chars.shrink_to_fit();
        self.blocks.shrink_to_fit();
        self.index.shrink_to_fit();
    }

    // ---- block surgery ------------------------------------------------

    /// Split block `b` at offset `at`, returning the index of the tail block
    /// holding `[at, len)`. No text moves; the halves point into the same
    /// buffer.
    fn split_block(&mut self, b: u32, at: u32) -> u32 {
        let (clock, peer, block_at, len, deleted, next) = {
            let x = &self.blocks[b as usize];
            (x.clock, x.peer, x.at, x.len, x.deleted, x.next)
        };
        debug_assert!(at > 0 && at < len, "split at {at} of {len}");
        let tail = Block {
            clock: clock + at as u64,
            peer,
            origin: Some(CharId {
                clock: clock + at as u64 - 1,
                peer,
            }),
            at: block_at + at,
            len: len - at,
            deleted,
            next,
            prev: b,
        };
        let ti = self.blocks.len() as u32;
        self.blocks.push(tail);
        self.blocks[b as usize].len = at;
        self.blocks[b as usize].next = ti;
        if next == NONE {
            self.tail = ti;
        } else {
            self.blocks[next as usize].prev = ti;
        }
        self.index.split(peer, clock, at, ti);
        ti
    }

    /// Split as needed so that character `off` of block `b` is a block of its
    /// own, and return that block.
    fn isolate(&mut self, b: u32, off: u32) -> u32 {
        let len = self.blocks[b as usize].len;
        if off + 1 < len {
            self.split_block(b, off + 1);
        }
        if off > 0 {
            self.split_block(b, off)
        } else {
            b
        }
    }

    /// Link a freshly created block after `prev` (or at the head when `None`).
    fn splice_after(&mut self, prev: Option<u32>, new: u32) {
        match prev {
            Some(p) => {
                let next = self.blocks[p as usize].next;
                self.blocks[new as usize].prev = p;
                self.blocks[new as usize].next = next;
                self.blocks[p as usize].next = new;
                if next == NONE {
                    self.tail = new;
                } else {
                    self.blocks[next as usize].prev = new;
                }
            }
            None => {
                let old_head = self.head;
                self.blocks[new as usize].prev = NONE;
                self.blocks[new as usize].next = old_head;
                if old_head == NONE {
                    self.tail = new;
                } else {
                    self.blocks[old_head as usize].prev = new;
                }
                self.head = new;
            }
        }
    }

    /// RGA integration: walk forward from `origin`, skipping elements that
    /// concurrent-and-later replicas would have placed ahead of us. The first
    /// element with a *smaller* id than ours is our insertion point.
    fn integrate_insert(&mut self, id: CharId, origin: Option<CharId>, ch: char) {
        // Position of the element we insert after, and of the one we insert
        // before, as (block, offset).
        let mut prev = origin.and_then(|o| self.index.locate(&o));
        let mut next = match prev {
            Some((b, off)) => {
                let blk = &self.blocks[b as usize];
                if off + 1 < blk.len {
                    Some((b, off + 1))
                } else if blk.next == NONE {
                    None
                } else {
                    Some((blk.next, 0))
                }
            }
            None => (self.head != NONE).then_some((self.head, 0)),
        };

        // Only concurrent siblings are ever scanned here, so this stays short.
        // Ids rise with offset inside a block, so if the first candidate in a
        // block outranks us then so does every later one — the whole block is
        // skipped in one step rather than one character at a time.
        while let Some((b, off)) = next {
            let blk = &self.blocks[b as usize];
            if blk.id_at(off) < id {
                break;
            }
            prev = Some((b, blk.len - 1));
            next = (blk.next != NONE).then_some((blk.next, 0));
        }

        // A tombstone may have been waiting for this element.
        let deleted = self.pending_deletes.remove(&id);

        if let Some((b, off)) = prev {
            // Make `off` the last character of its block, so the new character
            // lands between two whole blocks.
            if off + 1 < self.blocks[b as usize].len {
                self.split_block(b, off + 1);
            }
            // The common case by far: typing forward. The character continues
            // this block's run and its text is the next thing in the buffer, so
            // the block simply grows — no new block, no new index entry.
            let blk = &self.blocks[b as usize];
            if blk.peer == id.peer
                && blk.end_clock() == id.clock
                && blk.deleted == deleted
                && origin == Some(blk.last_id())
                && (blk.at + blk.len) as usize == self.chars.len()
            {
                let (peer, clock) = (blk.peer, blk.clock);
                self.chars.push(ch);
                self.blocks[b as usize].len += 1;
                self.index.grow(peer, clock);
                return;
            }
            let new = self.push_block(id, origin, ch, deleted);
            self.splice_after(Some(b), new);
        } else {
            let new = self.push_block(id, origin, ch, deleted);
            self.splice_after(None, new);
        }
    }

    /// Append a one-character block to the arena (unlinked).
    fn push_block(&mut self, id: CharId, origin: Option<CharId>, ch: char, deleted: bool) -> u32 {
        let at = self.chars.len() as u32;
        self.chars.push(ch);
        let idx = self.blocks.len() as u32;
        self.blocks.push(Block {
            clock: id.clock,
            peer: id.peer,
            origin,
            at,
            len: 1,
            deleted,
            next: NONE,
            prev: NONE,
        });
        self.index.add(id.peer, id.clock, 1, idx);
        idx
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
                        if self.index.locate(&id).is_some() {
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
        doc.chars.reserve(char_count);
        let mut ops = Vec::with_capacity(char_count);
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
        doc.chars.reserve(text.chars().count());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every invariant the block representation has to maintain, checked
    /// against the document's own linked list.
    fn check(doc: &Doc) {
        let mut seen = 0usize;
        let mut cur = doc.head;
        let mut prev_expected = NONE;
        while cur != NONE {
            let b = &doc.blocks[cur as usize];
            assert!(b.len > 0, "empty block");
            assert_eq!(b.prev, prev_expected, "broken prev link");
            assert!(
                (b.at + b.len) as usize <= doc.chars.len(),
                "block text out of range"
            );
            // Every character in the block must resolve back to it.
            for off in 0..b.len {
                assert_eq!(
                    doc.index.locate(&b.id_at(off)),
                    Some((cur, off)),
                    "index disagrees for offset {off}"
                );
            }
            seen += b.len as usize;
            prev_expected = cur;
            cur = b.next;
        }
        assert_eq!(doc.tail, prev_expected, "tail is not the last block");
        assert_eq!(seen, doc.len_raw());
    }

    #[test]
    fn typed_text_is_a_single_block() {
        let (doc, _) = Doc::from_text(1, "the quick brown fox jumps over the lazy dog");
        assert_eq!(doc.block_count(), 1, "sequential typing must coalesce");
        assert_eq!(doc.text(), "the quick brown fox jumps over the lazy dog");
        check(&doc);
    }

    #[test]
    fn a_large_document_stays_one_block() {
        let text: String = (0..5000).map(|i| format!("line {i}: some content here\n")).collect();
        let (doc, _) = Doc::from_text(1, &text);
        assert_eq!(doc.block_count(), 1);
        assert_eq!(doc.text(), text);
        check(&doc);
    }

    #[test]
    fn inserting_in_the_middle_splits_once() {
        let (mut doc, _) = Doc::from_text(1, "abcdef");
        let ids = doc.visible_ids();
        doc.local_insert(ids.get(2), 'X');
        assert_eq!(doc.text(), "abcXdef");
        // abc | X | def
        assert_eq!(doc.block_count(), 3);
        check(&doc);
    }

    #[test]
    fn deleting_isolates_only_the_deleted_character() {
        let (mut doc, _) = Doc::from_text(1, "abcdef");
        let ids = doc.visible_ids();
        doc.local_delete(ids.get(2).unwrap());
        assert_eq!(doc.text(), "abdef");
        assert_eq!(doc.len_raw(), 6);
        assert_eq!(doc.tombstones(), 1);
        check(&doc);
    }

    #[test]
    fn deleting_at_the_ends_does_not_over_split() {
        let (mut doc, _) = Doc::from_text(1, "abc");
        let ids = doc.visible_ids();
        doc.local_delete(ids.get(0).unwrap());
        check(&doc);
        assert_eq!(doc.text(), "bc");
        let ids = doc.visible_ids();
        doc.local_delete(ids.get(1).unwrap());
        assert_eq!(doc.text(), "b");
        assert_eq!(doc.tombstones(), 2);
        check(&doc);
    }

    #[test]
    fn deleting_the_whole_document_then_typing_again() {
        let (mut doc, _) = Doc::from_text(1, "hello");
        let ids: Vec<CharId> = doc.visible_ids().iter().collect();
        for id in ids {
            doc.local_delete(id);
        }
        assert_eq!(doc.text(), "");
        assert_eq!(doc.len_visible(), 0);
        check(&doc);
        let mut anchor = None;
        for ch in "again".chars() {
            anchor = Some(doc.local_insert(anchor, ch).id());
        }
        assert_eq!(doc.text(), "again");
        check(&doc);
    }

    #[test]
    fn adopt_recoalesces_a_fragmented_document() {
        let (mut doc, _) = Doc::from_text(1, "abcdef");
        let ids = doc.visible_ids();
        doc.local_insert(ids.get(2), 'X');
        doc.local_insert(ids.get(4), 'Y');
        assert!(doc.block_count() > 1);
        let snap = doc.snapshot();
        let text = doc.text();
        let mut fresh = Doc::new(1);
        fresh.adopt(snap);
        assert_eq!(fresh.text(), text);
        check(&fresh);
    }

    #[test]
    fn snapshot_round_trips_through_adopt() {
        let (mut doc, _) = Doc::from_text(1, "hello world");
        let ids = doc.visible_ids();
        doc.local_delete(ids.get(4).unwrap());
        doc.local_insert(ids.get(0), 'Z');
        let snap = doc.snapshot();
        let mut fresh = Doc::new(2);
        fresh.adopt(snap.clone());
        assert_eq!(fresh.snapshot(), snap, "adopt must preserve every element");
        assert_eq!(fresh.text(), doc.text());
        check(&fresh);
    }

    #[test]
    fn visible_ids_matches_element_order() {
        let (mut doc, _) = Doc::from_text(1, "abcdef");
        let ids = doc.visible_ids();
        doc.local_delete(ids.get(1).unwrap());
        doc.local_delete(ids.get(3).unwrap());
        let expect: Vec<CharId> = doc.iter().filter(|e| !e.deleted).map(|e| e.id).collect();
        let got: Vec<CharId> = doc.visible_ids().iter().collect();
        assert_eq!(got, expect);
        for (i, id) in expect.iter().enumerate() {
            assert_eq!(doc.visible_ids().get(i), Some(*id), "get({i})");
        }
        assert_eq!(doc.visible_ids().len(), expect.len());
    }

    #[test]
    fn concurrent_inserts_at_one_position_converge() {
        let (base, ops) = Doc::from_text(1, "ab");
        let _ = base;
        let mut a = Doc::new(7);
        let mut b = Doc::new(9);
        a.apply_all(ops.clone());
        b.apply_all(ops.clone());
        let anchor = a.visible_ids().get(0);

        let oa = a.local_insert(anchor, 'A');
        let ob = b.local_insert(anchor, 'B');
        a.apply(ob);
        b.apply(oa);
        assert_eq!(a.text(), b.text(), "concurrent inserts must converge");
        check(&a);
        check(&b);
    }

    #[test]
    fn out_of_order_delivery_converges_and_keeps_invariants() {
        let mut x = 0x243F6A8885A308D3u64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..150 {
            let (_, ops) = Doc::from_text(1, "seed text");
            let mut a = Doc::new(2);
            a.apply_all(ops.clone());

            // Build a batch of edits on `a`.
            let mut batch = Vec::new();
            for _ in 0..25 {
                let ids: Vec<CharId> = a.visible_ids().iter().collect();
                if !ids.is_empty() && rnd() % 3 == 0 {
                    let victim = ids[(rnd() as usize) % ids.len()];
                    batch.push(a.local_delete(victim));
                } else {
                    let anchor = if ids.is_empty() {
                        None
                    } else {
                        Some(ids[(rnd() as usize) % ids.len()])
                    };
                    let ch = char::from(b'a' + (rnd() % 26) as u8);
                    batch.push(a.local_insert(anchor, ch));
                }
            }
            check(&a);

            // Deliver them to a fresh replica in scrambled order, twice over.
            let mut b = Doc::new(3);
            b.apply_all(ops.clone());
            let mut shuffled = batch.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, (rnd() as usize) % (i + 1));
            }
            b.apply_all(shuffled.clone());
            b.apply_all(shuffled);
            assert_eq!(a.text(), b.text(), "scrambled delivery must converge");
            check(&b);
        }
    }
}
