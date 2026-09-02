# p2psync — implementation notes

The internals, and a record of what was fixed along the way. Both used to live
in the README.

## How it works

### CRDT engine (`src/crdt.rs`)

An RGA (Replicated Growable Array) over `char`s. Every character carries a
unique `CharId` of `(lamport_clock, peer_id)`; inserts name the character they
follow, deletes are tombstones. Insertion walks forward from the origin and
skips elements with a *higher* id, so concurrent inserts at the same position
land in an order every replica computes identically — the tiebreak is the peer
id. Operations are commutative and idempotent, so arrival order and
retransmission don't matter.

**Storage is one record per run, not per character.** That distinction is the
whole memory story. A character needs its own id, its origin, a tombstone flag
and list links; even packed hard that is 40 bytes to hold one letter, so a
660 KB file cost 26 MB of arena plus 41 MB of `CharId -> slot` hash map. Over
100x the text, to store the text.

Nearly all of it is derivable, because of how ids are handed out: when someone
types, each character's origin is the character before it, its clock is the
previous clock plus one, and the peer never changes. A typed file is one
arithmetic progression. So a `Block` holds a first id, a first origin, and a
slice of text; character *i* has clock `clock + i` and origin "character
*i - 1*", both computed. Characters live in one flat `Vec<char>` that blocks
point into, so splitting a block moves no text — it hands out two ranges over
the same buffer.

A file typed start to finish is **one block**. An edit splits the block where it
lands, so the count tracks edited *regions* rather than document length, and a
checkpoint rebuilds it back to one. `examples/fragmentation_probe.rs` drives
5,000 edits through the real change detector: 240 K characters end up in 16,480
blocks (~15 characters each), per-edit cost stays flat at ~0.9 ms rather than
degrading, and a checkpoint returns it to a single block.

The linked list gives document order; `BlockIndex` maps an id to its block by
binary search over the clock ranges each peer contributed. Both are sized by the
number of blocks, not the number of characters.

The obvious first implementation — a flat `Vec` with linear search — is
quadratic in document length: loading a 220 KB file took **17.9 seconds**,
because each of 220,000 inserts scanned and memmoved the whole vector. It is now
**17.3 ms**. The only forward scan left is across the handful of *concurrent*
siblings competing for one position, and because ids rise with offset inside a
block, a whole block is skipped in one comparison rather than one per character.

Two details that matter in practice:

- **Causal buffering.** An insert whose origin hasn't arrived yet is parked in a
  pending map keyed by that missing origin, and integrated the moment it lands.
  Same for a delete that beats its insert. Without this, a 3-peer mesh drops
  operations whenever one link is slower than another.
- **Relay with dedup.** A peer forwards operations it hasn't seen before to its
  other peers, so partial meshes converge. `Doc::apply` returns whether the op
  was new, and only new ops are relayed — that's what stops a forwarding loop.
  New *files* are relayed the same way, and for a while they were not: ops were
  forwarded but the `Snapshot` and `BinaryMeta` that carry a file's first
  appearance were not, so in a chain `a—b—c` a file created at `a` reached `c`
  only when the 30-second manifest sweep got round to it. Relaying those two is
  conditional on the message having actually changed local state — there is no
  op-level dedup to lean on, so "did this change anything?" is what has to
  terminate the forwarding, and it does because CRDT merge is monotone: once a
  peer holds the content, handling it again is a no-op and the relay stops.
  `chain_topology_converges_quickly` builds a real chain (the older
  `three_peer_mesh_converges` has every peer dialing every other, which is
  exactly why this went unnoticed).

### Change detector (`src/diff.rs`)

The hard part. The user edits a file behind your back; you have to
reverse-engineer their keystrokes into operations on character ids. On each
filesystem event we diff the new file content against the CRDT's current text
with **Myers diff** (common prefix/suffix trimmed first, which is nearly the
whole file for a normal save) and walk the edit script: `Keep` advances the
anchor, `Del` tombstones that character id, `Ins` mints a new one after the
anchor. Editing one word in a 10 KB file produces ~19 operations, not 10,000.

### Wire format (`src/wire.rs`)

```
[4 bytes big-endian length][MessagePack body]
```

Operations are compressed before they hit the socket. The CRDT is
character-granular, but a *run* of typing has consecutive clocks where each
character's origin is the previous character — so a run collapses into one
frame carrying the text, and expands back into individual ops on arrival. This
is the difference between 3510 and 102 bytes for a typed sentence. Encoding is
positional MessagePack; field names would otherwise dominate a small payload.

### Filesystem watcher and echo suppression (`src/watcher.rs`, `src/engine.rs`)

`notify` gives FSEvents on macOS. Events are debounced 40ms per path, because
editors save in bursts (write temp, rename, touch). When we write a file
ourselves in response to a remote operation, we record the content hash, and the
resulting event is recognized as our own and dropped rather than diffed back
into "user edits".

That alone isn't enough, and the gap is a genuine lost-update bug worth calling
out: within the debounce window the user's save hasn't been processed yet, and a
remote write would clobber it on disk — then the echo check would see our own
hash and discard the user's edit permanently. So before applying any remote
change, the engine re-reads the file and folds any un-detected local edit into
the CRDT first (`absorb_local_edits`). The `simultaneous_edits_converge_on_disk`
and `three_peer_mesh_converges` tests both fail without it.

### Binary files (`src/binary.rs`)

Non-text files (invalid UTF-8 or containing NULs) don't get character-level
merging. They're identified by SHA-256 and transferred with a receiver-driven
rsync exchange: the receiver sends block signatures (rsync's rolling checksum
plus a truncated SHA-256), the sender computes copy/literal instructions
against them, and the receiver reconstructs and verifies the hash before
writing. The rolling checksum slides one byte at a time, so an insertion near
the front of a file resyncs instead of resending everything.

Conflicts are last-writer-wins, but on a **logical (Lamport) version** rather
than a wall clock: two machines whose clocks disagree would otherwise hand the
win to whichever one is set further ahead. Each local write bumps the version
past anything seen for that path, and the peer id breaks ties.

**Large files (video, images, archives) get four things a small file doesn't
need:**

- **The block size grows with the file** (`adaptive_block_size`), from 4 KB up
  to 4 MB, so the signature list stays a few hundred KB instead of scaling
  linearly with file size — a naive fixed 4 KB block would need 3 MB of
  signatures for a 1 GB file.
- **The delta streams as chunks, not one message.** A first sync of any file is
  entirely literal bytes (there's nothing on the far side to diff against yet),
  and one `BinaryDelta` message used to have to fit under `MAX_FRAME` (64 MB) —
  a hard, silent failure for anything bigger. `BinaryDeltaChunk` messages,
  numbered and bounded to ~4 MB each, remove the ceiling.
- **The receiver never buffers the reconstructed file.** Each chunk applies
  straight to a temp file as it arrives — matched blocks are read by seeking
  into the receiver's own existing file, not loaded into memory — so
  reconstructing an N-byte file costs O(chunk size), not O(N), on the
  receiving side. The temp file is renamed into place only after the whole
  hash verifies. Getting this bound to actually hold took a second fix: the
  socket-reading loop originally read frames as fast as the network delivered
  them regardless of whether anything downstream had caught up, so on
  loopback a 120 MB transfer arrived in one burst and briefly cost 112 MB
  resident anyway — the chunking was real, but nothing was pacing it. The read
  loop now waits for an acknowledgment that a chunk has been handed to its
  writer before reading the next frame, which throttles the TCP receive
  window and, transitively, the sender. Measured: **28 MB peak resident for
  both a 120 MB and a 300 MB transfer** — bounded by chunk size and pipeline
  depth, not file size, confirmed by the fact that 2.5x the file didn't move
  the number.
- **Hashing and diffing happen off the single-threaded engine loop.**
  Detecting that a large local file changed, and computing or applying a
  delta for one, all run via `spawn_blocking` and report back through the
  event channel — otherwise a multi-GB video would stall every other peer and
  file until it finished. The one place this isn't fully true: handing a
  chunk to its writer task goes through a bounded channel that the engine
  loop itself awaits, so a pathologically slow disk on the receiving end
  could still delay other peers by the time it takes that one send to clear —
  a real edge of the single-loop design, not eliminated here.

One place still scales with file size: generating a delta needs the *sender's*
file resident to slide the rolling-checksum window, so that side still costs
O(file size) memory (measured: ~280 MB resident for a 120 MB file, on the
sending side only). Signature generation (the receiver fingerprinting its own
file) and reconstruction (the receiver writing the result) don't — see
`binary.rs`'s module docs for the reasoning.

### Authentication (`src/auth.rs`)

Self-signed certificates encrypt but authenticate nothing, which leaves two
holes: anyone who can reach the port becomes a peer, and an active MITM can
impersonate one. `--secret` closes both. After `Hello` (which carries a 32-byte
nonce from each side) both peers send

```text
HMAC-SHA256(secret, role || nonce_initiator || nonce_responder || cert_fingerprint)
```

`role` stops an attacker reflecting our own proof back at us. The certificate
fingerprint is the part that defeats interception: to sit in the middle, a relay
must present its own certificate to the dialer while speaking to the real
listener as a client, so the two ends hash different certificates and the MACs
disagree. `intercepted_tls_connection_fails_authentication` builds exactly that
relay and asserts no data crosses it — with a control peer dialing directly, so
the test can't pass vacuously. Mismatched configurations fail loudly rather than
silently downgrading to an open link.

### Catch-up by manifest

On connect a peer sends a *manifest* of `(path, text_hash, base)` rather than a
snapshot per file, and the other side asks only for what it's missing or what
differs. The common reconnect case — where most files already agree — costs a
hash per file instead of a full CRDT document. The manifest is also re-broadcast
periodically, which keeps peers' views of each other fresh and doubles as
anti-entropy: a replica that somehow drifted sees the hash mismatch and pulls a
snapshot.

### Tombstone compaction

Deleted characters are tombstones, so a churned document grows without bound.
Collecting them safely needs causal stability — proof that every replica has seen
the deletions — and unanimous agreement on the text hash is that proof. When a
document is sufficiently bloated and every known peer reports the same hash and
lineage, the lowest-numbered peer issues a `Checkpoint`: the same visible text
with tombstones dropped and a fresh content-derived lineage. Peers adopt it only
if their own text already matches, so it can never silently discard an edit.

Two guards matter. Only the lowest peer id issues checkpoints, so two peers can't
compact into divergent lineages. And *every peer ever seen* must be connected —
rewriting the id space while someone is away would demote their offline edits
from a clean merge to last-writer-wins, so a missing peer defers compaction
indefinitely rather than risking that.

### Reconnection

Each configured peer has a dialer that retries every second. Catch-up runs
through the manifest above, so a returning peer receives snapshots only for what
actually changed — which subsumes whatever operations it missed, with no separate
replay log. Document state is persisted to `.p2psync/state.msgpack`, so a
restarted peer keeps its character ids instead of re-deriving a fresh,
incompatible id space.

### Shared-content lineage

Two independently created documents for one path have disjoint id spaces, so
merging them as CRDTs would duplicate the text. The fix is to derive character
ids from the content itself: a new file's lineage is a hash of its path and
bytes, so two machines handed *the same* folder build byte-identical documents
and merge as a true CRDT — no conflict copy, no lost edit. The peer component of
the id includes the content hash, so genuinely different content still gets
distinct ids and two different characters can never collide on one id.

That covers the overwhelmingly common real case (copy a folder to two laptops,
then edit both). Unrelated content at the same path has no correct merge, so it
still falls back to last-writer-wins with the loser preserved as
`<path>.conflict-<peer>`.


## History

### Fixed since the first cut

Each of these was a real limitation; each has a test that fails without the fix.

- **Quadratic CRDT loading** → arena + linked list, then block-per-run storage.
  17.9s → 17.3ms for 220 KB.
- **TLS without authentication** → `--secret` with HMAC proofs bound to the TLS
  certificate. Verified against an actual interception proxy.
- **Anyone could join** → same mechanism; mismatched configs fail loudly instead
  of downgrading.
- **Unbounded tombstone growth** → checkpoint compaction on unanimous agreement.
- **Whole-snapshot catch-up** → manifest of hashes; identical files cost nothing.
- **Binary conflicts trusted the wall clock** → logical Lamport versions.
- **Hardcoded ignore rules** → `.p2psyncignore` with glob support.
- **Two copies of one file couldn't merge** → content-derived lineage, so the
  copy-a-folder-to-two-machines case is now a true CRDT merge.

### Fixed in the second pass

These four came from *running* the daemon rather than reading it — the test
suite was green throughout, and stayed green while every one of them was live.

- **New files reached indirect peers 1200x slower than direct ones.** In a chain
  `a—b—c`, a file created at `a` took ~30 s to appear at `c` (the manifest
  sweep) versus ~60 ms at `b`. Ops were relayed; the messages that carry a
  file's first appearance were not. Now ~25 ms for text, ~95 ms for a binary.
  The window was not just slow — it was long enough for `c` to independently
  create the same path and produce a spurious `.conflict-` copy.
- **Persisting state cost 100 MB of allocation.** `#[serde(into = "DocRepr")]`
  clones the entire document — arena *and* id index — then builds a second copy
  as `Vec<Elem>`, then encodes the whole thing to one `Vec<u8>`, every 2 seconds
  while you type. Serialization now streams into the file. Resident memory for a
  660 KB document: 178 MB → 95 MB, with the on-disk format unchanged (a state
  file written by the old build still loads).
- **A CRDT node was 64 bytes per character**, of which 24 were an
  `Option<CharId>` origin that is recoverable from a `u32` slot index. Now 36.
- **A rejected peer retried once a second forever.** Found by noticing a daemon
  from an earlier session still knocking after 6h22m. Now exponential to a 30 s
  cap with jitter: 60 attempts/minute → 5. The subtlety is what counts as
  success — a wrong-secret peer *does* complete its TCP connect and fails at the
  handshake, so resetting the backoff there would have fixed nothing. Only a
  link that survived a few seconds counts, which keeps reconnect after a genuine
  peer restart at ~25 ms.

