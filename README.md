# p2psync

Peer-to-peer file synchronization with **streaming** CRDT sync. No central
server, no polling, no conflict prompts. Two peers watch local directories, and
edits stream between them as character-level CRDT operations that merge
automatically — even when both sides edit the same file at the same instant.

```
Peer A                                     Peer B
┌──────────────────────┐                  ┌──────────────────────┐
│ Filesystem Watcher   │  FSEvents        │ Filesystem Watcher   │
│  (debounced, 40ms)   │                  │                      │
│         ↓            │                  │         ↓            │
│ Change Detector      │  Myers diff of   │ Change Detector      │
│  (file → CRDT ops)   │  file vs CRDT    │                      │
│         ↓            │                  │         ↓            │
│ CRDT Engine (RGA)    │◄──TCP / TLS─────►│ CRDT Engine (RGA)    │
│  one doc per file    │  MessagePack     │                      │
│         ↓            │  frames          │         ↓            │
│ File Writer          │                  │ File Writer          │
│  (+ echo suppression)│                  │                      │
└──────────────────────┘                  └──────────────────────┘
```

## Quick start

```bash
cargo build --release

# terminal 1
./target/release/p2psync ~/sync-a -l 127.0.0.1:7901 --name alice

# terminal 2
./target/release/p2psync ~/sync-b -l 127.0.0.1:7902 --peer 127.0.0.1:7901 --name bob

# now edit a file in ~/sync-a and watch it appear in ~/sync-b
```

On a LAN, let the peers find each other, encrypt the link, and require a
shared secret so only your machines can join:

```bash
./target/release/p2psync ~/sync -l 0.0.0.0:7901 --discover --tls --secret "$MY_KEY"
```

Exclude paths with a `.p2psyncignore` in the sync root (gitignore-style globs,
`*`/`?`/`**`, trailing `/` for directories):

```
*.log
build/
node_modules
**/*.tmp
```

`--discover` advertises over mDNS (`_p2psync._tcp.local.`). Bind to a routable
interface, not `127.0.0.1` — mDNS publishes your LAN address, so a loopback-only
peer advertises an address nothing can reach.

The full guided tour — create, edit, concurrent edit, rename, binary delta,
delete — is one command:

```bash
./scripts/demo.sh
```

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
rsync exchange: the receiver sends 4 KB block signatures (rsync's rolling
checksum plus a truncated SHA-256), the sender computes copy/literal
instructions against them, and the receiver reconstructs and verifies the hash
before writing. The rolling checksum slides one byte at a time, so an insertion
near the front of a file resyncs instead of resending everything.

Conflicts are last-writer-wins, but on a **logical (Lamport) version** rather
than a wall clock: two machines whose clocks disagree would otherwise hand the
win to whichever one is set further ahead. Each local write bumps the version
past anything seen for that path, and the peer id breaks ties.

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

## Measured performance

Apple M4, macOS 15.3.1, 10 cores, release build, peers on localhost.
Reproduce with `cargo run --release --bin bench`.

**Sync latency** — file written on peer A to updated content readable on peer B:

| debounce | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 40ms (default) | 56.0ms | 57.7ms | 58.3ms | 58.5ms |
| 1ms (`P2PSYNC_DEBOUNCE_MS=1`) | 14.4ms | 15.5ms | 16.2ms | 16.3ms |

The debounce is a hard floor, so the second row is the honest measure of the
transport plus CRDT path; the remaining ~13ms is FSEvents delivery latency, not
compute. Both are inside the 100ms target.

**Bandwidth** — bytes on the wire per byte of user edit, in a 10,690-byte file:

| edit | ops | wire bytes |
|---|---|---|
| one character typed | 1 | 40 |
| word replaced (15→14 chars) | 19 | 105 |
| 62-char sentence appended | 62 | 102 (1.6× the changed bytes) |
| 1 byte flipped in a 4 MB binary | — | 17,491 (0.44% of the file) |

Bulk typing approaches 1:1 thanks to run compression. An *isolated* single
character costs ~40 bytes, and that floor is inherent to character-granular
CRDTs: the op carries two 64-bit ids plus the file path.

**CRDT cost vs. document size** (`cargo run --release --example scale_probe`),
before and after the arena rewrite:

| document | load (Vec) | load (arena) | 1-char edit | 2000-char paste |
|---|---|---|---|---|
| 3 K chars | 0.6ms | 0.2ms | 0.015ms | 0.3ms |
| 12 K chars | 49.5ms | 1.7ms | 0.109ms | 0.4ms |
| 56 K chars | 1,072ms | 5.0ms | 0.349ms | 0.5ms |
| 221 K chars | 17,884ms | **17.3ms** | 1.2ms | 1.2ms |

Loading is now linear rather than quadratic. Single-character edits got slightly
slower (1.6ms → 2.5ms at 221 K chars) because walking a linked list has worse
cache locality than scanning a `Vec` — a fine trade for removing an 18-second
stall.

**Scalability** — latency to the slowest peer in a full mesh:

| peers | p50 (40ms debounce) | p50 (1ms) |
|---|---|---|
| 3 | 56.9ms | 14.6ms |
| 5 | 57.0ms | 14.9ms |
| 10 | 58.1ms | 16.6ms |

Fan-out is per-peer serialization, so 10 peers costs ~1ms more than 3.

**Footprint** — a 660 KB text file, measured end to end. Every one of these
numbers is the same root cause: a character carries two 16-byte `CharId`s, its
own and its origin's.

| | before | after |
|---|---|---|
| bytes on the wire to sync a 108 KB file | 3.44 MB (31.6x) | **218 KB (2.0x)** |
| `.p2psync/state.msgpack` | 50.6 MB (77x) | **1.3 MB (2.0x)** |
| resident memory | 178 MB | **12.2 MB** |
| propagation latency while syncing it | p50 57ms, spikes to 117ms | **p50 57ms, max 60ms** |

One idea, applied three times: store a *run*, not a character.

`src/elemcodec.rs` encodes an element sequence that way for the wire and the
state file — sequential text costs a flags byte and the UTF-8 of the character,
two bytes rather than 45. `Doc` itself stores blocks rather than per-character
nodes, which is what took memory from 95 MB to 12 MB; the per-character design
spent 26 MB on the arena and 41 MB on the `CharId -> slot` hash map, and `heap`
showing that index *larger than the arena it indexed* is what pointed at the
representation rather than at any one allocation. State serialization also
streams instead of building the whole encoding in memory first.

The latency spikes were the state file: 50 MB written synchronously every two
seconds, landing on top of whatever edit was in flight. At 1.3 MB they are gone.

TLS costs 0.2% on top of that (2,260 bytes versus 2,254 for a small sync), so
encryption was never the expense — encrypting 31x more bytes than necessary was.

**Recovery** — a cold peer joining after missing 200 edits (6,096 bytes) caught
up in **4.5ms** and streamed normally afterwards.

**Convergence** — verified by fuzzing rather than asserted: 2–5 replicas, random
concurrent inserts and deletes, shuffled delivery, duplicated messages, and
causally-out-of-order arrival, all asserting byte-identical final text. Plus
end-to-end tests that run real engines over real sockets and compare files on
disk.

## Tests

```bash
cargo test              # 67 tests
```

- `src/crdt.rs` unit tests — block invariants checked against the document's own
  linked list after every operation (each character resolves back to its block,
  links are consistent, counts agree), split/merge behaviour, and scrambled
  delivery.
- `tests/crdt.rs` — convergence and commutativity fuzzing, causal buffering,
  change-detector round-trips, wire compression round-trips.
- `tests/binary.rs` — rolling checksum against recomputation, block reuse,
  delta reconstruction fuzzing.
- `tests/sync_e2e.rs` — two and three real engines on real sockets: both
  directions, incremental edits, echo suppression, simultaneous edits
  converging on disk, binary delta, deletes, nested directories, TLS,
  reconnect-after-offline, and a three-peer *chain* — peer 3 dials only peer 2,
  so a new file has to be relayed to reach it.
- `tests/hardening.rs` — authentication (right secret, wrong secret, missing
  secret, and a real TLS-terminating MITM relay), independently-created files
  merging without a conflict copy, ignore rules, manifest catch-up, and
  tombstone compaction.
- `src/auth.rs` unit tests — reflected proofs, wrong keys, mismatched channel
  bindings, nonce reuse.

## Limitations

### Fixed since the first cut

Each of these was a real limitation; each has a test that fails without the fix.

- **Quadratic CRDT loading** → arena + linked list. 17.9s → 27.8ms for 220 KB.
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

### Still real

- **RGA can interleave** two peers typing *different words at the identical
  position*. The result is deterministic and identical everywhere, which is what
  convergence guarantees, but it may not be what either author wanted. This is
  inherent to RGA; avoiding it means a different algorithm (Fugue, Peritext), not
  a patch. In practice file sync diffs whole files, so edits arrive as contiguous
  runs anchored to surviving characters, and interleaving is rare.
- **Genuinely different content at the same path is still last-writer-wins**,
  now on mtime, with the loser kept as `<path>.conflict-<peer>`. There is no
  shared history to merge and no logical clock spanning unrelated documents, so
  something has to lose; the choice is which, and whether the loser survives.
- **Compaction defers to absent peers.** It requires every peer ever seen to be
  connected, so one permanently-dead peer means tombstones accumulate forever.
  Correct-but-conservative: the alternative risks demoting an offline peer's
  edits to last-writer-wins.
- **A differing file still ships its whole document.** The manifest skips files
  that agree, but a one-character difference sends the full CRDT state. Version
  vectors and an op log would fix it.
- **Per-edit work is O(file size)** regardless of the CRDT: every save re-reads
  the file, diffs it, and rewrites it on the far side. Fine to a few hundred KB.
- **Memory is ~18x the text**, down from ~270x. A 660 KB file costs 12.2 MB
  resident against a 5.5 MB empty-directory baseline, so the document itself is
  about 6.7 MB. Most of that is now the character buffer, which is a `Vec<char>`
  at a flat 4 bytes per character — a `String` would be 1 byte for ASCII, at the
  cost of mapping character offsets onto byte offsets inside every block.
  Tombstoned text also stays in the buffer until a checkpoint rebuilds the
  document.
- **Sustained editing fragments a document** until a checkpoint recoalesces it:
  every edit splits a block, and `examples/fragmentation_probe.rs` measures
  5,000 edits taking 240 K characters from 1 block to 16,480. Per-edit cost is
  flat across that range (~0.9 ms) so it is not a performance cliff, but the
  memory win narrows as a document is edited, and it depends on compaction —
  which itself defers while any known peer is offline, per the entry above.
- **The state file is rewritten whole every 2 seconds** while you are typing.
  It is 2.0x the document now rather than 77x, so that is 1.3 MB rather than
  50 MB per write, but it is still the entire document each time. An op log with
  periodic compaction would make it incremental.
- **`--secret` over plaintext is authorization only.** With no certificate there
  is nothing to bind to, so an active MITM is still possible; the process says so
  on startup. Use `--tls`.
- **Encryption is opt-in, and the default is not it.** Without `--tls` the
  contents cross the wire in the clear. Verified rather than assumed, with a
  relay between two peers: in the default mode the synced text is recoverable
  straight off the socket, and with `--tls` the stream is TLS records and
  nothing is recoverable. Note that a naive `grep` of the traffic finds nothing
  either way — the CRDT ships one element per character, so the text is never
  contiguous on the wire. That is not encryption, and it should not be mistaken
  for it.
- **No NAT traversal.** mDNS covers a LAN; anything else needs manual
  `--peer host:port` and reachable ports.
- **Untested territory:** two physically separate machines, Linux/inotify (the
  code is portable via `notify` but only FSEvents has been exercised),
  multi-hour uptime, thousands of files, symlinks, permissions, and non-UTF-8
  filenames.

## Layout

```
src/crdt.rs      RGA: CharId, Op, Doc, arena + linked list, causal buffering
src/diff.rs      Myers diff + change detector (file content → CRDT ops)
src/elemcodec.rs run-encoded element blocks, shared by the wire and state file
src/wire.rs      framing, MessagePack messages, run compression
src/net.rs       TCP and TLS transport, certificate fingerprints
src/auth.rs      pre-shared-key proofs with TLS channel binding
src/ignore.rs    .p2psyncignore parsing and glob matching
src/watcher.rs   FSEvents watching, debouncing
src/binary.rs    SHA-256, rolling checksum, rsync-style delta
src/engine.rs    the state machine tying it together
src/discovery.rs mDNS advertise + browse
src/bin/bench.rs benchmark harness
examples/        scale_probe: CRDT cost vs. document size
```
