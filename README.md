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

On a LAN, let the peers find each other and encrypt the link:

```bash
./target/release/p2psync ~/sync -l 0.0.0.0:7901 --discover --tls
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

Two details that matter in practice:

- **Causal buffering.** An insert whose origin hasn't arrived yet is parked in a
  pending map keyed by that missing origin, and integrated the moment it lands.
  Same for a delete that beats its insert. Without this, a 3-peer mesh drops
  operations whenever one link is slower than another.
- **Relay with dedup.** A peer forwards operations it hasn't seen before to its
  other peers, so partial meshes converge. `Doc::apply` returns whether the op
  was new, and only new ops are relayed — that's what stops a forwarding loop.

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
near the front of a file resyncs instead of resending everything. Conflicts are
last-writer-wins on mtime, tiebroken by peer id.

### Reconnection

Each configured peer has a dialer that retries every second. On connect, a peer
sends a snapshot of every text document and metadata for every binary file —
which subsumes whatever operations were missed while the link was down, so
there's no separate replay log. Document state is persisted to
`.p2psync/state.msgpack`, so a restarted peer keeps its character ids instead of
re-deriving a fresh, incompatible id space.

## Measured performance

Apple M4, macOS 15.3.1, 10 cores, release build, peers on localhost.
Reproduce with `cargo run --release --bin bench`.

**Sync latency** — file written on peer A to updated content readable on peer B:

| debounce | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 40ms (default) | 58.6ms | 62.9ms | 64.3ms | 64.4ms |
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

**Scalability** — latency to the slowest peer in a full mesh:

| peers | p50 (40ms debounce) | p50 (1ms) |
|---|---|---|
| 3 | 58.2ms | 14.6ms |
| 5 | 59.4ms | 14.9ms |
| 10 | 63.4ms | 16.6ms |

Fan-out is per-peer serialization, so 10 peers costs ~2ms more than 3.

**Recovery** — a cold peer joining after missing 200 edits (6,096 bytes) caught
up in **2.4ms** and streamed normally afterwards.

**Convergence** — verified by fuzzing rather than asserted: 2–5 replicas, random
concurrent inserts and deletes, shuffled delivery, duplicated messages, and
causally-out-of-order arrival, all asserting byte-identical final text. Plus
end-to-end tests that run real engines over real sockets and compare files on
disk.

## Tests

```bash
cargo test              # 29 tests
```

- `tests/crdt.rs` — convergence and commutativity fuzzing, causal buffering,
  change-detector round-trips, wire compression round-trips.
- `tests/binary.rs` — rolling checksum against recomputation, block reuse,
  delta reconstruction fuzzing.
- `tests/sync_e2e.rs` — two and three real engines on real sockets: both
  directions, incremental edits, echo suppression, simultaneous edits
  converging on disk, binary delta, deletes, nested directories, TLS, and
  reconnect-after-offline.

## Limitations

These are real, and none of them are hidden by the tests:

- **TLS is encryption without authentication.** Certificates are self-signed and
  the verifier accepts any of them, so a passive observer is defeated but an
  active MITM is not. Real use needs pinned fingerprints or a shared CA. There
  is no pairing or authorization step: anything that can reach the port and
  speak the protocol becomes a peer.
- **RGA is stored as a flat `Vec` with linear scans**, so integrating one op is
  O(document length). Fine for source files and notes; a megabyte-scale text
  file will crawl. A block-wise RGA with an index is the fix.
- **Tombstones are never collected.** A long-lived document grows monotonically
  with deleted characters.
- **Catch-up sends whole snapshots**, not a delta of CRDT state, so reconnecting
  with many large files is heavier than it needs to be.
- **First contact between two independently-created copies of the same path is
  not a CRDT merge.** Their id spaces are disjoint, so merging would duplicate
  the text. Instead the lower peer id wins the lineage, the other rebases onto
  it, and the surviving *content* is last-writer-wins on mtime with the loser
  kept as `<path>.conflict-<peer>`. Every edit after that point is a true merge.
  Reconciling unrelated histories is a policy choice, not something a CRDT
  decides for you.
- **Binary conflicts trust mtime**, so clock skew between machines can pick the
  wrong winner.
- **Ignore rules are hardcoded** (`.p2psync`, `.git`, `.DS_Store`, editor swap
  files). No `.syncignore`.
- **RGA can interleave** two peers typing different words at the identical
  position. The result is deterministic and identical everywhere, which is what
  convergence guarantees, but it may not be what either author wanted.

## Layout

```
src/crdt.rs      RGA: CharId, Op, Doc, causal buffering, snapshot merge
src/diff.rs      Myers diff + change detector (file content → CRDT ops)
src/wire.rs      framing, MessagePack messages, run compression
src/net.rs       TCP and TLS transport
src/watcher.rs   FSEvents watching, debouncing, ignore rules
src/binary.rs    SHA-256, rolling checksum, rsync-style delta
src/engine.rs    the state machine tying it together
src/discovery.rs mDNS advertise + browse
src/bin/bench.rs benchmark harness
```

Roughly 2,300 lines of implementation and 730 of tests.
