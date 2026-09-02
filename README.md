p2psync
-------

p2psync keeps directories in sync between peers with no central server, no
polling and no conflict prompts. Two peers watch local directories and edits
stream between them as character-level CRDT operations that merge
automatically, including when both sides edit the same file at the same
instant.

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

Links are encrypted by default, and 82 tests cover it including convergence
fuzzing and a real TLS-terminating MITM.

### Documentation quick links

* [Quick start](#quick-start)
* [Benchmarks](#benchmarks)
* [Limitations](#limitations)
* [NOTES.md](NOTES.md) — how each piece works, and what was fixed along the way

### Quick start

```
$ cargo build --release

# terminal 1
$ ./target/release/p2psync ~/sync-a -l 127.0.0.1:7901 --name alice

# terminal 2
$ ./target/release/p2psync ~/sync-b -l 127.0.0.1:7902 --peer 127.0.0.1:7901 --name bob
```

Edit a file in `~/sync-a` and it appears in `~/sync-b`.

On a LAN, let peers find each other and require a shared secret so only your
machines can join:

```
$ ./target/release/p2psync ~/sync -l 0.0.0.0:7901 --discover --secret "$MY_KEY"
```

`--discover` advertises over mDNS (`_p2psync._tcp.local.`). Bind to a routable
interface, not `127.0.0.1`, since mDNS publishes your LAN address and a
loopback-only peer advertises an address nothing can reach.

`--insecure` talks plaintext instead. Only use it on a link you already trust
some other way, or while debugging.

Exclude paths with a `.p2psyncignore` in the sync root, gitignore-style:

```
*.log
build/
node_modules
**/*.tmp
```

The guided tour, covering create, edit, concurrent edit, rename, binary delta
and delete:

```
$ ./scripts/demo.sh
```

### Benchmarks

Apple M4, macOS 15.3.1, 10 cores, release build, peers on localhost.
Reproduce with `cargo run --release --bin bench`.

Sync latency, from a file written on peer A to updated content readable on B:

| debounce | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 40ms (default) | 56.0ms | 57.7ms | 58.3ms | 58.5ms |
| 1ms (`P2PSYNC_DEBOUNCE_MS=1`) | 14.4ms | 15.5ms | 16.2ms | 16.3ms |

The debounce is a hard floor, so the second row is the honest measure of the
transport plus CRDT path. The remaining ~13ms is FSEvents delivery latency, not
compute.

Bytes on the wire per byte of user edit, in a 10,690-byte file:

| edit | ops | wire bytes |
|---|---|---|
| one character typed | 1 | 40 |
| word replaced (15→14 chars) | 19 | 105 |
| 62-char sentence appended | 62 | 102 (1.6x the changed bytes) |
| 1 byte flipped in a 4 MB binary | | 17,491 (0.44% of the file) |

Bulk typing approaches 1:1 thanks to run compression. An isolated single
character costs ~40 bytes, and that floor is inherent to character-granular
CRDTs: the op carries two 64-bit ids plus the file path.

CRDT cost against document size, before and after the arena rewrite
(`cargo run --release --example scale_probe`):

| document | load (Vec) | load (arena) | 1-char edit | 2000-char paste |
|---|---|---|---|---|
| 3 K chars | 0.6ms | 0.2ms | 0.015ms | 0.3ms |
| 12 K chars | 49.5ms | 1.7ms | 0.109ms | 0.4ms |
| 56 K chars | 1,072ms | 5.0ms | 0.349ms | 0.5ms |
| 221 K chars | 17,884ms | 17.3ms | 1.2ms | 1.2ms |

Loading is linear rather than quadratic now. Single-character edits got slightly
slower, 1.6ms to 2.5ms at 221 K chars, because walking a linked list has worse
cache locality than scanning a `Vec`. That is a fine trade for removing an
18-second stall.

Footprint on a 660 KB text file, measured end to end. Every one of these has the
same root cause: a character carried two 16-byte `CharId`s, its own and its
origin's.

| | before | after |
|---|---|---|
| wire bytes to sync a 108 KB file | 3.44 MB (31.6x) | 218 KB (2.0x) |
| `.p2psync/state.msgpack` | 50.6 MB (77x) | 1.3 MB (2.0x) |
| resident memory | 178 MB | 12.2 MB |
| propagation latency while syncing it | p50 57ms, spikes to 117ms | p50 57ms, max 60ms |

One idea applied three times: store a run, not a character. `src/elemcodec.rs`
encodes element sequences that way for both the wire and the state file, so
sequential text costs a flags byte and the UTF-8 of the character, two bytes
rather than 45. `Doc` stores blocks rather than per-character nodes, which took
memory from 95 MB to 12 MB; the per-character design spent 26 MB on the arena
and 41 MB on the `CharId -> slot` hash map, and seeing that index larger than
the arena it indexed is what pointed at the representation rather than any one
allocation.

The latency spikes were the state file: 50 MB written synchronously every two
seconds, landing on top of whatever edit was in flight. At 1.3 MB they are gone.

TLS costs 0.2% on top of that, 2,260 bytes against 2,254 for a small sync, so
encryption was never the expense. Encrypting 31x more bytes than necessary was.

Latency to the slowest peer in a full mesh, since fan-out is per-peer
serialization:

| peers | p50 (40ms debounce) | p50 (1ms) |
|---|---|---|
| 3 | 56.9ms | 14.6ms |
| 5 | 57.0ms | 14.9ms |
| 10 | 58.1ms | 16.6ms |

A cold peer joining after missing 200 edits (6,096 bytes) caught up in 4.5ms and
streamed normally afterwards.

Convergence is verified by fuzzing rather than asserted: 2 to 5 replicas, random
concurrent inserts and deletes, shuffled delivery, duplicated messages and
causally out-of-order arrival, all asserting byte-identical final text.

### How it works

One CRDT document per file, an RGA over `char`s. Every character carries a
unique `CharId` of `(lamport_clock, peer_id)`; inserts name the character they
follow and deletes are tombstones.

The change detector is the hard part. The user edits a file behind your back, so
you have to reverse-engineer their keystrokes into operations on character ids:
on each filesystem event the new content is diffed against the CRDT's current
text with Myers diff. Writes we make ourselves are recognized by content hash
and dropped rather than diffed back out, which is what keeps the loop from
echoing.

Non-text files are identified by SHA-256 and transferred with a receiver-driven
rsync exchange rather than character merging.

On connect a peer sends a manifest of `(path, text_hash, base)` rather than a
snapshot per file, so the common reconnect case costs a hash per file instead of
a full CRDT document.

[NOTES.md](NOTES.md) covers all of it, plus authentication, tombstone
compaction and shared-content lineage.

### Limitations

RGA can interleave two peers typing different words at the identical position.
The result is deterministic and identical everywhere, which is what convergence
guarantees, but it may not be what either author wanted. This is inherent to
RGA; avoiding it means a different algorithm, Fugue or Peritext, not a patch. In
practice file sync diffs whole files, so edits arrive as contiguous runs
anchored to surviving characters and interleaving is rare.

Genuinely different content at the same path is last-writer-wins on mtime, with
the loser kept as `<path>.conflict-<peer>`. There is no shared history to merge
and no logical clock spanning unrelated documents, so something has to lose.

Compaction defers to absent peers. It requires every peer ever seen to be
connected, so one permanently dead peer means tombstones accumulate forever.
The alternative risks demoting an offline peer's edits to last-writer-wins.

A differing file still ships its whole document. The manifest skips files that
agree, but a one-character difference sends the full CRDT state. Version vectors
and an op log would fix it.

Per-edit work is O(file size) regardless of the CRDT, since every save re-reads
the file, diffs it, and rewrites it on the far side. Fine to a few hundred KB.

Memory is ~10x the text, down from ~270x. A 660 KB file costs 12.2 MB resident
against a 5.5 MB empty-directory baseline. A `String` would cut text storage to
1 byte per ASCII character, saving ~2 MB, but blocks would then need to map
character offsets to byte offsets.

Sustained editing fragments a document until a checkpoint recoalesces it. Every
edit splits a block, and `examples/fragmentation_probe.rs` measures 5,000 edits
taking 240 K characters from 1 block to 16,480. Per-edit cost is flat across
that range at ~0.9 ms so it is not a performance cliff, but the memory win
narrows as a document is edited, and it depends on compaction, which itself
defers while any known peer is offline.

The state file is rewritten whole every 2 seconds while you type. At 2.0x the
document that is 1.3 MB rather than 50 MB per write, but it is still the entire
document each time. An op log with periodic compaction would make it
incremental.

`--secret` with `--insecure` is authorization only. With no certificate there is
nothing to bind to, so an active MITM is still possible, and the process says so
on startup.

Encryption on by default was verified rather than assumed, with a relay between
two peers: in `--insecure` mode the synced text is recoverable straight off the
socket, and by default the stream is TLS records. Note that a naive `grep` of
the traffic finds nothing either way, because the CRDT ships one element per
character so the text is never contiguous on the wire. That is not encryption
and should not be mistaken for it.

No NAT traversal. mDNS covers a LAN; anything else needs manual `--peer
host:port` and reachable ports.

Untested: two physically separate machines, Linux/inotify (the code is portable
via `notify` but only FSEvents has been exercised), multi-hour uptime, thousands
of files, symlinks, permissions, and non-UTF-8 filenames.

### Tests

```
$ cargo test        # 82 tests
```

`tests/sync_e2e.rs` runs two and three real engines over real sockets: both
directions, incremental edits, echo suppression, simultaneous edits converging
on disk, binary delta, an 18 MB file crossing multiple wire chunks, deletes,
nested directories, TLS, reconnect-after-offline, and a three-peer chain where
peer 3 dials only peer 2 so a new file has to be relayed to reach it.

`tests/hardening.rs` covers authentication with the right secret, the wrong
secret, a missing secret, and a real TLS-terminating MITM relay, plus
independently-created files merging without a conflict copy, ignore rules,
manifest catch-up and tombstone compaction.

### Layout

```
src/crdt.rs      RGA: CharId, Op, Doc, arena + linked list, causal buffering
src/diff.rs      Myers diff + change detector (file content -> CRDT ops)
src/elemcodec.rs run-encoded element blocks, shared by the wire and state file
src/wire.rs      framing, MessagePack messages, run compression
src/net.rs       TCP and TLS transport, certificate fingerprints
src/auth.rs      pre-shared-key proofs with TLS channel binding
src/ignore.rs    .p2psyncignore parsing and glob matching
src/watcher.rs   FSEvents watching, debouncing
src/binary.rs    SHA-256, adaptive-block rolling checksum, streamed rsync delta
src/engine.rs    the state machine tying it together
src/discovery.rs mDNS advertise + browse
src/bin/bench.rs benchmark harness
examples/        scale_probe, fragmentation_probe
```
