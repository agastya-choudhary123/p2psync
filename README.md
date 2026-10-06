# p2psync

Peer-to-peer directory sync in Rust with no central server. Each peer watches
a local directory. Text edits are sent as character-level CRDT operations,
so two people editing the same file at the same time end up with the same
merged result, and nobody gets a conflict prompt. Binary files sync with an
rsync-style delta.

```
Peer A                                   Peer B
watcher (FSEvents, 40ms debounce)        watcher
   ↓                                        ↓
change detector (Myers diff vs CRDT)     change detector
   ↓                                        ↓
CRDT engine (RGA, one doc per file) <──TLS──> CRDT engine
   ↓                     MessagePack frames ↓
file writer (skips its own echoes)       file writer
```

## Quick start

```sh
cargo build --release

# terminal 1
./target/release/p2psync ~/sync-a -l 127.0.0.1:7901 --name alice

# terminal 2
./target/release/p2psync ~/sync-b -l 127.0.0.1:7902 --peer 127.0.0.1:7901 --name bob
```

Edit a file in `~/sync-a`, and the change shows up in `~/sync-b`.

On a LAN you can use mDNS discovery and a shared secret:

```sh
./target/release/p2psync ~/sync -l 0.0.0.0:7901 --discover --secret "$MY_KEY"
```

Bind to a real interface, not `127.0.0.1`, because mDNS advertises the address
you bind to. The secret can also be passed in `P2PSYNC_SECRET`.

| Flag | |
|---|---|
| `-l, --listen <addr>` | Listen address (default `0.0.0.0:7878`) |
| `-p, --peer <addr>` | Peer to connect to. Repeatable. |
| `-n, --name <name>` | Display name |
| `--discover` | Find peers with mDNS (`_p2psync._tcp.local.`) |
| `--secret <key>` | Shared secret that every peer has to prove it knows |
| `--insecure` | Use plaintext instead of TLS. Only for trusted links or debugging. |
| `-v, --verbose` | Also log merges and snapshots |

To exclude files, put a gitignore-style `.p2psyncignore` in the sync root:

```
*.log
build/
node_modules
**/*.tmp
```

`./scripts/demo.sh` walks through create, edit, concurrent edit, rename,
binary delta, and delete.

## How it works

- **CRDT.** Each file is its own RGA document over `char`s. Every character
  has an id `(lamport_clock, peer_id)`. An insert says which character it
  comes after, and deletes leave tombstones.
- **Change detection.** The user edits files outside p2psync's control, so on
  each filesystem event the new file contents are diffed (Myers) against the
  CRDT's current text, and the diff is turned into insert and delete ops.
  When p2psync writes a file itself, it recognizes the content hash on the
  next event and skips it, so changes don't echo back and forth.
- **Binary files** are identified by SHA-256 and synced with an rsync-style
  delta that the receiver drives.
- **Reconnects.** On connect, each peer sends a manifest of
  `(path, text_hash, base)`. Only files that differ get synced.
- **Storage.** Characters are stored and sent as runs instead of one at a
  time (`src/elemcodec.rs`), so sequential text costs about 2 bytes per
  character on the wire and in the state file.

[NOTES.md](NOTES.md) has more detail on all of this, plus authentication,
tombstone compaction, and what got fixed along the way.

## Benchmarks

Measured on an Apple M4 running macOS 15.3.1, release build, with all peers on
localhost. To reproduce, run `cargo run --release --bin bench`.

**Latency** from a write on peer A until the new content can be read on
peer B:

| debounce | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 40ms (default) | 56.0ms | 57.7ms | 58.3ms | 58.5ms |
| 1ms (`P2PSYNC_DEBOUNCE_MS=1`) | 14.4ms | 15.5ms | 16.2ms | 16.3ms |

With a 1ms debounce, most of the remaining ~13ms is the time FSEvents takes
to deliver the event.

**Bytes on the wire** for edits to a 10,690-byte file:

| edit | ops | wire bytes |
|---|---|---|
| one character typed | 1 | 40 |
| word replaced (15→14 chars) | 19 | 105 |
| 62-char sentence appended | 62 | 102 |
| 1 byte flipped in a 4 MB binary | | 17,491 (0.44% of the file) |

A single character costs about 40 bytes, because each op carries two 64-bit
ids and the file path. Longer runs of typing get close to 1 byte per byte.

**CRDT load time vs. document size**, before and after switching from a
`Vec` to an arena with a linked list (`cargo run --release --example
scale_probe`):

| document | load (Vec) | load (arena) | 1-char edit | 2000-char paste |
|---|---|---|---|---|
| 3K chars | 0.6ms | 0.2ms | 0.015ms | 0.3ms |
| 12K chars | 49.5ms | 1.7ms | 0.109ms | 0.4ms |
| 56K chars | 1,072ms | 5.0ms | 0.349ms | 0.5ms |
| 221K chars | 17,884ms | 17.3ms | 1.2ms | 1.2ms |

**Run encoding**, measured on a 660 KB text file. Before, every character
carried two 16-byte ids (its own and the one it was inserted after):

| | before | after |
|---|---|---|
| wire bytes to sync a 108 KB file | 3.44 MB | 218 KB |
| `.p2psync/state.msgpack` | 50.6 MB | 1.3 MB |
| resident memory | 178 MB | 12.2 MB |
| latency during that sync | p50 57ms, spikes to 117ms | p50 57ms, max 60ms |

The latency spikes came from writing the 50 MB state file every 2 seconds.
TLS overhead was small in comparison: 2,260 bytes vs. 2,254 for a small
sync.

**Full mesh**, latency to the slowest peer:

| peers | p50 (40ms debounce) | p50 (1ms) |
|---|---|---|
| 3 | 56.9ms | 14.6ms |
| 5 | 57.0ms | 14.9ms |
| 10 | 58.1ms | 16.6ms |

A peer that rejoined after missing 200 edits (6,096 bytes) caught up in
4.5ms.

## Limitations

- **Interleaving.** If two peers type different words at exactly the same
  position, RGA can interleave their characters. Every peer ends up with the
  same result, but it might not be what either person meant. Fixing that
  would take a different algorithm, like Fugue. In practice it rarely
  happens, because edits arrive as runs anchored to existing text.
- **Unrelated files at the same path** are resolved by last-writer-wins on
  mtime. The losing version is kept as `<path>.conflict-<peer>`.
- **Compaction waits for every peer.** Tombstones are only cleaned up when
  every peer that has ever been seen is connected, so a peer that never comes
  back means tombstones pile up forever.
- **A file that differs at all is sent in full.** The manifest skips files
  that already match, but a one-character difference sends the whole CRDT
  state. Version vectors and an op log would fix this.
- **Each edit costs O(file size)**, because every save re-reads and re-diffs
  the file. That's fine up to a few hundred KB.
- **Memory** is about 10x the text size. A 660 KB file uses 12.2 MB, compared
  with a 5.5 MB baseline for an empty directory.
- **Fragmentation.** Each edit splits a block. In
  `examples/fragmentation_probe.rs`, 5,000 edits turn one block into 16,480.
  Per-edit time stays flat at about 0.9ms, but memory use grows until
  compaction runs.
- **The state file is rewritten in full** every 2 seconds while you're
  editing.
- **`--secret` with `--insecure`** only checks that peers know the secret.
  Without TLS there's nothing to stop a man-in-the-middle, and p2psync warns
  about this at startup.
- **No NAT traversal.** Outside a LAN you have to pass `--peer host:port` and
  make sure the port is reachable.
- **Not tested yet:** two physically separate machines, Linux/inotify (the
  `notify` crate should handle it, but only FSEvents has been tried),
  multi-hour uptime, thousands of files, symlinks, permissions, and non-UTF-8
  filenames.

## Tests

```sh
cargo test
```

- `tests/crdt.rs` fuzzes convergence with 2 to 5 replicas, random concurrent
  edits, and messages delivered shuffled, duplicated, and out of causal
  order. Every replica has to end up with byte-identical text.
- `tests/sync_e2e.rs` runs two or three real engines over real sockets. It
  covers both directions, concurrent edits, binary deltas, an 18 MB file,
  deletes, nested directories, TLS, reconnecting after being offline, and a
  three-peer chain where a file has to be relayed through the middle peer.
- `tests/hardening.rs` checks auth with the right secret, the wrong secret,
  and no secret, and runs a TLS-terminating MITM relay. It also covers ignore
  rules, manifest catch-up, and compaction.
- `tests/binary.rs` covers the rsync delta.

## Layout

```
src/crdt.rs        RGA: ids, ops, documents, causal buffering
src/diff.rs        Myers diff and change detection
src/elemcodec.rs   run encoding for the wire and the state file
src/wire.rs        framing and MessagePack messages
src/net.rs         TCP/TLS transport, certificate fingerprints
src/auth.rs        pre-shared-key proofs bound to the TLS channel
src/ignore.rs      .p2psyncignore
src/watcher.rs     filesystem watching and debouncing
src/binary.rs      SHA-256, rolling checksum, rsync delta
src/discovery.rs   mDNS
src/engine.rs      ties everything together
src/bin/bench.rs   benchmarks
examples/          scale_probe, fragmentation_probe
```
