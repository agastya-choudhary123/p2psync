#!/usr/bin/env bash
# Two peers, two directories, live streaming sync.
#
#   ./scripts/demo.sh
#
# Starts peer A and peer B on localhost, then walks through the interesting
# cases: create, incremental edit, reverse direction, simultaneous edit,
# rename, binary delta, delete. Watch the two log files to see the ops flow.
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=target/release/p2psync
[ -x "$BIN" ] || cargo build --release

DIR=${TMPDIR:-/tmp}/p2psync-demo
rm -rf "$DIR"
mkdir -p "$DIR/alice" "$DIR/bob"

cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

echo "=== starting peers ==="
$BIN "$DIR/alice" -l 127.0.0.1:7901 --name alice >"$DIR/alice.log" 2>&1 &
PIDS=($!)
sleep 0.4
$BIN "$DIR/bob" -l 127.0.0.1:7902 --peer 127.0.0.1:7901 --name bob >"$DIR/bob.log" 2>&1 &
PIDS+=($!)
sleep 1.2
echo "alice: $DIR/alice   (log: $DIR/alice.log)"
echo "bob:   $DIR/bob     (log: $DIR/bob.log)"

show() { printf '\n--- %s ---\n' "$1"; }
settle() { sleep 1.2; }

show "1. alice creates poem.txt"
printf 'roses are red\n' > "$DIR/alice/poem.txt"
settle
echo "bob sees: $(cat "$DIR/bob/poem.txt")"

show "2. alice appends a line (streamed as CRDT ops, not a resend)"
printf 'roses are red\nviolets are blue\n' > "$DIR/alice/poem.txt"
settle
cat "$DIR/bob/poem.txt"
grep '+.*-' "$DIR/bob.log" | tail -1

show "3. bob edits in a nested directory; it flows back to alice"
mkdir -p "$DIR/bob/notes"
printf 'bob was here\n' > "$DIR/bob/notes/memo.txt"
settle
echo "alice sees: $(cat "$DIR/alice/notes/memo.txt")"

show "4. simultaneous edits to the same file, different regions"
printf 'roses are red (alice edit)\nviolets are blue\n' > "$DIR/alice/poem.txt" &
w1=$!
printf 'roses are red\nviolets are blue (bob edit)\n' > "$DIR/bob/poem.txt" &
w2=$!
# Only these two writes — a bare `wait` would also block on the peer daemons.
wait $w1 $w2
sleep 2.5
echo "alice:"; cat "$DIR/alice/poem.txt"
echo "bob:"; cat "$DIR/bob/poem.txt"
if diff -q "$DIR/alice/poem.txt" "$DIR/bob/poem.txt" >/dev/null; then
  echo "=> converged to identical content, both edits preserved"
else
  echo "=> DIVERGED (bug)"
fi

show "5. rename is detected, not resent as delete+create"
mv "$DIR/alice/poem.txt" "$DIR/alice/verse.txt"
settle
ls "$DIR/bob"
grep rename "$DIR/bob.log" | tail -1

show "6. binary file: full transfer, then a delta for a 1-byte change"
head -c 200000 /dev/urandom > "$DIR/alice/image.bin"
settle; settle
cmp -s "$DIR/alice/image.bin" "$DIR/bob/image.bin" && echo "initial transfer: identical"
printf 'X' | dd of="$DIR/alice/image.bin" bs=1 seek=100000 conv=notrunc 2>/dev/null
settle; settle
cmp -s "$DIR/alice/image.bin" "$DIR/bob/image.bin" && echo "after 1-byte edit: identical"
grep 'delta to' "$DIR/alice.log" | tail -1

show "7. delete propagates"
rm "$DIR/alice/verse.txt"
settle
[ -f "$DIR/bob/verse.txt" ] && echo "still there (bug)" || echo "gone from bob too"

printf '\n=== done. logs: %s ===\n' "$DIR"
