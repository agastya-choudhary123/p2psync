#!/bin/bash
# Pure shell version of media generation (no Python required)

set -e

OUTDIR="${1:-.}"

echo "=== Generating test media files in $OUTDIR ==="
mkdir -p "$OUTDIR"

# 1. Incompressible binary (10 MB random)
echo "Generating 10 MB incompressible binary..."
dd if=/dev/urandom of="$OUTDIR/test-video.bin" bs=1M count=10 2>/dev/null
ls -lh "$OUTDIR/test-video.bin"

# 2. Partially compressible binary (simulates image with structure)
echo "Generating 5 MB partially-compressible binary..."
{
    # JPEG-like header
    printf '\xff\xd8\xff\xe0'
    # Repeating pattern blocks with occasional random data
    for i in $(seq 1 80000); do
        if [ $((i % 1000)) -eq 0 ]; then
            dd if=/dev/urandom bs=64 count=1 2>/dev/null
        else
            # Repeating pattern
            yes "A" | head -64 | tr -d '\n'
        fi
    done
} > "$OUTDIR/test-image.bin"
ls -lh "$OUTDIR/test-image.bin"

# 3. Highly compressible text (20 MB)
echo "Generating 20 MB highly-compressible text..."
{
    # Repeating lines
    for i in $(seq 1 500000); do
        echo "This is a test line number $i with repetitive content for compression testing"
    done
} > "$OUTDIR/test-text.txt"
ls -lh "$OUTDIR/test-text.txt"

# 4. Mixed binary (2 MB archive-like)
echo "Generating 2 MB mixed binary..."
{
    # ASCII text section
    seq 1 50000 | sed 's/^/Test line: /'
    # Binary section
    dd if=/dev/urandom bs=1K count=512 2>/dev/null
    # More text
    yes "END MARKER" | head -10000
} > "$OUTDIR/test-archive.bin"
ls -lh "$OUTDIR/test-archive.bin"

# 5. Markdown document
echo "Generating test document..."
cat > "$OUTDIR/test-document.md" << 'EOF'
# p2psync Test Document

Generated for testing p2psync across multiple peers.

## Features Tested

- **Character-level CRDT**: Text edits merge automatically
- **Binary Deltas**: Images and videos sync with rsync-style deltas
- **Peer-to-Peer**: No central server required
- **Streaming**: Large files streamed in chunks to bound memory

## Test Files

1. test-video.bin (10 MB) - Incompressible binary
2. test-image.bin (5 MB) - Structured binary
3. test-text.txt (20 MB) - Highly compressible text
4. test-archive.bin (2 MB) - Mixed binary
5. test-document.md (this file)

## Expected Results

- All peers should converge to identical state
- Binary files should have identical SHA-256 hashes
- Text edits from concurrent peers should merge without conflicts
- Large files should be synced efficiently (rsync deltas)

---
Test suite for p2psync Docker containers
EOF
ls -lh "$OUTDIR/test-document.md"

echo ""
echo "=== Test Media Summary ==="
du -sh "$OUTDIR"/* 2>/dev/null | sort -h
echo ""
echo "Total: $(du -sh "$OUTDIR" | cut -f1)"
