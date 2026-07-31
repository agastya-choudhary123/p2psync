#!/bin/bash
# Generate test media files for p2psync testing without internet access

set -e

OUTDIR="${1:-.}"

echo "=== Generating test media files in $OUTDIR ==="
mkdir -p "$OUTDIR"

# Generate realistic-looking media that tests different properties

# 1. Incompressible "video" file (10 MB of random data)
echo "Generating 10 MB incompressible binary (simulates compressed video)..."
dd if=/dev/urandom of="$OUTDIR/test-video.bin" bs=1M count=10 2>/dev/null
ls -lh "$OUTDIR/test-video.bin"

# 2. Partially compressible "image" file
# Create a structured file with repeating patterns (like JPEG with structure)
echo "Generating 5 MB partially-compressible binary (simulates image)..."
python3 << 'PYTHON' "$OUTDIR"
import sys
import os

outdir = sys.argv[1]
with open(f"{outdir}/test-image.bin", "wb") as f:
    # JPEG-like header
    f.write(b'\xff\xd8\xff\xe0')  # JPEG SOI + APP0

    # Simulate JPEG structure: repeating blocks with some variation
    block_size = 64
    for i in range(5 * 1024 * 1024 // block_size):
        # Repeating pattern with occasional variation
        if i % 1000 == 0:
            # Add some entropy periodically
            f.write(os.urandom(block_size))
        else:
            # Mostly repeating pattern
            pattern = bytes([(i + j) % 256 for j in range(block_size)])
            f.write(pattern)
PYTHON
ls -lh "$OUTDIR/test-image.bin"

# 3. Highly compressible text file
echo "Generating 20 MB highly-compressible text..."
{
    echo "Lorem ipsum dolor sit amet..."
    seq 1 1000000 | sed 's/^/Line: /'
} > "$OUTDIR/test-text.txt"
ls -lh "$OUTDIR/test-text.txt"

# 4. Mixed binary (simulates archive/container)
echo "Generating 2 MB mixed binary (simulates archive)..."
{
    # Some ASCII text
    python3 -c "print('This is a test file ' * 1000)"
    # Some random binary
    dd if=/dev/urandom bs=1K count=512 2>/dev/null
    # More text
    python3 -c "print('End of file ' * 100)"
} > "$OUTDIR/test-archive.bin"
ls -lh "$OUTDIR/test-archive.bin"

# 5. Markdown document
echo "Generating test document..."
cat > "$OUTDIR/test-document.md" << 'EOF'
# p2psync Test Document

Generated on $(date) for testing p2psync across multiple peers.

## Features

- **Character-level CRDT**: Text edits merge automatically without conflicts
- **Binary Deltas**: Images and videos sync efficiently using rsync-style deltas
- **Peer-to-Peer**: No central server, works on LAN or with explicit peering
- **Encryption**: TLS by default, optional pre-shared key authentication

## Test Plan

1. Create this document on peer alice
2. Edit on bob and charlie concurrently
3. All peers should converge to identical content
4. Binary files should match by hash across all peers

## Test Metadata

- Text file: test-document.md (multiline, UTF-8)
- Image: test-image.bin (5 MB structured binary)
- Video: test-video.bin (10 MB random binary)
- Archive: test-archive.bin (2 MB mixed)
- Large text: test-text.txt (20 MB highly compressible)

Each file tests different compression patterns and sync efficiency.

---

Generated for: p2psync Docker testing
Purpose: Multi-peer convergence validation
Expected behavior: All peers should have identical hashes for binary files
EOF
ls -lh "$OUTDIR/test-document.md"

echo ""
echo "=== Test Media Summary ==="
du -sh "$OUTDIR"/* 2>/dev/null | sort -h
echo ""
echo "Total size: $(du -sh "$OUTDIR" | cut -f1)"
echo ""
echo "Files ready for testing!"
echo "Copy to Docker container:"
echo "  docker cp $OUTDIR p2psync-alice:/sync/"
