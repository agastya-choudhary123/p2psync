#!/bin/bash
set -e

cd "$(dirname "$0")/.."

echo "=== Building Docker image ==="
docker-compose build

echo ""
echo "=== Starting p2psync peers in Docker ==="
docker-compose up -d

echo ""
echo "Waiting for peers to start..."
sleep 3

# Function to copy files into a container
copy_to() {
    local container=$1
    local file=$2
    local dest=$3
    docker cp "$file" "$container:$dest"
    echo "Copied $(basename $file) to $container"
}

echo ""
echo "=== Downloading test media ==="
TESTDIR=$(mktemp -d)
trap "rm -rf $TESTDIR" EXIT

# Download small test image (CC0)
echo "Downloading test image (unsplash.com via pexels)..."
curl -sL "https://images.pexels.com/photos/8797307/pexels-photo-8797307.jpeg" -o "$TESTDIR/test-image.jpg" 2>/dev/null || {
    echo "Note: Could not download image (network issue). Using generated test file instead..."
    dd if=/dev/urandom of="$TESTDIR/test-image.bin" bs=1M count=5 2>/dev/null
}

# Generate a test video-like binary (since downloading video is slow)
echo "Generating test binary file (simulating video)..."
dd if=/dev/urandom of="$TESTDIR/test-video.bin" bs=1M count=10 2>/dev/null

# Create test text file
echo "Creating test text file..."
cat > "$TESTDIR/test-document.txt" << 'EOF'
# Collaborative Document

This is a test document for p2psync.
It will be synced across peers in Docker containers.

The p2psync system handles:
- Character-level CRDT merging for text (no conflicts)
- Binary rsync-style deltas for images/videos
- Peer-to-peer sync without central server
- Automatic convergence even with concurrent edits

Test metadata:
- Created with docker test script
- Multiple peers in isolated network
- Open-source media where available
EOF

echo ""
echo "=== Adding test files to alice ==="
copy_to p2psync-alice "$TESTDIR/test-document.txt" "/sync/"
copy_to p2psync-alice "$TESTDIR/test-image.jpg" "/sync/" 2>/dev/null || copy_to p2psync-alice "$TESTDIR/test-image.bin" "/sync/"
copy_to p2psync-alice "$TESTDIR/test-video.bin" "/sync/"

echo ""
echo "=== Waiting for sync to propagate ==="
sleep 5

echo ""
echo "=== Verifying sync across peers ==="
echo "Files on alice:"
docker exec p2psync-alice ls -lh /sync/ || echo "alice files: (empty)"

echo ""
echo "Files on bob:"
docker exec p2psync-bob ls -lh /sync/ || echo "bob files: (empty)"

echo ""
echo "Files on charlie:"
docker exec p2psync-charlie ls -lh /sync/ || echo "charlie files: (empty)"

echo ""
echo "=== Testing concurrent edits on text file ==="
# Modify on alice
docker exec p2psync-alice sh -c 'echo "" >> /sync/test-document.txt && echo "Edit from alice at $(date)" >> /sync/test-document.txt'

# Modify on bob after a delay
sleep 1
docker exec p2psync-bob sh -c 'echo "" >> /sync/test-document.txt && echo "Edit from bob at $(date)" >> /sync/test-document.txt'

sleep 3

echo ""
echo "Final document on alice:"
docker exec p2psync-alice cat /sync/test-document.txt

echo ""
echo "Final document on bob:"
docker exec p2psync-bob cat /sync/test-document.txt

echo ""
echo "Final document on charlie:"
docker exec p2psync-charlie cat /sync/test-document.txt

echo ""
echo "=== Checking hash consistency ==="
echo "alice test-image hash:"
docker exec p2psync-alice sh -c 'sha256sum /sync/test-image.* 2>/dev/null || echo "no image"'

echo "bob test-image hash:"
docker exec p2psync-bob sh -c 'sha256sum /sync/test-image.* 2>/dev/null || echo "no image"'

echo "charlie test-image hash:"
docker exec p2psync-charlie sh -c 'sha256sum /sync/test-image.* 2>/dev/null || echo "no image"'

echo ""
echo "=== Docker setup complete ==="
echo ""
echo "To view logs:"
echo "  docker-compose logs -f alice"
echo "  docker-compose logs -f bob"
echo "  docker-compose logs -f charlie"
echo ""
echo "To stop:"
echo "  docker-compose down"
echo ""
echo "To inspect container filesystems:"
echo "  docker exec p2psync-alice ls -la /sync/"
