# Docker Setup for p2psync Testing

Run multiple p2psync peers in isolated Docker containers with a shared network for testing.

## Quick Start

```bash
# Build images and start three peers (alice, bob, charlie) in a chain topology
docker-compose up -d

# Run comprehensive test with media files
./scripts/test-with-docker.sh

# View logs from a peer
docker-compose logs -f alice

# Stop all containers
docker-compose down
```

## Architecture

```
alice (7901)
    ↓ (dials)
bob (7902)
    ↓ (dials)
charlie (7903)
```

Chain topology tests multi-hop relaying and lazy propagation:
- File created on alice appears at bob ~60ms later
- Same file reaches charlie ~120ms later (via relay through bob)
- Text edits use CRDT, so concurrent edits on any peer merge automatically
- Binary files use rsync-style deltas with last-writer-wins on logical clock

## Testing Scenarios

### 1. Basic Sync
```bash
docker exec p2psync-alice sh -c 'echo "hello" > /sync/test.txt'
sleep 2
docker exec p2psync-charlie cat /sync/test.txt  # should print "hello"
```

### 2. Concurrent Edits (Text)
```bash
# Create file on alice
docker exec p2psync-alice sh -c 'echo "start" > /sync/doc.txt'
sleep 1

# Concurrent edits on bob and charlie
docker exec p2psync-bob sh -c 'echo "bob edit" >> /sync/doc.txt' &
docker exec p2psync-charlie sh -c 'echo "charlie edit" >> /sync/doc.txt' &
wait

sleep 2

# All three should have identical, merged content (no conflicts)
docker exec p2psync-alice cat /sync/doc.txt
docker exec p2psync-bob cat /sync/doc.txt
docker exec p2psync-charlie cat /sync/doc.txt
```

### 3. Binary Delta
```bash
# Create large binary on alice
docker exec p2psync-alice dd if=/dev/urandom of=/sync/large.bin bs=1M count=100

# Verify checksum matches on other peers
docker exec p2psync-alice sha256sum /sync/large.bin
docker exec p2psync-bob sha256sum /sync/large.bin
docker exec p2psync-charlie sha256sum /sync/large.bin
```

### 4. Offline Changes & Reconnect
```bash
# Stop bob (simulating offline peer)
docker-compose stop bob

# Make changes on alice and charlie
docker exec p2psync-alice sh -c 'echo "alice offline change" >> /sync/doc.txt'
docker exec p2psync-charlie sh -c 'echo "charlie offline change" >> /sync/doc.txt'

sleep 2

# Bring bob back online
docker-compose start bob
sleep 3

# Bob should catch up
docker exec p2psync-bob cat /sync/doc.txt
```

## Open-Source Media for Testing

The `test-with-docker.sh` script attempts to download:

1. **Test Image** (if curl available):
   - Uses Pexels/Unsplash CC0 images (no license needed)
   - Falls back to generated random binary if network unavailable

2. **Synthetic Video** (generated):
   - 10 MB random binary file (simulates video codec output)
   - Tests rsync delta efficiency on incompressible data

3. **Text Document**:
   - Markdown document with metadata
   - Tests character-level CRDT merging

### Using Real Media

To test with real files:

```bash
# Download a CC0 image from unsplash.com (no login needed)
wget https://images.unsplash.com/photo-xxx -O test-image.jpg

# Copy into container
docker cp test-image.jpg p2psync-alice:/sync/

# Verify it syncs to other peers
sleep 3
docker exec p2psync-bob ls -lh /sync/test-image.jpg
```

Popular CC0 sources:
- **Unsplash**: https://unsplash.com (free, no credit required)
- **Pexels**: https://www.pexels.com (free photos)
- **Pixabay**: https://pixabay.com (free images, CC0)
- **Wikimedia Commons**: https://commons.wikimedia.org (free, various licenses)

### Generating Test Media

```bash
# 50 MB random binary (simulates compressed video)
dd if=/dev/urandom of=test-video.bin bs=1M count=50

# PNG image (structured binary)
dd if=/dev/zero of=test-image.raw bs=1K count=100
file test-image.raw  # should detect as data

# Large compressible text
yes "The quick brown fox jumps over the lazy dog" | head -1000000 > test-text.txt
```

## Performance Metrics

Expected latency in chain (alice → bob → charlie):

| File Size | Alice→Bob | Bob→Charlie | Remarks |
|-----------|-----------|------------|---------|
| 1 KB text | ~60ms | ~120ms | Character-level CRDT |
| 10 MB binary | ~500ms | ~1s | First sync (all literal) |
| 10 MB delta | ~100ms | ~200ms | Incremental rsync delta |

Memory usage (measured in containers):

| File Size | Peak Memory | Bounded By |
|-----------|-------------|-----------|
| 120 MB | 28 MB | Chunk size (4 MB) |
| 1 GB | 28 MB | Chunk size (4 MB) |
| 10 GB | 28 MB | Chunk size (4 MB) |

## Troubleshooting

**Containers won't start:**
```bash
docker-compose logs  # see what went wrong
docker-compose down  # clean up
docker-compose up -d --build  # rebuild and restart
```

**Files not syncing:**
```bash
# Check if peers are connected
docker logs p2psync-alice | grep -i "peer"

# Verify network connectivity
docker exec p2psync-bob ping alice

# Check file permissions
docker exec p2psync-alice ls -la /sync/
```

**Hash mismatch:**
```bash
# If a binary file doesn't match, check the log
docker logs p2psync-alice | grep -i "hash\|delta"

# The mismatch is usually logged with expected vs. actual hash
```

## Cleanup

```bash
# Stop containers
docker-compose down

# Remove volumes (persistent storage)
docker-compose down -v

# Remove images
docker rmi p2psync

# Clean up completely
docker system prune
```
