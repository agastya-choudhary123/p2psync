# Quick Start: Docker Testing

Test p2psync with multiple peers in Docker containers.

## 1. Start Peers

```bash
docker-compose up -d
```

This starts three peers (alice, bob, charlie) in a chain:
```
alice:7901 → bob:7901 → charlie:7901
```

## 2. Generate Test Media

```bash
./scripts/generate-test-media-pure-sh.sh /tmp/test-media
```

Creates:
- **test-video.bin** (10 MB) - Incompressible binary
- **test-image.bin** (5 MB) - Structured binary  
- **test-text.txt** (20 MB) - Highly compressible
- **test-archive.bin** (2 MB) - Mixed
- **test-document.md** - Markdown doc

## 3. Copy Files to alice

```bash
docker cp /tmp/test-media/test-video.bin p2psync-alice:/sync/
docker cp /tmp/test-media/test-image.bin p2psync-alice:/sync/
docker cp /tmp/test-media/test-text.txt p2psync-alice:/sync/
docker cp /tmp/test-media/test-document.md p2psync-alice:/sync/
docker cp /tmp/test-media/test-archive.bin p2psync-alice:/sync/
```

## 4. Verify Sync

```bash
sleep 5  # wait for sync

# Check alice
docker exec p2psync-alice ls -lh /sync/

# Check bob
docker exec p2psync-bob ls -lh /sync/

# Check charlie (should have everything via relay through bob)
docker exec p2psync-charlie ls -lh /sync/
```

## 5. Verify Hash Consistency

```bash
# Get hashes from each peer
echo "=== alice hashes ==="
docker exec p2psync-alice sh -c 'sha256sum /sync/*'

echo ""
echo "=== bob hashes ==="
docker exec p2psync-bob sh -c 'sha256sum /sync/*'

echo ""
echo "=== charlie hashes ==="
docker exec p2psync-charlie sh -c 'sha256sum /sync/*'
```

All hashes should be identical.

## 6. Test Concurrent Edits

```bash
# Create file
docker exec p2psync-alice sh -c 'echo "start" > /sync/concurrent.txt'
sleep 1

# Edit on multiple peers simultaneously
docker exec p2psync-bob sh -c 'echo "bob edit" >> /sync/concurrent.txt' &
docker exec p2psync-charlie sh -c 'echo "charlie edit" >> /sync/concurrent.txt' &
wait

sleep 2

# Check convergence (all should be identical)
echo "alice:"
docker exec p2psync-alice cat /sync/concurrent.txt

echo ""
echo "bob:"
docker exec p2psync-bob cat /sync/concurrent.txt

echo ""
echo "charlie:"
docker exec p2psync-charlie cat /sync/concurrent.txt
```

## 7. View Logs

```bash
docker-compose logs -f alice
docker-compose logs -f bob
docker-compose logs -f charlie
```

Look for:
- `[sync]` - File sync events
- `[recv]` - File received
- `delta to` - Binary delta info
- `[peer]` - Peer connection events

## 8. Stop

```bash
docker-compose down
docker-compose down -v  # also remove volumes
```

## Performance Expectations

| Operation | Expected Time |
|-----------|---------------|
| File appears on bob (alice→bob) | ~60ms |
| File appears on charlie (bob→charlie) | ~60ms |
| Total chain (alice→charlie) | ~120ms |
| Hash verification | <1ms |

For a 10 MB file:
- First sync: ~500ms (all literal bytes)
- Edit 1 byte: rsync delta ~100ms
- Hash verification: <10ms

## Media File Properties

Each file tests different aspects:

| File | Size | Compression | Tests |
|------|------|------------|-------|
| test-video.bin | 10 MB | 0% | Large incompressible, streaming |
| test-image.bin | 5 MB | ~50% | Structured binary, partial reuse |
| test-text.txt | 20 MB | ~90% | Highly compressible, large text |
| test-archive.bin | 2 MB | ~30% | Mixed binary/text |
| test-document.md | ~2 KB | ~80% | CRDT text merging |

## Troubleshooting

**Files not syncing:**
```bash
docker logs p2psync-alice | tail -20
docker logs p2psync-bob | tail -20
```

**Hash mismatch:**
```bash
# Check if file exists on all peers
docker exec p2psync-alice ls -la /sync/test-video.bin
docker exec p2psync-bob ls -la /sync/test-video.bin

# Compare file sizes
docker exec p2psync-alice wc -c /sync/test-video.bin
docker exec p2psync-bob wc -c /sync/test-video.bin
```

**Peers not connected:**
```bash
docker exec p2psync-bob ping alice
docker exec p2psync-charlie ping bob
```

## Next Steps

- Modify docker-compose.yml for different topologies (full mesh, etc.)
- Use `tc` (traffic control) in containers to simulate latency
- Test with even larger files (100 MB+)
- Test with simulated packet loss
