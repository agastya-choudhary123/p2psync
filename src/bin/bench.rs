//! Benchmark harness. Run with `cargo run --release --bin bench`.
//!
//! Measures the four things that actually characterize this system: sync
//! latency, bytes on the wire per byte changed, how latency scales with mesh
//! size, and how long a peer takes to catch up after being offline.

use p2psync::binary;
use p2psync::crdt::Doc;
use p2psync::diff;
use p2psync::engine::{Config, Engine};
use p2psync::wire::{self, Msg};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn spawn_peer(root: PathBuf, id: u64, port: u16, peers: Vec<String>) {
    let cfg = Config {
        root,
        peer_id: id,
        name: format!("bench{id}"),
        listen: format!("127.0.0.1:{port}"),
        peers,
        tls: false,
        discovery: false,
        verbose: false,
        secret: None,
    };
    tokio::spawn(async move {
        let _ = Engine::run(cfg).await;
    });
}

/// Spin until `path` contains `needle`. Tight poll: we are measuring
/// milliseconds, so a coarse sleep would dominate the result.
async fn wait_for(path: &Path, needle: &str, timeout: Duration) -> Option<Duration> {
    let start = Instant::now();
    loop {
        if let Ok(s) = std::fs::read_to_string(path) {
            if s.contains(needle) {
                return Some(start.elapsed());
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let n = samples.len();
    let mean = samples.iter().sum::<Duration>() / n.max(1) as u32;
    println!(
        "  {label:<28} n={n:<4} p50={:>7.2}ms  p90={:>7.2}ms  p99={:>7.2}ms  max={:>7.2}ms  mean={:>7.2}ms",
        pct(&samples, 0.50).as_secs_f64() * 1e3,
        pct(&samples, 0.90).as_secs_f64() * 1e3,
        pct(&samples, 0.99).as_secs_f64() * 1e3,
        samples[n - 1].as_secs_f64() * 1e3,
        mean.as_secs_f64() * 1e3,
    );
}

/// 1. Keystroke on peer A to updated file on peer B.
async fn bench_latency(iters: usize) {
    println!("\n[1] sync latency, 2 peers on localhost");
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 1, 19001, vec![]);
    spawn_peer(b.path().to_path_buf(), 2, 19002, vec!["127.0.0.1:19001".into()]);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let fa = a.path().join("bench.txt");
    let fb = b.path().join("bench.txt");
    std::fs::write(&fa, "start\n").unwrap();
    wait_for(&fb, "start", Duration::from_secs(10)).await.expect("initial sync");

    let mut samples = Vec::new();
    let mut content = String::from("start\n");
    for i in 0..iters {
        // One "keystroke": append a unique marker and time its arrival.
        let marker = format!("k{i}~");
        content.push_str(&marker);
        let t0 = Instant::now();
        std::fs::write(&fa, &content).unwrap();
        match wait_for(&fb, &marker, Duration::from_secs(5)).await {
            Some(_) => samples.push(t0.elapsed()),
            None => eprintln!("  (iteration {i} timed out)"),
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    report("end-to-end (write→visible)", samples);
}

/// 2. Bytes put on the wire per byte of user edit.
fn bench_bandwidth() {
    println!("\n[2] bandwidth efficiency (wire bytes per changed byte)");
    let body: String = (0..200)
        .map(|i| format!("line {i}: the quick brown fox jumps over the lazy dog\n"))
        .collect();

    // A single typed character.
    let (mut doc, _) = Doc::from_text(1, &body);
    let mut edited = body.clone();
    edited.insert(body.len() / 2, 'X');
    let ops = diff::detect(&mut doc, &edited);
    let frame = wire::encode(&Msg::Ops {
        path: "notes.txt".into(),
        ops: wire::compress(&ops),
        base: 1,
    })
    .unwrap();
    println!(
        "  1-char insert                ops={:<4} wire={:>6} bytes   ({:.1}x the 1 changed byte, file is {} bytes)",
        ops.len(),
        frame.len(),
        frame.len() as f64,
        body.len()
    );

    // A word replacement.
    let (mut doc, _) = Doc::from_text(1, &body);
    let edited = body.replacen("quick brown fox", "slow purple ox", 1);
    let ops = diff::detect(&mut doc, &edited);
    let frame = wire::encode(&Msg::Ops {
        path: "notes.txt".into(),
        ops: wire::compress(&ops),
        base: 1,
    })
    .unwrap();
    println!(
        "  word replace (15→14 chars)   ops={:<4} wire={:>6} bytes   ({:.1} bytes per op)",
        ops.len(),
        frame.len(),
        frame.len() as f64 / ops.len() as f64
    );

    // A whole new paragraph.
    let (mut doc, _) = Doc::from_text(1, &body);
    let addition = "a freshly typed sentence of about sixty-four characters here.\n";
    let edited = format!("{body}{addition}");
    let ops = diff::detect(&mut doc, &edited);
    let frame = wire::encode(&Msg::Ops {
        path: "notes.txt".into(),
        ops: wire::compress(&ops),
        base: 1,
    })
    .unwrap();
    println!(
        "  append {} chars             ops={:<4} wire={:>6} bytes   ({:.1}x changed bytes)",
        addition.len(),
        ops.len(),
        frame.len(),
        frame.len() as f64 / addition.len() as f64
    );

    // Binary: one byte flipped in a 4 MB file.
    let mut blob: Vec<u8> = (0..4_000_000u32).map(|i| (i.wrapping_mul(2654435761) >> 24) as u8).collect();
    let sigs = binary::signatures(&blob);
    blob[2_000_000] ^= 0xff;
    let t0 = Instant::now();
    let ops = binary::delta(&blob, &sigs);
    let elapsed = t0.elapsed();
    let chunks = wire::chunk_ops(ops);
    let total = chunks.len() as u32;
    let frame_bytes: usize = chunks
        .into_iter()
        .enumerate()
        .map(|(seq, ops)| {
            wire::encode(&Msg::BinaryDeltaChunk {
                path: "image.bin".into(),
                hash: binary::sha256_hex(&blob),
                version: 1,
                seq: seq as u32,
                total,
                ops,
            })
            .unwrap()
            .len()
        })
        .sum();
    println!(
        "  binary 1-byte flip in 4 MB   wire={:>6} bytes   ({:.4}% of the file; delta computed in {:.1}ms)",
        frame_bytes,
        100.0 * frame_bytes as f64 / blob.len() as f64,
        elapsed.as_secs_f64() * 1e3
    );
}

/// 3. Latency as the mesh grows.
async fn bench_scale(sizes: &[usize], iters: usize) {
    println!("\n[3] scalability: latency to the slowest peer in an N-peer mesh");
    let mut base_port = 19100u16;
    for &n in sizes {
        let dirs: Vec<_> = (0..n).map(|_| tempfile::tempdir().unwrap()).collect();
        let ports: Vec<u16> = (0..n).map(|i| base_port + i as u16).collect();
        base_port += 100;
        // Full mesh: peer i dials every earlier peer.
        for i in 0..n {
            let peers: Vec<String> = (0..i).map(|j| format!("127.0.0.1:{}", ports[j])).collect();
            spawn_peer(dirs[i].path().to_path_buf(), 500 + i as u64, ports[i], peers);
        }
        tokio::time::sleep(Duration::from_millis(200 + 120 * n as u64)).await;

        let fa = dirs[0].path().join("mesh.txt");
        std::fs::write(&fa, "start\n").unwrap();
        let mut ready = true;
        for d in &dirs {
            if wait_for(&d.path().join("mesh.txt"), "start", Duration::from_secs(15))
                .await
                .is_none()
            {
                ready = false;
            }
        }
        if !ready {
            println!("  {n:>2} peers: mesh failed to form, skipping");
            continue;
        }

        let mut samples = Vec::new();
        let mut content = String::from("start\n");
        for i in 0..iters {
            let marker = format!("m{i}~");
            content.push_str(&marker);
            let t0 = Instant::now();
            std::fs::write(&fa, &content).unwrap();
            // The interesting number is when the *last* peer has it.
            let mut worst = Duration::ZERO;
            let mut ok = true;
            for d in dirs.iter().skip(1) {
                match wait_for(&d.path().join("mesh.txt"), &marker, Duration::from_secs(10)).await {
                    Some(_) => worst = worst.max(t0.elapsed()),
                    None => ok = false,
                }
            }
            if ok {
                samples.push(worst);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        report(&format!("{n:>2} peers → slowest"), samples);
    }
}

/// 4. Offline peer reconnects and catches up.
async fn bench_recovery(missed_edits: usize) {
    println!("\n[4] recovery: peer offline during {missed_edits} edits, then reconnects");
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 901, 19501, vec![]);
    spawn_peer(b.path().to_path_buf(), 902, 19502, vec!["127.0.0.1:19501".into()]);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let fa = a.path().join("log.txt");
    let fb = b.path().join("log.txt");
    std::fs::write(&fa, "start\n").unwrap();
    wait_for(&fb, "start", Duration::from_secs(10)).await.expect("initial sync");

    // Peer C is the one that misses everything: it isn't running yet.
    let c = tempfile::tempdir().unwrap();
    let mut content = String::from("start\n");
    for i in 0..missed_edits {
        content.push_str(&format!("edit {i} while a peer was away\n"));
        std::fs::write(&fa, &content).unwrap();
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    wait_for(&fb, &format!("edit {} ", missed_edits - 1), Duration::from_secs(30))
        .await
        .expect("peer B keeps up while online");

    let t0 = Instant::now();
    spawn_peer(c.path().to_path_buf(), 903, 19503, vec!["127.0.0.1:19501".into()]);
    let marker = format!("edit {} ", missed_edits - 1);
    match wait_for(&c.path().join("log.txt"), &marker, Duration::from_secs(60)).await {
        Some(_) => println!(
            "  cold peer caught up {} lines ({} bytes) in {:.1}ms",
            missed_edits,
            content.len(),
            t0.elapsed().as_secs_f64() * 1e3
        ),
        None => println!("  cold peer failed to catch up"),
    }

    // Now a live-reconnect: the edits keep flowing after catch-up.
    let t0 = Instant::now();
    content.push_str("post-recovery edit\n");
    std::fs::write(&fa, &content).unwrap();
    match wait_for(&c.path().join("log.txt"), "post-recovery", Duration::from_secs(10)).await {
        Some(d) => println!(
            "  streaming resumed: next edit landed {:.1}ms later (total {:.1}ms)",
            d.as_secs_f64() * 1e3,
            t0.elapsed().as_secs_f64() * 1e3
        ),
        None => println!("  streaming did not resume"),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let debounce = std::env::var("P2PSYNC_DEBOUNCE_MS").unwrap_or_else(|_| "40".into());
    println!("p2psync benchmarks (watcher debounce = {debounce}ms)");
    println!("note: debounce is a hard floor on end-to-end latency; set");
    println!("      P2PSYNC_DEBOUNCE_MS=1 to isolate the transport + CRDT path.");

    bench_latency(100).await;
    bench_bandwidth();
    bench_scale(&[3, 5, 10], 20).await;
    bench_recovery(200).await;
    println!();
}
