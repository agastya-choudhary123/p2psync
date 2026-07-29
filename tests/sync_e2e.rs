//! End-to-end: two real engines, two real directories, real sockets.

use p2psync::engine::{Config, Engine};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn spawn_peer(root: PathBuf, id: u64, port: u16, peers: Vec<String>) {
    let cfg = Config {
        root,
        peer_id: id,
        name: format!("peer{id}"),
        listen: format!("127.0.0.1:{port}"),
        peers,
        tls: false,
        discovery: false,
        verbose: false,
    };
    tokio::spawn(async move {
        if let Err(e) = Engine::run(cfg).await {
            eprintln!("engine died: {e:#}");
        }
    });
}

/// Poll until `path` holds `want`, or fail after `timeout`.
async fn expect_content(path: &Path, want: &str, timeout: Duration) {
    let start = Instant::now();
    let mut last = String::from("<missing>");
    while start.elapsed() < timeout {
        if let Ok(s) = std::fs::read_to_string(path) {
            if s == want {
                return;
            }
            last = s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {}\n  want: {want:?}\n  got:  {last:?}",
        path.display()
    );
}

async fn expect_bytes(path: &Path, want: &[u8], timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(b) = std::fs::read(path) {
            if b == want {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {} ({} bytes)", path.display(), want.len());
}

const T: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_file_syncs_both_directions() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 1, 17801, vec![]);
    spawn_peer(b.path().to_path_buf(), 2, 17802, vec!["127.0.0.1:17801".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // A -> B, a brand new file.
    std::fs::write(a.path().join("notes.txt"), "hello from A\n").unwrap();
    expect_content(&b.path().join("notes.txt"), "hello from A\n", T).await;

    // A -> B, an incremental edit streamed as ops.
    std::fs::write(a.path().join("notes.txt"), "hello from A, edited\n").unwrap();
    expect_content(&b.path().join("notes.txt"), "hello from A, edited\n", T).await;

    // B -> A, proving the link is symmetric.
    std::fs::write(b.path().join("reply.txt"), "hi from B\n").unwrap();
    expect_content(&a.path().join("reply.txt"), "hi from B\n", T).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edits_do_not_echo_back_and_forth() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 11, 17811, vec![]);
    spawn_peer(b.path().to_path_buf(), 12, 17812, vec!["127.0.0.1:17811".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    std::fs::write(a.path().join("doc.txt"), "line one\n").unwrap();
    expect_content(&b.path().join("doc.txt"), "line one\n", T).await;

    // A sequence of rapid edits; if the writer's own FSEvents were fed back
    // into the change detector, the content would oscillate or duplicate.
    for i in 2..8 {
        let content: String = (1..=i).map(|n| format!("line {n}\n")).collect();
        std::fs::write(a.path().join("doc.txt"), &content).unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let final_content: String = (1..=7).map(|n| format!("line {n}\n")).collect();
    expect_content(&b.path().join("doc.txt"), &final_content, T).await;
    // Let any echo settle, then confirm both sides are still stable.
    tokio::time::sleep(Duration::from_millis(500)).await;
    expect_content(&a.path().join("doc.txt"), &final_content, T).await;
    expect_content(&b.path().join("doc.txt"), &final_content, T).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_edits_converge_on_disk() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 21, 17821, vec![]);
    spawn_peer(b.path().to_path_buf(), 22, 17822, vec!["127.0.0.1:17821".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Establish a shared base document.
    std::fs::write(a.path().join("shared.txt"), "AAAA\nBBBB\nCCCC\n").unwrap();
    expect_content(&b.path().join("shared.txt"), "AAAA\nBBBB\nCCCC\n", T).await;

    // Both peers edit different regions at the same instant.
    std::fs::write(a.path().join("shared.txt"), "AAAA-from-a\nBBBB\nCCCC\n").unwrap();
    std::fs::write(b.path().join("shared.txt"), "AAAA\nBBBB\nCCCC-from-b\n").unwrap();

    // Whatever the merge produces, both sides must agree on it, and neither
    // edit may be lost.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let ta = std::fs::read_to_string(a.path().join("shared.txt")).unwrap_or_default();
        let tb = std::fs::read_to_string(b.path().join("shared.txt")).unwrap_or_default();
        if ta == tb && ta.contains("from-a") && ta.contains("from-b") {
            break;
        }
        if Instant::now() > deadline {
            panic!("did not converge\n  A: {ta:?}\n  B: {tb:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_file_syncs_and_deltas() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 31, 17831, vec![]);
    spawn_peer(b.path().to_path_buf(), 32, 17832, vec!["127.0.0.1:17831".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Binary content: NUL bytes keep it off the text path.
    let mut blob: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    blob[0] = 0;
    std::fs::write(a.path().join("image.bin"), &blob).unwrap();
    expect_bytes(&b.path().join("image.bin"), &blob, T).await;

    // A small change should sync too (over a delta, not a full resend).
    blob[20_000] ^= 0xff;
    std::fs::write(a.path().join("image.bin"), &blob).unwrap();
    expect_bytes(&b.path().join("image.bin"), &blob, T).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_propagate() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 41, 17841, vec![]);
    spawn_peer(b.path().to_path_buf(), 42, 17842, vec!["127.0.0.1:17841".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    std::fs::write(a.path().join("temp.txt"), "delete me\n").unwrap();
    expect_content(&b.path().join("temp.txt"), "delete me\n", T).await;

    std::fs::remove_file(a.path().join("temp.txt")).unwrap();
    let deadline = Instant::now() + T;
    while b.path().join("temp.txt").exists() {
        if Instant::now() > deadline {
            panic!("delete did not propagate");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_directories_sync() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn_peer(a.path().to_path_buf(), 51, 17851, vec![]);
    spawn_peer(b.path().to_path_buf(), 52, 17852, vec!["127.0.0.1:17851".into()]);
    tokio::time::sleep(Duration::from_millis(400)).await;

    std::fs::create_dir_all(a.path().join("src/deep")).unwrap();
    std::fs::write(a.path().join("src/deep/mod.rs"), "pub fn f() {}\n").unwrap();
    expect_content(&b.path().join("src/deep/mod.rs"), "pub fn f() {}\n", T).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_peer_mesh_converges() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    spawn_peer(dirs[0].path().to_path_buf(), 61, 17861, vec![]);
    spawn_peer(dirs[1].path().to_path_buf(), 62, 17862, vec!["127.0.0.1:17861".into()]);
    spawn_peer(
        dirs[2].path().to_path_buf(),
        63,
        17863,
        vec!["127.0.0.1:17861".into(), "127.0.0.1:17862".into()],
    );
    tokio::time::sleep(Duration::from_millis(700)).await;

    std::fs::write(dirs[0].path().join("mesh.txt"), "start\n").unwrap();
    for d in &dirs {
        expect_content(&d.path().join("mesh.txt"), "start\n", T).await;
    }

    // Each peer appends its own line concurrently.
    for (i, d) in dirs.iter().enumerate() {
        let cur = std::fs::read_to_string(d.path().join("mesh.txt")).unwrap();
        std::fs::write(d.path().join("mesh.txt"), format!("{cur}peer{i}\n")).unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let texts: Vec<String> = dirs
            .iter()
            .map(|d| std::fs::read_to_string(d.path().join("mesh.txt")).unwrap_or_default())
            .collect();
        let all_agree = texts.iter().all(|t| *t == texts[0]);
        let complete = (0..3).all(|i| texts[0].contains(&format!("peer{i}")));
        if all_agree && complete {
            break;
        }
        if Instant::now() > deadline {
            panic!("mesh did not converge: {texts:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_link_syncs() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for (root, id, port, peers) in [
        (a.path().to_path_buf(), 71u64, 17871u16, vec![]),
        (b.path().to_path_buf(), 72, 17872, vec!["127.0.0.1:17871".to_string()]),
    ] {
        let cfg = Config {
            root,
            peer_id: id,
            name: format!("tls{id}"),
            listen: format!("127.0.0.1:{port}"),
            peers,
            tls: true,
            discovery: false,
            verbose: false,
        };
        tokio::spawn(async move {
            let _ = Engine::run(cfg).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(600)).await;

    std::fs::write(a.path().join("secret.txt"), "over TLS\n").unwrap();
    expect_content(&b.path().join("secret.txt"), "over TLS\n", T).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_replays_offline_changes() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    // B dials A. Start A only; B will retry until A appears.
    spawn_peer(b.path().to_path_buf(), 82, 17882, vec!["127.0.0.1:17881".into()]);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Changes made while there is no peer at all.
    std::fs::write(b.path().join("offline.txt"), "written while alone\n").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A comes up; B's dialer reconnects and catch-up should deliver the file.
    spawn_peer(a.path().to_path_buf(), 81, 17881, vec![]);
    expect_content(
        &a.path().join("offline.txt"),
        "written while alone\n",
        Duration::from_secs(15),
    )
    .await;
}
