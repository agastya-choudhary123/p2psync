//! Tests for the fixes to the known limitations: authentication, shared-content
//! lineage, manifest catch-up, ignore rules, and tombstone compaction.

use p2psync::engine::{Config, Engine};
use p2psync::ignore::{glob_match, Ignore};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[allow(clippy::too_many_arguments)]
fn cfg(root: PathBuf, id: u64, port: u16, peers: Vec<String>, tls: bool, secret: Option<&str>) -> Config {
    Config {
        root,
        peer_id: id,
        name: format!("peer{id}"),
        listen: format!("127.0.0.1:{port}"),
        peers,
        tls,
        discovery: false,
        verbose: false,
        secret: secret.map(|s| s.as_bytes().to_vec()),
    }
}

fn spawn(c: Config) {
    tokio::spawn(async move {
        let _ = Engine::run(c).await;
    });
}

async fn wait_content(path: &Path, want: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if std::fs::read_to_string(path).ok().as_deref() == Some(want) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

const T: Duration = Duration::from_secs(8);

/// Wait for the persisted CRDT state to shrink well below its own peak, which
/// only happens when tombstones are dropped — the visible text never shrinks.
async fn wait_state_shrinks(state: &Path, timeout: Duration) -> bool {
    let start = Instant::now();
    let mut peak = 0usize;
    while start.elapsed() < timeout {
        if let Ok(meta) = std::fs::metadata(state) {
            let size = meta.len() as usize;
            peak = peak.max(size);
            // Compaction dropped ~90% of the elements, so the file must fall
            // far below its high-water mark.
            if peak > 1000 && size * 2 < peak {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

// ---- ignore rules --------------------------------------------------------

#[test]
fn glob_matching() {
    assert!(glob_match("*.log", "server.log"));
    assert!(!glob_match("*.log", "server.log.gz"));
    assert!(glob_match("build", "build"));
    assert!(glob_match("a/*.txt", "a/b.txt"));
    // A single star must not cross a separator; ** must.
    assert!(!glob_match("a/*.txt", "a/b/c.txt"));
    assert!(glob_match("a/**/*.txt", "a/b/c.txt"));
    assert!(glob_match("**/target", "x/y/target"));
    assert!(glob_match("?.rs", "a.rs"));
    assert!(!glob_match("?.rs", "ab.rs"));
}

#[test]
fn builtin_ignores_apply() {
    let ig = Ignore::default();
    assert!(ig.is_ignored(".p2psync/state.msgpack"));
    assert!(ig.is_ignored(".git/HEAD"));
    assert!(ig.is_ignored("sub/.DS_Store"));
    assert!(ig.is_ignored("notes.txt.swp"));
    assert!(!ig.is_ignored("notes.txt"));
    assert!(!ig.is_ignored("src/main.rs"));
}

#[test]
fn ignorefile_patterns_are_loaded() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(".p2psyncignore"),
        "# comment\n\n*.log\nbuild/\nnode_modules\n**/*.tmp\n",
    )
    .unwrap();
    let ig = Ignore::load(dir.path());
    assert!(ig.is_ignored("server.log"));
    assert!(ig.is_ignored("build"));
    assert!(ig.is_ignored("build/out/thing.o"), "a directory pattern covers its contents");
    assert!(ig.is_ignored("node_modules/react/index.js"));
    assert!(ig.is_ignored("a/b/scratch.tmp"));
    assert!(!ig.is_ignored("src/main.rs"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_files_do_not_sync() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    std::fs::write(a.path().join(".p2psyncignore"), "*.log\nsecrets/\n").unwrap();
    spawn(cfg(a.path().into(), 101, 18101, vec![], false, None));
    spawn(cfg(b.path().into(), 102, 18102, vec!["127.0.0.1:18101".into()], false, None));
    tokio::time::sleep(Duration::from_millis(500)).await;

    std::fs::create_dir_all(a.path().join("secrets")).unwrap();
    std::fs::write(a.path().join("noisy.log"), "should not travel\n").unwrap();
    std::fs::write(a.path().join("secrets/key.txt"), "very secret\n").unwrap();
    std::fs::write(a.path().join("shared.txt"), "should travel\n").unwrap();

    assert!(wait_content(&b.path().join("shared.txt"), "should travel\n", T).await);
    // Give the ignored ones every chance to leak.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!b.path().join("noisy.log").exists(), "ignored file synced anyway");
    assert!(!b.path().join("secrets/key.txt").exists(), "ignored directory synced anyway");
}

// ---- authentication ------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matching_secret_over_tls_syncs() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn(cfg(a.path().into(), 111, 18111, vec![], true, Some("correct horse")));
    spawn(cfg(
        b.path().into(),
        112,
        18112,
        vec!["127.0.0.1:18111".into()],
        true,
        Some("correct horse"),
    ));
    tokio::time::sleep(Duration::from_millis(700)).await;

    std::fs::write(a.path().join("auth.txt"), "authenticated\n").unwrap();
    assert!(wait_content(&b.path().join("auth.txt"), "authenticated\n", T).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_secret_is_refused() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn(cfg(a.path().into(), 121, 18121, vec![], true, Some("right")));
    spawn(cfg(
        b.path().into(),
        122,
        18122,
        vec!["127.0.0.1:18121".into()],
        true,
        Some("wrong"),
    ));
    tokio::time::sleep(Duration::from_millis(900)).await;

    std::fs::write(a.path().join("auth.txt"), "should not travel\n").unwrap();
    assert!(
        !wait_content(&b.path().join("auth.txt"), "should not travel\n", Duration::from_secs(3)).await,
        "a peer with the wrong secret received data"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_peer_cannot_join_a_secured_peer() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    // A requires a secret; B offers none. The mismatch must fail loudly rather
    // than silently downgrading to an open link.
    spawn(cfg(a.path().into(), 131, 18131, vec![], false, Some("shared")));
    spawn(cfg(b.path().into(), 132, 18132, vec!["127.0.0.1:18131".into()], false, None));
    tokio::time::sleep(Duration::from_millis(900)).await;

    std::fs::write(a.path().join("auth.txt"), "should not travel\n").unwrap();
    assert!(
        !wait_content(&b.path().join("auth.txt"), "should not travel\n", Duration::from_secs(3)).await,
        "an unauthenticated peer received data"
    );
}

// ---- shared-content lineage ---------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independently_created_identical_files_merge_without_conflict() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    // The realistic setup: the same folder copied to two machines *before*
    // either peer ever ran. Two independent CRDT documents for one path.
    let base = "shared line one\nshared line two\n";
    std::fs::write(a.path().join("doc.txt"), base).unwrap();
    std::fs::write(b.path().join("doc.txt"), base).unwrap();

    spawn(cfg(a.path().into(), 141, 18141, vec![], false, None));
    spawn(cfg(b.path().into(), 142, 18142, vec!["127.0.0.1:18141".into()], false, None));
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Neither side should have been clobbered, and no conflict copy made.
    assert_eq!(std::fs::read_to_string(a.path().join("doc.txt")).unwrap(), base);
    assert_eq!(std::fs::read_to_string(b.path().join("doc.txt")).unwrap(), base);

    // Now edit both ends. With a content-derived lineage these are true CRDT
    // merges, so both edits must survive on both peers.
    std::fs::write(a.path().join("doc.txt"), "shared line one (a)\nshared line two\n").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::write(
        b.path().join("doc.txt"),
        "shared line one (a)\nshared line two (b)\n",
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let ta = std::fs::read_to_string(a.path().join("doc.txt")).unwrap_or_default();
        let tb = std::fs::read_to_string(b.path().join("doc.txt")).unwrap_or_default();
        if ta == tb && ta.contains("(a)") && ta.contains("(b)") {
            break;
        }
        if Instant::now() > deadline {
            panic!("independent identical files did not merge cleanly\n  A: {ta:?}\n  B: {tb:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let conflicts: Vec<_> = std::fs::read_dir(a.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains("conflict"))
        .collect();
    assert!(conflicts.is_empty(), "unexpected conflict copies: {conflicts:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independently_created_differing_files_keep_both_versions() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    // Genuinely unrelated content at the same path: there is no correct merge,
    // so the loser must at least be preserved rather than destroyed.
    std::fs::write(a.path().join("doc.txt"), "written on machine A\n").unwrap();
    std::fs::write(b.path().join("doc.txt"), "totally different, machine B\n").unwrap();

    spawn(cfg(a.path().into(), 151, 18151, vec![], false, None));
    spawn(cfg(b.path().into(), 152, 18152, vec!["127.0.0.1:18151".into()], false, None));
    tokio::time::sleep(Duration::from_secs(3)).await;

    let ta = std::fs::read_to_string(a.path().join("doc.txt")).unwrap_or_default();
    let tb = std::fs::read_to_string(b.path().join("doc.txt")).unwrap_or_default();
    assert_eq!(ta, tb, "peers must converge even on unrelated histories");

    // Whichever side lost should have kept a conflict copy of its version.
    let all: Vec<String> = [a.path(), b.path()]
        .iter()
        .flat_map(|d| std::fs::read_dir(d).unwrap())
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    let saved = all.iter().any(|n| n.contains("conflict"));
    assert!(saved, "the losing version was discarded; files present: {all:?}");
}

// ---- manifest catch-up ---------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_with_agreeing_files_transfers_no_snapshots() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn(cfg(a.path().into(), 161, 18161, vec![], false, None));
    spawn(cfg(b.path().into(), 162, 18162, vec!["127.0.0.1:18161".into()], false, None));
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..5 {
        std::fs::write(a.path().join(format!("f{i}.txt")), format!("content {i}\n")).unwrap();
    }
    for i in 0..5 {
        assert!(
            wait_content(&b.path().join(format!("f{i}.txt")), &format!("content {i}\n"), T).await,
            "f{i}.txt did not sync"
        );
    }
    // Both peers now agree on all five files. A fresh peer C pointed at A must
    // still converge — the manifest exchange should ask only for what it lacks.
    let c = tempfile::tempdir().unwrap();
    spawn(cfg(c.path().into(), 163, 18163, vec!["127.0.0.1:18161".into()], false, None));
    for i in 0..5 {
        assert!(
            wait_content(&c.path().join(format!("f{i}.txt")), &format!("content {i}\n"), T).await,
            "new peer did not receive f{i}.txt"
        );
    }
}

// ---- tombstone compaction ----------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstones_are_compacted_once_peers_agree() {
    // Force a low threshold and a fast check so this doesn't take 30s.
    std::env::set_var("P2PSYNC_COMPACT_AFTER", "20");
    std::env::set_var("P2PSYNC_COMPACT_EVERY_MS", "300");
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    spawn(cfg(a.path().into(), 171, 18171, vec![], false, None));
    spawn(cfg(b.path().into(), 172, 18172, vec!["127.0.0.1:18171".into()], false, None));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Churn: fully replace the content each round so the diff is a wholesale
    // delete-and-reinsert. A minimal diff of "generation 3" -> "generation 4"
    // only touches one character and would never accumulate tombstones.
    // Extend churn to 50+ iterations spanning multiple save cycles, with a large
    // document per generation to guarantee the peak well exceeds 1000 bytes and
    // ensures a save captures the pre-compaction state before compaction completes.
    let generation = |i: u8| format!("{}\n", ((b'a' + i) as char).to_string().repeat(2000));
    for i in 0..30u8 {
        std::fs::write(a.path().join("churn.txt"), generation(i)).unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let final_owned = generation(29);
    let final_text: &str = &final_owned;
    assert!(wait_content(&b.path().join("churn.txt"), final_text, T).await);

    // The churn leaves far more stored elements than visible characters. Once
    // both peers agree on the text, compaction should collapse that back down.
    // Observe it through the persisted state, which is written every 2s.
    let state = a.path().join(".p2psync").join("state.msgpack");
    assert!(
        wait_state_shrinks(&state, Duration::from_secs(25)).await,
        "tombstones were never compacted; persisted state never shrank"
    );
    assert_eq!(std::fs::read_to_string(a.path().join("churn.txt")).unwrap(), final_text);
    assert_eq!(std::fs::read_to_string(b.path().join("churn.txt")).unwrap(), final_text);

    // And edits still merge after all that churn.
    let after = format!("{final_text}post\n");
    std::fs::write(a.path().join("churn.txt"), &after).unwrap();
    assert!(
        wait_content(&b.path().join("churn.txt"), &after, T).await,
        "sync broke after tombstone compaction"
    );
}

// ---- channel binding vs. a real man-in-the-middle ------------------------

/// A TLS-terminating relay: it presents its *own* certificate to whoever dials
/// it, opens a separate TLS session to the real peer, and forwards bytes
/// verbatim. This is exactly the attack self-signed certs are open to.
async fn mitm_proxy(listen: String, target: String) {
    use p2psync::net::{self, Tls};
    use tokio::net::TcpListener;
    let tls = Tls::self_signed().unwrap();
    let listener = TcpListener::bind(&listen).await.unwrap();
    loop {
        let Ok((tcp, _)) = listener.accept().await else { continue };
        let tls = tls.clone();
        let target = target.clone();
        tokio::spawn(async move {
            let Ok(victim) = net::accept(tcp, Some(&tls)).await else { return };
            let Ok(upstream) = net::connect(&target, Some(&tls)).await else { return };
            let (mut vr, mut vw) = victim.split();
            let (mut ur, mut uw) = upstream.split();
            tokio::select! {
                _ = tokio::io::copy(&mut vr, &mut uw) => {}
                _ = tokio::io::copy(&mut ur, &mut vw) => {}
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn intercepted_tls_connection_fails_authentication() {
    let secret = "shared pre-shared key";
    let a = tempfile::tempdir().unwrap();
    let victim = tempfile::tempdir().unwrap();
    let control = tempfile::tempdir().unwrap();

    // The real listener.
    spawn(cfg(a.path().into(), 191, 18191, vec![], true, Some(secret)));
    tokio::spawn(mitm_proxy("127.0.0.1:18192".into(), "127.0.0.1:18191".into()));
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The victim dials the proxy, believing it is peer A. Both ends hold the
    // correct secret, so only the certificate binding can detect the relay.
    spawn(cfg(
        victim.path().into(),
        192,
        18193,
        vec!["127.0.0.1:18192".into()],
        true,
        Some(secret),
    ));
    // Control: an identical peer dialing A directly must sync, so that a
    // failure above can't be blamed on the configuration.
    spawn(cfg(
        control.path().into(),
        193,
        18194,
        vec!["127.0.0.1:18191".into()],
        true,
        Some(secret),
    ));
    tokio::time::sleep(Duration::from_millis(1200)).await;

    std::fs::write(a.path().join("secret.txt"), "confidential\n").unwrap();

    assert!(
        wait_content(&control.path().join("secret.txt"), "confidential\n", T).await,
        "control peer failed to sync, so this test proves nothing"
    );
    assert!(
        !wait_content(
            &victim.path().join("secret.txt"),
            "confidential\n",
            Duration::from_secs(3)
        )
        .await,
        "data reached a peer through a man-in-the-middle"
    );
}
