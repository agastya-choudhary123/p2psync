//! How the block representation degrades under sustained editing.
//!
//! Storing a run of characters per block is what makes a document cheap, but
//! every edit splits a block, so the interesting question is not the cost of a
//! freshly loaded file — it is whether an hour of typing turns the document
//! back into one block per character. This drives edits through the real change
//! detector and reports the block count and per-edit cost as it goes.

use p2psync::crdt::Doc;
use p2psync::diff;
use std::time::Instant;

fn main() {
    let text: String = (0..4400)
        .map(|i| format!("line {i}: some representative source-ish content here\n"))
        .collect();
    let (mut doc, _) = Doc::from_text(1, &text);
    let mut current = text.clone();
    println!(
        "start: {} chars, {} blocks, {} bytes of text",
        doc.len_visible(),
        doc.block_count(),
        current.len()
    );

    // Deterministic pseudo-random edit positions.
    let mut x = 0x9E3779B97F4A7C15u64;
    let mut rnd = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };

    let mut edits = 0;
    for round in 1..=20 {
        let t0 = Instant::now();
        for _ in 0..250 {
            // A realistic save: type a few characters somewhere, occasionally
            // deleting a few first.
            let at = (rnd() as usize) % current.len();
            let at = floor_char_boundary(&current, at);
            if rnd() % 3 == 0 {
                let end = floor_char_boundary(&current, (at + 5).min(current.len()));
                current.replace_range(at..end, "");
            } else {
                current.insert_str(at, "xy");
            }
            diff::detect(&mut doc, &current);
            edits += 1;
        }
        let per_edit = t0.elapsed().as_secs_f64() * 1e3 / 250.0;
        println!(
            "after {edits:5} edits: {:6} blocks for {:6} visible chars ({:5} stored) | {per_edit:6.3}ms per edit",
            doc.block_count(),
            doc.len_visible(),
            doc.len_raw(),
        );
        let _ = round;
    }

    // A checkpoint rebuilds the document, which is what recoalesces it.
    let snapshot = doc.snapshot();
    let t0 = Instant::now();
    let mut rebuilt = Doc::new(1);
    rebuilt.adopt(snapshot);
    println!(
        "after adopt (what a checkpoint does): {} blocks, {:.1}ms",
        rebuilt.block_count(),
        t0.elapsed().as_secs_f64() * 1e3
    );
    let (fresh, _) = Doc::from_text(1, &rebuilt.text());
    println!(
        "after a checkpoint's from_shared_content: {} blocks",
        fresh.block_count()
    );
    assert_eq!(rebuilt.text(), doc.text(), "adopt must preserve the text");
}

fn floor_char_boundary(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}
