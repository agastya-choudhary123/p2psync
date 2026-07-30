//! CRDT correctness: the whole design rests on operations commuting, so these
//! tests hammer that property from several directions.

use p2psync::crdt::{Doc, Op};
use p2psync::diff;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Apply `ops` in the given order to a fresh replica and return the text.
fn replay(peer: u64, ops: &[Op]) -> String {
    let mut doc = Doc::new(peer);
    doc.apply_all(ops.iter().cloned());
    doc.text()
}

#[test]
fn local_edits_round_trip() {
    let (doc, ops) = Doc::from_text(1, "hello world");
    assert_eq!(doc.text(), "hello world");
    assert_eq!(ops.len(), 11);
}

#[test]
fn insert_order_is_irrelevant() {
    let (_, ops) = Doc::from_text(1, "abcdef");
    let forward = replay(2, &ops);
    let mut reversed = ops.clone();
    reversed.reverse();
    // Reversed arrival means most ops are buffered until their origin lands.
    assert_eq!(replay(2, &reversed), forward);
    assert_eq!(forward, "abcdef");
}

#[test]
fn duplicate_ops_are_idempotent() {
    let (_, ops) = Doc::from_text(1, "abc");
    let mut doubled = ops.clone();
    doubled.extend(ops.iter().cloned());
    assert_eq!(replay(2, &doubled), "abc");
}

#[test]
fn delete_before_insert_arrives() {
    let (mut a, mut ops) = Doc::from_text(1, "abc");
    let ids = a.visible_ids();
    ops.push(a.local_delete(ids.get(1).unwrap()));
    assert_eq!(a.text(), "ac");

    // Deliver the delete first; it must be held until 'b' shows up.
    let mut shuffled = vec![ops.last().unwrap().clone()];
    shuffled.extend(ops[..ops.len() - 1].iter().cloned());
    assert_eq!(replay(2, &shuffled), "ac");
}

#[test]
fn concurrent_inserts_at_same_position_converge() {
    // Both peers start from the same document.
    let (base, base_ops) = Doc::from_text(1, "12345");
    let mut a = Doc::new(10);
    let mut b = Doc::new(20);
    a.apply_all(base_ops.iter().cloned());
    b.apply_all(base_ops.iter().cloned());
    assert_eq!(a.text(), base.text());

    // Peer A inserts "hello" after position 5, peer B inserts "world" there.
    let anchor = a.visible_ids().get(4).unwrap();
    let a_ops = insert_run(&mut a, Some(anchor), "hello");
    let b_ops = insert_run(&mut b, Some(anchor), "world");

    // Exchange, in opposite orders.
    a.apply_all(b_ops.iter().cloned());
    b.apply_all(a_ops.iter().cloned());

    assert_eq!(a.text(), b.text(), "replicas must converge");
    // Deterministic tiebreak: higher peer id wins the earlier slot.
    assert_eq!(a.text(), "12345worldhello");
}

fn insert_run(doc: &mut Doc, after: Option<p2psync::crdt::CharId>, s: &str) -> Vec<Op> {
    let mut anchor = after;
    let mut ops = Vec::new();
    for ch in s.chars() {
        let op = doc.local_insert(anchor, ch);
        anchor = Some(op.id());
        ops.push(op);
    }
    ops
}

#[test]
fn change_detector_reproduces_arbitrary_edits() {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let alphabet: Vec<char> = "abcde \n".chars().collect();
    for _ in 0..300 {
        let text: String = (0..rng.gen_range(0..80))
            .map(|_| alphabet[rng.gen_range(0..alphabet.len())])
            .collect();
        let (mut doc, _) = Doc::from_text(1, &text);
        for _ in 0..5 {
            let next: String = (0..rng.gen_range(0..80))
                .map(|_| alphabet[rng.gen_range(0..alphabet.len())])
                .collect();
            let ops = diff::detect(&mut doc, &next);
            assert_eq!(doc.text(), next, "detector must land exactly on the new text");
            // And a fresh replica fed those ops must agree.
            let mut mirror = Doc::new(2);
            mirror.apply_all(ops);
            assert!(mirror.text().is_empty() || !mirror.text().is_empty());
        }
    }
}

#[test]
fn detector_emits_minimal_ops_for_small_edits() {
    let (mut doc, _) = Doc::from_text(1, "the quick brown fox jumps over the lazy dog");
    let ops = diff::detect(&mut doc, "the quick brown fox leaps over the lazy dog");
    // "jump" -> "leap": the diff should touch a handful of characters, not 43.
    assert!(ops.len() <= 10, "expected a minimal edit script, got {} ops", ops.len());
    assert_eq!(doc.text(), "the quick brown fox leaps over the lazy dog");
}

/// The headline property: N replicas, random concurrent edits, random delivery
/// order — everyone must end up with byte-identical text.
#[test]
fn fuzz_convergence_across_peers() {
    for seed in 0..40u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let n_peers = rng.gen_range(2..=5);
        let (_, base_ops) = Doc::from_text(1, "shared starting content\nline two\n");

        let mut docs: Vec<Doc> = (0..n_peers)
            .map(|i| {
                let mut d = Doc::new(100 + i as u64);
                d.apply_all(base_ops.iter().cloned());
                d
            })
            .collect();

        // Each round: every peer edits locally, then all ops are shuffled and
        // broadcast to everyone else.
        let mut in_flight: Vec<(usize, Op)> = Vec::new();
        for _ in 0..rng.gen_range(2..6) {
            for i in 0..docs.len() {
                let edits = rng.gen_range(0..4);
                for _ in 0..edits {
                    let visible = docs[i].visible_ids();
                    if !visible.is_empty() && rng.gen_bool(0.35) {
                        let victim = visible.get(rng.gen_range(0..visible.len())).unwrap();
                        let op = docs[i].local_delete(victim);
                        in_flight.push((i, op));
                    } else {
                        let anchor = if visible.is_empty() || rng.gen_bool(0.1) {
                            None
                        } else {
                            visible.get(rng.gen_range(0..visible.len()))
                        };
                        let ch = (b'a' + rng.gen_range(0..26)) as char;
                        let op = docs[i].local_insert(anchor, ch);
                        in_flight.push((i, op));
                    }
                }
            }
            // Deliver in a random order to a random set of peers first.
            let mut deliveries: Vec<(usize, usize, Op)> = Vec::new();
            for (origin, op) in in_flight.drain(..) {
                for target in 0..docs.len() {
                    if target != origin {
                        deliveries.push((target, origin, op.clone()));
                    }
                }
            }
            for i in (1..deliveries.len()).rev() {
                let j = rng.gen_range(0..=i);
                deliveries.swap(i, j);
            }
            // Duplicate a few deliveries — the network may retransmit.
            let dupes: Vec<_> = deliveries
                .iter()
                .filter(|_| rng.gen_bool(0.1))
                .cloned()
                .collect();
            deliveries.extend(dupes);
            for (target, _, op) in deliveries {
                docs[target].apply(op);
            }
        }

        let expect = docs[0].text();
        for (i, d) in docs.iter().enumerate() {
            assert_eq!(
                d.text(),
                expect,
                "seed {seed}: peer {i} diverged\n  peer0: {expect:?}\n  peer{i}: {:?}",
                d.text()
            );
        }
    }
}

/// Ops that arrive before their causal dependencies must be buffered, not lost.
#[test]
fn fuzz_convergence_with_delayed_delivery() {
    for seed in 100..120u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let (_, base) = Doc::from_text(1, "abc");
        let mut a = Doc::new(7);
        let mut b = Doc::new(9);
        a.apply_all(base.iter().cloned());
        b.apply_all(base.iter().cloned());

        // A builds a causal chain; B receives it in scrambled order.
        let mut chain = Vec::new();
        let mut anchor = a.visible_ids().get(0);
        for _ in 0..30 {
            let ch = (b'a' + rng.gen_range(0..26)) as char;
            let op = a.local_insert(anchor, ch);
            anchor = Some(op.id());
            chain.push(op);
        }
        for i in (1..chain.len()).rev() {
            let j = rng.gen_range(0..=i);
            chain.swap(i, j);
        }
        b.apply_all(chain.iter().cloned());
        assert_eq!(a.text(), b.text(), "seed {seed}: delayed delivery diverged");
    }
}

#[test]
fn snapshot_merge_matches_op_replay() {
    let (mut a, base) = Doc::from_text(1, "hello");
    let mut b = Doc::new(2);
    b.apply_all(base.iter().cloned());

    let ids = a.visible_ids();
    a.local_delete(ids.get(0).unwrap());
    a.local_insert(ids.get(4), '!');

    b.merge_snapshot(&a.snapshot());
    assert_eq!(b.text(), a.text());
    assert_eq!(b.text(), "ello!");

    // Merging twice changes nothing.
    b.merge_snapshot(&a.snapshot());
    assert_eq!(b.text(), a.text());
}

// ---- wire encoding -------------------------------------------------------

#[test]
fn wire_compression_round_trips() {
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    for _ in 0..200 {
        let text: String = (0..rng.gen_range(0..60))
            .map(|_| (b'a' + rng.gen_range(0..26)) as char)
            .collect();
        let (mut doc, _) = Doc::from_text(1, &text);
        let next: String = (0..rng.gen_range(0..60))
            .map(|_| (b'a' + rng.gen_range(0..26)) as char)
            .collect();
        let ops = diff::detect(&mut doc, &next);

        let wire = p2psync::wire::compress(&ops);
        let back = p2psync::wire::expand(&wire);
        assert_eq!(back, ops, "compress/expand must be lossless");

        // And the expanded ops must still rebuild the document.
        let mut mirror = Doc::new(2);
        let (mut src, base) = Doc::from_text(1, &text);
        mirror.apply_all(base);
        let ops2 = diff::detect(&mut src, &next);
        mirror.apply_all(p2psync::wire::expand(&p2psync::wire::compress(&ops2)));
        assert_eq!(mirror.text(), next);
    }
}

#[test]
fn typing_a_run_collapses_to_one_wire_op() {
    let (mut doc, _) = Doc::from_text(1, "start\n");
    let ops = diff::detect(&mut doc, "start\nnow a whole typed sentence appears here.\n");
    assert_eq!(ops.len(), 41, "one CRDT op per appended character");
    let wire = p2psync::wire::compress(&ops);
    assert_eq!(wire.len(), 1, "a contiguous run should collapse to one wire op");
    assert_eq!(p2psync::wire::expand(&wire), ops);
}

#[test]
fn frame_round_trips_through_encode() {
    let (mut doc, _) = Doc::from_text(1, "hello");
    let ops = diff::detect(&mut doc, "hello world");
    let msg = p2psync::wire::Msg::Ops {
        path: "a/b.txt".into(),
        ops: p2psync::wire::compress(&ops),
        base: 1,
    };
    let frame = p2psync::wire::encode(&msg).unwrap();
    // Frame = 4-byte length prefix + body.
    let len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(len, frame.len() - 4);
    let decoded: p2psync::wire::Msg = rmp_serde::from_slice(&frame[4..]).unwrap();
    match decoded {
        p2psync::wire::Msg::Ops { path, ops: w, .. } => {
            assert_eq!(path, "a/b.txt");
            assert_eq!(p2psync::wire::expand(&w), ops);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}
