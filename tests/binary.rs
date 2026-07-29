//! Binary file path: rolling checksum, block matching, delta reconstruction.

use p2psync::binary::{apply_delta, delta, is_binary, sha256_hex, signatures, Rolling};
use p2psync::wire::{DeltaOp, BLOCK_SIZE};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

fn literal_bytes(ops: &[DeltaOp]) -> usize {
    ops.iter()
        .map(|o| match o {
            DeltaOp::Literal(b) => b.len(),
            DeltaOp::CopyBlock(_) => 0,
        })
        .sum()
}

#[test]
fn rolling_checksum_matches_recomputation() {
    let mut rng = StdRng::seed_from_u64(1);
    let mut data = vec![0u8; 4096];
    rng.fill_bytes(&mut data);
    let window = 64;

    let mut roll = Rolling::new(&data[..window]);
    for start in 0..(data.len() - window - 1) {
        let expect = Rolling::new(&data[start..start + window]).digest();
        assert_eq!(roll.digest(), expect, "drifted at offset {start}");
        roll.roll(data[start], data[start + window]);
    }
}

#[test]
fn identical_files_transfer_no_literals() {
    let mut rng = StdRng::seed_from_u64(2);
    let mut data = vec![0u8; BLOCK_SIZE * 10];
    rng.fill_bytes(&mut data);

    let sigs = signatures(&data);
    let ops = delta(&data, &sigs);
    assert_eq!(literal_bytes(&ops), 0, "no bytes should be sent for an identical file");
    assert_eq!(apply_delta(&data, &ops), data);
}

#[test]
fn small_change_sends_roughly_one_block() {
    let mut rng = StdRng::seed_from_u64(3);
    let mut old = vec![0u8; BLOCK_SIZE * 20];
    rng.fill_bytes(&mut old);
    let mut new = old.clone();
    // Flip one byte in the middle.
    new[BLOCK_SIZE * 7 + 100] ^= 0xff;

    let ops = delta(&new, &signatures(&old));
    let sent = literal_bytes(&ops);
    assert_eq!(apply_delta(&old, &ops), new, "reconstruction must be exact");
    assert!(
        sent <= BLOCK_SIZE * 2,
        "changing one byte in {} should not resend {sent} bytes",
        new.len()
    );
}

#[test]
fn insertion_realigns_via_rolling_window() {
    let mut rng = StdRng::seed_from_u64(4);
    let mut old = vec![0u8; BLOCK_SIZE * 12];
    rng.fill_bytes(&mut old);

    // Insert bytes near the front: every subsequent block shifts, so a
    // fixed-block scheme would resend everything. The rolling window should
    // resync and reuse the shifted blocks.
    let mut new = Vec::new();
    new.extend_from_slice(&old[..500]);
    new.extend_from_slice(b"-- inserted text --");
    new.extend_from_slice(&old[500..]);

    let ops = delta(&new, &signatures(&old));
    assert_eq!(apply_delta(&old, &ops), new);
    let sent = literal_bytes(&ops);
    assert!(
        sent < BLOCK_SIZE * 3,
        "insertion should resync quickly, but {sent} literal bytes were sent"
    );
}

#[test]
fn delta_from_nothing_sends_everything() {
    let data = b"brand new file".to_vec();
    let ops = delta(&data, &[]);
    assert_eq!(literal_bytes(&ops), data.len());
    assert_eq!(apply_delta(&[], &ops), data);
}

#[test]
fn fuzz_delta_reconstruction_is_exact() {
    let mut rng = StdRng::seed_from_u64(5);
    for _ in 0..60 {
        let len = rng.gen_range(0..BLOCK_SIZE * 6);
        let mut old = vec![0u8; len];
        rng.fill_bytes(&mut old);

        // Random mutation: truncate, extend, splice, or scribble.
        let mut new = old.clone();
        match rng.gen_range(0..4) {
            0 => new.truncate(rng.gen_range(0..=new.len())),
            1 => {
                let extra = rng.gen_range(1..2000);
                let mut tail = vec![0u8; extra];
                rng.fill_bytes(&mut tail);
                new.extend_from_slice(&tail);
            }
            2 if !new.is_empty() => {
                let at = rng.gen_range(0..new.len());
                let chunk: Vec<u8> = (0..rng.gen_range(1..300)).map(|_| rng.gen()).collect();
                new.splice(at..at, chunk);
            }
            _ => {
                for _ in 0..rng.gen_range(1..20) {
                    if new.is_empty() {
                        break;
                    }
                    let at = rng.gen_range(0..new.len());
                    new[at] = rng.gen();
                }
            }
        }

        let ops = delta(&new, &signatures(&old));
        let rebuilt = apply_delta(&old, &ops);
        assert_eq!(rebuilt, new, "reconstruction mismatch");
        assert_eq!(sha256_hex(&rebuilt), sha256_hex(&new));
    }
}

#[test]
fn binary_detection() {
    assert!(!is_binary(b"plain ascii"));
    assert!(!is_binary("unicode \u{1F600} text".as_bytes()));
    assert!(is_binary(b"has\0nul"));
    assert!(is_binary(&[0xff, 0xfe, 0xfd]));
}
