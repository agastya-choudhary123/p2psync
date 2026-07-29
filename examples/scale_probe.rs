use p2psync::crdt::Doc;
use p2psync::diff;
use std::time::Instant;

fn main() {
    for chars in [1_000usize, 10_000, 50_000, 200_000] {
        let text: String = (0..chars / 50)
            .map(|i| format!("line {i}: some representative source-ish content here\n"))
            .collect();
        let t0 = Instant::now();
        let (mut doc, _) = Doc::from_text(1, &text);
        let build = t0.elapsed();

        // A one-character edit in the middle: detect + apply.
        let mut edited = text.clone();
        let mid = text.len() / 2;
        edited.insert(mid, 'X');
        let t1 = Instant::now();
        let ops = diff::detect(&mut doc, &edited);
        let one_edit = t1.elapsed();

        // A 2000-char paste.
        let paste: String = "p".repeat(2000);
        let mut edited2 = edited.clone();
        edited2.insert_str(mid, &paste);
        let t2 = Instant::now();
        let ops2 = diff::detect(&mut doc, &edited2);
        let paste_time = t2.elapsed();

        println!(
            "{:>7} chars | build {:>8.1}ms | 1-char edit {:>8.3}ms ({} ops) | 2000-char paste {:>9.1}ms ({} ops)",
            doc.len_visible(), build.as_secs_f64()*1e3, one_edit.as_secs_f64()*1e3, ops.len(),
            paste_time.as_secs_f64()*1e3, ops2.len()
        );
    }
}
