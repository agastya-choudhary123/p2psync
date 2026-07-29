//! Myers diff over `char` slices, plus the change detector that turns a
//! rewritten file into a minimal set of CRDT operations.

use crate::crdt::{CharId, Doc, Op};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edit {
    Keep,
    /// Delete one element from the old sequence.
    Del,
    /// Insert one element from the new sequence.
    Ins(char),
}

/// Myers O((N+M)D) diff. Returns an edit script walking the old sequence.
///
/// `max_d` bounds the search; past that we give up and return a wholesale
/// replace, which keeps a pathological rewrite from costing quadratic time.
pub fn myers(old: &[char], new: &[char], max_d: usize) -> Vec<Edit> {
    // Trim the common prefix/suffix first — for an interactive editor this is
    // almost the whole file, so the actual Myers search stays tiny.
    let mut pre = 0;
    while pre < old.len() && pre < new.len() && old[pre] == new[pre] {
        pre += 1;
    }
    let mut suf = 0;
    while suf < old.len() - pre && suf < new.len() - pre && old[old.len() - 1 - suf] == new[new.len() - 1 - suf] {
        suf += 1;
    }
    let a = &old[pre..old.len() - suf];
    let b = &new[pre..new.len() - suf];

    let mut script = vec![Edit::Keep; pre];
    match myers_core(a, b, max_d) {
        Some(mid) => script.extend(mid),
        None => {
            script.extend(std::iter::repeat_n(Edit::Del, a.len()));
            script.extend(b.iter().map(|c| Edit::Ins(*c)));
        }
    }
    script.extend(std::iter::repeat_n(Edit::Keep, suf));
    script
}

fn myers_core(a: &[char], b: &[char], max_d: usize) -> Option<Vec<Edit>> {
    let (n, m) = (a.len(), b.len());
    if n == 0 {
        return Some(b.iter().map(|c| Edit::Ins(*c)).collect());
    }
    if m == 0 {
        return Some(vec![Edit::Del; n]);
    }
    let max = (n + m).min(max_d);
    let offset = max as isize;
    // v[k + offset] = furthest x reached on diagonal k.
    let mut v = vec![usize::MAX; 2 * max + 2];
    let mut trace: Vec<Vec<usize>> = Vec::new();
    v[(1 + offset) as usize] = 0;

    for d in 0..=max {
        trace.push(v.clone());
        let di = d as isize;
        let mut k = -di;
        while k <= di {
            let ki = (k + offset) as usize;
            let down = k == -di
                || (k != di && {
                    let l = v[ki - 1];
                    let r = v[ki + 1];
                    l == usize::MAX || (r != usize::MAX && l < r)
                });
            let mut x = if down { v[ki + 1] } else { v[ki - 1] + 1 };
            if x == usize::MAX {
                k += 2;
                continue;
            }
            let mut y = (x as isize - k) as usize;
            while x < n && y < m && a[x] == b[y] {
                x += 1;
                y += 1;
            }
            v[ki] = x;
            if x >= n && y >= m {
                return Some(backtrack(a, b, &trace, offset));
            }
            k += 2;
        }
    }
    None
}

fn backtrack(a: &[char], b: &[char], trace: &[Vec<usize>], offset: isize) -> Vec<Edit> {
    let (mut x, mut y) = (a.len(), b.len());
    let mut out: Vec<Edit> = Vec::new();
    for (d, v) in trace.iter().enumerate().rev() {
        let di = d as isize;
        let k = x as isize - y as isize;
        let ki = (k + offset) as usize;
        let down = k == -di
            || (k != di && {
                let l = v[ki - 1];
                let r = v[ki + 1];
                l == usize::MAX || (r != usize::MAX && l < r)
            });
        let prev_k = if down { k + 1 } else { k - 1 };
        let prev_x = v[(prev_k + offset) as usize];
        if prev_x == usize::MAX {
            continue;
        }
        let prev_y = (prev_x as isize - prev_k) as usize;

        // Snake: the diagonal run we followed to get here.
        while x > prev_x && y > prev_y {
            out.push(Edit::Keep);
            x -= 1;
            y -= 1;
        }
        if d > 0 {
            if x == prev_x {
                y -= 1;
                out.push(Edit::Ins(b[y]));
            } else {
                x -= 1;
                out.push(Edit::Del);
            }
        }
    }
    out.reverse();
    out
}

/// Diff the document's current text against `new_text` and emit the CRDT ops
/// that transform the document to match.
///
/// This is the reverse-engineering step: the user edited a file behind our
/// back, and we have to describe what they did in terms of character ids.
pub fn detect(doc: &mut Doc, new_text: &str) -> Vec<Op> {
    let old: Vec<char> = doc.text().chars().collect();
    let new: Vec<char> = new_text.chars().collect();
    if old == new {
        return Vec::new();
    }
    let ids = doc.visible_ids();
    let script = myers(&old, &new, 100_000);

    let mut ops = Vec::new();
    // `anchor` is the id the next inserted character attaches after, i.e. the
    // last element that survives at this point in the new sequence.
    let mut anchor: Option<CharId> = None;
    let mut oi = 0usize; // cursor into the old visible elements

    for edit in script {
        match edit {
            Edit::Keep => {
                anchor = Some(ids[oi]);
                oi += 1;
            }
            Edit::Del => {
                ops.push(doc.local_delete(ids[oi]));
                oi += 1;
            }
            Edit::Ins(ch) => {
                let op = doc.local_insert(anchor, ch);
                anchor = Some(op.id());
                ops.push(op);
            }
        }
    }
    debug_assert_eq!(doc.text(), new_text, "change detector must reproduce the file exactly");
    ops
}
