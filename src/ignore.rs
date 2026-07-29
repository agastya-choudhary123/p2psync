//! Which paths take part in sync.
//!
//! Built-in rules cover the sync metadata and the usual editor and OS noise; a
//! `.p2psyncignore` file in the sync root adds gitignore-style globs.

use std::path::Path;

/// Always excluded, regardless of `.p2psyncignore`.
const BUILTIN: &[&str] = &[
    ".p2psync",
    ".git",
    ".DS_Store",
    "*.swp",
    "*.swx",
    "*~",
    ".#*",
    "*.p2ptmp*",
    "*.tmp",
];

pub struct Ignore {
    patterns: Vec<String>,
}

impl Default for Ignore {
    fn default() -> Self {
        Self {
            patterns: BUILTIN.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Ignore {
    /// Load `.p2psyncignore` from the sync root, if present.
    ///
    /// Syntax is the useful subset of gitignore: one glob per line, `#`
    /// comments, blank lines skipped, `*` and `?` within a path component,
    /// `**` across components, and a trailing `/` to mean "this directory".
    pub fn load(root: &Path) -> Self {
        let mut me = Self::default();
        let Ok(text) = std::fs::read_to_string(root.join(".p2psyncignore")) else {
            return me;
        };
        let mut added = 0;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            me.patterns.push(line.trim_end_matches('/').to_string());
            added += 1;
        }
        if added > 0 {
            println!("[ignore] loaded {added} pattern(s) from .p2psyncignore");
        }
        me
    }

    /// Is this root-relative, slash-separated path excluded?
    ///
    /// A pattern matches if it matches the whole path or any leading directory
    /// of it, so `build` excludes `build/out/x.o` as well.
    pub fn is_ignored(&self, rel: &str) -> bool {
        let components: Vec<&str> = rel.split('/').filter(|c| !c.is_empty()).collect();
        for pat in &self.patterns {
            if pat.contains('/') {
                // Anchored pattern: match against the path from the root.
                if glob_match(pat, rel) {
                    return true;
                }
                // ...and against every parent directory.
                for end in 1..components.len() {
                    if glob_match(pat, &components[..end].join("/")) {
                        return true;
                    }
                }
            } else {
                // Bare pattern: match any single component.
                if components.iter().any(|c| glob_match(pat, c)) {
                    return true;
                }
            }
        }
        false
    }
}

/// Glob matcher supporting `*`, `?`, and `**`.
///
/// `*` and `?` stop at `/`; `**` crosses separators.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    m(&p, 0, &t, 0)
}

fn m(p: &[char], mut pi: usize, t: &[char], mut ti: usize) -> bool {
    while pi < p.len() {
        match p[pi] {
            '*' => {
                let double = pi + 1 < p.len() && p[pi + 1] == '*';
                let rest = pi + if double { 2 } else { 1 };
                // Try every split point, shortest first.
                let mut k = ti;
                loop {
                    if m(p, rest, t, k) {
                        return true;
                    }
                    if k >= t.len() || (!double && t[k] == '/') {
                        return false;
                    }
                    k += 1;
                }
            }
            '?' => {
                if ti >= t.len() || t[ti] == '/' {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            c => {
                if ti >= t.len() || t[ti] != c {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti == t.len()
}
