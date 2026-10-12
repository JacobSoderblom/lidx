//! Index freshness: how far the checkout has moved since the last completed
//! index/sync, cheap enough to attach to every response.
//!
//! The check is one `git status` spawn (current HEAD plus untracked files)
//! and one `stat` per indexed file; only files whose mtime or size differ are
//! re-hashed, so a `touch` or a checkout that restores identical content does
//! not count as a change. Callers cache the result for [`CACHE_TTL`].

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use crate::db::Db;
use crate::indexer::scan;

/// At most one check per this interval per indexer.
pub const CACHE_TTL: Duration = Duration::from_secs(1);

/// `meta` key holding git HEAD at the last completed reindex or sync.
pub const INDEXED_HEAD_KEY: &str = "indexed_head";

/// Files re-hashed per check; mismatches beyond this are counted as changed.
const MAX_REHASH: usize = 200;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Stale {
    pub head_moved: bool,
    pub indexed_commit: Option<String>,
    pub current_head: Option<String>,
    pub changed_files: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Freshness {
    pub indexed_commit: Option<String>,
    pub indexed_at: Option<i64>,
    pub index_age_s: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale: Option<Stale>,
}

impl Freshness {
    /// The `_meta` fields for a response. `None` when the index is fresh, so
    /// the common path adds no bytes.
    pub fn stale_meta(&self) -> Option<Value> {
        let stale = self.stale.as_ref()?;
        Some(json!({
            "indexed_commit": self.indexed_commit,
            "indexed_at": self.indexed_at,
            "index_age_s": self.index_age_s,
            "stale": stale,
        }))
    }

    /// The trailing compact-text warning, `None` when fresh.
    pub fn warning_line(&self) -> Option<String> {
        let stale = self.stale.as_ref()?;
        let mut parts = Vec::new();
        if stale.head_moved {
            parts.push(format!(
                "HEAD moved {}\u{2192}{}",
                short(stale.indexed_commit.as_deref()),
                short(stale.current_head.as_deref())
            ));
        }
        if stale.changed_files > 0 {
            parts.push(format!(
                "{} changed file{}",
                stale.changed_files,
                if stale.changed_files == 1 { "" } else { "s" }
            ));
        }
        Some(format!(
            "\u{26a0} index stale: {} \u{2014} run reindex",
            parts.join(", ")
        ))
    }
}

fn short(sha: Option<&str>) -> &str {
    sha.map_or("none", |s| &s[..s.len().min(7)])
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Current git HEAD and the paths `git status` lists, or `(None, [])`
/// outside a git work tree.
fn git_state(repo_root: &Path) -> (Option<String>, Vec<String>) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args([
            "status",
            "--porcelain=v2",
            "--branch",
            "--untracked-files=all",
            "-z",
        ])
        .output();
    let Ok(output) = output else {
        return (None, Vec::new());
    };
    if !output.status.success() {
        return (None, Vec::new());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut head = None;
    let mut paths = Vec::new();
    let mut tokens = text.split('\0');
    while let Some(token) = tokens.next() {
        if let Some(oid) = token.strip_prefix("# branch.oid ") {
            // "(initial)" on a repo with no commits yet.
            if oid != "(initial)" {
                head = Some(oid.to_string());
            }
        } else if token.starts_with('#') {
        } else if let Some(path) = token.strip_prefix("? ") {
            paths.push(path.to_string());
        } else if token.starts_with("1 ") || token.starts_with("u ") {
            // 1: 8 fields then path; u: 10 fields then path.
            let skip = if token.starts_with("1 ") { 8 } else { 10 };
            if let Some(path) = token.splitn(skip + 1, ' ').nth(skip) {
                paths.push(path.to_string());
            }
        } else if token.starts_with("2 ") {
            // Rename/copy: 9 fields then path, original path in the next token.
            if let Some(path) = token.splitn(10, ' ').nth(9) {
                paths.push(path.to_string());
            }
            tokens.next();
        }
    }
    (head, paths)
}

pub fn compute(db: &Db, repo_root: &Path, graph_version: i64) -> Result<Freshness> {
    let indexed_commit = match db.get_meta_str(INDEXED_HEAD_KEY)? {
        Some(sha) if !sha.is_empty() => Some(sha),
        _ => db.graph_version_commit(graph_version)?,
    };
    let indexed_at = db.get_meta_i64("last_indexed")?;
    let index_age_s = indexed_at.map(|at| (now_secs() - at).max(0));

    let (current_head, git_paths) = git_state(repo_root);
    let head_moved = matches!(
        (&indexed_commit, &current_head),
        (Some(a), Some(b)) if a != b
    );

    let indexed: HashMap<String, (String, i64, i64)> = db
        .list_file_stats(graph_version)?
        .into_iter()
        .map(|(path, hash, size, modified)| (path, (hash, size, modified)))
        .collect();

    let mut changed = 0usize;
    let mut rehashed = 0usize;
    for (path, (hash, size, modified)) in &indexed {
        let abs = repo_root.join(path);
        let Ok(meta) = std::fs::metadata(&abs) else {
            changed += 1;
            continue;
        };
        let mtime = meta
            .modified()
            .ok()
            .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_secs() as i64);
        if meta.len() as i64 == *size && mtime == *modified {
            continue;
        }
        if meta.len() as i64 != *size || rehashed >= MAX_REHASH {
            changed += 1;
            continue;
        }
        rehashed += 1;
        match std::fs::read(&abs) {
            Ok(data) if scan::hash_bytes(&data) == *hash => {}
            _ => changed += 1,
        }
    }
    // Files git sees that the index has never held (new, untracked, renamed
    // targets); indexed paths were already covered by the stat pass.
    for path in git_paths {
        if !indexed.contains_key(&path)
            && scan::language_for_path(Path::new(&path)).is_some()
            && repo_root.join(&path).is_file()
        {
            changed += 1;
        }
    }

    let stale = (head_moved || changed > 0).then(|| Stale {
        head_moved,
        indexed_commit: indexed_commit.clone(),
        current_head,
        changed_files: changed,
    });
    Ok(Freshness {
        indexed_commit,
        indexed_at,
        index_age_s,
        stale,
    })
}
