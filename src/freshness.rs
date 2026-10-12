//! Index freshness: how far the checkout has moved since the last completed
//! index/sync, cheap enough to attach to every response.
//!
//! The check reads HEAD from the `.git` files, stats every indexed file and
//! stats the indexable directories recorded at index time (listing only those
//! whose mtime moved, with the indexer's ignore rules). A file whose size and
//! mtime match is still re-hashed when its mtime is within a second of the
//! scan (git's racy-entry rule); one whose mtime alone differs is re-hashed
//! too, so a `touch` or a checkout restoring identical content is not a
//! change. Callers cache the result for [`CACHE_TTL`].

use std::collections::{HashMap, HashSet};
use std::path::Path;
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

/// `meta` key: start time (unix seconds) of the last completed reindex scan.
pub const INDEXED_SCAN_AT_KEY: &str = "indexed_scan_at";

/// `meta` key: newline-joined indexable directories present at index time.
pub const INDEXED_DIRS_KEY: &str = "indexed_dirs";

/// Files re-hashed per check; beyond this they are counted as changed (the
/// safe direction: a warning, never silent staleness).
const MAX_REHASH: usize = 1000;

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

fn mtime_secs(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs() as i64)
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Current git HEAD read straight from the `.git` files (no process spawn,
/// which alone costs more than the whole freshness budget). Handles
/// worktrees, detached HEADs and packed refs; `None` outside a git work tree.
/// Current HEAD: the `.git` file reader, falling back to `git rev-parse` only
/// where that cannot resolve it. The one source for both recording and
/// comparing, so the two can never disagree.
pub fn current_head(repo_root: &Path) -> Option<String> {
    read_head(repo_root).or_else(|| crate::util::git_head_sha(repo_root))
}

fn read_head(repo_root: &Path) -> Option<String> {
    let dot_git = repo_root
        .ancestors()
        .map(|d| d.join(".git"))
        .find(|p| p.exists())?;
    let git_dir = if dot_git.is_file() {
        let text = std::fs::read_to_string(&dot_git).ok()?;
        let target = text.trim().strip_prefix("gitdir:")?.trim();
        dot_git.parent()?.join(target)
    } else {
        dot_git
    };
    // A linked worktree keeps shared refs in the common dir.
    let common_dir = match std::fs::read_to_string(git_dir.join("commondir")) {
        Ok(rel) => git_dir.join(rel.trim()),
        Err(_) => git_dir.clone(),
    };
    let head = std::fs::read_to_string(git_dir.join("HEAD")).ok()?;
    let head = head.trim();
    let Some(reference) = head.strip_prefix("ref:").map(str::trim) else {
        return (head.len() >= 40).then(|| head.to_string());
    };
    for dir in [&git_dir, &common_dir] {
        if let Ok(sha) = std::fs::read_to_string(dir.join(reference)) {
            return Some(sha.trim().to_string());
        }
    }
    let packed = std::fs::read_to_string(common_dir.join("packed-refs")).ok()?;
    packed.lines().find_map(|line| {
        let (sha, name) = line.split_once(' ')?;
        (name == reference).then(|| sha.to_string())
    })
}

pub fn compute(
    db: &Db,
    repo_root: &Path,
    graph_version: i64,
    options: scan::ScanOptions,
) -> Result<Freshness> {
    let indexed_commit = match db.get_meta_str(INDEXED_HEAD_KEY)? {
        Some(sha) if !sha.is_empty() => Some(sha),
        _ => db.graph_version_commit(graph_version)?,
    };
    let indexed_at = db.get_meta_i64("last_indexed")?;
    let index_age_s = indexed_at.map(|at| (now_secs() - at).max(0));

    let current_head = current_head(repo_root);
    let head_moved = matches!(
        (&indexed_commit, &current_head),
        (Some(a), Some(b)) if a != b
    );

    let indexed: HashMap<String, (String, i64, i64)> = db
        .list_file_stats(graph_version)?
        .into_iter()
        .map(|(path, hash, size, modified)| (path, (hash, size, modified)))
        .collect();

    // Reindex scan start; older indexes fall back to when they finished.
    let scan_at = db
        .get_meta_i64(INDEXED_SCAN_AT_KEY)?
        .or(indexed_at)
        .unwrap_or(i64::MAX);
    let mut changed = 0usize;
    let mut rehashed = 0usize;
    // Indexed files: one stat each. A differing mtime at equal size may be a
    // touch or a checkout that restored identical content: the hash decides.
    for (path, (hash, size, mtime)) in &indexed {
        let abs = repo_root.join(path);
        let Ok(meta) = std::fs::metadata(&abs) else {
            changed += 1;
            continue;
        };
        let racy = *mtime >= scan_at - 1;
        if meta.len() as i64 == *size && mtime_secs(&meta) == *mtime && !racy {
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

    // New files: creating one bumps its directory's mtime, so only
    // directories touched since the index finished are listed (with the
    // indexer's ignore rules); a never-seen subdirectory is walked whole.
    if scan_at != i64::MAX {
        let mut known_dirs: HashSet<String> = HashSet::from([String::new()]);
        if let Some(dirs) = db.get_meta_str(INDEXED_DIRS_KEY)? {
            known_dirs.extend(dirs.lines().map(str::to_string));
        }
        // Older indexes recorded no directories: derive what indexed files imply.
        for path in indexed.keys() {
            let mut dir = path.as_str();
            while let Some((parent, _)) = dir.rsplit_once('/') {
                if !known_dirs.insert(parent.to_string()) {
                    break;
                }
                dir = parent;
            }
        }
        let mut added: HashSet<String> = HashSet::new();
        for dir in &known_dirs {
            let abs = repo_root.join(dir);
            let touched = std::fs::metadata(&abs)
                .map(|m| mtime_secs(&m) >= scan_at)
                .unwrap_or(false);
            if !touched {
                continue;
            }
            let (files, subdirs) = scan::list_dir(repo_root, &abs, options, true);
            added.extend(files.into_iter().filter(|f| !indexed.contains_key(f)));
            for sub in subdirs.into_iter().filter(|d| !known_dirs.contains(d)) {
                let (files, _) = scan::list_dir(repo_root, &repo_root.join(sub), options, false);
                added.extend(files.into_iter().filter(|f| !indexed.contains_key(f)));
            }
        }
        changed += added.len();
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
