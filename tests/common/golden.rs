//! Golden-corpus correctness scoreboard support (issue #71).
//!
//! A test indexes a fixture (via `Indexer`), takes a normalized snapshot of
//! its current graph version's edges (`snapshot_edges`, backed by the
//! public `Db::edges_snapshot` accessor — no test reaches into SQL or
//! resolver internals directly), and compares it against a fixture's
//! plain-text expected-edges file (`parse_expected_edges` / `compare`).
//!
//! ## Expected-edges file format
//!
//! One edge per line:
//!
//! ```text
//! <source qualname> <EDGE_KIND> <target qualname | UNRESOLVED>
//! ```
//!
//! A line starting with `#` (after trimming) is a full-line comment and is
//! ignored. A line may also end with a trailing `# xfail: <why>` marker —
//! chosen over a leading `!` because it composes with the existing `#`
//! comment syntax instead of adding a second one. A known-failing line:
//!
//! - is excluded from the precision/recall accounting entirely (it is
//!   neither a true positive nor a "wrong" edge while it keeps failing);
//! - is reported every run, via `ScoreboardReport::xfail_still_failing`;
//! - causes `ScoreboardReport::assert_floors` to panic loudly if the graph
//!   now resolves it correctly (`xfail_now_passing`), so the marker gets
//!   removed by hand rather than silently staying stale.
//!
//! Any other trailing `# ...` text is treated as an ordinary comment, not
//! a marker.

use lidx::db::Db;
use lidx::model::EdgeSnapshotRow;
use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// Copy `fixture` (a path under `tests/fixtures/`, may contain `/`) into a
/// fresh temp dir and return `(repo_root, db_path)` — same shape as every
/// other integration test's `setup_repo`.
pub fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let label = fixture.replace('/', "-");
    let repo_root = temp_repo_dir(&label);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

/// A normalized edge key: source qualname, edge kind, and the target's
/// qualname (or `None` for `UNRESOLVED`). Deliberately drops
/// `resolution_kind` — the expected-edges file doesn't encode it, so
/// comparison never depends on it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeKey {
    pub source_qualname: String,
    pub kind: String,
    pub target_qualname: Option<String>,
}

impl std::fmt::Display for EdgeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.source_qualname,
            self.kind,
            self.target_qualname.as_deref().unwrap_or("UNRESOLVED")
        )
    }
}

impl From<&EdgeSnapshotRow> for EdgeKey {
    fn from(row: &EdgeSnapshotRow) -> Self {
        EdgeKey {
            source_qualname: row.source_qualname.clone(),
            kind: row.kind.clone(),
            target_qualname: row.target_qualname.clone(),
        }
    }
}

/// Read `graph_version`'s edges via the public `Db::edges_snapshot`
/// accessor into a sorted, normalized, deduplicated set. The one seam a
/// test needs — no SQL, no resolver internals.
pub fn snapshot_edges(db: &Db, graph_version: i64) -> anyhow::Result<BTreeSet<EdgeKey>> {
    Ok(db
        .edges_snapshot(graph_version)?
        .iter()
        .map(EdgeKey::from)
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedEdge {
    pub key: EdgeKey,
    pub xfail: bool,
}

/// Parse an expected-edges file's text — see this module's doc comment for
/// the format. Panics on a malformed data line (not a comment, but not
/// exactly `<source> <KIND> <target|UNRESOLVED>` either): a fixture typo
/// should fail loudly and immediately, not silently drop a row.
pub fn parse_expected_edges(text: &str) -> Vec<ExpectedEdge> {
    let mut out = Vec::new();
    for (line_no, raw_line) in text.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (fields, marker) = match trimmed.split_once('#') {
            Some((f, m)) => (f.trim(), Some(m.trim())),
            None => (trimmed, None),
        };
        let xfail = marker
            .map(|m| m.to_ascii_lowercase().starts_with("xfail"))
            .unwrap_or(false);
        let tokens: Vec<&str> = fields.split_whitespace().collect();
        assert_eq!(
            tokens.len(),
            3,
            "expected-edges line {} is malformed (want '<source> <KIND> <target|UNRESOLVED>'): {:?}",
            line_no + 1,
            raw_line
        );
        let target = if tokens[2] == "UNRESOLVED" {
            None
        } else {
            Some(tokens[2].to_string())
        };
        out.push(ExpectedEdge {
            key: EdgeKey {
                source_qualname: tokens[0].to_string(),
                kind: tokens[1].to_string(),
                target_qualname: target,
            },
            xfail,
        });
    }
    out
}

/// Precision/recall over an expected-edges file, plus the exact wrong,
/// missed, and known-failing edges — see `compare`.
#[derive(Debug, Clone)]
pub struct ScoreboardReport {
    pub precision: f64,
    pub recall: f64,
    pub true_positive_count: usize,
    /// In-scope snapshot edges, minus any edge a known-failing line
    /// already accounts for (the precision denominator).
    pub scoped_count: usize,
    /// Non-`xfail` expected edges (the recall denominator).
    pub expected_count: usize,
    /// In scope, in the snapshot, not expected (and not a known-failing
    /// line's edge either).
    pub wrong: Vec<EdgeKey>,
    /// Expected (non-`xfail`), not in the snapshot.
    pub missed: Vec<EdgeKey>,
    /// `xfail` lines still absent from the snapshot — reported, not fatal.
    pub xfail_still_failing: Vec<EdgeKey>,
    /// `xfail` lines now present in the snapshot — the marker is stale.
    pub xfail_now_passing: Vec<EdgeKey>,
}

fn format_edges(edges: &[EdgeKey]) -> String {
    if edges.is_empty() {
        return "  (none)".to_string();
    }
    edges
        .iter()
        .map(|e| format!("  {e}"))
        .collect::<Vec<_>>()
        .join("\n")
}

impl ScoreboardReport {
    /// Prints a human-readable summary every run (wrong/missed/xfail
    /// listed by name), then panics if precision or recall drop below the
    /// given floor, or if any known-failing line has started passing.
    pub fn assert_floors(&self, precision_floor: f64, recall_floor: f64) {
        println!(
            "golden edge scoreboard: precision {:.4} ({}/{}), recall {:.4} ({}/{})",
            self.precision,
            self.true_positive_count,
            self.scoped_count,
            self.recall,
            self.true_positive_count,
            self.expected_count
        );
        println!(
            "wrong edges (in snapshot, not expected):\n{}",
            format_edges(&self.wrong)
        );
        println!(
            "missed edges (expected, not in snapshot):\n{}",
            format_edges(&self.missed)
        );
        if !self.xfail_still_failing.is_empty() {
            println!(
                "known-failing (still failing, as expected):\n{}",
                format_edges(&self.xfail_still_failing)
            );
        }

        let mut failures = Vec::new();
        if self.precision < precision_floor {
            failures.push(format!(
                "precision {:.4} dropped below floor {:.4}\nwrong edges:\n{}",
                self.precision,
                precision_floor,
                format_edges(&self.wrong)
            ));
        }
        if self.recall < recall_floor {
            failures.push(format!(
                "recall {:.4} dropped below floor {:.4}\nmissed edges:\n{}",
                self.recall,
                recall_floor,
                format_edges(&self.missed)
            ));
        }
        if !self.xfail_now_passing.is_empty() {
            failures.push(format!(
                "{} known-failing line(s) now resolve correctly -- remove their xfail marker(s):\n{}",
                self.xfail_now_passing.len(),
                format_edges(&self.xfail_now_passing)
            ));
        }
        if !failures.is_empty() {
            panic!(
                "golden edge scoreboard failed:\n\n{}",
                failures.join("\n\n")
            );
        }
    }
}

/// Compare a graph snapshot against an expected-edges file.
///
/// Precision/recall are scoped to the edge kinds and sources the expected
/// file itself declares (e.g. only `CALLS` edges from fixture-defined
/// sources) — an edge kind or source the fixture never mentions (like
/// `CONTAINS`/`IMPORTS`, or a symbol outside the fixture) is out of scope
/// entirely, so it can't dilute the score.
///
/// A known-failing (`xfail`) line's edge is excluded from precision and
/// recall in both directions: whether or not the snapshot has it, it
/// contributes to neither the numerator nor the denominator. It is instead
/// tracked in `xfail_still_failing` / `xfail_now_passing`.
pub fn compare(snapshot: &BTreeSet<EdgeKey>, expected: &[ExpectedEdge]) -> ScoreboardReport {
    let expected_kinds: HashSet<&str> = expected.iter().map(|e| e.key.kind.as_str()).collect();
    let expected_sources: HashSet<&str> = expected
        .iter()
        .map(|e| e.key.source_qualname.as_str())
        .collect();

    let scoped: BTreeSet<EdgeKey> = snapshot
        .iter()
        .filter(|edge| {
            expected_kinds.contains(edge.kind.as_str())
                && expected_sources.contains(edge.source_qualname.as_str())
        })
        .cloned()
        .collect();

    let expected_ok: BTreeSet<EdgeKey> = expected
        .iter()
        .filter(|e| !e.xfail)
        .map(|e| e.key.clone())
        .collect();
    let expected_xfail: BTreeSet<EdgeKey> = expected
        .iter()
        .filter(|e| e.xfail)
        .map(|e| e.key.clone())
        .collect();

    // Snapshot edges a known-failing line already accounts for are tracked
    // separately below, not folded into precision/recall.
    let effective_scoped: BTreeSet<EdgeKey> = scoped.difference(&expected_xfail).cloned().collect();

    let true_positives: Vec<EdgeKey> = effective_scoped
        .intersection(&expected_ok)
        .cloned()
        .collect();
    let wrong: Vec<EdgeKey> = effective_scoped.difference(&expected_ok).cloned().collect();
    let missed: Vec<EdgeKey> = expected_ok.difference(&scoped).cloned().collect();
    let xfail_now_passing: Vec<EdgeKey> = expected_xfail.intersection(&scoped).cloned().collect();
    let xfail_still_failing: Vec<EdgeKey> = expected_xfail.difference(&scoped).cloned().collect();

    let precision = if effective_scoped.is_empty() {
        1.0
    } else {
        true_positives.len() as f64 / effective_scoped.len() as f64
    };
    let recall = if expected_ok.is_empty() {
        1.0
    } else {
        true_positives.len() as f64 / expected_ok.len() as f64
    };

    ScoreboardReport {
        precision,
        recall,
        true_positive_count: true_positives.len(),
        scoped_count: effective_scoped.len(),
        expected_count: expected_ok.len(),
        wrong,
        missed,
        xfail_still_failing,
        xfail_now_passing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(source: &str, kind: &str, target: Option<&str>) -> EdgeKey {
        EdgeKey {
            source_qualname: source.to_string(),
            kind: kind.to_string(),
            target_qualname: target.map(str::to_string),
        }
    }

    #[test]
    fn parses_plain_lines_unresolved_and_comments() {
        let text = "\
# a full-line comment
a.b CALLS a.c
a.d CALLS UNRESOLVED
";
        let edges = parse_expected_edges(text);
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].key, key("a.b", "CALLS", Some("a.c")));
        assert!(!edges[0].xfail);
        assert_eq!(edges[1].key, key("a.d", "CALLS", None));
    }

    #[test]
    fn parses_xfail_marker() {
        let text = "a.b CALLS a.c  # xfail: known gap\n";
        let edges = parse_expected_edges(text);
        assert_eq!(edges.len(), 1);
        assert!(edges[0].xfail);
    }

    #[test]
    fn non_xfail_trailing_comment_is_not_a_marker() {
        let text = "a.b CALLS a.c  # just a note\n";
        let edges = parse_expected_edges(text);
        assert_eq!(edges.len(), 1);
        assert!(!edges[0].xfail);
    }

    #[test]
    #[should_panic(expected = "malformed")]
    fn malformed_line_panics() {
        parse_expected_edges("a.b CALLS\n");
    }

    #[test]
    fn compare_reports_wrong_and_missed_by_name() {
        let expected = parse_expected_edges("a.b CALLS a.c\na.d CALLS a.e\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c"))); // matches
        snapshot.insert(key("a.d", "CALLS", Some("a.WRONG"))); // wrong target

        let report = compare(&snapshot, &expected);

        assert_eq!(report.true_positive_count, 1);
        assert_eq!(report.wrong, vec![key("a.d", "CALLS", Some("a.WRONG"))]);
        assert_eq!(report.missed, vec![key("a.d", "CALLS", Some("a.e"))]);
        assert!((report.precision - 0.5).abs() < 1e-9);
        assert!((report.recall - 0.5).abs() < 1e-9);
    }

    #[test]
    fn out_of_scope_edges_do_not_affect_precision() {
        let expected = parse_expected_edges("a.b CALLS a.c\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c")));
        // Different kind and different source entirely — out of the
        // expected file's declared scope, must not dilute precision.
        snapshot.insert(key("a.b", "CONTAINS", Some("a.c.Thing")));
        snapshot.insert(key("z.unrelated", "CALLS", Some("z.other")));

        let report = compare(&snapshot, &expected);

        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        assert!(report.wrong.is_empty());
        assert!(report.missed.is_empty());
    }

    #[test]
    fn xfail_line_absent_from_snapshot_is_reported_not_failed() {
        let expected = parse_expected_edges("a.b CALLS a.c  # xfail: not implemented\n");
        let snapshot: BTreeSet<EdgeKey> = BTreeSet::new();

        let report = compare(&snapshot, &expected);

        assert_eq!(
            report.xfail_still_failing,
            vec![key("a.b", "CALLS", Some("a.c"))]
        );
        assert!(report.xfail_now_passing.is_empty());
        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        // Must not panic: a still-failing xfail line never fails the test.
        report.assert_floors(1.0, 1.0);
    }

    #[test]
    #[should_panic(expected = "remove their xfail marker")]
    fn xfail_line_now_passing_fails_loudly() {
        let expected = parse_expected_edges("a.b CALLS a.c  # xfail: not implemented\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c")));

        let report = compare(&snapshot, &expected);

        assert_eq!(
            report.xfail_now_passing,
            vec![key("a.b", "CALLS", Some("a.c"))]
        );
        // A known-failing line that now passes must fail the test, even
        // though precision/recall floors alone are satisfied.
        report.assert_floors(1.0, 1.0);
    }
}
