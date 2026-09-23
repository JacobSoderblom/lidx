//! Golden-corpus correctness scoreboard support (issue #71).
//!
//! A test indexes a fixture (via `Indexer`), takes a normalized snapshot of
//! its current graph version's edges (`snapshot_edges`, backed by the
//! public `Db::edges_snapshot` accessor — no test reaches into SQL or
//! resolver internals directly), and compares it against a fixture's
//! plain-text expected-edges file (`parse_expected_edges` / `compare`).
//!
//! `setup_repo`/`copy_dir` (materializing a fixture into a temp dir) live
//! in `tests/common/mod.rs`, shared with any future golden-corpus fixture;
//! this module only holds the snapshot/compare machinery.
//!
//! ## Expected-edges file format
//!
//! One edge per line:
//!
//! ```text
//! <source qualname> <EDGE_KIND> <target qualname | UNRESOLVED> [<resolution kind>]
//! ```
//!
//! The trailing `<resolution kind>` column is optional. When present, it
//! must match the exact tier that resolved the edge (`exact`, `import`,
//! `receiver_type`, `inherited`, `two_segment`, or `bare_name` -- see
//! `resolve_fuzzy_target` in `src/db/mod.rs`), not merely the target
//! qualname: a call that lands on the right symbol for the wrong reason
//! (e.g. a `receiver_type` bind silently degrading to `bare_name` because a
//! same-named decoy no longer disambiguates it) still fails the line. When
//! absent, the line is satisfied by target qualname alone, regardless of
//! which tier produced it. Put the column on lines where a specific tier is
//! the point of the fixture shape (e.g. `receiver_type`, `inherited`);
//! leave it off lines where any resolution route is fine.
//!
//! A line starting with `#` (after trimming) is a full-line comment and is
//! ignored. A line may also end with a trailing `# xfail: <why>` marker —
//! chosen over a leading `!` because it composes with the existing `#`
//! comment syntax instead of adding a second one. A known-failing line:
//!
//! - is excluded from the precision/recall accounting entirely, along with
//!   whatever edge(s) that call site actually produces instead (matched by
//!   source + edge kind, not just the exact xfail target) -- neither counts
//!   as a true positive nor as a "wrong" edge while the line keeps failing;
//! - is reported every run, via `ScoreboardReport::xfail_still_failing`,
//!   alongside what the call site actually produced instead (if anything);
//! - causes `ScoreboardReport::assert_floors` to panic loudly if the graph
//!   now resolves it correctly (`xfail_now_passing`), so the marker gets
//!   removed by hand rather than silently staying stale.
//!
//! Any other trailing `# ...` text is treated as an ordinary comment, not
//! a marker.
//!
//! ## Scope
//!
//! Precision/recall are scoped to edges whose kind the expected file
//! declares (e.g. only `CALLS`) *and* whose source symbol is defined in one
//! of the fixture's own source files (see `fixture_source_modules`) -- not
//! just symbols the expected file happens to mention by name. A stray edge
//! from any fixture-defined symbol with no expected line for it counts as
//! "wrong"; a symbol the fixture never defines (an external library, or a
//! decoy some *other* fixture provides) is out of scope entirely.

use lidx::db::Db;
use lidx::model::EdgeSnapshotRow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// Qualname roots for every Python, TypeScript or Go source file under a
/// fixture directory -- the "fixture's own symbols" scope `compare` filters
/// edge sources against. A root is the stem of the file's first path
/// component: the module for a top-level file (`caller.py` -> `caller`),
/// the package directory for a nested one (`helper/helper.go` ->
/// `helper`). Rust and C# qualnames are rooted in `crate` and the
/// namespace instead, so those fixtures pass their roots directly (see
/// `tests/golden_languages.rs`).
pub fn fixture_source_modules(fixture: &str) -> HashSet<String> {
    fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut HashSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, root, out);
                continue;
            }
            let ext = path.extension().and_then(|e| e.to_str());
            if !matches!(ext, Some("py" | "ts" | "go")) {
                continue;
            }
            let first = path
                .strip_prefix(root)
                .unwrap()
                .components()
                .next()
                .unwrap();
            let stem = std::path::Path::new(first.as_os_str()).file_stem().unwrap();
            out.insert(stem.to_string_lossy().into_owned());
        }
    }
    let root = fixture_path(fixture);
    let mut modules = HashSet::new();
    walk(&root, &root, &mut modules);
    modules
}

/// True when `source_qualname` names a symbol defined in one of
/// `modules` (a module itself, or `module` followed by a qualname
/// separator: `.` for Python/C#/TS, `::` for Rust, `/` for Go paths).
fn is_fixture_source(source_qualname: &str, modules: &HashSet<String>) -> bool {
    modules.iter().any(|module| {
        source_qualname == module.as_str()
            || source_qualname
                .strip_prefix(module.as_str())
                .is_some_and(|rest| rest.starts_with(['.', '/']) || rest.starts_with("::"))
    })
}

/// A normalized edge key: source qualname, edge kind, the target's
/// qualname (or `None` for `UNRESOLVED`), and the tier that resolved it
/// (or `None` for `UNRESOLVED`, or when an expected line doesn't name one).
///
/// `resolution_kind` is *not* part of existence matching (see `base` in
/// this module) -- two `EdgeKey`s with the same source/kind/target but
/// different `resolution_kind` still refer to "the same edge" for
/// wrong/missed/xfail purposes. It's carried here so `compare` can run its
/// separate, opt-in tier check for expected lines that name one, and so
/// every printed edge shows which tier actually produced it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeKey {
    pub source_qualname: String,
    pub kind: String,
    pub target_qualname: Option<String>,
    pub resolution_kind: Option<String>,
}

impl std::fmt::Display for EdgeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.source_qualname,
            self.kind,
            self.target_qualname.as_deref().unwrap_or("UNRESOLVED")
        )?;
        if let Some(kind) = self.resolution_kind.as_deref() {
            write!(f, " [{kind}]")?;
        }
        Ok(())
    }
}

impl From<&EdgeSnapshotRow> for EdgeKey {
    fn from(row: &EdgeSnapshotRow) -> Self {
        EdgeKey {
            source_qualname: row.source_qualname.clone(),
            kind: row.kind.clone(),
            target_qualname: row.target_qualname.clone(),
            resolution_kind: row.resolution_kind.clone(),
        }
    }
}

/// `(source, edge kind, target)`, ignoring `resolution_kind` -- the
/// granularity every existence check (wrong/missed/xfail) matches on. See
/// this module's doc comment and `EdgeKey::resolution_kind`.
fn base(key: &EdgeKey) -> (&str, &str, Option<&str>) {
    (
        key.source_qualname.as_str(),
        key.kind.as_str(),
        key.target_qualname.as_deref(),
    )
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
/// exactly `<source> <KIND> <target|UNRESOLVED> [<resolution kind>]`
/// either): a fixture typo should fail loudly and immediately, not
/// silently drop a row.
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
        assert!(
            tokens.len() == 3 || tokens.len() == 4,
            "expected-edges line {} is malformed (want '<source> <KIND> <target|UNRESOLVED> [<resolution kind>]'): {:?}",
            line_no + 1,
            raw_line
        );
        let target = if tokens[2] == "UNRESOLVED" {
            None
        } else {
            Some(tokens[2].to_string())
        };
        let resolution_kind = tokens.get(3).map(|s| s.to_string());
        out.push(ExpectedEdge {
            key: EdgeKey {
                source_qualname: tokens[0].to_string(),
                kind: tokens[1].to_string(),
                target_qualname: target,
                resolution_kind,
            },
            xfail,
        });
    }
    out
}

/// One `xfail` line's current status: the expected (known-failing) edge,
/// and whatever the call site actually produced instead (same source +
/// edge kind, but not the same target) -- empty when the call site
/// currently produces nothing in scope at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XfailStatus {
    pub expected: EdgeKey,
    pub actual: Vec<EdgeKey>,
}

impl std::fmt::Display for XfailStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "known-failing: want {}", self.expected)?;
        if self.actual.is_empty() {
            write!(f, " -- got nothing")
        } else {
            let got = self
                .actual
                .iter()
                .map(|e| e.target_qualname.as_deref().unwrap_or("UNRESOLVED"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, " -- got {got}")
        }
    }
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
    /// In scope, in the snapshot, not satisfying any expected (non-`xfail`)
    /// line (and not a known-failing line's edge either) -- either an
    /// unexpected edge outright, or one whose target matched but whose
    /// resolution tier didn't match what the line named.
    pub wrong: Vec<EdgeKey>,
    /// Expected (non-`xfail`), not satisfied by the snapshot -- either
    /// wholly absent, or present with the wrong resolution tier.
    pub missed: Vec<EdgeKey>,
    /// `xfail` lines still absent from the snapshot — reported, not fatal.
    pub xfail_still_failing: Vec<XfailStatus>,
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

fn format_xfail(statuses: &[XfailStatus]) -> String {
    if statuses.is_empty() {
        return "  (none)".to_string();
    }
    statuses
        .iter()
        .map(|s| format!("  {s}"))
        .collect::<Vec<_>>()
        .join("\n")
}

impl ScoreboardReport {
    /// Prints a human-readable summary every run (wrong/missed/xfail
    /// listed by name), then panics if precision or recall drop below the
    /// given floor, or if any known-failing line has started passing. The
    /// panic message doesn't re-list wrong/missed/xfail edges -- they're
    /// already in the run's output above, printed exactly once.
    pub fn assert_floors(&self, language: &str, precision_floor: f64, recall_floor: f64) {
        println!(
            "golden edge scoreboard [{language}]: precision {:.4} ({}/{}), recall {:.4} ({}/{}), known-failing {}",
            self.precision,
            self.true_positive_count,
            self.scoped_count,
            self.recall,
            self.true_positive_count,
            self.expected_count,
            self.xfail_still_failing.len()
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
                format_xfail(&self.xfail_still_failing)
            );
        }

        let mut failures = Vec::new();
        if self.precision < precision_floor {
            failures.push(format!(
                "precision {:.4} dropped below floor {:.4} -- see wrong edges above",
                self.precision, precision_floor
            ));
        }
        if self.recall < recall_floor {
            failures.push(format!(
                "recall {:.4} dropped below floor {:.4} -- see missed edges above",
                self.recall, recall_floor
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
/// Precision/recall are scoped to the edge kinds the expected file itself
/// declares (e.g. only `CALLS`), restricted further to edges whose source
/// symbol is defined in `fixture_modules` (see `fixture_source_modules`) --
/// every symbol the fixture itself defines, not just the ones the expected
/// file happens to name. An edge kind the fixture never mentions (like
/// `CONTAINS`/`IMPORTS`), or a source symbol outside the fixture entirely
/// (an external library), is out of scope and can't dilute the score.
///
/// A known-failing (`xfail`) line's edge is excluded from precision and
/// recall in both directions: whether or not the snapshot has it, it
/// contributes to neither the numerator nor the denominator -- and neither
/// does whatever else that same source+kind call site actually produced
/// instead. It is instead tracked in `xfail_still_failing` /
/// `xfail_now_passing`.
///
/// An expected line that names a `resolution_kind` (the optional 4th
/// column) additionally requires the snapshot edge to have been resolved
/// through that exact tier -- a target-qualname match through a *different*
/// tier than the one the line names still counts as missed (recall) and
/// wrong (precision), surfaced as a resolution-kind mismatch.
pub fn compare(
    snapshot: &BTreeSet<EdgeKey>,
    expected: &[ExpectedEdge],
    fixture_modules: &HashSet<String>,
) -> ScoreboardReport {
    let expected_kinds: HashSet<&str> = expected.iter().map(|e| e.key.kind.as_str()).collect();

    let scoped: BTreeSet<EdgeKey> = snapshot
        .iter()
        .filter(|edge| {
            expected_kinds.contains(edge.kind.as_str())
                && is_fixture_source(&edge.source_qualname, fixture_modules)
        })
        .cloned()
        .collect();

    let expected_ok: Vec<&ExpectedEdge> = expected.iter().filter(|e| !e.xfail).collect();
    let expected_xfail: Vec<&ExpectedEdge> = expected.iter().filter(|e| e.xfail).collect();

    let xfail_source_kind: HashSet<(&str, &str)> = expected_xfail
        .iter()
        .map(|e| (e.key.source_qualname.as_str(), e.key.kind.as_str()))
        .collect();

    // A known-failing call site's actual output -- right or wrong -- sits
    // out of precision/recall entirely; it's tracked in xfail_* below
    // instead of folded into wrong/missed.
    let effective_scoped: BTreeSet<EdgeKey> = scoped
        .iter()
        .filter(|edge| {
            !xfail_source_kind.contains(&(edge.source_qualname.as_str(), edge.kind.as_str()))
        })
        .cloned()
        .collect();

    // Every in-scope, non-xfail-excluded snapshot edge, indexed by its
    // existence-matching base -- usually one edge per base, but a fixture
    // could in principle have more than one call site sharing a target.
    let mut actual_by_base: HashMap<(&str, &str, Option<&str>), Vec<&EdgeKey>> = HashMap::new();
    for edge in &effective_scoped {
        actual_by_base.entry(base(edge)).or_default().push(edge);
    }

    let mut true_positives: Vec<EdgeKey> = Vec::new();
    let mut missed: Vec<EdgeKey> = Vec::new();
    let mut wrong: Vec<EdgeKey> = Vec::new();
    let mut matched_bases: HashSet<(&str, &str, Option<&str>)> = HashSet::new();

    for expected_edge in &expected_ok {
        let b = base(&expected_edge.key);
        let Some(actual) = actual_by_base.get(&b) else {
            missed.push(expected_edge.key.clone());
            continue;
        };
        matched_bases.insert(b);
        match expected_edge.key.resolution_kind.as_deref() {
            None => true_positives.push(expected_edge.key.clone()),
            Some(want) => {
                if actual
                    .iter()
                    .any(|e| e.resolution_kind.as_deref() == Some(want))
                {
                    true_positives.push(expected_edge.key.clone());
                } else {
                    missed.push(expected_edge.key.clone());
                    wrong.extend(actual.iter().map(|e| (*e).clone()));
                }
            }
        }
    }

    // Any remaining in-scope edge whose base was never matched to an
    // expected line at all is a plain unexpected edge.
    wrong.extend(
        effective_scoped
            .iter()
            .filter(|edge| !matched_bases.contains(&base(edge)))
            .cloned(),
    );
    wrong.sort();
    wrong.dedup();

    let xfail_still_failing: Vec<XfailStatus> = expected_xfail
        .iter()
        .filter(|e| !scoped.iter().any(|edge| base(edge) == base(&e.key)))
        .map(|e| {
            let actual: Vec<EdgeKey> = scoped
                .iter()
                .filter(|edge| {
                    edge.source_qualname == e.key.source_qualname && edge.kind == e.key.kind
                })
                .cloned()
                .collect();
            XfailStatus {
                expected: e.key.clone(),
                actual,
            }
        })
        .collect();
    let xfail_now_passing: Vec<EdgeKey> = expected_xfail
        .iter()
        .filter(|e| scoped.iter().any(|edge| base(edge) == base(&e.key)))
        .map(|e| e.key.clone())
        .collect();

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

    fn modules(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn key(source: &str, kind: &str, target: Option<&str>) -> EdgeKey {
        EdgeKey {
            source_qualname: source.to_string(),
            kind: kind.to_string(),
            target_qualname: target.map(str::to_string),
            resolution_kind: None,
        }
    }

    fn key_with_kind(source: &str, kind: &str, target: Option<&str>, resolution: &str) -> EdgeKey {
        EdgeKey {
            resolution_kind: Some(resolution.to_string()),
            ..key(source, kind, target)
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
    fn parses_optional_resolution_kind_column() {
        let text = "a.b CALLS a.c receiver_type\n";
        let edges = parse_expected_edges(text);
        assert_eq!(edges.len(), 1);
        assert_eq!(
            edges[0].key.resolution_kind.as_deref(),
            Some("receiver_type")
        );
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

        let report = compare(&snapshot, &expected, &modules(&["a"]));

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

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        assert!(report.wrong.is_empty());
        assert!(report.missed.is_empty());
    }

    #[test]
    fn source_outside_fixture_modules_is_out_of_scope_even_if_named_in_expected() {
        // A symbol the fixture doesn't define at all (e.g. belongs to a
        // different fixture, or is purely hypothetical) never enters
        // scope, even though it shares a module prefix with an expected
        // source -- only `fixture_modules` decides scope now, not the
        // expected file's own source list.
        let expected = parse_expected_edges("a.b CALLS a.c\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c")));
        snapshot.insert(key("q.other", "CALLS", Some("q.thing")));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.precision, 1.0);
        assert!(report.wrong.is_empty());
    }

    #[test]
    fn fixture_symbol_with_no_expected_line_counts_as_wrong() {
        // The whole point of comment 5: scope is "every symbol the
        // fixture defines", not "every source the expected file
        // mentions". A spurious edge from an in-fixture symbol that the
        // expected file never talks about must still count.
        let expected = parse_expected_edges("a.b CALLS a.c\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c")));
        snapshot.insert(key("a.spurious", "CALLS", Some("a.other")));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(
            report.wrong,
            vec![key("a.spurious", "CALLS", Some("a.other"))]
        );
        assert!((report.precision - 0.5).abs() < 1e-9);
    }

    #[test]
    fn fixture_scope_accepts_rust_and_go_separators() {
        let scope = modules(&["crate", "caller"]);
        assert!(is_fixture_source("crate::caller::entry", &scope));
        assert!(is_fixture_source("caller/caller.Entry", &scope));
        assert!(!is_fixture_source("crater::x", &scope));
        assert!(!is_fixture_source("crate:x", &scope));
    }

    #[test]
    fn resolution_kind_is_ignored_when_expected_line_does_not_name_one() {
        let expected = parse_expected_edges("a.b CALLS a.c\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key_with_kind("a.b", "CALLS", Some("a.c"), "bare_name"));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
    }

    #[test]
    fn resolution_kind_mismatch_is_missed_and_wrong() {
        let expected = parse_expected_edges("a.b CALLS a.c receiver_type\n");
        let mut snapshot = BTreeSet::new();
        // Right target, but resolved through a weaker tier than the line
        // names -- e.g. `receiver_type` silently degrading to
        // `bare_name`. Must not be counted as a true positive.
        snapshot.insert(key_with_kind("a.b", "CALLS", Some("a.c"), "bare_name"));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.true_positive_count, 0);
        assert_eq!(report.precision, 0.0);
        assert_eq!(report.recall, 0.0);
        assert_eq!(
            report.missed,
            vec![key_with_kind("a.b", "CALLS", Some("a.c"), "receiver_type")]
        );
        assert_eq!(
            report.wrong,
            vec![key_with_kind("a.b", "CALLS", Some("a.c"), "bare_name")]
        );
    }

    #[test]
    fn resolution_kind_match_is_a_true_positive() {
        let expected = parse_expected_edges("a.b CALLS a.c receiver_type\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key_with_kind("a.b", "CALLS", Some("a.c"), "receiver_type"));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.true_positive_count, 1);
        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        assert!(report.wrong.is_empty());
        assert!(report.missed.is_empty());
    }

    #[test]
    fn xfail_line_absent_from_snapshot_is_reported_not_failed() {
        let expected = parse_expected_edges("a.b CALLS a.c  # xfail: not implemented\n");
        let snapshot: BTreeSet<EdgeKey> = BTreeSet::new();

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(report.xfail_still_failing.len(), 1);
        assert_eq!(
            report.xfail_still_failing[0].expected,
            key("a.b", "CALLS", Some("a.c"))
        );
        assert!(report.xfail_still_failing[0].actual.is_empty());
        assert!(report.xfail_now_passing.is_empty());
        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        // Must not panic: a still-failing xfail line never fails the test.
        report.assert_floors("test", 1.0, 1.0);
    }

    #[test]
    fn xfail_line_with_competing_emitted_edge_does_not_fail_or_lower_precision() {
        // The reviewer's repro: the call site behind an xfail line still
        // emits *some* edge (right or wrong) -- that edge must not be
        // counted as "wrong" just because it shares the xfail key's
        // source and kind but not its exact (stale/aspirational) target.
        let expected = parse_expected_edges(
            "caller.call_ambiguous CALLS ambiguous_a.run  # xfail: want import-aware\n",
        );
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("caller.call_ambiguous", "CALLS", None)); // UNRESOLVED

        let report = compare(&snapshot, &expected, &modules(&["caller", "ambiguous_a"]));

        assert!(report.wrong.is_empty());
        assert_eq!(report.precision, 1.0);
        assert_eq!(report.recall, 1.0);
        assert_eq!(report.xfail_still_failing.len(), 1);
        assert_eq!(
            report.xfail_still_failing[0].actual,
            vec![key("caller.call_ambiguous", "CALLS", None)]
        );
        report.assert_floors("test", 1.0, 1.0);
    }

    #[test]
    #[should_panic(expected = "remove their xfail marker")]
    fn xfail_line_now_passing_fails_loudly() {
        let expected = parse_expected_edges("a.b CALLS a.c  # xfail: not implemented\n");
        let mut snapshot = BTreeSet::new();
        snapshot.insert(key("a.b", "CALLS", Some("a.c")));

        let report = compare(&snapshot, &expected, &modules(&["a"]));

        assert_eq!(
            report.xfail_now_passing,
            vec![key("a.b", "CALLS", Some("a.c"))]
        );
        // A known-failing line that now passes must fail the test, even
        // though precision/recall floors alone are satisfied.
        report.assert_floors("test", 1.0, 1.0);
    }
}
