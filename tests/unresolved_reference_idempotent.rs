//! Issue #251: a no-op reindex must leave each graph version's
//! `unresolved_references` rows identical, not append a second copy of every
//! ROUTE reference (carry-forward copies a pending row; the cross-language
//! link pass re-derives it). Counts are always filtered by `graph_version`:
//! the whole table grows across retained versions by design.

mod common;

use lidx::indexer::Indexer;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const API_PY: &str = r#"
def handler():
    url = "/api/users/list"
    other = "/api/orders/{}/items"
    missing_helper()
    missing_helper()
    return url, other


def second():
    return "/api/health/check"
"#;

const OTHER_PY: &str = r#"
def worker():
    ghost_call()
"#;

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    db_path: PathBuf,
    indexer: Indexer,
}

fn fixture(files: &[(&str, &str)]) -> Fixture {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-unresolved-idem-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    Fixture {
        _tmp: tmp,
        root,
        db_path,
        indexer,
    }
}

impl Fixture {
    fn conn(&self) -> Connection {
        Connection::open(&self.db_path).unwrap()
    }

    fn gv(&self) -> i64 {
        self.indexer.graph_version()
    }

    fn total_rows(&self) -> i64 {
        self.conn()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = ?",
                [self.gv()],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn route_rows(&self) -> i64 {
        self.conn()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references
                 WHERE graph_version = ? AND edge_kind = 'ROUTE'",
                [self.gv()],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn distinct_rows(&self) -> i64 {
        self.conn()
            .query_row(
                "SELECT COUNT(*) FROM (SELECT DISTINCT source_symbol_id, edge_kind, reference_name
                 FROM unresolved_references WHERE graph_version = ?)",
                [self.gv()],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn symbol_rows(&self) -> i64 {
        self.conn()
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE graph_version = ?",
                [self.gv()],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// Logical contents of the current version's store, independent of ids.
    fn contents(&self) -> BTreeSet<(String, String, String, Option<i64>, String)> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT COALESCE(s.qualname, ''), ur.edge_kind, ur.reference_name,
                        ur.evidence_start_line, ur.reason
                 FROM unresolved_references ur
                 LEFT JOIN symbols s ON s.id = ur.source_symbol_id
                 WHERE ur.graph_version = ?",
            )
            .unwrap();
        stmt.query_map([self.gv()], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                r.get(3)?,
                r.get(4)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    fn reindex(&mut self) {
        self.indexer.reindex().unwrap();
    }
}

#[test]
fn route_rows_stay_constant_across_three_noop_reindexes() {
    let mut fx = fixture(&[("api.py", API_PY), ("other.py", OTHER_PY)]);
    let route0 = fx.route_rows();
    assert!(route0 >= 3, "fixture must produce ROUTE rows, got {route0}");
    let total0 = fx.total_rows();
    let symbols0 = fx.symbol_rows();
    let contents0 = fx.contents();
    for run in 1..=3 {
        fx.reindex();
        assert_eq!(fx.route_rows(), route0, "ROUTE rows after no-op {run}");
        assert_eq!(fx.total_rows(), total0, "total rows after no-op {run}");
        assert_eq!(fx.symbol_rows(), symbols0, "symbols after no-op {run}");
        assert_eq!(fx.contents(), contents0, "contents after no-op {run}");
    }
}

#[test]
fn ten_noop_reindexes_leave_the_store_identical_to_one() {
    let mut fx = fixture(&[("api.py", API_PY), ("other.py", OTHER_PY)]);
    fx.reindex();
    let after_one = fx.contents();
    let total_one = fx.total_rows();
    for _ in 0..9 {
        fx.reindex();
    }
    assert_eq!(fx.total_rows(), total_one);
    assert_eq!(fx.contents(), after_one);
}

#[test]
fn no_duplicate_rows_within_a_graph_version() {
    let mut fx = fixture(&[("api.py", API_PY), ("other.py", OTHER_PY)]);
    fx.reindex();
    fx.reindex();
    // Rows are keyed by source + kind + name (+ evidence span); the fixture's
    // two `missing_helper()` calls sit on different lines, so compare against
    // the span-aware distinct count instead of the coarse triple.
    let spans: i64 = fx
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM (SELECT DISTINCT source_symbol_id, edge_kind, reference_name,
                    evidence_start_line, evidence_end_line
             FROM unresolved_references WHERE graph_version = ?)",
            [fx.gv()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fx.total_rows(), spans);
    assert!(fx.distinct_rows() <= fx.total_rows());
}

#[test]
fn noop_reindex_wall_time_does_not_grow() {
    let mut fx = fixture(&[("api.py", API_PY), ("other.py", OTHER_PY)]);
    let mut times: Vec<Duration> = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        fx.reindex();
        times.push(start.elapsed());
    }
    let first = times[0];
    let last = *times.last().unwrap();
    assert!(
        last <= first * 4 + Duration::from_secs(2),
        "no-op reindex time grew: {times:?}"
    );
}

#[test]
fn edit_removes_stale_rows_and_keeps_unresolved_once() {
    let mut fx = fixture(&[("api.py", API_PY), ("other.py", OTHER_PY)]);
    fx.reindex();
    assert!(
        fx.contents().iter().any(|c| c.2.ends_with("ghost_call")),
        "ghost_call should be pending: {:?}",
        fx.contents()
    );
    // Drop `ghost_call`, keep `missing_helper` unresolved, remove one route.
    common::write_files(
        &fx.root,
        &[
            ("other.py", "def worker():\n    return 1\n"),
            (
                "api.py",
                "def handler():\n    url = \"/api/users/list\"\n    missing_helper()\n    return url\n",
            ),
        ],
    );
    fx.reindex();
    let contents = fx.contents();
    assert!(
        !contents.iter().any(|c| c.2.ends_with("ghost_call")),
        "stale reference must be removed: {contents:?}"
    );
    assert_eq!(
        contents
            .iter()
            .filter(|c| c.2.ends_with("missing_helper"))
            .count(),
        1,
        "still-unresolved reference appears exactly once"
    );
    assert_eq!(fx.route_rows(), 1, "only the remaining route is stored");
    // And another no-op keeps it that way.
    let total = fx.total_rows();
    fx.reindex();
    assert_eq!(fx.total_rows(), total);
}

#[test]
fn unique_index_rejects_duplicate_insert() {
    let fx = fixture(&[("api.py", API_PY)]);
    let conn = fx.conn();
    conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    let gv = fx.gv();
    let (file_id, src, kind, name, line, end): (i64, Option<i64>, String, String, i64, i64) = conn
        .query_row(
            "SELECT file_id, source_symbol_id, edge_kind, reference_name, evidence_start_line, evidence_end_line
             FROM unresolved_references WHERE graph_version = ? AND edge_kind = 'ROUTE' LIMIT 1",
            [gv],
            |r| Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
    )
        .unwrap();
    let before = fx.total_rows();
    let res = conn.execute(
        "INSERT INTO unresolved_references
            (source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason,
             evidence_start_line, evidence_end_line, graph_version)
         VALUES (?, ?, ?, ?, ?, 'no_match', ?, ?, ?)",
        rusqlite::params![src, file_id, kind, name, name, line, end, gv],
    );
    assert!(res.is_err(), "duplicate insert must be rejected");
    assert_eq!(fx.total_rows(), before);
}
