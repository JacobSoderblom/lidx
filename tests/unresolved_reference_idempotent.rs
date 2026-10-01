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

/// A database reindexed several times under the pre-fix code holds duplicate
/// pending rows in its current graph version. After migration and one
/// reindex on the new code it must match a fresh index exactly.
#[test]
fn database_with_old_duplicates_converges_after_one_reindex() {
    let files = [("api.py", API_PY), ("other.py", OTHER_PY)];
    let fresh = fixture(&files);
    let expected_contents = fresh.contents();
    let expected_total = fresh.total_rows();
    let expected_routes = fresh.route_rows();

    let mut old = fixture(&files);
    old.reindex();
    let gv = old.gv();
    {
        // Simulate the old behaviour: no identity index, schema at v26, and
        // two extra copies of every pending row in the current version.
        let conn = old.conn();
        conn.execute_batch(
            "DROP INDEX idx_unresolved_references_identity;
             UPDATE meta SET value = '26' WHERE key = 'schema_version';",
        )
        .unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO unresolved_references
                    (source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason,
                     import_candidates, detail, evidence_snippet, evidence_start_line,
                     evidence_end_line, confidence, commit_sha, graph_version)
                 SELECT source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason,
                        import_candidates, detail, evidence_snippet, evidence_start_line,
                        evidence_end_line, confidence, commit_sha, graph_version
                 FROM unresolved_references WHERE graph_version = ? AND edge_id IS NULL",
                [gv],
            )
            .unwrap();
        }
    }
    assert!(old.total_rows() > expected_total, "duplicates injected");

    // Reopen through the normal path (runs migration 27), then reindex once.
    let mut reopened = Indexer::new(old.root.clone(), old.db_path.clone()).unwrap();
    reopened.reindex().unwrap();
    let conn = old.conn();
    let gv = reopened.graph_version();
    let total: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = ?",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    let routes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references
             WHERE graph_version = ? AND edge_kind = 'ROUTE'",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(total, expected_total);
    assert_eq!(routes, expected_routes);
    old.indexer = reopened;
    assert_eq!(old.contents(), expected_contents);
}
