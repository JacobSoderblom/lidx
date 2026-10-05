//! Issue #319: Python method calls whose receiver is itself a call
//! (`make().stage(1).storage()`) must produce CALLS edges, resolved through
//! the return annotations / constructed class of the inner calls.

use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn repo(files: &[(&str, &str)]) -> (Indexer, i64) {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!(
        "lidx-pychain-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    for (path, body) in files {
        let full = dir.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, body).unwrap();
    }
    let db_path: PathBuf = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir, db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    (indexer, gv)
}

/// `(source qualname, resolved target qualname)` of every bound CALLS edge.
fn bound_calls(indexer: &Indexer, gv: i64) -> Vec<(String, String)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ?",
        )
        .unwrap();
    stmt.query_map(params![gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn unresolved_names(indexer: &Indexer, source: &str, gv: i64) -> Vec<String> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT u.reference_name FROM unresolved_references u
             JOIN symbols s ON s.id = u.source_symbol_id
             WHERE u.edge_kind = 'CALLS' AND s.qualname = ? AND u.graph_version = ?",
        )
        .unwrap();
    stmt.query_map(params![source, gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

const BUILDER: &str = "class Builder:
    def stage(self, x):
        return self
    def storage(self):
        return self

def make():
    return Builder()

def use():
    return make().stage(1).storage()
";

#[test]
fn chained_calls_in_one_file_resolve_through_constructed_class() {
    let (indexer, gv) = repo(&[("b.py", BUILDER)]);
    let calls = bound_calls(&indexer, gv);
    for target in ["b.Builder.stage", "b.Builder.storage", "b.make"] {
        assert!(
            calls.contains(&("b.use".to_string(), target.to_string())),
            "b.use must call {target}, got {calls:?}"
        );
    }
}

#[test]
fn chained_calls_resolve_across_files_via_return_annotation() {
    let (indexer, gv) = repo(&[
        ("lib/__init__.py", ""),
        (
            "lib/builder.py",
            "class Builder:
    def stage(self, x) -> \"Builder\":
        return self
    def storage(self) -> \"Builder\":
        return self

def dataproduct(name) -> Builder:
    return make_it(name)
",
        ),
        (
            "app/main.py",
            "from lib.builder import dataproduct

def run():
    return (
        dataproduct(\"x\")
        .stage(1)
        .storage()
    )
",
        ),
    ]);
    let calls = bound_calls(&indexer, gv);
    for target in ["lib.builder.Builder.stage", "lib.builder.Builder.storage"] {
        assert!(
            calls.contains(&("app.main.run".to_string(), target.to_string())),
            "app.main.run must call {target}, got {calls:?}"
        );
    }
}

#[test]
fn chain_with_unknown_receiver_stays_unresolved_but_recorded() {
    let (indexer, gv) = repo(&[(
        "c.py",
        "def use(items):
    return items[0].stage(1).storage()
",
    )]);
    let names = unresolved_names(&indexer, "c.use", gv);
    assert!(
        names.iter().any(|n| n == "stage") && names.iter().any(|n| n == "storage"),
        "unresolvable chain calls must be recorded as unresolved, got {names:?}"
    );
}
