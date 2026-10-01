//! Issue #248: a bare call inside a Python method resolves along Python's
//! own scope chain (enclosing function, module globals, builtins) -- the
//! class body is skipped. A same-file module-level match must win over a
//! same-named function in another file.

use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    indexer: Indexer,
    gv: i64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn index(files: &[(&str, &str)]) -> Fixture {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!(
        "lidx-py-method-bare-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    for (name, src) in files {
        std::fs::write(dir.join(name), src).unwrap();
    }
    let mut indexer = Indexer::new(dir.clone(), dir.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    Fixture { dir, indexer, gv }
}

impl Fixture {
    /// (target qualname, resolution_kind) of resolved CALLS edges out of `caller`.
    fn targets(&self, caller: &str) -> Vec<(String, Option<String>)> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT t.qualname, e.resolution_kind FROM edges e
                 JOIN symbols s ON s.id = e.source_symbol_id
                 JOIN symbols t ON t.id = e.target_symbol_id
                 WHERE e.kind = 'CALLS' AND s.qualname = ? AND e.graph_version = ?
                   AND t.kind != 'external'",
            )
            .unwrap();
        stmt.query_map(params![caller, self.gv], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn target_names(&self, caller: &str) -> Vec<String> {
        self.targets(caller).into_iter().map(|t| t.0).collect()
    }

    fn unresolved(&self, caller: &str) -> Vec<String> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT ur.reason FROM unresolved_references ur
                 JOIN symbols s ON s.id = ur.source_symbol_id
                 WHERE s.qualname = ? AND ur.edge_kind = 'CALLS'",
            )
            .unwrap();
        stmt.query_map(params![caller], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
}

const OTHER: &str =
    "def _step(step_id):\n    return step_id\n\ndef test_module_level_caller():\n    _step('s1')\n";

/// Runs `check` with the fixture file scanned first, then last.
fn both_orders(main: &str, check: impl Fn(&Fixture)) {
    check(&index(&[("a_main.py", main), ("b_other.py", OTHER)]));
    check(&index(&[("a_other.py", OTHER), ("z_main.py", main)]));
}

fn main_mod(f: &Fixture) -> String {
    let conn = f.indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT qualname FROM symbols WHERE name = 'test_in_method'",
        [],
        |r| r.get::<_, String>(0),
    )
    .map(|q| q.trim_end_matches(".TestCancel.test_in_method").to_string())
    .unwrap()
}

const MAIN: &str = "def _step(step_id):
    return step_id

class TestCancel:
    def test_in_method(self):
        return _step('s1')
";

#[test]
fn method_bare_call_binds_same_file_module_function_exactly() {
    both_orders(MAIN, |f| {
        let m = main_mod(f);
        let caller = format!("{m}.TestCancel.test_in_method");
        assert_eq!(
            f.targets(&caller),
            vec![(format!("{m}._step"), Some("exact".to_string()))]
        );
        assert!(
            f.unresolved(&caller).is_empty(),
            "{:?}",
            f.unresolved(&caller)
        );
        // The module-scope caller in the other file is unchanged.
        let other = f
            .indexer
            .db()
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT qualname FROM symbols WHERE name = 'test_module_level_caller'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        let t = f.target_names(&other);
        assert_eq!(t.len(), 1);
        assert!(t[0].ends_with("other._step"), "{t:?}");
    });
}

#[test]
fn self_and_cls_calls_still_bind_class_members() {
    let src = "def helper():\n    pass\n\nclass C:\n    def a(self):\n        self.b()\n    @classmethod\n    def c(cls):\n        cls.b()\n    def b(self):\n        helper()\n";
    both_orders(src, |f| {
        let conn = f.indexer.db().read_conn().unwrap();
        let m: String = conn
            .query_row(
                "SELECT qualname FROM symbols WHERE name = 'helper'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let m = m.trim_end_matches(".helper").to_string();
        assert_eq!(
            f.target_names(&format!("{m}.C.a")),
            vec![format!("{m}.C.b")]
        );
        assert_eq!(
            f.target_names(&format!("{m}.C.c")),
            vec![format!("{m}.C.b")]
        );
        assert_eq!(
            f.target_names(&format!("{m}.C.b")),
            vec![format!("{m}.helper")]
        );
    });
}

#[test]
fn closure_beats_module_level_and_builtin_and_unknown_stay_unbound() {
    let src = "def _step():\n    pass\n\nclass C:\n    def m(self):\n        def _step():\n            pass\n        _step()\n        len([])\n        nothing_here()\n";
    both_orders(src, |f| {
        let conn = f.indexer.db().read_conn().unwrap();
        let m: String = conn
            .query_row("SELECT qualname FROM symbols WHERE name = 'm'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let t = f.target_names(&m);
        assert!(
            !t.iter()
                .any(|q| q.ends_with("._step") && !q.contains("other")),
            "closure call must not bind module-level _step: {t:?}"
        );
        assert!(
            !t.iter()
                .any(|q| q.ends_with("nothing_here") || q.ends_with("len")),
            "{t:?}"
        );
    });
}

#[test]
fn nested_class_method_bare_call_binds_module_scope() {
    let src = "def _step():\n    pass\n\nclass Outer:\n    class Inner:\n        def m(self):\n            _step()\n";
    both_orders(src, |f| {
        let conn = f.indexer.db().read_conn().unwrap();
        let s: String = conn
            .query_row(
                "SELECT qualname FROM symbols WHERE name = '_step' AND qualname NOT LIKE '%other%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let m = s.trim_end_matches("._step").to_string();
        assert_eq!(
            f.targets(&format!("{m}.Outer.Inner.m")),
            vec![(s, Some("exact".to_string()))]
        );
    });
}
