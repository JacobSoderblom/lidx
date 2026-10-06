//! Issue #248: a bare call inside a Python method resolves along Python's
//! own scope chain (enclosing function, module globals, builtins) -- the
//! class body is skipped. A same-file module-level match must win over a
//! same-named function in another file.

mod common;

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
    common::write_files(&dir, files);
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

    /// Qualname of the one symbol named `name` whose qualname does not
    /// contain `exclude` (used to skip the `other` file's same-named twin).
    fn qualname_of(&self, name: &str, exclude: &str) -> String {
        let conn = self.indexer.db().read_conn().unwrap();
        conn.query_row(
            "SELECT qualname FROM symbols WHERE name = ? AND qualname NOT LIKE ?",
            params![name, format!("%{exclude}%")],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// (name_tail, reason) of the unresolved CALLS rows out of `caller`,
    /// sorted.
    fn unresolved_rows(&self, caller: &str) -> Vec<(String, String)> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT ur.name_tail, ur.reason FROM unresolved_references ur
                 JOIN symbols s ON s.id = ur.source_symbol_id
                 WHERE s.qualname = ? AND ur.edge_kind = 'CALLS'
                 ORDER BY ur.name_tail",
            )
            .unwrap();
        stmt.query_map(params![caller], |r| Ok((r.get(0)?, r.get(1)?)))
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

/// Module qualname of the fixture file, derived from `anchor`, a symbol
/// whose qualname is `<module>.<suffix>` and which exists once outside the
/// `other` file.
fn module_of(f: &Fixture, anchor: &str, suffix: &str) -> String {
    f.qualname_of(anchor, "other")
        .strip_suffix(suffix)
        .unwrap()
        .to_string()
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
        let m = module_of(f, "test_in_method", ".TestCancel.test_in_method");
        let caller = format!("{m}.TestCancel.test_in_method");
        assert_eq!(
            f.targets(&caller),
            vec![(format!("{m}._step"), Some("exact".to_string()))]
        );
        assert!(f.unresolved_rows(&caller).is_empty());
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
        let m = module_of(f, "helper", ".helper");
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
fn closure_call_does_not_bind_module_level_function() {
    let src = "def _step():\n    pass\n\nclass C:\n    def m(self):\n        def _step():\n            pass\n        _step()\n";
    both_orders(src, |f| {
        let m = module_of(f, "m", ".C.m");
        let caller = format!("{m}.C.m");
        assert_eq!(f.target_names(&caller), Vec::<String>::new());
        assert_eq!(
            f.unresolved_rows(&caller),
            vec![("_step".to_string(), "external".to_string())]
        );
    });
}

#[test]
fn parameter_shadowing_module_function_does_not_bind() {
    let src = "def _step():\n    pass\n\nclass C:\n    def m(self, _step):\n        _step()\n";
    both_orders(src, |f| {
        let m = module_of(f, "m", ".C.m");
        let caller = format!("{m}.C.m");
        assert_eq!(f.target_names(&caller), Vec::<String>::new());
        assert_eq!(
            f.unresolved_rows(&caller),
            vec![("_step".to_string(), "external".to_string())]
        );
    });
}

#[test]
fn builtin_bare_call_stays_unresolved() {
    let src = "class C:\n    def m(self):\n        len([])\n";
    both_orders(src, |f| {
        let m = module_of(f, "m", ".C.m");
        let caller = format!("{m}.C.m");
        assert_eq!(f.target_names(&caller), Vec::<String>::new());
        assert_eq!(
            f.unresolved_rows(&caller),
            // A builtin is provably outside the repo: `external`, not a
            // lookup that found nothing.
            vec![("len".to_string(), "external".to_string())]
        );
    });
}

#[test]
fn unknown_bare_call_stays_unresolved() {
    let src = "class C:\n    def m(self):\n        nothing_here()\n";
    both_orders(src, |f| {
        let m = module_of(f, "m", ".C.m");
        let caller = format!("{m}.C.m");
        assert_eq!(f.target_names(&caller), Vec::<String>::new());
        assert_eq!(
            f.unresolved_rows(&caller),
            vec![("nothing_here".to_string(), "no_candidates".to_string())]
        );
    });
}

#[test]
fn nested_class_method_bare_call_binds_module_scope() {
    let src = "def _step():\n    pass\n\nclass Outer:\n    class Inner:\n        def m(self):\n            _step()\n";
    both_orders(src, |f| {
        let s = f.qualname_of("_step", "other");
        let m = s.trim_end_matches("._step").to_string();
        assert_eq!(
            f.targets(&format!("{m}.Outer.Inner.m")),
            vec![(s, Some("exact".to_string()))]
        );
    });
}
