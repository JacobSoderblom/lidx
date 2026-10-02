//! Issue #202: a Python module's qualname is its importable dotted path
//! (package root detected structurally), so absolute imports resolve in
//! `src/` layouts, namespace packages and multi-package repos.

mod common;

use lidx::indexer::Indexer;
use rusqlite::params;

const PYPROJECT: &str = "[project]\nname = \"mypkg\"\n";

struct Idx {
    _tmp: tempfile::TempDir,
    indexer: Indexer,
    gv: i64,
}

fn index(files: &[(&str, &str)]) -> Idx {
    let (tmp, root, db_path) = common::index_repo("lidx-py-roots-", files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    Idx {
        _tmp: tmp,
        indexer,
        gv,
    }
}

impl Idx {
    fn modules(&self) -> Vec<String> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT qualname FROM symbols WHERE kind = 'module' AND graph_version = ?
                 ORDER BY qualname",
            )
            .unwrap();
        stmt.query_map(params![self.gv], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// `(target qualname, resolution_kind)` of IMPORTS edges from `src`.
    fn imports(&self, src: &str) -> Vec<(String, Option<String>)> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT t.qualname, e.resolution_kind FROM edges e
                 JOIN symbols s ON s.id = e.source_symbol_id
                 JOIN symbols t ON t.id = e.target_symbol_id
                 WHERE e.kind = 'IMPORTS' AND s.qualname = ? AND e.graph_version = ?
                 ORDER BY t.qualname",
            )
            .unwrap();
        stmt.query_map(params![src, self.gv], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn unresolved(&self) -> Vec<String> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare("SELECT edge_kind || ' ' || reference_name FROM unresolved_references")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
}

fn assert_exact(imports: &[(String, Option<String>)], target: &str) {
    let hit = imports
        .iter()
        .find(|(t, _)| t == target)
        .unwrap_or_else(|| panic!("no IMPORTS edge to {target}: {imports:?}"));
    let kind = hit.1.as_deref();
    assert!(
        matches!(kind, Some("exact") | Some("import")),
        "{target} resolved via {kind:?}, expected exact/import"
    );
}

#[test]
fn src_layout_modules_and_imports_resolve() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("src/mypkg/__init__.py", ""),
        (
            "src/mypkg/util.py",
            "LIMIT = 3\n\n\ndef helper():\n    return 1\n",
        ),
        (
            "tests/test_util.py",
            "from mypkg.util import helper, LIMIT\nfrom mypkg import util\n\n\ndef test_it():\n    helper()\n",
        ),
    ]);
    assert_eq!(
        idx.modules(),
        ["mypkg", "mypkg.util", "tests.test_util"].map(String::from)
    );
    let imports = idx.imports("tests.test_util");
    assert_exact(&imports, "mypkg.util.helper");
    assert_exact(&imports, "mypkg.util.LIMIT");
    assert_exact(&imports, "mypkg.util");
    assert!(
        idx.unresolved().is_empty(),
        "unresolved: {:?}",
        idx.unresolved()
    );
}

#[test]
fn flat_layout_modules_use_package_names() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("mypkg/__init__.py", ""),
        ("mypkg/util.py", "def helper():\n    return 1\n"),
        ("tests/test_util.py", "from mypkg.util import helper\n"),
    ]);
    assert_eq!(
        idx.modules(),
        ["mypkg", "mypkg.util", "tests.test_util"].map(String::from)
    );
    assert_exact(&idx.imports("tests.test_util"), "mypkg.util.helper");
}

#[test]
fn nested_package_and_two_packages_cross_import() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("src/a/__init__.py", ""),
        ("src/a/sub/__init__.py", ""),
        ("src/a/sub/mod.py", "def f():\n    return 1\n"),
        ("src/b/__init__.py", ""),
        ("src/b/use.py", "from a.sub.mod import f\n"),
    ]);
    let modules = idx.modules();
    assert!(modules.contains(&"a.sub.mod".to_string()), "{modules:?}");
    assert!(modules.contains(&"b.use".to_string()), "{modules:?}");
    assert_exact(&idx.imports("b.use"), "a.sub.mod.f");
}

#[test]
fn namespace_package_gets_importable_name() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("src/regular/__init__.py", ""),
        ("src/ns/mod.py", "def f():\n    return 1\n"),
        ("src/regular/use.py", "from ns.mod import f\n"),
    ]);
    let modules = idx.modules();
    assert!(modules.contains(&"ns.mod".to_string()), "{modules:?}");
    assert_exact(&idx.imports("regular.use"), "ns.mod.f");
}

#[test]
fn pure_namespace_package_without_any_regular_package() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("src/ns/a.py", "def f():\n    return 1\n"),
        ("tests/test_a.py", "from ns.a import f\n"),
    ]);
    assert_eq!(idx.modules(), ["ns.a", "tests.test_a"].map(String::from));
    assert_exact(&idx.imports("tests.test_a"), "ns.a.f");
}

#[test]
fn declared_package_dir_is_authoritative() {
    let idx = index(&[
        (
            "pyproject.toml",
            "[tool.setuptools.package-dir]\n\"\" = \"lib\"\n",
        ),
        ("lib/real/m.py", "def f():\n    return 1\n"),
        ("other/x.py", "from real.m import f\n"),
    ]);
    assert_eq!(idx.modules(), ["other.x", "real.m"].map(String::from));
    assert_exact(&idx.imports("other.x"), "real.m.f");
}

#[test]
fn setup_cfg_where_is_authoritative() {
    let idx = index(&[
        ("setup.cfg", "[options.packages.find]\nwhere = lib\n"),
        ("lib/real/m.py", "def f():\n    return 1\n"),
    ]);
    assert_eq!(idx.modules(), ["real.m".to_string()]);
}

#[test]
fn subdirectory_project_with_src_layout() {
    let idx = index(&[
        ("pkg/pyproject.toml", PYPROJECT),
        ("pkg/src/mypkg/__init__.py", ""),
        ("pkg/src/mypkg/util.py", "def helper():\n    return 1\n"),
        ("pkg/tests/test_u.py", "from mypkg.util import helper\n"),
    ]);
    assert_eq!(
        idx.modules(),
        ["mypkg", "mypkg.util", "tests.test_u"].map(String::from)
    );
    assert_exact(&idx.imports("tests.test_u"), "mypkg.util.helper");
}

/// `(path, module qualname)` of every Python module symbol -- the part of
/// the index a layout change can leave stale.
fn module_map(root: &std::path::Path) -> Vec<(String, String)> {
    let indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let mut out = indexer.db().python_module_names(gv).unwrap();
    out.sort();
    out
}

fn fresh_modules(files: &[(&str, &str)]) -> Vec<(String, String)> {
    let (_tmp, root, _) = common::index_repo("lidx-py-fresh-", files);
    module_map(&root)
}

const UTIL: &str = "def helper():\n    return 1\n";

#[test]
fn incremental_matches_fresh_when_markers_added_then_removed() {
    for use_sync in [false, true] {
        let tmp = tempfile::Builder::new()
            .prefix("lidx-py-incr-")
            .tempdir()
            .unwrap();
        let root = tmp.path();
        common::write_files(root, &[("src/mypkg/util.py", UTIL)]);
        let mut indexer =
            Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
        indexer.reindex().unwrap();
        assert_eq!(module_map(root)[0].1, "src.mypkg.util");

        let marked = [
            ("pyproject.toml", PYPROJECT),
            ("src/mypkg/__init__.py", ""),
            ("src/mypkg/util.py", UTIL),
        ];
        let marker_paths = [
            "pyproject.toml".to_string(),
            "src/mypkg/__init__.py".to_string(),
        ];
        common::write_files(root, &marked);
        if use_sync {
            indexer.sync_rel_paths(&marker_paths).unwrap();
        } else {
            indexer.reindex().unwrap();
        }
        assert_eq!(
            module_map(root),
            fresh_modules(&marked),
            "add, sync={use_sync}"
        );

        std::fs::remove_file(root.join("pyproject.toml")).unwrap();
        std::fs::remove_file(root.join("src/mypkg/__init__.py")).unwrap();
        if use_sync {
            indexer.sync_rel_paths(&marker_paths).unwrap();
        } else {
            indexer.reindex().unwrap();
        }
        assert_eq!(
            module_map(root),
            fresh_modules(&[("src/mypkg/util.py", UTIL)]),
            "remove, sync={use_sync}"
        );
    }
}

#[test]
fn loose_script_keeps_path_name() {
    let idx = index(&[("scripts/run.py", "def main():\n    return 1\n")]);
    assert_eq!(idx.modules(), ["scripts.run".to_string()]);
}

#[test]
fn src_that_is_a_package_is_not_stripped() {
    let idx = index(&[
        ("pyproject.toml", PYPROJECT),
        ("src/__init__.py", ""),
        ("src/util.py", "def helper():\n    return 1\n"),
    ]);
    assert_eq!(idx.modules(), ["src", "src.util"].map(String::from));
}
