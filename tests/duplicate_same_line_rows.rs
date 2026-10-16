//! Two identical uses on one line stay two rows through every later
//! resolution. A reference that cannot be bound when its file is indexed is
//! stored as a pending row; identical references on one line must not
//! collapse into one, or the edge that appears once the callee does is
//! missing next to a fresh index (which binds both at once). The checks
//! count rows -- `assert_matches_fresh` compares sets and cannot see this.

mod common;

use lidx::indexer::Indexer;

type Files = Vec<(String, String)>;

fn files(list: &[(&str, &str)]) -> Files {
    list.iter()
        .map(|(p, s)| (p.to_string(), s.to_string()))
        .collect()
}

fn write(root: &std::path::Path, list: &Files) {
    let borrowed: Vec<(&str, &str)> = list.iter().map(|(p, s)| (p.as_str(), s.as_str())).collect();
    common::write_files(root, &borrowed);
}

/// `(bound edges, pending references)` of the calls and references in `path`:
/// each bound edge as `kind|target|resolution_kind|confidence`, sorted with
/// duplicates kept, and the number of pending rows.
fn rows(indexer: &Indexer, path: &str) -> (Vec<String>, i64) {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.kind || '|' || COALESCE(t.qualname, '') || '|'
                    || COALESCE(e.resolution_kind, '') || '|'
                    || COALESCE(CAST(ROUND(e.confidence * 1000) AS TEXT), '')
             FROM edges e JOIN files f ON f.id = e.file_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE f.path = ?1 AND e.kind IN ('CALLS', 'REFERENCES')
               AND e.target_symbol_id IS NOT NULL AND e.graph_version = ?2",
        )
        .unwrap();
    let mut bound: Vec<String> = stmt
        .query_map(rusqlite::params![path, gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    bound.sort();
    let pending = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references u JOIN files f ON f.id = u.file_id
             WHERE f.path = ?1 AND u.edge_kind IN ('CALLS', 'REFERENCES') AND u.graph_version = ?2",
            rusqlite::params![path, gv],
            |r| r.get(0),
        )
        .unwrap();
    (bound, pending)
}

/// Index `base`, apply each of `steps` (written, then synced or reindexed),
/// and require the rows of `watched` to equal a fresh index of the final
/// tree, with both uses bound.
fn check(base: &Files, steps: &[Files], watched: &[&str], reindex: bool) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-dup-rows-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    write(&root, base);
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();

    let mut current = base.clone();
    for step in steps {
        write(&root, step);
        for (path, content) in step {
            match current.iter_mut().find(|(p, _)| p == path) {
                Some(entry) => entry.1 = content.clone(),
                None => current.push((path.clone(), content.clone())),
            }
        }
        if reindex {
            indexer.reindex().unwrap();
        } else {
            let paths: Vec<String> = step.iter().map(|(p, _)| p.clone()).collect();
            indexer.sync_rel_paths(&paths).unwrap();
        }
    }

    let fresh_tmp = tempfile::Builder::new()
        .prefix("lidx-dup-rows-fresh-")
        .tempdir()
        .unwrap();
    write(fresh_tmp.path(), &current);
    let mut fresh = Indexer::new(
        fresh_tmp.path().to_path_buf(),
        fresh_tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    fresh.reindex().unwrap();

    for path in watched {
        let (incremental, expected) = (rows(&indexer, path), rows(&fresh, path));
        assert_eq!(
            incremental, expected,
            "{path} (reindex: {reindex}): incremental rows differ from a fresh index"
        );
        assert_eq!(
            expected.0.len(),
            2,
            "{path}: both uses are bound in a fresh index"
        );
    }
}

fn both_ways(base: &Files, steps: &[Files], watched: &[&str]) {
    check(base, steps, watched, false);
    check(base, steps, watched, true);
}

// ---- Python -----------------------------------------------------------

const PY_INIT: &str = "from .a import table\n";
const PY_USE: &str = "from pkg import table as t\n\n\ndef run():\n    return t(1), t(2)\n";
const PY_DEF: &str = "def table(x):\n    return x\n";
const PY_RENAMED: &str = "def table_two(x):\n    return x\n";

fn py_base(a: &str) -> Files {
    files(&[
        ("pkg/__init__.py", PY_INIT),
        ("pkg/a.py", a),
        ("use.py", PY_USE),
    ])
}

#[test]
fn python_callee_added_later() {
    both_ways(
        &py_base("x = 1\n"),
        &[files(&[("pkg/a.py", PY_DEF)])],
        &["use.py"],
    );
}

#[test]
fn python_callee_renamed_away_and_back() {
    both_ways(
        &py_base(PY_DEF),
        &[
            files(&[("pkg/a.py", PY_RENAMED)]),
            files(&[("pkg/a.py", PY_DEF)]),
        ],
        &["use.py"],
    );
}

#[test]
fn python_importer_edited() {
    let edited = format!("{PY_USE}# c\n");
    both_ways(
        &py_base(PY_DEF),
        &[files(&[("use.py", &edited)])],
        &["use.py"],
    );
}

#[test]
fn python_importer_added() {
    both_ways(
        &py_base(PY_DEF),
        &[files(&[("use2.py", PY_USE)])],
        &["use.py", "use2.py"],
    );
}

#[test]
fn python_plain_module_callee_added_later() {
    let use_src = "from lib import f\n\n\ndef run():\n    return f(1), f(2)\n";
    both_ways(
        &files(&[("lib.py", "x = 1\n"), ("use.py", use_src)]),
        &[files(&[("lib.py", "def f(a):\n    return a\n")])],
        &["use.py"],
    );
}

// ---- Go ---------------------------------------------------------------

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const GO_CALLER: &str = "package main\n\nfunc Run() int {\n\treturn helper(1) + helper(2)\n}\n";
const GO_HELPER: &str = "package main\n\nfunc helper(a int) int { return a }\n";
const GO_RENAMED: &str = "package main\n\nfunc helper2(a int) int { return a }\n";

fn go_base(helper: &str) -> Files {
    files(&[
        ("go.mod", GO_MOD),
        ("main.go", GO_CALLER),
        ("helper.go", helper),
    ])
}

#[test]
fn go_callee_added_later() {
    both_ways(
        &go_base("package main\n"),
        &[files(&[("helper.go", GO_HELPER)])],
        &["main.go"],
    );
}

#[test]
fn go_callee_renamed_away_and_back() {
    both_ways(
        &go_base(GO_HELPER),
        &[
            files(&[("helper.go", GO_RENAMED)]),
            files(&[("helper.go", GO_HELPER)]),
        ],
        &["main.go"],
    );
}

// ---- Rust -------------------------------------------------------------

const RS_MANIFEST: &str = "[package]\nname = \"x\"\n";
const RS_LIB: &str = "mod caller;\nmod helper;\n";
const RS_CALLER: &str = "pub fn run() -> i32 {\n    crate::helper::f(1) + crate::helper::f(2)\n}\n";
const RS_HELPER: &str = "pub fn f(a: i32) -> i32 { a }\n";
const RS_RENAMED: &str = "pub fn g(a: i32) -> i32 { a }\n";

fn rs_base(helper: &str) -> Files {
    files(&[
        ("Cargo.toml", RS_MANIFEST),
        ("src/lib.rs", RS_LIB),
        ("src/caller.rs", RS_CALLER),
        ("src/helper.rs", helper),
    ])
}

#[test]
fn rust_callee_added_later() {
    both_ways(
        &rs_base("// nothing yet\n"),
        &[files(&[("src/helper.rs", RS_HELPER)])],
        &["src/caller.rs"],
    );
}

#[test]
fn rust_callee_renamed_away_and_back() {
    both_ways(
        &rs_base(RS_HELPER),
        &[
            files(&[("src/helper.rs", RS_RENAMED)]),
            files(&[("src/helper.rs", RS_HELPER)]),
        ],
        &["src/caller.rs"],
    );
}

// ---- C# ---------------------------------------------------------------

const CS_CALLER: &str =
    "public class Caller { public int Run() { return Helper.F(1) + Helper.F(2); } }\n";
const CS_HELPER: &str = "public static class Helper { public static int F(int a) { return a; } }\n";
const CS_EMPTY: &str = "public static class Helper { }\n";
const CS_RENAMED: &str =
    "public static class Helper { public static int G(int a) { return a; } }\n";

fn cs_base(helper: &str) -> Files {
    files(&[("Caller.cs", CS_CALLER), ("Helper.cs", helper)])
}

#[test]
fn csharp_callee_added_later() {
    both_ways(
        &cs_base(CS_EMPTY),
        &[files(&[("Helper.cs", CS_HELPER)])],
        &["Caller.cs"],
    );
}

#[test]
fn csharp_callee_renamed_away_and_back() {
    both_ways(
        &cs_base(CS_HELPER),
        &[
            files(&[("Helper.cs", CS_RENAMED)]),
            files(&[("Helper.cs", CS_HELPER)]),
        ],
        &["Caller.cs"],
    );
}
