//! Issue #256: after an incremental reindex that renames a C# extension
//! method, its cross-file callers must not keep CALLS edges to phantom
//! `ext:` stubs; edges and unresolved references must equal a fresh index.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::PathBuf;

const EXTENSIONS: &str = "namespace Dpb.Common.Database;\n\
public static class Extensions\n{\n    public static void AddDatabase(this object services) { }\n}\n";
const EXTENSIONS_RENAMED: &str = "namespace Dpb.Common.Database;\n\
public static class Extensions\n{\n    public static void AddDatabaseRenamed(this object services) { }\n}\n";
const MGR: &str = "using Dpb.Common.Database;\nnamespace Dpb.DataMgr;\n\
public class Program\n{\n    public static void Main(Builder builder)\n    {\n        builder.Services.AddDatabase();\n    }\n}\n";
const PROXY: &str = "using Dpb.Common.Database;\nnamespace Dpb.DataProxy;\n\
public class Program\n{\n    public void RegisterServices(object services)\n    {\n        services.AddDatabase();\n    }\n}\n";

// "a_common" sorts first so the declaration is scanned before its callers
// (sidesteps #210) and the baseline resolves through the import tier.
const BASE: &[(&str, &str)] = &[
    ("a_common/Extensions.cs", EXTENSIONS),
    ("b_mgr/Program.cs", MGR),
    ("c_proxy/Program.cs", PROXY),
];

type Dump = (BTreeSet<String>, BTreeSet<String>);

/// Joins the first `n` columns of a row with `|`; NULLs render as empty.
fn render_row(row: &rusqlite::Row<'_>, n: usize) -> rusqlite::Result<String> {
    let mut cols = Vec::with_capacity(n);
    for i in 0..n {
        cols.push(row.get::<_, Option<String>>(i)?.unwrap_or_default());
    }
    Ok(cols.join("|"))
}

fn dump(indexer: &Indexer, db_path: &PathBuf) -> Dump {
    let gv = indexer.graph_version();
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.kind, e.target_qualname, t.qualname
             FROM edges e
             LEFT JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS'",
        )
        .unwrap();
    let edges = stmt
        .query_map([gv], |r| render_row(r, 4))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, ur.edge_kind, ur.reference_name, ur.reason
             FROM unresolved_references ur
             LEFT JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE ur.graph_version = ?",
        )
        .unwrap();
    let unresolved = stmt
        .query_map([gv], |r| render_row(r, 4))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    (edges, unresolved)
}

/// Index `files` into an empty repo with a brand-new indexer.
fn fresh_dump(files: &[(&str, &str)]) -> Dump {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-256-fresh-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut fresh = Indexer::new(root, db_path.clone()).unwrap();
    fresh.reindex().unwrap();
    dump(&fresh, &db_path)
}

#[derive(Clone, Copy)]
enum Process {
    /// The same long-lived `Indexer` (MCP server) across the edit.
    Same,
    /// A new `Indexer` per reindex (CLI): no in-memory state carries over.
    New,
}

enum Edit<'a> {
    Write(&'a str, &'a str),
    Delete(&'a str),
}

/// `base` with `edits` applied: the tree a fresh index must be compared to.
fn apply_edits<'a>(base: &[(&'a str, &'a str)], edits: &[Edit<'a>]) -> Vec<(&'a str, &'a str)> {
    let mut tree: Vec<(&str, &str)> = base.to_vec();
    for edit in edits {
        match *edit {
            Edit::Write(path, src) => match tree.iter_mut().find(|(p, _)| *p == path) {
                Some(entry) => entry.1 = src,
                None => tree.push((path, src)),
            },
            Edit::Delete(path) => tree.retain(|(p, _)| *p != path),
        }
    }
    tree
}

struct Incremental {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    db_path: PathBuf,
    dump: Dump,
}

/// Index `base`, apply `edits`, reindex incrementally and assert the CALLS
/// edges and unresolved references (reason included) equal those of a fresh
/// index of the edited tree. Returns the incremental state.
fn edit_and_compare(
    base: &[(&str, &str)],
    edits: &[Edit<'_>],
    process: Process,
    prefix: &str,
) -> Incremental {
    let tmp = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, base);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    for edit in edits {
        match *edit {
            Edit::Write(path, src) => common::write_files(&root, &[(path, src)]),
            Edit::Delete(path) => std::fs::remove_file(root.join(path)).unwrap(),
        }
    }
    if matches!(process, Process::New) {
        indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    }
    indexer.reindex().unwrap();
    let inc = dump(&indexer, &db_path);
    let fresh = fresh_dump(&apply_edits(base, edits));

    assert!(
        !inc.0.iter().any(|e| e.contains("ext:")),
        "no phantom external stub edges: {:#?}",
        inc.0
    );
    assert_eq!(inc.0, fresh.0, "CALLS edges must match fresh");
    assert_eq!(
        inc.1, fresh.1,
        "unresolved references (reason included) must match fresh"
    );
    Incremental {
        _tmp: tmp,
        root,
        db_path,
        dump: inc,
    }
}

fn rename_and_compare(process: Process) {
    let inc = edit_and_compare(
        BASE,
        &[Edit::Write("a_common/Extensions.cs", EXTENSIONS_RENAMED)],
        process,
        "lidx-256-",
    );
    let result = rpc::call(
        inc.root.clone(),
        inc.db_path.clone(),
        "explain_symbol".to_string(),
        r#"{"qualname":"Dpb.DataProxy.Program.RegisterServices","sections":["callees"]}"#,
        "1",
    )
    .unwrap();
    assert!(
        !result.contains("AddDatabase"),
        "explain_symbol must report no AddDatabase callee: {result}"
    );
}

/// Same long-lived `Indexer` (MCP server) across the rename.
#[test]
fn csharp_method_rename_leaves_no_phantom_stub_and_matches_fresh() {
    rename_and_compare(Process::Same);
}

/// A new process per reindex (CLI): only the stale-caller re-extraction can
/// help, there is no in-memory extension registry to carry over.
#[test]
fn csharp_method_rename_in_new_process_matches_fresh() {
    rename_and_compare(Process::New);
}

const OTHER: &str = "namespace Dpb.Other;\n\
public static class OtherExtensions\n{\n    public static void AddOther(this object services) { }\n}\n";
const TWO_CALLS: &str = "using Dpb.Common.Database;\nusing Dpb.Other;\nnamespace Dpb.DataMgr;\n\
public class Program\n{\n    public static void Main(Builder builder)\n    {\n        builder.Services.AddDatabase();\n        builder.Services.AddOther();\n    }\n}\n";
const TWO_CALLS_COMMENTED: &str = "using Dpb.Common.Database;\nusing Dpb.Other;\nnamespace Dpb.DataMgr;\n// touched\n\
public class Program\n{\n    public static void Main(Builder builder)\n    {\n        builder.Services.AddDatabase();\n        builder.Services.AddOther();\n    }\n}\n";

const TWO_DECLS: &[(&str, &str)] = &[
    ("a_common/Extensions.cs", EXTENSIONS),
    ("a_other/Other.cs", OTHER),
    ("b_mgr/Program.cs", TWO_CALLS),
];

fn assert_resolved(inc: &Incremental, method: &str) {
    assert!(
        inc.dump
            .0
            .iter()
            .any(|e| e.ends_with(&format!(".{method}"))),
        "{method} must stay resolved: {:#?}",
        inc.dump.0
    );
}

/// Scenario A: renaming one extension method must not cost an unchanged
/// declaration's caller edge (the registry may not depend on which files
/// were extracted this run).
fn rename_one_of_two(process: Process) {
    let inc = edit_and_compare(
        TWO_DECLS,
        &[Edit::Write("a_common/Extensions.cs", EXTENSIONS_RENAMED)],
        process,
        "lidx-256-a-",
    );
    assert_resolved(&inc, "AddOther");
}

#[test]
fn renaming_one_extension_keeps_the_other_callee_same_process() {
    rename_one_of_two(Process::Same);
}

#[test]
fn renaming_one_extension_keeps_the_other_callee_new_process() {
    rename_one_of_two(Process::New);
}

/// Scenario B: editing only the caller file leaves every callee intact.
fn edit_only_the_caller(process: Process) {
    let inc = edit_and_compare(
        TWO_DECLS,
        &[Edit::Write("b_mgr/Program.cs", TWO_CALLS_COMMENTED)],
        process,
        "lidx-256-b-",
    );
    assert_resolved(&inc, "AddDatabase");
    assert_resolved(&inc, "AddOther");
}

#[test]
fn editing_only_the_caller_keeps_extension_edges_same_process() {
    edit_only_the_caller(Process::Same);
}

#[test]
fn editing_only_the_caller_keeps_extension_edges_new_process() {
    edit_only_the_caller(Process::New);
}

/// Deleting the declaring file leaves its callers like a fresh index: no
/// edge to the vanished method, the other extension's edge intact.
fn delete_declaring_file(process: Process) {
    let inc = edit_and_compare(
        TWO_DECLS,
        &[Edit::Delete("a_common/Extensions.cs")],
        process,
        "lidx-256-del-",
    );
    assert_resolved(&inc, "AddOther");
}

#[test]
fn deleting_the_declaring_file_matches_fresh_same_process() {
    delete_declaring_file(Process::Same);
}

#[test]
fn deleting_the_declaring_file_matches_fresh_new_process() {
    delete_declaring_file(Process::New);
}

/// The path that already worked: a renamed Python function.
#[test]
fn python_function_rename_still_matches_fresh() {
    let lib = "def add_database():\n    pass\n";
    let lib_renamed = "def add_database_renamed():\n    pass\n";
    let app = "from lib import add_database\n\n\ndef main():\n    add_database()\n";
    let tmp = tempfile::Builder::new()
        .prefix("lidx-256-py-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &[("lib.py", lib), ("app.py", app)]);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    common::write_files(&root, &[("lib.py", lib_renamed)]);
    indexer.reindex().unwrap();
    let inc = dump(&indexer, &db_path);

    assert_eq!(inc, fresh_dump(&[("lib.py", lib_renamed), ("app.py", app)]));
}
