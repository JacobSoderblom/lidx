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

fn dump(indexer: &Indexer, db_path: &PathBuf) -> Dump {
    let gv = indexer.graph_version();
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(s.qualname,''), e.kind, COALESCE(e.target_qualname,''),
                    COALESCE(t.qualname,'')
             FROM edges e
             LEFT JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS'",
        )
        .unwrap();
    let edges = stmt
        .query_map([gv], |r| {
            Ok(format!(
                "{}|{}|{}|{}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(s.qualname,''), ur.edge_kind, COALESCE(ur.reference_name,''), ur.reason
             FROM unresolved_references ur
             LEFT JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE ur.graph_version = ?",
        )
        .unwrap();
    let unresolved = stmt
        .query_map([gv], |r| {
            Ok(format!(
                "{}|{}|{}|{}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    (edges, unresolved)
}

fn rename_and_compare(new_process: bool) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-256-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, BASE);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let (base_edges, _) = dump(&indexer, &db_path);
    assert!(
        base_edges
            .iter()
            .any(|e| e.ends_with("|Dpb.Common.Database.Extensions.AddDatabase")),
        "baseline must resolve through the import tier: {base_edges:#?}"
    );

    common::write_files(&root, &[("a_common/Extensions.cs", EXTENSIONS_RENAMED)]);
    if new_process {
        indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    }
    indexer.reindex().unwrap();
    let (inc_edges, inc_unresolved) = dump(&indexer, &db_path);

    let fresh_tmp = tempfile::Builder::new()
        .prefix("lidx-256-fresh-")
        .tempdir()
        .unwrap();
    let fresh_root = fresh_tmp.path().to_path_buf();
    common::write_files(
        &fresh_root,
        &[
            ("a_common/Extensions.cs", EXTENSIONS_RENAMED),
            ("b_mgr/Program.cs", MGR),
            ("c_proxy/Program.cs", PROXY),
        ],
    );
    let fresh_db = fresh_root.join(".lidx").join(".lidx.sqlite");
    let mut fresh = Indexer::new(fresh_root, fresh_db.clone()).unwrap();
    fresh.reindex().unwrap();
    let (fresh_edges, fresh_unresolved) = dump(&fresh, &fresh_db);

    assert!(
        !inc_edges.iter().any(|e| e.contains("ext:")),
        "no phantom external stub edges: {inc_edges:#?}"
    );
    assert_eq!(inc_edges, fresh_edges, "CALLS edges must match fresh");
    assert_eq!(
        inc_unresolved, fresh_unresolved,
        "unresolved references (reason included) must match fresh"
    );

    let result = rpc::call(
        root,
        db_path,
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
    rename_and_compare(false);
}

/// A new process per reindex (CLI): only the stale-caller re-extraction can
/// help, there is no in-memory extension registry to carry over.
#[test]
fn csharp_method_rename_in_new_process_matches_fresh() {
    rename_and_compare(true);
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

    let fresh_tmp = tempfile::Builder::new()
        .prefix("lidx-256-py-fresh-")
        .tempdir()
        .unwrap();
    let fresh_root = fresh_tmp.path().to_path_buf();
    common::write_files(&fresh_root, &[("lib.py", lib_renamed), ("app.py", app)]);
    let fresh_db = fresh_root.join(".lidx").join(".lidx.sqlite");
    let mut fresh = Indexer::new(fresh_root, fresh_db.clone()).unwrap();
    fresh.reindex().unwrap();
    assert_eq!(inc, dump(&fresh, &fresh_db));
}
