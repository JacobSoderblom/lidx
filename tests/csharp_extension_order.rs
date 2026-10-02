//! Issue #210: C# extension-method calls resolve whenever the declaration
//! exists anywhere in the repo and its namespace is in scope, regardless of
//! the order files are extracted in.

mod common;

use lidx::indexer::Indexer;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::PathBuf;

const EXTENSIONS: &str = "namespace Dpb.Common.Database;\n\
public static class Extensions\n{\n    public static IServiceCollection AddDatabase(this IServiceCollection services) { return services; }\n}\n";
const MGR: &str = "using Dpb.Common.Database;\nnamespace Dpb.DataMgr;\n\
public class Program\n{\n    public static void Main(Builder builder)\n    {\n        builder.Services.AddDatabase();\n    }\n}\n";
const PROXY: &str = "using Dpb.Common.Database;\nnamespace Dpb.DataProxy;\n\
public class Program\n{\n    public void RegisterServices(IServiceCollection services)\n    {\n        services.AddDatabase();\n    }\n}\n";

const MAIN_EDGE: &str = "Dpb.DataMgr.Program.Main->Dpb.Common.Database.Extensions.AddDatabase";
const PROXY_EDGE: &str =
    "Dpb.DataProxy.Program.RegisterServices->Dpb.Common.Database.Extensions.AddDatabase";

fn three(common_dir: &str) -> Vec<(String, &'static str)> {
    vec![
        (format!("{common_dir}/Extensions.cs"), EXTENSIONS),
        ("AppA/Program.cs".to_string(), MGR),
        ("AppB/Program.cs".to_string(), PROXY),
    ]
}

fn borrowed<'a>(files: &'a [(String, &'static str)]) -> Vec<(&'a str, &'static str)> {
    files.iter().map(|(p, s)| (p.as_str(), *s)).collect()
}

/// Two-column rows of `sql` (bound to `gv`) joined with `sep`.
fn rows(db_path: &PathBuf, gv: i64, sql: &str, sep: &str) -> BTreeSet<String> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([gv], |r| {
        Ok(format!(
            "{}{sep}{}",
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?
        ))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn calls(db_path: &PathBuf, gv: i64) -> BTreeSet<String> {
    rows(
        db_path,
        gv,
        "SELECT s.qualname, COALESCE(t.qualname, e.target_qualname)
         FROM edges e
         JOIN symbols s ON s.id = e.source_symbol_id
         LEFT JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.graph_version = ? AND e.kind = 'CALLS'",
        "->",
    )
}

fn unresolved(db_path: &PathBuf, gv: i64) -> BTreeSet<String> {
    rows(
        db_path,
        gv,
        "SELECT reference_name, reason FROM unresolved_references WHERE graph_version = ?",
        "|",
    )
}

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-210-", files);
    let indexer = Indexer::new(root, db_path.clone()).unwrap();
    (tmp, db_path, indexer)
}

fn index_calls(files: &[(&str, &str)]) -> BTreeSet<String> {
    let (_tmp, db_path, indexer) = index(files);
    calls(&db_path, indexer.graph_version())
}

#[test]
fn declaration_sorted_last_resolves_both_calls() {
    let edges = index_calls(&borrowed(&three("Common")));
    assert!(edges.contains(MAIN_EDGE), "{edges:?}");
    assert!(edges.contains(PROXY_EDGE), "{edges:?}");
}

#[test]
fn declaration_sorted_first_resolves_both_calls() {
    let edges = index_calls(&borrowed(&three("AAACommon")));
    assert!(edges.contains(MAIN_EDGE), "{edges:?}");
    assert!(edges.contains(PROXY_EDGE), "{edges:?}");
}

#[test]
fn call_edge_set_is_independent_of_directory_naming() {
    let last = index_calls(&borrowed(&three("Common")));
    let first = index_calls(&borrowed(&three("AAACommon")));
    assert_eq!(last, first);
}

#[test]
fn extension_in_a_namespace_not_imported_does_not_resolve() {
    let caller = "namespace Dpb.DataMgr;\npublic class Program\n{\n    public static void Main(IServiceCollection services)\n    {\n        services.AddDatabase();\n    }\n}\n";
    let edges = index_calls(&[
        ("Common/Extensions.cs", EXTENSIONS),
        ("AppA/Program.cs", caller),
    ]);
    assert!(
        !edges.iter().any(|e| e.ends_with("Extensions.AddDatabase")),
        "{edges:?}"
    );
}

#[test]
fn extension_declared_and_called_in_one_file_resolves() {
    let src = "namespace Dpb.App;\npublic static class Ext\n{\n    public static void Twice(this Widget w) { }\n}\npublic class Zed\n{\n    public void Run(Widget w)\n    {\n        w.Twice();\n    }\n}\n";
    let edges = index_calls(&[("Only.cs", src)]);
    assert!(
        edges.contains("Dpb.App.Zed.Run->Dpb.App.Ext.Twice"),
        "{edges:?}"
    );
}

const NESTED_EXT: &str = "namespace Dpb.Outer.Inner;\npublic static class Ext\n{\n    public static void Twice(this Widget w) { }\n}\n";

#[test]
fn extension_via_using_static_resolves() {
    let stat = "using static Dpb.Outer.Inner.Ext;\nnamespace Dpb.A;\npublic class Zed\n{\n    public void Run(Widget w)\n    {\n        w.Twice();\n    }\n}\n";
    let edges = index_calls(&[("Zlib/Ext.cs", NESTED_EXT), ("A/Zed.cs", stat)]);
    assert!(
        edges.contains("Dpb.A.Zed.Run->Dpb.Outer.Inner.Ext.Twice"),
        "{edges:?}"
    );
}

#[test]
fn extension_via_using_inside_a_block_namespace_resolves() {
    let nested = "namespace Dpb.B\n{\n    using Dpb.Outer.Inner;\n    public class Yak\n    {\n        public void Run(Widget w)\n        {\n            w.Twice();\n        }\n    }\n}\n";
    let edges = index_calls(&[("Zlib/Ext.cs", NESTED_EXT), ("B/Yak.cs", nested)]);
    assert!(
        edges.contains("Dpb.B.Yak.Run->Dpb.Outer.Inner.Ext.Twice"),
        "{edges:?}"
    );
}

#[test]
fn declaration_with_tab_or_newline_after_this_is_prescanned() {
    for sep in ["\t", "\n"] {
        let ext = EXTENSIONS.replace(
            "this IServiceCollection",
            &format!("this{sep}IServiceCollection"),
        );
        let edges = index_calls(&[
            ("Common/Extensions.cs", &ext),
            ("AppA/Program.cs", MGR),
            ("AppB/Program.cs", PROXY),
        ]);
        assert!(edges.contains(PROXY_EDGE), "{sep:?}: {edges:?}");
    }
}

fn ancestor_call(ns: &str) -> String {
    format!(
        "namespace {ns};\npublic class Caller\n{{\n    public void Run(Widget w)\n    {{\n        w.Twice();\n    }}\n}}\n"
    )
}

#[test]
fn extension_in_an_enclosing_namespace_is_in_scope_without_a_using() {
    let acme = "namespace Acme;\npublic static class AcmeExt\n{\n    public static void Twice(this Widget w) { }\n}\n";
    let mid = "namespace Acme.Mid;\npublic static class MidExt\n{\n    public static void Thrice(this Widget w) { }\n}\n";
    let call =
        ancestor_call("Acme.Mid.Inner").replace("Twice();\n", "Twice();\n        w.Thrice();\n");
    let edges = index_calls(&[
        ("Zlib/Acme.cs", acme),
        ("Zlib/Mid.cs", mid),
        ("Caller.cs", &call),
    ]);
    assert!(
        edges.contains("Acme.Mid.Inner.Caller.Run->Acme.AcmeExt.Twice"),
        "{edges:?}"
    );
    assert!(
        edges.contains("Acme.Mid.Inner.Caller.Run->Acme.Mid.MidExt.Thrice"),
        "{edges:?}"
    );
}

#[test]
fn extension_in_a_sibling_namespace_is_not_in_scope() {
    let other = "namespace Acme.Other;\npublic static class OtherExt\n{\n    public static void Twice(this Widget w) { }\n}\n";
    let edges = index_calls(&[
        ("Zlib/Other.cs", other),
        ("Caller.cs", &ancestor_call("Acme.Mid")),
    ]);
    assert!(
        !edges.iter().any(|e| e.ends_with("OtherExt.Twice")),
        "{edges:?}"
    );
}

#[test]
fn editing_only_the_caller_keeps_the_edge() {
    let files = three("Common");
    let (_tmp, db_path, mut indexer) = index(&borrowed(&files));
    let root = db_path.parent().unwrap().parent().unwrap().to_path_buf();
    let edited = format!("{MGR}// edited\n");
    std::fs::write(root.join("AppA/Program.cs"), edited).unwrap();
    indexer.sync_rel_paths(&["AppA/Program.cs".into()]).unwrap();
    let gv = indexer.graph_version();
    let edges = calls(&db_path, gv);
    assert!(edges.contains(MAIN_EDGE), "{edges:?}");
    assert!(edges.contains(PROXY_EDGE), "{edges:?}");
}

#[test]
fn unresolved_extension_reasons_do_not_depend_on_receiver_shape() {
    // Same failure (the declaration's namespace is not imported): one reason,
    // whether or not the receiver's type is known.
    let mgr = MGR.replace("using Dpb.Common.Database;\n", "");
    let proxy = PROXY.replace("using Dpb.Common.Database;\n", "");
    let (_tmp, db_path, indexer) = index(&[
        ("Common/Extensions.cs", EXTENSIONS),
        ("AppA/Program.cs", &mgr),
        ("AppB/Program.cs", &proxy),
    ]);
    let rows = unresolved(&db_path, indexer.graph_version());
    let reasons: BTreeSet<&str> = rows
        .iter()
        .filter(|r| r.contains("AddDatabase"))
        .map(|r| r.split('|').nth(1).unwrap())
        .collect();
    assert!(
        reasons.len() == 1 && rows.iter().filter(|r| r.contains("AddDatabase")).count() == 2,
        "{rows:?}"
    );
    assert_eq!(reasons.len(), 1, "{rows:?}");
}

fn root_of(db_path: &std::path::Path) -> PathBuf {
    db_path.parent().unwrap().parent().unwrap().to_path_buf()
}

#[test]
fn deleting_then_restoring_only_the_declaring_file_matches_fresh() {
    let files = three("Common");
    let (_tmp, db_path, mut indexer) = index(&borrowed(&files));
    let root = root_of(&db_path);
    let decl = root.join("Common/Extensions.cs");
    std::fs::remove_file(&decl).unwrap();
    indexer
        .sync_rel_paths(&["Common/Extensions.cs".into()])
        .unwrap();
    let gone = calls(&db_path, indexer.graph_version());
    assert!(!gone.contains(MAIN_EDGE), "{gone:?}");
    std::fs::write(&decl, EXTENSIONS).unwrap();
    indexer
        .sync_rel_paths(&["Common/Extensions.cs".into()])
        .unwrap();
    let gv = indexer.graph_version();
    let restored = (calls(&db_path, gv), unresolved(&db_path, gv));
    let fresh = {
        let (_t, p, i) = index(&borrowed(&files));
        (
            calls(&p, i.graph_version()),
            unresolved(&p, i.graph_version()),
        )
    };
    assert_eq!(restored, fresh);
    assert!(restored.0.contains(MAIN_EDGE));
}

#[test]
fn adding_a_declaring_file_resolves_untouched_callers() {
    let files = three("Common");
    let callers: Vec<(&str, &str)> = borrowed(&files).into_iter().skip(1).collect();
    let (_tmp, db_path, mut indexer) = index(&callers);
    let before = calls(&db_path, indexer.graph_version());
    assert!(before.is_empty(), "{before:?}");
    let root = root_of(&db_path);
    std::fs::create_dir_all(root.join("Common")).unwrap();
    std::fs::write(root.join("Common/Extensions.cs"), EXTENSIONS).unwrap();
    indexer
        .sync_rel_paths(&["Common/Extensions.cs".into()])
        .unwrap();
    let edges = calls(&db_path, indexer.graph_version());
    assert!(edges.contains(MAIN_EDGE), "{edges:?}");
    assert!(edges.contains(PROXY_EDGE), "{edges:?}");
}
