//! Go interface method specs are symbols; a call on an interface-typed
//! receiver binds to the spec; types that structurally implement the
//! interface get `IMPLEMENTS` edges, so `analyze_impact` reaches the caller
//! from an implementing method through interface dispatch. Every incremental
//! scenario ends equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const DOER: &str = "package app\n\ntype Doer interface {\n\tDo() error\n}\n";
const IMPLS: &str = "package app\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do() error { return nil }\n";
const USE: &str = "package app\n\nfunc Use() {\n\tvar d Doer\n\td.Do()\n}\n";
/// Wrong arity for `Do() error`: not an implementation.
const NOT_IMPL: &str =
    "package app\n\ntype Other struct{}\n\nfunc (o Other) Do(x int) error { return nil }\n";

fn base() -> Vec<(&'static str, &'static str)> {
    vec![
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("impls.go", IMPLS),
        ("use.go", USE),
        ("other.go", NOT_IMPL),
    ]
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-iface-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

fn edges(indexer: &Indexer, kind: &str, source_like: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname || ' -> ' || COALESCE(t.qualname, '?') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = ?1 AND e.graph_version = ?2 AND s.qualname LIKE ?3
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![kind, gv, source_like], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn sync(indexer: &mut Indexer, root: &std::path::Path, files: &[(&str, &str)]) {
    common::write_files(root, files);
    let paths: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&paths).unwrap();
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

fn with(
    files: &[(&'static str, &'static str)],
    edit: (&'static str, &'static str),
) -> Vec<(&'static str, &'static str)> {
    let mut out: Vec<_> = files
        .iter()
        .filter(|(p, _)| *p != edit.0)
        .copied()
        .collect();
    out.push(edit);
    out
}

#[test]
fn a_call_on_an_interface_receiver_binds_the_spec() {
    let (_tmp, _root, indexer) = indexed(&base());
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
}

#[test]
fn structural_implementers_get_implements_edges() {
    let (_tmp, _root, indexer) = indexed(&base());
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "impls.%"),
        vec!["impls.ImplA -> doer.Doer", "impls.ImplB -> doer.Doer"]
    );
    assert!(edges(&indexer, "IMPLEMENTS", "other.%").is_empty());
}

#[test]
fn analyze_impact_on_an_implementation_reaches_the_caller_through_dispatch() {
    let (tmp, root, _indexer) = indexed(&base());
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    for target in ["impls.ImplA.Do", "impls.ImplB.Do"] {
        let response = rpc::call(
            root.clone(),
            db_path.clone(),
            "analyze_impact".to_string(),
            &format!(r#"{{"qualname":"{target}","direction":"upstream"}}"#),
            "1",
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        let affected = value["result"]["affected"].to_string();
        assert!(affected.contains("use.Use"), "{target}: {affected}");
    }
    drop(tmp);
}

#[test]
fn embedded_interfaces_contribute_their_methods() {
    let files = [
        ("go.mod", GO_MOD),
        (
            "ifaces.go",
            "package app\n\ntype Reader interface{ Read() int }\n\ntype Closer interface{ Close() }\n\ntype RC interface {\n\tReader\n\tCloser\n}\n",
        ),
        (
            "file.go",
            "package app\n\ntype File struct{}\n\nfunc (f *File) Read() int { return 0 }\n\nfunc (f *File) Close() {}\n\ntype Half struct{}\n\nfunc (h Half) Read() int { return 0 }\n",
        ),
        (
            "use.go",
            "package app\n\nfunc Use(rc RC) {\n\trc.Read()\n\trc.Close()\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec![
            "use.Use -> ifaces.Closer.Close",
            "use.Use -> ifaces.Reader.Read"
        ]
    );
    let impls = edges(&indexer, "IMPLEMENTS", "file.%");
    assert!(
        impls.contains(&"file.File -> ifaces.RC".to_string()),
        "{impls:?}"
    );
    assert!(
        impls.contains(&"file.Half -> ifaces.Reader".to_string()),
        "{impls:?}"
    );
    assert!(
        !impls.contains(&"file.Half -> ifaces.RC".to_string()),
        "{impls:?}"
    );
}

#[test]
fn a_struct_embedding_an_interface_promotes_its_methods() {
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        (
            "wrap.go",
            "package app\n\ntype Wrapper struct {\n\tDoer\n}\n",
        ),
        (
            "use.go",
            "package app\n\nfunc Use(w *Wrapper) {\n\tw.Do()\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "wrap.%"),
        vec!["wrap.Wrapper -> doer.Doer"]
    );
}

#[test]
fn editing_an_interface_method_matches_a_fresh_reindex() {
    let files = base();
    let (_tmp, root, mut indexer) = indexed(&files);
    assert_incremental_equals_fresh(&indexer, &files);

    // The spec gains a method nobody implements: no implementers left.
    let grown = "package app\n\ntype Doer interface {\n\tDo() error\n\tStop()\n}\n";
    sync(&mut indexer, &root, &[("doer.go", grown)]);
    assert!(edges(&indexer, "IMPLEMENTS", "impls.%").is_empty());
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &with(&files, ("doer.go", grown)));

    // An implementation gains it: only that one implements again.
    let impls = "package app\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n\nfunc (a *ImplA) Stop() {}\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do() error { return nil }\n";
    sync(&mut indexer, &root, &[("impls.go", impls)]);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "impls.%"),
        vec!["impls.ImplA -> doer.Doer"]
    );
    let step2 = with(&with(&files, ("doer.go", grown)), ("impls.go", impls));
    assert_incremental_equals_fresh(&indexer, &step2);

    // The spec method is renamed: the call no longer binds.
    let renamed = "package app\n\ntype Doer interface {\n\tDo2() error\n\tStop()\n}\n";
    sync(&mut indexer, &root, &[("doer.go", renamed)]);
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    let step3 = with(&step2, ("doer.go", renamed));
    assert_incremental_equals_fresh(&indexer, &step3);

    // And back.
    sync(
        &mut indexer,
        &root,
        &[("doer.go", DOER), ("impls.go", IMPLS)],
    );
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn deleting_the_interface_file_drops_its_edges_like_a_fresh_reindex() {
    let files = base();
    let (_tmp, root, mut indexer) = indexed(&files);
    std::fs::remove_file(root.join("doer.go")).unwrap();
    indexer.sync_rel_paths(&["doer.go".to_string()]).unwrap();
    assert!(edges(&indexer, "IMPLEMENTS", "impls.%").is_empty());
    let rest: Vec<_> = files
        .iter()
        .filter(|(p, _)| *p != "doer.go")
        .copied()
        .collect();
    assert_incremental_equals_fresh(&indexer, &rest);
}

#[test]
fn a_method_with_other_parameter_or_result_types_is_no_implementation() {
    let files = [
        ("go.mod", GO_MOD),
        (
            "artifact/artifact.go",
            "package artifact\n\ntype ID string\n\ntype Identifiable interface {\n\tID() ID\n}\n\ntype Sink interface {\n\tPut(v int) error\n}\n",
        ),
        ("sbom/sbom.go", "package sbom\n\ntype FormatID string\n"),
        (
            "enc/enc.go",
            "package enc\n\nimport \"example.com/m/sbom\"\n\ntype Encoder struct{}\n\nfunc (e Encoder) ID() sbom.FormatID { return \"\" }\n",
        ),
        (
            "enc/ok.go",
            "package enc\n\nimport \"example.com/m/artifact\"\n\ntype Plain struct{}\n\nfunc (p Plain) ID() artifact.ID { return \"\" }\n\ntype Store struct{}\n\nfunc (s Store) Put(v int) error { return nil }\n\ntype Wrong struct{}\n\nfunc (w Wrong) Put(v string) error { return nil }\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    let mut got = edges(&indexer, "IMPLEMENTS", "enc/%");
    got.sort();
    assert_eq!(
        got,
        vec![
            "enc/ok.Plain -> artifact/artifact.Identifiable",
            "enc/ok.Store -> artifact/artifact.Sink",
        ]
    );
}
