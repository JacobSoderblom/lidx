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

/// Only `ImplA` implements `Doer`.
const IMPLS_ONE: &str =
    "package app\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n";

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

fn rpc_result(root: &std::path::Path, method: &str, params: &str) -> serde_json::Value {
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let response = rpc::call(root.to_path_buf(), db_path, method.to_string(), params, "1").unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    value["result"].clone()
}

#[test]
fn several_implementations_leave_the_call_on_the_spec_as_a_dispatch_edge() {
    let (_tmp, _root, indexer) = indexed(&base());
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
}

#[test]
fn the_one_implementation_is_bound_directly() {
    let files = with(&base(), ("impls.go", IMPLS_ONE));
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    assert!(edges(&indexer, "DISPATCH", "use.Use").is_empty());
}

#[test]
fn a_type_with_another_signature_is_no_implementation_for_uniqueness() {
    // `Other.Do(x int)` does not implement `Doer`, so ImplA stays the only one.
    let files = with(&base(), ("impls.go", IMPLS_ONE));
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(edges(&indexer, "IMPLEMENTS", "%").len(), 1);
}

const PROVABLE: &str = "package app\n\ntype Holder struct{ h Doer }\n\nfunc NewB() *ImplB { return &ImplB{} }\n\nfunc Literal() {\n\tvar d Doer = &ImplA{}\n\td.Do()\n}\n\nfunc Factory() {\n\tvar d Doer = NewB()\n\td.Do()\n}\n\nfunc Field() {\n\ts := &Holder{h: &ImplA{}}\n\ts.h.Do()\n}\n\nfunc Reassigned() {\n\tvar d Doer = &ImplA{}\n\td = &ImplB{}\n\td.Do()\n}\n\nfunc Param(d Doer) {\n\td.Do()\n}\n\nfunc FieldReassigned() {\n\ts := &Holder{h: &ImplA{}}\n\ts.h = &ImplB{}\n\ts.h.Do()\n}\n";

#[test]
fn a_receiver_type_proven_at_the_call_site_binds_the_concrete_method() {
    let files = with(&base(), ("use.go", PROVABLE));
    let (_tmp, _root, indexer) = indexed(&files);
    for (caller, target) in [
        ("use.Literal", "impls.ImplA.Do"),
        ("use.Factory", "impls.ImplB.Do"),
        ("use.Field", "impls.ImplA.Do"),
    ] {
        // (`Factory` also calls the constructor)
        let calls = edges(&indexer, "CALLS", caller);
        assert!(
            calls.contains(&format!("{caller} -> {target}")),
            "{caller}: {calls:?}"
        );
        assert!(!calls.iter().any(|c| c.ends_with("Doer.Do")), "{calls:?}");
        assert!(edges(&indexer, "DISPATCH", caller).is_empty(), "{caller}");
    }
    // not provable: assigned again, a parameter, a reassigned field
    for caller in ["use.Reassigned", "use.Param", "use.FieldReassigned"] {
        assert!(edges(&indexer, "CALLS", caller).is_empty(), "{caller}");
        assert_eq!(
            edges(&indexer, "DISPATCH", caller),
            vec![format!("{caller} -> doer.Doer.Do")],
            "{caller}"
        );
    }
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
fn analyze_impact_on_an_implementation_reaches_the_caller_through_either_path() {
    // two implementations: the DISPATCH edge, fanned out through IMPLEMENTS
    let (_tmp, root, _indexer) = indexed(&base());
    for target in ["impls.ImplA.Do", "impls.ImplB.Do"] {
        let result = rpc_result(
            &root,
            "analyze_impact",
            &format!(r#"{{"qualname":"{target}","direction":"upstream"}}"#),
        );
        let affected = result["affected"].to_string();
        assert!(affected.contains("use.Use"), "{target}: {affected}");
    }
    // one implementation: the direct CALLS edge
    let files = with(&base(), ("impls.go", IMPLS_ONE));
    let (_tmp2, root2, _indexer2) = indexed(&files);
    let result = rpc_result(
        &root2,
        "analyze_impact",
        r#"{"qualname":"impls.ImplA.Do","direction":"upstream"}"#,
    );
    assert!(result["affected"].to_string().contains("use.Use"));
}

#[test]
fn trace_flow_from_a_caller_fans_out_through_the_dispatch_edge() {
    let (_tmp, root, _indexer) = indexed(&base());
    let result = rpc_result(
        &root,
        "trace_flow",
        r#"{"start_qualname":"use.Use","direction":"downstream","max_hops":3}"#,
    );
    let text = result.to_string();
    for target in ["doer.Doer.Do", "impls.ImplA.Do", "impls.ImplB.Do"] {
        assert!(text.contains(target), "{target}: {text}");
    }
    assert!(text.contains("DISPATCH"), "{text}");
}

#[test]
fn explain_symbol_lists_the_dispatch_edge_with_its_kind() {
    let (_tmp, root, _indexer) = indexed(&base());
    let caller = rpc_result(&root, "explain_symbol", r#"{"qualname":"use.Use"}"#);
    let callees = caller["callees"].to_string();
    assert!(callees.contains("doer.Doer.Do"), "{callees}");
    assert!(callees.contains("DISPATCH"), "{callees}");
    let spec = rpc_result(&root, "explain_symbol", r#"{"qualname":"doer.Doer.Do"}"#);
    assert!(spec["callers"].to_string().contains("use.Use"), "{spec}");
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
    // Reader has two implementations, Closer one
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> ifaces.Reader.Read"]
    );
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> file.File.Close"]
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
    // the promoted method is the spec itself: nothing concrete to bind
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "wrap.%"),
        vec!["wrap.Wrapper -> doer.Doer"]
    );
}

#[test]
fn a_second_implementation_turns_the_direct_call_into_a_dispatch_and_back() {
    let one = with(&base(), ("impls.go", IMPLS_ONE));
    let (_tmp, root, mut indexer) = indexed(&one);
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &one);

    // a second implementation in a new file
    let second = "package app\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do() error { return nil }\n";
    sync(&mut indexer, &root, &[("second.go", second)]);
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    let mut two = one.clone();
    two.push(("second.go", second));
    assert_incremental_equals_fresh(&indexer, &two);

    // removing it turns the edge back
    std::fs::remove_file(root.join("second.go")).unwrap();
    indexer.sync_rel_paths(&["second.go".to_string()]).unwrap();
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    assert!(edges(&indexer, "DISPATCH", "use.Use").is_empty());
    assert_incremental_equals_fresh(&indexer, &one);
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
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &with(&files, ("doer.go", grown)));

    // An implementation gains it: it is the only one again, so the call binds it.
    let impls = "package app\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n\nfunc (a *ImplA) Stop() {}\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do() error { return nil }\n";
    sync(&mut indexer, &root, &[("impls.go", impls)]);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "impls.%"),
        vec!["impls.ImplA -> doer.Doer"]
    );
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    let step2 = with(&with(&files, ("doer.go", grown)), ("impls.go", impls));
    assert_incremental_equals_fresh(&indexer, &step2);

    // The spec method is renamed: the call no longer binds.
    let renamed = "package app\n\ntype Doer interface {\n\tDo2() error\n\tStop()\n}\n";
    sync(&mut indexer, &root, &[("doer.go", renamed)]);
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    assert!(edges(&indexer, "DISPATCH", "use.Use").is_empty());
    let step3 = with(&step2, ("doer.go", renamed));
    assert_incremental_equals_fresh(&indexer, &step3);

    // And back.
    sync(
        &mut indexer,
        &root,
        &[("doer.go", DOER), ("impls.go", IMPLS)],
    );
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
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
    assert!(edges(&indexer, "DISPATCH", "use.Use").is_empty());
    let rest: Vec<_> = files
        .iter()
        .filter(|(p, _)| *p != "doer.go")
        .copied()
        .collect();
    assert_incremental_equals_fresh(&indexer, &rest);
}

#[test]
fn editing_the_proof_matches_a_fresh_reindex() {
    // the proven receiver loses its proof when `d` is assigned again
    let proven = "package app\n\nfunc Use() {\n\tvar d Doer = &ImplA{}\n\td.Do()\n}\n";
    let files = with(&base(), ("use.go", proven));
    let (_tmp, root, mut indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &files);

    let unproven =
        "package app\n\nfunc Use() {\n\tvar d Doer = &ImplA{}\n\td = &ImplB{}\n\td.Do()\n}\n";
    sync(&mut indexer, &root, &[("use.go", unproven)]);
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &with(&files, ("use.go", unproven)));
}

#[test]
fn a_production_caller_does_not_see_an_implementation_declared_in_a_test_file() {
    let mock =
        "package app\n\ntype mockDoer struct{}\n\nfunc (m mockDoer) Do() error { return nil }\n";
    let test_caller = "package app\n\nfunc UseInTest(d Doer) {\n\td.Do()\n}\n";
    let mut files = with(&base(), ("impls.go", IMPLS_ONE));
    files.push(("mock_test.go", mock));
    files.push(("use_test.go", test_caller));
    let (_tmp, _root, indexer) = indexed(&files);
    // the production caller: ImplA is the only implementation it can reach
    assert_eq!(
        edges(&indexer, "CALLS", "use.Use"),
        vec!["use.Use -> impls.ImplA.Do"]
    );
    // the test caller sees both
    assert!(edges(&indexer, "CALLS", "use_test.UseInTest").is_empty());
    assert_eq!(
        edges(&indexer, "DISPATCH", "use_test.UseInTest"),
        vec!["use_test.UseInTest -> doer.Doer.Do"]
    );
}

#[test]
fn a_method_on_an_undeclared_receiver_type_makes_the_implementation_unproven() {
    // `A2` is an alias of `ImplB`; its method has no type symbol to judge, so
    // `ImplA` is not shown to be the only implementation
    let alias = "package app\n\ntype A2 = ImplA\n\nfunc (a *A2) Do() error { return nil }\n";
    let mut files = with(&base(), ("impls.go", IMPLS_ONE));
    files.push(("alias.go", alias));
    let (_tmp, _root, indexer) = indexed(&files);
    assert!(edges(&indexer, "CALLS", "use.Use").is_empty());
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.Use"),
        vec!["use.Use -> doer.Doer.Do"]
    );
}

#[test]
fn an_implementation_reached_only_through_a_dispatch_edge_is_not_dead() {
    let (_tmp, root, mut indexer) = indexed(&base());
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let dead = result["dead_symbols"].to_string();
    assert!(!dead.contains("impls.ImplA.Do"), "{dead}");
    assert!(!dead.contains("impls.ImplB.Do"), "{dead}");
    drop(root);
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

// ---- #482: a call fans out to the implementers of the receiver's interface

const EMBED_IFACES: &str = "package app

type Identifiable interface{ ID() string }

type Source interface {
	Identifiable
	Close() error
}
";
const EMBED_TYPES: &str = "package app

type Adapter struct{}

func (a Adapter) ID() string { return \"a\" }

type File struct{}

func (f File) ID() string   { return \"f\" }
func (f File) Close() error { return nil }

type Dir struct{}

func (d Dir) ID() string   { return \"d\" }
func (d Dir) Close() error { return nil }
";
const EMBED_USE: &str = "package app

func UseSource(s Source) { s.ID() }

func UseIdentifiable(i Identifiable) { i.ID() }
";

fn embed_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("go.mod", GO_MOD),
        ("ifaces.go", EMBED_IFACES),
        ("types.go", EMBED_TYPES),
        ("use.go", EMBED_USE),
    ]
}

fn impact_up(root: &std::path::Path, target: &str) -> String {
    impact_up_to(root, target, 3)
}

fn impact_up_to(root: &std::path::Path, target: &str, depth: usize) -> String {
    let result = rpc_result(
        root,
        "analyze_impact",
        &format!(
            r#"{{"qualname":"{target}","direction":"upstream","max_depth":{depth},"kinds":["CALLS","DISPATCH"],
                "enable_test":false,"enable_historical":false}}"#
        ),
    );
    result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn trace_down(root: &std::path::Path, start: &str) -> String {
    let result = rpc_result(
        root,
        "trace_flow",
        &format!(r#"{{"start_qualname":"{start}","direction":"downstream","max_hops":3}}"#),
    );
    result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn a_call_on_the_outer_interface_does_not_reach_a_type_of_the_inner_one_only() {
    let (_tmp, root, _indexer) = indexed(&embed_files());
    // `Adapter` implements `Identifiable` but not `Source`: it can never be
    // the dynamic type of `UseSource`'s receiver.
    let callers = impact_up(&root, "types.Adapter.ID");
    assert!(callers.contains("use.UseIdentifiable"), "{callers}");
    assert!(!callers.contains("use.UseSource"), "{callers}");
    // Types of the outer interface are reached by both callers.
    for target in ["types.File.ID", "types.Dir.ID"] {
        let callers = impact_up(&root, target);
        assert!(
            callers.contains("use.UseIdentifiable"),
            "{target}: {callers}"
        );
        assert!(callers.contains("use.UseSource"), "{target}: {callers}");
    }
}

#[test]
fn trace_flow_from_the_outer_interface_call_reaches_only_its_implementers() {
    let (_tmp, root, _indexer) = indexed(&embed_files());
    let from_source = trace_down(&root, "use.UseSource");
    assert!(from_source.contains("types.File.ID"), "{from_source}");
    assert!(from_source.contains("types.Dir.ID"), "{from_source}");
    assert!(!from_source.contains("types.Adapter.ID"), "{from_source}");
    let from_inner = trace_down(&root, "use.UseIdentifiable");
    for t in ["types.Adapter.ID", "types.File.ID", "types.Dir.ID"] {
        assert!(from_inner.contains(t), "{t}: {from_inner}");
    }
}

#[test]
fn the_receiver_interface_follows_edits_and_ends_equal_to_a_fresh_reindex() {
    let (_tmp, root, mut indexer) = indexed(&embed_files());
    assert!(!impact_up(&root, "types.Adapter.ID").contains("use.UseSource"));

    // `Adapter` gains `Close`: it now implements `Source` too.
    let widened = "package app

type Adapter struct{}

func (a Adapter) ID() string    { return \"a\" }
func (a Adapter) Close() error  { return nil }

type File struct{}

func (f File) ID() string   { return \"f\" }
func (f File) Close() error { return nil }

type Dir struct{}

func (d Dir) ID() string   { return \"d\" }
func (d Dir) Close() error { return nil }
";
    let files = with(&embed_files(), ("types.go", widened));
    sync(&mut indexer, &root, &files);
    assert!(impact_up(&root, "types.Adapter.ID").contains("use.UseSource"));
    assert_incremental_equals_fresh(&indexer, &files);
    let (_tmp2, root2, _i2) = indexed(&files);
    assert!(impact_up(&root2, "types.Adapter.ID").contains("use.UseSource"));

    // ...and loses it again.
    let narrowed = embed_files();
    sync(&mut indexer, &root, &narrowed);
    assert!(!impact_up(&root, "types.Adapter.ID").contains("use.UseSource"));
    assert_incremental_equals_fresh(&indexer, &narrowed);
}

#[test]
fn a_type_embedding_the_inner_implementer_still_reaches_the_outer_call() {
    // `Wrapper` embeds `Adapter` (which has `ID`) and adds `Close`: it
    // implements `Source`, and its `ID` is `Adapter.ID`.
    let wrapper = "package app

type Wrapper struct{ Adapter }

func (w Wrapper) Close() error { return nil }
";
    let mut files = embed_files();
    files.push(("wrapper.go", wrapper));
    let (_tmp, root, _indexer) = indexed(&files);
    let callers = impact_up(&root, "types.Adapter.ID");
    assert!(callers.contains("use.UseSource"), "{callers}");
}

// ---- #482: the narrowing follows the method sets, not the embedding alone

const SHADOW: &str = "package app

type Doer interface{ Do() }

type Wider interface {
	Doer
	Extra()
}

type L0 struct{}

func (L0) Do() {}

type W struct{ L0 }

func (w W) Do()    { w.L0.Do() }
func (w W) Extra() {}

func use(d Wider) { d.Do() }
";

#[test]
fn an_embedder_that_redeclares_the_method_does_not_lend_its_interfaces() {
    let (_tmp, root, _indexer) = indexed(&[("go.mod", GO_MOD), ("app.go", SHADOW)]);
    // `L0` is no `Wider`; `W` is, but runs `L0.Do` only from its own body, so
    // `use` is two calls from `L0.Do`, not one.
    let near = impact_up_to(&root, "app.L0.Do", 1);
    assert!(!near.contains("app.use"), "{near}");
    assert!(near.contains("app.W.Do"), "{near}");
    let far = impact_up_to(&root, "app.L0.Do", 2);
    assert!(far.contains("app.use"), "{far}");
    // `W.Do` itself is a `Wider`: `use` is one call away.
    assert!(impact_up_to(&root, "app.W.Do", 1).contains("app.use"));
}

const CHAIN: &str = "package app

type Doer interface{ Do() }

type Wider interface {
	Doer
	Extra()
}

type L0 struct{}

func (L0) Do() {}

type L1 struct{ L0 }
type L2 struct{ L1 }
type L3 struct{ L2 }
type L4 struct{ L3 }

func (L4) Extra() {}

// A second `Doer` method keeps the call a dispatch.
type K struct{}

func (K) Do() {}

func use(d Wider) { d.Do() }
";

#[test]
fn the_embedding_chain_is_followed_as_deep_as_it_goes() {
    // `L4` reaches `L0.Do` through four embeddings and is a `Wider`.
    let (_tmp, root, _indexer) = indexed(&[("go.mod", GO_MOD), ("app.go", CHAIN)]);
    let callers = impact_up_to(&root, "app.L0.Do", 1);
    assert!(callers.contains("app.use"), "{callers}");
}

const GENERIC: &str = "package app

type Getter[T any] interface{ Get() T }

type Named[T any] interface {
	Getter[T]
	Name() string
}

type A[T any] struct{ v T }

func (a A[T]) Get() T { return a.v }

type B[T any] struct{ v T }

func (b B[T]) Get() T       { return b.v }
func (b B[T]) Name() string { return \"b\" }

func useNamed(n Named[int])   { n.Get() }
func useGetter(g Getter[int]) { g.Get() }
";

#[test]
fn a_generic_interface_narrows_like_a_plain_one() {
    let (_tmp, root, _indexer) = indexed(&[("go.mod", GO_MOD), ("app.go", GENERIC)]);
    let a = impact_up_to(&root, "app.A.Get", 1);
    assert!(a.contains("app.useGetter"), "{a}");
    assert!(!a.contains("app.useNamed"), "{a}");
    let b = impact_up_to(&root, "app.B.Get", 1);
    assert!(
        b.contains("app.useGetter") && b.contains("app.useNamed"),
        "{b}"
    );
}

const BASE_IFACE: &str = "package app\n\ntype Base interface{ Do() }\n";
const EXT_EMBEDS: &str = "package app\n\ntype Ext interface {\n\tBase\n\tClose()\n}\n";
const EXT_OWN: &str = "package app\n\ntype Ext interface {\n\tDo()\n\tClose()\n}\n";
const EXT_TYPES: &str = "package app

type P struct{}

func (P) Do()    {}
func (P) Close() {}

type Q struct{}

func (Q) Do()    {}
func (Q) Close() {}

type R struct{}

func (R) Do() {}
";
const EXT_USE: &str = "package app\n\nfunc use(e Ext) { e.Do() }\n";

#[test]
fn an_interface_that_stops_embedding_matches_a_fresh_reindex() {
    let files = vec![
        ("go.mod", GO_MOD),
        ("base.go", BASE_IFACE),
        ("ext.go", EXT_EMBEDS),
        ("types.go", EXT_TYPES),
        ("use.go", EXT_USE),
    ];
    let (_tmp, root, mut indexer) = indexed(&files);
    assert!(!impact_up(&root, "types.R.Do").contains("use.use"));
    assert!(impact_up(&root, "types.P.Do").contains("use.use"));

    // Only the declaring file changes: `Ext` now declares `Do` itself.
    let edited = with(&files, ("ext.go", EXT_OWN));
    sync(&mut indexer, &root, &[("ext.go", EXT_OWN)]);
    assert_incremental_equals_fresh(&indexer, &edited);
    assert!(impact_up(&root, "types.P.Do").contains("use.use"));
    assert!(!impact_up(&root, "types.R.Do").contains("use.use"));

    // ...and embeds again.
    sync(&mut indexer, &root, &[("ext.go", EXT_EMBEDS)]);
    assert_incremental_equals_fresh(&indexer, &files);
}

fn use_edges(indexer: &Indexer) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.kind || ' ' || COALESCE(t.qualname, '?') || ' ' || COALESCE(e.receiver_scope, '-')
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE s.qualname = 'use.use' AND e.kind IN ('CALLS', 'DISPATCH') AND e.graph_version = ?1
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn the_call_edge_of_an_unchanged_caller_follows_an_edit_of_the_declaring_file() {
    let files = vec![
        ("go.mod", GO_MOD),
        ("base.go", BASE_IFACE),
        ("ext.go", EXT_EMBEDS),
        ("types.go", EXT_TYPES),
        ("use.go", EXT_USE),
    ];
    let (_tmp, root, mut indexer) = indexed(&files);
    let edited = with(&files, ("ext.go", EXT_OWN));
    sync(&mut indexer, &root, &[("ext.go", EXT_OWN)]);
    let (_tmp2, _root2, fresh) = indexed(&edited);
    assert_eq!(use_edges(&indexer), use_edges(&fresh));
    sync(&mut indexer, &root, &[("ext.go", EXT_EMBEDS)]);
    let (_tmp3, _root3, fresh) = indexed(&files);
    assert_eq!(use_edges(&indexer), use_edges(&fresh));
}

#[test]
fn every_way_a_type_gains_or_loses_the_embedded_method_matches_a_fresh_reindex() {
    let base = "package app\n\ntype Base interface{ Do() }\n";
    let types = "package app\n\ntype P struct{}\n\nfunc (P) Do() {}\n\ntype Q struct{}\n\nfunc (Q) Do() {}\n";
    let usep = "package app\n\nfunc use(e Ext) { e.Do() }\n";
    let iface_embeds = "package app\n\ntype Ext interface {\n\tBase\n\tClose()\n}\n";
    let iface_own = "package app\n\ntype Ext interface {\n\tDo()\n\tClose()\n}\n";
    let iface_none = "package app\n\ntype Ext interface {\n\tClose()\n}\n";
    let struct_embeds = "package app\n\ntype Ext struct{ Base }\n";
    let struct_own = "package app\n\ntype Ext struct{}\n\nfunc (Ext) Do() {}\n";
    let struct_none = "package app\n\ntype Ext struct{}\n";
    for (from, to) in [
        (iface_embeds, iface_own),
        (iface_own, iface_embeds),
        (iface_embeds, iface_none),
        (struct_embeds, struct_own),
        (struct_own, struct_embeds),
        (struct_embeds, struct_none),
    ] {
        let before = vec![
            ("go.mod", GO_MOD),
            ("base.go", base),
            ("ext.go", from),
            ("types.go", types),
            ("use.go", usep),
        ];
        let (_tmp, root, mut indexer) = indexed(&before);
        let after = with(&before, ("ext.go", to));
        sync(&mut indexer, &root, &[("ext.go", to)]);
        assert_incremental_equals_fresh(&indexer, &after);
        let (_tmp2, _root2, fresh) = indexed(&after);
        assert_eq!(use_edges(&indexer), use_edges(&fresh), "{from} -> {to}");
    }
}
