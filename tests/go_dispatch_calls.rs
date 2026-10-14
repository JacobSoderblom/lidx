//! A Go call left on an interface method is a `DISPATCH` edge. The methods
//! that can run there are `CALLS` edges derived from it (`resolution_kind =
//! dispatch_impl`): one per implementer the program puts behind an interface,
//! found by a rapid-type-analysis fixpoint, for production code and (with
//! test files) for tests. Incremental sync ends equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const DOER: &str = "package main\n\ntype Doer interface {\n\tDo() error\n}\n";
const IMPLS: &str = "package main\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do() error { return nil }\n\ntype ImplC struct{}\n\nfunc (c ImplC) Do() error { return nil }\n";
const USE: &str = "package main\n\nfunc run(d Doer) error {\n\treturn d.Do()\n}\n";
/// `ImplA` is built by a constructor that returns the interface, `ImplB` is
/// built inside `main` and passed on, `ImplC` is built by a function nothing
/// runs.
const MAIN: &str = "package main\n\nfunc NewA() Doer { return &ImplA{} }\n\nfunc unused() ImplC { return ImplC{} }\n\nfunc main() {\n\t_ = run(NewA())\n\tb := ImplB{}\n\t_ = run(b)\n}\n";

fn base() -> Vec<(&'static str, &'static str)> {
    vec![
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("impls.go", IMPLS),
        ("use.go", USE),
        ("main.go", MAIN),
    ]
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-dispatch-calls-", files);
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

fn dispatch_impl(indexer: &Indexer, source_like: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname || ' -> ' || t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.resolution_kind = 'dispatch_impl'
               AND e.graph_version = ?1 AND s.qualname LIKE ?2
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source_like], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
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

#[test]
fn a_call_on_the_interface_reaches_the_types_the_program_converts() {
    let (_tmp, _root, indexer) = indexed(&base());
    // the dispatch edge stays ...
    assert_eq!(
        edges(&indexer, "DISPATCH", "use.run"),
        vec!["use.run -> doer.Doer.Do"]
    );
    // ... ImplA is returned as a Doer and ImplB is built by a function that
    // runs; ImplC is built only where nothing runs
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do", "use.run -> impls.ImplB.Do"]
    );
}

#[test]
fn a_type_only_test_code_builds_is_no_target_of_production_code() {
    let test = "package main\n\nimport \"testing\"\n\nfunc TestC(t *testing.T) {\n\t_ = run(ImplC{})\n\t_ = Check(ImplC{})\n}\n\nfunc Check(d Doer) error { return d.Do() }\n";
    let files = with(&base(), ("c_test.go", test));
    let (_tmp, _root, indexer) = indexed(&files);
    // production `run` never meets ImplC
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do", "use.run -> impls.ImplB.Do"]
    );
}

#[test]
fn the_type_a_test_function_builds_is_a_target_of_the_test_caller() {
    let test = "package main\n\nimport \"testing\"\n\nfunc newC() Doer { return ImplC{} }\n\nfunc TestC(t *testing.T) {\n\t_ = checkC(newC())\n}\n\nfunc checkC(d Doer) error { return d.Do() }\n";
    let files = with(&base(), ("c_test.go", test));
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        dispatch_impl(&indexer, "c_test.checkC"),
        vec![
            "c_test.checkC -> impls.ImplA.Do",
            "c_test.checkC -> impls.ImplB.Do",
            "c_test.checkC -> impls.ImplC.Do"
        ]
    );
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do", "use.run -> impls.ImplB.Do"]
    );
}

#[test]
fn a_generic_instantiation_is_not_a_converted_type() {
    let generic = "package main\n\ntype Box[T any] struct{ v T }\n\nfunc (b *Box[T]) Do() error { return nil }\n\nfunc NewBox() Doer { return &Box[int]{} }\n";
    let files = with(
        &with(&base(), ("generic.go", generic)),
        (
            "main.go",
            "package main\n\nfunc main() {\n\t_ = run(NewBox())\n\t_ = run(&ImplA{})\n}\n",
        ),
    );
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do"]
    );
}

#[test]
fn a_package_variable_set_by_a_func_literal_is_typed_by_its_result() {
    let files = vec![
        ("go.mod", GO_MOD),
        (
            "a.go",
            "package main\n\ntype T struct{}\n\nfunc (t *T) Go() {}\n\nvar shared = func() *T {\n\tt := T{}\n\treturn &t\n}()\n\nfunc main() {\n\tshared.Go()\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(edges(&indexer, "CALLS", "a.main"), vec!["a.main -> a.T.Go"]);
}

#[test]
fn incremental_sync_equals_a_fresh_reindex() {
    let (_tmp, root, mut indexer) = indexed(&base());
    // ImplC gets built by a function that runs
    let main = "package main\n\nfunc NewA() Doer { return &ImplA{} }\n\nfunc unused() ImplC { return ImplC{} }\n\nfunc main() {\n\t_ = run(NewA())\n\t_ = run(ImplC{})\n}\n";
    sync(&mut indexer, &root, &[("main.go", main)]);
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do", "use.run -> impls.ImplC.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &with(&base(), ("main.go", main)));
    // ImplB stops implementing Doer
    let impls = "package main\n\ntype ImplA struct{}\n\nfunc (a *ImplA) Do() error { return nil }\n\ntype ImplB struct{}\n\nfunc (b ImplB) Do(x int) error { return nil }\n\ntype ImplC struct{}\n\nfunc (c ImplC) Do() error { return nil }\n";
    sync(&mut indexer, &root, &[("impls.go", impls)]);
    let final_files = with(&with(&base(), ("main.go", main)), ("impls.go", impls));
    assert_eq!(
        dispatch_impl(&indexer, "use.run"),
        vec!["use.run -> impls.ImplA.Do", "use.run -> impls.ImplC.Do"]
    );
    assert_incremental_equals_fresh(&indexer, &final_files);
}

/// `(target, confidence, evidence)` of the derived edges of the callers
/// matching `source_like`.
fn derived_rows(indexer: &Indexer, source_like: &str) -> Vec<(String, f64, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname, e.confidence, COALESCE(e.evidence_snippet, '')
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.resolution_kind = 'dispatch_impl'
               AND e.graph_version = ?1 AND s.qualname LIKE ?2
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source_like], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn a_derived_edge_is_less_than_certain_and_names_its_evidence() {
    // `orphan` calls the interface too, but nothing runs it
    let use_go = "package main\n\nfunc run(d Doer) error {\n\treturn d.Do()\n}\n\nfunc orphan(d Doer) error {\n\treturn d.Do()\n}\n";
    let files = with(&base(), ("use.go", use_go));
    let (_tmp, _root, indexer) = indexed(&files);
    let reached = derived_rows(&indexer, "use.run");
    assert_eq!(reached.len(), 2, "{reached:?}");
    for (target, confidence, evidence) in &reached {
        assert!(
            *confidence < 1.0 && *confidence > 0.5,
            "{target}: {confidence}"
        );
        assert!(evidence.contains("doer.Doer.Do"), "{evidence}");
        assert!(!evidence.contains("not reached"), "{evidence}");
    }
    // the evidence names the function that converts the type
    assert!(reached[0].0.ends_with("ImplA.Do") && reached[0].2.contains("main.NewA"));
    assert!(reached[1].0.ends_with("ImplB.Do") && reached[1].2.contains("main.main"));
    let unreached = derived_rows(&indexer, "use.orphan");
    assert_eq!(unreached.len(), 2, "{unreached:?}");
    for (target, confidence, evidence) in &unreached {
        assert!(*confidence < reached[0].1, "{target}: {confidence}");
        assert!(evidence.contains("not reached"), "{evidence}");
    }
    // the evidence points at the call site
    let gv = indexer.db().current_graph_version().unwrap();
    let line: Option<i64> = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT e.evidence_start_line FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.resolution_kind = 'dispatch_impl' AND s.qualname = 'use.run'
               AND e.graph_version = ?1 LIMIT 1",
            rusqlite::params![gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(line, Some(4));
}

const ROOT_MOD: &str = "module example.com/root\n\ngo 1.22\n";
const EX_MOD: &str = "module example.com/examples\n\ngo 1.22\n\nrequire example.com/root v0.0.0\n\nreplace example.com/root => ../\n";
const ROOT_DOER: &str = "package root\n\ntype Doer interface {\n\tDo() error\n}\n\nfunc Run(d Doer) error {\n\treturn d.Do()\n}\n\ntype Base struct{}\n\nfunc (b *Base) Do() error { return nil }\n\nfunc NewBase() Doer { return &Base{} }\n";
const EX_MAIN: &str = "package main\n\nimport \"example.com/root\"\n\ntype Mine struct{}\n\nfunc (m *Mine) Do() error { return nil }\n\nfunc drive(d root.Doer) error {\n\treturn d.Do()\n}\n\nfunc main() {\n\t_ = root.Run(root.NewBase())\n\t_ = root.Run(&Mine{})\n\t_ = drive(&Mine{})\n}\n";

fn modules() -> Vec<(&'static str, &'static str)> {
    vec![
        ("go.mod", ROOT_MOD),
        ("doer.go", ROOT_DOER),
        ("examples/go.mod", EX_MOD),
        ("examples/main.go", EX_MAIN),
    ]
}

#[test]
fn a_caller_reaches_only_implementers_of_modules_it_can_see() {
    let (_tmp, _root, indexer) = indexed(&modules());
    // the root module does not require `examples`, so `Run` does not reach
    // the example's type even though the example passes one in
    let from_root = derived_rows(&indexer, "doer.Run");
    let targets: Vec<&str> = from_root.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(targets, vec!["doer.Base.Do"], "{from_root:?}");
    // `examples` requires the root module and sees both
    let from_example = derived_rows(&indexer, "%main.drive");
    let targets: Vec<&str> = from_example.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(
        targets,
        vec!["doer.Base.Do", "examples/main.Mine.Do"],
        "{from_example:?}"
    );
}

#[test]
fn a_go_work_lets_every_module_see_every_other() {
    let mut files = modules();
    files.push(("go.work", "go 1.22\n\nuse (\n\t.\n\t./examples\n)\n"));
    let (_tmp, _root, indexer) = indexed(&files);
    let from_root = derived_rows(&indexer, "doer.Run");
    assert_eq!(from_root.len(), 2, "{from_root:?}");
}

#[test]
fn editing_a_go_mod_moves_the_bound_like_a_fresh_reindex() {
    let (_tmp, root, mut indexer) = indexed(&modules());
    // the root module now requires the examples module
    let root_mod = "module example.com/root\n\ngo 1.22\n\nrequire example.com/examples v0.0.0\n\nreplace example.com/examples => ./examples\n";
    sync(
        &mut indexer,
        &root,
        &[("go.mod", root_mod), ("doer.go", ROOT_DOER)],
    );
    assert_eq!(derived_rows(&indexer, "doer.Run").len(), 2);
    let final_files = with(&modules(), ("go.mod", root_mod));
    assert_incremental_equals_fresh(&indexer, &final_files);
    sync(
        &mut indexer,
        &root,
        &[("go.mod", ROOT_MOD), ("doer.go", ROOT_DOER)],
    );
    assert_eq!(derived_rows(&indexer, "doer.Run").len(), 1);
    assert_incremental_equals_fresh(&indexer, &modules());
}

#[test]
fn a_changed_confidence_is_updated_in_place() {
    let (_tmp, root, mut indexer) = indexed(&base());
    let before = derived_rows(&indexer, "use.run");
    assert!(!before.is_empty());
    // `run` is no longer called: same dispatch edge, lower confidence
    let main = "package main\n\nfunc NewA() Doer { return &ImplA{} }\n\nfunc main() {\n\t_ = NewA()\n\t_ = ImplB{}\n}\n";
    sync(&mut indexer, &root, &[("main.go", main)]);
    let after = derived_rows(&indexer, "use.run");
    assert!(
        after.is_empty() || after.iter().all(|a| a.1 < before[0].1),
        "{after:?}"
    );
}

#[test]
fn a_calls_only_trace_leaves_the_derived_fan_out_out() {
    use lidx::rpc;
    use serde_json::json;
    let (_tmp, _root, mut indexer) = indexed(&base());
    let mut paths = |kinds: serde_json::Value| -> i64 {
        let r = rpc::handle_method(
            &mut indexer,
            "trace_flow",
            json!({"start_qualname": "use.run", "max_hops": 3, "kinds": kinds}),
        )
        .unwrap();
        r["paths_found"].as_i64().unwrap_or_else(|| panic!("{r}"))
    };
    // DISPATCH is followed (the default): the call reaches both implementers
    assert_eq!(paths(json!(["CALLS", "DISPATCH"])), 3);
    // CALLS alone: the inferred fan-out is declined, not a pile of paths
    assert_eq!(paths(json!(["CALLS"])), 0);
}
