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
