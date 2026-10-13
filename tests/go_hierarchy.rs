//! Go type hierarchy (issues #455, #456): a method declared in another file
//! than its receiver type still belongs to the type (so the dispatch query
//! pairs it with the interface method), and struct embedding is an `EXTENDS`
//! edge (so a type satisfying an interface through an embedded type, even a
//! standard-library one, implements it). Every incremental scenario ends
//! equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const DOER: &str = "package app\n\ntype Doer interface {\n\tDo() error\n}\n";
const TYPES: &str = "package app\n\ntype Impl struct{}\n\ntype Other struct{}\n";
const METHODS: &str = "package app\n\nfunc (i *Impl) Do() error { return nil }\n";

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-hier-", files);
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

/// `(base qualname, implementing qualname)` of every dispatch pair.
fn pairs(indexer: &Indexer) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let mut out: Vec<String> = indexer
        .db()
        .all_dispatch_pairs(gv)
        .unwrap()
        .into_iter()
        .map(|(b, i)| {
            let q = |id| indexer.db().get_symbol_by_id(id).unwrap().unwrap().qualname;
            format!("{} <- {}", q(b), q(i))
        })
        .collect();
    out.sort();
    out
}

fn sync(indexer: &mut Indexer, root: &std::path::Path, files: &[(&str, &str)]) {
    common::write_files(root, files);
    let paths: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&paths).unwrap();
}

fn remove(indexer: &mut Indexer, root: &std::path::Path, path: &str) {
    std::fs::remove_file(root.join(path)).unwrap();
    indexer.sync_rel_paths(&[path.to_string()]).unwrap();
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
    // the dispatch pairs agree too
    let (_t, _r, fresh_indexer) = indexed(final_files);
    assert_eq!(pairs(indexer), pairs(&fresh_indexer));
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

fn split() -> Vec<(&'static str, &'static str)> {
    vec![
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("types.go", TYPES),
        ("methods.go", METHODS),
    ]
}

#[test]
fn a_method_in_another_file_than_its_type_belongs_to_the_type() {
    let (_tmp, _root, indexer) = indexed(&split());
    assert_eq!(
        edges(&indexer, "CONTAINS", "types.Impl"),
        vec!["types.Impl -> methods.Impl.Do"]
    );
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "types.%"),
        vec!["types.Impl -> doer.Doer"]
    );
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- methods.Impl.Do"]);
}

#[test]
fn the_dispatch_peers_of_a_split_method_are_found_from_either_side() {
    let (_tmp, _root, indexer) = indexed(&split());
    let gv = indexer.db().current_graph_version().unwrap();
    let id = |q: &str| {
        indexer
            .db()
            .get_symbol_by_qualname(q, gv)
            .unwrap()
            .unwrap()
            .id
    };
    let (spec, imp) = (id("doer.Doer.Do"), id("methods.Impl.Do"));
    assert_eq!(
        indexer.db().dispatch_pairs(&[imp], gv).unwrap(),
        vec![(spec, imp)]
    );
    assert_eq!(
        indexer.db().dispatch_pairs(&[spec], gv).unwrap(),
        vec![(spec, imp)]
    );
    let peers = indexer.db().dispatch_peers(imp, gv).unwrap();
    assert_eq!(peers.interface_methods, vec![spec]);
}

#[test]
fn a_method_in_the_same_file_keeps_its_own_container() {
    let same = "package app\n\ntype Impl struct{}\n\nfunc (i *Impl) Do() error { return nil }\n";
    let files = [("go.mod", GO_MOD), ("doer.go", DOER), ("impl.go", same)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- impl.Impl.Do"]);
    // no derived containment edge: the extractor's own edge resolves
    let derived: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM edges WHERE kind = 'CONTAINS' AND derived <> 0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(derived, 0);
}

#[test]
fn a_method_of_a_type_in_another_package_directory_is_not_joined() {
    // `sub.Impl` and `app.Impl` share a name only
    let sub_types = "package sub\n\ntype Impl struct{}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("sub/types.go", sub_types),
        ("methods.go", METHODS),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert!(edges(&indexer, "CONTAINS", "sub/types.Impl").is_empty());
    assert!(pairs(&indexer).is_empty());
}

#[test]
fn moving_a_method_or_its_type_between_files_matches_a_fresh_reindex() {
    let files = split();
    let (_tmp, root, mut indexer) = indexed(&files);
    assert_incremental_equals_fresh(&indexer, &files);

    // the method moves to a third file
    let moved = "package app\n\nfunc (i *Impl) Do() error { return nil }\n";
    sync(&mut indexer, &root, &[("elsewhere.go", moved)]);
    remove(&mut indexer, &root, "methods.go");
    let step1: Vec<_> = files
        .iter()
        .filter(|(p, _)| *p != "methods.go")
        .copied()
        .chain([("elsewhere.go", moved)])
        .collect();
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- elsewhere.Impl.Do"]);
    assert_incremental_equals_fresh(&indexer, &step1);

    // the type moves: the method follows it without being touched
    let types2 = "package app\n\ntype Other struct{}\n";
    let impl_file = "package app\n\ntype Impl struct{}\n";
    sync(
        &mut indexer,
        &root,
        &[("types.go", types2), ("impl.go", impl_file)],
    );
    let step2: Vec<_> = with(&with(&step1, ("types.go", types2)), ("impl.go", impl_file));
    assert_eq!(
        edges(&indexer, "CONTAINS", "impl.Impl"),
        vec!["impl.Impl -> elsewhere.Impl.Do"]
    );
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- elsewhere.Impl.Do"]);
    assert_incremental_equals_fresh(&indexer, &step2);

    // the type is deleted: the method is an orphan, nothing pairs
    remove(&mut indexer, &root, "impl.go");
    let step3: Vec<_> = step2
        .iter()
        .filter(|(p, _)| *p != "impl.go")
        .copied()
        .collect();
    assert!(pairs(&indexer).is_empty());
    assert!(edges(&indexer, "CONTAINS", "impl.%").is_empty());
    assert_incremental_equals_fresh(&indexer, &step3);

    // and back
    sync(&mut indexer, &root, &[("impl.go", impl_file)]);
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- elsewhere.Impl.Do"]);
    assert_incremental_equals_fresh(&indexer, &step2);
}

#[test]
fn a_type_declared_in_several_constrained_files_takes_the_methods_of_the_selected_twin() {
    let linux = "//go:build linux\n\npackage app\n\ntype Impl struct{}\n";
    let windows = "//go:build windows\n\npackage app\n\ntype Impl struct{}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("impl_linux.go", linux),
        ("impl_windows.go", windows),
        ("methods.go", METHODS),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "CONTAINS", "%.Impl"),
        vec!["impl_linux.Impl -> methods.Impl.Do"]
    );
}

const BASE: &str =
    "package app\n\ntype Base struct{}\n\nfunc (b *Base) Do() error { return nil }\n";

#[test]
fn struct_embedding_is_an_extends_edge() {
    let files = [
        ("go.mod", GO_MOD),
        ("base.go", BASE),
        (
            "wrap.go",
            "package app\n\ntype Plain struct {\n\tBase\n\tn int\n}\n\ntype Ptr struct {\n\t*Base\n}\n\ntype Named struct {\n\tb Base\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    // an embedded field, by value or pointer; a named field is no embedding
    assert_eq!(
        edges(&indexer, "EXTENDS", "wrap.%"),
        vec!["wrap.Plain -> base.Base", "wrap.Ptr -> base.Base"]
    );
}

#[test]
fn embedding_resolves_across_files_generics_packages_and_interfaces() {
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("box.go", "package app\n\ntype Box[T any] struct{ v T }\n"),
        (
            "sub/inner.go",
            "package sub\n\ntype Inner struct{}\n\nfunc (Inner) Do() error { return nil }\n",
        ),
        (
            "wrap.go",
            "package app\n\nimport (\n\t\"example.com/m/sub\"\n\t\"net/http\"\n)\n\ntype G struct {\n\tBox[int]\n}\n\ntype P struct {\n\tsub.Inner\n}\n\ntype I struct {\n\tDoer\n}\n\ntype F struct {\n\thttp.Handler\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "EXTENDS", "wrap.%"),
        vec![
            "wrap.G -> box.Box",
            "wrap.I -> doer.Doer",
            "wrap.P -> sub/inner.Inner"
        ]
    );
}

#[test]
fn adding_and_removing_an_embedding_matches_a_fresh_reindex() {
    let plain = "package app\n\ntype Wrap struct {\n\tn int\n}\n";
    let embedding = "package app\n\ntype Wrap struct {\n\tBase\n}\n";
    let files = [("go.mod", GO_MOD), ("base.go", BASE), ("wrap.go", plain)];
    let (_tmp, root, mut indexer) = indexed(&files);
    assert!(edges(&indexer, "EXTENDS", "wrap.%").is_empty());
    assert_incremental_equals_fresh(&indexer, &files);

    sync(&mut indexer, &root, &[("wrap.go", embedding)]);
    assert_eq!(
        edges(&indexer, "EXTENDS", "wrap.%"),
        vec!["wrap.Wrap -> base.Base"]
    );
    let embedded = with(&files, ("wrap.go", embedding));
    assert_incremental_equals_fresh(&indexer, &embedded);

    // the embedded type goes away: the edge goes with it
    remove(&mut indexer, &root, "base.go");
    assert!(edges(&indexer, "EXTENDS", "wrap.%").is_empty());
    let without_base: Vec<_> = embedded
        .iter()
        .filter(|(p, _)| *p != "base.go")
        .copied()
        .collect();
    assert_incremental_equals_fresh(&indexer, &without_base);

    // and comes back
    sync(&mut indexer, &root, &[("base.go", BASE)]);
    assert_eq!(
        edges(&indexer, "EXTENDS", "wrap.%"),
        vec!["wrap.Wrap -> base.Base"]
    );
    assert_incremental_equals_fresh(&indexer, &embedded);

    // the embedding is dropped
    sync(&mut indexer, &root, &[("wrap.go", plain)]);
    assert!(edges(&indexer, "EXTENDS", "wrap.%").is_empty());
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn a_type_embedding_an_implementer_implements_through_promotion() {
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("base.go", BASE),
        (
            "wrap.go",
            "package app\n\ntype Wrap struct {\n\t*Base\n}\n\ntype Nope struct {\n\tn int\n}\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "wrap.%"),
        vec!["wrap.Wrap -> doer.Doer"]
    );
    // the embedded type's method is the one that implements: no pair for Wrap
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- base.Base.Do"]);
}

#[test]
fn a_type_embedding_a_standard_library_interface_implements_through_it() {
    let iface = "package app\n\nimport \"net/http\"\n\ntype Wrapped interface {\n\thttp.ResponseWriter\n\tStatus() int\n}\n";
    let writers = "package app\n\nimport \"net/http\"\n\ntype basic struct {\n\thttp.ResponseWriter\n\tcode int\n}\n\nfunc (b *basic) Status() int { return b.code }\n\ntype fancy struct {\n\t*basic\n}\n\ntype lacking struct {\n\thttp.ResponseWriter\n}\n";
    let files = [("go.mod", GO_MOD), ("iface.go", iface), ("w.go", writers)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "w.%"),
        vec!["w.basic -> iface.Wrapped", "w.fancy -> iface.Wrapped"]
    );
    // foreign embedded types are no edges of the repository
    assert_eq!(
        edges(&indexer, "EXTENDS", "w.%"),
        vec!["w.fancy -> w.basic"]
    );
}

#[test]
fn a_type_embedding_exec_cmd_gets_its_run_method() {
    let iface = "package app\n\ntype Command interface {\n\tRun() error\n\tSetDir(string)\n}\n";
    let cmd = "package app\n\nimport \"os/exec\"\n\ntype osCommand struct {\n\t*exec.Cmd\n}\n\nfunc (c osCommand) SetDir(d string) { c.Dir = d }\n";
    let files = [("go.mod", GO_MOD), ("iface.go", iface), ("cmd.go", cmd)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "cmd.%"),
        vec!["cmd.osCommand -> iface.Command"]
    );
}

#[test]
fn embedding_adds_no_override_pair_of_its_own() {
    // `Wrap` has its own `Do`, different from `Doer.Do`: no pair for it,
    // although the embedded `Base.Do` implements
    let files = [
        ("go.mod", GO_MOD),
        ("doer.go", DOER),
        ("base.go", BASE),
        (
            "wrap.go",
            "package app\n\ntype Wrap struct {\n\tBase\n}\n\nfunc (w Wrap) Do(x int) error { return nil }\n",
        ),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert!(edges(&indexer, "IMPLEMENTS", "wrap.%").is_empty());
    assert_eq!(pairs(&indexer), vec!["doer.Doer.Do <- base.Base.Do"]);
}

#[test]
fn promotion_through_an_embedded_exec_cmd_covers_output_and_the_pipes() {
    let iface = "package app\n\nimport \"io\"\n\ntype Runner interface {\n\tOutput() ([]byte, error)\n\tStdoutPipe() (io.ReadCloser, error)\n\tString() string\n}\n\ntype Wrong interface {\n\tOutput() (string, error)\n}\n";
    let cmd = "package app\n\nimport \"os/exec\"\n\ntype osCommand struct {\n\t*exec.Cmd\n}\n";
    let files = [("go.mod", GO_MOD), ("iface.go", iface), ("cmd.go", cmd)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        edges(&indexer, "IMPLEMENTS", "cmd.%"),
        vec!["cmd.osCommand -> iface.Runner"]
    );
}
