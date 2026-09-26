//! Issue #77 incremental-sync regressions: repair-pass coverage past large
//! unresolved sets, Python import fallbacks, ambiguity-driven unbinding,
//! delete-then-restore, and overload collapse. Each incremental scenario
//! asserts its post-sync snapshot matches a fresh full reindex of the same
//! final tree (`common::assert_matches_fresh`).

mod common;

use common::golden::{self, EdgeKey, ExpectedEdge};
use lidx::indexer::Indexer;
use std::path::PathBuf;

/// Precision/recall floors for the golden/python fixture-based scenarios
/// below -- see `golden/python/expected_edges.txt`'s header. Raise these
/// only alongside a real resolver fix that moves the measured baseline.
const PRECISION_FLOOR: f64 = 1.0;
const RECALL_FLOOR: f64 = 1.0;

fn expected_edges() -> Vec<ExpectedEdge> {
    let text =
        std::fs::read_to_string(golden::fixture_path("golden/python/expected_edges.txt")).unwrap();
    golden::parse_expected_edges(&text)
}

fn fixture_modules() -> std::collections::HashSet<String> {
    golden::fixture_source_modules("golden/python")
}

/// Shared setup for every synthetic (non-fixture) scenario below that syncs
/// more files after its initial reindex: materializes `files` into a fresh
/// temp dir prefixed `lidx-<label>-` and runs one full reindex, returning
/// the indexer (and its temp dir guard, and repo root for further
/// `write_files` calls) ready for the test's incremental step.
fn indexed_tree(label: &str, files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix(&format!("lidx-{label}-"))
        .tempdir()
        .unwrap();
    let repo_root = tmp.path().to_path_buf();
    common::write_files(&repo_root, files);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo_root, indexer)
}

/// Noise: `count` calls to distinct names nothing ever defines, all
/// in one function -- a large pile of permanently-unresolved rows in the
/// same graph version, so the repair pass has more than a handful of NULL
/// targets to work through.
fn many_undefined_calls_source(count: usize) -> String {
    let mut src = String::from("def caller():\n");
    for i in 0..count {
        src.push_str(&format!("    undefined_{i}()\n"));
    }
    src
}

/// 1200 calls to names nothing defines can never resolve; the repair pass
/// must still resolve `z.py`'s edges once `lib.py` appears, not be thrown
/// off by that surrounding noise.
#[test]
fn incremental_add_file_resolves_edges_behind_many_unresolved_ones() {
    let a_py = many_undefined_calls_source(1200);
    let lib_py = "def helper() -> None:\n    pass\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "many-unresolved",
        &[("a.py", &a_py), ("z.py", "import lib\n")],
    );

    common::write_files(&repo_root, &[("lib.py", lib_py)]);
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    for kind in ["IMPORTS", "IMPORTS_FILE"] {
        let edge = snapshot
            .iter()
            .find(|e| e.source_qualname == "z" && e.kind == kind)
            .unwrap_or_else(|| panic!("z must have a {kind} edge: {snapshot:#?}"));
        assert_eq!(
            edge.target_qualname.as_deref(),
            Some("lib"),
            "z's {kind} edge must reattach to lib once lib.py exists, even with 1200 \
             never-resolving NULL-target rows ahead of it in the repair pass: {edge:?}"
        );
    }

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("a.py", &a_py),
        ("z.py", "import lib\n"),
        ("lib.py", lib_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `from pkg.mod import X` with neither candidate on disk: once
/// `pkg/mod.py` appears, `IMPORTS_FILE` must bind to the module `pkg.mod`,
/// not the function `pkg.mod.X`, matching a fresh reindex.
#[test]
fn incremental_add_module_file_binds_imports_file_to_module_not_imported_name() {
    let init_py = "";
    let main_py = "from pkg.mod import X\n";
    let mod_py = "def X() -> None:\n    pass\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "module-not-name",
        &[("pkg/__init__.py", init_py), ("main.py", main_py)],
    );

    common::write_files(&repo_root, &[("pkg/mod.py", mod_py)]);
    indexer.sync_rel_paths(&["pkg/mod.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let imports_file = snapshot
        .iter()
        .find(|e| e.source_qualname == "main" && e.kind == "IMPORTS_FILE")
        .unwrap_or_else(|| panic!("main must have an IMPORTS_FILE edge: {snapshot:#?}"));
    assert_eq!(
        imports_file.target_qualname.as_deref(),
        Some("pkg.mod"),
        "IMPORTS_FILE must bind to the module pkg.mod, not the imported name pkg.mod.X: \
         {imports_file:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("pkg/__init__.py", init_py),
        ("main.py", main_py),
        ("pkg/mod.py", mod_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `from . import x` at the repo root: the relative base (`.`) absolutizes
/// to nothing there, so the only usable candidate is the imported name
/// itself. Before `x.py` exists, no candidate resolves to a real file; once
/// it's added and synced, `main` must end up with an `IMPORTS_FILE` edge
/// into it, exactly as a fresh reindex of the final tree would.
#[test]
fn incremental_add_file_for_relative_import_creates_imports_file_edge() {
    let main_py = "from . import x\n";
    let x_py = "";

    let (_tmp, repo_root, mut indexer) = indexed_tree("relative-import", &[("main.py", main_py)]);

    common::write_files(&repo_root, &[("x.py", x_py)]);
    indexer.sync_rel_paths(&["x.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let imports_file = snapshot
        .iter()
        .find(|e| e.source_qualname == "main" && e.kind == "IMPORTS_FILE")
        .unwrap_or_else(|| panic!("main must have an IMPORTS_FILE edge: {snapshot:#?}"));
    assert_eq!(
        imports_file.target_qualname.as_deref(),
        Some("x"),
        "main's IMPORTS_FILE edge must bind to x once x.py exists, matching a fresh reindex: \
         {imports_file:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[("main.py", main_py), ("x.py", x_py)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `from pkg import mod` with neither `pkg/` nor `pkg/mod.py` on disk yet:
/// once both are added and synced together, `IMPORTS_FILE` must bind to the
/// more specific candidate `pkg.mod`, matching a fresh reindex.
#[test]
fn incremental_add_package_and_module_binds_imports_file_to_specific_module() {
    let main_py = "from pkg import mod\n";
    let init_py = "";
    let mod_py = "def helper() -> None:\n    pass\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("add-package-and-module", &[("main.py", main_py)]);

    common::write_files(
        &repo_root,
        &[("pkg/__init__.py", init_py), ("pkg/mod.py", mod_py)],
    );
    indexer
        .sync_rel_paths(&["pkg/__init__.py".to_string(), "pkg/mod.py".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let imports_file = snapshot
        .iter()
        .find(|e| e.source_qualname == "main" && e.kind == "IMPORTS_FILE")
        .unwrap_or_else(|| panic!("main must have an IMPORTS_FILE edge: {snapshot:#?}"));
    assert_eq!(
        imports_file.target_qualname.as_deref(),
        Some("pkg.mod"),
        "IMPORTS_FILE must bind to the specific module pkg.mod once both pkg/__init__.py and \
         pkg/mod.py exist, matching a fresh reindex: {imports_file:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("main.py", main_py),
        ("pkg/__init__.py", init_py),
        ("pkg/mod.py", mod_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `from pkg import mod` with only `pkg/__init__.py` on disk: `IMPORTS_FILE`
/// binds to the package fallback `pkg`, the only candidate that exists yet.
/// Adding `pkg/mod.py` afterward, syncing only that file (never
/// `main.py`), must rebind the edge to the now-more-specific `pkg.mod`, not
/// leave it on the stale fallback -- matching a fresh reindex of the final
/// tree.
#[test]
fn incremental_add_module_file_rebinds_imports_file_from_package_fallback() {
    let main_py = "from pkg import mod\n";
    let init_py = "";
    let mod_py = "def helper() -> None:\n    pass\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "rebind-fallback",
        &[("pkg/__init__.py", init_py), ("main.py", main_py)],
    );

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot_before = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let bound_before = snapshot_before
        .iter()
        .find(|e| e.source_qualname == "main" && e.kind == "IMPORTS_FILE")
        .and_then(|e| e.target_qualname.as_deref());
    assert_eq!(
        bound_before,
        Some("pkg"),
        "precondition: IMPORTS_FILE must fall back to the package pkg before pkg/mod.py exists: \
         {snapshot_before:#?}"
    );

    common::write_files(&repo_root, &[("pkg/mod.py", mod_py)]);
    indexer.sync_rel_paths(&["pkg/mod.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let imports_file = snapshot
        .iter()
        .find(|e| e.source_qualname == "main" && e.kind == "IMPORTS_FILE")
        .unwrap_or_else(|| panic!("main must have an IMPORTS_FILE edge: {snapshot:#?}"));
    assert_eq!(
        imports_file.target_qualname.as_deref(),
        Some("pkg.mod"),
        "IMPORTS_FILE must rebind from the package fallback pkg to the more specific pkg.mod \
         once pkg/mod.py appears, matching a fresh reindex: {imports_file:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("pkg/__init__.py", init_py),
        ("main.py", main_py),
        ("pkg/mod.py", mod_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// An import-bound bare call stores a caller-module placeholder as its
/// `target_qualname`; adding a same-qualname class to its target's module
/// must still unbind it, via the resolved target's now-ambiguous qualname.
#[test]
fn incremental_add_ambiguous_symbol_unbinds_import_bound_bare_call() {
    let other_module_initial = "def local_util() -> str:\n    return \"util\"\n";
    let other_module_ambiguous = "\
def local_util() -> str:
    return \"util\"


class local_util:
    pass
";
    let downstream_py = "\
from other_module import local_util


def use() -> str:
    return local_util()
";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ambiguous-bare-call",
        &[
            ("other_module.py", other_module_initial),
            ("downstream.py", downstream_py),
        ],
    );

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot_before = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let bound_before = snapshot_before.iter().any(|e| {
        e.source_qualname == "downstream.use"
            && e.kind == "CALLS"
            && e.target_qualname.as_deref() == Some("other_module.local_util")
    });
    assert!(
        bound_before,
        "precondition: downstream.use must resolve before the ambiguous addition: \
         {snapshot_before:#?}"
    );

    common::write_files(&repo_root, &[("other_module.py", other_module_ambiguous)]);
    indexer
        .sync_rel_paths(&["other_module.py".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let still_bound = snapshot.iter().any(|e| {
        e.source_qualname == "downstream.use" && e.kind == "CALLS" && e.target_qualname.is_some()
    });
    assert!(
        !still_bound,
        "downstream.use's import-bound bare call must go unresolved once other_module.local_util \
         is ambiguous, not stay bound to the old function: {snapshot:#?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("other_module.py", other_module_ambiguous),
        ("downstream.py", downstream_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Restored `caller.py` content for the delete-then-restore regression
/// below -- deliberately different from the original fixture (a
/// byte-identical restore is hash-skipped by `sync_abs_paths`'s own change
/// detection, a separate, pre-existing gap this test doesn't cover), but
/// still defines `entry` so `downstream.py`'s import and call into it have
/// something to reattach to.
const RESTORED_CALLER_SOURCE: &str = "\
def entry() -> str:
    \"\"\"Restored after caller.py was deleted -- issue #77.\"\"\"
    return \"restored\"
";

fn write_restored_caller(root: &std::path::Path) {
    std::fs::write(root.join("caller.py"), RESTORED_CALLER_SOURCE).unwrap();
}

/// `expected_edges()`, transformed for the state after `caller.py` is
/// deleted and then restored with `RESTORED_CALLER_SOURCE`: every line
/// sourced from `caller.py`'s original body is dropped along with the
/// functions that no longer exist; `downstream.use_entry`'s call is left
/// as-is, since it must still resolve to the restored `caller.entry`.
fn expected_edges_after_caller_restored() -> Vec<ExpectedEdge> {
    expected_edges()
        .into_iter()
        .filter(|edge| !edge.key.source_qualname.starts_with("caller."))
        .collect()
}

/// Delete an imported file, then
/// restore it with different content, syncing only that one path both
/// times -- `downstream.py` (never itself resynced) must end up with its
/// `IMPORTS_FILE`/`CALLS` edges into `caller` resolved again, exactly as a
/// fresh reindex of the final tree would have them, not left permanently
/// unresolved just because the delete happened to run first.
///
/// `downstream.py`'s `IMPORTS_FILE` edge is written once, when
/// `downstream.py` is itself extracted, with a `target_qualname` of
/// `caller` regardless of whether `caller.py` exists at that moment
/// (`python::resolve_import_file_edges`) -- the same edge row survives
/// `caller.py`'s deletion (its `target_symbol_id` nulled by the `edges`
/// foreign key, see issue #76) and its restoration; `caller`'s module
/// symbol reappearing is what gives `resolve_null_target_edges`'s exact
/// tier something to rebind it to. No re-extraction of `downstream.py`
/// itself is needed either way.
#[test]
fn incremental_delete_then_restore_reattaches_incoming_edges() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    std::fs::remove_file(&caller_path).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    write_restored_caller(&repo_root);
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let incoming_target = snapshot
        .iter()
        .find(|edge| edge.source_qualname == "downstream.use_entry" && edge.kind == "CALLS")
        .and_then(|edge| edge.target_qualname.as_deref());
    assert_eq!(
        incoming_target,
        Some("caller.entry"),
        "downstream.use_entry's edge must reattach to the restored caller.entry, not stay \
         permanently unresolved just because caller.py was deleted before it came back"
    );

    let report = golden::compare(
        &snapshot,
        &expected_edges_after_caller_restored(),
        &fixture_modules(),
    );
    report.assert_floors(
        "python (post-delete-restore)",
        PRECISION_FLOOR,
        RECALL_FLOOR,
    );

    let fresh = common::fresh_reindex_snapshot("golden/python", write_restored_caller);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Issue #77: same shape as the delete-then-restore test above, but with
/// an unrelated file synced in between the delete and the restore --
/// `downstream.py` must still end up resolved, not missed because it only
/// gets "one deferred second chance". The redesign has no such
/// second-chance bookkeeping at all: every later sync's
/// `resolve_null_target_edges` call re-tries every still-NULL edge in the
/// whole graph, not just ones from files that sync touched, so an
/// intervening unrelated sync changes nothing about when `downstream.py`'s
/// edges get retried.
#[test]
fn incremental_delete_then_restore_across_intervening_sync_reattaches_incoming_edges() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    std::fs::remove_file(&caller_path).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let helper_path = repo_root.join("helper.py");
    let mut helper_src = std::fs::read_to_string(&helper_path).unwrap();
    helper_src.push_str("\n\ndef unrelated() -> str:\n    return \"unrelated\"\n");
    std::fs::write(&helper_path, helper_src).unwrap();
    indexer.sync_rel_paths(&["helper.py".to_string()]).unwrap();

    write_restored_caller(&repo_root);
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let incoming_target = snapshot
        .iter()
        .find(|edge| edge.source_qualname == "downstream.use_entry" && edge.kind == "CALLS")
        .and_then(|edge| edge.target_qualname.as_deref());
    assert_eq!(
        incoming_target,
        Some("caller.entry"),
        "downstream.use_entry's edge must reattach to the restored caller.entry even with an \
         unrelated sync in between the delete and the restore"
    );

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        write_restored_caller(root);
        let helper_path = root.join("helper.py");
        let mut helper_src = std::fs::read_to_string(&helper_path).unwrap();
        helper_src.push_str("\n\ndef unrelated() -> str:\n    return \"unrelated\"\n");
        std::fs::write(&helper_path, helper_src).unwrap();
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A fresh, temp-only file (never in the checked-in fixture) the ambiguity
/// test below adds, defining a single `g(x)` -- the target of
/// `downstream.py`'s extra call, before the incremental edit that makes
/// its qualname ambiguous.
const AMBIGUITY_TARGET_SOURCE: &str = "\
def g(x) -> int:
    return x
";

/// The incremental edit under test: `g`'s original signature is replaced
/// (its stable_id changes -- the row is deleted, not merely updated) and a
/// second, unrelated `g` (a class, not a function) is added alongside it.
/// Both now share the qualname `ambiguity_target.g`; neither is the symbol
/// `downstream.py`'s call originally bound to.
const AMBIGUITY_TARGET_AMBIGUOUS_SOURCE: &str = "\
def g(x, y) -> int:
    return x + y


class g:
    pass
";

/// The extra call this test gives `downstream.py`, purely in its temp-repo
/// copy -- an import-bound call into `ambiguity_target.g`.
const DOWNSTREAM_AMBIGUITY_CALL_SOURCE: &str = "\
from ambiguity_target import g


def use_ambiguity_target() -> str:
    return str(g(1))
";

/// Writes `ambiguity_target.py` (unambiguous) and gives `downstream.py` its
/// extra call into it -- the state both the incremental test reindexes from
/// and `write_ambiguity_target_ambiguous` edits away from.
fn write_ambiguity_initial_tree(root: &std::path::Path) {
    std::fs::write(root.join("ambiguity_target.py"), AMBIGUITY_TARGET_SOURCE).unwrap();
    let downstream_path = root.join("downstream.py");
    let mut downstream_src = std::fs::read_to_string(&downstream_path).unwrap();
    downstream_src.push_str(DOWNSTREAM_AMBIGUITY_CALL_SOURCE);
    std::fs::write(&downstream_path, downstream_src).unwrap();
}

fn write_ambiguity_target_ambiguous(root: &std::path::Path) {
    std::fs::write(
        root.join("ambiguity_target.py"),
        AMBIGUITY_TARGET_AMBIGUOUS_SOURCE,
    )
    .unwrap();
}

/// `expected_edges()`, plus one line for `use_ambiguity_target`'s call,
/// UNRESOLVED: `ambiguity_target.g` names two symbols after the edit below,
/// neither of which is the one the call originally bound to, so it must
/// stay unresolved rather than guess.
fn expected_edges_after_ambiguity_target_edited() -> Vec<ExpectedEdge> {
    let mut edges = expected_edges();
    edges.push(ExpectedEdge {
        key: EdgeKey {
            source_qualname: "downstream.use_ambiguity_target".to_string(),
            kind: "CALLS".to_string(),
            target_qualname: None,
            resolution_kind: None,
        },
        xfail: false,
    });
    edges
}

/// An edit that leaves a target's
/// qualname matching more than one symbol must leave an incoming edge into
/// it unresolved, not silently rebind to whichever candidate a bare
/// `ORDER BY id` happened to return first -- and an incremental sync must
/// agree with a fresh reindex on that, not merely on *a* pick.
#[test]
fn incremental_edit_creating_ambiguous_qualname_stays_unresolved() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    write_ambiguity_initial_tree(&repo_root);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let resolved_before = indexer
        .db()
        .get_symbol_by_qualname("ambiguity_target.g", graph_version)
        .unwrap();
    assert!(
        resolved_before.is_some(),
        "precondition: ambiguity_target.g must exist, unambiguously, before the edit"
    );

    write_ambiguity_target_ambiguous(&repo_root);
    indexer
        .sync_rel_paths(&["ambiguity_target.py".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let incoming = snapshot
        .iter()
        .find(|edge| {
            edge.source_qualname == "downstream.use_ambiguity_target" && edge.kind == "CALLS"
        })
        .expect("downstream.use_ambiguity_target must still have a CALLS edge");
    assert_eq!(
        incoming.target_qualname, None,
        "an edge into a now-ambiguous qualname must stay unresolved, not guess between the two \
         g candidates: {incoming:?}"
    );

    let report = golden::compare(
        &snapshot,
        &expected_edges_after_ambiguity_target_edited(),
        &fixture_modules(),
    );
    report.assert_floors(
        "python (post-ambiguous-edit)",
        PRECISION_FLOOR,
        RECALL_FLOOR,
    );

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        write_ambiguity_initial_tree(root);
        write_ambiguity_target_ambiguous(root);
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Issue #77: Python's `@overload` stub pattern -- several `def g` bodies
/// sharing one qualname, all in the same file, all kind `"function"` --
/// must still collapse to one target on a full reindex, not go unresolved
/// just because more than one symbol shares the name. A cross-module
/// `from p.b import g` call into it exercises the import tier's identical
/// fast path (`resolve_import`'s exact round calls the same
/// `Resolver::exact` that the plain exact tier does).
#[test]
fn full_reindex_resolves_python_overload_stubs_and_their_import() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::create_dir_all(root.join("p")).unwrap();
    std::fs::write(root.join("p").join("__init__.py"), "").unwrap();
    std::fs::write(
        root.join("p").join("b.py"),
        "from typing import overload\n\n\n\
         @overload\n\
         def g(x: int) -> int: ...\n\
         @overload\n\
         def g(x: str) -> str: ...\n\
         def g(x):\n    return x\n",
    )
    .unwrap();
    std::fs::write(
        root.join("caller2.py"),
        "from p.b import g\n\n\ndef use() -> object:\n    return g(1)\n",
    )
    .unwrap();

    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let bound = snapshot
        .iter()
        .find(|e| e.source_qualname == "caller2.use" && e.kind == "CALLS");
    assert_eq!(
        bound.and_then(|e| e.target_qualname.as_deref()),
        Some("p.b.g"),
        "an overload set (same file, same kind) must collapse to one target, not go unresolved: \
         {snapshot:#?}"
    );
}

/// Two symbols sharing a qualname in the *same* file but
/// of *different* kinds (a `class g` and a `def g`, not an overload set)
/// must stay unresolved on a full reindex -- both `Resolver::exact`'s SQL
/// fallback and the in-batch `build_exact_symbol_map` fast path apply the
/// identical rule, so neither can silently pick "whichever was inserted
/// last" the way the pre-#77 `symbol_map: HashMap<String, i64>` did.
#[test]
fn full_reindex_same_file_class_and_def_same_qualname_stays_unresolved() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(
        root.join("dupe.py"),
        "class g:\n    pass\n\n\ndef g():\n    return 1\n",
    )
    .unwrap();
    std::fs::write(
        root.join("caller2.py"),
        "from dupe import g\n\n\ndef use() -> object:\n    return g()\n",
    )
    .unwrap();

    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let bound = snapshot
        .iter()
        .find(|e| e.source_qualname == "caller2.use" && e.kind == "CALLS");
    assert_eq!(
        bound.and_then(|e| e.target_qualname.as_deref()),
        None,
        "a same-file, different-kind qualname clash must stay unresolved, never guess: \
         {snapshot:#?}"
    );
}

/// The extra call the ambiguity tests below give `downstream.py`: a
/// module-qualified call (`other_module.local_util()`) so its stored
/// `target_qualname` is the literal `other_module.local_util` and
/// resolves through the plain exact tier, which
/// `Db::unbind_edges_for_qualnames`'s qualname-text match can catch
/// directly once a second symbol shares that name.
const DOWNSTREAM_QUALIFIED_CALL_SOURCE: &str = "\
import other_module


def use_qualified_local_util() -> str:
    return other_module.local_util()
";

fn give_downstream_qualified_local_util_call(root: &std::path::Path) {
    let downstream_path = root.join("downstream.py");
    let mut src = std::fs::read_to_string(&downstream_path).unwrap();
    src.push_str(DOWNSTREAM_QUALIFIED_CALL_SOURCE);
    std::fs::write(&downstream_path, src).unwrap();
}

/// Appended to `other_module.py` below: a second `local_util`, sharing the
/// existing one's qualname but not its kind (a class, not a function) --
/// purely additive, so the existing `def local_util`'s row and id survive
/// untouched.
const OTHER_MODULE_AMBIGUOUS_LOCAL_UTIL_ADDITION: &str = "\n\n\nclass local_util:
    \"\"\"Issue #77: now ambiguous with the function above -- same
    qualname, different kind, added without touching it.\"\"\"

    pass
";

fn append_ambiguous_local_util(root: &std::path::Path) {
    let other_module_path = root.join("other_module.py");
    let mut src = std::fs::read_to_string(&other_module_path).unwrap();
    src.push_str(OTHER_MODULE_AMBIGUOUS_LOCAL_UTIL_ADDITION);
    std::fs::write(&other_module_path, src).unwrap();
}

/// Issue #77: a sync that touches only `other_module.py` adds a second,
/// differently-kinded `local_util` there, without deleting or modifying
/// the existing one. `downstream.py`'s existing call into
/// `other_module.local_util` (bound during the initial full reindex,
/// never resynced itself) must be re-checked and left unresolved -- not
/// stay bound to the now-ambiguous name -- exactly like a fresh reindex of
/// the same final tree.
#[test]
fn incremental_add_ambiguous_same_qualname_symbol_unbinds_existing_caller() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    give_downstream_qualified_local_util_call(&repo_root);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot_before = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let bound_before = snapshot_before.iter().any(|e| {
        e.source_qualname == "downstream.use_qualified_local_util"
            && e.kind == "CALLS"
            && e.target_qualname.as_deref() == Some("other_module.local_util")
    });
    assert!(
        bound_before,
        "precondition: use_qualified_local_util must resolve before the ambiguous addition: \
         {snapshot_before:#?}"
    );

    append_ambiguous_local_util(&repo_root);
    indexer
        .sync_rel_paths(&["other_module.py".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let still_bound = snapshot.iter().any(|e| {
        e.source_qualname == "downstream.use_qualified_local_util"
            && e.kind == "CALLS"
            && e.target_qualname.is_some()
    });
    assert!(
        !still_bound,
        "use_qualified_local_util's call must go unresolved once other_module.local_util is \
         ambiguous, not stay bound to the old target: {snapshot:#?}"
    );

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        give_downstream_qualified_local_util_call(root);
        append_ambiguous_local_util(root);
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Issue #77: the positive counterpart to the test above -- a sync that
/// adds a second, *same*-kinded `local_util` overload to
/// `other_module.py` (same file, same kind: `collapse_exact_candidates`
/// collapses these) must still resolve `downstream.py`'s existing call,
/// not unbind it just because the qualname now names two symbols.
#[test]
fn incremental_add_overload_same_qualname_same_kind_still_resolves() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    give_downstream_qualified_local_util_call(&repo_root);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let add_overload = |root: &std::path::Path| {
        let other_module_path = root.join("other_module.py");
        let mut src = std::fs::read_to_string(&other_module_path).unwrap();
        src.push_str(
            "\n\n\ndef local_util(loud: bool = False) -> str:\n    \
             \"\"\"Issue #77: an overload -- same file, same kind.\"\"\"\n    \
             return \"other loud\"\n",
        );
        std::fs::write(&other_module_path, src).unwrap();
    };
    add_overload(&repo_root);
    indexer
        .sync_rel_paths(&["other_module.py".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let bound = snapshot.iter().any(|e| {
        e.source_qualname == "downstream.use_qualified_local_util"
            && e.kind == "CALLS"
            && e.target_qualname.as_deref() == Some("other_module.local_util")
    });
    assert!(
        bound,
        "an overload addition (same file, same kind) must still collapse to one target, not \
         unbind the existing caller: {snapshot:#?}"
    );

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        give_downstream_qualified_local_util_call(root);
        add_overload(root);
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Issue #77: one sync batch that edits `Caller.cs` (inserting extra calls
/// before the one that matters), deletes `Old.cs`, and adds `New.cs`
/// defining the same overloaded `Foo` the call targets -- all in the same
/// batch, not staged across separate syncs. Every file's edges are resolved
/// fresh, by qualname, once every file in the batch has written its
/// symbols, so the result must match a fresh reindex of the same final
/// tree.
#[test]
fn csharp_one_batch_edits_caller_and_moves_overload_target_across_files() {
    let foo_source = "namespace Golden.App\n{\n    public class Foo\n    {\n        \
         public static string Bar(int x) => \"int\";\n        \
         public static string Bar(string x) => \"string\";\n    }\n}\n";
    let caller_before = "namespace Golden.App\n{\n    public class Caller\n    {\n        \
         public string Entry() => Golden.App.Foo.Bar(1);\n    }\n}\n";
    let caller_after = "namespace Golden.App\n{\n    public class Caller\n    {\n        \
         public string Entry()\n        {\n            \
         System.Console.WriteLine(\"noise 1\");\n            \
         System.Console.WriteLine(\"noise 2\");\n            \
         return Golden.App.Foo.Bar(1);\n        }\n    }\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "csharp-move-overload",
        &[("Old.cs", foo_source), ("Caller.cs", caller_before)],
    );

    common::write_files(
        &repo_root,
        &[("Caller.cs", caller_after), ("New.cs", foo_source)],
    );
    std::fs::remove_file(repo_root.join("Old.cs")).unwrap();
    indexer
        .sync_rel_paths(&[
            "Caller.cs".to_string(),
            "Old.cs".to_string(),
            "New.cs".to_string(),
        ])
        .unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let bound = snapshot.iter().any(|e| {
        e.source_qualname == "Golden.App.Caller.Entry"
            && e.kind == "CALLS"
            && e.target_qualname.as_deref() == Some("Golden.App.Foo.Bar")
    });
    assert!(
        bound,
        "the call must resolve to the moved Foo.Bar, not misbind or go unresolved: {snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("Caller.cs", caller_after), ("New.cs", foo_source)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `from pkg import mod` while `pkg.mod` names two modules (`mod.py` plus a
/// `mod.pyi` stub) must stay unresolved, not fall back to `pkg`; deleting
/// the stub then binds it to `pkg.mod`, matching a fresh reindex.
#[test]
fn incremental_ambiguous_module_candidate_does_not_fall_back_to_package() {
    let main_py = "from pkg import mod\n";
    let mod_py = "def helper():\n    pass\n";
    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ambiguous-module",
        &[
            ("pkg/__init__.py", ""),
            ("pkg/mod.py", mod_py),
            ("pkg/mod.pyi", "def helper() -> None: ...\n"),
            ("main.py", main_py),
        ],
    );

    std::fs::remove_file(repo_root.join("pkg/mod.pyi")).unwrap();
    indexer
        .sync_rel_paths(&["pkg/mod.pyi".to_string()])
        .unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("pkg/__init__.py", ""),
        ("pkg/mod.py", mod_py),
        ("main.py", main_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Issue #79 follow-up: the test above unblocks a stored `Ambiguous`
/// reference by deleting a whole competing file. This is the other way
/// that can happen -- an in-place edit that renames a competing definition
/// away without deleting its file at all. `a.py` and `b.py` each define a
/// top-level `helper()`; `caller.py`'s bare `helper()` call can only bind
/// by name (no import, no qualname prefix), so it matches both and stays
/// unresolved. Renaming `b.py`'s `helper` (the file survives, edited in
/// place) leaves exactly one `helper` behind and must resolve the call to
/// it, not leave the stored reference stranded waiting for some unrelated
/// symbol to be *inserted* -- `retry_unresolved_references`'s watermark
/// only tracks insertions, so the widened, deletion-triggered join for
/// `reason = 'ambiguous'` rows is what has to catch this.
#[test]
fn incremental_symbol_rename_resolves_bare_call_ambiguity_without_file_deletion() {
    let a_py = "def helper():\n    pass\n";
    let b_py = "def helper():\n    pass\n";
    let caller_py = "def use():\n    helper()\n";
    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "rename-unblocks-ambiguity",
        &[("a.py", a_py), ("b.py", b_py), ("caller.py", caller_py)],
    );

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot_before = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let before = snapshot_before
        .iter()
        .find(|edge| edge.source_qualname == "caller.use" && edge.kind == "CALLS")
        .expect("caller.use must have a CALLS edge into helper() before the edit");
    assert_eq!(
        before.target_qualname, None,
        "two same-named top-level functions must leave the bare call unresolved, not guess \
         between them: {before:?}"
    );

    // In-place edit: b.py keeps existing -- only the competing `helper`
    // definition inside it is renamed away, not the file itself.
    let b_py_renamed = "def helper_renamed():\n    pass\n";
    std::fs::write(repo_root.join("b.py"), b_py_renamed).unwrap();
    indexer.sync_rel_paths(&["b.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let after = snapshot
        .iter()
        .find(|edge| edge.source_qualname == "caller.use" && edge.kind == "CALLS")
        .expect("caller.use must still have a CALLS edge after the edit");
    assert_eq!(
        after.target_qualname.as_deref(),
        Some("a.helper"),
        "renaming away the competing definition (b.py's file is never deleted) must resolve \
         the now-unique call to the remaining a.helper: {after:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("a.py", a_py),
        ("b.py", b_py_renamed),
        ("caller.py", caller_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}
