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
/// symbol reappearing is what gives the repair pass's exact tier
/// (`Db::reconcile_unresolved_reference_store`) something to rebind it to.
/// No re-extraction of `downstream.py` itself is needed either way.
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
/// second-chance bookkeeping at all: every later sync's repair pass
/// (`Db::repair_unresolved`) re-judges every edge/reference its reconcile
/// and targeted-retry steps are scoped to, not just ones from files that
/// sync touched, so an intervening unrelated sync changes nothing about
/// when `downstream.py`'s edges get retried.
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
         public static string Bar(int x, int y) => \"two\";\n    }\n}\n";
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

/// Issue #247: a C# `Color.Red` read whose enum file does not exist yet must
/// stay retryable, so adding the enum later binds it like a fresh reindex.
#[test]
fn csharp_enum_member_read_resolves_when_enum_file_is_added_later() {
    let reader = "using N;\nnamespace M\n{\n    public class Painter\n    {\n        \
         public object Pick() => Color.Red;\n    }\n}\n";
    let color = "namespace N\n{\n    public enum Color { Red, Green }\n}\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("csharp-enum-later", &[("Painter.cs", reader)]);
    common::write_files(&repo_root, &[("Color.cs", color)]);
    indexer.sync_rel_paths(&["Color.cs".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot
            .iter()
            .any(|e| e.source_qualname == "M.Painter.Pick"
                && e.kind == "USES"
                && e.target_qualname.as_deref() == Some("N.Color.Red")),
        "{snapshot:#?}"
    );
    let (_fresh_tmp, fresh) = common::index_files(&[("Painter.cs", reader), ("Color.cs", color)]);
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

/// Issue #78/#79 follow-up, finding G1: a call resolvable only through the
/// receiver-type/inheritance tier (`Resolver::resolve_via_inheritance`) must
/// still resolve after `sync_rel_paths` gives its receiver type a brand-new
/// EXTENDS edge to an already-indexed ancestor. `Foo` itself isn't a new
/// symbol here (it already existed as `class Foo: pass`) and no symbol named
/// `bar`/`Foo.bar` is inserted by this sync -- `Base.bar`/`Decoy.bar` both
/// already existed before the watermark -- so only the newly-written EXTENDS
/// edge itself can unblock `sub.run`'s stored `x.bar()` reference.
#[test]
fn incremental_sync_resolves_call_via_inheritance_edge_added_this_batch() {
    let base_py = "class Base:\n    def bar(self):\n        pass\n";
    let decoy_py = "class Decoy:\n    def bar(self):\n        pass\n";
    let sub_py = "from foo import Foo\n\ndef run(x: Foo):\n    return x.bar()\n";
    let foo_py_before = "class Foo:\n    pass\n";
    let foo_py_after = "from base import Base\n\nclass Foo(Base):\n    pass\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "inheritance-edge-added",
        &[
            ("base.py", base_py),
            ("decoy.py", decoy_py),
            ("sub.py", sub_py),
            ("foo.py", foo_py_before),
        ],
    );

    common::write_files(&repo_root, &[("foo.py", foo_py_after)]);
    indexer.sync_rel_paths(&["foo.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let edge = snapshot
        .iter()
        .find(|e| e.source_qualname == "sub.run" && e.kind == "CALLS")
        .unwrap_or_else(|| panic!("sub.run must have a CALLS edge: {snapshot:#?}"));
    assert_eq!(
        edge.target_qualname.as_deref(),
        Some("base.Base.bar"),
        "sub.run's call to x.bar() must resolve via Foo's newly-added EXTENDS Base edge, \
         not stay stuck behind the stored reference from before Foo had any ancestor: {edge:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("base.py", base_py),
        ("decoy.py", decoy_py),
        ("sub.py", sub_py),
        ("foo.py", foo_py_after),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Same finding, but `Foo` doesn't exist at all until the sync that also
/// gives it its EXTENDS edge -- `Foo` is a brand-new symbol, but the stored
/// reference is keyed on `bar`/`x.bar`, not `Foo`, so the existing
/// name-tail-match retry still can't pick it up on its own.
#[test]
fn incremental_sync_resolves_call_via_inheritance_on_newly_added_type() {
    let base_py = "class Base:\n    def bar(self):\n        pass\n";
    let decoy_py = "class Decoy:\n    def bar(self):\n        pass\n";
    let sub_py = "from foo import Foo\n\ndef run(x: Foo):\n    return x.bar()\n";
    let foo_py = "from base import Base\n\nclass Foo(Base):\n    pass\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "inheritance-new-type",
        &[
            ("base.py", base_py),
            ("decoy.py", decoy_py),
            ("sub.py", sub_py),
        ],
    );

    common::write_files(&repo_root, &[("foo.py", foo_py)]);
    indexer.sync_rel_paths(&["foo.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let edge = snapshot
        .iter()
        .find(|e| e.source_qualname == "sub.run" && e.kind == "CALLS")
        .unwrap_or_else(|| panic!("sub.run must have a CALLS edge: {snapshot:#?}"));
    assert_eq!(
        edge.target_qualname.as_deref(),
        Some("base.Base.bar"),
        "sub.run's call to x.bar() must resolve once Foo (with its EXTENDS Base edge) is \
         added, even though Foo itself doesn't share bar's name/tail: {edge:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("base.py", base_py),
        ("decoy.py", decoy_py),
        ("sub.py", sub_py),
        ("foo.py", foo_py),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

const DEFERRED_TYPES: &str = "namespace App { public class Store { public void Write() { } } \
public class Other { public void Write() { } } }\n";
const DEFERRED_CALLER: &str = "namespace App { public class Caller { \
public void Run(Repo repo) { var s = repo.Open(); s.Write(); } } }\n";

fn deferred_repo(open_return: &str) -> String {
    format!(
        "namespace App {{ public class Repo {{ public {open_return} Open() {{ return null; }} }} }}\n"
    )
}

/// The `Write` target `Caller.Run`'s edge is bound to in the current graph.
fn deferred_write_target(indexer: &Indexer) -> Option<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv)
        .unwrap()
        .into_iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.source_qualname == "App.Caller.Run"
                && e.target_qualname
                    .as_deref()
                    .is_some_and(|t| t.ends_with(".Write"))
        })
        .and_then(|e| e.target_qualname)
}

/// A deferred receiver (`var s = repo.Open(); s.Write()`) hangs on the
/// callee's return type: editing `Repo.Open` alone -- caller and `Write`
/// untouched -- must retarget the caller's edge, as a fresh reindex would.
#[test]
fn csharp_deferred_receiver_retargets_when_callee_return_type_is_edited() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "deferred-edit",
        &[
            ("Types.cs", DEFERRED_TYPES),
            ("Repo.cs", &deferred_repo("Store")),
            ("Caller.cs", DEFERRED_CALLER),
        ],
    );
    assert_eq!(
        deferred_write_target(&indexer).as_deref(),
        Some("App.Store.Write")
    );

    common::write_files(&root, &[("Repo.cs", &deferred_repo("Other"))]);
    indexer.sync_rel_paths(&["Repo.cs".to_string()]).unwrap();
    assert_eq!(
        deferred_write_target(&indexer).as_deref(),
        Some("App.Other.Write")
    );
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("Types.cs", DEFERRED_TYPES),
        ("Repo.cs", &deferred_repo("Other")),
        ("Caller.cs", DEFERRED_CALLER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);

    // An int return is untracked: the edge must unbind.
    common::write_files(&root, &[("Repo.cs", &deferred_repo("int"))]);
    indexer.sync_rel_paths(&["Repo.cs".to_string()]).unwrap();
    assert_eq!(deferred_write_target(&indexer), None);
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("Types.cs", DEFERRED_TYPES),
        ("Repo.cs", &deferred_repo("int")),
        ("Caller.cs", DEFERRED_CALLER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// The callee added after the caller: the stored unresolved reference must
/// resolve once `Repo.Open` exists.
#[test]
fn csharp_deferred_receiver_resolves_when_callee_is_added_later() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "deferred-add",
        &[("Types.cs", DEFERRED_TYPES), ("Caller.cs", DEFERRED_CALLER)],
    );
    assert_eq!(deferred_write_target(&indexer), None);

    common::write_files(&root, &[("Repo.cs", &deferred_repo("Store"))]);
    indexer.sync_rel_paths(&["Repo.cs".to_string()]).unwrap();
    assert_eq!(
        deferred_write_target(&indexer).as_deref(),
        Some("App.Store.Write")
    );
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("Types.cs", DEFERRED_TYPES),
        ("Repo.cs", &deferred_repo("Store")),
        ("Caller.cs", DEFERRED_CALLER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A namespace declared by several files is one symbol per file, and the
/// other files' CONTAINS edges bind to one of them: after some of the files
/// were re-synced, deleting another must hand those edges to a surviving
/// declaration, as a fresh reindex would.
#[test]
fn csharp_deleting_a_file_of_a_shared_namespace_keeps_contains_edges() {
    let a = "namespace App { public class A { } }\n";
    let b = "namespace App { public class B { } }\n";
    let c = "namespace App { public class C { } }\n";
    let b2 = "namespace App { public class B { public void M() { } } }\n";
    let c2 = "namespace App { public class C { public void M() { } } }\n";
    let (_tmp, root, mut indexer) =
        indexed_tree("shared-ns", &[("A.cs", a), ("B.cs", b), ("C.cs", c)]);
    common::write_files(&root, &[("B.cs", b2)]);
    indexer.sync_rel_paths(&["B.cs".to_string()]).unwrap();
    common::write_files(&root, &[("C.cs", c2)]);
    indexer.sync_rel_paths(&["C.cs".to_string()]).unwrap();
    std::fs::remove_file(root.join("A.cs")).unwrap();
    indexer.sync_rel_paths(&["A.cs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[("B.cs", b2), ("C.cs", c2)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Sync `edits` (on top of `initial`) incrementally and compare with a fresh
/// index of `finals` (issue #198). `None` content deletes the file.
fn shared_container_matches_fresh(
    initial: &[(&str, &str)],
    edits: &[(&str, Option<&str>)],
    finals: &[(&str, &str)],
) {
    let (_tmp, root, mut indexer) = indexed_tree("shared-container", initial);
    for (path, content) in edits {
        match content {
            Some(c) => common::write_files(&root, &[(path, c)]),
            None => std::fs::remove_file(root.join(path)).unwrap(),
        }
        indexer.sync_rel_paths(&[path.to_string()]).unwrap();
    }
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(finals);
    common::assert_matches_fresh(&snapshot, &fresh);
}

const NS_A: &str = "namespace App { public class A { } }\n";
const NS_B: &str = "namespace App { public class B { } }\n";
const NS_C: &str = "namespace App { public class C { } }\n";
const NS_A2: &str = "namespace App { public class A { public void M() { } } }\n";
const NS_B2: &str = "namespace App { public class B { public void M() { } } }\n";
const NS_D: &str = "namespace App { public class D { } }\n";

#[test]
fn csharp_editing_a_file_of_a_shared_namespace_keeps_contains_edges() {
    let init = [("A.cs", NS_A), ("B.cs", NS_B), ("C.cs", NS_C)];
    for (edited, new) in [("A.cs", NS_A2), ("B.cs", NS_B2)] {
        let finals: Vec<(&str, &str)> = init
            .iter()
            .map(|(p, c)| if *p == edited { (*p, new) } else { (*p, *c) })
            .collect();
        shared_container_matches_fresh(&init, &[(edited, Some(new))], &finals);
    }
}

#[test]
fn csharp_adding_a_file_to_a_shared_namespace_keeps_contains_edges() {
    let init = [("B.cs", NS_B), ("C.cs", NS_C)];
    shared_container_matches_fresh(
        &init,
        &[("A.cs", Some(NS_A))],
        &[("A.cs", NS_A), ("B.cs", NS_B), ("C.cs", NS_C)],
    );
    shared_container_matches_fresh(
        &init,
        &[("D.cs", Some(NS_D))],
        &[("B.cs", NS_B), ("C.cs", NS_C), ("D.cs", NS_D)],
    );
}

#[test]
fn csharp_deleting_each_file_of_a_shared_namespace_keeps_contains_edges() {
    let init = [("A.cs", NS_A), ("B.cs", NS_B), ("C.cs", NS_C)];
    shared_container_matches_fresh(&init, &[("A.cs", None)], &[("B.cs", NS_B), ("C.cs", NS_C)]);
    shared_container_matches_fresh(&init, &[("B.cs", None)], &[("A.cs", NS_A), ("C.cs", NS_C)]);
}

/// How a file of a shared container (namespace, package, module) is written.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// The container with one plain member.
    Plain,
    /// The container with an extra member calling the first.
    WithMember,
    /// No container at all, only a free member.
    NoContainer,
    /// C#: file-scoped `namespace App;`.
    FileScoped,
    /// C#: the container plus a second, nested-name container `App.Sub`.
    SecondContainer,
}

type Render = fn(&str, Shape) -> String;

/// Every edit of, and delete of, one of three files sharing a container that
/// starts out as `start`, each compared with a fresh index of the final tree.
fn shared_container_edits_match_fresh(ext: &str, render: Render, start: Shape, edits: &[Shape]) {
    let names = ["a", "b", "c"];
    let path = |i: usize| format!("{}.{ext}", names[i]);
    let tree = |states: [Option<Shape>; 3]| -> Vec<(String, String)> {
        (0..3)
            .filter_map(|i| states[i].map(|s| (path(i), render(names[i], s))))
            .collect()
    };
    let borrow = |t: &[(String, String)]| -> Vec<(String, String)> { t.to_vec() };
    for who in 0..3 {
        for edit in edits.iter().map(|e| Some(*e)).chain([None]) {
            let mut states = [Some(start); 3];
            states[who] = edit;
            let (init, fin) = (tree([Some(start); 3]), borrow(&tree(states)));
            let init: Vec<(&str, &str)> =
                init.iter().map(|(p, c)| (p.as_str(), c.as_str())).collect();
            let fin: Vec<(&str, &str)> =
                fin.iter().map(|(p, c)| (p.as_str(), c.as_str())).collect();
            let content = edit.map(|e| render(names[who], e));
            shared_container_matches_fresh(&init, &[(&path(who), content.as_deref())], &fin);
        }
    }
}

fn csharp_shape(cls: &str, shape: Shape) -> String {
    match shape {
        Shape::Plain => format!("namespace App {{ public class {cls} {{ }} }}\n"),
        Shape::WithMember => format!(
            "using System;\n\nnamespace App\n{{\n    public class {cls}\n    {{\n        public void M() {{ }}\n    }}\n}}\n"
        ),
        Shape::NoContainer => format!("public class {cls} {{ }}\n"),
        Shape::FileScoped => format!("namespace App;\npublic class {cls} {{ }}\n"),
        Shape::SecondContainer => format!(
            "namespace App {{ public class {cls} {{ }} }}\nnamespace App.Sub {{ public class {cls}S {{ }} }}\n"
        ),
    }
}

const CSHARP_EDITS: [Shape; 4] = [
    Shape::Plain,
    Shape::WithMember,
    Shape::FileScoped,
    Shape::SecondContainer,
];

/// Issue #198: an edit that stops declaring one of several shared namespaces
/// must leave the other files' CONTAINS edges bound, as a fresh reindex does.
#[test]
fn csharp_edit_dropping_a_shared_second_namespace_keeps_contains_edges() {
    shared_container_edits_match_fresh("cs", csharp_shape, Shape::SecondContainer, &CSHARP_EDITS);
}

#[test]
fn csharp_edits_of_a_shared_block_namespace_match_fresh() {
    shared_container_edits_match_fresh("cs", csharp_shape, Shape::Plain, &CSHARP_EDITS);
}

#[test]
fn csharp_edits_of_a_shared_file_scoped_namespace_match_fresh() {
    shared_container_edits_match_fresh("cs", csharp_shape, Shape::FileScoped, &CSHARP_EDITS);
}

#[test]
fn csharp_edits_of_a_multi_line_shared_namespace_match_fresh() {
    shared_container_edits_match_fresh("cs", csharp_shape, Shape::WithMember, &CSHARP_EDITS);
}

fn go_shape(n: &str, shape: Shape) -> String {
    match shape {
        Shape::WithMember => format!("package app\n\nfunc {n}() {{}}\nfunc {n}2() {{ {n}() }}\n"),
        Shape::NoContainer => format!("package app\n\ntype T{n} struct{{}}\n"),
        _ => format!("package app\n\nfunc {n}() {{}}\n"),
    }
}

fn python_shape(n: &str, shape: Shape) -> String {
    match shape {
        Shape::WithMember => format!("def {n}():\n    pass\n\ndef {n}2():\n    {n}()\n"),
        Shape::NoContainer => format!("class T{n}:\n    pass\n"),
        _ => format!("def {n}():\n    pass\n"),
    }
}

fn rust_shape(n: &str, shape: Shape) -> String {
    match shape {
        Shape::WithMember => {
            format!("pub mod shared {{ pub fn {n}() {{}} pub fn {n}2() {{ {n}() }} }}\n")
        }
        Shape::NoContainer => format!("pub fn {n}() {{}}\n"),
        _ => format!("pub mod shared {{ pub fn {n}() {{}} }}\n"),
    }
}

fn ts_shape(n: &str, shape: Shape) -> String {
    match shape {
        Shape::WithMember => format!(
            "namespace Shared {{ export function {n}() {{}}\n export function {n}2() {{ {n}(); }} }}\n"
        ),
        Shape::NoContainer => format!("export function {n}() {{}}\n"),
        _ => format!("namespace Shared {{ export function {n}() {{}} }}\n"),
    }
}

const PLAIN_EDITS: [Shape; 2] = [Shape::WithMember, Shape::NoContainer];

#[test]
fn go_shared_package_edits_match_fresh() {
    shared_container_edits_match_fresh("go", go_shape, Shape::Plain, &PLAIN_EDITS);
}

#[test]
fn python_shared_package_edits_match_fresh() {
    shared_container_edits_match_fresh("py", python_shape, Shape::Plain, &PLAIN_EDITS);
}

#[test]
fn rust_shared_module_edits_match_fresh() {
    shared_container_edits_match_fresh("rs", rust_shape, Shape::Plain, &PLAIN_EDITS);
}

#[test]
fn ts_shared_namespace_edits_match_fresh() {
    shared_container_edits_match_fresh("ts", ts_shape, Shape::Plain, &PLAIN_EDITS);
}

/// A C# partial class is one symbol per declaring file: when one file drops
/// its declaration, the others' edges to that copy must survive.
#[test]
fn csharp_partial_class_dropped_by_one_file_keeps_other_files_edges() {
    let part = |m: &str| {
        format!("namespace App {{ public partial class Service {{ void {m}() {{ }} }} }}\n")
    };
    let (a, b, c) = (part("A"), part("B"), part("C"));
    let (a2, b2) = (
        "namespace App { public class Other { } }\n".to_string(),
        part("B2"),
    );
    for (edited, new, finals) in [
        ("a.cs", &a2, [("a.cs", &a2), ("b.cs", &b), ("c.cs", &c)]),
        ("b.cs", &b2, [("a.cs", &a), ("b.cs", &b2), ("c.cs", &c)]),
    ] {
        let finals: Vec<(&str, &str)> = finals.iter().map(|(p, s)| (*p, s.as_str())).collect();
        shared_container_matches_fresh(
            &[("a.cs", &a), ("b.cs", &b), ("c.cs", &c)]
                .map(|(p, s): (&str, &String)| (p, s.as_str())),
            &[(edited, Some(new.as_str()))],
            &finals,
        );
    }
}

/// Carry-forward remaps symbols by `stable_id`, which files can share: a
/// full reindex that only re-parses one unrelated file must leave the shared
/// declarations' edges and metrics as a fresh index has them.
fn reindex_carry_forward_matches_fresh(files: &[(&str, &str)], edit: (&str, &str)) {
    let (_tmp, root, mut indexer) = indexed_tree("carry-shared", files);
    common::write_files(&root, &[edit]);
    indexer.reindex().unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    // Edge snapshots key on qualnames, so also check the raw ids: an edge's
    // source and a metrics row's symbol must live in the row's own file.
    let conn = indexer.db().read_conn().unwrap();
    for sql in [
        "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
         WHERE s.file_id != e.file_id",
        "SELECT COUNT(*) FROM symbol_metrics m JOIN symbols s ON s.id = m.symbol_id
         WHERE s.file_id != m.file_id",
    ] {
        let bad: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
        assert_eq!(
            bad, 0,
            "carry-forward remapped to another file's copy: {sql}"
        );
    }
    drop(conn);
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let finals: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, c)| if *p == edit.0 { edit } else { (*p, *c) })
        .collect();
    let (_t, fresh) = common::index_files(&finals);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn carry_forward_of_files_sharing_a_namespace_matches_fresh() {
    reindex_carry_forward_matches_fresh(
        &[
            ("a.cs", NS_A),
            ("b.cs", NS_B),
            ("c.cs", "namespace Other { public class C { } }\n"),
        ],
        (
            "c.cs",
            "namespace Other { public class C { public void M() { } } }\n",
        ),
    );
}

#[test]
fn carry_forward_of_files_sharing_a_partial_class_matches_fresh() {
    let part =
        |m: &str| format!("namespace App {{ public partial class S {{ void {m}() {{ }} }} }}\n");
    let (a, b) = (part("A"), part("B"));
    reindex_carry_forward_matches_fresh(
        &[("a.cs", &a), ("b.cs", &b), ("c.cs", "public class C { }\n")],
        ("c.cs", "public class C { public void M() { } }\n"),
    );
}

#[test]
fn carry_forward_of_files_sharing_an_identical_function_matches_fresh() {
    let helper = "def helper():\n    return 1\n";
    reindex_carry_forward_matches_fresh(
        &[
            ("a.py", helper),
            ("b.py", helper),
            ("c.py", "def c():\n    pass\n"),
        ],
        ("c.py", "def c():\n    return 2\n"),
    );
}

// Issue #187: JS/TS import candidates are chased through re-export barrels
// and default exports at extraction time, so editing/adding/deleting only
// the barrel or target must re-extract the (hash-unchanged) importers.

const TS_CALLER: &str =
    "import { foo } from './lib';\nexport function go() {\n  return foo();\n}\n";

fn ts_calls(indexer: &Indexer) -> Vec<(String, Option<String>)> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "use.go")
        .map(|e| (e.source_qualname, e.target_qualname))
        .collect()
}

/// Incremental sync of `synced` after `edit` must equal a fresh reindex of
/// `final_files`; returns the incremental indexer for extra assertions.
fn ts_incremental_matches_fresh(
    label: &str,
    initial: &[(&str, &str)],
    edit: impl FnOnce(&std::path::Path),
    synced: &[&str],
    final_files: &[(&str, &str)],
) -> (tempfile::TempDir, Indexer) {
    let (tmp, repo_root, mut indexer) = indexed_tree(label, initial);
    edit(&repo_root);
    let rels: Vec<String> = synced.iter().map(|s| s.to_string()).collect();
    indexer.sync_rel_paths(&rels).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
    (tmp, indexer)
}

fn target_of(indexer: &Indexer) -> Option<String> {
    ts_calls(indexer).into_iter().next().and_then(|(_, t)| t)
}

const FOO_A: (&str, &str) = ("lib/a.ts", "export function foo() {}\n");
const FOO_B: (&str, &str) = ("lib/b.ts", "export function foo() {}\n");

#[test]
fn incremental_barrel_edit_reextracts_unchanged_importer() {
    let barrel_v2 = "export { foo } from './b';\n";
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-barrel-edit",
        &[
            FOO_A,
            FOO_B,
            ("lib/index.ts", "export { foo } from './a';\n"),
            ("use.ts", TS_CALLER),
        ],
        |root| common::write_files(root, &[("lib/index.ts", barrel_v2)]),
        &["lib/index.ts"],
        &[
            FOO_A,
            FOO_B,
            ("lib/index.ts", barrel_v2),
            ("use.ts", TS_CALLER),
        ],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/b.foo"));
}

#[test]
fn incremental_default_export_rename_reextracts_unchanged_importer() {
    let caller = "import api from './api';\nexport function go() {\n  return api.get();\n}\n";
    let v1 = "const apiClient = { get() { return 1; } };\nexport default apiClient;\n";
    let v2 = "const renamedClient = { get() { return 1; } };\nexport default renamedClient;\n";
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-default-rename",
        &[("api.ts", v1), ("use.ts", caller)],
        |root| common::write_files(root, &[("api.ts", v2)]),
        &["api.ts"],
        &[("api.ts", v2), ("use.ts", caller)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("api.renamedClient.get"));
}

#[test]
fn incremental_barrel_repointed_to_new_file_reextracts_importer() {
    let barrel_v2 = "export { foo } from './c';\n";
    let c = ("lib/c.ts", "export function foo() {}\n");
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-barrel-repoint",
        &[
            FOO_A,
            ("lib/index.ts", "export { foo } from './a';\n"),
            ("use.ts", TS_CALLER),
        ],
        |root| {
            common::write_files(root, &[c, ("lib/index.ts", barrel_v2)]);
        },
        &["lib/c.ts", "lib/index.ts"],
        &[FOO_A, c, ("lib/index.ts", barrel_v2), ("use.ts", TS_CALLER)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/c.foo"));
}

#[test]
fn incremental_added_barrel_target_reextracts_importer_through_barrel() {
    let barrel = ("lib/index.ts", "export * from './x';\n");
    let x = ("lib/x.ts", "export function foo() {}\n");
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-target-added",
        &[barrel, ("use.ts", TS_CALLER)],
        |root| common::write_files(root, &[x]),
        &["lib/x.ts"],
        &[barrel, x, ("use.ts", TS_CALLER)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/x.foo"));
}

#[test]
fn incremental_deleted_barrel_target_reextracts_importer_through_barrel() {
    let barrel = ("lib/index.ts", "export * from './x';\n");
    let x = ("lib/x.ts", "export function foo() {}\n");
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-target-deleted",
        &[barrel, x, ("use.ts", TS_CALLER)],
        |root| std::fs::remove_file(root.join("lib/x.ts")).unwrap(),
        &["lib/x.ts"],
        &[barrel, ("use.ts", TS_CALLER)],
    );
    assert_ne!(target_of(&idx).as_deref(), Some("lib/x.foo"));
}

#[test]
fn full_reindex_reextracts_importer_of_edited_barrel() {
    let barrel_v2 = "export { foo } from './b';\n";
    let (_tmp, root, mut indexer) = indexed_tree(
        "ts-reindex-barrel",
        &[
            FOO_A,
            FOO_B,
            ("lib/index.ts", "export { foo } from './a';\n"),
            ("use.ts", TS_CALLER),
        ],
    );
    common::write_files(&root, &[("lib/index.ts", barrel_v2)]);
    indexer.reindex().unwrap();
    assert_eq!(target_of(&indexer).as_deref(), Some("lib/b.foo"));
}

const ALIAS_TSCONFIG: (&str, &str) = (
    "tsconfig.json",
    "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"]}}}",
);
const ALIAS_CALLER: &str =
    "import { foo } from '@/lib/foo';\nexport function go() {\n  return foo();\n}\n";
const LIB_FOO: (&str, &str) = ("lib/foo.ts", "export function foo() {}\n");

#[test]
fn incremental_tsconfig_paths_added_reextracts_alias_importer() {
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-config-added",
        &[("tsconfig.json", "{}"), LIB_FOO, ("use.ts", ALIAS_CALLER)],
        |root| common::write_files(root, &[ALIAS_TSCONFIG]),
        &["tsconfig.json"],
        &[ALIAS_TSCONFIG, LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn incremental_tsconfig_deleted_reextracts_alias_importer() {
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-config-deleted",
        &[ALIAS_TSCONFIG, LIB_FOO, ("use.ts", ALIAS_CALLER)],
        |root| std::fs::remove_file(root.join("tsconfig.json")).unwrap(),
        &["tsconfig.json"],
        &[LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    assert_ne!(target_of(&idx).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn full_reindex_reextracts_alias_importer_after_tsconfig_edit() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "ts-reindex-config",
        &[("tsconfig.json", "{}"), LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    common::write_files(&root, &[ALIAS_TSCONFIG]);
    indexer.reindex().unwrap();
    assert_eq!(target_of(&indexer).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn incremental_alias_import_of_missing_file_resolves_once_file_added() {
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-alias-missing",
        &[ALIAS_TSCONFIG, ("use.ts", ALIAS_CALLER)],
        |root| common::write_files(root, &[LIB_FOO]),
        &["lib/foo.ts"],
        &[ALIAS_TSCONFIG, LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/foo.foo"));
}

const BASE_ALIAS_CFG: (&str, &str) = ("tsconfig.json", "{\"extends\":\"./tsconfig.base.json\"}");

#[test]
fn incremental_base_config_edit_reextracts_alias_importer() {
    let base_v1 = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"nope/*\"]}}}";
    let base_v2 = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"]}}}";
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-base-config-edit",
        &[
            ("tsconfig.base.json", base_v1),
            BASE_ALIAS_CFG,
            LIB_FOO,
            ("use.ts", ALIAS_CALLER),
        ],
        |root| common::write_files(root, &[("tsconfig.base.json", base_v2)]),
        &["tsconfig.base.json"],
        &[
            ("tsconfig.base.json", base_v2),
            BASE_ALIAS_CFG,
            LIB_FOO,
            ("use.ts", ALIAS_CALLER),
        ],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn incremental_base_config_deleted_reextracts_alias_importer() {
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-base-config-deleted",
        &[
            ("tsconfig.base.json", ALIAS_TSCONFIG.1),
            BASE_ALIAS_CFG,
            LIB_FOO,
            ("use.ts", ALIAS_CALLER),
        ],
        |root| std::fs::remove_file(root.join("tsconfig.base.json")).unwrap(),
        &["tsconfig.base.json"],
        &[BASE_ALIAS_CFG, LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    assert_ne!(target_of(&idx).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn full_reindex_reextracts_alias_importer_after_base_config_edit() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "ts-reindex-base-config",
        &[
            ("tsconfig.base.json", "{}"),
            BASE_ALIAS_CFG,
            LIB_FOO,
            ("use.ts", ALIAS_CALLER),
        ],
    );
    common::write_files(&root, &[("tsconfig.base.json", ALIAS_TSCONFIG.1)]);
    indexer.reindex().unwrap();
    assert_eq!(target_of(&indexer).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn full_reindex_reextracts_alias_importer_after_package_base_config_deleted() {
    let pkg = "node_modules/@shared/tsconfig/tsconfig.json";
    let pkg_cfg = "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"../../../*\"]}}}";
    let child = (
        "tsconfig.json",
        "{\"extends\":\"@shared/tsconfig/tsconfig.json\"}",
    );
    let (_tmp, root, mut indexer) = indexed_tree(
        "ts-package-base-deleted",
        &[(pkg, pkg_cfg), child, LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    assert_eq!(target_of(&indexer).as_deref(), Some("lib/foo.foo"));
    std::fs::remove_file(root.join(pkg)).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[child, LIB_FOO, ("use.ts", ALIAS_CALLER)]);
    common::assert_matches_fresh(&snapshot, &fresh);
    assert_ne!(target_of(&indexer).as_deref(), Some("lib/foo.foo"));
}

#[test]
fn incremental_import_then_export_barrel_repoint_reextracts_importer() {
    let v1 = "import { foo } from './a';\nexport { foo };\n";
    let v2 = "import { foo } from './b';\nexport { foo };\n";
    let (_t, idx) = ts_incremental_matches_fresh(
        "ts-import-export-repoint",
        &[FOO_A, FOO_B, ("lib/index.ts", v1), ("use.ts", TS_CALLER)],
        |root| common::write_files(root, &[("lib/index.ts", v2)]),
        &["lib/index.ts"],
        &[FOO_A, FOO_B, ("lib/index.ts", v2), ("use.ts", TS_CALLER)],
    );
    assert_eq!(target_of(&idx).as_deref(), Some("lib/b.foo"));
}

#[test]
fn incremental_leaf_body_edit_does_not_reextract_importers() {
    let barrel = ("lib/index.ts", "export * from './a';\n");
    let (_t, root, mut indexer) =
        indexed_tree("ts-body-edit", &[FOO_A, barrel, ("use.ts", TS_CALLER)]);
    // Body only: same export surface.
    common::write_files(
        &root,
        &[("lib/a.ts", "export function foo() { return 1; }\n")],
    );
    let stats = indexer.sync_rel_paths(&["lib/a.ts".to_string()]).unwrap();
    assert_eq!(stats.indexed, 1, "importers must not be re-extracted");
    // A new export changes the surface, so importers are re-extracted.
    common::write_files(
        &root,
        &[(
            "lib/a.ts",
            "export function foo() { return 1; }\nexport function bar() {}\n",
        )],
    );
    let stats = indexer.sync_rel_paths(&["lib/a.ts".to_string()]).unwrap();
    assert!(stats.indexed > 1, "{stats:?}");
}

#[test]
fn sync_of_tsconfig_change_keeps_next_reindex_from_redoing_js_files() {
    let (_t, root, mut indexer) = indexed_tree(
        "ts-config-fingerprint",
        &[("tsconfig.json", "{}"), LIB_FOO, ("use.ts", ALIAS_CALLER)],
    );
    common::write_files(&root, &[ALIAS_TSCONFIG]);
    indexer
        .sync_rel_paths(&["tsconfig.json".to_string()])
        .unwrap();
    let stats = indexer.reindex().unwrap();
    assert_eq!(stats.indexed, 0, "{stats:?}");
}

const ARG_TYPES: &str = "namespace App { public class A { public A(int x) { } } \
public class B { public B(int x) { } public B(int x, int y) { } } }\n";
const ARG_CALLER: &str = "namespace App { public class Caller { \
public void Run(Sink sink) { sink.Take(new(1)); } } }\n";

fn arg_sink(param: &str) -> String {
    format!("namespace App {{ public class Sink {{ public void Take({param} p) {{ }} }} }}\n")
}

/// The constructor `Caller.Run`'s target-typed `new(1)` argument is bound to.
fn arg_ctor_target(indexer: &Indexer) -> Option<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv)
        .unwrap()
        .into_iter()
        .find(|e| e.kind == "CALLS" && e.source_qualname == "App.Caller.Run")
        .and_then(|e| e.target_qualname)
}

fn assert_arg_matches_fresh(indexer: &Indexer, sink: Option<&str>) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let leaked: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM edges WHERE target_symbol_id IS NOT NULL
             AND target_qualname LIKE '@%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(leaked, 0, "a bound edge kept a placeholder target_qualname");
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let sink_src = sink.map(arg_sink);
    let mut files = vec![("Types.cs", ARG_TYPES), ("Caller.cs", ARG_CALLER)];
    if let Some(src) = &sink_src {
        files.push(("Sink.cs", src));
    }
    let (_t, fresh) = common::index_files(&files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A target-typed `new(..)` argument hangs on the callee's parameter type:
/// editing `Sink.Take` alone must retarget (or unbind) it, and adding the
/// callee later must resolve it, exactly as a fresh reindex would.
#[test]
fn csharp_deferred_argument_follows_callee_parameter_edits() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "deferred-arg",
        &[("Types.cs", ARG_TYPES), ("Caller.cs", ARG_CALLER)],
    );
    assert_eq!(arg_ctor_target(&indexer), None);

    common::write_files(&root, &[("Sink.cs", &arg_sink("A"))]);
    indexer.sync_rel_paths(&["Sink.cs".to_string()]).unwrap();
    assert_eq!(arg_ctor_target(&indexer).as_deref(), Some("App.A..ctor"));
    assert_arg_matches_fresh(&indexer, Some("A"));

    common::write_files(&root, &[("Sink.cs", &arg_sink("B"))]);
    indexer.sync_rel_paths(&["Sink.cs".to_string()]).unwrap();
    assert_eq!(arg_ctor_target(&indexer).as_deref(), Some("App.B..ctor"));
    assert_arg_matches_fresh(&indexer, Some("B"));

    // A builtin parameter type is untracked: the edge must unbind.
    common::write_files(&root, &[("Sink.cs", &arg_sink("int"))]);
    indexer.sync_rel_paths(&["Sink.cs".to_string()]).unwrap();
    assert_eq!(arg_ctor_target(&indexer), None);
    assert_arg_matches_fresh(&indexer, Some("int"));
}

const BASE_DERIVED: &str = "namespace App { public class D : Base<int> { \
public D() : base(1) { } public D(string s) : base(1, 2) { } } }\n";
const BASE_TYPE: &str = "namespace App { public class Base<T> { public Base(int x) { } \
public Base(int x, int y) { } } }\n";

/// `(source signature, target signature)` of every `D..ctor` -> `Base..ctor` edge.
fn base_ctor_edges(indexer: &Indexer) -> Vec<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.signature, t.signature FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = 'App.D..ctor'
               AND t.qualname = 'App.Base..ctor' AND e.graph_version = ?
             ORDER BY s.signature",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// `: base(args)` on a generic base declared in another file: unresolved
/// until the base is indexed, then bound per overload by arity, and still
/// bound to the right overload after unrelated syncs -- as a fresh reindex.
#[test]
fn csharp_base_initializer_binds_when_the_base_file_is_added_later() {
    let (_tmp, root, mut indexer) = indexed_tree("base-init", &[("D.cs", BASE_DERIVED)]);
    assert!(base_ctor_edges(&indexer).is_empty());

    common::write_files(&root, &[("Base.cs", BASE_TYPE)]);
    indexer.sync_rel_paths(&["Base.cs".to_string()]).unwrap();
    let expected = vec![
        ("(string s)".to_string(), "(int x, int y)".to_string()),
        ("()".to_string(), "(int x)".to_string()),
    ];
    let mut got = base_ctor_edges(&indexer);
    got.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
    let mut want = expected.clone();
    want.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
    assert_eq!(got, want);

    // Unrelated edit: carried-forward edges keep their overload.
    common::write_files(
        &root,
        &[("Other.cs", "namespace App { public class Other { } }\n")],
    );
    indexer.sync_rel_paths(&["Other.cs".to_string()]).unwrap();
    let mut got = base_ctor_edges(&indexer);
    got.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
    assert_eq!(got, want);

    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("D.cs", BASE_DERIVED),
        ("Base.cs", BASE_TYPE),
        ("Other.cs", "namespace App { public class Other { } }\n"),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// An unchanged C# caller whose receiver is an unqualified interface name
/// (looked up through its enclosing namespace scope) is carried forward
/// across a sync of an unrelated file and still resolves like a fresh index
/// once the interface appears.
#[test]
fn csharp_scoped_interface_receiver_survives_carry_forward() {
    const CALLER: &str = "namespace App {
    public class Caller {
        private IStore _store;
        public void Go() { _store.Write(); }
    }
}
";
    const STORE: &str = "namespace App {
    public interface IStore { void Write(); }
}
";
    const OTHER: &str = "public class Other { public void M() { } }\n";
    let (_tmp, root, mut indexer) = indexed_tree(
        "scoped-carry",
        &[("Caller.cs", CALLER), ("Other.cs", OTHER)],
    );
    // Unrelated edit first: the caller is carried into the new graph version.
    common::write_files(&root, &[("Other.cs", &format!("{OTHER}// edit\n"))]);
    indexer.sync_rel_paths(&["Other.cs".to_string()]).unwrap();
    // Then the interface appears.
    common::write_files(&root, &[("Store.cs", STORE)]);
    indexer.sync_rel_paths(&["Store.cs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("Caller.cs", CALLER),
        ("Other.cs", &format!("{OTHER}// edit\n")),
        ("Store.cs", STORE),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
    let resolved: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'CALLS' AND t.qualname = 'App.IStore.Write'",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(resolved, 1, "the scoped call binds to the interface member");
}

// Issue #258: an incremental reindex must resolve a re-parsed file's
// references against the complete new-version symbol set (carried symbols
// included), so it lands on exactly the graph a fresh index of the same tree
// produces -- edges and stored unresolved references alike.

type UnresolvedRow = (String, String, Option<String>, String);

fn unresolved_snapshot(indexer: &Indexer) -> std::collections::BTreeSet<UnresolvedRow> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(s.qualname, ''), ur.edge_kind, ur.reference_name, ur.reason
             FROM unresolved_references ur
             LEFT JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE ur.graph_version = ?",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// A reference is never both a bound edge and a stored unresolved row. A
/// reference is identified by its source symbol, edge kind and evidence line
/// (the columns both tables share), not by its target name, so an aliased
/// reference (`th()` -> `thing`) is covered too.
fn assert_no_edge_and_unresolved_overlap(indexer: &Indexer) {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let both: Vec<(String, String, Option<i64>)> = conn
        .prepare(
            "SELECT s.qualname, ur.edge_kind, ur.evidence_start_line
             FROM unresolved_references ur
             JOIN symbols s ON s.id = ur.source_symbol_id
             JOIN edges e ON e.source_symbol_id = ur.source_symbol_id
                         AND e.kind = ur.edge_kind
                         AND e.evidence_start_line IS ur.evidence_start_line
                         AND e.target_symbol_id IS NOT NULL
             WHERE ur.graph_version = ? AND e.graph_version = ?",
        )
        .unwrap()
        .query_map([gv, gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(
        both.is_empty(),
        "reference(s) recorded as both edge and unresolved row: {both:?}"
    );
}

fn fresh_graph(
    files: &[(&str, String)],
) -> (
    std::collections::BTreeSet<EdgeKey>,
    std::collections::BTreeSet<UnresolvedRow>,
) {
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_t, _root, indexer) = indexed_tree("fresh-258", &borrowed);
    let gv = indexer.db().current_graph_version().unwrap();
    (
        golden::snapshot_edges(indexer.db(), gv).unwrap(),
        unresolved_snapshot(&indexer),
    )
}

/// Index `files`, then touch `touch` twice (two successive incremental
/// reindexes, each with different content), asserting after each that the
/// graph equals a fresh index of the same tree. Returns the final indexer.
fn assert_touch_matches_fresh(
    files: &[(&str, &str)],
    touch: &str,
    comment: &str,
) -> (tempfile::TempDir, Indexer) {
    let (tmp, root, mut indexer) = indexed_tree("touch-258", files);
    for round in 1..=2 {
        let original = files.iter().find(|(p, _)| *p == touch).unwrap().1;
        let edited = format!("{original}\n{comment} touched {round}\n");
        common::write_files(&root, &[(touch, edited.as_str())]);
        indexer.reindex().unwrap();

        let finals: Vec<(&str, String)> = files
            .iter()
            .map(|(p, c)| {
                (
                    *p,
                    if *p == touch {
                        edited.clone()
                    } else {
                        c.to_string()
                    },
                )
            })
            .collect();
        assert_no_edge_and_unresolved_overlap(&indexer);
        let (fresh_edges, fresh_unresolved) = fresh_graph(&finals);
        let gv = indexer.db().current_graph_version().unwrap();
        let edges = golden::snapshot_edges(indexer.db(), gv).unwrap();
        assert_eq!(edges, fresh_edges, "edges differ after touch round {round}");
        assert_eq!(
            unresolved_snapshot(&indexer),
            fresh_unresolved,
            "unresolved references differ after touch round {round}"
        );
        common::assert_no_dangling_edge_targets(indexer.db());
    }
    (tmp, indexer)
}

const ALIAS_FILES: &[(&str, &str)] = &[
    ("pkgqa/__init__.py", ""),
    (
        "pkgqa/mod.py",
        "def thing():\n    return 1\n\n\ndef helper():\n    return 2\n",
    ),
    (
        "pkgqa/user.py",
        "from pkgqa.mod import thing as th\nfrom pkgqa import mod as m\n\n\ndef go():\n    th()\n    m.helper()\n",
    ),
];

#[test]
fn reindex_touching_alias_importer_matches_fresh_and_keeps_alias_edge() {
    let (_tmp, mut indexer) = assert_touch_matches_fresh(ALIAS_FILES, "pkgqa/user.py", "#");
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = golden::snapshot_edges(indexer.db(), gv).unwrap();
    assert!(
        edges.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "pkgqa.user.go"
            && e.target_qualname.as_deref() == Some("pkgqa.mod.thing")),
        "aliased call edge missing: {edges:#?}"
    );
    let out = lidx::rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "pkgqa.mod.thing"}),
    )
    .unwrap();
    let total = out
        .get("callers_total")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| panic!("no callers_total in {out}"));
    assert!(total > 0, "explain_symbol callers_total is 0: {out}");
}

const DP: &str = "def _dataproduct():\n    return 1\n";

fn ambiguity_files() -> Vec<(&'static str, String)> {
    vec![
        (
            "tests/test_a.py",
            format!("{DP}\n\nclass TestA:\n    def test_one(self):\n        _dataproduct()\n"),
        ),
        ("tests/test_b.py", DP.to_string()),
        (
            "tests/test_c.py",
            "class TestC:\n    def test_three(self):\n        _dataproduct()\n".to_string(),
        ),
    ]
}

fn ambiguity_case(touch: &str) -> (tempfile::TempDir, Indexer) {
    let files = ambiguity_files();
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, c)| (*p, c.as_str())).collect();
    assert_touch_matches_fresh(&borrowed, touch, "#")
}

/// Issue #248: `TestA`'s bare call binds to the same-file module-level
/// `_dataproduct` (a same-file match wins outright); `TestC`'s has no
/// same-file definition and two cross-file candidates, so it stays ambiguous.
fn assert_same_file_binds_cross_file_stays_ambiguous(indexer: &Indexer) {
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let bound: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == "CALLS" && e.target_qualname.is_some())
        .map(|e| (e.source_qualname.as_str(), e.target_qualname.as_deref()))
        .collect();
    assert_eq!(
        bound,
        vec![(
            "tests.test_a.TestA.test_one",
            Some("tests.test_a._dataproduct")
        )],
        "{edges:#?}"
    );
    let ambiguous = unresolved_snapshot(indexer)
        .into_iter()
        .filter(|r| r.1 == "CALLS" && r.3 == "ambiguous")
        .count();
    assert_eq!(ambiguous, 1, "TestC's bare call stays ambiguous");
}

#[test]
fn reindex_touching_file_with_one_of_two_candidates_matches_fresh() {
    let (_tmp, indexer) = ambiguity_case("tests/test_a.py");
    assert_same_file_binds_cross_file_stays_ambiguous(&indexer);
}

#[test]
fn reindex_touching_other_candidate_file_matches_fresh() {
    let (_tmp, indexer) = ambiguity_case("tests/test_b.py");
    assert_same_file_binds_cross_file_stays_ambiguous(&indexer);
}

#[test]
fn reindex_touching_file_with_neither_candidate_matches_fresh() {
    let (_tmp, indexer) = ambiguity_case("tests/test_c.py");
    assert_same_file_binds_cross_file_stays_ambiguous(&indexer);
}

/// The issue's literal two-file fixture: two `_dataproduct` definitions, one
/// bare call from a method in the file that defines one. The same-file
/// definition wins (issue #248): bound edge, no ambiguous row.
fn literal_ambiguity_case(touch: &str) {
    let files = [
        (
            "tests/test_a.py",
            format!("{DP}\n\nclass TestA:\n    def test_one(self):\n        _dataproduct()\n"),
        ),
        ("tests/test_b.py", DP.to_string()),
    ];
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, c)| (*p, c.as_str())).collect();
    let (_tmp, indexer) = assert_touch_matches_fresh(&borrowed, touch, "#");
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = golden::snapshot_edges(indexer.db(), gv).unwrap();
    assert!(
        edges.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "tests.test_a.TestA.test_one"
            && e.target_qualname.as_deref() == Some("tests.test_a._dataproduct")),
        "same-file module-level def must win over the other file's: {edges:#?}"
    );
    let ambiguous: Vec<_> = unresolved_snapshot(&indexer)
        .into_iter()
        .filter(|r| r.1 == "CALLS" && r.3 == "ambiguous")
        .collect();
    assert!(ambiguous.is_empty(), "{ambiguous:?}");
}

#[test]
fn reindex_two_file_ambiguity_touching_caller_file_binds_same_file_definition() {
    literal_ambiguity_case("tests/test_a.py");
}

#[test]
fn reindex_two_file_ambiguity_touching_other_file_binds_same_file_definition() {
    literal_ambiguity_case("tests/test_b.py");
}

#[test]
fn reindex_touching_csharp_caller_of_unchanged_file_matches_fresh() {
    let files = [
        (
            "Util.cs",
            "namespace App { public class Util { public static void Help() { } } }\n",
        ),
        (
            "Caller.cs",
            "namespace App { public class Caller { public void Go() { Util.Help(); } } }\n",
        ),
    ];
    let (_tmp, indexer) = assert_touch_matches_fresh(&files, "Caller.cs", "//");
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = golden::snapshot_edges(indexer.db(), gv).unwrap();
    assert!(
        edges.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "App.Caller.Go"
            && e.target_qualname.as_deref() == Some("App.Util.Help")),
        "cross-file C# call edge missing: {edges:#?}"
    );
}

/// The overlap invariant must actually fire: fabricate an unresolved row for a
/// reference that also has a bound edge (aliased call `th()` -> `thing`, whose
/// `name_tail` differs from the target's name) and expect the check to panic.
#[test]
#[should_panic(expected = "recorded as both edge and unresolved row")]
fn overlap_invariant_detects_a_bound_reference_that_is_also_unresolved() {
    let (_tmp, root, indexer) = indexed_tree("overlap-258", ALIAS_FILES);
    assert_no_edge_and_unresolved_overlap(&indexer);
    let conn = rusqlite::Connection::open(root.join(".lidx").join(".lidx.sqlite")).unwrap();
    let inserted = conn
        .execute(
            "INSERT INTO unresolved_references
                (source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason,
                 evidence_start_line, graph_version)
             SELECT e.source_symbol_id, e.file_id, e.kind, 'th', 'th', 'no_candidates',
                    e.evidence_start_line, e.graph_version
             FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND t.qualname = 'pkgqa.mod.thing'",
            [],
        )
        .unwrap();
    assert_eq!(inserted, 1);
    assert_no_edge_and_unresolved_overlap(&indexer);
}

const WRAPPER_TSCONFIG: &str = r#"{"compilerOptions": {"paths": {"@/*": ["./*"]}}}"#;
const WRAPPER_V1: &str = "export function req(url: string) { return fetch(url); }\n\
    export const api = { get: (url: string) => req(url) };\n";
const WRAPPER_CALLER: &str = "import { api } from \"@/lib/w\";\n\
    export function list() { return api.get(\"/api/t\"); }\n";

/// (method) of every HTTP_CALL edge in the current graph version.
fn http_call_methods(indexer: &Indexer) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT detail FROM edges WHERE graph_version = ? AND kind = 'HTTP_CALL'")
        .unwrap();
    stmt.query_map(rusqlite::params![gv], |r| {
        let d: serde_json::Value = serde_json::from_str(&r.get::<_, String>(0)?).unwrap();
        Ok(d["method"].as_str().unwrap().to_string())
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

/// Edit only the wrapper file `lib/w.ts` to `v2`, sync it alone, and require
/// the importing call site's HTTP_CALL edges to match a fresh reindex.
fn wrapper_edit_matches_fresh(label: &str, v2: &str) -> (Indexer, Vec<String>) {
    let files = [
        ("tsconfig.json", WRAPPER_TSCONFIG),
        ("lib/w.ts", WRAPPER_V1),
        ("q/c.ts", WRAPPER_CALLER),
    ];
    let (tmp, repo_root, mut indexer) = indexed_tree(label, &files);
    assert_eq!(http_call_methods(&indexer), ["GET"], "precondition");
    common::write_files(&repo_root, &[("lib/w.ts", v2)]);
    indexer.sync_rel_paths(&["lib/w.ts".to_string()]).unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("tsconfig.json", WRAPPER_TSCONFIG),
        ("lib/w.ts", v2),
        ("q/c.ts", WRAPPER_CALLER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
    drop(tmp);
    let methods = http_call_methods(&indexer);
    (indexer, methods)
}

/// A wrapper that stops calling `fetch` is no longer an HTTP wrapper: its
/// importers' HTTP_CALL edges must go.
#[test]
fn incremental_wrapper_loses_fetch_drops_importer_http_call() {
    let (_indexer, methods) = wrapper_edit_matches_fresh(
        "wrapper-no-fetch",
        "export function req(url: string) { return url; }\n\
         export const api = { get: (url: string) => req(url) };\n",
    );
    assert!(methods.is_empty(), "stale HTTP_CALL: {methods:?}");
}

/// Renaming the wrapper's method orphans the importer's `api.get(..)` call.
#[test]
fn incremental_wrapper_method_rename_drops_importer_http_call() {
    let (_indexer, methods) = wrapper_edit_matches_fresh(
        "wrapper-rename",
        "export function req(url: string) { return fetch(url); }\n\
         export const api = { fetchAll: (url: string) => req(url) };\n",
    );
    assert!(methods.is_empty(), "stale HTTP_CALL: {methods:?}");
}

/// Changing the wrapper's HTTP method changes the importer's HTTP_CALL.
#[test]
fn incremental_wrapper_method_change_updates_importer_http_call() {
    let (_indexer, methods) = wrapper_edit_matches_fresh(
        "wrapper-method",
        "export function req(url: string, init?: RequestInit) { return fetch(url, init); }\n\
         export const api = { get: (url: string) => req(url, { method: \"POST\" }) };\n",
    );
    assert_eq!(methods, ["POST"]);
}

/// Issue #337: `import('./x.js')` is an IMPORTS edge (hence IMPORTS_FILE),
/// never a CALLS to `import`; an edit-then-sync matches a fresh reindex,
/// including the unresolved-reference rows.
#[test]
fn ts_dynamic_import_emits_imports_file_and_matches_fresh() {
    let index_ts = "import { helper } from './helper.js';\n\
                    export async function run() {\n\
                    \x20 const { buildApp } = await import('./app.js');\n\
                    \x20 const { plugin } = await import('./plugin.js');\n\
                    \x20 const ns = await import('./ns.js');\n\
                    \x20 await import('@opentelemetry/api');\n\
                    \x20 ns.go();\n\
                    \x20 buildApp();\n\
                    \x20 plugin();\n\
                    \x20 return helper;\n\
                    }\n";
    let files = [
        ("src/index.ts", index_ts),
        ("src/helper.ts", "export const helper = 1;\n"),
        ("src/app.ts", "export function buildApp() {}\n"),
        ("src/plugin.ts", "export function plugin() {}\n"),
        ("src/ns.ts", "export function go() {}\n"),
        (
            "src/sub/static.ts",
            "import { run } from '../index.js';\nexport const s = run;\n",
        ),
        (
            "src/sub/dynamic.ts",
            "export const d = () => import('../index.js');\n",
        ),
    ];
    let unresolved = |indexer: &Indexer| -> Vec<(String, Option<String>, String)> {
        let gv = indexer.db().current_graph_version().unwrap();
        let conn = indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT edge_kind, reference_name, reason FROM unresolved_references
                 WHERE graph_version = ? ORDER BY 1, 2, 3",
            )
            .unwrap();
        stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };

    let (_tmp, root, mut indexer) = indexed_tree("ts-dynamic-import", &files);
    let edited = format!("{index_ts}// touched\n");
    common::write_files(&root, &[("src/index.ts", edited.as_str())]);
    indexer
        .sync_rel_paths(&["src/index.ts".to_string()])
        .unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();

    let imports_file: Vec<_> = snapshot
        .iter()
        .filter(|e| e.kind == "IMPORTS_FILE")
        .filter_map(|e| e.target_qualname.as_deref())
        .collect();
    for want in ["src/app", "src/plugin", "src/helper", "src/ns"] {
        assert!(imports_file.contains(&want), "{want} in {imports_file:?}");
    }
    // A dynamic import of a parent-relative specifier resolves exactly like
    // the static one.
    let importers_of_index = |src: &str| -> Vec<String> {
        snapshot
            .iter()
            .filter(|e| e.kind == "IMPORTS_FILE" && e.source_qualname == src)
            .filter_map(|e| e.target_qualname.clone())
            .collect()
    };
    let static_target = importers_of_index("src/sub/static");
    assert_eq!(static_target.len(), 1, "{snapshot:#?}");
    assert_eq!(importers_of_index("src/sub/dynamic"), static_target);
    assert!(
        !snapshot.iter().any(|e| e
            .target_qualname
            .as_deref()
            .is_some_and(|t| t.ends_with(".import"))),
        "no edge to `import`: {snapshot:#?}"
    );
    assert!(
        !unresolved(&indexer)
            .iter()
            .any(|(_, name, _)| name.as_deref().is_some_and(|n| n.ends_with("import"))),
        "no unresolved `import` row"
    );

    let fresh_files: Vec<(&str, &str)> = std::iter::once(("src/index.ts", edited.as_str()))
        .chain(files[1..].iter().copied())
        .collect();
    let (_t, _r, fresh) = indexed_tree("ts-dynamic-import-fresh", &fresh_files);
    let fresh_gv = fresh.db().current_graph_version().unwrap();
    let fresh_snapshot = golden::snapshot_edges(fresh.db(), fresh_gv).unwrap();
    common::assert_matches_fresh(&snapshot, &fresh_snapshot);
    assert_eq!(unresolved(&indexer), unresolved(&fresh));
}

#[test]
fn incremental_ts_function_expression_calls_match_fresh() {
    let app_ts = "import { helper } from './lib';\nexport class Foo {\n  m() {}\n  run() {\n    list.forEach(function (x) {\n      this.m();\n      helper(x);\n    });\n  }\n}\n";
    let lib_ts = "export function helper(x: number) {}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree("ts-fn-expr", &[("app.ts", app_ts)]);
    common::write_files(&repo_root, &[("lib.ts", lib_ts)]);
    indexer.sync_rel_paths(&["lib.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        !snapshot
            .iter()
            .any(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("app.Foo.m")),
        "this.m() inside a function expression must not bind to Foo.m: {snapshot:#?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("lib.ts", lib_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_class_field_arrow_calls_match_fresh() {
    let app_ts = "import { mergePath } from './lib';\nexport class Hono {\n  fetch(r: string) {}\n  request = (input: string) => {\n    return this.fetch(mergePath('/', input));\n  };\n}\n";
    let lib_ts = "export function mergePath(a: string, b: string) { return a + b; }\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree("ts-field-arrow", &[("app.ts", app_ts)]);
    common::write_files(&repo_root, &[("lib.ts", lib_ts)]);
    indexer.sync_rel_paths(&["lib.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "app.Hono.request"
            && e.target_qualname.as_deref() == Some("lib.mergePath")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("lib.ts", lib_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_callback_local_const_calls_match_fresh() {
    let app_test_ts = "import { fail } from './lib';\ndescribe('x', () => {\n  it('y', () => {\n    const handler = wrap(() => { fail(); });\n  });\n});\n";
    let lib_ts = "export function fail() {}\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("ts-cb-const", &[("app.test.ts", app_test_ts)]);
    common::write_files(&repo_root, &[("lib.ts", lib_ts)]);
    indexer.sync_rel_paths(&["lib.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "app.test"
            && e.target_qualname.as_deref() == Some("lib.fail")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("app.test.ts", app_test_ts), ("lib.ts", lib_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_bare_dot_import_matches_fresh() {
    let test_ts = "import { del } from '.';\nexport function t() { return del('a'); }\n";
    let index_ts = "export function del(n: string) { return n; }\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("ts-bare-dot", &[("cookie/a.test.ts", test_ts)]);
    common::write_files(&repo_root, &[("cookie/index.ts", index_ts)]);
    indexer
        .sync_rel_paths(&["cookie/index.ts".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "cookie/a.test.t"
            && e.target_qualname.as_deref() == Some("cookie.del")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("cookie/a.test.ts", test_ts), ("cookie/index.ts", index_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_sibling_callback_same_name_locals_match_fresh() {
    let spec_ts = "import { HTTPException } from './http-exception';\ndescribe('x', () => {\n  it('a', () => { const e = new HTTPException(); e.getResponse(); });\n  it('b', () => { const e = new HTTPException(); e.getResponse(); });\n});\n";
    let lib_ts = "export class HTTPException {\n  getResponse() {}\n}\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("ts-sibling-locals", &[("e.test.ts", spec_ts)]);
    common::write_files(&repo_root, &[("http-exception.ts", lib_ts)]);
    indexer
        .sync_rel_paths(&["http-exception.ts".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "e.test"
            && e.target_qualname.as_deref() == Some("http-exception.HTTPException.getResponse")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("e.test.ts", spec_ts), ("http-exception.ts", lib_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_instanceof_narrowing_matches_fresh() {
    let handler_ts = "import { HTTPException } from './http-exception';\nexport const handle = (context: { error: unknown }) => {\n  if (context.error instanceof HTTPException) {\n    return context.error.getResponse();\n  }\n};\n";
    let lib_ts = "export class HTTPException {\n  getResponse() {}\n}\n";

    let (_tmp, repo_root, mut indexer) =
        indexed_tree("ts-instanceof", &[("handler.ts", handler_ts)]);
    common::write_files(&repo_root, &[("http-exception.ts", lib_ts)]);
    indexer
        .sync_rel_paths(&["http-exception.ts".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "handler.handle"
            && e.target_qualname.as_deref() == Some("http-exception.HTTPException.getResponse")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("handler.ts", handler_ts), ("http-exception.ts", lib_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_non_null_subscript_receiver_matches_fresh() {
    let router_ts = "import { Trie } from './trie';\nexport class Router {\n  #tries?: Record<string, Trie>\n  add(m: string) {\n    this.#tries![m].insert('a');\n  }\n}\n";
    let trie_ts = "export class Trie {\n  insert(p: string) {}\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree("ts-subscript", &[("router.ts", router_ts)]);
    common::write_files(&repo_root, &[("trie.ts", trie_ts)]);
    indexer.sync_rel_paths(&["trie.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "router.Router.add"
            && e.target_qualname.as_deref() == Some("trie.Trie.insert")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) =
        common::index_files(&[("router.ts", router_ts), ("trie.ts", trie_ts)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_pinned_receiver_type_matches_fresh() {
    let trie_ts = "import { Node } from './node';\nexport class Trie {\n  #root: Node = new Node();\n  add(p: string) {\n    this.#root.insert(p);\n    this.#root.base(p);\n  }\n}\n";
    let node_ts = "import { Base } from './base';\nexport class Node extends Base {\n  insert(p: string) {}\n}\n";
    let base_ts = "export class Base {\n  base(p: string) {}\n}\n";
    let decoy_ts = "export class Node {\n  insert(p: string) {}\n  base(p: string) {}\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-pinned",
        &[("reg/trie.ts", trie_ts), ("other/node.ts", decoy_ts)],
    );
    common::write_files(
        &repo_root,
        &[("reg/node.ts", node_ts), ("reg/base.ts", base_ts)],
    );
    indexer
        .sync_rel_paths(&["reg/node.ts".to_string(), "reg/base.ts".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    for target in ["reg/node.Node.insert", "reg/base.Base.base"] {
        assert!(
            snapshot.iter().any(|e| e.kind == "CALLS"
                && e.source_qualname == "reg/trie.Trie.add"
                && e.target_qualname.as_deref() == Some(target)),
            "{target}: {snapshot:#?}"
        );
    }

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("reg/trie.ts", trie_ts),
        ("other/node.ts", decoy_ts),
        ("reg/node.ts", node_ts),
        ("reg/base.ts", base_ts),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_object_literal_namespace_member_matches_fresh() {
    let lib_ts =
        "export const sign = (p: string) => p;\nexport const verifyWithJwks = (t: string) => t;\n";
    let index_before = "import { sign } from './jwt';\nexport const Jwt = { sign };\n";
    let index_after = "import { sign, verifyWithJwks } from './jwt';\nexport const Jwt = { sign, verifyWithJwks };\n";
    let caller_ts = "import { Jwt } from './jwt';\nexport function check(t: string) {\n  Jwt.verifyWithJwks(t);\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-object-ns",
        &[
            ("src/jwt/jwt.ts", lib_ts),
            ("src/jwt/index.ts", index_before),
            ("src/mw.ts", caller_ts),
        ],
    );
    common::write_files(&repo_root, &[("src/jwt/index.ts", index_after)]);
    indexer
        .sync_rel_paths(&["src/jwt/index.ts".to_string()])
        .unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "src/mw.check"
            && e.target_qualname.as_deref() == Some("src/jwt/jwt.verifyWithJwks")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("src/jwt/jwt.ts", lib_ts),
        ("src/jwt/index.ts", index_after),
        ("src/mw.ts", caller_ts),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_new_binds_constructor_matches_fresh() {
    let app_ts =
        "import { Context } from './context';\nexport function run() {\n  new Context('r');\n}\n";
    let ctx_before = "export class Context {\n  render() {}\n}\n";
    let ctx_after = "export class Context {\n  constructor(r: string) {}\n  render() {}\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-new-ctor",
        &[("app.ts", app_ts), ("context.ts", ctx_before)],
    );
    common::write_files(&repo_root, &[("context.ts", ctx_after)]);
    indexer.sync_rel_paths(&["context.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "app.run"
            && e.target_qualname.as_deref() == Some("context.Context.constructor")),
        "{snapshot:#?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("context.ts", ctx_after)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_new_after_constructor_removed_matches_fresh() {
    let app_ts =
        "import { Context } from './context';\nexport function run() {\n  new Context('r');\n}\n";
    let ctx_before = "export class Context {\n  constructor(r: string) {}\n  render() {}\n}\n";
    let ctx_after = "export class Context {\n  render() {}\n}\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-new-ctor-removed",
        &[("app.ts", app_ts), ("context.ts", ctx_before)],
    );
    common::write_files(&repo_root, &[("context.ts", ctx_after)]);
    indexer.sync_rel_paths(&["context.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("context.ts", ctx_after)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_value_reference_follows_renamed_export_matches_fresh() {
    let app_ts = "import { pick } from './util';\nexport function run(o: { f?: () => void }) {\n  return o.f ?? pick;\n}\n";
    let util_before = "export const pick = () => 1;\n";
    let util_after = "export const other = () => 1;\nexport const pick = () => 2;\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-value-ref",
        &[("app.ts", app_ts), ("util.ts", util_before)],
    );
    common::write_files(&repo_root, &[("util.ts", util_after)]);
    indexer.sync_rel_paths(&["util.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "app.run"
            && e.target_qualname.as_deref() == Some("util.pick")),
        "{snapshot:#?}"
    );
    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("util.ts", util_after)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn incremental_ts_type_use_follows_declaration_matches_fresh() {
    let app_ts =
        "import type { Opts } from './types';\nexport function run(o: Opts) {\n  return o;\n}\n";
    let types_before = "export type Other = number;\n";
    let types_after = "export type Other = number;\nexport type Opts = { a: number };\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(
        "ts-type-use",
        &[("app.ts", app_ts), ("types.ts", types_before)],
    );
    common::write_files(&repo_root, &[("types.ts", types_after)]);
    indexer.sync_rel_paths(&["types.ts".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "USES"
            && e.source_qualname == "app.run"
            && e.target_qualname.as_deref() == Some("types.Opts")),
        "{snapshot:#?}"
    );
    let (_fresh_tmp, fresh) = common::index_files(&[("app.ts", app_ts), ("types.ts", types_after)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

const CTOR_USER: &str = "namespace App { public class User { \
public T Make() { return new T(); } } }\n";
const CTOR_T_PLAIN: &str = "namespace App { public class T { public void Run() { } } }\n";
const CTOR_T_EXPLICIT: &str = "namespace App { public class T { public T() { } \
public void Run() { } } }\n";

/// The target `User.Make`'s `new T()` is bound to.
fn new_target(indexer: &Indexer) -> Option<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv)
        .unwrap()
        .into_iter()
        .find(|e| e.kind == "CALLS" && e.source_qualname == "App.User.Make")
        .and_then(|e| e.target_qualname)
}

fn assert_new_matches_fresh(indexer: &Indexer, t_src: &str) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[("T.cs", t_src), ("User.cs", CTOR_USER)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `new T()` binds to class `T` until `T` gains an explicit constructor,
/// which moves it to `T..ctor`; losing the constructor moves it back.
#[test]
fn csharp_new_follows_the_class_gaining_and_losing_an_explicit_constructor() {
    let (_tmp, root, mut indexer) = indexed_tree(
        "new-ctor",
        &[("T.cs", CTOR_T_PLAIN), ("User.cs", CTOR_USER)],
    );
    assert_eq!(new_target(&indexer).as_deref(), Some("App.T"));
    assert_new_matches_fresh(&indexer, CTOR_T_PLAIN);

    common::write_files(&root, &[("T.cs", CTOR_T_EXPLICIT)]);
    indexer.sync_rel_paths(&["T.cs".to_string()]).unwrap();
    assert_eq!(new_target(&indexer).as_deref(), Some("App.T..ctor"));
    assert_new_matches_fresh(&indexer, CTOR_T_EXPLICIT);

    common::write_files(&root, &[("T.cs", CTOR_T_PLAIN)]);
    indexer.sync_rel_paths(&["T.cs".to_string()]).unwrap();
    assert_eq!(new_target(&indexer).as_deref(), Some("App.T"));
    assert_new_matches_fresh(&indexer, CTOR_T_PLAIN);
}

const TS_NEW_USER: &str = "import { T } from './t';\nexport function make() { return new T(); }\n";
const TS_T_PLAIN: &str = "export class T { run() {} }\n";
const TS_T_EXPLICIT: &str = "export class T { constructor() {} run() {} }\n";

#[test]
fn typescript_new_follows_the_class_gaining_and_losing_a_constructor() {
    let finals = |t: &'static str| [("t.ts", t), ("user.ts", TS_NEW_USER)];
    let (_tmp, root, mut indexer) = indexed_tree("ts-new-ctor", &finals(TS_T_PLAIN));
    for t in [TS_T_EXPLICIT, TS_T_PLAIN] {
        common::write_files(&root, &[("t.ts", t)]);
        indexer.sync_rel_paths(&["t.ts".to_string()]).unwrap();
        common::assert_no_dangling_edge_targets(indexer.db());
        let gv = indexer.db().current_graph_version().unwrap();
        let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
        let (_t, fresh) = common::index_files(&finals(t));
        common::assert_matches_fresh(&snapshot, &fresh);
    }
}
