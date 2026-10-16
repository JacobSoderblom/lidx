//! An importer's calls through a package re-export keep the same
//! `resolution_kind` and confidence after an incremental sync or reindex as
//! on a fresh index (issue #528), including when the edited `__init__`
//! belongs to a *different* package than the one the call goes through.

mod common;

use common::golden::{self, EdgeKey};
use lidx::indexer::Indexer;
use std::collections::BTreeSet;

type Files = Vec<(&'static str, &'static str)>;

const PKG_INIT: &str = "from .a import fa\nfrom .b import fb\n";
const OTHER_INIT: &str = "from .impl import fc\n";
const USER: &str = "from pkg import fa, fb\nfrom other import fc\n\n\ndef run():\n    fa()\n    fb()\n    return fc()\n";

fn base() -> Files {
    vec![
        ("pkg/__init__.py", PKG_INIT),
        ("pkg/a.py", "def fa():\n    return 1\n"),
        ("pkg/b.py", "def fb():\n    return 2\n"),
        ("other/__init__.py", OTHER_INIT),
        ("other/impl.py", "def fc():\n    return 3\n"),
        ("user.py", USER),
    ]
}

fn with(files: &Files, edits: &Files) -> Files {
    let mut out = files.clone();
    for (path, content) in edits {
        match out.iter_mut().find(|(p, _)| p == path) {
            Some(entry) => entry.1 = content,
            None => out.push((path, content)),
        }
    }
    out
}

/// Index `before`, apply `edits`, bring the index up to date by `sync` or
/// `reindex`, and require the rows (kind, confidence, count) to equal a fresh
/// index of the edited tree. Returns the snapshot.
fn check(before: &Files, edits: &Files, reindex: bool) -> BTreeSet<EdgeKey> {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-labels-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, before);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    common::write_files(&root, edits);
    if reindex {
        indexer.reindex().unwrap();
    } else {
        let paths: Vec<String> = edits.iter().map(|(p, _)| p.to_string()).collect();
        indexer.sync_rel_paths(&paths).unwrap();
    }
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&with(before, edits));
    common::assert_matches_fresh(&snapshot, &fresh);
    snapshot
}

fn both(before: &Files, edits: &Files) -> BTreeSet<EdgeKey> {
    check(before, edits, true);
    check(before, edits, false)
}

fn label(snapshot: &BTreeSet<EdgeKey>, target: &str) -> Option<String> {
    snapshot
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.source_qualname == "user.run"
                && e.target_qualname.as_deref() == Some(target)
        })
        .unwrap_or_else(|| panic!("no user.run -> {target} edge in {snapshot:#?}"))
        .resolution_kind
        .clone()
}

fn import(snapshot: &BTreeSet<EdgeKey>, target: &str) {
    assert_eq!(
        label(snapshot, target).as_deref(),
        Some("import"),
        "{target}"
    );
}

#[test]
fn removing_a_line_from_one_init_keeps_the_other_packages_calls_labelled_import() {
    let snapshot = both(&base(), &vec![("pkg/__init__.py", "from .a import fa\n")]);
    import(&snapshot, "other.impl.fc");
    import(&snapshot, "pkg.a.fa");
}

#[test]
fn editing_the_other_init_keeps_the_first_packages_calls_labelled_import() {
    let snapshot = both(
        &base(),
        &vec![("other/__init__.py", "from .impl import fc\n\nX = 1\n")],
    );
    import(&snapshot, "pkg.b.fb");
    let snapshot = both(
        &base(),
        &vec![(
            "other/__init__.py",
            "from .impl import fc\nfrom .impl import fc as fd\n",
        )],
    );
    import(&snapshot, "pkg.a.fa");
}

fn star() -> Files {
    with(
        &base(),
        &vec![
            ("pkg/__init__.py", "from .a import *\nfrom .b import *\n"),
            ("other/__init__.py", "from .impl import *\n"),
        ],
    )
}

#[test]
fn a_star_import_edit_keeps_the_other_packages_calls_labelled_import() {
    let snapshot = both(&star(), &vec![("pkg/__init__.py", "from .a import *\n")]);
    import(&snapshot, "other.impl.fc");
    let snapshot = both(
        &star(),
        &vec![("other/__init__.py", "from .impl import *\n\nY = 2\n")],
    );
    import(&snapshot, "pkg.a.fa");
}

fn aliased() -> Files {
    with(
        &base(),
        &vec![
            (
                "pkg/__init__.py",
                "from .a import fa as pa\nfrom .b import fb as pb\n",
            ),
            ("other/__init__.py", "from .impl import fc as oc\n"),
            (
                "user.py",
                "from pkg import pa, pb\nfrom other import oc\n\n\ndef run():\n    pa()\n    pb()\n    return oc()\n",
            ),
        ],
    )
}

#[test]
fn an_alias_edit_keeps_the_other_packages_calls_labelled_import() {
    let snapshot = both(
        &aliased(),
        &vec![("pkg/__init__.py", "from .a import fa as pa\n")],
    );
    import(&snapshot, "other.impl.fc");
    let snapshot = both(
        &aliased(),
        &vec![("other/__init__.py", "from .impl import fc as oc\n\nZ = 3\n")],
    );
    import(&snapshot, "pkg.b.fb");
}

#[test]
fn an_importer_sorting_before_both_packages_matches_fresh_too() {
    let before: Files = base()
        .into_iter()
        .map(|(p, c)| {
            if p == "user.py" {
                ("a_user.py", c)
            } else {
                (p, c)
            }
        })
        .collect();
    for edits in [
        vec![("pkg/__init__.py", "from .a import fa\n")],
        vec![("other/__init__.py", "from .impl import fc\n\nX = 1\n")],
    ] {
        check(&before, &edits, true);
        check(&before, &edits, false);
    }
}

#[test]
fn a_re_exported_call_is_labelled_import_on_a_fresh_index() {
    let (_tmp, fresh) = common::index_files(&base());
    import(&fresh, "other.impl.fc");
}
