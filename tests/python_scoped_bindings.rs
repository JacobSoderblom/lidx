//! Python names bound inside a comprehension or a lambda are scoped to it,
//! and the variable of a `for` loop or comprehension over an annotated
//! container takes the container's element class.
//!
//! Fixture `py_scoped_bindings`: `cols.py` iterates annotated containers and
//! `use.py` uses a comprehension variable named like an imported function.

use lidx::indexer::Indexer;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// `(source qualname, resolved target qualname)` of every resolved CALLS edge.
fn resolved_calls() -> Vec<(String, String)> {
    let mut repo = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    repo.push(format!(
        "lidx-py-scoped-{nanos}-{}",
        TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/py_scoped_bindings");
    copy_dir(&fixture, &repo);
    let db_path = repo.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ?",
        )
        .unwrap();
    let rows = stmt
        .query_map([gv], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<(String, String)>>>()
        .unwrap();
    let _ = std::fs::remove_dir_all(&repo);
    rows
}

fn has(calls: &[(String, String)], source: &str, target: &str) -> bool {
    calls.iter().any(|(s, t)| s == source && t == target)
}

fn calls_from<'a>(calls: &'a [(String, String)], source: &str) -> Vec<&'a str> {
    calls
        .iter()
        .filter(|(s, _)| s == source)
        .map(|(_, t)| t.as_str())
        .collect()
}

#[test]
fn comprehension_over_a_mapping_attribute_binds_the_value_class() {
    let calls = resolved_calls();
    assert!(has(&calls, "cols.Table.defs", "cols.Col.to_def"));
    assert!(has(&calls, "cols.Table.by_name", "cols.Col.to_def"));
}

#[test]
fn comprehension_and_loop_over_an_annotated_parameter_bind_the_element_class() {
    let calls = resolved_calls();
    assert!(has(&calls, "cols.nullable", "cols.Col.with_nullable"));
    assert!(has(&calls, "cols.each", "cols.Col.to_def"));
    assert!(has(&calls, "cols.numbered", "cols.Col.with_nullable"));
}

#[test]
fn a_declared_container_survives_reassignment_that_keeps_it() {
    let calls = resolved_calls();
    assert!(has(&calls, "cols.reassigned", "cols.Col.to_def"));
    assert!(has(&calls, "cols.reassigned", "cols.Col.with_nullable"));
}

#[test]
fn a_declared_variable_keeps_its_declared_type_across_every_reassignment() {
    let calls = resolved_calls();
    assert!(has(&calls, "cols.reassigned_to_other", "cols.Col.to_def"));
    assert!(has(
        &calls,
        "cols.reassigned_to_new_list",
        "cols.Col.with_nullable"
    ));
    assert!(has(&calls, "cols.reassigned_by_mapping", "cols.Col.to_def"));
    assert!(has(
        &calls,
        "cols.declared_class_survives_reassignment",
        "cols.Col.to_def"
    ));
}

#[test]
fn an_undeclared_variable_assigned_twice_is_unknown() {
    let calls = resolved_calls();
    assert!(
        calls_from(&calls, "cols.undeclared_class_collapses")
            .iter()
            .all(|t| *t != "cols.Col.to_def")
    );
}

#[test]
fn a_comprehension_takes_its_element_class_from_its_body() {
    let calls = resolved_calls();
    let from = calls_from(&calls, "cols.comprehension_body_type");
    // a conditional of the loop variable and a `-> "Col"` method, a `-> Self`
    // method, and a function annotated `-> Col`
    assert!(
        from.iter()
            .filter(|t| **t == "cols.Col.with_nullable")
            .count()
            >= 2
    );
    assert!(from.contains(&"cols.Col.copy"));
    assert!(
        calls_from(&calls, "cols.comprehension_body_unknown")
            .iter()
            .all(|t| *t != "cols.Col.with_nullable")
    );
}

#[test]
fn a_nested_def_has_its_own_scope() {
    let calls = resolved_calls();
    assert!(calls_from(&calls, "cols.nested_def_has_its_own_scope").is_empty());
}

#[test]
fn a_nested_def_types_its_own_locals_from_a_private_class() {
    let calls = resolved_calls();
    assert!(has(
        &calls,
        "cols.private_class_in_nested_def",
        "cols._Builder.build"
    ));
}

#[test]
fn a_comprehension_target_is_not_visible_to_its_own_iterable() {
    let calls = resolved_calls();
    assert!(calls_from(&calls, "cols.clause_targets").is_empty());
}

#[test]
fn a_lambda_default_is_evaluated_outside_the_lambda() {
    let calls = resolved_calls();
    assert!(has(&calls, "use.lambda_default", "pkg.dp.table.table"));
}

#[test]
fn a_name_re_exported_by_a_package_binds_the_member_not_the_module() {
    let calls = resolved_calls();
    assert!(has(&calls, "use.build", "pkg.dp.table.table"));
    assert!(has(&calls, "use.build", "pkg.ds.table.table"));
    assert!(has(
        &calls,
        "use.reexported_through_the_package",
        "pkg.dp.table.table"
    ));
    assert!(
        calls
            .iter()
            .all(|(_, target)| target != "pkg.dp.table" && target != "pkg.ds.table"),
        "a call never targets a module: {calls:?}"
    );
}

#[test]
fn untyped_or_builtin_elements_stay_unbound() {
    let calls = resolved_calls();
    assert!(calls_from(&calls, "cols.untyped").is_empty());
    assert!(calls_from(&calls, "cols.not_a_class").is_empty());
    assert!(calls_from(&calls, "cols.Table.name_calls").is_empty());
}

#[test]
fn a_comprehension_variable_does_not_hide_an_import_called_outside_it() {
    let calls = resolved_calls();
    assert!(has(&calls, "use.build", "pkg.dp.table.table"));
}

#[test]
fn a_lambda_parameter_does_not_hide_an_import_called_outside_it() {
    let calls = resolved_calls();
    assert!(has(&calls, "use.with_callback", "pkg.ds.table.table"));
}

#[test]
fn a_comprehension_variable_still_shadows_the_import_inside_it() {
    let calls = resolved_calls();
    assert!(calls_from(&calls, "use.shadowed").is_empty());
}
