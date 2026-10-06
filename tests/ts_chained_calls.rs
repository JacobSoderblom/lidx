//! Issue #320: TypeScript calls whose receiver is a call expression
//! (`make().stage(1).storage()`) record a CALLS edge per link, resolved
//! through declared return types and never bound by name alone.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::params;
use std::path::PathBuf;

const FIXTURE: &str = r#"export class Builder {
  stage(x: number): Builder { return this; }
  storage(): Builder { return this; }
}
export function make(): Builder { return new Builder(); }
export function use() { return make().stage(1).storage(); }
"#;

/// The fixture with every return type removed.
const UNTYPED: &str = r#"export class Builder {
  stage(x: number) { return this; }
  storage() { return this; }
}
export function make() { return new Builder(); }
export function use() { return make().stage(1).storage(); }
"#;

/// One CALLS edge row as stored.
#[derive(Debug)]
struct Call {
    target: String,
    bound: bool,
    receiver_type: Option<String>,
    resolution_kind: Option<String>,
}

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let root = tmp.path().to_path_buf();
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

fn calls_from(indexer: &Indexer, src: &str) -> Vec<Call> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, ''), e.target_symbol_id IS NOT NULL,
                    e.receiver_type, e.resolution_kind
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(params![gv, src], |r| {
        Ok(Call {
            target: r.get(0)?,
            bound: r.get(1)?,
            receiver_type: r.get(2)?,
            resolution_kind: r.get(3)?,
        })
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn find<'a>(calls: &'a [Call], target: &str) -> &'a Call {
    calls
        .iter()
        .find(|c| c.target == target)
        .unwrap_or_else(|| panic!("no call to {target}: {calls:?}"))
}

/// Names of `unresolved_references` rows (the not-yet-bound references).
fn unresolved_names(indexer: &Indexer) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT reference_name FROM unresolved_references WHERE graph_version = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(params![gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn chained_calls_resolve_through_declared_return_types() {
    let (_tmp, _root, indexer) = index(&[("b.ts", FIXTURE)]);
    let calls = calls_from(&indexer, "b.use");
    assert!(find(&calls, "b.make").bound, "{calls:?}");
    for (target, name) in [
        ("b.Builder.stage", "stage"),
        ("b.Builder.storage", "storage"),
    ] {
        let call = find(&calls, target);
        assert!(call.bound, "{name} not bound: {calls:?}");
        // `Builder` is declared in this file, so the type is pinned to it.
        assert_eq!(
            call.receiver_type.as_deref(),
            Some("\u{2}b.Builder"),
            "{call:?}"
        );
        // Bound by the resolver's receiver-type tier, not by name.
        assert_eq!(
            call.resolution_kind.as_deref(),
            Some("receiver_type"),
            "{call:?}"
        );
    }
}

#[test]
fn chained_calls_without_return_types_are_unresolved_not_bound() {
    let (_tmp, _root, indexer) = index(&[("b.ts", UNTYPED)]);
    let calls = calls_from(&indexer, "b.use");
    // Only the innermost `make()` binds; Builder.stage/storage exist but are
    // never picked by method name alone.
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].target, "b.make");
    assert_eq!(unresolved_names(&indexer), ["stage", "storage"]);
}

#[test]
fn chains_in_callbacks_and_promise_chains_are_recorded() {
    let src = r#"export class Builder {
  stage(x: number): Builder { return this; }
}
export function make(): Builder { return new Builder(); }
export function cb(arr: number[]) { return arr.map(x => make().stage(x)); }
export function prom() { return fetchData().then(f).catch(g); }
"#;
    let (_tmp, _root, indexer) = index(&[("b.ts", src)]);
    let cb = calls_from(&indexer, "b.cb");
    assert!(find(&cb, "b.Builder.stage").bound, "{cb:?}");
    assert!(find(&cb, "b.make").bound, "{cb:?}");

    let names = unresolved_names(&indexer);
    for name in ["then", "catch"] {
        assert!(names.iter().any(|n| n == name), "{name} dropped: {names:?}");
    }
}

#[test]
fn chain_on_external_object_is_not_bound_to_a_repo_method() {
    let decoy = "export class Decoy {\n  then(f: number) { return f; }\n  get(u: string) { return u; }\n}\n";
    let user = "import axios from 'axios';\nexport function fetchIt(u: string) { return axios.get(u).then(r => r); }\n";
    let check = |indexer: &Indexer| {
        let calls = calls_from(indexer, "user.fetchIt");
        // `axios.get` is an external stub; `.then` on its result stays an
        // unresolved reference, never bound to `Decoy.then`/`Decoy.get`.
        assert_eq!(
            find(&calls, "ext:axios.get").resolution_kind.as_deref(),
            Some("external")
        );
        assert!(
            calls.iter().all(|c| !c.target.contains("Decoy")),
            "{calls:?}"
        );
        assert!(unresolved_names(indexer).iter().any(|n| n == "then"));
    };
    // Decoy indexed alongside, then added after the fact (retry pass).
    let (_tmp, _root, indexer) = index(&[("decoy.ts", decoy), ("user.ts", user)]);
    check(&indexer);
    let (_tmp, root, mut indexer) = index(&[("user.ts", user)]);
    common::write_files(&root, &[("decoy.ts", decoy)]);
    indexer.sync_rel_paths(&["decoy.ts".to_string()]).unwrap();
    check(&indexer);
}

#[test]
fn analyze_impact_upstream_of_builder_stage_lists_the_chained_caller() {
    let (_tmp, root, indexer) = index(&[("b.ts", FIXTURE)]);
    drop(indexer);
    let response = rpc::call(
        root.clone(),
        root.join(".lidx").join(".lidx.sqlite"),
        "analyze_impact".to_string(),
        r#"{"qualname":"b.Builder.stage","direction":"upstream","max_depth":2}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let affected = value["result"]["affected"].as_array().unwrap();
    assert!(
        affected.iter().any(|a| a["symbol"]["qualname"] == "b.use"),
        "{response}"
    );
}

#[test]
fn incremental_reindex_matches_fresh_index() {
    let (_tmp, root, mut indexer) = index(&[("b.ts", FIXTURE)]);
    let edited = format!("{FIXTURE}export function other() {{ return make().stage(2); }}\n");
    common::write_files(&root, &[("b.ts", &edited)]);
    indexer.sync_rel_paths(&["b.ts".to_string()]).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let incremental = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let inc_unresolved = unresolved_names(&indexer);

    let (_tmp2, _root2, fresh) = index(&[("b.ts", &edited)]);
    let fgv = fresh.db().current_graph_version().unwrap();
    common::assert_matches_fresh(
        &incremental,
        &golden::snapshot_edges(fresh.db(), fgv).unwrap(),
    );
    assert_eq!(inc_unresolved, unresolved_names(&fresh));
}
