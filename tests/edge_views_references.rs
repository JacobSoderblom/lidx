//! Issue #408: a symbol reached only by a symbol-level `REFERENCES` edge is
//! alive (`dead_symbols`), not an execution target (`trace_flow`), and is
//! reported under `explain_symbol`'s `other_edges`, never `callers`.
//!
//! The edge is inserted straight into the database: no extractor emits
//! `REFERENCES` between symbols yet, and the named edge views must already
//! place it correctly when one does.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::Connection;
use serde_json::{Value, json};
use std::path::Path;

const SRC: &str = "def registered():\n    return 1\n\n\ndef orphan():\n    return 2\n\n\ndef table():\n    return [registered]\n";

fn call(indexer: &mut Indexer, method: &str, params: Value) -> Value {
    rpc::handle_method(indexer, method, params).unwrap()
}

fn dead(indexer: &mut Indexer) -> Vec<String> {
    call(indexer, "dead_symbols", json!({}))["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["qualname"].as_str().unwrap().to_string())
        .collect()
}

fn insert_references_edge(db_path: &Path, source: &str, target: &str) {
    insert_edge(db_path, "REFERENCES", source, target);
}

fn insert_edge(db_path: &Path, kind: &str, source: &str, target: &str) {
    let conn = Connection::open(db_path).unwrap();
    let (gv, file_id, source_id, target_id): (i64, i64, i64, i64) = conn
        .query_row(
            "SELECT s.graph_version, s.file_id, s.id, t.id
             FROM symbols s JOIN symbols t ON t.graph_version = s.graph_version
             WHERE s.qualname = ?1 AND t.qualname = ?2",
            [source, target],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    conn.execute(
        "INSERT INTO edges (file_id, source_symbol_id, target_symbol_id, kind,
                            target_qualname, graph_version, resolution_kind)
         VALUES (?1, ?2, ?3, ?6, ?4, ?5, 'exact')",
        rusqlite::params![file_id, source_id, target_id, target, gv, kind],
    )
    .unwrap();
}

#[test]
fn references_only_symbol_is_live_not_executed_and_is_a_relation() {
    let (_tmp, root, db_path) = common::index_repo("lidx-edge-views-", &[("m.py", SRC)]);
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();

    let before = dead(&mut indexer);
    assert!(
        before.contains(&"m.registered".to_string()),
        "baseline: nothing uses registered, got {before:?}"
    );

    insert_references_edge(&db_path, "m.table", "m.registered");

    // (a) a REFERENCES edge keeps the target alive; an unreferenced
    // sibling stays dead.
    let after = dead(&mut indexer);
    assert!(
        !after.contains(&"m.registered".to_string()),
        "REFERENCES must count as a use, got {after:?}"
    );
    assert!(after.contains(&"m.orphan".to_string()), "got {after:?}");

    // (b) REFERENCES is not an execution edge: trace_flow never walks it
    // with default kinds.
    let trace = call(
        &mut indexer,
        "trace_flow",
        json!({"start_qualname": "m.table", "direction": "downstream", "max_hops": 3}),
    );
    let hops = trace["hops"].as_array().cloned().unwrap_or_default();
    assert!(
        !hops
            .iter()
            .any(|h| h["symbol"]["qualname"] == "m.registered" || h["edge_kind"] == "REFERENCES"),
        "trace_flow must not follow REFERENCES: {trace}"
    );

    // (c) explain_symbol reports it under other_edges.REFERENCES, not callers.
    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "m.registered", "sections": ["callers", "callees"]}),
    );
    let callers = explained["callers"].as_array().cloned().unwrap_or_default();
    assert!(
        callers.is_empty(),
        "a REFERENCES edge is not a caller: {explained}"
    );
    assert_eq!(explained["callers_total"], 0, "{explained}");
    let group = &explained["other_edges"]["REFERENCES"];
    assert_eq!(group["total"], 1, "{explained}");
    assert_eq!(group["items"][0]["symbol"]["qualname"], "m.table");
    assert_eq!(group["items"][0]["direction"], "incoming");

    // The referencing side sees it as an outgoing relation, not a callee.
    let referencing = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "m.table", "sections": ["callers", "callees"]}),
    );
    assert_eq!(referencing["callees_total"], 0, "{referencing}");
    assert_eq!(
        referencing["other_edges"]["REFERENCES"]["items"][0]["direction"],
        "outgoing"
    );
}

#[test]
fn other_edges_cap_items_per_kind_and_keep_the_true_total() {
    let mut src = String::from("def target():\n    return 1\n\n\n");
    for i in 0..8 {
        src.push_str(&format!("def user{i}():\n    return 0\n\n\n"));
    }
    let (_tmp, root, db_path) = common::index_repo("lidx-edge-views-cap-", &[("m.py", &src)]);
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    for i in 0..8 {
        insert_references_edge(&db_path, &format!("m.user{i}"), "m.target");
    }

    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "m.target", "sections": ["callers"]}),
    );
    let group = &explained["other_edges"]["REFERENCES"];
    assert_eq!(group["total"], 8, "{explained}");
    assert_eq!(group["items"].as_array().unwrap().len(), 5, "{explained}");
    assert_eq!(explained["callers_total"], 0, "{explained}");
}

const OPTIONS_APP: &str = "class DatabaseOptions:\n    connection_string = ''\n\n\n\
def consumer0():\n    return 0\n\n\ndef consumer1():\n    return 1\n\n\n\
def consumer2():\n    return 2\n\n\ndef consumer3():\n    return 3\n\n\n\
def consumer4():\n    return 4\n\n\ndef consumer5():\n    return 5\n\n\n\
def consumer6():\n    return 6\n\n\ndef consumer7():\n    return 7\n";

const OPTIONS_TEST: &str = "def test_database_options():\n    return True\n";

fn options_repo() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let (tmp, root, db_path) = common::index_repo(
        "lidx-edge-views-options-",
        &[
            ("app/options.py", OPTIONS_APP),
            ("tests/test_options.py", OPTIONS_TEST),
        ],
    );
    for i in 0..8 {
        insert_edge(
            &db_path,
            "CONFIG_BIND",
            &format!("app.options.consumer{i}"),
            "app.options.DatabaseOptions",
        );
    }
    insert_edge(
        &db_path,
        "CONFIG_BIND",
        "tests.test_options.test_database_options",
        "app.options.DatabaseOptions",
    );
    (tmp, root, db_path)
}

/// Regression (#408 review): CONFIG_BIND consumers stay in `callers` and
/// `tests` (and the binding stays a callee), exactly as before the split;
/// nothing leaks into `other_edges`.
#[test]
fn config_bind_consumers_stay_callers_and_tests() {
    let (_tmp, root, db_path) = options_repo();
    let mut indexer = Indexer::new(root, db_path).unwrap();

    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.DatabaseOptions", "max_refs": 20}),
    );
    assert_eq!(explained["callers_total"], 9, "{explained}");
    assert_eq!(explained["tests_total"], 1, "{explained}");
    assert!(explained.get("other_edges").is_none(), "{explained}");

    let consumer = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.consumer0", "sections": ["callees"]}),
    );
    assert_eq!(consumer["callees_total"], 1, "{consumer}");
    assert_eq!(
        consumer["callees"][0]["symbol"]["qualname"],
        "app.options.DatabaseOptions"
    );
}

/// An options class whose only consumers bind it through CONFIG_BIND is live.
#[test]
fn config_bind_keeps_options_class_alive() {
    let (_tmp, root, db_path) =
        common::index_repo("lidx-edge-views-dead-", &[("app/options.py", OPTIONS_APP)]);
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    assert!(dead(&mut indexer).contains(&"app.options.DatabaseOptions".to_string()));
    insert_edge(
        &db_path,
        "CONFIG_BIND",
        "app.options.consumer0",
        "app.options.DatabaseOptions",
    );
    let after = dead(&mut indexer);
    assert!(
        !after.contains(&"app.options.DatabaseOptions".to_string()),
        "{after:?}"
    );
}

#[test]
fn other_edges_order_mark_tests_hint_truncation_and_gate_uses() {
    let (_tmp, root, db_path) = options_repo();
    // Same shape, REFERENCES instead: 8 production users + 1 test user.
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    for i in (0..8).rev() {
        insert_references_edge(
            &db_path,
            &format!("app.options.consumer{i}"),
            "app.options.DatabaseOptions",
        );
    }
    insert_references_edge(
        &db_path,
        "tests.test_options.test_database_options",
        "app.options.DatabaseOptions",
    );
    insert_edge(
        &db_path,
        "USES",
        "app.options.consumer0",
        "app.options.DatabaseOptions",
    );

    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.DatabaseOptions", "sections": ["callers"]}),
    );
    let group = &explained["other_edges"]["REFERENCES"];
    assert_eq!(group["total"], 9, "{explained}");
    let names: Vec<&str> = group["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["symbol"]["qualname"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "app.options.consumer0",
            "app.options.consumer1",
            "app.options.consumer2",
            "app.options.consumer3",
            "app.options.consumer4"
        ],
        "production first, by file and line"
    );
    assert!(group["items"][0].get("is_test").is_none());
    // USES is opt-in.
    assert!(
        explained["other_edges"].get("USES").is_none(),
        "{explained}"
    );
    // Truncation points at the full list.
    assert!(
        explained["next_hops"].as_array().unwrap().iter().any(|h| {
            h["method"] == "analyze_impact" && h["params"]["kinds"] == json!(["REFERENCES"])
        }),
        "{explained}"
    );

    // Explicit section: USES appears, and test items are marked.
    let full = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.DatabaseOptions", "sections": ["other_edges"]}),
    );
    assert_eq!(full["other_edges"]["USES"]["total"], 1, "{full}");
    let test_ref = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "tests.test_options.test_database_options", "sections": ["other_edges"]}),
    );
    assert_eq!(
        test_ref["other_edges"]["REFERENCES"]["items"][0]["direction"],
        "outgoing"
    );
    let marked = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.consumer0", "sections": ["other_edges"], "max_refs": 20}),
    );
    assert!(
        marked["other_edges"].get("REFERENCES").is_some(),
        "{marked}"
    );
}

#[test]
fn outgoing_hierarchy_stays_in_implements_incoming_is_an_other_edge() {
    let src = "class Base:\n    pass\n\n\nclass Child(Base):\n    pass\n";
    let (_tmp, root, db_path) = common::index_repo("lidx-edge-views-hier-", &[("m.py", src)]);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let child = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "m.Child"}),
    );
    assert_eq!(child["implements_total"], 1, "{child}");
    assert!(child.get("other_edges").is_none(), "{child}");
    let base = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "m.Base"}),
    );
    assert_eq!(base["other_edges"]["EXTENDS"]["total"], 1, "{base}");
    assert_eq!(
        base["other_edges"]["EXTENDS"]["items"][0]["direction"],
        "incoming"
    );
}

#[test]
fn test_sources_are_marked_and_sorted_last() {
    let (_tmp, root, db_path) = options_repo();
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    insert_references_edge(
        &db_path,
        "tests.test_options.test_database_options",
        "app.options.DatabaseOptions",
    );
    insert_references_edge(
        &db_path,
        "app.options.consumer3",
        "app.options.DatabaseOptions",
    );
    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "app.options.DatabaseOptions", "sections": ["other_edges"]}),
    );
    let items = explained["other_edges"]["REFERENCES"]["items"]
        .as_array()
        .unwrap();
    assert_eq!(items[0]["symbol"]["qualname"], "app.options.consumer3");
    assert!(items[0].get("is_test").is_none());
    assert_eq!(
        items[1]["symbol"]["qualname"],
        "tests.test_options.test_database_options"
    );
    assert_eq!(items[1]["is_test"], true);
}
