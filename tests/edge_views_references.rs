//! Issue #408: a symbol reached only by a symbol-level `REFERENCES` edge is
//! alive (`dead_symbols`), not an execution target (`trace_flow`), and is
//! reported under `explain_symbol`'s `relations`, never `callers`.
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
         VALUES (?1, ?2, ?3, 'REFERENCES', ?4, ?5, 'exact')",
        rusqlite::params![file_id, source_id, target_id, target, gv],
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

    // (c) explain_symbol reports it under relations.REFERENCES, not callers.
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
    let group = &explained["relations"]["REFERENCES"];
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
        referencing["relations"]["REFERENCES"]["items"][0]["direction"],
        "outgoing"
    );
}

#[test]
fn relations_cap_items_per_kind_and_keep_the_true_total() {
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
    let group = &explained["relations"]["REFERENCES"];
    assert_eq!(group["total"], 8, "{explained}");
    assert_eq!(group["items"].as_array().unwrap().len(), 5, "{explained}");
    assert_eq!(explained["callers_total"], 0, "{explained}");
}
