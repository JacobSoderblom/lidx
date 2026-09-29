//! Issue #188: config-scoped traversal visits per (node, entry URI). Built
//! straight in the DB so the arrival order of each edge is controlled.

use lidx::db::Db;
use lidx::impact::layers::direct::{TraversalDirection, analyze_direct_impact};
use lidx::indexer::extract::{EdgeInput, ReceiverType, SymbolInput};
use lidx::traversal::{TraceConfig, TraceDirection, trace_flow};
use std::collections::{BTreeSet, HashMap};

struct Graph {
    db: Db,
    ids: HashMap<String, i64>,
    _tmp: tempfile::TempDir,
}

fn sym(name: &str, line: i64) -> SymbolInput {
    SymbolInput {
        kind: "function".to_string(),
        name: name.to_string(),
        qualname: format!("app.{name}"),
        start_line: line,
        start_col: 0,
        end_line: line + 5,
        end_col: 0,
        start_byte: 0,
        end_byte: 100,
        signature: None,
        docstring: None,
    }
}

fn edge(kind: &str, src: &str, target: &str) -> EdgeInput {
    EdgeInput {
        kind: kind.to_string(),
        source_qualname: Some(format!("app.{src}")),
        target_qualname: Some(if target.contains("://") {
            target.to_string()
        } else {
            format!("app.{target}")
        }),
        detail: target
            .contains("://")
            .then(|| format!(r#"{{"config_uri":"{target}","role":"r"}}"#)),
        evidence_snippet: None,
        evidence_start_line: None,
        evidence_end_line: None,
        confidence: Some(1.0),
        trace_id: None,
        span_id: None,
        event_ts: None,
        receiver_type: ReceiverType::NotTracked,
        import_candidates: Vec::new(),
        bare_call: false,
        call_shape: None,
    }
}

fn graph(names: &[&str], edges: &[(&str, &str, &str)]) -> Graph {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut db = Db::new(&tmp.path().join("g.db")).unwrap();
    let file_id = db.upsert_file("app.py", "h", "python", 100, 0).unwrap();
    let symbols: Vec<SymbolInput> = names
        .iter()
        .enumerate()
        .map(|(i, n)| sym(n, 10 * i as i64 + 1))
        .collect();
    let inserted = db
        .insert_symbols(file_id, "app.py", &symbols, 1, None)
        .unwrap();
    let ids: HashMap<String, i64> = inserted.iter().map(|s| (s.name.clone(), s.id)).collect();
    let map: HashMap<String, i64> = inserted
        .iter()
        .map(|s| (s.qualname.clone(), s.id))
        .collect();
    let inputs: Vec<EdgeInput> = edges.iter().map(|(k, s, t)| edge(k, s, t)).collect();
    db.insert_edges(file_id, &inputs, &map, 1, None).unwrap();
    Graph { db, ids, _tmp: tmp }
}

const KINDS: [&str; 4] = ["CALLS", "CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"];

fn trace(g: &Graph, start: &str) -> lidx::traversal::TraceResult {
    let config = TraceConfig {
        direction: TraceDirection::Downstream,
        max_hops: 6,
        ..Default::default()
    };
    trace_flow(&g.db, vec![g.ids[start]], None, None, 1, &config).unwrap()
}

fn impact(g: &Graph, start: &str) -> lidx::impact::types::LayerResult {
    analyze_direct_impact(
        &g.db,
        &[g.ids[start]],
        6,
        TraversalDirection::Downstream,
        &KINDS.iter().map(|k| k.to_string()).collect(),
        &[],
        true,
        1000,
        None,
        1,
    )
    .unwrap()
}

fn hop_names(r: &lidx::traversal::TraceResult) -> BTreeSet<String> {
    r.hops.iter().map(|h| h.symbol.name.clone()).collect()
}

fn impact_names(g: &Graph, r: &lidx::impact::types::LayerResult) -> BTreeSet<String> {
    let by_id: HashMap<i64, &String> = g.ids.iter().map(|(n, i)| (*i, n)).collect();
    r.impacts.iter().map(|(i, _)| by_id[i].clone()).collect()
}

/// X is reached under env://A (bridge) and unscoped (plain CALLS), and its
/// env://B edge is only followed unscoped. Whichever arrives first, Z is
/// reached and both traversals agree.
fn order_graphs() -> (Graph, Graph) {
    let names = ["S", "M", "X", "Z"];
    let common = [
        ("CONFIG_SOURCE", "X", "env://A"),
        ("CONFIG_SOURCE", "X", "env://B"),
        ("CONFIG_READ", "Z", "env://B"),
    ];
    // Bridge reaches X at level 1; the plain CALLS reaches it at level 2.
    let mut bridge_first = vec![
        ("CONFIG_READ", "S", "env://A"),
        ("CALLS", "S", "M"),
        ("CALLS", "M", "X"),
    ];
    bridge_first.extend(common);
    // Plain CALLS reaches X at level 1; the bridge only at level 2.
    let mut plain_first = vec![
        ("CALLS", "S", "X"),
        ("CALLS", "S", "M"),
        ("CONFIG_READ", "M", "env://A"),
    ];
    plain_first.extend(common);
    (graph(&names, &bridge_first), graph(&names, &plain_first))
}

#[test]
fn either_arrival_order_reaches_the_same_nodes() {
    let (bridge_first, plain_first) = order_graphs();
    let expected: BTreeSet<String> = ["M", "X", "Z"].iter().map(|s| s.to_string()).collect();

    assert_eq!(hop_names(&trace(&bridge_first, "S")), expected);
    assert_eq!(hop_names(&trace(&plain_first, "S")), expected);

    let a = impact(&bridge_first, "S");
    let b = impact(&plain_first, "S");
    assert_eq!(impact_names(&bridge_first, &a), expected);
    assert_eq!(impact_names(&plain_first, &b), expected);
    assert!(!a.truncated && !b.truncated);
}

/// X under two URIs from two parents: the first path and minimum distance
/// are kept, and the second parent is recorded as an additional path.
#[test]
fn reentry_keeps_min_distance_and_records_both_paths() {
    let g = graph(
        &["S", "M", "X"],
        &[
            ("CONFIG_READ", "S", "env://A"),
            ("CALLS", "S", "M"),
            ("CONFIG_READ", "M", "env://B"),
            ("CONFIG_SOURCE", "X", "env://A"),
            ("CONFIG_SOURCE", "X", "env://B"),
        ],
    );
    let r = impact(&g, "S");
    let (s, m, x) = (g.ids["S"], g.ids["M"], g.ids["X"]);
    assert_eq!(r.parent_map[&x].0, s, "first path is kept");
    let alts: Vec<i64> = r.alt_parents[&x].iter().map(|l| l.0).collect();
    assert_eq!(alts, vec![m], "second path recorded");
    let distance = r.evidence[&x]
        .iter()
        .find_map(|e| match e {
            lidx::impact::types::ImpactSource::DirectEdge { distance, .. } => Some(*distance),
            _ => None,
        })
        .unwrap();
    assert_eq!(distance, 1, "re-entry must not raise the distance");

    // trace_flow reports a hop per scope.
    let t = trace(&g, "S");
    assert_eq!(t.hops.iter().filter(|h| h.symbol.name == "X").count(), 2);
}

fn fan_in_graph(uris: usize, reversed: bool) -> Graph {
    let names = ["S", "X"];
    let mut uri_list: Vec<String> = (0..uris).map(|i| format!("env://U{i}")).collect();
    if reversed {
        uri_list.reverse();
    }
    let mut edges: Vec<(&str, &str, &str)> = Vec::new();
    for u in &uri_list {
        edges.push(("CONFIG_READ", "S", u));
        edges.push(("CONFIG_SOURCE", "X", u));
    }
    graph(&names, &edges)
}

/// Hitting the per-node URI cap is reported, and the URIs that survive are
/// the sorted-first ones whatever order the edges were written in.
#[test]
fn reentry_cap_is_reported_as_truncation_and_sorted() {
    let expected: BTreeSet<String> = (0..8).map(|i| format!("env://U{i}")).collect();
    for reversed in [false, true] {
        let g = fan_in_graph(10, reversed);
        let t = trace(&g, "S");
        assert!(t.truncated, "cap must set truncated");
        assert!(
            t.truncation_reason
                .as_deref()
                .is_some_and(|r| r.contains("cap")),
            "{:?}",
            t.truncation_reason
        );
        let survivors: BTreeSet<String> = t
            .hops
            .iter()
            .filter(|h| h.symbol.name == "X")
            .map(|h| {
                h.protocol_context.as_ref().unwrap()["config_uri"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(survivors, expected, "reversed={reversed}");

        let r = impact(&g, "S");
        assert!(r.truncated);
        assert!(r.truncation_reason.is_some());
    }

    // Under the cap: no truncation.
    let g = fan_in_graph(3, false);
    let t = trace(&g, "S");
    assert!(!t.truncated && t.truncation_reason.is_none());
    let r = impact(&g, "S");
    assert!(!r.truncated && r.truncation_reason.is_none());
}
