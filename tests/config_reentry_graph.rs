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
        identity: None,
    }
}

fn edge(kind: &str, src: &str, target: &str) -> EdgeInput {
    // "Target#snippet" attaches an evidence snippet to the edge.
    let (target, snippet) = match target.split_once('#') {
        Some((t, sn)) => (t, Some(sn.to_string())),
        None => (target, None),
    };
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
        evidence_snippet: snippet,
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
        drop_if_unresolved: false,
        source_start_byte: None,
        target_start_byte: None,
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
    trace_with_budget(g, start, TraceConfig::default().max_bytes)
}

fn trace_with_budget(g: &Graph, start: &str, max_bytes: usize) -> lidx::traversal::TraceResult {
    let config = TraceConfig {
        direction: TraceDirection::Downstream,
        max_hops: 6,
        max_bytes,
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

fn hops_json(r: &lidx::traversal::TraceResult) -> Vec<String> {
    r.hops
        .iter()
        .map(|h| serde_json::to_string(h).unwrap())
        .collect()
}

/// X is reached under env://A (bridge, level 1), unscoped (plain CALLS,
/// levels 1 and 2), and its env://B edge is only followed unscoped. The
/// edges are written forward and reversed (bridge/plain arrival order flips
/// at every level): hops and impact sets must come out identical.
#[test]
fn edge_order_does_not_change_hops_or_reached_nodes() {
    let names = ["S", "M", "X", "Z"];
    let edges = [
        ("CONFIG_READ", "S", "env://A"),
        ("CALLS", "S", "X"),
        ("CALLS", "S", "M"),
        ("CALLS", "M", "X"),
        ("CONFIG_SOURCE", "X", "env://A"),
        ("CONFIG_SOURCE", "X", "env://B"),
        ("CONFIG_READ", "Z", "env://B"),
    ];
    let mut reversed = edges;
    reversed.reverse();
    let fwd = graph(&names, &edges);
    let rev = graph(&names, &reversed);

    let (tf, tr) = (trace(&fwd, "S"), trace(&rev, "S"));
    assert_eq!(hops_json(&tf), hops_json(&tr));
    assert_eq!(
        hop_names(&tf),
        ["M", "X", "Z"].iter().map(|s| s.to_string()).collect()
    );
    // X is reported once per scope: unscoped, env://A (from S), env://B (from Z).
    assert_eq!(tf.hops.iter().filter(|h| h.symbol.name == "X").count(), 3);

    // Two parents at the same level reach the same (X, unscoped) pair with
    // different edge kinds (CALLS vs a channel bridge): the hop content must
    // not depend on which edge is processed first.
    let tie_edges = [
        ("CALLS", "S", "P1"),
        ("CALLS", "S", "P2"),
        ("CHANNEL_PUBLISH", "P2", "topic.t"),
        ("CHANNEL_SUBSCRIBE", "X", "topic.t"),
        ("CALLS", "P1", "X"),
    ];
    let mut tie_reversed = tie_edges;
    tie_reversed.reverse();
    let tie_names = ["S", "P1", "P2", "X"];
    let (t1, t2) = (
        trace(&graph(&tie_names, &tie_edges), "S"),
        trace(&graph(&tie_names, &tie_reversed), "S"),
    );
    assert_eq!(hops_json(&t1), hops_json(&t2));
    for t in [&t1, &t2] {
        let x: Vec<&str> = t
            .hops
            .iter()
            .filter(|h| h.symbol.name == "X")
            .map(|h| h.edge_kind.as_str())
            .collect();
        // Parent P1 sorts before P2, so its CALLS edge is the reported one.
        assert_eq!(x, vec!["CALLS"]);
    }

    // Byte budget and truncation are decided on the settled hops, so a
    // replaced hop (CALLS wins over the larger channel-bridge hop) cannot
    // change them: a budget that only the winning hops fit, and one that
    // cuts exactly at the last hop, behave the same in both edge orders.
    let g1 = graph(&tie_names, &tie_edges);
    let g2 = graph(&tie_names, &tie_reversed);
    let total: usize = t1
        .hops
        .iter()
        .map(|h| serde_json::to_string(h).unwrap().len())
        .sum();
    for (budget, expect_truncated) in [(total + 1, false), (total, true)] {
        let (a, b) = (
            trace_with_budget(&g1, "S", budget),
            trace_with_budget(&g2, "S", budget),
        );
        assert_eq!(hops_json(&a), hops_json(&b), "budget {budget}");
        assert_eq!(a.truncated, expect_truncated, "budget {budget}");
        assert_eq!(b.truncated, expect_truncated, "budget {budget}");
        assert_eq!(a.used_bytes, b.used_bytes, "budget {budget}");
        assert_eq!(a.hops.len(), t1.hops.len(), "budget {budget}");
    }
    // Tighter budgets cut identically too.
    for budget in [1, total / 2] {
        let (a, b) = (
            trace_with_budget(&g1, "S", budget),
            trace_with_budget(&g2, "S", budget),
        );
        assert_eq!(hops_json(&a), hops_json(&b), "budget {budget}");
        assert!(a.truncated && b.truncated);
    }

    // Same parent and kind, differing only in snippet: the smaller snippet
    // is reported whichever edge comes first.
    let snip_edges = [("CALLS", "S", "X#aaa"), ("CALLS", "S", "X#zzz")];
    let mut snip_reversed = snip_edges;
    snip_reversed.reverse();
    for edges in [snip_edges, snip_reversed] {
        let t = trace(&graph(&["S", "X"], &edges), "S");
        let snippets: Vec<Option<&str>> = t.hops.iter().map(|h| h.snippet.as_deref()).collect();
        assert_eq!(snippets, vec![Some("aaa")], "{edges:?}");
    }

    let (a, b) = (impact(&fwd, "S"), impact(&rev, "S"));
    assert_eq!(impact_names(&fwd, &a), impact_names(&rev, &b));
    assert!(impact_names(&fwd, &a).contains("Z"));
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

/// X has 10 config URIs. S bridges in on U9..U2 first (level 1); the lower
/// U0 and U1 only arrive later, via M (level 2). The cap keeps the 8 lowest
/// URIs whatever the arrival order, so U8 and U9 are refused and U0/U1 kept.
fn fan_in_graph(reversed: bool) -> Graph {
    let mut edges: Vec<(&str, &str, String)> = Vec::new();
    for i in 0..10 {
        edges.push(("CONFIG_SOURCE", "X", format!("env://U{i}")));
    }
    for i in 2..10 {
        edges.push(("CONFIG_READ", "S", format!("env://U{i}")));
    }
    for i in 0..2 {
        edges.push(("CONFIG_READ", "M", format!("env://U{i}")));
    }
    edges.push(("CALLS", "S", "M".to_string()));
    if reversed {
        edges.reverse();
    }
    let refs: Vec<(&str, &str, &str)> =
        edges.iter().map(|(k, s, t)| (*k, *s, t.as_str())).collect();
    graph(&["S", "M", "X"], &refs)
}

/// Hitting the per-node URI cap is reported, and which URIs survive does not
/// depend on arrival order.
#[test]
fn reentry_cap_is_reported_and_independent_of_arrival_order() {
    let expected: BTreeSet<String> = (0..8).map(|i| format!("env://U{i}")).collect();
    for reversed in [false, true] {
        let g = fan_in_graph(reversed);
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
            .filter(|h| h.symbol.name == "X" && h.protocol_context.is_some())
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
    let g = graph(
        &["S", "X"],
        &[
            ("CONFIG_READ", "S", "env://U0"),
            ("CONFIG_SOURCE", "X", "env://U0"),
        ],
    );
    let t = trace(&g, "S");
    assert!(!t.truncated && t.truncation_reason.is_none());
    let r = impact(&g, "S");
    assert!(!r.truncated && r.truncation_reason.is_none());
}
