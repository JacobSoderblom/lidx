//! Issue #201: crossing a Bridge Edge respects the direction of the walk, in
//! both `trace_flow` and `analyze_impact` (one shared predicate).

use lidx::db::Db;
use lidx::impact::layers::direct::{TraversalDirection, analyze_direct_impact};
use lidx::indexer::extract::{EdgeInput, ReceiverType, SymbolInput};
use lidx::traversal::{TraceConfig, TraceDirection, trace_flow};
use std::collections::{HashMap, HashSet};

fn symbol(qualname: &str, line: i64) -> SymbolInput {
    SymbolInput {
        kind: "function".to_string(),
        name: qualname.rsplit('.').next().unwrap().to_string(),
        qualname: qualname.to_string(),
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

fn edge(kind: &str, source: &str, target: &str) -> EdgeInput {
    EdgeInput {
        kind: kind.to_string(),
        source_qualname: Some(source.to_string()),
        target_qualname: Some(target.to_string()),
        detail: None,
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
        source_start_byte: None,
        target_start_byte: None,
    }
}

/// Chain `svc.entry -CALLS-> svc.caller =bridge=> svc.callee -CALLS-> svc.leaf`.
struct Fixture {
    db: Db,
    ids: HashMap<String, i64>,
    _temp: tempfile::TempDir,
}

fn fixture(callee_kind: &str, caller_kind: &str, key: &str) -> Fixture {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::new(&temp.path().join("t.db")).unwrap();
    let file_id = db.upsert_file("svc.py", "h", "python", 100, 0).unwrap();
    let names = ["svc.entry", "svc.caller", "svc.callee", "svc.leaf"];
    let symbols: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, n)| symbol(n, 1 + 10 * i as i64))
        .collect();
    let inserted = db
        .insert_symbols(file_id, "svc.py", &symbols, 1, None)
        .unwrap();
    let ids: HashMap<String, i64> = inserted
        .iter()
        .map(|s| (s.qualname.clone(), s.id))
        .collect();
    let edges = [
        edge("CALLS", "svc.entry", "svc.caller"),
        edge(caller_kind, "svc.caller", key),
        edge(callee_kind, "svc.callee", key),
        edge("CALLS", "svc.callee", "svc.leaf"),
    ];
    db.insert_edges(file_id, &edges, &ids, 1, None).unwrap();
    Fixture {
        db,
        ids,
        _temp: temp,
    }
}

impl Fixture {
    fn trace(&self, start: &str, up: bool) -> HashSet<String> {
        let config = TraceConfig {
            direction: if up {
                TraceDirection::Upstream
            } else {
                TraceDirection::Downstream
            },
            ..Default::default()
        };
        let hops = trace_flow(&self.db, vec![self.ids[start]], None, None, 1, &config)
            .unwrap()
            .hops;
        hops.into_iter().map(|h| h.symbol.qualname).collect()
    }

    fn impact(&self, start: &str, direction: TraversalDirection) -> HashSet<String> {
        let result = analyze_direct_impact(
            &self.db,
            &[self.ids[start]],
            5,
            direction,
            &HashSet::new(),
            &[],
            true,
            100,
            None,
            1,
        )
        .unwrap();
        let by_id: HashMap<i64, &String> = self.ids.iter().map(|(q, i)| (*i, q)).collect();
        result
            .impacts
            .iter()
            .map(|(id, _)| by_id[id].clone())
            .collect()
    }
}

fn set(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

const PAIRS: [(&str, &str, &str); 3] = [
    ("CHANNEL_SUBSCRIBE", "CHANNEL_PUBLISH", "orders"),
    ("RPC_IMPL", "RPC_CALL", "pkg.Svc.Do"),
    ("HTTP_ROUTE", "HTTP_CALL", "GET /orders"),
];

#[test]
fn trace_flow_crosses_bridge_only_with_the_flow() {
    for (callee_kind, caller_kind, key) in PAIRS {
        let f = fixture(callee_kind, caller_kind, key);
        let label = format!("{callee_kind}/{caller_kind}");
        // Downstream from the callee side: its own body, never its callers.
        assert_eq!(
            f.trace("svc.callee", false),
            set(&["svc.leaf"]),
            "{label}: callee downstream"
        );
        // Upstream from the callee side: across the bridge to its callers.
        assert_eq!(
            f.trace("svc.callee", true),
            set(&["svc.caller", "svc.entry"]),
            "{label}: callee upstream"
        );
        // Mirror: the caller side goes downstream over the bridge, not up.
        assert_eq!(
            f.trace("svc.caller", false),
            set(&["svc.callee", "svc.leaf"]),
            "{label}: caller downstream"
        );
        assert_eq!(
            f.trace("svc.caller", true),
            set(&["svc.entry"]),
            "{label}: caller upstream"
        );
        // Multi-hop: a chain crossing the bridge mid-walk reaches the far side.
        assert_eq!(
            f.trace("svc.entry", false),
            set(&["svc.caller", "svc.callee", "svc.leaf"]),
            "{label}: entry downstream"
        );
        assert_eq!(
            f.trace("svc.leaf", true),
            set(&["svc.callee", "svc.caller", "svc.entry"]),
            "{label}: leaf upstream"
        );
    }
}

#[test]
fn trace_flow_bridged_hop_records_direction_crossed() {
    for (callee_kind, caller_kind, key) in PAIRS {
        let f = fixture(callee_kind, caller_kind, key);
        let dir_of = |start: &str, up: bool, target: &str| {
            let config = TraceConfig {
                direction: if up {
                    TraceDirection::Upstream
                } else {
                    TraceDirection::Downstream
                },
                ..Default::default()
            };
            trace_flow(&f.db, vec![f.ids[start]], None, None, 1, &config)
                .unwrap()
                .hops
                .into_iter()
                .find(|h| h.symbol.qualname == target)
                .unwrap()
                .bridge_direction
        };
        assert_eq!(dir_of("svc.callee", true, "svc.caller"), Some("upstream"));
        assert_eq!(
            dir_of("svc.caller", false, "svc.callee"),
            Some("downstream")
        );
        // A direct edge is not a bridged hop.
        assert_eq!(dir_of("svc.callee", false, "svc.leaf"), None);
    }
}

#[test]
fn analyze_impact_crosses_bridge_only_with_the_flow() {
    use TraversalDirection::{Both, Downstream, Upstream};
    for (callee_kind, caller_kind, key) in PAIRS {
        let f = fixture(callee_kind, caller_kind, key);
        let label = format!("{callee_kind}/{caller_kind}");
        assert_eq!(
            f.impact("svc.callee", Downstream),
            set(&["svc.leaf"]),
            "{label}: callee downstream"
        );
        assert_eq!(
            f.impact("svc.callee", Upstream),
            set(&["svc.caller", "svc.entry"]),
            "{label}: callee upstream"
        );
        assert_eq!(
            f.impact("svc.caller", Downstream),
            set(&["svc.callee", "svc.leaf"]),
            "{label}: caller downstream"
        );
        assert_eq!(
            f.impact("svc.caller", Upstream),
            set(&["svc.entry"]),
            "{label}: caller upstream"
        );
        assert_eq!(
            f.impact("svc.entry", Downstream),
            set(&["svc.caller", "svc.callee", "svc.leaf"]),
            "{label}: entry downstream"
        );
        assert_eq!(
            f.impact("svc.leaf", Upstream),
            set(&["svc.callee", "svc.caller", "svc.entry"]),
            "{label}: leaf upstream"
        );
        // A walk in both directions still crosses the bridge either way.
        assert_eq!(
            f.impact("svc.callee", Both),
            set(&["svc.caller", "svc.entry", "svc.leaf"]),
            "{label}: callee both"
        );
    }
}
