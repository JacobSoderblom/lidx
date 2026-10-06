//! Issue #201: crossing a Bridge Edge respects the direction of the walk, in
//! both `trace_flow` and `analyze_impact` (one shared predicate).

use lidx::db::Db;
use lidx::impact::layers::direct::{TraversalDirection, analyze_direct_impact};
use lidx::impact::orchestrator::reconstruct_path_steps;
use lidx::indexer::channel::WalkDirection;
use lidx::indexer::extract::{EdgeInput, ReceiverType, SymbolInput};
use lidx::model::Symbol;
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
        py_site: None,
    }
}

/// Chain `svc.entry -CALLS-> svc.caller =bridge=> svc.callee -CALLS-> svc.leaf`.
struct Fixture {
    db: Db,
    ids: HashMap<String, i64>,
    symbols: HashMap<i64, Symbol>,
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
    let symbols = inserted.into_iter().map(|s| (s.id, s)).collect();
    Fixture {
        db,
        ids,
        symbols,
        _temp: temp,
    }
}

impl Fixture {
    fn trace_hops(&self, start: &str, up: bool) -> Vec<lidx::model::TraceHop> {
        let config = TraceConfig {
            direction: if up {
                TraceDirection::Upstream
            } else {
                TraceDirection::Downstream
            },
            ..Default::default()
        };
        trace_flow(&self.db, vec![self.ids[start]], None, None, 1, &config)
            .unwrap()
            .hops
    }

    fn trace(&self, start: &str, up: bool) -> HashSet<String> {
        self.trace_hops(start, up)
            .into_iter()
            .map(|h| h.symbol.qualname)
            .collect()
    }

    fn impact_layer(
        &self,
        start: &str,
        direction: TraversalDirection,
    ) -> lidx::impact::types::LayerResult {
        analyze_direct_impact(
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
        .unwrap()
    }

    /// The (from_symbol, to_symbol) labels of the step that reaches `target`.
    fn labels(&self, start: &str, direction: TraversalDirection, target: &str) -> (String, String) {
        let layer = self.impact_layer(start, direction);
        let seeds = HashSet::from([self.ids[start]]);
        let steps =
            reconstruct_path_steps(self.ids[target], &seeds, &layer.parent_map, &self.symbols);
        let step = steps.last().unwrap();
        (step.from_symbol.clone(), step.to_symbol.clone())
    }

    fn impact(&self, start: &str, direction: TraversalDirection) -> HashSet<String> {
        let by_id: HashMap<i64, &String> = self.ids.iter().map(|(q, i)| (*i, q)).collect();
        self.impact_layer(start, direction)
            .impacts
            .iter()
            .map(|(id, _)| by_id[id].clone())
            .collect()
    }
}

fn set(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

const PAIRS: [(&str, &str, &str); 5] = [
    ("CHANNEL_SUBSCRIBE", "CHANNEL_PUBLISH", "orders"),
    ("RPC_IMPL", "RPC_CALL", "pkg.Svc.Do"),
    ("HTTP_ROUTE", "HTTP_CALL", "GET /orders"),
    // The .proto side: downstream from the rpc definition reaches the impl.
    ("RPC_IMPL", "RPC_ROUTE", "pkg.Svc.Do"),
    ("CONFIG_READ", "CONFIG_SOURCE", "env://ORDERS"),
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
            f.trace_hops(start, up)
                .into_iter()
                .find(|h| h.symbol.qualname == target)
                .unwrap()
                .bridge_direction
        };
        assert_eq!(
            dir_of("svc.callee", true, "svc.caller"),
            Some(WalkDirection::Upstream)
        );
        assert_eq!(
            dir_of("svc.caller", false, "svc.callee"),
            Some(WalkDirection::Downstream)
        );
        // A direct edge is not a bridged hop.
        assert_eq!(dir_of("svc.callee", false, "svc.leaf"), None);
    }
}

/// `from_symbol` is always the caller/publisher side, whichever end the walk
/// started from.
#[test]
fn analyze_impact_labels_bridged_step_caller_to_callee() {
    use TraversalDirection::{Downstream, Upstream};
    for (callee_kind, caller_kind, key) in PAIRS {
        let f = fixture(callee_kind, caller_kind, key);
        let want = ("svc.caller".to_string(), "svc.callee".to_string());
        assert_eq!(
            f.labels("svc.callee", Upstream, "svc.caller"),
            want,
            "{callee_kind}: upstream"
        );
        assert_eq!(
            f.labels("svc.caller", Downstream, "svc.callee"),
            want,
            "{callee_kind}: downstream"
        );
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

/// The `.proto` route sits in the middle: `client =RPC_CALL=> route =RPC_IMPL=> impl`.
fn route_middle_fixture() -> Fixture {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::new(&temp.path().join("t.db")).unwrap();
    let file_id = db.upsert_file("svc.py", "h", "python", 100, 0).unwrap();
    let names = ["svc.client", "svc.route", "svc.impl"];
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
    let key = "pkg.Svc.Do";
    let edges = [
        edge("RPC_CALL", "svc.client", key),
        edge("RPC_ROUTE", "svc.route", key),
        edge("RPC_IMPL", "svc.impl", key),
    ];
    db.insert_edges(file_id, &edges, &ids, 1, None).unwrap();
    let symbols = inserted.into_iter().map(|s| (s.id, s)).collect();
    Fixture {
        db,
        ids,
        symbols,
        _temp: temp,
    }
}

#[test]
fn rpc_route_in_the_middle_reaches_callers_upstream_and_impl_downstream() {
    use TraversalDirection::{Downstream, Upstream};
    let f = route_middle_fixture();
    assert_eq!(f.trace("svc.route", true), set(&["svc.client"]));
    assert_eq!(f.trace("svc.route", false), set(&["svc.impl"]));
    assert_eq!(f.impact("svc.route", Upstream), set(&["svc.client"]));
    assert_eq!(f.impact("svc.route", Downstream), set(&["svc.impl"]));
    // Through the middle, in both directions.
    assert_eq!(f.trace("svc.client", false), set(&["svc.impl"]));
    assert_eq!(f.trace("svc.impl", true), set(&["svc.client", "svc.route"]));
}
