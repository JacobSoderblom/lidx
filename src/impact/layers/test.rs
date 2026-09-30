//! Test Impact Layer (Layer 2)
//!
//! Discovers the tests that reach the changed code through the symbol graph.
//!
//! ## Discovery
//!
//! A symbol is reported only when it reaches a seed through graph edges: an
//! upstream traversal (the same BFS the direct layer runs) collects every
//! test symbol within `max_depth`, and each carries the parent chain that
//! reaches the seed. The strategy is read off that chain: `import`/`call`
//! for a test hanging directly off the seed, `call_via_interface` when the
//! chain crosses an interface-dispatch hop, `graph` for anything else.
//!
//! Name matching (`test_foo` for `foo`) and directory proximity are
//! deliberately not strategies: neither is evidence a test reaches the seed
//! (issues #103, #231). An empty result is a truthful signal about
//! resolution coverage, not a claim that the code is untested.
//!
//! ## Usage
//!
//! ```ignore
//! let layer = TestImpactLayer::new(&db);
//! let result = layer.analyze(&[seed_id], &[], graph_version)?;
//! ```

use crate::db::Db;
use crate::impact::confidence::confidence_from_source;
use crate::impact::layers::direct::{TraversalDirection, analyze_direct_impact};
use crate::impact::types::{ImpactSource, LayerResult, ParentLink, TestStrategy};
use crate::indexer::test_detection::{classify_test_type, is_test_symbol};
use crate::model::INTERFACE_DISPATCH_KIND;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Traversal depth used when the caller does not set one.
const DEFAULT_MAX_DEPTH: usize = 3;
/// Visited-symbol cap for the upstream traversal.
pub const TRAVERSAL_LIMIT: usize = 2000;

/// Test Impact Layer
///
/// Finds tests that should be run when production code changes
pub struct TestImpactLayer<'a> {
    db: &'a Db,
    max_depth: usize,
}

impl<'a> TestImpactLayer<'a> {
    /// Create a new test impact layer
    pub fn new(db: &'a Db) -> Self {
        Self {
            db,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// Set how many hops upstream of a seed a test may sit.
    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Analyze test impact for changed symbols
    ///
    /// Runs its own upstream traversal, then keeps the tests it reached.
    pub fn analyze(
        &self,
        seed_ids: &[i64],
        exclude_resolution_kinds: &[String],
        graph_version: i64,
    ) -> Result<LayerResult> {
        let start = Instant::now();
        // Test files are included -- they are the point here -- and every
        // edge kind is followed, bridges (RPC/HTTP/channel) included.
        let traversal = analyze_direct_impact(
            self.db,
            seed_ids,
            self.max_depth,
            TraversalDirection::Upstream,
            &HashSet::new(),
            exclude_resolution_kinds,
            true,
            TRAVERSAL_LIMIT,
            None,
            graph_version,
        )?;
        let mut result = self.analyze_traversal(&traversal, graph_version)?;
        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Keep the tests a finished upstream traversal reached. The traversal
    /// must be equivalent to the one `analyze` runs (upstream, all edge
    /// kinds, test files included, this layer's depth); the orchestrator
    /// hands over the direct layer's own result when that holds.
    pub fn analyze_traversal(
        &self,
        traversal: &LayerResult,
        graph_version: i64,
    ) -> Result<LayerResult> {
        let start = Instant::now();

        let ids: Vec<i64> = traversal.impacts.iter().map(|(id, _)| *id).collect();
        let symbols = self.db.symbols_by_ids(&ids, None, graph_version)?;

        let mut impacts: Vec<(i64, f32)> = Vec::new();
        let mut evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();
        let mut parent_map: HashMap<i64, ParentLink> = HashMap::new();
        for sym in symbols.iter().filter(|s| is_test_symbol(s)) {
            let distance = traversal
                .evidence
                .get(&sym.id)
                .and_then(|e| {
                    e.iter().find_map(|e| match e {
                        ImpactSource::DirectEdge { distance, .. } => Some(*distance),
                        _ => None,
                    })
                })
                .unwrap_or(1);
            let source = ImpactSource::TestLink {
                strategy: strategy_for(sym.id, distance, &traversal.parent_map),
                test_type: classify_test_type(sym).to_string(),
                distance,
            };
            impacts.push((sym.id, confidence_from_source(&source)));
            evidence.insert(sym.id, vec![source]);

            // Keep only the parent chains of the tests reported, so fusion
            // can rebuild each test's path to the seed.
            let mut current = sym.id;
            while let Some(link) = traversal.parent_map.get(&current) {
                if parent_map.insert(current, link.clone()).is_some() {
                    break;
                }
                current = link.0;
            }
        }

        let alt_parents = traversal
            .alt_parents
            .iter()
            .filter(|(id, _)| parent_map.contains_key(id))
            .map(|(id, links)| (*id, links.clone()))
            .collect();

        Ok(LayerResult {
            layer_name: "test".to_string(),
            impacts,
            evidence,
            duration_ms: start.elapsed().as_millis() as u64,
            truncated: traversal.truncated,
            truncation_reason: traversal.truncation_reason.clone(),
            parent_map,
            alt_parents,
            traversed_heuristic_kind: traversal.traversed_heuristic_kind,
        })
    }
}

/// Strategy of the chain from `test_id` back to the seed: a chain crossing
/// an interface-dispatch hop is `CallViaInterface`; otherwise a test hanging
/// directly off the seed takes its label from that edge's kind, and anything
/// further out is `Graph`.
fn strategy_for(
    test_id: i64,
    distance: usize,
    parent_map: &HashMap<i64, ParentLink>,
) -> TestStrategy {
    let mut current = test_id;
    let mut seen = HashSet::new();
    while let Some((parent, _, resolution_kind, _)) = parent_map.get(&current) {
        if resolution_kind.as_deref() == Some(INTERFACE_DISPATCH_KIND) {
            return TestStrategy::CallViaInterface;
        }
        if !seen.insert(current) {
            break;
        }
        current = *parent;
    }
    match parent_map.get(&test_id) {
        Some((_, edge_kind, _, _)) if distance == 1 => TestStrategy::from_edge_kind(edge_kind),
        _ => TestStrategy::Graph,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn sym(qualname: &str, kind: &str, line: i64) -> crate::indexer::extract::SymbolInput {
        crate::indexer::extract::SymbolInput {
            kind: kind.to_string(),
            name: qualname.rsplit('.').next().unwrap().to_string(),
            qualname: qualname.to_string(),
            start_line: line,
            start_col: 0,
            end_line: line + 3,
            end_col: 0,
            start_byte: 0,
            end_byte: 10,
            signature: None,
            docstring: None,
        }
    }

    /// Issue #103: a test that merely shares path components with the seed
    /// (same package layout) but never reaches it through the graph must
    /// not be listed; a test that calls the seed must be.
    #[test]
    fn only_tests_reaching_the_seed_through_the_graph_are_listed() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = Db::new(&temp.path().join("t.db")).unwrap();
        let src = db
            .upsert_file("pkg/core/mod.py", "h1", "python", 10, 0)
            .unwrap();
        let seed_syms = db
            .insert_symbols(
                src,
                "pkg/core/mod.py",
                &[sym("pkg.core.mod.run", "function", 1)],
                1,
                None,
            )
            .unwrap();
        let tst = db
            .upsert_file("tests/pkg/core/test_mod.py", "h2", "python", 10, 0)
            .unwrap();
        let test_syms = db
            .insert_symbols(
                tst,
                "tests/pkg/core/test_mod.py",
                &[
                    sym("tests.test_mod.test_run_calls", "function", 1),
                    sym("tests.test_mod.test_unrelated", "function", 10),
                ],
                1,
                None,
            )
            .unwrap();
        let map: HashMap<String, i64> = test_syms
            .iter()
            .chain(seed_syms.iter())
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edge = crate::indexer::extract::EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some("tests.test_mod.test_run_calls".to_string()),
            target_qualname: Some("pkg.core.mod.run".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
        };
        db.insert_edges(tst, &[edge], &map, 1, None).unwrap();

        let layer = TestImpactLayer::new(&db);
        let result = layer.analyze(&[seed_syms[0].id], &[], 1).unwrap();
        let ids: Vec<i64> = result.impacts.iter().map(|(id, _)| *id).collect();
        let called = test_syms
            .iter()
            .find(|s| s.name == "test_run_calls")
            .unwrap()
            .id;
        assert_eq!(ids, vec![called], "got {ids:?}");
    }

    /// Every reported test carries a non-empty path whose last step reaches
    /// the seed, for an import-linked test as well as a call-linked one.
    #[test]
    fn import_and_call_tests_carry_paths_ending_at_the_seed() {
        use crate::impact::config::MultiLayerConfig;
        use crate::impact::types::{ImpactSource, TestStrategy};

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = Db::new(&temp.path().join("t.db")).unwrap();
        let src = db
            .upsert_file("pkg/core/mod.py", "h1", "python", 10, 0)
            .unwrap();
        let seed_syms = db
            .insert_symbols(
                src,
                "pkg/core/mod.py",
                &[sym("pkg.core.mod.run", "function", 1)],
                1,
                None,
            )
            .unwrap();
        let tst = db
            .upsert_file("tests/test_mod.py", "h2", "python", 10, 0)
            .unwrap();
        let test_syms = db
            .insert_symbols(
                tst,
                "tests/test_mod.py",
                &[
                    sym("tests.test_mod.test_imports_run", "function", 1),
                    sym("tests.test_mod.test_calls_run", "function", 10),
                ],
                1,
                None,
            )
            .unwrap();
        let map: HashMap<String, i64> = test_syms
            .iter()
            .chain(seed_syms.iter())
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edge = |kind: &str, source: &str| crate::indexer::extract::EdgeInput {
            kind: kind.to_string(),
            source_qualname: Some(source.to_string()),
            target_qualname: Some("pkg.core.mod.run".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
        };
        db.insert_edges(
            tst,
            &[
                edge("IMPORTS", "tests.test_mod.test_imports_run"),
                edge("CALLS", "tests.test_mod.test_calls_run"),
            ],
            &map,
            1,
            None,
        )
        .unwrap();

        let config = MultiLayerConfig::builder()
            .max_depth(3)
            .direction("downstream".to_string())
            .include_paths(true)
            .build();
        let result =
            crate::impact::analyze_impact_multi_layer(&db, &[seed_syms[0].id], config, 1).unwrap();
        let tests: Vec<_> = result
            .affected
            .iter()
            .filter(|e| e.relationship == "TEST")
            .collect();
        assert_eq!(tests.len(), 2, "{:?}", result.affected);
        for entry in tests {
            let steps = &entry.path.as_ref().expect("path").steps;
            assert!(!steps.is_empty(), "{}", entry.symbol.qualname);
            assert_eq!(steps.last().unwrap().to_symbol, "pkg.core.mod.run");
            assert_eq!(steps[0].from_symbol, entry.symbol.qualname);
        }

        let layer = TestImpactLayer::new(&db);
        let result = layer.analyze(&[seed_syms[0].id], &[], 1).unwrap();
        let strategy_of = |name: &str| {
            let id = test_syms.iter().find(|s| s.name == name).unwrap().id;
            match &result.evidence[&id][0] {
                ImpactSource::TestLink { strategy, .. } => *strategy,
                other => panic!("unexpected evidence {other:?}"),
            }
        };
        assert_eq!(strategy_of("test_imports_run"), TestStrategy::Import);
        assert_eq!(strategy_of("test_calls_run"), TestStrategy::Call);
    }
}
