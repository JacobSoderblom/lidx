//! Test Impact Layer (Layer 2)
//!
//! Discovers the tests that reach the changed code through the symbol graph.
//!
//! ## Discovery
//!
//! A symbol is reported only when it reaches a seed through graph edges: an
//! upstream traversal (the same BFS the direct layer runs) collects every
//! test symbol within `max_depth`, and each carries the parent chain that
//! reaches the seed. Direct callers/importers are labelled `call`/`import`;
//! callers of the interface method the seed implements are
//! `call_via_interface` (one dispatch hop further out); any other path is
//! `graph`.
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
//! let result = layer.analyze(&[seed_id], graph_version)?;
//! ```

use crate::db::Db;
use crate::impact::layers::direct::{TraversalDirection, analyze_direct_impact};
use crate::impact::types::{ImpactSource, LayerResult};
use crate::indexer::test_detection::{classify_test_type, is_test_symbol};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Traversal depth used when the caller does not set one.
const DEFAULT_MAX_DEPTH: usize = 3;
/// Visited-symbol cap for the upstream traversal.
const TRAVERSAL_LIMIT: usize = 2000;

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
    /// Returns tests that are likely affected by changes to the seed symbols
    pub fn analyze(
        &self,
        seed_ids: &[i64],
        exclude_resolution_kinds: &[String],
        graph_version: i64,
    ) -> Result<LayerResult> {
        let start = Instant::now();

        // Single notion of "reaches": the direct layer's upstream traversal.
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

        // Labels: how the test reaches the seed.
        let labels = self.caller_labels(seed_ids, exclude_resolution_kinds, graph_version)?;

        let ids: Vec<i64> = traversal.impacts.iter().map(|(id, _)| *id).collect();
        let symbols = self.db.symbols_by_ids(&ids, None, graph_version)?;

        let mut impacts: Vec<(i64, f32)> = Vec::new();
        let mut evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();
        let mut reached: Vec<i64> = Vec::new();
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
            let strategy = labels.get(&sym.id).copied().unwrap_or("graph");
            let source = ImpactSource::TestLink {
                strategy: strategy.to_string(),
                test_type: classify_test_type(sym).to_string(),
                distance,
            };
            impacts.push((
                sym.id,
                crate::impact::confidence::confidence_from_source(&source),
            ));
            evidence.insert(sym.id, vec![source]);
            reached.push(sym.id);
        }

        // Keep only the parent chains of the tests reported, so fusion can
        // rebuild each test's path to the seed.
        let mut parent_map = HashMap::new();
        for id in reached {
            let mut current = id;
            while let Some(link) = traversal.parent_map.get(&current) {
                if parent_map.insert(current, link.clone()).is_some() {
                    break;
                }
                current = link.0;
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(LayerResult {
            layer_name: "test".to_string(),
            impacts,
            evidence,
            duration_ms,
            truncated: traversal.truncated,
            truncation_reason: traversal.truncation_reason,
            parent_map,
            alt_parents: HashMap::new(),
            traversed_heuristic_kind: false,
        })
    }

    /// How each direct caller/importer (or interface-method caller) reaches the seeds: `import`,
    /// `call`, or `call_via_interface` (a call through the interface method
    /// the seed implements). Labels only -- reachability is decided by the
    /// traversal in `analyze`.
    fn caller_labels(
        &self,
        seed_ids: &[i64],
        exclude_resolution_kinds: &[String],
        graph_version: i64,
    ) -> Result<HashMap<i64, &'static str>> {
        let mut labels: HashMap<i64, &'static str> = HashMap::new();

        for seed_id in seed_ids {
            let via_interface_edges =
                self.db
                    .interface_caller_edges(*seed_id, None, graph_version)?;
            let edges = self.db.edges_for_symbol(*seed_id, None, graph_version)?;

            for (edge, via_interface) in edges
                .into_iter()
                .map(|e| (e, false))
                .chain(via_interface_edges.into_iter().map(|e| (e, true)))
            {
                // Same filter discipline as the traversal (issue #81).
                if crate::model::is_resolution_excluded(
                    edge.resolution_kind.as_deref(),
                    exclude_resolution_kinds,
                ) {
                    continue;
                }
                let label = match edge.kind.as_str() {
                    "IMPORTS" if edge.target_symbol_id == Some(*seed_id) => "import",
                    "CALLS" if via_interface => "call_via_interface",
                    "CALLS" if edge.target_symbol_id == Some(*seed_id) => "call",
                    _ => continue,
                };
                if let Some(source_id) = edge.source_symbol_id {
                    // A direct call outranks a dispatch-only one.
                    let slot = labels.entry(source_id).or_insert(label);
                    if *slot == "call_via_interface" && label == "call" {
                        *slot = label;
                    }
                }
            }
        }

        Ok(labels)
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
}
