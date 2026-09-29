//! Test Impact Layer (Layer 2)
//!
//! Discovers test relationships and prioritizes tests for changed code.
//!
//! ## Discovery Strategies
//!
//! 1. **Import Analysis** - Test imports production code (confidence: 0.9)
//! 2. **Call Analysis** - Test calls production functions (confidence: 0.95)
//! 3. **Naming Convention** - `test_foo()` tests `foo()` (confidence: 0.7)
//!
//! Directory proximity is deliberately not a strategy: sharing path components
//! with the seed is not evidence a test reaches it (issue #103).
//!
//! ## Usage
//!
//! ```ignore
//! let layer = TestImpactLayer::new(&db);
//! let result = layer.analyze(&[seed_id], graph_version)?;
//! ```

use crate::db::Db;
use crate::impact::types::{ImpactSource, LayerResult};
use crate::indexer::test_detection::{
    classify_test_type, extract_test_target_name, is_test_symbol,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Test Impact Layer
///
/// Finds tests that should be run when production code changes
pub struct TestImpactLayer<'a> {
    db: &'a Db,
}

impl<'a> TestImpactLayer<'a> {
    /// Create a new test impact layer
    pub fn new(db: &'a Db) -> Self {
        Self { db }
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

        // Track all discovered test symbols and their evidence
        let mut test_impacts: HashMap<i64, Vec<ImpactSource>> = HashMap::new();

        // Strategy 1: Import-based discovery
        let import_tests = self.discover_import_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in import_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Strategy 2: Call-based discovery
        let call_tests =
            self.discover_call_tests(seed_ids, exclude_resolution_kinds, graph_version)?;
        for (test_id, evidence) in call_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Strategy 3: Naming convention discovery
        let naming_tests = self.discover_naming_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in naming_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Convert to LayerResult format
        let impacts: Vec<(i64, f32)> = test_impacts
            .iter()
            .map(|(test_id, evidence)| {
                // Calculate confidence from evidence (max confidence from all strategies)
                let confidence = evidence
                    .iter()
                    .filter_map(|e| match e {
                        ImpactSource::TestLink { .. } => {
                            // Extract confidence from strategy name (embedded in strategy field)
                            Some(0.8) // Default if we can't parse
                        }
                        _ => None,
                    })
                    .fold(0.0f32, f32::max);

                (*test_id, confidence)
            })
            .collect();

        // Build evidence map
        let evidence: HashMap<i64, Vec<ImpactSource>> = test_impacts;

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(LayerResult {
            layer_name: "test".to_string(),
            impacts,
            evidence,
            duration_ms,
            truncated: false,
            parent_map: HashMap::new(),
            traversed_heuristic_kind: false,
        })
    }

    /// Strategy 1: Import-based test discovery
    ///
    /// Find test symbols that IMPORT the changed symbols
    /// Confidence: 0.9 (high - direct import relationship)
    fn discover_import_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        for seed_id in seed_ids {
            // Find edges where this symbol is the source or target
            let edges = self.db.edges_for_symbol(*seed_id, None, graph_version)?;

            for edge in edges {
                // We want IMPORT edges where the seed is the TARGET (being imported)
                if edge.kind != "IMPORTS" {
                    continue;
                }

                if edge.target_symbol_id == Some(*seed_id)
                    && let Some(source_id) = edge.source_symbol_id
                {
                    if seen.contains(&source_id) {
                        continue;
                    }
                    seen.insert(source_id);

                    // Check if source is a test symbol
                    if let Ok(symbols) = self.db.symbols_by_ids(&[source_id], None, graph_version)
                        && let Some(sym) = symbols.first()
                        && is_test_symbol(sym)
                    {
                        let test_type = classify_test_type(sym);
                        results.push((
                            source_id,
                            ImpactSource::TestLink {
                                strategy: "import".to_string(),
                                test_type: test_type.to_string(),
                            },
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Strategy 2: Call-based test discovery
    ///
    /// Find test symbols that CALL the changed symbols
    /// Confidence: 0.95 (very high - direct call relationship)
    fn discover_call_tests(
        &self,
        seed_ids: &[i64],
        exclude_resolution_kinds: &[String],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        for seed_id in seed_ids {
            // Find edges where this symbol is the source or target
            let edges = self.db.edges_for_symbol(*seed_id, None, graph_version)?;

            for edge in edges {
                // We want CALL edges where the seed is the TARGET (being called)
                if edge.kind != "CALLS" {
                    continue;
                }
                // Issue #81 (R1): same filter discipline as the direct layer
                // (impact/layers/direct.rs) -- this layer walks CALLS edges
                // too, so it must honor the same `exclude_resolution_kinds`
                // filter the orchestrator only used to pass to the direct
                // layer.
                if crate::model::is_resolution_excluded(
                    edge.resolution_kind.as_deref(),
                    exclude_resolution_kinds,
                ) {
                    continue;
                }

                if edge.target_symbol_id == Some(*seed_id)
                    && let Some(source_id) = edge.source_symbol_id
                {
                    if seen.contains(&source_id) {
                        continue;
                    }
                    seen.insert(source_id);

                    // Check if source is a test symbol
                    if let Ok(symbols) = self.db.symbols_by_ids(&[source_id], None, graph_version)
                        && let Some(sym) = symbols.first()
                        && is_test_symbol(sym)
                    {
                        let test_type = classify_test_type(sym);
                        results.push((
                            source_id,
                            ImpactSource::TestLink {
                                strategy: "call".to_string(),
                                test_type: test_type.to_string(),
                            },
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Strategy 3: Naming convention matching
    ///
    /// Match test names to production code names
    /// Examples: `test_calculate` matches `calculate`, `TestFoo` matches `Foo`
    /// Confidence: 0.7 (medium - heuristic-based)
    fn discover_naming_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        // Load seed symbols to get their names
        let seeds = self.db.symbols_by_ids(seed_ids, None, graph_version)?;

        for seed in &seeds {
            // Extract potential test names for this symbol
            let seed_name_lower = seed.name.to_lowercase();
            let possible_test_names = vec![
                format!("test_{}", seed_name_lower),
                format!("Test{}", seed.name),
                format!("{}Test", seed.name),
                format!("{}_test", seed_name_lower),
                format!("{}Spec", seed.name),
            ];

            // Search for symbols with these names using find_symbols
            for test_name in possible_test_names {
                if let Ok(candidates) = self.db.find_symbols(&test_name, 100, None, graph_version) {
                    for candidate in candidates {
                        if seen.contains(&candidate.id) {
                            continue;
                        }

                        if is_test_symbol(&candidate) {
                            seen.insert(candidate.id);
                            let test_type = classify_test_type(&candidate);
                            results.push((
                                candidate.id,
                                ImpactSource::TestLink {
                                    strategy: "naming".to_string(),
                                    test_type: test_type.to_string(),
                                },
                            ));
                        }
                    }
                }
            }

            // Also search in reverse: find tests and extract target names
            // This helps find tests like `test_calculate` when we change `calculate`
            let seed_lang = Self::infer_language(&seed.file_path);
            if let Ok(all_tests) = self.db.find_symbols("test", 1000, None, graph_version) {
                for test in all_tests {
                    if seen.contains(&test.id) {
                        continue;
                    }

                    if !is_test_symbol(&test) {
                        continue;
                    }

                    // Skip tests from different languages to avoid cross-language false positives
                    if let (Some(sl), Some(tl)) = (seed_lang, Self::infer_language(&test.file_path))
                        && sl != tl
                    {
                        continue;
                    }

                    if let Some(target_name) = extract_test_target_name(&test.name)
                        && (target_name.to_lowercase() == seed_name_lower
                            || seed
                                .name
                                .to_lowercase()
                                .contains(&target_name.to_lowercase()))
                    {
                        seen.insert(test.id);
                        let test_type = classify_test_type(&test);
                        results.push((
                            test.id,
                            ImpactSource::TestLink {
                                strategy: "naming".to_string(),
                                test_type: test_type.to_string(),
                            },
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Infer language from file extension for cross-language filtering
    fn infer_language(path: &str) -> Option<&'static str> {
        let ext = path.rsplit('.').next()?;
        match ext {
            "py" => Some("python"),
            "cs" => Some("csharp"),
            "ts" | "tsx" => Some("typescript"),
            "js" | "jsx" => Some("javascript"),
            "rs" => Some("rust"),
            "proto" => Some("proto"),
            "sql" => Some("sql"),
            "md" => Some("markdown"),
            _ => None,
        }
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
