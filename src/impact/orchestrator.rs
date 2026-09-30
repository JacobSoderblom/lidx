//! Multi-layer impact analysis orchestrator
//!
//! This module coordinates execution of multiple impact analysis layers,
//! fuses their results, and handles graceful degradation when layers fail.

use crate::db::Db;
use crate::impact::confidence::fuse_evidence;
use crate::impact::config::MultiLayerConfig;
use crate::impact::layers::test::TRAVERSAL_LIMIT;
use crate::impact::layers::{
    HistoricalImpactLayer, TestImpactLayer, TraversalDirection, analyze_direct_impact_scoped,
};
use crate::impact::types::{
    ImpactEntry, ImpactSource, ImpactSummary, LayerMetadata, LayerResult, LayerStats, ParentLink,
    PathStep, UnifiedImpactResult,
};
use crate::model::{Symbol, SymbolCompact};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

/// Multi-layer impact analysis orchestrator
///
/// Executes enabled layers, fuses their results, and provides unified output
/// with per-layer metadata and graceful degradation.
pub struct MultiLayerOrchestrator<'a> {
    db: &'a Db,
    config: MultiLayerConfig,
}

impl<'a> MultiLayerOrchestrator<'a> {
    /// Create a new orchestrator
    pub fn new(db: &'a Db, config: MultiLayerConfig) -> Self {
        Self { db, config }
    }

    /// Analyze impact from seed symbols using configured layers (sequential execution)
    pub fn analyze(&self, seed_ids: &[i64], graph_version: i64) -> Result<UnifiedImpactResult> {
        self.analyze_sequential(seed_ids, graph_version)
    }

    /// Analyze impact from seed symbols using parallel layer execution
    ///
    /// This method runs all enabled layers in parallel using threads,
    /// providing 2-3x speedup compared to sequential execution.
    pub fn analyze_parallel(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<UnifiedImpactResult> {
        let start = Instant::now();

        // Load seed symbols
        let seeds = self.load_seeds(seed_ids, graph_version)?;
        if seeds.is_empty() {
            return Err(anyhow::anyhow!("No valid seed symbols found"));
        }

        // Shared layer metadata with thread-safe access
        let layer_metadata = Arc::new(Mutex::new(LayerMetadata {
            direct: None,
            test: None,
            historical: None,
        }));

        // Collect layer results from threads
        let layer_results = Arc::new(Mutex::new(Vec::new()));

        // Spawn threads for each enabled layer
        let mut handles = vec![];

        // Layer 1: Direct impact (BFS traversal)
        if self.config.direct.enabled {
            let db_path = self.db.db_path().to_path_buf();
            let seed_ids = seed_ids.to_vec();
            let config = self.config.clone();
            let metadata = Arc::clone(&layer_metadata);
            let results = Arc::clone(&layer_results);

            handles.push(thread::spawn(move || {
                // Create new DB connection for this thread
                let db = match Db::new(&db_path) {
                    Ok(db) => db,
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to create DB connection for direct layer: {}",
                            e
                        );
                        let mut meta = metadata.lock().unwrap();
                        meta.direct = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                        return;
                    }
                };

                let kinds = config.direct.kinds.iter().cloned().collect();
                let languages = config.direct.languages.as_deref();

                match analyze_direct_impact_scoped(
                    &db,
                    &seed_ids,
                    config.direct.max_depth,
                    crate::impact::TraversalDirection::from(config.direct.direction.as_str()),
                    &kinds,
                    &config.direct.exclude_resolution_kinds,
                    config.direct.include_tests,
                    config.limit,
                    languages,
                    graph_version,
                    config.direct.seed_config_uri.as_deref(),
                ) {
                    Ok(result) => {
                        let mut meta = metadata.lock().unwrap();
                        meta.direct = Some(LayerStats {
                            enabled: true,
                            duration_ms: result.duration_ms,
                            result_count: result.impacts.len(),
                            truncated: result.truncated,
                            error: None,
                        });
                        results.lock().unwrap().push(result);
                    }
                    Err(e) => {
                        eprintln!("Warning: Direct layer failed: {}", e);
                        let mut meta = metadata.lock().unwrap();
                        meta.direct = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }));
        } else {
            let mut meta = layer_metadata.lock().unwrap();
            meta.direct = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Layer 2: Test impact
        if self.config.test.enabled {
            let db_path = self.db.db_path().to_path_buf();
            let seed_ids = seed_ids.to_vec();
            let exclude_resolution_kinds = self.config.direct.exclude_resolution_kinds.clone();
            let max_depth = self.config.direct.max_depth;
            let metadata = Arc::clone(&layer_metadata);
            let results = Arc::clone(&layer_results);

            handles.push(thread::spawn(move || {
                let db = match Db::new(&db_path) {
                    Ok(db) => db,
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to create DB connection for test layer: {}",
                            e
                        );
                        let mut meta = metadata.lock().unwrap();
                        meta.test = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                        return;
                    }
                };

                let test_layer = TestImpactLayer::new(&db).with_max_depth(max_depth);
                match test_layer.analyze(&seed_ids, &exclude_resolution_kinds, graph_version) {
                    Ok(result) => {
                        let mut meta = metadata.lock().unwrap();
                        meta.test = Some(LayerStats {
                            enabled: true,
                            duration_ms: result.duration_ms,
                            result_count: result.impacts.len(),
                            truncated: result.truncated,
                            error: None,
                        });
                        results.lock().unwrap().push(result);
                    }
                    Err(e) => {
                        eprintln!("Warning: Test layer failed: {}", e);
                        let mut meta = metadata.lock().unwrap();
                        meta.test = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }));
        } else {
            let mut meta = layer_metadata.lock().unwrap();
            meta.test = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Layer 3: Historical impact
        if self.config.historical.enabled {
            let db_path = self.db.db_path().to_path_buf();
            let seed_ids = seed_ids.to_vec();
            let config = self.config.clone();
            let metadata = Arc::clone(&layer_metadata);
            let results = Arc::clone(&layer_results);

            handles.push(thread::spawn(move || {
                let db = match Db::new(&db_path) {
                    Ok(db) => db,
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to create DB connection for historical layer: {}",
                            e
                        );
                        let mut meta = metadata.lock().unwrap();
                        meta.historical = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                        return;
                    }
                };

                let historical_layer = HistoricalImpactLayer::new(&db);
                match historical_layer.analyze(
                    &seed_ids,
                    config.historical.time_window_days,
                    config.historical.min_occurrences,
                    graph_version,
                ) {
                    Ok(result) => {
                        let mut meta = metadata.lock().unwrap();
                        meta.historical = Some(LayerStats {
                            enabled: true,
                            duration_ms: result.duration_ms,
                            result_count: result.impacts.len(),
                            truncated: result.truncated,
                            error: None,
                        });
                        results.lock().unwrap().push(result);
                    }
                    Err(e) => {
                        eprintln!("Warning: Historical layer failed: {}", e);
                        let mut meta = metadata.lock().unwrap();
                        meta.historical = Some(LayerStats {
                            enabled: true,
                            duration_ms: 0,
                            result_count: 0,
                            truncated: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }));
        } else {
            let mut meta = layer_metadata.lock().unwrap();
            meta.historical = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Wait for all threads to complete
        for handle in handles {
            let _ = handle.join();
        }

        // Extract results from Arc<Mutex<>>
        let layer_results = Arc::try_unwrap(layer_results)
            .map(|mutex| mutex.into_inner().unwrap())
            .unwrap_or_else(|arc| arc.lock().unwrap().clone());

        let layer_metadata = Arc::try_unwrap(layer_metadata)
            .map(|mutex| mutex.into_inner().unwrap())
            .unwrap_or_else(|arc| arc.lock().unwrap().clone());

        // Issue #81: see `analyze_sequential`'s identical extraction for why
        // this is captured before `fuse_results` consumes `layer_results`.
        let direct_traversed_ids: Vec<i64> = layer_results
            .iter()
            .find(|r| r.layer_name == "direct")
            .map(|r| r.impacts.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default();
        // Issue #81 (R5): see `analyze_sequential`'s identical extraction.
        let traversed_heuristic_kind = layer_results
            .iter()
            .find(|r| r.layer_name == "direct")
            .is_some_and(|r| r.traversed_heuristic_kind);

        // Fuse results from all layers
        let num_layers = layer_results.len();
        let (affected, summary, truncated, truncation_reason) =
            self.fuse_results(layer_results, seed_ids, graph_version)?;

        // Apply global confidence filter
        let filtered_affected = if self.config.min_confidence > 0.0 {
            affected
                .into_iter()
                .filter(|entry| entry.confidence.unwrap_or(1.0) >= self.config.min_confidence)
                .collect()
        } else {
            affected
        };

        // Rebuild summary after filtering
        let final_summary = if self.config.min_confidence > 0.0 {
            crate::impact::build_summary_from_entries(&filtered_affected)
        } else {
            summary
        };

        let lower_bound =
            self.compute_lower_bound(seed_ids, &direct_traversed_ids, graph_version)?;

        eprintln!(
            "Multi-layer analysis (parallel) complete in {}ms: {} layers executed, {} symbols affected",
            start.elapsed().as_millis(),
            num_layers,
            filtered_affected.len()
        );

        Ok(UnifiedImpactResult {
            seeds: seeds.into_iter().map(SymbolCompact::from).collect(),
            affected: filtered_affected,
            summary: final_summary,
            truncated,
            truncation_reason,
            config: self.build_config_summary(),
            layers: layer_metadata,
            lower_bound,
            traversed_heuristic_kind,
        })
    }

    /// Analyze impact from seed symbols using sequential layer execution (original implementation)
    fn analyze_sequential(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<UnifiedImpactResult> {
        let start = Instant::now();

        // Load seed symbols
        let seeds = self.load_seeds(seed_ids, graph_version)?;
        if seeds.is_empty() {
            return Err(anyhow::anyhow!("No valid seed symbols found"));
        }

        // Execute layers
        let mut layer_results = Vec::new();
        let mut layer_metadata = LayerMetadata {
            direct: None,
            test: None,
            historical: None,
        };

        // Layer 1: Direct impact (BFS traversal)
        if self.config.direct.enabled {
            match self.run_direct_layer(seed_ids, graph_version) {
                Ok(result) => {
                    layer_metadata.direct = Some(LayerStats {
                        enabled: true,
                        duration_ms: result.duration_ms,
                        result_count: result.impacts.len(),
                        truncated: result.truncated,
                        error: None,
                    });
                    layer_results.push(result);
                }
                Err(e) => {
                    eprintln!("Warning: Direct layer failed: {}", e);
                    layer_metadata.direct = Some(LayerStats {
                        enabled: true,
                        duration_ms: 0,
                        result_count: 0,
                        truncated: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        } else {
            layer_metadata.direct = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Layer 2: Test impact
        if self.config.test.enabled {
            match self.run_test_layer(seed_ids, &layer_results, graph_version) {
                Ok(result) => {
                    layer_metadata.test = Some(LayerStats {
                        enabled: true,
                        duration_ms: result.duration_ms,
                        result_count: result.impacts.len(),
                        truncated: result.truncated,
                        error: None,
                    });
                    layer_results.push(result);
                }
                Err(e) => {
                    eprintln!("Warning: Test layer failed: {}", e);
                    layer_metadata.test = Some(LayerStats {
                        enabled: true,
                        duration_ms: 0,
                        result_count: 0,
                        truncated: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        } else {
            layer_metadata.test = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Layer 3: Historical impact
        if self.config.historical.enabled {
            match self.run_historical_layer(seed_ids, graph_version) {
                Ok(result) => {
                    layer_metadata.historical = Some(LayerStats {
                        enabled: true,
                        duration_ms: result.duration_ms,
                        result_count: result.impacts.len(),
                        truncated: result.truncated,
                        error: None,
                    });
                    layer_results.push(result);
                }
                Err(e) => {
                    eprintln!("Warning: Historical layer failed: {}", e);
                    layer_metadata.historical = Some(LayerStats {
                        enabled: true,
                        duration_ms: 0,
                        result_count: 0,
                        truncated: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        } else {
            layer_metadata.historical = Some(LayerStats {
                enabled: false,
                duration_ms: 0,
                result_count: 0,
                truncated: false,
                error: None,
            });
        }

        // Issue #81: the direct layer's own traversed set (seeds + everything
        // it visited), captured before `fuse_results` consumes `layer_results`
        // -- the lower-bound signal is specific to graph-edge resolution, so
        // only the direct layer (not test/historical) feeds it.
        let direct_traversed_ids: Vec<i64> = layer_results
            .iter()
            .find(|r| r.layer_name == "direct")
            .map(|r| r.impacts.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default();
        // Issue #81 (R5): whether the direct layer's own traversal crossed
        // at least one heuristic-kind edge -- gates the "retry excluding
        // heuristics" next_hops suggestion, unlike `lower_bound` (unresolved
        // references), which is a different signal entirely.
        let traversed_heuristic_kind = layer_results
            .iter()
            .find(|r| r.layer_name == "direct")
            .is_some_and(|r| r.traversed_heuristic_kind);

        // Fuse results from all layers
        let num_layers = layer_results.len();
        let (affected, summary, truncated, truncation_reason) =
            self.fuse_results(layer_results, seed_ids, graph_version)?;

        // Apply global confidence filter
        let filtered_affected = if self.config.min_confidence > 0.0 {
            affected
                .into_iter()
                .filter(|entry| entry.confidence.unwrap_or(1.0) >= self.config.min_confidence)
                .collect()
        } else {
            affected
        };

        // Rebuild summary after filtering
        let final_summary = if self.config.min_confidence > 0.0 {
            crate::impact::build_summary_from_entries(&filtered_affected)
        } else {
            summary
        };

        let lower_bound =
            self.compute_lower_bound(seed_ids, &direct_traversed_ids, graph_version)?;

        eprintln!(
            "Multi-layer analysis complete in {}ms: {} layers executed, {} symbols affected",
            start.elapsed().as_millis(),
            num_layers,
            filtered_affected.len()
        );

        Ok(UnifiedImpactResult {
            seeds: seeds.into_iter().map(SymbolCompact::from).collect(),
            affected: filtered_affected,
            summary: final_summary,
            truncated,
            truncation_reason,
            config: self.build_config_summary(),
            layers: layer_metadata,
            traversed_heuristic_kind,
            lower_bound,
        })
    }

    /// Issue #81's lower-bound indicator: pending `unresolved_references`
    /// rows whose source symbol is a seed or one of the direct layer's own
    /// impacted symbols. `false`/`0` when the direct layer is disabled --
    /// test/historical layers don't traverse resolvable graph edges, so
    /// they have nothing meaningful to report here.
    fn compute_lower_bound(
        &self,
        seed_ids: &[i64],
        direct_traversed_ids: &[i64],
        graph_version: i64,
    ) -> Result<crate::model::LowerBound> {
        if !self.config.direct.enabled {
            return Ok(crate::model::LowerBound {
                is_lower_bound: false,
                unresolved_count: 0,
            });
        }
        let mut ids = seed_ids.to_vec();
        ids.extend_from_slice(direct_traversed_ids);
        ids.sort_unstable();
        ids.dedup();
        let count = self
            .db
            .unresolved_reference_count_for_symbols(&ids, graph_version)?;
        Ok(crate::model::LowerBound {
            is_lower_bound: count > 0,
            unresolved_count: count,
        })
    }

    /// Run Layer 1: Direct impact (BFS traversal)
    fn run_direct_layer(&self, seed_ids: &[i64], graph_version: i64) -> Result<LayerResult> {
        let kinds = self.config.direct.kinds.iter().cloned().collect();
        let languages = self.config.direct.languages.as_deref();

        analyze_direct_impact_scoped(
            self.db,
            seed_ids,
            self.config.direct.max_depth,
            crate::impact::TraversalDirection::from(self.config.direct.direction.as_str()),
            &kinds,
            &self.config.direct.exclude_resolution_kinds,
            self.config.direct.include_tests,
            self.config.limit,
            languages,
            graph_version,
            self.config.direct.seed_config_uri.as_deref(),
        )
    }

    /// Run Layer 2: Test impact
    ///
    /// When the direct layer already ran the traversal the test layer would
    /// run (see [`Self::reusable_direct`]), its result is reused instead of
    /// a second BFS; otherwise the test layer traverses upstream itself.
    fn run_test_layer(
        &self,
        seed_ids: &[i64],
        layer_results: &[LayerResult],
        graph_version: i64,
    ) -> Result<LayerResult> {
        let test_layer = TestImpactLayer::new(self.db).with_max_depth(self.config.direct.max_depth);
        match self.reusable_direct(layer_results, seed_ids) {
            Some(direct) => test_layer.analyze_traversal(direct, graph_version),
            None => test_layer.analyze(
                seed_ids,
                &self.config.direct.exclude_resolution_kinds,
                graph_version,
            ),
        }
    }

    /// The direct layer's result, when it is exactly the traversal the test
    /// layer needs: upstream, same depth, every edge kind, test files
    /// included, no language or config-URI scoping, and finished within the
    /// test layer's own traversal cap (so the two BFS runs cannot differ).
    fn reusable_direct<'r>(
        &self,
        layer_results: &'r [LayerResult],
        seed_ids: &[i64],
    ) -> Option<&'r LayerResult> {
        let direct = &self.config.direct;
        let same_traversal = direct.enabled
            && TraversalDirection::from(direct.direction.as_str()) == TraversalDirection::Upstream
            && direct.kinds.is_empty()
            && direct.include_tests
            && direct.languages.is_none()
            && direct.seed_config_uri.is_none();
        if !same_traversal {
            return None;
        }
        layer_results
            .iter()
            .find(|r| r.layer_name == "direct")
            .filter(|r| !r.truncated && r.impacts.len() + seed_ids.len() < TRAVERSAL_LIMIT)
    }

    /// Run Layer 3: Historical impact (co-change patterns)
    fn run_historical_layer(&self, seed_ids: &[i64], graph_version: i64) -> Result<LayerResult> {
        let historical_layer = HistoricalImpactLayer::new(self.db);
        historical_layer.analyze(
            seed_ids,
            self.config.historical.time_window_days,
            self.config.historical.min_occurrences,
            graph_version,
        )
    }

    /// Fuse results from multiple layers
    fn fuse_results(
        &self,
        layer_results: Vec<LayerResult>,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<(Vec<ImpactEntry>, ImpactSummary, bool, Option<String>)> {
        // Collect all unique symbol IDs and their evidence
        let mut symbol_evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();
        let mut any_truncated = false;
        let mut truncation_reason: Option<String> = None;
        let mut merged_alts: HashMap<i64, Vec<ParentLink>> = HashMap::new();

        // Merge parent maps from all layers (direct layer is primary)
        let mut merged_parents: HashMap<i64, ParentLink> = HashMap::new();
        for layer_result in &layer_results {
            any_truncated = any_truncated || layer_result.truncated;
            if truncation_reason.is_none() {
                truncation_reason = layer_result.truncation_reason.clone();
            }
            for (child, links) in &layer_result.alt_parents {
                // Layers can share a traversal (test layer reusing or
                // repeating the direct layer's), so skip links already kept.
                let kept = merged_alts.entry(*child).or_default();
                for link in links {
                    if !kept.contains(link) {
                        kept.push(link.clone());
                    }
                }
            }

            for (child, parent_info) in &layer_result.parent_map {
                merged_parents
                    .entry(*child)
                    .or_insert_with(|| parent_info.clone());
            }

            for (symbol_id, _confidence) in &layer_result.impacts {
                if let Some(evidence) = layer_result.evidence.get(symbol_id) {
                    symbol_evidence
                        .entry(*symbol_id)
                        .or_default()
                        .extend(evidence.iter().cloned());
                }
            }
        }

        // Load all impacted symbols, plus the seeds themselves -- a path
        // step's parent can be a seed (e.g. the distance-1 step off the
        // start symbol), and `reconstruct_path_steps` needs a qualname for
        // it too, not just for non-seed impacted symbols.
        let mut symbol_ids: Vec<i64> = symbol_evidence.keys().copied().collect();
        symbol_ids.extend(seed_ids.iter().copied());
        let symbols = self.db.symbols_by_ids(&symbol_ids, None, graph_version)?;
        let symbol_map: HashMap<i64, Symbol> = symbols.into_iter().map(|s| (s.id, s)).collect();

        // Build impact entries with fused confidence
        let seed_set: std::collections::HashSet<i64> = seed_ids.iter().copied().collect();
        let mut affected = Vec::new();

        for (symbol_id, evidence) in symbol_evidence {
            if seed_set.contains(&symbol_id) {
                continue; // Skip seed symbols
            }

            if let Some(symbol) = symbol_map.get(&symbol_id) {
                // Fuse confidence from all evidence sources
                let confidence = fuse_evidence(&evidence);

                // Calculate distance from first evidence
                let has_direct = evidence
                    .iter()
                    .any(|e| matches!(e, ImpactSource::DirectEdge { .. }));
                let has_test = evidence
                    .iter()
                    .any(|e| matches!(e, ImpactSource::TestLink { .. }));

                let distance = evidence
                    .iter()
                    .filter_map(|e| match e {
                        ImpactSource::DirectEdge { distance, .. }
                        | ImpactSource::TestLink { distance, .. } => Some(*distance),
                        _ => None,
                    })
                    .min()
                    .unwrap_or(1);

                // Determine relationship type based on evidence sources
                let relationship = if has_direct && distance == 1 {
                    "DIRECT".to_string()
                } else if has_direct && distance > 1 {
                    format!("INDIRECT_{}", distance)
                } else if has_test && !has_direct {
                    "TEST".to_string()
                } else if has_direct && distance == 0 {
                    "SEED".to_string()
                } else {
                    format!("INDIRECT_{}", distance)
                };

                // Build path if requested — reconstruct from parent_map
                let path = if self.config.include_paths {
                    let steps =
                        reconstruct_path_steps(symbol_id, &seed_set, &merged_parents, &symbol_map);
                    Some(crate::impact::types::ImpactPath { steps })
                } else {
                    None
                };
                // Paths through the other parents this symbol was re-entered
                // by (e.g. a second config URI).
                let also_via = if self.config.include_paths {
                    merged_alts
                        .get(&symbol_id)
                        .into_iter()
                        .flatten()
                        .map(|link| {
                            let mut parents = merged_parents.clone();
                            parents.insert(symbol_id, link.clone());
                            crate::impact::types::ImpactPath {
                                steps: reconstruct_path_steps(
                                    symbol_id,
                                    &seed_set,
                                    &parents,
                                    &symbol_map,
                                ),
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                affected.push(ImpactEntry {
                    symbol: SymbolCompact::from(symbol),
                    distance,
                    relationship,
                    path,
                    confidence: Some(confidence),
                    also_via,
                });
            }
        }

        // Filter out module/namespace-level symbols that add noise
        affected.retain(|entry| {
            !matches!(
                entry.symbol.kind.as_str(),
                "module" | "namespace" | "package"
            )
        });

        // Sort by distance, then by qualname for determinism
        affected.sort_by(|a, b| {
            a.distance
                .cmp(&b.distance)
                .then_with(|| a.symbol.qualname.cmp(&b.symbol.qualname))
        });

        // Build summary
        let summary = crate::impact::build_summary_from_entries(&affected);

        Ok((affected, summary, any_truncated, truncation_reason))
    }

    /// Load seed symbols
    fn load_seeds(&self, seed_ids: &[i64], graph_version: i64) -> Result<Vec<Symbol>> {
        let languages = self.config.direct.languages.as_deref();
        self.db.symbols_by_ids(seed_ids, languages, graph_version)
    }

    /// Build configuration summary for result
    fn build_config_summary(&self) -> crate::impact::types::ImpactConfig {
        crate::impact::types::ImpactConfig {
            max_depth: self.config.direct.max_depth,
            direction: self.config.direct.direction.clone(),
            relationship_types: self.config.direct.kinds.clone(),
            include_tests: self.config.direct.include_tests,
            limit: self.config.limit,
        }
    }
}

/// Walk parent chain from a symbol back to a seed, returning path steps in root-to-leaf order.
fn reconstruct_path_steps(
    symbol_id: i64,
    seed_set: &HashSet<i64>,
    parent_map: &HashMap<i64, ParentLink>,
    symbol_map: &HashMap<i64, Symbol>,
) -> Vec<PathStep> {
    let mut steps = Vec::new();
    let mut current = symbol_id;
    // Walk parent chain back to seed, max 20 hops to avoid loops
    for _ in 0..20 {
        if seed_set.contains(&current) {
            break;
        }
        let Some((parent_id, edge_kind, resolution_kind, reversed)) = parent_map.get(&current)
        else {
            break;
        };
        let from_qn = symbol_map
            .get(parent_id)
            .map(|s| s.qualname.clone())
            .unwrap_or_default();
        let to_qn = symbol_map
            .get(&current)
            .map(|s| s.qualname.clone())
            .unwrap_or_default();
        // Always caller -> callee: an upstream walk traverses the edge backwards.
        let (from_symbol, to_symbol) = if *reversed {
            (to_qn, from_qn)
        } else {
            (from_qn, to_qn)
        };
        steps.push(PathStep {
            edge_kind: edge_kind.clone(),
            from_symbol,
            to_symbol,
            resolution_kind: resolution_kind.clone(),
        });
        current = *parent_id;
    }
    steps.reverse(); // Root-to-leaf order
    steps
}

#[cfg(test)]
mod tests {
    use crate::impact::config::MultiLayerConfig;

    #[test]
    fn orchestrator_direct_only_config() {
        let config = MultiLayerConfig::direct_only();
        assert!(config.direct.enabled);
        assert!(!config.test.enabled);
        assert!(!config.historical.enabled);
    }

    #[test]
    fn orchestrator_all_layers_config() {
        let config = MultiLayerConfig::all_layers();
        assert!(config.direct.enabled);
        assert!(config.test.enabled);
        assert!(config.historical.enabled);
    }

    #[test]
    fn orchestrator_builder() {
        let config = MultiLayerConfig::builder()
            .max_depth(5)
            .min_confidence(0.7)
            .enable_test_layer(true)
            .build();

        assert_eq!(config.direct.max_depth, 5);
        assert_eq!(config.min_confidence, 0.7);
        assert!(config.test.enabled);
    }

    /// Issue #103: path steps read caller -> callee even when the BFS walked
    /// the edge backwards (upstream).
    #[test]
    fn reversed_traversal_steps_are_caller_to_callee() {
        use super::reconstruct_path_steps;
        use crate::model::Symbol;
        use std::collections::{HashMap, HashSet};
        let mk = |id: i64, qn: &str| Symbol {
            id,
            file_path: "a.rs".into(),
            kind: "function".into(),
            name: qn.into(),
            qualname: qn.into(),
            start_line: 1,
            start_col: 0,
            end_line: 2,
            end_col: 0,
            start_byte: 0,
            end_byte: 1,
            signature: None,
            docstring: None,
            graph_version: 1,
            commit_sha: None,
            stable_id: None,
        };
        let symbols: HashMap<i64, Symbol> = [(1, mk(1, "callee")), (2, mk(2, "caller"))]
            .into_iter()
            .collect();
        // seed 1 (callee); caller 2 reached upstream, i.e. against the edge.
        let parents = HashMap::from([(2, (1, "CALLS".to_string(), None, true))]);
        let seeds = HashSet::from([1]);
        let steps = reconstruct_path_steps(2, &seeds, &parents, &symbols);
        assert_eq!(steps[0].from_symbol, "caller");
        assert_eq!(steps[0].to_symbol, "callee");
    }
}
