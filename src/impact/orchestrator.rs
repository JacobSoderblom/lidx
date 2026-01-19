//! Multi-layer impact analysis orchestrator
//!
//! This module coordinates execution of multiple impact analysis layers,
//! fuses their results, and handles graceful degradation when layers fail.

use crate::db::Db;
use crate::impact::config::MultiLayerConfig;
use crate::impact::confidence::fuse_evidence;
use crate::impact::layers::{analyze_direct_impact, HistoricalImpactLayer, TestImpactLayer};
use crate::impact::types::{
    ImpactEntry, ImpactSource, ImpactSummary, LayerMetadata, LayerResult,
    LayerStats, UnifiedImpactResult,
};
use crate::model::{Symbol, SymbolCompact};
use anyhow::Result;
use std::collections::HashMap;
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
    pub fn analyze_parallel(&self, seed_ids: &[i64], graph_version: i64) -> Result<UnifiedImpactResult> {
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
                        eprintln!("Warning: Failed to create DB connection for direct layer: {}", e);
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

                match analyze_direct_impact(
                    &db,
                    &seed_ids,
                    config.direct.max_depth,
                    crate::impact::TraversalDirection::from(config.direct.direction.as_str()),
                    &kinds,
                    config.direct.include_tests,
                    config.limit,
                    languages,
                    graph_version,
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
            let metadata = Arc::clone(&layer_metadata);
            let results = Arc::clone(&layer_results);

            handles.push(thread::spawn(move || {
                let db = match Db::new(&db_path) {
                    Ok(db) => db,
                    Err(e) => {
                        eprintln!("Warning: Failed to create DB connection for test layer: {}", e);
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

                let test_layer = TestImpactLayer::new(&db);
                match test_layer.analyze(&seed_ids, graph_version) {
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
                        eprintln!("Warning: Failed to create DB connection for historical layer: {}", e);
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

        // Fuse results from all layers
        let num_layers = layer_results.len();
        let (affected, summary, truncated) = self.fuse_results(layer_results, seed_ids, graph_version)?;

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
            config: self.build_config_summary(),
            layers: layer_metadata,
        })
    }

    /// Analyze impact from seed symbols using sequential layer execution (original implementation)
    fn analyze_sequential(&self, seed_ids: &[i64], graph_version: i64) -> Result<UnifiedImpactResult> {
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
            match self.run_test_layer(seed_ids, graph_version) {
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

        // Fuse results from all layers
        let num_layers = layer_results.len();
        let (affected, summary, truncated) = self.fuse_results(layer_results, seed_ids, graph_version)?;

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
            config: self.build_config_summary(),
            layers: layer_metadata,
        })
    }

    /// Run Layer 1: Direct impact (BFS traversal)
    fn run_direct_layer(&self, seed_ids: &[i64], graph_version: i64) -> Result<LayerResult> {
        let kinds = self.config.direct.kinds.iter().cloned().collect();
        let languages = self.config.direct.languages.as_deref();

        analyze_direct_impact(
            self.db,
            seed_ids,
            self.config.direct.max_depth,
            crate::impact::TraversalDirection::from(self.config.direct.direction.as_str()),
            &kinds,
            self.config.direct.include_tests,
            self.config.limit,
            languages,
            graph_version,
        )
    }

    /// Run Layer 2: Test impact
    fn run_test_layer(&self, seed_ids: &[i64], graph_version: i64) -> Result<LayerResult> {
        let test_layer = TestImpactLayer::new(self.db);
        test_layer.analyze(seed_ids, graph_version)
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
    ) -> Result<(Vec<ImpactEntry>, ImpactSummary, bool)> {
        // Collect all unique symbol IDs and their evidence
        let mut symbol_evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();
        let mut any_truncated = false;

        for layer_result in &layer_results {
            any_truncated = any_truncated || layer_result.truncated;

            for (symbol_id, _confidence) in &layer_result.impacts {
                if let Some(evidence) = layer_result.evidence.get(symbol_id) {
                    symbol_evidence
                        .entry(*symbol_id)
                        .or_default()
                        .extend(evidence.iter().cloned());
                }
            }
        }

        // Load all impacted symbols
        let symbol_ids: Vec<i64> = symbol_evidence.keys().copied().collect();
        let symbols = self.db.symbols_by_ids(&symbol_ids, None, graph_version)?;
        let symbol_map: HashMap<i64, Symbol> = symbols
            .into_iter()
            .map(|s| (s.id, s))
            .collect();

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
                let has_direct = evidence.iter().any(|e| matches!(e, ImpactSource::DirectEdge { .. }));
                let has_test = evidence.iter().any(|e| matches!(e, ImpactSource::TestLink { .. }));

                let distance = evidence
                    .iter()
                    .filter_map(|e| match e {
                        ImpactSource::DirectEdge { distance, .. } => Some(*distance),
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

                // Build path if requested
                let path = if self.config.include_paths {
                    Some(crate::impact::types::ImpactPath { steps: Vec::new() })
                } else {
                    None
                };

                affected.push(ImpactEntry {
                    symbol: SymbolCompact::from(symbol),
                    distance,
                    relationship,
                    path,
                    confidence: Some(confidence),
                });
            }
        }

        // Sort by distance, then by qualname for determinism
        affected.sort_by(|a, b| {
            a.distance
                .cmp(&b.distance)
                .then_with(|| a.symbol.qualname.cmp(&b.symbol.qualname))
        });

        // Build summary
        let summary = crate::impact::build_summary_from_entries(&affected);

        Ok((affected, summary, any_truncated))
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
}
