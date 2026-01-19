//! Impact analysis engine for refactoring assistance
//!
//! Answers "what breaks if I change this?" by traversing the symbol graph
//! and tracking relationship paths.
//!
//! Uses a layered architecture:
//! - Layer 1: Direct impact (BFS graph traversal)
//! - Layer 2: Test impact (test-to-source relationships)
//! - Layer 3: Historical impact (co-change patterns)

pub mod confidence;
pub mod config;
pub mod layers;
pub mod orchestrator;
pub mod types;

// Re-export key types
pub use layers::direct::{analyze_direct_impact, is_test_file, TraversalDirection};
pub use types::{
    FileImpact, ImpactConfig, ImpactEntry, ImpactPath, ImpactResult, ImpactSummary, PathStep,
};

use crate::db::Db;
use crate::model::{Symbol, SymbolCompact};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// Get relationship type for a symbol based on distance
fn get_relationship(distance: usize) -> String {
    if distance == 0 {
        "SEED".to_string()
    } else if distance == 1 {
        "DIRECT".to_string()
    } else {
        format!("INDIRECT_{}", distance)
    }
}

/// Reconstruct path from seed to target symbol
/// This is a simplified version for the refactored code
fn reconstruct_path(
    _symbol_id: i64,
    _seed_ids: &HashSet<i64>,
    _symbol_cache: &HashMap<i64, Symbol>,
) -> ImpactPath {
    // TODO(future): Implement path reconstruction showing how impact propagates
    // This is a Phase 2 enhancement - see architecture/implementation-plan.md Phase 2
    // For now, return empty path
    ImpactPath { steps: Vec::new() }
}

/// Aggregate affected symbols by file
fn aggregate_by_file(entries: &[ImpactEntry]) -> Vec<FileImpact> {
    let mut by_file: HashMap<String, Vec<String>> = HashMap::new();

    for entry in entries {
        by_file
            .entry(entry.symbol.file_path.clone())
            .or_default()
            .push(entry.symbol.qualname.clone());
    }

    let mut result: Vec<FileImpact> = by_file
        .into_iter()
        .map(|(path, symbols)| FileImpact {
            path,
            symbol_count: symbols.len(),
            symbols,
        })
        .collect();

    result.sort_by(|a, b| b.symbol_count.cmp(&a.symbol_count));
    result
}

/// Build complete impact summary
fn build_summary(entries: &[ImpactEntry]) -> ImpactSummary {
    let by_file = aggregate_by_file(entries);

    let mut by_relationship: HashMap<String, usize> = HashMap::new();
    for entry in entries {
        *by_relationship
            .entry(entry.relationship.clone())
            .or_insert(0) += 1;
    }

    let mut by_distance: HashMap<usize, usize> = HashMap::new();
    for entry in entries {
        *by_distance.entry(entry.distance).or_insert(0) += 1;
    }

    ImpactSummary {
        by_file,
        by_relationship,
        by_distance,
        total_affected: entries.len(),
    }
}

/// Build summary from entries (public API for RPC filtering)
pub fn build_summary_from_entries(entries: &[ImpactEntry]) -> ImpactSummary {
    build_summary(entries)
}

/// Main impact analysis function (v1 API)
///
/// Performs BFS traversal from seed symbols to find all affected symbols within
/// the specified constraints. Returns impact analysis with paths and summary.
///
/// This function maintains backward compatibility with the original implementation
/// while using the new layered architecture under the hood.
#[allow(clippy::too_many_arguments)]
pub fn analyze_impact(
    db: &Db,
    seed_ids: &[i64],
    max_depth: usize,
    direction: TraversalDirection,
    kinds: &HashSet<String>,
    include_tests: bool,
    include_paths: bool,
    limit: usize,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<ImpactResult> {
    // Use the new direct layer implementation
    let layer_result = analyze_direct_impact(
        db,
        seed_ids,
        max_depth,
        direction,
        kinds,
        include_tests,
        limit,
        languages,
        graph_version,
    )?;

    // Load seed symbols
    let seed_set: HashSet<i64> = seed_ids.iter().copied().collect();
    let mut symbol_cache: HashMap<i64, Symbol> = HashMap::new();
    let symbols = db.symbols_by_ids(seed_ids, languages, graph_version)?;
    for symbol in symbols.clone() {
        symbol_cache.insert(symbol.id, symbol);
    }
    let seeds = symbols;

    // Load impacted symbols
    let impacted_ids: Vec<i64> = layer_result.impacts.iter().map(|(id, _)| *id).collect();
    let impacted_symbols = db.symbols_by_ids(&impacted_ids, languages, graph_version)?;
    for symbol in &impacted_symbols {
        symbol_cache.insert(symbol.id, symbol.clone());
    }

    // Build impact entries with confidence
    let mut affected: Vec<ImpactEntry> = Vec::new();
    let mut distance_map: HashMap<i64, usize> = HashMap::new();

    // Calculate distances from confidence decay
    for (symbol_id, confidence) in &layer_result.impacts {
        // Reverse-engineer distance from confidence (approximate)
        // confidence = 0.95 * 0.9^distance
        // This is a simplification; we'll improve this in Week 2
        let distance = if *confidence >= 0.95 {
            0
        } else if *confidence >= 0.855 {
            1
        } else if *confidence >= 0.7695 {
            2
        } else {
            3
        };
        distance_map.insert(*symbol_id, distance);
    }

    for (symbol_id, confidence) in &layer_result.impacts {
        if let Some(symbol) = symbol_cache.get(symbol_id) {
            let distance = *distance_map.get(symbol_id).unwrap_or(&0);
            let relationship = get_relationship(distance);
            let path = if include_paths {
                Some(reconstruct_path(*symbol_id, &seed_set, &symbol_cache))
            } else {
                None
            };

            affected.push(ImpactEntry {
                symbol: SymbolCompact::from(symbol),
                distance,
                relationship,
                path,
                confidence: Some(*confidence),
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
    let summary = build_summary(&affected);

    // Build config for response
    let config = ImpactConfig {
        max_depth,
        direction: format!("{:?}", direction),
        relationship_types: kinds.iter().cloned().collect(),
        include_tests,
        limit,
    };

    Ok(ImpactResult {
        seeds: seeds.into_iter().map(SymbolCompact::from).collect(),
        affected,
        summary,
        truncated: layer_result.truncated,
        config,
    })
}

/// Multi-layer impact analysis (v2 API)
///
/// Performs impact analysis using multiple layers (direct, test, historical, semantic)
/// based on the provided configuration. Layers can be enabled/disabled individually.
///
/// This is the new v2 API that supports the full multi-layer architecture.
/// Use `analyze_impact()` for backward compatibility with v1 API.
pub fn analyze_impact_multi_layer(
    db: &Db,
    seed_ids: &[i64],
    config: config::MultiLayerConfig,
    graph_version: i64,
) -> Result<types::UnifiedImpactResult> {
    let orchestrator = orchestrator::MultiLayerOrchestrator::new(db, config);
    orchestrator.analyze(seed_ids, graph_version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relationship_from_distance() {
        assert_eq!(get_relationship(0), "SEED");
        assert_eq!(get_relationship(1), "DIRECT");
        assert_eq!(get_relationship(2), "INDIRECT_2");
        assert_eq!(get_relationship(3), "INDIRECT_3");
    }
}
