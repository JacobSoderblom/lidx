//! Direct impact layer (Layer 1)
//!
//! Implements BFS graph traversal to find directly connected symbols.
//! This is the core impact analysis algorithm, refactored from the original
//! src/impact.rs to support the layered architecture.

use crate::db::Db;
use crate::impact::confidence::apply_distance_decay;
use crate::impact::types::{
    ConfidenceScore, ImpactSource, LayerResult,
};
use crate::model::{Edge, Symbol};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Direction to traverse the symbol graph
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversalDirection {
    /// Follow incoming edges (who calls/imports this)
    Upstream,
    /// Follow outgoing edges (what does this call/import)
    Downstream,
    /// Follow all edges
    Both,
}

impl Default for TraversalDirection {
    fn default() -> Self {
        TraversalDirection::Both
    }
}

impl From<&str> for TraversalDirection {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "upstream" | "up" | "callers" | "in" => TraversalDirection::Upstream,
            "downstream" | "down" | "callees" | "out" => TraversalDirection::Downstream,
            _ => TraversalDirection::Both,
        }
    }
}

/// Determine the next symbol to visit based on edge direction
fn next_symbol(edge: &Edge, current_id: i64, direction: TraversalDirection) -> Option<i64> {
    match direction {
        TraversalDirection::Upstream => {
            if edge.target_symbol_id == Some(current_id) {
                edge.source_symbol_id
            } else {
                None
            }
        }
        TraversalDirection::Downstream => {
            if edge.source_symbol_id == Some(current_id) {
                edge.target_symbol_id
            } else {
                None
            }
        }
        TraversalDirection::Both => {
            if edge.source_symbol_id == Some(current_id) {
                edge.target_symbol_id
            } else if edge.target_symbol_id == Some(current_id) {
                edge.source_symbol_id
            } else {
                None
            }
        }
    }
}

/// Check if a file path appears to be a test file
pub fn is_test_file(path: &str) -> bool {
    let path_lower = path.to_lowercase();
    path_lower.contains("/test/")
        || path_lower.contains("/tests/")
        || path_lower.contains("/_test/")
        || path_lower.contains("/__tests__/")
        || path_lower.contains("/spec/")
        || path_lower.contains("test_")
        || path_lower.contains("_test.")
        || path_lower.contains(".test.")
        || path_lower.contains(".spec.")
        || path_lower.ends_with("_test.rs")
        || path_lower.ends_with("_test.py")
        || path_lower.ends_with(".test.ts")
        || path_lower.ends_with(".test.tsx")
        || path_lower.ends_with(".spec.ts")
        || path_lower.ends_with(".spec.tsx")
        || path_lower.ends_with("_spec.rb")
        || path_lower.ends_with("test.java")
}

/// Check if an edge matches the filtering criteria
fn edge_matches_filter(edge: &Edge, kinds: &HashSet<String>, include_tests: bool) -> bool {
    // Check edge kind
    if !kinds.is_empty() && !kinds.contains(&edge.kind) {
        return false;
    }
    // Check test file
    if !include_tests && is_test_file(&edge.file_path) {
        return false;
    }
    true
}

/// Cache symbols in bulk to avoid N+1 queries
fn cache_symbols(
    db: &Db,
    cache: &mut HashMap<i64, Symbol>,
    checked: &mut HashSet<i64>,
    ids: &[i64],
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<()> {
    let mut missing: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| !checked.contains(id))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    missing.dedup();
    let symbols = db.symbols_by_ids(&missing, languages, graph_version)?;
    for symbol in symbols {
        cache.insert(symbol.id, symbol);
    }
    for id in missing {
        checked.insert(id);
    }
    Ok(())
}

/// Analyze direct impact using BFS traversal
///
/// This is the core Layer 1 implementation that performs breadth-first search
/// through the symbol graph to find directly connected symbols.
#[allow(clippy::too_many_arguments)]
pub fn analyze_direct_impact(
    db: &Db,
    seed_ids: &[i64],
    max_depth: usize,
    direction: TraversalDirection,
    kinds: &HashSet<String>,
    include_tests: bool,
    limit: usize,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<LayerResult> {
    let start = Instant::now();
    let timeout = Duration::from_secs(5);

    // Initialize BFS data structures
    let mut queue: VecDeque<(i64, usize)> = VecDeque::new();
    let mut visited: HashSet<i64> = HashSet::new();
    let mut distance_map: HashMap<i64, usize> = HashMap::new();
    let mut symbol_cache: HashMap<i64, Symbol> = HashMap::new();
    let mut symbol_checked: HashSet<i64> = HashSet::new();

    // Load and cache seed symbols
    let seed_set: HashSet<i64> = seed_ids.iter().copied().collect();
    cache_symbols(
        db,
        &mut symbol_cache,
        &mut symbol_checked,
        seed_ids,
        languages,
        graph_version,
    )?;

    // Filter seeds by language if specified
    let valid_seeds: Vec<i64> = seed_ids
        .iter()
        .copied()
        .filter(|id| symbol_cache.contains_key(id))
        .collect();

    // Seed the queue
    for &id in &valid_seeds {
        queue.push_back((id, 0));
        visited.insert(id);
        distance_map.insert(id, 0);
    }

    let mut truncated = false;

    // BFS traversal with level-by-level batch queries
    while !queue.is_empty() {
        // Check timeout
        if start.elapsed() > timeout {
            truncated = true;
            break;
        }

        // Check limit
        if visited.len() >= limit {
            truncated = true;
            break;
        }

        // Collect all symbols at current level
        let mut current_level = Vec::new();
        let mut current_distance = usize::MAX;

        while let Some((id, distance)) = queue.front() {
            if current_distance == usize::MAX {
                current_distance = *distance;
            } else if *distance != current_distance {
                break;
            }
            current_level.push(*id);
            queue.pop_front();
        }

        // Don't expand beyond max depth
        if current_distance >= max_depth {
            continue;
        }

        // Batch fetch edges for all symbols at this level
        let mut edges_by_symbol = db.edges_for_symbols(&current_level, languages, graph_version)?;

        // For upstream/both directions, also fetch incoming edges via qualname pattern
        // This catches callers where target_symbol_id is NULL but target_qualname matches
        if matches!(direction, TraversalDirection::Upstream | TraversalDirection::Both) {
            for &current_id in &current_level {
                if let Some(sym) = symbol_cache.get(&current_id) {
                    let incoming = db.incoming_edges_by_qualname_pattern(
                        &sym.name, "CALLS", languages, graph_version
                    )?;
                    if !incoming.is_empty() {
                        let entry = edges_by_symbol.entry(current_id).or_default();
                        let existing_ids: HashSet<i64> = entry.iter().map(|e| e.id).collect();
                        for edge in incoming {
                            if !existing_ids.contains(&edge.id) {
                                // Verify qualname actually matches this symbol
                                let matches = edge.target_qualname.as_ref().map_or(false, |qn| {
                                    qn == &sym.qualname || qn.ends_with(&format!(".{}", sym.name))
                                });
                                if matches {
                                    entry.push(edge);
                                }
                            }
                        }
                    }
                }
            }
        }

        // First pass: collect unresolved target qualnames for batch resolution
        let mut unresolved_qualnames: Vec<String> = Vec::new();
        for current_id in &current_level {
            if let Some(edges) = edges_by_symbol.get(current_id) {
                for edge in edges {
                    if !edge_matches_filter(edge, kinds, include_tests) {
                        continue;
                    }
                    if next_symbol(edge, *current_id, direction).is_none() {
                        if let Some(ref qn) = edge.target_qualname {
                            // For downstream/both: resolve target when source is current
                            if edge.source_symbol_id == Some(*current_id)
                                && matches!(direction, TraversalDirection::Downstream | TraversalDirection::Both)
                            {
                                unresolved_qualnames.push(qn.clone());
                            }
                        }
                    }
                }
            }
        }

        // Batch resolve qualnames to symbol IDs (using fuzzy matching for short qualnames)
        let mut resolved_qualnames: HashMap<String, i64> = HashMap::new();
        for qn in &unresolved_qualnames {
            if !resolved_qualnames.contains_key(qn) {
                if let Ok(Some(id)) = db.lookup_symbol_id_fuzzy(qn, None, graph_version) {
                    resolved_qualnames.insert(qn.clone(), id);
                }
            }
        }

        // Collect all neighbor IDs for batch symbol loading
        let mut neighbor_ids = Vec::new();
        for current_id in &current_level {
            if let Some(edges) = edges_by_symbol.get(current_id) {
                for edge in edges {
                    if !edge_matches_filter(edge, kinds, include_tests) {
                        continue;
                    }

                    let next_id = next_symbol(edge, *current_id, direction)
                        .or_else(|| {
                            // Downstream: resolve unresolved target via qualname
                            if edge.source_symbol_id == Some(*current_id) {
                                edge.target_qualname.as_ref()
                                    .and_then(|qn| resolved_qualnames.get(qn).copied())
                            } else {
                                None
                            }
                        })
                        .or_else(|| {
                            // Upstream: edge has source but unresolved target — use source as caller
                            if matches!(direction, TraversalDirection::Upstream | TraversalDirection::Both)
                                && edge.target_symbol_id.is_none()
                                && edge.source_symbol_id.is_some()
                                && edge.source_symbol_id != Some(*current_id)
                            {
                                edge.source_symbol_id
                            } else {
                                None
                            }
                        });
                    if let Some(id) = next_id {
                        if !visited.contains(&id) {
                            neighbor_ids.push(id);
                        }
                    }
                }
            }
        }

        // Batch load neighbor symbols
        cache_symbols(
            db,
            &mut symbol_cache,
            &mut symbol_checked,
            &neighbor_ids,
            languages,
            graph_version,
        )?;

        // Collect bridgeable edges for cross-service traversal
        let mut bridge_targets: Vec<(String, String)> = Vec::new();

        // Process edges and update BFS state
        for current_id in &current_level {
            if let Some(edges) = edges_by_symbol.get(current_id) {
                for edge in edges {
                    if !edge_matches_filter(edge, kinds, include_tests) {
                        continue;
                    }

                    // Collect bridge targets
                    if let Some(ref tq) = edge.target_qualname {
                        if crate::indexer::channel::bridge_complement(&edge.kind).is_some() {
                            bridge_targets.push((tq.clone(), edge.kind.clone()));
                        }
                    }

                    let next_id = match next_symbol(edge, *current_id, direction)
                        .or_else(|| {
                            if edge.source_symbol_id == Some(*current_id) {
                                edge.target_qualname.as_ref()
                                    .and_then(|qn| resolved_qualnames.get(qn).copied())
                            } else {
                                None
                            }
                        })
                        .or_else(|| {
                            if matches!(direction, TraversalDirection::Upstream | TraversalDirection::Both)
                                && edge.target_symbol_id.is_none()
                                && edge.source_symbol_id.is_some()
                                && edge.source_symbol_id != Some(*current_id)
                            {
                                edge.source_symbol_id
                            } else {
                                None
                            }
                        }) {
                        Some(id) => id,
                        None => continue,
                    };

                    // Skip if already visited
                    if !visited.insert(next_id) {
                        continue;
                    }

                    // Only queue if symbol was successfully loaded
                    if !symbol_cache.contains_key(&next_id) {
                        continue;
                    }

                    // Track distance
                    distance_map.insert(next_id, current_distance + 1);
                    queue.push_back((next_id, current_distance + 1));

                    // Check limit
                    if visited.len() >= limit {
                        truncated = true;
                        break;
                    }
                }

                if truncated {
                    break;
                }
            }
        }

        // Bridge pass: follow cross-service edges via bridge complements
        if !truncated {
            for (tq, edge_kind) in &bridge_targets {
                if let Some(complement_kinds) = crate::indexer::channel::bridge_complement(edge_kind) {
                    let bridged = db.edges_by_target_qualname_and_kinds(
                        tq, complement_kinds, languages, graph_version
                    ).unwrap_or_default();
                    for bridged_edge in &bridged {
                        let Some(bridged_id) = bridged_edge.source_symbol_id else { continue };
                        if !visited.insert(bridged_id) { continue; }
                        // Load and cache the bridged symbol
                        cache_symbols(db, &mut symbol_cache, &mut symbol_checked, &[bridged_id], languages, graph_version)?;
                        if !symbol_cache.contains_key(&bridged_id) { continue; }
                        distance_map.insert(bridged_id, current_distance + 1);
                        queue.push_back((bridged_id, current_distance + 1));
                        if visited.len() >= limit {
                            truncated = true;
                            break;
                        }
                    }
                    if truncated { break; }
                }
            }
        }

        if truncated {
            break;
        }
    }

    // Build results (exclude seeds)
    let mut impacts: Vec<(i64, ConfidenceScore)> = Vec::new();
    let mut evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();

    for &symbol_id in visited.iter() {
        if seed_set.contains(&symbol_id) {
            continue; // Skip seed symbols
        }

        let distance = *distance_map.get(&symbol_id).unwrap_or(&0);

        // Calculate confidence with distance decay
        // Base confidence for direct edges is 0.95
        let confidence = apply_distance_decay(0.95, distance);

        impacts.push((symbol_id, confidence));

        // Track evidence source
        evidence.insert(
            symbol_id,
            vec![ImpactSource::DirectEdge {
                edge_kind: "DIRECT".to_string(), // Simplified for now
                distance,
            }],
        );
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(LayerResult {
        layer_name: "direct".to_string(),
        impacts,
        evidence,
        duration_ms,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_from_string() {
        assert_eq!(TraversalDirection::from("upstream"), TraversalDirection::Upstream);
        assert_eq!(TraversalDirection::from("DOWNSTREAM"), TraversalDirection::Downstream);
        assert_eq!(TraversalDirection::from("both"), TraversalDirection::Both);
        assert_eq!(TraversalDirection::from("invalid"), TraversalDirection::Both);
    }

    #[test]
    fn test_file_detection() {
        assert!(is_test_file("src/tests/foo.rs"));
        assert!(is_test_file("src/__tests__/foo.js"));
        assert!(is_test_file("spec/foo_spec.rb"));
        assert!(is_test_file("foo_test.py"));
        assert!(is_test_file("foo.spec.ts"));
        assert!(!is_test_file("src/main.rs"));
        assert!(!is_test_file("testimony.py"));
    }

    #[test]
    fn edge_filter_respects_kinds() {
        let edge = Edge {
            id: 1,
            file_path: "src/main.rs".to_string(),
            kind: "CALL".to_string(),
            source_symbol_id: Some(1),
            target_symbol_id: Some(2),
            target_qualname: None,
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
        };

        let mut kinds = HashSet::new();
        kinds.insert("CALL".to_string());
        assert!(edge_matches_filter(&edge, &kinds, true));

        kinds.clear();
        kinds.insert("IMPORT".to_string());
        assert!(!edge_matches_filter(&edge, &kinds, true));

        // Empty kinds means no filtering
        kinds.clear();
        assert!(edge_matches_filter(&edge, &kinds, true));
    }

    #[test]
    fn edge_filter_respects_test_files() {
        let mut edge = Edge {
            id: 1,
            file_path: "src/test/foo.rs".to_string(),
            kind: "CALL".to_string(),
            source_symbol_id: Some(1),
            target_symbol_id: Some(2),
            target_qualname: None,
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
        };

        let kinds = HashSet::new();
        assert!(!edge_matches_filter(&edge, &kinds, false));
        assert!(edge_matches_filter(&edge, &kinds, true));

        edge.file_path = "src/main.rs".to_string();
        assert!(edge_matches_filter(&edge, &kinds, false));
    }
}
