//! Direct impact layer (Layer 1)
//!
//! Implements BFS graph traversal to find directly connected symbols.
//! This is the core impact analysis algorithm, refactored from the original
//! src/impact.rs to support the layered architecture.

use crate::db::Db;
use crate::impact::confidence::apply_distance_decay;
use crate::impact::types::{ConfidenceScore, ImpactSource, LayerResult};
use crate::indexer::config::{BridgeTarget, ConfigScope, config_edge_allowed, prefer_same_service};
use crate::indexer::test_detection::is_test_file;
use crate::model::{Edge, Symbol};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Direction to traverse the symbol graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TraversalDirection {
    /// Follow incoming edges (who calls/imports this)
    Upstream,
    /// Follow outgoing edges (what does this call/import)
    Downstream,
    /// Follow all edges
    #[default]
    Both,
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
    // CONTAINS runs parent -> child. Walking it child -> parent would report a
    // container as affected by a change to its member (issue #103).
    if edge.kind == "CONTAINS" && edge.target_symbol_id == Some(current_id) {
        return None;
    }
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

/// Check if an edge matches the filtering criteria
///
/// An empty `kinds` set means "no explicit filter" and matches every edge kind.
///
/// XREF is the exception, and its *grade* decides rather than its kind. A bare
/// `name_exact` match (confidence 0.7: a Rust `use serde::Deserialize`, a Python
/// docstring and an unrelated C# method all share the token `Deserialize`) must
/// never be presented with the authority of a real CALLS edge, asked for or not.
/// A qualified `qualname_exact` match (a literal `"dpb.pipeline_run"` naming
/// that exact table) is genuine and is kept. See
/// `crate::model::xref_is_traversable`.
fn edge_matches_filter(edge: &Edge, kinds: &HashSet<String>, include_tests: bool) -> bool {
    // Check edge kind
    if !kinds.is_empty() && !kinds.contains(&edge.kind) {
        return false;
    }
    // A bare-name XREF never drives an answer, asked for or not.
    if !crate::model::xref_is_traversable(edge) {
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

/// Resolve the next symbol ID to visit from an edge.
///
/// This is exactly `next_symbol`: a direct source/target lookup based on
/// direction. `target_symbol_id` (or `source_symbol_id`) being None means
/// the write path could not attribute this edge -- the read path must not
/// invent an attribution via fuzzy qualname lookup (that used to surface,
/// e.g., a Python `trim` function as the callee of an unrelated C#
/// `value.Trim()` call; see the equivalent fix in subgraph.rs /
/// rpc/handlers.rs). A NULL-target edge is simply not traversed.
fn resolve_next_id(edge: &Edge, current_id: i64, direction: TraversalDirection) -> Option<i64> {
    next_symbol(edge, current_id, direction)
}

/// Whether crossing a bridge from a symbol holding an `edge_kind` edge walks
/// against caller/publisher -> callee/subscriber order. The hop's parent is the
/// symbol holding `edge_kind`, so a callee-side kind means the bridged symbol
/// is the caller. Independent of the BFS direction (issue #103).
fn bridge_hop_is_reversed(edge_kind: &str) -> bool {
    matches!(
        edge_kind,
        "RPC_IMPL" | "CHANNEL_SUBSCRIBE" | "HTTP_ROUTE" | "CONFIG_READ"
    )
}

/// Follow cross-service edges via bridge complements (CHANNEL_PUBLISH↔SUBSCRIBE, RPC_CALL↔IMPL, etc.)
///
/// Returns true if the limit was hit (truncated).
#[allow(clippy::too_many_arguments)]
fn resolve_bridge_targets(
    db: &Db,
    bridge_targets: &[BridgeTarget],
    scope: &mut ConfigScope,
    visited: &mut HashSet<i64>,
    symbol_cache: &mut HashMap<i64, Symbol>,
    symbol_checked: &mut HashSet<i64>,
    distance_map: &mut HashMap<i64, usize>,
    parent_map: &mut HashMap<i64, (i64, String, Option<String>, bool)>,
    queue: &mut VecDeque<(i64, usize)>,
    current_distance: usize,
    limit: usize,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<bool> {
    for BridgeTarget {
        uri: tq,
        edge_kind,
        origin_path,
        source_id,
    } in bridge_targets
    {
        if let Some(complement_kinds) = crate::indexer::channel::bridge_complement(edge_kind) {
            let bridged = db
                .edges_by_target_qualname_and_kinds(tq, complement_kinds, languages, graph_version)
                .unwrap_or_default();
            for bridged_edge in prefer_same_service(tq, origin_path, &bridged) {
                let Some(bridged_id) = bridged_edge.source_symbol_id else {
                    continue;
                };
                if !visited.insert(bridged_id) {
                    continue;
                }
                scope.record_bridge(bridged_id, edge_kind, tq);
                cache_symbols(
                    db,
                    symbol_cache,
                    symbol_checked,
                    &[bridged_id],
                    languages,
                    graph_version,
                )?;
                if !symbol_cache.contains_key(&bridged_id) {
                    continue;
                }
                distance_map.insert(bridged_id, current_distance + 1);
                parent_map.entry(bridged_id).or_insert((
                    *source_id,
                    edge_kind.clone(),
                    bridged_edge.resolution_kind.clone(),
                    bridge_hop_is_reversed(edge_kind),
                ));
                if !symbol_cache[&bridged_id].is_external() {
                    queue.push_back((bridged_id, current_distance + 1));
                }
                if visited.len() >= limit {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
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
    exclude_resolution_kinds: &[String],
    include_tests: bool,
    limit: usize,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<LayerResult> {
    analyze_direct_impact_scoped(
        db,
        seed_ids,
        max_depth,
        direction,
        kinds,
        exclude_resolution_kinds,
        include_tests,
        limit,
        languages,
        graph_version,
        None,
    )
}

/// `analyze_direct_impact` for seeds resolved from a config URI
/// (`seed_config_uri`, issue #131): seed nodes only follow config edges
/// carrying that URI (see `ConfigScope`).
#[allow(clippy::too_many_arguments)]
pub fn analyze_direct_impact_scoped(
    db: &Db,
    seed_ids: &[i64],
    max_depth: usize,
    direction: TraversalDirection,
    kinds: &HashSet<String>,
    exclude_resolution_kinds: &[String],
    include_tests: bool,
    limit: usize,
    languages: Option<&[String]>,
    graph_version: i64,
    seed_config_uri: Option<&str>,
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
    // Config URI each node was entered through (issue #131).
    let mut scope = ConfigScope::new(&valid_seeds, seed_config_uri);
    for &id in &valid_seeds {
        queue.push_back((id, 0));
        visited.insert(id);
        distance_map.insert(id, 0);
    }

    let mut truncated = false;
    let mut parent_map: HashMap<i64, (i64, String, Option<String>, bool)> = HashMap::new();
    // Issue #81 (R5): every edge that actually contributed a newly-visited
    // symbol -- checked once, after the BFS, against `HEURISTIC_RESOLUTION_KINDS`
    // to decide whether suggesting the exclude-heuristics retry is useful at
    // all. Bridge-crossed edges aren't tracked here (see `resolve_bridge_targets`);
    // this is a "was a heuristic edge traversed" signal, not an exhaustive audit.
    let mut traversed_edge_ids: Vec<i64> = Vec::new();

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
        let edges_by_symbol =
            db.edges_for_symbols_with_dispatch(&current_level, languages, graph_version)?;

        // Issue #81: an edge with no resolution kind (a Bridge Edge kind) is
        // always traversable, since bridging is governed separately below.
        let excluded = |edge: &Edge| {
            crate::model::is_resolution_excluded(
                edge.resolution_kind.as_deref(),
                exclude_resolution_kinds,
            )
        };

        // Collect all neighbor IDs for batch symbol loading
        let mut neighbor_ids = Vec::new();
        for current_id in &current_level {
            if let Some(edges) = edges_by_symbol.get(current_id) {
                for edge in edges {
                    if !edge_matches_filter(edge, kinds, include_tests) || excluded(edge) {
                        continue;
                    }
                    if let Some(id) = resolve_next_id(edge, *current_id, direction)
                        && !visited.contains(&id)
                    {
                        neighbor_ids.push(id);
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
        let mut bridge_targets: Vec<BridgeTarget> = Vec::new();

        // Process edges and update BFS state
        for current_id in &current_level {
            if let Some(edges) = edges_by_symbol.get(current_id) {
                let allowed = scope.allowed(*current_id, edges);
                for edge in edges {
                    if !edge_matches_filter(edge, kinds, include_tests)
                        || excluded(edge)
                        || !config_edge_allowed(edge, allowed.as_ref())
                    {
                        continue;
                    }

                    // Collect bridge targets
                    if let Some(ref tq) = edge.target_qualname
                        && crate::indexer::channel::bridge_complement(&edge.kind).is_some()
                    {
                        bridge_targets.push(BridgeTarget {
                            uri: tq.clone(),
                            edge_kind: edge.kind.clone(),
                            origin_path: edge.file_path.clone(),
                            source_id: *current_id,
                        });
                    }

                    let Some(next_id) = resolve_next_id(edge, *current_id, direction) else {
                        continue;
                    };

                    if !visited.insert(next_id) {
                        continue;
                    }
                    if !symbol_cache.contains_key(&next_id) {
                        continue;
                    }

                    distance_map.insert(next_id, current_distance + 1);
                    parent_map.entry(next_id).or_insert((
                        *current_id,
                        edge.kind.clone(),
                        edge.resolution_kind.clone(),
                        edge.source_symbol_id == Some(next_id),
                    ));
                    // An external stub is a leaf: its other callers are
                    // unrelated to this impact set (issue #175).
                    if !symbol_cache[&next_id].is_external() {
                        queue.push_back((next_id, current_distance + 1));
                    }
                    traversed_edge_ids.push(edge.id);

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
            truncated = resolve_bridge_targets(
                db,
                &bridge_targets,
                &mut scope,
                &mut visited,
                &mut symbol_cache,
                &mut symbol_checked,
                &mut distance_map,
                &mut parent_map,
                &mut queue,
                current_distance,
                limit,
                languages,
                graph_version,
            )?;
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

        // Resolution tier of the edge that first reached this symbol
        // (issue #62's AC), read off the same parent_map entry
        // reconstruct_path_steps walks later.
        let resolution_kind = parent_map
            .get(&symbol_id)
            .and_then(|(_, _, rk, _)| rk.clone());

        // Track evidence source
        evidence.insert(
            symbol_id,
            vec![ImpactSource::DirectEdge {
                edge_kind: "DIRECT".to_string(), // Simplified for now
                distance,
                resolution_kind,
            }],
        );
    }

    // Issue #81 (R5): one batched query over every edge actually traversed,
    // rather than per-level -- paid only once, and only when the traversal
    // found something to traverse at all.
    let traversed_heuristic_kind = if traversed_edge_ids.is_empty() {
        false
    } else {
        let resolution_kinds = db.edge_resolution_kinds(&traversed_edge_ids)?;
        resolution_kinds
            .values()
            .any(|rk| crate::db::resolver::HEURISTIC_RESOLUTION_KINDS.contains(&rk.as_str()))
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(LayerResult {
        layer_name: "direct".to_string(),
        impacts,
        evidence,
        duration_ms,
        truncated,
        parent_map,
        traversed_heuristic_kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_from_string() {
        assert_eq!(
            TraversalDirection::from("upstream"),
            TraversalDirection::Upstream
        );
        assert_eq!(
            TraversalDirection::from("DOWNSTREAM"),
            TraversalDirection::Downstream
        );
        assert_eq!(TraversalDirection::from("both"), TraversalDirection::Both);
        assert_eq!(
            TraversalDirection::from("invalid"),
            TraversalDirection::Both
        );
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
            resolution_kind: None,
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
    fn edge_filter_crosses_only_qualified_xref() {
        let mut edge = Edge {
            id: 1,
            file_path: "src/main.rs".to_string(),
            kind: "XREF".to_string(),
            source_symbol_id: Some(1),
            target_symbol_id: Some(2),
            target_qualname: None,
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            resolution_kind: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
        };

        // A bare `name_exact` XREF is the Deserialize-class fabrication: one
        // shared word, confidence 0.7. It must never drive an answer.
        edge.detail = Some(
            r#"{"confidence":0.7,"match":"name_exact","source":"string_literal","token":"Deserialize"}"#
                .to_string(),
        );

        // Not on the unrestricted default...
        let kinds = HashSet::new();
        assert!(!edge_matches_filter(&edge, &kinds, true));

        // ...and not even when a caller names XREF explicitly. Asking for the
        // kind does not make a bare word match trustworthy.
        let mut xref_kinds = HashSet::new();
        xref_kinds.insert("XREF".to_string());
        assert!(!edge_matches_filter(&edge, &xref_kinds, true));

        // A qualified match is real evidence -- a SQL literal naming that exact
        // table -- and is crossed, including on the unrestricted default.
        edge.detail = Some(
            r#"{"confidence":1.0,"match":"qualname_exact","source":"string_literal","token":"dpb.pipeline_run"}"#
                .to_string(),
        );
        assert!(edge_matches_filter(&edge, &HashSet::new(), true));
        assert!(edge_matches_filter(&edge, &xref_kinds, true));

        // A non-empty kinds set that doesn't include XREF still excludes it.
        let mut other_kinds = HashSet::new();
        other_kinds.insert("CALLS".to_string());
        assert!(!edge_matches_filter(&edge, &other_kinds, true));

        // Sanity: the grade check is XREF-only, not a general filter. A CALLS
        // edge with no detail at all still passes.
        edge.kind = "CALLS".to_string();
        edge.detail = None;
        assert!(edge_matches_filter(&edge, &HashSet::new(), true));
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
            resolution_kind: None,
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

    // -- Regression tests: the read path must not re-attribute edges the
    // write path refused to resolve. Since issue #79, a genuinely
    // unresolved CALLS reference has no edge at all (only a store row).
    // These build a minimal DB directly so `insert_edges`' ambiguity guard
    // deterministically leaves a reference unresolved. --

    fn test_db() -> (crate::db::Db, tempfile::TempDir) {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("test.db");
        let db = crate::db::Db::new(&db_path).unwrap();
        (db, temp)
    }

    fn symbol(qualname: &str, kind: &str, start_line: i64) -> crate::indexer::extract::SymbolInput {
        crate::indexer::extract::SymbolInput {
            kind: kind.to_string(),
            name: qualname.rsplit('.').next().unwrap_or(qualname).to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line: start_line + 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 100,
            signature: None,
            docstring: None,
        }
    }

    fn calls_edge(
        source_qualname: &str,
        target_qualname: &str,
    ) -> crate::indexer::extract::EdgeInput {
        crate::indexer::extract::EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some(source_qualname.to_string()),
            target_qualname: Some(target_qualname.to_string()),
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
        }
    }

    /// A bare-name call with two same-language candidates is genuinely
    /// ambiguous, so `insert_edges`' ambiguity guard leaves it unresolved.
    /// `analyze_direct_impact` must not traverse it via a fuzzy qualname
    /// guess -- neither candidate should appear as an affected symbol.
    #[test]
    fn downstream_does_not_traverse_null_target_edge() {
        let (mut db, _temp) = test_db();
        let file_id = db
            .upsert_file("pkg/store.py", "h1", "python", 100, 0)
            .unwrap();
        let symbols = vec![
            symbol("builtins.list.append", "method", 1),
            symbol("pkg.store.EventStore.append", "method", 10),
            symbol("pkg.store.caller", "function", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "pkg/store.py", &symbols, 1, None)
            .unwrap();
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.store.caller")
            .unwrap()
            .id;

        let edges = vec![calls_edge("pkg.store.caller", "append")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Issue #79: an ambiguous CALLS edge is no longer written at all --
        // confirm the write path really did refuse to attribute it by
        // checking the unresolved-reference store instead of a NULL-target
        // edge row.
        let edge_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            edge_count, 0,
            "an ambiguous, unresolved CALLS edge must not be written at all"
        );
        let unresolved: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            unresolved, 1,
            "edge should be unresolved (ambiguous bare name)"
        );

        let mut kinds = HashSet::new();
        kinds.insert("CALLS".to_string());
        let result = analyze_direct_impact(
            &db,
            &[caller_id],
            5,
            TraversalDirection::Downstream,
            &kinds,
            &[],
            true,
            100,
            None,
            1,
        )
        .unwrap();
        assert!(
            result.impacts.is_empty(),
            "an unresolved reference must not be traversed downstream, got {:?}",
            result.impacts
        );
    }

    /// Sanity check that the fix didn't throw out the happy path: an edge
    /// the write path genuinely resolved (exact qualname match, no
    /// ambiguity) must still be traversed.
    #[test]
    fn downstream_still_traverses_genuinely_resolved_edge() {
        let (mut db, _temp) = test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            symbol("mod.Caller", "function", 1),
            symbol("mod.Callee", "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "mod.Caller")
            .unwrap()
            .id;
        let callee_id = inserted
            .iter()
            .find(|s| s.qualname == "mod.Callee")
            .unwrap()
            .id;

        let edges = vec![calls_edge("mod.Caller", "mod.Callee")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let mut kinds = HashSet::new();
        kinds.insert("CALLS".to_string());
        let result = analyze_direct_impact(
            &db,
            &[caller_id],
            5,
            TraversalDirection::Downstream,
            &kinds,
            &[],
            true,
            100,
            None,
            1,
        )
        .unwrap();
        assert_eq!(
            result.impacts.len(),
            1,
            "resolved edge should still produce an affected symbol"
        );
        assert_eq!(result.impacts[0].0, callee_id);
    }

    /// Issue #103: changing a method does not "affect" its container, so
    /// upstream traversal must not walk a CONTAINS edge child -> parent.
    /// Downstream (parent -> child) still may.
    #[test]
    fn upstream_does_not_traverse_contains_to_parent() {
        let (mut db, _temp) = test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            symbol("mod.Klass", "class", 1),
            symbol("mod.Klass.method", "method", 2),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();
        let id_of = |qn: &str| inserted.iter().find(|s| s.qualname == qn).unwrap().id;
        let mut edge = calls_edge("mod.Klass", "mod.Klass.method");
        edge.kind = "CONTAINS".to_string();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &[edge], &symbol_map, 1, None)
            .unwrap();

        let run = |seed: i64, direction| {
            analyze_direct_impact(
                &db,
                &[seed],
                3,
                direction,
                &HashSet::new(),
                &[],
                true,
                100,
                None,
                1,
            )
            .unwrap()
        };
        let up = run(id_of("mod.Klass.method"), TraversalDirection::Upstream);
        assert!(up.impacts.is_empty(), "got {:?}", up.impacts);
        let down = run(id_of("mod.Klass"), TraversalDirection::Downstream);
        assert_eq!(down.impacts.len(), 1);
    }

    /// Issue #103: a bridge hop keeps caller/publisher -> callee/subscriber
    /// order. Tracing from the callee side crosses the bridge to the caller,
    /// so the recorded hop is reversed; from the caller side it is forward.
    #[test]
    fn bridge_hop_orientation_follows_edge_kind() {
        // (callee-side kind, caller-side kind, join key)
        for (callee_kind, caller_kind, key) in [
            ("RPC_IMPL", "RPC_CALL", "pkg.Svc.Do"),
            ("CHANNEL_SUBSCRIBE", "CHANNEL_PUBLISH", "orders"),
            ("HTTP_ROUTE", "HTTP_CALL", "GET /orders"),
        ] {
            let (mut db, _temp) = test_db();
            let file_id = db
                .upsert_file("src/svc.py", "h1", "python", 100, 0)
                .unwrap();
            let symbols = vec![
                symbol("svc.Handler", "function", 1),
                symbol("svc.client", "function", 10),
            ];
            let inserted = db
                .insert_symbols(file_id, "src/svc.py", &symbols, 1, None)
                .unwrap();
            let id_of = |qn: &str| inserted.iter().find(|s| s.qualname == qn).unwrap().id;
            let mut callee = calls_edge("svc.Handler", key);
            callee.kind = callee_kind.to_string();
            let mut caller = calls_edge("svc.client", key);
            caller.kind = caller_kind.to_string();
            let symbol_map: HashMap<String, i64> = inserted
                .iter()
                .map(|s| (s.qualname.clone(), s.id))
                .collect();
            db.insert_edges(file_id, &[callee, caller], &symbol_map, 1, None)
                .unwrap();

            let run = |seed: i64| {
                analyze_direct_impact(
                    &db,
                    &[seed],
                    3,
                    TraversalDirection::Upstream,
                    &HashSet::new(),
                    &[],
                    true,
                    100,
                    None,
                    1,
                )
                .unwrap()
            };
            let from_callee = run(id_of("svc.Handler"));
            let hop = from_callee
                .parent_map
                .get(&id_of("svc.client"))
                .unwrap_or_else(|| panic!("{callee_kind}: bridge not crossed"));
            assert!(
                hop.3,
                "{callee_kind} -> caller hop must be reversed: {hop:?}"
            );
            let from_caller = run(id_of("svc.client"));
            let hop = from_caller
                .parent_map
                .get(&id_of("svc.Handler"))
                .unwrap_or_else(|| panic!("{caller_kind}: bridge not crossed"));
            assert!(
                !hop.3,
                "{caller_kind} -> callee hop must be forward: {hop:?}"
            );
        }
    }
}
