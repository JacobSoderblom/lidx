use crate::db::Db;
use crate::indexer::test_detection::is_test_file;
use crate::model::Symbol;
use crate::subgraph::{EdgeFilter, edge_allowed};
use anyhow::Result;
use std::collections::HashSet;

use super::GatherConfig;

/// Expand symbol seeds via subgraph to find related symbols
pub(super) fn expand_via_subgraph(
    db: &Db,
    symbol_ids: &[i64],
    config: &GatherConfig,
) -> Result<Vec<Symbol>> {
    if symbol_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Always fetch the seed symbols themselves first
    // These will be in the subgraph but we need to make sure they're included
    // even if include_related is false
    if !config.include_related {
        // Just return the seed symbols themselves without expansion
        let mut symbols = Vec::new();
        for id in symbol_ids {
            if let Some(symbol) = db.get_symbol_by_id(*id)? {
                symbols.push(symbol);
            }
        }
        return Ok(symbols);
    }

    // Use existing subgraph logic — include cross-file edge kinds
    let filter = EdgeFilter {
        include: Some(
            [
                "CALLS",
                "CONTAINS",
                "IMPLEMENTS",
                "EXTENDS",
                "IMPORTS",
                "RPC_IMPL",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        ),
        exclude: Default::default(),
        exclude_all: false,
        resolved_only: false,
    };

    expand_non_test_first(db, symbol_ids, config, &filter)
}

/// Breadth-first expansion (up to `config.depth` hops) that collects every
/// non-test neighbor before any test neighbor, so test callers never consume
/// the `max_nodes` cap ahead of non-test callers/callees (issue #359).
/// Test symbols are not expanded further. Gather-local: the shared
/// `build_subgraph_filtered` used by other methods is untouched.
fn expand_non_test_first(
    db: &Db,
    symbol_ids: &[i64],
    config: &GatherConfig,
    filter: &EdgeFilter,
) -> Result<Vec<Symbol>> {
    let languages = config.languages.as_deref();
    let mut seeds: Vec<i64> = symbol_ids.to_vec();
    seeds.sort_unstable();
    seeds.dedup();
    let mut visited: HashSet<i64> = HashSet::new();
    let mut frontier: Vec<i64> = Vec::new();
    let mut non_test: Vec<Symbol> = Vec::new();
    for sym in db.symbols_by_ids(&seeds, languages, config.graph_version)? {
        if visited.insert(sym.id) {
            frontier.push(sym.id);
            non_test.push(sym);
        }
    }
    let mut test_nodes: Vec<Symbol> = Vec::new();
    let mut deferred_tests: Vec<Symbol> = Vec::new();

    for _ in 0..config.depth {
        if frontier.is_empty() || visited.len() >= config.max_nodes {
            break;
        }
        let mut found: HashSet<i64> = HashSet::new();
        for id in &frontier {
            let edges = db.edges_for_symbol_with_dispatch(*id, languages, config.graph_version)?;
            for edge in edges.iter().filter(|e| edge_allowed(e, filter)) {
                let nid = if edge.source_symbol_id == Some(*id) {
                    edge.target_symbol_id
                } else {
                    edge.source_symbol_id
                };
                if let Some(nid) = nid
                    && !visited.contains(&nid)
                {
                    found.insert(nid);
                }
            }
        }
        let mut ids: Vec<i64> = found.into_iter().collect();
        ids.sort_unstable();
        let mut symbols = db.symbols_by_ids(&ids, languages, config.graph_version)?;
        symbols.sort_by(|a, b| a.qualname.cmp(&b.qualname).then_with(|| a.id.cmp(&b.id)));
        frontier.clear();
        for sym in symbols {
            if is_test_file(&sym.file_path) {
                deferred_tests.push(sym);
            } else if visited.len() < config.max_nodes && visited.insert(sym.id) {
                frontier.push(sym.id);
                non_test.push(sym);
            }
        }
    }

    // Test nodes only use whatever cap the non-test nodes left over.
    for sym in deferred_tests {
        if visited.len() >= config.max_nodes {
            break;
        }
        if visited.insert(sym.id) {
            test_nodes.push(sym);
        }
    }

    let by_name =
        |a: &Symbol, b: &Symbol| a.qualname.cmp(&b.qualname).then_with(|| a.id.cmp(&b.id));
    non_test.sort_by(by_name);
    test_nodes.sort_by(by_name);
    non_test.extend(test_nodes);
    Ok(non_test)
}
