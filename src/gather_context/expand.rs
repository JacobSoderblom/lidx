use crate::db::Db;
use crate::indexer::test_detection::is_test_file;
use crate::model::Symbol;
use anyhow::Result;

use super::{GatherConfig, MAX_TEST_NODES};

/// Expand symbol seeds via subgraph to find related symbols
pub(super) fn expand_via_subgraph(
    db: &Db,
    symbol_ids: &[i64],
    config: &GatherConfig,
) -> Result<Vec<Symbol>> {
    use crate::subgraph::{Deferral, EdgeFilter, build_subgraph_deferring};

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

    // Test code is deprioritised (issue #359): admitted only after all
    // non-test nodes within `depth`, capped at MAX_TEST_NODES. Test-scope
    // files stay leaf nodes (their own callers/callees are not expanded).
    let defer_tests = |s: &Symbol| is_test_file(&s.file_path);
    let deferral = Deferral {
        is_deferred: &defer_tests,
        max: MAX_TEST_NODES,
    };
    let subgraph = build_subgraph_deferring(
        db,
        symbol_ids,
        config.depth,
        config.max_nodes,
        config.languages.as_deref(),
        config.graph_version,
        Some(&filter),
        Some(&deferral),
    )?;

    // Stable partition: non-test nodes first, test nodes last.
    let mut nodes = subgraph.nodes;
    nodes.sort_by_key(|s| is_test_file(&s.file_path));
    Ok(nodes)
}
