use crate::db::Db;
use crate::model::Symbol;
use anyhow::Result;

use super::GatherConfig;

pub(super) fn expand_via_subgraph(
    db: &Db,
    symbol_ids: &[i64],
    config: &GatherConfig,
) -> Result<Vec<Symbol>> {
    use crate::subgraph::{EdgeFilter, build_subgraph_filtered};

    if symbol_ids.is_empty() {
        return Ok(Vec::new());
    }

    if !config.include_related {
        let mut symbols = Vec::new();
        for id in symbol_ids {
            if let Some(symbol) = db.get_symbol_by_id(*id)? {
                symbols.push(symbol);
            }
        }
        return Ok(symbols);
    }

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

    let subgraph = build_subgraph_filtered(
        db,
        symbol_ids,
        config.depth,
        config.max_nodes,
        config.languages.as_deref(),
        config.graph_version,
        Some(&filter),
    )?;

    Ok(subgraph.nodes)
}
