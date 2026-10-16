use crate::db::Db;
use crate::edge_views;
use crate::model::Symbol;
use anyhow::Result;

use super::GatherConfig;

/// Expand symbol seeds via subgraph to find related symbols
pub(super) fn expand_via_subgraph(
    db: &Db,
    symbol_ids: &[i64],
    config: &GatherConfig,
) -> Result<Vec<Symbol>> {
    use crate::subgraph::{EdgeFilter, build_subgraph_filtered};

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
            edge_views::CONTEXT_EXPANSION
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

    let mut nodes = subgraph.nodes;

    // A const, static, field, type alias or similar is used, not called, so
    // the call-graph expansion above finds nothing around it. Add its
    // one-hop USES/REFERENCES neighbours (its users), seeded only from such
    // symbols so that no type-use edge fans out from every class seed.
    let mut value_seeds = Vec::new();
    for id in symbol_ids {
        if let Some(symbol) = db.get_symbol_by_id(*id)?
            && crate::resolve::is_value_like_kind(&symbol.kind)
        {
            value_seeds.push(*id);
        }
    }
    if !value_seeds.is_empty() && nodes.len() < config.max_nodes {
        let uses_filter = EdgeFilter {
            include: Some(
                [edge_views::USES, edge_views::REFERENCES]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            exclude: Default::default(),
            exclude_all: false,
            resolved_only: true,
        };
        let users = build_subgraph_filtered(
            db,
            &value_seeds,
            1,
            config.max_nodes,
            config.languages.as_deref(),
            config.graph_version,
            Some(&uses_filter),
        )?;
        let mut seen: std::collections::HashSet<i64> = nodes.iter().map(|n| n.id).collect();
        for node in users.nodes {
            if nodes.len() >= config.max_nodes {
                break;
            }
            if seen.insert(node.id) {
                nodes.push(node);
            }
        }
    }

    Ok(nodes)
}
