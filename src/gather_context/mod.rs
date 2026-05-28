mod collect;
mod expand;
mod format;
mod resolve;
mod sort;

use crate::db::Db;
use crate::model::{ContextMetadata, GatherContextResult};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use collect::collect_content;
use expand::expand_via_subgraph;
use resolve::{ResolvedSeed, resolve_seeds};
use sort::sort_items;

pub struct GatherConfig {
    pub max_bytes: usize,
    pub depth: usize,
    pub max_nodes: usize,
    pub include_snippets: bool,
    pub include_related: bool,
    pub dry_run: bool,
    pub languages: Option<Vec<String>>,
    pub paths: Option<Vec<String>>,
    pub graph_version: i64,
    pub strategy: Option<String>,
}

impl Default for GatherConfig {
    fn default() -> Self {
        Self {
            max_bytes: 100_000,
            depth: 2,
            max_nodes: 200,
            include_snippets: true,
            include_related: true,
            dry_run: false,
            languages: None,
            paths: None,
            graph_version: 1,
            strategy: None,
        }
    }
}

pub fn gather_context(
    db: &Db,
    repo_root: &Path,
    seeds: &[crate::rpc::ContextSeed],
    config: &GatherConfig,
) -> Result<GatherContextResult> {
    let start_time = Instant::now();

    let (resolved, skip_reasons) = resolve_seeds(db, repo_root, seeds, config)?;
    let seeds_processed = resolved.len();
    let seeds_skipped = skip_reasons.len();

    let mut symbol_ids = Vec::new();
    for (_, resolved_seed) in &resolved {
        match resolved_seed {
            ResolvedSeed::Symbol { symbol, .. } => {
                symbol_ids.push(symbol.id);
            }
            ResolvedSeed::SearchResults {
                symbol_ids: ids, ..
            } => {
                symbol_ids.extend(ids.iter().map(|(id, _)| *id));
            }
            ResolvedSeed::FileRegion { .. } => {}
        }
    }

    let related_symbols = if config.dry_run {
        Vec::new()
    } else {
        expand_via_subgraph(db, &symbol_ids, config)?
    };
    let symbols_resolved = symbol_ids.len();

    let mut all_match_locations = HashMap::new();
    for (_, resolved_seed) in &resolved {
        if let ResolvedSeed::SearchResults {
            match_locations, ..
        } = resolved_seed
        {
            all_match_locations.extend(match_locations.clone());
        }
    }

    let (mut items, total_bytes, truncated, dedup_count, estimated_bytes) = collect_content(
        db,
        repo_root,
        &resolved,
        &related_symbols,
        &all_match_locations,
        config,
    )?;

    sort_items(&mut items);

    let duration_ms = start_time.elapsed().as_millis() as u64;

    Ok(GatherContextResult {
        items,
        total_bytes,
        budget_bytes: config.max_bytes,
        truncated,
        estimated_bytes: if config.dry_run {
            Some(estimated_bytes)
        } else {
            None
        },
        metadata: ContextMetadata {
            seeds_processed,
            seeds_skipped,
            skip_reasons,
            symbols_resolved,
            items_deduplicated: dedup_count,
            duration_ms,
        },
    })
}
