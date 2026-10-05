use crate::db::Db;
use crate::model::{ContextItem, ItemSource, MatchLocation, SourceType, Symbol};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::GatherConfig;
use super::format::{
    format_tier0, format_tier1, format_tier2, read_file_region, read_symbol_content,
};
use super::resolve::ResolvedSeed;

/// Tracks deduplication state
pub(super) struct DeduplicationTracker {
    /// Map of path -> sorted list of (start_byte, end_byte) regions
    regions_by_path: HashMap<String, Vec<(i64, i64)>>,
    /// Count of deduplicated items
    dedup_count: usize,
}

impl DeduplicationTracker {
    pub(super) fn new() -> Self {
        Self {
            regions_by_path: HashMap::new(),
            dedup_count: 0,
        }
    }

    /// Returns true if this region was NOT seen before (and marks it as seen)
    pub(super) fn mark_if_new(&mut self, path: &str, start_byte: i64, end_byte: i64) -> bool {
        let regions = self.regions_by_path.entry(path.to_string()).or_default();

        // Check if new region is fully contained in any existing region
        for &(existing_start, existing_end) in regions.iter() {
            if start_byte >= existing_start && end_byte <= existing_end {
                self.dedup_count += 1;
                return false;
            }
        }

        // Also check if new region fully contains any existing region
        // (we still add it - the larger region subsumes the smaller ones)
        regions.push((start_byte, end_byte));
        true
    }

    pub(super) fn dedup_count(&self) -> usize {
        self.dedup_count
    }
}

/// Encapsulates budget tracking, dedup, and item collection for content strategies.
pub(super) struct ContentCollector<'a> {
    pub(super) items: Vec<ContextItem>,
    pub(super) total_bytes: usize,
    pub(super) truncated: bool,
    dedup: DeduplicationTracker,
    pub(super) max_bytes: usize,
    pub(super) repo_root: &'a Path,
}

impl<'a> ContentCollector<'a> {
    pub(super) fn new(repo_root: &'a Path, max_bytes: usize) -> Self {
        Self {
            items: Vec::new(),
            total_bytes: 0,
            truncated: false,
            dedup: DeduplicationTracker::new(),
            max_bytes,
            repo_root,
        }
    }

    pub(super) fn over_budget(&self) -> bool {
        self.total_bytes >= self.max_bytes
    }

    pub(super) fn remaining(&self) -> usize {
        self.max_bytes.saturating_sub(self.total_bytes)
    }

    /// Check if the given size fits in the remaining budget. If not, mark
    /// truncation and return false. Used to enforce whole-items-only semantics
    /// (issue #104): never emit a byte-sliced fragment.
    fn check_fits(&mut self, size: usize) -> bool {
        if size > self.remaining() {
            self.truncated = true;
            false
        } else {
            true
        }
    }

    /// Try to add symbol content. Returns true if added.
    pub(super) fn try_add_symbol(
        &mut self,
        symbol: &Symbol,
        start: i64,
        end: i64,
        source: ItemSource,
        match_loc: Option<MatchLocation>,
    ) -> Result<bool> {
        if !self.dedup.mark_if_new(&symbol.file_path, start, end) {
            return Ok(false);
        }
        // Whole items only: never emit a byte-sliced fragment (issue #104).
        // If the full region can't fit in what's left of the budget, drop
        // the item instead of truncating it mid-token.
        let size = (end - start).max(0) as usize;
        if !self.check_fits(size) {
            return Ok(false);
        }
        if let Some(item) = read_symbol_content(
            self.repo_root,
            symbol,
            start,
            end,
            source,
            match_loc,
            self.remaining(),
        )? {
            self.total_bytes += item.content.len();
            self.items.push(item);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Try to add a file region. Returns true if added.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_add_file_region(
        &mut self,
        path: &str,
        start_byte: i64,
        end_byte: i64,
        start_line: Option<i64>,
        end_line: Option<i64>,
        source: ItemSource,
        match_loc: Option<MatchLocation>,
    ) -> Result<bool> {
        if !self.dedup.mark_if_new(path, start_byte, end_byte) {
            return Ok(false);
        }
        // Whole items only: never emit a byte-sliced fragment (issue #104).
        // If the full region can't fit in what's left of the budget, drop
        // the item instead of truncating it mid-token.
        let size = (end_byte - start_byte).max(0) as usize;
        if !self.check_fits(size) {
            return Ok(false);
        }
        if let Some(item) = read_file_region(
            self.repo_root,
            path,
            start_byte,
            end_byte,
            start_line,
            end_line,
            source,
            match_loc,
            self.remaining(),
        )? {
            self.total_bytes += item.content.len();
            self.items.push(item);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Try to add pre-formatted content. Returns true if added (fits budget + not deduped).
    pub(super) fn try_add_formatted(
        &mut self,
        symbol: &Symbol,
        content: String,
        source: ItemSource,
        match_loc: Option<MatchLocation>,
    ) -> bool {
        if !self
            .dedup
            .mark_if_new(&symbol.file_path, symbol.start_byte, symbol.end_byte)
        {
            return false;
        }
        if content.len() > self.remaining() {
            self.truncated = true;
            return false;
        }
        self.total_bytes += content.len();
        self.items.push(ContextItem {
            source,
            path: symbol.file_path.clone(),
            start_line: Some(symbol.start_line),
            end_line: Some(symbol.end_line),
            start_byte: symbol.start_byte,
            end_byte: symbol.end_byte,
            content,
            symbol: Some(symbol.clone()),
            score: None,
            match_location: match_loc,
        });
        true
    }

    pub(super) fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    pub(super) fn finish(self) -> (Vec<ContextItem>, usize, bool, usize, usize) {
        let dedup_count = self.dedup.dedup_count();
        (self.items, self.total_bytes, self.truncated, dedup_count, 0)
    }
}

/// Add related symbols to the collector. `stubs_only` emits signature stubs instead of bodies.
/// With `skip_covered_files`, symbols whose file already has an item are skipped.
fn add_related(
    c: &mut ContentCollector,
    symbols: &[&Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    labels: &HashMap<i64, &'static str>,
    stubs_only: bool,
    skip_covered_files: bool,
) -> Result<()> {
    for symbol in symbols {
        if c.over_budget() {
            c.mark_truncated();
            break;
        }
        if skip_covered_files && c.items.iter().any(|i| i.path == symbol.file_path) {
            continue;
        }
        let source = ItemSource {
            source_type: SourceType::Subgraph,
            seed_index: None,
            relationship: Some(labels.get(&symbol.id).unwrap_or(&"related").to_string()),
            distance: None,
        };
        let match_loc = match_locations.get(&symbol.id).cloned();
        if stubs_only {
            c.try_add_formatted(symbol, format_tier2(symbol), source, match_loc);
        } else {
            c.try_add_symbol(
                symbol,
                symbol.start_byte,
                symbol.end_byte,
                source,
                match_loc,
            )?;
        }
    }
    Ok(())
}

/// Label CALLS neighbours of the seed symbols by edge direction ("caller" or "callee").
fn direction_labels(
    db: &Db,
    resolved: &[(usize, ResolvedSeed)],
    config: &GatherConfig,
) -> Result<HashMap<i64, &'static str>> {
    let mut labels = HashMap::new();
    for (_, r) in resolved {
        let ResolvedSeed::Symbol { symbol, .. } = r else {
            continue;
        };
        let edges = db.edges_for_symbol_with_dispatch(
            symbol.id,
            config.languages.as_deref(),
            config.graph_version,
        )?;
        for e in edges.iter().filter(|e| e.kind == "CALLS") {
            if e.source_symbol_id == Some(symbol.id) {
                labels.extend(e.target_symbol_id.map(|id| (id, "callee")));
            } else if e.target_symbol_id == Some(symbol.id) {
                labels.extend(e.source_symbol_id.map(|id| (id, "caller")));
            }
        }
    }
    Ok(labels)
}

/// Split related symbols into (graph-connected symbols, module stubs). Module stubs span a whole
/// file, so they are added last and must never claim a file before real symbols do.
fn split_modules(related: &[Symbol]) -> (Vec<&Symbol>, Vec<&Symbol>) {
    related.iter().partition(|s| s.kind != "module")
}

/// Collect content for resolved seeds within byte budget
pub(super) fn collect_content(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut collected = match config.strategy.as_deref() {
        Some(super::STRATEGY_SYMBOL) => collect_content_symbol_strategy(
            db,
            repo_root,
            resolved,
            related_symbols,
            match_locations,
            config,
        ),
        _ => collect_content_file_strategy(
            db,
            repo_root,
            resolved,
            related_symbols,
            match_locations,
            config,
        ),
    }?;

    // dry_run runs the real collection so the estimate matches, then drops the content.
    if config.dry_run {
        collected.4 = collected.1;
        collected.1 = 0;
        for item in &mut collected.0 {
            item.content.clear();
        }
    }
    Ok(collected)
}

/// Process search-seed matches: they must win the budget over unrelated subgraph
/// expansion, and be tagged distinctly as the actual search hit rather than a
/// generic "related" item (issue #104).
///
/// Calls the provided callback for each symbol found. The callback receives
/// the collector mutably so it can decide how to add the symbol.
fn add_search_seed_matches<F>(
    db: &Db,
    resolved: &[(usize, ResolvedSeed)],
    match_locations: &HashMap<i64, MatchLocation>,
    collector: &mut ContentCollector,
    mut add_symbol_fn: F,
) -> Result<()>
where
    F: FnMut(&mut ContentCollector, &Symbol, &ItemSource, Option<MatchLocation>) -> Result<()>,
{
    for (seed_idx, resolved_seed) in resolved {
        if collector.over_budget() {
            collector.mark_truncated();
            break;
        }
        let ResolvedSeed::SearchResults { symbol_ids, .. } = resolved_seed else {
            continue;
        };
        for (symbol_id, _score) in symbol_ids {
            if collector.over_budget() {
                collector.mark_truncated();
                break;
            }
            let Some(symbol) = db.get_symbol_by_id(*symbol_id)? else {
                continue;
            };
            let source = ItemSource {
                source_type: SourceType::Search,
                seed_index: Some(*seed_idx),
                relationship: None,
                distance: Some(0),
            };
            add_symbol_fn(
                collector,
                &symbol,
                &source,
                match_locations.get(&symbol.id).cloned(),
            )?;
        }
    }
    Ok(())
}

/// Collect content using file strategy (original behavior)
fn collect_content_file_strategy(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut c = ContentCollector::new(repo_root, config.max_bytes);

    // Process direct seeds
    for (seed_idx, resolved_seed) in resolved {
        if c.over_budget() {
            c.mark_truncated();
            break;
        }
        let seed_source = |idx: usize| ItemSource {
            source_type: SourceType::DirectSeed,
            seed_index: Some(idx),
            relationship: None,
            distance: Some(0),
        };
        match resolved_seed {
            ResolvedSeed::Symbol {
                symbol,
                content_region,
            } => {
                if let Some((start, end)) = content_region {
                    c.try_add_symbol(
                        symbol,
                        *start,
                        *end,
                        seed_source(*seed_idx),
                        match_locations.get(&symbol.id).cloned(),
                    )?;
                }
            }
            ResolvedSeed::FileRegion {
                path,
                start_byte,
                end_byte,
                start_line,
                end_line,
            } => {
                c.try_add_file_region(
                    path,
                    *start_byte,
                    *end_byte,
                    *start_line,
                    *end_line,
                    seed_source(*seed_idx),
                    None,
                )?;
            }
            ResolvedSeed::SearchResults { .. } => {}
        }
    }

    // Process search-seed matches next
    add_search_seed_matches(
        db,
        resolved,
        match_locations,
        &mut c,
        |collector, symbol, source, match_loc| {
            collector.try_add_symbol(
                symbol,
                symbol.start_byte,
                symbol.end_byte,
                source.clone(),
                match_loc,
            )?;
            Ok(())
        },
    )?;

    // Process related symbols (module stubs are added after cross-file expansion)
    let (graph_related, module_stubs) = split_modules(related_symbols);
    let labels = direction_labels(db, resolved, config)?;
    add_related(
        &mut c,
        &graph_related,
        match_locations,
        &labels,
        !config.include_snippets,
        false,
    )?;

    // Secondary expansion: if budget underutilized, fetch callers from other files
    if c.total_bytes < (config.max_bytes * 60 / 100) && config.include_related {
        let mut current_symbol_ids: HashSet<i64> = HashSet::new();
        let mut current_file_paths: HashSet<String> = HashSet::new();
        for item in &c.items {
            if let Some(symbol) = &item.symbol {
                current_symbol_ids.insert(symbol.id);
                if symbol.kind != "module" {
                    current_file_paths.insert(symbol.file_path.clone());
                }
            }
        }

        let symbol_ids_to_check: Vec<i64> = current_symbol_ids.iter().copied().collect();
        let mut caller_symbols = Vec::new();
        let mut seen_caller_ids = HashSet::new();

        for symbol_id in symbol_ids_to_check {
            if c.over_budget() {
                break;
            }

            let edges = db.edges_for_symbol_with_dispatch(
                symbol_id,
                config.languages.as_deref(),
                config.graph_version,
            )?;
            for edge in &edges {
                if edge.kind == "CALLS" && edge.target_symbol_id == Some(symbol_id) {
                    let Some(source_id) = edge.source_symbol_id else {
                        continue;
                    };
                    if current_symbol_ids.contains(&source_id)
                        || seen_caller_ids.contains(&source_id)
                    {
                        continue;
                    }
                    if let Some(caller) = db.get_symbol_by_id(source_id)?
                        && !current_file_paths.contains(&caller.file_path)
                    {
                        caller_symbols.push(caller);
                        seen_caller_ids.insert(source_id);
                    }
                }
            }
        }

        for caller in caller_symbols {
            if c.over_budget() {
                c.mark_truncated();
                break;
            }
            let source = ItemSource {
                source_type: SourceType::Subgraph,
                seed_index: None,
                relationship: Some("caller".to_string()),
                distance: Some(1),
            };
            if config.include_snippets {
                c.try_add_symbol(&caller, caller.start_byte, caller.end_byte, source, None)?;
            } else {
                c.try_add_formatted(&caller, format_tier2(&caller), source, None);
            }
        }
    }

    add_related(
        &mut c,
        &module_stubs,
        match_locations,
        &labels,
        !config.include_snippets,
        true,
    )?;

    Ok(c.finish())
}

/// Collect content using symbol strategy (symbol bodies only with tiered detail)
fn collect_content_symbol_strategy(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut c = ContentCollector::new(repo_root, config.max_bytes);
    let mut file_cache: HashMap<String, String> = HashMap::new();

    // Process direct symbol seeds at Tier 0
    for (seed_idx, resolved_seed) in resolved {
        if c.over_budget() {
            c.mark_truncated();
            break;
        }
        let seed_source = |idx: usize| ItemSource {
            source_type: SourceType::DirectSeed,
            seed_index: Some(idx),
            relationship: None,
            distance: Some(0),
        };
        match resolved_seed {
            ResolvedSeed::Symbol { symbol, .. } => {
                let file_content =
                    file_cache
                        .entry(symbol.file_path.clone())
                        .or_insert_with(|| {
                            let abs_path = repo_root.join(&symbol.file_path);
                            std::fs::read_to_string(&abs_path).unwrap_or_default()
                        });
                let content = format_tier0(repo_root, symbol, file_content)?;
                c.try_add_formatted(
                    symbol,
                    content,
                    seed_source(*seed_idx),
                    match_locations.get(&symbol.id).cloned(),
                );
            }
            ResolvedSeed::FileRegion {
                path,
                start_byte,
                end_byte,
                start_line,
                end_line,
            } => {
                c.try_add_file_region(
                    path,
                    *start_byte,
                    *end_byte,
                    *start_line,
                    *end_line,
                    seed_source(*seed_idx),
                    None,
                )?;
            }
            ResolvedSeed::SearchResults { .. } => {}
        }
    }

    // Process search-seed matches next at Tier 0 (full source body)
    add_search_seed_matches(
        db,
        resolved,
        match_locations,
        &mut c,
        |collector, symbol, source, match_loc| {
            let file_content = file_cache
                .entry(symbol.file_path.clone())
                .or_insert_with(|| {
                    let abs_path = repo_root.join(&symbol.file_path);
                    std::fs::read_to_string(&abs_path).unwrap_or_default()
                });
            let content = format_tier0(repo_root, symbol, file_content)?;
            collector.try_add_formatted(symbol, content, source.clone(), match_loc);
            Ok(())
        },
    )?;

    // Process related symbols at Tier 1/2 (module stubs are added last)
    if !c.over_budget() {
        let seed_symbol_ids: HashSet<i64> = resolved
            .iter()
            .filter_map(|(_, r)| match r {
                ResolvedSeed::Symbol { symbol, .. } => Some(symbol.id),
                _ => None,
            })
            .collect();

        let (graph_related, module_stubs) = split_modules(related_symbols);
        let labels = direction_labels(db, resolved, config)?;
        add_related(
            &mut c,
            &graph_related,
            match_locations,
            &labels,
            !config.include_snippets,
            false,
        )?;

        // Cross-file expansion via CALLS edges (up to 30% of remaining budget)
        if config.include_related && !c.over_budget() {
            let cross_file_budget = (c.remaining() * 30 / 100).max(1000);
            let mut cross_file_bytes = 0usize;

            let current_file_paths: HashSet<String> = c
                .items
                .iter()
                .filter_map(|item| item.symbol.as_ref())
                .filter(|s| s.kind != "module")
                .map(|s| s.file_path.clone())
                .collect();

            for seed_id in &seed_symbol_ids {
                if cross_file_bytes >= cross_file_budget {
                    break;
                }
                let edges = db.edges_for_symbol_with_dispatch(
                    *seed_id,
                    config.languages.as_deref(),
                    config.graph_version,
                )?;
                for edge in &edges {
                    if cross_file_bytes >= cross_file_budget {
                        break;
                    }
                    if edge.kind == "CALLS" {
                        let (target_id, relationship) = if edge.source_symbol_id == Some(*seed_id) {
                            (edge.target_symbol_id, "callee")
                        } else if edge.target_symbol_id == Some(*seed_id) {
                            (edge.source_symbol_id, "caller")
                        } else {
                            (None, "")
                        };
                        if let Some(tid) = target_id
                            && let Some(target_symbol) = db.get_symbol_by_id(tid)?
                            && !current_file_paths.contains(&target_symbol.file_path)
                        {
                            let source = ItemSource {
                                source_type: SourceType::Subgraph,
                                seed_index: None,
                                relationship: Some(relationship.to_string()),
                                distance: Some(1),
                            };
                            if config.include_snippets {
                                let size = (target_symbol.end_byte - target_symbol.start_byte)
                                    .max(0) as usize;
                                if size <= cross_file_budget - cross_file_bytes {
                                    let before = c.total_bytes;
                                    if c.try_add_symbol(
                                        &target_symbol,
                                        target_symbol.start_byte,
                                        target_symbol.end_byte,
                                        source,
                                        None,
                                    )? {
                                        cross_file_bytes += c.total_bytes - before;
                                    }
                                }
                            } else {
                                let content = format_tier1(&target_symbol, Some(edge));
                                if content.len() <= cross_file_budget - cross_file_bytes
                                    && c.try_add_formatted(
                                        &target_symbol,
                                        content.clone(),
                                        source,
                                        None,
                                    )
                                {
                                    cross_file_bytes += content.len();
                                }
                            }
                        }
                    }
                }
            }
        }

        add_related(&mut c, &module_stubs, match_locations, &labels, true, true)?;
    }

    Ok(c.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_tracker_marks_unique_regions() {
        let mut tracker = DeduplicationTracker::new();

        // First insertion returns true (was new)
        assert!(tracker.mark_if_new("foo.rs", 0, 100));

        // Same region returns false (already seen)
        assert!(!tracker.mark_if_new("foo.rs", 0, 100));

        // Different region returns true
        assert!(tracker.mark_if_new("foo.rs", 100, 200));

        // Same region in different file returns true
        assert!(tracker.mark_if_new("bar.rs", 0, 100));

        assert_eq!(tracker.dedup_count(), 1);
    }

    #[test]
    fn dedup_tracker_detects_overlapping_regions() {
        let mut tracker = DeduplicationTracker::new();

        // Add a class region (0-500)
        assert!(tracker.mark_if_new("foo.rs", 0, 500));

        // Method inside the class (100-200) should be detected as overlapping
        assert!(!tracker.mark_if_new("foo.rs", 100, 200));
        assert_eq!(tracker.dedup_count(), 1);

        // Another method inside (300-400) should also be detected
        assert!(!tracker.mark_if_new("foo.rs", 300, 400));
        assert_eq!(tracker.dedup_count(), 2);

        // Adjacent region after the class should be new
        assert!(tracker.mark_if_new("foo.rs", 500, 600));
        assert_eq!(tracker.dedup_count(), 2);

        // Partial overlap at the boundary (exact boundary is not contained)
        assert!(tracker.mark_if_new("foo.rs", 490, 510));
        assert_eq!(tracker.dedup_count(), 2);
    }
}
