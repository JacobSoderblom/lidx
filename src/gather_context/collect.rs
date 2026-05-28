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

pub(super) struct DeduplicationTracker {
    regions_by_path: HashMap<String, Vec<(i64, i64)>>,
    dedup_count: usize,
}

impl DeduplicationTracker {
    pub(super) fn new() -> Self {
        Self {
            regions_by_path: HashMap::new(),
            dedup_count: 0,
        }
    }

    pub(super) fn mark_if_new(&mut self, path: &str, start_byte: i64, end_byte: i64) -> bool {
        let regions = self.regions_by_path.entry(path.to_string()).or_default();

        for &(existing_start, existing_end) in regions.iter() {
            if start_byte >= existing_start && end_byte <= existing_end {
                self.dedup_count += 1;
                return false;
            }
        }

        regions.push((start_byte, end_byte));
        true
    }

    pub(super) fn dedup_count(&self) -> usize {
        self.dedup_count
    }
}

struct ContentCollector<'a> {
    items: Vec<ContextItem>,
    total_bytes: usize,
    truncated: bool,
    dedup: DeduplicationTracker,
    max_bytes: usize,
    repo_root: &'a Path,
}

impl<'a> ContentCollector<'a> {
    fn new(repo_root: &'a Path, max_bytes: usize) -> Self {
        Self {
            items: Vec::new(),
            total_bytes: 0,
            truncated: false,
            dedup: DeduplicationTracker::new(),
            max_bytes,
            repo_root,
        }
    }

    fn over_budget(&self) -> bool {
        self.total_bytes >= self.max_bytes
    }

    fn remaining(&self) -> usize {
        self.max_bytes.saturating_sub(self.total_bytes)
    }

    fn try_add_symbol(
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

    fn try_add_file_region(
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

    fn try_add_formatted(
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

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn finish(self) -> (Vec<ContextItem>, usize, bool, usize, usize) {
        let dedup_count = self.dedup.dedup_count();
        (self.items, self.total_bytes, self.truncated, dedup_count, 0)
    }
}

pub(super) fn collect_content(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    if config.dry_run {
        return collect_content_dry_run(resolved, related_symbols, match_locations, config);
    }

    match config.strategy.as_deref() {
        Some("symbol") => collect_content_symbol_strategy(
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
    }
}

fn collect_content_file_strategy(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut c = ContentCollector::new(repo_root, config.max_bytes);

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

    if config.include_snippets {
        for symbol in related_symbols {
            if c.over_budget() {
                c.mark_truncated();
                break;
            }
            let source = ItemSource {
                source_type: SourceType::Subgraph,
                seed_index: None,
                relationship: Some("related".to_string()),
                distance: None,
            };
            c.try_add_symbol(
                symbol,
                symbol.start_byte,
                symbol.end_byte,
                source,
                match_locations.get(&symbol.id).cloned(),
            )?;
        }
    }

    // Secondary expansion: if budget underutilized, fetch callers from other files
    if c.total_bytes < (config.max_bytes * 60 / 100) && config.include_related {
        let mut current_symbol_ids: HashSet<i64> = HashSet::new();
        let mut current_file_paths: HashSet<String> = HashSet::new();
        for item in &c.items {
            if let Some(symbol) = &item.symbol {
                current_symbol_ids.insert(symbol.id);
                current_file_paths.insert(symbol.file_path.clone());
            }
        }

        let symbol_ids_to_check: Vec<i64> = current_symbol_ids.iter().copied().collect();
        let mut caller_symbols = Vec::new();
        let mut seen_caller_ids = HashSet::new();

        for symbol_id in symbol_ids_to_check {
            if c.over_budget() {
                break;
            }

            let edges =
                db.edges_for_symbol(symbol_id, config.languages.as_deref(), config.graph_version)?;
            for edge in &edges {
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(symbol_id)
                    && edge.source_symbol_id.is_some()
                {
                    let source_id = edge.source_symbol_id.unwrap();
                    if current_symbol_ids.contains(&source_id)
                        || seen_caller_ids.contains(&source_id)
                    {
                        continue;
                    }
                    if let Some(caller) = db.get_symbol_by_id(source_id)? {
                        if !current_file_paths.contains(&caller.file_path) {
                            caller_symbols.push(caller);
                            seen_caller_ids.insert(source_id);
                        }
                    }
                }
            }

            if let Some(symbol) = c
                .items
                .iter()
                .filter_map(|item| item.symbol.as_ref())
                .find(|s| s.id == symbol_id)
            {
                let incoming = db.incoming_edges_by_qualname_pattern(
                    &symbol.name,
                    "CALLS",
                    config.languages.as_deref(),
                    config.graph_version,
                )?;
                for edge in &incoming {
                    let matches = edge.target_qualname.as_ref().map_or(false, |qn| {
                        qn == &symbol.qualname || qn.ends_with(&format!(".{}", symbol.name))
                    });
                    if matches {
                        if let Some(source_id) = edge.source_symbol_id {
                            if !current_symbol_ids.contains(&source_id)
                                && !seen_caller_ids.contains(&source_id)
                            {
                                if let Some(caller) = db.get_symbol_by_id(source_id)? {
                                    if !current_file_paths.contains(&caller.file_path) {
                                        caller_symbols.push(caller);
                                        seen_caller_ids.insert(source_id);
                                    }
                                }
                            }
                        }
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
            c.try_add_symbol(&caller, caller.start_byte, caller.end_byte, source, None)?;
        }
    }

    Ok(c.finish())
}

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

    if config.include_snippets && !c.over_budget() {
        let seed_symbol_ids: HashSet<i64> = resolved
            .iter()
            .filter_map(|(_, r)| match r {
                ResolvedSeed::Symbol { symbol, .. } => Some(symbol.id),
                _ => None,
            })
            .collect();

        for symbol in related_symbols {
            if c.over_budget() {
                c.mark_truncated();
                break;
            }
            let content = format_tier2(symbol);
            let source = ItemSource {
                source_type: SourceType::Subgraph,
                seed_index: None,
                relationship: Some("related".to_string()),
                distance: None,
            };
            c.try_add_formatted(
                symbol,
                content,
                source,
                match_locations.get(&symbol.id).cloned(),
            );
        }

        // Cross-file expansion via CALLS edges (up to 30% of remaining budget)
        if config.include_related && !c.over_budget() {
            let cross_file_budget = (c.remaining() * 30 / 100).max(1000);
            let mut cross_file_bytes = 0usize;

            let current_file_paths: HashSet<String> = c
                .items
                .iter()
                .filter_map(|item| item.symbol.as_ref().map(|s| s.file_path.clone()))
                .collect();

            for seed_id in &seed_symbol_ids {
                if cross_file_bytes >= cross_file_budget {
                    break;
                }
                let edges = db.edges_for_symbol(
                    *seed_id,
                    config.languages.as_deref(),
                    config.graph_version,
                )?;
                for edge in &edges {
                    if cross_file_bytes >= cross_file_budget {
                        break;
                    }
                    if edge.kind == "CALLS" {
                        let target_id = if edge.source_symbol_id == Some(*seed_id) {
                            edge.target_symbol_id
                        } else if edge.target_symbol_id == Some(*seed_id) {
                            edge.source_symbol_id
                        } else {
                            None
                        };
                        if let Some(tid) = target_id {
                            if let Some(target_symbol) = db.get_symbol_by_id(tid)? {
                                if !current_file_paths.contains(&target_symbol.file_path) {
                                    let content = format_tier1(&target_symbol, Some(edge));
                                    let source = ItemSource {
                                        source_type: SourceType::Subgraph,
                                        seed_index: None,
                                        relationship: Some("caller".to_string()),
                                        distance: Some(1),
                                    };
                                    if content.len() <= cross_file_budget - cross_file_bytes {
                                        if c.try_add_formatted(
                                            &target_symbol,
                                            content.clone(),
                                            source,
                                            None,
                                        ) {
                                            cross_file_bytes += content.len();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(c.finish())
}

fn collect_content_dry_run(
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut items = Vec::new();
    let mut estimated_bytes = 0usize;
    let mut dedup = DeduplicationTracker::new();

    for (seed_idx, resolved_seed) in resolved {
        match resolved_seed {
            ResolvedSeed::Symbol {
                symbol,
                content_region,
            } => {
                if let Some((start, end)) = content_region {
                    if dedup.mark_if_new(&symbol.file_path, *start, *end) {
                        let est_size = (end - start) as usize;
                        estimated_bytes += est_size;
                        let source = ItemSource {
                            source_type: SourceType::DirectSeed,
                            seed_index: Some(*seed_idx),
                            relationship: None,
                            distance: Some(0),
                        };
                        let match_loc = match_locations.get(&symbol.id).cloned();
                        items.push(ContextItem {
                            source,
                            path: symbol.file_path.clone(),
                            start_line: Some(symbol.start_line),
                            end_line: Some(symbol.end_line),
                            start_byte: *start,
                            end_byte: *end,
                            content: String::new(),
                            symbol: Some(symbol.clone()),
                            score: None,
                            match_location: match_loc,
                        });
                    }
                }
            }
            ResolvedSeed::FileRegion {
                path,
                start_byte,
                end_byte,
                start_line,
                end_line,
            } => {
                if dedup.mark_if_new(path, *start_byte, *end_byte) {
                    let est_size = (end_byte - start_byte) as usize;
                    estimated_bytes += est_size;
                    let source = ItemSource {
                        source_type: SourceType::DirectSeed,
                        seed_index: Some(*seed_idx),
                        relationship: None,
                        distance: Some(0),
                    };
                    items.push(ContextItem {
                        source,
                        path: path.clone(),
                        start_line: *start_line,
                        end_line: *end_line,
                        start_byte: *start_byte,
                        end_byte: *end_byte,
                        content: String::new(),
                        symbol: None,
                        score: None,
                        match_location: None,
                    });
                }
            }
            ResolvedSeed::SearchResults { .. } => {}
        }
    }

    if config.include_snippets {
        for symbol in related_symbols {
            if dedup.mark_if_new(&symbol.file_path, symbol.start_byte, symbol.end_byte) {
                let est_size = (symbol.end_byte - symbol.start_byte) as usize;
                estimated_bytes += est_size;
                let source = ItemSource {
                    source_type: SourceType::Subgraph,
                    seed_index: None,
                    relationship: Some("related".to_string()),
                    distance: None,
                };
                let match_loc = match_locations.get(&symbol.id).cloned();
                items.push(ContextItem {
                    source,
                    path: symbol.file_path.clone(),
                    start_line: Some(symbol.start_line),
                    end_line: Some(symbol.end_line),
                    start_byte: symbol.start_byte,
                    end_byte: symbol.end_byte,
                    content: String::new(),
                    symbol: Some(symbol.clone()),
                    score: None,
                    match_location: match_loc,
                });
            }
        }
    }

    Ok((items, 0, false, dedup.dedup_count(), estimated_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_tracker_marks_unique_regions() {
        let mut tracker = DeduplicationTracker::new();

        assert!(tracker.mark_if_new("foo.rs", 0, 100));
        assert!(!tracker.mark_if_new("foo.rs", 0, 100));
        assert!(tracker.mark_if_new("foo.rs", 100, 200));
        assert!(tracker.mark_if_new("bar.rs", 0, 100));

        assert_eq!(tracker.dedup_count(), 1);
    }

    #[test]
    fn dedup_tracker_detects_overlapping_regions() {
        let mut tracker = DeduplicationTracker::new();

        assert!(tracker.mark_if_new("foo.rs", 0, 500));
        assert!(!tracker.mark_if_new("foo.rs", 100, 200));
        assert_eq!(tracker.dedup_count(), 1);

        assert!(!tracker.mark_if_new("foo.rs", 300, 400));
        assert_eq!(tracker.dedup_count(), 2);

        assert!(tracker.mark_if_new("foo.rs", 500, 600));
        assert_eq!(tracker.dedup_count(), 2);

        assert!(tracker.mark_if_new("foo.rs", 490, 510));
        assert_eq!(tracker.dedup_count(), 2);
    }
}
