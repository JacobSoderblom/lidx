use crate::db::Db;
use crate::model::{
    ContextItem, ContextMetadata, GatherContextResult, ItemSource, MatchLocation, SkipReason,
    SourceType, Symbol,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

/// Configuration for gather_context operation
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
    /// Pre-resolved semantic search results: seed_index -> Vec<(symbol_id, score)>
    pub semantic_results: HashMap<usize, Vec<(i64, f32)>>,
    /// Content strategy: "symbol" (symbol bodies only) or "file" (full files)
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
            semantic_results: HashMap::new(),
            strategy: None,
        }
    }
}

/// Collects skip reasons during seed resolution
struct SkipReasonCollector {
    reasons: Vec<SkipReason>,
}

impl SkipReasonCollector {
    fn new() -> Self {
        Self {
            reasons: Vec::new(),
        }
    }

    fn add(&mut self, reason: SkipReason) {
        self.reasons.push(reason);
    }

    fn into_vec(self) -> Vec<SkipReason> {
        self.reasons
    }
}

/// Tracks deduplication state
struct DeduplicationTracker {
    /// Map of path -> sorted list of (start_byte, end_byte) regions
    regions_by_path: HashMap<String, Vec<(i64, i64)>>,
    /// Count of deduplicated items
    dedup_count: usize,
}

impl DeduplicationTracker {
    fn new() -> Self {
        Self {
            regions_by_path: HashMap::new(),
            dedup_count: 0,
        }
    }

    /// Returns true if this region was NOT seen before (and marks it as seen)
    fn mark_if_new(&mut self, path: &str, start_byte: i64, end_byte: i64) -> bool {
        let regions = self.regions_by_path
            .entry(path.to_string())
            .or_default();

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

    fn dedup_count(&self) -> usize {
        self.dedup_count
    }
}

/// Resolved seed ready for content gathering
#[derive(Debug)]
enum ResolvedSeed {
    /// Symbol with ID and optional content region
    Symbol {
        symbol: Symbol,
        content_region: Option<(i64, i64)>, // start_byte, end_byte
    },
    /// File region
    FileRegion {
        path: String,
        start_byte: i64,
        end_byte: i64,
        start_line: Option<i64>,
        end_line: Option<i64>,
    },
    /// Search results (symbol IDs with scores and match locations)
    SearchResults {
        symbol_ids: Vec<(i64, f32)>,                  // (id, score)
        match_locations: HashMap<i64, MatchLocation>, // symbol_id -> match
    },
}

/// Main entry point for gathering context
pub fn gather_context(
    db: &Db,
    repo_root: &Path,
    seeds: &[crate::rpc::ContextSeed],
    config: &GatherConfig,
) -> Result<GatherContextResult> {
    let start_time = Instant::now();

    // Resolve all seeds
    let (resolved, skip_reasons) = resolve_seeds(db, repo_root, seeds, config)?;
    let seeds_processed = resolved.len();
    let seeds_skipped = skip_reasons.len();

    // Collect symbol IDs for subgraph expansion
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

    // Expand via subgraph to find related symbols (skip in dry_run mode)
    let related_symbols = if config.dry_run {
        Vec::new()
    } else {
        expand_via_subgraph(db, &symbol_ids, config)?
    };
    let symbols_resolved = symbol_ids.len();

    // Build a map of all match locations from search results
    let mut all_match_locations = HashMap::new();
    for (_, resolved_seed) in &resolved {
        if let ResolvedSeed::SearchResults {
            match_locations, ..
        } = resolved_seed
        {
            all_match_locations.extend(match_locations.clone());
        }
    }

    // Collect content within byte budget
    let (mut items, total_bytes, truncated, dedup_count, estimated_bytes) = collect_content(
        db,
        repo_root,
        &resolved,
        &related_symbols,
        &all_match_locations,
        config,
    )?;

    // Sort items deterministically
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

/// Resolve all seeds to concrete references in one pass
fn resolve_seeds(
    db: &Db,
    repo_root: &Path,
    seeds: &[crate::rpc::ContextSeed],
    config: &GatherConfig,
) -> Result<(Vec<(usize, ResolvedSeed)>, Vec<SkipReason>)> {
    use crate::rpc::ContextSeed;

    let mut resolved = Vec::new();
    let mut skip_collector = SkipReasonCollector::new();

    // Collect all qualnames for batch lookup
    let qualnames: Vec<&str> = seeds
        .iter()
        .filter_map(|seed| match seed {
            ContextSeed::Symbol { qualname } => Some(qualname.as_str()),
            _ => None,
        })
        .collect();

    // Batch resolve symbols (single query per qualname)
    let symbol_map: HashMap<String, Symbol> = batch_resolve_qualnames(
        db,
        &qualnames,
        config.languages.as_deref(),
        config.graph_version,
    )?;

    // Process each seed
    for (idx, seed) in seeds.iter().enumerate() {
        match seed {
            ContextSeed::Symbol { qualname } => {
                if let Some(symbol) = symbol_map.get(qualname) {
                    resolved.push((
                        idx,
                        ResolvedSeed::Symbol {
                            symbol: symbol.clone(),
                            content_region: Some((symbol.start_byte, symbol.end_byte)),
                        },
                    ));
                } else {
                    // Get suggestions using find_symbols
                    let suggestions = get_symbol_suggestions(db, qualname, config)?;
                    skip_collector.add(SkipReason::symbol_not_found(idx, qualname, suggestions));
                }
            }
            ContextSeed::File {
                path,
                start_line,
                end_line,
            } => {
                match resolve_file_region(repo_root, path, *start_line, *end_line)? {
                    Some(region) => resolved.push((idx, region)),
                    None => {
                        // Determine specific reason
                        let abs_path = repo_root.join(path);
                        if !abs_path.exists() {
                            skip_collector.add(SkipReason::file_not_found(idx, path));
                        } else if let (Some(s), Some(e)) = (start_line, end_line) {
                            if s > e {
                                skip_collector
                                    .add(SkipReason::invalid_line_range(idx, path, *s, *e));
                            } else {
                                skip_collector.add(SkipReason::file_outside_repo(idx, path));
                            }
                        } else {
                            skip_collector.add(SkipReason::file_outside_repo(idx, path));
                        }
                    }
                }
            }
            ContextSeed::Search { query, limit } => {
                let (symbol_ids, match_locations) =
                    resolve_search_seed(db, repo_root, query, *limit, config)?;
                if symbol_ids.is_empty() {
                    skip_collector.add(SkipReason::search_no_results(idx, query));
                } else {
                    resolved.push((
                        idx,
                        ResolvedSeed::SearchResults {
                            symbol_ids,
                            match_locations,
                        },
                    ));
                }
            }
        }
    }

    Ok((resolved, skip_collector.into_vec()))
}

/// Batch resolve qualnames to symbols
fn batch_resolve_qualnames(
    db: &Db,
    qualnames: &[&str],
    _languages: Option<&[String]>,
    graph_version: i64,
) -> Result<HashMap<String, Symbol>> {
    let mut map = HashMap::new();

    // Use existing find_symbols with exact match
    for qualname in qualnames {
        if let Some(symbol) = db.get_symbol_by_qualname(qualname, graph_version)? {
            map.insert(qualname.to_string(), symbol);
        }
    }

    Ok(map)
}

/// Get symbol suggestions for a qualname that was not found
fn get_symbol_suggestions(db: &Db, qualname: &str, config: &GatherConfig) -> Result<Vec<String>> {
    // Extract name part from qualname for search
    let name = qualname.rsplit("::").next().unwrap_or(qualname);
    let symbols = db.find_symbols(name, 3, config.languages.as_deref(), config.graph_version)?;
    Ok(symbols.into_iter().map(|s| s.qualname).collect())
}

/// Resolve a file path and optional line range to byte offsets
fn resolve_file_region(
    repo_root: &Path,
    rel_path: &str,
    start_line: Option<i64>,
    end_line: Option<i64>,
) -> Result<Option<ResolvedSeed>> {
    let abs_path = repo_root.join(rel_path);

    // Security: Validate path is within repo root (reuse existing pattern)
    let canonical = abs_path.canonicalize().ok();
    let repo_canonical = repo_root.canonicalize().ok();

    match (canonical, repo_canonical) {
        (Some(file_path), Some(root_path)) if file_path.starts_with(&root_path) => {
            // Path is valid, continue
        }
        _ => return Ok(None), // Invalid path, skip silently
    }

    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };

    let (start_byte, end_byte, actual_start, actual_end) =
        if let (Some(start), Some(end)) = (start_line, end_line) {
            // Convert line numbers to byte offsets
            line_range_to_bytes(&content, start, end)
        } else {
            (0, content.len() as i64, None, None)
        };

    Ok(Some(ResolvedSeed::FileRegion {
        path: rel_path.to_string(),
        start_byte,
        end_byte,
        start_line: actual_start,
        end_line: actual_end,
    }))
}

/// Convert line range to byte offsets
/// This uses char_indices which returns byte offsets in the UTF-8 string,
/// consistent with the byte positions stored in the symbol table.
fn line_range_to_bytes(
    content: &str,
    start: i64,
    end: i64,
) -> (i64, i64, Option<i64>, Option<i64>) {
    let mut current_line = 1i64;
    let mut start_byte = 0i64;
    let mut end_byte = content.len() as i64;
    let mut found_start = false;

    for (idx, ch) in content.char_indices() {
        if current_line == start && !found_start {
            start_byte = idx as i64;
            found_start = true;
        }
        if ch == '\n' {
            current_line += 1;
            if current_line > end {
                end_byte = idx as i64 + 1; // Include the newline
                break;
            }
        }
    }

    (
        start_byte,
        end_byte,
        Some(start),
        Some(end.min(current_line - 1)),
    )
}

/// Resolve search query to symbol IDs with scores and match locations
/// NOTE: This function performs N+1 database queries (one per search hit)
/// to resolve enclosing symbols. This is a known trade-off documented
/// in the Staff Engineer review (Critical Issue #1). The search operation
/// itself (ripgrep) dominates latency (~38ms), so the additional database
/// queries (~15ms each) are acceptable for the MVP. Future optimization
/// could batch these lookups if search seeds become performance-critical.
fn resolve_search_seed(
    db: &Db,
    repo_root: &Path,
    query: &str,
    limit: Option<usize>,
    config: &GatherConfig,
) -> Result<(Vec<(i64, f32)>, HashMap<i64, MatchLocation>)> {
    use crate::search::{SearchOptions, search_text};

    let limit = limit.unwrap_or(10);
    let options = SearchOptions {
        languages: config.languages.as_deref(),
        scope: None,
        exclude_generated: false,
        rank: true,
        no_ignore: false,
        paths: config.paths.as_deref(),
    };

    let hits = search_text(repo_root, query, limit, options)?;

    // Map hits to symbols via enclosing symbol lookup
    let mut results = Vec::new();
    let mut locations = HashMap::new();
    for hit in hits {
        if let Some(symbol) =
            db.enclosing_symbol_for_line(&hit.path, hit.line as i64, config.graph_version)?
        {
            let score = hit.score.unwrap_or(1.0);
            results.push((symbol.id, score));
            locations.insert(
                symbol.id,
                MatchLocation {
                    line: hit.line as i64,
                    column: hit.column as i64,
                    match_text: hit.line_text.clone(),
                },
            );
        }
    }

    Ok((results, locations))
}

/// Expand symbol seeds via subgraph to find related symbols
fn expand_via_subgraph(db: &Db, symbol_ids: &[i64], config: &GatherConfig) -> Result<Vec<Symbol>> {
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
            ["CALLS", "CONTAINS", "IMPLEMENTS", "EXTENDS", "IMPORTS", "RPC_IMPL"]
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

/// Collect content for resolved seeds within byte budget
fn collect_content(
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

    // Dispatch based on strategy
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

/// Collect content using file strategy (original behavior)
fn collect_content_file_strategy(
    db: &Db,
    repo_root: &Path,
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {

    let mut items = Vec::new();
    let mut total_bytes = 0usize;
    let mut dedup = DeduplicationTracker::new();
    let mut truncated = false;

    // Priority order: direct seeds first, then related symbols

    // Process direct seeds
    for (seed_idx, resolved_seed) in resolved {
        if total_bytes >= config.max_bytes {
            truncated = true;
            break;
        }

        match resolved_seed {
            ResolvedSeed::Symbol {
                symbol,
                content_region,
            } => {
                if let Some((start, end)) = content_region {
                    if dedup.mark_if_new(&symbol.file_path, *start, *end) {
                        let source = ItemSource {
                            source_type: SourceType::DirectSeed,
                            seed_index: Some(*seed_idx),
                            relationship: None,
                            distance: Some(0),
                        };
                        let match_loc = match_locations.get(&symbol.id).cloned();
                        if let Some(item) = read_symbol_content(
                            repo_root,
                            symbol,
                            *start,
                            *end,
                            source,
                            match_loc,
                            config.max_bytes - total_bytes,
                        )? {
                            total_bytes += item.content.len();
                            items.push(item);
                        }
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
                    let source = ItemSource {
                        source_type: SourceType::DirectSeed,
                        seed_index: Some(*seed_idx),
                        relationship: None,
                        distance: Some(0),
                    };
                    if let Some(item) = read_file_region(
                        repo_root,
                        path,
                        *start_byte,
                        *end_byte,
                        *start_line,
                        *end_line,
                        source,
                        None, // File seeds don't have match locations
                        config.max_bytes - total_bytes,
                    )? {
                        total_bytes += item.content.len();
                        items.push(item);
                    }
                }
            }
            ResolvedSeed::SearchResults { .. } => {
                // Search results are processed via related_symbols below
            }
        }
    }

    // Process related symbols (from subgraph expansion and search results)
    if config.include_snippets {
        for symbol in related_symbols {
            if total_bytes >= config.max_bytes {
                truncated = true;
                break;
            }

            if dedup.mark_if_new(&symbol.file_path, symbol.start_byte, symbol.end_byte) {
                let source = ItemSource {
                    source_type: SourceType::Subgraph,
                    seed_index: None,
                    relationship: Some("related".to_string()),
                    distance: None,
                };
                let match_loc = match_locations.get(&symbol.id).cloned();
                if let Some(item) = read_symbol_content(
                    repo_root,
                    symbol,
                    symbol.start_byte,
                    symbol.end_byte,
                    source,
                    match_loc,
                    config.max_bytes - total_bytes,
                )? {
                    total_bytes += item.content.len();
                    items.push(item);
                }
            }
        }
    }

    // Secondary expansion: if budget underutilized, fetch callers from other files
    if total_bytes < (config.max_bytes * 60 / 100) && config.include_related {
        // Collect all symbol IDs and file paths from current items
        let mut current_symbol_ids: HashSet<i64> = HashSet::new();
        let mut current_file_paths: HashSet<String> = HashSet::new();
        for item in &items {
            if let Some(symbol) = &item.symbol {
                current_symbol_ids.insert(symbol.id);
                current_file_paths.insert(symbol.file_path.clone());
            }
        }

        // Convert to Vec to avoid borrow issues during iteration
        let symbol_ids_to_check: Vec<i64> = current_symbol_ids.iter().copied().collect();

        // Find incoming CALLS edges (callers)
        let mut caller_symbols = Vec::new();
        let mut seen_caller_ids = HashSet::new();

        for symbol_id in symbol_ids_to_check {
            if total_bytes >= config.max_bytes {
                break;
            }

            // Get edges for this symbol
            let edges = db.edges_for_symbol(symbol_id, config.languages.as_deref(), config.graph_version)?;

            // Find incoming CALLS edges from different files
            for edge in &edges {
                if edge.kind == "CALLS"
                    && edge.target_symbol_id == Some(symbol_id)
                    && edge.source_symbol_id.is_some()
                {
                    let source_id = edge.source_symbol_id.unwrap();

                    // Skip if we've already seen this caller or it's in the current set
                    if current_symbol_ids.contains(&source_id) || seen_caller_ids.contains(&source_id) {
                        continue;
                    }

                    // Fetch the caller symbol
                    if let Some(caller) = db.get_symbol_by_id(source_id)? {
                        // Only add callers from different files
                        if !current_file_paths.contains(&caller.file_path) {
                            caller_symbols.push(caller);
                            seen_caller_ids.insert(source_id);
                        }
                    }
                }
            }

            // Also check for callers via qualname pattern (catches unresolved target_symbol_id)
            if let Some(symbol) = items.iter()
                .filter_map(|item| item.symbol.as_ref())
                .find(|s| s.id == symbol_id)
            {
                let incoming = db.incoming_edges_by_qualname_pattern(
                    &symbol.name, "CALLS", config.languages.as_deref(), config.graph_version
                )?;
                for edge in &incoming {
                    let matches = edge.target_qualname.as_ref().map_or(false, |qn| {
                        qn == &symbol.qualname || qn.ends_with(&format!(".{}", symbol.name))
                    });
                    if matches {
                        if let Some(source_id) = edge.source_symbol_id {
                            if !current_symbol_ids.contains(&source_id) && !seen_caller_ids.contains(&source_id) {
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

        // Add caller content to items
        for caller in caller_symbols {
            if total_bytes >= config.max_bytes {
                truncated = true;
                break;
            }

            if dedup.mark_if_new(&caller.file_path, caller.start_byte, caller.end_byte) {
                let source = ItemSource {
                    source_type: SourceType::Subgraph,
                    seed_index: None,
                    relationship: Some("caller".to_string()),
                    distance: Some(1),
                };
                if let Some(item) = read_symbol_content(
                    repo_root,
                    &caller,
                    caller.start_byte,
                    caller.end_byte,
                    source,
                    None,
                    config.max_bytes - total_bytes,
                )? {
                    total_bytes += item.content.len();
                    items.push(item);
                }
            }
        }
    }

    Ok((items, total_bytes, truncated, dedup.dedup_count(), 0))
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
    let mut items = Vec::new();
    let mut total_bytes = 0usize;
    let mut dedup = DeduplicationTracker::new();
    let mut truncated = false;

    // Track which files we've already read to avoid re-reading
    let mut file_cache: HashMap<String, String> = HashMap::new();

    // Priority order: direct seeds (Tier 0), then related symbols (Tier 1/2)

    // Process direct symbol seeds at Tier 0
    for (seed_idx, resolved_seed) in resolved {
        if total_bytes >= config.max_bytes {
            truncated = true;
            break;
        }

        match resolved_seed {
            ResolvedSeed::Symbol { symbol, .. } => {
                if dedup.mark_if_new(&symbol.file_path, symbol.start_byte, symbol.end_byte) {
                    // Read file if not cached
                    let file_content = file_cache.entry(symbol.file_path.clone()).or_insert_with(|| {
                        let abs_path = repo_root.join(&symbol.file_path);
                        std::fs::read_to_string(&abs_path).unwrap_or_default()
                    });

                    // Format at Tier 0: full body with header
                    let content = format_tier0(repo_root, symbol, file_content)?;

                    if content.len() > config.max_bytes - total_bytes {
                        truncated = true;
                        continue;
                    }

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
                        start_byte: symbol.start_byte,
                        end_byte: symbol.end_byte,
                        content: content.clone(),
                        symbol: Some(symbol.clone()),
                        score: None,
                        match_location: match_loc,
                    });
                    total_bytes += content.len();
                }
            }
            ResolvedSeed::FileRegion {
                path,
                start_byte,
                end_byte,
                start_line,
                end_line,
            } => {
                // File seeds still use full file region (not symbol-based)
                if dedup.mark_if_new(path, *start_byte, *end_byte) {
                    let source = ItemSource {
                        source_type: SourceType::DirectSeed,
                        seed_index: Some(*seed_idx),
                        relationship: None,
                        distance: Some(0),
                    };
                    if let Some(item) = read_file_region(
                        repo_root,
                        path,
                        *start_byte,
                        *end_byte,
                        *start_line,
                        *end_line,
                        source,
                        None,
                        config.max_bytes - total_bytes,
                    )? {
                        total_bytes += item.content.len();
                        items.push(item);
                    }
                }
            }
            ResolvedSeed::SearchResults { .. } => {
                // Search results are processed via related_symbols below
            }
        }
    }

    // Process related symbols at Tier 1 (with cross-file expansion for direct callers/callees)
    if config.include_snippets && total_bytes < config.max_bytes {
        // Collect seed symbol IDs for cross-file expansion
        let seed_symbol_ids: HashSet<i64> = resolved
            .iter()
            .filter_map(|(_, r)| match r {
                ResolvedSeed::Symbol { symbol, .. } => Some(symbol.id),
                _ => None,
            })
            .collect();

        // First, process related symbols from subgraph expansion
        for symbol in related_symbols {
            if total_bytes >= config.max_bytes {
                truncated = true;
                break;
            }

            if dedup.mark_if_new(&symbol.file_path, symbol.start_byte, symbol.end_byte) {
                // Use Tier 2 for general related symbols (signature only)
                let content = format_tier2(symbol);

                if content.len() > config.max_bytes - total_bytes {
                    truncated = true;
                    continue;
                }

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
                    content: content.clone(),
                    symbol: Some(symbol.clone()),
                    score: None,
                    match_location: match_loc,
                });
                total_bytes += content.len();
            }
        }

        // Cross-file expansion: follow CALLS edges from seed symbols (up to 30% of remaining budget)
        if config.include_related && total_bytes < config.max_bytes {
            let cross_file_budget = ((config.max_bytes - total_bytes) * 30 / 100).max(1000);
            let mut cross_file_bytes = 0usize;

            let current_file_paths: HashSet<String> = items
                .iter()
                .filter_map(|item| item.symbol.as_ref().map(|s| s.file_path.clone()))
                .collect();

            for seed_id in &seed_symbol_ids {
                if cross_file_bytes >= cross_file_budget {
                    break;
                }

                // Get edges for this seed symbol
                let edges = db.edges_for_symbol(*seed_id, config.languages.as_deref(), config.graph_version)?;

                for edge in &edges {
                    if cross_file_bytes >= cross_file_budget {
                        break;
                    }

                    // Find CALLS edges (both incoming and outgoing)
                    if edge.kind == "CALLS" {
                        let target_id = if edge.source_symbol_id == Some(*seed_id) {
                            edge.target_symbol_id // Outgoing call
                        } else if edge.target_symbol_id == Some(*seed_id) {
                            edge.source_symbol_id // Incoming call
                        } else {
                            None
                        };

                        if let Some(tid) = target_id {
                            if let Some(target_symbol) = db.get_symbol_by_id(tid)? {
                                // Only include if from different file
                                if !current_file_paths.contains(&target_symbol.file_path) {
                                    if dedup.mark_if_new(
                                        &target_symbol.file_path,
                                        target_symbol.start_byte,
                                        target_symbol.end_byte,
                                    ) {
                                        // Use Tier 1: signature + evidence
                                        let content = format_tier1(&target_symbol, Some(edge));

                                        if content.len() <= cross_file_budget - cross_file_bytes {
                                            let source = ItemSource {
                                                source_type: SourceType::Subgraph,
                                                seed_index: None,
                                                relationship: Some("caller".to_string()),
                                                distance: Some(1),
                                            };
                                            items.push(ContextItem {
                                                source,
                                                path: target_symbol.file_path.clone(),
                                                start_line: Some(target_symbol.start_line),
                                                end_line: Some(target_symbol.end_line),
                                                start_byte: target_symbol.start_byte,
                                                end_byte: target_symbol.end_byte,
                                                content: content.clone(),
                                                symbol: Some(target_symbol.clone()),
                                                score: None,
                                                match_location: None,
                                            });
                                            cross_file_bytes += content.len();
                                            total_bytes += content.len();
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

    Ok((items, total_bytes, truncated, dedup.dedup_count(), 0))
}

/// Collect content metadata in dry_run mode (no file reads)
fn collect_content_dry_run(
    resolved: &[(usize, ResolvedSeed)],
    related_symbols: &[Symbol],
    match_locations: &HashMap<i64, MatchLocation>,
    config: &GatherConfig,
) -> Result<(Vec<ContextItem>, usize, bool, usize, usize)> {
    let mut items = Vec::new();
    let mut estimated_bytes = 0usize;
    let mut dedup = DeduplicationTracker::new();

    // Process direct seeds
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
                            content: String::new(), // Empty in dry_run
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
                        content: String::new(), // Empty in dry_run
                        symbol: None,
                        score: None,
                        match_location: None,
                    });
                }
            }
            ResolvedSeed::SearchResults { .. } => {
                // Search results are processed via related_symbols below
            }
        }
    }

    // Process related symbols (from subgraph expansion and search results)
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
                    content: String::new(), // Empty in dry_run
                    symbol: Some(symbol.clone()),
                    score: None,
                    match_location: match_loc,
                });
            }
        }
    }

    // In dry_run, truncated is always false since we're not actually reading
    Ok((items, 0, false, dedup.dedup_count(), estimated_bytes))
}

/// Read file header (first 10 lines, capped at 500 bytes)
fn read_file_header(repo_root: &Path, file_path: &str) -> Result<String> {
    let abs_path = repo_root.join(file_path);
    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(String::new()),
    };

    let mut header = String::new();
    let mut lines = 0;
    for line in content.lines() {
        if lines >= 10 || header.len() > 500 {
            break;
        }
        header.push_str(line);
        header.push('\n');
        lines += 1;
    }

    // Truncate at 500 bytes if needed
    if header.len() > 500 {
        if let Some(pos) = header[..500].rfind('\n') {
            header.truncate(pos + 1);
        } else {
            header.truncate(500);
        }
    }

    Ok(header)
}

/// Format symbol at Tier 0: full source body with file header
fn format_tier0(
    repo_root: &Path,
    symbol: &Symbol,
    file_content: &str,
) -> Result<String> {
    let header = read_file_header(repo_root, &symbol.file_path)?;

    let start = symbol.start_byte as usize;
    let end = (symbol.end_byte as usize).min(file_content.len());

    if start >= end || start >= file_content.len() {
        return Ok(String::new());
    }

    let body = &file_content[start..end];

    let mut result = String::new();
    if !header.is_empty() {
        result.push_str(&format!("// File: {} (header)\n", symbol.file_path));
        result.push_str(&header);
        result.push_str("\n");
    }
    result.push_str(&format!("// Symbol: {} ({})\n", symbol.qualname, symbol.kind));
    result.push_str(body);

    Ok(result)
}

/// Format symbol at Tier 1: signature + call site evidence
fn format_tier1(symbol: &Symbol, edge: Option<&crate::model::Edge>) -> String {
    let mut result = String::new();

    result.push_str(&format!("// File: {} ({})\n", symbol.file_path, symbol.kind));

    if let Some(sig) = &symbol.signature {
        result.push_str(sig);
        result.push('\n');
    } else {
        result.push_str(&format!("{} {}\n", symbol.kind, symbol.name));
    }

    if let Some(e) = edge {
        if let (Some(snippet), Some(line)) = (&e.evidence_snippet, e.evidence_start_line) {
            result.push_str(&format!("  // {} at line {}\n", e.kind.to_lowercase(), line));
            result.push_str("  ");
            result.push_str(&snippet.trim());
            result.push('\n');
        }
    }

    result
}

/// Format symbol at Tier 2: signature only
fn format_tier2(symbol: &Symbol) -> String {
    let mut result = String::new();

    result.push_str(&format!("// File: {} ({})\n", symbol.file_path, symbol.kind));

    if let Some(sig) = &symbol.signature {
        result.push_str(sig);
        result.push('\n');
    } else {
        result.push_str(&format!("{} {}\n", symbol.kind, symbol.name));
    }

    result
}

/// Read content for a symbol
/// Addresses Critical Issue #2: File modification time check
fn read_symbol_content(
    repo_root: &Path,
    symbol: &Symbol,
    start_byte: i64,
    end_byte: i64,
    source: ItemSource,
    match_location: Option<MatchLocation>,
    remaining_budget: usize,
) -> Result<Option<ContextItem>> {
    let abs_path = repo_root.join(&symbol.file_path);

    // Critical Issue #2: File modification time check
    // TODO: Need to compare file mtime against graph_version.created timestamp
    // Currently disabled because symbol.graph_version is an ID (1, 2, 3), not a timestamp.
    // To properly implement this, we need to:
    // 1. Store graph_version created timestamp in GatherConfig
    // 2. Pass it through to read_symbol_content
    // 3. Compare file mtime > created_timestamp
    // For MVP, we skip this check and read potentially stale content.

    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };

    let start = start_byte as usize;
    let end = (end_byte as usize).min(content.len());

    if start >= end || start >= content.len() {
        return Ok(None);
    }

    let mut snippet = content[start..end].to_string();

    // Truncate to remaining budget if needed
    if snippet.len() > remaining_budget {
        // Truncate at a line boundary if possible
        if let Some(pos) = snippet[..remaining_budget].rfind('\n') {
            snippet.truncate(pos + 1);
        } else {
            snippet.truncate(remaining_budget);
        }
    }

    Ok(Some(ContextItem {
        source,
        path: symbol.file_path.clone(),
        start_line: Some(symbol.start_line),
        end_line: Some(symbol.end_line),
        start_byte,
        end_byte: start_byte + snippet.len() as i64,
        content: snippet,
        symbol: Some(symbol.clone()),
        score: None,
        match_location,
    }))
}

/// Read content for a file region
fn read_file_region(
    repo_root: &Path,
    rel_path: &str,
    start_byte: i64,
    end_byte: i64,
    start_line: Option<i64>,
    end_line: Option<i64>,
    source: ItemSource,
    match_location: Option<MatchLocation>,
    remaining_budget: usize,
) -> Result<Option<ContextItem>> {
    let abs_path = repo_root.join(rel_path);

    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };

    let start = start_byte as usize;
    let end = (end_byte as usize).min(content.len());

    if start >= end || start >= content.len() {
        return Ok(None);
    }

    let mut snippet = content[start..end].to_string();

    // Truncate to remaining budget if needed
    if snippet.len() > remaining_budget {
        // Truncate at a line boundary if possible
        if let Some(pos) = snippet[..remaining_budget].rfind('\n') {
            snippet.truncate(pos + 1);
        } else {
            snippet.truncate(remaining_budget);
        }
    }

    Ok(Some(ContextItem {
        source,
        path: rel_path.to_string(),
        start_line,
        end_line,
        start_byte,
        end_byte: start_byte + snippet.len() as i64,
        content: snippet,
        symbol: None,
        score: None,
        match_location,
    }))
}

/// Sort items deterministically for consistent output
fn sort_items(items: &mut [ContextItem]) {
    items.sort_by(|a, b| {
        // Primary: source type (seeds before subgraph)
        let source_rank = |source: &ItemSource| -> u8 {
            match source.source_type {
                SourceType::DirectSeed => 0,
                SourceType::Subgraph => 1,
                SourceType::Search => 2,
            }
        };

        source_rank(&a.source)
            .cmp(&source_rank(&b.source))
            // Secondary: seed index (if both are direct seeds)
            .then_with(|| a.source.seed_index.cmp(&b.source.seed_index))
            // Tertiary: path (alphabetical)
            .then_with(|| a.path.cmp(&b.path))
            // Fourth: start line
            .then_with(|| a.start_line.cmp(&b.start_line))
            // Finally: start byte for regions within same line
            .then_with(|| a.start_byte.cmp(&b.start_byte))
    });
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

    #[test]
    fn line_range_to_bytes_handles_edge_cases() {
        let content = "line1\nline2\nline3\n";

        // Normal case
        let (start, end, _, _) = line_range_to_bytes(content, 2, 2);
        assert_eq!(&content[start as usize..end as usize], "line2\n");

        // Range beyond end
        let (_, end, _, actual_end) = line_range_to_bytes(content, 1, 100);
        assert_eq!(end, content.len() as i64);
        assert!(actual_end.unwrap() <= 4);

        // Empty content
        let (start, end, _, _) = line_range_to_bytes("", 1, 1);
        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn line_range_to_bytes_byte_offset_consistency() {
        // Critical Issue #3: Verify byte offset consistency
        // Ensure line_range_to_bytes produces offsets consistent with symbol table
        let content = "fn test() {\n    println!(\"hello\");\n}\n";

        // Test that byte offsets align correctly
        let (start, end, _, _) = line_range_to_bytes(content, 2, 2);

        // Line 2 should be "    println!("hello");\n"
        let line2 = &content[start as usize..end as usize];
        assert!(line2.contains("println"));

        // Verify the byte positions are valid UTF-8 boundaries
        assert!(content.is_char_boundary(start as usize));
        assert!(content.is_char_boundary(end as usize));
    }

    #[test]
    fn sort_items_is_deterministic() {
        let mut items1 = vec![
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::Subgraph,
                    seed_index: None,
                    relationship: Some("related".to_string()),
                    distance: None,
                },
                path: "b.rs".into(),
                start_line: Some(10),
                end_line: Some(10),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::DirectSeed,
                    seed_index: Some(0),
                    relationship: None,
                    distance: Some(0),
                },
                path: "a.rs".into(),
                start_line: Some(1),
                end_line: Some(1),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::DirectSeed,
                    seed_index: Some(1),
                    relationship: None,
                    distance: Some(0),
                },
                path: "a.rs".into(),
                start_line: Some(5),
                end_line: Some(5),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
        ];

        let mut items2 = items1.clone();
        items2.reverse();

        sort_items(&mut items1);
        sort_items(&mut items2);

        // Both should have same order after sorting
        assert_eq!(items1[0].path, "a.rs");
        assert_eq!(items1[0].start_line, Some(1));
        assert_eq!(items1[1].start_line, Some(5));
        assert!(matches!(items1[2].source.source_type, SourceType::Subgraph));

        for (a, b) in items1.iter().zip(items2.iter()) {
            assert_eq!(a.path, b.path);
            assert_eq!(a.start_line, b.start_line);
        }
    }
}
