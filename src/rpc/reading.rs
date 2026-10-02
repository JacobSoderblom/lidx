//! Reading-feature RPC handlers: `outline` and `read_symbol`.
//! Moved out of handlers.rs as its own feature module (issue #93).
//!
//! Wired from `rpc/mod.rs` the same way `handlers` is: a private sibling
//! module reached via `reading::handle_outline`/`reading::handle_read_symbol`
//! in `handle_method`'s dispatch table.

use super::*;

/// Extensions treated as Markdown for `outline`. Markdown files get a
/// `files` row (issue #133) but no `symbols` rows, so `outline` reads
/// Markdown straight off disk and parses ATX headings, rather than
/// requiring indexed symbols like every other language here.
///
/// `pub(super)`: also used by `handlers::handle_search_rg` to decide whether
/// a hit's file is one `outline` can handle before emitting an `outline` hop.
pub(super) fn is_markdown_path(path: &str) -> bool {
    matches!(
        std::path::Path::new(path)
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref(),
        Some("md") | Some("markdown")
    )
}

/// First non-empty, trimmed line of a (possibly multi-line) docstring.
fn first_doc_line(docstring: &str) -> Option<String> {
    let line = docstring.lines().find(|l| !l.trim().is_empty())?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

/// One ATX Markdown heading (`# ... ######`), CommonMark-ish: up to 3 leading
/// spaces tolerated, requires a space (or EOL) after the hashes, and strips an
/// optional closing hash run (`## Heading ##`).
struct MarkdownHeading {
    level: usize,
    text: String,
    start_line: i64,
}

fn parse_markdown_headings(content: &str) -> Vec<MarkdownHeading> {
    let mut headings = Vec::new();
    // Tracks an open fenced code block (``` or ~~~) by its fence character and
    // length, so `#` lines inside one (a shell comment, a C preprocessor
    // directive, ...) are never mistaken for ATX headings.
    let mut fence: Option<(char, usize)> = None;
    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();

        if let Some((fence_char, fence_len)) = fence {
            // Only a closing fence of the same character, at least as long as
            // the opening one, alone on its line (trailing whitespace only),
            // ends the block. Anything else -- including a `#` line -- is
            // code, not a heading.
            let close_len = trimmed.chars().take_while(|&c| c == fence_char).count();
            let rest_after = trimmed[close_len..].trim();
            if indent <= 3 && close_len >= fence_len && close_len >= 3 && rest_after.is_empty() {
                fence = None;
            }
            continue;
        }

        if indent <= 3
            && let Some(fence_char) = trimmed.chars().next().filter(|&c| c == '`' || c == '~')
        {
            let run_len = trimmed.chars().take_while(|&c| c == fence_char).count();
            if run_len >= 3 {
                fence = Some((fence_char, run_len));
                continue;
            }
        }

        if indent > 3 {
            continue; // indented code block, not a heading
        }
        let level = trimmed.chars().take_while(|c| *c == '#').count();
        if level == 0 || level > 6 {
            continue;
        }
        let rest = &trimmed[level..];
        if !(rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t')) {
            continue; // e.g. "#tag", not a heading
        }
        let text = rest.trim_start().trim_end();
        // CommonMark: an optional closing run of `#`s is only stripped when
        // it's preceded by a space/tab, or when the text is nothing but
        // `#`s -- "## F#" keeps its trailing "#" (real text), while
        // "## Heading ##" and "## ###" both have it stripped.
        let hash_run = text.chars().rev().take_while(|&c| c == '#').count();
        let text = if hash_run > 0 {
            let before = &text[..text.len() - hash_run];
            if before.is_empty() || before.ends_with(' ') || before.ends_with('\t') {
                before.trim_end()
            } else {
                text
            }
        } else {
            text
        };
        let text = text.trim().to_string();
        if text.is_empty() {
            continue;
        }
        headings.push(MarkdownHeading {
            level,
            text,
            start_line: (idx + 1) as i64,
        });
    }
    headings
}

/// Builds `outline` entries for a Markdown file from its headings: `kind` is
/// `"h1"`.."h6"`, `qualname` is the breadcrumb of ancestor headings (the
/// closest thing Markdown has to a qualname), and depth/parent follow the
/// heading level stack -- a level skip (h1 straight to h3) still nests under
/// the nearest actual ancestor, not a synthesized one.
fn markdown_outline_entries(
    content: &str,
    total_lines: i64,
    kinds_filter: Option<&HashSet<String>>,
    max_depth: Option<usize>,
) -> Vec<OutlineEntry> {
    let headings = parse_markdown_headings(content);
    let mut entries = Vec::new();
    let mut stack: Vec<(usize, String)> = Vec::new();

    for (i, h) in headings.iter().enumerate() {
        while stack.last().is_some_and(|(level, _)| *level >= h.level) {
            stack.pop();
        }
        let parent = stack.last().map(|(_, qualname)| qualname.clone());
        let depth = stack.len();
        let qualname = match &parent {
            Some(p) => format!("{p} > {}", h.text),
            None => h.text.clone(),
        };
        stack.push((h.level, qualname.clone()));

        let end_line = headings[(i + 1)..]
            .iter()
            .find(|next| next.level <= h.level)
            .map(|next| next.start_line - 1)
            .unwrap_or(total_lines);

        let kind = format!("h{}", h.level);
        if kinds_filter.is_some_and(|filter| !filter.contains(&kind)) {
            continue;
        }
        if max_depth.is_some_and(|max| depth > max) {
            continue;
        }
        entries.push(OutlineEntry {
            kind,
            name: h.text.clone(),
            qualname,
            signature: None,
            start_line: h.start_line,
            end_line,
            parent,
            doc: None,
        });
    }
    entries
}

/// A file's whole-file `module` root symbol has no incoming `CONTAINS` edge
/// of its own within that file (every extractor emits exactly one; see
/// `symbol_outline_entries`'s doc comment) -- the one rule both
/// `symbol_outline_entries` (hiding it as an outline entry) and
/// `container_children` (denying it visible children) key off of, each from
/// its own already-loaded edge data (a batch-built parent map vs. one
/// symbol's own touching edges), so it's factored out here rather than
/// inlined twice.
fn is_file_root_module(kind: &str, has_incoming_contains_edge: bool) -> bool {
    kind == "module" && !has_incoming_contains_edge
}

/// Builds `outline` entries for a code file from its indexed symbols, using
/// existing `CONTAINS` edges for nesting (the same edges every extractor
/// already emits for parent/child structure) rather than re-deriving nesting
/// from qualname string-splitting (which varies by language delimiter) or
/// byte-range containment (which is wrong for Rust: a method's byte span sits
/// inside its `impl` block, not inside the struct symbol it's qualname-nested
/// under -- `impl` blocks have no symbol of their own).
///
/// Each file's extractor also emits one whole-file `module`-kind root symbol
/// (see `indexer::tree_helpers::module_symbol_with_span`); since `path`/
/// `language` already say "this is the file", that root is identified (a
/// `module` symbol with no incoming `CONTAINS` edge in this file) and hidden
/// from entries -- its direct children become top-level (depth 0) instead of
/// nesting one level under a redundant "whole file" entry. A *nested* `mod`
/// block does have an incoming `CONTAINS` edge (from its enclosing module) and
/// stays a normal entry.
fn symbol_outline_entries(
    db: &crate::db::Db,
    path: &str,
    graph_version: i64,
    kinds_filter: Option<&HashSet<String>>,
    max_depth: Option<usize>,
) -> Result<Vec<OutlineEntry>> {
    let symbols: Vec<Symbol> = db
        .get_symbols_for_file(path, graph_version)?
        .into_iter()
        // Defensive: external stubs are attributed to a synthetic `ext:` location,
        // not a real repo file, so this shouldn't normally match -- excluded anyway
        // to match their exclusion from every other repo-internal listing.
        .filter(|s| !s.is_external())
        .collect();
    if symbols.is_empty() {
        return Ok(Vec::new());
    }

    let ids: Vec<i64> = symbols.iter().map(|s| s.id).collect();
    let id_set: HashSet<i64> = ids.iter().copied().collect();
    let by_id: std::collections::HashMap<i64, &Symbol> =
        symbols.iter().map(|s| (s.id, s)).collect();

    let edges_map = db.edges_for_symbols(&ids, None, graph_version)?;
    let mut parent_of: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    let mut seen_edge_ids = HashSet::new();
    for edges in edges_map.values() {
        for edge in edges {
            if edge.kind != "CONTAINS" || !seen_edge_ids.insert(edge.id) {
                continue;
            }
            if let (Some(source_id), Some(target_id)) =
                (edge.source_symbol_id, edge.target_symbol_id)
                && id_set.contains(&source_id)
                && id_set.contains(&target_id)
            {
                parent_of.insert(target_id, source_id);
            }
        }
    }

    let root_ids: HashSet<i64> = symbols
        .iter()
        .filter(|s| is_file_root_module(&s.kind, parent_of.contains_key(&s.id)))
        .map(|s| s.id)
        .collect();

    // The visible parent: the raw `CONTAINS` parent, unless that parent is a
    // hidden file-root module -- in which case there's no visible parent.
    let effective_parent = |id: i64| -> Option<i64> {
        parent_of
            .get(&id)
            .copied()
            .filter(|parent_id| !root_ids.contains(parent_id))
    };
    let depth_of = |id: i64| -> usize {
        let mut depth = 0;
        let mut current = id;
        // Guard against a pathological cycle; real containment trees are a
        // handful of levels deep at most.
        for _ in 0..64 {
            match effective_parent(current) {
                Some(parent_id) => {
                    depth += 1;
                    current = parent_id;
                }
                None => break,
            }
        }
        depth
    };

    let mut entries = Vec::new();
    for symbol in &symbols {
        if root_ids.contains(&symbol.id) {
            continue;
        }
        if kinds_filter.is_some_and(|filter| !filter.contains(&symbol.kind)) {
            continue;
        }
        let depth = depth_of(symbol.id);
        if max_depth.is_some_and(|max| depth > max) {
            continue;
        }
        let parent = effective_parent(symbol.id)
            .and_then(|parent_id| by_id.get(&parent_id))
            .map(|p| p.qualname.clone());
        let doc = symbol.docstring.as_deref().and_then(first_doc_line);
        entries.push(OutlineEntry::from_symbol(symbol, parent, doc));
    }
    Ok(entries)
}

/// (language, total_lines, entries) for a Markdown `outline` path: no
/// `files`/`symbols` DB row to check (see `is_markdown_path`), so disk
/// presence is its only "indexed" check, and entries come from parsing ATX
/// headings straight off disk.
fn markdown_outline(
    full_path: &std::path::Path,
    path: &str,
    kinds_filter: Option<&HashSet<String>>,
    max_depth: Option<usize>,
) -> Result<(String, i64, Vec<OutlineEntry>)> {
    let content = crate::util::read_to_string(full_path)
        .map_err(|_| anyhow::anyhow!("{}", super::validate::not_indexed_message(path)))?;
    let total_lines = crate::indexer::tree_helpers::line_count(&content);
    let entries = markdown_outline_entries(&content, total_lines, kinds_filter, max_depth);
    Ok(("markdown".to_string(), total_lines, entries))
}

/// (language, total_lines, entries) for an indexed (non-Markdown) `outline`
/// path: requires a `files` DB row -- checked before touching disk at all --
/// and entries come from indexed symbols/`CONTAINS` edges.
fn indexed_outline(
    db: &crate::db::Db,
    file_record: crate::db::FileRecord,
    full_path: &std::path::Path,
    path: &str,
    graph_version: i64,
    kinds_filter: Option<&HashSet<String>>,
    max_depth: Option<usize>,
) -> Result<(String, i64, Vec<OutlineEntry>)> {
    let content = crate::util::read_to_string(full_path).map_err(|_| {
        anyhow::anyhow!(
            "file '{}' is missing from disk; run 'reindex' to refresh the index",
            path
        )
    })?;
    let total_lines = crate::indexer::tree_helpers::line_count(&content);
    let entries = symbol_outline_entries(db, path, graph_version, kinds_filter, max_depth)?;
    Ok((file_record.language, total_lines, entries))
}

/// Kinds that wrap content rather than being content: never chosen as the
/// `read_symbol` target of an `outline` next_hop (#236).
const CONTAINER_WRAPPER_KINDS: &[&str] = &["namespace", "module"];

/// Whether an entry of this kind may be an `outline` `read_symbol` hop target.
fn is_hop_target_kind(kind: &str) -> bool {
    !CONTAINER_WRAPPER_KINDS.contains(&kind)
}

/// True when `qualname` resolves exactly -- no fuzzy fallback -- and every
/// indexed symbol bearing it is declared in `path`.
fn qualname_resolves_only_in(
    db: &crate::db::Db,
    qualname: &str,
    path: &str,
    graph_version: i64,
) -> Result<bool> {
    let found: Vec<Symbol> = db
        .get_symbols_by_qualname(qualname, graph_version)?
        .into_iter()
        .filter(|s| !s.is_external())
        .collect();
    Ok(!found.is_empty() && found.iter().all(|s| s.file_path == path))
}

/// `outline`'s next_hops (#236). A `read_symbol` hop is emitted only for an
/// indexed, non-wrapper entry whose qualname resolves exactly to a symbol in
/// the outlined file. Otherwise (Markdown, whose entries are headings and not
/// symbols; or a file holding only namespace/module wrappers) the hop reads a
/// line range of that same file through `gather_context`'s file seed.
fn outline_next_hops(
    db: &crate::db::Db,
    path: &str,
    entries: &[OutlineEntry],
    total_lines: i64,
    markdown: bool,
) -> Result<Vec<Value>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    if !markdown {
        let graph_version = db.current_graph_version()?;
        for entry in entries.iter().filter(|e| is_hop_target_kind(&e.kind)) {
            if qualname_resolves_only_in(db, &entry.qualname, path, graph_version)? {
                return Ok(vec![json!({
                    "method": "read_symbol",
                    "params": {"qualname": entry.qualname},
                    "description": "read_symbol fetches this entry's exact source; it is the first non-namespace/module entry that resolves uniquely within this file",
                })]);
            }
        }
    }
    let (start_line, end_line) = if markdown {
        // From line 1 so any preamble before the first heading is included.
        (1, entries[0].end_line)
    } else {
        (1, total_lines.max(1))
    };
    Ok(vec![json!({
        "method": "gather_context",
        "params": {"seeds": [{
            "type": "file",
            "path": path,
            "start_line": start_line,
            "end_line": end_line,
        }]},
        "description": "read a line range of this file (its entries have no symbol to read)",
    })])
}

/// `outline` (#95): a compact, no-bodies skeleton of an indexed file's symbols
/// in source order -- kind, qualname, signature, line range, nesting parent,
/// first doc line. Answers "what's in this file?" for a fraction of the
/// file's size; the response is still subject to the default response byte
/// cap like any other method (see `handle_method`'s `effective_max`).
pub(super) fn handle_outline(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OutlineParams = super::parse_params("outline", params)?;
    let repo_root = indexer.repo_root().clone();
    let validated =
        super::validate::validate_repo_path("outline", indexer.db(), &repo_root, &params.path)?;
    let path = validated.path;
    let kinds_filter: Option<HashSet<String>> =
        params.kinds.map(|kinds| kinds.into_iter().collect());
    let max_depth = params.max_depth;

    let full_path = repo_root.join(path);

    let (language, total_lines, entries) = if let Some(file_record) = validated.file {
        let graph_version = indexer.db().current_graph_version()?;
        indexed_outline(
            indexer.db(),
            file_record,
            &full_path,
            path,
            graph_version,
            kinds_filter.as_ref(),
            max_depth,
        )?
    } else {
        markdown_outline(&full_path, path, kinds_filter.as_ref(), max_depth)?
    };

    let next_hops = outline_next_hops(
        indexer.db(),
        path,
        &entries,
        total_lines,
        is_markdown_path(path),
    )?;

    let result = OutlineResult {
        path: path.to_string(),
        language,
        total_lines,
        entries,
        next_hops,
    };
    Ok(serde_json::to_value(result)?)
}

/// Prefixes each line of `text` with its real file line number (1-based, starting
/// at `start_line`), so a caller's follow-up edits/references use correct locations.
fn number_source_lines(text: &str, start_line: i64) -> String {
    text.lines()
        .enumerate()
        .map(|(i, line)| format!("{}: {}", start_line + i as i64, line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Direct children (one nesting level) of `container`, via the same
/// `CONTAINS`-edge nesting `outline` uses (`symbol_outline_entries`) rather than
/// re-deriving containment from byte ranges, which is wrong for Rust `impl`
/// blocks (see that function's doc comment): a method's byte span sits inside
/// its `impl` block, not inside the struct symbol it's qualname-nested under.
///
/// Note: the whole-file `module` root symbol is hidden by
/// `symbol_outline_entries` (its direct children come back as top-level,
/// parentless entries instead of nested under it), so skeletonizing that root
/// symbol itself currently returns no children here -- `build_symbol_entry`
/// falls back to a normal read in that case.
///
/// Fetches each child by id one at a time (`get_symbol_by_id` in the loop
/// below) rather than in a single batch: `Db` has no batch-by-ids lookup
/// (only by file or by qualname), and adding one just for this N+1 wasn't
/// judged worth it here.
fn container_children(
    db: &crate::db::Db,
    container: &Symbol,
    graph_version: i64,
) -> Result<Vec<OutlineEntry>> {
    let touching = db.edges_for_symbol(container.id, None, graph_version)?;

    // Mirror `symbol_outline_entries`'s file-root-module rule (see
    // `is_file_root_module`'s doc comment): skeletonizing the root itself has
    // no visible children.
    let has_incoming_contains = touching
        .iter()
        .any(|e| e.kind == "CONTAINS" && e.target_symbol_id == Some(container.id));
    if is_file_root_module(&container.kind, has_incoming_contains) {
        return Ok(Vec::new());
    }

    let mut seen_children: HashSet<i64> = HashSet::new();
    let mut entries = Vec::new();
    for edge in &touching {
        if edge.kind != "CONTAINS" || edge.source_symbol_id != Some(container.id) {
            continue;
        }
        let Some(child_id) = edge.target_symbol_id else {
            continue;
        };
        if !seen_children.insert(child_id) {
            continue;
        }
        let Some(child) = db.get_symbol_by_id(child_id)? else {
            continue;
        };
        // Defensive: external stubs are attributed to a synthetic `ext:`
        // location, not a real repo file, so this shouldn't normally match --
        // excluded anyway to mirror `symbol_outline_entries`'s own exclusion.
        if child.is_external() {
            continue;
        }
        let doc = child.docstring.as_deref().and_then(first_doc_line);
        entries.push(OutlineEntry::from_symbol(
            &child,
            Some(container.qualname.clone()),
            doc,
        ));
    }
    // `edges_for_symbol` orders by edge id, not source position -- sort by
    // start_line to match `symbol_outline_entries`' source-order output.
    entries.sort_by_key(|e| e.start_line);
    Ok(entries)
}

/// Bound on `read_symbol` next_hops emitted for a skeleton container's children,
/// so a large class/module doesn't blow the response on hop suggestions alone.
const MAX_SKELETON_CHILD_HOPS: usize = 25;

/// Builds the non-skeleton `read_symbol` result: exact source from disk (by
/// stored byte span, no re-parse), optionally widened by `context_lines` of
/// surrounding lines (clamped at file bounds), line-numbered against the real
/// file. `stale` is threaded in rather than recomputed so single and
/// multi-symbol callers share one staleness check per symbol. `content` is
/// the file's already-loaded text (see `build_symbol_entry`), so this never
/// re-reads the file that was just read to compute `stale`.
fn build_source_response(
    symbol: &Symbol,
    content: &str,
    context_lines: usize,
    stale: bool,
) -> Result<ReadSymbolEntry> {
    let stale_span_err = || {
        anyhow::anyhow!(
            "symbol span no longer valid in '{}' (file changed extensively); run 'reindex' to refresh the index",
            symbol.file_path
        )
    };

    // If only whitespace precedes `start_byte` on its own line (e.g. an
    // indented method), widen the slice to that line's true start so every
    // returned line -- including the first -- matches the real file line,
    // rather than a dedented fragment starting mid-line. `start` must land on
    // a real char boundary before it's used to index `content` at all: a
    // stale `start_byte` (file changed on disk since indexing, without a
    // reindex) can point into the middle of a multibyte character, and
    // indexing a `&str` at a non-boundary offset panics rather than
    // returning an error -- so that case falls back to the same
    // "symbol span no longer valid" error as an out-of-range slice below.
    let start = (symbol.start_byte.max(0) as usize).min(content.len());
    if !content.is_char_boundary(start) {
        return Err(stale_span_err());
    }
    let effective_start_byte = {
        let line_start = content[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
        if content[line_start..start]
            .chars()
            .all(|c| c == ' ' || c == '\t')
        {
            line_start as i64
        } else {
            symbol.start_byte
        }
    };

    let raw_source = crate::util::slice_bytes(content, effective_start_byte, symbol.end_byte)
        .ok_or_else(stale_span_err)?;

    let (source_text, numbering_start_line) = if context_lines > 0 {
        let all_lines: Vec<&str> = content.lines().collect();
        let total_lines = all_lines.len() as i64;
        let ctx_lines = context_lines as i64;
        let ctx_start = (symbol.start_line - ctx_lines).max(1);
        let ctx_end = (symbol.end_line + ctx_lines).min(total_lines);

        // The symbol's own body stays the exact byte slice above; only the
        // surrounding context is pulled from line-splitting, so a follow-up
        // edit against `start_line`/`end_line` still targets exactly what was
        // indexed. All slice indices are clamped to `all_lines`' *actual*
        // bounds: the indexed start_line/end_line can be stale (file changed
        // on disk since indexing) and outlive the file's current line count,
        // so an unclamped slice here would panic instead of just yielding
        // less context than requested.
        let mut combined = String::new();
        let mut numbering_start_line = symbol.start_line;
        if ctx_start < symbol.start_line {
            let start_idx = ((ctx_start - 1).max(0) as usize).min(all_lines.len());
            let end_idx = ((symbol.start_line - 1).max(0) as usize).min(all_lines.len());
            if start_idx < end_idx {
                let before = &all_lines[start_idx..end_idx];
                combined.push_str(&before.join("\n"));
                combined.push('\n');
                numbering_start_line = (start_idx + 1) as i64;
            }
        }
        combined.push_str(&raw_source);
        if ctx_end > symbol.end_line {
            let start_idx = (symbol.end_line.max(0) as usize).min(all_lines.len());
            let end_idx = (ctx_end.max(0) as usize).min(all_lines.len());
            if start_idx < end_idx {
                let after = &all_lines[start_idx..end_idx];
                combined.push('\n');
                combined.push_str(&after.join("\n"));
            }
        }
        (combined, numbering_start_line)
    } else {
        (raw_source, symbol.start_line)
    };

    let source = number_source_lines(&source_text, numbering_start_line);

    Ok(ReadSymbolEntry::source(symbol, stale, source))
}

/// Builds one `read_symbol` result for an already-resolved, already-validated
/// (non-external) symbol: staleness check, then either the full source
/// (optionally widened by `context_lines`) or, for `skeleton: true` on a
/// container, its children's signatures/line ranges plus a bounded
/// `read_symbol` next_hop per child. Shared by the single (`qualname`/`query`)
/// and multi (`qualnames`) selector paths so both behave identically.
fn build_symbol_entry(
    indexer: &Indexer,
    symbol: &Symbol,
    skeleton: bool,
    context_lines: usize,
    graph_version: i64,
) -> Result<ReadSymbolEntry> {
    let repo_root = indexer.repo_root().clone();
    let full_path = repo_root.join(&symbol.file_path);
    if !full_path.is_file() {
        anyhow::bail!(
            "file '{}' is missing from disk; run 'reindex' to refresh the index",
            symbol.file_path
        );
    }
    // Read once and hash the loaded content (rather than calling
    // scan::scan_path, which would re-read the file from disk just to hash
    // it) -- build_source_response below reuses this same content instead of
    // reading the file a second time.
    let content = crate::util::read_to_string(&full_path)?;
    let hash = scan::hash_bytes(content.as_bytes());
    let indexed_hash = indexer
        .db()
        .get_file_by_path(&symbol.file_path)?
        .map(|f| f.hash);
    let stale = indexed_hash.is_some_and(|indexed| indexed != hash);

    let mut next_hops: Vec<Value> = Vec::new();
    let mut entry = if skeleton {
        let children = container_children(indexer.db(), symbol, graph_version)?;
        if children.is_empty() {
            // Not a container (or has none in this file) -- skeleton has
            // nothing to skeletonize, so fall back to a normal read rather
            // than returning an empty, useless response.
            build_source_response(symbol, &content, context_lines, stale)?
        } else {
            for child in children.iter().take(MAX_SKELETON_CHILD_HOPS) {
                next_hops.push(json!({
                    "method": "read_symbol",
                    "params": {"qualname": child.qualname},
                    "description": format!("read_symbol fetches the full source of '{}'", child.qualname),
                }));
            }
            ReadSymbolEntry::skeleton(symbol, stale, children)
        }
    } else {
        build_source_response(symbol, &content, context_lines, stale)?
    };

    if stale {
        next_hops.push(json!({
            "method": "reindex",
            "params": {},
            "description": format!(
                "'{}' changed on disk since indexing; reindex to refresh symbol spans",
                symbol.file_path
            ),
        }));
    }
    entry.next_hops = next_hops;
    Ok(entry)
}

/// The overload set behind `qn`: every non-external symbol sharing that
/// exact qualname, when there are several and they are all C# or all Rust
/// declarations. That covers C# method overloads plus the same-qualname
/// declarations issue #212 keeps apart (generic-arity types, `cfg` twins,
/// inherent and trait-impl methods): a read must list each one with its
/// own span, never silently pick one. Empty otherwise -- other languages'
/// duplicate qualnames (Python property setters, `@overload`, TS overload
/// signatures) keep the single-symbol shape.
pub(super) fn overloaded_symbols(
    indexer: &Indexer,
    qn: &str,
    graph_version: i64,
) -> Result<Vec<Symbol>> {
    let overloads: Vec<Symbol> = indexer
        .db()
        .get_symbols_by_qualname(qn, graph_version)?
        .into_iter()
        .filter(|s| !s.is_external())
        .collect();
    let all_csharp_methods = overloads
        .iter()
        .all(|s| s.kind == "method" && s.file_path.ends_with(".cs"));
    // Twins are declarations of one file; a namespace or partial type spread
    // over several files is not a twin set.
    let one_file = overloads
        .iter()
        .all(|s| s.file_path == overloads[0].file_path);
    let twins_in_file = one_file
        && overloads
            .iter()
            .all(|s| s.file_path.ends_with(".cs") || s.file_path.ends_with(".rs"));
    Ok(
        if overloads.len() > 1 && (all_csharp_methods || twins_in_file) {
            overloads
        } else {
            Vec::new()
        },
    )
}

/// The `overloaded` response header shared by `read_symbol` and
/// `explain_symbol`, so the two methods cannot drift: `entries` is each
/// method's per-overload payload.
pub(super) fn overload_set_response(qn: &str, count: usize, entries: Value) -> Value {
    json!({
        "overloaded": true,
        "qualname": qn,
        "count": count,
        "overloads": entries,
    })
}

/// The `overloaded` response object shared by the `qualname`, `qualnames` and
/// `query` selectors: every overload's entry, bounded by `max_bytes` (the
/// first entry is always kept).
fn overload_response(
    indexer: &Indexer,
    qn: &str,
    overloads: &[Symbol],
    skeleton: bool,
    context_lines: usize,
    max_bytes: usize,
    graph_version: i64,
) -> Result<Value> {
    let (entries, omitted) = overload_entries(
        indexer,
        overloads,
        skeleton,
        context_lines,
        max_bytes,
        graph_version,
    )?;
    let mut response = overload_set_response(qn, overloads.len(), json!(entries));
    response["omitted"] = json!(omitted);
    Ok(response)
}

/// One `ReadSymbolEntry` per overload (the same entry a single symbol gets),
/// bounded by `max_bytes` with the first always kept; the overloads that
/// didn't fit come back as `qualname signature` strings.
fn overload_entries(
    indexer: &Indexer,
    overloads: &[Symbol],
    skeleton: bool,
    context_lines: usize,
    max_bytes: usize,
    graph_version: i64,
) -> Result<(Vec<ReadSymbolEntry>, Vec<String>)> {
    let mut entries: Vec<ReadSymbolEntry> = Vec::new();
    let mut omitted: Vec<String> = Vec::new();
    let mut used = 0usize;
    for symbol in overloads {
        let entry = build_symbol_entry(indexer, symbol, skeleton, context_lines, graph_version)?;
        let len = serde_json::to_string(&entry).map(|s| s.len()).unwrap_or(0);
        if !entries.is_empty() && used + len > max_bytes {
            // Twins can share qualname and signature: the line tells them apart.
            omitted.push(format!(
                "{}{}@L{}",
                symbol.qualname,
                symbol.signature.as_deref().unwrap_or_default(),
                symbol.start_line
            ));
            continue;
        }
        used += len;
        entries.push(entry);
    }
    Ok((entries, omitted))
}

/// Assembles the `read_symbol` `qualnames` response object from its four
/// parts, gating `not_found`/`errors` on non-empty the same way both the
/// greedy pass and the exact-trim pass below need to -- one place so they
/// can't drift.
fn build_multi_response(
    symbols: &[Value],
    omitted: &[String],
    not_found: &[String],
    errors: &[Value],
) -> Value {
    let mut response = json!({"symbols": symbols, "omitted": omitted});
    if !not_found.is_empty() {
        response["not_found"] = json!(not_found);
    }
    if !errors.is_empty() {
        response["errors"] = json!(errors);
    }
    response
}

/// Multi-symbol `read_symbol`: resolves each qualname in request order by
/// exact match (batch reads name symbols precisely -- unlike the single
/// `query` selector, there's no fuzzy fallback here). A qualname that doesn't
/// resolve to a real, non-external symbol is listed in `not_found`; one that
/// resolves but fails to read (e.g. its file went missing/stale-beyond-repair)
/// is listed in `errors`; either way the batch keeps going. `max_bytes` is a
/// hard budget on the whole response, `not_found`/`errors` included -- a
/// symbol that would push it over budget, and every symbol after it, is
/// omitted whole (never cut mid-symbol) and listed by qualname in `omitted`.
///
/// Two passes keep this both simple and exact rather than re-deriving
/// `serde_json`'s own byte-counting rules by hand:
/// 1. A greedy O(n) pass tracks a running sum of just the accepted symbols'
///    own serialized bytes, stopping (and routing the rest straight to
///    `omitted` without paying for a DB/file lookup) once that alone would
///    exceed `max_bytes`. This ignores the envelope and `not_found`/`errors`,
///    so it's only an approximation.
/// 2. The real response is serialized once; while it's still over budget,
///    the last accepted symbol is moved to `omitted` and it's re-serialized.
///    This is the exact check -- it's what guarantees the final response
///    fits, correcting anything the approximation in pass 1 missed.
fn handle_read_symbol_multi(
    indexer: &Indexer,
    qualnames: &[String],
    skeleton: bool,
    context_lines: usize,
    max_bytes: usize,
    graph_version: i64,
) -> Result<Value> {
    let mut symbols: Vec<Value> = Vec::new();
    let mut omitted: Vec<String> = Vec::new();
    let mut not_found: Vec<String> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();

    let mut running_symbol_bytes = 0usize;
    let mut budget_exhausted = false;

    for qn in qualnames {
        if budget_exhausted {
            omitted.push(qn.clone());
            continue;
        }
        // An overloaded C# qualname contributes one entry per overload.
        let overloads = overloaded_symbols(indexer, qn, graph_version)?;
        let built: Result<Vec<Value>> = if overloads.is_empty() {
            let found = indexer.db().get_symbol_by_qualname(qn, graph_version)?;
            let symbol = match found {
                Some(s) if !s.is_external() => s,
                _ => {
                    not_found.push(qn.clone());
                    continue;
                }
            };
            // A single qualname's file being missing/stale-beyond-repair
            // shouldn't abort the whole batch -- record it and keep going so
            // the rest of the request still resolves.
            build_symbol_entry(indexer, &symbol, skeleton, context_lines, graph_version)
                .and_then(|e| Ok(vec![serde_json::to_value(e)?]))
        } else {
            overload_entries(
                indexer,
                &overloads,
                skeleton,
                context_lines,
                max_bytes,
                graph_version,
            )
            .and_then(|(entries, _)| {
                entries
                    .into_iter()
                    .map(|e| Ok(serde_json::to_value(e)?))
                    .collect()
            })
        };
        let new_entries = match built {
            Ok(entries) => entries,
            Err(err) => {
                errors.push(json!({"qualname": qn, "error": err.to_string()}));
                continue;
            }
        };
        let entry_len = new_entries
            .iter()
            .map(|e| {
                serde_json::to_string(e)
                    .map(|s| s.len())
                    .unwrap_or(usize::MAX)
            })
            .fold(0usize, usize::saturating_add);
        if running_symbol_bytes.saturating_add(entry_len) > max_bytes {
            omitted.push(qn.clone());
            budget_exhausted = true;
            continue;
        }
        running_symbol_bytes += entry_len;
        symbols.extend(new_entries);
    }

    let mut response = build_multi_response(&symbols, &omitted, &not_found, &errors);
    while !symbols.is_empty()
        && serde_json::to_string(&response)
            .map(|s| s.len())
            .unwrap_or(0)
            > max_bytes
    {
        let removed = symbols.pop().expect("just checked symbols is non-empty");
        omitted.insert(
            0,
            removed["qualname"].as_str().unwrap_or_default().to_string(),
        );
        response = build_multi_response(&symbols, &omitted, &not_found, &errors);
    }

    Ok(response)
}

/// `read_symbol`: fetch one or more symbols' exact source from disk by their
/// stored byte spans, no re-parse (#96, #98). `qualname`/`query` resolve and
/// return one symbol, with the same fuzzy fallback and "did you mean"
/// ambiguity handling as `explain_symbol`/`trace_flow`. `qualnames` reads
/// several exact qualnames in one call under a shared byte budget (see
/// `handle_read_symbol_multi`). `skeleton` and `context_lines` apply to
/// either mode (see `build_symbol_entry`).
pub(super) fn handle_read_symbol(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let raw_params = params.clone();
    let params: ReadSymbolParams = super::parse_params("read_symbol", params)?;
    let selectors_given = [
        params.qualname.is_some(),
        params.query.is_some(),
        params.qualnames.is_some(),
    ]
    .into_iter()
    .filter(|given| *given)
    .count();
    if selectors_given != 1 {
        anyhow::bail!(
            "read_symbol requires exactly one of 'qualname', 'query', or 'qualnames' ({} provided)",
            selectors_given
        );
    }

    let graph_version = indexer.db().current_graph_version()?;
    let skeleton = params.skeleton.unwrap_or(false);
    let context_lines = params.context_lines.unwrap_or(0);

    if let Some(qualnames) = params.qualnames {
        let max_bytes = params
            .max_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
            .min(200_000);
        return handle_read_symbol_multi(
            indexer,
            &qualnames,
            skeleton,
            context_lines,
            max_bytes,
            graph_version,
        );
    }

    // An exact qualname shared by several symbols (overloads) returns all
    // of them, rather than silently whichever the lookup found first.
    let max_bytes = params
        .max_bytes
        .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
        .min(200_000);
    if let Some(qn) = &params.qualname {
        let overloads = overloaded_symbols(indexer, qn, graph_version)?;
        if !overloads.is_empty() {
            return overload_response(
                indexer,
                qn,
                &overloads,
                skeleton,
                context_lines,
                max_bytes,
                graph_version,
            );
        }
    }

    // Resolves the same way `explain_symbol` does: an exact qualname hit
    // short-circuits, otherwise falls back to the fuzzy query path -- but via
    // `resolve_symbol_with_candidates` rather than `resolve_symbol`, so a tie
    // at the exact-name-match tier (`find_symbols`'s own top ranking
    // criterion) comes back as `Ambiguous` instead of a silent pick. Unlike
    // `explain_symbol`, which takes the best match, `read_symbol` returns one
    // symbol's exact source, so guessing between two equally-ranked
    // candidates is costly. Exactly one of qualname/query is `Some` here --
    // `qualnames` already returned above, and exactly one selector was
    // validated at the top of this function.
    let sym_ref = match &params.qualname {
        Some(qn) => crate::resolve::SymbolRef::Qualname(qn.clone()),
        None => crate::resolve::SymbolRef::Query(params.query.clone().unwrap_or_default()),
    };
    let resolution = match crate::resolve::resolve_symbol_with_candidates(
        indexer.db(),
        sym_ref,
        None,
        graph_version,
    ) {
        Ok(r) => r,
        Err(e) => {
            return match crate::resolve::recovery_from_error(
                indexer.db(),
                &e,
                graph_version,
                "read_symbol",
                &raw_params,
            ) {
                Some(payload) => Ok(payload),
                None => Err(e),
            };
        }
    };

    let resolved = match resolution {
        crate::resolve::QueryResolution::Ambiguous(candidates) => {
            let query_text = params
                .qualname
                .as_deref()
                .or(params.query.as_deref())
                .unwrap_or_default();
            let candidates_json: Vec<Value> = candidates
                .iter()
                .map(|s| json!({"qualname": s.qualname, "kind": s.kind, "path": s.file_path}))
                .collect();
            return Ok(json!({
                "ambiguous": true,
                "query": query_text,
                "candidates": candidates_json,
            }));
        }
        crate::resolve::QueryResolution::Found(resolved) => resolved,
    };

    // Issue #235: disclose how the symbol was resolved (fuzzy fallback etc.).
    let mut response = read_resolved_symbol(
        indexer,
        &resolved.symbol,
        skeleton,
        context_lines,
        max_bytes,
        graph_version,
    )?;
    resolved.annotate(&mut response);
    Ok(response)
}

/// Builds the `read_symbol` response for one already-resolved symbol: the
/// external-stub bail, C# overload fan-out, the single entry, and the
/// over-budget `omitted` header.
fn read_resolved_symbol(
    indexer: &Indexer,
    symbol: &Symbol,
    skeleton: bool,
    context_lines: usize,
    max_bytes: usize,
    graph_version: i64,
) -> Result<Value> {
    if symbol.is_external() {
        anyhow::bail!(
            "symbol not found: '{}' is an external stub with no indexed source",
            symbol.qualname
        );
    }

    // A `query` that lands on an overloaded C# method reads all overloads.
    let overloads = overloaded_symbols(indexer, &symbol.qualname, graph_version)?;
    if !overloads.is_empty() {
        return overload_response(
            indexer,
            &symbol.qualname,
            &overloads,
            skeleton,
            context_lines,
            max_bytes,
            graph_version,
        );
    }

    let entry = build_symbol_entry(indexer, symbol, skeleton, context_lines, graph_version)?;

    // A single symbol's response is otherwise uncapped (the outer generic
    // response-size cap can't safely shrink a plain object with no array
    // field to slice -- see `format::truncate_response`, and `read_symbol` is
    // exempt from it anyway so it can honour its own `max_bytes`). Rather
    // than ever return a source cut mid-body, an over-budget response is
    // replaced by just its header fields plus `omitted: true`.
    let entry_size = serde_json::to_string(&entry).map(|s| s.len()).unwrap_or(0);
    if entry_size > max_bytes {
        let stale = entry.stale;
        let header = ReadSymbolEntry {
            next_hops: vec![
                json!({
                    "method": "read_symbol",
                    "params": {"qualname": symbol.qualname.clone(), "skeleton": true},
                    "description": format!(
                        "'{}' is too large to read in full ({} bytes > {} budget) -- read_symbol with skeleton:true returns just its children's signatures (containers only)",
                        symbol.qualname, entry_size, max_bytes
                    ),
                }),
                json!({
                    "method": "outline",
                    "params": {"path": symbol.file_path.clone()},
                    "description": "Outline the file to pick a narrower symbol to read",
                }),
            ],
            ..ReadSymbolEntry::omitted_header(symbol, stale, entry_size)
        };
        return Ok(serde_json::to_value(header)?);
    }

    Ok(serde_json::to_value(entry)?)
}

#[cfg(test)]
mod parse_markdown_headings_tests {
    use super::*;

    #[test]
    fn hash_lines_inside_fenced_code_blocks_are_not_headings() {
        let md = "# Title\n\n```\n# not a heading\n```\n\n## Real Heading\n";
        let headings = parse_markdown_headings(md);
        let texts: Vec<&str> = headings.iter().map(|h| h.text.as_str()).collect();
        assert_eq!(texts, vec!["Title", "Real Heading"]);
    }

    #[test]
    fn trailing_hash_not_preceded_by_space_is_kept_as_real_text() {
        let headings = parse_markdown_headings("## F#\n");
        assert_eq!(headings.len(), 1);
        assert_eq!(headings[0].text, "F#");
        assert_eq!(headings[0].level, 2);
    }

    #[test]
    fn trailing_hash_run_preceded_by_space_is_stripped() {
        let headings = parse_markdown_headings("## Closed Heading ##\n");
        assert_eq!(headings.len(), 1);
        assert_eq!(headings[0].text, "Closed Heading");
    }

    #[test]
    fn skipped_heading_levels_are_recorded_verbatim() {
        // parse_markdown_headings is flat (level/text/start_line only) --
        // nesting a h1-straight-to-h3 skip under the nearest actual ancestor
        // is markdown_outline_entries' job, not this function's; this just
        // proves the skipped level itself is preserved, not coerced.
        let headings = parse_markdown_headings("# One\n### Three\n");
        assert_eq!(headings.len(), 2);
        assert_eq!(headings[0].level, 1);
        assert_eq!(headings[0].text, "One");
        assert_eq!(headings[1].level, 3);
        assert_eq!(headings[1].text, "Three");
    }
}
