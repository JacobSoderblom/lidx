//! Reading-feature RPC handlers: `outline` and `read_symbol`.
//! Moved out of handlers.rs as its own feature module (issue #93).
//!
//! Wired from `rpc/mod.rs` the same way `handlers` is: a private sibling
//! module reached via `reading::handle_outline`/`reading::handle_read_symbol`
//! in `handle_method`'s dispatch table.

use super::*;

/// Extensions treated as Markdown for `outline`. Markdown isn't a scanned
/// language (no entry in `indexer::scan`'s `LANGUAGE_SPECS`, so `.md` files
/// never get a `files`/`symbols` row) -- `search` still finds them via
/// ripgrep, so `outline` reads Markdown straight off disk and parses ATX
/// headings, rather than requiring a DB row like every other language here.
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
        .filter(|s| s.kind != "external" && !s.qualname.starts_with("ext:"))
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
        .filter(|s| s.kind == "module" && !parent_of.contains_key(&s.id))
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
        entries.push(OutlineEntry {
            kind: symbol.kind.clone(),
            name: symbol.name.clone(),
            qualname: symbol.qualname.clone(),
            signature: symbol.signature.clone(),
            start_line: symbol.start_line,
            end_line: symbol.end_line,
            parent,
            doc,
        });
    }
    Ok(entries)
}

/// Rejects a `path` that's absolute or that escapes the repo root via a `..`
/// component -- applies to every `outline`/`read_symbol` path, not just
/// Markdown (whose branch reads straight off disk with no DB row to bound
/// it; see `is_markdown_path`'s doc comment). A relative path never needs
/// `..` to name a file inside the repo, so any `..` component is rejected
/// outright rather than resolved and checked against the repo root.
fn reject_path_escape(path: &str) -> Result<()> {
    let candidate = std::path::Path::new(path);
    if candidate.is_absolute() {
        anyhow::bail!(
            "path '{}' must be relative to the repo root, not absolute",
            path
        );
    }
    if candidate
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        anyhow::bail!("path '{}' escapes the repo root (contains '..')", path);
    }
    Ok(())
}

/// `outline` (#95): a compact, no-bodies skeleton of an indexed file's symbols
/// in source order -- kind, qualname, signature, line range, nesting parent,
/// first doc line. Answers "what's in this file?" for a fraction of the
/// file's size; the response is still subject to the default response byte
/// cap like any other method (see `handle_method`'s `effective_max`).
pub(super) fn handle_outline(indexer: &mut Indexer, params: Value) -> Result<Value> {
    let params: OutlineParams = serde_json::from_value(params)?;
    let path = params.path.trim();
    if path.is_empty() {
        anyhow::bail!("outline requires a non-empty 'path'");
    }
    reject_path_escape(path)?;
    let kinds_filter: Option<HashSet<String>> =
        params.kinds.map(|kinds| kinds.into_iter().collect());
    let max_depth = params.max_depth;

    let repo_root = indexer.repo_root().clone();
    let full_path = repo_root.join(path);
    let markdown = is_markdown_path(path);

    // Markdown has no `files`/`symbols` row to check (see `is_markdown_path`), so
    // disk presence is its "indexed" check; every other language must already be
    // in the DB, which is checked before touching disk at all.
    let language = if markdown {
        "markdown".to_string()
    } else {
        let file_record = indexer.db().get_file_by_path(path)?;
        let Some(file_record) = file_record else {
            anyhow::bail!(
                "path '{}' is not indexed -- fall back to Read, or run 'reindex' if it should be tracked",
                path
            );
        };
        file_record.language
    };

    let content = crate::util::read_to_string(&full_path).map_err(|_| {
        if markdown {
            anyhow::anyhow!(
                "path '{}' is not indexed -- fall back to Read for this file",
                path
            )
        } else {
            anyhow::anyhow!(
                "file '{}' is missing from disk; run 'reindex' to refresh the index",
                path
            )
        }
    })?;
    let total_lines = crate::indexer::tree_helpers::line_count(&content);

    let entries = if markdown {
        markdown_outline_entries(&content, total_lines, kinds_filter.as_ref(), max_depth)
    } else {
        let graph_version = indexer.db().current_graph_version()?;
        symbol_outline_entries(
            indexer.db(),
            path,
            graph_version,
            kinds_filter.as_ref(),
            max_depth,
        )?
    };

    let next_hops = match entries.first() {
        Some(first) => vec![json!({
            "method": "read_symbol",
            "params": {"qualname": first.qualname},
            "description": "read_symbol accepts any entry's qualname above to fetch its exact source",
        })],
        None => vec![],
    };

    let result = OutlineResult {
        path: path.to_string(),
        language,
        total_lines,
        entries,
        next_hops,
    };
    Ok(serde_json::to_value(result)?)
}

/// Resolution outcome for a `read_symbol` `qualname`/`query` selector.
enum ReadTarget {
    Found(Box<Symbol>),
    /// Multiple candidates tied for best match -- returning one would be a guess.
    Ambiguous(Vec<Symbol>),
}

/// Resolves a `read_symbol` selector (either `qualname` or `query` text) the same
/// way `explain_symbol` does: an exact qualname hit short-circuits, otherwise
/// falls back to the fuzzy query path -- but via
/// `resolve::resolve_symbol_with_candidates` rather than `resolve::resolve_symbol`,
/// so a tie at the exact-name-match tier (`find_symbols`'s own top ranking
/// criterion) comes back as `ReadTarget::Ambiguous` instead of a silent pick.
/// Unlike `explain_symbol`, which takes the best match, `read_symbol` returns
/// one symbol's exact source, so guessing between two equally-ranked candidates
/// is costly. When there is no tie, resolution is the same `resolve::resolve_symbol`
/// would produce -- both share the same candidates lookup rather than each
/// running `find_symbols` on its own, so a non-ambiguous `read_symbol` query
/// always resolves to the same symbol `explain_symbol` would, at the cost of
/// one shared query rather than two.
fn resolve_read_target(
    db: &crate::db::Db,
    qualname: Option<&str>,
    query: Option<&str>,
    graph_version: i64,
) -> Result<ReadTarget> {
    let Some(text) = qualname.or(query) else {
        anyhow::bail!("resolve_read_target requires a qualname or query");
    };
    let sym_ref = match qualname {
        Some(qn) => crate::resolve::SymbolRef::Qualname(qn.to_string()),
        None => crate::resolve::SymbolRef::Query(text.to_string()),
    };
    match crate::resolve::resolve_symbol_with_candidates(db, sym_ref, None, graph_version)? {
        crate::resolve::QueryResolution::Found(symbol) => Ok(ReadTarget::Found(symbol)),
        crate::resolve::QueryResolution::Ambiguous(candidates) => {
            Ok(ReadTarget::Ambiguous(candidates))
        }
    }
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
fn container_children(
    db: &crate::db::Db,
    container: &Symbol,
    graph_version: i64,
) -> Result<Vec<OutlineEntry>> {
    let touching = db.edges_for_symbol(container.id, None, graph_version)?;

    // The whole-file `module` root has no incoming `CONTAINS` edge of its own
    // within its file -- `symbol_outline_entries` hides it as a container, so
    // its direct children come back parentless instead of nested under it
    // (see this function's doc comment). Mirror that here: skeletonizing the
    // root itself has no visible children.
    let is_root_module = container.kind == "module"
        && !touching
            .iter()
            .any(|e| e.kind == "CONTAINS" && e.target_symbol_id == Some(container.id));
    if is_root_module {
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
        if child.kind == "external" || child.qualname.starts_with("ext:") {
            continue;
        }
        let doc = child.docstring.as_deref().and_then(first_doc_line);
        entries.push(OutlineEntry {
            kind: child.kind,
            name: child.name,
            qualname: child.qualname,
            signature: child.signature,
            start_line: child.start_line,
            end_line: child.end_line,
            parent: Some(container.qualname.clone()),
            doc,
        });
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
/// multi-symbol callers share one staleness check per symbol.
fn build_source_response(
    indexer: &Indexer,
    symbol: &Symbol,
    context_lines: usize,
    stale: bool,
) -> Result<Value> {
    let repo_root = indexer.repo_root().clone();
    let full_path = repo_root.join(&symbol.file_path);
    let content = crate::util::read_to_string(&full_path)?;

    // If only whitespace precedes `start_byte` on its own line (e.g. an
    // indented method), widen the slice to that line's true start so every
    // returned line -- including the first -- matches the real file line,
    // rather than a dedented fragment starting mid-line.
    let effective_start_byte = {
        let start = (symbol.start_byte.max(0) as usize).min(content.len());
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

    let raw_source = crate::util::slice_bytes(&content, effective_start_byte, symbol.end_byte)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "symbol span no longer valid in '{}' (file changed extensively); run 'reindex' to refresh the index",
                symbol.file_path
            )
        })?;

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

    Ok(json!({
        "qualname": symbol.qualname,
        "kind": symbol.kind,
        "path": symbol.file_path,
        "start_line": symbol.start_line,
        "end_line": symbol.end_line,
        "stale": stale,
        "source": source,
    }))
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
) -> Result<Value> {
    let repo_root = indexer.repo_root().clone();
    let full_path = repo_root.join(&symbol.file_path);
    let scanned = scan::scan_path(&repo_root, &full_path)?;
    let Some(scanned) = scanned else {
        anyhow::bail!(
            "file '{}' is missing from disk; run 'reindex' to refresh the index",
            symbol.file_path
        );
    };
    let indexed_hash = indexer
        .db()
        .get_file_by_path(&symbol.file_path)?
        .map(|f| f.hash);
    let stale = indexed_hash.is_some_and(|hash| hash != scanned.hash);

    let mut next_hops: Vec<Value> = Vec::new();
    let mut response = if skeleton {
        let children = container_children(indexer.db(), symbol, graph_version)?;
        if children.is_empty() {
            // Not a container (or has none in this file) -- skeleton has
            // nothing to skeletonize, so fall back to a normal read rather
            // than returning an empty, useless response.
            build_source_response(indexer, symbol, context_lines, stale)?
        } else {
            for child in children.iter().take(MAX_SKELETON_CHILD_HOPS) {
                next_hops.push(json!({
                    "method": "read_symbol",
                    "params": {"qualname": child.qualname},
                    "description": format!("read_symbol fetches the full source of '{}'", child.qualname),
                }));
            }
            json!({
                "qualname": symbol.qualname,
                "kind": symbol.kind,
                "path": symbol.file_path,
                "start_line": symbol.start_line,
                "end_line": symbol.end_line,
                "stale": stale,
                "skeleton": true,
                "children": children,
            })
        }
    } else {
        build_source_response(indexer, symbol, context_lines, stale)?
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
    if !next_hops.is_empty() {
        response["next_hops"] = json!(next_hops);
    }
    Ok(response)
}

/// Byte length of `s` as a JSON string literal (quotes and any escaping
/// included) -- used to track a JSON string array's running size without
/// re-serialising the whole array.
fn quoted_len(s: &str) -> usize {
    serde_json::to_string(s)
        .map(|q| q.len())
        .unwrap_or(s.len() + 2)
}

/// Byte cost of adding one more element to a JSON array that already holds
/// `existing_count` elements (0 if this is the first): the element itself,
/// plus a leading comma once the array is non-empty. The array's own `[`/`]`
/// brackets are accounted separately, once, by the caller.
fn array_element_cost(existing_count: usize, element_len: usize) -> usize {
    if existing_count == 0 {
        element_len
    } else {
        1 + element_len
    }
}

/// Multi-symbol `read_symbol`: resolves each qualname in request order by
/// exact match (batch reads name symbols precisely -- unlike the single
/// `query` selector, there's no fuzzy fallback here) and fills `symbols` while
/// the running response stays within `max_bytes`. A symbol that would push the
/// response over budget, and every symbol after it, is omitted whole (never
/// cut mid-symbol) and listed by qualname in `omitted`. A qualname that
/// doesn't resolve to a real, non-external symbol is listed in `not_found`
/// instead and doesn't consume budget.
///
/// The per-symbol budget check tracks each of `symbols`/`omitted`/`not_found`'s
/// serialized-array byte length as a running total (`*_array_bytes`, each
/// starting at 2 for `[]`) rather than re-serialising the whole response on
/// every qualname -- serialising only the one new entry keeps this loop O(n)
/// instead of O(n^2). The byte arithmetic mirrors exactly what
/// `serde_json::to_string` would produce for `{"symbols":[...],"omitted":[...]}`
/// (plus `,"not_found":[...]` once that list is non-empty), so the decisions
/// -- and the final response size -- match what re-serialising every time
/// would have produced.
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
    let mut budget_exhausted = false;

    let mut symbols_array_bytes = 2usize; // "[]"
    let mut omitted_array_bytes = 2usize;
    let mut not_found_array_bytes = 2usize;

    for qn in qualnames {
        if budget_exhausted {
            omitted_array_bytes += array_element_cost(omitted.len(), quoted_len(qn));
            omitted.push(qn.clone());
            continue;
        }
        let found = indexer.db().get_symbol_by_qualname(qn, graph_version)?;
        let symbol = match found {
            Some(s) if s.kind != "external" && !s.qualname.starts_with("ext:") => s,
            _ => {
                not_found_array_bytes += array_element_cost(not_found.len(), quoted_len(qn));
                not_found.push(qn.clone());
                continue;
            }
        };
        // A single qualname's file being missing/stale-beyond-repair
        // shouldn't abort the whole batch -- record it and keep going so the
        // rest of the request still resolves.
        let entry =
            match build_symbol_entry(indexer, &symbol, skeleton, context_lines, graph_version) {
                Ok(entry) => entry,
                Err(err) => {
                    errors.push(json!({"qualname": qn, "error": err.to_string()}));
                    continue;
                }
            };

        let entry_len = serde_json::to_string(&entry)
            .map(|s| s.len())
            .unwrap_or(usize::MAX);
        let candidate_symbols_bytes =
            symbols_array_bytes + array_element_cost(symbols.len(), entry_len);

        // `{` + `}` + `"symbols":<array>` + `,` + `"omitted":<array>`, plus
        // `,"not_found":<array>` once that list is non-empty -- the same
        // fields (and the same not_found gating) the old per-iteration
        // `json!({"symbols": ..., "omitted": ...})` probe serialised.
        let mut probe_size = 2 + 10 + candidate_symbols_bytes + 1 + 10 + omitted_array_bytes;
        if !not_found.is_empty() {
            probe_size += 1 + 12 + not_found_array_bytes;
        }

        if probe_size <= max_bytes {
            symbols_array_bytes = candidate_symbols_bytes;
            symbols.push(entry);
        } else {
            omitted_array_bytes += array_element_cost(omitted.len(), quoted_len(qn));
            omitted.push(qn.clone());
            budget_exhausted = true;
        }
    }

    let mut response = json!({"symbols": symbols, "omitted": omitted});
    if !not_found.is_empty() {
        response["not_found"] = json!(not_found);
    }
    if !errors.is_empty() {
        response["errors"] = json!(errors);
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
    let params: ReadSymbolParams = serde_json::from_value(params)?;
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

    let resolution = resolve_read_target(
        indexer.db(),
        params.qualname.as_deref(),
        params.query.as_deref(),
        graph_version,
    )?;

    let symbol = match resolution {
        ReadTarget::Ambiguous(candidates) => {
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
        ReadTarget::Found(symbol) => symbol,
    };

    if symbol.kind == "external" || symbol.qualname.starts_with("ext:") {
        anyhow::bail!(
            "symbol not found: '{}' is an external stub with no indexed source",
            symbol.qualname
        );
    }

    let entry = build_symbol_entry(indexer, &symbol, skeleton, context_lines, graph_version)?;

    // A single symbol's response is otherwise uncapped (the outer generic
    // response-size cap can't safely shrink a plain object with no array
    // field to slice -- see `format::truncate_response`, and `read_symbol` is
    // exempt from it anyway so it can honour its own `max_bytes`). Rather
    // than ever return a source cut mid-body, an over-budget response is
    // replaced by just its header fields plus `omitted: true`.
    let max_bytes = params
        .max_bytes
        .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
        .min(200_000);
    let entry_size = serde_json::to_string(&entry).map(|s| s.len()).unwrap_or(0);
    if entry_size > max_bytes {
        let stale = entry.get("stale").cloned().unwrap_or(json!(false));
        return Ok(json!({
            "qualname": symbol.qualname,
            "kind": symbol.kind,
            "path": symbol.file_path,
            "start_line": symbol.start_line,
            "end_line": symbol.end_line,
            "stale": stale,
            "omitted": true,
            "size_bytes": entry_size,
            "next_hops": [
                {
                    "method": "read_symbol",
                    "params": {"qualname": symbol.qualname, "skeleton": true},
                    "description": format!(
                        "'{}' is too large to read in full ({} bytes > {} budget) -- read_symbol with skeleton:true returns just its children's signatures (containers only)",
                        symbol.qualname, entry_size, max_bytes
                    ),
                },
                {
                    "method": "outline",
                    "params": {"path": symbol.file_path},
                    "description": "Outline the file to pick a narrower symbol to read",
                },
            ],
        }));
    }

    Ok(entry)
}

#[cfg(test)]
mod resolve_read_target_tests {
    use super::*;
    use tempfile::TempDir;

    /// `resolve_read_target` is only ever called (from `handle_read_symbol`)
    /// after validating exactly one of qualname/query/qualnames was given, so
    /// this invariant violation is unreachable through the public RPC seam --
    /// this test calls the private function directly to exercise it, the way
    /// a future refactor that drops that upstream guard would.
    #[test]
    fn errors_instead_of_panicking_when_neither_qualname_nor_query_given() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        std::fs::write(root.join("m.py"), "def foo():\n    return 1\n").unwrap();
        let mut indexer =
            Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
        indexer.reindex().unwrap();
        let graph_version = indexer.db().current_graph_version().unwrap();

        let result = resolve_read_target(indexer.db(), None, None, graph_version);
        assert!(
            result.is_err(),
            "resolve_read_target with neither qualname nor query must return an error, not panic"
        );
    }
}
