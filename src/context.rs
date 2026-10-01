//! Passive context injection: compact cross-file context for a single file.
//!
//! Operates on `Db` directly (not `Indexer`) for fast startup (~30ms).

use crate::db::Db;
use crate::indexer::test_detection::is_test_file;
use crate::model::{Edge, Symbol};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const MAX_CALLERS: usize = 15;
const MAX_CALLEES: usize = 15;
const MAX_XREFS: usize = 15;

#[derive(Debug, Clone, Serialize)]
pub struct CrossRef {
    pub symbol_name: String,
    pub file_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileContext {
    pub path: String,
    pub symbol_summary: String,
    pub cross_file_callers: Vec<CrossRef>,
    pub cross_file_callees: Vec<CrossRef>,
    pub test_files: Vec<String>,
    pub xrefs: Vec<CrossRef>,
}

pub fn build_file_context(
    db: &Db,
    _repo_root: &Path,
    file_path: &str,
    graph_version: i64,
) -> Result<FileContext> {
    // 1. Get all symbols for this file
    let symbols = db.get_symbols_for_file(file_path, graph_version)?;
    if symbols.is_empty() {
        return Ok(FileContext {
            path: file_path.to_string(),
            symbol_summary: "0 symbols".to_string(),
            cross_file_callers: vec![],
            cross_file_callees: vec![],
            test_files: vec![],
            xrefs: vec![],
        });
    }

    // Build symbol summary
    let symbol_summary = build_symbol_summary(&symbols);

    // Collect symbol IDs and names for this file
    let symbol_ids: Vec<i64> = symbols.iter().map(|s| s.id).collect();
    let symbol_id_set: HashSet<i64> = symbol_ids.iter().copied().collect();

    // 2. Get all edges involving these symbols
    let edges_map = db.edges_for_symbols(&symbol_ids, None, graph_version)?;
    let all_edges: Vec<&Edge> = edges_map.values().flat_map(|v| v.iter()).collect();

    // Deduplicate edges by id
    let mut seen_edge_ids = HashSet::new();
    let unique_edges: Vec<&Edge> = all_edges
        .into_iter()
        .filter(|e| seen_edge_ids.insert(e.id))
        .collect();

    // 3. Partition into callers/callees/xrefs
    let mut callers: Vec<CrossRef> = Vec::new();
    let mut callees: Vec<CrossRef> = Vec::new();
    let mut xrefs: Vec<CrossRef> = Vec::new();
    let mut caller_seen: HashSet<(String, String)> = HashSet::new();
    let mut callee_seen: HashSet<(String, String)> = HashSet::new();
    let mut xref_seen: HashSet<(String, String)> = HashSet::new();

    // Cache of resolved symbols (qualname, file_path) by id, so each id costs one query.
    let mut symbol_cache: HashMap<i64, Option<(String, String)>> = HashMap::new();
    let mut lookup = |id: i64| -> Result<Option<(String, String)>> {
        if let Some(hit) = symbol_cache.get(&id) {
            return Ok(hit.clone());
        }
        let found = db
            .get_symbol_by_id(id)
            .with_context(|| format!("failed to look up symbol {id} for file context"))?
            .map(|s| (s.qualname, s.file_path));
        symbol_cache.insert(id, found.clone());
        Ok(found)
    };

    for edge in &unique_edges {
        // edge.file_path is the file where the edge was *emitted* (source file).
        // For outgoing calls (source in our file), this equals our file — we need
        // to resolve the target symbol's file instead.

        match edge.kind.as_str() {
            "CALLS" | "RPC_CALL" | "HTTP_CALL" | "CHANNEL_PUBLISH" => {
                // If source is in our file → outgoing call (callee)
                if let Some(src_id) = edge.source_symbol_id
                    && symbol_id_set.contains(&src_id)
                {
                    // Unresolved targets are omitted: the raw target_qualname is only
                    // an extractor guess and may name a symbol that does not exist.
                    let Some(tgt_id) = edge.target_symbol_id else {
                        continue;
                    };
                    let Some((name, callee_file)) = lookup(tgt_id)? else {
                        continue;
                    };
                    if callee_file.is_empty() || callee_file == file_path {
                        continue;
                    }

                    let key = (name.clone(), callee_file.clone());
                    if callee_seen.insert(key) && callees.len() < MAX_CALLEES {
                        callees.push(CrossRef {
                            symbol_name: name,
                            file_path: callee_file,
                        });
                    }
                    continue;
                }
                // If target is in our file → incoming call (caller) from edge.file_path
                if edge.file_path == file_path {
                    continue; // Same-file edge, not cross-file
                }
                if let Some(tgt_id) = edge.target_symbol_id
                    && symbol_id_set.contains(&tgt_id)
                    && let Some(src_id) = edge.source_symbol_id
                {
                    let Some((name, _)) = lookup(src_id)? else {
                        continue;
                    };

                    let key = (name.clone(), edge.file_path.clone());
                    if caller_seen.insert(key) && callers.len() < MAX_CALLERS {
                        callers.push(CrossRef {
                            symbol_name: name,
                            file_path: edge.file_path.clone(),
                        });
                    }
                }
            }
            "XREF" => {
                if edge.file_path == file_path {
                    continue;
                }
                let name = edge
                    .target_qualname
                    .as_deref()
                    .or(edge.detail.as_deref())
                    .unwrap_or("?")
                    .to_string();
                let key = (name.clone(), edge.file_path.clone());
                if xref_seen.insert(key) && xrefs.len() < MAX_XREFS {
                    xrefs.push(CrossRef {
                        symbol_name: short_name(&name),
                        file_path: edge.file_path.clone(),
                    });
                }
            }
            _ => {}
        }
    }

    // 4. Detect test files from callers
    let mut test_files: Vec<String> = Vec::new();
    let mut test_file_set: HashSet<String> = HashSet::new();
    for cr in &callers {
        if is_test_file(&cr.file_path) && test_file_set.insert(cr.file_path.clone()) {
            test_files.push(cr.file_path.clone());
        }
    }

    // Remove test callers from callers list (they're in test_files)
    callers.retain(|cr| !is_test_file(&cr.file_path));

    Ok(FileContext {
        path: file_path.to_string(),
        symbol_summary,
        cross_file_callers: callers,
        cross_file_callees: callees,
        test_files,
        xrefs,
    })
}

pub fn format_text(ctx: &FileContext) -> String {
    if ctx.symbol_summary == "0 symbols" {
        return String::new();
    }

    let mut out = format!("# {} ({})\n", ctx.path, ctx.symbol_summary);

    if !ctx.cross_file_callers.is_empty() {
        out.push_str("Callers: ");
        let shown: Vec<String> = ctx
            .cross_file_callers
            .iter()
            .take(5)
            .map(|cr| format!("{} [{}]", cr.symbol_name, cr.file_path))
            .collect();
        out.push_str(&shown.join(", "));
        if ctx.cross_file_callers.len() > 5 {
            out.push_str(&format!(" +{} more", ctx.cross_file_callers.len() - 5));
        }
        out.push('\n');
    }

    if !ctx.cross_file_callees.is_empty() {
        out.push_str("Callees: ");
        let shown: Vec<String> = ctx
            .cross_file_callees
            .iter()
            .take(5)
            .map(|cr| format!("{} [{}]", cr.symbol_name, cr.file_path))
            .collect();
        out.push_str(&shown.join(", "));
        if ctx.cross_file_callees.len() > 5 {
            out.push_str(&format!(" +{} more", ctx.cross_file_callees.len() - 5));
        }
        out.push('\n');
    }

    if !ctx.test_files.is_empty() {
        out.push_str("Tests: ");
        out.push_str(&ctx.test_files.join(", "));
        out.push('\n');
    }

    if !ctx.xrefs.is_empty() {
        out.push_str("XRefs: ");
        let shown: Vec<String> = ctx
            .xrefs
            .iter()
            .take(5)
            .map(|cr| format!("{} [{}]", cr.symbol_name, cr.file_path))
            .collect();
        out.push_str(&shown.join(", "));
        if ctx.xrefs.len() > 5 {
            out.push_str(&format!(" +{} more", ctx.xrefs.len() - 5));
        }
        out.push('\n');
    }

    out
}

pub fn format_json(ctx: &FileContext) -> Value {
    json!(ctx)
}

fn build_symbol_summary(symbols: &[Symbol]) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in symbols {
        *counts.entry(s.kind.as_str()).or_default() += 1;
    }

    let total = symbols.len();
    let mut parts: Vec<String> = Vec::new();

    // Sort by count descending for stable output
    let mut entries: Vec<(&&str, &usize)> = counts.iter().collect();
    entries.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));

    for (kind, count) in entries {
        parts.push(format!("{} {}", count, kind));
    }

    format!("{} symbols: {}", total, parts.join(", "))
}

fn short_name(qualname: &str) -> String {
    // "pkg.core.Greeter.greet" → "Greeter.greet" (last 2 segments)
    let parts: Vec<&str> = qualname.split('.').collect();
    if parts.len() <= 2 {
        qualname.to_string()
    } else {
        parts[parts.len() - 2..].join(".")
    }
}
