use crate::model::{ContextItem, ItemSource, MatchLocation, Symbol};
use anyhow::Result;
use std::path::Path;

/// Largest index <= `idx` that lies on a UTF-8 char boundary of `s`.
fn floor_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut i = idx;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Truncate `s` to at most `max_bytes`, preferring a line boundary,
/// never splitting a UTF-8 char.
fn truncate_at_line_boundary(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let cut = floor_char_boundary(s, max_bytes);
    if let Some(pos) = s[..cut].rfind('\n') {
        s.truncate(pos + 1);
    } else {
        s.truncate(cut);
    }
}

/// Read file header (first 10 lines, capped at 500 bytes)
pub(super) fn read_file_header(repo_root: &Path, file_path: &str) -> Result<String> {
    let abs_path = repo_root.join(file_path);
    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(String::new()),
    };

    let mut header = String::new();
    for (i, line) in content.lines().enumerate() {
        if i >= 10 || header.len() > 500 {
            break;
        }
        header.push_str(line);
        header.push('\n');
    }

    // Truncate at 500 bytes if needed
    truncate_at_line_boundary(&mut header, 500);

    Ok(header)
}

/// Format symbol at Tier 0: full source body with file header
pub(super) fn format_tier0(
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

    // Stale index data can put byte offsets mid-char in the current file content
    let Some(body) = file_content.get(start..end) else {
        return Ok(String::new());
    };

    let mut result = String::new();
    if !header.is_empty() {
        result.push_str(&format!("// File: {} (header)\n", symbol.file_path));
        result.push_str(&header);
        result.push('\n');
    }
    result.push_str(&format!(
        "// Symbol: {} ({})\n",
        symbol.qualname, symbol.kind
    ));
    result.push_str(body);

    Ok(result)
}

/// Format symbol at Tier 1: signature + call site evidence
pub(super) fn format_tier1(symbol: &Symbol, edge: Option<&crate::model::Edge>) -> String {
    let mut result = String::new();

    result.push_str(&format!(
        "// File: {} ({})\n",
        symbol.file_path, symbol.kind
    ));

    if let Some(sig) = &symbol.signature {
        result.push_str(sig);
        result.push('\n');
    } else {
        result.push_str(&format!("{} {}\n", symbol.kind, symbol.name));
    }

    if let Some(e) = edge
        && let (Some(snippet), Some(line)) = (&e.evidence_snippet, e.evidence_start_line)
    {
        result.push_str(&format!(
            "  // {} at line {}\n",
            e.kind.to_lowercase(),
            line
        ));
        result.push_str("  ");
        result.push_str(snippet.trim());
        result.push('\n');
    }

    result
}

/// Format symbol at Tier 2: signature only
pub(super) fn format_tier2(symbol: &Symbol) -> String {
    let mut result = String::new();

    result.push_str(&format!(
        "// File: {} ({})\n",
        symbol.file_path, symbol.kind
    ));

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
pub(super) fn read_symbol_content(
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

    // Stale index data can put byte offsets mid-char in the current file content
    let Some(region) = content.get(start..end) else {
        return Ok(None);
    };
    let mut snippet = region.to_string();

    // Truncate to remaining budget if needed
    truncate_at_line_boundary(&mut snippet, remaining_budget);

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
#[allow(clippy::too_many_arguments)]
pub(super) fn read_file_region(
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

    // Stale index data can put byte offsets mid-char in the current file content
    let Some(region) = content.get(start..end) else {
        return Ok(None);
    };
    let mut snippet = region.to_string();

    // Truncate to remaining budget if needed
    truncate_at_line_boundary(&mut snippet, remaining_budget);

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
