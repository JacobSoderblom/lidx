use crate::model::{ContextItem, ItemSource, MatchLocation, Symbol};
use anyhow::Result;
use std::path::Path;

pub(super) fn read_file_header(repo_root: &Path, file_path: &str) -> Result<String> {
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

    if header.len() > 500 {
        if let Some(pos) = header[..500].rfind('\n') {
            header.truncate(pos + 1);
        } else {
            header.truncate(500);
        }
    }

    Ok(header)
}

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

    let body = &file_content[start..end];

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

    if let Some(e) = edge {
        if let (Some(snippet), Some(line)) = (&e.evidence_snippet, e.evidence_start_line) {
            result.push_str(&format!(
                "  // {} at line {}\n",
                e.kind.to_lowercase(),
                line
            ));
            result.push_str("  ");
            result.push_str(snippet.trim());
            result.push('\n');
        }
    }

    result
}

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

    if snippet.len() > remaining_budget {
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

    let mut snippet = content[start..end].to_string();

    if snippet.len() > remaining_budget {
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
