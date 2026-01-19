use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use anyhow::Result;
use std::path::Path;

struct LineInfo {
    start: usize,
    content_end: usize,
}

struct HeadingInfo {
    level: usize,
    text: String,
    start_line: usize,
    start_col: usize,
    start_byte: usize,
}

struct FenceInfo {
    marker: u8,
    count: usize,
}

pub struct MarkdownExtractor;

impl MarkdownExtractor {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        let lines = collect_lines(source);
        let total_lines = lines.len();
        let module_span = if total_lines == 0 {
            (1, 1, 1, 1, 0, 0)
        } else {
            let end_col = line_end_col(&lines, total_lines);
            (1, 1, total_lines as i64, end_col, 0, source.len() as i64)
        };
        output
            .symbols
            .push(module_symbol_with_span(module_name, module_span));

        let headings = parse_headings(source, &lines);
        if headings.is_empty() {
            return Ok(output);
        }

        let mut stack: Vec<(usize, String)> = Vec::new();
        for (idx, heading) in headings.iter().enumerate() {
            let (end_line, end_byte) = heading_bounds(&headings, idx, total_lines, source.len());
            let end_col = line_end_col(&lines, end_line);
            let qualname = heading_qualname(module_name, &heading.text);
            output.symbols.push(SymbolInput {
                kind: "heading".to_string(),
                name: heading.text.clone(),
                qualname: qualname.clone(),
                start_line: heading.start_line as i64,
                start_col: heading.start_col as i64,
                end_line: end_line as i64,
                end_col,
                start_byte: heading.start_byte as i64,
                end_byte: end_byte as i64,
                signature: Some(format!("H{}", heading.level)),
                docstring: None,
            });

            while let Some((level, _)) = stack.last() {
                if *level < heading.level {
                    break;
                }
                stack.pop();
            }
            let parent = stack
                .last()
                .map(|(_, qualname)| qualname.clone())
                .unwrap_or_else(|| module_name.to_string());
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(parent),
                target_qualname: Some(qualname.clone()),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
            stack.push((heading.level, qualname));
        }

        Ok(output)
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "module".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if !stem.is_empty() {
        parts.push(stem);
    }
    if parts.is_empty() {
        "module".to_string()
    } else {
        parts.join("/")
    }
}

fn module_symbol_with_span(module_name: &str, span: (i64, i64, i64, i64, i64, i64)) -> SymbolInput {
    let name = module_name
        .rsplit('/')
        .next()
        .unwrap_or(module_name)
        .to_string();
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span;
    SymbolInput {
        kind: "module".to_string(),
        name,
        qualname: module_name.to_string(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    }
}

fn collect_lines(source: &str) -> Vec<LineInfo> {
    let bytes = source.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            let end = idx;
            let content_end = if end > start && bytes[end - 1] == b'\r' {
                end - 1
            } else {
                end
            };
            lines.push(LineInfo { start, content_end });
            start = idx + 1;
        }
    }
    if start <= bytes.len() {
        let end = bytes.len();
        let content_end = if end > start && bytes[end - 1] == b'\r' {
            end - 1
        } else {
            end
        };
        lines.push(LineInfo { start, content_end });
    }
    lines
}

fn line_slice<'a>(source: &'a str, line: &LineInfo) -> &'a str {
    &source[line.start..line.content_end]
}

fn line_end_col(lines: &[LineInfo], line_num: usize) -> i64 {
    if line_num == 0 || line_num > lines.len() {
        return 1;
    }
    let line = &lines[line_num - 1];
    let len = line.content_end.saturating_sub(line.start);
    (len + 1) as i64
}

fn heading_bounds(
    headings: &[HeadingInfo],
    idx: usize,
    total_lines: usize,
    total_bytes: usize,
) -> (usize, usize) {
    if idx + 1 < headings.len() {
        let next = &headings[idx + 1];
        let end_line = next
            .start_line
            .saturating_sub(1)
            .max(headings[idx].start_line);
        (end_line, next.start_byte)
    } else {
        let end_line = total_lines.max(headings[idx].start_line);
        (end_line, total_bytes)
    }
}

fn heading_qualname(module_name: &str, text: &str) -> String {
    format!("{module_name}#{text}")
}

fn parse_headings(source: &str, lines: &[LineInfo]) -> Vec<HeadingInfo> {
    let mut headings = Vec::new();
    let mut i = 0usize;
    let mut fence: Option<FenceInfo> = None;
    while i < lines.len() {
        let line = line_slice(source, &lines[i]);
        if let Some(active) = fence.as_ref() {
            if is_fence_end(line, active) {
                fence = None;
            }
            i += 1;
            continue;
        }
        if let Some(next_fence) = is_fence_start(line) {
            fence = Some(next_fence);
            i += 1;
            continue;
        }

        if let Some((level, text, start_col, start_offset)) = parse_atx_heading(line) {
            let start_byte = lines[i].start + start_offset;
            headings.push(HeadingInfo {
                level,
                text,
                start_line: i + 1,
                start_col,
                start_byte,
            });
            i += 1;
            continue;
        }

        if i + 1 < lines.len() {
            let next_line = line_slice(source, &lines[i + 1]);
            if let Some((level, text, start_col)) = parse_setext_heading(line, next_line) {
                let start_offset = start_col.saturating_sub(1);
                let start_byte = lines[i].start + start_offset;
                headings.push(HeadingInfo {
                    level,
                    text,
                    start_line: i + 1,
                    start_col,
                    start_byte,
                });
                i += 2;
                continue;
            }
        }

        i += 1;
    }
    headings
}

fn parse_atx_heading(line: &str) -> Option<(usize, String, usize, usize)> {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx] == b' ' {
        idx += 1;
    }
    if idx > 3 {
        return None;
    }
    let indent = idx;
    let mut level = 0usize;
    while idx < bytes.len() && bytes[idx] == b'#' {
        level += 1;
        idx += 1;
    }
    if level == 0 || level > 6 {
        return None;
    }
    if idx < bytes.len() {
        let next = bytes[idx];
        if next != b' ' && next != b'\t' {
            return None;
        }
    }
    let text = trim_atx_heading(&line[idx..]);
    if text.is_empty() {
        return None;
    }
    Some((level, text, indent + 1, indent))
}

fn trim_atx_heading(raw: &str) -> String {
    let mut text = raw.trim().to_string();
    if text.is_empty() {
        return text;
    }
    let trimmed = text.trim_end();
    let bytes = trimmed.as_bytes();
    let mut hash_end = bytes.len();
    while hash_end > 0 && bytes[hash_end - 1] == b'#' {
        hash_end -= 1;
    }
    if hash_end < bytes.len() {
        if hash_end > 0 && bytes[hash_end - 1].is_ascii_whitespace() {
            text.truncate(hash_end);
            text = text.trim_end().to_string();
        }
    }
    text
}

fn parse_setext_heading(line: &str, next_line: &str) -> Option<(usize, String, usize)> {
    let text = line.trim();
    if text.is_empty() {
        return None;
    }
    let underline = next_line.trim();
    if underline.len() < 3 {
        return None;
    }
    let (level, target) = if underline.chars().all(|ch| ch == '=') {
        (1, '=')
    } else if underline.chars().all(|ch| ch == '-') {
        (2, '-')
    } else {
        return None;
    };
    if underline.chars().any(|ch| ch != target) {
        return None;
    }
    let start_col = first_non_space_col(line);
    Some((level, text.to_string(), start_col))
}

fn first_non_space_col(line: &str) -> usize {
    for (idx, byte) in line.as_bytes().iter().enumerate() {
        if *byte != b' ' && *byte != b'\t' {
            return idx + 1;
        }
    }
    1
}

fn is_fence_start(line: &str) -> Option<FenceInfo> {
    let trimmed = line.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    let marker = bytes[0];
    if marker != b'`' && marker != b'~' {
        return None;
    }
    let count = bytes.iter().take_while(|b| **b == marker).count();
    if count >= 3 {
        Some(FenceInfo { marker, count })
    } else {
        None
    }
}

fn is_fence_end(line: &str, fence: &FenceInfo) -> bool {
    let trimmed = line.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.len() < fence.count || bytes[0] != fence.marker {
        return false;
    }
    let count = bytes.iter().take_while(|b| **b == fence.marker).count();
    count >= fence.count
}
