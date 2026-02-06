use crate::indexer::extract::SymbolInput;
use blake3::Hasher;

#[derive(Debug, Clone)]
pub struct FileMetricsInput {
    pub loc: i64,
    pub blank: i64,
    pub comment: i64,
    pub code: i64,
}

#[derive(Debug, Clone)]
pub struct SymbolMetricsInput {
    pub qualname: String,
    pub loc: i64,
    pub complexity: i64,
    pub duplication_hash: Option<String>,
}

pub fn compute_file_metrics(source: &str, language: &str) -> FileMetricsInput {
    let mut loc = 0;
    let mut blank = 0;
    let mut comment = 0;
    let mut code = 0;
    let (line_comment, block_start, block_end) = comment_style(language);
    let mut in_block_comment = false;

    for line in source.lines() {
        loc += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            blank += 1;
            continue;
        }
        if in_block_comment {
            comment += 1;
            if let Some(end) = block_end {
                if trimmed.contains(end) {
                    in_block_comment = false;
                }
            }
            continue;
        }
        if let Some(prefix) = line_comment {
            if trimmed.starts_with(prefix) {
                comment += 1;
                continue;
            }
        }
        if let Some(start) = block_start {
            if trimmed.starts_with(start) {
                comment += 1;
                if let Some(end) = block_end {
                    if !trimmed.contains(end) {
                        in_block_comment = true;
                    }
                }
                continue;
            }
        }
        code += 1;
    }
    if loc == 0 {
        loc = 1;
    }
    FileMetricsInput {
        loc,
        blank,
        comment,
        code,
    }
}

pub fn compute_symbol_metrics(
    source: &str,
    language: &str,
    symbols: &[SymbolInput],
) -> Vec<SymbolMetricsInput> {
    let mut metrics = Vec::new();
    for symbol in symbols {
        if !is_callable_kind(&symbol.kind) {
            continue;
        }
        let loc = (symbol.end_line - symbol.start_line + 1).max(1);
        let snippet = slice_symbol(source, symbol.start_byte, symbol.end_byte);
        let complexity = complexity_for(language, &snippet);
        let duplication_hash = duplication_hash(&snippet);
        metrics.push(SymbolMetricsInput {
            qualname: symbol.qualname.clone(),
            loc,
            complexity,
            duplication_hash,
        });
    }
    metrics
}

fn is_callable_kind(kind: &str) -> bool {
    matches!(kind, "function" | "method")
}

fn slice_symbol(source: &str, start_byte: i64, end_byte: i64) -> String {
    if start_byte < 0 || end_byte <= start_byte {
        return String::new();
    }
    let start = start_byte as usize;
    let mut end = end_byte as usize;
    if start >= source.len() {
        return String::new();
    }
    if end > source.len() {
        end = source.len();
    }
    source.get(start..end).unwrap_or("").to_string()
}

fn complexity_for(language: &str, snippet: &str) -> i64 {
    let (keywords, operators) = match language {
        "python" => (
            &[
                "if", "elif", "for", "while", "except", "case", "and", "or", "with",
            ][..],
            &[][..],
        ),
        "rust" => (
            &["if", "for", "while", "loop", "match"][..],
            &["&&", "||"][..],
        ),
        "javascript" | "typescript" | "tsx" => (
            &["if", "for", "while", "case", "catch", "switch"][..],
            &["&&", "||", "?"][..],
        ),
        "csharp" => (
            &["if", "for", "foreach", "while", "case", "catch", "switch"][..],
            &["&&", "||", "?"][..],
        ),
        "go" => (
            &["if", "for", "switch", "case", "select"][..],
            &["&&", "||"][..],
        ),
        "lua" => (
            &["if", "elseif", "for", "while", "repeat", "and", "or"][..],
            &[][..],
        ),
        _ => (&[][..], &[][..]),
    };
    let keyword_hits = count_keyword_hits(snippet, keywords);
    let operator_hits = count_operator_hits(snippet, operators);
    let mut complexity = 1 + keyword_hits + operator_hits;
    if complexity < 1 {
        complexity = 1;
    }
    complexity
}

fn count_keyword_hits(snippet: &str, keywords: &[&str]) -> i64 {
    if keywords.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut token = String::new();
    for ch in snippet.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            token.push(ch);
        } else if !token.is_empty() {
            if keywords.contains(&token.as_str()) {
                count += 1;
            }
            token.clear();
        }
    }
    if !token.is_empty() && keywords.contains(&token.as_str()) {
        count += 1;
    }
    count
}

fn count_operator_hits(snippet: &str, operators: &[&str]) -> i64 {
    let mut count = 0;
    for op in operators {
        count += snippet.matches(op).count() as i64;
    }
    count
}

fn duplication_hash(snippet: &str) -> Option<String> {
    let normalized: String = snippet.chars().filter(|ch| !ch.is_whitespace()).collect();
    if normalized.is_empty() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(normalized.as_bytes());
    Some(hasher.finalize().to_hex().to_string())
}

fn comment_style(
    language: &str,
) -> (
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
) {
    match language {
        "python" => (Some("#"), None, None),
        "rust" | "javascript" | "typescript" | "tsx" | "csharp" | "go" => {
            (Some("//"), Some("/*"), Some("*/"))
        }
        "lua" => (Some("--"), Some("--[["), Some("]]")),
        "postgres" | "sql" | "tsql" => (Some("--"), Some("/*"), Some("*/")),
        _ => (None, None, None),
    }
}
