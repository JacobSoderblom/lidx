use crate::db::{Db, SymbolRefRecord};
use crate::indexer::extract::EdgeInput;
use crate::indexer::scan::ScannedFile;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::collections::{HashMap, HashSet};

const XREF_KIND: &str = "XREF";
const XREF_MIN_CONFIDENCE: f64 = 0.7;
const ROUTE_KIND: &str = "ROUTE";
const ROUTE_MIN_CONFIDENCE: f64 = 0.85;
const ROUTE_MAX_LEN: usize = 200;
const ROUTE_RAW_MAX_BYTES: usize = 200;
const TOKEN_MIN_LEN: usize = 4;
const TOKEN_MIN_LOWER_LEN: usize = 6;
const STOPWORDS: &[&str] = &[
    "a", "an", "and", "any", "as", "asc", "begin", "between", "by", "case", "create", "delete",
    "desc", "distinct", "drop", "else", "end", "exists", "false", "from", "full", "group",
    "having", "if", "in", "inner", "insert", "into", "is", "join", "left", "like", "limit", "not",
    "null", "offset", "on", "or", "order", "outer", "primary", "return", "right", "select", "set",
    "then", "true", "union", "update", "values", "when", "where", "with",
];

pub fn link_cross_language_refs(
    db: &mut Db,
    files: &[ScannedFile],
    clear_existing: bool,
    graph_version: i64,
) -> Result<usize> {
    if clear_existing {
        db.delete_edges_by_kind(XREF_KIND, graph_version)?;
        db.delete_edges_by_kind(ROUTE_KIND, graph_version)?;
    }
    let index = SymbolRefIndex::build(db, graph_version)?;
    let commit_sha = db.graph_version_commit(graph_version)?;
    let mut total = 0;
    for file in files {
        if !should_scan_file(file) {
            continue;
        }
        let Some(record) = db.get_file_by_path(&file.rel_path)? else {
            continue;
        };
        let source = util::read_to_string(&file.abs_path)?;
        let xref_edges = collect_xref_edges(db, &index, file, &source, graph_version)?;
        let route_edges = collect_route_edges(db, file, &source, graph_version)?;
        if xref_edges.is_empty() && route_edges.is_empty() {
            continue;
        }
        let mut edges = xref_edges;
        edges.extend(route_edges);
        let symbol_map = db.symbol_map_for_file(record.id, graph_version)?;
        let count = db.insert_edges(
            record.id,
            &edges,
            &symbol_map,
            graph_version,
            commit_sha.as_deref(),
        )?;
        total += count;
    }
    Ok(total)
}

fn should_scan_file(file: &ScannedFile) -> bool {
    file.language != "yaml" && file.language != "bicep"
}

fn collect_xref_edges(
    db: &Db,
    index: &SymbolRefIndex,
    file: &ScannedFile,
    source: &str,
    graph_version: i64,
) -> Result<Vec<EdgeInput>> {
    let literals = scan_string_literals_for(source, &file.language);
    if literals.is_empty() {
        return Ok(Vec::new());
    }
    let mut edges_by_key: HashMap<(String, String), EdgeInput> = HashMap::new();
    let mut line_cache: HashMap<i64, Option<String>> = HashMap::new();
    for literal in literals {
        if file.language == "python"
            && (is_python_docstring(source, &literal) || is_in_python_comment(source, &literal))
        {
            continue;
        }
        let Some(source_qualname) = lookup_source_qualname(
            db,
            &file.rel_path,
            literal.start_line,
            &mut line_cache,
            graph_version,
        )?
        else {
            continue;
        };
        let snippet = util::edge_evidence_snippet(
            source,
            literal.start_byte,
            literal.end_byte,
            literal.start_line,
            literal.end_line,
        );
        for token in extract_tokens(&literal.text) {
            let Some(match_info) = index.resolve_token(&token, &file.language, &file.rel_path)
            else {
                continue;
            };
            let key = (source_qualname.clone(), match_info.symbol.qualname.clone());
            let detail = Some(
                json!({
                    "token": token,
                    "confidence": match_info.confidence,
                    "match": match_info.match_kind.as_str(),
                    "source": "string_literal",
                })
                .to_string(),
            );
            let edge = EdgeInput {
                kind: XREF_KIND.to_string(),
                source_qualname: Some(source_qualname.clone()),
                target_qualname: Some(match_info.symbol.qualname.clone()),
                detail,
                evidence_snippet: snippet.clone(),
                evidence_start_line: Some(literal.start_line),
                evidence_end_line: Some(literal.end_line),
                confidence: Some(match_info.confidence),
                ..Default::default()
            };
            match edges_by_key.get(&key) {
                Some(existing) => {
                    if match_info.confidence > existing.confidence.unwrap_or(0.0) {
                        edges_by_key.insert(key, edge);
                    }
                }
                None => {
                    edges_by_key.insert(key, edge);
                }
            }
        }
    }
    Ok(edges_by_key.into_values().collect())
}

/// A triple-quoted literal that starts its own line right after a
/// `def`/`class` header (or at the top of the file, past blank and `#` lines
/// such as a shebang) is prose, not a reference. String prefixes are allowed.
fn is_python_docstring(source: &str, literal: &StringLiteral) -> bool {
    let start = literal.start_byte as usize;
    let Some(before) = source.get(..start) else {
        return false;
    };
    if !source[start..].starts_with("\"\"\"") && !source[start..].starts_with("'''") {
        return false;
    }
    let before = before.trim_end_matches(|c| "rRfFbBuU".contains(c));
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    if !before[line_start..].trim().is_empty() {
        return false;
    }
    let prev = before[..line_start]
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with('#'));
    prev.is_none_or(|l| l.ends_with(':'))
}

/// True when the literal starts after a `#` that is not itself inside a string.
fn is_in_python_comment(source: &str, literal: &StringLiteral) -> bool {
    let start = literal.start_byte as usize;
    let Some(before) = source.get(..start) else {
        return false;
    };
    let line = &before[before.rfind('\n').map_or(0, |i| i + 1)..];
    let (mut dq, mut sq) = (0, 0);
    for ch in line.chars() {
        match ch {
            '"' => dq += 1,
            '\'' => sq += 1,
            '#' if dq % 2 == 0 && sq % 2 == 0 => return true,
            _ => {}
        }
    }
    false
}

fn collect_route_edges(
    db: &Db,
    file: &ScannedFile,
    source: &str,
    graph_version: i64,
) -> Result<Vec<EdgeInput>> {
    let literals = scan_string_literals(source);
    if literals.is_empty() {
        return Ok(Vec::new());
    }
    let mut edges = Vec::new();
    let mut seen = HashSet::new();
    let mut line_cache: HashMap<i64, Option<String>> = HashMap::new();
    for literal in literals {
        let Some(route) = normalize_route_literal(&literal.text) else {
            continue;
        };
        let Some(source_qualname) = lookup_source_qualname(
            db,
            &file.rel_path,
            literal.start_line,
            &mut line_cache,
            graph_version,
        )?
        else {
            continue;
        };
        let key = (source_qualname.clone(), route.clone());
        if !seen.insert(key) {
            continue;
        }
        let snippet = util::edge_evidence_snippet(
            source,
            literal.start_byte,
            literal.end_byte,
            literal.start_line,
            literal.end_line,
        );
        let raw = util::truncate_str_bytes(literal.text.trim(), ROUTE_RAW_MAX_BYTES);
        let route_key = route.clone();
        let detail = Some(
            json!({
                "route": route_key,
                "raw": raw,
                "source": "string_literal",
                "language": file.language,
            })
            .to_string(),
        );
        edges.push(EdgeInput {
            kind: ROUTE_KIND.to_string(),
            source_qualname: Some(source_qualname),
            target_qualname: Some(route),
            detail,
            evidence_snippet: snippet,
            evidence_start_line: Some(literal.start_line),
            evidence_end_line: Some(literal.end_line),
            confidence: Some(ROUTE_MIN_CONFIDENCE),
            ..Default::default()
        });
    }
    Ok(edges)
}

fn lookup_source_qualname(
    db: &Db,
    rel_path: &str,
    line: i64,
    cache: &mut HashMap<i64, Option<String>>,
    graph_version: i64,
) -> Result<Option<String>> {
    if let Some(cached) = cache.get(&line) {
        return Ok(cached.clone());
    }
    let symbol = db.enclosing_symbol_for_line(rel_path, line, graph_version)?;
    let qualname = symbol.map(|value| value.qualname);
    cache.insert(line, qualname.clone());
    Ok(qualname)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum KeyKind {
    QualnameExact,
    QualnameNormalized,
    QualnameLower,
    QualnameLowerNormalized,
    NameExact,
    NameLower,
}

impl KeyKind {
    fn as_str(&self) -> &'static str {
        match self {
            KeyKind::QualnameExact => "qualname_exact",
            KeyKind::QualnameNormalized => "qualname_normalized",
            KeyKind::QualnameLower => "qualname_lower",
            KeyKind::QualnameLowerNormalized => "qualname_lower_normalized",
            KeyKind::NameExact => "name_exact",
            KeyKind::NameLower => "name_lower",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TokenKind {
    Exact,
    Normalized,
    Lower,
    LowerNormalized,
}

struct TokenKey {
    value: String,
    kind: TokenKind,
}

struct KeyRef {
    idx: usize,
    kind: KeyKind,
}

struct XrefMatch<'a> {
    symbol: &'a SymbolRef,
    confidence: f64,
    match_kind: KeyKind,
}

#[derive(Debug)]
struct SymbolRef {
    name: String,
    qualname: String,
    language: String,
    path: String,
}

struct SymbolRefIndex {
    symbols: Vec<SymbolRef>,
    by_key: HashMap<String, Vec<KeyRef>>,
}

impl SymbolRefIndex {
    fn build(db: &Db, graph_version: i64) -> Result<Self> {
        let records = db.list_symbol_refs(graph_version)?;
        Ok(Self::from_records(records))
    }

    fn from_records(records: Vec<SymbolRefRecord>) -> Self {
        let mut symbols = Vec::new();
        let mut by_key: HashMap<String, Vec<KeyRef>> = HashMap::new();
        for record in records {
            if record.kind == "module" || record.kind == "namespace" {
                continue;
            }
            let idx = symbols.len();
            symbols.push(SymbolRef {
                name: record.name,
                qualname: record.qualname,
                language: record.language,
                path: record.path,
            });
            let mut seen: HashSet<(String, KeyKind)> = HashSet::new();
            let symbol = &symbols[idx];
            insert_symbol_keys(&mut by_key, &mut seen, idx, symbol);
        }
        Self { symbols, by_key }
    }

    fn resolve_token(
        &self,
        token: &str,
        source_language: &str,
        source_path: &str,
    ) -> Option<XrefMatch<'_>> {
        if !token_eligible(token) {
            return None;
        }
        let mut best: Option<(usize, f64, KeyKind)> = None;
        let mut ambiguous = false;
        for token_key in token_keys(token) {
            let Some(candidates) = self.by_key.get(&token_key.value) else {
                continue;
            };
            for candidate in candidates {
                let symbol = &self.symbols[candidate.idx];
                let score = score_match(token, candidate.kind, token_key.kind);
                if score < XREF_MIN_CONFIDENCE {
                    continue;
                }
                if symbol.language == source_language {
                    // A same-language symbol in this very file wins over any
                    // cross-language coincidence; elsewhere it is just skipped.
                    if symbol.path == source_path {
                        return None;
                    }
                    continue;
                }
                match best {
                    Some((best_idx, best_score, _)) => {
                        if (score - best_score).abs() < 0.0001 {
                            if candidate.idx != best_idx {
                                ambiguous = true;
                            }
                        } else if score > best_score {
                            best = Some((candidate.idx, score, candidate.kind));
                            ambiguous = false;
                        }
                    }
                    None => {
                        best = Some((candidate.idx, score, candidate.kind));
                    }
                }
            }
        }
        if ambiguous {
            return None;
        }
        best.map(|(idx, score, kind)| XrefMatch {
            symbol: &self.symbols[idx],
            confidence: score,
            match_kind: kind,
        })
    }
}

fn insert_symbol_keys(
    by_key: &mut HashMap<String, Vec<KeyRef>>,
    seen: &mut HashSet<(String, KeyKind)>,
    idx: usize,
    symbol: &SymbolRef,
) {
    let qualname = symbol.qualname.trim();
    if !qualname.is_empty() {
        insert_key(
            by_key,
            seen,
            qualname.to_string(),
            idx,
            KeyKind::QualnameExact,
        );
        let normalized = normalize_separators(qualname);
        if normalized != qualname {
            insert_key(by_key, seen, normalized, idx, KeyKind::QualnameNormalized);
        }
        let lower = qualname.to_ascii_lowercase();
        if lower != qualname {
            insert_key(by_key, seen, lower.clone(), idx, KeyKind::QualnameLower);
        }
        let lower_norm = normalize_separators(&lower);
        if lower_norm != lower {
            insert_key(
                by_key,
                seen,
                lower_norm,
                idx,
                KeyKind::QualnameLowerNormalized,
            );
        }
    }
    let name = symbol.name.trim();
    if !name.is_empty() {
        insert_key(by_key, seen, name.to_string(), idx, KeyKind::NameExact);
        let lower = name.to_ascii_lowercase();
        if lower != name {
            insert_key(by_key, seen, lower, idx, KeyKind::NameLower);
        }
    }
}

fn insert_key(
    by_key: &mut HashMap<String, Vec<KeyRef>>,
    seen: &mut HashSet<(String, KeyKind)>,
    key: String,
    idx: usize,
    kind: KeyKind,
) {
    let entry = (key.clone(), kind);
    if !seen.insert(entry) {
        return;
    }
    by_key.entry(key).or_default().push(KeyRef { idx, kind });
}

fn token_keys(raw: &str) -> Vec<TokenKey> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut keys = Vec::new();
    push_token_key(&mut keys, trimmed.to_string(), TokenKind::Exact);
    let normalized = normalize_separators(trimmed);
    if normalized != trimmed {
        push_token_key(&mut keys, normalized, TokenKind::Normalized);
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower != trimmed {
        push_token_key(&mut keys, lower.clone(), TokenKind::Lower);
    }
    let lower_norm = normalize_separators(&lower);
    if lower_norm != lower {
        push_token_key(&mut keys, lower_norm, TokenKind::LowerNormalized);
    }
    keys
}

fn push_token_key(keys: &mut Vec<TokenKey>, value: String, kind: TokenKind) {
    if keys.last().map(|last| last.value != value).unwrap_or(true) {
        keys.push(TokenKey { value, kind });
    }
}

fn score_match(token: &str, key_kind: KeyKind, token_kind: TokenKind) -> f64 {
    let score = base_score(key_kind) + token_bonus(token) + token_penalty(token_kind);
    score.clamp(0.0, 1.0)
}

fn base_score(kind: KeyKind) -> f64 {
    match kind {
        KeyKind::QualnameExact => 0.7,
        KeyKind::QualnameNormalized => 0.65,
        KeyKind::QualnameLower => 0.6,
        KeyKind::QualnameLowerNormalized => 0.55,
        KeyKind::NameExact => 0.55,
        KeyKind::NameLower => 0.45,
    }
}

fn token_penalty(kind: TokenKind) -> f64 {
    match kind {
        TokenKind::Exact => 0.0,
        TokenKind::Normalized => -0.05,
        TokenKind::Lower => -0.1,
        TokenKind::LowerNormalized => -0.15,
    }
}

fn token_bonus(token: &str) -> f64 {
    let mut bonus = 0.0;
    let len = token.chars().count();
    if has_separator(token) {
        bonus += 0.2;
    }
    if len >= 12 {
        bonus += 0.2;
    } else if len >= 8 {
        bonus += 0.1;
    }
    if is_mixed_case(token) {
        bonus += 0.05;
    }
    bonus
}

fn is_mixed_case(token: &str) -> bool {
    let mut has_upper = false;
    let mut has_lower = false;
    for ch in token.chars() {
        if ch.is_ascii_uppercase() {
            has_upper = true;
        } else if ch.is_ascii_lowercase() {
            has_lower = true;
        }
        if has_upper && has_lower {
            return true;
        }
    }
    false
}

fn token_eligible(token: &str) -> bool {
    let trimmed = token.trim();
    if trimmed.len() < TOKEN_MIN_LEN {
        return false;
    }
    if !trimmed.chars().any(|ch| ch.is_ascii_alphabetic()) {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    if STOPWORDS.iter().any(|word| *word == lower) {
        return false;
    }
    if !has_separator(trimmed) && lower == trimmed && trimmed.len() < TOKEN_MIN_LOWER_LEN {
        return false;
    }
    true
}

fn has_separator(value: &str) -> bool {
    value.contains('.') || value.contains('/') || value.contains("::")
}

fn normalize_separators(value: &str) -> String {
    value.replace("::", ".").replace('/', ".")
}

fn extract_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut buf = String::new();
    for ch in text.chars() {
        if is_token_char(ch) {
            buf.push(ch);
        } else {
            flush_token(&mut tokens, &mut buf);
        }
    }
    flush_token(&mut tokens, &mut buf);
    tokens
}

fn flush_token(tokens: &mut Vec<String>, buf: &mut String) {
    if buf.is_empty() {
        return;
    }
    let trimmed = buf.trim_matches(|ch| ch == '.' || ch == ':' || ch == '/');
    let candidate = trimmed.trim();
    if token_eligible(candidate) {
        tokens.push(candidate.to_string());
    }
    buf.clear();
}

fn is_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | ':' | '/' | '$' | '@')
}

#[derive(Clone)]
struct StringLiteral {
    text: String,
    start_line: i64,
    end_line: i64,
    start_byte: i64,
    end_byte: i64,
}

/// How a lone `'` is read in a language.
#[derive(Clone, Copy, PartialEq)]
enum SingleQuote {
    /// `'...'` is a string (TS/JS, SQL, Bicep, ...).
    StringDelimiter,
    /// `'x'` / `'\n'` is a char literal and is not a string; a stray `'` is skipped.
    CharLiteral,
    /// Like `CharLiteral`, but a lone `'a` is a lifetime or loop label.
    CharOrLifetime,
}

/// Comment and non-string quote syntax of one language, used to keep
/// comments and char literals from being read as string content.
struct CommentSyntax {
    line_comments: &'static [&'static str],
    /// `/* ... */` block comments.
    block_comments: bool,
    /// Whether block comments nest (Rust, PostgreSQL, T-SQL).
    nested_block_comments: bool,
    single_quote: SingleQuote,
    /// JS/TS lexing: `/regex/` literals and `${}`-nesting template literals.
    js_lexing: bool,
}

const SLASH_COMMENTS: CommentSyntax = CommentSyntax {
    line_comments: &["//"],
    block_comments: true,
    nested_block_comments: false,
    single_quote: SingleQuote::StringDelimiter,
    js_lexing: false,
};

/// Comment syntax for `language`, or `None` when literals are scanned with
/// the plain quote scanner.
///
/// Languages that deliberately have no profile:
/// - `python`: `#` comments and docstrings are filtered after scanning
///   (`is_in_python_comment`, `is_python_docstring`).
/// - `markdown`: prose has no comment syntax; its quotes are ordinary text.
/// - `lua`: not an indexed language (no entry in `scan.rs`), so no id reaches
///   this function.
fn comment_syntax(language: &str) -> Option<&'static CommentSyntax> {
    const CSHARP: CommentSyntax = CommentSyntax {
        single_quote: SingleQuote::CharLiteral,
        ..SLASH_COMMENTS
    };
    const RUST: CommentSyntax = CommentSyntax {
        nested_block_comments: true,
        single_quote: SingleQuote::CharOrLifetime,
        ..SLASH_COMMENTS
    };
    const JS: CommentSyntax = CommentSyntax {
        js_lexing: true,
        ..SLASH_COMMENTS
    };
    const SQL: CommentSyntax = CommentSyntax {
        line_comments: &["--"],
        ..SLASH_COMMENTS
    };
    const NESTED_SQL: CommentSyntax = CommentSyntax {
        nested_block_comments: true,
        ..SQL
    };
    const YAML: CommentSyntax = CommentSyntax {
        line_comments: &["#"],
        block_comments: false,
        ..SLASH_COMMENTS
    };
    match language {
        "csharp" | "go" => Some(&CSHARP),
        "rust" => Some(&RUST),
        "typescript" | "tsx" | "javascript" => Some(&JS),
        "proto" | "bicep" => Some(&SLASH_COMMENTS),
        "sql" => Some(&SQL),
        "postgres" | "tsql" => Some(&NESTED_SQL),
        "yaml" => Some(&YAML),
        _ => None,
    }
}

fn scan_string_literals(source: &str) -> Vec<StringLiteral> {
    scan_literals(source, None)
}

/// Literal scan that respects the comment syntax of `language`.
fn scan_string_literals_for(source: &str, language: &str) -> Vec<StringLiteral> {
    scan_literals(source, comment_syntax(language))
}

/// The single literal scanner. Without a `syntax` every quote opens a
/// literal; with one, comments, char literals, lifetimes and regex literals
/// are skipped so their quotes cannot open a phantom string.
fn scan_literals(source: &str, syntax: Option<&CommentSyntax>) -> Vec<StringLiteral> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 1;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            line += 1;
            i += 1;
            continue;
        }
        if let Some(syntax) = syntax {
            if let Some(next) = skip_comment(bytes, i, syntax, &mut line) {
                i = next;
                continue;
            }
            if let Some(next) = skip_non_string_quote(source, i, syntax) {
                i = next;
                continue;
            }
            if syntax.js_lexing {
                if let Some(next) = skip_regex_literal(bytes, i) {
                    i = next;
                    continue;
                }
                if bytes[i] == b'`'
                    && let Some((literal, next, next_line)) = scan_js_template(source, i, line)
                {
                    out.push(literal);
                    i = next;
                    line = next_line;
                    continue;
                }
            }
            // A prefix letter glued to an identifier (`bar"`) is no string prefix.
            if i > 0 && is_ident_byte(bytes[i - 1]) && !matches!(bytes[i], b'"' | b'`' | b'\'') {
                i += 1;
                continue;
            }
        }
        if let Some((literal, next_i, next_line)) = scan_literal_at(source, bytes, i, line) {
            out.push(literal);
            i = next_i;
            line = next_line;
            continue;
        }
        i += 1;
    }
    out
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// If a comment starts at `i`, returns the index just past it and advances
/// `line` over any newlines it contains (a line comment stops before `\n`).
fn skip_comment(bytes: &[u8], i: usize, syntax: &CommentSyntax, line: &mut i64) -> Option<usize> {
    let rest = &bytes[i..];
    for marker in syntax.line_comments {
        // `#` starts a comment only at a word boundary (`a#b` in YAML is text).
        let boundary_ok = *marker != "#" || i == 0 || bytes[i - 1].is_ascii_whitespace();
        if rest.starts_with(marker.as_bytes()) && boundary_ok {
            let len = rest.iter().position(|&c| c == b'\n').unwrap_or(rest.len());
            return Some(i + len);
        }
    }
    if !syntax.block_comments || !rest.starts_with(b"/*") {
        return None;
    }
    let mut depth = 1;
    let mut j = i + 2;
    while j < bytes.len() && depth > 0 {
        if bytes[j] == b'\n' {
            *line += 1;
            j += 1;
        } else if bytes[j..].starts_with(b"*/") {
            depth -= 1;
            j += 2;
        } else if syntax.nested_block_comments && bytes[j..].starts_with(b"/*") {
            depth += 1;
            j += 2;
        } else {
            j += 1;
        }
    }
    Some(j)
}

/// Skips char literals (`'x'`, `'\u{1F600}'`, `b'x'`) and lifetimes (`'a`)
/// for languages where `'` does not delimit strings. Returns the resume index.
fn skip_non_string_quote(source: &str, i: usize, syntax: &CommentSyntax) -> Option<usize> {
    if syntax.single_quote == SingleQuote::StringDelimiter {
        return None;
    }
    let bytes = source.as_bytes();
    // `b'x'` byte literal: the prefix is skipped, the quote is handled next.
    if bytes[i] == b'b'
        && bytes.get(i + 1) == Some(&b'\'')
        && (i == 0 || !is_ident_byte(bytes[i - 1]))
    {
        return Some(i + 1);
    }
    if bytes[i] != b'\'' {
        return None;
    }
    Some(char_literal_end(source, i).unwrap_or(i + 1))
}

/// End (exclusive) of a char literal opening at `idx`, or `None` when the
/// quote is a lifetime, label or stray tick.
fn char_literal_end(source: &str, idx: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut j = idx + 1;
    let first = source.get(j..)?.chars().next()?;
    j += first.len_utf8();
    if first == '\\' {
        // Escape: one escaped char, then the digits of `\xNN`, `\uNNNN`, `\u{...}`.
        let escaped = source.get(j..)?.chars().next()?;
        j += escaped.len_utf8();
        if matches!(escaped, 'u' | 'U' | 'x') {
            if bytes.get(j) == Some(&b'{') {
                j += bytes[j..].iter().position(|&c| c == b'}')? + 1;
            } else {
                while bytes.get(j).is_some_and(u8::is_ascii_hexdigit) {
                    j += 1;
                }
            }
        }
    } else if first == '\n' || first == '\'' {
        return None;
    }
    (bytes.get(j) == Some(&b'\'')).then_some(j + 1)
}

/// True when a `/` at `i` begins a JS regex literal rather than a division,
/// judged by the previous significant token.
fn skip_regex_literal(bytes: &[u8], i: usize) -> Option<usize> {
    if bytes[i] != b'/' || matches!(bytes.get(i + 1), Some(b'/') | Some(b'*') | None) {
        return None;
    }
    let prev = bytes[..i].iter().rposition(|c| !c.is_ascii_whitespace());
    let starts_expression = match prev {
        None => true,
        Some(p) if is_ident_byte(bytes[p]) => {
            let word_start = bytes[..=p]
                .iter()
                .rposition(|&c| !is_ident_byte(c))
                .map_or(0, |q| q + 1);
            matches!(
                &bytes[word_start..=p],
                b"return"
                    | b"typeof"
                    | b"case"
                    | b"in"
                    | b"of"
                    | b"delete"
                    | b"void"
                    | b"throw"
                    | b"new"
                    | b"else"
                    | b"do"
                    | b"yield"
                    | b"await"
            )
        }
        Some(p) => matches!(
            bytes[p],
            b'(' | b','
                | b'='
                | b':'
                | b'['
                | b'!'
                | b'&'
                | b'|'
                | b'?'
                | b'{'
                | b'}'
                | b';'
                | b'+'
                | b'-'
                | b'*'
                | b'%'
                | b'<'
                | b'>'
                | b'~'
                | b'^'
        ),
    };
    if !starts_expression {
        return None;
    }
    let mut j = i + 1;
    let mut in_class = false;
    while j < bytes.len() {
        match bytes[j] {
            b'\n' => return None,
            b'\\' => j += 1,
            b'[' => in_class = true,
            b']' => in_class = false,
            b'/' if !in_class => return Some(j + 1),
            _ => {}
        }
        j += 1;
    }
    None
}

/// Scans a JS/TS template literal opening at `idx`, including `${ ... }`
/// holes that may contain strings, comments and nested templates.
fn scan_js_template(source: &str, idx: usize, line: i64) -> Option<(StringLiteral, usize, i64)> {
    let bytes = source.as_bytes();
    let close = template_close(bytes, idx)?;
    let newlines = bytes[idx..close].iter().filter(|&&c| c == b'\n').count() as i64;
    let literal = build_literal(source, idx, close, line, line + newlines, 1)?;
    Some((literal, close + 1, line + newlines))
}

/// Index of the backtick closing the template opened at `open`.
fn template_close(bytes: &[u8], open: usize) -> Option<usize> {
    let mut i = open + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'`' => return Some(i),
            b'$' if bytes.get(i + 1) == Some(&b'{') => i = template_hole_end(bytes, i + 2)?,
            _ => i += 1,
        }
    }
    None
}

/// Index just past the `}` closing a `${` hole whose body starts at `start`.
fn template_hole_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 1;
    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            b'`' => i = template_close(bytes, i)?,
            quote @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() && bytes[i] != quote && bytes[i] != b'\n' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() && !bytes[i..].starts_with(b"*/") {
                    i += 1;
                }
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn scan_literal_at(
    source: &str,
    bytes: &[u8],
    idx: usize,
    line: i64,
) -> Option<(StringLiteral, usize, i64)> {
    if let Some((literal, next, next_line)) = scan_verbatim_string(source, bytes, idx, line) {
        return Some((literal, next, next_line));
    }
    if let Some((literal, next, next_line)) = scan_rust_raw_string(source, bytes, idx, line) {
        return Some((literal, next, next_line));
    }
    if let Some((literal, next, next_line)) = scan_prefixed_string(source, bytes, idx, line) {
        return Some((literal, next, next_line));
    }
    let quote = bytes.get(idx).copied()?;
    if quote == b'"' || quote == b'\'' || quote == b'`' {
        return scan_quoted_string(source, bytes, idx, line, quote);
    }
    None
}

fn scan_verbatim_string(
    source: &str,
    bytes: &[u8],
    idx: usize,
    line: i64,
) -> Option<(StringLiteral, usize, i64)> {
    let start = match bytes.get(idx..idx + 3) {
        Some(slice) if slice == b"$@\"" => Some(idx + 2),
        Some(slice) if slice == b"@$\"" => Some(idx + 2),
        _ => {
            if bytes.get(idx..idx + 2) == Some(b"@\"") {
                Some(idx + 1)
            } else {
                None
            }
        }
    }?;
    scan_csharp_verbatim(source, bytes, start, line)
}

fn scan_csharp_verbatim(
    source: &str,
    bytes: &[u8],
    quote_idx: usize,
    line: i64,
) -> Option<(StringLiteral, usize, i64)> {
    let mut i = quote_idx + 1;
    let mut current_line = line;
    while i < bytes.len() {
        match bytes[i] {
            b'\n' => {
                current_line += 1;
                i += 1;
            }
            b'"' => {
                if bytes.get(i + 1) == Some(&b'"') {
                    i += 2;
                } else {
                    let literal = build_literal(source, quote_idx, i, line, current_line, 1)?;
                    return Some((literal, i + 1, current_line));
                }
            }
            _ => i += 1,
        }
    }
    None
}

fn scan_rust_raw_string(
    source: &str,
    bytes: &[u8],
    idx: usize,
    line: i64,
) -> Option<(StringLiteral, usize, i64)> {
    let mut start = idx;
    if bytes.get(idx) == Some(&b'b') && bytes.get(idx + 1) == Some(&b'r') {
        start += 1;
    }
    if bytes.get(start) != Some(&b'r') {
        return None;
    }
    let mut hash_count = 0;
    let mut j = start + 1;
    while bytes.get(j) == Some(&b'#') {
        hash_count += 1;
        j += 1;
    }
    if bytes.get(j) != Some(&b'"') {
        return None;
    }
    let quote_idx = j;
    let mut i = quote_idx + 1;
    let mut current_line = line;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            current_line += 1;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            let mut ok = true;
            for offset in 0..hash_count {
                if bytes.get(i + 1 + offset) != Some(&b'#') {
                    ok = false;
                    break;
                }
            }
            if ok {
                let literal =
                    build_literal(source, quote_idx, i, line, current_line, 1 + hash_count)?;
                return Some((literal, i + 1 + hash_count, current_line));
            }
        }
        i += 1;
    }
    None
}

fn scan_prefixed_string(
    source: &str,
    bytes: &[u8],
    idx: usize,
    line: i64,
) -> Option<(StringLiteral, usize, i64)> {
    let prefix = bytes.get(idx).copied()?;
    let quote_idx = match prefix {
        b'$' if bytes.get(idx + 1) == Some(&b'"') => idx + 1,
        b'b' | b'B' | b'r' | b'R' | b'u' | b'U' | b'f' | b'F' => {
            let next = bytes.get(idx + 1)?;
            if *next == b'"' || *next == b'\'' {
                idx + 1
            } else {
                return None;
            }
        }
        _ => return None,
    };
    let quote = bytes.get(quote_idx).copied()?;
    scan_quoted_string(source, bytes, quote_idx, line, quote)
}

fn scan_quoted_string(
    source: &str,
    bytes: &[u8],
    quote_idx: usize,
    line: i64,
    quote: u8,
) -> Option<(StringLiteral, usize, i64)> {
    let is_triple = quote != b'`'
        && bytes.get(quote_idx + 1) == Some(&quote)
        && bytes.get(quote_idx + 2) == Some(&quote);
    if is_triple {
        return scan_triple_quoted(source, bytes, quote_idx, line, quote);
    }
    scan_simple_quoted(source, bytes, quote_idx, line, quote)
}

fn scan_simple_quoted(
    source: &str,
    bytes: &[u8],
    quote_idx: usize,
    line: i64,
    quote: u8,
) -> Option<(StringLiteral, usize, i64)> {
    let mut i = quote_idx + 1;
    let mut current_line = line;
    while i < bytes.len() {
        match bytes[i] {
            b'\n' => {
                current_line += 1;
                i += 1;
            }
            b'\\' => {
                i += 2;
            }
            value if value == quote => {
                let literal = build_literal(source, quote_idx, i, line, current_line, 1)?;
                return Some((literal, i + 1, current_line));
            }
            _ => i += 1,
        }
    }
    None
}

fn scan_triple_quoted(
    source: &str,
    bytes: &[u8],
    quote_idx: usize,
    line: i64,
    quote: u8,
) -> Option<(StringLiteral, usize, i64)> {
    let mut i = quote_idx + 3;
    let mut current_line = line;
    while i + 2 < bytes.len() {
        if bytes[i] == b'\n' {
            current_line += 1;
            i += 1;
            continue;
        }
        if bytes[i] == quote && bytes.get(i + 1) == Some(&quote) && bytes.get(i + 2) == Some(&quote)
        {
            let literal = build_literal(source, quote_idx, i, line, current_line, 3)?;
            return Some((literal, i + 3, current_line));
        }
        i += 1;
    }
    None
}

fn build_literal(
    source: &str,
    quote_idx: usize,
    end_idx: usize,
    start_line: i64,
    end_line: i64,
    closing_len: usize,
) -> Option<StringLiteral> {
    let text_start = quote_idx + 1;
    let text_end = end_idx;
    let text = source.get(text_start..text_end)?.to_string();
    Some(StringLiteral {
        text,
        start_line,
        end_line,
        start_byte: quote_idx as i64,
        end_byte: (end_idx + closing_len) as i64,
    })
}

pub(crate) fn normalize_route_literal(raw: &str) -> Option<String> {
    let mut value = raw.trim();
    if value.is_empty() || value.len() > ROUTE_MAX_LEN {
        return None;
    }
    if value.chars().any(|ch| ch.is_whitespace()) {
        return None;
    }
    if value.contains('\\') || value.starts_with("./") || value.starts_with("../") {
        return None;
    }
    if let Some(stripped) = strip_url_prefix(value) {
        value = stripped;
    }
    if !value.starts_with('/') {
        return None;
    }
    let value = strip_query_fragment(value);
    if !value.contains('/') {
        return None;
    }
    let collapsed = collapse_slashes(value);
    let mut path = collapsed;
    while path.len() > 1 && path.ends_with('/') {
        path.pop();
    }
    let mut out = String::new();
    out.push('/');
    let mut has_alpha = false;
    let trimmed = path.trim_start_matches('/');
    for (idx, segment) in trimmed.split('/').enumerate() {
        if idx > 0 {
            out.push('/');
        }
        let normalized = normalize_route_segment(segment);
        if normalized.chars().any(|ch| ch.is_ascii_alphabetic()) {
            has_alpha = true;
        }
        out.push_str(&normalized);
    }
    if !has_alpha {
        return None;
    }
    Some(out.to_ascii_lowercase())
}

fn strip_url_prefix(value: &str) -> Option<&str> {
    let stripped = if let Some(rest) = value.strip_prefix("http://") {
        rest
    } else {
        value.strip_prefix("https://")?
    };
    let slash = stripped.find('/')?;
    Some(&stripped[slash..])
}

fn strip_query_fragment(value: &str) -> &str {
    let mut end = value.len();
    if let Some(idx) = value.find('?') {
        end = end.min(idx);
    }
    if let Some(idx) = value.find('#') {
        end = end.min(idx);
    }
    &value[..end]
}

fn collapse_slashes(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_slash = false;
    for ch in value.chars() {
        if ch == '/' {
            if !last_slash {
                out.push(ch);
                last_slash = true;
            }
        } else {
            out.push(ch);
            last_slash = false;
        }
    }
    out
}

fn normalize_route_segment(segment: &str) -> String {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with(':')
        || trimmed.starts_with('{')
        || trimmed.starts_with('<')
        || trimmed.starts_with('$')
    {
        return "{}".to_string();
    }
    if trimmed.contains("${") || trimmed.contains('*') {
        return "{}".to_string();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return "{}".to_string();
    }
    if looks_like_uuid(trimmed) {
        return "{}".to_string();
    }
    trimmed.to_string()
}

fn looks_like_uuid(segment: &str) -> bool {
    let mut hex = 0usize;
    let mut dash = 0usize;
    for ch in segment.chars() {
        if ch == '-' {
            dash += 1;
            continue;
        }
        if ch.is_ascii_hexdigit() {
            hex += 1;
            continue;
        }
        return false;
    }
    if dash > 0 {
        return hex >= 16;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: i64, qualname: &str, language: &str, path: &str) -> SymbolRefRecord {
        SymbolRefRecord {
            id,
            name: qualname.rsplit('.').next().unwrap().to_string(),
            qualname: qualname.to_string(),
            kind: "class".to_string(),
            language: language.to_string(),
            path: path.to_string(),
        }
    }

    #[test]
    fn same_file_same_language_match_suppresses_cross_language_xref() {
        let index = SymbolRefIndex::from_records(vec![
            rec(
                1,
                "broker._core.ReceivedMessage",
                "python",
                "broker/_core.py",
            ),
            rec(
                2,
                "Dpb.Common.Messaging.ReceivedMessage",
                "csharp",
                "Msg.cs",
            ),
        ]);
        assert!(
            index
                .resolve_token("ReceivedMessage", "python", "broker/_core.py")
                .is_none()
        );
        // Without a same-file symbol the cross-language match still works.
        let index = SymbolRefIndex::from_records(vec![rec(
            2,
            "Dpb.Common.Messaging.ReceivedMessage",
            "csharp",
            "Msg.cs",
        )]);
        assert!(
            index
                .resolve_token("ReceivedMessage", "python", "broker/_core.py")
                .is_some()
        );
    }

    #[test]
    fn same_language_homonym_in_other_file_keeps_cross_language_xref() {
        let index = SymbolRefIndex::from_records(vec![
            rec(1, "Other.WidgetRegistryEntry", "python", "other.py"),
            rec(2, "Db.WidgetRegistryEntry", "sql", "db.sql"),
        ]);
        let hit = index
            .resolve_token("WidgetRegistryEntry", "python", "app.py")
            .unwrap();
        assert_eq!(hit.symbol.qualname, "Db.WidgetRegistryEntry");
    }

    #[test]
    fn python_docstrings_are_detected_but_sql_strings_are_not() {
        let src = "def f():\n    \"\"\"Talks to DataProxy.\"\"\"\n    q = \"\"\"SELECT 1\"\"\"\n";
        let lits = scan_string_literals(src);
        assert_eq!(lits.len(), 2);
        assert!(is_python_docstring(src, &lits[0]));
        assert!(!is_python_docstring(src, &lits[1]));
        let module = "\"\"\"Module doc.\"\"\"\nx = 1\n";
        let lits = scan_string_literals(module);
        assert!(is_python_docstring(module, &lits[0]));
    }

    #[test]
    fn python_docstring_after_shebang_and_with_prefix() {
        let src = "#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\nr\"\"\"Doc.\"\"\"\n";
        let lits = scan_string_literals(src);
        assert!(is_python_docstring(src, &lits[0]));
        let src = "class A:\n    f\"\"\"Doc {x}.\"\"\"\n";
        let lits = scan_string_literals(src);
        assert!(is_python_docstring(src, &lits[0]));
    }

    #[test]
    fn python_comment_literals_are_detected() {
        let src = "x = 1  # see \"DataProxy\" here\ny = \"a#b\" + \"Real\"\n";
        let lits = scan_string_literals(src);
        assert_eq!(lits.len(), 3);
        assert!(is_in_python_comment(src, &lits[0]));
        assert!(!is_in_python_comment(src, &lits[1]));
        assert!(!is_in_python_comment(src, &lits[2]));
    }

    fn texts(src: &str, lang: &str) -> Vec<String> {
        scan_string_literals_for(src, lang)
            .into_iter()
            .map(|l| l.text)
            .collect()
    }

    /// Fixtures tag every comment with `CMT`. No literal may contain a tagged
    /// comment, so both whole-line and trailing comments are caught.
    fn assert_no_literal_swallows_comment(src: &str, lang: &str) {
        for lit in scan_string_literals_for(src, lang) {
            let span = &src[lit.start_byte as usize..lit.end_byte as usize];
            assert!(
                !span.contains("CMT"),
                "{lang}: literal swallowed a comment: {span:?}"
            );
        }
    }

    #[test]
    fn csharp_comment_apostrophe_does_not_open_literal() {
        let src = "// CMT the proc's own SELECT\nvar r = SecretHandler;\nint x = 1; // CMT it's\nvar s = \"it's done\";\n";
        assert_eq!(texts(src, "csharp"), vec!["it's done"]);
        assert_no_literal_swallows_comment(src, "csharp");
        let block = "/* CMT don't \" */\nvar r = SecretHandler;\nvar s = \"Real\";\n";
        assert_eq!(texts(block, "csharp"), vec!["Real"]);
        assert_no_literal_swallows_comment(block, "csharp");
    }

    #[test]
    fn csharp_string_forms_and_char_literals() {
        let src = "var a = @\"say \"\"hi\"\" DataProxy\";\nvar c = '\"';\nvar d = '\\'';\nvar e = '\\u0041';\nvar r = \"\"\"\nraw \"quoted\" Thing\n\"\"\";\n";
        let t = texts(src, "csharp");
        assert_eq!(t.len(), 2, "{t:?}");
        assert!(t[0].contains("DataProxy"));
        assert!(t[1].contains("raw \"quoted\" Thing"));
        let interp = "var a = $@\"x \"\"{y}\"\" Alpha\";\nvar b = $\"\"\"\nq \"z\" {v} Beta\n\"\"\";\nvar c = @$\"Gamma \"\"q\"\"\";\n";
        let t = texts(interp, "csharp");
        assert_eq!(t.len(), 3, "{t:?}");
        assert!(t[0].contains("Alpha") && t[1].contains("Beta") && t[2].contains("Gamma"));
    }

    #[test]
    fn comment_markers_inside_strings_are_not_comments() {
        let cases = [
            ("csharp", "var u = \"http://x\"; var v = \"Real\";\n"),
            ("rust", "let u = \"a // b\"; let v = \"Real\";\n"),
            ("typescript", "const u = \"a // b\"; const v = 'Real';\n"),
            ("go", "u := \"a // b\"; v := \"Real\"\n"),
            ("sql", "SELECT 'a -- b', 'Real';\n"),
        ];
        for (lang, src) in cases {
            let t = texts(src, lang);
            assert_eq!(t.last().map(String::as_str), Some("Real"), "{lang}: {t:?}");
            assert_eq!(t.len(), 2, "{lang}: {t:?}");
        }
        for (lang, src) in [
            ("csharp", "var u = \"/* x\"; var v = \"Real\"; // CMT\n"),
            ("rust", "let u = \"/* x\"; let v = \"Real\";\n"),
            ("typescript", "const u = '/* x'; const v = \"Real\";\n"),
            ("sql", "SELECT '/* x', 'Real';\n"),
        ] {
            let t = texts(src, lang);
            assert_eq!(t.len(), 2, "{lang}: {t:?}");
            assert_eq!(t[1], "Real");
        }
    }

    #[test]
    fn rust_lifetimes_and_comments_do_not_open_literals() {
        let src = "// CMT it's a helper\nfn f<'a>(x: &'a str) -> &'static str {\n    let _m = CancellationRegistry;\n    let c = 'x'; // CMT don't\n    let q = '\"';\n    \"real\"\n}\n";
        assert_eq!(texts(src, "rust"), vec!["real"]);
        assert_no_literal_swallows_comment(src, "rust");
        let nested = "/* CMT a /* don't */ still \" */\nlet s = \"ok\";\n";
        assert_eq!(texts(nested, "rust"), vec!["ok"]);
        let label = "'outer: loop { break 'outer; } let s = \"lab\";\n";
        assert_eq!(texts(label, "rust"), vec!["lab"]);
    }

    #[test]
    fn rust_string_and_char_forms() {
        let raw = "let s = r#\"has \"quote\" inside\"#;\n";
        assert_eq!(texts(raw, "rust"), vec!["has \"quote\" inside"]);
        let raw2 = "let s = r##\"a \"# b\"##; let t = \"after\";\n";
        assert_eq!(texts(raw2, "rust"), vec!["a \"# b", "after"]);
        let bytes = "let a = br\"raw \\ bytes\"; let b = b'x'; let s = \"after\";\n";
        assert_eq!(texts(bytes, "rust"), vec!["raw \\ bytes", "after"]);
        let chars = "let a = '\\u{1F600}'; let b = '\\''; let c = '\\\\'; let d = '\"'; let s = \"after\";\n";
        assert_eq!(texts(chars, "rust"), vec!["after"]);
    }

    #[test]
    fn typescript_and_javascript_comments_and_templates() {
        for lang in ["typescript", "tsx", "javascript"] {
            let src = "// CMT don't\nconst r = SecretHandler;\nconst n = 1; // CMT it's\n/* CMT isn't \" */\nconst t = `tpl ${x} it's`;\nconst s = 'real';\n";
            assert_eq!(texts(src, lang), vec!["tpl ${x} it's", "real"], "{lang}");
            assert_no_literal_swallows_comment(src, lang);
        }
    }

    #[test]
    fn typescript_regex_literals_and_nested_templates() {
        for lang in ["typescript", "javascript"] {
            let re =
                "const a = x.replace(/'/g, ''); const b = /[\"'`]/.test(y); const c = \"real\";\n";
            assert_eq!(texts(re, lang), vec!["", "real"], "{lang}");
            let div = "const q = a / b; const r = c / d; const s = 'real';\n";
            assert_eq!(texts(div, lang), vec!["real"], "{lang}");
            let nested = "const t = `a ${ `b ${ \"c`\" } d` } e`; const s = 'real';\n";
            assert_eq!(
                texts(nested, lang),
                vec!["a ${ `b ${ \"c`\" } d` } e", "real"],
                "{lang}"
            );
            let obj = "const t = `x ${ {a: 1}.a } y`; const s = 'real';\n";
            assert_eq!(
                texts(obj, lang),
                vec!["x ${ {a: 1}.a } y", "real"],
                "{lang}"
            );
        }
    }

    #[test]
    fn go_comments_runes_and_raw_strings() {
        let src = "// CMT don't\nx := SecretHandler\n/* CMT it's */\nr := '\\''\ns := `raw \"q\"`\nt := \"real\" // CMT it's\n";
        assert_eq!(texts(src, "go"), vec!["raw \"q\"", "real"]);
        assert_no_literal_swallows_comment(src, "go");
    }

    #[test]
    fn sql_dash_and_block_comments() {
        let src = "-- CMT the proc's SELECT\nSELECT SecretHandler; -- CMT it's\n/* CMT don't */\nSELECT 'real';\n";
        assert_eq!(texts(src, "sql"), vec!["real"]);
        assert_no_literal_swallows_comment(src, "sql");
        let nested = "/* CMT a /* b */ don't */ SELECT 'real';\n";
        assert_eq!(texts(nested, "postgres"), vec!["real"]);
        assert_eq!(texts(nested, "tsql"), vec!["real"]);
    }

    #[test]
    fn bicep_and_yaml_comments() {
        let bicep = "// CMT it's\nparam a string = 'real'\n/* CMT don't */\n";
        assert_eq!(texts(bicep, "bicep"), vec!["real"]);
        let yaml = "# CMT it's\nkey: \"real\" # CMT don't\nurl: a#b'c'\n";
        assert_eq!(texts(yaml, "yaml"), vec!["real", "c"]);
        assert_no_literal_swallows_comment(yaml, "yaml");
    }

    #[test]
    fn unprofiled_languages_use_plain_scanning() {
        assert!(comment_syntax("python").is_none());
        assert!(comment_syntax("markdown").is_none());
        assert_eq!(texts("it's \"a\"", "markdown").len(), 1);
    }

    #[test]
    fn genuine_literals_still_yield_xref_tokens() {
        let src = "// CMT it's\nvar s = \"DataProxy.Query\";\n";
        let t = texts(src, "csharp");
        assert_eq!(t, vec!["DataProxy.Query"]);
        assert!(!extract_tokens(&t[0]).is_empty());
    }

    #[test]
    fn normalize_route_literal_handles_paths() {
        assert_eq!(
            normalize_route_literal("/api/users/123").as_deref(),
            Some("/api/users/{}")
        );
        assert_eq!(
            normalize_route_literal("https://example.com/api/users/:id").as_deref(),
            Some("/api/users/{}")
        );
        assert!(normalize_route_literal("api/users").is_none());
        assert!(normalize_route_literal("./src/api/users").is_none());
    }
}
