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
    file.language != "markdown"
}

fn collect_xref_edges(
    db: &Db,
    index: &SymbolRefIndex,
    file: &ScannedFile,
    source: &str,
    graph_version: i64,
) -> Result<Vec<EdgeInput>> {
    let literals = scan_string_literals(source);
    if literals.is_empty() {
        return Ok(Vec::new());
    }
    let mut edges_by_key: HashMap<(String, String), EdgeInput> = HashMap::new();
    let mut line_cache: HashMap<i64, Option<String>> = HashMap::new();
    for literal in literals {
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
            let Some(match_info) = index.resolve_token(&token, &file.language) else {
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
            if record.language == "markdown" {
                continue;
            }
            let idx = symbols.len();
            symbols.push(SymbolRef {
                name: record.name,
                qualname: record.qualname,
                language: record.language,
            });
            let mut seen: HashSet<(String, KeyKind)> = HashSet::new();
            let symbol = &symbols[idx];
            insert_symbol_keys(&mut by_key, &mut seen, idx, symbol);
        }
        Self { symbols, by_key }
    }

    fn resolve_token(&self, token: &str, source_language: &str) -> Option<XrefMatch<'_>> {
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
                if symbol.language == source_language {
                    continue;
                }
                let score = score_match(token, candidate.kind, token_key.kind);
                if score < XREF_MIN_CONFIDENCE {
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
    let mut score = base_score(key_kind) + token_bonus(token) + token_penalty(token_kind);
    if score < 0.0 {
        score = 0.0;
    } else if score > 1.0 {
        score = 1.0;
    }
    score
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

fn scan_string_literals(source: &str) -> Vec<StringLiteral> {
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
        b'$' => {
            if bytes.get(idx + 1) == Some(&b'"') {
                idx + 1
            } else {
                return None;
            }
        }
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
    } else if let Some(rest) = value.strip_prefix("https://") {
        rest
    } else {
        return None;
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
    use super::normalize_route_literal;

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
