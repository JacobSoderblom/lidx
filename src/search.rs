use crate::config::Config;
use crate::indexer::scan;
use crate::model::{GrepHit, SearchHit};
use crate::util;
use anyhow::{Context, Result, bail};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchScope {
    Code,
    Docs,
    Tests,
    Examples,
    All,
}

#[derive(Debug, Clone, Copy)]
pub struct SearchOptions<'a> {
    pub languages: Option<&'a [String]>,
    pub scope: Option<SearchScope>,
    pub exclude_generated: bool,
    pub rank: bool,
    pub no_ignore: bool,
    pub paths: Option<&'a [String]>,
}

impl<'a> SearchOptions<'a> {
    pub fn new(languages: Option<&'a [String]>) -> Self {
        Self {
            languages,
            scope: None,
            exclude_generated: false,
            rank: true,
            no_ignore: false,
            paths: None,
        }
    }
}

pub fn parse_scope(raw: Option<&str>) -> Result<Option<SearchScope>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(None);
    }
    let scope = match value.as_str() {
        "code" | "code-only" | "code_only" => SearchScope::Code,
        "docs" | "doc" | "docs-only" | "docs_only" => SearchScope::Docs,
        "tests" | "test" | "tests-only" | "tests_only" => SearchScope::Tests,
        "examples" | "example" | "examples-only" | "examples_only" | "samples" => {
            SearchScope::Examples
        }
        "all" | "any" => SearchScope::All,
        _ => bail!("unknown scope: {raw}"),
    };
    Ok(Some(scope))
}

pub fn search_text(
    repo_root: &Path,
    query: &str,
    limit: usize,
    options: SearchOptions<'_>,
) -> Result<Vec<SearchHit>> {
    if options.languages.map_or(false, |langs| langs.is_empty()) {
        return Ok(Vec::new());
    }
    if options.paths.map_or(false, |paths| paths.is_empty()) {
        return Ok(Vec::new());
    }
    let fetch_limit = rank_fetch_limit(limit, options.rank);
    let fixed_string = query_disables_fuzzy(query);
    let exact_hits = match search_with_rg(repo_root, query, fetch_limit, options, fixed_string) {
        Ok(results) => results,
        Err(_) => search_fallback_exact(repo_root, query, fetch_limit, options)?,
    };
    let mut seen = HashSet::new();
    let mut scored: Vec<ScoredHit> = Vec::with_capacity(exact_hits.len());
    for hit in exact_hits {
        let key = (hit.path.clone(), hit.line, hit.column);
        if seen.insert(key) {
            let score = score_exact_hit(&hit);
            let hit = apply_match_metadata(hit, score, "exact");
            scored.push(ScoredHit { hit, score });
        }
    }
    let mut needs_fuzzy = scored.len() < limit;
    if !needs_fuzzy && options.rank {
        let has_codeish = scored
            .iter()
            .any(|entry| path_bonus(&entry.hit.path) >= 0.0);
        if !has_codeish {
            needs_fuzzy = true;
        }
    }
    if needs_fuzzy && !fixed_string {
        let query_tokens = collect_query_tokens(query);
        if should_run_fuzzy(&query_tokens) {
            let fuzzy_hits = search_fuzzy(repo_root, &query_tokens, fetch_limit, options)?;
            for fuzzy in fuzzy_hits {
                let key = (fuzzy.hit.path.clone(), fuzzy.hit.line, fuzzy.hit.column);
                if seen.insert(key) {
                    let score = score_fuzzy_hit(&fuzzy);
                    let hit = apply_match_metadata(fuzzy.hit, score, "fuzzy");
                    scored.push(ScoredHit { hit, score });
                }
            }
        }
    }
    if options.rank {
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.hit.path.cmp(&b.hit.path))
                .then_with(|| a.hit.line.cmp(&b.hit.line))
                .then_with(|| a.hit.column.cmp(&b.hit.column))
        });
    }
    scored.truncate(limit);
    Ok(scored.into_iter().map(|entry| entry.hit).collect())
}

pub fn grep_text(
    repo_root: &Path,
    query: &str,
    limit: usize,
    include_text: bool,
    options: SearchOptions<'_>,
) -> Result<Vec<GrepHit>> {
    if options.languages.map_or(false, |langs| langs.is_empty()) {
        return Ok(Vec::new());
    }
    if options.paths.map_or(false, |paths| paths.is_empty()) {
        return Ok(Vec::new());
    }
    let fetch_limit = rank_fetch_limit(limit, options.rank);
    let hits = if let Ok(results) = search_with_rg(repo_root, query, fetch_limit, options, false) {
        results
    } else {
        search_fallback_exact(repo_root, query, fetch_limit, options)?
    };
    let mut seen = HashSet::new();
    let mut scored: Vec<ScoredHit> = Vec::with_capacity(hits.len());
    for hit in hits {
        let key = (hit.path.clone(), hit.line, hit.column);
        if seen.insert(key) {
            let score = score_exact_hit(&hit);
            let hit = apply_match_metadata(hit, score, "exact");
            scored.push(ScoredHit { hit, score });
        }
    }
    if options.rank {
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.hit.path.cmp(&b.hit.path))
                .then_with(|| a.hit.line.cmp(&b.hit.line))
                .then_with(|| a.hit.column.cmp(&b.hit.column))
        });
    }
    scored.truncate(limit);
    Ok(scored
        .into_iter()
        .map(|entry| GrepHit {
            path: entry.hit.path,
            line: entry.hit.line,
            column: entry.hit.column,
            line_text: if include_text {
                Some(entry.hit.line_text)
            } else {
                None
            },
            context: None,
            enclosing_symbol: None,
            score: entry.hit.score,
            reasons: entry.hit.reasons,
            engine: None,
            next_hops: None,
        })
        .collect())
}

fn search_with_rg(
    repo_root: &Path,
    query: &str,
    max_hits: usize,
    options: SearchOptions<'_>,
    fixed_string: bool,
) -> Result<Vec<SearchHit>> {
    let mut glob_args = Vec::new();
    if let Some(languages) = options.languages {
        let exts = scan::extensions_for_languages(languages);
        if exts.is_empty() {
            return Ok(Vec::new());
        }
        for ext in exts {
            glob_args.push(format!("*.{ext}"));
        }
    }

    let build_cmd = |allow_no_require_git: bool, allow_timeout: bool| {
        let mut cmd = Command::new("rg");
        cmd.arg("--json").arg("-n").arg("--column");
        // Security: Prevent ReDoS and resource exhaustion
        if allow_timeout {
            let timeout = format!("{}s", Config::get().search_timeout_secs);
            cmd.arg("--timeout").arg(&timeout);
        }
        cmd.arg("--regex-size-limit").arg("10M");
        cmd.arg("--dfa-size-limit").arg("10M");
        if fixed_string {
            cmd.arg("--fixed-strings");
        }
        if options.no_ignore {
            cmd.arg("--no-ignore");
        } else if allow_no_require_git {
            cmd.arg("--no-require-git");
        }
        for glob in &glob_args {
            cmd.arg("-g").arg(glob);
        }
        cmd
    };

    let mut output = build_cmd(true, true)
        .arg(query)
        .arg(repo_root)
        .output()
        .with_context(|| "run rg")?;

    if !output.status.success()
        && !options.no_ignore
        && rg_flag_unsupported(&output, "--no-require-git")
    {
        output = build_cmd(false, true)
            .arg(query)
            .arg(repo_root)
            .output()
            .with_context(|| "run rg")?;
    }
    // Retry without --timeout if rg doesn't support it
    if !output.status.success() && rg_flag_unsupported(&output, "--timeout") {
        output = build_cmd(!options.no_ignore, false)
            .arg(query)
            .arg(repo_root)
            .output()
            .with_context(|| "run rg")?;
    }
    let exit_code = output.status.code().unwrap_or(2);
    if exit_code == 1 {
        // Exit code 1 = no matches found. Return empty.
        return Ok(Vec::new());
    }
    if exit_code != 0 {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("exceeded") || stderr.contains("timeout") {
            eprintln!(
                "lidx: Security: Search timeout after {}s",
                Config::get().search_timeout_secs
            );
        }
        anyhow::bail!("rg failed (exit code {}): {}", exit_code, stderr.trim());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut hits = Vec::new();
    let lang_set = options
        .languages
        .map(|langs| langs.iter().map(String::as_str).collect::<HashSet<_>>());
    for line in stdout.lines() {
        if hits.len() >= max_hits {
            break;
        }
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if value.get("type").and_then(|t| t.as_str()) != Some("match") {
            continue;
        }
        let data = &value["data"];
        let raw_path = data["path"]["text"].as_str().unwrap_or("");
        let path = match std::path::Path::new(raw_path).strip_prefix(repo_root) {
            Ok(rel) => util::normalize_path(rel),
            Err(_) => raw_path.to_string(),
        };
        let line_number = data["line_number"].as_u64().unwrap_or(0) as usize;
        let line_text = data["lines"]["text"]
            .as_str()
            .unwrap_or("")
            .trim_end()
            .to_string();
        let column = data["submatches"]
            .get(0)
            .and_then(|v| v["start"].as_u64())
            .map(|v| v as usize + 1)
            .unwrap_or(1);
        if let Some(ref set) = lang_set {
            let path_lang = scan::language_for_path(std::path::Path::new(&path));
            if path_lang.map_or(true, |lang| !set.contains(lang)) {
                continue;
            }
        }
        if !scope_allows(
            &path,
            options.scope,
            options.exclude_generated,
            options.paths,
        ) {
            continue;
        }
        hits.push(SearchHit {
            path,
            line: line_number,
            column,
            line_text,
            context: None,
            enclosing_symbol: None,
            score: None,
            reasons: None,
            engine: None,
            next_hops: None,
        });
    }
    Ok(hits)
}

fn query_disables_fuzzy(query: &str) -> bool {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.contains('/') || trimmed.contains("://") || trimmed.contains('\\')
}

fn rg_flag_unsupported(output: &std::process::Output, flag: &str) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    stderr.contains(flag)
}

fn search_fallback_exact(
    repo_root: &Path,
    query: &str,
    max_hits: usize,
    options: SearchOptions<'_>,
) -> Result<Vec<SearchHit>> {
    let mut hits = Vec::new();
    let files = scan::scan_repo_with_options(repo_root, scan::ScanOptions::new(options.no_ignore))?;
    let lang_set = options
        .languages
        .map(|langs| langs.iter().map(String::as_str).collect::<HashSet<_>>());
    for file in files {
        if hits.len() >= max_hits {
            break;
        }
        if let Some(ref set) = lang_set {
            if !set.contains(file.language.as_str()) {
                continue;
            }
        }
        if !scope_allows(
            &file.rel_path,
            options.scope,
            options.exclude_generated,
            options.paths,
        ) {
            continue;
        }
        let content = match std::fs::read_to_string(&file.abs_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        for (idx, line) in content.lines().enumerate() {
            if let Some(pos) = line.find(query) {
                hits.push(SearchHit {
                    path: file.rel_path.clone(),
                    line: idx + 1,
                    column: pos + 1,
                    line_text: line.to_string(),
                    context: None,
                    enclosing_symbol: None,
                    score: None,
                    reasons: None,
                    engine: None,
                    next_hops: None,
                });
                if hits.len() >= max_hits {
                    break;
                }
            }
        }
    }
    Ok(hits)
}

const RANK_FETCH_MULTIPLIER: usize = 4;
const RANK_FETCH_CAP: usize = 500;
const EXACT_BASE_SCORE: f32 = 10.0;

fn rank_fetch_limit(limit: usize, rank: bool) -> usize {
    if !rank {
        return limit;
    }
    let scaled = limit.saturating_mul(RANK_FETCH_MULTIPLIER);
    let scaled = scaled.max(limit);
    scaled.min(RANK_FETCH_CAP)
}

struct ScoredHit {
    hit: SearchHit,
    score: f32,
}

fn apply_match_metadata(mut hit: SearchHit, score: f32, reason: &str) -> SearchHit {
    hit.score = Some(score);
    hit.reasons = Some(vec![reason.to_string()]);
    hit
}

fn score_exact_hit(hit: &SearchHit) -> f32 {
    EXACT_BASE_SCORE + path_bonus(&hit.path)
}

fn score_fuzzy_hit(hit: &FuzzyHit) -> f32 {
    // Scale up base fuzzy score to reduce cliff between exact (10.0+) and fuzzy (~3.5)
    let base = hit.score * 1.5;
    base + path_bonus(&hit.hit.path)
}

struct FuzzyHit {
    hit: SearchHit,
    score: f32,
}

#[derive(Clone, Copy)]
struct BestMatch {
    score: f32,
}

fn search_fuzzy(
    repo_root: &Path,
    query_tokens: &[String],
    limit: usize,
    options: SearchOptions<'_>,
) -> Result<Vec<FuzzyHit>> {
    if query_tokens.is_empty() {
        return Ok(Vec::new());
    }
    let files = scan::scan_repo_with_options(repo_root, scan::ScanOptions::new(options.no_ignore))?;
    let lang_set = options
        .languages
        .map(|langs| langs.iter().map(String::as_str).collect::<HashSet<_>>());
    let mut hits = Vec::new();
    for file in files {
        if let Some(ref set) = lang_set {
            if !set.contains(file.language.as_str()) {
                continue;
            }
        }
        if !scope_allows(
            &file.rel_path,
            options.scope,
            options.exclude_generated,
            options.paths,
        ) {
            continue;
        }
        let content = match std::fs::read_to_string(&file.abs_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let mut best_per_query = vec![BestMatch { score: 0.0 }; query_tokens.len()];
        let mut best_lines: Vec<Option<usize>> = vec![None; query_tokens.len()];
        let mut matching_line_count = 0usize;
        let mut best_line_score = 0.0f32;
        let mut best_line_num = 0usize;
        let mut best_line_col = 1usize;
        let mut best_line_text = String::new();
        for (idx, line) in content.lines().enumerate() {
            let tokens = extract_line_tokens(line);
            if tokens.is_empty() {
                continue;
            }
            let mut line_score = 0.0f32;
            let mut line_best_score = 0.0f32;
            let mut line_best_col = 1usize;
            let mut line_matched_any = false;
            for (q_idx, query) in query_tokens.iter().enumerate() {
                let mut best = 0.0f32;
                let mut best_col = 1usize;
                for token in &tokens {
                    let score = token_similarity(query, &token.token);
                    if score > best {
                        best = score;
                        best_col = token.column;
                    }
                }
                if best > 0.0 {
                    line_score += best;
                    line_matched_any = true;
                    if best > best_per_query[q_idx].score {
                        best_per_query[q_idx].score = best;
                        best_lines[q_idx] = Some(idx);
                    }
                    if best > line_best_score {
                        line_best_score = best;
                        line_best_col = best_col;
                    }
                }
            }
            if line_matched_any {
                matching_line_count += 1;
            }
            if line_score > best_line_score {
                best_line_score = line_score;
                best_line_num = idx + 1;
                best_line_col = line_best_col;
                best_line_text = line.trim_end().to_string();
            }
        }

        let mut total = 0.0f32;
        let mut matched = true;
        for (idx, query) in query_tokens.iter().enumerate() {
            let min_score = min_token_score(query_tokens.len(), query.len());
            if best_per_query[idx].score < min_score {
                matched = false;
                break;
            }
            total += best_per_query[idx].score;
        }
        if !matched || best_line_num == 0 {
            continue;
        }

        // Proximity bonus: reward tokens matching close together
        let matched_lines: Vec<usize> = best_lines.iter().filter_map(|l| *l).collect();
        let proximity_bonus = if matched_lines.len() >= 2 {
            let span = matched_lines.iter().max().unwrap() - matched_lines.iter().min().unwrap();
            if span == 0 {
                2.0
            } else if span <= 3 {
                1.0
            } else if span <= 10 {
                0.5
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Frequency bonus: reward multiple matching lines (capped)
        let match_count_bonus = (matching_line_count as f32 * 0.1).min(1.0);

        total += proximity_bonus + match_count_bonus;

        hits.push(FuzzyHit {
            hit: SearchHit {
                path: file.rel_path.clone(),
                line: best_line_num,
                column: best_line_col,
                line_text: best_line_text,
                context: None,
                enclosing_symbol: None,
                score: None,
                reasons: None,
                engine: None,
                next_hops: None,
            },
            score: total,
        });
    }
    hits.sort_by(|a, b| {
        let a_score = a.score + path_bonus(&a.hit.path);
        let b_score = b.score + path_bonus(&b.hit.path);
        b_score
            .partial_cmp(&a_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.hit.path.cmp(&b.hit.path))
    });
    hits.truncate(limit);
    Ok(hits)
}

fn should_run_fuzzy(query_tokens: &[String]) -> bool {
    if query_tokens.is_empty() {
        return false;
    }
    if query_tokens.len() > 1 {
        return true;
    }
    query_tokens[0].len() >= 4
}

fn min_token_score(token_count: usize, token_len: usize) -> f32 {
    let base = if token_count > 1 { 0.45 } else { 0.6 };
    if token_len <= 3 { base + 0.1 } else { base }
}

#[derive(Clone)]
struct TokenOccurrence {
    token: String,
    column: usize,
}

fn collect_query_tokens(query: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for token in extract_line_tokens(query) {
        if seen.insert(token.token.clone()) {
            out.push(token.token);
        }
    }
    out
}

fn extract_line_tokens(line: &str) -> Vec<TokenOccurrence> {
    let bytes = line.as_bytes();
    let mut tokens = Vec::new();
    let mut start: Option<usize> = None;
    for idx in 0..=bytes.len() {
        let is_word = idx < bytes.len() && is_word_char(bytes[idx]);
        if is_word {
            if start.is_none() {
                start = Some(idx);
            }
        } else if let Some(word_start) = start {
            let word = &line[word_start..idx];
            tokens.extend(expand_word(word, word_start + 1));
            start = None;
        }
    }
    tokens
}

fn is_word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

fn expand_word(word: &str, column: usize) -> Vec<TokenOccurrence> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    if let Some(base) = normalize_token(word) {
        if seen.insert(base.clone()) {
            out.push(TokenOccurrence {
                token: base,
                column,
            });
        }
    }
    for (part, offset) in split_identifier(word) {
        if let Some(norm) = normalize_token(&part) {
            if seen.insert(norm.clone()) {
                out.push(TokenOccurrence {
                    token: norm,
                    column: column + offset,
                });
            }
        }
    }
    out
}

fn split_identifier(word: &str) -> Vec<(String, usize)> {
    let bytes = word.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut prev_cat: Option<u8> = None;
    for idx in 0..bytes.len() {
        let byte = bytes[idx];
        if byte == b'_' || byte == b'-' {
            if start < idx {
                parts.push((word[start..idx].to_string(), start));
            }
            start = idx + 1;
            prev_cat = None;
            continue;
        }
        let cat = classify_byte(byte);
        if let Some(prev) = prev_cat {
            if prev == CAT_LOWER && cat == CAT_UPPER {
                if start < idx {
                    parts.push((word[start..idx].to_string(), start));
                }
                start = idx;
            } else if (prev == CAT_DIGIT && cat != CAT_DIGIT)
                || (prev != CAT_DIGIT && cat == CAT_DIGIT)
            {
                if start < idx {
                    parts.push((word[start..idx].to_string(), start));
                }
                start = idx;
            } else if prev == CAT_UPPER && cat == CAT_LOWER {
                if idx > 0 && idx - 1 > start {
                    parts.push((word[start..idx - 1].to_string(), start));
                    start = idx - 1;
                }
            }
        }
        prev_cat = Some(cat);
    }
    if start < bytes.len() {
        parts.push((word[start..].to_string(), start));
    }
    parts
}

const CAT_LOWER: u8 = 0;
const CAT_UPPER: u8 = 1;
const CAT_DIGIT: u8 = 2;
const CAT_OTHER: u8 = 3;

fn classify_byte(byte: u8) -> u8 {
    if byte.is_ascii_lowercase() {
        CAT_LOWER
    } else if byte.is_ascii_uppercase() {
        CAT_UPPER
    } else if byte.is_ascii_digit() {
        CAT_DIGIT
    } else {
        CAT_OTHER
    }
}

fn normalize_token(raw: &str) -> Option<String> {
    let mut out = String::new();
    let mut has_alpha = false;
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() {
            let ch = (byte as char).to_ascii_lowercase();
            if ch.is_ascii_alphabetic() {
                has_alpha = true;
            }
            out.push(ch);
        }
    }
    if out.len() < 2 || !has_alpha {
        return None;
    }
    Some(stem_token(&out))
}

fn stem_token(token: &str) -> String {
    let mut value = token.to_string();
    if value.len() > 5 && value.ends_with("ing") {
        value.truncate(value.len() - 3);
    } else if value.len() > 5 && value.ends_with("ers") {
        value.truncate(value.len() - 3);
    } else if value.len() > 4 && value.ends_with("er") {
        value.truncate(value.len() - 2);
    } else if value.len() > 4 && value.ends_with("ed") {
        value.truncate(value.len() - 2);
    } else if value.len() > 4 && value.ends_with("es") {
        value.truncate(value.len() - 2);
    } else if value.len() > 3 && value.ends_with('s') && !value.ends_with("ss") {
        value.truncate(value.len() - 1);
    }
    value
}

fn token_similarity(query: &str, token: &str) -> f32 {
    if query == token {
        return 1.0;
    }
    if token.starts_with(query) || query.starts_with(token) {
        return 0.9;
    }
    if token.contains(query) || query.contains(token) {
        return 0.8;
    }
    if abbrev_match(query, token) {
        return 0.6;
    }
    let dist = levenshtein_distance(query.as_bytes(), token.as_bytes());
    let max_len = query.len().max(token.len());
    if max_len == 0 {
        return 0.0;
    }
    let ratio = 1.0 - (dist as f32 / max_len as f32);
    if ratio >= 0.65 { ratio } else { 0.0 }
}

fn abbrev_match(a: &str, b: &str) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if short.len() < 2 || short.len() > 3 {
        return false;
    }
    if long.len() < short.len() + 3 {
        return false;
    }
    is_subsequence(short.as_bytes(), long.as_bytes())
}

fn is_subsequence(short: &[u8], long: &[u8]) -> bool {
    let mut idx = 0usize;
    for &byte in long {
        if idx >= short.len() {
            break;
        }
        if byte == short[idx] {
            idx += 1;
        }
    }
    idx == short.len()
}

pub(crate) fn levenshtein_distance(a: &[u8], b: &[u8]) -> usize {
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            let insert = curr[j] + 1;
            let delete = prev[j + 1] + 1;
            let replace = prev[j] + cost;
            curr[j + 1] = insert.min(delete).min(replace);
        }
        prev.clone_from_slice(&curr);
    }
    prev[b.len()]
}

#[derive(Default)]
struct PathFlags {
    docs: bool,
    tests: bool,
    examples: bool,
    generated: bool,
    src: bool,
    lib: bool,
    app: bool,
}

fn path_matches_filters(path: &str, filters: Option<&[String]>) -> bool {
    let Some(filters) = filters else {
        return true;
    };
    if filters.is_empty() {
        return false;
    }
    let normalized = path.replace('\\', "/");
    for raw in filters {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut filter = trimmed.replace('\\', "/");
        while filter.starts_with("./") {
            filter = filter[2..].to_string();
        }
        let prefix = filter.trim_end_matches('/');
        if prefix.is_empty() {
            continue;
        }
        if normalized == prefix {
            return true;
        }
        if normalized.starts_with(prefix) {
            let boundary = prefix.len();
            if normalized.len() > boundary && normalized.as_bytes()[boundary] == b'/' {
                return true;
            }
        }
    }
    false
}

pub fn scope_allows(
    path: &str,
    scope: Option<SearchScope>,
    exclude_generated: bool,
    paths: Option<&[String]>,
) -> bool {
    if !path_matches_filters(path, paths) {
        return false;
    }
    let flags = classify_path(path);
    if exclude_generated && flags.generated {
        return false;
    }
    match scope.unwrap_or(SearchScope::All) {
        SearchScope::All => true,
        SearchScope::Docs => flags.docs,
        SearchScope::Tests => flags.tests,
        SearchScope::Examples => flags.examples,
        SearchScope::Code => !(flags.docs || flags.tests || flags.examples),
    }
}

fn path_bonus(path: &str) -> f32 {
    let flags = classify_path(path);
    if flags.generated {
        return -2.5;
    }
    if flags.docs {
        return -2.0;
    }
    if flags.tests {
        return -1.5;
    }
    if flags.examples {
        return -1.0;
    }
    if flags.src {
        return 1.5;
    }
    if flags.lib {
        return 1.3;
    }
    if flags.app {
        return 1.1;
    }
    0.0
}

fn classify_path(path: &str) -> PathFlags {
    let lower = path.to_ascii_lowercase();
    let mut flags = PathFlags::default();
    let segments: Vec<&str> = lower.split('/').collect();
    let filename = segments.last().copied().unwrap_or("");
    for segment in &segments {
        match *segment {
            "docs" | "doc" | "documentation" => flags.docs = true,
            "test" | "tests" | "__tests__" | "spec" | "specs" => flags.tests = true,
            "examples" | "example" | "samples" | "sample" | "demo" | "demos" => {
                flags.examples = true
            }
            "dist" | "build" | "out" | "target" | "node_modules" | "vendor" | "coverage"
            | "__snapshots__" | "__pycache__" | "obj" | "bin" | "__generated__" | "generated"
            | "codegen" | ".next" | ".nuxt" => flags.generated = true,
            "src" => flags.src = true,
            "lib" => flags.lib = true,
            "app" => flags.app = true,
            _ => {}
        }
    }
    if is_docs_filename(filename) || is_docs_extension(filename) {
        flags.docs = true;
    }
    if is_tests_filename(filename) {
        flags.tests = true;
    }
    if is_generated_filename(filename) {
        flags.generated = true;
    }
    flags
}

fn is_docs_filename(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name);
    matches!(
        stem,
        "readme" | "changelog" | "contributing" | "license" | "copying" | "notes"
    )
}

fn is_docs_extension(name: &str) -> bool {
    matches!(
        name.rsplit('.').next().unwrap_or(""),
        "md" | "markdown" | "rst" | "adoc" | "txt"
    )
}

fn is_tests_filename(name: &str) -> bool {
    name.starts_with("test_")
        || name.contains("_test.")
        || name.contains(".test.")
        || name.contains(".spec.")
}

fn is_generated_filename(name: &str) -> bool {
    name.contains(".min.") || name.contains(".generated.")
}
