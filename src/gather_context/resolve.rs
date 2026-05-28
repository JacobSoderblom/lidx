use crate::db::Db;
use crate::model::{MatchLocation, SkipReason, Symbol};
use crate::rpc::ContextSeed;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

use super::GatherConfig;

#[derive(Debug)]
pub(super) enum ResolvedSeed {
    Symbol {
        symbol: Symbol,
        content_region: Option<(i64, i64)>,
    },
    FileRegion {
        path: String,
        start_byte: i64,
        end_byte: i64,
        start_line: Option<i64>,
        end_line: Option<i64>,
    },
    SearchResults {
        symbol_ids: Vec<(i64, f32)>,
        match_locations: HashMap<i64, MatchLocation>,
    },
}

struct SkipReasonCollector {
    reasons: Vec<SkipReason>,
}

impl SkipReasonCollector {
    fn new() -> Self {
        Self {
            reasons: Vec::new(),
        }
    }

    fn add(&mut self, reason: SkipReason) {
        self.reasons.push(reason);
    }

    fn into_vec(self) -> Vec<SkipReason> {
        self.reasons
    }
}

pub(super) fn resolve_seeds(
    db: &Db,
    repo_root: &Path,
    seeds: &[ContextSeed],
    config: &GatherConfig,
) -> Result<(Vec<(usize, ResolvedSeed)>, Vec<SkipReason>)> {
    let mut resolved = Vec::new();
    let mut skip_collector = SkipReasonCollector::new();

    let qualnames: Vec<&str> = seeds
        .iter()
        .filter_map(|seed| match seed {
            ContextSeed::Symbol { qualname } => Some(qualname.as_str()),
            _ => None,
        })
        .collect();

    let symbol_map: HashMap<String, Symbol> = batch_resolve_qualnames(
        db,
        &qualnames,
        config.languages.as_deref(),
        config.graph_version,
    )?;

    for (idx, seed) in seeds.iter().enumerate() {
        match seed {
            ContextSeed::Symbol { qualname } => {
                if let Some(symbol) = symbol_map.get(qualname) {
                    resolved.push((
                        idx,
                        ResolvedSeed::Symbol {
                            symbol: symbol.clone(),
                            content_region: Some((symbol.start_byte, symbol.end_byte)),
                        },
                    ));
                } else {
                    let suggestions = get_symbol_suggestions(db, qualname, config)?;
                    skip_collector.add(SkipReason::symbol_not_found(idx, qualname, suggestions));
                }
            }
            ContextSeed::File {
                path,
                start_line,
                end_line,
            } => match resolve_file_region(repo_root, path, *start_line, *end_line)? {
                Some(region) => resolved.push((idx, region)),
                None => {
                    let abs_path = repo_root.join(path);
                    if !abs_path.exists() {
                        skip_collector.add(SkipReason::file_not_found(idx, path));
                    } else if let (Some(s), Some(e)) = (start_line, end_line) {
                        if s > e {
                            skip_collector.add(SkipReason::invalid_line_range(idx, path, *s, *e));
                        } else {
                            skip_collector.add(SkipReason::file_outside_repo(idx, path));
                        }
                    } else {
                        skip_collector.add(SkipReason::file_outside_repo(idx, path));
                    }
                }
            },
            ContextSeed::Search { query, limit } => {
                let (symbol_ids, match_locations) =
                    resolve_search_seed(db, repo_root, query, *limit, config)?;
                if symbol_ids.is_empty() {
                    skip_collector.add(SkipReason::search_no_results(idx, query));
                } else {
                    resolved.push((
                        idx,
                        ResolvedSeed::SearchResults {
                            symbol_ids,
                            match_locations,
                        },
                    ));
                }
            }
        }
    }

    Ok((resolved, skip_collector.into_vec()))
}

fn batch_resolve_qualnames(
    db: &Db,
    qualnames: &[&str],
    _languages: Option<&[String]>,
    graph_version: i64,
) -> Result<HashMap<String, Symbol>> {
    let mut map = HashMap::new();

    for qualname in qualnames {
        if let Some(symbol) = db.get_symbol_by_qualname(qualname, graph_version)? {
            map.insert(qualname.to_string(), symbol);
        }
    }

    Ok(map)
}

fn get_symbol_suggestions(db: &Db, qualname: &str, config: &GatherConfig) -> Result<Vec<String>> {
    let name = qualname.rsplit("::").next().unwrap_or(qualname);
    let symbols = db.find_symbols(name, 3, config.languages.as_deref(), config.graph_version)?;
    Ok(symbols.into_iter().map(|s| s.qualname).collect())
}

fn resolve_file_region(
    repo_root: &Path,
    rel_path: &str,
    start_line: Option<i64>,
    end_line: Option<i64>,
) -> Result<Option<ResolvedSeed>> {
    let abs_path = repo_root.join(rel_path);

    let canonical = abs_path.canonicalize().ok();
    let repo_canonical = repo_root.canonicalize().ok();

    match (canonical, repo_canonical) {
        (Some(file_path), Some(root_path)) if file_path.starts_with(&root_path) => {}
        _ => return Ok(None),
    }

    let content = match std::fs::read_to_string(&abs_path) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };

    let (start_byte, end_byte, actual_start, actual_end) =
        if let (Some(start), Some(end)) = (start_line, end_line) {
            line_range_to_bytes(&content, start, end)
        } else {
            (0, content.len() as i64, None, None)
        };

    Ok(Some(ResolvedSeed::FileRegion {
        path: rel_path.to_string(),
        start_byte,
        end_byte,
        start_line: actual_start,
        end_line: actual_end,
    }))
}

pub(super) fn line_range_to_bytes(
    content: &str,
    start: i64,
    end: i64,
) -> (i64, i64, Option<i64>, Option<i64>) {
    let mut current_line = 1i64;
    let mut start_byte = 0i64;
    let mut end_byte = content.len() as i64;
    let mut found_start = false;

    for (idx, ch) in content.char_indices() {
        if current_line == start && !found_start {
            start_byte = idx as i64;
            found_start = true;
        }
        if ch == '\n' {
            current_line += 1;
            if current_line > end {
                end_byte = idx as i64 + 1;
                break;
            }
        }
    }

    (
        start_byte,
        end_byte,
        Some(start),
        Some(end.min(current_line - 1)),
    )
}

fn resolve_search_seed(
    db: &Db,
    repo_root: &Path,
    query: &str,
    limit: Option<usize>,
    config: &GatherConfig,
) -> Result<(Vec<(i64, f32)>, HashMap<i64, MatchLocation>)> {
    use crate::search::{SearchOptions, search_text};

    let limit = limit.unwrap_or(10);
    let options = SearchOptions {
        languages: config.languages.as_deref(),
        scope: None,
        exclude_generated: false,
        rank: true,
        no_ignore: false,
        paths: config.paths.as_deref(),
    };

    let hits = search_text(repo_root, query, limit, options)?;

    let mut results = Vec::new();
    let mut locations = HashMap::new();
    for hit in hits {
        if let Some(symbol) =
            db.enclosing_symbol_for_line(&hit.path, hit.line as i64, config.graph_version)?
        {
            let score = hit.score.unwrap_or(1.0);
            results.push((symbol.id, score));
            locations.insert(
                symbol.id,
                MatchLocation {
                    line: hit.line as i64,
                    column: hit.column as i64,
                    match_text: hit.line_text.clone(),
                },
            );
        }
    }

    Ok((results, locations))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_range_to_bytes_handles_edge_cases() {
        let content = "line1\nline2\nline3\n";

        let (start, end, _, _) = line_range_to_bytes(content, 2, 2);
        assert_eq!(&content[start as usize..end as usize], "line2\n");

        let (_, end, _, actual_end) = line_range_to_bytes(content, 1, 100);
        assert_eq!(end, content.len() as i64);
        assert!(actual_end.unwrap() <= 4);

        let (start, end, _, _) = line_range_to_bytes("", 1, 1);
        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn line_range_to_bytes_byte_offset_consistency() {
        let content = "fn test() {\n    println!(\"hello\");\n}\n";

        let (start, end, _, _) = line_range_to_bytes(content, 2, 2);

        let line2 = &content[start as usize..end as usize];
        assert!(line2.contains("println"));

        assert!(content.is_char_boundary(start as usize));
        assert!(content.is_char_boundary(end as usize));
    }
}
