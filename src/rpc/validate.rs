//! Shared param validation: invalid values for known params are errors,
//! never an empty result that reads as "the index has no match".
//!
//! Rules: enum params are checked against one const list per enum (the same
//! const the matching code uses); count/limit/byte-budget params reject 0;
//! unsigned params given a negative or fractional number name the param;
//! `[0, 1]` params reject anything outside it; repo-relative paths go
//! through `validate_repo_path`.

use crate::config::Config;
use crate::indexer::{channel, config, http, proto, python, sql_extractor, xref};
use crate::model::ValidationResult;

use super::{ContextSeed, GatherContextParams};

pub(super) const VIEW_ALL: &str = "all";
pub(super) const VIEW_OVERVIEW: &str = "overview";
pub(super) const VIEW_MAP: &str = "map";
pub(super) const VIEW_MODULES: &str = "modules";
pub(super) const ORIENT_VIEWS: &[&str] = &[VIEW_ALL, VIEW_OVERVIEW, VIEW_MAP, VIEW_MODULES];

pub(super) const FORMAT_TEXT: &str = "text";
pub(super) const FORMAT_JSON: &str = "json";
pub(super) const CONTEXT_FORMATS: &[&str] = &[FORMAT_TEXT, FORMAT_JSON];

pub(super) const FORMAT_FULL: &str = "full";
pub(super) const FORMAT_SIGNATURES: &str = "signatures";
pub(super) const EXPLAIN_FORMATS: &[&str] = &[FORMAT_FULL, FORMAT_SIGNATURES];

pub(super) const FORMAT_COMPACT: &str = "compact";
pub(super) const TRACE_FORMATS: &[&str] = &[FORMAT_FULL, FORMAT_COMPACT];

pub(super) const DIR_UPSTREAM: &str = "upstream";
pub(super) const DIR_DOWNSTREAM: &str = "downstream";
pub(super) const DIR_BOTH: &str = "both";
const TRACE_DIRECTIONS: &[&str] = &[DIR_DOWNSTREAM, DIR_UPSTREAM];
const IMPACT_DIRECTIONS: &[&str] = &[DIR_DOWNSTREAM, DIR_UPSTREAM, DIR_BOTH];

/// Every edge kind the indexer can write to the `edges` table, so `kinds`
/// can never reject a kind that exists in the graph. Kinds with a `*_KIND`
/// const in the indexer reference it; the rest are literals. Matching is
/// case-insensitive; see `normalize_kinds`. `tests/direction_kinds_validation.rs`
/// asserts every kind found in indexed fixtures is accepted.
pub(super) const EDGE_KINDS: &[&str] = &[
    "CALLS",
    "IMPORTS",
    "CONTAINS",
    "EXTENDS",
    "IMPLEMENTS",
    "INHERITS",
    "USES",
    "MODULE_FILE",
    "IMPORTS_FILE",
    proto::RPC_IMPL_KIND,
    proto::RPC_CALL_KIND,
    proto::RPC_ROUTE_KIND,
    http::HTTP_ROUTE_KIND,
    http::HTTP_CALL_KIND,
    http::PAGE_ROUTE_KIND,
    channel::CHANNEL_PUBLISH_KIND,
    channel::CHANNEL_SUBSCRIBE_KIND,
    config::CONFIG_SOURCE_KIND,
    config::CONFIG_READ_KIND,
    config::CONFIG_BIND_KIND,
    xref::XREF_KIND,
    xref::ROUTE_KIND,
    python::MODULE_EXPORT_KIND,
    sql_extractor::REFERENCES_KIND,
];

/// Canonicalizes a `direction` value (case-insensitive; `up`/`callers` and
/// `down`/`callees` are aliases) or errors naming the value and the valid
/// ones. `None` means the param was omitted; the caller applies its default.
/// `allow_both` is true for `analyze_impact` only.
pub(super) fn normalize_direction(
    value: Option<&str>,
    allow_both: bool,
) -> anyhow::Result<Option<&'static str>> {
    let Some(v) = value else { return Ok(None) };
    match v.to_lowercase().as_str() {
        "upstream" | "up" | "callers" => Ok(Some(DIR_UPSTREAM)),
        "downstream" | "down" | "callees" => Ok(Some(DIR_DOWNSTREAM)),
        "both" if allow_both => Ok(Some(DIR_BOTH)),
        _ => {
            let valid = if allow_both {
                IMPACT_DIRECTIONS
            } else {
                TRACE_DIRECTIONS
            };
            anyhow::bail!("{}", unknown_value_message("direction", v, valid))
        }
    }
}

/// Upper-cases each `kinds` entry and rejects any that is not an edge kind,
/// so a wrongly cased or misspelled kind can never silently match nothing.
pub(super) fn normalize_kinds(kinds: Option<&[String]>) -> anyhow::Result<Option<Vec<String>>> {
    let Some(kinds) = kinds else { return Ok(None) };
    kinds
        .iter()
        .map(|k| {
            let upper = k.to_uppercase();
            if EDGE_KINDS.contains(&upper.as_str()) {
                Ok(upper)
            } else {
                anyhow::bail!("{}", unknown_value_message("edge kind", k, EDGE_KINDS))
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(Some)
}

pub(super) fn validate_pattern_length(pattern: &str, operation: &str) -> anyhow::Result<()> {
    let max_length = Config::get().pattern_max_length;
    if pattern.len() > max_length {
        eprintln!(
            "lidx: Security: {} pattern too long: {} bytes (max: {})",
            operation,
            pattern.len(),
            max_length
        );
        anyhow::bail!(
            "{} pattern too long: {} bytes (max: {})",
            operation,
            pattern.len(),
            max_length
        );
    }
    Ok(())
}

/// serde's "invalid value: integer `-5`, expected usize" never says which
/// param it was. When deserialization fails, name the offending top-level
/// param: a negative or fractional number given for a field the schema
/// declares as an unsigned integer (`minimum: 0`). Any other failure keeps
/// serde's message untouched.
pub(super) fn name_bad_unsigned_param<T: schemars::JsonSchema>(
    params: &serde_json::Value,
    serde_msg: String,
) -> anyhow::Error {
    // The raw schema: the published one strips `minimum` from integers.
    let schema = serde_json::to_value(schemars::schema_for!(T)).unwrap_or_default();
    let props = schema.get("properties").and_then(|p| p.as_object());
    if let (Some(props), Some(obj)) = (props, params.as_object()) {
        let mut keys: Vec<&String> = obj.keys().collect();
        keys.sort();
        for key in keys {
            let value = &obj[key];
            let is_unsigned = props
                .get(key)
                .is_some_and(|p| p.get("minimum").and_then(|m| m.as_f64()) == Some(0.0));
            if is_unsigned && value.is_number() && !value.is_u64() {
                return anyhow::anyhow!(
                    "invalid value for '{key}': expected a non-negative integer, got {value}"
                );
            }
        }
    }
    anyhow::anyhow!(serde_msg)
}

/// Rejects a zero for a count/limit/bound param. A zero would produce an
/// empty result that callers read as "the index has no match".
pub(super) fn require_at_least_one(name: &str, value: Option<usize>) -> anyhow::Result<()> {
    if value == Some(0) {
        anyhow::bail!("{name} must be at least 1 (got 0)");
    }
    Ok(())
}

/// Rejects a string-enum param whose value is not one of `valid`, listing
/// the valid values.
pub(super) fn require_one_of(
    name: &str,
    value: Option<&str>,
    valid: &[&str],
) -> anyhow::Result<()> {
    if let Some(v) = value
        && !valid.contains(&v)
    {
        anyhow::bail!("{}", unknown_value_message(name, v, valid));
    }
    Ok(())
}

/// The one wording for an unknown enum value, shared by `require_one_of` and
/// `validate_gather_context_params`.
pub(super) fn unknown_value_message(name: &str, value: &str, valid: &[&str]) -> String {
    format!(
        "unknown {name} '{value}' -- valid values: {}",
        valid.join(", ")
    )
}

/// Rejects a fraction param (e.g. `min_confidence`) outside `[0, 1]`, NaN
/// included.
pub(super) fn require_unit_interval(name: &str, value: Option<f32>) -> anyhow::Result<()> {
    if let Some(v) = value
        && !(0.0..=1.0).contains(&v)
    {
        anyhow::bail!("{name} must be between 0 and 1 (got {v})");
    }
    Ok(())
}

/// Rejects a `path` that's absolute or that escapes the repo root via a `..`
/// component. A relative path never needs `..` to name a file inside the
/// repo, so any `..` component is rejected outright rather than resolved.
pub(super) fn reject_path_escape(path: &str) -> anyhow::Result<()> {
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

/// The one "not indexed" message `outline` and `context` both report.
pub(super) fn not_indexed_message(path: &str) -> String {
    format!(
        "path '{path}' is not indexed -- fall back to Read, or run 'reindex' if it should be tracked"
    )
}

/// A validated repo-relative path; `file` is its `files` row (`None` for
/// Markdown, which has no row -- see `is_markdown_path`).
pub(super) struct RepoPath<'a> {
    pub path: &'a str,
    pub file: Option<crate::db::FileRecord>,
}

/// Shared path check for `outline` and `context`: trims, rejects empty,
/// absolute and `..` paths, then requires the path be indexed (a `files`
/// row; for Markdown, presence on disk).
pub(super) fn validate_repo_path<'a>(
    method: &str,
    db: &crate::db::Db,
    repo_root: &std::path::Path,
    raw: &'a str,
) -> anyhow::Result<RepoPath<'a>> {
    let path = raw.trim();
    if path.is_empty() {
        anyhow::bail!("{method} requires a non-empty 'path'");
    }
    reject_path_escape(path)?;
    if super::reading::is_markdown_path(path) {
        if !repo_root.join(path).is_file() {
            anyhow::bail!("{}", not_indexed_message(path));
        }
        return Ok(RepoPath { path, file: None });
    }
    let file = db
        .get_file_by_path(path)?
        .ok_or_else(|| anyhow::anyhow!("{}", not_indexed_message(path)))?;
    Ok(RepoPath {
        path,
        file: Some(file),
    })
}

pub(super) fn validate_gather_context_params(params: &GatherContextParams) -> ValidationResult {
    let mut result = ValidationResult::new();

    // Validate max_bytes
    if let Some(max_bytes) = params.max_bytes
        && max_bytes == 0
    {
        result.add("max_bytes", "out_of_range", "max_bytes must be at least 1");
    }

    // Validate depth
    if let Some(depth) = params.depth
        && depth > 10
    {
        result.add("depth", "out_of_range", "depth must be 10 or less");
    }

    // Validate max_nodes
    if let Some(max_nodes) = params.max_nodes {
        if max_nodes == 0 {
            result.add("max_nodes", "out_of_range", "max_nodes must be at least 1");
        } else if max_nodes > 500 {
            result.add("max_nodes", "out_of_range", "max_nodes must be 500 or less");
        }
    }

    // Validate strategy
    if let Some(strategy) = params.strategy.as_deref()
        && !crate::gather_context::STRATEGIES.contains(&strategy)
    {
        result.add(
            "strategy",
            "invalid_value",
            &unknown_value_message("strategy", strategy, crate::gather_context::STRATEGIES),
        );
    }

    // Validate seeds
    for (idx, seed) in params.seeds.iter().enumerate() {
        match seed {
            ContextSeed::Symbol { qualname } => {
                if qualname.trim().is_empty() {
                    result.add(
                        &format!("seeds[{}].qualname", idx),
                        "required",
                        "Symbol seed requires non-empty qualname",
                    );
                }
            }
            ContextSeed::File {
                path,
                start_line,
                end_line,
            } => {
                if path.trim().is_empty() {
                    result.add(
                        &format!("seeds[{}].path", idx),
                        "required",
                        "File seed requires non-empty path",
                    );
                }
                if let (Some(start), Some(end)) = (start_line, end_line) {
                    if start > end {
                        result.add(
                            &format!("seeds[{}]", idx),
                            "invalid_range",
                            &format!("start_line ({}) must be <= end_line ({})", start, end),
                        );
                    }
                    if *start < 1 {
                        result.add(
                            &format!("seeds[{}].start_line", idx),
                            "out_of_range",
                            "start_line must be >= 1",
                        );
                    }
                }
            }
            ContextSeed::Search { query, limit } => {
                if *limit == Some(0) {
                    result.add(
                        &format!("seeds[{}].limit", idx),
                        "out_of_range",
                        "limit must be at least 1",
                    );
                }
                if query.trim().is_empty() {
                    result.add(
                        &format!("seeds[{}].query", idx),
                        "required",
                        "Search seed requires non-empty query",
                    );
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_pattern_length ---

    #[test]
    fn validate_pattern_short_ok() {
        assert!(validate_pattern_length("hello", "test").is_ok());
    }

    #[test]
    fn validate_pattern_at_limit() {
        let max = Config::get().pattern_max_length;
        let pat = "a".repeat(max);
        assert!(validate_pattern_length(&pat, "test").is_ok());
    }

    #[test]
    fn validate_pattern_exceeds_limit() {
        let max = Config::get().pattern_max_length;
        let pat = "a".repeat(max + 1);
        let err = validate_pattern_length(&pat, "search");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("pattern too long"));
    }

    // --- validate_gather_context_params ---

    fn make_params(seeds: Vec<ContextSeed>) -> GatherContextParams {
        GatherContextParams {
            seeds,
            max_bytes: None,
            depth: None,
            max_nodes: None,
            include_snippets: None,
            include_related: None,
            dry_run: None,
            strategy: None,
            common: super::super::CommonParams::default(),
            extra: Default::default(),
        }
    }

    #[test]
    fn validate_empty_seeds_is_valid() {
        let params = make_params(vec![]);
        assert!(validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_max_bytes_zero() {
        let mut params = make_params(vec![]);
        params.max_bytes = Some(0);
        let result = validate_gather_context_params(&params);
        assert!(!result.is_valid());
    }

    #[test]
    fn validate_max_bytes_one() {
        let mut params = make_params(vec![]);
        params.max_bytes = Some(1);
        assert!(validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_depth_11_rejected() {
        let mut params = make_params(vec![]);
        params.depth = Some(11);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_depth_10_ok() {
        let mut params = make_params(vec![]);
        params.depth = Some(10);
        assert!(validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_max_nodes_zero() {
        let mut params = make_params(vec![]);
        params.max_nodes = Some(0);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_max_nodes_501() {
        let mut params = make_params(vec![]);
        params.max_nodes = Some(501);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_max_nodes_500_ok() {
        let mut params = make_params(vec![]);
        params.max_nodes = Some(500);
        assert!(validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_symbol_seed_empty_qualname() {
        let params = make_params(vec![ContextSeed::Symbol {
            qualname: "   ".to_string(),
        }]);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_file_seed_empty_path() {
        let params = make_params(vec![ContextSeed::File {
            path: "".to_string(),
            start_line: None,
            end_line: None,
        }]);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_file_seed_start_after_end() {
        let params = make_params(vec![ContextSeed::File {
            path: "foo.rs".to_string(),
            start_line: Some(10),
            end_line: Some(5),
        }]);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_file_seed_start_zero() {
        let params = make_params(vec![ContextSeed::File {
            path: "foo.rs".to_string(),
            start_line: Some(0),
            end_line: Some(5),
        }]);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_search_seed_empty_query() {
        let params = make_params(vec![ContextSeed::Search {
            query: "  ".to_string(),
            limit: None,
        }]);
        assert!(!validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_valid_seeds() {
        let params = make_params(vec![
            ContextSeed::Symbol {
                qualname: "Foo::bar".to_string(),
            },
            ContextSeed::File {
                path: "src/lib.rs".to_string(),
                start_line: Some(1),
                end_line: Some(10),
            },
            ContextSeed::Search {
                query: "hello".to_string(),
                limit: Some(5),
            },
        ]);
        assert!(validate_gather_context_params(&params).is_valid());
    }

    #[test]
    fn validate_multiple_errors_accumulated() {
        let mut params = make_params(vec![
            ContextSeed::Symbol {
                qualname: "".to_string(),
            },
            ContextSeed::File {
                path: "".to_string(),
                start_line: None,
                end_line: None,
            },
        ]);
        params.max_bytes = Some(0);
        params.depth = Some(11);
        let result = validate_gather_context_params(&params);
        assert!(!result.is_valid());
    }
}
