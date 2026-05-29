use crate::config::Config;
use crate::model::ValidationResult;

use super::{ContextSeed, GatherContextParams};

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
            ContextSeed::Search { query, .. } => {
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
            common: Default::default(),
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
