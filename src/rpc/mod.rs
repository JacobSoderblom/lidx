mod compact;
mod format;
mod handlers;
mod schema;
mod validate;

pub(crate) use crate::indexer::differ::{ChangedFile, parse_diff_with_ranges};
use crate::indexer::{Indexer, scan, test_detection};
use crate::model::{
    AnalyzeDiffResult, BudgetInfo, ChangedSymbol, DiffImpactEntry, ExplainRef, ExplainSymbolResult,
    ModuleEdge, ModuleNode, RiskAssessment, RiskFactor, Symbol, TestCoverageEntry, TestRef,
    TraceFlowResult,
};
use crate::util::normalize_search_paths;
use crate::watch;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

pub(crate) use compact::compact_symbol_value;
pub(crate) use schema::method_param_schema;

#[derive(Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct RpcResponse {
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    message: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReindexParams {
    summary: Option<bool>,
    fields: Option<Vec<String>>,
    resolve_edges: Option<bool>,
    mine_git: Option<bool>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TopComplexityParams {
    limit: Option<usize>,
    min_complexity: Option<i64>,
    #[serde(flatten)]
    common: CommonParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RepoMapParams {
    /// Maximum bytes of output text (default: 8000, min: 1000, max: 50000)
    max_bytes: Option<usize>,
    #[serde(flatten)]
    common: CommonParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DeadSymbolsParams {
    /// Maximum number of results per category (default: 50)
    limit: Option<usize>,
    /// Include unused imports (default: true)
    include_unused_imports: Option<bool>,
    /// Include orphan tests (default: true)
    include_orphan_tests: Option<bool>,
    #[serde(flatten)]
    common: CommonParams,
}

// Active param structs used by the remaining methods
#[derive(Deserialize, schemars::JsonSchema)]
struct AnalyzeImpactParams {
    id: Option<i64>,
    qualname: Option<String>,
    /// Fuzzy search query to find symbol (alternative to id/qualname)
    query: Option<String>,
    /// Batch mode: multiple qualnames or config URIs to analyze in one call
    qualnames: Option<Vec<String>>,
    /// Multi-layer configuration
    enable_direct: Option<bool>,
    enable_test: Option<bool>,
    enable_historical: Option<bool>,
    /// Direct layer configuration
    max_depth: Option<usize>,
    /// "upstream" (find consumers/callers), "downstream" (follow calls), or "both" (default). Use "upstream" for "what depends on this?"
    direction: Option<String>,
    kinds: Option<Vec<String>>,
    include_tests: Option<bool>,
    include_paths: Option<bool>,
    /// Global configuration
    limit: Option<usize>,
    min_confidence: Option<f32>,
    #[serde(flatten)]
    common: LangVersionParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AnalyzeDiffParams {
    /// Git diff text (unified diff format)
    diff: Option<String>,
    /// Changed file paths (simpler input)
    #[serde(alias = "path")]
    paths: Option<Vec<String>>,
    /// Max impact traversal depth
    max_depth: Option<usize>,
    /// Include test mapping
    include_tests: Option<bool>,
    /// Include risk assessment
    include_risk: Option<bool>,
    max_bytes: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RgParams {
    #[serde(alias = "pattern", alias = "text", alias = "q")]
    query: String,
    limit: Option<usize>,
    context_lines: Option<usize>,
    include_text: Option<bool>,
    include_symbol: Option<bool>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    globs: Option<Vec<String>>,
    case_sensitive: Option<bool>,
    fixed_string: Option<bool>,
    hidden: Option<bool>,
    no_ignore: Option<bool>,
    follow: Option<bool>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct OnboardParams {
    #[serde(flatten)]
    common: LangVersionParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OrientParams {
    /// "overview", "map", "modules", or "all" (default: "all")
    view: Option<String>,
    depth: Option<usize>,
    max_bytes: Option<usize>,
    /// Focus on a specific symbol by qualname (filters orient output to symbol's context)
    focus_qualname: Option<String>,
    /// Focus on a specific symbol by fuzzy query (alternative to focus_qualname)
    focus_query: Option<String>,
    #[serde(flatten)]
    common: CommonParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct GatherContextParams {
    /// Starting points: symbol qualnames, file paths, or search queries
    #[serde(default)]
    seeds: Vec<ContextSeed>,
    /// Maximum bytes of content to return (default: 100_000, hard cap: 2_000_000)
    max_bytes: Option<usize>,
    /// Maximum depth for subgraph expansion (default: 2)
    depth: Option<usize>,
    /// Maximum nodes in subgraph (default: 50)
    max_nodes: Option<usize>,
    /// Include file content for symbols (default: true)
    include_snippets: Option<bool>,
    /// Include related symbols via call graph (default: true)
    include_related: Option<bool>,
    /// If true, return metadata and item skeletons without content
    dry_run: Option<bool>,
    /// Content strategy: "symbol" (symbol bodies only) or "file" (full files)
    /// Defaults to "symbol" when all seeds are symbol/id seeds, "file" otherwise
    strategy: Option<String>,
    #[serde(flatten)]
    common: CommonParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContextSeed {
    Symbol {
        qualname: String,
    },
    File {
        path: String,
        start_line: Option<i64>,
        end_line: Option<i64>,
    },
    Search {
        query: String,
        limit: Option<usize>,
    },
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ExplainSymbolParams {
    id: Option<i64>,
    qualname: Option<String>,
    query: Option<String>,
    max_bytes: Option<usize>,
    sections: Option<Vec<String>>,
    max_refs: Option<usize>,
    format: Option<String>,
    #[serde(flatten)]
    common: LangVersionParams,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TraceFlowParams {
    start_id: Option<i64>,
    start_qualname: Option<String>,
    /// Fuzzy search query to find start symbol (alternative to start_id/start_qualname)
    #[serde(alias = "start_query")]
    query: Option<String>,
    end_id: Option<i64>,
    end_qualname: Option<String>,
    /// "downstream" (follow calls) or "upstream" (follow callers). Default: "downstream"
    direction: Option<String>,
    /// Max hops (default: 5, max: 10)
    max_hops: Option<usize>,
    /// Edge kinds to follow (default: ["CALLS", "RPC_IMPL"])
    kinds: Option<Vec<String>>,
    /// Include source snippets
    include_snippets: Option<bool>,
    format: Option<String>,
    trace_offset: Option<usize>,
    max_bytes: Option<usize>,
    #[serde(flatten)]
    common: LangVersionParams,
}

/// Hard cap on result count to prevent huge responses that blow LLM context windows.
const MAX_RESPONSE_LIMIT: usize = 500;

pub const METHOD_LIST: &[&str] = &[
    "search",
    "explain_symbol",
    "trace_flow",
    "analyze_impact",
    "analyze_diff",
    "gather_context",
    "context",
    "orient",
    "onboard",
    "reindex",
    "top_complexity",
    "repo_map",
    "dead_symbols",
];

pub fn serve(repo_root: PathBuf, db_path: PathBuf, watch_config: watch::WatchConfig) -> Result<()> {
    let watch_repo = repo_root.clone();
    let watch_db = db_path.clone();
    let mut app = App::new(repo_root, db_path, watch_config.scan_options)?;
    let _watcher = watch::start(watch_repo, watch_db, watch_config)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(err) => {
                eprintln!("stdin error: {err}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => app.handle_request(request),
            Err(err) => format::error_response(Value::Null, &format!("invalid request: {err}")),
        };

        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }

    Ok(())
}

pub fn call(
    repo_root: PathBuf,
    db_path: PathBuf,
    method: String,
    params_raw: &str,
    id_raw: &str,
) -> Result<String> {
    let params: Value = serde_json::from_str(params_raw).with_context(|| "parse params JSON")?;
    let id = format::parse_value(id_raw);
    let mut app = App::new(repo_root, db_path, scan::ScanOptions::default())?;
    let request = RpcRequest { id, method, params };
    let response = app.handle_request(request);
    Ok(serde_json::to_string(&response)?)
}

struct App {
    indexer: Indexer,
}

impl App {
    fn new(repo_root: PathBuf, db_path: PathBuf, scan_options: scan::ScanOptions) -> Result<Self> {
        let indexer = Indexer::new_with_options(repo_root.clone(), db_path, scan_options)?;
        Ok(Self { indexer })
    }

    fn handle_request(&mut self, req: RpcRequest) -> RpcResponse {
        let id = req.id.clone();
        let result = handle_method(&mut self.indexer, &req.method, req.params);

        match result {
            Ok(value) => RpcResponse {
                id,
                result: Some(value),
                error: None,
            },
            Err(err) => format::error_response(id, &err.to_string()),
        }
    }
}

/// Default response size cap (30KB ≈ 7,500 tokens).
/// Applied when caller doesn't specify max_response_bytes/max_tokens.
/// Methods that manage their own budgets or intentionally return large content are exempt.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 30_000;

pub fn handle_method(indexer: &mut Indexer, method: &str, params: Value) -> Result<Value> {
    let start = Instant::now();
    let max_response_bytes = format::extract_max_response_bytes(&params);
    let value = match method {
        "search" => handlers::handle_search_rg(indexer, params)?,
        "explain_symbol" => handlers::handle_explain_symbol(indexer, params)?,
        "trace_flow" => handlers::handle_trace_flow(indexer, params)?,
        "analyze_impact" => handlers::handle_analyze_impact(indexer, params)?,
        "analyze_diff" => handlers::handle_analyze_diff(indexer, params)?,
        "gather_context" => handlers::handle_gather_context(indexer, params)?,
        "orient" => handlers::handle_orient(indexer, params)?,
        "onboard" => handlers::handle_onboard(indexer, params)?,
        "reindex" => handlers::handle_reindex(indexer, params)?,
        "top_complexity" => handlers::handle_top_complexity(indexer, params)?,
        "context" => handlers::handle_context(indexer, params)?,
        "repo_map" => handlers::handle_repo_map(indexer, params)?,
        "dead_symbols" => handlers::handle_dead_symbols(indexer, params)?,
        other => {
            return Err(anyhow::anyhow!("unknown method: {other}"));
        }
    };

    let elapsed = start.elapsed();
    if elapsed.as_millis() > 100 {
        eprintln!("lidx: Slow query: {} took {:?}", method, elapsed);
    }

    let exempt = matches!(
        method,
        "gather_context" | "onboard" | "orient" | "context" | "repo_map"
    );
    let effective_max = max_response_bytes.or({
        if exempt {
            None
        } else {
            Some(DEFAULT_MAX_RESPONSE_BYTES)
        }
    });
    if let Some(max_bytes) = effective_max {
        let (truncated_value, was_truncated, total_available) =
            format::truncate_response(value, max_bytes);
        if was_truncated {
            let mut response = json!({
                "data": truncated_value,
                "truncated": true,
                "max_response_bytes": max_bytes,
            });
            if let Some(total) = total_available {
                response
                    .as_object_mut()
                    .unwrap()
                    .insert("total_available".to_string(), json!(total));
            }
            Ok(response)
        } else {
            Ok(truncated_value)
        }
    } else {
        Ok(value)
    }
}

pub(super) fn resolve_graph_version(indexer: &Indexer, value: Option<i64>) -> Result<i64> {
    if let Some(version) = value {
        return Ok(version);
    }
    indexer.db().current_graph_version()
}

/// A single path or a list of paths. Accepting both shapes keeps `path` forgiving
/// for clients that pass an array (previously supported by repo_map via an alias).
#[derive(Deserialize, Clone, schemars::JsonSchema)]
#[serde(untagged)]
pub(super) enum PathArg {
    One(String),
    Many(Vec<String>),
}

/// Common query parameters shared by handlers that support path filters.
/// Use `#[serde(flatten)]` in a params struct to include these fields automatically.
#[derive(Deserialize, Default, Clone, schemars::JsonSchema)]
pub(super) struct CommonParams {
    /// Language filter (e.g. ["rust", "python"])
    pub languages: Option<Vec<String>>,
    /// Path prefix filter: a single path or an array (alternative to `paths`)
    pub path: Option<PathArg>,
    /// Path prefix filters
    pub paths: Option<Vec<String>>,
    /// Graph version to query (defaults to current)
    #[serde(alias = "as_of", alias = "version")]
    pub graph_version: Option<i64>,
}

/// Common query parameters for handlers that filter by language but do not
/// support path filters. Keeping `path`/`paths` out of these params means the
/// published schemas only advertise filters the handlers actually honor.
#[derive(Deserialize, Default, Clone, schemars::JsonSchema)]
pub(super) struct LangVersionParams {
    /// Language filter (e.g. ["rust", "python"])
    pub languages: Option<Vec<String>>,
    /// Graph version to query (defaults to current)
    #[serde(alias = "as_of", alias = "version")]
    pub graph_version: Option<i64>,
}

impl From<LangVersionParams> for CommonParams {
    fn from(params: LangVersionParams) -> Self {
        Self {
            languages: params.languages,
            path: None,
            paths: None,
            graph_version: params.graph_version,
        }
    }
}

/// Resolved common handler state: graph version, normalized language filter, normalized paths.
pub(super) struct HandlerContext {
    pub graph_version: i64,
    pub languages: Option<Vec<String>>,
    pub paths: Option<Vec<String>>,
}

impl HandlerContext {
    /// Resolve common params into ready-to-use values, normalising language and path filters.
    pub fn new(indexer: &Indexer, common: impl Into<CommonParams>) -> Result<Self> {
        let common = common.into();
        let graph_version = resolve_graph_version(indexer, common.graph_version)?;
        let languages = scan::normalize_language_filter(common.languages.as_deref())?;
        let mut raw_paths = common.paths.unwrap_or_default();
        match common.path {
            Some(PathArg::One(path)) => raw_paths.push(path),
            Some(PathArg::Many(paths)) => raw_paths.extend(paths),
            None => {}
        }
        let paths = normalize_search_paths(indexer.repo_root(), None, Some(raw_paths))?;
        Ok(Self {
            graph_version,
            languages,
            paths,
        })
    }

    /// Resolve only graph version (for handlers whose params don't use CommonParams).
    pub fn from_version(indexer: &Indexer, version: Option<i64>) -> Result<Self> {
        Ok(Self {
            graph_version: resolve_graph_version(indexer, version)?,
            languages: None,
            paths: None,
        })
    }
}

fn is_test_symbol(s: &Symbol) -> bool {
    test_detection::is_test_symbol(s)
}

fn infer_language(file_path: &str) -> String {
    scan::language_for_path(std::path::Path::new(file_path))
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    // --- Schema generation tests ---

    #[test]
    fn all_methods_have_param_schema() {
        for method in super::METHOD_LIST {
            let schema = super::method_param_schema(method);
            assert!(
                schema.is_object(),
                "method '{}' did not produce an object schema",
                method
            );
            // Every schema should either have "type":"object" or be the paramless default
            let obj = schema.as_object().unwrap();
            let has_type = obj.get("type").and_then(|v| v.as_str()) == Some("object");
            let has_one_of = obj.contains_key("oneOf");
            assert!(
                has_type || has_one_of,
                "method '{}' schema has neither type:object nor oneOf: {:?}",
                method,
                obj.keys().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn param_schema_has_required_fields() {
        // search requires "query"
        let schema = super::method_param_schema("search");
        let required = schema.get("required").and_then(|v| v.as_array());
        assert!(required.is_some(), "search should have required fields");
        let required: Vec<&str> = required
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            required.contains(&"query"),
            "search should require 'query', got: {:?}",
            required
        );

        // gather_context — seeds has a default, so it may not be in required array.
        // Just check the schema is valid.
        let schema = super::method_param_schema("gather_context");
        assert!(
            schema.is_object(),
            "gather_context should have valid schema"
        );
    }

    #[test]
    fn param_schema_no_refs() {
        fn check_no_refs(value: &serde_json::Value, path: &str) {
            match value {
                serde_json::Value::Object(map) => {
                    assert!(
                        !map.contains_key("$ref"),
                        "$ref found at {}: {:?}",
                        path,
                        map.get("$ref")
                    );
                    for (k, v) in map {
                        check_no_refs(v, &format!("{}.{}", path, k));
                    }
                }
                serde_json::Value::Array(arr) => {
                    for (i, v) in arr.iter().enumerate() {
                        check_no_refs(v, &format!("{}[{}]", path, i));
                    }
                }
                _ => {}
            }
        }

        for method in super::METHOD_LIST {
            let schema = super::method_param_schema(method);
            check_no_refs(&schema, method);
        }
    }

    #[test]
    fn param_schema_no_null_types() {
        fn check_no_null(value: &serde_json::Value, path: &str) {
            match value {
                serde_json::Value::Object(map) => {
                    if map.get("type").and_then(|v| v.as_str()) == Some("null") {
                        panic!("type:null found at {}", path);
                    }
                    for (k, v) in map {
                        check_no_null(v, &format!("{}.{}", path, k));
                    }
                }
                serde_json::Value::Array(arr) => {
                    for (i, v) in arr.iter().enumerate() {
                        check_no_null(v, &format!("{}[{}]", path, i));
                    }
                }
                _ => {}
            }
        }

        for method in super::METHOD_LIST {
            let schema = super::method_param_schema(method);
            check_no_null(&schema, method);
        }
    }

    #[test]
    fn param_schema_total_size_cap() {
        let mut total = 0;
        for method in super::METHOD_LIST {
            let schema = super::method_param_schema(method);
            let serialized = serde_json::to_string(&schema).unwrap();
            total += serialized.len();
        }
        assert!(
            total < 25_000,
            "Total schema size {} exceeds 25KB cap",
            total
        );
    }

    #[test]
    fn context_seed_schema_is_clean() {
        let schema = super::method_param_schema("gather_context");
        // Navigate to seeds.items — should have oneOf with 3 variants
        let seeds_schema = schema
            .get("properties")
            .and_then(|p| p.get("seeds"))
            .and_then(|s| s.get("items"));
        assert!(
            seeds_schema.is_some(),
            "gather_context should have seeds.items"
        );
        let seeds_items = seeds_schema.unwrap();
        let one_of = seeds_items.get("oneOf");
        assert!(
            one_of.is_some(),
            "seeds.items should have oneOf: {}",
            seeds_items
        );
        let variants = one_of.unwrap().as_array().unwrap();
        assert_eq!(
            variants.len(),
            3,
            "ContextSeed should have 3 variants (symbol, file, search), got {}",
            variants.len()
        );
        // Each variant should have a type discriminator property
        for variant in variants {
            let props = variant.get("properties");
            assert!(
                props.is_some(),
                "variant should have properties: {}",
                variant
            );
            assert!(
                props.unwrap().get("type").is_some(),
                "variant should have 'type' discriminator property: {}",
                variant
            );
        }
    }

    #[test]
    fn method_list_matches_dispatch() {
        // Ensure every method in METHOD_LIST is handled by handle_method
        let dir = std::env::temp_dir().join(format!(
            "lidx-dispatch-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join(".lidx.sqlite");
        let mut indexer = crate::indexer::Indexer::new(dir.clone(), db_path).unwrap();
        for method in super::METHOD_LIST {
            let result = super::handle_method(&mut indexer, method, serde_json::json!({}));
            if let Err(ref err) = result {
                let msg = err.to_string();
                assert!(
                    !msg.contains("unknown method"),
                    "METHOD_LIST contains '{}' but handle_method does not dispatch it",
                    method
                );
            }
        }
    }

    // --- Common params (flattened) tests ---

    #[test]
    fn common_params_honor_graph_version_aliases_through_flatten() {
        let p: super::TopComplexityParams =
            serde_json::from_value(serde_json::json!({"as_of": 3})).unwrap();
        assert_eq!(p.common.graph_version, Some(3));
        let p: super::TopComplexityParams =
            serde_json::from_value(serde_json::json!({"version": 7})).unwrap();
        assert_eq!(p.common.graph_version, Some(7));
        let p: super::ExplainSymbolParams =
            serde_json::from_value(serde_json::json!({"qualname": "x", "as_of": 5})).unwrap();
        assert_eq!(p.common.graph_version, Some(5));
        let p: super::TraceFlowParams =
            serde_json::from_value(serde_json::json!({"query": "x", "version": 2})).unwrap();
        assert_eq!(p.common.graph_version, Some(2));
    }

    #[test]
    fn path_filter_methods_advertise_path_params() {
        for method in [
            "orient",
            "repo_map",
            "dead_symbols",
            "top_complexity",
            "gather_context",
        ] {
            let schema = super::method_param_schema(method);
            let props = schema
                .get("properties")
                .and_then(|p| p.as_object())
                .unwrap();
            for key in ["languages", "path", "paths", "graph_version"] {
                assert!(
                    props.contains_key(key),
                    "{} schema should advertise '{}', got: {:?}",
                    method,
                    key,
                    props.keys().collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn non_path_methods_do_not_advertise_path_params() {
        for method in ["explain_symbol", "trace_flow", "analyze_impact", "onboard"] {
            let schema = super::method_param_schema(method);
            let props = schema
                .get("properties")
                .and_then(|p| p.as_object())
                .unwrap();
            assert!(
                props.contains_key("languages"),
                "{} schema should advertise 'languages'",
                method
            );
            for key in ["path", "paths"] {
                assert!(
                    !props.contains_key(key),
                    "{} ignores '{}', so its schema must not advertise it",
                    method,
                    key
                );
            }
        }
    }
}
