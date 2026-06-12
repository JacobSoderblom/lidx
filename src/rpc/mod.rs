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
#[cfg(test)]
use crate::model::{Edge, FlowStatusEntry, FlowStatusResult};
use crate::watch;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(test)]
use std::collections::HashMap;
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
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RepoMapParams {
    /// Maximum bytes of output text (default: 8000, min: 1000, max: 50000)
    max_bytes: Option<usize>,
    /// Language filter (e.g. ["rust", "python"])
    languages: Option<Vec<String>>,
    /// Path filter
    #[serde(alias = "path")]
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DeadSymbolsParams {
    /// Maximum number of results per category (default: 50)
    limit: Option<usize>,
    /// Language filter (e.g. ["rust", "python"])
    languages: Option<Vec<String>>,
    /// Single path filter (alternative to paths array)
    path: Option<String>,
    /// Path filter array
    paths: Option<Vec<String>>,
    /// Include unused imports (default: true)
    include_unused_imports: Option<bool>,
    /// Include orphan tests (default: true)
    include_orphan_tests: Option<bool>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OrientParams {
    /// "overview", "map", "modules", or "all" (default: "all")
    view: Option<String>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    depth: Option<usize>,
    max_bytes: Option<usize>,
    languages: Option<Vec<String>>,
    /// Focus on a specific symbol by qualname (filters orient output to symbol's context)
    focus_qualname: Option<String>,
    /// Focus on a specific symbol by fuzzy query (alternative to focus_qualname)
    focus_query: Option<String>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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
    /// Language filter
    languages: Option<Vec<String>>,
    /// Path filter
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    /// Content strategy: "symbol" (symbol bodies only) or "file" (full files)
    /// Defaults to "symbol" when all seeds are symbol/id seeds, "file" otherwise
    strategy: Option<String>,
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
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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

fn is_test_symbol(s: &Symbol) -> bool {
    test_detection::is_test_symbol(s)
}

fn infer_language(file_path: &str) -> String {
    scan::language_for_path(std::path::Path::new(file_path))
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
fn build_flow_status(
    routes: Vec<Edge>,
    calls: Vec<Edge>,
    include_routes: bool,
    include_calls: bool,
    limit: usize,
    edge_limit: usize,
) -> FlowStatusResult {
    let truncated = routes.len() == edge_limit || calls.len() == edge_limit;
    let mut route_map: HashMap<String, Vec<Edge>> = HashMap::new();
    for edge in routes {
        let Some(path) = edge.target_qualname.clone() else {
            continue;
        };
        route_map.entry(path).or_default().push(edge);
    }
    let mut call_map: HashMap<String, Vec<Edge>> = HashMap::new();
    for edge in calls {
        let Some(path) = edge.target_qualname.clone() else {
            continue;
        };
        call_map.entry(path).or_default().push(edge);
    }
    let routes_total = route_map.len();
    let calls_total = call_map.len();
    let call_paths: HashSet<String> = call_map.keys().cloned().collect();
    let route_paths: HashSet<String> = route_map.keys().cloned().collect();
    let mut routes_without_calls = Vec::new();
    for (path, edges) in route_map {
        if call_paths.contains(&path) {
            continue;
        }
        routes_without_calls.push(FlowStatusEntry {
            path,
            route_count: edges.len(),
            call_count: 0,
            routes: if include_routes { Some(edges) } else { None },
            calls: None,
        });
    }
    let mut calls_without_routes = Vec::new();
    for (path, edges) in call_map {
        if route_paths.contains(&path) {
            continue;
        }
        calls_without_routes.push(FlowStatusEntry {
            path,
            route_count: 0,
            call_count: edges.len(),
            routes: None,
            calls: if include_calls { Some(edges) } else { None },
        });
    }
    routes_without_calls.sort_by(|a, b| {
        b.route_count
            .cmp(&a.route_count)
            .then_with(|| a.path.cmp(&b.path))
    });
    calls_without_routes.sort_by(|a, b| {
        b.call_count
            .cmp(&a.call_count)
            .then_with(|| a.path.cmp(&b.path))
    });
    if limit == 0 {
        routes_without_calls.clear();
        calls_without_routes.clear();
    } else {
        routes_without_calls.truncate(limit);
        calls_without_routes.truncate(limit);
    }
    FlowStatusResult {
        routes_total,
        calls_total,
        edge_limit,
        truncated,
        routes_without_calls,
        calls_without_routes,
    }
}

#[cfg(test)]
mod tests {
    use super::build_flow_status;
    use crate::model::Edge;

    fn edge(kind: &str, path: &str, id: i64) -> Edge {
        Edge {
            id,
            file_path: "test.rs".to_string(),
            kind: kind.to_string(),
            source_symbol_id: None,
            target_symbol_id: None,
            target_qualname: Some(path.to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
        }
    }

    #[test]
    fn flow_status_flags_unmatched_paths() {
        let routes = vec![
            edge("HTTP_ROUTE", "/api/users/{}", 1),
            edge("HTTP_ROUTE", "/api/items/{}", 2),
        ];
        let calls = vec![
            edge("HTTP_CALL", "/api/users/{}", 3),
            edge("HTTP_CALL", "/api/orders/{}", 4),
        ];
        let result = build_flow_status(routes, calls, true, true, 10, 100);
        assert_eq!(result.routes_total, 2);
        assert_eq!(result.calls_total, 2);
        assert_eq!(result.routes_without_calls.len(), 1);
        assert_eq!(result.calls_without_routes.len(), 1);
        assert_eq!(result.routes_without_calls[0].path, "/api/items/{}");
        assert_eq!(result.calls_without_routes[0].path, "/api/orders/{}");
        assert!(result.routes_without_calls[0].routes.is_some());
        assert!(result.calls_without_routes[0].calls.is_some());
    }

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
}
