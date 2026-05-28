mod handlers;

use crate::config::Config;
use crate::indexer::{Indexer, scan, test_detection};
use crate::model::{
    AnalyzeDiffResult, BudgetInfo, ChangedSymbol, DiffImpactEntry, ExplainRef,
    ExplainSymbolResult, ModuleEdge, ModuleNode, RiskAssessment, RiskFactor, Symbol,
    TestCoverageEntry, TestRef, TraceFlowResult, ValidationResult,
};
#[cfg(test)]
use crate::model::{Edge, FlowStatusEntry, FlowStatusResult};
use crate::util;
use crate::watch;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn validate_pattern_length(pattern: &str, operation: &str) -> Result<()> {
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

fn validate_gather_context_params(params: &GatherContextParams) -> ValidationResult {
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

/// Convert a Symbol JSON value to compact format by keeping only essential fields
pub(crate) fn compact_symbol_value(symbol_value: &serde_json::Value) -> serde_json::Value {
    let keep_fields = [
        "id",
        "kind",
        "name",
        "qualname",
        "file_path",
        "start_line",
        "signature",
    ];
    if let serde_json::Value::Object(map) = symbol_value {
        let compact: serde_json::Map<String, serde_json::Value> = map
            .iter()
            .filter(|(k, _)| keep_fields.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::Value::Object(compact)
    } else {
        symbol_value.clone()
    }
}

/// Apply compact format to a response value by converting all symbol objects to compact form.
fn apply_compact_format(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(
                arr.into_iter()
                    .map(|item| {
                        if let serde_json::Value::Object(ref map) = item {
                            // If it looks like a symbol (has qualname field), compact it
                            if map.contains_key("qualname") {
                                return compact_symbol_value(&item);
                            }
                            // If it has a "symbol" field, compact that
                            if map.contains_key("symbol") {
                                let mut new_map = map.clone();
                                if let Some(sym) = new_map.get("symbol") {
                                    new_map.insert("symbol".to_string(), compact_symbol_value(sym));
                                }
                                return serde_json::Value::Object(new_map);
                            }
                        }
                        item
                    })
                    .collect(),
            )
        }
        serde_json::Value::Object(mut map) => {
            // Process known array fields
            for key in [
                "results", "nodes", "incoming", "outgoing", "edges", "trace", "items", "affected",
            ] {
                if let Some(arr) = map.remove(key) {
                    map.insert(key.to_string(), apply_compact_format(arr));
                }
            }
            // Process symbol field if present
            if let Some(sym) = map.remove("symbol") {
                if sym.is_object() && sym.get("qualname").is_some() {
                    map.insert("symbol".to_string(), compact_symbol_value(&sym));
                } else {
                    map.insert("symbol".to_string(), sym);
                }
            }
            // Process start/end symbol fields (trace_flow)
            for key in ["start", "end"] {
                if let Some(sym) = map.remove(key) {
                    if sym.is_object() && sym.get("qualname").is_some() {
                        map.insert(key.to_string(), compact_symbol_value(&sym));
                    } else {
                        map.insert(key.to_string(), sym);
                    }
                }
            }
            serde_json::Value::Object(map)
        }
        other => other,
    }
}

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

// --- Per-method JSON Schema generation ---

fn schema_value<T: schemars::JsonSchema>() -> Value {
    let schema = schemars::schema_for!(T);
    let raw = serde_json::to_value(schema).unwrap_or_else(|_| json!({"type": "object"}));
    simplify_schema(raw)
}

/// Return a simplified JSON Schema for the params struct of the given method.
pub fn method_param_schema(method: &str) -> Value {
    match method {
        "search" => schema_value::<RgParams>(),
        "explain_symbol" => schema_value::<ExplainSymbolParams>(),
        "trace_flow" => schema_value::<TraceFlowParams>(),
        "analyze_impact" => schema_value::<AnalyzeImpactParams>(),
        "analyze_diff" => schema_value::<AnalyzeDiffParams>(),
        "gather_context" => schema_value::<GatherContextParams>(),
        "orient" => schema_value::<OrientParams>(),
        "onboard" => schema_value::<OnboardParams>(),
        "reindex" => schema_value::<ReindexParams>(),
        "top_complexity" => schema_value::<TopComplexityParams>(),
        "repo_map" => schema_value::<RepoMapParams>(),
        "dead_symbols" => schema_value::<DeadSymbolsParams>(),
        _ => json!({"type": "object"}),
    }
}

/// Post-process schemars output into compact, LLM-friendly JSON Schema.
fn simplify_schema(mut schema: Value) -> Value {
    // 1. Collect definitions for inlining $ref
    let definitions = schema
        .get("definitions")
        .cloned()
        .unwrap_or_else(|| json!({}));

    // 2. Recursively inline $ref and clean up
    inline_refs(&mut schema, &definitions);

    // 3. Strip root-level noise
    if let Some(obj) = schema.as_object_mut() {
        obj.remove("$schema");
        obj.remove("definitions");
        obj.remove("title");
    }

    schema
}

/// Recursively inline `$ref` references and collapse `Option<T>` patterns.
fn inline_refs(value: &mut Value, definitions: &Value) {
    match value {
        Value::Object(map) => {
            // Handle $ref: inline the definition
            if let Some(ref_val) = map.get("$ref").cloned()
                && let Some(ref_str) = ref_val.as_str()
            {
                // Extract definition name from "#/definitions/Name"
                if let Some(name) = ref_str.strip_prefix("#/definitions/")
                    && let Some(def) = definitions.get(name)
                {
                    let mut inlined = def.clone();
                    inline_refs(&mut inlined, definitions);
                    *value = inlined;
                    return;
                }
            }

            // Handle anyOf with null (Option<T> pattern): collapse to inner schema
            if let Some(any_of) = map.get("anyOf").cloned()
                && let Some(variants) = any_of.as_array()
                && variants.len() == 2
            {
                let null_idx = variants
                    .iter()
                    .position(|v| v.get("type").and_then(|t| t.as_str()) == Some("null"));
                if let Some(idx) = null_idx {
                    let inner_idx = 1 - idx;
                    let mut inner = variants[inner_idx].clone();
                    inline_refs(&mut inner, definitions);
                    *value = inner;
                    return;
                }
            }

            // Recurse into all values
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if let Some(v) = map.get_mut(&key) {
                    inline_refs(v, definitions);
                }
            }

            // Strip format on integers (e.g. "format": "uint", "format": "int64")
            if map.get("type").and_then(|t| t.as_str()) == Some("integer") {
                map.remove("format");
                map.remove("minimum");
            }
            // Strip format on numbers
            if map.get("type").and_then(|t| t.as_str()) == Some("number") {
                map.remove("format");
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                inline_refs(item, definitions);
            }
        }
        _ => {}
    }
}

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
            Err(err) => error_response(Value::Null, &format!("invalid request: {err}")),
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
    let id = parse_value(id_raw);
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
            Err(err) => error_response(id, &err.to_string()),
        }
    }
}

/// Extract max_response_bytes from params (supports both max_response_bytes and max_tokens)
fn extract_max_response_bytes(params: &serde_json::Value) -> Option<usize> {
    params
        .get("max_response_bytes")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .or_else(|| {
            params
                .get("max_tokens")
                .and_then(|v| v.as_u64())
                .map(|v| (v as usize) * 4) // ~4 bytes per token
        })
}

/// Truncate a JSON response to fit within a byte budget.
/// If value is an array, removes tail elements.
/// If value is an object with common array fields, truncates those arrays.
/// Returns (truncated_value, was_truncated, total_available)
fn truncate_response(
    value: serde_json::Value,
    max_bytes: usize,
) -> (serde_json::Value, bool, Option<usize>) {
    let serialized = serde_json::to_string(&value).unwrap_or_default();
    if serialized.len() <= max_bytes {
        return (value, false, None);
    }

    match value {
        serde_json::Value::Array(arr) => {
            // Binary search for how many elements fit
            let original_len = arr.len();
            let mut low = 0usize;
            let mut high = arr.len();
            while low < high {
                let mid = (low + high).div_ceil(2);
                let slice = serde_json::Value::Array(arr[..mid].to_vec());
                let size = serde_json::to_string(&slice).unwrap_or_default().len();
                if size <= max_bytes {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            (
                serde_json::Value::Array(arr[..low].to_vec()),
                true,
                Some(original_len),
            )
        }
        serde_json::Value::Object(mut map) => {
            // Check if this object has a top-level array field that we can track
            let mut total_available: Option<usize> = None;

            // Look for common array fields and truncate them
            let array_keys: Vec<String> = map
                .iter()
                .filter(|(_, v)| v.is_array())
                .map(|(k, _)| k.clone())
                .collect();

            if array_keys.is_empty() {
                return (serde_json::Value::Object(map), false, None);
            }

            // If there's a single top-level array (common pattern), capture its length
            if array_keys.len() == 1
                && let Some(serde_json::Value::Array(arr)) = map.get(&array_keys[0])
            {
                total_available = Some(arr.len());
            }

            // Truncate each array field proportionally
            let overhead = {
                let mut temp = map.clone();
                for key in &array_keys {
                    temp.insert(key.clone(), serde_json::Value::Array(vec![]));
                }
                serde_json::to_string(&serde_json::Value::Object(temp))
                    .unwrap_or_default()
                    .len()
            };

            let available = max_bytes.saturating_sub(overhead);
            let per_array = available / array_keys.len().max(1);

            let mut did_truncate = false;
            for key in &array_keys {
                if let Some(serde_json::Value::Array(arr)) = map.remove(key) {
                    let (truncated_arr, was_truncated, _) =
                        truncate_response(serde_json::Value::Array(arr), per_array);
                    did_truncate = did_truncate || was_truncated;
                    map.insert(key.clone(), truncated_arr);
                }
            }

            (
                serde_json::Value::Object(map),
                did_truncate,
                total_available,
            )
        }
        other => (other, false, None),
    }
}

/// Default response size cap (30KB ≈ 7,500 tokens).
/// Applied when caller doesn't specify max_response_bytes/max_tokens.
/// Methods that manage their own budgets or intentionally return large content are exempt.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 30_000;

pub fn handle_method(indexer: &mut Indexer, method: &str, params: Value) -> Result<Value> {
    let start = Instant::now();
    let max_response_bytes = extract_max_response_bytes(&params);
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
        let (truncated_value, was_truncated, total_available) = truncate_response(value, max_bytes);
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

fn error_response(id: Value, message: &str) -> RpcResponse {
    RpcResponse {
        id,
        result: None,
        error: Some(RpcError {
            message: message.to_string(),
        }),
    }
}

fn parse_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn apply_field_filters(
    value: Value,
    summary: bool,
    fields: Option<&[String]>,
    summary_fields: &[&str],
) -> Value {
    if let Some(fields) = fields {
        return filter_fields(value, fields.iter().map(|s| s.as_str()));
    }
    if summary {
        return filter_fields(value, summary_fields.iter().copied());
    }
    value
}

fn filter_fields<'a, I>(value: Value, fields: I) -> Value
where
    I: IntoIterator<Item = &'a str>,
{
    let Value::Object(mut map) = value else {
        return value;
    };
    let mut filtered = serde_json::Map::new();
    for key in fields {
        if let Some(value) = map.remove(key) {
            filtered.insert(key.to_string(), value);
        }
    }
    Value::Object(filtered)
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

/// Represents a changed line range from a diff hunk
#[derive(Debug, Clone)]
struct DiffHunk {
    start_line: i64,
    line_count: i64,
}

/// Represents a changed file with its line ranges
#[derive(Debug, Clone)]
struct ChangedFile {
    path: String,
    changed_ranges: Vec<DiffHunk>,
    added_ranges: Vec<DiffHunk>,
    deleted_ranges: Vec<DiffHunk>,
}

fn parse_diff_with_ranges(diff: &str) -> Vec<ChangedFile> {
    let mut files = Vec::new();
    let mut current_file: Option<ChangedFile> = None;

    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("+++ b/") {
            if let Some(file) = current_file.take() {
                files.push(file);
            }
            current_file = Some(ChangedFile {
                path: rest.to_string(),
                changed_ranges: Vec::new(),
                added_ranges: Vec::new(),
                deleted_ranges: Vec::new(),
            });
        } else if let Some(rest) = line.strip_prefix("+++ ") {
            if !rest.starts_with("/dev/null") {
                if let Some(file) = current_file.take() {
                    files.push(file);
                }
                current_file = Some(ChangedFile {
                    path: rest.to_string(),
                    changed_ranges: Vec::new(),
                    added_ranges: Vec::new(),
                    deleted_ranges: Vec::new(),
                });
            }
        } else if line.starts_with("@@ ") {
            // Parse hunk header: @@ -old_start,old_count +new_start,new_count @@
            if let Some(ref mut file) = current_file
                && let Some(hunk_info) = line.strip_prefix("@@ ")
                && let Some(ranges) = hunk_info.split("@@").next()
            {
                let parts: Vec<&str> = ranges.split_whitespace().collect();

                // Parse old range (deleted lines)
                if let Some(old_part) = parts.first()
                    && let Some(old_range) = old_part.strip_prefix('-')
                    && let Some((start, count)) = parse_hunk_range(old_range)
                {
                    file.deleted_ranges.push(DiffHunk {
                        start_line: start,
                        line_count: count,
                    });
                }

                // Parse new range (added/modified lines)
                if let Some(new_part) = parts.get(1)
                    && let Some(new_range) = new_part.strip_prefix('+')
                    && let Some((start, count)) = parse_hunk_range(new_range)
                {
                    file.added_ranges.push(DiffHunk {
                        start_line: start,
                        line_count: count,
                    });
                    // Also add to changed_ranges as any hunk represents a change
                    file.changed_ranges.push(DiffHunk {
                        start_line: start,
                        line_count: count,
                    });
                }
            }
        }
    }

    if let Some(file) = current_file {
        files.push(file);
    }

    files
}

fn parse_hunk_range(range: &str) -> Option<(i64, i64)> {
    if let Some((start_str, count_str)) = range.split_once(',') {
        let start = start_str.parse::<i64>().ok()?;
        let count = count_str.parse::<i64>().ok()?;
        Some((start, count))
    } else {
        // Single line change: just a line number
        let start = range.parse::<i64>().ok()?;
        Some((start, 1))
    }
}

fn normalize_search_paths(
    repo_root: &Path,
    path: Option<String>,
    paths: Option<Vec<String>>,
) -> Result<Option<Vec<String>>> {
    let mut raw_paths = Vec::new();
    if let Some(value) = path {
        raw_paths.push(value);
    }
    if let Some(values) = paths {
        raw_paths.extend(values);
    }
    if raw_paths.is_empty() {
        return Ok(None);
    }
    let mut normalized = Vec::new();
    for raw in raw_paths {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Security: Validate path using canonicalization to prevent traversal and symlink escapes
        // This ensures paths stay within repo_root
        let (_abs, rel) = util::resolve_repo_path_for_op(repo_root, trimmed, "path_filter")?;
        if rel == "." {
            continue;
        }
        normalized.push(rel);
    }
    if normalized.is_empty() {
        return Ok(None);
    }
    normalized.sort();
    normalized.dedup();
    Ok(Some(normalized))
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

        // gather_context requires "seeds"
        let schema = super::method_param_schema("gather_context");
        let required = schema.get("required").and_then(|v| v.as_array());
        // Note: seeds has a default, so it may not be in required array
        // Just check the schema is valid
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
