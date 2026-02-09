mod handlers;

use crate::config::Config;
use crate::diagnostics;
use crate::indexer::{Indexer, http, proto, scan, test_detection, xref};
use crate::model::{
    AnalyzeDiffResult, BudgetInfo, ChangedSymbol, ContextLine, DiffImpactEntry, Edge, EdgeReference,
    ExplainRef, ExplainSymbolResult, FindTestsResult, FlowStatusEntry, FlowStatusResult, GrepHit,
    IndexChangeCounts, IndexStatus, ModuleEdge, ModuleMapResult, ModuleNode, ReferencesMetadata,
    ReferencesResult, RepoInsights, RiskAssessment, RiskFactor, RouteRefsResult, RpcSuggestion,
    SearchHit, Subgraph, Symbol, SymbolCompact, TestCoverageEntry, TestMatch, TestRef, TestSummary, TraceFlowResult,
    TraceHop, ValidationResult,
};
use crate::watch;
use crate::{search, subgraph, util};
use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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
    if let Some(max_bytes) = params.max_bytes {
        if max_bytes == 0 {
            result.add("max_bytes", "out_of_range", "max_bytes must be at least 1");
        }
    }

    // Validate depth
    if let Some(depth) = params.depth {
        if depth > 10 {
            result.add("depth", "out_of_range", "depth must be 10 or less");
        }
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
fn compact_symbol_value(symbol_value: &serde_json::Value) -> serde_json::Value {
    let keep_fields = ["id", "kind", "name", "qualname", "file_path", "start_line", "signature"];
    if let serde_json::Value::Object(map) = symbol_value {
        let compact: serde_json::Map<String, serde_json::Value> = map.iter()
            .filter(|(k, _)| keep_fields.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::Value::Object(compact)
    } else {
        symbol_value.clone()
    }
}

/// Apply compact format to a response value by converting all symbol objects
/// Estimate serialized size of a TraceHop, using compact symbol size when in compact mode.
fn estimate_hop_size(hop: &crate::model::TraceHop, compact: bool) -> usize {
    if compact {
        let mut hop_val = serde_json::to_value(hop).unwrap_or_default();
        if let Some(sym) = hop_val.get("symbol").cloned() {
            if let Some(obj) = hop_val.as_object_mut() {
                obj.insert("symbol".to_string(), compact_symbol_value(&sym));
            }
        }
        serde_json::to_string(&hop_val).unwrap_or_default().len()
    } else {
        serde_json::to_string(hop).unwrap_or_default().len()
    }
}

fn apply_compact_format(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(|item| {
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
            }).collect())
        }
        serde_json::Value::Object(mut map) => {
            // Process known array fields
            for key in ["results", "nodes", "incoming", "outgoing", "edges", "trace"] {
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
        other => other
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
struct FindSymbolParams {
    #[serde(alias = "symbol", alias = "name")]
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OpenSymbolParams {
    id: Option<i64>,
    qualname: Option<String>,
    include_snippet: Option<bool>,
    max_snippet_bytes: Option<usize>,
    include_symbol: Option<bool>,
    snippet_only: Option<bool>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OpenFileParams {
    path: String,
    start_line: Option<i64>,
    end_line: Option<i64>,
    max_bytes: Option<usize>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OverviewParams {
    summary: Option<bool>,
    fields: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReindexParams {
    summary: Option<bool>,
    fields: Option<Vec<String>>,
    resolve_edges: Option<bool>,
    mine_git: Option<bool>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ModuleMapParams {
    depth: Option<usize>,
    include_edges: Option<bool>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RepoMapParams {
    max_bytes: Option<usize>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct InsightsParams {
    complexity_limit: Option<usize>,
    min_complexity: Option<i64>,
    duplicate_limit: Option<usize>,
    duplicate_min_count: Option<i64>,
    duplicate_min_loc: Option<i64>,
    duplicate_per_group_limit: Option<usize>,
    coupling_limit: Option<usize>,
    include_staleness: Option<bool>,
    staleness_limit: Option<usize>,
    include_coupling_hotspots: Option<bool>,
    coupling_hotspots_limit: Option<usize>,
    coupling_hotspots_min_confidence: Option<f64>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
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
struct DuplicateGroupsParams {
    limit: Option<usize>,
    min_count: Option<i64>,
    min_loc: Option<i64>,
    per_group_limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TopCouplingParams {
    limit: Option<usize>,
    direction: Option<String>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CoChangesParams {
    path: Option<String>,
    paths: Option<Vec<String>>,
    qualname: Option<String>,
    limit: Option<usize>,
    min_confidence: Option<f64>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DeadSymbolsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct UnusedImportsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct OrphanTestsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DiagnosticsImportParams {
    path: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DiagnosticsListParams {
    limit: Option<usize>,
    offset: Option<usize>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    severity: Option<String>,
    rule_id: Option<String>,
    tool: Option<String>,
    languages: Option<Vec<String>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DiagnosticsSummaryParams {
    path: Option<String>,
    paths: Option<Vec<String>>,
    severity: Option<String>,
    rule_id: Option<String>,
    tool: Option<String>,
    languages: Option<Vec<String>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DiagnosticsRunParams {
    tools: Option<Vec<String>>,
    tool: Option<String>,
    languages: Option<Vec<String>>,
    output_dir: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct NeighborsParams {
    id: i64,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SubgraphParams {
    start_ids: Option<Vec<i64>>,
    #[serde(alias = "roots", alias = "start_qualnames", alias = "qualnames")]
    start_qualnames: Option<Vec<String>>,
    depth: Option<usize>,
    max_nodes: Option<usize>,
    languages: Option<Vec<String>>,
    kinds: Option<Vec<String>>,
    exclude_kinds: Option<Vec<String>>,
    resolved_only: Option<bool>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SearchParams {
    #[serde(alias = "pattern", alias = "text", alias = "q")]
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    scope: Option<String>,
    exclude_generated: Option<bool>,
    rank: Option<bool>,
    no_ignore: Option<bool>,
    context_lines: Option<usize>,
    include_symbol: Option<bool>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct GrepParams {
    #[serde(alias = "pattern", alias = "text", alias = "q")]
    query: String,
    limit: Option<usize>,
    include_text: Option<bool>,
    languages: Option<Vec<String>>,
    scope: Option<String>,
    exclude_generated: Option<bool>,
    rank: Option<bool>,
    no_ignore: Option<bool>,
    context_lines: Option<usize>,
    include_symbol: Option<bool>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReferencesParams {
    id: Option<i64>,
    qualname: Option<String>,
    direction: Option<String>,
    kinds: Option<Vec<String>>,
    limit: Option<usize>,
    include_symbols: Option<bool>,
    include_snippet: Option<bool>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FindTestsForParams {
    id: Option<i64>,
    qualname: Option<String>,
    query: Option<String>,
    include_indirect: Option<bool>,
    indirect_depth: Option<usize>,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AnalyzeImpactParams {
    id: Option<i64>,
    qualname: Option<String>,
    /// Multi-layer configuration
    enable_direct: Option<bool>,
    enable_test: Option<bool>,
    enable_historical: Option<bool>,
    /// Direct layer configuration
    max_depth: Option<usize>,
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
struct EdgesParams {
    kind: Option<String>,
    kinds: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    source_id: Option<i64>,
    source_qualname: Option<String>,
    target_id: Option<i64>,
    target_qualname: Option<String>,
    resolved_only: Option<bool>,
    min_confidence: Option<f64>,
    trace_id: Option<String>,
    event_after: Option<i64>,
    event_before: Option<i64>,
    limit: Option<usize>,
    offset: Option<usize>,
    include_symbols: Option<bool>,
    include_snippet: Option<bool>,
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

#[derive(Deserialize, schemars::JsonSchema)]
struct SuggestQualNamesParams {
    #[serde(alias = "query", alias = "pattern", alias = "name")]
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ChangedFilesParams {
    languages: Option<Vec<String>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct IndexStatusParams {
    languages: Option<Vec<String>>,
    include_paths: Option<bool>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct OnboardParams {
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct ReflectParams {
    text: Option<String>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct ChangedSinceParams {
    commit: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RouteRefsParams {
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    include_symbols: Option<bool>,
    include_snippet: Option<bool>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FlowStatusParams {
    limit: Option<usize>,
    edge_limit: Option<usize>,
    include_routes: Option<bool>,
    include_calls: Option<bool>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
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
struct GraphVersionsParams {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct ListMethodsParams {
    format: Option<String>,
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

#[derive(Serialize)]
struct DiagnosticsRunResult {
    repo_root: String,
    output_dir: String,
    languages: Vec<String>,
    summary: DiagnosticsRunSummary,
    tools: Vec<ToolRunResult>,
}

#[derive(Serialize)]
struct DiagnosticsRunSummary {
    ok: usize,
    skipped: usize,
    failed: usize,
    imported: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ToolRunStatus {
    Ok,
    Skipped,
    Failed,
}

#[derive(Serialize)]
struct ToolRunResult {
    name: String,
    status: ToolRunStatus,
    reason: Option<String>,
    message: Option<String>,
    hint: Option<String>,
    command: Option<Vec<String>>,
    sarif_path: Option<String>,
    imported: Option<usize>,
    exit_code: Option<i32>,
    duration_ms: Option<u128>,
    stderr: Option<String>,
}

/// Hard cap on result count to prevent huge responses that blow LLM context windows.
const MAX_RESPONSE_LIMIT: usize = 500;

pub const METHOD_LIST: &[&str] = &[
    // -- Workflow methods (recommended for LLM use) --
    "explain_symbol",   // One-call deep understanding of any symbol
    "analyze_diff",     // Diff-aware impact + test coverage + risk
    "find_tests_for",   // Find tests covering a symbol
    "trace_flow",       // Trace call chains end-to-end
    "module_map",       // Architecture overview DAG
    "repo_map",         // Compact architecture digest
    "gather_context",   // Budget-aware context assembly
    // -- Discovery --
    "find_symbol",
    "suggest_qualnames",
    "search",
    "search_text",
    "grep",
    "search_rg",
    // -- Symbol & file access --
    "open_symbol",
    "open_file",
    // -- Graph traversal --
    "references",
    "neighbors",
    "subgraph",
    "list_edges",
    "list_xrefs",
    // -- Impact & quality --
    "analyze_impact",
    "repo_insights",
    "top_complexity",
    "top_coupling",
    "co_changes",
    "duplicate_groups",
    "dead_symbols",
    "unused_imports",
    "orphan_tests",
    // -- Cross-language & routes --
    "route_refs",
    "flow_status",
    // -- Repository info --
    "repo_overview",
    "help",
    "list_methods",
    "list_languages",
    "list_graph_versions",
    "changed_files",
    "index_status",
    "reindex",
    // -- Diagnostics --
    "diagnostics_run",
    "diagnostics_import",
    "diagnostics_list",
    "diagnostics_summary",
    // -- Agent workflow --
    "onboard",
    "reflect",
    "changed_since",
];

const METHOD_ALIASES: &[(&str, &str)] = &[
    ("search", "search_text"),
    ("edges", "list_edges"),
    ("xrefs", "list_xrefs"),
    ("graph_versions", "list_graph_versions"),
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
        "find_symbol" => schema_value::<FindSymbolParams>(),
        "open_symbol" => schema_value::<OpenSymbolParams>(),
        "open_file" => schema_value::<OpenFileParams>(),
        "explain_symbol" => schema_value::<ExplainSymbolParams>(),
        "repo_overview" => schema_value::<OverviewParams>(),
        "reindex" => schema_value::<ReindexParams>(),
        "module_map" => schema_value::<ModuleMapParams>(),
        "repo_map" => schema_value::<RepoMapParams>(),
        "repo_insights" => schema_value::<InsightsParams>(),
        "top_complexity" => schema_value::<TopComplexityParams>(),
        "duplicate_groups" => schema_value::<DuplicateGroupsParams>(),
        "top_coupling" => schema_value::<TopCouplingParams>(),
        "co_changes" => schema_value::<CoChangesParams>(),
        "dead_symbols" => schema_value::<DeadSymbolsParams>(),
        "unused_imports" => schema_value::<UnusedImportsParams>(),
        "orphan_tests" => schema_value::<OrphanTestsParams>(),
        "neighbors" => schema_value::<NeighborsParams>(),
        "subgraph" => schema_value::<SubgraphParams>(),
        "search" | "search_text" => schema_value::<SearchParams>(),
        "grep" => schema_value::<GrepParams>(),
        "search_rg" => schema_value::<RgParams>(),
        "references" => schema_value::<ReferencesParams>(),
        "find_tests_for" => schema_value::<FindTestsForParams>(),
        "analyze_impact" => schema_value::<AnalyzeImpactParams>(),
        "analyze_diff" => schema_value::<AnalyzeDiffParams>(),
        "list_edges" | "list_xrefs" => schema_value::<EdgesParams>(),
        "suggest_qualnames" => schema_value::<SuggestQualNamesParams>(),
        "changed_files" => schema_value::<ChangedFilesParams>(),
        "index_status" => schema_value::<IndexStatusParams>(),
        "route_refs" => schema_value::<RouteRefsParams>(),
        "flow_status" => schema_value::<FlowStatusParams>(),
        "gather_context" => schema_value::<GatherContextParams>(),
        "list_graph_versions" => schema_value::<GraphVersionsParams>(),
        "list_methods" => schema_value::<ListMethodsParams>(),
        "trace_flow" => schema_value::<TraceFlowParams>(),
        "onboard" => schema_value::<OnboardParams>(),
        "reflect" => schema_value::<ReflectParams>(),
        "changed_since" => schema_value::<ChangedSinceParams>(),
        "diagnostics_run" => schema_value::<DiagnosticsRunParams>(),
        "diagnostics_import" => schema_value::<DiagnosticsImportParams>(),
        "diagnostics_list" => schema_value::<DiagnosticsListParams>(),
        "diagnostics_summary" => schema_value::<DiagnosticsSummaryParams>(),
        // Paramless methods
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
            if let Some(ref_val) = map.get("$ref").cloned() {
                if let Some(ref_str) = ref_val.as_str() {
                    // Extract definition name from "#/definitions/Name"
                    if let Some(name) = ref_str.strip_prefix("#/definitions/") {
                        if let Some(def) = definitions.get(name) {
                            let mut inlined = def.clone();
                            inline_refs(&mut inlined, definitions);
                            *value = inlined;
                            return;
                        }
                    }
                }
            }

            // Handle anyOf with null (Option<T> pattern): collapse to inner schema
            if let Some(any_of) = map.get("anyOf").cloned() {
                if let Some(variants) = any_of.as_array() {
                    if variants.len() == 2 {
                        let null_idx = variants.iter().position(|v| {
                            v.get("type").and_then(|t| t.as_str()) == Some("null")
                        });
                        if let Some(idx) = null_idx {
                            let inner_idx = 1 - idx;
                            let mut inner = variants[inner_idx].clone();
                            inline_refs(&mut inner, definitions);
                            *value = inner;
                            return;
                        }
                    }
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

struct MethodDoc {
    name: &'static str,
    summary: &'static str,
    key_params: &'static [&'static str],
}

const METHOD_DOCS: &[MethodDoc] = &[
    MethodDoc {
        name: "help",
        summary: "Show RPC help, aliases, and examples.",
        key_params: &[],
    },
    MethodDoc {
        name: "list_methods",
        summary: "List supported methods with short descriptions.",
        key_params: &["format (details|names)"],
    },
    MethodDoc {
        name: "list_languages",
        summary: "List supported languages and extension filters.",
        key_params: &[],
    },
    MethodDoc {
        name: "list_graph_versions",
        summary: "List indexed graph versions.",
        key_params: &["limit", "offset"],
    },
    MethodDoc {
        name: "repo_overview",
        summary: "Repo counts and last indexed metadata.",
        key_params: &["summary", "fields", "languages", "graph_version|as_of"],
    },
    MethodDoc {
        name: "repo_insights",
        summary: "Complexity, duplicates, diagnostics snapshot.",
        key_params: &[
            "languages",
            "path|paths",
            "complexity_limit",
            "duplicate_limit",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "module_map",
        summary: "Compact DAG of modules/packages with edge counts and metrics.",
        key_params: &[
            "depth",
            "include_edges",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "repo_map",
        summary: "Single text block architecture overview with modules, dependencies, and key symbols.",
        key_params: &[
            "max_bytes",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "top_complexity",
        summary: "Return most complex symbols.",
        key_params: &[
            "limit",
            "min_complexity",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "duplicate_groups",
        summary: "Return groups of duplicated symbols.",
        key_params: &[
            "limit",
            "min_count",
            "min_loc",
            "per_group_limit",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "top_coupling",
        summary: "Return symbols with highest fan-in (most callers) or fan-out (most callees).",
        key_params: &[
            "limit",
            "direction (in|out|both)",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "co_changes",
        summary: "Find files that frequently change together in git history (requires reindex with mine_git=true).",
        key_params: &[
            "path|paths|qualname",
            "limit",
            "min_confidence",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "dead_symbols",
        summary: "Find symbols with no references (potentially unused code).",
        key_params: &[
            "limit",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "unused_imports",
        summary: "Find import statements with no usage in the file.",
        key_params: &[
            "limit",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "orphan_tests",
        summary: "Find test functions whose target no longer exists.",
        key_params: &[
            "limit",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "find_symbol",
        summary: "Find symbols by name or qualname.",
        key_params: &["query", "limit", "languages", "graph_version|as_of"],
    },
    MethodDoc {
        name: "suggest_qualnames",
        summary: "Suggest symbol qualnames with fuzzy matching for typo correction.",
        key_params: &["query", "limit", "languages", "graph_version|as_of"],
    },
    MethodDoc {
        name: "open_symbol",
        summary: "Return symbol metadata and snippet.",
        key_params: &[
            "id|qualname",
            "include_snippet",
            "include_symbol",
            "snippet_only",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "explain_symbol",
        summary: "Complete context for a symbol: source, callers, callees, tests, implements (budget-aware).",
        key_params: &[
            "id|qualname|query",
            "max_bytes",
            "sections",
            "max_refs",
            "languages",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "analyze_diff",
        summary: "Analyze impact of a code diff or changed files. Returns affected symbols, test coverage, risk assessment, and callers. Provide either 'diff' (unified diff text) or 'paths' (changed file paths).",
        key_params: &[
            "diff|paths",
            "max_depth",
            "include_tests",
            "include_risk",
            "max_bytes",
            "languages",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "open_file",
        summary: "Read file content or line slice.",
        key_params: &["path", "start_line", "end_line", "max_bytes"],
    },
    MethodDoc {
        name: "neighbors",
        summary: "Adjacent symbols and edges for a symbol id.",
        key_params: &["id", "languages", "graph_version|as_of"],
    },
    MethodDoc {
        name: "subgraph",
        summary: "Traverse edges from root ids or qualnames.",
        key_params: &[
            "start_ids|roots",
            "depth",
            "max_nodes",
            "kinds",
            "exclude_kinds",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "analyze_impact",
        summary: "Multi-layer impact analysis. Answers \"what breaks if I change this?\" using direct graph traversal, test discovery, and historical co-change patterns. Each layer can be enabled/disabled. Results include confidence scores (0.0-1.0).",
        key_params: &[
            "id|qualname",
            "enable_direct",
            "enable_test",
            "enable_historical",
            "max_depth",
            "direction",
            "limit",
            "min_confidence",
            "include_paths",
            "languages",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "find_tests_for",
        summary: "Find direct and indirect test callers for a symbol. Replaces 3+ manual calls (references + test file filtering + caller-of-caller lookups).",
        key_params: &[
            "id|qualname|query",
            "include_indirect",
            "indirect_depth",
            "limit",
            "languages",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "references",
        summary: "Incoming/outgoing edges for a symbol.",
        key_params: &[
            "id|qualname",
            "direction",
            "kinds",
            "limit",
            "include_symbols",
            "include_snippet",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "trace_flow",
        summary: "Trace call chain from start symbol (replaces iterative references calls).",
        key_params: &[
            "start_id|start_qualname",
            "end_id|end_qualname",
            "direction (downstream|upstream)",
            "max_hops",
            "kinds",
            "include_snippets",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "list_edges",
        summary: "Query edges by kind, path, or symbol.",
        key_params: &[
            "kind|kinds",
            "path|paths",
            "limit",
            "offset",
            "min_confidence",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "list_xrefs",
        summary: "Query cross-language edges.",
        key_params: &[
            "path|paths",
            "limit",
            "offset",
            "min_confidence",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "route_refs",
        summary: "Route/URL string references grouped by normalized path.",
        key_params: &[
            "query",
            "path|paths",
            "limit",
            "languages",
            "include_symbols",
            "include_snippet",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "flow_status",
        summary: "Find routes without calls and calls without routes.",
        key_params: &[
            "limit",
            "edge_limit",
            "include_routes",
            "include_calls",
            "path|paths",
            "languages",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "gather_context",
        summary: "Assemble LLM-ready context from symbols, files, and searches.",
        key_params: &[
            "seeds",
            "max_bytes",
            "depth",
            "max_nodes",
            "include_snippets",
            "include_related",
            "languages",
            "path|paths",
            "graph_version|as_of",
        ],
    },
    MethodDoc {
        name: "search_rg",
        summary: "Raw ripgrep regex search with context lines. Best for regex patterns (e.g. 'def\\s+trigger'). Returns raw text matches with optional surrounding context.",
        key_params: &[
            "query",
            "path|paths",
            "limit",
            "context_lines",
            "include_text",
            "include_symbol",
            "globs",
            "case_sensitive",
        ],
    },
    MethodDoc {
        name: "grep",
        summary: "Literal text search with scope filtering and symbol resolution. Best for exact string matching within code/tests/docs scopes. Use search_rg for regex.",
        key_params: &[
            "query",
            "path|paths",
            "limit",
            "include_text",
            "scope (code|tests|docs|examples|all)",
            "rank",
            "include_symbol",
        ],
    },
    MethodDoc {
        name: "search_text",
        summary: "Ranked text search with fuzzy matching and scope filtering. Default search method — use this for natural language queries and concept search. Alias: search.",
        key_params: &[
            "query",
            "path|paths",
            "limit",
            "scope (code|tests|docs|examples|all)",
            "rank",
            "include_symbol",
        ],
    },
    MethodDoc {
        name: "search",
        summary: "Alias for search_text. Ranked fuzzy text search — the default search method for finding code by concept or keyword.",
        key_params: &[
            "query",
            "path|paths",
            "limit",
            "scope (code|tests|docs|examples|all)",
            "rank",
            "include_symbol",
        ],
    },
    MethodDoc {
        name: "changed_files",
        summary: "List changed files vs the index.",
        key_params: &["languages"],
    },
    MethodDoc {
        name: "index_status",
        summary: "Index freshness and reindex hint.",
        key_params: &["languages", "include_paths"],
    },
    MethodDoc {
        name: "reindex",
        summary: "Reindex repository and return stats. Optionally resolve unresolved edges.",
        key_params: &["summary", "fields", "resolve_edges"],
    },
    MethodDoc {
        name: "diagnostics_run",
        summary: "Run analyzers and import SARIF diagnostics.",
        key_params: &["tools|tool", "languages", "output_dir"],
    },
    MethodDoc {
        name: "diagnostics_import",
        summary: "Import SARIF diagnostics file.",
        key_params: &["path"],
    },
    MethodDoc {
        name: "diagnostics_list",
        summary: "List diagnostics with filters.",
        key_params: &[
            "limit",
            "offset",
            "path|paths",
            "severity",
            "rule_id",
            "tool",
            "languages",
        ],
    },
    MethodDoc {
        name: "diagnostics_summary",
        summary: "Diagnostics counts by severity and tool.",
        key_params: &["path|paths", "severity", "rule_id", "tool", "languages"],
    },
    // -- Agent workflow --
    MethodDoc {
        name: "onboard",
        summary: "One-call project orientation: overview, modules, languages, index status, suggested queries.",
        key_params: &["languages", "graph_version|as_of"],
    },
    MethodDoc {
        name: "reflect",
        summary: "Echo text back. Use to structure reasoning before acting.",
        key_params: &["text"],
    },
    MethodDoc {
        name: "changed_since",
        summary: "Files changed since a git commit. Useful for multi-session continuity.",
        key_params: &["commit"],
    },
];

fn alias_map() -> serde_json::Map<String, Value> {
    let mut aliases = serde_json::Map::new();
    for (alias, target) in METHOD_ALIASES {
        aliases.insert((*alias).to_string(), Value::String((*target).to_string()));
    }
    aliases
}

fn alias_for_map() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    for (alias, target) in METHOD_ALIASES {
        map.insert(*alias, *target);
    }
    map
}

fn alias_reverse_map() -> HashMap<&'static str, Vec<&'static str>> {
    let mut map: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
    for (alias, target) in METHOD_ALIASES {
        map.entry(*target).or_default().push(*alias);
    }
    map
}

fn method_docs_json() -> Vec<Value> {
    let alias_for = alias_for_map();
    let alias_reverse = alias_reverse_map();
    METHOD_DOCS
        .iter()
        .map(|doc| {
            let mut entry = serde_json::Map::new();
            entry.insert("name".to_string(), Value::String(doc.name.to_string()));
            entry.insert(
                "summary".to_string(),
                Value::String(doc.summary.to_string()),
            );
            if !doc.key_params.is_empty() {
                entry.insert("key_params".to_string(), json!(doc.key_params));
            }
            if let Some(target) = alias_for.get(doc.name) {
                entry.insert(
                    "alias_for".to_string(),
                    Value::String((*target).to_string()),
                );
            }
            if let Some(aliases) = alias_reverse.get(doc.name) {
                entry.insert("aliases".to_string(), json!(aliases));
            }
            Value::Object(entry)
        })
        .collect()
}

fn method_help() -> Value {
    let aliases = alias_map();
    let method_docs = method_docs_json();
    json!({
        "summary": "lidx indexes a repo into sqlite and serves JSONL RPC over stdin/stdout.",
        "start_here": "Use explain_symbol to deeply understand any symbol (one call replaces 5+). Use analyze_diff for change impact. Use find_tests_for for test coverage. Use trace_flow for call chains. Use repo_map for quick architecture overview. Use module_map for detailed architecture DAG. Use gather_context for budget-aware context assembly.",
        "global_params": {
            "max_response_bytes": "Optional: Truncate response to fit within byte budget (default: unlimited)",
            "max_tokens": "Optional: Truncate response to fit within token budget (~4 bytes per token, default: unlimited). When truncated, response becomes {data, truncated: true, max_response_bytes}",
        },
        "decision_guide": {
            "understand a symbol": "explain_symbol — returns source, callers, callees, tests, implements in one call",
            "assess change impact": "analyze_diff — provide diff text or changed file paths, get affected symbols + test coverage + risk",
            "find test coverage": "find_tests_for — finds direct and indirect test callers for any symbol",
            "trace call chain": "trace_flow — follow calls downstream or upstream, optionally to a target",
            "quick architecture overview": "repo_map — single text block with modules, dependencies, and key symbols",
            "detailed architecture": "module_map — DAG of modules/packages with edge counts",
            "assemble context": "gather_context — budget-aware context from symbol/file/search seeds",
            "find a symbol by name": "find_symbol — search by name/qualname, returns signatures",
            "search code by concept": "search — ranked fuzzy text search with scope filtering",
            "search by regex": "search_rg — raw ripgrep regex with context lines",
            "search exact text": "grep — literal text search with scope filtering",
            "read symbol source": "open_symbol — metadata + source snippet",
            "read file contents": "open_file — full file or line range",
            "explore call graph": "references — raw incoming/outgoing edges for a symbol",
            "expand graph neighborhood": "subgraph — BFS from root symbols with edge kind filtering",
            "cross-language links": "list_xrefs — edges crossing language boundaries (e.g. C#→Proto, Python→SQL)",
            "HTTP route mapping": "route_refs — find route definitions and their callers",
            "refactoring impact": "analyze_impact — multi-layer impact analysis with direct, test, and historical layers",
            "code quality": "repo_insights / top_complexity / top_coupling / duplicate_groups",
        },
        "edge_kinds": [
            "CALLS — function/method call",
            "IMPORTS — import/using statement",
            "CONTAINS — parent contains child (module→class, class→method)",
            "EXTENDS — class inheritance",
            "IMPLEMENTS — interface implementation",
            "INHERITS — base class relationship",
            "RPC_IMPL — gRPC service implementation (C#/Python method implements Proto service)",
            "RPC_CALL — gRPC client call",
            "RPC_ROUTE — Proto service definition",
            "HTTP_ROUTE — HTTP endpoint definition (controller action, Flask route)",
            "HTTP_CALL — HTTP client call (HttpClient, requests)",
            "CHANNEL_PUBLISH — message bus publish (Azure Service Bus, RabbitMQ, etc.)",
            "CHANNEL_SUBSCRIBE — message bus subscribe/handler",
            "XREF — cross-language reference",
            "MODULE_FILE — module maps to file",
            "IMPORTS_FILE — file imports another file",
        ],
        "enum_values": {
            "scope": ["code", "tests", "docs", "examples", "all"],
            "direction (references)": ["in", "out"],
            "direction (trace_flow)": ["downstream", "upstream"],
            "direction (analyze_impact)": ["downstream", "upstream"],
            "direction (top_coupling)": ["in", "out", "both"],
            "sections (explain_symbol)": ["source", "callers", "callees", "tests", "implements"],
            "format (explain_symbol)": ["full", "signatures"],
        },
        "methods": METHOD_LIST,
        "method_docs": method_docs,
        "aliases": aliases,
        "examples": [
            { "method": "explain_symbol", "params": { "query": "DataProduct", "max_bytes": 40000 } },
            { "method": "explain_symbol", "params": { "qualname": "mymodule.MyClass", "sections": ["source", "callers"], "format": "signatures" } },
            { "method": "analyze_diff", "params": { "paths": ["src/models/data_product.py"], "include_tests": true, "include_risk": true } },
            { "method": "find_tests_for", "params": { "query": "DataProduct", "include_indirect": true } },
            { "method": "trace_flow", "params": { "start_qualname": "mymodule.trigger_pipeline", "direction": "downstream", "max_hops": 5 } },
            { "method": "trace_flow", "params": { "start_qualname": "mymodule.save_result", "direction": "upstream", "max_hops": 3 } },
            { "method": "repo_map", "params": { "max_bytes": 8000 } },
            { "method": "module_map", "params": { "depth": 2, "include_edges": true } },
            { "method": "gather_context", "params": { "seeds": [{"type": "symbol", "qualname": "mymodule.MyClass"}, {"type": "search", "query": "data product", "limit": 3}], "max_bytes": 50000, "depth": 2 } },
            { "method": "repo_overview", "params": { "summary": true } },
            { "method": "find_symbol", "params": { "query": "Indexer", "limit": 10 } },
            { "method": "search", "params": { "query": "handle_method", "limit": 20, "scope": "code" } },
            { "method": "open_symbol", "params": { "qualname": "crate::indexer::Indexer::reindex", "include_snippet": true } },
            { "method": "analyze_impact", "params": { "qualname": "crate::db::Db::read_conn", "max_depth": 3, "direction": "upstream" } },
            { "method": "references", "params": { "qualname": "crate::indexer::Indexer::reindex", "direction": "out", "kinds": ["CALLS"] } },
            { "method": "subgraph", "params": { "roots": ["mymodule.MyClass"], "depth": 2, "max_nodes": 30, "kinds": ["CALLS", "RPC_IMPL"] } },
            { "method": "list_xrefs", "params": { "min_confidence": 0.8, "limit": 50 } },
            { "method": "route_refs", "params": { "query": "/api/users/123" } },
            { "method": "search_rg", "params": { "query": "def\\s+greet", "context_lines": 8 } },
            { "method": "index_status", "params": { "include_paths": false } },
            { "method": "diagnostics_run", "params": { "languages": ["python"], "tools": ["ruff"] } }
        ],
        "cli_examples": [
            "lidx reindex --repo .",
            r#"lidx request --method repo_overview --params '{"summary":true}'"#,
            r#"lidx request --method list_languages --params '{}'"#,
            r#"lidx request --method list_graph_versions --params '{"limit":5}'"#,
            r#"lidx request --method search --params '{"query":"Indexer","limit":10}'"#,
            r#"lidx request --method references --params '{"qualname":"crate::indexer::Indexer::reindex","direction":"out","kinds":["CALLS"]}'"#,
            r#"lidx request --method list_xrefs --params '{"min_confidence":0.8,"limit":50}'"#,
            r#"lidx request --method route_refs --params '{"query":"/api/users/123"}'"#,
            r#"lidx request --method flow_status --params '{"limit":50,"include_routes":false,"include_calls":false}'"#,
            r#"lidx request --method index_status --params '{"include_paths":false}'"#,
            "lidx diagnostics-run --repo . --tool ruff --language python",
            r#"lidx request --method search_rg --params '{"query":"def\\s+greet","context_lines":8}'"#,
            "lidx serve --repo . --watch auto",
            "lidx mcp-serve --repo ."
        ]
    })
}

fn method_list(params: Value) -> Result<Value> {
    let params = if params.is_null() {
        ListMethodsParams::default()
    } else {
        serde_json::from_value(params)?
    };
    let format = params
        .format
        .as_deref()
        .unwrap_or("details")
        .trim()
        .to_ascii_lowercase();
    if format == "names" || format == "name" || format == "list" {
        return Ok(json!(METHOD_LIST));
    }
    Ok(json!({
        "methods": method_docs_json(),
        "aliases": alias_map(),
        "names": METHOD_LIST,
    }))
}

fn method_list_languages() -> Value {
    let mut languages = Vec::new();
    for spec in scan::language_specs() {
        languages.push(json!({
            "name": spec.name,
            "extensions": spec.extensions,
        }));
    }
    let mut filters = serde_json::Map::new();
    for filter in scan::language_filters() {
        filters.insert(filter.name.to_string(), json!(filter.languages));
    }
    json!({
        "languages": languages,
        "filters": filters,
    })
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
fn truncate_response(value: serde_json::Value, max_bytes: usize) -> (serde_json::Value, bool, Option<usize>) {
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
                let mid = (low + high + 1) / 2;
                let slice = serde_json::Value::Array(arr[..mid].to_vec());
                let size = serde_json::to_string(&slice).unwrap_or_default().len();
                if size <= max_bytes {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            (serde_json::Value::Array(arr[..low].to_vec()), true, Some(original_len))
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
            if array_keys.len() == 1 {
                if let Some(serde_json::Value::Array(arr)) = map.get(&array_keys[0]) {
                    total_available = Some(arr.len());
                }
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

            (serde_json::Value::Object(map), did_truncate, total_available)
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
    // Extract token budget before params is moved
    let max_response_bytes = extract_max_response_bytes(&params);
    let value = match method {
        "help" => method_help(),
        "list_methods" => method_list(params)?,
        "list_languages" => method_list_languages(),
        "list_graph_versions" => {
            let params: GraphVersionsParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(50);
            let offset = params.offset.unwrap_or(0);
            let versions = indexer.db().list_graph_versions(limit, offset)?;
            json!(versions)
        }
        "repo_overview" => handlers::handle_repo_overview(indexer, params)?,
        "repo_insights" => handlers::handle_repo_insights(indexer, params)?,
        "module_map" => handlers::handle_module_map(indexer, params)?,
        "repo_map" => handlers::handle_repo_map(indexer, params)?,
        "top_complexity" => handlers::handle_top_complexity(indexer, params)?,
        "duplicate_groups" => handlers::handle_duplicate_groups(indexer, params)?,
        "top_coupling" => handlers::handle_top_coupling(indexer, params)?,
        "co_changes" => handlers::handle_co_changes(indexer, params)?,
        "dead_symbols" => handlers::handle_dead_symbols(indexer, params)?,
        "unused_imports" => handlers::handle_unused_imports(indexer, params)?,
        "orphan_tests" => handlers::handle_orphan_tests(indexer, params)?,
        "find_symbol" => handlers::handle_find_symbol(indexer, params)?,
        "suggest_qualnames" => handlers::handle_suggest_qualnames(indexer, params)?,
        "open_symbol" => handlers::handle_open_symbol(indexer, params)?,
        "explain_symbol" => handlers::handle_explain_symbol(indexer, params)?,
        "open_file" => handlers::handle_open_file(indexer, params)?,
        "neighbors" => handlers::handle_neighbors(indexer, params)?,
        "subgraph" => handlers::handle_subgraph(indexer, params)?,
        "find_tests_for" => handlers::handle_find_tests_for(indexer, params)?,
        "analyze_impact" => handlers::handle_analyze_impact(indexer, params)?,
        "analyze_diff" => handlers::handle_analyze_diff(indexer, params)?,
        "references" => handlers::handle_references(indexer, params)?,
        "trace_flow" => handlers::handle_trace_flow(indexer, params)?,
        "list_edges" => {
            let params: EdgesParams = serde_json::from_value(params)?;
            list_edges_response(indexer, params, None, false, false)?
        }
        "list_xrefs" => {
            let params: EdgesParams = serde_json::from_value(params)?;
            list_edges_response(indexer, params, Some("XREF"), true, true)?
        }
        "route_refs" => handlers::handle_route_refs(indexer, params)?,
        "flow_status" => handlers::handle_flow_status(indexer, params)?,
        "search_rg" => handlers::handle_search_rg(indexer, params)?,
        "search" | "search_text" => handlers::handle_search_text(indexer, params)?,
        "grep" => handlers::handle_grep(indexer, params)?,
        "changed_files" => {
            let params: ChangedFilesParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let changed = indexer.changed_files(languages.as_deref())?;
            json!(changed)
        }
        "index_status" => handlers::handle_index_status(indexer, params)?,
        "reindex" => handlers::handle_reindex(indexer, params)?,
        "diagnostics_run" => {
            let params: DiagnosticsRunParams = serde_json::from_value(params)?;
            let result = diagnostics_run(indexer, params)?;
            json!(result)
        }
        "diagnostics_import" => handlers::handle_diagnostics_import(indexer, params)?,
        "diagnostics_list" => handlers::handle_diagnostics_list(indexer, params)?,
        "diagnostics_summary" => handlers::handle_diagnostics_summary(indexer, params)?,
        "gather_context" => handlers::handle_gather_context(indexer, params)?,
        "reflect" => {
            let params: ReflectParams = serde_json::from_value(params)?;
            let text = params.text.unwrap_or_default();
            if text.len() > 50_000 {
                anyhow::bail!("reflect text exceeds 50KB limit");
            }
            json!({ "reflected": text })
        }
        "onboard" => handlers::handle_onboard(indexer, params)?,
        "changed_since" => handlers::handle_changed_since(indexer, params)?,
        other => {
            return Err(anyhow::anyhow!("unknown method: {other}"));
        }
    };

    // Log slow queries
    let elapsed = start.elapsed();
    if elapsed.as_millis() > 100 {
        eprintln!("lidx: Slow query: {} took {:?}", method, elapsed);
    }

    // Apply response size cap: explicit param > default (exempt methods get no cap)
    let exempt = matches!(method, "gather_context" | "open_file" | "help" | "onboard" | "reflect");
    let effective_max = max_response_bytes.or_else(|| if exempt { None } else { Some(DEFAULT_MAX_RESPONSE_BYTES) });
    if let Some(max_bytes) = effective_max {
        let (truncated_value, was_truncated, total_available) = truncate_response(value, max_bytes);
        if was_truncated {
            let mut response = json!({
                "data": truncated_value,
                "truncated": true,
                "max_response_bytes": max_bytes,
            });
            if let Some(total) = total_available {
                response.as_object_mut().unwrap().insert("total_available".to_string(), json!(total));
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

fn normalize_context_lines(value: Option<usize>, default: usize) -> usize {
    value.unwrap_or(default).min(5)
}

fn resolve_graph_version(indexer: &Indexer, value: Option<i64>) -> Result<i64> {
    if let Some(version) = value {
        return Ok(version);
    }
    indexer.db().current_graph_version()
}

fn detect_language(file_path: &str) -> String {
    if file_path.ends_with(".py") {
        "python".to_string()
    } else if file_path.ends_with(".cs") {
        "csharp".to_string()
    } else if file_path.ends_with(".ts") || file_path.ends_with(".tsx") {
        "typescript".to_string()
    } else if file_path.ends_with(".js") || file_path.ends_with(".jsx") {
        "javascript".to_string()
    } else if file_path.ends_with(".rs") {
        "rust".to_string()
    } else if file_path.ends_with(".proto") {
        "proto".to_string()
    } else if file_path.ends_with(".sql") {
        "sql".to_string()
    } else if file_path.ends_with(".md") {
        "markdown".to_string()
    } else {
        "unknown".to_string()
    }
}

/// Detect the type of language boundary crossing
fn detect_boundary_type(edge_kind: &str, source_lang: &str, target_lang: &str) -> String {
    match edge_kind {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => "grpc".to_string(),
        "HTTP_CALL" | "HTTP_ROUTE" => "http".to_string(),
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus".to_string(),
        "XREF" if source_lang == "csharp" && target_lang == "sql" => "stored_procedure".to_string(),
        "XREF" if source_lang == "sql" && target_lang == "csharp" => "stored_procedure".to_string(),
        "XREF" => "xref".to_string(),
        _ => "other".to_string(),
    }
}

/// Build a human-readable boundary detail string
fn build_boundary_detail(boundary_type: &str, source_lang: &str, target_lang: &str) -> String {
    let source_display = source_lang.replace("csharp", "C#")
        .replace("javascript", "JavaScript")
        .replace("typescript", "TypeScript");
    let target_display = target_lang.replace("csharp", "C#")
        .replace("javascript", "JavaScript")
        .replace("typescript", "TypeScript");

    match boundary_type {
        "grpc" => format!("{} → {} via gRPC", source_display, target_display),
        "http" => format!("{} → {} via HTTP", source_display, target_display),
        "message_bus" => format!("{} → {} via message bus", source_display, target_display),
        "stored_procedure" => format!("{} → {} via stored procedure", source_display, target_display),
        "xref" => format!("{} → {} via cross-reference", source_display, target_display),
        _ => format!("{} → {}", source_display, target_display),
    }
}

/// Extract protocol context from edge detail field (RPC, HTTP, or channel edges)
fn extract_protocol_context(edge: &Edge) -> Option<serde_json::Value> {
    let detail_str = edge.detail.as_ref()?;
    let detail: serde_json::Value = serde_json::from_str(detail_str).ok()?;

    match edge.kind.as_str() {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => {
            let service = detail.get("service")?.as_str()?;
            let rpc = detail.get("rpc")?.as_str()?;
            let package = detail.get("package").and_then(|p| p.as_str());
            let framework = detail.get("framework").and_then(|f| f.as_str()).unwrap_or("grpc");
            Some(json!({
                "framework": framework,
                "service": service,
                "rpc": rpc,
                "package": package,
            }))
        }
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => {
            let channel_name = detail.get("channel").and_then(|c| c.as_str());
            let framework = detail.get("framework").and_then(|f| f.as_str()).unwrap_or("unknown");
            let role = detail.get("role").and_then(|r| r.as_str()).unwrap_or("unknown");
            Some(json!({
                "framework": framework,
                "channel": channel_name,
                "role": role,
            }))
        }
        "HTTP_CALL" | "HTTP_ROUTE" => {
            let method = detail.get("method").and_then(|m| m.as_str());
            let path = detail.get("path").and_then(|p| p.as_str());
            let framework = detail.get("framework").and_then(|f| f.as_str()).unwrap_or("http");
            Some(json!({
                "framework": framework,
                "method": method,
                "path": path,
            }))
        }
        _ => None,
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

fn normalize_rg_context_lines(value: Option<usize>) -> usize {
    value.unwrap_or(0).min(50)
}

struct RgSearchOptions {
    include_text: bool,
    case_sensitive: Option<bool>,
    fixed_string: bool,
    hidden: bool,
    no_ignore: bool,
    follow: bool,
    globs: Vec<String>,
    paths: Vec<PathBuf>,
}

#[derive(Clone, Copy)]
enum EdgeDirection {
    In,
    Out,
    Both,
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
        if line.starts_with("+++ b/") {
            if let Some(file) = current_file.take() {
                files.push(file);
            }
            current_file = Some(ChangedFile {
                path: line[6..].to_string(),
                changed_ranges: Vec::new(),
                added_ranges: Vec::new(),
                deleted_ranges: Vec::new(),
            });
        } else if line.starts_with("+++ ") && !line.starts_with("+++ /dev/null") {
            if let Some(file) = current_file.take() {
                files.push(file);
            }
            current_file = Some(ChangedFile {
                path: line[4..].to_string(),
                changed_ranges: Vec::new(),
                added_ranges: Vec::new(),
                deleted_ranges: Vec::new(),
            });
        } else if line.starts_with("@@ ") {
            // Parse hunk header: @@ -old_start,old_count +new_start,new_count @@
            if let Some(ref mut file) = current_file {
                if let Some(hunk_info) = line.strip_prefix("@@ ") {
                    if let Some(ranges) = hunk_info.split("@@").next() {
                        let parts: Vec<&str> = ranges.split_whitespace().collect();

                        // Parse old range (deleted lines)
                        if let Some(old_part) = parts.get(0) {
                            if let Some(old_range) = old_part.strip_prefix('-') {
                                if let Some((start, count)) = parse_hunk_range(old_range) {
                                    file.deleted_ranges.push(DiffHunk {
                                        start_line: start,
                                        line_count: count,
                                    });
                                }
                            }
                        }

                        // Parse new range (added/modified lines)
                        if let Some(new_part) = parts.get(1) {
                            if let Some(new_range) = new_part.strip_prefix('+') {
                                if let Some((start, count)) = parse_hunk_range(new_range) {
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

fn parse_edge_direction(raw: Option<&str>) -> Result<EdgeDirection> {
    let Some(raw) = raw else {
        return Ok(EdgeDirection::Both);
    };
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(EdgeDirection::Both);
    }
    let direction = match value.as_str() {
        "in" | "incoming" | "inbound" => EdgeDirection::In,
        "out" | "outgoing" | "outbound" => EdgeDirection::Out,
        "both" | "all" | "any" => EdgeDirection::Both,
        _ => anyhow::bail!("unknown direction: {raw}"),
    };
    Ok(direction)
}

fn normalize_search_paths(
    repo_root: &PathBuf,
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
        let (_abs, rel) = resolve_repo_path_for_op(repo_root, trimmed, "path_filter")?;
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

fn diagnostics_run(
    indexer: &mut Indexer,
    params: DiagnosticsRunParams,
) -> Result<DiagnosticsRunResult> {
    let language_override = scan::normalize_language_filter(params.languages.as_deref())?;
    let mut languages = resolve_diagnostics_languages(indexer, language_override)?;
    normalize_language_list(&mut languages);

    let mut requested_tools = Vec::new();
    if let Some(tool) = params.tool {
        requested_tools.push(tool);
    }
    if let Some(tools) = params.tools {
        requested_tools.extend(tools);
    }
    let tools = if requested_tools.is_empty() {
        default_tools_for_languages(&languages)
    } else {
        normalize_tool_list(requested_tools)
    };

    let output_dir = resolve_output_dir(indexer.repo_root(), params.output_dir.as_deref())?;
    fs::create_dir_all(&output_dir)?;

    let language_set: HashSet<String> = languages.iter().cloned().collect();
    let mut results = Vec::new();
    for tool in tools {
        let result = match tool.as_str() {
            "eslint" => run_eslint(indexer, &output_dir, &language_set),
            "ruff" => run_ruff(indexer, &output_dir, &language_set),
            "semgrep" => run_semgrep(indexer, &output_dir, &language_set),
            "dotnet" => run_dotnet(indexer, &output_dir, &language_set),
            "clippy" => run_clippy(indexer, &output_dir, &language_set),
            other => ToolRunResult {
                name: other.to_string(),
                status: ToolRunStatus::Skipped,
                reason: Some("unknown_tool".to_string()),
                message: Some(format!("Unknown diagnostics tool '{other}'.")),
                hint: None,
                command: None,
                sarif_path: None,
                imported: None,
                exit_code: None,
                duration_ms: None,
                stderr: None,
            },
        };
        results.push(result);
    }

    let mut summary = DiagnosticsRunSummary {
        ok: 0,
        skipped: 0,
        failed: 0,
        imported: 0,
    };
    for result in &results {
        match result.status {
            ToolRunStatus::Ok => summary.ok += 1,
            ToolRunStatus::Skipped => summary.skipped += 1,
            ToolRunStatus::Failed => summary.failed += 1,
        }
        if let Some(imported) = result.imported {
            summary.imported += imported;
        }
    }

    Ok(DiagnosticsRunResult {
        repo_root: indexer.repo_root().to_string_lossy().to_string(),
        output_dir: render_path(indexer.repo_root(), &output_dir),
        languages,
        summary,
        tools: results,
    })
}

fn resolve_diagnostics_languages(
    indexer: &Indexer,
    override_languages: Option<Vec<String>>,
) -> Result<Vec<String>> {
    if let Some(languages) = override_languages {
        return Ok(languages);
    }
    let from_db = indexer.db().list_languages(indexer.graph_version())?;
    if !from_db.is_empty() {
        return Ok(from_db);
    }
    let scanned = scan::scan_repo_with_options(indexer.repo_root(), scan::ScanOptions::default())?;
    let mut languages: Vec<String> = scanned.into_iter().map(|file| file.language).collect();
    languages.sort();
    languages.dedup();
    Ok(languages)
}

fn normalize_language_list(languages: &mut Vec<String>) {
    for language in languages.iter_mut() {
        *language = language.trim().to_ascii_lowercase();
    }
    languages.sort();
    languages.dedup();
}

fn normalize_tool_list(tools: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for raw in tools {
        let key = raw.trim().to_ascii_lowercase();
        if key.is_empty() {
            continue;
        }
        let normalized = match key.as_str() {
            "cargo-clippy" | "cargo_clippy" => "clippy",
            value => value,
        };
        if seen.insert(normalized.to_string()) {
            out.push(normalized.to_string());
        }
    }
    out
}

fn default_tools_for_languages(languages: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |tool: &str| {
        if seen.insert(tool.to_string()) {
            out.push(tool.to_string());
        }
    };
    let has = |lang: &str| languages.iter().any(|value| value == lang);
    if has("javascript") || has("typescript") || has("tsx") {
        push("eslint");
    }
    if has("python") {
        push("ruff");
    }
    if has("rust") {
        push("clippy");
    }
    if has("csharp") {
        push("dotnet");
    }
    if has("sql") || has("postgres") || has("tsql") || has("markdown") || has("proto") {
        push("semgrep");
    }
    out
}

fn resolve_output_dir(repo_root: &PathBuf, output_dir: Option<&str>) -> Result<PathBuf> {
    let dir = match output_dir {
        Some(raw) if !raw.trim().is_empty() => {
            let trimmed = raw.trim();
            let candidate = PathBuf::from(trimmed);

            // Security: Reject absolute paths outside repo
            if candidate.is_absolute() {
                eprintln!("lidx: Security: diagnostics_run absolute path rejected");
                anyhow::bail!("diagnostics_run output_dir must be relative to repo root");
            }

            // Security: Normalize and check for path traversal
            // We can't use canonicalize() because the directory might not exist yet
            // Instead, check that the normalized path still starts with repo_root
            let normalized_repo = repo_root
                .canonicalize()
                .unwrap_or_else(|_| repo_root.clone());

            // Check each component for .. that would escape
            let mut current = normalized_repo.clone();
            for component in candidate.components() {
                match component {
                    std::path::Component::Normal(part) => {
                        current.push(part);
                    }
                    std::path::Component::ParentDir => {
                        if !current.pop() || !current.starts_with(&normalized_repo) {
                            eprintln!("lidx: Security: diagnostics_run path escapes repo root");
                            anyhow::bail!("diagnostics_run output_dir escapes repo root");
                        }
                    }
                    std::path::Component::CurDir => {}
                    _ => {}
                }
            }

            current
        }
        _ => repo_root.join(".lidx").join("diagnostics"),
    };
    Ok(dir)
}

fn render_path(repo_root: &PathBuf, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .map(|rel| util::normalize_path(rel))
        .unwrap_or_else(|_| path.to_string_lossy().to_string())
}

fn has_language(languages: &HashSet<String>, targets: &[&str]) -> bool {
    targets.iter().any(|value| languages.contains(*value))
}

fn run_eslint(
    indexer: &mut Indexer,
    output_dir: &Path,
    languages: &HashSet<String>,
) -> ToolRunResult {
    let name = "eslint";
    if !has_language(languages, &["javascript", "typescript", "tsx"]) {
        return tool_skipped(
            name,
            "no_language_match",
            "No JavaScript/TypeScript files detected.",
            None,
        );
    }
    if !eslint_config_present(indexer.repo_root()) {
        return tool_skipped(
            name,
            "config_missing",
            "No ESLint config found in repo root.",
            Some("Add eslint.config.js/.eslintrc or package.json config."),
        );
    }
    let cmd_path = resolve_node_tool(indexer.repo_root(), "eslint")
        .unwrap_or_else(|| OsString::from("eslint"));
    let sarif_path = output_dir.join("eslint.sarif");
    let mut cmd = Command::new(&cmd_path);
    cmd.current_dir(indexer.repo_root());
    cmd.arg(".")
        .arg("-f")
        .arg("sarif")
        .arg("-o")
        .arg(&sarif_path);
    let command_display = vec![
        cmd_path.to_string_lossy().to_string(),
        ".".to_string(),
        "-f".to_string(),
        "sarif".to_string(),
        "-o".to_string(),
        sarif_path.to_string_lossy().to_string(),
    ];
    run_sarif_command(
        indexer,
        name,
        cmd,
        command_display,
        &sarif_path,
        "eslint not found on PATH or node_modules/.bin.",
        Some("Install with npm/pnpm/yarn (e.g., npm i -D eslint)."),
    )
}

fn run_ruff(
    indexer: &mut Indexer,
    output_dir: &Path,
    languages: &HashSet<String>,
) -> ToolRunResult {
    let name = "ruff";
    if !has_language(languages, &["python"]) {
        return tool_skipped(name, "no_language_match", "No Python files detected.", None);
    }
    let cmd_path =
        resolve_python_tool(indexer.repo_root(), "ruff").unwrap_or_else(|| OsString::from("ruff"));
    let sarif_path = output_dir.join("ruff.sarif");
    let mut cmd = Command::new(&cmd_path);
    cmd.current_dir(indexer.repo_root());
    cmd.arg("check")
        .arg(".")
        .arg("--output-format")
        .arg("sarif")
        .arg("-o")
        .arg(&sarif_path);
    let command_display = vec![
        cmd_path.to_string_lossy().to_string(),
        "check".to_string(),
        ".".to_string(),
        "--output-format".to_string(),
        "sarif".to_string(),
        "-o".to_string(),
        sarif_path.to_string_lossy().to_string(),
    ];
    run_sarif_command(
        indexer,
        name,
        cmd,
        command_display,
        &sarif_path,
        "ruff not found on PATH or .venv/venv.",
        Some("Install with pipx install ruff or pip install ruff."),
    )
}

fn run_semgrep(
    indexer: &mut Indexer,
    output_dir: &Path,
    languages: &HashSet<String>,
) -> ToolRunResult {
    let name = "semgrep";
    if !has_language(languages, &["sql", "postgres", "tsql", "markdown", "proto"]) {
        return tool_skipped(
            name,
            "no_language_match",
            "No SQL/Markdown/Proto files detected.",
            None,
        );
    }
    let Some(config) = semgrep_config(indexer.repo_root()) else {
        return tool_skipped(
            name,
            "config_missing",
            "No Semgrep config found in repo root.",
            Some("Add .semgrep.yml or .semgrep.yaml."),
        );
    };
    let cmd_path = resolve_python_tool(indexer.repo_root(), "semgrep")
        .unwrap_or_else(|| OsString::from("semgrep"));
    let sarif_path = output_dir.join("semgrep.sarif");
    let mut cmd = Command::new(&cmd_path);
    cmd.current_dir(indexer.repo_root());
    cmd.arg("--config")
        .arg(&config)
        .arg("--sarif")
        .arg("-o")
        .arg(&sarif_path);
    let command_display = vec![
        cmd_path.to_string_lossy().to_string(),
        "--config".to_string(),
        config.to_string_lossy().to_string(),
        "--sarif".to_string(),
        "-o".to_string(),
        sarif_path.to_string_lossy().to_string(),
    ];
    run_sarif_command(
        indexer,
        name,
        cmd,
        command_display,
        &sarif_path,
        "semgrep not found on PATH or .venv/venv.",
        Some("Install with pipx install semgrep or pip install semgrep."),
    )
}

fn run_dotnet(
    indexer: &mut Indexer,
    output_dir: &Path,
    languages: &HashSet<String>,
) -> ToolRunResult {
    let name = "dotnet";
    if !has_language(languages, &["csharp"]) {
        return tool_skipped(name, "no_language_match", "No C# files detected.", None);
    }
    let Some(project) = find_dotnet_project(indexer.repo_root()) else {
        return tool_skipped(
            name,
            "config_missing",
            "No .sln or .csproj found.",
            Some("Add a solution/project file or pass a tools list."),
        );
    };
    let sarif_path = output_dir.join("dotnet.sarif");
    let mut cmd = Command::new("dotnet");
    cmd.current_dir(indexer.repo_root());
    cmd.arg("build")
        .arg(&project)
        .arg(format!("-p:ErrorLog={}", sarif_path.display()))
        .arg("-p:ErrorLogFormat=Sarif");
    let command_display = vec![
        "dotnet".to_string(),
        "build".to_string(),
        project.to_string_lossy().to_string(),
        format!("-p:ErrorLog={}", sarif_path.display()),
        "-p:ErrorLogFormat=Sarif".to_string(),
    ];
    run_sarif_command(
        indexer,
        name,
        cmd,
        command_display,
        &sarif_path,
        "dotnet not found on PATH.",
        Some("Install the .NET SDK and ensure dotnet is on PATH."),
    )
}

fn run_clippy(
    indexer: &mut Indexer,
    output_dir: &Path,
    languages: &HashSet<String>,
) -> ToolRunResult {
    let name = "clippy";
    if !has_language(languages, &["rust"]) {
        return tool_skipped(name, "no_language_match", "No Rust files detected.", None);
    }
    let Some(cargo_root) = find_cargo_root(indexer.repo_root()) else {
        return tool_skipped(
            name,
            "config_missing",
            "No Cargo.toml found.",
            Some("Add a Cargo.toml workspace or pass a tools list."),
        );
    };
    let sarif_path = output_dir.join("clippy.sarif");
    let clippy_sarif = OsString::from("clippy-sarif");
    if !command_available(&clippy_sarif) {
        return tool_skipped(
            name,
            "not_installed",
            "clippy-sarif not found on PATH.",
            Some("Install with cargo install clippy-sarif."),
        );
    }
    if !command_available(&OsString::from("cargo")) {
        return tool_skipped(
            name,
            "not_installed",
            "cargo not found on PATH.",
            Some("Install Rust toolchain and ensure cargo is on PATH."),
        );
    }

    let _ = fs::remove_file(&sarif_path);
    let start = Instant::now();

    let mut cargo_cmd = Command::new("cargo");
    cargo_cmd
        .current_dir(&cargo_root)
        .arg("clippy")
        .arg("--message-format=json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut cargo_child = match cargo_cmd.spawn() {
        Ok(child) => child,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return tool_skipped(
                name,
                "not_installed",
                "cargo not found on PATH.",
                Some("Install Rust toolchain and ensure cargo is on PATH."),
            );
        }
        Err(err) => {
            return tool_failed(
                name,
                Some(format!("Failed to start cargo: {err}")),
                Some(vec![
                    "cargo".to_string(),
                    "clippy".to_string(),
                    "--message-format=json".to_string(),
                ]),
                None,
                Some(start.elapsed().as_millis()),
                None,
                None,
            );
        }
    };

    let stdout = match cargo_child.stdout.take() {
        Some(pipe) => pipe,
        None => {
            return tool_failed(
                name,
                Some("Failed to capture cargo output.".to_string()),
                Some(vec![
                    "cargo".to_string(),
                    "clippy".to_string(),
                    "--message-format=json".to_string(),
                ]),
                None,
                Some(start.elapsed().as_millis()),
                None,
                None,
            );
        }
    };

    let sarif_file = match File::create(&sarif_path) {
        Ok(file) => file,
        Err(err) => {
            return tool_failed(
                name,
                Some(format!("Failed to create SARIF output file: {err}")),
                Some(vec![
                    "cargo".to_string(),
                    "clippy".to_string(),
                    "--message-format=json".to_string(),
                    "|".to_string(),
                    "clippy-sarif".to_string(),
                    ">".to_string(),
                    sarif_path.to_string_lossy().to_string(),
                ]),
                None,
                Some(start.elapsed().as_millis()),
                None,
                None,
            );
        }
    };
    let mut sarif_cmd = Command::new(&clippy_sarif);
    sarif_cmd
        .current_dir(&cargo_root)
        .stdin(Stdio::from(stdout))
        .stdout(Stdio::from(sarif_file))
        .stderr(Stdio::piped());
    let mut sarif_child = match sarif_cmd.spawn() {
        Ok(child) => child,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return tool_skipped(
                name,
                "not_installed",
                "clippy-sarif not found on PATH.",
                Some("Install with cargo install clippy-sarif."),
            );
        }
        Err(err) => {
            return tool_failed(
                name,
                Some(format!("Failed to start clippy-sarif: {err}")),
                Some(vec![
                    "cargo".to_string(),
                    "clippy".to_string(),
                    "--message-format=json".to_string(),
                    "|".to_string(),
                    "clippy-sarif".to_string(),
                    ">".to_string(),
                    sarif_path.to_string_lossy().to_string(),
                ]),
                None,
                Some(start.elapsed().as_millis()),
                None,
                None,
            );
        }
    };

    let sarif_stderr = read_child_stderr(&mut sarif_child);
    let sarif_status = sarif_child.wait().ok();
    let cargo_stderr = read_child_stderr(&mut cargo_child);
    let cargo_status = cargo_child.wait().ok();

    let duration_ms = start.elapsed().as_millis();
    let exit_code = cargo_status.and_then(|status| status.code());

    if !sarif_path.exists()
        || fs::metadata(&sarif_path)
            .map(|meta| meta.len())
            .unwrap_or(0)
            == 0
    {
        return tool_failed(
            name,
            Some("clippy-sarif did not produce a SARIF file.".to_string()),
            Some(vec![
                "cargo".to_string(),
                "clippy".to_string(),
                "--message-format=json".to_string(),
                "|".to_string(),
                "clippy-sarif".to_string(),
                ">".to_string(),
                sarif_path.to_string_lossy().to_string(),
            ]),
            exit_code,
            Some(duration_ms),
            merge_stderr(&[cargo_stderr.as_str(), sarif_stderr.as_str()]),
            None,
        );
    }

    match import_sarif(indexer, &sarif_path) {
        Ok(imported) => ToolRunResult {
            name: name.to_string(),
            status: ToolRunStatus::Ok,
            reason: None,
            message: sarif_status.and_then(|status| {
                if status.success() {
                    None
                } else {
                    Some("clippy-sarif exited with non-zero status.".to_string())
                }
            }),
            hint: None,
            command: Some(vec![
                "cargo".to_string(),
                "clippy".to_string(),
                "--message-format=json".to_string(),
                "|".to_string(),
                "clippy-sarif".to_string(),
                ">".to_string(),
                sarif_path.to_string_lossy().to_string(),
            ]),
            sarif_path: Some(render_path(indexer.repo_root(), &sarif_path)),
            imported: Some(imported),
            exit_code,
            duration_ms: Some(duration_ms),
            stderr: merge_stderr(&[cargo_stderr.as_str(), sarif_stderr.as_str()]),
        },
        Err(err) => tool_failed(
            name,
            Some(format!("Failed to import SARIF: {err}")),
            Some(vec![
                "cargo".to_string(),
                "clippy".to_string(),
                "--message-format=json".to_string(),
                "|".to_string(),
                "clippy-sarif".to_string(),
                ">".to_string(),
                sarif_path.to_string_lossy().to_string(),
            ]),
            exit_code,
            Some(duration_ms),
            merge_stderr(&[cargo_stderr.as_str(), sarif_stderr.as_str()]),
            Some(render_path(indexer.repo_root(), &sarif_path)),
        ),
    }
}

fn run_sarif_command(
    indexer: &mut Indexer,
    name: &str,
    mut cmd: Command,
    command_display: Vec<String>,
    sarif_path: &Path,
    not_installed_message: &str,
    not_installed_hint: Option<&str>,
) -> ToolRunResult {
    let _ = fs::remove_file(sarif_path);
    let start = Instant::now();
    let output = match cmd.output() {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return tool_skipped(
                name,
                "not_installed",
                not_installed_message,
                not_installed_hint,
            );
        }
        Err(err) => {
            return tool_failed(
                name,
                Some(format!("Failed to run tool: {err}")),
                Some(command_display),
                None,
                Some(start.elapsed().as_millis()),
                None,
                None,
            );
        }
    };
    let duration_ms = start.elapsed().as_millis();
    let exit_code = output.status.code();
    let stderr = truncate_output(&output.stderr, 2000);

    if !sarif_path.exists() || fs::metadata(sarif_path).map(|meta| meta.len()).unwrap_or(0) == 0 {
        let message = if let Some(code) = exit_code {
            format!("Command exited with code {code}; SARIF file missing.")
        } else {
            "Command failed; SARIF file missing.".to_string()
        };
        return tool_failed(
            name,
            Some(message),
            Some(command_display),
            exit_code,
            Some(duration_ms),
            if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            },
            None,
        );
    }

    match import_sarif(indexer, sarif_path) {
        Ok(imported) => ToolRunResult {
            name: name.to_string(),
            status: ToolRunStatus::Ok,
            reason: None,
            message: None,
            hint: None,
            command: Some(command_display),
            sarif_path: Some(render_path(indexer.repo_root(), sarif_path)),
            imported: Some(imported),
            exit_code,
            duration_ms: Some(duration_ms),
            stderr: if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            },
        },
        Err(err) => tool_failed(
            name,
            Some(format!("Failed to import SARIF: {err}")),
            Some(command_display),
            exit_code,
            Some(duration_ms),
            if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            },
            Some(render_path(indexer.repo_root(), sarif_path)),
        ),
    }
}

fn import_sarif(indexer: &mut Indexer, sarif_path: &Path) -> Result<usize> {
    let content = util::read_to_string(sarif_path)?;
    let diagnostics = diagnostics::parse_sarif(&content, indexer.repo_root())?;
    indexer.db_mut().insert_diagnostics(&diagnostics)
}

fn tool_skipped(name: &str, reason: &str, message: &str, hint: Option<&str>) -> ToolRunResult {
    ToolRunResult {
        name: name.to_string(),
        status: ToolRunStatus::Skipped,
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        hint: hint.map(|value| value.to_string()),
        command: None,
        sarif_path: None,
        imported: None,
        exit_code: None,
        duration_ms: None,
        stderr: None,
    }
}

fn tool_failed(
    name: &str,
    message: Option<String>,
    command: Option<Vec<String>>,
    exit_code: Option<i32>,
    duration_ms: Option<u128>,
    stderr: Option<String>,
    sarif_path: Option<String>,
) -> ToolRunResult {
    ToolRunResult {
        name: name.to_string(),
        status: ToolRunStatus::Failed,
        reason: None,
        message,
        hint: None,
        command,
        sarif_path,
        imported: None,
        exit_code,
        duration_ms,
        stderr,
    }
}

fn truncate_output(bytes: &[u8], max_bytes: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text.trim();
    util::truncate_str_bytes(trimmed, max_bytes)
}

fn merge_stderr(values: &[&str]) -> Option<String> {
    let mut out = String::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(trimmed);
    }
    if out.is_empty() { None } else { Some(out) }
}

fn read_child_stderr(child: &mut std::process::Child) -> String {
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf);
    }
    let trimmed = buf.trim();
    util::truncate_str_bytes(trimmed, 2000)
}

fn eslint_config_present(repo_root: &PathBuf) -> bool {
    let candidates = [
        "eslint.config.js",
        "eslint.config.cjs",
        "eslint.config.mjs",
        ".eslintrc",
        ".eslintrc.js",
        ".eslintrc.cjs",
        ".eslintrc.json",
        ".eslintrc.yaml",
        ".eslintrc.yml",
        "package.json",
    ];
    candidates.iter().any(|name| repo_root.join(name).exists())
}

fn semgrep_config(repo_root: &PathBuf) -> Option<PathBuf> {
    let candidates = [
        ".semgrep.yml",
        ".semgrep.yaml",
        "semgrep.yml",
        "semgrep.yaml",
    ];
    for name in candidates {
        let path = repo_root.join(name);
        if path.is_file() {
            return Some(path);
        }
    }
    let dir = repo_root.join(".semgrep");
    if dir.is_dir() {
        return Some(dir);
    }
    None
}

fn find_cargo_root(repo_root: &PathBuf) -> Option<PathBuf> {
    let root_manifest = repo_root.join("Cargo.toml");
    if root_manifest.is_file() {
        return Some(repo_root.clone());
    }
    find_first_named_file(repo_root, &["Cargo.toml"], 6)
        .and_then(|path| path.parent().map(|parent| parent.to_path_buf()))
}

fn find_dotnet_project(repo_root: &PathBuf) -> Option<PathBuf> {
    find_first_extension(repo_root, "sln", 6)
        .or_else(|| find_first_extension(repo_root, "csproj", 6))
}

fn find_first_named_file(repo_root: &PathBuf, names: &[&str], max_depth: usize) -> Option<PathBuf> {
    let mut best: Option<(usize, PathBuf)> = None;
    let mut builder = WalkBuilder::new(repo_root);
    builder.max_depth(Some(max_depth)).hidden(true);
    for entry in builder.build() {
        let entry = match entry {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(file_name) = entry.path().file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !names.iter().any(|name| *name == file_name) {
            continue;
        }
        let depth = entry
            .path()
            .strip_prefix(repo_root)
            .map(|rel| rel.components().count())
            .unwrap_or(usize::MAX);
        match &best {
            Some((best_depth, _)) if *best_depth <= depth => {}
            _ => best = Some((depth, entry.path().to_path_buf())),
        }
    }
    best.map(|(_, path)| path)
}

fn find_first_extension(repo_root: &PathBuf, ext: &str, max_depth: usize) -> Option<PathBuf> {
    let mut best: Option<(usize, PathBuf)> = None;
    let mut builder = WalkBuilder::new(repo_root);
    builder.max_depth(Some(max_depth)).hidden(true);
    for entry in builder.build() {
        let entry = match entry {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(extension) = entry.path().extension().and_then(|value| value.to_str()) else {
            continue;
        };
        if extension != ext {
            continue;
        }
        let depth = entry
            .path()
            .strip_prefix(repo_root)
            .map(|rel| rel.components().count())
            .unwrap_or(usize::MAX);
        match &best {
            Some((best_depth, _)) if *best_depth <= depth => {}
            _ => best = Some((depth, entry.path().to_path_buf())),
        }
    }
    best.map(|(_, path)| path)
}

fn resolve_node_tool(repo_root: &PathBuf, tool: &str) -> Option<OsString> {
    let base = repo_root.join("node_modules").join(".bin");
    let candidates = [tool.to_string(), format!("{tool}.cmd")];
    for name in candidates {
        let path = base.join(name);
        if path.is_file() {
            return Some(path.into_os_string());
        }
    }
    None
}

fn resolve_python_tool(repo_root: &PathBuf, tool: &str) -> Option<OsString> {
    let candidates = [
        repo_root.join(".venv").join("bin").join(tool),
        repo_root.join("venv").join("bin").join(tool),
        repo_root
            .join(".venv")
            .join("Scripts")
            .join(format!("{tool}.exe")),
        repo_root
            .join("venv")
            .join("Scripts")
            .join(format!("{tool}.exe")),
    ];
    for path in candidates {
        if path.is_file() {
            return Some(path.into_os_string());
        }
    }
    None
}

fn command_available(cmd: &OsString) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

fn normalize_edge_kinds(kinds: &[String]) -> Option<HashSet<String>> {
    if kinds.is_empty() {
        return Some(HashSet::new());
    }
    let mut set = HashSet::new();
    for raw in kinds {
        let Some(normalized) = normalize_edge_kind(raw) else {
            continue;
        };
        if normalized == "*" || normalized == "ALL" || normalized == "ANY" {
            return None;
        }
        set.insert(normalized);
    }
    Some(set)
}

fn normalize_edge_kinds_exclude(kinds: &[String]) -> (HashSet<String>, bool) {
    if kinds.is_empty() {
        return (HashSet::new(), false);
    }
    let mut set = HashSet::new();
    for raw in kinds {
        let Some(normalized) = normalize_edge_kind(raw) else {
            continue;
        };
        if normalized == "*" || normalized == "ALL" || normalized == "ANY" {
            return (HashSet::new(), true);
        }
        set.insert(normalized);
    }
    (set, false)
}

fn normalize_edge_kind(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed == "*" {
        return Some("*".to_string());
    }
    let mut out = String::new();
    let mut last_was_sep = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            last_was_sep = false;
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            if !last_was_sep {
                out.push('_');
                last_was_sep = true;
            }
        }
    }
    let normalized = out.trim_matches('_').to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn edge_kind_matches(kind: &str, filter: &Option<HashSet<String>>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.is_empty() {
        return false;
    }
    let normalized = normalize_edge_kind(kind).unwrap_or_else(|| kind.trim().to_ascii_uppercase());
    filter.contains(&normalized)
}

fn normalize_edge_kind_list(
    kind: Option<String>,
    kinds: Option<Vec<String>>,
    force_kind: Option<&str>,
) -> Option<Vec<String>> {
    let mut values = Vec::new();
    if let Some(force_kind) = force_kind {
        values.push(force_kind.to_string());
    }
    if let Some(kind) = kind {
        values.push(kind);
    }
    if let Some(kinds) = kinds {
        values.extend(kinds);
    }
    if values.is_empty() {
        return None;
    }
    let mut normalized = Vec::new();
    for raw in values {
        let Some(value) = normalize_edge_kind(&raw) else {
            continue;
        };
        if value == "*" || value == "ALL" || value == "ANY" {
            return None;
        }
        normalized.push(value);
    }
    if normalized.is_empty() {
        return Some(Vec::new());
    }
    normalized.sort();
    normalized.dedup();
    Some(normalized)
}

fn list_edges_response(
    indexer: &mut Indexer,
    params: EdgesParams,
    force_kind: Option<&str>,
    default_include_symbols: bool,
    default_include_snippet: bool,
) -> Result<Value> {
    let limit = params.limit.unwrap_or(200).min(MAX_RESPONSE_LIMIT);
    let offset = params.offset.unwrap_or(0);
    let include_symbols = params.include_symbols.unwrap_or(default_include_symbols);
    let include_snippet = params.include_snippet.unwrap_or(default_include_snippet);
    let languages = scan::normalize_language_filter(params.languages.as_deref())?;
    let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
    let kinds = normalize_edge_kind_list(params.kind, params.kinds, force_kind);
    let resolved_only = params.resolved_only.unwrap_or(false);
    let graph_version = resolve_graph_version(indexer, params.graph_version)?;

    let source_id = match (params.source_id, params.source_qualname.as_deref()) {
        (Some(id), _) => Some(id),
        (None, Some(qualname)) => {
            let qualname = qualname.trim();
            if qualname.is_empty() {
                None
            } else {
                indexer.db().lookup_symbol_id_filtered(
                    qualname,
                    languages.as_deref(),
                    graph_version,
                )?
            }
        }
        _ => None,
    };
    if params.source_qualname.is_some() && source_id.is_none() {
        return Ok(json!([]));
    }

    let mut target_id = params.target_id;
    let mut target_qualname = None;
    if target_id.is_none() {
        if let Some(raw) = params.target_qualname.as_deref() {
            let raw = raw.trim();
            if !raw.is_empty() {
                let resolved = indexer.db().lookup_symbol_id_filtered(
                    raw,
                    languages.as_deref(),
                    graph_version,
                )?;
                if let Some(id) = resolved {
                    target_id = Some(id);
                } else {
                    target_qualname = Some(raw.to_string());
                }
            }
        }
    }

    let edges = indexer.db().list_edges(
        limit,
        offset,
        languages.as_deref(),
        paths.as_deref(),
        kinds.as_deref(),
        source_id,
        target_id,
        target_qualname.as_ref(),
        resolved_only,
        params.min_confidence,
        graph_version,
        params.trace_id.as_ref(),
        params.event_after,
        params.event_before,
    )?;

    let mut symbol_map = HashMap::new();
    if include_symbols {
        let mut ids = HashSet::new();
        for edge in &edges {
            if let Some(id) = edge.source_symbol_id {
                ids.insert(id);
            }
            if let Some(id) = edge.target_symbol_id {
                ids.insert(id);
            }
        }
        if !ids.is_empty() {
            let mut id_list: Vec<i64> = ids.into_iter().collect();
            id_list.sort_unstable();
            let symbols = indexer.db().symbols_by_ids(&id_list, None, graph_version)?;
            for symbol in symbols {
                symbol_map.insert(symbol.id, symbol);
            }
        }
    }
    let refs = build_edge_references(edges, &symbol_map, include_symbols, include_snippet);
    Ok(json!(refs))
}

fn build_edge_references(
    edges: Vec<crate::model::Edge>,
    symbols: &HashMap<i64, crate::model::Symbol>,
    include_symbols: bool,
    include_snippet: bool,
) -> Vec<EdgeReference> {
    edges
        .into_iter()
        .map(|mut edge| {
            if !include_snippet {
                edge.evidence_snippet = None;
            }
            let source = if include_symbols {
                edge.source_symbol_id
                    .and_then(|id| symbols.get(&id).cloned())
            } else {
                None
            };
            let target = if include_symbols {
                edge.target_symbol_id
                    .and_then(|id| symbols.get(&id).cloned())
            } else {
                None
            };
            EdgeReference {
                edge,
                source,
                target,
            }
        })
        .collect()
}

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

    #[test]
    fn test_detect_language() {
        use super::detect_language;
        assert_eq!(detect_language("test.py"), "python");
        assert_eq!(detect_language("test.cs"), "csharp");
        assert_eq!(detect_language("test.rs"), "rust");
        assert_eq!(detect_language("test.proto"), "proto");
        assert_eq!(detect_language("test.ts"), "typescript");
        assert_eq!(detect_language("test.tsx"), "typescript");
        assert_eq!(detect_language("test.js"), "javascript");
        assert_eq!(detect_language("test.jsx"), "javascript");
        assert_eq!(detect_language("test.sql"), "sql");
        assert_eq!(detect_language("test.md"), "markdown");
        assert_eq!(detect_language("test.txt"), "unknown");
    }

    #[test]
    fn test_detect_boundary_type() {
        use super::detect_boundary_type;

        // gRPC boundaries
        assert_eq!(detect_boundary_type("RPC_IMPL", "proto", "csharp"), "grpc");
        assert_eq!(detect_boundary_type("RPC_CALL", "csharp", "proto"), "grpc");

        // Stored procedure boundaries
        assert_eq!(detect_boundary_type("XREF", "csharp", "sql"), "stored_procedure");
        assert_eq!(detect_boundary_type("XREF", "sql", "csharp"), "stored_procedure");

        // Generic XREF
        assert_eq!(detect_boundary_type("XREF", "python", "csharp"), "xref");

        // Other edges
        assert_eq!(detect_boundary_type("CALLS", "python", "python"), "other");
    }

    #[test]
    fn test_build_boundary_detail() {
        use super::build_boundary_detail;

        assert_eq!(
            build_boundary_detail("grpc", "proto", "csharp"),
            "proto → C# via gRPC"
        );
        assert_eq!(
            build_boundary_detail("stored_procedure", "csharp", "sql"),
            "C# → sql via stored procedure"
        );
        assert_eq!(
            build_boundary_detail("xref", "python", "csharp"),
            "python → C# via cross-reference"
        );
    }

    #[test]
    fn test_extract_protocol_context() {
        use super::extract_protocol_context;

        // Test with RPC_IMPL edge
        let rpc_impl_edge = Edge {
            id: 1,
            file_path: "test.cs".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: Some(200),
            target_qualname: Some("myservice.MyService.GetUser".to_string()),
            detail: Some(r#"{"framework":"grpc-csharp","role":"server","service":"MyService","rpc":"GetUser","package":"myservice","raw":"/myservice.MyService/GetUser"}"#.to_string()),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: None,
            graph_version: 1,
            commit_sha: None,
            trace_id: None,
            span_id: None,
            event_ts: None,
        };

        let context = extract_protocol_context(&rpc_impl_edge);
        assert!(context.is_some());
        let context = context.unwrap();
        assert_eq!(context["service"], "MyService");
        assert_eq!(context["rpc"], "GetUser");
        assert_eq!(context["package"], "myservice");
        assert_eq!(context["framework"], "grpc-csharp");

        // Test with non-RPC edge
        let call_edge = Edge {
            id: 2,
            file_path: "test.rs".to_string(),
            kind: "CALLS".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: Some(200),
            target_qualname: Some("module::function".to_string()),
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
        };

        let context = extract_protocol_context(&call_edge);
        assert!(context.is_none());
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
        // find_symbol requires "query"
        let schema = super::method_param_schema("find_symbol");
        let required = schema.get("required").and_then(|v| v.as_array());
        assert!(required.is_some(), "find_symbol should have required fields");
        let required: Vec<&str> = required
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            required.contains(&"query"),
            "find_symbol should require 'query', got: {:?}",
            required
        );

        // neighbors requires "id"
        let schema = super::method_param_schema("neighbors");
        let required = schema.get("required").and_then(|v| v.as_array());
        assert!(required.is_some(), "neighbors should have required fields");
        let required: Vec<&str> = required
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            required.contains(&"id"),
            "neighbors should require 'id', got: {:?}",
            required
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
        assert!(seeds_schema.is_some(), "gather_context should have seeds.items");
        let seeds_items = seeds_schema.unwrap();
        let one_of = seeds_items.get("oneOf");
        assert!(one_of.is_some(), "seeds.items should have oneOf: {}", seeds_items);
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
            assert!(props.is_some(), "variant should have properties: {}", variant);
            assert!(
                props.unwrap().get("type").is_some(),
                "variant should have 'type' discriminator property: {}",
                variant
            );
        }
    }
}

fn resolve_repo_path(repo_root: &PathBuf, raw_path: &str) -> Result<(PathBuf, String)> {
    resolve_repo_path_for_op(repo_root, raw_path, "open_file")
}

fn resolve_repo_path_for_op(
    repo_root: &PathBuf,
    raw_path: &str,
    op: &str,
) -> Result<(PathBuf, String)> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        eprintln!("lidx: Security: {} rejected: empty path", op);
        return Err(anyhow::anyhow!("{op} requires path"));
    }
    let candidate = PathBuf::from(trimmed);
    let abs = if candidate.is_absolute() {
        candidate
    } else {
        repo_root.join(&candidate)
    };
    let abs = match abs.canonicalize() {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            eprintln!("lidx: Security: {} path not found", op);
            return Err(anyhow::anyhow!("{op} path not found: {trimmed}"));
        }
        Err(err) => {
            return Err(anyhow::Error::from(err))
                .with_context(|| format!("resolve {}", abs.display()));
        }
    };
    let root = repo_root
        .canonicalize()
        .with_context(|| format!("resolve {}", repo_root.display()))?;
    if !abs.starts_with(&root) {
        eprintln!("lidx: Security: {} path escapes repo root", op);
        return Err(anyhow::anyhow!("{op} path escapes repo root"));
    }
    let rel = util::normalize_rel_path(&root, &abs)?;
    Ok((abs, rel))
}

fn resolve_rg_paths(
    repo_root: &PathBuf,
    path: Option<String>,
    paths: Option<Vec<String>>,
) -> Result<Vec<PathBuf>> {
    let mut raw_paths = Vec::new();
    if let Some(value) = path {
        raw_paths.push(value);
    }
    if let Some(values) = paths {
        raw_paths.extend(values);
    }
    if raw_paths.is_empty() {
        return Ok(vec![repo_root.clone()]);
    }
    let mut resolved = Vec::new();
    for raw in raw_paths {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (abs, _) = resolve_repo_path_for_op(repo_root, trimmed, "search_rg")?;
        resolved.push(abs);
    }
    if resolved.is_empty() {
        return Ok(vec![repo_root.clone()]);
    }
    Ok(resolved)
}

fn search_rg(
    repo_root: &PathBuf,
    query: &str,
    limit: usize,
    options: RgSearchOptions,
) -> Result<Vec<GrepHit>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let build_cmd = |allow_no_require_git: bool| {
        let mut cmd = Command::new("rg");
        cmd.arg("--json").arg("-n").arg("--column");
        if options.fixed_string {
            cmd.arg("-F");
        }
        if let Some(case_sensitive) = options.case_sensitive {
            if case_sensitive {
                cmd.arg("-s");
            } else {
                cmd.arg("-i");
            }
        }
        if options.hidden {
            cmd.arg("--hidden");
        }
        if options.no_ignore {
            cmd.arg("--no-ignore");
        } else if allow_no_require_git {
            cmd.arg("--no-require-git");
        }
        if options.follow {
            cmd.arg("--follow");
        }
        for glob in &options.globs {
            let trimmed = glob.trim();
            if trimmed.is_empty() {
                continue;
            }
            cmd.arg("-g").arg(trimmed);
        }
        cmd.arg("--").arg(query);
        for path in &options.paths {
            cmd.arg(path);
        }
        cmd
    };
    let mut output = match build_cmd(true).output() {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(anyhow::anyhow!("rg not found in PATH"));
        }
        Err(err) => return Err(err).with_context(|| "run rg"),
    };
    if !output.status.success()
        && !options.no_ignore
        && rg_flag_unsupported(&output, "--no-require-git")
    {
        output = match build_cmd(false).output() {
            Ok(value) => value,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(anyhow::anyhow!("rg not found in PATH"));
            }
            Err(err) => return Err(err).with_context(|| "run rg"),
        };
    }
    let exit_code = output.status.code().unwrap_or(2);
    if exit_code == 1 {
        // Exit code 1 = no matches found. Return empty.
        return Ok(Vec::new());
    }
    if exit_code != 0 {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("rg failed (exit code {}): {}", exit_code, stderr.trim());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut hits = Vec::new();
    for line in stdout.lines() {
        if hits.len() >= limit {
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
        hits.push(GrepHit {
            path,
            line: line_number,
            column,
            line_text: if options.include_text {
                Some(line_text)
            } else {
                None
            },
            context: None,
            enclosing_symbol: None,
            score: None,
            reasons: Some(vec!["regex".to_string()]),
            engine: Some("search_rg".to_string()),
            next_hops: None,
        });
    }
    Ok(hits)
}

fn rg_flag_unsupported(output: &std::process::Output, flag: &str) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    stderr.contains(flag)
}

const DEFAULT_JUMP_CONTEXT_LINES: usize = 3;

fn push_reason(reasons: &mut Option<Vec<String>>, reason: &str) {
    match reasons {
        Some(values) => values.push(reason.to_string()),
        None => *reasons = Some(vec![reason.to_string()]),
    }
}

fn push_next_hop(next_hops: &mut Option<Vec<RpcSuggestion>>, hop: RpcSuggestion) {
    match next_hops {
        Some(values) => values.push(hop),
        None => *next_hops = Some(vec![hop]),
    }
}

fn build_open_file_hop(path: &str, line: usize, context_lines: usize) -> RpcSuggestion {
    let context = if context_lines > 0 {
        context_lines
    } else {
        DEFAULT_JUMP_CONTEXT_LINES
    };
    let start_line = line.saturating_sub(context).max(1) as i64;
    let end_line = line.saturating_add(context).max(1) as i64;
    RpcSuggestion {
        method: "open_file".to_string(),
        params: json!({
            "path": path,
            "start_line": start_line,
            "end_line": end_line,
        }),
        label: Some("Open file around hit".to_string()),
    }
}

fn build_open_symbol_hop(symbol: &Symbol) -> RpcSuggestion {
    RpcSuggestion {
        method: "open_symbol".to_string(),
        params: json!({
            "id": symbol.id,
            "include_snippet": true,
        }),
        label: Some("Open enclosing symbol".to_string()),
    }
}

fn build_reference_hops(symbol: &Symbol, graph_version: i64) -> Vec<RpcSuggestion> {
    vec![
        RpcSuggestion {
            method: "references".to_string(),
            params: json!({
                "id": symbol.id,
                "direction": "in",
                "kinds": ["CALLS"],
                "limit": 50,
                "graph_version": graph_version,
            }),
            label: Some("Callers".to_string()),
        },
        RpcSuggestion {
            method: "references".to_string(),
            params: json!({
                "id": symbol.id,
                "direction": "out",
                "kinds": ["CALLS"],
                "limit": 50,
                "graph_version": graph_version,
            }),
            label: Some("Calls".to_string()),
        },
    ]
}

fn query_tokens(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in query.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out.into_iter().filter(|token| token.len() > 1).collect()
}

fn symbol_matches_query(symbol: &Symbol, query: &str) -> bool {
    let tokens = query_tokens(query);
    if tokens.is_empty() {
        return false;
    }
    let name = symbol.name.to_ascii_lowercase();
    let qualname = symbol.qualname.to_ascii_lowercase();
    tokens
        .iter()
        .any(|token| name.contains(token) || qualname.contains(token))
}

fn annotate_search_hits(
    indexer: &Indexer,
    hits: &mut [SearchHit],
    context_lines: usize,
    include_symbol: bool,
    graph_version: i64,
    query: Option<&str>,
) -> Result<()> {
    let repo_root = indexer.repo_root();
    let mut cache: HashMap<String, Vec<String>> = HashMap::new();
    for hit in hits {
        push_next_hop(
            &mut hit.next_hops,
            build_open_file_hop(&hit.path, hit.line, context_lines),
        );
        if context_lines > 0 {
            let lines = cache
                .entry(hit.path.clone())
                .or_insert_with(|| read_lines(repo_root, &hit.path).unwrap_or_default());
            if let Some(line_text) = lines.get(hit.line.saturating_sub(1)) {
                hit.line_text = line_text.clone();
            }
            if let Some(context) = build_context(lines, hit.line, context_lines) {
                hit.context = Some(context);
            }
        }
        if include_symbol {
            if let Some(symbol) =
                indexer
                    .db()
                    .enclosing_symbol_for_line(&hit.path, hit.line as i64, graph_version)?
            {
                hit.enclosing_symbol = Some(format!("{} {}", symbol.kind, symbol.qualname));
                push_next_hop(&mut hit.next_hops, build_open_symbol_hop(&symbol));
                for hop in build_reference_hops(&symbol, graph_version) {
                    push_next_hop(&mut hit.next_hops, hop);
                }
                if let Some(query) = query {
                    if symbol_matches_query(&symbol, query) {
                        push_reason(&mut hit.reasons, "symbol_name");
                    }
                }
            }
        }
    }
    Ok(())
}

fn annotate_grep_hits(
    indexer: &Indexer,
    hits: &mut [GrepHit],
    context_lines: usize,
    include_symbol: bool,
    graph_version: i64,
    query: Option<&str>,
) -> Result<()> {
    let repo_root = indexer.repo_root();
    let mut cache: HashMap<String, Vec<String>> = HashMap::new();
    for hit in hits {
        push_next_hop(
            &mut hit.next_hops,
            build_open_file_hop(&hit.path, hit.line, context_lines),
        );
        if context_lines > 0 {
            let lines = cache
                .entry(hit.path.clone())
                .or_insert_with(|| read_lines(repo_root, &hit.path).unwrap_or_default());
            if let Some(context) = build_context(lines, hit.line, context_lines) {
                hit.context = Some(context);
            }
        }
        if include_symbol {
            if let Some(symbol) =
                indexer
                    .db()
                    .enclosing_symbol_for_line(&hit.path, hit.line as i64, graph_version)?
            {
                hit.enclosing_symbol = Some(format!("{} {}", symbol.kind, symbol.qualname));
                push_next_hop(&mut hit.next_hops, build_open_symbol_hop(&symbol));
                for hop in build_reference_hops(&symbol, graph_version) {
                    push_next_hop(&mut hit.next_hops, hop);
                }
                if let Some(query) = query {
                    if symbol_matches_query(&symbol, query) {
                        push_reason(&mut hit.reasons, "symbol_name");
                    }
                }
            }
        }
    }
    Ok(())
}

fn read_lines(repo_root: &PathBuf, rel_path: &str) -> Option<Vec<String>> {
    let path = repo_root.join(rel_path);
    let content = util::read_to_string(&path).ok()?;
    Some(content.lines().map(|line| line.to_string()).collect())
}

fn build_context(lines: &[String], line: usize, context_lines: usize) -> Option<Vec<ContextLine>> {
    if lines.is_empty() || line == 0 {
        return None;
    }
    let line_idx = line.saturating_sub(1);
    if line_idx >= lines.len() {
        return None;
    }
    let start = line_idx.saturating_sub(context_lines);
    let end = (line_idx + context_lines).min(lines.len() - 1);
    let mut out = Vec::new();
    for idx in start..=end {
        out.push(ContextLine {
            line: idx + 1,
            text: lines[idx].clone(),
        });
    }
    Some(out)
}
