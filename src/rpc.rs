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
            for key in ["results", "nodes", "incoming", "outgoing", "edges"] {
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

#[derive(Deserialize)]
struct FindSymbolParams {
    #[serde(alias = "symbol", alias = "name")]
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct OpenFileParams {
    path: String,
    start_line: Option<i64>,
    end_line: Option<i64>,
    max_bytes: Option<usize>,
}

#[derive(Deserialize)]
struct OverviewParams {
    summary: Option<bool>,
    fields: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct ReindexParams {
    summary: Option<bool>,
    fields: Option<Vec<String>>,
    resolve_edges: Option<bool>,
    mine_git: Option<bool>,
}

#[derive(Deserialize)]
struct ModuleMapParams {
    depth: Option<usize>,
    include_edges: Option<bool>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct RepoMapParams {
    max_bytes: Option<usize>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct TopComplexityParams {
    limit: Option<usize>,
    min_complexity: Option<i64>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct TopCouplingParams {
    limit: Option<usize>,
    direction: Option<String>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct CoChangesParams {
    path: Option<String>,
    paths: Option<Vec<String>>,
    qualname: Option<String>,
    limit: Option<usize>,
    min_confidence: Option<f64>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct DeadSymbolsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct UnusedImportsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct OrphanTestsParams {
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    path: Option<String>,
    paths: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct DiagnosticsImportParams {
    path: String,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct DiagnosticsSummaryParams {
    path: Option<String>,
    paths: Option<Vec<String>>,
    severity: Option<String>,
    rule_id: Option<String>,
    tool: Option<String>,
    languages: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct DiagnosticsRunParams {
    tools: Option<Vec<String>>,
    tool: Option<String>,
    languages: Option<Vec<String>>,
    output_dir: Option<String>,
}

#[derive(Deserialize)]
struct NeighborsParams {
    id: i64,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct SuggestQualNamesParams {
    #[serde(alias = "query", alias = "pattern", alias = "name")]
    query: String,
    limit: Option<usize>,
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
}

#[derive(Deserialize)]
struct ChangedFilesParams {
    languages: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct IndexStatusParams {
    languages: Option<Vec<String>>,
    include_paths: Option<bool>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct GraphVersionsParams {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize, Default)]
struct ListMethodsParams {
    format: Option<String>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

const METHOD_LIST: &[&str] = &[
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
];

const METHOD_ALIASES: &[(&str, &str)] = &[
    ("search", "search_text"),
    ("edges", "list_edges"),
    ("xrefs", "list_xrefs"),
    ("graph_versions", "list_graph_versions"),
];

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
        "repo_overview" => {
            let params: OverviewParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let overview = indexer.db().repo_overview(
                indexer.repo_root().clone(),
                languages.as_deref(),
                graph_version,
            )?;
            apply_field_filters(
                json!(overview),
                params.summary.unwrap_or(false),
                params.fields.as_deref(),
                &["files", "symbols", "edges"],
            )
        }
        "repo_insights" => {
            let params: InsightsParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let complexity_limit = params.complexity_limit.unwrap_or(10);
            let min_complexity = params.min_complexity.unwrap_or(1);
            let duplicate_limit = params.duplicate_limit.unwrap_or(10);
            let duplicate_min_count = params.duplicate_min_count.unwrap_or(2);
            let duplicate_min_loc = params.duplicate_min_loc.unwrap_or(5);
            let duplicate_per_group_limit = params.duplicate_per_group_limit.unwrap_or(10);
            let call_edges = indexer.db().call_edge_count(
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let top_complexity = indexer.db().top_complexity(
                complexity_limit,
                min_complexity,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let duplicate_groups = indexer.db().duplicate_groups(
                duplicate_limit,
                duplicate_min_count,
                duplicate_min_loc,
                duplicate_per_group_limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let coupling_limit = params.coupling_limit.unwrap_or(10);
            let top_fan_in = indexer.db().top_fan_in(
                coupling_limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let top_fan_out = indexer.db().top_fan_out(
                coupling_limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let diagnostics = indexer.db().diagnostics_summary(
                languages.as_deref(),
                paths.as_deref(),
                None,
                None,
                None,
            )?;
            let staleness = if params.include_staleness.unwrap_or(false) {
                let staleness_limit = params.staleness_limit.unwrap_or(1000);
                let dead_symbols_list = indexer.db().dead_symbols(
                    staleness_limit,
                    languages.as_deref(),
                    paths.as_deref(),
                    graph_version,
                )?;
                let dead_symbols_count = dead_symbols_list.iter().filter(|s| !is_test_symbol(s)).count() as i64;
                let unused_imports_count = indexer.db().unused_imports(
                    staleness_limit,
                    languages.as_deref(),
                    paths.as_deref(),
                    graph_version,
                )?.len() as i64;
                let orphan_tests_count = indexer.db().orphan_tests(
                    staleness_limit,
                    languages.as_deref(),
                    paths.as_deref(),
                    graph_version,
                )?.len() as i64;
                Some(crate::model::StalenessMetrics {
                    dead_symbols: dead_symbols_count,
                    unused_imports: unused_imports_count,
                    orphan_tests: orphan_tests_count,
                })
            } else {
                None
            };
            let coupling_hotspots = if params.include_coupling_hotspots.unwrap_or(false) {
                let hotspots_limit = params.coupling_hotspots_limit.unwrap_or(10);
                let min_confidence = params.coupling_hotspots_min_confidence.unwrap_or(0.5);
                Some(indexer.db().coupling_hotspots(hotspots_limit, min_confidence)?)
            } else {
                None
            };
            let last_indexed = indexer.db().get_meta_i64("last_indexed")?;
            let commit_sha = indexer.db().graph_version_commit(graph_version)?;
            let insights = RepoInsights {
                repo_root: indexer.repo_root().to_string_lossy().to_string(),
                call_edges,
                top_complexity,
                duplicate_groups,
                top_fan_in,
                top_fan_out,
                coupling_hotspots,
                diagnostics,
                staleness,
                last_indexed,
                graph_version: Some(graph_version),
                commit_sha,
            };
            json!(insights)
        }
        "module_map" => {
            let params: ModuleMapParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let depth = params.depth.unwrap_or(1).max(1).min(5);
            let include_edges = params.include_edges.unwrap_or(true);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

            let summary = indexer.db().module_summary(
                depth,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;

            let modules: Vec<ModuleNode> = summary
                .into_iter()
                .map(|m| ModuleNode {
                    path: m.path,
                    file_count: m.file_count,
                    symbol_count: m.symbol_count,
                    languages: m.languages,
                })
                .collect();

            let edges = if include_edges {
                let edge_data = indexer.db().module_edges(
                    depth,
                    languages.as_deref(),
                    graph_version,
                )?;
                edge_data
                    .into_iter()
                    .map(|(src, dst, calls, imports)| ModuleEdge {
                        source_module: src,
                        target_module: dst,
                        call_count: calls,
                        import_count: imports,
                    })
                    .collect()
            } else {
                vec![]
            };

            let next_hops: Vec<serde_json::Value> = modules
                .iter()
                .take(5)
                .map(|m| {
                    json!({
                        "method": "find_symbol",
                        "params": {"query": &m.path, "limit": 20},
                        "description": format!("Explore {}", m.path)
                    })
                })
                .collect();

            let result = ModuleMapResult {
                modules,
                edges,
                next_hops,
            };
            json!(result)
        }
        "repo_map" => {
            let params: RepoMapParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let max_bytes = params.max_bytes.unwrap_or(8000).max(1000).min(50000);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

            let config = crate::repo_map::RepoMapConfig {
                max_bytes,
                languages,
                paths,
                graph_version,
            };

            let result = crate::repo_map::build_repo_map(indexer.db(), &config)?;

            let next_hops: Vec<serde_json::Value> = vec![
                json!({
                    "method": "module_map",
                    "params": {"depth": 2, "include_edges": true},
                    "description": "Explore full module DAG"
                }),
                json!({
                    "method": "search",
                    "params": {"query": "main entry", "limit": 10},
                    "description": "Find entry points"
                }),
            ];

            json!({
                "text": result.text,
                "modules": result.modules,
                "symbols": result.symbols,
                "bytes": result.bytes,
                "next_hops": next_hops,
            })
        }
        "top_complexity" => {
            let params: TopComplexityParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(10);
            let min_complexity = params.min_complexity.unwrap_or(1);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let results = indexer.db().top_complexity(
                limit,
                min_complexity,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!(results)
        }
        "duplicate_groups" => {
            let params: DuplicateGroupsParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(10);
            let min_count = params.min_count.unwrap_or(2);
            let min_loc = params.min_loc.unwrap_or(5);
            let per_group_limit = params.per_group_limit.unwrap_or(10);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let groups = indexer.db().duplicate_groups(
                limit,
                min_count,
                min_loc,
                per_group_limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!(groups)
        }
        "top_coupling" => {
            let params: TopCouplingParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(10);
            let direction = params.direction.as_deref().unwrap_or("both");
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

            let result = match direction {
                "in" => {
                    let fan_in = indexer.db().top_fan_in(
                        limit,
                        languages.as_deref(),
                        paths.as_deref(),
                        graph_version,
                    )?;
                    json!({
                        "fan_in": fan_in,
                    })
                }
                "out" => {
                    let fan_out = indexer.db().top_fan_out(
                        limit,
                        languages.as_deref(),
                        paths.as_deref(),
                        graph_version,
                    )?;
                    json!({
                        "fan_out": fan_out,
                    })
                }
                "both" => {
                    let fan_in = indexer.db().top_fan_in(
                        limit,
                        languages.as_deref(),
                        paths.as_deref(),
                        graph_version,
                    )?;
                    let fan_out = indexer.db().top_fan_out(
                        limit,
                        languages.as_deref(),
                        paths.as_deref(),
                        graph_version,
                    )?;
                    json!({
                        "fan_in": fan_in,
                        "fan_out": fan_out,
                    })
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Invalid direction: {}. Must be 'in', 'out', or 'both'",
                        direction
                    ));
                }
            };
            result
        }
        "co_changes" => {
            let params: CoChangesParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let min_confidence = params.min_confidence.unwrap_or(0.3);
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;

            // Get paths from either path/paths params or qualname
            let paths = if params.path.is_some() || params.paths.is_some() {
                normalize_search_paths(indexer.repo_root(), params.path, params.paths)?
            } else if let Some(ref qualname) = params.qualname {
                // Resolve qualname to file path
                if let Some(symbol) = indexer.db().get_symbol_by_qualname(qualname, graph_version)? {
                    Some(vec![symbol.file_path])
                } else {
                    return Err(anyhow::anyhow!("Symbol not found: {}", qualname));
                }
            } else {
                return Err(anyhow::anyhow!("Must provide either path, paths, or qualname"));
            };

            let results = if let Some(ref paths) = paths {
                if paths.len() == 1 {
                    indexer.db().co_changes_for_file(&paths[0], limit, min_confidence, graph_version)?
                } else {
                    indexer.db().co_changes_for_files(paths, limit, min_confidence, graph_version)?
                }
            } else {
                Vec::new()
            };

            // Generate next_hops
            let mut next_hops = Vec::new();
            if let Some(ref paths) = paths {
                let first_path = &paths[0];
                for (i, result) in results.iter().take(3).enumerate() {
                    let other_file = if &result.file_a == first_path {
                        &result.file_b
                    } else {
                        &result.file_a
                    };
                    next_hops.push(json!({
                        "method": "open_file",
                        "params": {
                            "path": other_file,
                        },
                        "label": format!("Open co-changed file #{}", i + 1),
                    }));
                }
            }

            json!({
                "results": results,
                "next_hops": next_hops,
            })
        }
        "dead_symbols" => {
            let params: DeadSymbolsParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let mut results = indexer.db().dead_symbols(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            // Filter out test symbols
            results.retain(|s| !is_test_symbol(s));
            let compact: Vec<SymbolCompact> = results.into_iter().map(|s| s.into()).collect();
            json!(compact)
        }
        "unused_imports" => {
            let params: UnusedImportsParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let results = indexer.db().unused_imports(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            json!(results)
        }
        "orphan_tests" => {
            let params: OrphanTestsParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let results = indexer.db().orphan_tests(
                limit,
                languages.as_deref(),
                paths.as_deref(),
                graph_version,
            )?;
            let compact: Vec<SymbolCompact> = results.into_iter().map(|s| s.into()).collect();
            json!(compact)
        }
        "find_symbol" => {
            let params: FindSymbolParams = serde_json::from_value(params)?;
            if params.query.trim().is_empty() {
                return Err(anyhow::anyhow!("find_symbol requires a non-empty query"));
            }
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let symbols = indexer.db().find_symbols(
                &params.query,
                limit,
                languages.as_deref(),
                graph_version,
            )?;

            // Check format param
            let format = params.format.as_deref().unwrap_or("full");
            if format == "signatures" {
                // For signatures format, wrap in consistent shape
                let compact_symbols = apply_compact_format(json!(symbols));
                let next_hops: Vec<serde_json::Value> = if let Some(first_symbol) = symbols.first() {
                    vec![
                        json!({
                            "method": "open_symbol",
                            "params": {"id": first_symbol.id},
                            "label": format!("Open {}", first_symbol.name)
                        })
                    ]
                } else {
                    vec![]
                };
                json!({
                    "data": compact_symbols,
                    "next_hops": next_hops
                })
            } else {
                // Full format returns bare array for backward compatibility
                json!(symbols)
            }
        }
        "suggest_qualnames" => {
            // Helper: Extract 3-character trigrams from a string
            fn extract_trigrams(s: &str) -> Vec<String> {
                let chars: Vec<char> = s.chars().collect();
                chars
                    .windows(3)
                    .map(|w| w.iter().collect::<String>())
                    .collect()
            }

            // Helper: Split camelCase/PascalCase into components
            fn split_camel_case(s: &str) -> Vec<String> {
                let mut components = Vec::new();
                let mut current = String::new();
                for c in s.chars() {
                    if c.is_uppercase() && !current.is_empty() {
                        components.push(current);
                        current = String::new();
                    }
                    current.push(c);
                }
                if !current.is_empty() {
                    components.push(current);
                }
                components
            }

            let params: SuggestQualNamesParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(10).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;

            // Fast path: try exact substring match first
            let symbols = indexer.db().find_symbols(
                &params.query,
                limit,
                languages.as_deref(),
                graph_version,
            )?;

            let symbols = if symbols.is_empty() && params.query.len() >= 3 {
                // Fuzzy path: search by name prefix (starts-with), then rank by
                // Levenshtein distance against the full query.
                // Use progressively shorter prefixes to find candidates.
                let mut candidates = Vec::new();
                for prefix_len in (3..=params.query.len()).rev() {
                    let prefix = &params.query[..prefix_len];
                    candidates = indexer.db().find_symbols_by_name_prefix(
                        prefix,
                        limit * 20,
                        languages.as_deref(),
                        graph_version,
                    )?;
                    if !candidates.is_empty() {
                        break;
                    }
                }

                // Strategy 2: Trigram search
                // Extract 3-character substrings and search for symbols matching each
                let trigrams = extract_trigrams(&params.query);
                for trigram in trigrams.iter().take(5) {
                    let more = indexer.db().find_symbols_by_name_prefix(
                        trigram,
                        limit * 10,
                        languages.as_deref(),
                        graph_version,
                    )?;
                    candidates.extend(more);
                }

                // Strategy 3: CamelCase component search
                // Split query into camelCase components and search for each
                let components = split_camel_case(&params.query);
                if components.len() > 1 {
                    for component in &components {
                        if component.len() >= 3 {
                            let more = indexer.db().find_symbols_by_name_prefix(
                                component,
                                limit * 5,
                                languages.as_deref(),
                                graph_version,
                            )?;
                            candidates.extend(more);
                        }
                    }
                }

                // Deduplicate candidates by symbol ID
                candidates.sort_by_key(|s| s.id);
                candidates.dedup_by_key(|s| s.id);

                // Score candidates using case-insensitive Levenshtein distance
                let query_lower = params.query.to_lowercase();
                let max_dist = (params.query.len() / 4).max(2);
                let mut scored: Vec<(crate::model::Symbol, usize)> = candidates
                    .into_iter()
                    .filter_map(|s| {
                        let name_lower = s.name.to_lowercase();
                        let dist = search::levenshtein_distance(
                            query_lower.as_bytes(),
                            name_lower.as_bytes(),
                        );
                        if dist <= max_dist {
                            Some((s, dist))
                        } else {
                            None
                        }
                    })
                    .collect();
                scored.sort_by_key(|(_, dist)| *dist);
                scored.truncate(limit);
                scored.into_iter().map(|(s, _)| s).collect()
            } else {
                symbols
            };

            // Return just the qualnames with metadata
            let suggestions: Vec<serde_json::Value> = symbols
                .into_iter()
                .map(|s| {
                    json!({
                        "qualname": s.qualname,
                        "kind": s.kind,
                        "file_path": s.file_path,
                    })
                })
                .collect();

            json!(suggestions)
        }
        "open_symbol" => {
            let params: OpenSymbolParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let symbol = if let Some(id) = params.id {
                indexer.db().get_symbol_by_id(id)?
            } else if let Some(qualname) = params.qualname {
                indexer
                    .db()
                    .get_symbol_by_qualname(&qualname, graph_version)?
            } else {
                return Err(anyhow::anyhow!("open_symbol requires id or qualname"));
            };
            let symbol = symbol.ok_or_else(|| anyhow::anyhow!("symbol not found"))?;
            let include_snippet = params.include_snippet.unwrap_or(true);
            let include_symbol = if params.snippet_only.unwrap_or(false) {
                false
            } else {
                params.include_symbol.unwrap_or(true)
            };
            let max_snippet_bytes = params.max_snippet_bytes;
            let snippet = if include_snippet {
                let path = indexer.repo_root().join(&symbol.file_path);
                let content = util::read_to_string(&path)
                    .with_context(|| format!("read {}", symbol.file_path))?;
                let snippet = util::slice_bytes(&content, symbol.start_byte, symbol.end_byte)
                    .unwrap_or_else(|| {
                        util::slice_lines(&content, symbol.start_line, symbol.end_line)
                    });
                match max_snippet_bytes {
                    Some(max) => util::truncate_str_bytes(&snippet, max),
                    None => snippet,
                }
            } else {
                String::new()
            };
            let mut payload = serde_json::Map::new();
            if include_symbol {
                payload.insert("symbol".to_string(), json!(symbol));
            }
            if include_snippet {
                payload.insert("snippet".to_string(), json!(snippet));
            }
            let next_hops = if params.snippet_only.unwrap_or(false) {
                Vec::new()
            } else {
                build_reference_hops(&symbol, graph_version)
            };
            if !next_hops.is_empty() {
                payload.insert("next_hops".to_string(), json!(next_hops));
            }
            Value::Object(payload)
        }
        "explain_symbol" => {
            let params: ExplainSymbolParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let languages = params.languages.clone();

            let max_bytes = params.max_bytes.unwrap_or(40_000).min(200_000);
            let max_refs = params.max_refs.unwrap_or(10);
            let sections = params.sections.clone().unwrap_or_else(||
                vec!["source".into(), "callers".into(), "callees".into(), "tests".into(), "implements".into()]
            );

            // 1. Resolve symbol
            let symbol = if let Some(id) = params.id {
                indexer.db().get_symbol_by_id(id)?
                    .ok_or_else(|| anyhow::anyhow!("symbol not found: id={}", id))?
            } else if let Some(ref qn) = params.qualname {
                indexer.db().get_symbol_by_qualname(qn, graph_version)?
                    .ok_or_else(|| anyhow::anyhow!("symbol not found: {}", qn))?
            } else if let Some(ref query) = params.query {
                let results = indexer.db().find_symbols(query, 1, languages.as_deref(), graph_version)?;
                results.into_iter().next()
                    .ok_or_else(|| anyhow::anyhow!("no symbol found for query: {}", query))?
            } else {
                anyhow::bail!("explain_symbol requires id, qualname, or query");
            };

            // 2. Budget allocation (30% source, 20% callers, 20% callees, 10% tests, 20% expansion) - FIX #4
            let source_budget = max_bytes * 30 / 100;
            let callers_budget = max_bytes * 20 / 100;
            let callees_budget = max_bytes * 20 / 100;
            let tests_budget = max_bytes * 10 / 100;
            let expansion_budget = max_bytes * 20 / 100;
            let mut used_bytes = 0usize;
            let mut truncated = false;

            // 3. Read source (FIX #5: truncate at line boundaries)
            let source = if sections.contains(&"source".to_string()) {
                let repo_root = indexer.repo_root();
                let full_path = repo_root.join(&symbol.file_path);
                if full_path.exists() {
                    let content = std::fs::read_to_string(&full_path).unwrap_or_default();
                    let lines: Vec<&str> = content.lines().collect();
                    let start = (symbol.start_line as usize).saturating_sub(1);
                    let end = (symbol.end_line as usize).min(lines.len());
                    let snippet = lines[start..end].join("\n");
                    let snippet = if snippet.len() > source_budget {
                        truncated = true;
                        // Find last newline before budget limit to avoid mid-line truncation
                        let truncate_pos = snippet[..source_budget].rfind('\n').unwrap_or(source_budget);
                        snippet[..truncate_pos].to_string()
                    } else {
                        snippet
                    };
                    used_bytes += snippet.len();
                    Some(snippet)
                } else {
                    None
                }
            } else { None };

            // 4. Get edges for callers/callees
            let edges = indexer.db().edges_for_symbol(
                symbol.id, languages.as_deref(), graph_version
            )?;

            // 5. Build callers (incoming CALLS)
            let mut callers = if sections.contains(&"callers".to_string()) {
                let mut caller_refs = Vec::new();
                let mut caller_bytes = 0usize;
                let mut seen_caller_ids = std::collections::HashSet::new();

                // Determine which symbol IDs to collect callers for
                let is_class_symbol = symbol.kind == "class";
                let target_ids: Vec<(i64, String)> = if is_class_symbol {
                    // For class symbols, find all methods and collect callers for each
                    let all_symbols = indexer.db().get_symbols_for_file(&symbol.file_path, graph_version)?;
                    let mut ids: Vec<(i64, String)> = all_symbols.into_iter()
                        .filter(|s| {
                            (s.kind == "method" || s.kind == "function") &&
                            s.start_line >= symbol.start_line &&
                            s.end_line <= symbol.end_line
                        })
                        .map(|s| { let name = s.name.clone(); (s.id, name) })
                        .collect();
                    // Also include the class itself
                    ids.push((symbol.id, symbol.name.clone()));
                    ids
                } else {
                    vec![(symbol.id, symbol.name.clone())]
                };

                for (target_id, target_name) in &target_ids {
                    if caller_refs.len() >= max_refs || caller_bytes > callers_budget { break; }

                    // Get edges for this target
                    let target_edges = if *target_id == symbol.id {
                        edges.clone()
                    } else {
                        indexer.db().edges_for_symbol(*target_id, languages.as_deref(), graph_version)?
                    };

                    // Collect resolved callers
                    for edge in &target_edges {
                        if edge.kind == "CALLS" && edge.target_symbol_id == Some(*target_id) {
                            if let Some(source_id) = edge.source_symbol_id {
                                if seen_caller_ids.insert(source_id) {
                                    if let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id) {
                                        let evidence = edge.evidence_snippet.clone();
                                        let ref_json = serde_json::to_string(&caller_sym).unwrap_or_default();
                                        caller_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                        if caller_bytes > callers_budget {
                                            truncated = true;
                                            break;
                                        }
                                        caller_refs.push(ExplainRef {
                                            signature: caller_sym.signature.clone(),
                                            symbol: caller_sym,
                                            evidence,
                                            edge_kind: "CALLS".to_string(),
                                        });
                                        if caller_refs.len() >= max_refs { break; }
                                    }
                                }
                            }
                        }
                    }

                    // Check for unresolved callers by qualname
                    if caller_refs.len() < max_refs && caller_bytes <= callers_budget {
                        let unresolved_edges = indexer.db().incoming_edges_by_qualname_pattern(
                            target_name, "CALLS", languages.as_deref(), graph_version
                        )?;

                        for edge in &unresolved_edges {
                            if let Some(ref target_qn) = edge.target_qualname {
                                if target_qn.ends_with(target_name) {
                                    if let Some(source_id) = edge.source_symbol_id {
                                        if seen_caller_ids.insert(source_id) {
                                            if let Ok(Some(caller_sym)) = indexer.db().get_symbol_by_id(source_id) {
                                                let evidence = edge.evidence_snippet.clone();
                                                let ref_json = serde_json::to_string(&caller_sym).unwrap_or_default();
                                                caller_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                                if caller_bytes > callers_budget {
                                                    truncated = true;
                                                    break;
                                                }
                                                caller_refs.push(ExplainRef {
                                                    signature: caller_sym.signature.clone(),
                                                    symbol: caller_sym,
                                                    evidence,
                                                    edge_kind: "CALLS".to_string(),
                                                });
                                                if caller_refs.len() >= max_refs { break; }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                used_bytes += caller_bytes;
                Some(caller_refs)
            } else { None };

            // 6. Build callees (outgoing CALLS) - FIX #3: For class symbols, aggregate from methods
            let mut callees = if sections.contains(&"callees".to_string()) {
                let mut callee_refs = Vec::new();
                let mut callee_bytes = 0usize;
                let mut seen_callee_ids = std::collections::HashSet::new();

                // Determine if this is a class-level symbol
                let is_class_symbol = symbol.kind == "class";

                if is_class_symbol {
                    // For class symbols, find all methods in the same file within the class's line range
                    let all_symbols = indexer.db().get_symbols_for_file(&symbol.file_path, graph_version)?;
                    let methods: Vec<_> = all_symbols.into_iter()
                        .filter(|s| {
                            (s.kind == "method" || s.kind == "function") &&
                            s.start_line >= symbol.start_line &&
                            s.end_line <= symbol.end_line
                        })
                        .collect();

                    // Get callees from all methods
                    for method in methods {
                        let method_edges = indexer.db().edges_for_symbol(
                            method.id, languages.as_deref(), graph_version
                        )?;

                        for edge in &method_edges {
                            if edge.kind == "CALLS" && edge.source_symbol_id == Some(method.id) {
                                // Resolve target_id, with fuzzy fallback for unresolved edges
                                let target_id = match edge.target_symbol_id {
                                    Some(id) => Some(id),
                                    None => edge.target_qualname.as_deref().and_then(|qn| {
                                        indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version).ok().flatten()
                                    }),
                                };
                                if let Some(target_id) = target_id {
                                    if seen_callee_ids.insert(target_id) {
                                        if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                            let evidence = edge.evidence_snippet.clone();
                                            let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                                            callee_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                            if callee_bytes > callees_budget {
                                                truncated = true;
                                                break;
                                            }
                                            callee_refs.push(ExplainRef {
                                                signature: callee_sym.signature.clone(),
                                                symbol: callee_sym,
                                                evidence,
                                                edge_kind: "CALLS".to_string(),
                                            });
                                            if callee_refs.len() >= max_refs { break; }
                                        }
                                    }
                                }
                            }
                        }
                        if callee_refs.len() >= max_refs || callee_bytes > callees_budget {
                            break;
                        }
                    }
                } else {
                    // For non-class symbols, use direct edges
                    for edge in &edges {
                        if edge.kind == "CALLS" && edge.source_symbol_id == Some(symbol.id) {
                            let target_id = match edge.target_symbol_id {
                                Some(id) => Some(id),
                                None => edge.target_qualname.as_deref().and_then(|qn| {
                                    indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version).ok().flatten()
                                }),
                            };
                            if let Some(target_id) = target_id {
                                if seen_callee_ids.insert(target_id) {
                                    if let Ok(Some(callee_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                        let evidence = edge.evidence_snippet.clone();
                                        let ref_json = serde_json::to_string(&callee_sym).unwrap_or_default();
                                        callee_bytes += ref_json.len() + evidence.as_ref().map_or(0, |e| e.len());
                                        if callee_bytes > callees_budget {
                                            truncated = true;
                                            break;
                                        }
                                        callee_refs.push(ExplainRef {
                                            signature: callee_sym.signature.clone(),
                                            symbol: callee_sym,
                                            evidence,
                                            edge_kind: "CALLS".to_string(),
                                        });
                                        if callee_refs.len() >= max_refs { break; }
                                    }
                                }
                            }
                        }
                    }
                }

                used_bytes += callee_bytes;
                Some(callee_refs)
            } else { None };

            // 7. Find tests (incoming CALLS from test files)
            let mut tests = if sections.contains(&"tests".to_string()) {
                let mut test_refs = Vec::new();
                let mut test_bytes = 0usize;
                for edge in &edges {
                    if edge.kind == "CALLS" && edge.target_symbol_id == Some(symbol.id) {
                        if let Some(source_id) = edge.source_symbol_id {
                            if let Ok(Some(test_sym)) = indexer.db().get_symbol_by_id(source_id) {
                                let is_test = test_sym.file_path.contains("test")
                                    || test_sym.file_path.contains("spec")
                                    || test_sym.name.starts_with("test_")
                                    || test_sym.name.starts_with("Test");
                                if is_test {
                                    let ref_json = serde_json::to_string(&test_sym).unwrap_or_default();
                                    test_bytes += ref_json.len();
                                    if test_bytes > tests_budget {
                                        truncated = true;
                                        break;
                                    }
                                    test_refs.push(ExplainRef {
                                        signature: test_sym.signature.clone(),
                                        symbol: test_sym,
                                        evidence: edge.evidence_snippet.clone(),
                                        edge_kind: "CALLS".to_string(),
                                    });
                                    if test_refs.len() >= max_refs { break; }
                                }
                            }
                        }
                    }
                }
                used_bytes += test_bytes;
                Some(test_refs)
            } else { None };

            // 8. Find implements (EXTENDS/IMPLEMENTS/INHERITS edges) - FIX #2
            let implements = if sections.contains(&"implements".to_string()) {
                let mut impl_syms = Vec::new();
                for edge in &edges {
                    if (edge.kind == "EXTENDS" || edge.kind == "IMPLEMENTS" || edge.kind == "INHERITS")
                        && edge.source_symbol_id == Some(symbol.id) {
                        if let Some(target_id) = edge.target_symbol_id {
                            if let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(target_id) {
                                impl_syms.push(impl_sym);
                            }
                        }
                    }
                }
                if impl_syms.is_empty() { None } else { Some(impl_syms) }
            } else { None };

            // 9. FIX #4: Budget expansion - if >30% budget remaining, fetch source snippets for refs
            let budget_remaining = max_bytes.saturating_sub(used_bytes);
            let budget_utilization = (used_bytes as f64) / (max_bytes as f64);

            if budget_utilization < 0.70 && budget_remaining > expansion_budget {
                let repo_root = indexer.repo_root();
                let snippet_budget_per_ref = 500; // Max bytes per reference snippet

                // Expand callers with source snippets
                if let Some(ref caller_list) = callers {
                    for caller_ref in caller_list.iter() {
                        if used_bytes + snippet_budget_per_ref > max_bytes { break; }

                        let full_path = repo_root.join(&caller_ref.symbol.file_path);
                        if full_path.exists() {
                            if let Ok(content) = std::fs::read_to_string(&full_path) {
                                let lines: Vec<&str> = content.lines().collect();
                                let start = (caller_ref.symbol.start_line as usize).saturating_sub(1);
                                let end = ((caller_ref.symbol.start_line + 3) as usize).min(lines.len());
                                let snippet = lines[start..end].join("\n");
                                let snippet = if snippet.len() > snippet_budget_per_ref {
                                    let truncate_pos = snippet[..snippet_budget_per_ref].rfind('\n').unwrap_or(snippet_budget_per_ref);
                                    snippet[..truncate_pos].to_string()
                                } else {
                                    snippet
                                };
                                used_bytes += snippet.len();
                            }
                        }
                    }
                }

                // Expand callees with source snippets
                if let Some(ref callee_list) = callees {
                    for callee_ref in callee_list.iter() {
                        if used_bytes + snippet_budget_per_ref > max_bytes { break; }

                        let full_path = repo_root.join(&callee_ref.symbol.file_path);
                        if full_path.exists() {
                            if let Ok(content) = std::fs::read_to_string(&full_path) {
                                let lines: Vec<&str> = content.lines().collect();
                                let start = (callee_ref.symbol.start_line as usize).saturating_sub(1);
                                let end = ((callee_ref.symbol.start_line + 3) as usize).min(lines.len());
                                let snippet = lines[start..end].join("\n");
                                let snippet = if snippet.len() > snippet_budget_per_ref {
                                    let truncate_pos = snippet[..snippet_budget_per_ref].rfind('\n').unwrap_or(snippet_budget_per_ref);
                                    snippet[..truncate_pos].to_string()
                                } else {
                                    snippet
                                };
                                used_bytes += snippet.len();
                            }
                        }
                    }
                }
            }

            // 10. Apply format: "signatures" — strip symbols to compact form
            let format = params.format.as_deref().unwrap_or("full");
            let strip_to_compact = |refs: &mut Vec<ExplainRef>| {
                for r in refs.iter_mut() {
                    r.symbol.docstring = None;
                    r.symbol.commit_sha = None;
                    r.symbol.stable_id = None;
                    r.symbol.start_byte = 0;
                    r.symbol.end_byte = 0;
                    r.symbol.start_col = 0;
                    r.symbol.end_col = 0;
                }
            };
            if format == "signatures" {
                if let Some(ref mut c) = callers { strip_to_compact(c); }
                if let Some(ref mut c) = callees { strip_to_compact(c); }
                if let Some(ref mut t) = tests { strip_to_compact(t); }
            }

            // 11. Build next_hops
            let next_hops = vec![
                json!({"method": "analyze_impact", "params": {"id": symbol.id}, "description": "Analyze downstream impact"}),
                json!({"method": "subgraph", "params": {"start_ids": [symbol.id], "depth": 2}, "description": "Explore graph neighborhood"}),
                json!({"method": "gather_context", "params": {"seeds": [{"type": "symbol", "qualname": symbol.qualname}], "max_bytes": 80000}, "description": "Assemble full context"}),
            ];

            let result = ExplainSymbolResult {
                symbol,
                source,
                callers,
                callees,
                tests,
                implements,
                budget: BudgetInfo {
                    budget_bytes: max_bytes,
                    used_bytes,
                    truncated,
                },
                next_hops,
            };

            serde_json::to_value(&result)?
        }
        "open_file" => {
            let params: OpenFileParams = serde_json::from_value(params)?;
            let (abs_path, rel_path) = resolve_repo_path(indexer.repo_root(), &params.path)?;
            let content = util::read_to_string(&abs_path)?;
            let mut text = if params.start_line.is_some() || params.end_line.is_some() {
                let total_lines = content.lines().count() as i64;
                let start_line = params.start_line.unwrap_or(1);
                let end_line = params.end_line.unwrap_or(total_lines);
                util::slice_lines(&content, start_line, end_line)
            } else {
                content
            };
            if let Some(max_bytes) = params.max_bytes {
                text = util::truncate_str_bytes(&text, max_bytes);
            }
            json!({ "path": rel_path, "text": text })
        }
        "neighbors" => {
            let params: NeighborsParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let edges =
                indexer
                    .db()
                    .edges_for_symbol(params.id, languages.as_deref(), graph_version)?;
            let mut ids = std::collections::HashSet::new();
            ids.insert(params.id);
            for edge in &edges {
                if edge.source_symbol_id == Some(params.id) {
                    if let Some(id) = edge.target_symbol_id {
                        ids.insert(id);
                    }
                } else if let Some(id) = edge.source_symbol_id {
                    ids.insert(id);
                }
            }
            let mut id_list: Vec<i64> = ids.into_iter().collect();
            id_list.sort_unstable();
            let nodes =
                indexer
                    .db()
                    .symbols_by_ids(&id_list, languages.as_deref(), graph_version)?;
            let allowed: std::collections::HashSet<i64> =
                nodes.iter().map(|symbol| symbol.id).collect();
            let filtered_edges: Vec<_> = if languages.is_some() && allowed.is_empty() {
                Vec::new()
            } else {
                edges
                    .into_iter()
                    .filter(|edge| {
                        let source_ok = edge
                            .source_symbol_id
                            .map(|id| allowed.contains(&id))
                            .unwrap_or(true);
                        let target_ok = edge
                            .target_symbol_id
                            .map(|id| allowed.contains(&id))
                            .unwrap_or(true);
                        source_ok && target_ok
                    })
                    .collect()
            };
            let mut value = json!(Subgraph {
                nodes,
                edges: filtered_edges,
            });

            // Check format param
            let format = params.format.as_deref().unwrap_or("full");
            if format == "signatures" {
                value = apply_compact_format(value);
            }

            value
        }
        "subgraph" => {
            let params: SubgraphParams = serde_json::from_value(params)?;
            let depth = params.depth.unwrap_or(2);
            let max_nodes = params.max_nodes.unwrap_or(50);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let mut start_ids = params.start_ids.unwrap_or_default();
            let mut missing = Vec::new();
            if let Some(roots) = params.start_qualnames {
                for raw in roots {
                    let qualname = raw.trim();
                    if qualname.is_empty() {
                        continue;
                    }
                    let id = indexer.db().lookup_symbol_id_filtered(
                        qualname,
                        languages.as_deref(),
                        graph_version,
                    )?;
                    match id {
                        Some(id) => start_ids.push(id),
                        None => missing.push(qualname.to_string()),
                    }
                }
            }
            if start_ids.is_empty() {
                return Err(anyhow::anyhow!("subgraph requires start_ids or roots"));
            }
            if !missing.is_empty() {
                return Err(anyhow::anyhow!(
                    "subgraph roots not found: {}",
                    missing.join(", ")
                ));
            }
            start_ids.sort_unstable();
            start_ids.dedup();
            let include_kinds = match params.kinds.as_deref() {
                Some(kinds) => normalize_edge_kinds(kinds),
                None => None,
            };
            let (exclude_kinds, exclude_all) = match params.exclude_kinds.as_deref() {
                Some(kinds) => normalize_edge_kinds_exclude(kinds),
                None => (HashSet::new(), false),
            };
            let filter = subgraph::EdgeFilter {
                include: include_kinds,
                exclude: exclude_kinds,
                exclude_all,
                resolved_only: params.resolved_only.unwrap_or(false),
            };
            let graph = subgraph::build_subgraph_filtered(
                indexer.db(),
                &start_ids,
                depth,
                max_nodes,
                languages.as_deref(),
                graph_version,
                Some(&filter),
            )?;
            let mut value = json!(graph);

            // Check format param
            let format = params.format.as_deref().unwrap_or("full");
            if format == "signatures" {
                value = apply_compact_format(value);
            }

            value
        }        "find_tests_for" => {
            let params: FindTestsForParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let languages = params.languages.clone();
            let include_indirect = params.include_indirect.unwrap_or(true);
            let indirect_depth = params.indirect_depth.unwrap_or(1).min(5);
            let limit = params.limit.unwrap_or(20).min(100);

            // Resolve symbol by id, qualname, or query
            let symbol = if let Some(id) = params.id {
                indexer.db().get_symbol_by_id(id)?
            } else if let Some(qualname) = params.qualname.as_deref() {
                indexer.db().get_symbol_by_qualname(qualname, graph_version)?
            } else if let Some(query) = params.query.as_deref() {
                // Find symbols matching the query, preferring production symbols over test symbols
                let mut results = indexer.db().find_symbols(query, 10, languages.as_deref(), graph_version)?;
                // Sort to deprioritize test symbols (put them at the end)
                results.sort_by_key(|s| is_test_symbol(s));
                results.into_iter().next()
            } else {
                return Err(anyhow::anyhow!("find_tests_for requires id, qualname, or query"));
            };

            let symbol = symbol.ok_or_else(|| {
                if let Some(query) = params.query.as_deref() {
                    if let Ok(suggestions) = indexer.db().find_symbols(query, 5, languages.as_deref(), graph_version) {
                        if !suggestions.is_empty() {
                            let names: Vec<String> = suggestions.into_iter().map(|s| s.qualname).collect();
                            return anyhow::anyhow!(
                                "Symbol '{}' not found. Did you mean: {}?",
                                query,
                                names.join(", ")
                            );
                        }
                    }
                }
                anyhow::anyhow!("symbol not found")
            })?;

            // For proto service symbols, find RPC_IMPL edges by service name in detail JSON
            let mut impl_symbols: Vec<Symbol> = Vec::new();
            if symbol.kind == "service" {
                // Extract service name (last segment of qualname, e.g., "TriggerService")
                let service_name = symbol.name.clone();

                // Find all RPC_IMPL edges and match by service name in detail field
                let rpc_impl_edges = indexer.db().list_edges(
                    100, 0, languages.as_deref(), None,
                    Some(&["RPC_IMPL".to_string()]), None, None, None,
                    false, None, graph_version, None, None, None,
                )?;

                for edge in &rpc_impl_edges {
                    if let Some(ref detail_str) = edge.detail {
                        if let Ok(detail) = serde_json::from_str::<serde_json::Value>(detail_str) {
                            if detail.get("service").and_then(|v| v.as_str()) == Some(&service_name) {
                                if let Some(src_id) = edge.source_symbol_id {
                                    if let Ok(Some(impl_sym)) = indexer.db().get_symbol_by_id(src_id) {
                                        impl_symbols.push(impl_sym);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Build list of symbols to search tests for
            let search_symbols: Vec<&Symbol> = if impl_symbols.is_empty() {
                vec![&symbol]
            } else {
                impl_symbols.iter().collect()
            };

            // Find direct test callers and non-test callers across all search symbols
            let mut direct_tests = Vec::new();
            let mut non_test_callers = Vec::new();
            let mut test_files = HashSet::new();
            let mut seen_caller_ids = HashSet::new();

            for search_symbol in &search_symbols {
                // Get all incoming CALLS edges for the search symbol (resolved edges)
                let edges = indexer.db().edges_for_symbol(
                    search_symbol.id,
                    languages.as_deref(),
                    graph_version
                )?;

                // Also get unresolved edges by qualname pattern
                let unresolved_edges = indexer.db().incoming_edges_by_qualname_pattern(
                    &search_symbol.name,
                    "CALLS",
                    languages.as_deref(),
                    graph_version
                )?;

                // Process resolved edges
                for edge in &edges {
                    if edge.kind == "CALLS" && edge.target_symbol_id == Some(search_symbol.id) {
                        if let Some(source_id) = edge.source_symbol_id {
                            if seen_caller_ids.insert(source_id) {
                                if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                    let is_test = is_test_symbol(&caller);
                                    if is_test {
                                        test_files.insert(caller.file_path.clone());
                                        direct_tests.push(TestMatch {
                                            test_symbol: caller.into(),
                                            match_type: "direct".to_string(),
                                            via_symbol: None,
                                            relevance: 1.0,
                                        });
                                    } else {
                                        non_test_callers.push(caller);
                                    }
                                }
                            }
                        }
                    }
                }

                // Process unresolved edges (target_qualname matches)
                for edge in &unresolved_edges {
                    if let Some(target_qn) = &edge.target_qualname {
                        let matches = target_qn == &search_symbol.qualname
                            || target_qn.ends_with(&format!(".{}", search_symbol.name));

                        if matches {
                            if let Some(source_id) = edge.source_symbol_id {
                                if seen_caller_ids.insert(source_id) {
                                    if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                        let is_test = is_test_symbol(&caller);
                                        if is_test {
                                            test_files.insert(caller.file_path.clone());
                                            direct_tests.push(TestMatch {
                                                test_symbol: caller.into(),
                                                match_type: "direct".to_string(),
                                                via_symbol: None,
                                                relevance: 0.9,
                                            });
                                        } else {
                                            non_test_callers.push(caller);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Find indirect test callers via multi-level BFS (depth controlled by indirect_depth)
            let mut indirect_tests = Vec::new();
            if include_indirect {
                let mut current_callers = non_test_callers.clone();
                let mut all_seen_ids: HashSet<i64> = seen_caller_ids.clone();
                let mut base_relevance = 0.7;

                for _level in 0..indirect_depth {
                    let mut next_level_callers = Vec::new();

                    for caller in &current_callers {
                        // Get resolved edges for this caller
                        let caller_edges = indexer.db().edges_for_symbol(
                            caller.id,
                            languages.as_deref(),
                            graph_version
                        )?;

                        // Get unresolved edges for this caller
                        let unresolved_caller_edges = indexer.db().incoming_edges_by_qualname_pattern(
                            &caller.name,
                            "CALLS",
                            languages.as_deref(),
                            graph_version
                        )?;

                        // Process resolved edges
                        for edge in &caller_edges {
                            if edge.kind == "CALLS" && edge.target_symbol_id == Some(caller.id) {
                                if let Some(source_id) = edge.source_symbol_id {
                                    if all_seen_ids.insert(source_id) {
                                        if let Ok(Some(upstream)) = indexer.db().get_symbol_by_id(source_id) {
                                            if is_test_symbol(&upstream) {
                                                test_files.insert(upstream.file_path.clone());
                                                indirect_tests.push(TestMatch {
                                                    test_symbol: upstream.into(),
                                                    match_type: "indirect".to_string(),
                                                    via_symbol: Some(caller.clone().into()),
                                                    relevance: base_relevance,
                                                });
                                            } else {
                                                next_level_callers.push(upstream);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Process unresolved edges
                        for edge in &unresolved_caller_edges {
                            if let Some(target_qn) = &edge.target_qualname {
                                let matches = target_qn == &caller.qualname
                                    || target_qn.ends_with(&format!(".{}", caller.name));

                                if matches {
                                    if let Some(source_id) = edge.source_symbol_id {
                                        if all_seen_ids.insert(source_id) {
                                            if let Ok(Some(upstream)) = indexer.db().get_symbol_by_id(source_id) {
                                                if is_test_symbol(&upstream) {
                                                    test_files.insert(upstream.file_path.clone());
                                                    indirect_tests.push(TestMatch {
                                                        test_symbol: upstream.into(),
                                                        match_type: "indirect".to_string(),
                                                        via_symbol: Some(caller.clone().into()),
                                                        relevance: base_relevance * 0.9,
                                                    });
                                                } else {
                                                    next_level_callers.push(upstream);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if next_level_callers.is_empty() { break; }
                    current_callers = next_level_callers;
                    base_relevance *= 0.7; // Decay relevance per level
                }
            }

            // Truncate to limit
            direct_tests.truncate(limit);
            indirect_tests.truncate(limit);

            let summary = TestSummary {
                direct_count: direct_tests.len(),
                indirect_count: indirect_tests.len(),
                test_files: test_files.into_iter().collect(),
            };

            let next_hops = vec![
                json!({"method": "explain_symbol", "params": {"id": symbol.id}, "description": "Full symbol explanation"}),
                json!({"method": "analyze_impact", "params": {"id": symbol.id}, "description": "Impact analysis"}),
            ];

            let result = FindTestsResult {
                symbol: symbol.into(),
                direct_tests,
                indirect_tests,
                summary,
                next_hops,
            };

            serde_json::to_value(&result)?
        }


        "analyze_impact" => {
            let params: AnalyzeImpactParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;

            // Resolve symbol by id or qualname
            let symbol = if let Some(id) = params.id {
                indexer.db().get_symbol_by_id(id)?
            } else if let Some(qualname) = params.qualname.as_deref() {
                indexer
                    .db()
                    .get_symbol_by_qualname(qualname, graph_version)?
            } else {
                return Err(anyhow::anyhow!("analyze_impact requires id or qualname"));
            };

            let symbol = symbol.ok_or_else(|| {
                if let Some(qualname) = params.qualname.as_deref() {
                    if let Ok(suggestions) =
                        indexer.db().find_symbols(qualname, 10, None, graph_version)
                    {
                        if !suggestions.is_empty() {
                            let names: Vec<String> =
                                suggestions.into_iter().map(|s| s.qualname).collect();
                            return anyhow::anyhow!(
                                "Symbol '{}' not found. Did you mean: {}?",
                                qualname,
                                names.join(", ")
                            );
                        }
                    }
                }
                anyhow::anyhow!("symbol not found")
            })?;

            // Build multi-layer configuration
            let config = crate::impact::config::MultiLayerConfig::builder()
                .max_depth(params.max_depth.unwrap_or(3).min(10))
                .direction(params.direction.unwrap_or_else(|| "both".to_string()))
                .include_tests(params.include_tests.unwrap_or(false))
                .include_paths(params.include_paths.unwrap_or(true))
                .limit(params.limit.unwrap_or(500).min(2000))
                .min_confidence(params.min_confidence.unwrap_or(0.0))
                .build();

            // Apply layer enable/disable overrides if specified
            // If not specified, use config defaults (which are now enabled by default)
            let mut config = config;
            if let Some(enable_direct) = params.enable_direct {
                config.direct.enabled = enable_direct;
            }
            if let Some(enable_test) = params.enable_test {
                config.test.enabled = enable_test;
            }
            if let Some(enable_historical) = params.enable_historical {
                config.historical.enabled = enable_historical;
            }

            // Set languages if specified
            if let Some(languages) = params.languages.as_ref() {
                let normalized = scan::normalize_language_filter(Some(languages.as_slice()))?;
                config.direct.languages = normalized;
            }

            // Set kinds if specified
            if let Some(kinds) = params.kinds {
                config.direct.kinds = kinds;
            }

            // Perform multi-layer impact analysis
            let result = crate::impact::analyze_impact_multi_layer(
                indexer.db(),
                &[symbol.id],
                config,
                graph_version,
            )?;

            json!(result)
        }
        "analyze_diff" => {
            let params: AnalyzeDiffParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let max_bytes = params.max_bytes.unwrap_or(50_000).min(200_000);
            let max_depth = params.max_depth.unwrap_or(1).min(5);
            let include_tests = params.include_tests.unwrap_or(true);
            let include_risk = params.include_risk.unwrap_or(true);
            let languages = params.languages.clone();

            // Step 1: Get changed files with optional line ranges
            let mut warnings: Vec<String> = Vec::new();
            let changed_files: Vec<ChangedFile> = if let Some(ref diff) = params.diff {
                parse_diff_with_ranges(diff)
            } else if let Some(ref paths) = params.paths {
                paths.iter().map(|p| ChangedFile {
                    path: p.clone(),
                    changed_ranges: Vec::new(),
                    added_ranges: Vec::new(),
                    deleted_ranges: Vec::new(),
                }).collect()
            } else {
                anyhow::bail!("analyze_diff requires 'diff' or 'paths' parameter");
            };

            if changed_files.is_empty() {
                anyhow::bail!("No changed files found");
            }

            // Step 2: Find symbols in changed files, filtered by hunk ranges
            let mut changed_symbols = Vec::new();
            for cf in &changed_files {
                let symbols = indexer.db().get_symbols_for_file(&cf.path, graph_version).unwrap_or_default();
                if symbols.is_empty() {
                    warnings.push(format!("Path not found in index: {}", cf.path));
                    continue;
                }
                let has_ranges = !cf.changed_ranges.is_empty();
                for sym in symbols {
                    let change_type = if has_ranges {
                        // Check if symbol overlaps any changed hunk
                        let overlaps = cf.changed_ranges.iter().any(|h| {
                            let hunk_end = h.start_line + h.line_count - 1;
                            sym.start_line <= hunk_end && sym.end_line >= h.start_line
                        });
                        if !overlaps { continue; }
                        // Determine change type: if symbol is fully within added range, it's "added"
                        let fully_added = cf.added_ranges.iter().any(|h| {
                            let hunk_end = h.start_line + h.line_count - 1;
                            sym.start_line >= h.start_line && sym.end_line <= hunk_end
                        });
                        if fully_added { "added".to_string() } else { "modified".to_string() }
                    } else {
                        "modified".to_string()
                    };

                    // Step 2a: Detect signature changes by comparing with previous graph version
                    let mut old_signature = None;
                    let new_signature = sym.signature.clone();
                    let mut final_change_type = change_type.clone();

                    if change_type == "modified" && sym.stable_id.is_some() && graph_version > 1 {
                        // Try to find the symbol in the previous graph version
                        if let Ok(Some(old_sym)) = indexer.db().get_symbol_by_stable_id(
                            sym.stable_id.as_ref().unwrap(),
                            graph_version - 1,
                        ) {
                            // Compare signatures
                            if old_sym.signature != sym.signature {
                                final_change_type = "signature_changed".to_string();
                                old_signature = old_sym.signature;
                            }
                        }
                    }

                    changed_symbols.push(ChangedSymbol {
                        symbol: sym,
                        change_type: final_change_type,
                        old_signature,
                        new_signature,
                    });
                }
            }

            // Step 3: Compute downstream impact via multi-level BFS (depth controlled by max_depth)
            let seed_ids: Vec<i64> = changed_symbols.iter().map(|cs| cs.symbol.id).collect();
            let mut downstream = Vec::new();
            let mut seen_ids: HashSet<i64> = seed_ids.iter().copied().collect();
            let max_downstream = 50;

            // BFS: start with changed symbols, expand callers level by level
            let mut current_level: Vec<Symbol> = changed_symbols.iter().map(|cs| cs.symbol.clone()).collect();
            let mut current_distance = 1usize;
            let mut base_confidence = 0.9;

            for _depth in 0..max_depth {
                let mut next_level = Vec::new();

                for sym in &current_level {
                    if downstream.len() >= max_downstream { break; }

                    let edges = indexer.db().edges_for_symbol(
                        sym.id, languages.as_deref(), graph_version
                    )?;

                    // Find callers via resolved edges
                    for edge in &edges {
                        if downstream.len() >= max_downstream { break; }
                        if edge.kind == "CALLS" && edge.target_symbol_id == Some(sym.id) {
                            if let Some(source_id) = edge.source_symbol_id {
                                if seen_ids.insert(source_id) {
                                    if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                        next_level.push(caller.clone());
                                        downstream.push(DiffImpactEntry {
                                            symbol: caller,
                                            relationship: if current_distance == 1 { "caller".to_string() } else { format!("caller_depth_{}", current_distance) },
                                            distance: current_distance,
                                            confidence: base_confidence,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // Qualname fallback for unresolved incoming edges
                    if downstream.len() < max_downstream {
                        let unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                            &sym.name, "CALLS", languages.as_deref(), graph_version
                        ).unwrap_or_default();
                        for edge in &unresolved {
                            if downstream.len() >= max_downstream { break; }
                            if let Some(source_id) = edge.source_symbol_id {
                                if seen_ids.insert(source_id) {
                                    if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                        next_level.push(caller.clone());
                                        downstream.push(DiffImpactEntry {
                                            symbol: caller,
                                            relationship: if current_distance == 1 { "caller".to_string() } else { format!("caller_depth_{}", current_distance) },
                                            distance: current_distance,
                                            confidence: base_confidence * 0.8,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                if next_level.is_empty() || downstream.len() >= max_downstream { break; }
                current_level = next_level;
                current_distance += 1;
                base_confidence *= 0.8; // Decay confidence per level
            }

            // Step 4: Test coverage (with qualname fallback)
            let test_coverage = if include_tests {
                let mut coverage = Vec::new();
                for cs in &changed_symbols {
                    let mut tests = Vec::new();
                    let mut seen_test_ids = HashSet::new();
                    // Check resolved edges
                    let edges = indexer.db().edges_for_symbol(
                        cs.symbol.id, languages.as_deref(), graph_version
                    )?;
                    for edge in &edges {
                        if edge.kind == "CALLS" && edge.target_symbol_id == Some(cs.symbol.id) {
                            if let Some(source_id) = edge.source_symbol_id {
                                if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                    if is_test_symbol(&caller) && seen_test_ids.insert(source_id) {
                                        tests.push(TestRef {
                                            test_qualname: caller.qualname.clone(),
                                            test_file: caller.file_path.clone(),
                                            coverage_type: "direct".to_string(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    // Qualname fallback for unresolved edges
                    let unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                        &cs.symbol.name, "CALLS", languages.as_deref(), graph_version
                    ).unwrap_or_default();
                    for edge in &unresolved {
                        if let Some(source_id) = edge.source_symbol_id {
                            if let Ok(Some(caller)) = indexer.db().get_symbol_by_id(source_id) {
                                if is_test_symbol(&caller) && seen_test_ids.insert(source_id) {
                                    tests.push(TestRef {
                                        test_qualname: caller.qualname.clone(),
                                        test_file: caller.file_path.clone(),
                                        coverage_type: "direct".to_string(),
                                    });
                                }
                            }
                        }
                    }
                    let status = if tests.is_empty() { "uncovered" } else { "covered" };
                    coverage.push(TestCoverageEntry {
                        symbol_qualname: cs.symbol.qualname.clone(),
                        tests,
                        status: status.to_string(),
                    });
                }
                Some(coverage)
            } else { None };

            // Step 5: Enhanced risk assessment with review checklist
            let risk = if include_risk {
                let mut factors = Vec::new();
                let mut focus_areas = Vec::new();
                let mut review_checklist = Vec::new();

                // 1. Signature change + high fan-in = CRITICAL risk
                for cs in &changed_symbols {
                    if cs.change_type == "signature_changed" {
                        let caller_count = downstream.iter()
                            .filter(|d| d.relationship.starts_with("caller"))
                            .count();

                        if caller_count > 10 {
                            factors.push(RiskFactor {
                                factor: "Signature changed on high-traffic symbol".to_string(),
                                description: format!(
                                    "Signature changed on {} with {} callers",
                                    cs.symbol.qualname, caller_count
                                ),
                                severity: "critical".to_string(),
                            });
                            review_checklist.push(format!(
                                "Verify all {} callers of {} handle the new signature: {} → {}",
                                caller_count,
                                cs.symbol.qualname,
                                cs.old_signature.as_deref().unwrap_or("(none)"),
                                cs.new_signature.as_deref().unwrap_or("(none)")
                            ));
                        } else if caller_count > 0 {
                            factors.push(RiskFactor {
                                factor: "Signature change".to_string(),
                                description: format!(
                                    "Signature changed on {} with {} callers",
                                    cs.symbol.qualname, caller_count
                                ),
                                severity: "high".to_string(),
                            });
                            review_checklist.push(format!(
                                "Review callers of {} for signature compatibility",
                                cs.symbol.qualname
                            ));
                        }
                    }
                }

                // 2. Cross-language callers = HIGH risk
                let mut cross_lang_callers: Vec<String> = Vec::new();
                for impact in &downstream {
                    let changed_langs: HashSet<_> = changed_symbols.iter()
                        .map(|cs| infer_language(&cs.symbol.file_path))
                        .collect();
                    let caller_lang = infer_language(&impact.symbol.file_path);
                    if !changed_langs.contains(&caller_lang) {
                        cross_lang_callers.push(format!(
                            "{}:{} ({})",
                            impact.symbol.file_path, impact.symbol.name, caller_lang
                        ));
                    }
                }
                if !cross_lang_callers.is_empty() {
                    factors.push(RiskFactor {
                        factor: "Cross-language impact".to_string(),
                        description: format!("{} cross-language callers affected", cross_lang_callers.len()),
                        severity: "high".to_string(),
                    });
                    for caller in cross_lang_callers.iter().take(3) {
                        review_checklist.push(format!(
                            "Test cross-language caller: {}",
                            caller
                        ));
                    }
                }

                // 3. Interface/trait changes = HIGH risk
                for cs in &changed_symbols {
                    if matches!(cs.symbol.kind.as_str(), "interface" | "trait" | "abstract_class") {
                        factors.push(RiskFactor {
                            factor: "Interface/contract change".to_string(),
                            description: format!(
                                "{} {} changed",
                                cs.symbol.kind, cs.symbol.qualname
                            ),
                            severity: "high".to_string(),
                        });
                        review_checklist.push(format!(
                            "Review all implementers of {} {}",
                            cs.symbol.kind, cs.symbol.qualname
                        ));
                    }
                }

                // 4. High fan-in = HIGH risk
                let high_fan_in: Vec<_> = downstream.iter()
                    .filter(|d| d.relationship.starts_with("caller"))
                    .collect();
                if high_fan_in.len() > 10 {
                    factors.push(RiskFactor {
                        factor: "High fan-in".to_string(),
                        description: format!("{} callers affected", high_fan_in.len()),
                        severity: "high".to_string(),
                    });
                    let caller_files: HashSet<_> = high_fan_in.iter()
                        .map(|d| d.symbol.file_path.as_str())
                        .collect();
                    if caller_files.len() <= 5 {
                        for file in caller_files {
                            review_checklist.push(format!("Review callers in {}", file));
                        }
                    }
                }

                // 5. Wide blast radius = MEDIUM risk
                let affected_files: HashSet<_> = downstream.iter()
                    .map(|d| d.symbol.file_path.as_str())
                    .collect();
                if affected_files.len() > 3 {
                    factors.push(RiskFactor {
                        factor: "Wide blast radius".to_string(),
                        description: format!("{} files affected", affected_files.len()),
                        severity: "medium".to_string(),
                    });
                    focus_areas.extend(affected_files.iter().map(|f| f.to_string()));
                }

                // 6. Missing test coverage = MEDIUM risk
                if let Some(ref cov) = test_coverage {
                    let uncovered: Vec<_> = cov.iter().filter(|c| c.status == "uncovered").collect();
                    if !uncovered.is_empty() {
                        factors.push(RiskFactor {
                            factor: "Missing test coverage".to_string(),
                            description: format!("{} symbols without tests", uncovered.len()),
                            severity: "medium".to_string(),
                        });
                        for entry in uncovered.iter().take(5) {
                            review_checklist.push(format!(
                                "Add tests for {} (currently uncovered)",
                                entry.symbol_qualname
                            ));
                        }
                    }
                }

                // Compute overall risk level
                let level = if factors.iter().any(|f| f.severity == "critical") {
                    "critical"
                } else if factors.iter().any(|f| f.severity == "high") {
                    "high"
                } else if factors.iter().any(|f| f.severity == "medium") {
                    "medium"
                } else {
                    "low"
                };

                Some(RiskAssessment {
                    level: level.to_string(),
                    factors,
                    focus_areas,
                    review_checklist,
                })
            } else { None };

            let mut used_bytes = 0;
            let result_json = serde_json::to_value(&changed_symbols)?;
            used_bytes += serde_json::to_string(&result_json).unwrap_or_default().len();

            let mut next_hops: Vec<Value> = Vec::new();
            // Add explain_symbol for first changed symbol
            if let Some(cs) = changed_symbols.first() {
                next_hops.push(json!({"method": "explain_symbol", "params": {"id": cs.symbol.id}, "description": format!("Explain {}", cs.symbol.name)}));
            }
            // Add references for top changed symbol
            if let Some(cs) = changed_symbols.iter().find(|cs| cs.symbol.kind == "method" || cs.symbol.kind == "function") {
                next_hops.push(json!({"method": "references", "params": {"id": cs.symbol.id, "direction": "in"}, "description": format!("Callers of {}", cs.symbol.name)}));
            }
            // Add subgraph for exploration
            if let Some(cs) = changed_symbols.first() {
                next_hops.push(json!({"method": "subgraph", "params": {"start_ids": [cs.symbol.id], "depth": 2}, "description": "Explore impact graph"}));
            }

            let result = AnalyzeDiffResult {
                changed_symbols,
                downstream,
                test_coverage,
                risk,
                budget: BudgetInfo {
                    budget_bytes: max_bytes,
                    used_bytes,
                    truncated: false,
                },
                next_hops,
                warnings,
            };

            serde_json::to_value(&result)?
        }
        "references" => {
            let params: ReferencesParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let symbol = if let Some(id) = params.id {
                indexer.db().get_symbol_by_id(id)?
            } else if let Some(qualname) = params.qualname.as_deref() {
                indexer
                    .db()
                    .get_symbol_by_qualname(qualname, graph_version)?
            } else {
                return Err(anyhow::anyhow!("references requires id or qualname"));
            };
            let symbol = symbol.ok_or_else(|| anyhow::anyhow!("symbol not found"))?;
            let direction = parse_edge_direction(params.direction.as_deref())?;
            let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
            let include_symbols = params.include_symbols.unwrap_or(true);
            let include_snippet = params.include_snippet.unwrap_or(true);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let kinds = params.kinds.unwrap_or_else(|| vec!["CALLS".to_string()]);
            let kind_filter = normalize_edge_kinds(&kinds);

            // Detect if this is a container type (class, struct, interface, etc.)
            let is_container = matches!(
                symbol.kind.as_str(),
                "class" | "interface" | "struct" | "enum" | "trait" | "service"
            );

            // Build target_ids list: start with the symbol itself
            let mut target_ids = vec![symbol.id];
            let mut member_count = 0;

            // If it's a container, find all members via CONTAINS edges
            if is_container {
                let container_edges = indexer
                    .db()
                    .edges_for_symbol(symbol.id, languages.as_deref(), graph_version)?;
                for edge in &container_edges {
                    if edge.kind == "CONTAINS" && edge.source_symbol_id == Some(symbol.id) {
                        if let Some(target_id) = edge.target_symbol_id {
                            target_ids.push(target_id);
                            member_count += 1;
                        }
                    }
                }
            }

            // Query edges for all target IDs (class + members)
            let all_edges = if target_ids.len() > 1 {
                // Use batch query for multiple symbols
                let edges_by_symbol = indexer
                    .db()
                    .edges_for_symbols(&target_ids, languages.as_deref(), graph_version)?;
                // Flatten the HashMap into a single Vec, deduplicating by edge ID
                let mut edge_map: HashMap<i64, Edge> = HashMap::new();
                for edges in edges_by_symbol.values() {
                    for edge in edges {
                        edge_map.insert(edge.id, edge.clone());
                    }
                }
                edge_map.into_values().collect()
            } else {
                // Use single-symbol query (existing path)
                indexer
                    .db()
                    .edges_for_symbol(symbol.id, languages.as_deref(), graph_version)?
            };

            let mut incoming = Vec::new();
            let mut outgoing = Vec::new();
            let wants_in = matches!(direction, EdgeDirection::In | EdgeDirection::Both);
            let wants_out = matches!(direction, EdgeDirection::Out | EdgeDirection::Both);
            for edge in all_edges {
                if !edge_kind_matches(&edge.kind, &kind_filter) {
                    continue;
                }
                // Filter out CONTAINS edges from the results when showing incoming references
                if edge.kind == "CONTAINS" {
                    continue;
                }
                let is_out = edge
                    .source_symbol_id
                    .map_or(false, |id| target_ids.contains(&id));
                let is_in = edge
                    .target_symbol_id
                    .map_or(false, |id| target_ids.contains(&id));
                let include_out = wants_out && is_out;
                let include_in = wants_in && is_in;
                match (include_in, include_out) {
                    (true, true) => {
                        incoming.push(edge.clone());
                        outgoing.push(edge);
                    }
                    (true, false) => incoming.push(edge),
                    (false, true) => outgoing.push(edge),
                    (false, false) => {}
                }
            }
            if limit == 0 {
                incoming.clear();
                outgoing.clear();
            } else {
                incoming.truncate(limit);
                outgoing.truncate(limit);
            }

            let mut symbol_map = HashMap::new();
            if include_symbols {
                let mut ids = HashSet::new();
                for edge in incoming.iter().chain(outgoing.iter()) {
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

            let incoming =
                build_edge_references(incoming, &symbol_map, include_symbols, include_snippet);
            let outgoing =
                build_edge_references(outgoing, &symbol_map, include_symbols, include_snippet);

            // Add metadata if we aggregated members
            let metadata = if member_count > 0 {
                Some(ReferencesMetadata {
                    aggregated_members: member_count,
                    note: format!(
                        "Includes references to {} member methods/fields",
                        member_count
                    ),
                })
            } else {
                None
            };

            let mut value = json!(ReferencesResult {
                symbol,
                incoming,
                outgoing,
                metadata,
            });

            // Check format param
            let format = params.format.as_deref().unwrap_or("full");
            if format == "signatures" {
                value = apply_compact_format(value);
            }

            value
        }
        "trace_flow" => {
            let params: TraceFlowParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let languages = params.languages.clone();
            let max_hops = params.max_hops.unwrap_or(5).min(10);
            let direction = params.direction.as_deref().unwrap_or("downstream");
            let include_snippets = params.include_snippets.unwrap_or(true);
            let max_bytes = params.max_bytes.unwrap_or(30_000).min(200_000);
            let allowed_kinds: Vec<String> = params.kinds.clone().unwrap_or_else(||
                vec![
                    "CALLS".into(), "RPC_IMPL".into(), "RPC_CALL".into(), "XREF".into(),
                    "CHANNEL_PUBLISH".into(), "CHANNEL_SUBSCRIBE".into(),
                    "HTTP_CALL".into(), "HTTP_ROUTE".into(),
                ]
            );

            // Resolve start symbol
            let start = if let Some(id) = params.start_id {
                indexer.db().get_symbol_by_id(id)?
                    .ok_or_else(|| anyhow::anyhow!("start symbol not found: id={}", id))?
            } else if let Some(ref qn) = params.start_qualname {
                let id = indexer.db().lookup_symbol_id(qn, graph_version)?
                    .ok_or_else(|| anyhow::anyhow!("start symbol not found: {}", qn))?;
                indexer.db().get_symbol_by_id(id)?
                    .ok_or_else(|| anyhow::anyhow!("start symbol not found"))?
            } else {
                anyhow::bail!("trace_flow requires start_id or start_qualname");
            };

            // Resolve optional end symbol
            let end_id = if let Some(id) = params.end_id {
                Some(id)
            } else if let Some(ref qn) = params.end_qualname {
                indexer.db().lookup_symbol_id(qn, graph_version)?
            } else {
                None
            };

            // BFS from start
            let mut trace = Vec::new();
            let mut visited = std::collections::HashSet::new();
            visited.insert(start.id);
            let mut queue = std::collections::VecDeque::new();
            queue.push_back((start.id, 0usize, start.file_path.clone()));
            let mut used_bytes = 0;
            let mut truncated = false;
            let mut reached_target = false;

            while let Some((current_id, dist, prev_file)) = queue.pop_front() {
                if dist > max_hops {
                    truncated = true;
                    break;
                }
                if used_bytes >= max_bytes {
                    truncated = true;
                    break;
                }

                let mut edges = indexer.db().edges_for_symbol(
                    current_id, languages.as_deref(), graph_version
                )?;

                // For upstream direction, also find unresolved callers via qualname pattern
                if direction == "upstream" {
                    if let Ok(Some(current_sym)) = indexer.db().get_symbol_by_id(current_id) {
                        for kind in &allowed_kinds {
                            let mut unresolved = indexer.db().incoming_edges_by_qualname_pattern(
                                &current_sym.name, kind, languages.as_deref(), graph_version
                            ).unwrap_or_default();
                            edges.append(&mut unresolved);
                        }
                    }
                }

                // Collect bridgeable edges for a second pass
                let mut bridge_targets: Vec<(String, String)> = Vec::new(); // (target_qualname, edge_kind)

                for edge in &edges {
                    if !allowed_kinds.contains(&edge.kind) { continue; }

                    // Determine next symbol based on direction
                    let next_id = if direction == "downstream" {
                        // Follow outgoing calls: we are the source, get target
                        if edge.source_symbol_id != Some(current_id) { continue; }
                        edge.target_symbol_id
                    } else {
                        // Follow incoming calls: we are the target, get source
                        // For resolved edges, check target matches us
                        // For unresolved edges (from qualname pattern), source_symbol_id is the caller
                        if edge.target_symbol_id == Some(current_id) {
                            edge.source_symbol_id
                        } else if edge.target_symbol_id.is_none() {
                            // Unresolved edge from qualname pattern — source is the caller
                            edge.source_symbol_id
                        } else {
                            // This edge doesn't target us
                            continue;
                        }
                    };

                    // Check for bridgeable edge (e.g., CHANNEL_PUBLISH → CHANNEL_SUBSCRIBE)
                    if let Some(ref tq) = edge.target_qualname {
                        if crate::indexer::channel::bridge_complement(&edge.kind).is_some() {
                            bridge_targets.push((tq.clone(), edge.kind.clone()));
                        }
                    }

                    // Resolve next_id, trying fuzzy lookup if unresolved
                    let next_id = match next_id {
                        Some(id) => id,
                        None => {
                            // Try fuzzy resolve on target_qualname if available
                            if let Some(ref qn) = edge.target_qualname {
                                match indexer.db().lookup_symbol_id_fuzzy(qn, languages.as_deref(), graph_version) {
                                    Ok(Some(id)) => id,
                                    _ => continue,
                                }
                            } else {
                                continue;
                            }
                        }
                    };

                    if !visited.insert(next_id) { continue; }

                    if let Ok(Some(next_sym)) = indexer.db().get_symbol_by_id(next_id) {
                        let prev_lang = detect_language(&prev_file);
                        let next_lang = detect_language(&next_sym.file_path);
                        let cross_lang = prev_lang != next_lang;
                        let language = next_lang.clone();

                        // Read snippet if requested
                        let snippet = if include_snippets {
                            edge.evidence_snippet.clone()
                        } else { None };

                        // Detect language boundary and add annotations
                        let (boundary_type, boundary_detail, protocol_context) = if cross_lang {
                            let b_type = detect_boundary_type(&edge.kind, &prev_lang, &next_lang);
                            let b_detail = build_boundary_detail(&b_type, &prev_lang, &next_lang);
                            let p_context = extract_protocol_context(edge);
                            (Some(b_type), Some(b_detail), p_context)
                        } else {
                            (None, None, None)
                        };

                        let hop = TraceHop {
                            symbol: next_sym.clone(),
                            edge_kind: edge.kind.clone(),
                            distance: dist + 1,
                            language,
                            snippet,
                            cross_language: cross_lang,
                            boundary_type,
                            boundary_detail,
                            protocol_context,
                        };

                        let hop_size = serde_json::to_string(&hop).unwrap_or_default().len();
                        used_bytes += hop_size;
                        trace.push(hop);

                        // Check budget immediately after adding hop
                        if used_bytes >= max_bytes {
                            truncated = true;
                            break;
                        }

                        // Check if we reached the target
                        if end_id == Some(next_id) {
                            reached_target = true;
                            break;
                        }

                        queue.push_back((next_id, dist + 1, next_sym.file_path.clone()));
                    }
                }

                // Bridge pass: for edges with bridge complements, find cross-service symbols
                if !reached_target && !truncated {
                    for (tq, edge_kind) in &bridge_targets {
                        if let Some(complement_kinds) = crate::indexer::channel::bridge_complement(edge_kind) {
                            let bridged = indexer.db().edges_by_target_qualname_and_kinds(
                                tq, complement_kinds, languages.as_deref(), graph_version
                            ).unwrap_or_default();
                            let b_type = crate::indexer::channel::boundary_type_for_kind(edge_kind);
                            for bridged_edge in &bridged {
                                let Some(bridged_id) = bridged_edge.source_symbol_id else { continue };
                                if !visited.insert(bridged_id) { continue; }
                                if let Ok(Some(bridged_sym)) = indexer.db().get_symbol_by_id(bridged_id) {
                                    let prev_lang = detect_language(&prev_file);
                                    let next_lang = detect_language(&bridged_sym.file_path);
                                    let b_detail = build_boundary_detail(b_type, &prev_lang, &next_lang);
                                    let p_context = extract_protocol_context(bridged_edge);
                                    let hop = TraceHop {
                                        symbol: bridged_sym.clone(),
                                        edge_kind: bridged_edge.kind.clone(),
                                        distance: dist + 1,
                                        language: next_lang,
                                        snippet: if include_snippets { bridged_edge.evidence_snippet.clone() } else { None },
                                        cross_language: true,
                                        boundary_type: Some(b_type.to_string()),
                                        boundary_detail: Some(b_detail),
                                        protocol_context: p_context,
                                    };
                                    let hop_size = serde_json::to_string(&hop).unwrap_or_default().len();
                                    used_bytes += hop_size;
                                    trace.push(hop);
                                    if used_bytes >= max_bytes { truncated = true; break; }
                                    if end_id == Some(bridged_id) { reached_target = true; break; }
                                    queue.push_back((bridged_id, dist + 1, bridged_sym.file_path.clone()));
                                }
                            }
                            if reached_target || truncated { break; }
                        }
                    }
                }

                if reached_target || truncated { break; }
            }

            // Sort trace by distance
            trace.sort_by_key(|h| h.distance);

            let end_sym = if let Some(eid) = end_id {
                indexer.db().get_symbol_by_id(eid)?
            } else { None };

            let next_hops = trace.iter().take(3).map(|h| {
                json!({"method": "explain_symbol", "params": {"id": h.symbol.id},
                       "description": format!("Explain {}", h.symbol.name)})
            }).collect::<Vec<_>>();

            // Calculate paths_found: 0 if empty and no target reached, 1 if target reached, else count leaf nodes
            let paths_found = if trace.is_empty() {
                0
            } else if end_id.is_some() {
                if reached_target { 1 } else { 0 }
            } else {
                // Count distinct leaf nodes (max distance symbols)
                let max_dist = trace.iter().map(|h| h.distance).max().unwrap_or(0);
                trace.iter().filter(|h| h.distance == max_dist).count()
            };

            let result = TraceFlowResult {
                start,
                end: end_sym,
                trace,
                paths_found,
                reached_target,
                truncated,
                budget: BudgetInfo {
                    budget_bytes: max_bytes,
                    used_bytes,
                    truncated,
                },
                next_hops,
            };

            serde_json::to_value(&result)?
        }
        "list_edges" => {
            let params: EdgesParams = serde_json::from_value(params)?;
            list_edges_response(indexer, params, None, false, false)?
        }
        "list_xrefs" => {
            let params: EdgesParams = serde_json::from_value(params)?;
            list_edges_response(indexer, params, Some("XREF"), true, true)?
        }
        "route_refs" => {
            let params: RouteRefsParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let limit = params.limit.unwrap_or(200).min(MAX_RESPONSE_LIMIT);
            let include_symbols = params.include_symbols.unwrap_or(true);
            let include_snippet = params.include_snippet.unwrap_or(true);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            // Try HTTP route first, fall back to partial matching, then gRPC RPC_IMPL
            let (mut edges, normalized) =
                if let Some(norm) = xref::normalize_route_literal(&params.query) {
                    let kinds = Some(vec!["ROUTE".to_string()]);
                    let edges = indexer.db().list_edges(
                        limit, 0, languages.as_deref(), paths.as_deref(),
                        kinds.as_deref(), None, None, Some(&norm),
                        false, None, graph_version, None, None, None,
                    )?;
                    (edges, norm)
                } else {
                    // Fall back to gRPC service lookup via RPC_IMPL edges.
                    // Fetch all RPC_IMPL edges and filter by query (case-insensitive).
                    let kinds = Some(vec!["RPC_IMPL".to_string()]);
                    let all_rpc = indexer.db().list_edges(
                        500, 0, languages.as_deref(), paths.as_deref(),
                        kinds.as_deref(), None, None, None,
                        false, None, graph_version, None, None, None,
                    )?;
                    let query_lower = params.query.to_lowercase();
                    let edges: Vec<_> = all_rpc
                        .into_iter()
                        .filter(|e| {
                            e.target_qualname
                                .as_ref()
                                .map_or(false, |qn| qn.to_lowercase().contains(&query_lower))
                        })
                        .take(limit)
                        .collect();
                    (edges, params.query.clone())
                };

            // If exact match failed and we have a normalized route, try partial/prefix matching
            if edges.is_empty() {
                if let Some(norm) = xref::normalize_route_literal(&params.query) {
                    // Try partial matching on ROUTE and HTTP_ROUTE edges
                    let kinds = Some(vec!["ROUTE".to_string(), "HTTP_ROUTE".to_string()]);
                    let all_routes = indexer.db().list_edges(
                        2000, 0, languages.as_deref(), paths.as_deref(),
                        kinds.as_deref(), None, None, None,
                        false, None, graph_version, None, None, None,
                    )?;
                    let query_lower = norm.to_lowercase();
                    edges = all_routes
                        .into_iter()
                        .filter(|e| {
                            e.target_qualname
                                .as_ref()
                                .map_or(false, |qn| qn.to_lowercase().contains(&query_lower))
                        })
                        .take(limit)
                        .collect();
                }
            }
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
            let references =
                build_edge_references(edges, &symbol_map, include_symbols, include_snippet);
            json!(RouteRefsResult {
                query: params.query,
                normalized,
                references,
            })
        }
        "flow_status" => {
            let params: FlowStatusParams = serde_json::from_value(params)?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let limit = params.limit.unwrap_or(200).min(MAX_RESPONSE_LIMIT);
            let edge_limit = params.edge_limit.unwrap_or(50_000);
            let include_routes = params.include_routes.unwrap_or(true);
            let include_calls = params.include_calls.unwrap_or(true);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let route_kinds = vec![
                http::HTTP_ROUTE_KIND.to_string(),
                proto::RPC_IMPL_KIND.to_string(),
            ];
            let call_kinds = vec![http::HTTP_CALL_KIND.to_string()];
            let routes = indexer.db().list_edges(
                edge_limit,
                0,
                languages.as_deref(),
                paths.as_deref(),
                Some(&route_kinds),
                None,
                None,
                None,
                false,
                None,
                graph_version,
                None,
                None,
                None,
            )?;
            let calls = indexer.db().list_edges(
                edge_limit,
                0,
                languages.as_deref(),
                paths.as_deref(),
                Some(&call_kinds),
                None,
                None,
                None,
                false,
                None,
                graph_version,
                None,
                None,
                None,
            )?;
            let result = build_flow_status(
                routes,
                calls,
                include_routes,
                include_calls,
                limit,
                edge_limit,
            );
            json!(result)
        }
        "search_rg" => {
            let params: RgParams = serde_json::from_value(params)?;
            validate_pattern_length(&params.query, "search_rg")?;
            let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
            let context_lines = normalize_rg_context_lines(params.context_lines);
            let include_text = params.include_text.unwrap_or(true);
            let include_symbol = params.include_symbol.unwrap_or(false);
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = resolve_rg_paths(indexer.repo_root(), params.path, params.paths)?;
            let globs = params.globs.unwrap_or_default();
            let options = RgSearchOptions {
                include_text,
                case_sensitive: params.case_sensitive,
                fixed_string: params.fixed_string.unwrap_or(false),
                hidden: params.hidden.unwrap_or(false),
                no_ignore: params.no_ignore.unwrap_or(false),
                follow: params.follow.unwrap_or(false),
                globs,
                paths,
            };
            let mut results = search_rg(indexer.repo_root(), &params.query, limit, options)?;
            for hit in &mut results {
                if hit.engine.is_none() {
                    hit.engine = Some("search_rg".to_string());
                }
            }
            annotate_grep_hits(
                indexer,
                &mut results,
                context_lines,
                include_symbol,
                graph_version,
                Some(&params.query),
            )?;
            json!(results)
        }
        "search" | "search_text" => {
            let params: SearchParams = serde_json::from_value(params)?;
            validate_pattern_length(&params.query, "search_text")?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let scope = search::parse_scope(params.scope.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let options = search::SearchOptions {
                languages: languages.as_deref(),
                scope,
                exclude_generated: params.exclude_generated.unwrap_or(false),
                rank: params.rank.unwrap_or(true),
                no_ignore: params.no_ignore.unwrap_or(false),
                paths: paths.as_deref(),
            };
            let mut results =
                search::search_text(indexer.repo_root(), &params.query, limit, options)?;
            for hit in &mut results {
                hit.engine = Some("search_text".to_string());
            }
            let context_lines = normalize_context_lines(params.context_lines, 2);
            let include_symbol = params.include_symbol.unwrap_or(true);
            annotate_search_hits(
                indexer,
                &mut results,
                context_lines,
                include_symbol,
                graph_version,
                Some(&params.query),
            )?;
            // Add capped metadata to response
            let capped = results.len() >= limit;
            json!({
                "results": results,
                "capped": capped,
                "total_returned": results.len(),
            })
        }
        "grep" => {
            let params: GrepParams = serde_json::from_value(params)?;
            validate_pattern_length(&params.query, "grep")?;
            let limit = params.limit.unwrap_or(50).min(MAX_RESPONSE_LIMIT);
            let include_text = params.include_text.unwrap_or(false);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let scope = search::parse_scope(params.scope.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let options = search::SearchOptions {
                languages: languages.as_deref(),
                scope,
                exclude_generated: params.exclude_generated.unwrap_or(false),
                rank: params.rank.unwrap_or(true),
                no_ignore: params.no_ignore.unwrap_or(false),
                paths: paths.as_deref(),
            };
            let mut results = search::grep_text(
                indexer.repo_root(),
                &params.query,
                limit,
                include_text,
                options,
            )?;
            for hit in &mut results {
                hit.engine = Some("grep".to_string());
            }
            let context_lines = normalize_context_lines(params.context_lines, 0);
            let include_symbol = params.include_symbol.unwrap_or(false);
            annotate_grep_hits(
                indexer,
                &mut results,
                context_lines,
                include_symbol,
                graph_version,
                Some(&params.query),
            )?;
            // Add capped metadata to response
            let capped = results.len() >= limit;
            json!({
                "results": results,
                "capped": capped,
                "total_returned": results.len(),
            })
        }
        "changed_files" => {
            let params: ChangedFilesParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let changed = indexer.changed_files(languages.as_deref())?;
            json!(changed)
        }
        "index_status" => {
            let params: IndexStatusParams = serde_json::from_value(params)?;
            let include_paths = params.include_paths.unwrap_or(false);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let changed = indexer.changed_files(languages.as_deref())?;
            let counts = IndexChangeCounts {
                added: changed.added.len(),
                modified: changed.modified.len(),
                deleted: changed.deleted.len(),
            };
            let stale = counts.added > 0 || counts.modified > 0 || counts.deleted > 0;
            let last_indexed = indexer.db().get_meta_i64("last_indexed")?;
            let hint = if last_indexed.is_none() {
                "index missing; run reindex".to_string()
            } else if stale {
                "reindex needed".to_string()
            } else {
                "index current".to_string()
            };
            let commit_sha = indexer.db().graph_version_commit(indexer.graph_version())?;
            let status = IndexStatus {
                repo_root: indexer.repo_root().to_string_lossy().to_string(),
                last_indexed,
                graph_version: Some(indexer.graph_version()),
                commit_sha,
                stale,
                hint,
                counts,
                changed_files: if include_paths { Some(changed) } else { None },
            };
            json!(status)
        }
        "reindex" => {
            let params: ReindexParams = serde_json::from_value(params)?;
            let stats = indexer.reindex()?;

            // Optionally resolve unresolved edges after reindexing
            let mut json_stats = json!(stats);
            if params.resolve_edges.unwrap_or(false) {
                let graph_version = indexer.db().current_graph_version()?;
                let resolved = indexer.db().resolve_null_target_edges(graph_version)?;
                // Add resolved count to stats
                if let Some(obj) = json_stats.as_object_mut() {
                    obj.insert("edges_resolved".to_string(), json!(resolved));
                }
            }

            // Optionally mine git co-changes after reindexing
            if params.mine_git.unwrap_or(false) {
                use crate::git_mining;

                eprintln!("lidx: Mining git co-changes...");
                let max_commits = 1000;
                let since_days = 180;

                match git_mining::mine_co_changes(indexer.repo_root(), max_commits, since_days) {
                    Ok(entries) => {
                        let count = entries.len();
                        match indexer.db_mut().insert_co_changes_batch(&entries) {
                            Ok(inserted) => {
                                eprintln!("lidx: Inserted {} co-change patterns", inserted);
                                if let Some(obj) = json_stats.as_object_mut() {
                                    obj.insert("co_changes_mined".to_string(), json!(count));
                                    obj.insert("co_changes_inserted".to_string(), json!(inserted));
                                }
                            }
                            Err(e) => {
                                eprintln!("lidx: Warning: Failed to insert co-changes: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("lidx: Warning: Git mining failed: {}", e);
                    }
                }
            }

            apply_field_filters(
                json_stats,
                params.summary.unwrap_or(false),
                params.fields.as_deref(),
                &["scanned", "indexed", "skipped", "deleted"],
            )
        }
        "diagnostics_run" => {
            let params: DiagnosticsRunParams = serde_json::from_value(params)?;
            let result = diagnostics_run(indexer, params)?;
            json!(result)
        }
        "diagnostics_import" => {
            let params: DiagnosticsImportParams = serde_json::from_value(params)?;
            // Security fix: Use resolve_repo_path_for_op to validate path
            let (abs, _rel) =
                resolve_repo_path_for_op(indexer.repo_root(), &params.path, "diagnostics_import")?;
            let content = util::read_to_string(&abs)
                .with_context(|| format!("read diagnostics {}", abs.display()))?;
            let diagnostics = diagnostics::parse_sarif(&content, indexer.repo_root())?;
            let imported = indexer.db_mut().insert_diagnostics(&diagnostics)?;
            json!({ "imported": imported })
        }
        "diagnostics_list" => {
            let params: DiagnosticsListParams = serde_json::from_value(params)?;
            let limit = params.limit.unwrap_or(100).min(MAX_RESPONSE_LIMIT);
            let offset = params.offset.unwrap_or(0);
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let diagnostics = indexer.db().list_diagnostics(
                limit,
                offset,
                languages.as_deref(),
                paths.as_deref(),
                params.severity.as_ref(),
                params.rule_id.as_ref(),
                params.tool.as_ref(),
            )?;
            json!(diagnostics)
        }
        "diagnostics_summary" => {
            let params: DiagnosticsSummaryParams = serde_json::from_value(params)?;
            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;
            let summary = indexer.db().diagnostics_summary(
                languages.as_deref(),
                paths.as_deref(),
                params.severity.as_ref(),
                params.rule_id.as_ref(),
                params.tool.as_ref(),
            )?;
            json!(summary)
        }
        "gather_context" => {
            use crate::gather_context;

            const MAX_SEEDS: usize = 100;
            const MAX_BYTES_HARD_CAP: usize = 2_000_000; // 2MB

            let params: GatherContextParams = serde_json::from_value(params)?;

            // Validate parameters
            let validation = validate_gather_context_params(&params);
            if !validation.is_valid() {
                return Err(anyhow::anyhow!(
                    "Validation failed: {}",
                    serde_json::to_string(&validation.errors)?
                ));
            }

            // Moderate Concern #3: Validate seed count
            if params.seeds.len() > MAX_SEEDS {
                anyhow::bail!(
                    "Too many seeds: {} (max: {})",
                    params.seeds.len(),
                    MAX_SEEDS
                );
            }

            let languages = scan::normalize_language_filter(params.languages.as_deref())?;
            let graph_version = resolve_graph_version(indexer, params.graph_version)?;
            let paths = normalize_search_paths(indexer.repo_root(), params.path, params.paths)?;

            // Moderate Concern #1: Enforce hard cap on max_bytes
            let max_bytes = params.max_bytes.unwrap_or(100_000).min(MAX_BYTES_HARD_CAP);

            // Determine strategy: default to "symbol" if all seeds are symbol seeds
            let strategy = params.strategy.or_else(|| {
                let all_symbol_seeds = params.seeds.iter().all(|seed| {
                    matches!(seed, ContextSeed::Symbol { .. })
                });
                if all_symbol_seeds && !params.seeds.is_empty() {
                    Some("symbol".to_string())
                } else {
                    Some("file".to_string())
                }
            });

            let config = gather_context::GatherConfig {
                max_bytes,
                depth: params.depth.unwrap_or(2),
                max_nodes: params.max_nodes.unwrap_or(50),
                include_snippets: params.include_snippets.unwrap_or(true),
                include_related: params.include_related.unwrap_or(true),
                dry_run: params.dry_run.unwrap_or(false),
                languages,
                paths,
                graph_version,
                semantic_results: HashMap::new(),
                strategy,
            };

            let result = gather_context::gather_context(
                indexer.db(),
                indexer.repo_root(),
                &params.seeds,
                &config,
            )?;

            json!(result)
        }
        other => {
            return Err(anyhow::anyhow!("unknown method: {other}"));
        }
    };

    // Log slow queries
    let elapsed = start.elapsed();
    if elapsed.as_millis() > 100 {
        eprintln!("lidx: Slow query: {} took {:?}", method, elapsed);
    }

    // Apply token budget truncation if requested
    if let Some(max_bytes) = max_response_bytes {
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
