mod compact;
mod format;
mod handlers;
mod reading;
mod schema;
mod validate;

pub(crate) use crate::indexer::differ::{ChangedFile, parse_diff_with_ranges};
use crate::indexer::{Indexer, scan, test_detection};
use crate::model::{
    AnalyzeDiffResult, BudgetInfo, ChangedSymbol, DiffImpactEntry, ExplainRef, ExplainSymbolResult,
    LowerBound, ModuleEdge, ModuleNode, OutlineEntry, OutlineResult, ReadSymbolEntry,
    RiskAssessment, RiskFactor, RpcSuggestion, Symbol, TestCoverageEntry, TestRef, TraceFlowResult,
};
use crate::util::normalize_search_paths;
use crate::watch;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
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
    /// Force another unresolved-reference repair pass after reindexing,
    /// beyond the one reindex already runs when it detects work to do.
    resolve_edges: Option<bool>,
    mine_git: Option<bool>,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TopComplexityParams {
    limit: Option<usize>,
    min_complexity: Option<i64>,
    #[serde(flatten)]
    common: CommonParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RepoMapParams {
    /// Maximum bytes of output text (default: 8000, min: 1000, max: 50000)
    max_bytes: Option<usize>,
    #[serde(flatten)]
    common: CommonParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    /// "upstream" (find consumers/callers), "downstream" (follow calls), or "both" (default). Use "upstream" for "what depends on this?" Case-insensitive; aliases "up"/"callers" and "down"/"callees"; anything else is an error.
    direction: Option<String>,
    /// Edge kinds to follow, upper-case names such as CALLS, IMPORTS, EXTENDS, IMPLEMENTS, RPC_IMPL, RPC_CALL, HTTP_ROUTE, HTTP_CALL, CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE, CONFIG_SOURCE, CONFIG_READ, CONFIG_BIND, XREF (every indexed edge kind is accepted); matched case-insensitively, unknown kinds are an error. Default: CALLS, RPC_IMPL
    kinds: Option<Vec<String>>,
    /// Resolution kinds to exclude from traversal, e.g. ["bare_name", "two_segment"]
    /// to exclude the guarded name-fallback tier's heuristic edges. Default: none excluded.
    exclude_resolution_kinds: Option<Vec<String>>,
    include_tests: Option<bool>,
    include_paths: Option<bool>,
    /// Max affected symbols. In batch mode (`qualnames`) an explicit limit is an upper bound per seed; omitted, 500 is split across seeds with a floor of 50 each.
    limit: Option<usize>,
    /// Global configuration
    min_confidence: Option<f32>,
    #[serde(flatten)]
    common: LangVersionParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    /// Restrict hits to files of this scope: "code" (excludes tests, docs and
    /// examples), "docs", "tests", "examples" or "all" (default)
    scope: Option<crate::search::SearchScope>,
    /// Language filter (e.g. ["rust", "python"]): only hits in files of these languages
    languages: Option<Vec<String>>,
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, Default, schemars::JsonSchema)]
struct OnboardParams {
    #[serde(flatten)]
    common: LangVersionParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    /// Maximum test-code nodes among related symbols; tests are always ordered after
    /// non-test code (default: 8)
    max_test_nodes: Option<usize>,
    /// Include full bodies for related symbols (default: true). When false, related symbols
    /// are still expanded but returned as signature stubs; seed bodies are always returned.
    include_snippets: Option<bool>,
    /// Include related symbols via call graph (default: true)
    include_related: Option<bool>,
    /// If true, run the full collection but return items with empty content; `estimated_bytes`
    /// is the total a real run would return (`total_bytes` is 0)
    dry_run: Option<bool>,
    /// Content strategy: "symbol" (symbol bodies only) or "file" (full files)
    /// Defaults to "symbol" when all seeds are symbol/id seeds, "file" otherwise
    strategy: Option<String>,
    #[serde(flatten)]
    common: CommonParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
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
    /// Sections to include in the response. Accepts any of: "source",
    /// "callers", "callees", "tests", "implements". Aliases: "dependencies"
    /// -> "callees", "dependents" -> "callers", "summary"/"body" -> "source".
    /// Default: all five sections.
    sections: Option<Vec<String>>,
    /// Max references returned per section (callers, callees, tests,
    /// implements). Each capped section also reports its true `<section>_total`
    /// count, so a list capped here can be told apart from a complete one.
    /// Default: 10.
    max_refs: Option<usize>,
    format: Option<String>,
    /// Keep only callers, callees and tests refs whose edge resolved at or
    /// above this tier (exact, import, receiver_type, inherited,
    /// two_segment, bare_name, external -- strongest to weakest; see
    /// `Edge::resolution_kind`). A ref whose edge never resolved (no
    /// `resolution_kind` at all) is always excluded once this is set.
    /// Distinct from `analyze_impact`'s `min_confidence`, which filters an
    /// unrelated query-time heuristic. Omit to return every ref regardless
    /// of tier. An unknown tier name is ignored with a warning rather than
    /// an error.
    min_resolution: Option<String>,
    #[serde(flatten)]
    common: LangVersionParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

/// Trace calls/edges from a start symbol. The result's `paths_found` is the
/// number of leaf hops in the trace (hops nothing else was reached through,
/// including hops at the `max_hops` ceiling), not distinct root-to-leaf paths
/// since the trace is node-deduplicated; it is non-decreasing in `max_hops`
/// and unaffected by `trace_offset`/`max_bytes` paging. With an end target it
/// is 1 if the target was reached, else 0.
#[derive(Deserialize, schemars::JsonSchema)]
struct TraceFlowParams {
    start_id: Option<i64>,
    start_qualname: Option<String>,
    /// Fuzzy search query to find start symbol (alternative to start_id/start_qualname)
    #[serde(alias = "start_query")]
    query: Option<String>,
    end_id: Option<i64>,
    end_qualname: Option<String>,
    /// "downstream" (follow calls) or "upstream" (follow callers). Default: "downstream". Case-insensitive; aliases "up"/"callers" and "down"/"callees"; anything else is an error.
    direction: Option<String>,
    /// Max hops (default: 5, max: 10)
    max_hops: Option<usize>,
    /// Edge kinds to follow, upper-case names such as CALLS, IMPORTS, EXTENDS, IMPLEMENTS, RPC_IMPL, RPC_CALL, HTTP_ROUTE, HTTP_CALL, CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE, CONFIG_SOURCE, CONFIG_READ, CONFIG_BIND, XREF (every indexed edge kind is accepted); matched case-insensitively, unknown kinds are an error. Default: CALLS, RPC_IMPL
    kinds: Option<Vec<String>>,
    /// Resolution kinds to exclude from traversal, e.g. ["bare_name", "two_segment"]
    /// to exclude the guarded name-fallback tier's heuristic edges. Default: none excluded.
    exclude_resolution_kinds: Option<Vec<String>>,
    /// Include source snippets
    include_snippets: Option<bool>,
    format: Option<String>,
    trace_offset: Option<usize>,
    max_bytes: Option<usize>,
    #[serde(flatten)]
    common: LangVersionParams,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ContextParams {
    /// File path (relative to repo root) to retrieve structured context for
    path: String,
    /// Output format: "json" for structured JSON output, or omit for text (default: text)
    format: Option<String>,
    /// Graph version to query (defaults to current)
    #[serde(alias = "as_of", alias = "version")]
    graph_version: Option<i64>,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

/// Params for `outline`: a compact, no-bodies skeleton of a file's symbols.
#[derive(Deserialize, schemars::JsonSchema)]
struct OutlineParams {
    /// Repo-relative file path to outline
    path: String,
    /// Filter to specific symbol kinds (e.g. ["function", "class"]). For Markdown
    /// files, kinds are heading levels ("h1".."h6").
    kinds: Option<Vec<String>>,
    /// Maximum nesting depth to include. Depth 0 is a top-level entry (no parent
    /// in this file); a method inside a class is depth 1, and so on. Default:
    /// unlimited (all depths included).
    max_depth: Option<usize>,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

/// Params for `read_symbol`: fetch a symbol's exact source from disk.
/// Exactly one of `qualname`, `query`, `qualnames` must be given.
///
/// Response fields beyond the echoed header (qualname/kind/path/start_line/
/// end_line/stale/source): for a `qualnames` read, `omitted` lists qualnames
/// that resolved but didn't fit `max_bytes` (whole symbols, never cut
/// mid-body), `not_found` lists ones that didn't resolve to a real symbol,
/// and `errors` lists ones that resolved but failed to read (e.g. a missing
/// file), each as `{"qualname", "error"}`. For a single `qualname`/`query`
/// read, `omitted: true` plus `size_bytes` replace `source` when the result
/// would exceed `max_bytes`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ReadSymbolParams {
    /// Exact qualname of the symbol to read (exactly one of qualname/query/qualnames required)
    qualname: Option<String>,
    /// Fuzzy search query resolved the same way as explain_symbol/trace_flow (exactly one of qualname/query/qualnames required)
    query: Option<String>,
    /// Multiple qualnames to read in one call, filled in request order (exactly one of qualname/query/qualnames required)
    qualnames: Option<Vec<String>>,
    /// For a container symbol (class/struct/impl/module), return its children's
    /// signatures and line ranges instead of the full body (default: false).
    /// Falls back to a normal read for a symbol with no children.
    skeleton: Option<bool>,
    /// Lines of surrounding context to include around the symbol's span, clamped
    /// at file bounds (default: 0)
    context_lines: Option<usize>,
    /// Response byte budget (default: 30000), a hard cap on the whole
    /// response including `omitted`/`not_found`/`errors`. For a `qualnames`
    /// (multi-symbol) read, symbols are added in request order until the next
    /// one would exceed this, then it and every symbol after it are omitted
    /// whole and listed by qualname under `omitted`. For a single
    /// `qualname`/`query` read, if the resolved symbol's response would
    /// exceed this, only its header fields are returned with `omitted: true`
    /// -- a symbol is never cut mid-body.
    max_bytes: Option<usize>,
    #[serde(flatten)]
    #[schemars(skip)]
    extra: HashMap<String, Value>,
}

/// Hard cap on result count to prevent huge responses that blow LLM context windows.
const MAX_RESPONSE_LIMIT: usize = 500;

pub const METHOD_LIST: &[&str] = &[
    "search",
    "outline",
    "read_symbol",
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

        let response = match parse_request_line(&line) {
            Ok(request) => app.handle_request(request),
            Err(response) => response,
        };

        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }

    Ok(())
}

/// Turn one input line into a request, or the error response to send instead.
/// Parses to `Value` first so the `id` of an object that fails to deserialize
/// is still echoed; non-object input gets `id: null`.
fn parse_request_line(line: &str) -> std::result::Result<RpcRequest, RpcResponse> {
    let value = serde_json::from_str::<Value>(line)
        .map_err(|err| format::error_response(Value::Null, &format!("invalid request: {err}")))?;
    let id = match &value {
        Value::Object(map) => map.get("id").cloned().unwrap_or(Value::Null),
        Value::Array(items) if items.is_empty() => {
            return Err(format::error_response(
                Value::Null,
                "invalid request: empty array",
            ));
        }
        Value::Array(_) => {
            return Err(format::error_response(
                Value::Null,
                "invalid request: batches are not supported",
            ));
        }
        _ => {
            return Err(format::error_response(
                Value::Null,
                "invalid request: expected a JSON object",
            ));
        }
    };
    serde_json::from_value::<RpcRequest>(value)
        .map_err(|err| format::error_response(id, &format!("invalid request: {err}")))
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
    let id = request.id.clone();
    let response = match app.run(request) {
        Ok(value) => RpcResponse {
            id,
            result: Some(value),
            error: None,
        },
        // Unknown params fail the process (nonzero exit) instead of hiding in
        // a successful-looking response envelope.
        Err(err) if err.downcast_ref::<UnknownParamsError>().is_some() => return Err(err),
        Err(err) => format::error_response(id, &err.to_string()),
    };
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

    fn run(&mut self, req: RpcRequest) -> Result<Value> {
        handle_method(&mut self.indexer, &req.method, req.params)
    }

    fn handle_request(&mut self, req: RpcRequest) -> RpcResponse {
        let id = req.id.clone();
        match self.run(req) {
            Ok(value) => RpcResponse {
                id,
                result: Some(value),
                error: None,
            },
            Err(err) => format::error_response(id, &err.to_string()),
        }
    }
}

/// Who is the authority on a method's response size.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sizing {
    /// Generic dispatcher cap (`DEFAULT_MAX_RESPONSE_BYTES`) applies.
    Capped,
    /// Large by design: capped only when the caller asks.
    Uncapped,
    /// Budgets and paginates its own output (`max_bytes`, continuation
    /// next_hops); exempt from the default cap, but an explicit caller cap is
    /// handed down as its `max_bytes` and still enforced as a backstop.
    SelfBudgeting,
}

/// One dispatchable method: its handler and its sizing contract, declared in
/// one place so the two cannot drift apart.
struct MethodSpec {
    name: &'static str,
    sizing: Sizing,
    run: fn(&mut Indexer, Value) -> Result<Value>,
}

const METHOD_SPECS: &[MethodSpec] = &[
    MethodSpec {
        name: "search",
        sizing: Sizing::Capped,
        run: handlers::handle_search_rg,
    },
    MethodSpec {
        name: "outline",
        sizing: Sizing::Capped,
        run: reading::handle_outline,
    },
    MethodSpec {
        name: "read_symbol",
        sizing: Sizing::SelfBudgeting,
        run: reading::handle_read_symbol,
    },
    MethodSpec {
        name: "explain_symbol",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_explain_symbol,
    },
    MethodSpec {
        name: "trace_flow",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_trace_flow,
    },
    MethodSpec {
        name: "analyze_impact",
        sizing: Sizing::Capped,
        run: handlers::handle_analyze_impact,
    },
    MethodSpec {
        name: "analyze_diff",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_analyze_diff,
    },
    MethodSpec {
        name: "gather_context",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_gather_context,
    },
    MethodSpec {
        name: "orient",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_orient,
    },
    MethodSpec {
        name: "onboard",
        sizing: Sizing::Uncapped,
        run: handlers::handle_onboard,
    },
    MethodSpec {
        name: "reindex",
        sizing: Sizing::Capped,
        run: handlers::handle_reindex,
    },
    MethodSpec {
        name: "top_complexity",
        sizing: Sizing::Capped,
        run: handlers::handle_top_complexity,
    },
    MethodSpec {
        name: "context",
        sizing: Sizing::Uncapped,
        run: handlers::handle_context,
    },
    MethodSpec {
        name: "repo_map",
        sizing: Sizing::SelfBudgeting,
        run: handlers::handle_repo_map,
    },
    MethodSpec {
        name: "dead_symbols",
        sizing: Sizing::Capped,
        run: handlers::handle_dead_symbols,
    },
];

/// Whether `method` sizes its own output (see `Sizing::SelfBudgeting`).
pub fn is_self_budgeting(method: &str) -> bool {
    METHOD_SPECS
        .iter()
        .any(|s| s.name == method && s.sizing == Sizing::SelfBudgeting)
}

/// How many times a self-budgeting method is re-run under an explicit cap.
const SELF_BUDGET_RETRIES: usize = 8;

/// Default response size cap (30KB ≈ 7,500 tokens).
/// Applied to `Sizing::Capped` methods when the caller doesn't specify
/// max_response_bytes/max_tokens; `Uncapped` and `SelfBudgeting` methods skip it.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 30_000;

/// What to do with params a handler's params struct did not recognize.
enum UnknownMode {
    /// Reject the request (CLI, raw RPC).
    Strict,
    /// Run anyway, collecting the names (MCP).
    Collect(Vec<String>),
}

thread_local! {
    static UNKNOWN_MODE: std::cell::RefCell<UnknownMode> =
        const { std::cell::RefCell::new(UnknownMode::Strict) };
}

/// Params keys the deserialized params struct did not consume. Every params
/// struct ends with `#[serde(flatten)] extra: HashMap<..>`, so serde itself
/// (aliases and flattened sub-structs included) decides what is unknown.
pub(super) trait ParamsExtra {
    fn extra(&self) -> &HashMap<String, Value>;
}

macro_rules! params_extra {
    ($($ty:ident),* $(,)?) => {
        $(impl ParamsExtra for $ty {
            fn extra(&self) -> &HashMap<String, Value> {
                &self.extra
            }
        })*
    };
}

params_extra!(
    ReindexParams,
    TopComplexityParams,
    RepoMapParams,
    DeadSymbolsParams,
    AnalyzeImpactParams,
    AnalyzeDiffParams,
    RgParams,
    OnboardParams,
    OrientParams,
    GatherContextParams,
    ExplainSymbolParams,
    TraceFlowParams,
    ContextParams,
    OutlineParams,
    ReadSymbolParams,
);

/// Returned (inside `anyhow::Error`) when a strict entry point sees a param
/// its method does not accept.
#[derive(Debug)]
pub struct UnknownParamsError(String);

impl std::fmt::Display for UnknownParamsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UnknownParamsError {}

/// The one place params are deserialized and unknown keys are policed.
/// Strict mode errors naming the keys before the handler does any work;
/// collect mode records them for the caller and carries on.
pub(super) fn parse_params<T>(method: &str, params: Value) -> Result<T>
where
    T: serde::de::DeserializeOwned + ParamsExtra + schemars::JsonSchema,
{
    let parsed: T = T::deserialize(&params)
        .map_err(|e| validate::name_bad_unsigned_param::<T>(&params, e.to_string()))?;
    let mut unknown: Vec<String> = parsed
        .extra()
        .keys()
        .filter(|key| !format::RESPONSE_BUDGET_PARAMS.contains(&key.as_str()))
        .cloned()
        .collect();
    if unknown.is_empty() {
        return Ok(parsed);
    }
    unknown.sort();
    UNKNOWN_MODE.with(|mode| match &mut *mode.borrow_mut() {
        UnknownMode::Collect(ignored) => {
            ignored.extend(unknown);
            Ok(parsed)
        }
        UnknownMode::Strict => {
            let schema = schema::schema_value::<T>();
            let mut valid: Vec<&str> = schema
                .get("properties")
                .and_then(Value::as_object)
                .map(|p| p.keys().map(String::as_str).collect())
                .unwrap_or_default();
            valid.sort_unstable();
            Err(UnknownParamsError(format!(
                "unknown param(s) for method '{method}': {}. Valid params: {}",
                unknown.join(", "),
                valid.join(", ")
            ))
            .into())
        }
    })
}

/// Restores the previous unknown-param mode on drop.
struct ModeGuard(Option<UnknownMode>);

impl ModeGuard {
    fn enter(mode: UnknownMode) -> Self {
        Self(Some(
            UNKNOWN_MODE.with(|m| std::mem::replace(&mut *m.borrow_mut(), mode)),
        ))
    }

    /// Leaves the mode and returns the collected names (empty for strict).
    fn finish(mut self) -> Vec<String> {
        let prev = self.0.take().unwrap_or(UnknownMode::Strict);
        match UNKNOWN_MODE.with(|m| std::mem::replace(&mut *m.borrow_mut(), prev)) {
            UnknownMode::Collect(ignored) => ignored,
            UnknownMode::Strict => Vec::new(),
        }
    }
}

/// Strict dispatch (CLI, raw RPC): an unknown param is an error, raised
/// while the handler parses its params, before it does any work.
pub fn handle_method(indexer: &mut Indexer, method: &str, params: Value) -> Result<Value> {
    let guard = ModeGuard::enter(UnknownMode::Strict);
    let result = dispatch_method(indexer, method, params);
    guard.finish();
    result
}

/// Lenient dispatch (MCP): runs the method with unknown params ignored and
/// returns their names so the caller can surface them.
pub fn handle_method_lenient(
    indexer: &mut Indexer,
    method: &str,
    params: Value,
) -> Result<(Value, Vec<String>)> {
    let guard = ModeGuard::enter(UnknownMode::Collect(Vec::new()));
    let result = dispatch_method(indexer, method, params);
    let ignored = guard.finish();
    Ok((result?, ignored))
}

fn dispatch_method(indexer: &mut Indexer, method: &str, params: Value) -> Result<Value> {
    let start = Instant::now();
    let max_response_bytes = format::extract_max_response_bytes(method, &params);
    let spec = METHOD_SPECS
        .iter()
        .find(|s| s.name == method)
        .ok_or_else(|| anyhow::anyhow!("unknown method: {method}"))?;
    let mut params = params;
    // A self-budgeting method sizes itself, so an explicit response cap is
    // handed down as its own `max_bytes` rather than cutting its output after
    // the fact (which would leave its continuation offsets pointing past
    // dropped elements).
    let mut budget = None;
    if spec.sizing == Sizing::SelfBudgeting
        && let (Some(cap), Some(obj)) = (max_response_bytes, params.as_object_mut())
    {
        let b = obj
            .get("max_bytes")
            .and_then(Value::as_u64)
            .map_or(cap, |b| b as usize);
        obj.insert("max_bytes".into(), json!(b));
        budget = Some((cap, b));
    }
    let mut value = (spec.run)(indexer, params.clone()).map(hoist_symbol_run_metadata)?;
    // That budget covers only the paginated payload, so when the whole
    // response still overshoots the cap, re-run with the budget reduced by the
    // overshoot; the generic pass below stays as the last-resort backstop.
    if let Some((cap, mut b)) = budget {
        for _ in 0..SELF_BUDGET_RETRIES {
            let size = value.to_string().len();
            if size <= cap || b <= 1 {
                break;
            }
            // Shrink from what the payload actually used (not the nominal
            // budget, which a quantized payload may sit well under), so each
            // retry is guaranteed to drop at least one more element.
            let used = value
                .pointer("/budget/used_bytes")
                .and_then(Value::as_u64)
                .map_or(b, |u| (u as usize).min(b));
            b = used.saturating_sub(size - cap).max(1);
            params["max_bytes"] = json!(b);
            value = (spec.run)(indexer, params.clone()).map(hoist_symbol_run_metadata)?;
        }
    }

    let elapsed = start.elapsed();
    if elapsed.as_millis() > 100 {
        eprintln!("lidx: Slow query: {} took {:?}", method, elapsed);
    }

    // Only `Capped` methods get the default cap; an explicit caller cap is
    // enforced on every method as a backstop (self-budgeting ones were also
    // handed it as their own `max_bytes` above, so it rarely fires there).
    let effective_max = match spec.sizing {
        Sizing::Capped => Some(max_response_bytes.unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)),
        Sizing::Uncapped | Sizing::SelfBudgeting => max_response_bytes,
    };
    let Some(max_bytes) = effective_max else {
        return Ok(value);
    };
    let (mut value, was_truncated, total_available) = format::truncate_response(value, max_bytes);
    if was_truncated {
        attach_truncation_report(&mut value, max_bytes, total_available);
    }
    Ok(value)
}

/// Reports a generic truncation beside the payload, never by relocating it
/// (#221): `.result.<field>` reads the same whether or not it fired. Every
/// method returns an object, so there is always somewhere to put the fields.
fn attach_truncation_report(value: &mut Value, max_bytes: usize, total_available: Option<usize>) {
    if let Some(obj) = value.as_object_mut() {
        obj.insert("truncated".into(), json!(true));
        obj.insert("max_response_bytes".into(), json!(max_bytes));
        if let Some(total) = total_available {
            obj.insert("total_available".into(), json!(total));
        }
    }
}

/// `graph_version`/`commit_sha` are properties of the indexing run, not of
/// any individual symbol, but `Symbol`'s derive stamps both onto every
/// serialized instance (issue #66). This is the single mechanism, applied
/// to every method's result here at the dispatch boundary, that removes
/// the redundant copies from every nested object that looks like a
/// `Symbol` (carries both `qualname` and `graph_version`) and hoists one
/// copy to the top level of the result.
///
/// Two cases leave a result untouched:
/// - The result is a bare array (e.g. non-empty `search`): there is
///   nowhere to hoist a field to without changing the response's top-level
///   type, so the array -- and every symbol inside it -- is left exactly
///   as `Symbol`'s derive produced it. (`top_complexity` used to be an
///   example of this too, but always returns an object now -- see its
///   handler and `tests/response_metadata_hoist.rs`.)
/// - The top level already carries its own `graph_version` (e.g.
///   `explain_symbol`'s `ExplainSymbolResult.graph_version`, stamped
///   deliberately): the nested duplicates are still stripped, but nothing
///   is inserted, so a real field is never removed or shadowed by a second
///   copy.
///
/// `graph_version` and `commit_sha` are hoisted/stripped independently.
/// Before touching anything, every nested `Symbol`-shaped object's
/// `graph_version` and `commit_sha` are each collected and compared
/// separately. A method's entries are all stamped with the same
/// `graph_version` within a single response, so that field hoists whenever
/// it's found. `commit_sha` is not as reliable: `Db::carry_forward_references`
/// bumps a carried-forward file's `graph_version` to the new one but keeps
/// its *original* `commit_sha`, so a response spanning a reindex across two
/// commits (one file unchanged, another re-parsed) can legitimately mix two
/// different `commit_sha`s while every `graph_version` still agrees. When
/// `commit_sha` disagrees like that, it is left on every nested symbol
/// instead of hoisted -- stripping it there would throw away real
/// information about which commit each symbol actually came from -- while
/// `graph_version` still hoists and strips normally.
fn hoist_symbol_run_metadata(mut value: Value) -> Value {
    let Value::Object(ref mut top) = value else {
        // Bare array (or, in principle, a scalar) result: no top level to
        // hoist onto, so leave it untouched.
        return value;
    };

    let mut graph_version: Option<Value> = None;
    let mut graph_version_consistent = true;
    let mut commit_sha: Option<Value> = None;
    let mut commit_sha_consistent = true;
    for child in top.values() {
        collect_symbol_run_metadata(
            child,
            &mut graph_version,
            &mut graph_version_consistent,
            &mut commit_sha,
            &mut commit_sha_consistent,
        );
    }

    let hoist_graph_version = graph_version_consistent && graph_version.is_some();
    let hoist_commit_sha = commit_sha_consistent && commit_sha.is_some();
    if !hoist_graph_version && !hoist_commit_sha {
        return value;
    }

    for child in top.values_mut() {
        strip_symbol_run_metadata(child, hoist_graph_version, hoist_commit_sha);
    }

    if hoist_graph_version && !top.contains_key("graph_version") {
        top.insert("graph_version".to_string(), graph_version.unwrap());
    }
    if hoist_commit_sha && !top.contains_key("commit_sha") {
        let commit_sha = commit_sha.unwrap();
        if !commit_sha.is_null() {
            top.insert("commit_sha".to_string(), commit_sha);
        }
    }

    value
}

/// Does this JSON object look like a serialized `Symbol`? `qualname` plus
/// `graph_version` is the pair the finding behind #66 singles out --
/// specific enough that no other response shape in `model.rs` collides
/// with it (`TestCoverageEntry` carries `symbol_qualname`, not `qualname`;
/// `Edge` carries `graph_version` but no `qualname` at all).
fn is_symbol_shaped(obj: &serde_json::Map<String, Value>) -> bool {
    obj.contains_key("qualname") && obj.contains_key("graph_version")
}

/// First pass: walk `value` (a child of the top-level result, never the
/// top level itself) and record `graph_version` and `commit_sha` off every
/// `Symbol`-shaped object found, tracking each field's consistency
/// separately -- a `commit_sha` disagreement must not stop `graph_version`
/// from being collected, and vice versa. Read-only -- nothing is stripped
/// here.
fn collect_symbol_run_metadata(
    value: &Value,
    graph_version: &mut Option<Value>,
    graph_version_consistent: &mut bool,
    commit_sha: &mut Option<Value>,
    commit_sha_consistent: &mut bool,
) {
    match value {
        Value::Object(obj) => {
            if is_symbol_shaped(obj) {
                let gv = obj.get("graph_version").cloned().unwrap_or(Value::Null);
                match graph_version {
                    None => *graph_version = Some(gv),
                    Some(found) if *found == gv => {}
                    Some(_) => *graph_version_consistent = false,
                }
                let cs = obj.get("commit_sha").cloned().unwrap_or(Value::Null);
                match commit_sha {
                    None => *commit_sha = Some(cs),
                    Some(found) if *found == cs => {}
                    Some(_) => *commit_sha_consistent = false,
                }
            }
            for v in obj.values() {
                collect_symbol_run_metadata(
                    v,
                    graph_version,
                    graph_version_consistent,
                    commit_sha,
                    commit_sha_consistent,
                );
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_symbol_run_metadata(
                    v,
                    graph_version,
                    graph_version_consistent,
                    commit_sha,
                    commit_sha_consistent,
                );
            }
        }
        _ => {}
    }
}

/// Second pass, run only once `hoist_symbol_run_metadata` has decided which
/// of `graph_version`/`commit_sha` are consistent enough to hoist: remove
/// just those fields from every `Symbol`-shaped object, recursively. A
/// field whose nested copies disagree is left in place.
fn strip_symbol_run_metadata(value: &mut Value, strip_graph_version: bool, strip_commit_sha: bool) {
    match value {
        Value::Object(obj) => {
            if is_symbol_shaped(obj) {
                if strip_graph_version {
                    obj.remove("graph_version");
                }
                if strip_commit_sha {
                    obj.remove("commit_sha");
                }
            }
            for v in obj.values_mut() {
                strip_symbol_run_metadata(v, strip_graph_version, strip_commit_sha);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                strip_symbol_run_metadata(v, strip_graph_version, strip_commit_sha);
            }
        }
        _ => {}
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
    use serde_json::{Value, json};

    #[test]
    fn method_list_matches_method_specs() {
        let mut specs: Vec<&str> = super::METHOD_SPECS.iter().map(|s| s.name).collect();
        let mut listed = super::METHOD_LIST.to_vec();
        specs.sort_unstable();
        listed.sort_unstable();
        assert_eq!(specs, listed);
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
            // Every schema must describe an object (or a oneOf of objects)
            let obj = schema.as_object().unwrap();
            let has_type = obj.get("type").and_then(|v| v.as_str()) == Some("object");
            let has_one_of = obj.contains_key("oneOf");
            assert!(
                has_type || has_one_of,
                "method '{}' schema has neither type:object nor oneOf: {:?}",
                method,
                obj.keys().collect::<Vec<_>>()
            );
            // Must have real content: properties or required — empty default {} is not acceptable
            let has_properties = obj.contains_key("properties");
            let has_required = obj.contains_key("required");
            assert!(
                has_properties || has_required,
                "method '{}' schema is the empty default — register it in method_param_schema",
                method
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

        // context requires "path"
        let schema = super::method_param_schema("context");
        let required = schema.get("required").and_then(|v| v.as_array());
        assert!(required.is_some(), "context should have required fields");
        let required: Vec<&str> = required
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            required.contains(&"path"),
            "context should require 'path', got: {:?}",
            required
        );
        // context should also advertise format and graph_version as optional properties
        let props = schema.get("properties").and_then(|p| p.as_object());
        assert!(props.is_some(), "context should have properties");
        let props = props.unwrap();
        assert!(
            props.contains_key("format"),
            "context schema should advertise 'format'"
        );
        assert!(
            props.contains_key("graph_version"),
            "context schema should advertise 'graph_version'"
        );
    }

    /// Issue #67: `min_resolution` carries a doc comment specifically so it
    /// shows up in the generated tool schema with a description -- the
    /// omission #60 calls out for `sections`/`max_refs` on this same
    /// method.
    #[test]
    fn explain_symbol_schema_describes_min_resolution() {
        let schema = super::method_param_schema("explain_symbol");
        let props = schema
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("explain_symbol schema should have properties");
        let min_resolution = props
            .get("min_resolution")
            .expect("explain_symbol schema should advertise 'min_resolution'");
        let description = min_resolution
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_default();
        assert!(
            !description.is_empty(),
            "min_resolution should carry a non-empty description in the generated schema, got {:?}",
            min_resolution
        );
    }

    #[test]
    fn explain_symbol_schema_documents_sections_and_max_refs() {
        // issue #69: `sections` and `max_refs` carried no doc comments, so
        // the generated tool schema exposed them with no description and
        // callers never discovered they could narrow their request.
        let schema = super::method_param_schema("explain_symbol");
        let props = schema
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("explain_symbol should have properties");

        let sections_desc = props
            .get("sections")
            .and_then(|p| p.get("description"))
            .and_then(|d| d.as_str())
            .unwrap_or_else(|| panic!("'sections' should carry a description: {:?}", props));
        for value in ["source", "callers", "callees", "tests", "implements"] {
            assert!(
                sections_desc.contains(value),
                "'sections' description should list accepted value '{}': {:?}",
                value,
                sections_desc
            );
        }
        for alias in ["dependencies", "dependents", "summary", "body"] {
            assert!(
                sections_desc.contains(alias),
                "'sections' description should list alias '{}': {:?}",
                alias,
                sections_desc
            );
        }

        let max_refs_desc = props
            .get("max_refs")
            .and_then(|p| p.get("description"))
            .and_then(|d| d.as_str())
            .unwrap_or_else(|| panic!("'max_refs' should carry a description: {:?}", props));
        assert!(
            max_refs_desc.contains("10"),
            "'max_refs' description should state its default: {:?}",
            max_refs_desc
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

    #[test]
    fn every_advertised_param_is_accepted_by_every_method() {
        use super::{Indexer, METHOD_LIST, Value, handle_method, json, method_param_schema};
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("app.py"), "def needle():\n    pass\n").unwrap();
        let db = tmp.path().join(".lidx").join(".lidx.sqlite");
        let mut indexer = Indexer::new(tmp.path().to_path_buf(), db).unwrap();
        indexer.reindex().unwrap();
        for method in METHOD_LIST {
            let schema = method_param_schema(method);
            let mut params = serde_json::Map::new();
            for key in schema["properties"].as_object().unwrap().keys() {
                params.insert(key.clone(), Value::Null);
            }
            // Required fields cannot be null.
            match *method {
                "search" => params.insert("query".into(), json!("needle")),
                "outline" | "context" => params.insert("path".into(), json!("app.py")),
                _ => None,
            };
            params.insert("max_response_bytes".into(), Value::Null);
            params.insert("max_tokens".into(), Value::Null);
            // The handler may fail for other reasons (e.g. nothing to
            // resolve); it must never reject a param its own schema lists.
            if let Err(err) = handle_method(&mut indexer, method, Value::Object(params)) {
                assert!(
                    !err.to_string().contains("unknown param"),
                    "{method} rejected an advertised param: {err}"
                );
            }
        }
    }

    fn line_error(line: &str) -> Value {
        match super::parse_request_line(line) {
            Ok(_) => panic!("expected an error for {line}"),
            Err(resp) => serde_json::to_value(resp).unwrap(),
        }
    }

    #[test]
    fn missing_method_echoes_id() {
        let resp = line_error(r#"{"id":3}"#);
        assert_eq!(resp["id"], json!(3));
        let msg = resp["error"]["message"].as_str().unwrap();
        assert!(msg.contains("missing field `method`"), "{msg}");
    }

    #[test]
    fn missing_method_without_id_uses_null_id() {
        assert_eq!(line_error(r#"{"params":{}}"#)["id"], Value::Null);
    }

    #[test]
    fn empty_array_gets_clear_message_and_null_id() {
        let resp = line_error("[]");
        assert_eq!(resp["id"], Value::Null);
        assert_eq!(resp["error"]["message"], "invalid request: empty array");
    }

    #[test]
    fn non_empty_array_and_scalars_are_rejected_with_null_id() {
        for line in [r#"[{"id":1,"method":"ping"}]"#, "42", r#""hi""#, "null"] {
            let resp = line_error(line);
            assert_eq!(resp["id"], Value::Null, "{line}");
            assert!(
                resp["error"]["message"]
                    .as_str()
                    .unwrap()
                    .starts_with("invalid request: "),
                "{line}"
            );
        }
    }

    #[test]
    fn syntax_error_keeps_invalid_request_prefix() {
        let resp = line_error("{not json");
        assert_eq!(resp["id"], Value::Null);
        assert!(
            resp["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("invalid request: ")
        );
    }

    #[test]
    fn valid_request_parses() {
        let req = super::parse_request_line(r#"{"id":2,"method":"nope"}"#)
            .ok()
            .unwrap();
        assert_eq!(req.id, json!(2));
        assert_eq!(req.method, "nope");
    }
}
