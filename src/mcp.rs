use crate::indexer::Indexer;
use crate::rpc;
use crate::watch;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

const TOOL_NAME: &str = "lidx";

struct Defaults {
    repo_root: PathBuf,
    db_path: PathBuf,
}

#[derive(Clone, Copy)]
enum TextMode {
    None,
    Compact,
    Pretty,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct CacheKey {
    repo_root: PathBuf,
    db_path: PathBuf,
}

struct State {
    defaults: Defaults,
    indexers: HashMap<CacheKey, Indexer>,
    watch_config: watch::WatchConfig,
    watcher: Option<watch::WatchHandle>,
    watch_target: Option<(PathBuf, PathBuf)>,
}

impl State {
    fn new(defaults: Defaults, watch_config: watch::WatchConfig) -> Self {
        Self {
            defaults,
            indexers: HashMap::new(),
            watch_config,
            watcher: None,
            watch_target: None,
        }
    }

    fn set_defaults(&mut self, repo_root: PathBuf, db_path: PathBuf) {
        self.defaults = Defaults { repo_root, db_path };
    }

    fn get_indexer(&mut self, repo_root: PathBuf, db_path: PathBuf) -> Result<&mut Indexer> {
        let key = CacheKey {
            repo_root: repo_root.clone(),
            db_path: db_path.clone(),
        };
        if !self.indexers.contains_key(&key) {
            let indexer =
                Indexer::new_with_options(repo_root, db_path, self.watch_config.scan_options)?;
            self.indexers.insert(key.clone(), indexer);
        }
        Ok(self.indexers.get_mut(&key).expect("indexer cache"))
    }

    fn ensure_watch(&mut self, repo_root: &Path, db_path: &Path) -> Result<()> {
        if self.watch_config.mode == watch::WatchMode::Off {
            return Ok(());
        }
        let target = (repo_root.to_path_buf(), db_path.to_path_buf());
        if self.watch_target.as_ref() == Some(&target) {
            return Ok(());
        }
        let next = watch::start(
            repo_root.to_path_buf(),
            db_path.to_path_buf(),
            self.watch_config,
        )?;
        if let Some(handle) = self.watcher.take() {
            handle.stop();
        }
        self.watcher = next;
        self.watch_target = Some(target);
        Ok(())
    }
}

pub fn serve(repo_root: PathBuf, db_path: PathBuf, watch_config: watch::WatchConfig) -> Result<()> {
    let defaults = Defaults {
        repo_root: repo_root.clone(),
        db_path: db_path.clone(),
    };
    let mut state = State::new(defaults, watch_config);

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

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(value) => handle_message(value, &mut state),
            Err(err) => Some(jsonrpc_error(
                Value::Null,
                -32700,
                &format!("parse error: {err}"),
            )),
        };

        if let Some(payload) = response {
            writeln!(stdout, "{}", serde_json::to_string(&payload)?)?;
            stdout.flush()?;
        }
    }

    Ok(())
}

/// One input line yields at most one output line: a batch yields a single
/// response array, and only notifications (or an all-notification batch)
/// yield nothing.
fn handle_message(message: Value, state: &mut State) -> Option<Value> {
    match message {
        Value::Array(items) if items.is_empty() => Some(jsonrpc_error(
            Value::Null,
            -32600,
            "invalid request: empty batch",
        )),
        Value::Array(items) => {
            let responses: Vec<Value> = items
                .into_iter()
                .filter_map(|item| handle_single_message(item, state))
                .collect();
            (!responses.is_empty()).then_some(Value::Array(responses))
        }
        other => handle_single_message(other, state),
    }
}

fn handle_single_message(message: Value, state: &mut State) -> Option<Value> {
    let id = message.get("id").cloned();
    let method = message.get("method").and_then(|value| value.as_str());

    let Some(method) = method else {
        // No method field: this is an invalid request (not a notification)
        // Return error with id if present, or id: null if not
        let response_id = id.unwrap_or(Value::Null);
        return Some(jsonrpc_error(response_id, -32600, "invalid request"));
    };

    match method {
        "initialize" => {
            let id = id?;
            Some(jsonrpc_result(id, initialize_result(&message)))
        }
        "notifications/initialized" => None,
        "ping" => id.map(|id| jsonrpc_result(id, json!({}))),
        "tools/list" => {
            let id = id?;
            Some(jsonrpc_result(id, json!({ "tools": tool_specs() })))
        }
        "tools/call" => {
            let id = id?;
            Some(handle_tool_call(id, &message, state))
        }
        "resources/list" => id.map(|id| jsonrpc_result(id, json!({ "resources": [] }))),
        "resources/templates/list" => {
            id.map(|id| jsonrpc_result(id, json!({ "resourceTemplates": [] })))
        }
        "prompts/list" => id.map(|id| jsonrpc_result(id, json!({ "prompts": [] }))),
        "roots/list" => id.map(|id| jsonrpc_result(id, json!({ "roots": [] }))),
        _ => id.map(|id| jsonrpc_error(id, -32601, "method not found")),
    }
}

fn initialize_result(message: &Value) -> Value {
    let protocol = message
        .get("params")
        .and_then(|params| params.get("protocolVersion"))
        .cloned()
        .unwrap_or_else(|| Value::String("2024-11-05".to_string()));
    json!({
        "protocolVersion": protocol,
        "capabilities": { "tools": {} },
        "serverInfo": {
            "name": "lidx",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "instructions": INSTRUCTIONS,
    })
}

/// Server instructions: deliberately tiny; each tool description says when to use it.
const INSTRUCTIONS: &str = "lidx is a code index (symbol graph). For who-calls/what-breaks, \
reading one function, file outlines and symbol search, use the lidx_* tools before Grep/Read; \
the lidx tool covers the rest (orient, analyze_impact, analyze_diff, gather_context).";

/// Methods only reachable through the generic `lidx` tool: everything no job tool routes to.
fn generic_methods() -> Vec<&'static str> {
    rpc::METHOD_LIST
        .iter()
        .copied()
        .filter(|m| !JOB_TOOLS.iter().any(|t| t.method == *m))
        .collect()
}

/// Arguments handled by the MCP layer itself, never forwarded as method params.
const RESERVED_ARGS: &[&str] = &[
    "repo",
    "db",
    "text_mode",
    "include_structured",
    "set_default",
];

/// Generic tool description. Param names must match the param structs in `rpc/mod.rs`.
const TOOL_DESCRIPTION: &str = r#"Code index, everything the lidx_* tools do not cover. Call with {method, params}.
- orient {view:overview|map|modules|all, focus_query}: architecture overview; first call in a new repo.
- analyze_impact {query|qualname|qualnames, direction:upstream|downstream|both, max_depth, limit}: transitive blast radius of changing a symbol.
- analyze_diff {diff | paths}: what a diff or changed files affect.
- gather_context {seeds:[{"type":"symbol","qualname":..}|{"type":"file","path":..}|{"type":"search","query":..}], max_bytes}: LLM-ready context bundle.
- Also: context {path}, onboard, repo_map, top_complexity, dead_symbols, reindex.
- help {method?}: full param schema of any method (including the lidx_* ones); no method lists them all.
Unknown params are ignored and reported in _meta.ignored_params.
lidx_* tools accept any param listed by help {method}, not only those in their schema."#;

/// A job-named tool: a small typed schema routed to one RPC method. Property
/// types, enums and descriptions are picked out of the method's real params
/// schema so they cannot drift; any other param the method accepts is still
/// forwarded (`additionalProperties: true`, see `help {method}`).
struct JobTool {
    name: &'static str,
    method: &'static str,
    description: &'static str,
    /// Properties advertised up front, picked from the method's schema.
    picks: &'static [&'static str],
    /// Replacement descriptions for picked properties (name, text).
    describe: &'static [(&'static str, &'static str)],
    required: &'static [&'static str],
    /// Params always sent (override the caller), as a JSON object literal.
    fixed: &'static str,
    /// Params sent when the caller omits them, as a JSON object literal.
    defaults: &'static str,
}

impl JobTool {
    fn properties(&self) -> Value {
        let schema = rpc::method_param_schema(self.method);
        let mut out = serde_json::Map::new();
        for name in self.picks {
            let mut prop = schema["properties"]
                .get(*name)
                .cloned()
                .unwrap_or_else(|| panic!("{} has no param '{name}'", self.method));
            let text = self.describe.iter().find(|(n, _)| n == name);
            if let (Some((_, text)), Some(obj)) = (text, prop.as_object_mut()) {
                obj.insert("description".into(), json!(text));
            }
            out.insert((*name).to_string(), prop);
        }
        Value::Object(out)
    }

    fn fixed(&self) -> Value {
        serde_json::from_str(self.fixed).expect("fixed is a JSON object")
    }

    fn defaults(&self) -> Value {
        serde_json::from_str(self.defaults).expect("defaults is a JSON object")
    }
}

const JOB_TOOLS: &[JobTool] = &[
    JobTool {
        name: "lidx_find_callers",
        method: "explain_symbol",
        description: "List every caller of a function/method/class from the resolved call graph, with caller file and call-site line (evidence_line). Use instead of Grep for \"who calls X\", \"where is X used\", \"what breaks if I change X\". Capped by max_refs (default 50); callers_total is the true count.",
        picks: &["query", "max_refs"],
        describe: &[
            ("query", "Symbol name (fuzzy) or exact qualname."),
            ("max_refs", "Max callers returned (default 50)."),
        ],
        required: &["query"],
        fixed: r#"{"sections": ["callers"]}"#,
        defaults: r#"{"max_refs": 50}"#,
    },
    JobTool {
        name: "lidx_explain_symbol",
        method: "explain_symbol",
        description: "One symbol in a single call: source, callers, callees, tests, implementations. Use instead of several Grep/Read calls for what X does, who uses it, what tests cover it.",
        picks: &["query", "sections", "max_refs"],
        describe: &[
            ("query", "Symbol name (fuzzy) or exact qualname."),
            (
                "sections",
                "source, callers, callees, tests, implements. Default: all.",
            ),
            ("max_refs", "Max refs per section (default 10)."),
        ],
        required: &["query"],
        fixed: "{}",
        defaults: "{}",
    },
    JobTool {
        name: "lidx_trace_flow",
        method: "trace_flow",
        description: "Trace the call chain from a symbol, downstream (what it calls) or upstream (what calls it), across files and RPC, HTTP, message-bus and config links. Use instead of manually following calls with Grep/Read.",
        picks: &["query", "direction", "max_hops", "kinds", "end_qualname"],
        describe: &[
            ("query", "Start symbol: name (fuzzy) or exact qualname."),
            ("direction", "downstream (default) or upstream."),
            ("max_hops", "Default 5, max 10."),
            (
                "kinds",
                "Edge kinds, e.g. CALLS, RPC_IMPL, HTTP_CALL, CHANNEL_PUBLISH, CONFIG_READ.",
            ),
            ("end_qualname", "Only paths ending at this symbol."),
        ],
        required: &["query"],
        fixed: "{}",
        defaults: "{}",
    },
    JobTool {
        name: "lidx_read_symbol",
        method: "read_symbol",
        description: "Read the exact source of one or several functions/classes without reading the whole file. Use instead of Read on a big file when you know the symbol name. Give query, qualname or qualnames.",
        picks: &["query", "qualnames", "skeleton", "context_lines"],
        describe: &[
            ("query", "Symbol name (fuzzy) or exact qualname."),
            ("qualnames", "Read several symbols in one call."),
            ("skeleton", "For a class/module: member signatures only."),
            ("context_lines", "Extra surrounding lines."),
        ],
        required: &[],
        fixed: "{}",
        defaults: "{}",
    },
    JobTool {
        name: "lidx_outline",
        method: "outline",
        description: "Skeleton of one file: classes, functions and methods with line ranges. Use instead of Read to see what a file contains first.",
        picks: &["path", "kinds", "max_depth"],
        describe: &[
            ("path", "Repo-relative file path."),
            ("kinds", "e.g. function, class."),
            ("max_depth", "0 = top level only."),
        ],
        required: &["path"],
        fixed: "{}",
        defaults: "{}",
    },
    JobTool {
        name: "lidx_search",
        method: "search",
        description: "Find where something is defined or mentioned: regex or literal text search that names the enclosing symbol and filters by code/tests/docs. For who-calls questions use lidx_find_callers.",
        picks: &[
            "query",
            "limit",
            "scope",
            "path",
            "paths",
            "languages",
            "fixed_string",
            "case_sensitive",
        ],
        describe: &[
            ("query", "Regex, or literal with fixed_string."),
            ("path", "Restrict to a path prefix."),
        ],
        required: &["query"],
        fixed: "{}",
        defaults: "{}",
    },
];

fn job_tool(name: &str) -> Option<&'static JobTool> {
    JOB_TOOLS.iter().find(|t| t.name == name)
}

fn tool_specs() -> Vec<Value> {
    let mut tools: Vec<Value> = JOB_TOOLS
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": {
                    "type": "object",
                    "properties": t.properties(),
                    "required": t.required,
                    "additionalProperties": true,
                }
            })
        })
        .collect();
    tools.push(tool_spec());
    tools
}

fn tool_spec() -> Value {
    json!({
        "name": TOOL_NAME,
        "description": TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": generic_methods(),
                },
                "params": {
                    "type": "object",
                    "description": "Method parameters; see tool description."
                }
            },
            "required": ["method"]
        }
    })
}

/// Resolve a tools/call into (method, method params), or a JSON-RPC error message.
fn route_tool_call(tool_name: &str, arguments: &Value) -> Result<(String, Value), &'static str> {
    if let Some(tool) = job_tool(tool_name) {
        let mut params = serde_json::Map::new();
        if let Some(obj) = arguments.as_object() {
            for (k, v) in obj {
                if !RESERVED_ARGS.contains(&k.as_str()) {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(defaults) = tool.defaults().as_object() {
            for (k, v) in defaults {
                params.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        if let Some(fixed) = tool.fixed().as_object() {
            for (k, v) in fixed {
                params.insert(k.clone(), v.clone());
            }
        }
        return Ok((tool.method.to_string(), Value::Object(params)));
    }
    if tool_name != TOOL_NAME {
        return Err("unknown tool");
    }
    let method = arguments
        .get("method")
        .and_then(|value| value.as_str())
        .ok_or("missing method")?;
    let call_params = arguments
        .get("params")
        .cloned()
        .unwrap_or_else(|| json!({}));
    Ok((method.to_string(), call_params))
}

fn handle_tool_call(id: Value, message: &Value, state: &mut State) -> Value {
    let params = match message.get("params") {
        Some(value) => value,
        None => return jsonrpc_error(id, -32602, "missing params"),
    };
    let tool_name = params
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let (method, call_params) = match route_tool_call(tool_name, &arguments) {
        Ok(value) => value,
        Err(msg) => return jsonrpc_error(id, -32602, msg),
    };
    let text_mode = text_mode_from_args(&arguments);
    let include_structured = include_structured_from_args(&arguments);
    let (repo_root, db_path) = match repo_and_db(&arguments, &state.defaults) {
        Ok(value) => value,
        Err(err) => {
            return jsonrpc_result(
                id,
                call_result_error(&format!("{err:#}"), text_mode, include_structured),
            );
        }
    };
    let set_default = arguments
        .get("set_default")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    // Start watcher lazily on first call (or when defaults change)
    if (set_default || state.watcher.is_none())
        && let Err(err) = state.ensure_watch(&repo_root, &db_path)
    {
        eprintln!("watch error: {err}");
    }
    if set_default {
        state.set_defaults(repo_root.clone(), db_path.clone());
    }

    let indexer = match state.get_indexer(repo_root, db_path) {
        Ok(indexer) => indexer,
        Err(err) => {
            return jsonrpc_result(
                id,
                call_result_error(&err.to_string(), text_mode, include_structured),
            );
        }
    };

    // Unknown params must not cost an agent its turn: run the method and
    // report what was ignored instead of failing.
    match rpc::handle_method_lenient(indexer, &method, call_params) {
        Ok((result, ignored)) => {
            let payload = call_result_ok(result, text_mode, include_structured);
            let freshness = indexer.freshness();
            jsonrpc_result(
                id,
                with_meta(payload, &ignored, freshness.as_ref(), text_mode),
            )
        }
        Err(err) => jsonrpc_result(
            id,
            call_result_error(&err.to_string(), text_mode, include_structured),
        ),
    }
}

/// Report ignored params and a stale index without touching the result's own
/// shape: the tool result gains `_meta.ignored_params` and/or the
/// `_meta.stale` freshness fields and (unless `text_mode` is `none`) one extra
/// trailing text block per finding so a model that only reads text still sees
/// it. `structuredContent` and the first content block stay exactly as for a
/// well-formed call, and nothing is added when nothing was ignored and the
/// index is fresh.
fn with_meta(
    mut payload: Value,
    ignored: &[String],
    freshness: Option<&crate::freshness::Freshness>,
    text_mode: TextMode,
) -> Value {
    let mut meta = serde_json::Map::new();
    let mut lines = Vec::new();
    if !ignored.is_empty() {
        meta.insert("ignored_params".into(), json!(ignored));
        lines.push(format!(
            "ignored_params: {} (not accepted by this method; the call ran without them)",
            ignored.join(", ")
        ));
    }
    if let Some(fresh) = freshness
        && let Some(Value::Object(fields)) = fresh.stale_meta()
    {
        meta.extend(fields);
        lines.extend(fresh.warning_line());
    }
    if meta.is_empty() {
        return payload;
    }
    payload["_meta"] = Value::Object(meta);
    if !matches!(text_mode, TextMode::None)
        && let Some(content) = payload["content"].as_array_mut()
    {
        for text in lines {
            content.push(json!({ "type": "text", "text": text }));
        }
    }
    payload
}

const MAX_RESPONSE_BYTES: usize = 512_000; // 500KB hard cap

fn call_result_ok(result: Value, text_mode: TextMode, include_structured: bool) -> Value {
    let content = match format_text(&result, text_mode) {
        Some(text) if text.len() > MAX_RESPONSE_BYTES => vec![json!({
            "type": "text",
            "text": format!(
                "Response too large ({} bytes, {} est. tokens). Reduce limit or use a more specific query.",
                text.len(),
                text.len() / 4
            )
        })],
        Some(text) => vec![json!({ "type": "text", "text": text })],
        None => Vec::new(),
    };
    let mut payload = json!({
        "content": content,
        "isError": false
    });
    if include_structured {
        // Ensure structuredContent is always an object
        payload["structuredContent"] = ensure_object_response(result);
    }
    payload
}

fn ensure_object_response(result: Value) -> Value {
    if result.is_array() {
        json!({ "items": result })
    } else {
        result
    }
}

fn call_result_error(message: &str, text_mode: TextMode, _include_structured: bool) -> Value {
    let content = match text_mode {
        TextMode::None => Vec::new(),
        _ => vec![json!({ "type": "text", "text": message })],
    };
    json!({
        "content": content,
        "isError": true
    })
}

fn format_text(value: &Value, text_mode: TextMode) -> Option<String> {
    match text_mode {
        TextMode::None => None,
        TextMode::Compact => serde_json::to_string(value).ok(),
        TextMode::Pretty => serde_json::to_string_pretty(value).ok(),
    }
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn jsonrpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn repo_and_db(arguments: &Value, defaults: &Defaults) -> anyhow::Result<(PathBuf, PathBuf)> {
    let repo_override = arguments
        .get("repo")
        .and_then(|value| value.as_str())
        .map(PathBuf::from);
    let has_repo_override = repo_override.is_some();
    let db_override = arguments
        .get("db")
        .and_then(|value| value.as_str())
        .map(PathBuf::from);

    // A request-supplied repo is untrusted input: validate before any DB path
    // is derived so `Db::new` can never create directories for a bad repo.
    if let Some(repo) = &repo_override {
        crate::indexer::validate_repo_root(repo)?;
    }
    let repo_root = repo_override.unwrap_or_else(|| defaults.repo_root.clone());
    let db_path = match db_override {
        Some(path) => path,
        None if has_repo_override => default_db_path(&repo_root),
        None => defaults.db_path.clone(),
    };
    Ok((repo_root, db_path))
}

fn default_db_path(repo: &Path) -> PathBuf {
    repo.join(".lidx").join(".lidx.sqlite")
}

fn text_mode_from_args(arguments: &Value) -> TextMode {
    match arguments.get("text_mode").and_then(|value| value.as_str()) {
        Some("none") => TextMode::None,
        Some("compact") => TextMode::Compact,
        Some("pretty") => TextMode::Pretty,
        _ => TextMode::Compact,
    }
}

fn include_structured_from_args(arguments: &Value) -> bool {
    arguments
        .get("include_structured")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("lidx-mcp-{label}-{nanos}-{counter}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Everything the agent sees up front about the tool.
    fn agent_visible_text() -> String {
        let init = initialize_result(&json!({}));
        format!(
            "{}\n{}",
            init["instructions"].as_str().unwrap(),
            serde_json::to_string(&tool_specs()).unwrap()
        )
    }

    #[test]
    fn agent_visible_text_mentions_every_method() {
        let text = agent_visible_text();
        for method in rpc::METHOD_LIST {
            assert!(
                text.contains(method),
                "tool description/instructions do not mention dispatchable method '{method}'"
            );
        }
    }

    #[test]
    fn tool_description_param_names_exist_in_schemas() {
        // Spot-check the guide against the real param structs: every core
        // method's documented params must be properties of its schema.
        let guide: &[(&str, &[&str])] = &[
            (
                "explain_symbol",
                &["query", "qualname", "id", "sections", "max_refs"],
            ),
            (
                "analyze_impact",
                &[
                    "query",
                    "qualname",
                    "qualnames",
                    "direction",
                    "max_depth",
                    "limit",
                ],
            ),
            (
                "trace_flow",
                &[
                    "query",
                    "start_qualname",
                    "start_id",
                    "end_qualname",
                    "direction",
                    "max_hops",
                    "kinds",
                ],
            ),
            (
                "read_symbol",
                &[
                    "query",
                    "qualname",
                    "qualnames",
                    "skeleton",
                    "context_lines",
                ],
            ),
            ("outline", &["path", "kinds", "max_depth"]),
            (
                "search",
                &[
                    "query",
                    "limit",
                    "scope",
                    "path",
                    "paths",
                    "languages",
                    "fixed_string",
                    "case_sensitive",
                ],
            ),
            ("orient", &["view", "focus_query"]),
            ("analyze_diff", &["diff", "paths"]),
            ("gather_context", &["seeds", "max_bytes"]),
            ("context", &["path"]),
        ];
        for (method, params) in guide {
            let schema = rpc::method_param_schema(method);
            for p in *params {
                assert!(
                    schema["properties"].get(*p).is_some(),
                    "guide documents {method}.{p}, which is not in its schema"
                );
            }
        }
    }

    #[test]
    fn tool_spec_stays_within_token_budget() {
        // All tool entries and the instructions are sent on every agent turn.
        let specs = tool_specs();
        let init = initialize_result(&json!({}));
        let total = serde_json::to_string(&specs).unwrap().len()
            + init["instructions"].as_str().unwrap().len();
        assert!(
            total <= 6700,
            "tools/list + instructions are {total} chars, budget 6700"
        );
        assert!(
            tool_spec()["inputSchema"]["properties"]["params"]
                .get("oneOf")
                .is_none(),
            "params must stay a plain object; per-method docs live in the description"
        );
    }

    #[test]
    fn tools_list_payload_stays_under_size_ceiling() {
        // main advertised 21,460 bytes (about 5,365 tokens at chars/4); the
        // issue requires at least a 50% cut. Measured 5,895 bytes; ceiling is
        // about 10% above that.
        let (mut state, _repo) = mcp_state("sizeceiling");
        let msg = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
        let resp = handle_message(msg, &mut state).unwrap();
        let bytes = serde_json::to_string(&resp["result"]).unwrap().len();
        assert!(
            bytes <= 6_450,
            "tools/list payload is {bytes} bytes, ceiling 6450"
        );
    }

    #[test]
    fn help_tool_serves_every_schema_removed_from_tools_list() {
        let (mut state, _repo) = mcp_state("help");
        let listing = call_tool(&mut state, "help", json!({}))["structuredContent"].clone();
        let names: Vec<&str> = listing["methods"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        for method in rpc::METHOD_LIST {
            assert!(names.contains(method), "help listing lacks '{method}'");
            let detail =
                call_tool(&mut state, "help", json!({"method": method}))["structuredContent"]
                    .clone();
            assert_eq!(detail["method"], *method);
            assert!(
                detail["params"]["type"] == "object",
                "help for '{method}' has no object schema: {detail}"
            );
        }
        // help output is never hoisted: full properties, nothing at top level.
        for method in rpc::METHOD_LIST {
            let detail =
                call_tool(&mut state, "help", json!({"method": method}))["structuredContent"]
                    .clone();
            let expected = rpc::method_param_schema(method);
            assert_eq!(
                detail["params"]["properties"], expected["properties"],
                "help {method} lost or changed properties"
            );
            let mut keys: Vec<&str> = detail
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(keys, ["method", "params", "summary"], "help {method}");
        }
        for method in ["explain_symbol", "analyze_impact"] {
            let detail =
                call_tool(&mut state, "help", json!({"method": method}))["structuredContent"]
                    .clone();
            assert!(
                detail["params"]["properties"]
                    .get("graph_version")
                    .is_some(),
                "help {method} dropped graph_version"
            );
        }
        // context used to have an empty schema; it must expose `path`.
        let ctx = call_tool(&mut state, "help", json!({"method": "context"}))["structuredContent"]
            .clone();
        assert!(ctx["params"]["properties"].get("path").is_some());
        // Params dropped from lidx_search's up-front schema stay discoverable.
        let search =
            call_tool(&mut state, "help", json!({"method": "search"}))["structuredContent"].clone();
        assert!(search["params"]["properties"].get("scope").is_some());
        // Unknown method names the valid ones.
        let msg = json!({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": {"name": "lidx", "arguments": {"method": "help", "params": {"method": "nope"}}}});
        let resp = handle_message(msg, &mut state).unwrap();
        assert!(resp.to_string().contains("Valid methods"), "{resp}");
    }

    #[test]
    fn every_method_has_a_summary() {
        let summaries: Vec<_> = rpc::method_summaries().collect();
        for method in rpc::METHOD_LIST {
            let (_, summary) = summaries
                .iter()
                .find(|(n, _)| n == method)
                .unwrap_or_else(|| panic!("no MethodSpec for {method}"));
            assert!(!summary.trim().is_empty(), "empty summary for {method}");
        }
    }

    #[test]
    fn job_schemas_allow_additional_props_and_point_to_help() {
        for spec in tool_specs().iter().filter(|s| s["name"] != TOOL_NAME) {
            assert_eq!(spec["inputSchema"]["additionalProperties"], true);
        }
        assert!(TOOL_DESCRIPTION.contains("accept any param listed by help"));
    }

    #[test]
    fn undeclared_params_are_honoured_by_job_tools() {
        let (mut state, repo) = mcp_state("undeclared");
        std::fs::write(
            repo.join("caller.py"),
            "from app import needle\n\n\ndef user():\n    needle()\n",
        )
        .unwrap();
        call_tool(&mut state, "reindex", json!({}));
        // start_qualname is not in lidx_trace_flow's advertised schema.
        let trace = assert_clean(
            &job_call(
                &mut state,
                "lidx_trace_flow",
                json!({"start_qualname": "caller.user", "max_hops": 2}),
            ),
            "trace_flow",
        );
        assert_eq!(trace["start"]["qualname"], "caller.user");
        // qualname is not in lidx_read_symbol's advertised schema.
        let read = assert_clean(
            &job_call(
                &mut state,
                "lidx_read_symbol",
                json!({"qualname": "caller.user"}),
            ),
            "read_symbol",
        );
        assert!(read["source"].as_str().unwrap().contains("def user"));
    }

    #[test]
    fn every_method_is_reachable_through_some_tool() {
        let mut reachable: Vec<&str> = JOB_TOOLS.iter().map(|t| t.method).collect();
        reachable.extend(generic_methods());
        for method in rpc::METHOD_LIST {
            assert!(reachable.contains(method), "no tool reaches '{method}'");
        }
        for m in generic_methods() {
            assert!(rpc::METHOD_LIST.contains(&m), "'{m}' is not dispatchable");
        }
    }

    #[test]
    fn job_tool_schema_params_exist_in_method_schema() {
        for tool in JOB_TOOLS {
            let schema = rpc::method_param_schema(tool.method);
            let props = tool.properties();
            for name in props.as_object().unwrap().keys() {
                assert!(
                    schema["properties"].get(name).is_some(),
                    "{} exposes '{name}', which {} does not accept",
                    tool.name,
                    tool.method
                );
            }
            for req in tool.required {
                assert!(
                    props.get(*req).is_some(),
                    "{} requires unknown '{req}'",
                    tool.name
                );
            }
            for extra in [tool.fixed(), tool.defaults()] {
                for name in extra.as_object().unwrap().keys() {
                    assert!(
                        schema["properties"].get(name).is_some(),
                        "{}: '{name}'",
                        tool.name
                    );
                }
            }
        }
    }

    #[test]
    fn repo_and_db_defaults() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let args = json!({});
        let (repo, db) = repo_and_db(&args, &defaults).unwrap();
        assert_eq!(repo, PathBuf::from("/repo"));
        assert_eq!(db, PathBuf::from("/repo/.lidx/.lidx.sqlite"));
    }

    #[test]
    fn repo_and_db_repo_override() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let other = temp_dir("override");
        let args = json!({ "repo": other });
        let (repo, db) = repo_and_db(&args, &defaults).unwrap();
        assert_eq!(repo, other);
        assert_eq!(db, other.join(".lidx").join(".lidx.sqlite"));
    }

    #[test]
    fn repo_and_db_db_override() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let other = temp_dir("override-db");
        let args = json!({ "repo": other, "db": "/tmp/custom.sqlite" });
        let (repo, db) = repo_and_db(&args, &defaults).unwrap();
        assert_eq!(repo, other);
        assert_eq!(db, PathBuf::from("/tmp/custom.sqlite"));
    }

    #[test]
    fn repo_and_db_rejects_missing_repo_without_creating_dirs() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let parent = temp_dir("missing-parent");
        let missing = parent.join("typo");
        let args = json!({ "repo": missing });
        let err = repo_and_db(&args, &defaults).unwrap_err();
        assert!(format!("{err:#}").contains("typo"), "{err:#}");
        assert!(!missing.exists());
    }

    #[test]
    fn text_mode_parsing() {
        assert!(matches!(text_mode_from_args(&json!({})), TextMode::Compact));
        assert!(matches!(
            text_mode_from_args(&json!({ "text_mode": "compact" })),
            TextMode::Compact
        ));
        assert!(matches!(
            text_mode_from_args(&json!({ "text_mode": "none" })),
            TextMode::None
        ));
    }

    #[test]
    fn include_structured_parsing() {
        // Issue #66: structuredContent is now opt-in, not opt-out -- a
        // client that doesn't pass `include_structured` no longer gets it,
        // halving the payload for callers who never asked for the duplicate
        // structured serialization.
        assert!(!include_structured_from_args(&json!({})));
        assert!(!include_structured_from_args(
            &json!({ "include_structured": false })
        ));
        assert!(include_structured_from_args(
            &json!({ "include_structured": true })
        ));
    }

    #[test]
    fn call_result_ok_modes() {
        let result = json!({ "a": 1 });
        let pretty = call_result_ok(result.clone(), TextMode::Pretty, true);
        let pretty_text = pretty["content"][0]["text"].as_str().unwrap();
        assert_eq!(pretty_text, serde_json::to_string_pretty(&result).unwrap());
        assert!(pretty.get("structuredContent").is_some());

        let compact = call_result_ok(result.clone(), TextMode::Compact, true);
        let compact_text = compact["content"][0]["text"].as_str().unwrap();
        assert_eq!(compact_text, serde_json::to_string(&result).unwrap());

        let none = call_result_ok(result.clone(), TextMode::None, true);
        assert!(none["content"].as_array().unwrap().is_empty());

        let no_struct = call_result_ok(result, TextMode::Compact, false);
        assert!(no_struct.get("structuredContent").is_none());
    }

    #[test]
    fn call_result_ok_default_text_mode_keeps_test_layer() {
        // Issue #231: the empty-TEST-layer explanation is part of the text
        // an MCP client reads in the default (compact) text mode.
        let result = json!({
            "affected": [],
            "test_layer": {"empty": true, "reason": "r", "next_hops": [{"method": "search"}]}
        });
        let out = call_result_ok(result, text_mode_from_args(&json!({})), false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"test_layer\""), "{text}");
        assert!(text.contains("\"next_hops\""), "{text}");
    }

    #[test]
    fn call_result_ok_omits_structured_content_by_default() {
        // Issue #66: a tool call that never sets `include_structured` --
        // the common case -- must not carry structuredContent at all, while
        // text-mode output (the caller's actual signal) is untouched.
        let result = json!({ "a": 1 });
        let include_structured = include_structured_from_args(&json!({}));

        let default_pretty = call_result_ok(result.clone(), TextMode::Pretty, include_structured);
        assert!(default_pretty.get("structuredContent").is_none());
        assert_eq!(
            default_pretty["content"][0]["text"].as_str().unwrap(),
            serde_json::to_string_pretty(&result).unwrap()
        );

        let default_compact = call_result_ok(result.clone(), TextMode::Compact, include_structured);
        assert!(default_compact.get("structuredContent").is_none());
        assert_eq!(
            default_compact["content"][0]["text"].as_str().unwrap(),
            serde_json::to_string(&result).unwrap()
        );
    }

    #[test]
    fn state_caches_indexer() {
        let repo_root = temp_dir("repo");
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        let mut state = State::new(
            Defaults {
                repo_root: repo_root.clone(),
                db_path: db_path.clone(),
            },
            watch::WatchConfig::default(),
        );
        let _ = state
            .get_indexer(repo_root.clone(), db_path.clone())
            .unwrap();
        let _ = state.get_indexer(repo_root, db_path).unwrap();
        assert_eq!(state.indexers.len(), 1);
    }

    fn mcp_state(label: &str) -> (State, PathBuf) {
        let repo = temp_dir(label);
        std::fs::write(repo.join("app.py"), "def needle():\n    pass\n").unwrap();
        let defaults = Defaults {
            repo_root: repo.clone(),
            db_path: default_db_path(&repo),
        };
        let watch_config = watch::WatchConfig::new(watch::WatchMode::Off, 0, 0, 0, false);
        (State::new(defaults, watch_config), repo)
    }

    fn call_tool(state: &mut State, method: &str, params: Value) -> Value {
        let msg = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "lidx", "arguments": {
                "method": method, "params": params, "include_structured": true
            }}
        });
        handle_message(msg, state).unwrap()["result"].clone()
    }

    #[test]
    fn tools_list_advertises_search_scope() {
        let (mut state, _repo) = mcp_state("toolslist");
        let msg = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
        let resp = handle_message(msg, &mut state).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let search = tools.iter().find(|t| t["name"] == "lidx_search").unwrap();
        let list = search["inputSchema"]["properties"]["scope"]["enum"].to_string();
        let schema = rpc::method_param_schema("search").to_string();
        for v in ["code", "docs", "tests", "examples", "all"] {
            assert!(
                list.contains(v),
                "lidx_search should list scope {v}: {list}"
            );
            assert!(schema.contains(v), "search schema should accept {v}");
        }
    }

    fn full_response(state: &mut State, method: &str, params: Value) -> Value {
        let msg = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "lidx", "arguments": {
                "method": method, "params": params, "include_structured": true
            }}
        });
        handle_message(msg, state).unwrap()
    }

    #[test]
    fn well_formed_call_response_is_unchanged() {
        let (mut state, _repo) = mcp_state("shape");
        let resp = full_response(&mut state, "search", json!({"query": "needle"}));
        let hit = json!({
            "line": 1,
            "line_text": "def needle():",
            "next_hops": [{
                "description": "Outline app.py",
                "method": "outline",
                "params": {"path": "app.py"}
            }],
            "path": "app.py"
        });
        let expected = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "content": [{"type": "text", "text": json!({"results": [hit.clone()]}).to_string()}],
                "isError": false,
                "structuredContent": {"results": [hit]}
            }
        });
        assert_eq!(resp, expected);
    }

    #[test]
    fn stale_index_adds_meta_and_one_trailing_warning_line() {
        let (mut state, repo) = mcp_state("stale");
        full_response(&mut state, "reindex", json!({}));
        let fresh = full_response(&mut state, "search", json!({"query": "needle"}));
        assert!(fresh["result"].get("_meta").is_none(), "{fresh}");
        assert_eq!(fresh["result"]["content"].as_array().unwrap().len(), 1);

        std::fs::write(repo.join("app.py"), "def needle():\n    return 1\n").unwrap();
        // Freshness is cached for a second; drop the cached fresh result.
        state
            .get_indexer(repo.clone(), default_db_path(&repo))
            .unwrap()
            .invalidate_freshness();
        let stale = full_response(&mut state, "search", json!({"query": "needle"}));
        let result = &stale["result"];
        assert_eq!(result["_meta"]["stale"]["changed_files"], 1, "{result}");
        assert_eq!(result["_meta"]["stale"]["head_moved"], false);
        assert!(result["_meta"]["index_age_s"].is_i64());
        let blocks = result["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            blocks[1]["text"],
            "\u{26a0} index stale: 1 changed file \u{2014} run reindex"
        );
        // The result itself is unchanged.
        assert_eq!(blocks[0], fresh["result"]["content"][0]);
        assert_eq!(
            result["structuredContent"],
            fresh["result"]["structuredContent"]
        );
    }

    #[test]
    fn stray_param_on_list_result_keeps_shape_and_reports_ignored() {
        let (mut state, _repo) = mcp_state("stray");
        let clean = full_response(&mut state, "search", json!({"query": "needle"}));
        let stray = full_response(
            &mut state,
            "search",
            json!({"query": "needle", "bogus_param": 1}),
        );
        let (clean, stray) = (&clean["result"], &stray["result"]);
        assert_eq!(stray["isError"], false, "{stray}");
        // The result itself is untouched: the same `{results}` object in the
        // first text block and the same structuredContent.
        assert_eq!(stray["content"][0], clean["content"][0]);
        assert!(
            stray["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("{\"results\":[")
        );
        assert_eq!(stray["structuredContent"], clean["structuredContent"]);
        // The ignored param is reported beside it.
        assert_eq!(stray["_meta"]["ignored_params"], json!(["bogus_param"]));
        let extra = stray["content"][1]["text"].as_str().unwrap();
        assert!(extra.contains("bogus_param"), "{extra}");
        assert_eq!(stray["content"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn stray_param_on_object_result_keeps_shape() {
        let (mut state, _repo) = mcp_state("strayobj");
        let clean = full_response(&mut state, "onboard", json!({}));
        let stray = full_response(&mut state, "onboard", json!({"bogus_param": 1}));
        let (clean, stray) = (&clean["result"], &stray["result"]);
        assert_eq!(stray["content"][0], clean["content"][0]);
        assert_eq!(stray["structuredContent"], clean["structuredContent"]);
        assert_eq!(stray["_meta"]["ignored_params"], json!(["bogus_param"]));
    }

    #[test]
    fn mcp_invalid_scope_is_error_listing_valid_values() {
        let (mut state, _repo) = mcp_state("badscope");
        let res = call_tool(
            &mut state,
            "search",
            json!({"query": "needle", "scope": "bogus"}),
        );
        assert_eq!(res["isError"], true, "{res}");
        let text = res["content"][0]["text"].as_str().unwrap();
        for v in ["code", "docs", "tests", "examples", "all"] {
            assert!(text.contains(v), "error should list '{v}': {text}");
        }
    }

    fn assert_invalid_request(resp: &Value) {
        assert_eq!(resp["id"], Value::Null);
        assert_eq!(resp["error"]["code"], -32600);
    }

    #[test]
    fn scalars_and_empty_array_produce_invalid_request_with_null_id() {
        let (mut state, _repo) = mcp_state("scalars");
        for input in [
            json!(42),
            json!("hello"),
            json!(true),
            Value::Null,
            json!([]),
        ] {
            let resp = handle_message(input.clone(), &mut state)
                .unwrap_or_else(|| panic!("{input} must not be silent"));
            assert_invalid_request(&resp);
        }
    }

    #[test]
    fn batch_of_two_valid_requests_returns_one_array_in_order() {
        let (mut state, _repo) = mcp_state("batch_valid");
        let batch = json!([
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"}
        ]);
        let resp = handle_message(batch, &mut state).unwrap();
        let arr = resp.as_array().expect("batch response is an array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[1]["id"], 2);
        assert!(arr.iter().all(|r| r["result"].is_object()));
    }

    #[test]
    fn batch_with_invalid_elements_answers_every_request() {
        let (mut state, _repo) = mcp_state("batch_mixed");
        let batch = json!([
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            {"invalid": "not a request"},
            42,
            [],
            {"jsonrpc": "2.0", "method": "ping"}
        ]);
        let resp = handle_message(batch, &mut state).unwrap();
        let arr = resp.as_array().unwrap();
        // The trailing notification is the only element without a response.
        assert_eq!(arr.len(), 4);
        assert!(arr[0]["result"].is_object());
        for bad in &arr[1..] {
            assert_invalid_request(bad);
        }
    }

    #[test]
    fn notifications_alone_or_in_a_batch_produce_no_output() {
        let (mut state, _repo) = mcp_state("notif");
        let notif = json!({"jsonrpc": "2.0", "method": "ping"});
        assert!(handle_message(notif.clone(), &mut state).is_none());
        assert!(handle_message(json!([notif.clone(), notif]), &mut state).is_none());
    }

    #[test]
    fn explicit_null_id_is_answered_but_missing_id_is_not() {
        let (mut state, _repo) = mcp_state("null_id");
        let with_null = json!({"jsonrpc": "2.0", "id": null, "method": "ping"});
        let resp = handle_message(with_null, &mut state).expect("null id is a request");
        assert_eq!(resp["id"], Value::Null);
        assert!(resp["result"].is_object());
        let missing = json!({"jsonrpc": "2.0", "method": "ping"});
        assert!(handle_message(missing, &mut state).is_none());
    }

    #[test]
    fn unknown_tool_is_invalid_params_and_unknown_method_is_not_found() {
        let (mut state, _repo) = mcp_state("codes");
        let tool = json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": {"name": "nope", "arguments": {}}});
        let resp = handle_message(tool, &mut state).unwrap();
        assert_eq!(resp["error"]["code"], -32602);
        assert_eq!(resp["id"], 6);
        let method = json!({"jsonrpc": "2.0", "id": 1, "method": "nope/nope"});
        let resp = handle_message(method, &mut state).unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[test]
    fn valid_single_request_is_byte_identical() {
        let (mut state, _repo) = mcp_state("identical");
        let msg = json!({"jsonrpc": "2.0", "id": 1, "method": "ping"});
        let resp = handle_message(msg, &mut state).unwrap();
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"id":1,"jsonrpc":"2.0","result":{}}"#
        );
    }

    fn job_call(state: &mut State, tool: &str, args: Value) -> Value {
        let msg = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": args}
        });
        handle_message(msg, state).unwrap()["result"].clone()
    }

    fn assert_clean(result: &Value, tool: &str) -> Value {
        assert_eq!(result["isError"], false, "{tool}: {result}");
        assert!(
            result.get("_meta").is_none(),
            "{tool} ignored params: {result}"
        );
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
    }

    #[test]
    fn each_job_tool_round_trips() {
        let (mut state, repo) = mcp_state("jobtools");
        std::fs::write(
            repo.join("caller.py"),
            "from app import needle\n\n\ndef user():\n    needle()\n",
        )
        .unwrap();
        let reindex = call_tool(&mut state, "reindex", json!({}));
        assert_eq!(reindex["isError"], false);

        let callers = assert_clean(
            &job_call(&mut state, "lidx_find_callers", json!({"query": "needle"})),
            "find_callers",
        );
        assert!(callers.get("callees").is_none(), "callers-only: {callers}");
        let first = &callers["callers"][0];
        assert_eq!(first["symbol"]["file_path"], "caller.py");
        assert_eq!(first["evidence_line"], 5);

        let explained = assert_clean(
            &job_call(
                &mut state,
                "lidx_explain_symbol",
                json!({"query": "needle"}),
            ),
            "explain_symbol",
        );
        assert!(explained["callers"].is_array());

        assert_clean(
            &job_call(
                &mut state,
                "lidx_trace_flow",
                json!({"query": "user", "max_hops": 2}),
            ),
            "trace_flow",
        );
        let read = assert_clean(
            &job_call(&mut state, "lidx_read_symbol", json!({"query": "needle"})),
            "read_symbol",
        );
        assert!(read["source"].as_str().unwrap().contains("def needle"));
        let outline = assert_clean(
            &job_call(&mut state, "lidx_outline", json!({"path": "app.py"})),
            "outline",
        );
        assert!(outline["entries"].is_array());
        let search = assert_clean(
            &job_call(
                &mut state,
                "lidx_search",
                json!({"query": "needle", "scope": "code"}),
            ),
            "search",
        );
        assert!(search["results"].is_array());

        // Job tools report stray params like the generic tool does.
        let stray = job_call(
            &mut state,
            "lidx_outline",
            json!({"path": "app.py", "bogus": 1}),
        );
        assert_eq!(stray["_meta"]["ignored_params"], json!(["bogus"]));
    }

    #[test]
    fn job_tool_matches_generic_tool_response() {
        let (mut state, _repo) = mcp_state("jobparity");
        let generic = call_tool(&mut state, "outline", json!({"path": "app.py"}));
        let job = job_call(&mut state, "lidx_outline", json!({"path": "app.py"}));
        assert_eq!(generic["content"], job["content"]);
    }
}
