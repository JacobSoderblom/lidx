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
            Some(jsonrpc_result(id, json!({ "tools": [tool_spec()] })))
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

/// Server instructions: deliberately tiny; the tool description carries the guide.
const INSTRUCTIONS: &str = "lidx is a code index (symbol graph, call/impact analysis). \
For questions about code structure, who calls what, or reading one symbol, use the lidx tool \
(see its description) before Grep/Read. First call in an unfamiliar repo: method orient.";

/// Tool description: when to use lidx (as jobs), then a compact reference for the core methods.
/// Param names here must match the param structs in `rpc/mod.rs`.
const TOOL_DESCRIPTION: &str = r#"Code index with a resolved symbol graph. Prefer it over Grep/Read for code-structure questions: one call returns callers, callees, source and tests. Call with {method, params}.

Jobs:
- Who calls X / where is X used / what breaks if I change X: explain_symbol (callers, callees, tests, source in one call) or analyze_impact with direction "upstream" (transitive consumers).
- How does X work / what does X call / trace a flow: trace_flow (follows calls, plus RPC/HTTP/message-bus/config links via kinds), or explain_symbol.
- Read one function/class, not the whole file: read_symbol. File skeleton: outline.
- Find where something is defined, or text/regex search: search.
- Architecture overview: orient (first call in a new repo).
- What does a diff or set of changed files affect: analyze_diff.

Symbols: pass query (plain name like "tool_spec", fuzzy) or an exact qualname.

Methods (* = required, one-of groups in brackets):
- explain_symbol {[query|qualname|id], sections:[source|callers|callees|tests|implements], max_refs} e.g. {"query":"tool_spec"}
- analyze_impact {[query|qualname|qualnames]*, direction:upstream|downstream|both, max_depth, limit} e.g. {"query":"tool_spec","direction":"upstream"}
- trace_flow {[query|start_qualname|start_id]*, end_qualname, direction, max_hops, kinds:[CALLS,RPC_IMPL,HTTP_CALL,CHANNEL_PUBLISH,CONFIG_READ,..]} e.g. {"query":"handle_tool_call","max_hops":3}
- read_symbol {[query|qualname|qualnames]*, skeleton, context_lines} e.g. {"qualname":"crate::mcp::tool_spec"}
- outline {path*, kinds, max_depth} e.g. {"path":"src/mcp.rs"}
- search {query*, limit, scope:code|docs|tests|examples|all, path, paths, languages, fixed_string, case_sensitive} e.g. {"query":"fn tool_spec","scope":"code","limit":10}
- orient {view:overview|map|modules|all, focus_query} e.g. {"view":"overview"}
- analyze_diff {diff | paths}, e.g. {"paths":["src/mcp.rs"]}
- gather_context {seeds*:[{"type":"symbol","qualname":..}|{"type":"file","path":..}|{"type":"search","query":..}], max_bytes}
- Also: context {path}, onboard, repo_map, top_complexity, dead_symbols, reindex.
Unknown params are ignored and reported in _meta.ignored_params."#;

fn tool_spec() -> Value {
    json!({
        "name": TOOL_NAME,
        "description": TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": rpc::METHOD_LIST,
                    "description": "lidx method name."
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

fn handle_tool_call(id: Value, message: &Value, state: &mut State) -> Value {
    let params = match message.get("params") {
        Some(value) => value,
        None => return jsonrpc_error(id, -32602, "missing params"),
    };
    let tool_name = params
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if tool_name != TOOL_NAME {
        return jsonrpc_error(id, -32602, "unknown tool");
    }

    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let method = arguments
        .get("method")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let method = match method {
        Some(value) => value,
        None => return jsonrpc_error(id, -32602, "missing method"),
    };
    let call_params = arguments
        .get("params")
        .cloned()
        .unwrap_or_else(|| json!({}));
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
            jsonrpc_result(id, with_ignored_params(payload, &ignored, text_mode))
        }
        Err(err) => jsonrpc_result(
            id,
            call_result_error(&err.to_string(), text_mode, include_structured),
        ),
    }
}

/// Report ignored params without touching the result's own shape: the
/// tool result gains `_meta.ignored_params` and (unless `text_mode` is
/// `none`) one extra trailing text block so a model that only reads text
/// still sees it. `structuredContent` and the first content block stay
/// exactly as for a well-formed call, and nothing is added when nothing was
/// ignored.
fn with_ignored_params(mut payload: Value, ignored: &[String], text_mode: TextMode) -> Value {
    if ignored.is_empty() {
        return payload;
    }
    payload["_meta"] = json!({ "ignored_params": ignored });
    if !matches!(text_mode, TextMode::None)
        && let Some(content) = payload["content"].as_array_mut()
    {
        content.push(json!({
            "type": "text",
            "text": format!(
                "ignored_params: {} (not accepted by this method; the call ran without them)",
                ignored.join(", ")
            )
        }));
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
        let spec = tool_spec();
        format!(
            "{}\n{}\n{}",
            init["instructions"].as_str().unwrap(),
            spec["description"].as_str().unwrap(),
            spec["inputSchema"]
        )
    }

    #[test]
    fn agent_visible_text_mentions_every_method_and_no_phantom_help() {
        let text = agent_visible_text();
        for method in rpc::METHOD_LIST {
            assert!(
                text.contains(method),
                "tool description/instructions do not mention dispatchable method '{method}'"
            );
        }
        assert!(
            !text.to_lowercase().contains("help"),
            "tool description/instructions mention a 'help' method, which is not dispatchable"
        );
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
        // The tool entry and instructions are sent on every agent turn.
        let spec = tool_spec();
        let init = initialize_result(&json!({}));
        let total = serde_json::to_string(&spec).unwrap().len()
            + init["instructions"].as_str().unwrap().len();
        assert!(
            total <= 6000,
            "tools/list entry + instructions are {total} chars, budget 6000"
        );
        assert!(
            spec["inputSchema"]["properties"]["params"]
                .get("oneOf")
                .is_none(),
            "params must stay a plain object; per-method docs live in the description"
        );
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
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        let line = desc
            .split("scope:")
            .nth(1)
            .expect("description lists search scope values");
        let list = line.split([',', ' ']).next().unwrap();
        let schema = rpc::method_param_schema("search").to_string();
        for v in ["code", "docs", "tests", "examples", "all"] {
            assert!(
                list.contains(v),
                "description should list scope {v}: {list}"
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
}
