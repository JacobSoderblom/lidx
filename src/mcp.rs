use crate::indexer::Indexer;
use crate::rpc;
use crate::watch;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

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

    fn ensure_watch(&mut self, repo_root: &PathBuf, db_path: &PathBuf) -> Result<()> {
        if self.watch_config.mode == watch::WatchMode::Off {
            return Ok(());
        }
        let target = (repo_root.clone(), db_path.clone());
        if self.watch_target.as_ref() == Some(&target) {
            return Ok(());
        }
        let next = watch::start(repo_root.clone(), db_path.clone(), self.watch_config)?;
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

fn handle_message(message: Value, state: &mut State) -> Option<Value> {
    let id = message.get("id").cloned();
    let method = message.get("method").and_then(|value| value.as_str());

    let Some(method) = method else {
        return id.map(|id| jsonrpc_error(id, -32600, "invalid request"));
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
        "instructions": format!(
            "Use the {TOOL_NAME} tool to query a code index. Call method: help for full docs, examples, and parameter reference. \
\n\nSTART HERE: explain_symbol for deep symbol understanding (one call replaces 5+). \
analyze_diff for change impact. find_tests_for for test coverage. trace_flow for call chains. \
repo_map for architecture overview. gather_context for LLM-ready context. search for text search. search_rg for regex. \
\n\nOther methods: find_symbol, open_symbol, open_file, references, subgraph, neighbors, \
module_map, analyze_impact, list_edges, list_xrefs, route_refs, flow_status, \
repo_overview, repo_insights, top_complexity, top_coupling, co_changes, duplicate_groups, \
dead_symbols, unused_imports, orphan_tests, grep, suggest_qualnames, \
changed_files, index_status, reindex, onboard, reflect, changed_since, \
diagnostics_run, diagnostics_import, diagnostics_list, diagnostics_summary. \
\n\nOptional tool params: repo, db, set_default, text_mode (compact|pretty|none), include_structured. \
\n\nEdge kinds: CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, INHERITS, RPC_IMPL, RPC_CALL, RPC_ROUTE, HTTP_ROUTE, HTTP_CALL, CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE, XREF, MODULE_FILE, IMPORTS_FILE. \
Scope values: code, docs, tests, examples, all."
        ),
    })
}

fn tool_spec() -> Value {
    // Build oneOf array: one schema variant per method
    let one_of: Vec<Value> = rpc::METHOD_LIST
        .iter()
        .map(|&method| {
            let mut schema = rpc::method_param_schema(method);
            if let Some(obj) = schema.as_object_mut() {
                obj.insert("title".to_string(), json!(method));
            }
            schema
        })
        .collect();

    json!({
        "name": TOOL_NAME,
        "description": "Query the lidx code index using a method + params payload.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": rpc::METHOD_LIST,
                    "description": "lidx RPC method name."
                },
                "params": {
                    "oneOf": one_of,
                    "description": "Method parameters (object). Call with method: help for full parameter docs per method."
                },
                "repo": {
                    "type": "string",
                    "description": "Optional repo root override for this call."
                },
                "db": {
                    "type": "string",
                    "description": "Optional db path override for this call."
                },
                "set_default": {
                    "type": "boolean",
                    "description": "If true, update default repo/db for subsequent calls."
                },
                "text_mode": {
                    "type": "string",
                    "enum": ["pretty", "compact", "none"],
                    "description": "Controls textual output size in tool responses."
                },
                "include_structured": {
                    "type": "boolean",
                    "description": "If false, omit structuredContent from tool responses."
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
        return jsonrpc_error(id, -32601, "unknown tool");
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
    let (repo_root, db_path) = repo_and_db(&arguments, &state.defaults);
    let set_default = arguments
        .get("set_default")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    // Start watcher lazily on first call (or when defaults change)
    if set_default || state.watcher.is_none() {
        if let Err(err) = state.ensure_watch(&repo_root, &db_path) {
            eprintln!("watch error: {err}");
        }
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

    match rpc::handle_method(indexer, &method, call_params) {
        Ok(result) => jsonrpc_result(id, call_result_ok(result, text_mode, include_structured)),
        Err(err) => jsonrpc_result(
            id,
            call_result_error(&err.to_string(), text_mode, include_structured),
        ),
    }
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

fn repo_and_db(arguments: &Value, defaults: &Defaults) -> (PathBuf, PathBuf) {
    let repo_override = arguments
        .get("repo")
        .and_then(|value| value.as_str())
        .map(PathBuf::from);
    let has_repo_override = repo_override.is_some();
    let db_override = arguments
        .get("db")
        .and_then(|value| value.as_str())
        .map(PathBuf::from);

    let repo_root = repo_override.unwrap_or_else(|| defaults.repo_root.clone());
    let db_path = match db_override {
        Some(path) => path,
        None if has_repo_override => default_db_path(&repo_root),
        None => defaults.db_path.clone(),
    };
    (repo_root, db_path)
}

fn default_db_path(repo: &PathBuf) -> PathBuf {
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
        .unwrap_or(true)
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

    #[test]
    fn repo_and_db_defaults() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let args = json!({});
        let (repo, db) = repo_and_db(&args, &defaults);
        assert_eq!(repo, PathBuf::from("/repo"));
        assert_eq!(db, PathBuf::from("/repo/.lidx/.lidx.sqlite"));
    }

    #[test]
    fn repo_and_db_repo_override() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let args = json!({ "repo": "/other" });
        let (repo, db) = repo_and_db(&args, &defaults);
        assert_eq!(repo, PathBuf::from("/other"));
        assert_eq!(db, PathBuf::from("/other/.lidx/.lidx.sqlite"));
    }

    #[test]
    fn repo_and_db_db_override() {
        let defaults = Defaults {
            repo_root: PathBuf::from("/repo"),
            db_path: PathBuf::from("/repo/.lidx/.lidx.sqlite"),
        };
        let args = json!({ "repo": "/other", "db": "/tmp/custom.sqlite" });
        let (repo, db) = repo_and_db(&args, &defaults);
        assert_eq!(repo, PathBuf::from("/other"));
        assert_eq!(db, PathBuf::from("/tmp/custom.sqlite"));
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
        assert!(include_structured_from_args(&json!({})));
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
}
