use crate::indexer::Indexer;
use crate::rpc;
use crate::watch;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const TOOL_NAME: &str = "lidx_query";

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
        db_path: db_path.clone()
    };
    let mut state = State::new(defaults, watch_config);
    let watch_repo = state.defaults.repo_root.clone();
    let watch_db = state.defaults.db_path.clone();
    let _ = state.ensure_watch(&watch_repo, &watch_db)?;


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
            "Use the {TOOL_NAME} tool with method: help, list_methods, list_languages, list_graph_versions, \
repo_overview, repo_insights, top_complexity, top_coupling, co_changes, duplicate_groups, \
dead_symbols, unused_imports, orphan_tests, \
find_symbol, suggest_qualnames, open_symbol, explain_symbol, open_file, \
neighbors, subgraph, references, list_edges, list_xrefs, route_refs, flow_status, \
search_rg, grep, search_text, search, \
analyze_diff, analyze_impact, find_tests_for, trace_flow, \
repo_map, module_map, gather_context, \
changed_files, index_status, reindex, \
diagnostics_run, diagnostics_import, diagnostics_list, diagnostics_summary. \
\n\nSTART HERE: Use explain_symbol for deep understanding of any symbol (one call replaces 5+). \
Use analyze_diff to assess impact of code changes. Use find_tests_for to find test coverage. \
Use trace_flow to follow call chains. Use repo_map for quick architecture overview. \
Use module_map for detailed architecture DAG. Use gather_context to assemble LLM-ready context with budget control. \
For text search use search (ranked fuzzy). For regex use search_rg. \
For raw edges use references. For cross-language links use list_xrefs. \
\n\nOptional arguments: repo, db, set_default, text_mode, include_structured. \
\n\nCommon params: \
explain_symbol {{id|qualname|query, max_bytes, sections (source|callers|callees|tests|implements), max_refs, languages, graph_version|as_of}}; \
analyze_diff {{diff|paths, max_depth, include_tests, include_risk, max_bytes, languages, graph_version|as_of}}; \
find_tests_for {{id|qualname|query, include_indirect, indirect_depth, limit, languages, graph_version|as_of}}; \
trace_flow {{start_id|start_qualname, end_id|end_qualname, direction (downstream|upstream), max_hops, kinds, include_snippets, graph_version|as_of}}; \
repo_map {{max_bytes, languages, path|paths, graph_version|as_of}}; \
module_map {{depth, include_edges, languages, path|paths, graph_version|as_of}}; \
gather_context {{seeds, max_bytes, depth, max_nodes, include_snippets, include_related, dry_run, languages, graph_version|as_of}}; \
find_symbol {{query, limit, languages, graph_version|as_of}}; \
suggest_qualnames {{query, limit, languages, graph_version|as_of}}; \
open_symbol {{id|qualname, include_snippet, max_snippet_bytes, include_symbol, snippet_only, graph_version|as_of}}; \
open_file {{path, start_line, end_line, max_bytes}}; \
neighbors {{id, languages, graph_version|as_of}}; \
subgraph {{start_ids|roots, depth, max_nodes, languages, kinds, exclude_kinds, resolved_only, graph_version|as_of}}; \
references {{id|qualname, direction (in|out), kinds, limit, include_symbols, include_snippet, languages, graph_version|as_of}}; \
analyze_impact {{id|qualname, enable_direct, enable_test, enable_historical, max_depth, direction, kinds, include_tests, include_paths, limit, min_confidence, languages, graph_version|as_of}}; \
list_edges {{kind|kinds, path|paths, limit, offset, languages, source_id|source_qualname, target_id|target_qualname, resolved_only, min_confidence, trace_id, event_after, event_before, include_symbols, include_snippet, graph_version|as_of}}; \
list_xrefs {{path|paths, limit, offset, languages, source_id|source_qualname, target_id|target_qualname, resolved_only, min_confidence, trace_id, event_after, event_before, include_symbols, include_snippet, graph_version|as_of}}; \
route_refs {{query, path|paths, limit, languages, include_symbols, include_snippet, graph_version|as_of}}; \
flow_status {{limit, edge_limit, include_routes, include_calls, path|paths, languages, graph_version|as_of}}; \
search_rg {{query, path|paths, limit, context_lines, include_text, include_symbol, globs, case_sensitive, fixed_string, hidden, no_ignore, follow, graph_version|as_of}}; \
grep {{query, path|paths, limit, include_text, languages, scope, exclude_generated, rank, no_ignore, context_lines, include_symbol, graph_version|as_of}}; \
search_text|search {{query, path|paths, limit, languages, scope, exclude_generated, rank, no_ignore, context_lines, include_symbol, graph_version|as_of}}; \
repo_overview {{summary, fields, languages, graph_version|as_of}}; \
repo_insights {{languages, path|paths, complexity_limit, min_complexity, duplicate_limit, duplicate_min_count, duplicate_min_loc, duplicate_per_group_limit, graph_version|as_of}}; \
top_complexity {{limit, min_complexity, languages, path|paths, graph_version|as_of}}; \
top_coupling {{limit, direction (in|out|both), languages, path|paths, graph_version|as_of}}; \
co_changes {{path|paths|qualname, limit, min_confidence, graph_version|as_of}}; \
duplicate_groups {{limit, min_count, min_loc, per_group_limit, languages, path|paths, graph_version|as_of}}; \
dead_symbols {{limit, languages, path|paths, graph_version|as_of}}; \
unused_imports {{limit, languages, path|paths, graph_version|as_of}}; \
orphan_tests {{limit, languages, path|paths, graph_version|as_of}}; \
changed_files {{languages}}; index_status {{languages, include_paths}}; reindex {{summary, fields}}; \
diagnostics_run {{tools|tool, languages, output_dir}}; diagnostics_import {{path}}; \
diagnostics_list {{limit, offset, path|paths, severity, rule_id, tool, languages}}; \
diagnostics_summary {{path|paths, severity, rule_id, tool, languages}}. \
\n\nEdge kinds: CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, INHERITS, RPC_IMPL, RPC_CALL, RPC_ROUTE, HTTP_ROUTE, HTTP_CALL, CHANNEL_PUBLISH, CHANNEL_SUBSCRIBE, XREF, MODULE_FILE, IMPORTS_FILE. \
Scope values: code, docs, tests, examples, all."
        ),
    })
}

fn tool_spec() -> Value {
    json!({
        "name": TOOL_NAME,
        "description": "Query the lidx code index using a method + params payload.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": [
                        "help",
                        "list_methods",
                        "list_languages",
                        "list_graph_versions",
                        "repo_overview",
                        "repo_insights",
                        "top_complexity",
                        "top_coupling",
                        "co_changes",
                        "duplicate_groups",
                        "dead_symbols",
                        "unused_imports",
                        "orphan_tests",
                        "find_symbol",
                        "suggest_qualnames",
                        "open_symbol",
                        "explain_symbol",
                        "open_file",
                        "neighbors",
                        "subgraph",
                        "references",
                        "list_edges",
                        "list_xrefs",
                        "route_refs",
                        "flow_status",
                        "search_rg",
                        "grep",
                        "search_text",
                        "search",
                        "analyze_diff",
                        "analyze_impact",
                        "find_tests_for",
                        "trace_flow",
                        "repo_map",
                        "module_map",
                        "gather_context",
                        "changed_files",
                        "index_status",
                        "reindex",
                        "diagnostics_run",
                        "diagnostics_import",
                        "diagnostics_list",
                        "diagnostics_summary"
                    ],
                    "description": "lidx RPC method name."
                },
                "params": {
                    "type": "object",
                    "description": "Method parameters (object). Examples: list_languages {}; list_graph_versions {{limit, offset}}; find_symbol {{query, limit, languages, graph_version|as_of}}; suggest_qualnames {{query, limit, languages, graph_version|as_of}}; open_symbol {{id|qualname, include_snippet, max_snippet_bytes, include_symbol, snippet_only, graph_version|as_of}}; explain_symbol {{id|qualname|query, max_bytes, sections (source|callers|callees|tests|implements), max_refs, languages, graph_version|as_of}}; open_file {{path, start_line, end_line, max_bytes}}; neighbors {{id, languages, graph_version|as_of}}; subgraph {{start_ids|roots, depth, max_nodes, languages, kinds, exclude_kinds, resolved_only, graph_version|as_of}}; references {{id|qualname, direction, kinds, limit, include_symbols, include_snippet, languages, graph_version|as_of}}; analyze_diff {{diff|paths, max_depth, include_tests, include_risk, max_bytes, languages, graph_version|as_of}}; analyze_impact {{id|qualname, enable_direct, enable_test, enable_historical, max_depth, direction, kinds, include_tests, include_paths, limit, min_confidence, languages, graph_version|as_of}}; find_tests_for {{id|qualname|query, include_indirect, indirect_depth, limit, languages, graph_version|as_of}}; trace_flow {{start_id|start_qualname, end_id|end_qualname, direction (downstream|upstream), max_hops, kinds, include_snippets, graph_version|as_of}}; module_map {{depth, include_edges, languages, path|paths, graph_version|as_of}}; gather_context {{seeds, max_bytes, depth, max_nodes, include_snippets, include_related, dry_run, languages, graph_version|as_of}}; list_edges {{kind|kinds, path|paths, limit, offset, languages, source_id|source_qualname, target_id|target_qualname, resolved_only, min_confidence, trace_id, event_after, event_before, include_symbols, include_snippet, graph_version|as_of}}; list_xrefs {{path|paths, limit, offset, languages, source_id|source_qualname, target_id|target_qualname, resolved_only, min_confidence, trace_id, event_after, event_before, include_symbols, include_snippet, graph_version|as_of}}; route_refs {{query, path|paths, limit, languages, include_symbols, include_snippet, graph_version|as_of}}; flow_status {{limit, edge_limit, include_routes, include_calls, path|paths, languages, graph_version|as_of}}; search_rg {{query, path|paths, limit, context_lines, include_text, include_symbol, globs, case_sensitive, fixed_string, hidden, no_ignore, follow, graph_version|as_of}}; grep {{query, path|paths, limit, include_text, languages, scope, exclude_generated, rank, no_ignore, context_lines, include_symbol, graph_version|as_of}}; search_text|search {{query, path|paths, limit, languages, scope, exclude_generated, rank, no_ignore, context_lines, include_symbol, graph_version|as_of}}; repo_overview {{summary, fields, languages, graph_version|as_of}}; repo_insights {{languages, path|paths, complexity_limit, min_complexity, duplicate_limit, duplicate_min_count, duplicate_min_loc, duplicate_per_group_limit, graph_version|as_of}}; top_complexity {{limit, min_complexity, languages, path|paths, graph_version|as_of}}; top_coupling {{limit, direction (in|out|both), languages, path|paths, graph_version|as_of}}; duplicate_groups {{limit, min_count, min_loc, per_group_limit, languages, path|paths, graph_version|as_of}}; changed_files {{languages}}; index_status {{languages, include_paths}}; reindex {{summary, fields}}; diagnostics_run {{tools|tool, languages, output_dir}}; diagnostics_import {{path}}; diagnostics_list {{limit, offset, path|paths, severity, rule_id, tool, languages}}; diagnostics_summary {{path|paths, severity, rule_id, tool, languages}}. Scope values: code, docs, tests, examples, all."
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
    if arguments
        .get("set_default")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        if let Err(err) = state.ensure_watch(&repo_root, &db_path) {
            return jsonrpc_result(
                id,
                call_result_error(
                    &format!("watch error: {err}"),
                    text_mode,
                    include_structured,
                ),
            );
        }
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
        _ => TextMode::Pretty,
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
        assert!(matches!(text_mode_from_args(&json!({})), TextMode::Pretty));
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
