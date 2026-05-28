use crate::db::Db;
use crate::indexer::channel::{boundary_type_for_kind, bridge_complement, is_bridge_edge_kind};
use crate::model::{Edge, Symbol, TraceHop};
use anyhow::Result;
use std::collections::{HashSet, VecDeque};

/// Direction of a BFS trace through the symbol graph.
#[derive(Debug, Clone)]
pub enum TraceDirection {
    Downstream,
    Upstream,
}

/// Configuration for a `trace_flow` traversal.
#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub max_hops: usize,
    pub max_bytes: usize,
    pub direction: TraceDirection,
    pub include_snippets: bool,
    pub allowed_kinds: Vec<String>,
    pub trace_offset: usize,
    pub compact: bool,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            max_hops: 5,
            max_bytes: 30_000,
            direction: TraceDirection::Downstream,
            include_snippets: true,
            allowed_kinds: vec![
                "CALLS".into(),
                "RPC_IMPL".into(),
                "RPC_CALL".into(),
                "XREF".into(),
                "CHANNEL_PUBLISH".into(),
                "CHANNEL_SUBSCRIBE".into(),
                "HTTP_CALL".into(),
                "HTTP_ROUTE".into(),
                "CONFIG_SOURCE".into(),
                "CONFIG_READ".into(),
                "CONFIG_BIND".into(),
            ],
            trace_offset: 0,
            compact: false,
        }
    }
}

/// Result of a `trace_flow` BFS traversal.
#[derive(Debug)]
pub struct TraceResult {
    pub start: Symbol,
    pub end: Option<Symbol>,
    pub hops: Vec<TraceHop>,
    pub paths_found: usize,
    pub reached_target: bool,
    pub truncated: bool,
    pub budget_bytes: usize,
    pub used_bytes: usize,
}

/// BFS traversal of the symbol graph from `seeds`, following edges in the
/// configured direction with bridge-edge crossing and byte budgeting.
pub fn trace_flow(
    db: &Db,
    seeds: Vec<i64>,
    end_id: Option<i64>,
    languages: Option<&[String]>,
    graph_version: i64,
    config: &TraceConfig,
) -> Result<TraceResult> {
    let start_sym = db
        .get_symbol_by_id(
            *seeds
                .first()
                .ok_or_else(|| anyhow::anyhow!("empty seeds"))?,
        )?
        .ok_or_else(|| anyhow::anyhow!("start symbol not found"))?;

    let mut trace: Vec<TraceHop> = Vec::new();
    let mut visited = HashSet::new();
    let mut queue: VecDeque<(i64, usize, String)> = VecDeque::new();

    for &sid in &seeds {
        visited.insert(sid);
        queue.push_back((sid, 0, start_sym.file_path.clone()));
    }

    let mut used_bytes: usize = 0;
    let mut truncated = false;
    let mut reached_target = false;
    let is_upstream = matches!(config.direction, TraceDirection::Upstream);

    while let Some((current_id, dist, prev_file)) = queue.pop_front() {
        if dist > config.max_hops {
            truncated = true;
            break;
        }
        if used_bytes >= config.max_bytes {
            truncated = true;
            break;
        }

        let mut edges = db.edges_for_symbol(current_id, languages, graph_version)?;

        if is_upstream && let Ok(Some(current_sym)) = db.get_symbol_by_id(current_id) {
            for kind in &config.allowed_kinds {
                let mut unresolved = db
                    .incoming_edges_by_qualname_pattern(
                        &current_sym.name,
                        kind,
                        languages,
                        graph_version,
                    )
                    .unwrap_or_default();
                edges.append(&mut unresolved);
            }
        }

        let mut bridge_targets: Vec<(String, String)> = Vec::new();

        for edge in &edges {
            if !config.allowed_kinds.contains(&edge.kind) {
                continue;
            }

            let next_id = if is_upstream {
                if edge.target_symbol_id == Some(current_id) || edge.target_symbol_id.is_none() {
                    edge.source_symbol_id
                } else {
                    continue;
                }
            } else {
                if edge.source_symbol_id != Some(current_id) {
                    continue;
                }
                edge.target_symbol_id
            };

            if let Some(ref tq) = edge.target_qualname
                && bridge_complement(&edge.kind).is_some()
            {
                bridge_targets.push((tq.clone(), edge.kind.clone()));
            }

            let next_id = match next_id {
                Some(id) => id,
                None => {
                    if let Some(ref qn) = edge.target_qualname {
                        let prev_lang = detect_language(&prev_file);
                        let same_lang = vec![prev_lang];
                        let resolved = db
                            .lookup_symbol_id_fuzzy(qn, Some(&same_lang), graph_version)
                            .ok()
                            .flatten()
                            .or_else(|| {
                                if is_bridge_edge_kind(&edge.kind) {
                                    db.lookup_symbol_id_fuzzy(qn, languages, graph_version)
                                        .ok()
                                        .flatten()
                                } else {
                                    None
                                }
                            });
                        match resolved {
                            Some(id) => id,
                            None => continue,
                        }
                    } else {
                        continue;
                    }
                }
            };

            if !visited.insert(next_id) {
                continue;
            }

            if let Ok(Some(next_sym)) = db.get_symbol_by_id(next_id) {
                let hop = build_hop(
                    &next_sym,
                    edge,
                    dist + 1,
                    &prev_file,
                    config.include_snippets,
                );

                let hop_size = estimate_hop_size(&hop, config.compact);
                let hop_idx = trace.len();
                trace.push(hop);
                if hop_idx >= config.trace_offset {
                    used_bytes += hop_size;
                    if used_bytes >= config.max_bytes {
                        truncated = true;
                        break;
                    }
                }

                if end_id == Some(next_id) {
                    reached_target = true;
                    break;
                }

                queue.push_back((next_id, dist + 1, next_sym.file_path.clone()));
            }
        }

        if !reached_target && !truncated {
            for (tq, edge_kind) in &bridge_targets {
                if let Some(complement_kinds) = bridge_complement(edge_kind) {
                    let bridged = db
                        .edges_by_target_qualname_and_kinds(
                            tq,
                            complement_kinds,
                            languages,
                            graph_version,
                        )
                        .unwrap_or_default();
                    let b_type = boundary_type_for_kind(edge_kind);
                    for bridged_edge in &bridged {
                        let Some(bridged_id) = bridged_edge.source_symbol_id else {
                            continue;
                        };
                        if !visited.insert(bridged_id) {
                            continue;
                        }
                        if let Ok(Some(bridged_sym)) = db.get_symbol_by_id(bridged_id) {
                            let prev_lang = detect_language(&prev_file);
                            let next_lang = detect_language(&bridged_sym.file_path);
                            let b_detail = build_boundary_detail(b_type, &prev_lang, &next_lang);
                            let p_context = extract_protocol_context(bridged_edge);
                            let hop = TraceHop {
                                symbol: bridged_sym.clone(),
                                edge_kind: bridged_edge.kind.clone(),
                                distance: dist + 1,
                                language: next_lang,
                                snippet: if config.include_snippets {
                                    bridged_edge.evidence_snippet.clone()
                                } else {
                                    None
                                },
                                cross_language: true,
                                boundary_type: Some(b_type.to_string()),
                                boundary_detail: Some(b_detail),
                                protocol_context: p_context,
                            };
                            let hop_size = estimate_hop_size(&hop, config.compact);
                            let hop_idx = trace.len();
                            trace.push(hop);
                            if hop_idx >= config.trace_offset {
                                used_bytes += hop_size;
                                if used_bytes >= config.max_bytes {
                                    truncated = true;
                                    break;
                                }
                            }
                            if end_id == Some(bridged_id) {
                                reached_target = true;
                                break;
                            }
                            queue.push_back((bridged_id, dist + 1, bridged_sym.file_path.clone()));
                        }
                    }
                    if reached_target || truncated {
                        break;
                    }
                }
            }
        }

        if reached_target || truncated {
            break;
        }
    }

    trace.sort_by_key(|h| h.distance);
    let trace: Vec<TraceHop> = trace.into_iter().skip(config.trace_offset).collect();

    let end_sym = if let Some(eid) = end_id {
        db.get_symbol_by_id(eid)?
    } else {
        None
    };

    let paths_found = if trace.is_empty() {
        0
    } else if end_id.is_some() {
        if reached_target { 1 } else { 0 }
    } else {
        let max_dist = trace.iter().map(|h| h.distance).max().unwrap_or(0);
        trace.iter().filter(|h| h.distance == max_dist).count()
    };

    Ok(TraceResult {
        start: start_sym,
        end: end_sym,
        hops: trace,
        paths_found,
        reached_target,
        truncated,
        budget_bytes: config.max_bytes,
        used_bytes,
    })
}

fn build_hop(
    next_sym: &Symbol,
    edge: &Edge,
    distance: usize,
    prev_file: &str,
    include_snippets: bool,
) -> TraceHop {
    let prev_lang = detect_language(prev_file);
    let next_lang = detect_language(&next_sym.file_path);
    let cross_lang = prev_lang != next_lang;

    let snippet = if include_snippets {
        edge.evidence_snippet.clone()
    } else {
        None
    };

    let (boundary_type, boundary_detail, protocol_context) = if cross_lang {
        let b_type = detect_boundary_type(&edge.kind, &prev_lang, &next_lang);
        let b_detail = build_boundary_detail(&b_type, &prev_lang, &next_lang);
        let p_context = extract_protocol_context(edge);
        (Some(b_type), Some(b_detail), p_context)
    } else {
        (None, None, None)
    };

    TraceHop {
        symbol: next_sym.clone(),
        edge_kind: edge.kind.clone(),
        distance,
        language: next_lang,
        snippet,
        cross_language: cross_lang,
        boundary_type,
        boundary_detail,
        protocol_context,
    }
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

fn detect_boundary_type(edge_kind: &str, source_lang: &str, target_lang: &str) -> String {
    match edge_kind {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => "grpc".to_string(),
        "HTTP_CALL" | "HTTP_ROUTE" => "http".to_string(),
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus".to_string(),
        "CONFIG_SOURCE" | "CONFIG_READ" => "config".to_string(),
        "XREF" if source_lang == "csharp" && target_lang == "sql" => "stored_procedure".to_string(),
        "XREF" if source_lang == "sql" && target_lang == "csharp" => "stored_procedure".to_string(),
        "XREF" => "xref".to_string(),
        _ => "other".to_string(),
    }
}

fn build_boundary_detail(boundary_type: &str, source_lang: &str, target_lang: &str) -> String {
    let source_display = source_lang
        .replace("csharp", "C#")
        .replace("javascript", "JavaScript")
        .replace("typescript", "TypeScript");
    let target_display = target_lang
        .replace("csharp", "C#")
        .replace("javascript", "JavaScript")
        .replace("typescript", "TypeScript");

    match boundary_type {
        "grpc" => format!("{} \u{2192} {} via gRPC", source_display, target_display),
        "http" => format!("{} \u{2192} {} via HTTP", source_display, target_display),
        "message_bus" => format!(
            "{} \u{2192} {} via message bus",
            source_display, target_display
        ),
        "config" => format!(
            "{} \u{2192} {} via config/env",
            source_display, target_display
        ),
        "stored_procedure" => format!(
            "{} \u{2192} {} via stored procedure",
            source_display, target_display
        ),
        "xref" => format!(
            "{} \u{2192} {} via cross-reference",
            source_display, target_display
        ),
        _ => format!("{} \u{2192} {}", source_display, target_display),
    }
}

fn extract_protocol_context(edge: &Edge) -> Option<serde_json::Value> {
    let detail_str = edge.detail.as_ref()?;
    let detail: serde_json::Value = serde_json::from_str(detail_str).ok()?;

    match edge.kind.as_str() {
        "RPC_IMPL" | "RPC_CALL" | "RPC_ROUTE" => {
            let service = detail.get("service")?.as_str()?;
            let rpc = detail.get("rpc")?.as_str()?;
            let package = detail.get("package").and_then(|p| p.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("grpc");
            Some(serde_json::json!({
                "framework": framework,
                "service": service,
                "rpc": rpc,
                "package": package,
            }))
        }
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => {
            let channel_name = detail.get("channel").and_then(|c| c.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("unknown");
            let role = detail
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("unknown");
            Some(serde_json::json!({
                "framework": framework,
                "channel": channel_name,
                "role": role,
            }))
        }
        "CONFIG_SOURCE" | "CONFIG_READ" => {
            let config_uri = detail.get("config_uri").and_then(|c| c.as_str());
            let source_type = detail
                .get("source_type")
                .and_then(|s| s.as_str())
                .unwrap_or("env");
            let role = detail
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("unknown");
            Some(serde_json::json!({
                "source_type": source_type,
                "config_uri": config_uri,
                "role": role,
            }))
        }
        "HTTP_CALL" | "HTTP_ROUTE" => {
            let method = detail.get("method").and_then(|m| m.as_str());
            let path = detail.get("path").and_then(|p| p.as_str());
            let framework = detail
                .get("framework")
                .and_then(|f| f.as_str())
                .unwrap_or("http");
            Some(serde_json::json!({
                "framework": framework,
                "method": method,
                "path": path,
            }))
        }
        _ => None,
    }
}

fn estimate_hop_size(hop: &TraceHop, compact: bool) -> usize {
    if compact {
        let mut hop_val = serde_json::to_value(hop).unwrap_or_default();
        if let Some(sym) = hop_val.get("symbol").cloned()
            && let Some(obj) = hop_val.as_object_mut()
        {
            let compact_sym = crate::rpc::compact_symbol_value(&sym);
            obj.insert("symbol".to_string(), compact_sym);
        }
        serde_json::to_string(&hop_val).unwrap_or_default().len()
    } else {
        serde_json::to_string(hop).unwrap_or_default().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::model::Edge;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    fn temp_repo_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("lidx-traversal-{label}-{nanos}-{counter}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&path, &target);
            } else {
                std::fs::copy(&path, &target).unwrap();
            }
        }
    }

    struct TempRepo {
        pub repo_root: PathBuf,
        pub db_path: PathBuf,
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.repo_root);
        }
    }

    impl TempRepo {
        fn new(fixture: &str) -> Self {
            let src = fixture_path(fixture);
            let repo_root = temp_repo_dir(fixture);
            copy_dir(&src, &repo_root);
            let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
            Self { repo_root, db_path }
        }
    }

    fn indexed_repo(fixture: &str) -> (TempRepo, Indexer) {
        let temp = TempRepo::new(fixture);
        let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        (temp, indexer)
    }

    // -- Unit tests for boundary helpers (migrated from rpc/mod.rs) --

    #[test]
    fn test_detect_language() {
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
        assert_eq!(detect_boundary_type("RPC_IMPL", "proto", "csharp"), "grpc");
        assert_eq!(detect_boundary_type("RPC_CALL", "csharp", "proto"), "grpc");
        assert_eq!(
            detect_boundary_type("XREF", "csharp", "sql"),
            "stored_procedure"
        );
        assert_eq!(
            detect_boundary_type("XREF", "sql", "csharp"),
            "stored_procedure"
        );
        assert_eq!(detect_boundary_type("XREF", "python", "csharp"), "xref");
        assert_eq!(detect_boundary_type("CALLS", "python", "python"), "other");
    }

    #[test]
    fn test_build_boundary_detail() {
        assert_eq!(
            build_boundary_detail("grpc", "proto", "csharp"),
            "proto \u{2192} C# via gRPC"
        );
        assert_eq!(
            build_boundary_detail("stored_procedure", "csharp", "sql"),
            "C# \u{2192} sql via stored procedure"
        );
        assert_eq!(
            build_boundary_detail("xref", "python", "csharp"),
            "python \u{2192} C# via cross-reference"
        );
    }

    #[test]
    fn test_extract_protocol_context() {
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

    #[test]
    fn test_detect_boundary_type_all_kinds() {
        assert_eq!(detect_boundary_type("RPC_ROUTE", "proto", "csharp"), "grpc");
        assert_eq!(
            detect_boundary_type("HTTP_CALL", "typescript", "python"),
            "http"
        );
        assert_eq!(
            detect_boundary_type("HTTP_ROUTE", "python", "typescript"),
            "http"
        );
        assert_eq!(
            detect_boundary_type("CHANNEL_PUBLISH", "python", "csharp"),
            "message_bus"
        );
        assert_eq!(
            detect_boundary_type("CHANNEL_SUBSCRIBE", "csharp", "python"),
            "message_bus"
        );
        assert_eq!(
            detect_boundary_type("CONFIG_SOURCE", "python", "typescript"),
            "config"
        );
        assert_eq!(
            detect_boundary_type("CONFIG_READ", "typescript", "python"),
            "config"
        );
    }

    #[test]
    fn test_build_boundary_detail_all_types() {
        assert_eq!(
            build_boundary_detail("http", "typescript", "python"),
            "TypeScript \u{2192} python via HTTP"
        );
        assert_eq!(
            build_boundary_detail("message_bus", "python", "csharp"),
            "python \u{2192} C# via message bus"
        );
        assert_eq!(
            build_boundary_detail("config", "javascript", "python"),
            "JavaScript \u{2192} python via config/env"
        );
        assert_eq!(
            build_boundary_detail("other", "rust", "python"),
            "rust \u{2192} python"
        );
    }

    #[test]
    fn test_extract_protocol_context_channel() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "CHANNEL_PUBLISH".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("events.user_created".to_string()),
            detail: Some(
                r#"{"framework":"rabbitmq","channel":"user_created","role":"publisher"}"#
                    .to_string(),
            ),
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

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["framework"], "rabbitmq");
        assert_eq!(ctx["channel"], "user_created");
        assert_eq!(ctx["role"], "publisher");
    }

    #[test]
    fn test_extract_protocol_context_config() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "CONFIG_READ".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("env.DATABASE_URL".to_string()),
            detail: Some(
                r#"{"source_type":"env","config_uri":"DATABASE_URL","role":"reader"}"#.to_string(),
            ),
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

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["source_type"], "env");
        assert_eq!(ctx["config_uri"], "DATABASE_URL");
        assert_eq!(ctx["role"], "reader");
    }

    #[test]
    fn test_extract_protocol_context_http() {
        let edge = Edge {
            id: 1,
            file_path: "test.ts".to_string(),
            kind: "HTTP_CALL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: Some("api.users".to_string()),
            detail: Some(
                r#"{"framework":"express","method":"GET","path":"/api/users"}"#.to_string(),
            ),
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

        let ctx = extract_protocol_context(&edge).unwrap();
        assert_eq!(ctx["framework"], "express");
        assert_eq!(ctx["method"], "GET");
        assert_eq!(ctx["path"], "/api/users");
    }

    #[test]
    fn test_extract_protocol_context_malformed_json() {
        let edge = Edge {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: None,
            detail: Some("not valid json".to_string()),
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

        assert!(extract_protocol_context(&edge).is_none());
    }

    #[test]
    fn test_extract_protocol_context_missing_required_fields() {
        let edge = Edge {
            id: 1,
            file_path: "test.cs".to_string(),
            kind: "RPC_IMPL".to_string(),
            source_symbol_id: Some(100),
            target_symbol_id: None,
            target_qualname: None,
            detail: Some(r#"{"framework":"grpc"}"#.to_string()),
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

        assert!(
            extract_protocol_context(&edge).is_none(),
            "should return None when required service/rpc fields are missing"
        );
    }

    #[test]
    fn test_detect_language_paths_with_directories() {
        assert_eq!(detect_language("src/services/api.py"), "python");
        assert_eq!(detect_language("deep/nested/path/file.ts"), "typescript");
        assert_eq!(detect_language("no_extension"), "unknown");
        assert_eq!(detect_language(""), "unknown");
    }

    // -- Integration tests for trace_flow --

    #[test]
    fn downstream_bfs_traces_calls_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(!result.hops.is_empty(), "should find downstream hops");
        for hop in &result.hops {
            assert_eq!(hop.edge_kind, "CALLS");
            assert!(hop.distance >= 1);
            assert!(hop.distance <= 5);
        }
    }

    #[test]
    fn upstream_bfs_traces_incoming_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let target = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("helper".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = vec![target.id];

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Upstream,
            allowed_kinds: vec!["CALLS".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(!result.hops.is_empty(), "should find upstream callers");
        for hop in &result.hops {
            assert!(hop.distance >= 1);
        }
    }

    #[test]
    fn bridge_edge_crossing_produces_cross_language_hops() {
        let (_temp, indexer) = indexed_repo("poly_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("GetUser".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        let cross_lang_hops: Vec<&TraceHop> =
            result.hops.iter().filter(|h| h.cross_language).collect();
        assert!(
            !cross_lang_hops.is_empty(),
            "should find cross-language hops via bridge edges"
        );
        for hop in &cross_lang_hops {
            assert!(
                hop.boundary_type.is_some(),
                "cross-language hop should have boundary_type"
            );
        }
    }

    #[test]
    fn byte_budget_truncation() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_bytes: 1,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(result.truncated, "should be truncated with 1-byte budget");
    }

    #[test]
    fn max_hops_limit_respected() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let deep_config = TraceConfig {
            max_hops: 10,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let deep = trace_flow(indexer.db(), seeds.clone(), None, None, gv, &deep_config).unwrap();

        let shallow_config = TraceConfig {
            max_hops: 1,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let shallow = trace_flow(indexer.db(), seeds, None, None, gv, &shallow_config).unwrap();

        assert!(
            shallow.hops.len() <= deep.hops.len(),
            "shallow trace should have fewer or equal hops"
        );
        let max_dist = shallow.hops.iter().map(|h| h.distance).max().unwrap_or(0);
        assert!(
            max_dist <= 2,
            "with max_hops=1, nodes at distance 1 are processed and their children \
             (distance 2) are collected but not expanded further; got max_dist={}",
            max_dist
        );
    }

    #[test]
    fn empty_trace_when_no_matching_edges() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("helper".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = vec![start.id];

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            allowed_kinds: vec!["NONEXISTENT_KIND".into()],
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        assert!(
            result.hops.is_empty(),
            "should have no hops with non-matching edge kinds"
        );
        assert_eq!(result.paths_found, 0);
    }

    #[test]
    fn trace_offset_pagination_skips_hops() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("run".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config_full = TraceConfig {
            direction: TraceDirection::Downstream,
            ..Default::default()
        };
        let full_result =
            trace_flow(indexer.db(), seeds.clone(), None, None, gv, &config_full).unwrap();

        if full_result.hops.len() > 1 {
            let config_offset = TraceConfig {
                direction: TraceDirection::Downstream,
                trace_offset: 1,
                ..Default::default()
            };
            let offset_result =
                trace_flow(indexer.db(), seeds, None, None, gv, &config_offset).unwrap();

            assert_eq!(
                offset_result.hops.len(),
                full_result.hops.len() - 1,
                "offset=1 should skip 1 hop"
            );
        }
    }

    #[test]
    fn boundary_annotations_on_cross_language_hops() {
        let (_temp, indexer) = indexed_repo("poly_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let start = crate::resolve::resolve_symbol(
            indexer.db(),
            crate::resolve::SymbolRef::Query("GetUser".into()),
            None,
            gv,
        )
        .unwrap();
        let seeds = crate::resolve::expand_seeds(indexer.db(), start.id, gv).unwrap();

        let config = TraceConfig {
            max_hops: 5,
            direction: TraceDirection::Downstream,
            ..Default::default()
        };

        let result = trace_flow(indexer.db(), seeds, None, None, gv, &config).unwrap();

        let cross_lang_hops: Vec<&TraceHop> =
            result.hops.iter().filter(|h| h.cross_language).collect();
        for hop in &cross_lang_hops {
            assert!(
                hop.boundary_type.is_some(),
                "cross-language hop should have boundary_type"
            );
            assert!(
                hop.boundary_detail.is_some(),
                "cross-language hop should have boundary_detail"
            );
        }
    }
}
