use crate::indexer::channel;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::indexer::proto;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tree_sitter::{Node, Parser};

const JS_TS_EXTENSIONS: &[&str] = &["js", "jsx", "mjs", "cjs", "ts", "tsx", "mts", "cts", "d.ts"];
const HTTP_METHOD_NAMES: &[&str] = &[
    "get", "post", "put", "patch", "delete", "options", "head", "all",
];
const ROUTER_RECEIVERS: &[&str] = &["app", "router", "fastify", "server", "api", "koa"];
const GRPC_JS_RAW_METHODS: &[&str] = &[
    "makeUnaryRequest",
    "makeServerStreamRequest",
    "makeClientStreamRequest",
    "makeBidiStreamRequest",
];
const GRPC_JS_SKIP_METHODS: &[&str] = &[
    "close",
    "getChannel",
    "waitForReady",
    "makeUnaryRequest",
    "makeServerStreamRequest",
    "makeClientStreamRequest",
    "makeBidiStreamRequest",
];

#[derive(Clone, Debug)]
struct GrpcService {
    package: Option<String>,
    service: String,
}

#[derive(Clone)]
struct Context {
    module: String,
    class_stack: Vec<String>,
    fn_depth: usize,
    current_scope: String,
    route_prefix: Option<String>,
    grpc_clients: HashMap<String, GrpcService>,
}

pub struct JavascriptExtractor {
    parser: Parser,
}

pub struct TypescriptExtractor {
    parser: Parser,
}

pub struct TsxExtractor {
    parser: Parser,
}

impl JavascriptExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_javascript::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }
}

impl TypescriptExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }
}

impl TsxExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_typescript::LANGUAGE_TSX;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "index".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let mut stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if stem.ends_with(".d") {
        stem.truncate(stem.len() - 2);
    }
    if stem != "index" {
        parts.push(stem);
    }
    if parts.is_empty() {
        "index".to_string()
    } else {
        parts.join("/")
    }
}

pub fn resolve_import_file_edges(
    repo_root: &Path,
    file_rel_path: &str,
    _file_module: &str,
    edges: &mut Vec<EdgeInput>,
) {
    let mut resolved = Vec::new();
    for edge in edges.iter() {
        if edge.kind != "IMPORTS" {
            continue;
        }
        let target = match edge.target_qualname.as_deref() {
            Some(value) => value.trim(),
            None => continue,
        };
        if target.is_empty() {
            continue;
        }
        let dst_rel = match resolve_import_path(repo_root, file_rel_path, target) {
            Some(value) => value,
            None => continue,
        };
        let dst_module = module_name_from_rel_path(&dst_rel);
        resolved.push(EdgeInput {
            kind: "IMPORTS_FILE".to_string(),
            source_qualname: edge.source_qualname.clone(),
            target_qualname: Some(dst_module),
            detail: Some(
                json!({
                    "src_path": file_rel_path,
                    "dst_path": dst_rel,
                    "confidence": 1.0,
                })
                .to_string(),
            ),
            evidence_snippet: edge.evidence_snippet.clone(),
            evidence_start_line: edge.evidence_start_line,
            evidence_end_line: edge.evidence_end_line,
            ..Default::default()
        });
    }
    edges.extend(resolved);
}

fn resolve_import_path(repo_root: &Path, file_rel_path: &str, target: &str) -> Option<String> {
    let target = target
        .split(|ch| ch == '?' || ch == '#')
        .next()
        .unwrap_or(target)
        .trim();
    if target.is_empty() {
        return None;
    }
    let is_relative =
        target.starts_with("./") || target.starts_with("../") || target.starts_with('/');
    if !is_relative {
        return None;
    }
    let base_dir = Path::new(file_rel_path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let rel = if target.starts_with('/') {
        PathBuf::from(target.trim_start_matches('/'))
    } else {
        let mut rel = PathBuf::from(base_dir);
        rel.push(target);
        rel
    };
    if rel.extension().is_some() {
        if repo_root.join(&rel).is_file() {
            return Some(util::normalize_path(&rel));
        }
        return None;
    }
    for ext in JS_TS_EXTENSIONS {
        let candidate = rel.with_extension(ext);
        if repo_root.join(&candidate).is_file() {
            return Some(util::normalize_path(&candidate));
        }
    }
    for ext in JS_TS_EXTENSIONS {
        let candidate = rel.join("index").with_extension(ext);
        if repo_root.join(&candidate).is_file() {
            return Some(util::normalize_path(&candidate));
        }
    }
    None
}

fn extract_with_parser(
    parser: &mut Parser,
    source: &str,
    module_name: &str,
) -> Result<ExtractedFile> {
    let mut output = ExtractedFile::default();
    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => {
            output
                .symbols
                .push(module_symbol_fallback(module_name, source));
            return Ok(output);
        }
    };
    let root = tree.root_node();

    let module_span = span(root);
    output
        .symbols
        .push(module_symbol_with_span(module_name, module_span));
    if let Some(edge) = next_page_route_edge(module_name) {
        output.edges.push(edge);
    }
    if let Some(edge) = next_api_route_edge(module_name) {
        output.edges.push(edge);
    }
    let grpc_clients = collect_grpc_clients(root, source);
    let ctx = Context {
        module: module_name.to_string(),
        class_stack: Vec::new(),
        fn_depth: 0,
        current_scope: module_name.to_string(),
        route_prefix: None,
        grpc_clients,
    };
    walk_node(root, &ctx, source, &mut output);
    Ok(output)
}

fn collect_grpc_clients(root: Node<'_>, source: &str) -> HashMap<String, GrpcService> {
    let mut clients = HashMap::new();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node.kind() == "variable_declarator" {
            let Some(name_node) = node.child_by_field_name("name") else {
                continue;
            };
            if name_node.kind() != "identifier" {
                continue;
            }
            let Some(value_node) = node.child_by_field_name("value") else {
                continue;
            };
            let Some(service) = grpc_service_from_client_initializer(value_node, source) else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }
            clients.insert(name, service);
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            stack.push(child);
        }
    }
    clients
}

fn grpc_service_from_client_initializer(node: Node<'_>, source: &str) -> Option<GrpcService> {
    let mut current = node;
    loop {
        match current.kind() {
            "parenthesized_expression" => {
                current = current
                    .child_by_field_name("expression")
                    .or_else(|| current.named_child(0))?;
            }
            "await_expression" => {
                current = current
                    .child_by_field_name("argument")
                    .or_else(|| current.named_child(0))?;
            }
            "as_expression" | "type_assertion" | "non_null_expression" => {
                current = current
                    .child_by_field_name("expression")
                    .or_else(|| current.named_child(0))?;
            }
            _ => break,
        }
    }
    if current.kind() != "new_expression" && current.kind() != "call_expression" {
        return None;
    }
    let target_node = call_target_node(current)?;
    let raw = node_text(target_node, source);
    grpc_service_from_path(&raw)
}

fn module_symbol_with_span(module_name: &str, span: (i64, i64, i64, i64, i64, i64)) -> SymbolInput {
    let name = module_name
        .rsplit('/')
        .next()
        .unwrap_or(module_name)
        .to_string();
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span;
    SymbolInput {
        kind: "module".to_string(),
        name,
        qualname: module_name.to_string(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    }
}

fn module_symbol_fallback(module_name: &str, source: &str) -> SymbolInput {
    module_symbol_with_span(
        module_name,
        (1, 1, line_count(source), 1, 0, source.len() as i64),
    )
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "jsx_element" || node.kind() == "jsx_self_closing_element" {
        if let Some(edge) = jsx_route_edge(node, ctx, source) {
            output.edges.push(edge);
        }
    }
    if node.kind() == "call_expression" || node.kind() == "new_expression" {
        handle_call(node, ctx, source, output);
    }
    if is_nested_function_node(node.kind()) {
        return;
    }
    match node.kind() {
        "class_declaration" | "abstract_class_declaration" => {
            handle_class(node, ctx, source, output);
            return;
        }
        "function_declaration" | "generator_function_declaration" => {
            if ctx.fn_depth > 0 {
                return;
            }
            handle_function(node, ctx, source, output);
            return;
        }
        "interface_declaration" => {
            handle_interface(node, ctx, source, output);
            return;
        }
        "type_alias_declaration" => {
            handle_named_item(node, ctx, source, output, "type");
            return;
        }
        "enum_declaration" => {
            handle_named_item(node, ctx, source, output, "enum");
            return;
        }
        "lexical_declaration" | "variable_declaration" => {
            handle_variable_declaration(node, ctx, source, output);
            return;
        }
        "import_statement" | "import_declaration" => {
            handle_import(node, ctx, source, output, true);
            return;
        }
        "export_statement" | "export_declaration" => {
            handle_import(node, ctx, source, output, false);
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_class(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if ctx.fn_depth > 0 {
        return;
    }
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "class".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    });
    let parent = container_qualname(&ctx.module, &ctx.class_stack);
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    handle_class_heritage(node, &qualname, source, output);

    let mut next_ctx = ctx.clone();
    next_ctx.class_stack.push(name);
    next_ctx.current_scope = qualname.clone();
    if let Some(prefix) = controller_prefix_from_class(node, source) {
        next_ctx.route_prefix = Some(prefix);
    }
    if let Some(body) = node.child_by_field_name("body") {
        walk_class_body(body, &next_ctx, source, output);
    }
}

fn handle_class_heritage(
    node: Node<'_>,
    class_qualname: &str,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut extends_targets = Vec::new();
    if let Some(super_node) = node.child_by_field_name("superclass") {
        let base = node_text(super_node, source);
        if !base.is_empty() {
            extends_targets.push(base);
        }
    }
    extends_targets.extend(collect_clause_targets_from(node, "extends_clause", source));
    let mut seen = std::collections::HashSet::new();
    for target in extends_targets {
        if target.is_empty() || !seen.insert(target.clone()) {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "EXTENDS".to_string(),
            source_qualname: Some(class_qualname.to_string()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }

    for target in collect_clause_targets_from(node, "implements_clause", source) {
        if target.is_empty() {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "IMPLEMENTS".to_string(),
            source_qualname: Some(class_qualname.to_string()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn handle_interface(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let qualname = match handle_named_item(node, ctx, source, output, "interface") {
        Some(value) => value,
        None => return,
    };
    for target in collect_clause_targets_from(node, "extends_clause", source) {
        if target.is_empty() {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "EXTENDS".to_string(),
            source_qualname: Some(qualname.clone()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn collect_clause_targets_from(node: Node<'_>, clause_kind: &str, source: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            kind if kind == clause_kind => {
                targets.extend(clause_targets(child, source));
            }
            "class_heritage" | "heritage_clause" => {
                let mut saw_clause = false;
                let mut inner = child.walk();
                for clause in child.named_children(&mut inner) {
                    if clause.kind() == clause_kind {
                        targets.extend(clause_targets(clause, source));
                        saw_clause = true;
                    }
                }
                if !saw_clause && clause_kind == "extends_clause" {
                    if let Some(target) = class_heritage_target(child, source) {
                        targets.push(target);
                    }
                }
            }
            _ => {}
        }
    }
    targets
}

fn class_heritage_target(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "extends_clause" | "implements_clause" => continue,
            _ => {
                let name = node_text(child, source);
                if !name.is_empty() {
                    return Some(name);
                }
            }
        }
    }
    None
}

fn clause_targets(node: Node<'_>, source: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let kind = child.kind();
        if kind == "type_arguments" || kind == "type_parameters" {
            continue;
        }
        let name = node_text(child, source);
        if !name.is_empty() {
            targets.push(name);
        }
    }
    targets
}

fn walk_class_body(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "method_definition" {
            handle_method(child, ctx, source, output);
        }
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    for edge in grpc_impl_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    let Some(target_node) = call_target_node(node) else {
        return;
    };
    let raw = node_text(target_node, source);
    if raw.is_empty() {
        return;
    }
    let target = resolve_call_target(&raw, ctx);
    let detail = if target.is_some() { None } else { Some(raw) };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "CALLS".to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: target,
        detail,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if is_http_client_call(node, source) {
        return edges;
    }
    if let Some(edge) = express_direct_route_edge(node, ctx, source) {
        edges.push(edge);
    }
    if let Some(edge) = express_route_chain_edge(node, ctx, source) {
        edges.push(edge);
    }
    edges.extend(fastify_route_edges(node, ctx, source));
    edges
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if let Some(edge) = fetch_call_edge(node, ctx, source) {
        return Some(edge);
    }
    axios_call_edge(node, ctx, source)
}

fn grpc_impl_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if node.kind() != "call_expression" {
        return edges;
    }
    let Some(target_node) = call_target_node(node) else {
        return edges;
    };
    let Some((_receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return edges;
    };
    if method_name != "addService" {
        return edges;
    }
    let args = call_arguments(node);
    let Some(service_arg) = args.get(0) else {
        return edges;
    };
    let Some(service) = grpc_service_from_service_def(*service_arg, source) else {
        return edges;
    };
    let Some(handlers_arg) = args.get(1) else {
        return edges;
    };
    if handlers_arg.kind() != "object" {
        return edges;
    }
    for (rpc_name, handler) in grpc_handlers_from_object(*handlers_arg, ctx, source) {
        if let Some(edge) = grpc_impl_edge(node, &service, &rpc_name, handler, source) {
            edges.push(edge);
        }
    }
    edges
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "call_expression" {
        return None;
    }
    let Some(target_node) = call_target_node(node) else {
        return None;
    };
    let Some((object_node, method_name)) = member_object_and_method(target_node, source) else {
        return None;
    };
    let method_name = unquote_string_literal(&method_name).unwrap_or(method_name);
    if GRPC_JS_RAW_METHODS.contains(&method_name.as_str()) {
        return grpc_call_edge_from_raw_path(node, ctx, source);
    }
    if GRPC_JS_SKIP_METHODS.contains(&method_name.as_str()) {
        return None;
    }
    let service = grpc_service_for_receiver(object_node, ctx, source)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, &method_name)?;
    let detail = json!({
        "framework": "grpc-js",
        "role": "client",
        "service": service.service,
        "rpc": method_name,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_call_edge_from_raw_path(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let args = call_arguments(node);
    let raw_path = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let (service, rpc) = grpc_service_from_raw_path(&raw_path)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, &rpc)?;
    let detail = json!({
        "framework": "grpc-js",
        "role": "client",
        "service": service.service,
        "rpc": rpc,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_impl_edge(
    node: Node<'_>,
    service: &GrpcService,
    rpc_name: &str,
    handler: String,
    source: &str,
) -> Option<EdgeInput> {
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-js",
        "role": "server",
        "service": service.service.as_str(),
        "rpc": rpc_name,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let (receiver, method) = member_receiver_and_method(target_node, source)?;
    if !channel::is_bus_receiver(&receiver) {
        return None;
    }
    let kind = if channel::is_publish_method(&method) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&method) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };
    let args = call_arguments(node);
    let raw_topic = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))
        .or_else(|| args.first().map(|arg| node_text(*arg, source)))?;
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "js-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "js-bus")
    };
    Some(EdgeInput {
        kind: kind.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_handlers_from_object(node: Node<'_>, ctx: &Context, source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "pair" => {
                let Some(key_node) = child.child_by_field_name("key") else {
                    continue;
                };
                let Some(rpc_name) = grpc_property_name(key_node, source) else {
                    continue;
                };
                let handler = child
                    .child_by_field_name("value")
                    .and_then(|node| handler_node_qualname(node, ctx, source))
                    .unwrap_or_else(|| ctx.current_scope.clone());
                out.push((rpc_name, handler));
            }
            "shorthand_property_identifier" | "shorthand_property_identifier_pattern" => {
                let rpc_name = node_text(child, source);
                if rpc_name.is_empty() {
                    continue;
                }
                let handler = resolve_call_target(&rpc_name, ctx)
                    .unwrap_or_else(|| ctx.current_scope.clone());
                out.push((rpc_name, handler));
            }
            "method_definition" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let rpc_name = node_text(name_node, source);
                if rpc_name.is_empty() {
                    continue;
                }
                out.push((rpc_name, ctx.current_scope.clone()));
            }
            _ => {}
        }
    }
    out
}

fn grpc_property_name(node: Node<'_>, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    if let Some(value) = unquote_string_literal(&raw) {
        return Some(value);
    }
    Some(raw.trim_matches('"').trim_matches('\'').to_string())
}

fn grpc_service_from_service_def(node: Node<'_>, source: &str) -> Option<GrpcService> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let mut trimmed = raw.trim();
    if let Some(stripped) = trimmed.strip_suffix(".service") {
        trimmed = stripped;
    }
    grpc_service_from_path(trimmed)
}

fn grpc_service_for_receiver(node: Node<'_>, ctx: &Context, source: &str) -> Option<GrpcService> {
    if node.kind() == "new_expression" {
        let constructor = call_target_node(node)?;
        let raw = node_text(constructor, source);
        return grpc_service_from_path(&raw);
    }
    let receiver = node_text(node, source);
    grpc_service_from_receiver(&receiver, ctx)
}

fn grpc_service_from_receiver(receiver: &str, ctx: &Context) -> Option<GrpcService> {
    let receiver = receiver.trim();
    if receiver.is_empty() {
        return None;
    }
    if let Some(service) = ctx.grpc_clients.get(receiver) {
        return Some(service.clone());
    }
    if let Some(last) = receiver.rsplit('.').next() {
        if let Some(service) = ctx.grpc_clients.get(last) {
            return Some(service.clone());
        }
    }
    grpc_service_from_path(receiver)
}

fn grpc_service_from_raw_path(raw_path: &str) -> Option<(GrpcService, String)> {
    let trimmed = raw_path.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let trimmed = trimmed.trim_start_matches('/');
    let mut parts = trimmed.splitn(2, '/');
    let service_path = parts.next()?.trim();
    let rpc = parts.next()?.trim();
    if service_path.is_empty() || rpc.is_empty() {
        return None;
    }
    let service_parts: Vec<&str> = service_path
        .split('.')
        .filter(|part| !part.is_empty())
        .collect();
    let service = service_parts.last()?.trim();
    if service.is_empty() {
        return None;
    }
    let package = grpc_package_from_parts(&service_parts[..service_parts.len() - 1], false);
    Some((
        GrpcService {
            package,
            service: service.to_string(),
        },
        rpc.to_string(),
    ))
}

fn grpc_service_from_path(raw: &str) -> Option<GrpcService> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !is_simple_call_target(trimmed) {
        return None;
    }
    let parts: Vec<&str> = trimmed.split('.').filter(|part| !part.is_empty()).collect();
    let service_token = parts.last()?.trim();
    if service_token.is_empty() {
        return None;
    }
    let (service, stripped) = strip_grpc_service_token(service_token);
    if service.is_empty() {
        return None;
    }
    if !stripped && !service.chars().any(|ch| ch.is_ascii_uppercase()) {
        return None;
    }
    let package = grpc_package_from_parts(&parts[..parts.len() - 1], true);
    Some(GrpcService { package, service })
}

fn strip_grpc_service_token(raw: &str) -> (String, bool) {
    let mut token = raw.trim();
    if let Some(idx) = token.find('<') {
        token = &token[..idx];
    }
    if let Some(idx) = token.find('(') {
        token = &token[..idx];
    }
    let token = token.trim();
    if token.is_empty() {
        return (String::new(), false);
    }
    let mut stripped = false;
    let mut stripped_client = false;
    let mut value = token;
    if let Some(base) = value.strip_suffix("Client") {
        if !base.is_empty() {
            value = base;
            stripped = true;
            stripped_client = true;
        }
    }
    if !stripped_client {
        if let Some(base) = value.strip_suffix("Service") {
            if !base.is_empty() {
                value = base;
                stripped = true;
            }
        }
    }
    (value.to_string(), stripped)
}

fn grpc_package_from_parts(parts: &[&str], drop_root: bool) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let mut start = 0;
    if drop_root {
        if let Some(first) = parts.first() {
            if is_grpc_root_segment(first) {
                start = 1;
            }
        }
    }
    if start >= parts.len() {
        return None;
    }
    let mut package_parts = Vec::new();
    for part in &parts[start..] {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            package_parts.push(trimmed);
        }
    }
    if package_parts.is_empty() {
        None
    } else {
        Some(package_parts.join("."))
    }
}

fn is_grpc_root_segment(segment: &str) -> bool {
    let lower = segment.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "proto" | "root" | "pb" | "pkg" | "package" | "services" | "service"
    )
}

fn method_route_edges(
    node: Node<'_>,
    qualname: &str,
    ctx: &Context,
    source: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    for decorator in decorator_nodes(node) {
        let Some((name, args)) = decorator_name_and_args(decorator, source) else {
            continue;
        };
        let method = match http::normalize_method(&name) {
            Some(method) => method,
            None => continue,
        };
        let raw = args
            .get(0)
            .and_then(|arg| extract_string_literal(*arg, source))
            .unwrap_or_else(|| "/".to_string());
        let prefix = ctx.route_prefix.as_deref().unwrap_or("/");
        let raw_path = http::join_paths(prefix, &raw);
        let normalized = match http::normalize_path(&raw_path) {
            Some(value) => value,
            None => continue,
        };
        let detail = http::build_route_detail(&method, &normalized, &raw_path, "nestjs");
        edges.push(EdgeInput {
            kind: http::HTTP_ROUTE_KIND.to_string(),
            source_qualname: Some(qualname.to_string()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    edges
}

fn controller_prefix_from_class(node: Node<'_>, source: &str) -> Option<String> {
    for decorator in decorator_nodes(node) {
        let Some((name, args)) = decorator_name_and_args(decorator, source) else {
            continue;
        };
        if name == "Controller" {
            let raw = args
                .get(0)
                .and_then(|arg| extract_string_literal(*arg, source))
                .unwrap_or_else(|| "/".to_string());
            return Some(raw);
        }
    }
    None
}

fn decorator_nodes(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "decorator" {
            out.push(child);
        }
    }
    out
}

fn decorator_name_and_args<'a>(node: Node<'a>, source: &str) -> Option<(String, Vec<Node<'a>>)> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "call_expression" {
            let Some(target_node) = call_target_node(child) else {
                continue;
            };
            let raw = node_text(target_node, source);
            let name = raw.split('.').last().unwrap_or(raw.as_str()).to_string();
            let args = call_arguments(child);
            return Some((name, args));
        }
    }
    let raw = node_text(node, source);
    let name = raw
        .trim_start_matches('@')
        .split('.')
        .last()
        .unwrap_or(raw.as_str())
        .to_string();
    if name.is_empty() {
        None
    } else {
        Some((name, Vec::new()))
    }
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let Some(args) = node.child_by_field_name("arguments") else {
        return out;
    };
    let mut cursor = args.walk();
    for child in args.named_children(&mut cursor) {
        out.push(child);
    }
    out
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "template_string" {
        return None;
    }
    let raw = node_text(node, source);
    unquote_string_literal(&raw)
}

fn express_direct_route_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let Some(target_node) = call_target_node(node) else {
        return None;
    };
    let Some((receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return None;
    };
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    if !is_router_receiver(&receiver) {
        return None;
    }
    let args = call_arguments(node);
    let raw_path = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = http::normalize_method(&method_name)?;
    let handler = handler_from_args(&args[1..], ctx, source);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, "express");
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn express_route_chain_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let Some(target_node) = call_target_node(node) else {
        return None;
    };
    let Some((object_node, method_name)) = member_object_and_method(target_node, source) else {
        return None;
    };
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    if object_node.kind() != "call_expression" {
        return None;
    }
    let route_call = object_node;
    let Some(route_target) = call_target_node(route_call) else {
        return None;
    };
    let Some((_route_receiver, route_method)) = member_receiver_and_method(route_target, source)
    else {
        return None;
    };
    if route_method != "route" {
        return None;
    }
    let route_args = call_arguments(route_call);
    let raw_path = route_args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = http::normalize_method(&method_name)?;
    let args = call_arguments(node);
    let handler = handler_from_args(&args, ctx, source);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, "express");
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn fastify_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let Some(target_node) = call_target_node(node) else {
        return edges;
    };
    let Some((receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return edges;
    };
    if method_name != "route" || receiver != "fastify" {
        return edges;
    }
    let args = call_arguments(node);
    let Some(config) = args.get(0) else {
        return edges;
    };
    if config.kind() != "object" {
        return edges;
    }
    let raw_path = object_property_string(config, "url", source)
        .or_else(|| object_property_string(config, "path", source));
    let Some(raw_path) = raw_path else {
        return edges;
    };
    let normalized = match http::normalize_path(&raw_path) {
        Some(value) => value,
        None => return edges,
    };
    let handler = object_property_node(config, "handler", source)
        .and_then(|node| handler_node_qualname(node, ctx, source));
    let handler = handler.unwrap_or_else(|| ctx.current_scope.clone());
    let methods = object_property_methods(config, source);
    for method in methods {
        let detail = http::build_route_detail(&method, &normalized, &raw_path, "fastify");
        edges.push(EdgeInput {
            kind: http::HTTP_ROUTE_KIND.to_string(),
            source_qualname: Some(handler.clone()),
            target_qualname: Some(normalized.clone()),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    edges
}

fn fetch_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let Some(target_node) = call_target_node(node) else {
        return None;
    };
    if !is_fetch_callee(target_node, source) {
        return None;
    }
    let args = call_arguments(node);
    let raw_path = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = args
        .get(1)
        .and_then(|arg| object_property_string(arg, "method", source))
        .and_then(|raw| http::normalize_method(&raw))
        .unwrap_or_else(|| "GET".to_string());
    let detail = http::build_call_detail(&method, &normalized, &raw_path, "fetch");
    Some(EdgeInput {
        kind: http::HTTP_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn axios_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let Some(target_node) = call_target_node(node) else {
        return None;
    };
    let args = call_arguments(node);
    if is_axios_identifier(target_node, source) {
        let Some(config) = args.get(0) else {
            return None;
        };
        let raw_path = object_property_string(config, "url", source)?;
        let normalized = http::normalize_path(&raw_path)?;
        let method = object_property_string(config, "method", source)
            .and_then(|raw| http::normalize_method(&raw))
            .unwrap_or_else(|| "GET".to_string());
        let detail = http::build_call_detail(&method, &normalized, &raw_path, "axios");
        return Some(EdgeInput {
            kind: http::HTTP_CALL_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    let Some((receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return None;
    };
    if receiver != "axios" {
        return None;
    }
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    let raw_path = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = http::normalize_method(&method_name)?;
    let detail = http::build_call_detail(&method, &normalized, &raw_path, "axios");
    Some(EdgeInput {
        kind: http::HTTP_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn is_http_client_call(node: Node<'_>, source: &str) -> bool {
    let Some(target_node) = call_target_node(node) else {
        return false;
    };
    is_fetch_callee(target_node, source) || is_axios_callee(target_node, source)
}

fn is_fetch_callee(node: Node<'_>, source: &str) -> bool {
    if is_identifier_named(node, source, "fetch") {
        return true;
    }
    let Some((receiver, method)) = member_receiver_and_method(node, source) else {
        return false;
    };
    method == "fetch" && (receiver == "window" || receiver == "global" || receiver == "globalThis")
}

fn is_axios_callee(node: Node<'_>, source: &str) -> bool {
    if is_axios_identifier(node, source) {
        return true;
    }
    let Some((receiver, _method)) = member_receiver_and_method(node, source) else {
        return false;
    };
    receiver == "axios"
}

fn is_axios_identifier(node: Node<'_>, source: &str) -> bool {
    is_identifier_named(node, source, "axios")
}

fn is_identifier_named(node: Node<'_>, source: &str, name: &str) -> bool {
    node.kind() == "identifier" && node_text(node, source) == name
}

fn member_receiver_and_method(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "member_expression" && node.kind() != "optional_member_expression" {
        return None;
    }
    let receiver = node
        .child_by_field_name("object")
        .map(|obj| node_text(obj, source))?;
    let method = node
        .child_by_field_name("property")
        .map(|prop| node_text(prop, source))?;
    Some((receiver, method))
}

fn member_object_and_method<'a>(node: Node<'a>, source: &str) -> Option<(Node<'a>, String)> {
    if node.kind() != "member_expression" && node.kind() != "optional_member_expression" {
        return None;
    }
    let object = node.child_by_field_name("object")?;
    let method = node
        .child_by_field_name("property")
        .map(|prop| node_text(prop, source))?;
    Some((object, method))
}

fn is_router_receiver(raw: &str) -> bool {
    let head = raw.split('.').next().unwrap_or(raw);
    ROUTER_RECEIVERS.contains(&head)
}

fn handler_from_args(args: &[Node<'_>], ctx: &Context, source: &str) -> String {
    if let Some(last) = args.last() {
        if let Some(name) = handler_node_qualname(*last, ctx, source) {
            return name;
        }
    }
    ctx.current_scope.clone()
}

fn handler_node_qualname(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    match node.kind() {
        "identifier"
        | "member_expression"
        | "optional_member_expression"
        | "shorthand_property_identifier"
        | "shorthand_property_identifier_pattern" => {
            let raw = node_text(node, source);
            resolve_call_target(&raw, ctx)
        }
        _ => None,
    }
}

fn object_property_node<'a>(node: &'a Node<'a>, key: &str, source: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "pair" {
            continue;
        }
        let Some(key_node) = child.child_by_field_name("key") else {
            continue;
        };
        let key_text = node_text(key_node, source);
        let key_text = key_text.trim_matches('"').trim_matches('\'');
        if key_text != key {
            continue;
        }
        let value = child.child_by_field_name("value")?;
        return Some(value);
    }
    None
}

fn object_property_string(node: &Node<'_>, key: &str, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "pair" {
            continue;
        }
        let Some(key_node) = child.child_by_field_name("key") else {
            continue;
        };
        let key_text = node_text(key_node, source);
        let key_text = key_text.trim_matches('"').trim_matches('\'');
        if key_text != key {
            continue;
        }
        let Some(value_node) = child.child_by_field_name("value") else {
            continue;
        };
        if let Some(value) = extract_string_literal(value_node, source) {
            return Some(value);
        }
    }
    None
}

fn object_property_methods(node: &Node<'_>, source: &str) -> Vec<String> {
    let mut methods = Vec::new();
    let Some(value_node) = object_property_node(node, "method", source) else {
        return vec![http::HTTP_ANY.to_string()];
    };
    match value_node.kind() {
        "array" => {
            let mut cursor = value_node.walk();
            for child in value_node.named_children(&mut cursor) {
                if let Some(raw) = extract_string_literal(child, source) {
                    if let Some(method) = http::normalize_method(&raw) {
                        methods.push(method);
                    }
                }
            }
        }
        _ => {
            if let Some(raw) = extract_string_literal(value_node, source) {
                if let Some(method) = http::normalize_method(&raw) {
                    methods.push(method);
                }
            }
        }
    }
    if methods.is_empty() {
        methods.push(http::HTTP_ANY.to_string());
    }
    methods
}

fn jsx_route_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let opening = match node.kind() {
        "jsx_element" => node.child_by_field_name("opening_element")?,
        "jsx_self_closing_element" => node,
        _ => return None,
    };
    let name_node = opening.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name != "Route" {
        return None;
    }
    let mut raw_path = None;
    let mut cursor = opening.walk();
    for child in opening.named_children(&mut cursor) {
        if child.kind() != "jsx_attribute" {
            continue;
        }
        let Some(attr_name) = child.child_by_field_name("name") else {
            continue;
        };
        let attr_name = node_text(attr_name, source);
        if attr_name != "path" {
            continue;
        }
        if let Some(value_node) = child.child_by_field_name("value") {
            raw_path = extract_string_literal(value_node, source);
        }
    }
    let raw_path = raw_path?;
    let normalized = http::normalize_path(&raw_path)?;
    Some(EdgeInput {
        kind: http::PAGE_ROUTE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized.clone()),
        detail: Some(json!({"framework":"react_router","path":normalized}).to_string()),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn next_page_route_edge(module_name: &str) -> Option<EdgeInput> {
    let Some(raw) = next_route_from_module(module_name, false) else {
        return None;
    };
    let normalized = http::normalize_path(&raw)?;
    Some(EdgeInput {
        kind: http::PAGE_ROUTE_KIND.to_string(),
        source_qualname: Some(module_name.to_string()),
        target_qualname: Some(normalized.clone()),
        detail: Some(json!({"framework": "nextjs", "path": normalized}).to_string()),
        evidence_snippet: None,
        evidence_start_line: None,
        evidence_end_line: None,
        ..Default::default()
    })
}

fn next_api_route_edge(module_name: &str) -> Option<EdgeInput> {
    let Some(raw) = next_route_from_module(module_name, true) else {
        return None;
    };
    let normalized = http::normalize_path(&raw)?;
    let detail = http::build_route_detail(http::HTTP_ANY, &normalized, &raw, "nextjs");
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(module_name.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: None,
        evidence_end_line: None,
        ..Default::default()
    })
}

fn next_route_from_module(module_name: &str, api_only: bool) -> Option<String> {
    let parts: Vec<&str> = module_name.split('/').collect();
    if parts.is_empty() {
        return None;
    }
    let is_pages = parts.first() == Some(&"pages");
    let is_app = parts.first() == Some(&"app");
    if !is_pages && !is_app {
        return None;
    }
    let mut segments = parts[1..].to_vec();
    if api_only {
        if segments.first() != Some(&"api") {
            return None;
        }
        segments.remove(0);
        if let Some(last) = segments.last() {
            if *last == "route" {
                segments.pop();
            }
        }
    } else {
        if segments.first() == Some(&"api") {
            return None;
        }
        if is_app {
            if let Some(last) = segments.last() {
                if *last != "page" {
                    return None;
                }
            }
            segments.pop();
        }
    }
    let mut out = String::from("/");
    let mut first = true;
    for seg in segments {
        if seg.is_empty() || seg.starts_with('(') {
            continue;
        }
        if seg == "index" {
            continue;
        }
        if !first {
            out.push('/');
        }
        first = false;
        let normalized = seg
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim_start_matches("...");
        if seg.starts_with('[') && seg.ends_with(']') {
            out.push(':');
            out.push_str(normalized);
        } else {
            out.push_str(seg);
        }
    }
    if out.is_empty() {
        out.push('/');
    }
    Some(out)
}

fn call_target_node(node: Node<'_>) -> Option<Node<'_>> {
    node.child_by_field_name("function")
        .or_else(|| node.child_by_field_name("callee"))
        .or_else(|| node.child_by_field_name("constructor"))
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    let mut parts: Vec<&str> = raw.split('.').collect();
    if parts.is_empty() {
        return None;
    }
    if parts[0] == "this" || parts[0] == "super" {
        parts.remove(0);
        if parts.is_empty() {
            return None;
        }
        let container = container_qualname(&ctx.module, &ctx.class_stack);
        return Some(format!("{container}.{}", parts.join(".")));
    }
    if parts.len() == 1 {
        let container = container_qualname(&ctx.module, &ctx.class_stack);
        return Some(format!("{container}.{raw}"));
    }
    Some(raw.to_string())
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '$' || ch == '#')
}

fn is_nested_function_node(kind: &str) -> bool {
    matches!(
        kind,
        "function" | "function_expression" | "arrow_function" | "generator_function"
    )
}

fn handle_function(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source);
    output.symbols.push(SymbolInput {
        kind: "function".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_method(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source);
    output.symbols.push(SymbolInput {
        kind: "method".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature,
        docstring: None,
    });
    let parent = container_qualname(&ctx.module, &ctx.class_stack);
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    for edge in method_route_edges(node, &qualname, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_named_item(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    kind: &str,
) -> Option<String> {
    let Some(name_node) = node.child_by_field_name("name") else {
        return None;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: kind.to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    Some(qualname)
}

fn handle_variable_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    if !ctx.class_stack.is_empty() {
        return;
    }
    let decl_kind = declaration_keyword(node, source);
    let kind = if decl_kind == "const" {
        "const"
    } else {
        "variable"
    };
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declarator" {
            continue;
        }
        let Some(name_node) = child.child_by_field_name("name") else {
            continue;
        };
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
        let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
        output.symbols.push(SymbolInput {
            kind: kind.to_string(),
            name: name.clone(),
            qualname: qualname.clone(),
            start_line,
            start_col,
            end_line,
            end_col,
            start_byte,
            end_byte,
            signature: None,
            docstring: None,
        });
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(ctx.module.clone()),
            target_qualname: Some(qualname),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn declaration_keyword(node: Node<'_>, source: &str) -> &'static str {
    let text = node_text(node, source);
    let trimmed = text.trim_start();
    if trimmed.starts_with("const ") {
        "const"
    } else if trimmed.starts_with("let ") {
        "let"
    } else if trimmed.starts_with("var ") {
        "var"
    } else {
        "var"
    }
}

fn handle_import(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    allow_fallback: bool,
) {
    let target = match extract_import_target(node, source, allow_fallback) {
        Some(value) => value,
        None => return,
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(target),
        detail: None,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn extract_import_target(node: Node<'_>, source: &str, allow_fallback: bool) -> Option<String> {
    if let Some(source_node) = node.child_by_field_name("source") {
        let raw = node_text(source_node, source);
        return unquote_string_literal(&raw).or(Some(raw));
    }
    if !allow_fallback {
        return None;
    }
    let raw = node_text(node, source);
    extract_string_from_text(&raw)
}

fn extract_string_from_text(raw: &str) -> Option<String> {
    let mut chars = raw.char_indices();
    let mut quote = None;
    let mut start = 0;
    while let Some((idx, ch)) = chars.next() {
        if ch == '"' || ch == '\'' || ch == '`' {
            quote = Some(ch);
            start = idx + ch.len_utf8();
            break;
        }
    }
    let quote = quote?;
    let rest = &raw[start..];
    let end = rest.find(quote)?;
    Some(rest[..end].to_string())
}

fn unquote_string_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.len() < 2 {
        return None;
    }
    let first = trimmed.chars().next()?;
    if first == '"' || first == '\'' || first == '`' {
        let last = trimmed.chars().last()?;
        if last == first {
            return Some(trimmed[1..trimmed.len() - 1].to_string());
        }
    }
    None
}

fn extract_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    params.filter(|value| !value.is_empty())
}

fn span(node: Node<'_>) -> (i64, i64, i64, i64, i64, i64) {
    let start = node.start_position();
    let end = node.end_position();
    (
        start.row as i64 + 1,
        start.column as i64 + 1,
        end.row as i64 + 1,
        end.column as i64 + 1,
        node.start_byte() as i64,
        node.end_byte() as i64,
    )
}

fn build_qualname(module: &str, class_stack: &[String], name: &str) -> String {
    if class_stack.is_empty() {
        format!("{module}.{name}")
    } else {
        format!("{module}.{}.{}", class_stack.join("."), name)
    }
}

fn container_qualname(module: &str, class_stack: &[String]) -> String {
    if class_stack.is_empty() {
        module.to_string()
    } else {
        format!("{module}.{}", class_stack.join("."))
    }
}

fn node_text(node: Node<'_>, source: &str) -> String {
    let start = node.start_byte();
    let end = node.end_byte();
    source.get(start..end).unwrap_or("").trim().to_string()
}

fn line_count(source: &str) -> i64 {
    let count = source.lines().count();
    if count == 0 { 1 } else { count as i64 }
}

#[cfg(test)]
mod tests {
    use super::JavascriptExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_express_route_and_fetch_call() {
        let source = r#"
const app = require("express")();
function handler(req, res) {}
app.get("/api/users/:id", handler);
fetch("/api/users/123", { method: "POST" });
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let routes = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_ROUTE_KIND)
            .collect::<Vec<_>>();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
    }

    #[test]
    fn extracts_grpc_js_impl_and_call() {
        let source = r#"
const grpc = require("@grpc/grpc-js");
const proto = { helloworld: { Greeter: { service: {} } } };
function sayHello(call, callback) {}
const server = new grpc.Server();
server.addService(proto.helloworld.Greeter.service, { sayHello });
const client = new proto.helloworld.Greeter("localhost:50051", grpc.credentials.createInsecure());
client.sayHello({ name: "world" }, () => {});
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let impls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_IMPL_KIND)
            .collect::<Vec<_>>();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(impls.iter().any(|edge| {
            edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")
        }));
        assert!(calls.iter().any(|edge| {
            edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")
        }));
    }
}
