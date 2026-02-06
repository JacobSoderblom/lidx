use crate::indexer::channel;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::indexer::proto;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::path::Path;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
    class_stack: Vec<String>,
    fn_depth: usize,
    current_scope: String,
    grpc_service: Option<String>,
}

pub struct PythonExtractor {
    parser: Parser,
}

impl PythonExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_python::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for PythonExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        let tree = match self.parser.parse(source, None) {
            Some(tree) => tree,
            None => {
                output
                    .symbols
                    .push(module_symbol_fallback(module_name, source));
                return Ok(output);
            }
        };
        let root = tree.root_node();

        let module_docstring = extract_docstring(root, source);
        let module_span = span(root);
        output.symbols.push(module_symbol_with_span(
            module_name,
            module_span,
            module_docstring,
        ));
        let ctx = Context {
            module: module_name.to_string(),
            class_stack: Vec::new(),
            fn_depth: 0,
            current_scope: module_name.to_string(),
            grpc_service: None,
        };
        walk_node(root, &ctx, source, &mut output);
        Ok(output)
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        module_name: &str,
        edges: &mut Vec<crate::indexer::extract::EdgeInput>,
    ) {
        resolve_import_file_edges(repo_root, file_rel_path, module_name, edges);
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "__init__".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if stem != "__init__" {
        parts.push(stem);
    }
    if parts.is_empty() {
        "__init__".to_string()
    } else {
        parts.join(".")
    }
}

pub fn resolve_import_file_edges(
    repo_root: &Path,
    file_rel_path: &str,
    file_module: &str,
    edges: &mut Vec<EdgeInput>,
) {
    let base_package = base_package_parts(file_rel_path, file_module);
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
        let (import_kind, base_module) = import_kind_and_base(edge.evidence_snippet.as_deref());
        let mut candidates = Vec::new();
        if !target.contains('*') {
            candidates.push(target.to_string());
        }
        if import_kind == ImportKind::From {
            if let Some(base) = base_module {
                if !base.contains('*') {
                    candidates.push(base);
                }
            }
        }
        let mut resolved_edge = None;
        for candidate in candidates {
            let abs_module = match absolutize_module(&candidate, &base_package) {
                Some(value) => value,
                None => continue,
            };
            if let Some(dst_path) = resolve_module_to_file(repo_root, &abs_module) {
                resolved_edge = Some(EdgeInput {
                    kind: "IMPORTS_FILE".to_string(),
                    source_qualname: edge.source_qualname.clone(),
                    target_qualname: Some(abs_module),
                    detail: Some(
                        json!({
                            "src_path": file_rel_path,
                            "dst_path": dst_path,
                            "confidence": 1.0,
                        })
                        .to_string(),
                    ),
                    evidence_snippet: edge.evidence_snippet.clone(),
                    evidence_start_line: edge.evidence_start_line,
                    evidence_end_line: edge.evidence_end_line,
                    ..Default::default()
                });
                break;
            }
        }
        if let Some(edge) = resolved_edge {
            resolved.push(edge);
        }
    }
    edges.extend(resolved);
}

fn module_symbol_with_span(
    module_name: &str,
    span: (i64, i64, i64, i64, i64, i64),
    docstring: Option<String>,
) -> SymbolInput {
    let name = module_name
        .rsplit('.')
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
        docstring,
    }
}

fn module_symbol_fallback(module_name: &str, source: &str) -> SymbolInput {
    module_symbol_with_span(
        module_name,
        (1, 1, line_count(source), 1, 0, source.len() as i64),
        extract_docstring_fallback(source),
    )
}

fn extract_docstring_fallback(source: &str) -> Option<String> {
    let mut lines = source.lines();
    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(doc) = unquote_string_literal(trimmed) {
            return Some(doc);
        }
        break;
    }
    None
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "decorated_definition" {
        handle_decorated_definition(node, ctx, source, output);
        return;
    }
    if node.kind() == "call" {
        handle_call(node, ctx, source, output);
    }
    match node.kind() {
        "class_definition" => {
            if ctx.fn_depth > 0 {
                return;
            }
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = node_text(name_node, source);
                let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
                let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
                let docstring = node
                    .child_by_field_name("body")
                    .and_then(|body| extract_docstring(body, source));
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
                    docstring,
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

                let mut grpc_service = None;
                if let Some(superclasses) = node.child_by_field_name("superclasses") {
                    let mut cursor = superclasses.walk();
                    for child in superclasses.named_children(&mut cursor) {
                        let base = node_text(child, source);
                        if !base.is_empty() {
                            if grpc_service.is_none() {
                                grpc_service = grpc_service_name_from_base(&base);
                            }
                            output.edges.push(EdgeInput {
                                kind: "EXTENDS".to_string(),
                                source_qualname: Some(qualname.clone()),
                                target_qualname: Some(base),
                                detail: None,
                                evidence_snippet: None,
                                ..Default::default()
                            });
                        }
                    }
                }

                let mut next_ctx = ctx.clone();
                next_ctx.class_stack.push(name);
                next_ctx.current_scope = qualname.clone();
                next_ctx.grpc_service = grpc_service;
                if let Some(body) = node.child_by_field_name("body") {
                    walk_block(body, &next_ctx, source, output);
                }
            }
            return;
        }
        "function_definition" | "async_function_definition" => {
            if ctx.fn_depth > 0 {
                return;
            }
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = node_text(name_node, source);
                let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
                let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
                let signature = extract_signature(node, source);
                let docstring = node
                    .child_by_field_name("body")
                    .and_then(|body| extract_docstring(body, source));

                let kind = if ctx.class_stack.is_empty() {
                    "function"
                } else {
                    "method"
                };
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
                    signature,
                    docstring,
                });
                let parent = container_qualname(&ctx.module, &ctx.class_stack);
                output.edges.push(EdgeInput {
                    kind: "CONTAINS".to_string(),
                    source_qualname: Some(parent),
                    target_qualname: Some(qualname),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
                if kind == "method" {
                    if let Some(edge) = grpc_impl_edge(node, ctx, source, &name) {
                        output.edges.push(edge);
                    }
                }
                let mut next_ctx = ctx.clone();
                next_ctx.fn_depth += 1;
                next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
                if let Some(body) = node.child_by_field_name("body") {
                    walk_block(body, &next_ctx, source, output);
                }
            }
            return;
        }
        "import_statement" | "import_from_statement" => {
            if ctx.fn_depth == 0 {
                let module = ctx.module.clone();
                let text = node_text(node, source);
                let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
                let snippet =
                    util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
                for target in parse_imports(&text) {
                    output.edges.push(EdgeInput {
                        kind: "IMPORTS".to_string(),
                        source_qualname: Some(module.clone()),
                        target_qualname: Some(target),
                        detail: None,
                        evidence_snippet: snippet.clone(),
                        evidence_start_line: Some(start_line),
                        evidence_end_line: Some(end_line),
                        ..Default::default()
                    });
                }
            }
            return;
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn walk_block(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    let Some(function_node) = node.child_by_field_name("function") else {
        return;
    };
    let raw = node_text(function_node, source);
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

fn handle_decorated_definition(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut decorators = Vec::new();
    let mut definition = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "decorator" {
            decorators.push(child);
        } else if matches!(
            child.kind(),
            "function_definition" | "async_function_definition" | "class_definition"
        ) {
            definition = Some(child);
        }
    }
    if let Some(definition) = definition {
        if let Some(handler) = handler_qualname(definition, ctx, source) {
            let edges = route_edges_from_decorators(&decorators, &handler, source);
            output.edges.extend(edges);
            let edges = channel_edges_from_decorators(&decorators, &handler, source);
            output.edges.extend(edges);
        }
        walk_node(definition, ctx, source, output);
    }
}

fn handler_qualname(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    if node.kind() == "class_definition" {
        return None;
    }
    Some(build_qualname(&ctx.module, &ctx.class_stack, &name))
}

fn route_edges_from_decorators(
    decorators: &[Node<'_>],
    handler: &str,
    source: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    for decorator in decorators {
        let Some((name, args)) = decorator_call_info(*decorator, source) else {
            continue;
        };
        let name = name.to_ascii_lowercase();
        if let Some(method) = http::normalize_method(&name) {
            let raw_path = args
                .positional
                .get(0)
                .and_then(|arg| extract_string_literal(*arg, source))
                .unwrap_or_else(|| "/".to_string());
            if let Some(edge) =
                build_route_edge(handler, &method, &raw_path, "fastapi", *decorator, source)
            {
                edges.push(edge);
            }
            continue;
        }
        if name == "route" {
            let raw_path = args
                .positional
                .get(0)
                .and_then(|arg| extract_string_literal(*arg, source))
                .unwrap_or_else(|| "/".to_string());
            let mut methods = methods_from_keywords(&args, source);
            if methods.is_empty() {
                methods.push("GET".to_string());
            }
            for method in methods {
                if let Some(edge) =
                    build_route_edge(handler, &method, &raw_path, "flask", *decorator, source)
                {
                    edges.push(edge);
                }
            }
            continue;
        }
        if name == "api_route" {
            let raw_path = args
                .positional
                .get(0)
                .and_then(|arg| extract_string_literal(*arg, source))
                .unwrap_or_else(|| "/".to_string());
            let mut methods = methods_from_keywords(&args, source);
            if methods.is_empty() {
                methods.push(http::HTTP_ANY.to_string());
            }
            for method in methods {
                if let Some(edge) =
                    build_route_edge(handler, &method, &raw_path, "fastapi", *decorator, source)
                {
                    edges.push(edge);
                }
            }
        }
    }
    edges
}

fn build_route_edge(
    handler: &str,
    method: &str,
    raw_path: &str,
    framework: &str,
    node: Node<'_>,
    source: &str,
) -> Option<EdgeInput> {
    let mut path = raw_path.trim().to_string();
    if !path.starts_with('/') {
        path = format!("/{path}");
    }
    let normalized = http::normalize_path(&path)?;
    let detail = http::build_route_detail(method, &normalized, &path, framework);
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn decorator_call_info<'a>(node: Node<'a>, source: &str) -> Option<(String, CallArgs<'a>)> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "call" {
            let name = call_target_name(child, source)?;
            let args = parse_call_arguments(child, source);
            return Some((name, args));
        }
    }
    None
}

struct CallArgs<'a> {
    positional: Vec<Node<'a>>,
    keywords: Vec<(String, Node<'a>)>,
}

fn parse_call_arguments<'a>(node: Node<'a>, source: &str) -> CallArgs<'a> {
    let mut positional = Vec::new();
    let mut keywords = Vec::new();
    let Some(args) = node.child_by_field_name("arguments") else {
        return CallArgs {
            positional,
            keywords,
        };
    };
    let mut cursor = args.walk();
    for child in args.named_children(&mut cursor) {
        if child.kind() == "keyword_argument" {
            if let (Some(name_node), Some(value_node)) = (
                child.child_by_field_name("name"),
                child.child_by_field_name("value"),
            ) {
                let name = node_text(name_node, source);
                keywords.push((name, value_node));
            }
            continue;
        }
        positional.push(child);
    }
    CallArgs {
        positional,
        keywords,
    }
}

fn methods_from_keywords(args: &CallArgs<'_>, source: &str) -> Vec<String> {
    for (name, value) in &args.keywords {
        if name == "methods" {
            return extract_string_list(*value, source)
                .into_iter()
                .filter_map(|raw| http::normalize_method(&raw))
                .collect();
        }
    }
    Vec::new()
}

fn call_target_name(node: Node<'_>, source: &str) -> Option<String> {
    let function = node.child_by_field_name("function")?;
    if function.kind() == "attribute" {
        if let Some(attr) = function.child_by_field_name("attribute") {
            return Some(node_text(attr, source));
        }
    }
    Some(node_text(function, source))
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if let Some(edge) = django_path_edge(node, ctx, source) {
        edges.push(edge);
    }
    edges.extend(fastapi_add_api_route_edges(node, ctx, source));
    edges
}

fn django_path_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let name = call_target_name(node, source)?;
    if name != "path" && name != "re_path" {
        return None;
    }
    let args = parse_call_arguments(node, source);
    let raw_path = args
        .positional
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let handler = args
        .positional
        .get(1)
        .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
        .unwrap_or_else(|| ctx.current_scope.clone());
    build_route_edge(&handler, http::HTTP_ANY, &raw_path, "django", node, source)
}

fn fastapi_add_api_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let Some(function) = node.child_by_field_name("function") else {
        return Vec::new();
    };
    let Some((base, name)) = attribute_base_and_name(function, source) else {
        return Vec::new();
    };
    if name != "add_api_route" {
        return Vec::new();
    }
    if base.is_empty() {
        return Vec::new();
    }
    let args = parse_call_arguments(node, source);
    let Some(raw_path) = args
        .positional
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))
    else {
        return Vec::new();
    };
    let handler = args
        .positional
        .get(1)
        .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
        .unwrap_or_else(|| ctx.current_scope.clone());
    let mut methods = methods_from_keywords(&args, source);
    if methods.is_empty() {
        methods.push(http::HTTP_ANY.to_string());
    }
    let mut edges = Vec::new();
    for method in methods {
        if let Some(edge) = build_route_edge(&handler, &method, &raw_path, "fastapi", node, source)
        {
            edges.push(edge);
        }
    }
    edges
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function = node.child_by_field_name("function")?;
    let (base, name) = attribute_base_and_name(function, source)?;
    let client = http_client_label(&base)?;
    let args = parse_call_arguments(node, source);
    let (method, raw_path) = if name == "request" {
        let method = args
            .positional
            .get(0)
            .and_then(|arg| extract_string_literal(*arg, source))
            .and_then(|raw| http::normalize_method(&raw))?;
        let raw_path = args
            .positional
            .get(1)
            .and_then(|arg| extract_string_literal(*arg, source))?;
        (method, raw_path)
    } else if let Some(method) = http::normalize_method(&name) {
        let raw_path = args
            .positional
            .get(0)
            .and_then(|arg| extract_string_literal(*arg, source))?;
        (method, raw_path)
    } else {
        return None;
    };
    let normalized = http::normalize_path(&raw_path)?;
    let detail = http::build_call_detail(&method, &normalized, &raw_path, client);
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

fn grpc_impl_edge(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    rpc_name: &str,
) -> Option<EdgeInput> {
    let service = ctx.grpc_service.as_deref()?;
    let package = grpc_package_from_module(&ctx.module);
    let (raw_path, normalized) = proto::normalize_rpc_path(package.as_deref(), service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-python",
        "role": "server",
        "service": service,
        "rpc": rpc_name,
        "package": package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(build_qualname(&ctx.module, &ctx.class_stack, rpc_name)),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function = node.child_by_field_name("function")?;
    let (base, name) = attribute_base_and_name(function, source)?;
    if base.is_empty() || name.is_empty() {
        return None;
    }
    let service = grpc_service_from_stub_base(&base)?;
    let package = grpc_package_from_module(&ctx.module);
    let (raw_path, normalized) = proto::normalize_rpc_path(package.as_deref(), &service, &name)?;
    let detail = json!({
        "framework": "grpc-python",
        "role": "client",
        "service": service,
        "rpc": name,
        "package": package.as_deref(),
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

fn channel_edges_from_decorators(
    decorators: &[Node<'_>],
    handler: &str,
    source: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    for decorator in decorators {
        let Some((name, args)) = decorator_call_info(*decorator, source) else {
            continue;
        };
        let name_lower = name.to_ascii_lowercase();

        let kind = if name_lower.ends_with("subscribe") {
            channel::CHANNEL_SUBSCRIBE_KIND
        } else if name_lower.ends_with("publish") {
            channel::CHANNEL_PUBLISH_KIND
        } else {
            continue;
        };

        // Look for topic= keyword argument
        let raw_topic = args
            .keywords
            .iter()
            .find(|(k, _)| k == "topic")
            .map(|(_, v)| node_text(*v, source))
            .or_else(|| {
                // Fall back to first positional argument
                args.positional.first().map(|v| node_text(*v, source))
            });
        let Some(raw_topic) = raw_topic else {
            continue;
        };
        let Some(normalized) = channel::normalize_channel_name(&raw_topic) else {
            continue;
        };

        let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
            channel::build_publish_detail(&normalized, &raw_topic, "python-decorator")
        } else {
            channel::build_subscribe_detail(&normalized, &raw_topic, "python-decorator")
        };

        let (start_line, _start_col, end_line, _end_col, _start_byte, _end_byte) =
            span(*decorator);
        edges.push(EdgeInput {
            kind: kind.to_string(),
            source_qualname: Some(handler.to_string()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
    }
    edges
}

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function = node.child_by_field_name("function")?;
    let (base, name) = attribute_base_and_name(function, source)?;
    if base.is_empty() {
        return None;
    }
    if !channel::is_bus_receiver(&base) {
        return None;
    }
    let kind = if channel::is_publish_method(&name) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&name) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };
    let args = parse_call_arguments(node, source);
    // First positional arg or topic= keyword
    let raw_topic = args
        .keywords
        .iter()
        .find(|(k, _)| k == "topic")
        .map(|(_, v)| node_text(*v, source))
        .or_else(|| args.positional.first().map(|v| node_text(*v, source)))?;
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "python-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "python-bus")
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

fn attribute_base_and_name(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() == "attribute" {
        let base = node
            .child_by_field_name("object")
            .map(|n| node_text(n, source));
        let name = node
            .child_by_field_name("attribute")
            .map(|n| node_text(n, source));
        if let Some(name) = name {
            return Some((base.unwrap_or_default(), name));
        }
    }
    let name = node_text(node, source);
    Some((String::new(), name))
}

fn grpc_service_name_from_base(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let last = trimmed.rsplit('.').next().unwrap_or(trimmed).trim();
    if !last.ends_with("Servicer") {
        return None;
    }
    let service = last.trim_end_matches("Servicer");
    if service.is_empty() {
        None
    } else {
        Some(service.to_string())
    }
}

fn grpc_package_from_module(module: &str) -> Option<String> {
    let mut parts: Vec<&str> = module.split('.').collect();
    if parts.len() <= 1 {
        return None;
    }
    parts.pop();
    let package = parts.join(".");
    if package.is_empty() {
        None
    } else {
        Some(package)
    }
}

fn grpc_service_from_stub_base(base: &str) -> Option<String> {
    let mut token = base.trim();
    if let Some(idx) = token.find('(') {
        token = &token[..idx];
    }
    if let Some(idx) = token.rfind('.') {
        token = &token[idx + 1..];
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    if token.ends_with("Stub") {
        let service = token.trim_end_matches("Stub");
        if service.is_empty() {
            return None;
        }
        return Some(service.to_string());
    }
    let lower = token.to_ascii_lowercase();
    if lower.ends_with("stub") {
        let mut service = token[..token.len() - 4].to_string();
        while service.ends_with('_') {
            service.pop();
        }
        if service.is_empty() {
            return None;
        }
        return Some(service);
    }
    None
}

fn handler_name_from_expr(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    resolve_call_target(&raw, ctx)
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    unquote_string_literal(&raw).or(Some(raw))
}

fn extract_string_list(node: Node<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    if matches!(node.kind(), "list" | "tuple" | "set") {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if let Some(value) = extract_string_literal(child, source) {
                out.push(value);
            }
        }
        return out;
    }
    if let Some(value) = extract_string_literal(node, source) {
        out.push(value);
    }
    out
}

fn http_client_label(base: &str) -> Option<&'static str> {
    let base = base.trim();
    if base.starts_with("requests") {
        return Some("requests");
    }
    if base.starts_with("httpx") {
        return Some("httpx");
    }
    if base.starts_with("aiohttp") {
        return Some("aiohttp");
    }
    if base.ends_with("session") || base.ends_with("client") {
        return Some("http_client");
    }
    None
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
    if parts[0] == "self" || parts[0] == "cls" {
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
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.')
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

fn extract_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let return_type = node
        .child_by_field_name("return_type")
        .map(|n| node_text(n, source));
    match (params, return_type) {
        (Some(p), Some(r)) => Some(format!("{p} -> {r}")),
        (Some(p), None) => Some(p),
        _ => None,
    }
}

fn extract_docstring(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    let mut children = node.named_children(&mut cursor);
    let first = children.next()?;
    if first.kind() != "expression_statement" {
        return None;
    }
    let string_node = first.named_child(0)?;
    if string_node.kind() != "string" && string_node.kind() != "string_literal" {
        return None;
    }
    let raw = node_text(string_node, source);
    unquote_string_literal(&raw).or(Some(raw))
}

fn unquote_string_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut idx = 0;
    for (offset, ch) in trimmed.char_indices() {
        if ch.is_ascii_alphabetic() {
            idx = offset + ch.len_utf8();
        } else {
            break;
        }
    }
    let rest = &trimmed[idx..];
    if rest.starts_with("'''") && rest.ends_with("'''") && rest.len() >= 6 {
        return Some(rest[3..rest.len() - 3].to_string());
    }
    if rest.starts_with("\"\"\"") && rest.ends_with("\"\"\"") && rest.len() >= 6 {
        return Some(rest[3..rest.len() - 3].to_string());
    }
    if rest.starts_with('"') && rest.ends_with('"') && rest.len() >= 2 {
        return Some(rest[1..rest.len() - 1].to_string());
    }
    if rest.starts_with('\'') && rest.ends_with('\'') && rest.len() >= 2 {
        return Some(rest[1..rest.len() - 1].to_string());
    }
    None
}

fn parse_imports(text: &str) -> Vec<String> {
    let cleaned = text.replace('\n', " ");
    let cleaned = cleaned.trim().trim_end_matches(';');
    if let Some(rest) = cleaned.strip_prefix("import ") {
        return rest
            .split(',')
            .filter_map(|part| {
                let mut name = part.trim().split_whitespace();
                name.next().map(|s| s.to_string())
            })
            .collect();
    }
    if let Some(rest) = cleaned.strip_prefix("from ") {
        if let Some((module, names)) = rest.split_once(" import ") {
            let base = module.trim();
            return names
                .split(',')
                .filter_map(|part| {
                    let mut name = part.trim().split_whitespace();
                    let item = name.next()?;
                    if item == "*" {
                        return Some(format!("{base}.*"));
                    }
                    if base.is_empty() {
                        Some(item.to_string())
                    } else if base == "." || base.ends_with('.') {
                        Some(format!("{base}{item}"))
                    } else {
                        Some(format!("{base}.{item}"))
                    }
                })
                .collect();
        }
    }
    Vec::new()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ImportKind {
    Import,
    From,
}

fn import_kind_and_base(snippet: Option<&str>) -> (ImportKind, Option<String>) {
    let Some(snippet) = snippet else {
        return (ImportKind::Import, None);
    };
    let cleaned = snippet.trim().trim_end_matches(';');
    if let Some(rest) = cleaned.strip_prefix("from ") {
        if let Some((base, _)) = rest.split_once(" import ") {
            let base = base.trim();
            if !base.is_empty() {
                return (ImportKind::From, Some(base.to_string()));
            }
        }
    }
    (ImportKind::Import, None)
}

fn base_package_parts(file_rel_path: &str, file_module: &str) -> Vec<String> {
    let is_init = Path::new(file_rel_path)
        .file_name()
        .and_then(|s| s.to_str())
        == Some("__init__.py");
    let parts: Vec<&str> = file_module
        .split('.')
        .filter(|part| !part.is_empty())
        .collect();
    let keep = if is_init {
        parts.len()
    } else {
        parts.len().saturating_sub(1)
    };
    parts[..keep].iter().map(|part| part.to_string()).collect()
}

fn absolutize_module(candidate: &str, base_package: &[String]) -> Option<String> {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.starts_with('.') {
        return Some(trimmed.to_string());
    }
    let dot_count = trimmed.chars().take_while(|ch| *ch == '.').count();
    let rest = &trimmed[dot_count..];
    let up = dot_count.saturating_sub(1);
    if up > base_package.len() {
        return None;
    }
    let mut parts: Vec<String> = base_package.to_vec();
    let keep = parts.len().saturating_sub(up);
    parts.truncate(keep);
    if !rest.is_empty() {
        for segment in rest.split('.').filter(|part| !part.is_empty()) {
            parts.push(segment.to_string());
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

fn resolve_module_to_file(repo_root: &Path, module: &str) -> Option<String> {
    let parts: Vec<&str> = module.split('.').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        return None;
    }
    if !package_prefixes_have_init(repo_root, &parts) {
        return None;
    }
    let mut rel = std::path::PathBuf::new();
    for part in &parts {
        rel.push(part);
    }
    let module_file = rel.with_extension("py");
    if repo_root.join(&module_file).is_file() {
        return Some(util::normalize_path(&module_file));
    }
    let package_init = rel.join("__init__.py");
    if repo_root.join(&package_init).is_file() {
        return Some(util::normalize_path(&package_init));
    }
    None
}

fn package_prefixes_have_init(repo_root: &Path, parts: &[&str]) -> bool {
    if parts.len() <= 1 {
        return true;
    }
    let mut rel = std::path::PathBuf::new();
    for part in &parts[..parts.len() - 1] {
        rel.push(part);
        let init = repo_root.join(&rel).join("__init__.py");
        if !init.is_file() {
            return false;
        }
    }
    true
}

fn line_count(source: &str) -> i64 {
    let count = source.lines().count();
    if count == 0 { 1 } else { count as i64 }
}

#[cfg(test)]
mod tests {
    use super::PythonExtractor;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_fastapi_route_and_requests_call() {
        let source = r#"
from fastapi import FastAPI
import requests
app = FastAPI()

@app.get("/api/users/{id}")
def handler():
    pass

requests.post("/api/users/123")
"#;
        let mut extractor = PythonExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
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
    fn extracts_grpc_impl_and_call() {
        let source = r#"
import foo_pb2_grpc

class UserService(foo_pb2_grpc.UserServiceServicer):
    def GetUser(self, request, context):
        pass

def main(channel):
    foo_pb2_grpc.UserServiceStub(channel).GetUser(None)
"#;
        let mut extractor = PythonExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
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
        assert!(
            impls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/userservice/getuser"))
        );
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/userservice/getuser"))
        );
    }
}
