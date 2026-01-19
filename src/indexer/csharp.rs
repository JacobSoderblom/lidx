use crate::indexer::channel;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::indexer::proto;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
    namespace_stack: Vec<String>,
    type_stack: Vec<String>,
    fn_depth: usize,
    current_scope: String,
    route_prefix: Option<String>,
    route_groups: HashMap<String, String>,
    grpc_service: Option<String>,
    grpc_clients: HashMap<String, String>,
}

pub struct CSharpExtractor {
    parser: Parser,
}

impl CSharpExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_c_sharp::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }

    pub fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
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
        let module_span = span(root);
        output
            .symbols
            .push(module_symbol_with_span(module_name, module_span));
        let ctx = Context {
            module: module_name.to_string(),
            namespace_stack: Vec::new(),
            type_stack: Vec::new(),
            fn_depth: 0,
            current_scope: module_name.to_string(),
            route_prefix: None,
            route_groups: HashMap::new(),
            grpc_service: None,
            grpc_clients: HashMap::new(),
        };
        if root.kind() == "compilation_unit" {
            walk_compilation_unit(root, &ctx, source, &mut output);
        } else {
            walk_node(root, &ctx, source, &mut output);
        }
        Ok(output)
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "module".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if !stem.is_empty() {
        parts.push(stem);
    }
    if parts.is_empty() {
        "module".to_string()
    } else {
        parts.join("/")
    }
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

fn walk_compilation_unit(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut file_ns_name = None;
    let mut file_ns_span = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "file_scoped_namespace_declaration" {
            file_ns_name = namespace_name(child, source);
            file_ns_span = Some(span(child));
            break;
        }
    }

    let mut next_ctx = ctx.clone();
    if let Some(name) = file_ns_name {
        let parts = namespace_parts(&name);
        let qualname = parts.join(".");
        if !qualname.is_empty() {
            let name = parts.last().cloned().unwrap_or_else(|| qualname.clone());
            let span = file_ns_span.unwrap_or_else(|| span(node));
            output.symbols.push(SymbolInput {
                kind: "namespace".to_string(),
                name,
                qualname: qualname.clone(),
                start_line: span.0,
                start_col: span.1,
                end_line: span.2,
                end_col: span.3,
                start_byte: span.4,
                end_byte: span.5,
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
            next_ctx.namespace_stack = parts;
            next_ctx.current_scope = qualname;
        }
    }
    next_ctx.route_groups = collect_global_route_groups(node, source);
    next_ctx.grpc_clients = collect_global_grpc_clients(node, source);

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "file_scoped_namespace_declaration" {
            continue;
        }
        walk_node(child, &next_ctx, source, output);
    }
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "invocation_expression" || node.kind() == "object_creation_expression" {
        handle_call(node, ctx, source, output);
    }
    if is_nested_function_node(node.kind()) {
        return;
    }
    match node.kind() {
        "namespace_declaration" => {
            handle_namespace(node, ctx, source, output);
            return;
        }
        "class_declaration" => {
            handle_type(node, ctx, source, output, "class", TypeKind::Class);
            return;
        }
        "struct_declaration" => {
            handle_type(node, ctx, source, output, "struct", TypeKind::Struct);
            return;
        }
        "interface_declaration" => {
            handle_type(node, ctx, source, output, "interface", TypeKind::Interface);
            return;
        }
        "record_declaration" => {
            handle_type(node, ctx, source, output, "record", TypeKind::Record);
            return;
        }
        "enum_declaration" => {
            handle_type(node, ctx, source, output, "enum", TypeKind::Enum);
            return;
        }
        "method_declaration" => {
            handle_method(node, ctx, source, output);
            return;
        }
        "property_declaration" => {
            handle_property(node, ctx, source, output);
            return;
        }
        "field_declaration" => {
            handle_field(node, ctx, source, output);
            return;
        }
        "using_directive" => {
            handle_using(node, ctx, source, output);
            return;
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_namespace(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name) = namespace_name(node, source) else {
        return;
    };
    let parts = namespace_parts(&name);
    if parts.is_empty() {
        return;
    }
    let mut next_ctx = ctx.clone();
    let mut full_parts = next_ctx.namespace_stack.clone();
    full_parts.extend(parts.clone());
    let qualname = full_parts.join(".");
    let name = parts.last().cloned().unwrap_or_else(|| qualname.clone());
    let span = span(node);
    output.symbols.push(SymbolInput {
        kind: "namespace".to_string(),
        name,
        qualname: qualname.clone(),
        start_line: span.0,
        start_col: span.1,
        end_line: span.2,
        end_col: span.3,
        start_byte: span.4,
        end_byte: span.5,
        signature: None,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    next_ctx.namespace_stack = full_parts;
    next_ctx.current_scope = qualname;
    if let Some(body) = node.child_by_field_name("body") {
        walk_declaration_list(body, &next_ctx, source, output);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeKind {
    Class,
    Struct,
    Interface,
    Record,
    Enum,
}

fn handle_type(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    kind: &str,
    type_kind: TypeKind,
) {
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
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = type_signature(node, source);
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
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    if type_kind != TypeKind::Enum {
        handle_base_list(node, &qualname, source, output, type_kind);
    }

    let grpc_service = grpc_service_from_bases(node, source);
    let class_prefix = route_prefix_from_attributes(node, source);
    let combined_prefix =
        combine_route_prefix(ctx.route_prefix.as_deref(), class_prefix.as_deref());
    let mut next_ctx = ctx.clone();
    next_ctx.type_stack.push(name);
    next_ctx.current_scope = qualname;
    next_ctx.route_prefix = combined_prefix;
    next_ctx.grpc_service = grpc_service;
    if let Some(body) = node.child_by_field_name("body") {
        walk_declaration_list(body, &next_ctx, source, output);
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
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = method_signature(node, source);
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
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    if let Some(edge) = grpc_impl_edge(node, ctx, source, &name) {
        output.edges.push(edge);
    }
    for edge in route_edges_from_method_attributes(node, ctx, source, &qualname) {
        output.edges.push(edge);
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = qualname;
        next_ctx.route_groups = collect_route_groups(body, source);
        let mut grpc_clients = ctx.grpc_clients.clone();
        grpc_clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = grpc_clients;
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_property(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "property".to_string(),
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
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

fn handle_field(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declaration" {
            continue;
        }
        handle_variable_declaration(child, ctx, source, output);
    }
}

fn handle_variable_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
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
        let qualname = build_qualname(ctx, &name);
        let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
        output.symbols.push(SymbolInput {
            kind: "field".to_string(),
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
            source_qualname: Some(container_qualname(ctx)),
            target_qualname: Some(qualname),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn handle_using(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    let mut target = None;
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type" {
            let name = node_text(child, source);
            if !name.is_empty() {
                target = Some(name);
                break;
            }
        }
    }
    if target.is_none() {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "qualified_name" | "identifier" | "generic_name" | "alias_qualified_name" => {
                    let name = node_text(child, source);
                    if !name.is_empty() {
                        target = Some(name);
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    let Some(target) = target else {
        return;
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(base_qualname(ctx)),
        target_qualname: Some(target),
        detail: None,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
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
    if let Some(edge) = channel_publish_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_subscribe_edge(node, ctx, source) {
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

#[derive(Clone)]
struct AttributeInfo<'a> {
    name: String,
    args: Vec<Node<'a>>,
    node: Node<'a>,
}

struct CallTarget {
    receiver: Option<String>,
    name: String,
    full: String,
}

fn route_prefix_from_attributes(node: Node<'_>, source: &str) -> Option<String> {
    for attr in attributes_for_node(node, source) {
        let name = normalize_attribute_name(&attr.name);
        if name == "Route" || name == "RoutePrefix" {
            if let Some(template) = attribute_first_string_arg(&attr, source) {
                return Some(template);
            }
        }
    }
    None
}

fn combine_route_prefix(prefix: Option<&str>, next: Option<&str>) -> Option<String> {
    match (prefix, next) {
        (Some(prefix), Some(next)) => Some(http::join_paths(prefix, next)),
        (Some(prefix), None) => Some(prefix.to_string()),
        (None, Some(next)) => Some(next.to_string()),
        (None, None) => None,
    }
}

fn route_edges_from_method_attributes(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    handler: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let attrs = attributes_for_node(node, source);
    if attrs.is_empty() {
        return edges;
    }
    let mut route_template = None;
    let mut route_node = None;
    let mut saw_route_attr = false;
    let mut method_edges: Vec<(String, Option<String>, Node<'_>)> = Vec::new();
    for attr in attrs {
        let name = normalize_attribute_name(&attr.name);
        if name == "Route" || name == "RoutePrefix" {
            saw_route_attr = true;
            if route_template.is_none() {
                route_template = attribute_first_string_arg(&attr, source);
                route_node = Some(attr.node);
            }
            continue;
        }
        if name == "AcceptVerbs" {
            for method in attribute_string_list(&attr, source) {
                if let Some(method) = http::normalize_method(&method) {
                    method_edges.push((method, None, attr.node));
                }
            }
            continue;
        }
        if let Some(method) = http_method_from_attribute(&name) {
            let path = attribute_first_string_arg(&attr, source);
            method_edges.push((method, path, attr.node));
        }
    }

    let prefix = ctx.route_prefix.as_deref();
    if !method_edges.is_empty() {
        for (method, path, node) in method_edges {
            let raw_path = combine_route(prefix, path.as_deref().or(route_template.as_deref()));
            let Some(raw_path) = raw_path else {
                continue;
            };
            if let Some(edge) =
                build_route_edge(handler, &method, &raw_path, "aspnet", node, source)
            {
                edges.push(edge);
            }
        }
        return edges;
    }

    if saw_route_attr {
        let raw_path = combine_route(prefix, route_template.as_deref());
        let Some(raw_path) = raw_path else {
            return edges;
        };
        if let Some(node) = route_node {
            if let Some(edge) =
                build_route_edge(handler, http::HTTP_ANY, &raw_path, "aspnet", node, source)
            {
                edges.push(edge);
            }
        }
    }
    edges
}

fn combine_route(prefix: Option<&str>, path: Option<&str>) -> Option<String> {
    match (prefix, path) {
        (Some(prefix), Some(path)) => Some(http::join_paths(prefix, path)),
        (Some(prefix), None) => Some(http::join_paths(prefix, "")),
        (None, Some(path)) => Some(path.to_string()),
        (None, None) => None,
    }
}

fn attributes_for_node<'a>(node: Node<'a>, source: &str) -> Vec<AttributeInfo<'a>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "attribute_list" {
            continue;
        }
        let mut list_cursor = child.walk();
        for attr in child.named_children(&mut list_cursor) {
            if attr.kind() != "attribute" {
                continue;
            }
            let Some(name_node) = attr.child_by_field_name("name") else {
                continue;
            };
            let raw_name = node_text(name_node, source);
            if raw_name.is_empty() {
                continue;
            }
            let args = attribute_argument_exprs(attr);
            out.push(AttributeInfo {
                name: raw_name,
                args,
                node: attr,
            });
        }
    }
    out
}

fn attribute_argument_exprs(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "attribute_argument_list" {
            continue;
        }
        let mut arg_cursor = child.walk();
        for arg in child.named_children(&mut arg_cursor) {
            if arg.kind() != "attribute_argument" {
                continue;
            }
            if let Some(expr) = attribute_argument_expr(arg) {
                out.push(expr);
            }
        }
    }
    out
}

fn attribute_argument_expr(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    let mut expr = None;
    for child in node.named_children(&mut cursor) {
        expr = Some(child);
    }
    expr
}

fn attribute_first_string_arg(attr: &AttributeInfo<'_>, source: &str) -> Option<String> {
    for arg in &attr.args {
        if let Some(value) = extract_string_literal(*arg, source) {
            return Some(value);
        }
    }
    None
}

fn attribute_string_list(attr: &AttributeInfo<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for arg in &attr.args {
        out.extend(extract_string_list(*arg, source));
    }
    out
}

fn normalize_attribute_name(raw: &str) -> String {
    let name = raw.rsplit('.').next().unwrap_or(raw).to_string();
    name.strip_suffix("Attribute").unwrap_or(&name).to_string()
}

fn http_method_from_attribute(name: &str) -> Option<String> {
    let Some(rest) = name.strip_prefix("Http") else {
        return None;
    };
    if rest.is_empty() {
        return None;
    }
    http::normalize_method(rest)
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

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    edges.extend(route_edges_from_map_call(node, ctx, source));
    edges
}

fn route_edges_from_map_call(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if node.kind() != "invocation_expression" {
        return edges;
    }
    let Some(target_node) = node.child_by_field_name("function") else {
        return edges;
    };
    let Some(target) = call_target_parts(target_node, source) else {
        return edges;
    };
    if !target.name.starts_with("Map") {
        return edges;
    }
    let args = call_arguments(node);
    let Some(raw_path) = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))
    else {
        return edges;
    };
    let group_prefix = map_group_prefix_from_receiver(target_node, ctx, source);
    let prefix = combine_route_prefix(ctx.route_prefix.as_deref(), group_prefix.as_deref());
    let raw_path = combine_route(prefix.as_deref(), Some(&raw_path)).unwrap_or(raw_path);
    let mut methods = Vec::new();
    let handler = if target.name == "MapMethods" {
        let list = args.get(1).map(|arg| extract_string_list(*arg, source));
        if let Some(list) = list {
            methods.extend(
                list.into_iter()
                    .filter_map(|method| http::normalize_method(&method)),
            );
        }
        if methods.is_empty() {
            methods.push(http::HTTP_ANY.to_string());
        }
        args.get(2)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    } else if target.name == "Map" {
        methods.push(http::HTTP_ANY.to_string());
        args.get(1)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    } else {
        let Some(method) = target.name.strip_prefix("Map") else {
            return edges;
        };
        let Some(method) = http::normalize_method(method) else {
            return edges;
        };
        methods.push(method);
        args.get(1)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    };
    let handler = handler.unwrap_or_else(|| ctx.current_scope.clone());
    for method in methods {
        if let Some(edge) = build_route_edge(&handler, &method, &raw_path, "aspnet", node, source) {
            edges.push(edge);
        }
    }
    edges
}

fn map_group_prefix_from_receiver(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    if node.kind() != "member_access_expression" {
        return None;
    }
    let receiver = node.child_by_field_name("expression")?;
    if receiver.kind() == "invocation_expression" {
        return map_group_prefix_from_invocation(receiver, source);
    }
    let receiver_text = node_text(receiver, source);
    if receiver_text.is_empty() {
        return None;
    }
    if let Some(prefix) = ctx.route_groups.get(&receiver_text) {
        return Some(prefix.clone());
    }
    if let Some(last) = receiver_text.rsplit('.').next() {
        if let Some(prefix) = ctx.route_groups.get(last) {
            return Some(prefix.clone());
        }
    }
    None
}

fn map_group_prefix_from_invocation(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    if target.name != "MapGroup" {
        return None;
    }
    let args = call_arguments(node);
    let path = args
        .get(0)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let mut prefix = path;
    if function.kind() == "member_access_expression" {
        if let Some(receiver) = function.child_by_field_name("expression") {
            if let Some(parent) = map_group_prefix_from_invocation(receiver, source) {
                prefix = http::join_paths(&parent, &prefix);
            }
        }
    }
    Some(prefix)
}

fn collect_global_route_groups(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut groups = HashMap::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "global_statement" {
            continue;
        }
        collect_route_groups_inner(child, source, &mut groups);
    }
    groups
}

fn collect_route_groups(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut groups = HashMap::new();
    collect_route_groups_inner(node, source, &mut groups);
    groups
}

fn collect_global_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut clients = HashMap::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "global_statement" {
            continue;
        }
        collect_grpc_clients_inner(child, source, &mut clients);
    }
    clients
}

fn collect_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, &mut clients);
    clients
}

fn collect_route_groups_inner(node: Node<'_>, source: &str, groups: &mut HashMap<String, String>) {
    match node.kind() {
        "method_declaration"
        | "local_function_statement"
        | "class_declaration"
        | "struct_declaration"
        | "record_declaration"
        | "interface_declaration"
        | "enum_declaration" => {
            return;
        }
        _ => {}
    }
    if node.kind() == "variable_declarator" {
        if let Some((name, prefix)) = route_group_from_declarator(node, source) {
            groups.insert(name, prefix);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_route_groups_inner(child, source, groups);
    }
}

fn collect_grpc_clients_inner(node: Node<'_>, source: &str, clients: &mut HashMap<String, String>) {
    match node.kind() {
        "method_declaration"
        | "local_function_statement"
        | "class_declaration"
        | "struct_declaration"
        | "record_declaration"
        | "interface_declaration"
        | "enum_declaration" => {
            return;
        }
        _ => {}
    }
    if node.kind() == "variable_declarator" {
        if let Some((name, service)) = grpc_client_from_declarator(node, source) {
            clients.insert(name, service);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, clients);
    }
}

fn route_group_from_declarator(node: Node<'_>, source: &str) -> Option<(String, String)> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let prefix = node
        .child_by_field_name("initializer")
        .and_then(|initializer| map_group_prefix_in_node(initializer, source))
        .or_else(|| map_group_prefix_in_node(node, source))?;
    Some((name, prefix))
}

fn map_group_prefix_in_node(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "invocation_expression" {
        if let Some(prefix) = map_group_prefix_from_invocation(node, source) {
            return Some(prefix);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(prefix) = map_group_prefix_in_node(child, source) {
            return Some(prefix);
        }
    }
    None
}

fn grpc_client_from_declarator(node: Node<'_>, source: &str) -> Option<(String, String)> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let service = node
        .child_by_field_name("initializer")
        .and_then(|initializer| grpc_client_from_initializer(initializer, source))
        .or_else(|| grpc_client_from_initializer(node, source))?;
    Some((name, service))
}

fn grpc_client_from_initializer(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "object_creation_expression" {
        if let Some(service) = grpc_client_from_object_creation(node, source) {
            return Some(service);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(service) = grpc_client_from_initializer(child, source) {
            return Some(service);
        }
    }
    None
}

fn grpc_client_from_object_creation(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() != "object_creation_expression" {
        return None;
    }
    let type_node = node.child_by_field_name("type")?;
    let type_name = node_text(type_node, source);
    let type_name = type_name.trim();
    if type_name.is_empty() {
        return None;
    }
    let last = type_name.rsplit('.').next().unwrap_or(type_name).trim();
    let last = last.split('<').next().unwrap_or(last).trim();
    if let Some(service) = last.strip_suffix("Client") {
        if !service.is_empty() {
            return Some(service.to_string());
        }
    }
    None
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let Some(target_node) = node.child_by_field_name("function") else {
        return None;
    };
    let target = call_target_parts(target_node, source)?;
    let client = http_client_label(target.receiver.as_deref(), &target.full)?;
    let args = call_arguments(node);
    let (method, raw_path) = if target.name == "SendAsync" || target.name == "Send" {
        http_request_message_parts(args.get(0).copied()?, source)?
    } else if let Some(method) = normalize_http_method_name(&target.name) {
        let raw_path = args
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
    let package = grpc_package_from_namespace(ctx);
    let (raw_path, normalized) = proto::normalize_rpc_path(package.as_deref(), service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-csharp",
        "role": "server",
        "service": service,
        "rpc": rpc_name,
        "package": package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(build_qualname(ctx, rpc_name)),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let Some(target_node) = node.child_by_field_name("function") else {
        return None;
    };
    let target = call_target_parts(target_node, source)?;
    let rpc_name = normalize_grpc_method_name(&target.name)?;
    let service = grpc_service_from_client_receiver(target.receiver.as_deref())
        .or_else(|| grpc_service_from_client_binding(target.receiver.as_deref(), ctx))?;
    let package = grpc_package_from_namespace(ctx);
    let (raw_path, normalized) =
        proto::normalize_rpc_path(package.as_deref(), &service, &rpc_name)?;
    let detail = json!({
        "framework": "grpc-csharp",
        "role": "client",
        "service": service,
        "rpc": rpc_name,
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

fn channel_publish_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let target_node = node.child_by_field_name("function")?;
    let target = call_target_parts(target_node, source)?;
    if !channel::is_publish_method(&target.name) {
        return None;
    }
    if !channel::is_bus_receiver(target.receiver.as_deref().unwrap_or("")) {
        return None;
    }
    let args = call_arguments(node);
    let first_arg = args.first()?;
    let raw_topic = node_text(*first_arg, source);
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = channel::build_publish_detail(&normalized, &raw_topic, "azure-service-bus");
    Some(EdgeInput {
        kind: channel::CHANNEL_PUBLISH_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn channel_subscribe_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let target_node = node.child_by_field_name("function")?;
    let target = call_target_parts(target_node, source)?;
    if !channel::is_subscribe_method(&target.name) {
        return None;
    }
    if !channel::is_bus_receiver(target.receiver.as_deref().unwrap_or("")) {
        return None;
    }
    let args = call_arguments(node);
    let first_arg = args.first()?;
    let raw_topic = node_text(*first_arg, source);
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = channel::build_subscribe_detail(&normalized, &raw_topic, "azure-service-bus");
    Some(EdgeInput {
        kind: channel::CHANNEL_SUBSCRIBE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn normalize_grpc_method_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(base) = trimmed.strip_suffix("Async") {
        if !base.is_empty() {
            return Some(base.to_string());
        }
    }
    Some(trimmed.to_string())
}

fn grpc_service_from_bases(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        let bases = base_list_types(child, source);
        for base in bases {
            if let Some(service) = grpc_service_from_base(&base) {
                return Some(service);
            }
        }
    }
    None
}

fn grpc_service_from_base(base: &str) -> Option<String> {
    let trimmed = base.trim();
    if trimmed.is_empty() || !trimmed.contains('.') {
        return None;
    }
    let mut parts = trimmed.rsplit('.');
    let last = parts.next()?.trim();
    let prev = parts.next()?.trim();
    let last = last.split('<').next().unwrap_or(last).trim();
    if !last.ends_with("Base") {
        return None;
    }
    let service = last.trim_end_matches("Base");
    if service.is_empty() {
        return None;
    }
    if prev != service {
        return None;
    }
    Some(service.to_string())
}

fn grpc_package_from_namespace(ctx: &Context) -> Option<String> {
    if ctx.namespace_stack.is_empty() {
        return None;
    }
    Some(ctx.namespace_stack.join("."))
}

fn grpc_service_from_client_receiver(receiver: Option<&str>) -> Option<String> {
    let mut value = receiver?.trim().to_string();
    if value.is_empty() {
        return None;
    }
    if let Some(idx) = value.find('(') {
        value.truncate(idx);
    }
    value = value.trim_start_matches("new ").trim().to_string();
    let last = value.rsplit('.').next().unwrap_or(value.as_str()).trim();
    if last.is_empty() {
        return None;
    }
    if let Some(service) = last.strip_suffix("Client") {
        if !service.is_empty() {
            return Some(service.to_string());
        }
    }
    let lower = last.to_ascii_lowercase();
    if lower.ends_with("client") {
        let service = &last[..last.len() - 6];
        if !service.is_empty() {
            return Some(service.to_string());
        }
    }
    None
}

fn grpc_service_from_client_binding(receiver: Option<&str>, ctx: &Context) -> Option<String> {
    let receiver = receiver?.trim();
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
    None
}

fn http_request_message_parts(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "object_creation_expression" {
        return None;
    }
    let type_node = node.child_by_field_name("type")?;
    let type_name = node_text(type_node, source);
    if !type_name.ends_with("HttpRequestMessage") {
        return None;
    }
    let args = node
        .child_by_field_name("arguments")
        .map(argument_values)
        .unwrap_or_default();
    let method = args
        .get(0)
        .and_then(|arg| extract_method_from_expr(*arg, source))?;
    let raw_path = args
        .get(1)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    Some((method, raw_path))
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    node.child_by_field_name("arguments")
        .map(argument_values)
        .unwrap_or_default()
}

fn argument_values(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "argument" {
            continue;
        }
        if let Some(expr) = argument_expr(child) {
            out.push(expr);
        }
    }
    out
}

fn argument_expr(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    let mut expr = None;
    for child in node.named_children(&mut cursor) {
        expr = Some(child);
    }
    expr
}

fn call_target_parts(node: Node<'_>, source: &str) -> Option<CallTarget> {
    let full = node_text(node, source);
    if full.is_empty() {
        return None;
    }
    if node.kind() == "member_access_expression" {
        let receiver = node
            .child_by_field_name("expression")
            .map(|expr| node_text(expr, source))
            .filter(|value| !value.is_empty());
        let name = node
            .child_by_field_name("name")
            .map(|name| node_text(name, source))
            .unwrap_or_else(|| full.clone());
        return Some(CallTarget {
            receiver,
            name,
            full,
        });
    }
    let (receiver, name) = split_last_segment(&full);
    Some(CallTarget {
        receiver,
        name,
        full,
    })
}

fn split_last_segment(raw: &str) -> (Option<String>, String) {
    if let Some((left, right)) = raw.rsplit_once('.') {
        return (Some(left.to_string()), right.to_string());
    }
    (None, raw.to_string())
}

fn handler_name_from_expr(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    resolve_call_target(&raw, ctx).or(Some(raw))
}

fn normalize_http_method_name(name: &str) -> Option<String> {
    if let Some(method) = http::normalize_method(name) {
        return Some(method);
    }
    let mut trimmed = name.to_string();
    for suffix in ["FromJsonAsync", "AsJsonAsync", "JsonAsync", "Async"] {
        if trimmed.ends_with(suffix) {
            let end = trimmed.len() - suffix.len();
            trimmed.truncate(end);
            break;
        }
    }
    http::normalize_method(&trimmed)
}

fn extract_method_from_expr(node: Node<'_>, source: &str) -> Option<String> {
    if let Some(raw) = extract_string_literal(node, source) {
        return http::normalize_method(&raw);
    }
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let last = raw.rsplit('.').next().unwrap_or(raw.as_str());
    http::normalize_method(last)
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    match node.kind() {
        "string_literal" | "verbatim_string_literal" | "raw_string_literal" => {
            let raw = node_text(node, source);
            unquote_string_literal(&raw)
        }
        _ => None,
    }
}

fn extract_string_list(node: Node<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(value) = extract_string_literal(node, source) {
        out.push(value);
        return out;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        out.extend(extract_string_list(child, source));
    }
    out
}

fn unquote_string_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("@\"") {
        if rest.ends_with('"') {
            let value = &rest[..rest.len() - 1];
            return Some(value.replace("\"\"", "\""));
        }
    }
    let quote_count = trimmed.chars().take_while(|ch| *ch == '"').count();
    if quote_count >= 3 && trimmed.ends_with(&"\"".repeat(quote_count)) {
        let start = quote_count;
        let end = trimmed.len() - quote_count;
        if start <= end {
            return Some(trimmed[start..end].to_string());
        }
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    None
}

fn http_client_label(receiver: Option<&str>, full: &str) -> Option<&'static str> {
    let full_lower = full.to_ascii_lowercase();
    let receiver_lower = receiver.unwrap_or("").to_ascii_lowercase();
    if full_lower.contains("httpclient") || receiver_lower.contains("httpclient") {
        return Some("httpclient");
    }
    if receiver_lower.ends_with("client") || receiver_lower.contains("client") {
        return Some("http_client");
    }
    None
}

fn call_target_node(node: Node<'_>) -> Option<Node<'_>> {
    node.child_by_field_name("expression")
        .or_else(|| node.child_by_field_name("function"))
        .or_else(|| node.child_by_field_name("constructor"))
        .or_else(|| node.child_by_field_name("type"))
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    if let Some(rest) = raw.strip_prefix("this.") {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(rest.to_string());
        }
        return Some(format!("{container}.{rest}"));
    }
    if let Some(rest) = raw.strip_prefix("base.") {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(rest.to_string());
        }
        return Some(format!("{container}.{rest}"));
    }
    if !raw.contains('.') {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(raw.to_string());
        }
        return Some(format!("{container}.{raw}"));
    }
    Some(raw.to_string())
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '$' || ch == '@')
}

fn is_nested_function_node(kind: &str) -> bool {
    matches!(
        kind,
        "local_function_statement"
            | "anonymous_method_expression"
            | "lambda_expression"
            | "parenthesized_lambda_expression"
            | "simple_lambda_expression"
    )
}

fn walk_declaration_list(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_base_list(
    node: Node<'_>,
    qualname: &str,
    source: &str,
    output: &mut ExtractedFile,
    kind: TypeKind,
) {
    let mut cursor = node.walk();
    let mut bases = Vec::new();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        bases.extend(base_list_types(child, source));
    }
    if bases.is_empty() {
        return;
    }
    match kind {
        TypeKind::Class | TypeKind::Record => {
            let mut iter = bases.into_iter();
            if let Some(base) = iter.next() {
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(base),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
            for iface in iter {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Interface => {
            for iface in bases {
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Struct | TypeKind::Enum => {
            for iface in bases {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
    }
}

fn base_list_types(node: Node<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "argument_list" => {}
            "primary_constructor_base_type" => {
                if let Some(type_node) = child.child_by_field_name("type") {
                    let name = node_text(type_node, source);
                    if !name.is_empty() {
                        out.push(name);
                    }
                } else {
                    let name = node_text(child, source);
                    if !name.is_empty() {
                        out.push(name);
                    }
                }
            }
            _ => {
                let name = node_text(child, source);
                if !name.is_empty() {
                    out.push(name);
                }
            }
        }
    }
    out
}

fn namespace_name(node: Node<'_>, source: &str) -> Option<String> {
    node.child_by_field_name("name")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty())
}

fn namespace_parts(name: &str) -> Vec<String> {
    let normalized = name.replace("::", ".");
    normalized
        .split('.')
        .filter(|part| !part.trim().is_empty())
        .map(|part| part.trim().to_string())
        .collect()
}

fn type_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "parameter_list" {
                    let value = node_text(child, source);
                    if !value.is_empty() {
                        return Some(value);
                    }
                }
            }
            None
        });
    params
}

fn method_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let params = match params {
        Some(value) if !value.is_empty() => value,
        _ => return None,
    };
    let returns = node
        .child_by_field_name("returns")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty());
    match returns {
        Some(ret) => Some(format!("{params} -> {ret}")),
        None => Some(params),
    }
}

fn base_qualname(ctx: &Context) -> String {
    if !ctx.namespace_stack.is_empty() {
        ctx.namespace_stack.join(".")
    } else {
        ctx.module.clone()
    }
}

fn build_qualname(ctx: &Context, name: &str) -> String {
    let mut parts = Vec::new();
    let base = base_qualname(ctx);
    if !base.is_empty() {
        parts.push(base);
    }
    if !ctx.type_stack.is_empty() {
        parts.push(ctx.type_stack.join("."));
    }
    parts.push(name.to_string());
    parts.join(".")
}

fn container_qualname(ctx: &Context) -> String {
    let base = base_qualname(ctx);
    if ctx.type_stack.is_empty() {
        base
    } else if base.is_empty() {
        ctx.type_stack.join(".")
    } else {
        format!("{base}.{}", ctx.type_stack.join("."))
    }
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
    use super::CSharpExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_map_route_and_httpclient_call() {
        let source = r#"
var app = WebApplication.Create();
app.MapGet("/api/users/{id}", Handle);
var client = new HttpClient();
client.GetAsync("/api/users/123");
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
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
    fn extracts_mapgroup_routes() {
        let source = r#"
var app = WebApplication.Create();
var group = app.MapGroup("/api");
group.MapGet("/users/{id}", Handle);
app.MapGroup("/admin").MapPost("/users", HandlePost);
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let routes = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_ROUTE_KIND)
            .collect::<Vec<_>>();
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/admin/users"))
        );
    }

    #[test]
    fn extracts_grpc_impl_and_call() {
        let source = r#"
using Grpc.Core;

namespace Example.V1 {
  public class GreeterService : Greeter.GreeterBase {
    public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context) {
      return Task.FromResult(new HelloReply());
    }
  }
}

var client = new Greeter.GreeterClient(channel);
client.SayHelloAsync(new HelloRequest());
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
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
        assert!(impls
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")));
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/greeter/sayhello"))
        );
    }
}
