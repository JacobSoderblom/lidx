use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::indexer::proto;
use crate::indexer::tree_helpers::{
    module_symbol_fallback, module_symbol_with_span, node_text, span,
};
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use tree_sitter::{Node, Parser};

/// A local variable declaration tagged with the byte range of the block that
/// directly contains it, for scope-aware resolution.
#[derive(Clone)]
struct ScopedVarType {
    name: String,
    type_name: String,
    /// Byte offset of the start of the containing block / func_literal.
    block_start: usize,
    /// Byte offset of the end of the containing block / func_literal.
    block_end: usize,
}

#[derive(Clone)]
struct Context {
    module: String,
    current_scope: String,
    grpc_servers: HashMap<String, GrpcServerInfo>,
    grpc_clients: HashMap<String, GrpcClientInfo>,
    /// Local variable declarations with their containing-block byte ranges.
    local_var_types: Vec<ScopedVarType>,
}

#[derive(Clone)]
struct GrpcServerInfo {
    service: String,
}

#[derive(Clone)]
struct GrpcClientInfo {
    service: String,
}

pub struct GoExtractor {
    parser: Parser,
}

impl GoExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_go::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for GoExtractor {
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
                    .push(module_symbol_fallback(module_name, source, "/", None));
                return Ok(output);
            }
        };
        let root = tree.root_node();

        let module_span = span(root);
        output
            .symbols
            .push(module_symbol_with_span(module_name, module_span, "/", None));

        // First pass: collect gRPC server types
        let grpc_servers = collect_grpc_servers(root, source);

        let ctx = Context {
            module: module_name.to_string(),
            current_scope: module_name.to_string(),
            grpc_servers,
            grpc_clients: HashMap::new(),
            local_var_types: Vec::new(),
        };
        walk_node(root, &ctx, source, &mut output);
        Ok(output)
    }

    fn resolve_imports(
        &self,
        _repo_root: &Path,
        _file_rel_path: &str,
        _module_name: &str,
        edges: &mut Vec<EdgeInput>,
    ) {
        // For now, just add detail field with import path
        for edge in edges.iter_mut() {
            if edge.kind == "IMPORTS"
                && let Some(target) = edge.target_qualname.as_ref()
            {
                edge.detail = Some(
                    json!({
                        "import_path": target,
                    })
                    .to_string(),
                );
            }
        }
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "main".to_string();
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
        "main".to_string()
    } else {
        parts.join("/")
    }
}

fn collect_grpc_servers(root: Node<'_>, source: &str) -> HashMap<String, GrpcServerInfo> {
    let mut servers = HashMap::new();
    let mut cursor = root.walk();
    for child in root.named_children(&mut cursor) {
        if child.kind() == "type_declaration" {
            collect_grpc_servers_from_type_decl(child, source, &mut servers);
        }
    }
    servers
}

fn collect_grpc_servers_from_type_decl(
    node: Node<'_>,
    source: &str,
    servers: &mut HashMap<String, GrpcServerInfo>,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type_spec" {
            let Some(name_node) = child.child_by_field_name("name") else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }

            let Some(type_node) = child.child_by_field_name("type") else {
                continue;
            };

            if type_node.kind() == "struct_type"
                && is_grpc_server_struct(type_node, source)
                && let Some(service) = extract_grpc_service_from_server_name(&name)
            {
                servers.insert(name, GrpcServerInfo { service });
            }
        }
    }
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    match node.kind() {
        "source_file" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_node(child, ctx, source, output);
            }
        }
        "function_declaration" => {
            handle_function(node, ctx, source, output);
        }
        "method_declaration" => {
            handle_method(node, ctx, source, output);
        }
        "type_declaration" => {
            handle_type_declaration(node, ctx, source, output);
        }
        "const_declaration" => {
            handle_const_declaration(node, ctx, source, output);
        }
        "var_declaration" => {
            handle_var_declaration(node, ctx, source, output);
        }
        "import_declaration" => {
            handle_import(node, ctx, source, output);
        }
        "call_expression" => {
            handle_call(node, ctx, source, output);
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_node(child, ctx, source, output);
            }
        }
    }
}

fn handle_function(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = format!("{}.{}", ctx.module, name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_function_signature(node, source);
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
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = qualname;
    if let Some(body) = node.child_by_field_name("body") {
        // Collect gRPC clients from variable declarations in the function body
        let mut clients = ctx.grpc_clients.clone();
        clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = clients;
        // Collect local variable types for config bind resolution (scope-aware)
        let mut var_types = ctx.local_var_types.clone();
        var_types.extend(collect_local_var_types(body, source));
        next_ctx.local_var_types = var_types;
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

    let receiver_type = extract_receiver_type(node, source);
    let qualname = if let Some(ref rtype) = receiver_type {
        format!("{}.{}.{}", ctx.module, rtype, name)
    } else {
        format!("{}.{}", ctx.module, name)
    };

    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_function_signature(node, source);
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

    if let Some(ref rtype) = receiver_type {
        let parent_qualname = format!("{}.{}", ctx.module, rtype);
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(parent_qualname),
            target_qualname: Some(qualname.clone()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });

        // Check if this is a gRPC server implementation
        if let Some(edge) = grpc_impl_edge(node, ctx, source, &name, &qualname, rtype) {
            output.edges.push(edge);
        }
    }

    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = qualname;
    if let Some(body) = node.child_by_field_name("body") {
        // Collect gRPC clients from variable declarations in the method body
        let mut clients = ctx.grpc_clients.clone();
        clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = clients;
        // Collect local variable types for config bind resolution
        let mut var_types = ctx.local_var_types.clone();
        var_types.extend(collect_local_var_types(body, source));
        next_ctx.local_var_types = var_types;
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_type_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type_spec" {
            handle_type_spec(child, ctx, source, output);
        }
    }
}

fn handle_type_spec(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }

    let Some(type_node) = node.child_by_field_name("type") else {
        return;
    };

    let kind = match type_node.kind() {
        "struct_type" => "class",
        "interface_type" => {
            // Check for interface embedding (extends)
            handle_interface_embedding(node, ctx, source, output, &name);
            "interface"
        }
        _ => "type",
    };

    let qualname = format!("{}.{}", ctx.module, name);
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
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

fn handle_const_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let name = if child.kind() == "const_spec" {
            extract_name_from_spec(child, source)
        } else {
            None
        };
        if let Some(name) = name {
            let qualname = format!("{}.{}", ctx.module, name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
            output.symbols.push(SymbolInput {
                kind: "variable".to_string(),
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
}

fn handle_var_declaration(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // Only extract top-level vars (current_scope == module)
    if ctx.current_scope != ctx.module {
        return;
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let name = if child.kind() == "var_spec" {
            extract_name_from_spec(child, source)
        } else {
            None
        };
        if let Some(name) = name {
            let qualname = format!("{}.{}", ctx.module, name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
            output.symbols.push(SymbolInput {
                kind: "variable".to_string(),
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
}

fn handle_import(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "import_spec" || child.kind() == "import_spec_list" {
            extract_imports_from_node(child, ctx, source, output);
        }
    }
}

fn extract_imports_from_node(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    if node.kind() == "import_spec" {
        if let Some(path_node) = node.child_by_field_name("path") {
            let import_path = extract_string_literal(path_node, source);
            if !import_path.is_empty() {
                let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
                let snippet =
                    util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
                output.edges.push(EdgeInput {
                    kind: "IMPORTS".to_string(),
                    source_qualname: Some(ctx.module.clone()),
                    target_qualname: Some(import_path),
                    detail: None,
                    evidence_snippet: snippet,
                    evidence_start_line: Some(start_line),
                    evidence_end_line: Some(end_line),
                    ..Default::default()
                });
            }
        }
    } else {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            extract_imports_from_node(child, ctx, source, output);
        }
    }
}

fn handle_interface_embedding(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    interface_name: &str,
) {
    let Some(type_node) = node.child_by_field_name("type") else {
        return;
    };
    if type_node.kind() != "interface_type" {
        return;
    }

    let mut cursor = type_node.walk();
    for child in type_node.named_children(&mut cursor) {
        if child.kind() == "qualified_type" || child.kind() == "type_identifier" {
            let base_type = node_text(child, source);
            if !base_type.is_empty() {
                let source_qualname = format!("{}.{}", ctx.module, interface_name);
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(source_qualname),
                    target_qualname: Some(base_type),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // Try HTTP route detection
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try HTTP call detection
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try gRPC call detection
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try channel/bus detection
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Config/env read detection
    if let Some(edge) = config_read_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // viper.Unmarshal / viper.UnmarshalKey → CONFIG_BIND
    if let Some(edge) = viper_config_bind_edge(node, ctx, source) {
        output.edges.push(edge);
    }

    // General CALLS edge
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

/// Detect os.Getenv("KEY"), os.LookupEnv("KEY"), viper.GetString("key"), viper.Get("key") → CONFIG_READ
fn config_read_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method) = split_selector_expr(function_node, source)?;

    let (framework, source_type) =
        if receiver == "os" && (method == "Getenv" || method == "LookupEnv") {
            ("go", "env")
        } else if receiver == "viper" || receiver.ends_with(".viper") {
            match method.as_str() {
                "GetString" | "Get" | "GetBool" | "GetInt" | "GetFloat64" | "GetDuration" => {
                    ("viper", "config")
                }
                _ => return None,
            }
        } else {
            return None;
        };

    let args = call_arguments(node);
    let key_node = args.first()?;
    let key = extract_string_literal(*key_node, source);
    if key.is_empty() {
        return None;
    }
    let env_uri = config::normalize_env_var_name(&key)?;
    let detail = config::build_config_read_detail(source_type, &env_uri, &key, framework);
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_READ_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(env_uri),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Detect `viper.Unmarshal(&cfg)` and `viper.UnmarshalKey("key", &cfg)` → CONFIG_BIND edges.
/// Resolves the struct type from local variable tracking in the context.
fn viper_config_bind_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method) = split_selector_expr(function_node, source)?;

    // Only match viper.Unmarshal or viper.UnmarshalKey
    if receiver != "viper" && !receiver.ends_with(".viper") {
        return None;
    }
    let struct_arg_index = match method.as_str() {
        "Unmarshal" => 0,
        "UnmarshalKey" => 1,
        _ => return None,
    };

    let args = call_arguments(node);
    let struct_arg = args.get(struct_arg_index)?;

    // Argument should be &cfg — a unary_expression with operator "&"
    let var_name = extract_address_of_var(*struct_arg, source)?;

    // Scope-aware lookup: among all declarations of `var_name` whose containing
    // block spans the call site, prefer the innermost (smallest block range).
    // This correctly handles sibling-block shadowing (Finding 1) and closure
    // shadowing (Finding 2).
    let call_byte = node.start_byte();
    let struct_type = resolve_var_type_at(&ctx.local_var_types, &var_name, call_byte)?;

    // Qualify the struct type with the module if it's a simple (unqualified) name
    let target_qualname = if struct_type.contains('.') {
        struct_type.to_string()
    } else {
        format!("{}.{}", ctx.module, struct_type)
    };

    let detail = config::build_config_bind_detail(&target_qualname, &method, "unmarshal", "viper");
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_BIND_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(target_qualname),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Extract the variable name from a `&varName` unary expression node.
fn extract_address_of_var(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() != "unary_expression" {
        return None;
    }
    // Check operator is "&"
    let operator = node.child_by_field_name("operator")?;
    if node_text(operator, source) != "&" {
        return None;
    }
    let operand = node.child_by_field_name("operand")?;
    if operand.kind() == "identifier" {
        let name = node_text(operand, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// Collect local variable declarations with their containing-block byte ranges.
/// Handles:
///   `var cfg DatabaseConfig` (var_declaration → var_spec with explicit type)
///   `cfg := DatabaseConfig{}` (short_var_declaration with composite_literal)
///
/// Each declaration is tagged with the byte range of the innermost block or
/// func_literal that directly contains it, enabling scope-aware resolution at
/// the call site (see `resolve_var_type_at`).
fn collect_local_var_types(node: Node<'_>, source: &str) -> Vec<ScopedVarType> {
    let mut types = Vec::new();
    let block_start = node.start_byte();
    let block_end = node.end_byte();
    collect_local_var_types_inner(node, source, block_start, block_end, &mut types);
    types
}

fn collect_local_var_types_inner(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    match node.kind() {
        "var_declaration" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "var_spec" {
                    collect_var_spec_types_scoped(child, source, block_start, block_end, types);
                }
            }
        }
        "short_var_declaration" => {
            collect_short_var_types_scoped(node, source, block_start, block_end, types);
        }
        _ => {}
    }
    // Recurse into all children. When we cross into a new block or func_literal,
    // update the current block range so declarations inside are tagged correctly.
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let (child_block_start, child_block_end) =
            if child.kind() == "block" || child.kind() == "func_literal" {
                (child.start_byte(), child.end_byte())
            } else {
                (block_start, block_end)
            };
        collect_local_var_types_inner(child, source, child_block_start, child_block_end, types);
    }
}

/// Scope-aware variable type resolver.
///
/// Among all entries for `var_name` whose block range contains `call_byte`,
/// return the type from the innermost (narrowest) block. Returns `None` when:
/// - no declaration for `var_name` exists at the call site, OR
/// - multiple declarations exist in the same innermost block with *different*
///   types (ambiguous — prefer a missed edge over a wrong one).
fn resolve_var_type_at<'a>(
    scoped: &'a [ScopedVarType],
    var_name: &str,
    call_byte: usize,
) -> Option<&'a str> {
    // Candidates: declarations whose block encloses the call site.
    let candidates: Vec<&ScopedVarType> = scoped
        .iter()
        .filter(|sv| sv.name == var_name && sv.block_start <= call_byte && call_byte < sv.block_end)
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Innermost block = the one with the largest start byte (tightest enclosure).
    let innermost_start = candidates
        .iter()
        .map(|sv| sv.block_start)
        .max()
        .unwrap_or(0);

    let innermost: Vec<&ScopedVarType> = candidates
        .iter()
        .filter(|sv| sv.block_start == innermost_start)
        .copied()
        .collect();

    // All innermost entries must agree on the type.
    let first_type = innermost[0].type_name.as_str();
    if innermost.iter().all(|sv| sv.type_name == first_type) {
        Some(first_type)
    } else {
        // Ambiguous — skip rather than emit a wrong edge.
        None
    }
}

/// Record scoped name → type entries from a `var_spec` node.
/// Handles `var a, b TypeName` and `var cfg = TypeName{}`.
fn collect_var_spec_types_scoped(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    let mut cursor = node.walk();
    let names: Vec<String> = node
        .children_by_field_name("name", &mut cursor)
        .map(|name_node| node_text(name_node, source))
        .filter(|name| !name.is_empty())
        .collect();

    let type_name = match node.child_by_field_name("type") {
        Some(type_node) => named_type_text(type_node, source),
        // `var cfg = TypeName{}` — infer from the initializer, but only for a
        // single name; multiple initializers would need positional pairing.
        None if names.len() == 1 => node
            .child_by_field_name("value")
            .and_then(|value| extract_composite_literal_type(value, source)),
        None => None,
    };
    let Some(type_name) = type_name else {
        return;
    };
    for name in names {
        types.push(ScopedVarType {
            name,
            type_name: type_name.clone(),
            block_start,
            block_end,
        });
    }
}

/// Record scoped name → type entries from `cfg := TypeName{}` (short_var_declaration),
/// pairing each left-hand identifier with its right-hand expression positionally.
fn collect_short_var_types_scoped(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    let Some(left) = node.child_by_field_name("left") else {
        return;
    };
    let Some(right) = node.child_by_field_name("right") else {
        return;
    };
    let mut left_cursor = left.walk();
    let mut right_cursor = right.walk();
    let names = left.named_children(&mut left_cursor);
    let values = right.named_children(&mut right_cursor);
    for (name_node, value_node) in names.zip(values) {
        if name_node.kind() != "identifier" {
            continue;
        }
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        if let Some(type_name) = extract_composite_literal_type(value_node, source) {
            types.push(ScopedVarType {
                name,
                type_name,
                block_start,
                block_end,
            });
        }
    }
}

/// Extract the named type from a composite literal `TypeName{...}`,
/// drilling through an expression_list wrapper if present.
fn extract_composite_literal_type(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "expression_list" {
        let mut cursor = node.walk();
        let first = node.named_children(&mut cursor).next()?;
        return extract_composite_literal_type(first, source);
    }
    if node.kind() != "composite_literal" {
        return None;
    }
    named_type_text(node.child_by_field_name("type")?, source)
}

/// Text of a named type (`TypeName` or `pkg.TypeName`), unwrapping pointer
/// indirection. Returns None for slice/map/array/chan/func types, which
/// cannot name a bindable config struct.
fn named_type_text(node: Node<'_>, source: &str) -> Option<String> {
    match node.kind() {
        "pointer_type" => {
            let mut cursor = node.walk();
            let inner = node.named_children(&mut cursor).next()?;
            named_type_text(inner, source)
        }
        "type_identifier" | "qualified_type" => {
            let text = node_text(node, source);
            if text.is_empty() { None } else { Some(text) }
        }
        _ => None,
    }
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let Some(function_node) = node.child_by_field_name("function") else {
        return edges;
    };

    let Some((receiver, method_name)) = split_selector_expr(function_node, source) else {
        return edges;
    };

    // Check if method is an HTTP verb or HandleFunc
    let method_lower = method_name.to_ascii_lowercase();
    let is_http_verb = http::normalize_method(&method_name).is_some();
    let is_handle = method_lower == "handlefunc" || method_lower == "handle";

    if !is_http_verb && !is_handle {
        return edges;
    }

    // Extract arguments
    let args = call_arguments(node);
    if args.is_empty() {
        return edges;
    }

    // First argument should be the path
    let raw_path = extract_string_literal(args[0], source);
    if raw_path.is_empty() || !raw_path.starts_with('/') {
        return edges;
    }

    let method = if is_http_verb {
        match http::normalize_method(&method_name) {
            Some(m) => m,
            None => return edges,
        }
    } else {
        http::HTTP_ANY.to_string()
    };

    let normalized = match http::normalize_path(&raw_path) {
        Some(n) => n,
        None => return edges,
    };
    let framework = detect_framework(&receiver);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, framework);
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);

    edges.push(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });

    edges
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    // Check if it's an HTTP client call
    let method = http::normalize_method(&method_name)?;

    let args = call_arguments(node);
    if args.is_empty() {
        return None;
    }

    let raw_path = extract_string_literal(args[0], source);
    if raw_path.is_empty() {
        return None;
    }

    // Only consider it an HTTP call if receiver looks like a client or is http package
    let receiver_lower = receiver.to_ascii_lowercase();
    if !receiver_lower.contains("http")
        && !receiver_lower.contains("client")
        && !receiver_lower.contains("request")
    {
        return None;
    }

    let normalized = http::normalize_path(&raw_path)?;
    let client = if receiver_lower.contains("http") {
        "http"
    } else {
        "http_client"
    };
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
    qualname: &str,
    receiver_type: &str,
) -> Option<EdgeInput> {
    // Check if the receiver type is a known gRPC server
    let server_info = ctx.grpc_servers.get(receiver_type)?;
    let (raw_path, normalized) = proto::normalize_rpc_path(None, &server_info.service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-go",
        "role": "server",
        "service": server_info.service.as_str(),
        "rpc": rpc_name,
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(qualname.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    // Check if receiver is a known gRPC client
    let client_info = ctx.grpc_clients.get(&receiver)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(None, &client_info.service, &method_name)?;
    let detail = json!({
        "framework": "grpc-go",
        "role": "client",
        "service": client_info.service.as_str(),
        "rpc": method_name,
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

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    if !channel::is_bus_receiver(&receiver) {
        return None;
    }

    let kind = if channel::is_publish_method(&method_name) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&method_name) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };

    let args = call_arguments(node);
    let raw_topic = if args.is_empty() {
        return None;
    } else {
        extract_string_literal(args[0], source)
    };

    if raw_topic.is_empty() {
        return None;
    }

    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "go-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "go-bus")
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

fn collect_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, GrpcClientInfo> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, &mut clients);
    clients
}

fn collect_grpc_clients_inner(
    node: Node<'_>,
    source: &str,
    clients: &mut HashMap<String, GrpcClientInfo>,
) {
    // Look for short_var_declaration: client := pb.NewGreeterClient(conn)
    if node.kind() == "short_var_declaration"
        && let Some((name, service)) = grpc_client_from_short_var(node, source)
    {
        clients.insert(name, service);
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, clients);
    }
}

fn grpc_client_from_short_var(node: Node<'_>, source: &str) -> Option<(String, GrpcClientInfo)> {
    let left = node.child_by_field_name("left")?;
    let right = node.child_by_field_name("right")?;

    let var_name = extract_first_identifier(left, source)?;
    let service = extract_grpc_service_from_client_call(right, source)?;

    Some((var_name, GrpcClientInfo { service }))
}

fn extract_first_identifier(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "identifier" {
        let name = node_text(node, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(name) = extract_first_identifier(child, source) {
            return Some(name);
        }
    }
    None
}

fn extract_grpc_service_from_client_call(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "call_expression" {
        let function = node.child_by_field_name("function")?;
        let (_receiver, method_name) = split_selector_expr(function, source)?;
        // Check for NewXxxClient pattern
        if method_name.starts_with("New") && method_name.ends_with("Client") {
            let service = &method_name[3..method_name.len() - 6];
            if !service.is_empty() {
                return Some(service.to_string());
            }
        }
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(service) = extract_grpc_service_from_client_call(child, source) {
            return Some(service);
        }
    }
    None
}

fn is_grpc_server_struct(struct_node: Node<'_>, source: &str) -> bool {
    if struct_node.kind() != "struct_type" {
        return false;
    }

    // Find the field list - try field_declaration_list directly
    let mut cursor = struct_node.walk();
    let children: Vec<_> = struct_node.named_children(&mut cursor).collect();

    let field_list = if let Some(fl) = struct_node.child_by_field_name("fields") {
        fl
    } else {
        // Try finding field_declaration_list directly
        let mut found = None;
        for child in &children {
            if child.kind() == "field_declaration_list" {
                found = Some(*child);
                break;
            }
        }
        match found {
            Some(fl) => fl,
            None => return false,
        }
    };

    let mut cursor = field_list.walk();
    for child in field_list.named_children(&mut cursor) {
        // In Go, embedded fields don't have field names, just types
        let type_text = if child.kind() == "field_declaration" {
            // Could be: name type OR just type (embedded)
            if let Some(type_node) = child.child_by_field_name("type") {
                node_text(type_node, source)
            } else {
                // Might be an embedded field - get the first child
                node_text(child, source)
            }
        } else {
            continue;
        };

        if type_text.contains("Unimplemented") && type_text.contains("Server") {
            return true;
        }
    }

    false
}

fn extract_grpc_service_from_server_name(name: &str) -> Option<String> {
    if !name.ends_with("Server") {
        return None;
    }
    let service = &name[..name.len() - 6];
    if service.is_empty() {
        None
    } else {
        Some(service.to_string())
    }
}

fn extract_receiver_type(node: Node<'_>, source: &str) -> Option<String> {
    let receiver = node.child_by_field_name("receiver")?;
    let mut cursor = receiver.walk();
    for child in receiver.named_children(&mut cursor) {
        if child.kind() == "parameter_declaration"
            && let Some(type_node) = child.child_by_field_name("type")
        {
            let type_text = node_text(type_node, source);
            // Strip pointer prefix if present
            let type_text = type_text.trim_start_matches('*').trim();
            if !type_text.is_empty() {
                return Some(type_text.to_string());
            }
        }
    }
    None
}

fn extract_function_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let result = node
        .child_by_field_name("result")
        .map(|n| node_text(n, source));
    match (params, result) {
        (Some(p), Some(r)) => Some(format!("{} -> {}", p, r)),
        (Some(p), None) => Some(p),
        _ => None,
    }
}

fn extract_name_from_spec(node: Node<'_>, source: &str) -> Option<String> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() { None } else { Some(name) }
}

fn split_selector_expr(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() == "selector_expression" {
        let operand = node.child_by_field_name("operand")?;
        let field = node.child_by_field_name("field")?;
        let receiver = node_text(operand, source);
        let method = node_text(field, source);
        return Some((receiver, method));
    }
    None
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut args = Vec::new();
    let Some(arg_list) = node.child_by_field_name("arguments") else {
        return args;
    };
    let mut cursor = arg_list.walk();
    for child in arg_list.named_children(&mut cursor) {
        // tree-sitter-go emits `comment` as a named child of argument_list;
        // skip it so positional indexing (args[0], args[1]) is not disturbed.
        if child.kind() == "comment" {
            continue;
        }
        args.push(child);
    }
    args
}

fn extract_string_literal(node: Node<'_>, source: &str) -> String {
    let raw = node_text(node, source);
    unquote_go_string(&raw).unwrap_or(raw)
}

fn unquote_go_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Interpreted string: "..."
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    // Raw string: `...`
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    None
}

fn detect_framework(receiver: &str) -> &'static str {
    let lower = receiver.to_ascii_lowercase();
    if lower.contains("gin") {
        "gin"
    } else if lower.contains("echo") {
        "echo"
    } else if lower.contains("chi") {
        "chi"
    } else if lower.contains("mux") {
        "gorilla"
    } else {
        "net/http"
    }
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    // If it contains a dot, it's a qualified call
    if raw.contains('.') {
        let parts: Vec<&str> = raw.split('.').collect();
        if parts.len() >= 2 {
            // Assume module-level function for now
            return Some(raw.to_string());
        }
    }
    // Simple name - qualify with current module
    Some(format!("{}.{}", ctx.module, raw))
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.')
}

#[cfg(test)]
mod tests {
    use super::GoExtractor;
    use crate::indexer::channel;
    use crate::indexer::config;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_http_route_and_call() {
        let source = r#"
package main

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

func main() {
    r := gin.Default()
    r.GET("/api/users/:id", getUser)
    r.POST("/api/users", createUser)

    http.HandleFunc("/health", healthCheck)
    http.Get("http://example.com/api/data")
}

func getUser(c *gin.Context) {}
func createUser(c *gin.Context) {}
func healthCheck(w http.ResponseWriter, r *http.Request) {}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let routes: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == http::HTTP_ROUTE_KIND)
            .collect();
        let calls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == http::HTTP_CALL_KIND)
            .collect();
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/api/users/{}")),
            "Expected /api/users/{{}} route, got: {:?}",
            routes
        );
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/health")),
            "Expected /health route"
        );
        assert!(!calls.is_empty(), "Expected at least one HTTP call");
    }

    #[test]
    fn extracts_grpc_impl_and_call() {
        let source = r#"
package main

type GreeterServer struct {
    pb.UnimplementedGreeterServer
}

func (s *GreeterServer) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
    return &pb.HelloReply{}, nil
}

func run() {
    client := pb.NewGreeterClient(conn)
    client.SayHello(ctx, req)
}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let impls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == proto::RPC_IMPL_KIND)
            .collect();
        let calls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == proto::RPC_CALL_KIND)
            .collect();
        assert!(!impls.is_empty(), "should have RPC_IMPL edges");
        assert!(!calls.is_empty(), "should have RPC_CALL edges");
    }

    #[test]
    fn extracts_channel_publish_subscribe() {
        let source = r#"
package main

func publisher() {
    bus.Publish("Topics.UserCreated", event)
}

func subscriber() {
    bus.Subscribe("Topics.UserCreated", handler)
}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let pub_edges: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == channel::CHANNEL_PUBLISH_KIND)
            .collect();
        let sub_edges: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == channel::CHANNEL_SUBSCRIBE_KIND)
            .collect();
        assert!(!pub_edges.is_empty(), "should have CHANNEL_PUBLISH edges");
        assert!(!sub_edges.is_empty(), "should have CHANNEL_SUBSCRIBE edges");
    }

    /// Extract `source` as module "main" and return only the CONFIG_BIND edges.
    fn config_bind_edges(source: &str) -> Vec<crate::indexer::extract::EdgeInput> {
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        file.edges
            .into_iter()
            .filter(|e| e.kind == config::CONFIG_BIND_KIND)
            .collect()
    }

    /// Every local declaration form that should bind `cfg` to `DatabaseConfig`:
    /// explicit type, composite-literal initializers, multi-name var specs,
    /// and positional short var declarations.
    #[test]
    fn viper_unmarshal_resolves_local_declaration_forms() {
        let declarations = [
            "var cfg DatabaseConfig",
            "cfg := DatabaseConfig{}",
            "var cfg = DatabaseConfig{}",
            "var primary, cfg DatabaseConfig",
            r#"name, cfg := "prod", DatabaseConfig{}"#,
        ];
        for declaration in declarations {
            let source = format!(
                r#"
package main

type DatabaseConfig struct {{
    Host string
}}

func loadConfig() {{
    {declaration}
    viper.Unmarshal(&cfg)
}}
"#
            );
            let bind_edges = config_bind_edges(&source);
            assert!(
                bind_edges
                    .iter()
                    .any(|e| e.target_qualname.as_deref() == Some("main.DatabaseConfig")),
                "`{declaration}`: expected CONFIG_BIND edge to main.DatabaseConfig, got: {bind_edges:?}"
            );
        }
    }

    #[test]
    fn viper_unmarshal_key_emits_config_bind_edge_with_detail() {
        let source = r#"
package main

type AppConfig struct {
    Debug bool
}

func loadConfig() {
    var appCfg AppConfig
    viper.UnmarshalKey("app", &appCfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        let edge = bind_edges
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("main.AppConfig"))
            .unwrap_or_else(|| {
                panic!("Expected CONFIG_BIND edge to main.AppConfig, got: {bind_edges:?}")
            });
        let detail: serde_json::Value =
            serde_json::from_str(edge.detail.as_deref().unwrap()).unwrap();
        assert_eq!(detail["wrapper_type"], "UnmarshalKey");
        assert_eq!(detail["binding_kind"], "unmarshal");
        assert_eq!(detail["framework"], "viper");
    }

    #[test]
    fn viper_unmarshal_keeps_qualified_type_unprefixed() {
        let source = r#"
package main

func loadConfig() {
    var cfg settings.Database
    viper.Unmarshal(&cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("settings.Database")),
            "Expected CONFIG_BIND edge to settings.Database, got: {bind_edges:?}"
        );
    }

    #[test]
    fn viper_unmarshal_into_slice_var_emits_no_config_bind() {
        let source = r#"
package main

type ServerConfig struct {
    Host string
}

func loadConfig() {
    var servers []ServerConfig
    viper.UnmarshalKey("servers", &servers)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for slice-typed targets, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn viper_unmarshal_ignores_vars_declared_inside_closures() {
        let source = r#"
package main

type OtherConfig struct {
    Host string
}

func loadConfig() {
    helper := func() {
        cfg := OtherConfig{}
        _ = cfg
    }
    helper()
    viper.Unmarshal(&cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Closure-local declarations must not leak into the enclosing scope, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn viper_unmarshal_with_unresolvable_args_emits_no_config_bind() {
        let source = r#"
package main

func loadConfig() {
    viper.Unmarshal()
    viper.Unmarshal(&undeclared)
    viper.Unmarshal(cfgValue)
    viper.UnmarshalKey("app")
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for unresolvable arguments, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn non_viper_call_emits_no_config_bind() {
        let source = r#"
package main

type DatabaseConfig struct {
    Host string
}

func loadConfig() {
    var cfg DatabaseConfig
    json.Unmarshal(data, &cfg)
    yaml.Unmarshal(data, &cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for non-viper calls, got: {:?}",
            bind_edges
        );
    }

    // ── Finding 1 ──────────────────────────────────────────────────────────
    // Sibling-block `cfg` declarations must not bleed across scopes.
    // viper.Unmarshal(&cfg) in the FIRST if-block must bind to `AConfig`,
    // not to `BConfig` from the SECOND if-block.
    #[test]
    fn viper_unmarshal_sibling_block_cfg_binds_to_enclosing_scope_type() {
        let source = r#"
package main

type AConfig struct { Host string }
type BConfig struct { Host string }

func f() {
    if a {
        cfg := AConfig{}
        viper.Unmarshal(&cfg)
    }
    if b {
        cfg := BConfig{}
        _ = cfg
    }
}
"#;
        let bind_edges = config_bind_edges(source);
        // Must produce exactly one edge, bound to AConfig.
        assert_eq!(
            bind_edges.len(),
            1,
            "Expected exactly 1 CONFIG_BIND edge, got: {bind_edges:?}"
        );
        assert_eq!(
            bind_edges[0].target_qualname.as_deref(),
            Some("main.AConfig"),
            "Expected edge to main.AConfig (not BConfig from sibling block), got: {bind_edges:?}"
        );
    }

    // ── Finding 2 ──────────────────────────────────────────────────────────
    // A `cfg` shadowed inside a closure must NOT resolve to the outer `cfg`.
    // Acceptable outcomes: bind to `OtherConfig` (if closure locals are tracked)
    // or emit no edge — but never bind to the outer `DatabaseConfig`.
    #[test]
    fn viper_unmarshal_closure_shadow_does_not_bind_to_outer_type() {
        let source = r#"
package main
type DatabaseConfig struct{ Host string }
type OtherConfig struct{ Host string }
func f() {
    var cfg DatabaseConfig
    helper := func() {
        cfg := OtherConfig{}
        viper.Unmarshal(&cfg)
    }
    helper()
}
"#;
        let bind_edges = config_bind_edges(source);
        // Must NOT bind to the outer DatabaseConfig.
        assert!(
            !bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("main.DatabaseConfig")),
            "Closure-shadowed cfg must not bind to outer DatabaseConfig, got: {bind_edges:?}"
        );
    }

    // ── Finding 3 ──────────────────────────────────────────────────────────
    // A `comment` node before the struct argument must not shift the positional
    // index so that the edge is silently dropped.
    #[test]
    fn viper_unmarshal_comment_before_arg_does_not_drop_edge() {
        // tree-sitter represents inline block comments as named children of
        // argument_list; we feed the raw Go source through the extractor to
        // exercise the real parser.
        let source = r#"
package main

type AppConfig struct { Debug bool }

func loadConfig() {
    var appCfg AppConfig
    viper.Unmarshal(/* c */ &appCfg)
    viper.UnmarshalKey("app", /* c */ &appCfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        // Both calls should bind to main.AppConfig.
        assert!(
            bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("main.AppConfig")),
            "Expected at least one CONFIG_BIND edge to main.AppConfig despite inline comment, got: {bind_edges:?}"
        );
        assert_eq!(
            bind_edges
                .iter()
                .filter(|e| e.target_qualname.as_deref() == Some("main.AppConfig"))
                .count(),
            2,
            "Expected two CONFIG_BIND edges to main.AppConfig (Unmarshal + UnmarshalKey), got: {bind_edges:?}"
        );
    }
}
