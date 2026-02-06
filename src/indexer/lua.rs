use crate::indexer::channel;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::http;
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::path::Path;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
    current_scope: String,
}

pub struct LuaExtractor {
    parser: Parser,
}

impl LuaExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_lua::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for LuaExtractor {
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

        let module_span = span(root);
        output
            .symbols
            .push(module_symbol_with_span(module_name, module_span));

        let ctx = Context {
            module: module_name.to_string(),
            current_scope: module_name.to_string(),
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
        for edge in edges.iter_mut() {
            if edge.kind == "IMPORTS" && edge.target_qualname.is_some() {
                let target = edge.target_qualname.as_ref().unwrap();
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
    match node.kind() {
        "chunk" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_node(child, ctx, source, output);
            }
        }
        "function_declaration" => {
            handle_function_declaration(node, ctx, source, output);
        }
        "variable_declaration" => {
            handle_variable_declaration(node, ctx, source, output);
        }
        "assignment_statement" => {
            // Top-level assignments (global variables)
            if ctx.current_scope == ctx.module {
                handle_top_level_assignment(node, ctx, source, output);
            }
            // Also walk children for nested calls
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_calls_in_node(child, ctx, source, output);
            }
        }
        "function_call" => {
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

fn handle_function_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };

    match name_node.kind() {
        "identifier" => {
            // Simple function: `function foo()` or `local function foo()`
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
                ..Default::default()
            });

            let mut next_ctx = ctx.clone();
            next_ctx.current_scope = qualname;
            if let Some(body) = node.child_by_field_name("body") {
                walk_node(body, &next_ctx, source, output);
            }
        }
        "method_index_expression" => {
            // Method: `function Cls:method()`
            let (table_name, method_name) = match extract_method_index(name_node, source) {
                Some(v) => v,
                None => return,
            };
            let qualname = format!("{}.{}.{}", ctx.module, table_name, method_name);
            let parent_qualname = format!("{}.{}", ctx.module, table_name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
            let signature = extract_function_signature(node, source);
            output.symbols.push(SymbolInput {
                kind: "method".to_string(),
                name: method_name,
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
                source_qualname: Some(parent_qualname),
                target_qualname: Some(qualname.clone()),
                ..Default::default()
            });

            let mut next_ctx = ctx.clone();
            next_ctx.current_scope = qualname;
            if let Some(body) = node.child_by_field_name("body") {
                walk_node(body, &next_ctx, source, output);
            }
        }
        "dot_index_expression" => {
            // Static method: `function Cls.method()`
            let (table_name, field_name) = match extract_dot_index(name_node, source) {
                Some(v) => v,
                None => return,
            };
            let qualname = format!("{}.{}.{}", ctx.module, table_name, field_name);
            let parent_qualname = format!("{}.{}", ctx.module, table_name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
            let signature = extract_function_signature(node, source);
            output.symbols.push(SymbolInput {
                kind: "method".to_string(),
                name: field_name,
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
                source_qualname: Some(parent_qualname),
                target_qualname: Some(qualname.clone()),
                ..Default::default()
            });

            let mut next_ctx = ctx.clone();
            next_ctx.current_scope = qualname;
            if let Some(body) = node.child_by_field_name("body") {
                walk_node(body, &next_ctx, source, output);
            }
        }
        _ => {}
    }
}

fn handle_variable_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    // variable_declaration wraps assignment_statement for `local X = ...`
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "assignment_statement" {
            handle_local_assignment(child, node, ctx, source, output);
        }
    }
}

fn handle_local_assignment(
    assign_node: Node<'_>,
    decl_node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    // Only extract top-level locals as symbols
    if ctx.current_scope != ctx.module {
        // Still look for require calls in the assignment
        walk_calls_in_node(assign_node, ctx, source, output);
        return;
    }

    let Some(var_list) = assign_node
        .named_children(&mut assign_node.walk())
        .find(|c| c.kind() == "variable_list")
    else {
        return;
    };

    let Some(expr_list) = assign_node
        .named_children(&mut assign_node.walk())
        .find(|c| c.kind() == "expression_list")
    else {
        return;
    };

    // Get the variable names
    let names = extract_variable_names(var_list, source);
    let values = collect_expression_values(expr_list);

    for (i, name) in names.iter().enumerate() {
        if name.is_empty() {
            continue;
        }

        // Check if the value is a table constructor AND name starts uppercase -> treat as class
        let is_class_table = values
            .get(i)
            .is_some_and(|v| v.kind() == "table_constructor")
            && name.starts_with(|c: char| c.is_ascii_uppercase());

        // Check if the value is a require() call -> emit IMPORTS edge
        let is_require = values.get(i).is_some_and(|v| is_require_call(v, source));

        if is_require && let Some(req_node) = values.get(i) {
            emit_require_edge(req_node, ctx, source, output);
        }

        let kind = if is_class_table { "class" } else { "variable" };
        let qualname = format!("{}.{}", ctx.module, name);
        let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(decl_node);

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
            ..Default::default()
        });
    }

    // Walk the expression list for additional calls (but not the require we already handled)
    walk_calls_in_node(expr_list, ctx, source, output);
}

fn handle_top_level_assignment(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(var_list) = node
        .named_children(&mut node.walk())
        .find(|c| c.kind() == "variable_list")
    else {
        return;
    };

    let Some(expr_list) = node
        .named_children(&mut node.walk())
        .find(|c| c.kind() == "expression_list")
    else {
        return;
    };

    let names = extract_variable_names(var_list, source);
    let values = collect_expression_values(expr_list);

    for (i, name) in names.iter().enumerate() {
        if name.is_empty() {
            continue;
        }

        let is_class_table = values
            .get(i)
            .is_some_and(|v| v.kind() == "table_constructor")
            && name.starts_with(|c: char| c.is_ascii_uppercase());
        let kind = if is_class_table { "class" } else { "variable" };
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
            ..Default::default()
        });
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };

    // Check for require() - emit IMPORTS edge
    if name_node.kind() == "identifier" && node_text(name_node, source) == "require" {
        emit_require_edge(&node, ctx, source, output);
        return;
    }

    // Try HTTP route detection (Lapis: app:get("/path", handler))
    if let Some(edges) = http_route_edges(node, ctx, source) {
        for edge in edges {
            output.edges.push(edge);
        }
    }

    // Try channel/bus detection
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }

    // General CALLS edge
    let raw = node_text(name_node, source);
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

    // Walk arguments for nested calls
    if let Some(args) = node.child_by_field_name("arguments") {
        walk_calls_in_node(args, ctx, source, output);
    }
}

fn emit_require_edge(node: &Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // node is either function_call for require("mod") or the function_call itself
    let call_node = if node.kind() == "function_call" {
        *node
    } else {
        return;
    };

    let Some(args_node) = call_node.child_by_field_name("arguments") else {
        return;
    };

    // First argument should be a string
    let mut cursor = args_node.walk();
    let first_arg = args_node.named_children(&mut cursor).next();
    let Some(arg) = first_arg else {
        return;
    };

    let module_path = extract_string_content(arg, source);
    if module_path.is_empty() {
        return;
    }

    // Normalize require path: dots to slashes for qualname matching
    let target_qualname = module_path.replace('.', "/");

    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(call_node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(target_qualname),
        detail: None,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Option<Vec<EdgeInput>> {
    let name_node = node.child_by_field_name("name")?;
    if name_node.kind() != "method_index_expression" {
        return None;
    }

    let (receiver, method_name) = extract_method_index(name_node, source)?;

    // Check if method is an HTTP verb (Lapis pattern: app:get, app:post, etc.)
    let method = http::normalize_method(&method_name)?;

    let args = call_arguments(node);
    if args.is_empty() {
        return None;
    }

    // First argument should be the route path
    let raw_path = extract_string_content(args[0], source);
    if raw_path.is_empty() || !raw_path.starts_with('/') {
        return None;
    }

    let normalized = http::normalize_path(&raw_path)?;
    let framework = detect_framework(&receiver);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, framework);
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);

    let edges = vec![EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    }];
    Some(edges)
}

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let name_node = node.child_by_field_name("name")?;

    let (receiver, method_name) = if name_node.kind() == "method_index_expression" {
        extract_method_index(name_node, source)?
    } else if name_node.kind() == "dot_index_expression" {
        extract_dot_index(name_node, source)?
    } else {
        return None;
    };

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
    if args.is_empty() {
        return None;
    }

    let raw_topic = extract_string_content(args[0], source);
    if raw_topic.is_empty() {
        return None;
    }

    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "lua-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "lua-bus")
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

/// Walk a node tree looking only for function_call nodes (for calls inside expressions).
fn walk_calls_in_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "function_call" {
        handle_call(node, ctx, source, output);
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_calls_in_node(child, ctx, source, output);
    }
}

fn extract_method_index(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "method_index_expression" {
        return None;
    }
    let table = node.child_by_field_name("table")?;
    let method = node.child_by_field_name("method")?;
    let table_name = node_text(table, source);
    let method_name = node_text(method, source);
    if table_name.is_empty() || method_name.is_empty() {
        return None;
    }
    Some((table_name, method_name))
}

fn extract_dot_index(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "dot_index_expression" {
        return None;
    }
    let table = node.child_by_field_name("table")?;
    let field = node.child_by_field_name("field")?;
    let table_name = node_text(table, source);
    let field_name = node_text(field, source);
    if table_name.is_empty() || field_name.is_empty() {
        return None;
    }
    Some((table_name, field_name))
}

fn extract_variable_names(var_list: Node<'_>, source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut cursor = var_list.walk();
    for child in var_list.named_children(&mut cursor) {
        if child.kind() == "identifier" {
            let name = node_text(child, source);
            if !name.is_empty() {
                names.push(name);
            }
        }
    }
    names
}

fn collect_expression_values(expr_list: Node<'_>) -> Vec<Node<'_>> {
    let mut values = Vec::new();
    let mut cursor = expr_list.walk();
    for child in expr_list.named_children(&mut cursor) {
        values.push(child);
    }
    values
}

fn is_require_call(node: &Node<'_>, source: &str) -> bool {
    if node.kind() != "function_call" {
        return false;
    }
    let Some(name_node) = node.child_by_field_name("name") else {
        return false;
    };
    name_node.kind() == "identifier" && node_text(name_node, source) == "require"
}

fn extract_string_content(node: Node<'_>, source: &str) -> String {
    // Handle string nodes directly
    if node.kind() == "string" {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if child.kind() == "string_content" {
                return node_text(child, source);
            }
        }
        // Fall back to stripping quotes
        let raw = node_text(node, source);
        return unquote_lua_string(&raw).unwrap_or(raw);
    }
    // If it's a string_content node itself
    if node.kind() == "string_content" {
        return node_text(node, source);
    }
    // Try looking for string child
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "string" {
            return extract_string_content(child, source);
        }
    }
    String::new()
}

fn unquote_lua_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    // Long strings: [[...]]
    if trimmed.starts_with("[[") && trimmed.ends_with("]]") && trimmed.len() >= 4 {
        return Some(trimmed[2..trimmed.len() - 2].to_string());
    }
    None
}

fn extract_function_signature(node: Node<'_>, source: &str) -> Option<String> {
    node.child_by_field_name("parameters")
        .map(|n| node_text(n, source))
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut args = Vec::new();
    let Some(arg_list) = node.child_by_field_name("arguments") else {
        return args;
    };
    let mut cursor = arg_list.walk();
    for child in arg_list.named_children(&mut cursor) {
        args.push(child);
    }
    args
}

fn detect_framework(receiver: &str) -> &'static str {
    let lower = receiver.to_ascii_lowercase();
    if lower.contains("lapis") || lower == "app" {
        "lapis"
    } else if lower.contains("resty") {
        "lua-resty"
    } else {
        "lua-http"
    }
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    // If it contains a dot or colon, it's a qualified call
    if raw.contains('.') || raw.contains(':') {
        return Some(raw.replace(':', "."));
    }
    // Simple name - qualify with current module
    Some(format!("{}.{}", ctx.module, raw))
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == ':')
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
    use super::LuaExtractor;
    use crate::indexer::channel;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::http;

    #[test]
    fn extracts_http_routes_lapis() {
        let source = r#"
local lapis = require("lapis")
local app = lapis.Application()

app:get("/users/:id", function(self) end)
app:post("/users", function(self) end)
"#;
        let mut extractor = LuaExtractor::new().unwrap();
        let file = extractor.extract(source, "routes").unwrap();
        let routes: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == http::HTTP_ROUTE_KIND)
            .collect();
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/users/{}")),
            "Expected /users/{{}} route, got: {:?}",
            routes
        );
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/users")),
            "Expected /users route, got: {:?}",
            routes
        );
    }

    #[test]
    fn extracts_channel_publish_subscribe() {
        let source = r#"
function publisher()
    bus.Publish("Topics.UserCreated", event)
end

function subscriber()
    bus:Subscribe("Topics.UserCreated", handler)
end
"#;
        let mut extractor = LuaExtractor::new().unwrap();
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
}
