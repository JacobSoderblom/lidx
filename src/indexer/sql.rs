use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use anyhow::Result;
use std::path::Path;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
}

pub struct SqlExtractor {
    parser: Parser,
}

impl SqlExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_sequel::LANGUAGE;
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
        };
        walk_node(root, &ctx, source, &mut output);
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

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if let Some(kind) = create_kind(node.kind()) {
        if let Some((qualname, name)) = extract_object_name(node, source) {
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
            output.symbols.push(SymbolInput {
                kind: kind.to_string(),
                name,
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
        return;
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn create_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "create_table" => Some("table"),
        "create_view" => Some("view"),
        "create_materialized_view" => Some("materialized_view"),
        "create_function" => Some("function"),
        "create_index" => Some("index"),
        "create_trigger" => Some("trigger"),
        "create_type" => Some("type"),
        "create_schema" => Some("schema"),
        "create_sequence" => Some("sequence"),
        "create_database" => Some("database"),
        "create_extension" => Some("extension"),
        "create_role" => Some("role"),
        _ => None,
    }
}

fn extract_object_name(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if let Some(object_node) = find_object_reference(node) {
        let qualname = object_reference_name(object_node, source)?;
        let name = qualname.rsplit('.').next().unwrap_or(&qualname).to_string();
        return Some((qualname, name));
    }
    if matches!(
        node.kind(),
        "create_schema" | "create_database" | "create_role"
    ) {
        let qualname = first_identifier(node, source)?;
        return Some((qualname.clone(), qualname));
    }
    None
}

fn find_object_reference(node: Node<'_>) -> Option<Node<'_>> {
    if node.kind() == "object_reference" {
        return Some(node);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(found) = find_object_reference(child) {
            return Some(found);
        }
    }
    None
}

fn first_identifier(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "identifier" {
            let name = node_text(child, source);
            if !name.is_empty() {
                return Some(name);
            }
        }
        if let Some(found) = first_identifier(child, source) {
            return Some(found);
        }
    }
    None
}

fn object_reference_name(node: Node<'_>, source: &str) -> Option<String> {
    let name_node = node.child_by_field_name("name")?;
    let mut parts = Vec::new();
    if let Some(db) = node.child_by_field_name("database") {
        let value = node_text(db, source);
        if !value.is_empty() {
            parts.push(value);
        }
    }
    if let Some(schema) = node.child_by_field_name("schema") {
        let value = node_text(schema, source);
        if !value.is_empty() {
            parts.push(value);
        }
    }
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    parts.push(name);
    Some(parts.join("."))
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
