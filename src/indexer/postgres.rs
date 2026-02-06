use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use anyhow::Result;
use std::path::Path;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
}

pub struct PostgresExtractor {
    parser: Parser,
}

impl PostgresExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_sequel::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for PostgresExtractor {
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
        };
        walk_node(root, &ctx, source, &mut output);

        // Post-walk: scan for DO blocks
        extract_do_blocks(source, module_name, &mut output);

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
            let qualname_owned = qualname.clone();
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

            // PL/pgSQL enhancements
            match node.kind() {
                "create_function" => {
                    // Extract function body and scan for CALLS
                    let node_text_str = node_text(node, source);
                    if let Some(body) = extract_dollar_quoted_body(&node_text_str) {
                        scan_plpgsql_body(body, &qualname_owned, output);
                    }
                }
                "create_trigger" => {
                    // Extract EXECUTE FUNCTION/PROCEDURE reference
                    let node_text_str = node_text(node, source);
                    if let Some(func_name) = extract_trigger_function(&node_text_str) {
                        output.edges.push(EdgeInput {
                            kind: "CALLS".to_string(),
                            source_qualname: Some(qualname_owned.clone()),
                            target_qualname: Some(func_name),
                            detail: Some("trigger execution".to_string()),
                            evidence_snippet: None,
                            ..Default::default()
                        });
                    }
                }
                "create_table" => {
                    // Extract REFERENCES clauses (foreign keys)
                    let node_text_str = node_text(node, source);
                    let refs = extract_foreign_key_references(&node_text_str);
                    for target_table in refs {
                        output.edges.push(EdgeInput {
                            kind: "REFERENCES".to_string(),
                            source_qualname: Some(qualname_owned.clone()),
                            target_qualname: Some(target_table),
                            detail: Some("foreign key".to_string()),
                            evidence_snippet: None,
                            ..Default::default()
                        });
                    }
                }
                _ => {}
            }
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

// PL/pgSQL-specific helpers

fn extract_dollar_quoted_body(text: &str) -> Option<&str> {
    // Find $$...$$  or  $tag$...$tag$
    // Look for $<tag>$ where tag is optional

    // Find first $ sign
    if let Some(first_dollar) = text.find('$') {
        // Find the closing $ of the delimiter
        if let Some(second_dollar) = text[first_dollar + 1..].find('$') {
            let delimiter_end = first_dollar + 1 + second_dollar;
            let delimiter = text[first_dollar..=delimiter_end].to_string();
            let start_idx = delimiter_end + 1;

            // Find the matching closing delimiter
            if let Some(close_pos) = text[start_idx..].find(&delimiter) {
                let body = &text[start_idx..start_idx + close_pos];
                return Some(body);
            }
        }
    }

    None
}

fn scan_plpgsql_body(body: &str, function_qualname: &str, output: &mut ExtractedFile) {
    // Look for function calls in PL/pgSQL body
    // Patterns:
    // - PERFORM <identifier>(
    // - SELECT <identifier>(  (function call in SELECT)
    // - EXECUTE '<identifier>'
    // - <identifier>( (general function call)

    let lines: Vec<&str> = body.split('\n').collect();

    for line in lines {
        let line_upper = line.to_uppercase();
        let line_trimmed = line.trim();

        // PERFORM func_name(...)
        if let Some(perform_idx) = line_upper.find("PERFORM")
            && let Some(func_name) = extract_function_name_after(&line[perform_idx + 7..])
        {
            output.edges.push(EdgeInput {
                kind: "CALLS".to_string(),
                source_qualname: Some(function_qualname.to_string()),
                target_qualname: Some(func_name),
                detail: Some("PERFORM".to_string()),
                evidence_snippet: Some(line_trimmed.to_string()),
                ..Default::default()
            });
        }

        // SELECT func_name(...) - look for function call pattern
        if let Some(select_idx) = line_upper.find("SELECT") {
            // Look for identifier( pattern after SELECT
            let after_select = &line[select_idx + 6..];
            if let Some(func_name) = extract_function_name_after(after_select) {
                // Make sure it looks like a function call (contains parentheses)
                if after_select.contains('(') {
                    output.edges.push(EdgeInput {
                        kind: "CALLS".to_string(),
                        source_qualname: Some(function_qualname.to_string()),
                        target_qualname: Some(func_name),
                        detail: Some("SELECT".to_string()),
                        evidence_snippet: Some(line_trimmed.to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        // EXECUTE 'func_name' or EXECUTE func_name
        if let Some(exec_idx) = line_upper.find("EXECUTE")
            && let Some(func_name) = extract_execute_function(&line[exec_idx + 7..])
        {
            output.edges.push(EdgeInput {
                kind: "CALLS".to_string(),
                source_qualname: Some(function_qualname.to_string()),
                target_qualname: Some(func_name),
                detail: Some("EXECUTE".to_string()),
                evidence_snippet: Some(line_trimmed.to_string()),
                ..Default::default()
            });
        }

        // General function call: identifier(
        // Scan for word( pattern (but skip SQL keywords)
        for func_name in extract_general_function_calls(line) {
            output.edges.push(EdgeInput {
                kind: "CALLS".to_string(),
                source_qualname: Some(function_qualname.to_string()),
                target_qualname: Some(func_name),
                detail: None,
                evidence_snippet: Some(line_trimmed.to_string()),
                ..Default::default()
            });
        }
    }
}

fn extract_function_name_after(text: &str) -> Option<String> {
    // Extract identifier from text (skip whitespace first)
    let trimmed = text.trim_start();
    let mut chars = trimmed.chars();
    let mut name = String::new();

    while let Some(ch) = chars.next() {
        if ch.is_alphanumeric() || ch == '_' {
            name.push(ch);
        } else if ch == '(' {
            // Function call - return the name
            if !name.is_empty() {
                return Some(name);
            }
            break;
        } else if ch.is_whitespace() {
            // Continue looking
            if !name.is_empty() {
                // Check if next non-whitespace is (
                let rest: String = chars.collect();
                if rest.trim_start().starts_with('(') {
                    return Some(name);
                }
                break;
            }
        } else {
            break;
        }
    }

    if !name.is_empty() {
        Some(name)
    } else {
        None
    }
}

fn extract_execute_function(text: &str) -> Option<String> {
    // Look for quoted string or identifier after EXECUTE
    let trimmed = text.trim_start();

    // Check for quoted string
    if let Some(inner) = trimmed.strip_prefix('\'')
        && let Some(end_quote) = inner.find('\'')
    {
        let content = &inner[..end_quote];
        return extract_function_name_after(content);
    }

    // Otherwise, extract identifier
    extract_function_name_after(trimmed)
}

fn extract_general_function_calls(line: &str) -> Vec<String> {
    let mut results = Vec::new();
    let sql_keywords = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "AND", "OR",
        "NOT", "IN", "EXISTS", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
        "ON", "AS", "INTO", "VALUES", "SET", "CASE", "WHEN", "THEN", "ELSE",
        "END", "BEGIN", "IF", "WHILE", "LOOP", "FOR", "RETURN", "DECLARE",
        "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX", "TRIGGER",
        "FUNCTION", "PROCEDURE", "PERFORM", "EXECUTE", "RAISE", "EXCEPTION",
    ];

    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }

        // Check for identifier
        let start = i;
        if chars[i].is_alphabetic() || chars[i] == '_' {
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let identifier: String = chars[start..i].iter().collect();

            // Check if followed by (
            let mut j = i;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && chars[j] == '(' {
                // Check if it's a SQL keyword
                let upper_id = identifier.to_uppercase();
                if !sql_keywords.contains(&upper_id.as_str()) {
                    results.push(identifier);
                }
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }

    results
}

fn extract_trigger_function(text: &str) -> Option<String> {
    // Look for EXECUTE FUNCTION func_name() or EXECUTE PROCEDURE func_name()
    let text_upper = text.to_uppercase();

    if let Some(exec_idx) = text_upper.find("EXECUTE FUNCTION") {
        return extract_function_name_after(&text[exec_idx + 16..]);
    }

    if let Some(exec_idx) = text_upper.find("EXECUTE PROCEDURE") {
        return extract_function_name_after(&text[exec_idx + 17..]);
    }

    None
}

fn extract_foreign_key_references(text: &str) -> Vec<String> {
    let mut results = Vec::new();
    let text_upper = text.to_uppercase();

    // Find all occurrences of REFERENCES
    let mut search_start = 0;
    while let Some(ref_idx) = text_upper[search_start..].find("REFERENCES") {
        let abs_idx = search_start + ref_idx;
        let after_ref = &text[abs_idx + 10..];

        // Extract table name after REFERENCES
        if let Some(table_name) = extract_table_name(after_ref) {
            results.push(table_name);
        }

        search_start = abs_idx + 10;
    }

    results
}

fn extract_table_name(text: &str) -> Option<String> {
    let trimmed = text.trim_start();
    let mut name = String::new();

    for ch in trimmed.chars() {
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            name.push(ch);
        } else if ch == '(' || ch.is_whitespace() {
            break;
        } else {
            return None;
        }
    }

    if !name.is_empty() {
        Some(name)
    } else {
        None
    }
}

fn extract_do_blocks(source: &str, module_name: &str, output: &mut ExtractedFile) {
    // Scan for DO $$ ... $$ or DO $tag$ ... $tag$ blocks
    let source_upper = source.to_uppercase();
    let mut search_start = 0;
    let mut block_count = 0;

    while let Some(do_idx) = source_upper[search_start..].find("DO") {
        let abs_idx = search_start + do_idx;

        // Check if followed by whitespace and then $
        let after_do = &source[abs_idx + 2..];
        let after_do_trimmed = after_do.trim_start();

        if after_do_trimmed.starts_with('$') {
            // Extract the dollar-quoted body
            if let Some(body) = extract_dollar_quoted_body(after_do_trimmed) {
                block_count += 1;
                let block_name = format!("{}::do_block_{}", module_name, block_count);

                // Compute approximate line number
                let prefix = &source[..abs_idx];
                let line_num = prefix.lines().count() as i64 + 1;

                // Create symbol for DO block
                output.symbols.push(SymbolInput {
                    kind: "do_block".to_string(),
                    name: format!("do_block_{}", block_count),
                    qualname: block_name.clone(),
                    start_line: line_num,
                    start_col: 1,
                    end_line: line_num,
                    end_col: 1,
                    start_byte: abs_idx as i64,
                    end_byte: abs_idx as i64,
                    signature: None,
                    docstring: None,
                });

                output.edges.push(EdgeInput {
                    kind: "CONTAINS".to_string(),
                    source_qualname: Some(module_name.to_string()),
                    target_qualname: Some(block_name.clone()),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });

                // Scan the DO block body for function calls
                scan_plpgsql_body(body, &block_name, output);
            }
        }

        search_start = abs_idx + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::extract::LanguageExtractor;

    #[test]
    fn extracts_plpgsql_function_calls() {
        let source = r#"
CREATE OR REPLACE FUNCTION process_order(order_id INTEGER)
RETURNS VOID AS $$
BEGIN
    PERFORM validate_order(order_id);
    PERFORM update_inventory(order_id);
    INSERT INTO order_log SELECT * FROM get_order_details(order_id);
END;
$$ LANGUAGE plpgsql;
"#;
        let mut extractor = PostgresExtractor::new().unwrap();
        let file = extractor.extract(source, "migrations/001_orders").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("validate_order")));
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("update_inventory")));
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("get_order_details")));
    }

    #[test]
    fn extracts_trigger_function_ref() {
        let source = r#"
CREATE TABLE orders (id SERIAL PRIMARY KEY, status TEXT);

CREATE FUNCTION notify_order_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('order_changes', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER order_status_trigger
    AFTER UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION notify_order_change();
"#;
        let mut extractor = PostgresExtractor::new().unwrap();
        let file = extractor.extract(source, "triggers").unwrap();
        let symbols: Vec<_> = file.symbols.iter().map(|s| (s.kind.as_str(), s.name.as_str())).collect();
        assert!(symbols.iter().any(|s| s == &("table", "orders")));
        assert!(symbols.iter().any(|s| s == &("function", "notify_order_change")));
        assert!(symbols.iter().any(|s| s == &("trigger", "order_status_trigger")));
        // Trigger should reference the function
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(calls.iter().any(|e| {
            e.source_qualname.as_deref() == Some("order_status_trigger") &&
            e.target_qualname.as_deref() == Some("notify_order_change")
        }));
    }

    #[test]
    fn extracts_fk_references() {
        let source = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id)
);
"#;
        let mut extractor = PostgresExtractor::new().unwrap();
        let file = extractor.extract(source, "schema").unwrap();
        let refs: Vec<_> = file.edges.iter().filter(|e| e.kind == "REFERENCES").collect();
        assert!(refs.iter().any(|e| {
            e.source_qualname.as_deref() == Some("orders") &&
            e.target_qualname.as_deref() == Some("users")
        }));
        assert!(refs.iter().any(|e| {
            e.source_qualname.as_deref() == Some("orders") &&
            e.target_qualname.as_deref() == Some("products")
        }));
    }

    #[test]
    fn extracts_do_blocks() {
        let source = r#"
DO $$
BEGIN
    PERFORM setup_schema();
    PERFORM load_initial_data();
END
$$;
"#;
        let mut extractor = PostgresExtractor::new().unwrap();
        let file = extractor.extract(source, "init").unwrap();
        let do_blocks: Vec<_> = file.symbols.iter().filter(|s| s.kind == "do_block").collect();
        assert_eq!(do_blocks.len(), 1);
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("setup_schema")));
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("load_initial_data")));
    }

    #[test]
    fn extracts_nested_function_calls() {
        let source = r#"
CREATE FUNCTION complex_calc(x INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN add_one(multiply_two(x));
END;
$$ LANGUAGE plpgsql;
"#;
        let mut extractor = PostgresExtractor::new().unwrap();
        let file = extractor.extract(source, "math").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("add_one")));
        assert!(calls.iter().any(|e| e.target_qualname.as_deref() == Some("multiply_two")));
    }

    #[test]
    fn module_naming() {
        let extractor = PostgresExtractor::new().unwrap();
        assert_eq!(
            extractor.module_name_from_rel_path("migrations/001_init.sql"),
            "migrations/001_init"
        );
        assert_eq!(
            extractor.module_name_from_rel_path("db/schema.sql"),
            "db/schema"
        );
    }
}
