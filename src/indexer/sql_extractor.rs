use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
use crate::indexer::tree_helpers::{
    module_symbol_fallback, module_symbol_with_span, node_text, span,
};
use anyhow::Result;
use std::path::Path;
use tree_sitter::{Node, Parser};

/// Edge kind for SQL foreign-key `REFERENCES` clauses.
pub const REFERENCES_KIND: &str = "REFERENCES";

#[derive(Clone)]
struct Context {
    module: String,
}

/// Unified SQL extractor used for all SQL dialects (`.sql`, `.psql`, `.pgsql`, `.tsql`).
///
/// Emits symbols (table, view, function, trigger, …), CONTAINS edges, plus the
/// richer CALLS (PL/pgSQL PERFORM / trigger EXECUTE FUNCTION) and REFERENCES
/// (foreign key) edges that plain `.sql` migration files also benefit from.
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
}

impl crate::indexer::extract::LanguageExtractor for SqlExtractor {
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
        let ctx = Context {
            module: module_name.to_string(),
        };
        walk_node(root, &ctx, source, &mut output);
        let mut suspects = Suspects::default();
        collect_suspects(root, source, &mut suspects);

        // Post-walk: scan for DO blocks
        extract_do_blocks(source, module_name, &mut output);

        // The Postgres grammar has no T-SQL support: procedures parse as ERROR
        // nodes and one bad statement (MERGE, IF/BEGIN, GO) can swallow later
        // CREATE TABLEs. Recover them with a line scan.
        extract_tsql_fallback(source, module_name, &suspects, &mut output);
        extract_tsql_exec_calls(source, &mut output);
        resolve_overlaps(&mut output);

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

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if let Some(kind) = create_kind(node.kind()) {
        // Error recovery can re-lex a T-SQL parameter such as `@Type char(1)`
        // as a `CREATE TYPE` node. Only a statement that really reads
        // `CREATE [OR REPLACE] TYPE` becomes a symbol.
        if node.kind() == "create_type" && !is_real_create_type(&node_text(node, source)) {
            return;
        }
        // T-SQL `#temp` / `##temp` tables are procedure-local scratch
        // objects, not schema tables (#340).
        if node.kind() == "create_table" && is_tsql_temp_table(node, source) {
            return;
        }
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
                identity: None,
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
                        output.edges.push(fk_edge(&qualname_owned, target_table));
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

/// T-SQL `#local` / `##global` temp tables and `@table` variables are
/// procedure-local scratch objects, never schema objects. A leading quote or
/// bracket is ignored.
fn is_temp_name(name: &str) -> bool {
    name.trim_start_matches(['[', '"', '`'])
        .starts_with(['#', '@'])
}

/// The grammar parses the `#` of `CREATE TABLE #x` as an `ERROR` node just
/// before the name's `object_reference`, so the sigil is read from there.
fn is_tsql_temp_table(node: Node<'_>, source: &str) -> bool {
    find_object_reference(node)
        .and_then(|name| name.prev_sibling())
        .is_some_and(|prev| prev.is_error() && is_temp_name(node_text(prev, source).trim()))
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
    // `CREATE INDEX <name> ON <table>` must be named after the index itself,
    // never the table it's built on — the `object_reference` found by
    // `find_object_reference` below is the *table*. See #127.
    if node.kind() == "create_index" {
        return extract_index_name(node, source);
    }
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

/// Names an index symbol after the index's own name (the `column` field on
/// `create_index`, despite the name — it's the identifier right after
/// `CREATE [UNIQUE] INDEX`), qualified by the indexed table's schema, not the
/// table's own name. Anonymous indexes (no name given) yield `None`, matching
/// the prior behavior of emitting no symbol rather than a misnamed one.
fn extract_index_name(node: Node<'_>, source: &str) -> Option<(String, String)> {
    let name_node = node.child_by_field_name("column")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let qualname = find_object_reference(node)
        .and_then(|table_node| object_reference_schema_prefix(table_node, source))
        .map_or_else(|| name.clone(), |schema| format!("{schema}.{name}"));
    Some((qualname, name))
}

/// The database/schema portion of an `object_reference` (excluding the
/// object's own name), e.g. `"dpb"` for `dpb.dataproduct`.
fn object_reference_schema_prefix(node: Node<'_>, source: &str) -> Option<String> {
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
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
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
        // ASCII-only uppercasing preserves byte length, so indices found in
        // `line_upper` stay valid for slicing `line` (Unicode `to_uppercase`
        // can change byte length, e.g. 'ı' -> 'I', and misalign offsets).
        let line_upper = line.to_ascii_uppercase();
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

    if !name.is_empty() { Some(name) } else { None }
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
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "IN",
        "EXISTS",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "ON",
        "AS",
        "INTO",
        "VALUES",
        "SET",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "BEGIN",
        "IF",
        "WHILE",
        "LOOP",
        "FOR",
        "RETURN",
        "DECLARE",
        "CREATE",
        "ALTER",
        "DROP",
        "TABLE",
        "VIEW",
        "INDEX",
        "TRIGGER",
        "FUNCTION",
        "PROCEDURE",
        "PERFORM",
        "EXECUTE",
        "RAISE",
        "EXCEPTION",
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
    // (ASCII uppercasing keeps byte offsets aligned with `text`)
    let text_upper = text.to_ascii_uppercase();

    if let Some(exec_idx) = text_upper.find("EXECUTE FUNCTION") {
        return extract_function_name_after(&text[exec_idx + 16..]);
    }

    if let Some(exec_idx) = text_upper.find("EXECUTE PROCEDURE") {
        return extract_function_name_after(&text[exec_idx + 17..]);
    }

    None
}

fn extract_foreign_key_references(text: &str) -> Vec<String> {
    let masked = mask_noise_keep_identifiers(text);
    // ASCII uppercasing keeps byte offsets aligned with `masked`
    let upper = masked.to_ascii_uppercase();
    let is_ident_byte = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut results = Vec::new();
    let mut scan_from = 0;
    while let Some(i) = upper[scan_from..].find("REFERENCES") {
        let at = scan_from + i;
        scan_from = at + 10;
        let bytes = upper.as_bytes();
        if (at > 0 && is_ident_byte(bytes[at - 1]))
            || bytes.get(scan_from).is_some_and(|&c| is_ident_byte(c))
        {
            continue;
        }
        let target = strip_quotes(&leading_table_name(masked[scan_from..].trim_start()));
        if !target.is_empty() {
            results.push(target);
        }
    }
    results
}

/// The dotted, optionally `[bracketed]` / `"quoted"` name at the start of `s`.
fn leading_table_name(s: &str) -> String {
    let mut name = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        let close = match c {
            '[' => ']',
            '"' => '"',
            c if c.is_alphanumeric() || matches!(c, '_' | '.') => {
                name.push(c);
                continue;
            }
            _ => break,
        };
        name.push(c);
        for q in chars.by_ref() {
            name.push(q);
            if q == close {
                break;
            }
        }
    }
    name
}

/// `REFERENCES` edge for a foreign key from table `source` to `target`.
fn fk_edge(source: &str, target: String) -> EdgeInput {
    EdgeInput {
        kind: REFERENCES_KIND.to_string(),
        source_qualname: Some(source.to_string()),
        target_qualname: Some(target),
        detail: Some("foreign key".to_string()),
        evidence_snippet: None,
        ..Default::default()
    }
}

/// Whether `edge` is a REFERENCES edge from `src_norm` to `tnorm` (both
/// already `normalize_qualname`d).
fn is_reference_edge(edge: &EdgeInput, src_norm: &str, tnorm: &str) -> bool {
    let matches = |q: &Option<String>, norm: &str| {
        q.as_deref().is_some_and(|q| normalize_qualname(q) == norm)
    };
    edge.kind == REFERENCES_KIND
        && matches(&edge.source_qualname, src_norm)
        && matches(&edge.target_qualname, tnorm)
}

fn extract_do_blocks(source: &str, module_name: &str, output: &mut ExtractedFile) {
    // Scan for DO $$ ... $$ or DO $tag$ ... $tag$ blocks
    // (ASCII uppercasing keeps byte offsets aligned with `source`)
    let source_upper = source.to_ascii_uppercase();
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
                    identity: None,
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

/// True when `text` reads `CREATE [OR REPLACE|ALTER] TYPE <name>`.
fn is_real_create_type(text: &str) -> bool {
    parse_create(text).is_some_and(|(kind, _)| kind == "type")
}

/// Grammar-derived symbols whose spans cannot be trusted, keyed by
/// normalized qualname.
#[derive(Default)]
struct Suspects {
    /// The node contains parse errors.
    errored: std::collections::HashSet<String>,
    /// Functions/triggers without a dollar-quoted body: the Postgres grammar
    /// can only have guessed at a T-SQL `BEGIN ... END` body.
    undelimited: std::collections::HashSet<String>,
}

fn normalize_qualname(q: &str) -> String {
    strip_quotes(q).to_ascii_lowercase()
}

fn collect_suspects(node: Node<'_>, source: &str, out: &mut Suspects) {
    if let Some(kind) = create_kind(node.kind()) {
        if let Some((qualname, _)) = extract_object_name(node, source) {
            let norm = normalize_qualname(&qualname);
            if node.has_error() {
                out.errored.insert(norm.clone());
            }
            if matches!(kind, "function" | "trigger") && !node_text(node, source).contains('$') {
                out.undelimited.insert(norm);
            }
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_suspects(child, source, out);
    }
}

/// A `CREATE` statement found by the line scan.
struct Candidate {
    kind: &'static str,
    qualname: String,
    start: usize,
    end: usize,
    has_begin: bool,
}

/// Partial overlap only: one span properly containing the other is a
/// legitimate enclosing statement, not a corrupt span.
fn overlaps(a: (i64, i64), b: (i64, i64)) -> bool {
    let contains = |x: (i64, i64), y: (i64, i64)| x.0 <= y.0 && y.1 <= x.1;
    a.0 < b.1 && b.0 < a.1 && !contains(a, b) && !contains(b, a)
}

fn strip_quotes(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '[' | ']' | '"'))
        .collect()
}

/// `text` with string literals and comments blanked out, so keyword searches
/// only see code.
fn mask_noise(text: &str) -> String {
    mask_noise_impl::<MASK_ALL>(text)
}

/// `mask_noise`, but `[bracketed]` identifiers stay readable.
fn mask_noise_keep_brackets(text: &str) -> String {
    mask_noise_impl::<KEEP_BRACKETS>(text)
}

/// `mask_noise`, but `[bracketed]` and `"quoted"` identifiers stay readable.
fn mask_noise_keep_identifiers(text: &str) -> String {
    mask_noise_impl::<KEEP_IDENTIFIERS>(text)
}

const MASK_ALL: u8 = 0;
const KEEP_BRACKETS: u8 = 1;
const KEEP_IDENTIFIERS: u8 = 2;

/// Length of a `$tag$` / `$$` dollar-quote opener at the start of `b`.
fn dollar_tag_len(b: &[u8]) -> Option<usize> {
    let tag = b[1..]
        .iter()
        .position(|&c| !(c.is_ascii_alphanumeric() || c == b'_'))?;
    let valid = b[1 + tag] == b'$' && !b.get(1).is_some_and(|c| c.is_ascii_digit());
    valid.then_some(tag + 2)
}

fn mask_noise_impl<const MODE: u8>(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = text.as_bytes().to_vec();
    let mut i = 0;
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for k in from..to.min(out.len()) {
            if out[k] != b'\n' {
                out[k] = b' ';
            }
        }
    };
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    while i < b.len() {
        let start = i;
        match b[i] {
            b'[' if MODE >= KEEP_BRACKETS => {
                while i < b.len() && b[i] != b']' {
                    i += 1;
                }
                i += 1;
            }
            b'"' if MODE >= KEEP_IDENTIFIERS => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += 1;
                }
                i += 1;
            }
            b'\'' | b'"' | b'[' => {
                // `E'..'` strings take backslash escapes
                let escapes = b[i] == b'\''
                    && i > 0
                    && matches!(b[i - 1], b'e' | b'E')
                    && (i < 2 || !is_word(b[i - 2]));
                let close = if b[i] == b'[' { b']' } else { b[i] };
                i += 1;
                while i < b.len() && b[i] != close {
                    i += if escapes && b[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
                blank(&mut out, start, i);
            }
            b'$' if (i == 0 || !is_word(b[i - 1])) && dollar_tag_len(&b[i..]).is_some() => {
                let tag = &b[i..i + dollar_tag_len(&b[i..]).unwrap_or(0)];
                i += tag.len();
                while i < b.len() && !b[i..].starts_with(tag) {
                    i += 1;
                }
                i = (i + tag.len()).min(b.len());
                blank(&mut out, start, i);
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                blank(&mut out, start, i);
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // PostgreSQL block comments nest
                let mut nest = 1;
                i += 2;
                while i < b.len() && nest > 0 {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        nest += 1;
                        i += 1;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        nest -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                blank(&mut out, start, i);
            }
            _ => i += 1,
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

fn line_col(source: &str, byte: usize) -> (i64, i64) {
    let row = source[..byte].bytes().filter(|&b| b == b'\n').count();
    let col = byte - source[..byte].rfind('\n').map_or(0, |n| n + 1);
    (row as i64 + 1, col as i64 + 1)
}

fn set_span(sym: &mut SymbolInput, source: &str, start: usize, end: usize) {
    let (start_line, start_col) = line_col(source, start);
    let (end_line, end_col) = line_col(source, end);
    sym.start_line = start_line;
    sym.start_col = start_col;
    sym.end_line = end_line;
    sym.end_col = end_col;
    sym.start_byte = start as i64;
    sym.end_byte = end as i64;
}

fn has_word(text: &str, word: &str) -> bool {
    text.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| w.eq_ignore_ascii_case(word))
}

/// Line-based recovery of `CREATE` statements (procedures, functions, triggers,
/// views, types, tables, indexes) the tree-sitter pass missed or mis-spanned.
/// The Postgres grammar has no T-SQL support, so a missing symbol is added,
/// and a grammar symbol whose span is unreliable (parse errors, zero length,
/// overlapping a neighbour, or a truncated T-SQL body) takes the scanned span.
fn extract_tsql_fallback(
    source: &str,
    module_name: &str,
    suspects: &Suspects,
    output: &mut ExtractedFile,
) {
    for cand in scan_candidates(source) {
        let norm = normalize_qualname(&cand.qualname);
        if cand.kind == "table" {
            add_fallback_fk_edges(source, &cand, &norm, output);
        }
        let existing = output
            .symbols
            .iter()
            .position(|s| s.kind != "module" && normalize_qualname(&s.qualname) == norm);
        if let Some(idx) = existing {
            let sym = &output.symbols[idx];
            let range = (sym.start_byte, sym.end_byte);
            let bad = range.0 >= range.1
                || suspects.errored.contains(&norm)
                || (suspects.undelimited.contains(&norm) && cand.has_begin)
                || output.symbols.iter().enumerate().any(|(j, o)| {
                    j != idx && o.kind != "module" && overlaps(range, (o.start_byte, o.end_byte))
                });
            // Only adopt when the scan describes the same statement.
            let same_stmt = (cand.start as i64) >= range.0 && (cand.start as i64) <= range.1;
            if bad && same_stmt && cand.end > cand.start {
                set_span(&mut output.symbols[idx], source, cand.start, cand.end);
            }
            continue;
        }
        let name = cand
            .qualname
            .rsplit('.')
            .next()
            .unwrap_or(&cand.qualname)
            .to_string();
        let mut sym = SymbolInput {
            kind: cand.kind.to_string(),
            name,
            qualname: cand.qualname.clone(),
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
            start_byte: 0,
            end_byte: 0,
            signature: None,
            docstring: None,
            identity: None,
        };
        set_span(&mut sym, source, cand.start, cand.end);
        output.symbols.push(sym);
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(module_name.to_string()),
            target_qualname: Some(cand.qualname),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

/// REFERENCES edges for a line-scanned `CREATE TABLE` the grammar could not
/// fully parse. Each FK the grammar path already emitted for the table is
/// matched one-to-one and skipped; the rest are added.
fn add_fallback_fk_edges(source: &str, cand: &Candidate, norm: &str, output: &mut ExtractedFile) {
    let Some(text) = source.get(cand.start..cand.end) else {
        return;
    };
    let source_qualname = declared_qualname(output, &cand.qualname);
    let mut grammar_edges: Vec<usize> = (0..output.edges.len())
        .filter(|&i| {
            let e = &output.edges[i];
            e.kind == REFERENCES_KIND
                && e.source_qualname
                    .as_deref()
                    .is_some_and(|q| normalize_qualname(q) == norm)
        })
        .collect();
    for target in extract_foreign_key_references(text) {
        let tnorm = normalize_qualname(&target);
        if let Some(pos) = grammar_edges
            .iter()
            .position(|&i| is_reference_edge(&output.edges[i], norm, &tnorm))
        {
            grammar_edges.remove(pos);
        } else {
            output.edges.push(fk_edge(&source_qualname, target));
        }
    }
}

/// Declared symbol whose qualname matches `qualname` case-insensitively and
/// ignoring quoting, else `qualname` unchanged.
fn declared_qualname(output: &ExtractedFile, qualname: &str) -> String {
    let norm = normalize_qualname(qualname);
    output
        .symbols
        .iter()
        .find(|s| s.kind != "module" && normalize_qualname(&s.qualname) == norm)
        .map_or_else(|| qualname.to_string(), |s| s.qualname.clone())
}

/// Byte offsets (relative to `masked`) and procedure names of the `EXEC`
/// statements in `masked`, skipping dynamic forms and `sp_*`/`xp_*` procs.
fn scan_exec_targets(masked: &str) -> Vec<(usize, String)> {
    let mb = masked.as_bytes();
    let is_ident = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'@' | b'#' | b'.');
    let mut found = Vec::new();
    let mut i = 0;
    while i < mb.len() {
        if !(mb[i].is_ascii_alphabetic() && (i == 0 || !is_ident(mb[i - 1]))) {
            i += 1;
            continue;
        }
        let word_end = i + mb[i..]
            .iter()
            .take_while(|&&c| c.is_ascii_alphanumeric() || c == b'_')
            .count();
        let word = &masked[i..word_end];
        let at = i;
        i = word_end;
        if !(word.eq_ignore_ascii_case("exec") || word.eq_ignore_ascii_case("execute")) {
            continue;
        }
        let Some(target) = parse_exec_target(&masked[word_end..]) else {
            continue;
        };
        let last = target.rsplit('.').next().unwrap_or(&target);
        if last.len() >= 3
            && last.is_char_boundary(3)
            && matches!(last[..3].to_ascii_lowercase().as_str(), "sp_" | "xp_")
        {
            continue;
        }
        found.push((at, target));
    }
    found
}

/// CALLS edges for T-SQL `EXEC`/`EXECUTE [@rc =] [schema.]proc` statements
/// inside procedure (and undelimited function/trigger) bodies.
fn extract_tsql_exec_calls(source: &str, output: &mut ExtractedFile) {
    for cand in scan_candidates(source) {
        if !matches!(cand.kind, "procedure" | "function" | "trigger") {
            continue;
        }
        let body = &source[cand.start..cand.end];
        if body.contains("$$") {
            continue;
        }
        let caller = declared_qualname(output, &cand.qualname);
        for (at, target) in scan_exec_targets(&mask_noise_keep_brackets(body)) {
            let target = declared_qualname(output, &target);
            let line_start = source[..cand.start + at].rfind('\n').map_or(0, |n| n + 1);
            let line_end = source[cand.start + at..]
                .find('\n')
                .map_or(source.len(), |n| cand.start + at + n);
            let line = line_col(source, line_start).0;
            output.edges.push(EdgeInput {
                kind: "CALLS".to_string(),
                source_qualname: Some(caller.clone()),
                target_qualname: Some(target),
                detail: Some("EXEC".to_string()),
                evidence_snippet: Some(source[line_start..line_end].trim().to_string()),
                evidence_start_line: Some(line),
                evidence_end_line: Some(line),
                ..Default::default()
            });
        }
    }
}

/// Procedure name following an `EXEC` keyword: `[@rc =] [a].[b]`. `None` for
/// `EXEC (...)`, `EXEC @proc`, `EXEC AS ...` and anything not a plain name.
fn parse_exec_target(rest: &str) -> Option<String> {
    let mut r = rest.trim_start();
    if r.len() == rest.len() && !r.starts_with('[') {
        return None;
    }
    if let Some(after) = r.strip_prefix('@') {
        let after = after.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '_');
        r = after.trim_start().strip_prefix('=')?.trim_start();
    }
    let mut name = String::new();
    loop {
        if let Some(inner) = r.strip_prefix('[') {
            let end = inner.find(']')?;
            name.push_str(&inner[..end]);
            r = &inner[end + 1..];
        } else {
            let n = r
                .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '#' | '$')))
                .unwrap_or(r.len());
            if n == 0 {
                return None;
            }
            name.push_str(&r[..n]);
            r = &r[n..];
        }
        match r.strip_prefix('.') {
            Some(next) => {
                name.push('.');
                r = next;
            }
            None => break,
        }
    }
    if name.eq_ignore_ascii_case("as") || r.starts_with('(') {
        return None;
    }
    Some(name)
}

/// Last line of defence for the "no overlapping symbols" invariant: when a
/// grammar symbol still partially overlaps a later one, cut it back to where
/// the next symbol starts.
fn resolve_overlaps(output: &mut ExtractedFile) {
    let mut order: Vec<usize> = (0..output.symbols.len())
        .filter(|&i| output.symbols[i].kind != "module")
        .collect();
    order.sort_by_key(|&i| output.symbols[i].start_byte);
    for w in order.windows(2) {
        let (a, b) = (w[0], w[1]);
        let (a_start, a_end) = (output.symbols[a].start_byte, output.symbols[a].end_byte);
        let (b_start, b_end) = (output.symbols[b].start_byte, output.symbols[b].end_byte);
        // Partial overlap only: nested spans are left alone.
        if b_start < a_end && b_end > a_end && b_start > a_start {
            let (line, col) = (output.symbols[b].start_line, output.symbols[b].start_col);
            let sym = &mut output.symbols[a];
            sym.end_byte = b_start;
            sym.end_line = line;
            sym.end_col = col;
        }
    }
}

/// End of a block-style statement (procedure, function, trigger, view)
/// starting on line `i`: the next `GO` line (inclusive) or, failing that, just
/// before the next unindented `CREATE`, with trailing comment noise trimmed.
fn block_end(
    source: &str,
    lines: &[(usize, &str)],
    i: usize,
    start_byte: usize,
    is_go: &dyn Fn(&str) -> bool,
) -> usize {
    let mut end = source.len();
    for (j, &(js, l)) in lines.iter().enumerate().skip(i + 1) {
        if is_go(l) {
            return js + l.trim_end().len();
        }
        if l.starts_with(['c', 'C']) && create_at(lines, j).is_some_and(|(_, n)| !is_temp_name(&n))
        {
            end = js;
            break;
        }
    }
    start_byte + trim_trailing_noise(&source[start_byte..end]).len()
}

fn scan_candidates(source: &str) -> Vec<Candidate> {
    let mut lines: Vec<(usize, &str)> = Vec::new();
    let mut offset = 0;
    for line in source.split_inclusive('\n') {
        lines.push((offset, line));
        offset += line.len();
    }
    // `GO`, `GO 5`, `go;` end a batch.
    let is_go = |l: &str| {
        let l = l.trim().trim_end_matches(';');
        let mut w = l.split_whitespace();
        w.next().is_some_and(|f| f.eq_ignore_ascii_case("go"))
            && w.next().is_none_or(|n| n.parse::<u32>().is_ok())
            && w.next().is_none()
    };
    let mut in_block_comment = false;
    let mut in_dollar = false;
    // End of the last block-style statement; anything starting inside it is
    // part of its body.
    let mut covered_until = 0usize;
    let mut out: Vec<Candidate> = Vec::new();

    for (i, &(line_start, line)) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let skip = in_block_comment
            || in_dollar
            || trimmed.is_empty()
            || trimmed.starts_with("--")
            || line_start < covered_until;
        if !trimmed.starts_with("--") && (line.contains("/*") || line.contains("*/")) {
            // Last marker on the line decides the state.
            let open = line.rfind("/*");
            let close = line.rfind("*/");
            in_block_comment = match (open, close) {
                (Some(o), Some(c)) => o > c,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                _ => in_block_comment,
            };
        }
        if line.matches("$$").count() % 2 == 1 {
            in_dollar = !in_dollar;
        }
        if skip {
            continue;
        }
        let Some((kind, raw)) = create_at(&lines, i) else {
            continue;
        };
        if is_temp_name(&raw) {
            continue;
        }
        let qualname = strip_quotes(&raw);
        if qualname.is_empty() {
            continue;
        }

        let start_byte = line_start + (line.len() - trimmed.len());
        let end_byte = match kind {
            "table" | "type" => table_end(source, start_byte),
            "index" => index_end(source, start_byte),
            _ => block_end(source, &lines, i, start_byte, &is_go),
        };
        let end_byte = end_byte.max(start_byte);
        if matches!(kind, "procedure" | "function" | "trigger" | "view") {
            covered_until = end_byte;
        }
        let has_begin = has_word(&mask_noise(&source[start_byte..end_byte]), "begin");
        out.push(Candidate {
            kind,
            qualname,
            start: start_byte,
            end: end_byte,
            has_begin,
        });
    }
    out
}

/// Strips trailing whitespace, `--` lines and a trailing `/* ... */` block
/// (typically the banner of the next statement).
fn trim_trailing_noise(text: &str) -> &str {
    let mut t = text.trim_end();
    loop {
        let line_start = t.rfind('\n').map_or(0, |n| n + 1);
        let last = &t[line_start..];
        if line_start > 0 && last.trim_start().starts_with("--") {
            t = t[..line_start].trim_end();
        } else if t.ends_with("*/")
            && let Some(o) = t.rfind("/*")
            && t[t[..o].rfind('\n').map_or(0, |n| n + 1)..o]
                .trim()
                .is_empty()
            && o > 0
        {
            t = t[..o].trim_end();
        } else {
            return t;
        }
    }
}

/// Splits the next whitespace-delimited word off `s`.
fn next_word<'a>(s: &mut &'a str) -> Option<&'a str> {
    let t = s.trim_start();
    if t.is_empty() {
        return None;
    }
    let end = t.find(char::is_whitespace).unwrap_or(t.len());
    let (word, rest) = t.split_at(end);
    *s = rest;
    Some(word)
}

/// Reads a possibly dotted object name from `s`, honouring `[...]` and
/// `"..."` parts (which may contain spaces). Returns the raw text.
fn parse_name(s: &str) -> String {
    let s = s.trim_start();
    let mut end = 0;
    let mut close: Option<char> = None;
    for (i, c) in s.char_indices() {
        match close {
            Some(q) => {
                if c == q {
                    close = None;
                }
            }
            None => match c {
                '[' => close = Some(']'),
                '"' => close = Some('"'),
                c if c.is_whitespace() || matches!(c, '(' | ';') => break,
                _ => {}
            },
        }
        end = i + c.len_utf8();
    }
    s[..end].to_string()
}

/// `CREATE ...` starting at line `i`, allowing the keywords and name to
/// continue on following lines (blank and `--` lines are skipped).
fn create_at(lines: &[(usize, &str)], i: usize) -> Option<(&'static str, String)> {
    let text: String = lines[i..]
        .iter()
        .map(|&(_, l)| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .take(6)
        .collect::<Vec<_>>()
        .join("\n");
    parse_create(&text)
}

/// Parses `CREATE [OR ALTER|REPLACE] {PROC|PROCEDURE|FUNCTION|TRIGGER|VIEW|
/// TYPE|TABLE|[UNIQUE|CLUSTERED|...] INDEX} [IF NOT EXISTS] <name>`,
/// returning the symbol kind and the raw name. Indexes are named
/// `<table schema>.<index name>`, matching the grammar-derived symbols.
fn parse_create(text: &str) -> Option<(&'static str, String)> {
    let mut rest = text;
    if !next_word(&mut rest)?.eq_ignore_ascii_case("create") {
        return None;
    }
    let mut word = next_word(&mut rest)?;
    if word.eq_ignore_ascii_case("or") {
        let m = next_word(&mut rest)?;
        if !(m.eq_ignore_ascii_case("alter") || m.eq_ignore_ascii_case("replace")) {
            return None;
        }
        word = next_word(&mut rest)?;
    }
    let mut kind = None;
    // At most a few modifiers (`UNIQUE NONCLUSTERED COLUMNSTORE`...) may sit
    // between CREATE and INDEX; any other word ends the search.
    for _ in 0..4 {
        let w = word.to_ascii_lowercase();
        kind = match w.as_str() {
            "proc" | "procedure" => Some("procedure"),
            "function" => Some("function"),
            "trigger" => Some("trigger"),
            "view" => Some("view"),
            "type" => Some("type"),
            "table" => Some("table"),
            "index" => Some("index"),
            "unique" | "clustered" | "nonclustered" | "columnstore" => {
                word = next_word(&mut rest)?;
                continue;
            }
            _ => None,
        };
        break;
    }
    let kind = kind?;
    // `[CONCURRENTLY] [IF NOT EXISTS]` may precede the name of a table or index.
    let mut name = take_name(&mut rest);
    if kind == "index" && name.eq_ignore_ascii_case("concurrently") {
        name = take_name(&mut rest);
    }
    if matches!(kind, "table" | "index") && name.eq_ignore_ascii_case("if") {
        let not = next_word(&mut rest)?;
        let exists = next_word(&mut rest)?;
        if !(not.eq_ignore_ascii_case("not") && exists.eq_ignore_ascii_case("exists")) {
            return None;
        }
        name = take_name(&mut rest);
    }
    if kind == "index" {
        // `CREATE INDEX ON t (...)` is anonymous: nothing to name a symbol after.
        if name.eq_ignore_ascii_case("on") {
            return None;
        }
        let mut r = rest;
        if next_word(&mut r).is_some_and(|w| w.eq_ignore_ascii_case("on")) {
            // Postgres partitioned tables: `ON ONLY t`.
            let mut table = take_name(&mut r);
            if table.eq_ignore_ascii_case("only") {
                table = take_name(&mut r);
            }
            if let Some((schema, _)) = strip_quotes(&table).rsplit_once('.') {
                name = format!("{schema}.{}", strip_quotes(&name));
            }
        }
    }
    Some((kind, name))
}

/// Reads an object name off the front of `rest` and advances past it.
fn take_name(rest: &mut &str) -> String {
    let name = parse_name(rest);
    let t = rest.trim_start();
    *rest = &t[name.len()..];
    name
}

/// End byte of a `CREATE TABLE`/`TYPE` statement: the paren matching the first
/// `(`, ignoring parens inside string literals, bracketed or quoted
/// identifiers and comments. With no paren before the statement ends (a `;`,
/// a `GO` line or the next `CREATE`), the end of the first line.
fn table_end(source: &str, start: usize) -> usize {
    let rest = &source[start..];
    let bytes = rest.as_bytes();
    let first_line_end = rest.lines().next().map_or(0, |l| l.trim_end().len());
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'[' => {
                let close = if bytes[i] == b'[' { b']' } else { bytes[i] };
                i += 1;
                while i < bytes.len() && bytes[i] != close {
                    i += 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let mut nest = 1;
                i += 2;
                while i < bytes.len() && nest > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        nest += 1;
                        i += 1;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        nest -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                continue;
            }
            b'(' => depth += 1,
            b')' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    return start + i + 1;
                }
            }
            b';' if depth == 0 => return start + first_line_end.min(i),
            b'\n' if depth == 0 && i > 0 => {
                let next = rest[i + 1..].lines().next().unwrap_or("");
                let w = next.trim();
                let go = w
                    .split_whitespace()
                    .next()
                    .is_some_and(|f| f.trim_end_matches(';').eq_ignore_ascii_case("go"));
                if go || next.starts_with(['c', 'C']) && has_word(next, "create") {
                    return start + first_line_end.min(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    start + first_line_end
}

/// End byte of a `CREATE INDEX`: the column list's closing paren plus any
/// `INCLUDE (...)`/`WITH (...)`/`WHERE ...`/`ON ...` clauses that follow it
/// before the statement terminator. A `;` ends the statement and, as for
/// tables and grammar-derived symbols, is not part of the span.
fn index_end(source: &str, start: usize) -> usize {
    let mut end = table_end(source, start);
    loop {
        let after = &source[end..];
        let next = after.trim_start();
        let skipped = after.len() - next.len();
        if next.is_empty() || next.starts_with(';') || after[..skipped].matches('\n').count() > 1 {
            return end;
        }
        let first = next
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or("");
        let after_kw = next[first.len()..].trim_start();
        let continues = match first.to_ascii_lowercase().as_str() {
            // `WITH c AS (...)` is a CTE of the next statement, not an option list.
            "include" | "with" => after_kw.starts_with('('),
            "where" | "on" => true,
            _ => false,
        };
        if !continues {
            return end;
        }
        let line = next.split('\n').next().unwrap_or("");
        let line = line.split(';').next().unwrap_or(line);
        end += skipped + line.trim_end().len();
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
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "migrations/001_orders").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("validate_order"))
        );
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("update_inventory"))
        );
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("get_order_details"))
        );
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
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "triggers").unwrap();
        let symbols: Vec<_> = file
            .symbols
            .iter()
            .map(|s| (s.kind.as_str(), s.name.as_str()))
            .collect();
        assert!(symbols.iter().any(|s| s == &("table", "orders")));
        assert!(
            symbols
                .iter()
                .any(|s| s == &("function", "notify_order_change"))
        );
        assert!(
            symbols
                .iter()
                .any(|s| s == &("trigger", "order_status_trigger"))
        );
        // Trigger should reference the function
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(calls.iter().any(|e| {
            e.source_qualname.as_deref() == Some("order_status_trigger")
                && e.target_qualname.as_deref() == Some("notify_order_change")
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
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "schema").unwrap();
        let refs: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == "REFERENCES")
            .collect();
        assert!(refs.iter().any(|e| {
            e.source_qualname.as_deref() == Some("orders")
                && e.target_qualname.as_deref() == Some("users")
        }));
        assert!(refs.iter().any(|e| {
            e.source_qualname.as_deref() == Some("orders")
                && e.target_qualname.as_deref() == Some("products")
        }));
    }

    #[test]
    fn create_index_named_by_index_not_table() {
        // Regression test for #127: `extract_object_name` took the first
        // `object_reference` node under `create_index`, which is the table
        // being indexed, not the index itself — so the index symbol got the
        // table's qualname and shadowed the real table symbol.
        let source = r#"
CREATE UNIQUE INDEX ux_dataproduct_unique_name_active
    ON dpb.dataproduct (name)
    WHERE active;
"#;
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "migrations/002_index").unwrap();
        let index = file
            .symbols
            .iter()
            .find(|s| s.kind == "index")
            .expect("index symbol not extracted");
        assert_eq!(index.name, "ux_dataproduct_unique_name_active");
        assert_eq!(index.qualname, "dpb.ux_dataproduct_unique_name_active");
        assert_ne!(index.qualname, "dpb.dataproduct");
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
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "init").unwrap();
        let do_blocks: Vec<_> = file
            .symbols
            .iter()
            .filter(|s| s.kind == "do_block")
            .collect();
        assert_eq!(do_blocks.len(), 1);
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("setup_schema"))
        );
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("load_initial_data"))
        );
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
        let mut extractor = SqlExtractor::new().unwrap();
        let file = extractor.extract(source, "math").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("add_one"))
        );
        assert!(
            calls
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("multiply_two"))
        );
    }

    #[test]
    fn containment_is_not_overlap_but_partial_overlap_is() {
        assert!(!overlaps((0, 100), (10, 20)));
        assert!(!overlaps((10, 20), (0, 100)));
        assert!(!overlaps((0, 10), (10, 20)));
        assert!(overlaps((0, 15), (10, 20)));
    }

    #[test]
    fn mask_noise_hides_keywords_in_comments_and_strings() {
        let t = "x -- begin\n/* begin */ 'begin' [begin] BEGIN";
        let m = mask_noise(t);
        assert_eq!(m.len(), t.len());
        assert_eq!(m.matches("BEGIN").count(), 1);
        assert!(!has_word(&m[..m.len() - 5], "begin"));
        assert!(has_word(&m, "begin"));
    }

    #[test]
    fn module_naming() {
        let extractor = SqlExtractor::new().unwrap();
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
