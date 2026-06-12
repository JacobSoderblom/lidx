use tree_sitter::Node;

use crate::indexer::extract::SymbolInput;

/// Extracts 1-indexed (start_row, start_col, end_row, end_col, start_byte, end_byte) from a node.
pub fn span(node: Node<'_>) -> (i64, i64, i64, i64, i64, i64) {
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

/// Extracts the trimmed text content of a node.
pub fn node_text(node: Node<'_>, source: &str) -> String {
    let start = node.start_byte();
    let end = node.end_byte();
    source.get(start..end).unwrap_or("").trim().to_string()
}

/// Counts lines in source, minimum 1.
pub fn line_count(source: &str) -> i64 {
    let count = source.lines().count();
    if count == 0 { 1 } else { count as i64 }
}

/// Builds a module-level `SymbolInput` from a module name and root node span.
///
/// `name_delimiter` controls how the display name is extracted from the
/// qualified module name: the segment after the last delimiter becomes the
/// name (e.g. `"/"` for JS/Go/C#, `"::"` for Rust, `"."` for Python).
pub fn module_symbol_with_span(
    module_name: &str,
    span: (i64, i64, i64, i64, i64, i64),
    name_delimiter: &str,
    docstring: Option<String>,
) -> SymbolInput {
    let name = module_name
        .rsplit(name_delimiter)
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

/// Fallback module symbol when parsing fails (no tree available).
///
/// Spans the whole source: line 1, column 1 through the last line, covering
/// every byte.
pub fn module_symbol_fallback(
    module_name: &str,
    source: &str,
    name_delimiter: &str,
    docstring: Option<String>,
) -> SymbolInput {
    module_symbol_with_span(
        module_name,
        (1, 1, line_count(source), 1, 0, source.len() as i64),
        name_delimiter,
        docstring,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_count() {
        assert_eq!(line_count(""), 1, "empty source clamps to 1");
        assert_eq!(line_count("hello"), 1);
        assert_eq!(line_count("a\nb\nc"), 3);
        assert_eq!(line_count("a\nb\n"), 2, "trailing newline adds no line");
    }

    #[test]
    fn test_module_symbol_with_span_maps_fields() {
        let sym = module_symbol_with_span(
            "src/utils/helpers",
            (1, 1, 10, 1, 0, 200),
            "/",
            Some("A module".into()),
        );
        assert_eq!(sym.kind, "module");
        assert_eq!(sym.name, "helpers");
        assert_eq!(sym.qualname, "src/utils/helpers");
        assert_eq!(sym.start_line, 1);
        assert_eq!(sym.end_line, 10);
        assert_eq!(sym.docstring, Some("A module".into()));
        assert!(sym.signature.is_none());
    }

    #[test]
    fn test_module_symbol_name_uses_segment_after_last_delimiter() {
        for (module_name, delimiter, expected_name) in [
            ("mypackage.utils", ".", "utils"),
            ("crate::indexer::extract", "::", "extract"),
            ("simple", "/", "simple"),
        ] {
            let sym = module_symbol_with_span(module_name, (1, 1, 1, 1, 0, 10), delimiter, None);
            assert_eq!(sym.name, expected_name, "module_name {module_name:?}");
            assert_eq!(sym.qualname, module_name);
        }
    }

    #[test]
    fn test_module_symbol_fallback_spans_whole_source() {
        let source = "line1\nline2\nline3";
        let sym = module_symbol_fallback("mod/file", source, "/", None);
        assert_eq!(sym.name, "file");
        assert_eq!(sym.start_line, 1);
        assert_eq!(sym.start_col, 1);
        assert_eq!(sym.end_line, 3);
        assert_eq!(sym.end_col, 1);
        assert_eq!(sym.start_byte, 0);
        assert_eq!(sym.end_byte, source.len() as i64);

        let empty = module_symbol_fallback("mod/file", "", "/", None);
        assert_eq!(empty.start_line, 1);
        assert_eq!(empty.end_line, 1);
        assert_eq!(empty.end_byte, 0);
    }

    #[test]
    fn test_span_and_node_text_from_parsed_node() {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_javascript::LANGUAGE.into())
            .unwrap();
        let source = "var x = 1;";
        let tree = parser.parse(source, None).unwrap();
        let root = tree.root_node();

        let (sl, sc, el, ec, sb, eb) = span(root);
        assert_eq!((sl, sc), (1, 1), "positions are 1-indexed");
        assert_eq!(sb, 0);
        assert_eq!(eb, source.len() as i64);
        assert!(el >= 1);
        assert!(ec >= 1);

        assert_eq!(node_text(root, source), "var x = 1;");
    }
}
