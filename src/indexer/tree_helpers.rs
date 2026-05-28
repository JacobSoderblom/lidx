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
    fn test_line_count_empty() {
        assert_eq!(line_count(""), 1);
    }

    #[test]
    fn test_line_count_single() {
        assert_eq!(line_count("hello"), 1);
    }

    #[test]
    fn test_line_count_multiple() {
        assert_eq!(line_count("a\nb\nc"), 3);
    }

    #[test]
    fn test_module_symbol_with_span_slash_delimiter() {
        let sym = module_symbol_with_span("src/utils/helpers", (1, 1, 10, 1, 0, 200), "/", None);
        assert_eq!(sym.name, "helpers");
        assert_eq!(sym.qualname, "src/utils/helpers");
        assert_eq!(sym.kind, "module");
        assert_eq!(sym.start_line, 1);
        assert_eq!(sym.end_line, 10);
        assert!(sym.docstring.is_none());
    }

    #[test]
    fn test_module_symbol_with_span_dot_delimiter() {
        let sym = module_symbol_with_span(
            "mypackage.utils",
            (1, 1, 5, 1, 0, 100),
            ".",
            Some("A module".into()),
        );
        assert_eq!(sym.name, "utils");
        assert_eq!(sym.qualname, "mypackage.utils");
        assert_eq!(sym.docstring, Some("A module".into()));
    }

    #[test]
    fn test_module_symbol_with_span_double_colon_delimiter() {
        let sym =
            module_symbol_with_span("crate::indexer::extract", (1, 1, 50, 1, 0, 500), "::", None);
        assert_eq!(sym.name, "extract");
        assert_eq!(sym.qualname, "crate::indexer::extract");
    }

    #[test]
    fn test_module_symbol_fallback() {
        let source = "line1\nline2\nline3";
        let sym = module_symbol_fallback("mod/file", source, "/", None);
        assert_eq!(sym.name, "file");
        assert_eq!(sym.start_line, 1);
        assert_eq!(sym.start_col, 1);
        assert_eq!(sym.end_line, 3);
        assert_eq!(sym.end_col, 1);
        assert_eq!(sym.start_byte, 0);
        assert_eq!(sym.end_byte, source.len() as i64);
    }

    #[test]
    fn test_span_from_node() {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_javascript::LANGUAGE.into())
            .unwrap();
        let source = "var x = 1;";
        let tree = parser.parse(source, None).unwrap();
        let root = tree.root_node();
        let (sl, sc, el, ec, sb, eb) = span(root);
        assert_eq!(sl, 1);
        assert_eq!(sc, 1);
        assert_eq!(sb, 0);
        assert_eq!(eb, source.len() as i64);
        assert!(el >= 1);
        assert!(ec >= 1);
    }

    #[test]
    fn test_line_count_trailing_newline() {
        assert_eq!(line_count("a\nb\n"), 2);
    }

    #[test]
    fn test_module_symbol_with_span_no_delimiter_in_name() {
        let sym = module_symbol_with_span("simple", (1, 1, 1, 1, 0, 10), "/", None);
        assert_eq!(sym.name, "simple");
        assert_eq!(sym.qualname, "simple");
    }

    #[test]
    fn test_module_symbol_fallback_empty_source() {
        let sym = module_symbol_fallback("mod/file", "", "/", None);
        assert_eq!(sym.start_line, 1);
        assert_eq!(sym.end_line, 1);
        assert_eq!(sym.end_byte, 0);
    }

    #[test]
    fn test_node_text() {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_javascript::LANGUAGE.into())
            .unwrap();
        let source = "var x = 1;";
        let tree = parser.parse(source, None).unwrap();
        let root = tree.root_node();
        let text = node_text(root, source);
        assert_eq!(text, "var x = 1;");
    }
}
