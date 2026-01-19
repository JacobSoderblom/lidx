use lidx::indexer::markdown::{MarkdownExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("docs/readme.md"), "docs/readme");
    assert_eq!(module_name_from_rel_path("guide.markdown"), "guide");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
# Title

Intro text.

Overview
--------

## Install

```sh
# Not a heading
```
"#;
    let module = module_name_from_rel_path("docs/readme.md");
    let mut extractor = MarkdownExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.name.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "readme", "docs/readme")));
    assert!(names.contains(&("heading", "Title", "docs/readme#Title")));
    assert!(names.contains(&("heading", "Overview", "docs/readme#Overview")));
    assert!(names.contains(&("heading", "Install", "docs/readme#Install")));
    assert!(!names.iter().any(|(_, name, _)| *name == "Not a heading"));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
}
