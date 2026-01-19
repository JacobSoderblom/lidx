use lidx::indexer::javascript::{JavascriptExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/app.js"), "src/app");
    assert_eq!(module_name_from_rel_path("src/index.js"), "src");
    assert_eq!(module_name_from_rel_path("index.js"), "index");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
import React from "react";
import { foo } from "./lib/foo";
export { bar } from "../bar";

class Base {}

class Foo extends Base {
    constructor() {}
    method(x) { return x; }
}

function util(a, b) { return a + b; }

const MAX = 10;

util(1, 2);
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "src/app")));
    assert!(names.contains(&("class", "src/app.Base")));
    assert!(names.contains(&("class", "src/app.Foo")));
    assert!(names.contains(&("method", "src/app.Foo.method")));
    assert!(names.contains(&("function", "src/app.util")));
    assert!(names.contains(&("const", "src/app.MAX")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"EXTENDS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("src/app.util"))
    );
}
