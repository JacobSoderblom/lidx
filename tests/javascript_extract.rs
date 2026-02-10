use lidx::indexer::extract::LanguageExtractor;
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

#[test]
fn extract_process_env_config_read() {
    let source = r#"
const dbUrl = process.env.DATABASE_URL;
const apiKey = process.env["API_KEY"];
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }), "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
    config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://API_KEY")
    }), "expected CONFIG_READ for env://API_KEY");
}

#[test]
fn extract_process_env_destructuring() {
    let source = r#"
const { DATABASE_URL, API_KEY } = process.env;
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert_eq!(config_reads.len(), 2, "expected 2 CONFIG_READ edges, found: {:?}",
        config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }));
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://API_KEY")
    }));
}

#[test]
fn extract_process_env_destructuring_renamed() {
    let source = r#"
const { DB_URL: dbUrl } = process.env;
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert_eq!(config_reads.len(), 1, "expected 1 CONFIG_READ edge for renamed destructuring, found: {:?}",
        config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DB_URL")
    }));
}
