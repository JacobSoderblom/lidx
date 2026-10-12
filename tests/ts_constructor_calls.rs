//! `new X(..)` binds to the explicit `X.constructor` of a TS/JS class; a class
//! without one (including one whose ancestor has one) stays class-targeted.

mod common;

use common::call_targets;
use lidx::indexer::Indexer;
use lidx::rpc;

const FILES: &[(&str, &str)] = &[
    (
        "src/context.ts",
        "export class Context {\n  constructor(req: string, options?: number) {}\n  render() {}\n}\n",
    ),
    ("src/plain.ts", "export class Plain {\n  run() {}\n}\n"),
    (
        "src/derived.ts",
        "export class Base {\n  constructor(a: string) {}\n}\nexport class Derived extends Base {}\n",
    ),
    ("src/err.ts", "export class Err extends Error {}\n"),
    (
        "src/app.ts",
        "import { Context } from './context';\nimport { Plain } from './plain';\nimport { Derived } from './derived';\nimport { Err } from './err';\nexport function run() {\n  new Context('r', 1);\n  new Plain();\n  new Derived('a');\n  new Err();\n}\n",
    ),
];

#[test]
fn new_binds_to_the_explicit_constructor_else_the_class() {
    let mut targets = call_targets(FILES, "src/app.run");
    targets.sort();
    assert_eq!(
        targets,
        [
            "src/context.Context.constructor",
            "src/derived.Derived",
            "src/err.Err",
            "src/plain.Plain",
        ]
    );
}

fn indexed() -> (tempfile::TempDir, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-ts-ctor-", FILES);
    let indexer = Indexer::new(root, db_path).unwrap();
    (tmp, indexer)
}

#[test]
fn a_class_with_a_called_constructor_is_not_dead() {
    let (_tmp, mut indexer) = indexed();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let dead: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect();
    assert!(
        !dead
            .iter()
            .any(|q| q == "src/context.Context" || q == "src/context.Context.constructor"),
        "{dead:?}"
    );
}

#[test]
fn class_callers_include_new_sites_bound_to_the_constructor() {
    let (_tmp, mut indexer) = indexed();
    let result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "src/context.Context", "sections": ["callers"]}),
    )
    .unwrap();
    assert!(
        result["callers"].to_string().contains("src/app.run"),
        "{result}"
    );
}
