//! `tests_are_roots: false`: references from test code do not keep a
//! declaration alive, so what only tests reach is claimed.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn call(indexer: &mut Indexer, tests_are_roots: Option<bool>) -> Value {
    let mut params = json!({"include_unused_imports": false, "include_orphan_tests": false,
                            "include_public_api": true, "limit": 1000});
    if let Some(flag) = tests_are_roots {
        params["tests_are_roots"] = json!(flag);
    }
    rpc::handle_method(indexer, "dead_symbols", params).unwrap()
}

fn row(result: &Value, suffix: &str) -> Option<Value> {
    ["dead_symbols", "public_api"]
        .iter()
        .flat_map(|g| result[*g].as_array().into_iter().flatten())
        .find(|s| s["qualname"].as_str().unwrap().ends_with(suffix))
        .cloned()
}

const GO: &[(&str, &str)] = &[
    ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    (
        "main.go",
        "package main\n\nimport \"example.com/app/internal/app\"\n\nfunc main() { app.Used() }\n",
    ),
    (
        "internal/app/app.go",
        "package app\n\nfunc Used() { shared() }\n\nfunc shared() {}\n\nfunc testOnly() {}\n\ntype Fixture struct{}\n",
    ),
    (
        "internal/app/app_test.go",
        "package app\n\nimport \"testing\"\n\nfunc TestIt(t *testing.T) {\n\ttestOnly()\n\t_ = Fixture{}\n}\n",
    ),
];

#[test]
fn go_default_keeps_what_tests_use_and_the_option_claims_it() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-troots-go-", GO);
    let default = call(&mut ix, None);
    assert!(row(&default, "app.testOnly").is_none(), "{default}");
    assert!(row(&default, "app.Fixture").is_none(), "{default}");

    let strict = call(&mut ix, Some(false));
    for name in ["app.testOnly", "app.Fixture"] {
        let r = row(&strict, name).unwrap_or_else(|| panic!("{name} not claimed: {strict}"));
        assert!(
            r["reason"]
                .as_str()
                .unwrap()
                .starts_with("reached_only_from_tests"),
            "{r}"
        );
    }
    // reached from production code, and the test itself, stay out
    assert!(row(&strict, "app.shared").is_none(), "{strict}");
    assert!(row(&strict, "app.TestIt").is_none(), "{strict}");
    // an unreferenced declaration keeps its ordinary reason
    assert_eq!(call(&mut ix, Some(true)), default);
}

const RUST: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"c\"\nversion = \"0.1.0\"\n",
    ),
    (
        "src/main.rs",
        "fn used() {}\nfn only_unit_test() {}\nfn only_integration() {}\nfn only_cfg_test_helper() {}\n\nfn main() { used(); }\n\n\
         #[test]\nfn unit() { only_unit_test(); }\n\n\
         #[cfg(test)]\nmod tests {\n    fn helper() { super::only_cfg_test_helper(); }\n}\n",
    ),
    ("tests/it.rs", "#[test]\nfn it() {}\n\nfn helper() {}\n"),
];

#[test]
fn rust_test_attribute_functions_are_not_roots_with_the_option() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-troots-rs-", RUST);
    let default = call(&mut ix, None);
    assert!(row(&default, "only_unit_test").is_none(), "{default}");
    let strict = call(&mut ix, Some(false));
    let r = row(&strict, "only_unit_test").unwrap_or_else(|| panic!("{strict}"));
    assert!(
        r["reason"]
            .as_str()
            .unwrap()
            .starts_with("reached_only_from_tests"),
        "{r}"
    );
    // a helper inside a `#[cfg(test)]` module is test code too
    assert!(row(&default, "only_cfg_test_helper").is_none(), "{default}");
    assert!(row(&strict, "only_cfg_test_helper").is_some(), "{strict}");
    assert!(row(&strict, "::used").is_none(), "{strict}");
    assert!(row(&strict, "::unit").is_none(), "{strict}");
}

const TS: &[(&str, &str)] = &[
    (
        "package.json",
        "{\"name\":\"app\",\"main\":\"src/index.ts\"}\n",
    ),
    (
        "src/index.ts",
        "import { Box } from './box';\nexport const b = new Box();\nexport const v = b.size;\n",
    ),
    (
        "src/box.ts",
        "export class Box {\n  private n = 0;\n  get size(): number { return this.n; }\n  set size(v: number) { this.n = v; }\n  get only(): number { return 1; }\n  set only(v: number) {}\n}\n",
    ),
    (
        "src/box.test.ts",
        "import { Box } from './box';\nconst b = new Box();\nb.only = 3;\nconsole.log(b.only);\n",
    ),
];

#[test]
fn ts_accessor_read_only_in_a_test_is_claimed_with_the_option() {
    let (_tmp, _root, mut ix) = common::live_indexer("lidx-dead-troots-ts-", TS);
    let default = call(&mut ix, None);
    assert!(row(&default, "Box.only").is_none(), "{default}");
    assert!(row(&default, "Box.size").is_none(), "{default}");
    let strict = call(&mut ix, Some(false));
    assert!(row(&strict, "Box.only").is_some(), "{strict}");
    // read by production code: still live, with or without the option
    assert!(row(&strict, "Box.size").is_none(), "{strict}");
}
