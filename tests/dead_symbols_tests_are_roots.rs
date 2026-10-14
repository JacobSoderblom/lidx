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
        "fn used() {}\nfn only_unit_test() {}\nfn only_integration() {}\n\nfn main() { used(); }\n\n\
         #[test]\nfn unit() { only_unit_test(); }\n",
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
    assert!(row(&strict, "::used").is_none(), "{strict}");
    assert!(row(&strict, "::unit").is_none(), "{strict}");
}
