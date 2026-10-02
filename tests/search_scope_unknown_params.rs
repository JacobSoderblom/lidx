//! Issue #213: `search` honours `scope`/`languages`, and unknown params are
//! never silently dropped -- an error on the CLI/raw RPC surface, an
//! `ignored_params` list over MCP (the MCP half lives in `src/mcp.rs` tests).

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const FILES: &[(&str, &str)] = &[
    ("src/app.py", "def needle_app():\n    pass\n"),
    ("src/lib.ts", "export function needle_lib() {}\n"),
    ("tests/test_app.py", "def test_needle():\n    pass\n"),
    ("docs/guide.md", "# Guide\nneedle docs\n"),
    ("examples/demo.py", "def needle_demo():\n    pass\n"),
];

fn build() -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-scope-params-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FILES);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn hit_paths(result: &Value) -> BTreeSet<String> {
    let arr = result["results"]
        .as_array()
        .unwrap_or_else(|| panic!("unexpected search shape: {result}"));
    arr.iter()
        .map(|h| h["path"].as_str().unwrap().to_string())
        .collect()
}

fn search(indexer: &mut Indexer, extra: Value) -> anyhow::Result<Value> {
    let mut params = json!({"query": "needle"});
    for (k, v) in extra.as_object().unwrap() {
        params[k] = v.clone();
    }
    rpc::handle_method(indexer, "search", params)
}

fn set(paths: &[&str]) -> BTreeSet<String> {
    paths.iter().map(|s| s.to_string()).collect()
}

#[test]
fn scope_filters_search_hits() {
    let (_t, mut ix) = build();
    let all = hit_paths(&search(&mut ix, json!({})).unwrap());
    assert_eq!(
        all,
        set(&[
            "src/app.py",
            "src/lib.ts",
            "tests/test_app.py",
            "docs/guide.md",
            "examples/demo.py"
        ])
    );
    let explicit_all = hit_paths(&search(&mut ix, json!({"scope": "all"})).unwrap());
    assert_eq!(explicit_all, all);
    let tests = hit_paths(&search(&mut ix, json!({"scope": "tests"})).unwrap());
    assert_eq!(tests, set(&["tests/test_app.py"]));
    let code = hit_paths(&search(&mut ix, json!({"scope": "code"})).unwrap());
    assert_eq!(code, set(&["src/app.py", "src/lib.ts"]));
    let docs = hit_paths(&search(&mut ix, json!({"scope": "docs"})).unwrap());
    assert_eq!(docs, set(&["docs/guide.md"]));
    let examples = hit_paths(&search(&mut ix, json!({"scope": "examples"})).unwrap());
    assert_eq!(examples, set(&["examples/demo.py"]));
}

#[test]
fn bogus_scope_is_rejected_naming_valid_values() {
    let (_t, mut ix) = build();
    let err = search(&mut ix, json!({"scope": "bogus"}))
        .expect_err("bogus scope must not return results")
        .to_string();
    for valid in ["code", "docs", "tests", "examples", "all"] {
        assert!(err.contains(valid), "error should list '{valid}': {err}");
    }
}

#[test]
fn languages_filters_search_hits() {
    let (_t, mut ix) = build();
    let py = hit_paths(&search(&mut ix, json!({"languages": ["python"]})).unwrap());
    assert_eq!(
        py,
        set(&["src/app.py", "tests/test_app.py", "examples/demo.py"])
    );
    let combined =
        hit_paths(&search(&mut ix, json!({"languages": ["python"], "scope": "tests"})).unwrap());
    assert_eq!(combined, set(&["tests/test_app.py"]));
}

/// Minimal valid params per method (its required fields), plus a stray key.
fn with_bogus(method: &str) -> Value {
    let mut params = match method {
        "search" => json!({"query": "needle"}),
        "outline" | "context" => json!({"path": "src/app.py"}),
        "read_symbol" => json!({"qualname": "needle_app"}),
        _ => json!({}),
    };
    params["bogus_param"] = json!(1);
    params
}

#[test]
fn accepted_aliases_are_not_unknown() {
    let (_t, mut ix) = build();
    for (method, params) in [
        ("search", json!({"pattern": "needle", "version": null})),
        ("search", json!({"q": "needle", "as_of": null})),
        ("search", json!({"text": "needle"})),
        ("trace_flow", json!({"start_query": "needle_app"})),
        ("analyze_diff", json!({"path": ["src/app.py"]})),
        ("context", json!({"path": "src/app.py", "version": null})),
    ] {
        // The call itself may fail for other reasons; it must not be
        // rejected as carrying unknown params.
        if let Err(err) = rpc::handle_method(&mut ix, method, params.clone()) {
            assert!(
                !err.to_string().contains("unknown param"),
                "{method} {params}: {err}"
            );
        }
    }
}

#[test]
fn strict_dispatch_errors_naming_offending_key_for_every_method() {
    let (_t, mut ix) = build();
    for method in rpc::METHOD_LIST {
        let err = rpc::handle_method(&mut ix, method, with_bogus(method))
            .expect_err(&format!("{method} must reject unknown params"))
            .to_string();
        assert!(err.contains("bogus_param"), "{method}: {err}");
    }
}

#[test]
fn lenient_dispatch_runs_and_reports_ignored() {
    let (_t, mut ix) = build();
    let (value, ignored) = rpc::handle_method_lenient(
        &mut ix,
        "search",
        json!({"query": "needle", "bogus_param": true}),
    )
    .unwrap();
    assert_eq!(ignored, vec!["bogus_param".to_string()]);
    assert_eq!(hit_paths(&value).len(), 5);
}

#[test]
fn valid_params_report_nothing_ignored() {
    let (_t, mut ix) = build();
    let (lenient, ignored) =
        rpc::handle_method_lenient(&mut ix, "search", json!({"query": "needle"})).unwrap();
    assert!(ignored.is_empty());
    let strict = rpc::handle_method(&mut ix, "search", json!({"query": "needle"})).unwrap();
    // rg emits files in nondeterministic order; compare the hit sets.
    assert_eq!(hit_paths(&lenient), hit_paths(&strict));
    assert_eq!(
        lenient["results"].as_array().unwrap().len(),
        strict["results"].as_array().unwrap().len()
    );
}

#[test]
fn cli_request_with_unknown_param_exits_nonzero_naming_it() {
    let (tmp, mut ix) = build();
    drop(ix.reindex());
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args(["request", "--repo"])
        .arg(tmp.path())
        .args(["--method", "search", "--params"])
        .arg(r#"{"query":"needle","bogus_param":1}"#)
        .output()
        .unwrap();
    assert!(!out.status.success(), "expected nonzero exit");
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(all.contains("bogus_param"), "output: {all}");

    let ok = std::process::Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args(["request", "--repo"])
        .arg(tmp.path())
        .args(["--method", "search", "--params"])
        .arg(r#"{"query":"needle","scope":"tests"}"#)
        .output()
        .unwrap();
    assert!(ok.status.success());
}
