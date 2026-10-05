//! Issue #365: `explain_symbol` on a class must report test-scope callers of
//! its members in `tests`/`tests_total` (same aggregation `callers` uses), and
//! the response must say that `tests` is a subset of `callers`.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn explain(qualname: &str) -> Value {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-explain-class-tests-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "src/Svc.cs",
                "namespace Demo;\npublic class Svc { public int Run() => 1; }\n",
            ),
            (
                "tests/SvcTests.cs",
                "namespace Demo.Tests;\npublic class SvcTests { public void RunWorks(Svc s) { s.Run(); } }\n",
            ),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": qualname}),
    )
    .unwrap()
}

fn names(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn class_tests_include_test_callers_of_members() {
    let v = explain("Demo.Svc");
    assert_eq!(v["tests_total"], 1, "{v:#}");
    assert_eq!(names(&v, "tests"), vec!["Demo.Tests.SvcTests.RunWorks"]);
    assert_eq!(v["callers_total"], 1, "{v:#}");
    assert!(
        v["tests_note"].as_str().unwrap().contains("subset"),
        "{v:#}"
    );
}

#[test]
fn method_tests_are_documented_as_subset_of_callers() {
    let v = explain("Demo.Svc.Run");
    assert_eq!(v["callers_total"], 1, "{v:#}");
    assert_eq!(v["tests_total"], 1, "{v:#}");
    assert!(
        v["tests_note"].as_str().unwrap().contains("subset"),
        "{v:#}"
    );
}
