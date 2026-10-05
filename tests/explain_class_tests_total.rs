//! Issue #365: `explain_symbol` on a class must report test-scope callers of
//! its members in `tests`/`tests_total` (same aggregation `callers` uses), and
//! the response must say that `tests` is a subset of `callers`.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn explain(files: &[(&str, &str)], qualname: &str) -> Value {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-explain-class-tests-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
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

const BASIC: &[(&str, &str)] = &[
    (
        "src/Svc.cs",
        "namespace Demo;\npublic class Svc { public int Run() => 1; }\n",
    ),
    (
        "tests/SvcTests.cs",
        "namespace Demo.Tests;\npublic class SvcTests { public void RunWorks(Svc s) { s.Run(); } }\n",
    ),
];

const DISPATCH: &[(&str, &str)] = &[
    (
        "src/Svc.cs",
        "namespace Demo;\npublic interface IFoo { int Run(); }\npublic class Svc : IFoo { public int Run() => 1; public int Other() => 2; }\n",
    ),
    (
        "tests/SvcTests.cs",
        "namespace Demo.Tests;\npublic class SvcTests {\n  public void ViaIface(IFoo f) { f.Run(); }\n  public void Both(Svc s) { s.Run(); s.Other(); }\n}\n",
    ),
];

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
    let v = explain(BASIC, "Demo.Svc");
    assert_eq!(v["tests_total"], 1, "{v:#}");
    assert_eq!(names(&v, "tests"), vec!["Demo.Tests.SvcTests.RunWorks"]);
    assert_eq!(v["callers_total"], 1, "{v:#}");
    assert!(v["tests_note"].is_string(), "{v:#}");
}

#[test]
fn method_tests_are_a_subset_of_callers_and_note_says_so() {
    let v = explain(BASIC, "Demo.Svc.Run");
    assert_eq!(v["callers_total"], 1, "{v:#}");
    assert_eq!(v["tests_total"], 1, "{v:#}");
    let callers = names(&v, "callers");
    for t in names(&v, "tests") {
        assert!(callers.contains(&t), "{t} not in callers: {v:#}");
    }
    assert!(v["tests_note"].is_string(), "{v:#}");
    // Compact text mode is the serialized response; the note survives it.
    assert!(serde_json::to_string(&v).unwrap().contains("tests_note"));
}

#[test]
fn class_tests_count_interface_dispatch_and_dedup_members() {
    let v = explain(DISPATCH, "Demo.Svc");
    let mut got = names(&v, "tests");
    got.sort();
    assert_eq!(
        got,
        vec!["Demo.Tests.SvcTests.Both", "Demo.Tests.SvcTests.ViaIface"],
        "{v:#}"
    );
    assert_eq!(v["tests_total"], 2, "{v:#}");
}

#[test]
fn tests_note_absent_when_no_tests() {
    let v = explain(
        &[(
            "src/Svc.cs",
            "namespace Demo;\npublic class Svc { public int Run() => 1; }\n",
        )],
        "Demo.Svc",
    );
    assert_eq!(v["tests_total"], 0, "{v:#}");
    assert!(v.get("tests_note").is_none(), "{v:#}");
}
