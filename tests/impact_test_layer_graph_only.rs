//! Issue #231: the impact TEST layer reports a test only when it reaches the
//! seed through graph edges. Name construction (`test_<seed>`, `<Seed>Test`,
//! ...) is not evidence, so a test whose name merely contains the seed's
//! token, in any language, is never reported.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const FIXTURE: &[(&str, &str)] = &[
    (
        "src/Deployer.cs",
        "namespace Shop\n{\n    public class Deployer\n    {\n        public void Deploy()\n        {\n        }\n    }\n}\n",
    ),
    (
        "src/Runner.cs",
        "namespace Shop\n{\n    public class Runner\n    {\n        public void RunAll()\n        {\n            var d = new Deployer();\n            d.Deploy();\n        }\n    }\n}\n",
    ),
    (
        "src/Lonely.cs",
        "namespace Shop\n{\n    public class Lonely\n    {\n        public void Ping()\n        {\n        }\n    }\n}\n",
    ),
    (
        "tests/Shop.Tests/DeployerTests.cs",
        "namespace Shop.Tests\n{\n    public class DeployerTests\n    {\n        [Fact]\n        public void Calls_deploy_directly()\n        {\n            var d = new Deployer();\n            d.Deploy();\n        }\n\n        [Fact]\n        public void Calls_deploy_transitively()\n        {\n            var r = new Runner();\n            r.RunAll();\n        }\n    }\n}\n",
    ),
    // Same language, name contains the seed token, no edge to the seed.
    (
        "tests/Shop.Tests/DeployNameOnlyTests.cs",
        "namespace Shop.Tests\n{\n    public class DeployTests\n    {\n        [Fact]\n        public void DeployTest()\n        {\n            var x = 1;\n        }\n    }\n}\n",
    ),
    // Different language, name contains the seed token, no edge to the seed.
    (
        "py/tests/test_deploy.py",
        "def test_deploy():\n    assert True\n\n\nclass TestDeploy:\n    def test_deploy_thing(self):\n        assert True\n",
    ),
];

fn analyze(indexer: &mut Indexer, params: Value) -> Value {
    rpc::handle_method(indexer, "analyze_impact", params).unwrap()
}

fn test_entries(result: &Value) -> Vec<Value> {
    result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["relationship"] == "TEST")
        .cloned()
        .collect()
}

fn names(entries: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = entries
        .iter()
        .map(|e| e["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

fn build() -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-impact-test-layer-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FIXTURE);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

#[test]
fn name_only_tests_are_not_reported_but_graph_reachable_tests_are() {
    let (_tmp, mut indexer) = build();
    let result = analyze(
        &mut indexer,
        json!({"qualname": "Shop.Deployer.Deploy", "direction": "upstream", "max_depth": 3}),
    );
    let tests = test_entries(&result);
    let found = names(&tests);
    assert!(
        found
            .iter()
            .any(|q| q.ends_with("DeployerTests.Calls_deploy_directly")),
        "direct caller test missing: {found:?}"
    );
    assert!(
        found
            .iter()
            .any(|q| q.ends_with("DeployerTests.Calls_deploy_transitively")),
        "transitive caller test missing: {found:?}"
    );
    let all: Vec<String> = result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !all.iter()
            .any(|q| q.contains("test_deploy") || q.contains("TestDeploy")),
        "cross-language name-only python test reported: {all:?}"
    );
    assert!(
        !all.iter().any(|q| q.contains("DeployTests")),
        "same-language name-only test reported: {all:?}"
    );
}

#[test]
fn every_test_entry_carries_a_path_ending_at_the_seed() {
    let (_tmp, mut indexer) = build();
    let result = analyze(
        &mut indexer,
        json!({"qualname": "Shop.Deployer.Deploy", "direction": "downstream", "max_depth": 1}),
    );
    let tests = test_entries(&result);
    assert!(
        !tests.is_empty(),
        "expected graph-reachable tests: {result}"
    );
    for e in &tests {
        let steps = e["path"]["steps"].as_array().unwrap_or_else(|| {
            panic!("test entry has no path.steps: {e}");
        });
        assert!(!steps.is_empty(), "empty path.steps: {e}");
        assert_eq!(
            steps.last().unwrap()["to_symbol"],
            "Shop.Deployer.Deploy",
            "last step must reach the seed: {e}"
        );
        assert_eq!(steps[0]["from_symbol"], e["symbol"]["qualname"], "{e}");
        assert_ne!(e["confidence"], json!(0.6), "name-guess confidence: {e}");
    }
}

#[test]
fn empty_test_layer_explains_itself() {
    let (_tmp, mut indexer) = build();
    let result = analyze(
        &mut indexer,
        json!({"qualname": "Shop.Lonely.Ping", "direction": "upstream"}),
    );
    assert!(test_entries(&result).is_empty(), "{result}");
    let layer = &result["test_layer"];
    assert_eq!(layer["empty"], true, "{result}");
    assert!(
        layer["reason"].as_str().is_some_and(|r| !r.is_empty()),
        "{result}"
    );
    assert!(
        layer["next_hops"].as_array().is_some_and(|h| !h.is_empty()),
        "{result}"
    );
}

#[test]
fn non_empty_test_layer_has_no_empty_marker() {
    let (_tmp, mut indexer) = build();
    let result = analyze(
        &mut indexer,
        json!({"qualname": "Shop.Deployer.Deploy", "direction": "upstream"}),
    );
    assert!(!test_entries(&result).is_empty(), "{result}");
    assert!(result.get("test_layer").is_none(), "{result}");
}
