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

/// Steps run root-to-leaf: the step nearest the seed first, the test's own
/// step last. So the chain is connected end to end when the first step lands
/// on the seed and the last one starts at the test.
fn assert_path_connects(entry: &Value, seed: &str) {
    let steps = entry["path"]["steps"]
        .as_array()
        .unwrap_or_else(|| panic!("test entry has no path.steps: {entry}"));
    assert!(!steps.is_empty(), "empty path.steps: {entry}");
    assert_eq!(
        steps[0]["to_symbol"], seed,
        "first step must reach the seed: {entry}"
    );
    assert_eq!(
        steps.last().unwrap()["from_symbol"],
        entry["symbol"]["qualname"],
        "last step must start at the test: {entry}"
    );
    for pair in steps.windows(2) {
        assert_eq!(pair[1]["to_symbol"], pair[0]["from_symbol"], "{entry}");
    }
    assert_ne!(
        entry["confidence"],
        json!(0.6),
        "name-guess confidence: {entry}"
    );
}

#[test]
fn every_test_entry_carries_a_path_ending_at_the_seed() {
    let (_tmp, mut indexer) = build();
    for (direction, depth) in [("downstream", 1), ("upstream", 3)] {
        let result = analyze(
            &mut indexer,
            json!({"qualname": "Shop.Deployer.Deploy", "direction": direction, "max_depth": depth}),
        );
        let tests = test_entries(&result);
        assert!(
            !tests.is_empty(),
            "expected graph-reachable tests: {result}"
        );
        for e in &tests {
            assert_path_connects(e, "Shop.Deployer.Deploy");
        }
        if depth == 3 {
            let transitive = tests
                .iter()
                .find(|e| {
                    e["symbol"]["qualname"]
                        .as_str()
                        .unwrap()
                        .ends_with("Calls_deploy_transitively")
                })
                .unwrap_or_else(|| panic!("transitive test missing: {result}"));
            assert_eq!(transitive["path"]["steps"].as_array().unwrap().len(), 2);
        }
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

/// Batch mode carries the same explanation per seed, and only for the seed
/// whose layer is empty.
#[test]
fn batch_mode_reports_empty_test_layer_per_seed() {
    let (_tmp, mut indexer) = build();
    let result = analyze(
        &mut indexer,
        json!({
            "qualnames": ["Shop.Lonely.Ping", "Shop.Deployer.Deploy"],
            "direction": "upstream",
        }),
    );
    let results = result["results"].as_array().unwrap();
    let lonely = &results[0]["test_layer"];
    assert_eq!(lonely["empty"], true, "{result}");
    assert!(
        lonely["next_hops"]
            .as_array()
            .is_some_and(|h| !h.is_empty()),
        "{result}"
    );
    assert!(results[1].get("test_layer").is_none(), "{result}");
}

/// The full dispatch path (metadata hoisting, response cap, JSON encoding)
/// keeps `test_layer`, which is what the MCP compact text mode serializes.
#[test]
fn test_layer_survives_the_rpc_wire_format() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-impact-test-layer-wire-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FIXTURE);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    Indexer::new(tmp.path().to_path_buf(), db_path.clone())
        .unwrap()
        .reindex()
        .unwrap();
    let raw = rpc::call(
        tmp.path().to_path_buf(),
        db_path,
        "analyze_impact".to_string(),
        r#"{"qualname":"Shop.Lonely.Ping","direction":"upstream"}"#,
        "1",
    )
    .unwrap();
    let response: Value = serde_json::from_str(&raw).unwrap();
    let layer = &response["result"]["test_layer"];
    assert_eq!(layer["empty"], true, "{raw}");
    assert!(layer["reason"].is_string(), "{raw}");
}

/// Reusing the direct layer's upstream traversal must give exactly the TEST
/// entries the layer's own traversal gives.
#[test]
fn reused_and_standalone_traversals_give_identical_test_entries() {
    use lidx::impact::TraversalDirection;
    use lidx::impact::layers::{TestImpactLayer, analyze_direct_impact};
    use std::collections::HashSet;

    let (_tmp, indexer) = build();
    let gv = indexer.db().current_graph_version().unwrap();
    let seed = indexer
        .db()
        .get_symbol_by_qualname("Shop.Deployer.Deploy", gv)
        .unwrap()
        .unwrap();
    let layer = TestImpactLayer::new(indexer.db()).with_max_depth(3);
    let standalone = layer.analyze(&[seed.id], &[], gv).unwrap();
    let direct = analyze_direct_impact(
        indexer.db(),
        &[seed.id],
        3,
        TraversalDirection::Upstream,
        &HashSet::new(),
        &[],
        true,
        500,
        None,
        gv,
    )
    .unwrap();
    let reused = layer.analyze_traversal(&direct, gv).unwrap();
    assert!(!standalone.impacts.is_empty());

    let key = |r: &lidx::impact::types::LayerResult| {
        let mut impacts = r.impacts.clone();
        impacts.sort_by_key(|(id, _)| *id);
        let mut evidence: Vec<String> = r
            .evidence
            .iter()
            .map(|(id, e)| format!("{id}:{e:?}"))
            .collect();
        evidence.sort();
        let mut parents: Vec<String> = r
            .parent_map
            .iter()
            .map(|(id, p)| format!("{id}:{p:?}"))
            .collect();
        parents.sort();
        (impacts, evidence, parents, r.truncated)
    };
    assert_eq!(key(&standalone), key(&reused));

    // And through the orchestrator: an upstream, tests-included request
    // (reuse) reports the same tests as a "both" request (own traversal).
    let mut reuse = analyze_via_rpc(&indexer, "upstream");
    let mut own = analyze_via_rpc(&indexer, "both");
    reuse.sort();
    own.sort();
    assert_eq!(reuse, own);
    assert!(!reuse.is_empty());
}

fn analyze_via_rpc(indexer: &Indexer, direction: &str) -> Vec<String> {
    let config = lidx::impact::config::MultiLayerConfig::builder()
        .max_depth(3)
        .direction(direction.to_string())
        .include_tests(true)
        .include_paths(true)
        .build();
    let gv = indexer.db().current_graph_version().unwrap();
    let seed = indexer
        .db()
        .get_symbol_by_qualname("Shop.Deployer.Deploy", gv)
        .unwrap()
        .unwrap();
    let result =
        lidx::impact::analyze_impact_multi_layer(indexer.db(), &[seed.id], config, gv).unwrap();
    result
        .affected
        .iter()
        .filter(|e| e.symbol.file_path.contains("tests/"))
        .filter(|e| !e.symbol.file_path.ends_with(".py"))
        .map(|e| e.symbol.qualname.clone())
        .collect()
}
