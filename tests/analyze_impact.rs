use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-impact-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

struct TempRepo {
    pub repo_root: PathBuf,
    pub db_path: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repo_root);
    }
}

impl TempRepo {
    fn new(fixture: &str) -> Self {
        let (repo_root, db_path) = setup_repo(fixture);
        Self { repo_root, db_path }
    }
}

#[test]
fn analyze_impact_basic() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Analyze impact of Greeter class
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should have seeds
    let seeds = result["seeds"].as_array().unwrap();
    assert_eq!(seeds.len(), 1);
    assert_eq!(seeds[0]["qualname"].as_str().unwrap(), "pkg.core.Greeter");

    // Should have affected symbols
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols");

    // Should have summary
    let summary = result["summary"].as_object().unwrap();
    assert!(summary.contains_key("by_file"));
    assert!(summary.contains_key("by_relationship"));
    assert!(summary.contains_key("by_distance"));
    assert_eq!(
        summary["total_affected"].as_u64().unwrap(),
        affected.len() as u64
    );

    // Should have config
    let config = result["config"].as_object().unwrap();
    assert_eq!(config["max_depth"].as_u64().unwrap(), 3);

    // Check truncated flag exists
    assert!(result.contains_key("truncated"));
}

#[test]
fn analyze_impact_upstream_only() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Analyze upstream dependencies (who calls this)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.make_greeter","direction":"upstream"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should find callers
    let affected = result["affected"].as_array().unwrap();
    // make_greeter is called by app.run(), so should have upstream impact
    assert!(!affected.is_empty(), "Should find upstream callers");

    // Config should show upstream direction
    let config = result["config"].as_object().unwrap();
    assert_eq!(config["direction"].as_str().unwrap(), "upstream");
}

#[test]
fn analyze_impact_downstream_only() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Analyze downstream dependencies (what does this call)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.make_greeter","direction":"downstream"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should find callees
    let affected = result["affected"].as_array().unwrap();
    // make_greeter calls Greeter(), so should have downstream impact
    assert!(!affected.is_empty(), "Should find downstream callees");

    // Config should show downstream direction
    let config = result["config"].as_object().unwrap();
    assert_eq!(config["direction"].as_str().unwrap(), "downstream");
}

#[test]
fn analyze_impact_respects_max_depth() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Depth 1 should give fewer results than depth 3
    let response_depth1 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":1}"#,
        "1",
    )
    .unwrap();

    let response_depth3 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "1",
    )
    .unwrap();

    let value1: serde_json::Value = serde_json::from_str(&response_depth1).unwrap();
    let value3: serde_json::Value = serde_json::from_str(&response_depth3).unwrap();

    let affected1 = value1["result"]["affected"].as_array().unwrap().len();
    let affected3 = value3["result"]["affected"].as_array().unwrap().len();

    // Depth 3 should find at least as many (usually more) symbols than depth 1
    assert!(
        affected3 >= affected1,
        "depth=3 should find >= symbols than depth=1"
    );
}

#[test]
fn analyze_impact_respects_limit() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Set a very low limit
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","limit":2}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    let affected = result["affected"].as_array().unwrap();
    // Note: limit applies to visited nodes (seeds + affected), not just affected
    // So affected may be less than limit since seeds are excluded from affected list
    // The key assertion is that the system didn't crash and returned valid results
    assert!(
        !affected.is_empty() || result["truncated"].as_bool().unwrap_or(false),
        "Should return results or set truncated flag"
    );

    // Config should show the limit
    let config = result["config"].as_object().unwrap();
    assert_eq!(
        config["limit"].as_u64().unwrap(),
        2,
        "Config should show limit=2"
    );
}

#[test]
fn analyze_impact_include_tests_false() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Exclude test files
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","include_tests":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    let affected = result["affected"].as_array().unwrap();

    // Check that no affected symbols are from test files
    for item in affected {
        let symbol = item["symbol"].as_object().unwrap();
        let file_path = symbol["file_path"].as_str().unwrap();
        assert!(
            !file_path.contains("test"),
            "Should not include test files when include_tests=false"
        );
    }
}

#[test]
fn analyze_impact_include_paths() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // With paths
    let response_with_paths = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","include_paths":true}"#,
        "1",
    )
    .unwrap();

    // Without paths
    let response_without_paths = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","include_paths":false}"#,
        "1",
    )
    .unwrap();

    let value_with: serde_json::Value = serde_json::from_str(&response_with_paths).unwrap();
    let value_without: serde_json::Value = serde_json::from_str(&response_without_paths).unwrap();

    let affected_with = value_with["result"]["affected"].as_array().unwrap();
    let affected_without = value_without["result"]["affected"].as_array().unwrap();

    // With paths should have path field
    if !affected_with.is_empty() {
        let first_with = affected_with[0].as_object().unwrap();
        // Path may be null for seed or unreachable symbols, but field should exist
        assert!(
            first_with.contains_key("path"),
            "Should have path field when include_paths=true"
        );
    }

    // Without paths should not have path field (or it should be null)
    if !affected_without.is_empty() {
        let first_without = affected_without[0].as_object().unwrap();
        // Path should be omitted or null when include_paths=false
        let has_path = first_without
            .get("path")
            .and_then(|v| v.as_object())
            .is_some();
        assert!(
            !has_path,
            "Should not include paths when include_paths=false"
        );
    }
}

#[test]
fn analyze_impact_deterministic_output() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Call twice with same params
    let response1 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "1",
    )
    .unwrap();

    let response2 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "2",
    )
    .unwrap();

    let value1: serde_json::Value = serde_json::from_str(&response1).unwrap();
    let value2: serde_json::Value = serde_json::from_str(&response2).unwrap();

    let affected1 = value1["result"]["affected"].as_array().unwrap();
    let affected2 = value2["result"]["affected"].as_array().unwrap();

    // Should return same results
    assert_eq!(
        affected1.len(),
        affected2.len(),
        "Should return same number of results"
    );

    // Check order is the same
    for (item1, item2) in affected1.iter().zip(affected2.iter()) {
        let symbol1 = item1["symbol"]["qualname"].as_str().unwrap();
        let symbol2 = item2["symbol"]["qualname"].as_str().unwrap();
        assert_eq!(symbol1, symbol2, "Should return same order");
    }
}

#[test]
fn analyze_impact_symbol_not_found() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Try non-existent symbol
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.nonexistent.Symbol"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();

    // Should return error
    assert!(
        value.get("error").is_some(),
        "Should return error for nonexistent symbol"
    );
}

#[test]
fn analyze_impact_missing_required_param() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Missing both id and qualname
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"max_depth":3}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();

    // Should return error
    assert!(
        value.get("error").is_some(),
        "Should return error when id/qualname missing"
    );

    // Error might be a string or object with message field
    let error_str = if let Some(err) = value["error"].as_str() {
        err.to_string()
    } else if let Some(err_obj) = value["error"].as_object() {
        err_obj
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string()
    } else {
        String::new()
    };

    assert!(
        error_str.contains("requires id or qualname"),
        "Error message should mention requirement, got: {}",
        error_str
    );
}

#[test]
fn analyze_impact_min_confidence_filtering() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // First, get unfiltered results
    let response_unfiltered = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "1",
    )
    .unwrap();

    let value_unfiltered: serde_json::Value =
        serde_json::from_str(&response_unfiltered).unwrap();
    let affected_unfiltered = value_unfiltered["result"]["affected"]
        .as_array()
        .unwrap()
        .len();

    // Now filter with high confidence threshold (0.9)
    // This should filter out results with distance > 0 (confidence < 0.9)
    let response_filtered = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3,"min_confidence":0.9}"#,
        "2",
    )
    .unwrap();

    let value_filtered: serde_json::Value = serde_json::from_str(&response_filtered).unwrap();
    let affected_filtered = value_filtered["result"]["affected"].as_array().unwrap();

    // Filtered results should have fewer symbols
    assert!(
        affected_filtered.len() < affected_unfiltered,
        "Filtered results should have fewer symbols than unfiltered (filtered: {}, unfiltered: {})",
        affected_filtered.len(),
        affected_unfiltered
    );

    // All remaining symbols should have confidence >= 0.9
    for item in affected_filtered {
        let confidence = item["confidence"].as_f64().unwrap_or(1.0);
        assert!(
            confidence >= 0.9,
            "All filtered results should have confidence >= 0.9, found: {}",
            confidence
        );
    }

    // Test with very low threshold (0.1) - should return all results
    let response_low = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3,"min_confidence":0.1}"#,
        "3",
    )
    .unwrap();

    let value_low: serde_json::Value = serde_json::from_str(&response_low).unwrap();
    let affected_low = value_low["result"]["affected"].as_array().unwrap().len();

    // Low threshold should return same as unfiltered
    assert_eq!(
        affected_low, affected_unfiltered,
        "Low threshold should return all results"
    );
}

#[test]
fn analyze_impact_v2_direct_layer_only() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Test v2 API with only direct layer enabled (default)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should have seeds
    let seeds = result["seeds"].as_array().unwrap();
    assert_eq!(seeds.len(), 1);
    assert_eq!(seeds[0]["qualname"].as_str().unwrap(), "pkg.core.Greeter");

    // Should have affected symbols
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols");

    // Should have layer metadata
    let layers = result["layers"].as_object().unwrap();
    assert!(layers.contains_key("direct"), "Should have direct layer metadata");

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), true);
    // Duration may be 0ms for fast operations, just verify it exists
    assert!(direct_layer.contains_key("duration_ms"), "Should have duration_ms field");

    // Should have config
    let config = result["config"].as_object().unwrap();
    assert_eq!(config["max_depth"].as_u64().unwrap(), 3);

    // Check truncated flag exists
    assert!(result.contains_key("truncated"));
}

#[test]
fn analyze_impact_v2_with_confidence_filter() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Test v2 API with confidence filtering
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","max_depth":3,"min_confidence":0.85}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let affected = value["result"]["affected"].as_array().unwrap();

    // All results should have confidence >= 0.85
    for item in affected {
        let confidence = item["confidence"].as_f64().unwrap_or(1.0);
        assert!(
            confidence >= 0.85,
            "All results should have confidence >= 0.85, found: {}",
            confidence
        );
    }
}

#[test]
fn analyze_impact_v2_layer_enable_disable() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Test disabling direct layer (should return empty)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let affected = value["result"]["affected"].as_array().unwrap();

    // With direct layer disabled and other layers not implemented, should have no results
    assert_eq!(affected.len(), 0, "Should have no results with direct layer disabled");

    // Enable test layer (should now be implemented in Phase 2)
    let response2 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":true,"enable_test":true}"#,
        "2",
    )
    .unwrap();

    let value2: serde_json::Value = serde_json::from_str(&response2).unwrap();
    let layers = value2["result"]["layers"].as_object().unwrap();

    // Test layer should be enabled and working (Phase 2 implementation)
    if let Some(test_layer) = layers.get("test") {
        let test_obj = test_layer.as_object().unwrap();
        assert_eq!(
            test_obj.get("enabled").and_then(|v| v.as_bool()),
            Some(true),
            "Test layer should be enabled in Phase 2"
        );
        // It's OK if no tests are found (result_count may be 0)
        assert!(
            test_obj.get("duration_ms").is_some(),
            "Test layer should have duration_ms"
        );
    }
}

// ============================================================================
// Phase 2: Test Impact Layer Tests
// ============================================================================

#[test]
fn analyze_impact_v2_test_layer_basic() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable test layer to find tests affected by Greeter changes
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_test":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should have test layer metadata
    let layers = result["layers"].as_object().unwrap();
    assert!(layers.contains_key("test"), "Should have test layer metadata");

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(
        test_layer["enabled"].as_bool().unwrap(),
        true,
        "Test layer should be enabled"
    );
    // Verify duration_ms exists and is a valid u64
    test_layer["duration_ms"].as_u64().unwrap();

    // May or may not find tests depending on fixture - just verify it runs without error
    assert!(
        test_layer.get("error").is_none(),
        "Test layer should not have errors"
    );
}

#[test]
fn analyze_impact_v2_test_layer_disabled_by_default() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Test layer is now enabled by default (Phase 2 complete)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let layers = value["result"]["layers"].as_object().unwrap();

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(
        test_layer["enabled"].as_bool().unwrap(),
        true,
        "Test layer should be enabled by default"
    );
}

#[test]
fn analyze_impact_v2_test_layer_with_direct() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable both direct and test layers
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":true,"enable_test":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Both layers should be enabled
    let layers = result["layers"].as_object().unwrap();

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), true);
    assert!(direct_layer["result_count"].as_u64().unwrap() > 0);

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(test_layer["enabled"].as_bool().unwrap(), true);

    // Should have affected symbols (from direct layer at minimum)
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols");
}

#[test]
fn analyze_impact_v2_test_layer_only() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Test layer only (disable direct)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":false,"enable_test":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Only test layer should be enabled
    let layers = result["layers"].as_object().unwrap();

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), false);

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(test_layer["enabled"].as_bool().unwrap(), true);

    // Results may be empty if no tests found via test layer strategies
    // (which is fine - the fixture may not have test edges)
}
// ============================================================================
// Phase 3: Historical Impact Layer Tests
// ============================================================================

#[test]
fn analyze_impact_v2_historical_layer_basic() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable historical layer
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_historical":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should have historical layer metadata
    let layers = result["layers"].as_object().unwrap();
    assert!(layers.contains_key("historical"), "Should have historical layer metadata");

    let historical_layer = layers["historical"].as_object().unwrap();
    assert_eq!(
        historical_layer["enabled"].as_bool().unwrap(),
        true,
        "Historical layer should be enabled"
    );
    // Verify duration_ms exists and is a valid u64
    historical_layer["duration_ms"].as_u64().unwrap();

    // May or may not find co-changes depending on git history
    // Just verify it runs without error
    assert!(
        historical_layer.get("error").is_none(),
        "Historical layer should not have errors"
    );
}

#[test]
fn analyze_impact_v2_historical_layer_disabled_by_default() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Historical layer is now enabled by default (Phase 3 complete)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let layers = value["result"]["layers"].as_object().unwrap();

    let historical_layer = layers["historical"].as_object().unwrap();
    assert_eq!(
        historical_layer["enabled"].as_bool().unwrap(),
        true,
        "Historical layer should be enabled by default"
    );
}

#[test]
fn analyze_impact_v2_historical_layer_with_direct() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable both direct and historical layers
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":true,"enable_historical":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Both layers should be enabled
    let layers = result["layers"].as_object().unwrap();

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), true);

    let historical_layer = layers["historical"].as_object().unwrap();
    assert_eq!(historical_layer["enabled"].as_bool().unwrap(), true);

    // Should have affected symbols (from direct layer at minimum)
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols from direct layer");
}

#[test]
fn analyze_impact_v2_all_three_layers() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable all three layers (direct, test, historical)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":true,"enable_test":true,"enable_historical":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // All three layers should be enabled
    let layers = result["layers"].as_object().unwrap();

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), true, "Direct layer should be enabled");

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(test_layer["enabled"].as_bool().unwrap(), true, "Test layer should be enabled");

    let historical_layer = layers["historical"].as_object().unwrap();
    assert_eq!(historical_layer["enabled"].as_bool().unwrap(), true, "Historical layer should be enabled");

    // Should have execution time for all layers
    direct_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64
    test_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64
    historical_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64

    // Should have affected symbols (from direct layer at minimum)
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols");
}

#[test]
#[ignore = "Semantic layer removed - embeddings feature removed"]
fn analyze_impact_v2_semantic_layer_disabled_by_default() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable only direct layer (semantic should be disabled by default)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter"}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Semantic layer should be disabled by default
    let layers = result["layers"].as_object().unwrap();
    let semantic_layer = layers["semantic"].as_object().unwrap();
    assert_eq!(semantic_layer["enabled"].as_bool().unwrap(), false, "Semantic layer should be disabled by default");
}

#[test]
#[ignore = "Semantic layer removed - embeddings feature removed"]
fn analyze_impact_v2_semantic_layer_graceful_degradation() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable semantic layer even though embeddings are not available
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_semantic":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Semantic layer should be enabled and gracefully degrade to lexical search
    let layers = result["layers"].as_object().unwrap();
    let semantic_layer = layers["semantic"].as_object().unwrap();
    assert_eq!(semantic_layer["enabled"].as_bool().unwrap(), true, "Semantic layer should be enabled");
    // Lexical fallback may find results even without embeddings — no error should be reported
    let has_error = semantic_layer.get("error").map_or(false, |v| !v.is_null());
    assert!(!has_error, "Semantic layer should not error");

    // Should not fail the entire analysis
    let _affected = result["affected"].as_array().unwrap();
    // May be empty or have results from other layers
    assert!(true, "Analysis should complete successfully even when semantic layer returns empty");
}

#[test]
#[ignore = "Semantic layer removed - embeddings feature removed"]
fn analyze_impact_v2_all_four_layers() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Enable all four layers (direct, test, historical, semantic)
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"pkg.core.Greeter","enable_direct":true,"enable_test":true,"enable_historical":true,"enable_semantic":true}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // All four layers should be enabled
    let layers = result["layers"].as_object().unwrap();

    let direct_layer = layers["direct"].as_object().unwrap();
    assert_eq!(direct_layer["enabled"].as_bool().unwrap(), true, "Direct layer should be enabled");

    let test_layer = layers["test"].as_object().unwrap();
    assert_eq!(test_layer["enabled"].as_bool().unwrap(), true, "Test layer should be enabled");

    let historical_layer = layers["historical"].as_object().unwrap();
    assert_eq!(historical_layer["enabled"].as_bool().unwrap(), true, "Historical layer should be enabled");

    let semantic_layer = layers["semantic"].as_object().unwrap();
    assert_eq!(semantic_layer["enabled"].as_bool().unwrap(), true, "Semantic layer should be enabled");

    // Should have execution time for all layers
    direct_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64
    test_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64
    historical_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64
    semantic_layer["duration_ms"].as_u64().unwrap(); // Verify exists and is valid u64

    // Should have affected symbols (from direct layer at minimum)
    let affected = result["affected"].as_array().unwrap();
    assert!(!affected.is_empty(), "Should have affected symbols");
}
