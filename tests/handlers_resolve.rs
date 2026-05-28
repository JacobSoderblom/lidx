/// Integration tests verifying that remaining handlers use resolve::resolve_symbol.
/// These tests call the handlers via rpc::call and confirm that id, qualname, and
/// query all reach the same symbol, matching the behavior that resolve::resolve_symbol
/// provides.
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
    dir.push(format!("lidx-handlers-resolve-{label}-{nanos}-{counter}"));
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
        let src = fixture_path(fixture);
        let repo_root = temp_repo_dir(fixture);
        copy_dir(&src, &repo_root);
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        Self { repo_root, db_path }
    }
}

fn indexed_repo(fixture: &str) -> (TempRepo, Indexer) {
    let temp = TempRepo::new(fixture);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (temp, indexer)
}

/// Calls an RPC method and returns the response envelope as-is.
/// The envelope has the shape `{"id": ..., "result": {...}}` or `{"id": ..., "error": {...}}`.
fn call_raw(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str(&raw).unwrap()
}

/// Calls an RPC method and returns the inner result value (unwrapping the
/// `{"result": ...}` envelope and any truncation wrapper `{"data": ...}`).
/// Panics if the response contains an error.
fn call(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let envelope = call_raw(temp, method, params);
    if let Some(err) = envelope.get("error") {
        panic!("RPC error for {}: {:?}", method, err);
    }
    let result = envelope["result"].clone();
    // Unwrap the truncation envelope if present so tests see the actual result.
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

// ---------------------------------------------------------------------------
// explain_symbol — id / qualname / query all reach the same symbol
// ---------------------------------------------------------------------------

#[test]
fn explain_symbol_resolves_by_query() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();
    drop(indexer);

    // First get the symbol's id and qualname via a query
    let by_query = call(&temp, "explain_symbol", r#"{"query":"Greeter"}"#);
    let symbol = by_query.get("symbol").expect("should have symbol");
    assert_eq!(symbol["name"].as_str().unwrap(), "Greeter");

    let id = symbol["id"].as_i64().unwrap();
    let qualname = symbol["qualname"].as_str().unwrap().to_string();

    // Resolve by id — should reach the same symbol
    let by_id = call(&temp, "explain_symbol", &format!(r#"{{"id":{}}}"#, id));
    assert_eq!(
        by_id["symbol"]["id"].as_i64().unwrap(),
        id,
        "id lookup should return same symbol"
    );

    // Resolve by qualname — should reach the same symbol
    let by_qn = call(
        &temp,
        "explain_symbol",
        &format!(r#"{{"qualname":"{}"}}"#, qualname),
    );
    assert_eq!(
        by_qn["symbol"]["qualname"].as_str().unwrap(),
        qualname,
        "qualname lookup should return same symbol"
    );

    let _ = gv;
}

#[test]
fn explain_symbol_query_not_found_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let response = call_raw(
        &temp,
        "explain_symbol",
        r#"{"query":"xyzzy_does_not_exist_at_all"}"#,
    );
    assert!(
        response.get("error").is_some(),
        "unresolvable query should return an error response: {:?}",
        response
    );
}

#[test]
fn explain_symbol_missing_params_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let response = call_raw(&temp, "explain_symbol", r#"{}"#);
    assert!(
        response.get("error").is_some(),
        "missing id/qualname/query should return an error: {:?}",
        response
    );
}

// ---------------------------------------------------------------------------
// analyze_impact — id / qualname / query all reach the same symbol
// ---------------------------------------------------------------------------

#[test]
fn analyze_impact_resolves_by_query() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    // Get symbol id for Greeter
    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    // Analyze by query — seeds should include the resolved Greeter id
    let by_query = call(&temp, "analyze_impact", r#"{"query":"Greeter"}"#);
    let query_seeds: Vec<i64> = by_query["seeds"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|s| s["id"].as_i64())
        .collect();
    assert!(
        query_seeds.contains(&sym.id),
        "query seeds should include the resolved symbol id, got {:?}",
        query_seeds
    );

    // Analyze by id — should produce the same seed
    let by_id = call(&temp, "analyze_impact", &format!(r#"{{"id":{}}}"#, sym.id));
    let id_seeds: Vec<i64> = by_id["seeds"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|s| s["id"].as_i64())
        .collect();
    assert!(
        id_seeds.contains(&sym.id),
        "id seeds should include the symbol id, got {:?}",
        id_seeds
    );
}

#[test]
fn analyze_impact_query_not_found_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let response = call_raw(
        &temp,
        "analyze_impact",
        r#"{"query":"xyzzy_nonexistent_symbol_xyz"}"#,
    );
    assert!(
        response.get("error").is_some(),
        "unresolvable query should return error: {:?}",
        response
    );
}

// ---------------------------------------------------------------------------
// orient — focus parameter resolution
// ---------------------------------------------------------------------------

#[test]
fn orient_with_focus_query_succeeds() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let result = call(
        &temp,
        "orient",
        r#"{"focus_query":"Greeter","view":"overview"}"#,
    );
    assert!(
        result.get("overview").is_some(),
        "orient with valid focus_query should return overview, got: {:?}",
        result
    );
}

#[test]
fn orient_with_unknown_focus_query_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let response = call_raw(
        &temp,
        "orient",
        r#"{"focus_query":"xyzzy_does_not_exist_xyz"}"#,
    );
    assert!(
        response.get("error").is_some(),
        "orient with unknown focus_query should return error: {:?}",
        response
    );
}

// ---------------------------------------------------------------------------
// Edge case tests — precedence, empty inputs, focus_symbol output shape
// ---------------------------------------------------------------------------

#[test]
fn explain_symbol_id_takes_precedence_over_query() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    // Pass both id and query — id should win (query is nonsense)
    let result = call(
        &temp,
        "explain_symbol",
        &format!(r#"{{"id":{},"query":"xyzzy_nonexistent"}}"#, sym.id),
    );
    assert_eq!(
        result["symbol"]["id"].as_i64().unwrap(),
        sym.id,
        "id should take precedence over query"
    );
}

#[test]
fn analyze_impact_qualname_takes_precedence_over_query() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    // Pass both qualname and query — qualname should win
    let result = call(
        &temp,
        "analyze_impact",
        &format!(
            r#"{{"qualname":"{}","query":"xyzzy_nonexistent"}}"#,
            sym.qualname
        ),
    );
    let seeds: Vec<i64> = result["seeds"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|s| s["id"].as_i64())
        .collect();
    assert!(
        seeds.contains(&sym.id),
        "qualname should take precedence over query, got seeds: {:?}",
        seeds
    );
}

#[test]
fn orient_focus_qualname_takes_precedence_over_focus_query() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    // Pass both focus_qualname (valid) and focus_query (nonsense) — qualname should win
    let result = call(
        &temp,
        "orient",
        &format!(
            r#"{{"focus_qualname":"{}","focus_query":"xyzzy_nonexistent","view":"overview"}}"#,
            sym.qualname
        ),
    );
    // Should succeed (qualname wins), and include focus_symbol
    let focus = result
        .get("focus_symbol")
        .expect("should have focus_symbol when focus_qualname is valid");
    assert_eq!(focus["qualname"].as_str().unwrap(), sym.qualname);
}

#[test]
fn orient_focus_symbol_output_has_expected_fields() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    let result = call(
        &temp,
        "orient",
        &format!(
            r#"{{"focus_qualname":"{}","view":"overview"}}"#,
            sym.qualname
        ),
    );

    let focus = result
        .get("focus_symbol")
        .expect("should have focus_symbol");

    // Verify all expected fields are present and non-null
    assert!(focus.get("id").is_some(), "focus_symbol should have id");
    assert!(focus.get("name").is_some(), "focus_symbol should have name");
    assert!(
        focus.get("qualname").is_some(),
        "focus_symbol should have qualname"
    );
    assert!(focus.get("kind").is_some(), "focus_symbol should have kind");
    assert!(
        focus.get("file_path").is_some(),
        "focus_symbol should have file_path"
    );

    assert_eq!(focus["name"].as_str().unwrap(), "Greeter");
    assert_eq!(focus["kind"].as_str().unwrap(), "class");
}

#[test]
fn orient_without_focus_returns_overview_and_no_focus_symbol() {
    let (temp, _indexer) = indexed_repo("py_mvp");
    let result = call(&temp, "orient", r#"{"view":"overview"}"#);
    assert!(
        result.get("overview").is_some(),
        "orient should return overview when view=overview, got: {:?}",
        result
    );
    assert!(
        result.get("focus_symbol").is_none(),
        "orient without focus params should not include focus_symbol, got: {:?}",
        result.get("focus_symbol")
    );
}

#[test]
fn orient_focus_qualname_empty_string_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");

    let response = call_raw(
        &temp,
        "orient",
        r#"{"focus_qualname":"","view":"overview"}"#,
    );
    assert!(
        response.get("error").is_some(),
        "empty focus_qualname should return error: {:?}",
        response
    );
}

#[test]
fn explain_symbol_nonexistent_id_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");

    let response = call_raw(&temp, "explain_symbol", r#"{"id":999999}"#);
    assert!(
        response.get("error").is_some(),
        "nonexistent id should return error: {:?}",
        response
    );
}

#[test]
fn analyze_impact_nonexistent_id_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");

    let response = call_raw(&temp, "analyze_impact", r#"{"id":999999}"#);
    assert!(
        response.get("error").is_some(),
        "nonexistent id should return error: {:?}",
        response
    );
}

#[test]
fn analyze_impact_missing_params_returns_error() {
    let (temp, _indexer) = indexed_repo("py_mvp");

    let response = call_raw(&temp, "analyze_impact", r#"{}"#);
    assert!(
        response.get("error").is_some(),
        "missing id/qualname/query should return error: {:?}",
        response
    );
}

#[test]
fn analyze_impact_resolves_by_qualname() {
    let (temp, indexer) = indexed_repo("py_mvp");
    let gv = indexer.db().current_graph_version().unwrap();

    let sym = indexer
        .db()
        .find_symbols("Greeter", 5, None, gv)
        .unwrap()
        .into_iter()
        .next()
        .expect("Greeter should exist");
    drop(indexer);

    let result = call(
        &temp,
        "analyze_impact",
        &format!(r#"{{"qualname":"{}"}}"#, sym.qualname),
    );
    let seeds: Vec<i64> = result["seeds"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|s| s["id"].as_i64())
        .collect();
    assert!(
        seeds.contains(&sym.id),
        "qualname lookup should include the symbol, got seeds: {:?}",
        seeds
    );
}
