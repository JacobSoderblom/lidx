/// Regression tests for analyze_impact batch mode (issue #224): an oversized
/// per-seed result used to be dropped whole by the byte-budget truncator
/// (`results: []` next to `total_affected: 400`), and a small explicit
/// `limit` was silently raised to the per-seed floor of 50.
///
/// `limit` semantics pinned here: an explicit `limit` is an upper bound on
/// affected symbols per seed. Omitted, the derived per-seed value (with its
/// floor of 50) applies as before.
use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TempRepo {
    root: PathBuf,
    db: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// `hub()` plus a second `other()`, each called by `callers` generated functions.
fn repo(callers: usize) -> TempRepo {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!(
        "lidx-impact-batch-{nanos}-{}",
        TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut src = String::from("def hub():\n    return 1\n\n\ndef other():\n    return 2\n\n\n");
    for i in 0..callers {
        src.push_str(&format!("def caller_{i}():\n    hub()\n    other()\n\n\n"));
    }
    std::fs::write(dir.join("mod.py"), src).unwrap();
    let db = dir.join(".lidx").join(".lidx.sqlite");
    let repo = TempRepo { root: dir, db };
    let mut indexer = Indexer::new(repo.root.clone(), repo.db.clone()).unwrap();
    indexer.reindex().unwrap();
    repo
}

/// Returns the raw result (including any truncation envelope).
fn call(r: &TempRepo, method: &str, params: &str) -> Value {
    let raw = rpc::call(
        r.root.clone(),
        r.db.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let env: Value = serde_json::from_str(&raw).unwrap();
    if let Some(err) = env.get("error").filter(|e| !e.is_null()) {
        panic!("RPC error for {method}: {err:?}");
    }
    env["result"].clone()
}

fn data(result: &Value) -> &Value {
    result.get("data").unwrap_or(result)
}

fn affected_len(entry: &Value) -> usize {
    entry["affected"].as_array().unwrap().len()
}

#[test]
fn oversized_batch_entry_is_shrunk_not_dropped() {
    let r = repo(400);
    let result = call(
        &r,
        "analyze_impact",
        r#"{"qualnames":["mod.hub"],"direction":"upstream","max_depth":1}"#,
    );
    assert_eq!(result["truncated"], true, "{result}");
    let d = data(&result);
    let results = d["results"].as_array().unwrap();
    assert!(!results.is_empty(), "results must not be emptied: {result}");
    let entry = &results[0];
    assert!(affected_len(entry) > 0 && affected_len(entry) < 400);
    assert_eq!(entry["affected_total_available"], 400, "{entry}");
    assert_eq!(entry["truncated"], true);
}

#[test]
fn empty_results_never_coexist_with_nonzero_total_affected() {
    let r = repo(400);
    let mut hit_overflow = false;
    for params in [
        r#"{"qualnames":["mod.hub"],"direction":"upstream"}"#,
        r#"{"qualnames":["mod.hub","mod.other"],"direction":"upstream"}"#,
        r#"{"qualnames":["mod.hub"],"direction":"upstream","max_response_bytes":50}"#,
        r#"{"qualnames":["mod.hub","mod.other"],"direction":"upstream","max_response_bytes":50}"#,
    ] {
        let result = call(&r, "analyze_impact", params);
        let d = data(&result);
        hit_overflow |= result["truncated"] == true;
        assert!(d["total_affected"].as_u64().unwrap() > 0, "{params}");
        assert!(
            !d["results"].as_array().unwrap().is_empty(),
            "{params}: {result}"
        );
    }
    assert!(hit_overflow, "setup must actually overflow the budget");
}

#[test]
fn tiny_budget_keeps_an_emptied_entry_with_note() {
    let r = repo(50);
    let result = call(
        &r,
        "analyze_impact",
        r#"{"qualnames":["mod.hub"],"direction":"upstream","max_response_bytes":50}"#,
    );
    let entry = &data(&result)["results"][0];
    assert!(entry["truncation_note"].is_string(), "{result}");
    assert_eq!(affected_len(entry), 0);
    assert!(entry["affected_total_available"].as_u64().unwrap() >= 50);
}

#[test]
fn batch_and_single_agree_when_budget_is_large() {
    let r = repo(30);
    let args = r#""direction":"upstream","max_depth":1,"max_response_bytes":10000000"#;
    let single = call(
        &r,
        "analyze_impact",
        &format!(r#"{{"qualname":"mod.hub",{args}}}"#),
    );
    let batch = call(
        &r,
        "analyze_impact",
        &format!(r#"{{"qualnames":["mod.hub"],{args}}}"#),
    );
    let ids = |v: &Value| {
        let mut q: Vec<String> = v["affected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["symbol"]["qualname"].as_str().unwrap_or("?").to_string())
            .collect();
        q.sort();
        q
    };
    let s = ids(data(&single));
    assert_eq!(s.len(), 30, "{single}");
    assert_eq!(s, ids(&data(&batch)["results"][0]));
}

#[test]
fn explicit_limit_is_an_upper_bound_per_seed() {
    let r = repo(120);
    let ten = serde_json::to_string(
        &(0..10)
            .map(|i| if i % 2 == 0 { "mod.hub" } else { "mod.other" })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    for quals in [
        r#"["mod.hub"]"#.to_string(),
        r#"["mod.hub","mod.other"]"#.to_string(),
        ten,
    ] {
        let result = call(
            &r,
            "analyze_impact",
            &format!(
                r#"{{"qualnames":{quals},"direction":"upstream","limit":5,"max_depth":1,"max_response_bytes":10000000}}"#
            ),
        );
        for entry in data(&result)["results"].as_array().unwrap() {
            assert!(
                affected_len(entry) <= 5,
                "{quals}: got {}",
                affected_len(entry)
            );
        }
    }
}

#[test]
fn omitted_limit_keeps_per_seed_floor() {
    let r = repo(120);
    let result = call(
        &r,
        "analyze_impact",
        r#"{"qualnames":["mod.hub","mod.other"],"direction":"upstream","max_depth":1,"max_response_bytes":10000000}"#,
    );
    let n = affected_len(&data(&result)["results"][0]);
    assert!(n > 5 && n <= 120, "floor of 50 should apply, got {n}");
}
