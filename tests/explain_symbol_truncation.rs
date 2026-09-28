/// Regression tests for explain_symbol's truncation honesty.
///
/// Before this fix, explain_symbol capped callers/callees/tests at
/// `max_refs` (default 10) with no total-count field anywhere in the
/// response, and `truncated` was only set from byte-budget overflow -- never
/// from the max_refs cap itself. A symbol with (say) 74 callers would come
/// back with 10 of them and `"truncated": false`, and nothing in the
/// response let a caller tell the difference between "that's everything"
/// and "86% of the answer is missing".
use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-explain-trunc-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

fn call(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(err) = envelope.get("error") {
        panic!("RPC error for {method}: {err:?}");
    }
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

/// Writes `count` python files, each with a distinct top-level function that
/// calls `target.target()` directly, so each contributes exactly one
/// resolved CALLS edge/caller.
fn many_callers_repo(count: usize) -> TempRepo {
    let dir = temp_repo_dir("many-callers");
    std::fs::write(dir.join("target.py"), "def target():\n    return 1\n").unwrap();
    for i in 0..count {
        std::fs::write(
            dir.join(format!("caller_{i}.py")),
            format!("from target import target\n\n\ndef wrapper_{i}():\n    return target()\n"),
        )
        .unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let repo = TempRepo {
        repo_root: dir,
        db_path,
    };
    let mut indexer = Indexer::new(repo.repo_root.clone(), repo.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    repo
}

#[test]
fn callers_capped_by_max_refs_report_honest_total_and_truncation() {
    // 15 distinct callers, default max_refs (10) caps the returned list --
    // but every one of them resolves cleanly (no byte-budget pressure), so
    // the old `truncated` logic (which only fired on byte overflow) would
    // report `false` here. This is the exact shape of the reported bug.
    let temp = many_callers_repo(15);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"]}"#,
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert_eq!(
        callers.len(),
        10,
        "max_refs should still cap the list at 10"
    );
    assert_eq!(
        result["callers_total"],
        serde_json::json!(15),
        "callers_total must report the true count, not just what fit: {:?}",
        result
    );
    assert_eq!(
        result["budget"]["truncated"],
        serde_json::json!(true),
        "truncated must be true whenever items were dropped, even though \
         nothing exceeded the byte budget: {:?}",
        result["budget"]
    );
}

#[test]
fn callers_under_max_refs_report_untruncated_with_matching_total() {
    // 5 callers, well under the default max_refs (10) -- nothing should be
    // dropped, and callers_total should equal what's actually returned.
    let temp = many_callers_repo(5);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"]}"#,
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert_eq!(callers.len(), 5);
    assert_eq!(result["callers_total"], serde_json::json!(5));
    assert_eq!(result["budget"]["truncated"], serde_json::json!(false));
}

#[test]
fn callers_total_matches_returned_when_max_refs_raised_to_cover_all() {
    // Same 15-caller repo as the honesty test above, but with max_refs
    // raised high enough to return everything -- truncated must flip back
    // to false and the counts must agree.
    let temp = many_callers_repo(15);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"],"max_refs":50}"#,
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert_eq!(callers.len(), 15);
    assert_eq!(result["callers_total"], serde_json::json!(15));
    assert_eq!(result["budget"]["truncated"], serde_json::json!(false));
}

#[test]
fn requested_max_bytes_above_hard_cap_is_reported_not_silently_clamped() {
    let temp = many_callers_repo(3);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"],"max_bytes":2000000}"#,
    );

    let budget = &result["budget"];
    assert_eq!(
        budget["budget_bytes"],
        serde_json::json!(200_000),
        "internal budget is hard-capped at 200_000: {:?}",
        budget
    );
    assert_eq!(
        budget["requested_bytes"],
        serde_json::json!(2_000_000),
        "the clamp must be visible in the response, not silent: {:?}",
        budget
    );
}

#[test]
fn max_bytes_within_hard_cap_reports_no_clamp() {
    let temp = many_callers_repo(3);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"],"max_bytes":5000}"#,
    );

    let budget = &result["budget"];
    assert_eq!(budget["budget_bytes"], serde_json::json!(5000));
    assert!(
        budget.get("requested_bytes").is_none(),
        "no clamp happened, so requested_bytes should be absent: {:?}",
        budget
    );
}
