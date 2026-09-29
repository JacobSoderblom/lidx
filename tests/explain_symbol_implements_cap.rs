/// Regression tests for issue #69: `explain_symbol`'s `implements` section
/// used to have neither a `max_refs` cap nor a byte-budget check, so a
/// symbol with many EXTENDS/IMPLEMENTS/INHERITS edges (e.g. a class with a
/// long multiple-inheritance list) came back with every supertype
/// regardless of what the caller asked for.
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
    dir.push(format!("lidx-explain-implements-{label}-{nanos}-{counter}"));
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

/// Writes one file declaring `count` distinct base classes plus a `Foo` that
/// extends all of them via Python multiple inheritance, so `Foo` carries
/// exactly `count` resolved EXTENDS edges (all within the same module, so
/// bare-name resolution binds every one of them).
fn many_bases_repo(count: usize) -> TempRepo {
    let dir = temp_repo_dir("many-bases");
    let mut source = String::new();
    for i in 0..count {
        source.push_str(&format!("class Base{i}:\n    pass\n\n\n"));
    }
    let bases: Vec<String> = (0..count).map(|i| format!("Base{i}")).collect();
    source.push_str(&format!("class Foo({}):\n    pass\n", bases.join(", ")));
    std::fs::write(dir.join("bases.py"), source).unwrap();

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
fn implements_capped_by_max_refs_reports_honest_total_and_truncation() {
    // 15 distinct bases, default max_refs (10) caps the returned list -- but
    // every one of them resolves cleanly (no byte-budget pressure at the
    // default 40_000-byte budget), so this isolates the max_refs cap.
    let temp = many_bases_repo(15);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"bases.Foo","sections":["implements"]}"#,
    );

    let implements = result["implements"].as_array().expect("implements array");
    assert_eq!(
        implements.len(),
        10,
        "max_refs should cap implements at 10: {:?}",
        result
    );
    assert_eq!(
        result["implements_total"],
        serde_json::json!(15),
        "implements_total must report the true count, not just what fit: {:?}",
        result
    );
    assert_eq!(
        result["budget"]["truncated"],
        serde_json::json!(true),
        "truncated must be true whenever implements items were dropped: {:?}",
        result["budget"]
    );
}

#[test]
fn implements_under_max_refs_reports_untruncated_with_matching_total() {
    // 5 bases, well under the default max_refs (10) -- nothing should be
    // dropped, and implements_total should equal what's actually returned.
    let temp = many_bases_repo(5);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"bases.Foo","sections":["implements"]}"#,
    );

    let implements = result["implements"].as_array().expect("implements array");
    assert_eq!(implements.len(), 5);
    assert_eq!(result["implements_total"], serde_json::json!(5));
    assert_eq!(result["budget"]["truncated"], serde_json::json!(false));
}

#[test]
fn implements_total_matches_returned_when_max_refs_raised_to_cover_all() {
    // Same 15-base repo as the honesty test above, but with max_refs raised
    // high enough to return everything -- truncated must flip back to false.
    let temp = many_bases_repo(15);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"bases.Foo","sections":["implements"],"max_refs":50}"#,
    );

    let implements = result["implements"].as_array().expect("implements array");
    assert_eq!(implements.len(), 15);
    assert_eq!(result["implements_total"], serde_json::json!(15));
    assert_eq!(result["budget"]["truncated"], serde_json::json!(false));
}

#[test]
fn implements_respects_byte_budget_independent_of_max_refs() {
    // max_refs raised out of the way (50, well above the 15 bases) so the
    // only thing that can still cap the list is the byte budget itself.
    // `implements` is the only requested section, so it gets the whole
    // (renormalized, issue #120) max_bytes -- a tight max_bytes still
    // squeezes that budget below what 15 serialized Symbols need.
    let temp = many_bases_repo(15);

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"bases.Foo","sections":["implements"],"max_refs":50,"max_bytes":1200}"#,
    );

    let implements = result["implements"].as_array().expect("implements array");
    assert!(
        !implements.is_empty() && implements.len() < 15,
        "byte budget should have partially filled implements (some but not \
         all of the 15 bases fit in a 1200-byte budget), got {}: {:?}",
        implements.len(),
        result
    );
    assert_eq!(
        result["implements_total"],
        serde_json::json!(15),
        "implements_total must still report the true count: {:?}",
        result
    );
    assert_eq!(
        result["budget"]["truncated"],
        serde_json::json!(true),
        "truncated must reflect the byte-budget drop: {:?}",
        result["budget"]
    );
}
