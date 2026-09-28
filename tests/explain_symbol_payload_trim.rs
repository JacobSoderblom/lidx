/// Regression tests for issue #66: `explain_symbol` no longer repeats a
/// reference's signature in two fields, and no longer stamps
/// `graph_version`/`commit_sha` onto every symbol in the response when both
/// are constant for the whole call.
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
    dir.push(format!("lidx-explain-trim-{label}-{nanos}-{counter}"));
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

/// One target function, one plain caller, and one caller that also looks
/// like a test (file name + function name both carry the `test_` marker),
/// so a single `explain_symbol` call exercises the main `symbol` field, the
/// `callers` section, and the `tests` section all at once.
fn repo_with_caller_and_test() -> TempRepo {
    let dir = temp_repo_dir("caller-and-test");
    std::fs::write(dir.join("target.py"), "def target():\n    return 1\n").unwrap();
    std::fs::write(
        dir.join("caller.py"),
        "from target import target\n\n\ndef wrapper(x, y=1):\n    return target()\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("test_target.py"),
        "from target import target\n\n\ndef test_target_wrapper():\n    return target()\n",
    )
    .unwrap();
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
fn caller_ref_carries_signature_once() {
    let temp = repo_with_caller_and_test();

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"]}"#,
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert!(
        !callers.is_empty(),
        "expected at least one caller: {result:?}"
    );
    for caller in callers {
        assert!(
            caller.get("signature").is_none(),
            "outer signature field must be gone, the nested symbol's is authoritative: {caller:?}"
        );
        assert!(
            caller["symbol"].get("signature").is_some(),
            "nested symbol must still carry its own signature: {caller:?}"
        );
    }
}

#[test]
fn graph_version_appears_once_at_envelope_not_on_every_entry() {
    let temp = repo_with_caller_and_test();

    let result = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers","tests"]}"#,
    );

    let envelope_graph_version = result
        .get("graph_version")
        .unwrap_or_else(|| panic!("response must carry graph_version at the envelope: {result:?}"));
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    assert!(
        result["symbol"].get("graph_version").is_none(),
        "the main symbol must not repeat graph_version now that it's hoisted: {:?}",
        result["symbol"]
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert!(!callers.is_empty());
    for caller in callers {
        assert!(
            caller["symbol"].get("graph_version").is_none(),
            "a caller's nested symbol must not repeat graph_version: {caller:?}"
        );
    }

    let tests = result["tests"].as_array().expect("tests array");
    assert!(
        !tests.is_empty(),
        "expected the test-named caller to show up in tests: {result:?}"
    );
    for test_ref in tests {
        assert!(
            test_ref["symbol"].get("graph_version").is_none(),
            "a test ref's nested symbol must not repeat graph_version: {test_ref:?}"
        );
    }
}
