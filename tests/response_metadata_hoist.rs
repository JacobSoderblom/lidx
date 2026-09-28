//! Issue #66 finding: `explain_symbol` hoists `graph_version`/`commit_sha`
//! to the response envelope and strips the redundant copies off every
//! nested `Symbol` (see `tests/explain_symbol_payload_trim.rs`), but that
//! fix was hard-coded to `explain_symbol` alone. `trace_flow`'s `start` and
//! every `trace[].symbol`, and `analyze_diff`'s `changed_symbols[].symbol`
//! and `downstream[].symbol`, still repeat both fields on every entry even
//! though they're constant for the whole response. These seam-A checks pin
//! the same hoist for both methods through the shared dispatch-boundary
//! mechanism (`rpc::handle_method`) rather than a second hard-coded strip.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;

fn call(repo_root: PathBuf, db_path: PathBuf, method: &str, params: &str) -> Value {
    let raw = rpc::call(repo_root, db_path, method.to_string(), params, "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} returned an error: {envelope}"
    );
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

const TARGET_SOURCE: &str = "def target():\n    return 1\n";
const CALLER_SOURCE: &str = "from target import target\n\n\ndef wrapper():\n    return target()\n";

#[test]
fn trace_flow_hoists_graph_version_once_and_strips_nested_symbols() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-hoist-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "trace_flow",
        r#"{"start_qualname":"caller.wrapper","direction":"downstream"}"#,
    );

    let envelope_graph_version = result.get("graph_version").unwrap_or_else(|| {
        panic!("trace_flow must carry graph_version at the envelope: {result:?}")
    });
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    assert!(
        result["start"].get("graph_version").is_none(),
        "start symbol must not repeat graph_version now that it's hoisted: {:?}",
        result["start"]
    );

    let trace = result["trace"].as_array().expect("trace array");
    assert!(!trace.is_empty(), "expected at least one hop: {result:?}");
    for hop in trace {
        assert!(
            hop["symbol"].get("graph_version").is_none(),
            "a hop's nested symbol must not repeat graph_version: {hop:?}"
        );
    }
}

#[test]
fn analyze_diff_hoists_graph_version_once_and_strips_nested_symbols() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-hoist-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "analyze_diff",
        r#"{"paths":["target.py"]}"#,
    );

    let envelope_graph_version = result.get("graph_version").unwrap_or_else(|| {
        panic!("analyze_diff must carry graph_version at the envelope: {result:?}")
    });
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    let changed_symbols = result["changed_symbols"]
        .as_array()
        .expect("changed_symbols array");
    assert!(!changed_symbols.is_empty(), "{result:?}");
    for cs in changed_symbols {
        assert!(
            cs["symbol"].get("graph_version").is_none(),
            "a changed symbol must not repeat graph_version: {cs:?}"
        );
    }

    let downstream = result["downstream"].as_array().expect("downstream array");
    assert!(
        !downstream.is_empty(),
        "expected caller.wrapper to show up downstream of target.target: {result:?}"
    );
    for d in downstream {
        assert!(
            d["symbol"].get("graph_version").is_none(),
            "a downstream entry must not repeat graph_version: {d:?}"
        );
    }
}

fn git(repo_root: &std::path::Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?} failed in {repo_root:?}");
}

/// `git add -A && git commit` with a throwaway identity so this test has no
/// dependency on the machine's global git config, returning the new HEAD sha.
fn git_commit_all(repo_root: &std::path::Path, message: &str) -> String {
    git(repo_root, &["add", "-A"]);
    git(
        repo_root,
        &[
            "-c",
            "user.email=lidx-test@example.com",
            "-c",
            "user.name=lidx test",
            "commit",
            "-m",
            message,
        ],
    );
    lidx::util::git_head_sha(repo_root).expect("HEAD sha after commit")
}

/// Regression: `Db::carry_forward_files` stamps a carried-forward file's
/// symbols with the *new* `graph_version` but keeps their *original*
/// `commit_sha` (see its doc comment in `src/db/mod.rs`) -- so a normal
/// reindex spanning two commits, where one file is unchanged and another is
/// edited, produces a response whose nested `Symbol`-shaped objects agree on
/// `graph_version` but disagree on `commit_sha`. The old all-or-nothing
/// veto in `hoist_symbol_run_metadata` treated that as "any two symbols
/// disagree" and left *everything* untouched, including `graph_version`,
/// which was never actually in dispute. `graph_version` must still hoist
/// and strip whenever it's consistent (it always is, within one response);
/// `commit_sha` must only hoist/strip when it's consistent too, and
/// otherwise stay on the nested symbols that disagree -- dropping it there
/// would lose real information about which commit each symbol actually
/// came from.
#[test]
fn explain_symbol_hoists_graph_version_even_when_commit_sha_disagrees_after_carry_forward() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-hoist-carry-forward-")
        .tempdir()
        .unwrap();
    let repo_root = tmp.path().to_path_buf();
    common::write_files(
        &repo_root,
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );

    git(&repo_root, &["init", "-q"]);
    let commit1 = git_commit_all(&repo_root, "initial");

    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Edit caller.py only -- target.py stays byte-identical across commits,
    // so its symbols are carried forward with commit1's sha still attached,
    // while caller.py is freshly re-parsed and picks up commit2's sha.
    common::write_files(
        &repo_root,
        &[(
            "caller.py",
            "from target import target\n\n\ndef wrapper():\n    # edited\n    return target()\n",
        )],
    );
    let commit2 = git_commit_all(&repo_root, "edit caller");
    assert_ne!(commit1, commit2, "the two commits must actually differ");

    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "explain_symbol",
        r#"{"query":"target.target","sections":["callers"]}"#,
    );

    let envelope_graph_version = result.get("graph_version").unwrap_or_else(|| {
        panic!("explain_symbol must carry graph_version at the envelope: {result:?}")
    });
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    assert!(
        result["symbol"].get("graph_version").is_none(),
        "graph_version is consistent across this response's symbols and must still be \
         hoisted/stripped even though commit_sha disagrees: {:?}",
        result["symbol"]
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert!(!callers.is_empty(), "{result:?}");
    assert!(
        callers[0]["symbol"].get("graph_version").is_none(),
        "a caller's nested symbol must not repeat graph_version either: {:?}",
        callers[0]
    );

    let symbol_commit_sha = result["symbol"]["commit_sha"].as_str().unwrap_or_else(|| {
        panic!("carried-forward symbol must keep its real (disagreeing) commit_sha: {result:?}")
    });
    let caller_commit_sha = callers[0]["symbol"]["commit_sha"]
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "freshly parsed caller symbol must keep its real (disagreeing) commit_sha: {:?}",
                callers[0]
            )
        });
    assert_eq!(
        symbol_commit_sha, commit1,
        "target.py was carried forward, so its symbol should still carry commit1's sha"
    );
    assert_eq!(
        caller_commit_sha, commit2,
        "caller.py was re-parsed, so its symbol should carry commit2's sha"
    );
    assert_ne!(
        symbol_commit_sha, caller_commit_sha,
        "the disagreement is exactly what vetoed the old all-or-nothing hoist"
    );
}
