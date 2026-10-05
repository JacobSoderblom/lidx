//! Issue #106: `analyze_diff {paths: [...]}` (no `diff` text, so there are no
//! hunk ranges to compare against) marked every symbol in the file
//! `change_type: "modified"` even when nothing in the file actually changed,
//! and listed its callers under `downstream` with `relationship: "caller"` --
//! callers are upstream (things that depend on the changed symbol), not
//! downstream.

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
    envelope["result"].clone()
}

const TARGET_SOURCE: &str = "def target():\n    return 1\n";
const CALLER_SOURCE: &str = "from target import target\n\n\ndef wrapper():\n    return target()\n";

/// `paths`-only mode has no diff text and no git comparison, so there's no
/// basis to call every symbol in the file "modified" -- most of them likely
/// didn't change at all. It should use a neutral label instead.
#[test]
fn paths_only_mode_labels_symbols_neutrally_not_modified() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-paths-label-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("target.py", TARGET_SOURCE)]);
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

    let changed_symbols = result["changed_symbols"]
        .as_array()
        .expect("changed_symbols array");
    assert!(!changed_symbols.is_empty(), "{result:?}");
    for cs in changed_symbols {
        assert_ne!(
            cs["change_type"].as_str(),
            Some("modified"),
            "paths-only mode has no diff to confirm a modification against; \
             change_type must not claim \"modified\" without evidence: {cs:?}"
        );
    }
}

/// `crate::main`-equivalent `wrapper()` calls `target()`; in `paths`-only
/// mode, `wrapper` is a caller of the changed symbol `target`, which makes it
/// upstream (a consumer of `target`), not downstream.
#[test]
fn paths_only_mode_lists_callers_under_upstream_not_downstream() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-paths-upstream-")
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

    assert!(
        result.get("downstream").is_none(),
        "callers must not be reported under a \"downstream\" key: {result:?}"
    );

    let upstream = result["upstream"]
        .as_array()
        .expect("upstream array missing from analyze_diff response");
    assert!(
        upstream
            .iter()
            .any(|u| u["symbol"]["qualname"].as_str() == Some("caller.wrapper")),
        "expected caller.wrapper (which calls target.target) to show up as an \
         upstream caller: {result:?}"
    );
    for u in upstream {
        assert!(
            u["relationship"]
                .as_str()
                .is_some_and(|r| r.starts_with("caller")),
            "unexpected relationship in upstream entry: {u:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #205: hunk context lines must not count as changes.
//
// Each scenario commits a base tree in a real git repo, applies the edit,
// indexes the edited tree, and feeds `git diff -U<n>` to `analyze_diff` for
// n in {0, 3, 10}. All three context widths must give identical verdicts.
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::process::Command;

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// Returns `{qualname: change_type}` for function/method/class/const symbols,
/// asserting the verdicts are identical for -U0, -U3 and -U10 and that
/// nothing outside `expect_added` is ever reported `added`.
fn verdicts(
    base: &[(&str, &str)],
    edited: &[(&str, &str)],
    expect_added: &[&str],
) -> BTreeMap<String, String> {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-hunks-")
        .tempdir()
        .unwrap();
    let root = tmp.path();
    git(root, &["init", "-q"]);
    common::write_files(root, base);
    git(root, &["add", "-A"]);
    git(
        root,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "-m",
            "base",
        ],
    );
    common::write_files(root, edited);
    let repo_root = root.to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let mut results: Vec<BTreeMap<String, String>> = Vec::new();
    for ctx in ["-U0", "-U3", "-U10"] {
        let diff = git(root, &["diff", "--no-color", ctx]);
        assert!(!diff.is_empty(), "edit produced no diff");
        let params = serde_json::json!({"diff": diff, "max_depth": 1}).to_string();
        let result = call(repo_root.clone(), db_path.clone(), "analyze_diff", &params);
        let mut map = BTreeMap::new();
        for cs in result["changed_symbols"].as_array().unwrap() {
            let kind = cs["symbol"]["kind"].as_str().unwrap_or("");
            if !matches!(kind, "function" | "method" | "class" | "const" | "static") {
                continue;
            }
            map.insert(
                cs["symbol"]["qualname"].as_str().unwrap().to_string(),
                cs["change_type"].as_str().unwrap().to_string(),
            );
        }
        for (q, t) in &map {
            if t == "added" {
                assert!(
                    expect_added.contains(&q.as_str()),
                    "pre-existing symbol {q} reported added with {ctx}: {map:?}"
                );
            }
        }
        results.push(map);
    }
    assert_eq!(results[0], results[1], "-U0 vs -U3 disagree");
    assert_eq!(results[1], results[2], "-U3 vs -U10 disagree");
    results.remove(0)
}

const BASE: &str = "\
def alpha():
    a = 1
    b = 2
    return a + b


def beta():
    x = 10
    y = 20
    z = 30
    return x + y + z


def gamma():
    g = 7
    return g * 2
";

fn expect(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn hunk_inserted_function_is_added_and_neighbours_untouched() {
    let edited = BASE.replace(
        "\n\ndef beta():",
        "\n\ndef inserted():\n    w = 99\n    return w - 1\n\n\ndef beta():",
    );
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &["m.inserted"]);
    assert_eq!(v, expect(&[("m.inserted", "added")]));
}

#[test]
fn hunk_single_line_edit_marks_only_that_function_modified() {
    let edited = BASE.replace("y = 20", "y = 21");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.beta", "modified")]));
}

#[test]
fn hunk_deletion_only_inside_function_is_modified() {
    let edited = BASE.replace("    y = 20\n    z = 30\n", "");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.beta", "modified")]));
}

#[test]
fn hunk_deleting_a_whole_sibling_function_touches_no_neighbour() {
    let edited = BASE.replace("\n\ndef gamma():\n    g = 7\n    return g * 2\n", "");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert!(v.is_empty(), "{v:?}");
}

#[test]
fn hunk_edit_on_last_line_of_function_is_modified_not_added() {
    let edited = BASE.replace("return x + y + z", "return x - y - z");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.beta", "modified")]));
}

#[test]
fn hunk_multi_hunk_multi_file_attributes_each_change_to_its_file() {
    let other = BASE
        .replace("def alpha", "def alpha_b")
        .replace("g = 7", "g = 8");
    let edited_a = BASE.replace("a = 1", "a = 2").replace("z = 30", "z = 31");
    let edited_b = other.replace("b = 2", "b = 3");
    let v = verdicts(
        &[("a.py", BASE), ("b.py", &other)],
        &[("a.py", &edited_a), ("b.py", &edited_b)],
        &[],
    );
    assert_eq!(
        v,
        expect(&[
            ("a.alpha", "modified"),
            ("a.beta", "modified"),
            ("b.alpha_b", "modified"),
        ])
    );
}

/// Rename-only: the renamed function's `def` line is an added line but the
/// body is context, so it is `modified` (never `added`); its old name no
/// longer exists in the index, and neighbours are untouched.
#[test]
fn hunk_rename_only_marks_renamed_function_modified() {
    let edited = BASE.replace("def gamma():", "def gamma2():");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.gamma2", "modified")]));
}

/// Whitespace-only edits are still edited lines: the containing function is
/// `modified`, and nothing else is reported.
#[test]
fn hunk_whitespace_only_change_marks_function_modified() {
    let edited = BASE.replace("    x = 10", "    x  =  10");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.beta", "modified")]));
}

const RS_BASE: &str = "\
const A: i32 = 1;
const B: i32 = 2;
const C: i32 = 3;

fn keep() -> i32 {
    A + B + C
}
";

/// Replacing a pre-existing one-line symbol (`-B = 2` / `+B = 5`) is a
/// modification, never an addition.
#[test]
fn hunk_replaced_one_line_symbol_is_modified_not_added() {
    let edited = RS_BASE.replace("const B: i32 = 2;", "const B: i32 = 5;");
    let v = verdicts(&[("m.rs", RS_BASE)], &[("m.rs", &edited)], &[]);
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(v.values().all(|t| t == "modified"), "{v:?}");
}

/// A genuinely new one-line symbol inserted between two others is added.
#[test]
fn hunk_inserted_one_line_symbol_is_added() {
    let edited = RS_BASE.replace("const C:", "const NEW: i32 = 9;\nconst C:");
    let v = verdicts(
        &[("m.rs", RS_BASE)],
        &[("m.rs", &edited)],
        &["crate::m::NEW"],
    );
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(v.values().all(|t| t == "added"), "{v:?}");
}

/// Python has no closing delimiter, so deleting the last body line leaves no
/// line of the function after it: the deletion sits at the function's end.
#[test]
fn hunk_deleting_trailing_body_line_of_python_function_is_modified() {
    let edited = BASE.replace("    return x + y + z\n", "");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert_eq!(v, expect(&[("m.beta", "modified")]));
}

/// The same position, but the removed lines are a following sibling at the
/// same indentation as the function's `def`: not attributed to it.
#[test]
fn hunk_deleting_following_sibling_does_not_touch_preceding_function() {
    let edited = BASE.replace("def gamma():\n    g = 7\n    return g * 2\n", "");
    let v = verdicts(&[("m.py", BASE)], &[("m.py", &edited)], &[]);
    assert!(v.is_empty(), "{v:?}");
}

// ---------------------------------------------------------------------------
// Issues #338 / #341: diff header spellings, deletions and renames. The index
// holds the clean (pre-change) tree; the diff is of the working-tree edit.
// ---------------------------------------------------------------------------

/// Commit `base`, index it, run `edit`, then feed `git <diff_args>` to
/// `analyze_diff`.
fn diff_result(
    base: &[(&str, &str)],
    edit: impl FnOnce(&std::path::Path),
    diff_args: &[&str],
) -> Value {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-headers-")
        .tempdir()
        .unwrap();
    let root = tmp.path();
    git(root, &["init", "-q"]);
    common::write_files(root, base);
    git(root, &["add", "-A"]);
    git(
        root,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "-m",
            "base",
        ],
    );
    let repo_root = root.to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    edit(root);
    let mut args = vec!["-c", "core.quotePath=true"];
    args.extend_from_slice(diff_args);
    let diff = git(root, &args);
    assert!(!diff.is_empty(), "edit produced no diff");
    let params = serde_json::json!({"diff": diff}).to_string();
    call(repo_root, db_path, "analyze_diff", &params)
}

fn change_types(result: &Value) -> BTreeMap<String, String> {
    result["changed_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|cs| cs["symbol"]["kind"] == "function")
        .map(|cs| {
            (
                cs["symbol"]["qualname"].as_str().unwrap().to_string(),
                cs["change_type"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn diff_header_spellings_all_find_the_changed_symbol() {
    let base = [
        ("my file.py", "def a():\n    return 1\n"),
        ("\u{e5}\u{e4}\u{f6}.py", "def b():\n    return 1\n"),
        ("x.py", "def c():\n    return 1\n"),
    ];
    let edit = |root: &std::path::Path| {
        for f in ["my file.py", "\u{e5}\u{e4}\u{f6}.py", "x.py"] {
            let p = root.join(f);
            let t = std::fs::read_to_string(&p).unwrap();
            std::fs::write(&p, t.replace("return 1", "return 2")).unwrap();
        }
    };
    let want = expect(&[
        ("my file.a", "modified"),
        ("\u{e5}\u{e4}\u{f6}.b", "modified"),
        ("x.c", "modified"),
    ]);
    for args in [
        &["diff", "--no-color"][..],
        &["-c", "diff.mnemonicPrefix=true", "diff", "--no-color"],
        &["diff", "--no-color", "--no-prefix"],
    ] {
        let result = diff_result(&base, edit, args);
        assert_eq!(change_types(&result), want, "{args:?}: {result}");
        assert!(
            result["warnings"].as_array().is_none_or(|w| w.is_empty()),
            "{args:?}: {result}"
        );
    }
}

const A_PY: &str = "def f():\n    return 1\n";
const B_PY: &str = "from a import f\n\n\ndef g():\n    return f()\n";

#[test]
fn deleting_a_file_reports_deleted_symbols_and_upstream_callers() {
    let result = diff_result(
        &[("a.py", A_PY), ("b.py", B_PY)],
        |root| {
            git(root, &["rm", "-q", "a.py"]);
        },
        &["diff", "--cached", "--no-color"],
    );
    assert_eq!(
        change_types(&result),
        expect(&[("a.f", "deleted")]),
        "{result}"
    );
    let callers: Vec<&str> = result["upstream"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["symbol"]["qualname"].as_str().unwrap())
        .collect();
    assert_eq!(callers, ["b.g"], "{result}");
    assert_ne!(result["risk"]["level"], "low", "{result}");
}

#[test]
fn pure_rename_reports_renamed_symbols_instead_of_erroring() {
    let result = diff_result(
        &[("a.py", A_PY), ("b.py", B_PY)],
        |root| {
            git(root, &["mv", "a.py", "c.py"]);
        },
        &["diff", "--cached", "--no-color", "-M"],
    );
    assert_eq!(
        change_types(&result),
        expect(&[("a.f", "renamed")]),
        "{result}"
    );
    assert!(
        result["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("a.py -> c.py")),
        "{result}"
    );
    assert_eq!(
        result["upstream"][0]["symbol"]["qualname"], "b.g",
        "{result}"
    );
}

#[test]
fn mixed_modify_and_delete_reports_both() {
    let result = diff_result(
        &[("a.py", A_PY), ("b.py", B_PY)],
        |root| {
            git(root, &["rm", "-q", "a.py"]);
            std::fs::write(
                root.join("b.py"),
                B_PY.replace("return f()", "return f() + 1"),
            )
            .unwrap();
            git(root, &["add", "b.py"]);
        },
        &["diff", "--cached", "--no-color"],
    );
    assert_eq!(
        change_types(&result),
        expect(&[("a.f", "deleted"), ("b.g", "modified")]),
        "{result}"
    );
}
