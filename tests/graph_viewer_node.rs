//! The viewer's own logic, run under node against pages written by
//! `lidx graph` (tests/viewer_node/harness.js stubs the DOM). Skipped, with
//! a note on stderr, when `node` is not installed: the rest of the suite
//! has no node dependency, and this repository's CI does not run tests.
mod common;

use serde_json::Value;
use std::path::Path;
use std::process::Command;

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn page(root: &Path, db: &Path, args: &[&str]) -> std::path::PathBuf {
    let out = root.join("viewer-test.html");
    let res = Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .arg("--repo")
        .arg(root)
        .arg("--db")
        .arg(db)
        .arg("--out")
        .arg(&out)
        .args(args)
        .output()
        .unwrap();
    assert!(
        res.status.success(),
        "{}",
        String::from_utf8_lossy(&res.stderr)
    );
    out
}

fn run(page: &Path, scenario: &str, arg: &str) -> Value {
    let harness = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/viewer_node/harness.js");
    let out = Command::new("node")
        .arg(harness)
        .arg(page)
        .arg(scenario)
        .arg(arg)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "harness failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("harness prints JSON")
}

/// A symbol in a folder the visible budget folded is shown as itself once
/// searched for: the rebuild that opens its folders must not fold it again
/// and select the folder instead.
#[test]
fn a_searched_symbol_in_a_folded_folder_is_selected_as_itself() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut files: Vec<(String, String)> = Vec::new();
    for m in 0..60 {
        let mut src = String::new();
        if m + 1 < 60 {
            src.push_str(&format!("from mod{}.m import f_{}_0\n", m + 1, m + 1));
        }
        for f in 0..25 {
            src.push_str(&format!("\n\ndef f_{m}_{f}():\n"));
            if f + 1 < 25 {
                src.push_str(&format!("    f_{m}_{}()\n", f + 1));
            }
            if f == 0 && m + 1 < 60 {
                src.push_str(&format!("    f_{}_0()\n", m + 1));
            }
            src.push_str("    return 1\n");
        }
        files.push((format!("mod{m}/m.py"), src));
    }
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, root, db) = common::index_repo("lidx-viewer-fold-", &refs);
    let p = page(
        &root,
        &db,
        &[
            "--seed",
            "f_0_0",
            "--depth",
            "200",
            "--direction",
            "down",
            "--kinds",
            "CALLS",
            "--max-nodes",
            "3000",
        ],
    );
    let r = run(&p, "focus", "");
    assert!(
        r["folded"].as_u64().unwrap() > 0,
        "the fixture must exceed the budget: {r}"
    );
    assert_ne!(r["before"], r["id"], "starts folded into its folder: {r}");
    assert_eq!(r["after"], r["id"], "drawn as itself after the search: {r}");
    assert_eq!(r["selected"], r["id"], "and selected, not its folder: {r}");
}

/// File-level bridges merged into folders count distinct keys: two files of
/// one folder sharing a key with another folder count it once.
#[test]
fn folder_bridges_in_the_viewer_count_distinct_keys() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo(
        "lidx-viewer-keys-",
        &[
            (
                "sender/a.py",
                "def send(msg):\n    _bus.publish(\"alpha\", msg)\n    _bus.publish(\"beta\", msg)\n",
            ),
            (
                "sender/b.py",
                "def send(msg):\n    _bus.publish(\"beta\", msg)\n    _bus.publish(\"gamma\", msg)\n",
            ),
            ("worker/util.py", "def helper():\n    return 1\n"),
            (
                "worker/w.py",
                "@router.subscribe('alpha')\ndef h0(msg):\n    pass\n\n\n@router.subscribe('beta')\ndef h1(msg):\n    pass\n\n\n@router.subscribe('gamma')\ndef h2(msg):\n    pass\n",
            ),
        ],
    );
    // A file-level page opened at the top folders: sender/ and worker/ boxes.
    let p = page(&root, &db, &[]);
    let lines = run(&p, "lines", "");
    let bus: Vec<&Value> = lines
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kinds"].get("VIA_BUS").is_some())
        .collect();
    assert_eq!(bus.len(), 1, "{lines}");
    assert_eq!(bus[0]["from"], "c:sender/");
    assert_eq!(bus[0]["to"], "c:worker/");
    assert_eq!(bus[0]["weight"], 3, "distinct keys, not 2 + 2: {lines}");
}
