//! Issue #238: C# `dead_symbols` false positives -- classes whose members are
//! live, and `override`s / external-base members the framework calls.

use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

const FILES: &[(&str, &str)] = &[
    (
        "Helpers.cs",
        r#"namespace App;
public static class Helpers
{
    public static int Twice(int x) { return x * 2; }
    public static int NeverUsed(int x) { return x; }
}
"#,
    ),
    (
        "Constants.cs",
        r#"namespace App;
public static class Constants
{
    public const string Name = "x";
}
"#,
    ),
    (
        "Ghost.cs",
        r#"namespace App;
public class Ghost
{
    private int _state;
    public void Nothing() { }
}
"#,
    ),
    (
        "Framework.cs",
        r#"using System;
using Azure.Core;
namespace App;
public class FakeCredential : TokenCredential
{
    public override string GetToken() { return ""; }
}
public class Cleaner : IDisposable
{
    public void Dispose() { }
}
"#,
    ),
    (
        "Inrepo.cs",
        r#"namespace App;
public abstract class Base
{
    public abstract void Run();
}
public class Derived : Base
{
    public override void Run() { }
}
"#,
    ),
    (
        "Program.cs",
        r#"namespace App;
public class Program
{
    public static void Entry(Base b)
    {
        var n = Helpers.Twice(2);
        var c = Constants.Name;
        b.Run();
    }
}
"#,
    ),
];

fn setup() -> (PathBuf, Indexer) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let repo_root = std::env::temp_dir().join(format!("lidx-deadcs-{nanos}-{counter}"));
    std::fs::create_dir_all(&repo_root).unwrap();
    for (name, body) in FILES {
        std::fs::write(repo_root.join(name), body).unwrap();
    }
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (repo_root, indexer)
}

fn dead(indexer: &mut Indexer, params: serde_json::Value) -> Vec<String> {
    let mut params = params;
    params["include_unused_imports"] = false.into();
    params["include_orphan_tests"] = false.into();
    let result = rpc::handle_method(indexer, "dead_symbols", params).unwrap();
    result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect()
}

#[test]
fn class_with_live_member_is_not_dead() {
    let (root, mut ix) = setup();
    let names = dead(&mut ix, serde_json::json!({}));
    let _ = std::fs::remove_dir_all(&root);
    assert!(!names.contains(&"App.Helpers".into()), "{names:?}");
    assert!(!names.contains(&"App.Helpers.Twice".into()), "{names:?}");
    // Propagation must not mask a genuinely unused sibling member.
    assert!(names.contains(&"App.Helpers.NeverUsed".into()), "{names:?}");
}

#[test]
fn class_with_only_referenced_constant_is_not_dead() {
    let (root, mut ix) = setup();
    let names = dead(&mut ix, serde_json::json!({}));
    let _ = std::fs::remove_dir_all(&root);
    assert!(!names.contains(&"App.Constants".into()), "{names:?}");
}

#[test]
fn truly_unused_class_is_still_dead() {
    let (root, mut ix) = setup();
    let names = dead(&mut ix, serde_json::json!({}));
    let _ = std::fs::remove_dir_all(&root);
    assert!(names.contains(&"App.Ghost".into()), "{names:?}");
    assert!(names.contains(&"App.Ghost.Nothing".into()), "{names:?}");
}

#[test]
fn overrides_and_external_base_members_are_not_dead() {
    let (root, mut ix) = setup();
    let names = dead(&mut ix, serde_json::json!({}));
    let _ = std::fs::remove_dir_all(&root);
    // override of an external base class
    assert!(
        !names.contains(&"App.FakeCredential.GetToken".into()),
        "{names:?}"
    );
    // implicit implementation of an external interface
    assert!(!names.contains(&"App.Cleaner.Dispose".into()), "{names:?}");
    // override of an in-repo base whose base member has callers
    assert!(!names.contains(&"App.Derived.Run".into()), "{names:?}");
}

#[test]
fn override_modifier_is_stored_in_the_index() {
    let (root, ix) = setup();
    let db = ix.db();
    let gv = db.current_graph_version().unwrap();
    let vis = db
        .symbol_visibility("App.FakeCredential.GetToken", gv)
        .unwrap();
    let plain = db.symbol_visibility("App.Helpers.Twice", gv).unwrap();
    let _ = std::fs::remove_dir_all(&root);
    assert!(vis.unwrap_or_default().split(' ').any(|m| m == "override"));
    assert!(!plain.unwrap_or_default().contains("override"));
}

#[test]
fn limit_returns_a_full_page_of_genuine_results() {
    let (root, mut ix) = setup();
    let all = dead(&mut ix, serde_json::json!({"limit": 1000}));
    assert!(all.len() >= 3, "{all:?}");
    let page = dead(&mut ix, serde_json::json!({"limit": 3}));
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(page.len(), 3, "{page:?}");
    assert_eq!(page, all[..3].to_vec());
}
