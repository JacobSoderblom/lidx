//! Issue #249: `analyze_impact` upstream on a container (class, struct,
//! record, interface) must aggregate callers of its members and constructors,
//! agreeing with `trace_flow` and `explain_symbol` on the same seed.

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

const SOURCE: &str = r#"
namespace Acme
{
    public class DeleteCoordinator
    {
        public DeleteCoordinator() { }
        public void DeleteAll() { }
        public void Cleanup() { }
    }

    public class Orphan
    {
        public void Lonely() { }
    }

    public class Consumer
    {
        private readonly DeleteCoordinator _coordinator;
        public Consumer(DeleteCoordinator coordinator) { _coordinator = coordinator; }
        public void Run() { _coordinator.DeleteAll(); }
        public void Both() { _coordinator.DeleteAll(); _coordinator.Cleanup(); }
    }

    public class Builder
    {
        public void Build() { var c = new DeleteCoordinator(); }
    }

    public struct Point
    {
        public void Move() { }
    }

    public record Rec(int A)
    {
        public void Touch() { }
    }

    public interface IGreeter
    {
        void Greet();
    }

    public class Users
    {
        public void UseStruct(Point p) { p.Move(); }
        public void UseRecord(Rec r) { r.Touch(); }
        public void UseIface(IGreeter g) { g.Greet(); }
    }

    public class Settings
    {
        public int Level { get; set; }
    }
}
"#;

fn setup() -> (PathBuf, PathBuf) {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-impact-container-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("App.cs"), SOURCE).unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (dir, db_path)
}

fn call(repo: &Path, db: &Path, method: &str, params: &str) -> Value {
    let response = rpc::call(
        repo.to_path_buf(),
        db.to_path_buf(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let v: Value = serde_json::from_str(&response).unwrap();
    v["result"].clone()
}

fn upstream(repo: &Path, db: &Path, qualname: &str) -> Value {
    call(
        repo,
        db,
        "analyze_impact",
        &format!(r#"{{"qualname":"{qualname}","direction":"upstream","max_depth":1}}"#),
    )
}

fn affected_names(result: &Value) -> Vec<String> {
    let mut names: Vec<String> = result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn class_upstream_includes_member_and_constructor_callers() {
    let (repo, db) = setup();
    let result = upstream(&repo, &db, "Acme.DeleteCoordinator");
    let names = affected_names(&result);
    // Method callers (Run, Both), constructor caller via `new` (Build).
    for expected in [
        "Acme.Builder.Build",
        "Acme.Consumer.Run",
        "Acme.Consumer.Both",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "{expected} in {names:?}"
        );
    }
}

#[test]
fn class_upstream_agrees_with_trace_flow_and_explain_symbol() {
    let (repo, db) = setup();
    let impact = affected_names(&upstream(&repo, &db, "Acme.DeleteCoordinator"));

    let explain = call(
        &repo,
        &db,
        "explain_symbol",
        r#"{"qualname":"Acme.DeleteCoordinator","sections":["callers"]}"#,
    );
    let mut explain_callers: Vec<String> = explain["callers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    explain_callers.sort();
    explain_callers.dedup();
    assert_eq!(explain["callers_total"], 3);
    assert_eq!(explain_callers, impact);

    let trace = call(
        &repo,
        &db,
        "trace_flow",
        r#"{"start_qualname":"Acme.DeleteCoordinator","direction":"upstream","max_hops":1}"#,
    );
    let trace_text = trace.to_string();
    for name in &impact {
        let short = name.rsplit('.').next().unwrap();
        assert!(
            trace_text.contains(short),
            "analyze_impact symbol {name} missing from trace_flow"
        );
    }
}

#[test]
fn caller_through_two_members_appears_once_and_names_a_member() {
    let (repo, db) = setup();
    let result = upstream(&repo, &db, "Acme.DeleteCoordinator");
    let both: Vec<&Value> = result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["symbol"]["qualname"] == "Acme.Consumer.Both")
        .collect();
    // Dedup rule: one entry per caller, with the path naming the member
    // (the first one reached) the dependency runs through.
    assert_eq!(both.len(), 1);
    let step = &both[0]["path"]["steps"][0];
    let touched = format!("{} {}", step["from_symbol"], step["to_symbol"]);
    assert!(
        touched.contains("DeleteCoordinator.DeleteAll")
            || touched.contains("DeleteCoordinator.Cleanup"),
        "{touched}"
    );
    // Distance convention: a caller of a member is one hop from the class.
    assert_eq!(both[0]["distance"], 1);
}

#[test]
fn struct_record_interface_behave_like_class() {
    let (repo, db) = setup();
    for (seed, caller) in [
        ("Acme.Point", "Acme.Users.UseStruct"),
        ("Acme.Rec", "Acme.Users.UseRecord"),
        ("Acme.IGreeter", "Acme.Users.UseIface"),
    ] {
        let names = affected_names(&upstream(&repo, &db, seed));
        assert!(names.iter().any(|n| n == caller), "{seed}: {names:?}");
    }
}

#[test]
fn property_seed_keeps_parent_expansion() {
    let (repo, db) = setup();
    let result = upstream(&repo, &db, "Acme.Settings.Level");
    let seeds: Vec<&str> = result["seeds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["qualname"].as_str().unwrap())
        .collect();
    assert!(seeds.contains(&"Acme.Settings.Level"));
    assert!(seeds.contains(&"Acme.Settings"));
    // Parent class is not expanded into its members.
    assert_eq!(seeds.len(), 2, "{seeds:?}");
}

#[test]
fn downstream_on_class_is_unchanged() {
    let (repo, db) = setup();
    let result = call(
        &repo,
        &db,
        "analyze_impact",
        r#"{"qualname":"Acme.DeleteCoordinator","direction":"downstream","max_depth":1}"#,
    );
    // Members are not seeded for a downstream walk: only the CONTAINS
    // children appear, and no callers leak in.
    assert_eq!(
        affected_names(&result),
        [
            "Acme.DeleteCoordinator..ctor",
            "Acme.DeleteCoordinator.Cleanup",
            "Acme.DeleteCoordinator.DeleteAll"
        ]
    );
    assert_eq!(result["seeds"].as_array().unwrap().len(), 1);
}

#[test]
fn unused_class_is_empty_without_direction_flip_hint() {
    let (repo, db) = setup();
    let result = upstream(&repo, &db, "Acme.Orphan");
    assert_eq!(result["summary"]["total_affected"], 0);
    let hops = result["next_hops"].to_string();
    assert!(!hops.contains("Flip direction"), "{hops}");
}
