//! Issue #249: `analyze_impact` upstream on a container (class, struct,
//! record, interface) must aggregate callers of its members and constructors,
//! agreeing with `trace_flow` and `explain_symbol` on the same seed.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::{Path, PathBuf};

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

    public class Helper
    {
        public void Work() { }
    }

    public class Service
    {
        public void Go(Helper h) { h.Work(); }
    }

    public class Settings
    {
        public int Level { get; set; }
    }
}
"#;

fn setup() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-impact-container-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("App.cs", SOURCE)]);
    let repo = tmp.path().to_path_buf();
    let db_path = repo.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo, db_path)
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
    let (_tmp, repo, db) = setup();
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

/// Sorted, deduplicated caller qualnames `explain_symbol` reports for `seed`.
fn explain_callers(repo: &Path, db: &Path, seed: &str) -> (Vec<String>, Value) {
    let explain = call(
        repo,
        db,
        "explain_symbol",
        &format!(r#"{{"qualname":"{seed}","sections":["callers"]}}"#),
    );
    let mut callers: Vec<String> = explain["callers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    callers.sort();
    callers.dedup();
    (callers, explain["callers_total"].clone())
}

/// Sorted, deduplicated qualnames on `trace_flow`'s upstream trace from `seed`.
fn trace_upstream(repo: &Path, db: &Path, seed: &str) -> Vec<String> {
    let trace = call(
        repo,
        db,
        "trace_flow",
        &format!(r#"{{"start_qualname":"{seed}","direction":"upstream","max_hops":1}}"#),
    );
    let mut names: Vec<String> = trace["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names.dedup();
    names
}

#[test]
fn class_upstream_agrees_with_explain_symbol() {
    let (_tmp, repo, db) = setup();
    let impact = affected_names(&upstream(&repo, &db, "Acme.DeleteCoordinator"));
    let (callers, total) = explain_callers(&repo, &db, "Acme.DeleteCoordinator");
    assert_eq!(total, 3);
    assert_eq!(callers, impact);
}

#[test]
fn class_upstream_agrees_with_trace_flow() {
    let (_tmp, repo, db) = setup();
    let impact = affected_names(&upstream(&repo, &db, "Acme.DeleteCoordinator"));
    let trace = trace_upstream(&repo, &db, "Acme.DeleteCoordinator");
    for name in &impact {
        assert!(
            trace.contains(name),
            "analyze_impact symbol {name} missing from trace_flow {trace:?}"
        );
    }
}

#[test]
fn struct_record_interface_agree_across_all_three_methods() {
    let (_tmp, repo, db) = setup();
    for (seed, caller) in [
        ("Acme.Point", "Acme.Users.UseStruct"),
        ("Acme.Rec", "Acme.Users.UseRecord"),
        ("Acme.IGreeter", "Acme.Users.UseIface"),
    ] {
        let impact = affected_names(&upstream(&repo, &db, seed));
        let (callers, _) = explain_callers(&repo, &db, seed);
        let trace = trace_upstream(&repo, &db, seed);
        assert_eq!(callers, [caller], "explain_symbol {seed}");
        assert_eq!(impact, [caller], "analyze_impact {seed}");
        assert!(
            trace.iter().any(|n| n == caller),
            "trace_flow {seed}: {trace:?}"
        );
    }
}

#[test]
fn default_direction_includes_member_callers_and_keeps_downstream_class_only() {
    let (_tmp, repo, db) = setup();
    let both = call(
        &repo,
        &db,
        "analyze_impact",
        r#"{"qualname":"Acme.DeleteCoordinator","max_depth":1}"#,
    );
    // Member seeds are traversal scaffolding, not reported seeds.
    assert_eq!(both["seeds"].as_array().unwrap().len(), 1);
    let names = affected_names(&both);
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
    // The downstream half is exactly the class-only downstream result: no
    // member callee leaks in beyond it.
    let down = call(
        &repo,
        &db,
        "analyze_impact",
        r#"{"qualname":"Acme.DeleteCoordinator","direction":"downstream","max_depth":1}"#,
    );
    let up = affected_names(&upstream(&repo, &db, "Acme.DeleteCoordinator"));
    let mut expected: Vec<String> = affected_names(&down).into_iter().chain(up).collect();
    expected.sort();
    expected.dedup();
    assert_eq!(names, expected);
}

#[test]
fn default_direction_does_not_leak_member_callees() {
    let (_tmp, repo, db) = setup();
    // Service.Go calls Helper.Work: one hop below the member, two below the
    // class. Seeding the member for a "both" walk must not pull it in at
    // depth 1.
    let both = affected_names(&call(
        &repo,
        &db,
        "analyze_impact",
        r#"{"qualname":"Acme.Service","max_depth":1}"#,
    ));
    let down = affected_names(&call(
        &repo,
        &db,
        "analyze_impact",
        r#"{"qualname":"Acme.Service","direction":"downstream","max_depth":1}"#,
    ));
    assert_eq!(down, ["Acme.Service.Go"]);
    assert_eq!(both, down);
}

#[test]
fn batch_mode_expands_container_members_upstream_and_by_default() {
    let (_tmp, repo, db) = setup();
    for direction in ["upstream", "both"] {
        let result = call(
            &repo,
            &db,
            "analyze_impact",
            &format!(
                r#"{{"qualnames":["Acme.DeleteCoordinator"],"direction":"{direction}","max_depth":1}}"#
            ),
        );
        let entry = &result["results"][0];
        let names = affected_names(entry);
        for expected in [
            "Acme.Builder.Build",
            "Acme.Consumer.Run",
            "Acme.Consumer.Both",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "{direction}: {expected} in {names:?}"
            );
        }
    }
}

#[test]
fn trace_flow_from_struct_record_interface_expands_members() {
    let (_tmp, repo, db) = setup();
    for (seed, caller) in [
        ("Acme.Point", "Acme.Users.UseStruct"),
        ("Acme.Rec", "Acme.Users.UseRecord"),
        ("Acme.IGreeter", "Acme.Users.UseIface"),
    ] {
        let trace = trace_upstream(&repo, &db, seed);
        assert!(trace.iter().any(|n| n == caller), "{seed}: {trace:?}");
    }
}

#[test]
fn caller_through_two_members_appears_once_and_names_a_member() {
    let (_tmp, repo, db) = setup();
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
    let (_tmp, repo, db) = setup();
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
    let (_tmp, repo, db) = setup();
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
    let (_tmp, repo, db) = setup();
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
    let (_tmp, repo, db) = setup();
    let result = upstream(&repo, &db, "Acme.Orphan");
    assert_eq!(result["summary"]["total_affected"], 0);
    let hops = result["next_hops"].to_string();
    assert!(!hops.contains("Flip direction"), "{hops}");
}
