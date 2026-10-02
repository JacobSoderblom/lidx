//! Issue #227: `analyze_impact` upstream on an `env://` URI must also seed
//! from the readers of its `__`-delimited section ancestors, and report a
//! config-specific message (not "Symbol ... not found") for an unknown URI.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::{Path, PathBuf};

const DEPLOYMENT: &str = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      containers:
        - name: api
          image: img
          env:
            - name: Database__ConnectionString
              value: "x"
            - name: A__B__C
              value: "y"
            - name: Orphan__Var
              value: "z"
            - name: DB_SECRET
              valueFrom:
                secretKeyRef:
                  name: datamgr-db-conn-str
                  key: value
"#;

const BICEP: &str = r#"resource dbSecret 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'datamgr-db-conn-str'
  properties: {
    value: 'x'
  }
}
"#;

const WIRING: &str = r#"
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

namespace Acme
{
    public class DatabaseOptions { public string ConnectionString { get; set; } }

    public static class Wiring
    {
        public static void AddDatabase(IServiceCollection services)
        {
            services.AddOptions<DatabaseOptions>().BindConfiguration("Database");
        }

        public static void ReadSecret(IConfiguration config)
        {
            var s = config.GetSection("datamgr-db-conn-str");
        }
    }
}
"#;

const APP: &str = r#"
using System;

namespace Acme
{
    public class Startup
    {
        public void ReadFull() { var v = Environment.GetEnvironmentVariable("Database__ConnectionString"); }
        public void ReadSection() { var v = Environment.GetEnvironmentVariable("Database"); }
        public void ReadNearMiss() { var v = Environment.GetEnvironmentVariable("DatabaseOther"); }
        public void ReadA() { var v = Environment.GetEnvironmentVariable("A"); }
        public void ReadAB() { var v = Environment.GetEnvironmentVariable("A__B"); }
        public void ReadABC() { var v = Environment.GetEnvironmentVariable("A__B__C"); }
    }
}
"#;

fn setup() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-impact-env-section-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            ("k8s/deploy.yaml", DEPLOYMENT),
            ("infra/main.bicep", BICEP),
            ("App.cs", APP),
            ("Wiring.cs", WIRING),
        ],
    );
    let repo = tmp.path().to_path_buf();
    let db_path = repo.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo, db_path)
}

fn call(repo: &Path, db: &Path, params: &str) -> Value {
    let response = rpc::call(
        repo.to_path_buf(),
        db.to_path_buf(),
        "analyze_impact".to_string(),
        params,
        "1",
    )
    .unwrap();
    let v: Value = serde_json::from_str(&response).unwrap();
    v["result"].clone()
}

fn single(repo: &Path, db: &Path, uri: &str, direction: &str) -> Value {
    call(
        repo,
        db,
        &format!(r#"{{"qualname":"{uri}","direction":"{direction}","max_depth":1}}"#),
    )
}

/// Batch mode: the one entry for `uri`.
fn batch(repo: &Path, db: &Path, uri: &str, direction: &str) -> Value {
    let r = call(
        repo,
        db,
        &format!(r#"{{"qualnames":["{uri}"],"direction":"{direction}","max_depth":1}}"#),
    );
    r["results"][0].clone()
}

fn seed_names(result: &Value) -> Vec<String> {
    let mut names: Vec<String> = result["seeds"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|s| s["qualname"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

/// `(uri, match)` pairs of the result's `config_seeds` note.
fn matches(result: &Value) -> Vec<(String, String)> {
    result["config_seeds"]["matches"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|m| {
            (
                m["uri"].as_str().unwrap().to_string(),
                m["match"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn both_modes(repo: &Path, db: &Path, uri: &str, direction: &str) -> [Value; 2] {
    [
        single(repo, db, uri, direction),
        batch(repo, db, uri, direction),
    ]
}

#[test]
fn full_uri_seeds_from_section_reader() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://DATABASE__CONNECTIONSTRING", "upstream") {
        let names = seed_names(&r);
        assert!(
            names.contains(&"Acme.Startup.ReadFull".to_string()),
            "{names:?}"
        );
        assert!(
            names.contains(&"Acme.Startup.ReadSection".to_string()),
            "{names:?}"
        );
        assert!(
            !names.contains(&"Acme.Startup.ReadNearMiss".to_string()),
            "{names:?}"
        );
        // Exact and section readers are told apart.
        let m = matches(&r);
        assert_eq!(
            m,
            vec![
                (
                    "env://DATABASE__CONNECTIONSTRING".to_string(),
                    "exact".to_string()
                ),
                ("env://DATABASE".to_string(), "section".to_string()),
            ]
        );
    }
}

#[test]
fn section_uri_does_not_seed_from_near_miss() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://DATABASE", "upstream") {
        let names = seed_names(&r);
        assert!(
            names.contains(&"Acme.Startup.ReadSection".to_string()),
            "{names:?}"
        );
        assert!(
            !names.contains(&"Acme.Startup.ReadFull".to_string()),
            "{names:?}"
        );
        assert!(
            !names.contains(&"Acme.Startup.ReadNearMiss".to_string()),
            "{names:?}"
        );
        assert_eq!(
            matches(&r),
            vec![("env://DATABASE".to_string(), "exact".to_string())]
        );
    }
}

#[test]
fn three_level_uri_seeds_from_every_ancestor() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://A__B__C", "upstream") {
        let names = seed_names(&r);
        for expected in ["ReadABC", "ReadAB", "ReadA"] {
            let q = format!("Acme.Startup.{expected}");
            assert!(names.contains(&q), "{q} in {names:?}");
        }
        assert_eq!(
            matches(&r),
            vec![
                ("env://A__B__C".to_string(), "exact".to_string()),
                ("env://A__B".to_string(), "section".to_string()),
                ("env://A".to_string(), "section".to_string()),
            ]
        );
    }
}

#[test]
fn unknown_uri_reports_config_not_found() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://NOWHERE__AT_ALL", "upstream") {
        let text = r.to_string();
        assert!(!text.contains("Symbol 'env://"), "{text}");
        assert!(text.contains("No config source or reader"), "{text}");
        assert!(text.contains("next_hops"), "{text}");
    }
}

#[test]
fn both_direction_has_no_section_seeding() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://DATABASE__CONNECTIONSTRING", "both") {
        assert!(r.get("config_seeds").is_none(), "{r}");
        let names = seed_names(&r);
        assert!(
            !names.contains(&"Acme.Startup.ReadSection".to_string()),
            "{names:?}"
        );
    }
}

#[test]
fn bound_options_reader_found_via_section() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://DATABASE__CONNECTIONSTRING", "upstream") {
        let names = seed_names(&r);
        assert!(
            names.contains(&"Acme.Wiring.AddDatabase".to_string()),
            "{names:?}\n{r}"
        );
    }
}

#[test]
fn known_uri_without_readers_gets_accurate_message() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "env://ORPHAN__VAR", "upstream") {
        let text = r.to_string();
        assert!(text.contains("is in the index"), "{text}");
        assert!(!text.contains("appears in no CONFIG"), "{text}");
        assert!(!text.contains("Symbol 'env://"), "{text}");
    }
}

#[test]
fn unknown_message_is_identical_in_single_and_batch() {
    let (_tmp, repo, db) = setup();
    let [s, b] = both_modes(&repo, &db, "env://NOWHERE", "upstream");
    assert_eq!(s["message"], b["layers"]["direct"]["error"]);
    assert_eq!(s["message"], b["recovery"]["message"]);
}

#[test]
fn secret_uri_upstream_unaffected() {
    let (_tmp, repo, db) = setup();
    for r in both_modes(&repo, &db, "secret://datamgr-db-conn-str", "upstream") {
        // No section logic: at most an exact match, never a section one.
        assert!(matches(&r).iter().all(|(_, kind)| kind == "exact"), "{r}");
    }
    for r in both_modes(&repo, &db, "secret://nowhere-at-all", "upstream") {
        let text = r.to_string();
        assert!(
            text.contains("No config source or reader was found"),
            "{text}"
        );
        assert!(!text.contains("Symbol 'secret://"), "{text}");
    }
}

#[test]
fn downstream_and_both_ignore_sections() {
    let (_tmp, repo, db) = setup();
    let uri = "env://DATABASE__CONNECTIONSTRING";
    for direction in ["downstream", "both"] {
        for r in both_modes(&repo, &db, uri, direction) {
            assert!(r.get("config_seeds").is_none(), "{direction}: {r}");
            let names = seed_names(&r);
            assert!(
                !names.iter().any(|n| n == "Acme.Startup.ReadSection"),
                "{direction}: {names:?}"
            );
        }
    }
}
