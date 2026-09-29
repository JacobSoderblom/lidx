//! Issue #131: config-URI traversal must follow the specific URI through
//! aggregator nodes (SecretProviderClass, K8s container) and scope `env://`
//! bridges to the consuming service.

use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;

const SPC: &str = r#"apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: kv-secrets
spec:
  provider: azure
  parameters:
    objects: |
      - objectName: datamgr-db-conn-str
        objectType: secret
      - objectName: other-secret
        objectType: secret
"#;

const BICEP: &str = r#"resource dbSecret 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'datamgr-db-conn-str'
  properties: {
    value: 'x'
  }
}

resource otherSecret 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'other-secret'
  properties: {
    value: 'y'
  }
}
"#;

fn deployment(name: &str, secret_env: bool) -> String {
    let secret = if secret_env {
        r#"
            - name: DATABASE__CONNECTIONSTRING
              valueFrom:
                secretKeyRef:
                  name: datamgr-db-conn-str
                  key: value"#
    } else {
        ""
    };
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
spec:
  template:
    spec:
      containers:
        - name: {name}
          image: img
          env:
            - name: LOGGING
              value: "info"{secret}
"#
    )
}

fn csharp(ns: &str, var: &str) -> String {
    format!(
        r#"using System;

namespace {ns};

public class Startup {{
    public void Configure() {{
        var v = Environment.GetEnvironmentVariable("{var}");
    }}
}}
"#
    )
}

struct Repo {
    root: PathBuf,
    db: PathBuf,
}

impl Drop for Repo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Unique per test: pid + counter + nanos, so parallel tests never share a dir.
fn fresh_root(prefix: &str) -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{}-{n}-{nanos}", std::process::id()))
}

fn repo() -> Repo {
    let root = fresh_root("lidx-config-scope");
    let files = [
        ("infra/main.bicep", BICEP.to_string()),
        ("infra/apps/datamgr/spc.yaml", SPC.to_string()),
        (
            "infra/apps/datamgr/deployment.yaml",
            deployment("datamgr", true),
        ),
        (
            "infra/apps/dataproxy/deployment.yaml",
            deployment("dataproxy", false),
        ),
        (
            "src/Dpb.DataMgr/Startup.cs",
            csharp("Dpb.DataMgr", "DATABASE__CONNECTIONSTRING"),
        ),
        (
            "src/Dpb.DataProxy/Startup.cs",
            csharp("Dpb.DataProxy", "LOGGING"),
        ),
    ];
    for (path, content) in files {
        let p = root.join(path);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }
    let db = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    Repo { root, db }
}

fn call(repo: &Repo, method: &str, params: &str) -> serde_json::Value {
    let resp = rpc::call(
        repo.root.clone(),
        repo.db.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str::<serde_json::Value>(&resp).unwrap()["result"].clone()
}

/// Every `file_path` mentioned anywhere in the payload.
fn files_in(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, x) in m {
                if (k == "file_path" || k == "path")
                    && let Some(s) = x.as_str()
                {
                    out.push(s.to_string());
                }
                files_in(x, out);
            }
        }
        serde_json::Value::Array(a) => a.iter().for_each(|x| files_in(x, out)),
        _ => {}
    }
}

fn files(v: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    files_in(v, &mut out);
    out
}

#[test]
fn analyze_impact_from_secret_uri_stays_on_that_secret() {
    let repo = repo();
    for direction in ["downstream", "upstream", "both"] {
        let r = call(
            &repo,
            "analyze_impact",
            &format!(
                r#"{{"qualname":"secret://datamgr-db-conn-str","direction":"{direction}","max_depth":5,"kinds":["CONFIG_SOURCE","CONFIG_READ","CONFIG_BIND"]}}"#
            ),
        );
        let f = files(&r["affected"]);
        // The other secret's Bicep resource is reachable only through the
        // SecretProviderClass's unrelated CONFIG_READ.
        let bicep_names: Vec<String> = r["affected"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|a| a["symbol"]["qualname"].as_str().map(str::to_string))
            .collect();
        assert!(
            !bicep_names.is_empty(),
            "{direction}: no affected symbols, the leak check would be vacuous: {r}"
        );
        assert!(
            !bicep_names
                .iter()
                .any(|q| q.to_lowercase().contains("othersecret")),
            "{direction}: leaked unrelated secret: {bicep_names:?}"
        );
        assert!(
            !f.iter()
                .any(|p| p.contains("Dpb.DataProxy") || p.contains("dataproxy")),
            "{direction}: leaked other service: {f:?}"
        );
    }
}

#[test]
fn trace_flow_from_secret_uri_reaches_own_consumer_only() {
    let repo = repo();
    let r = call(
        &repo,
        "trace_flow",
        r#"{"start_qualname":"secret://datamgr-db-conn-str","direction":"downstream","max_hops":6}"#,
    );
    let f = files(&r["trace"]);
    assert!(
        f.iter().any(|p| p.contains("Dpb.DataMgr")),
        "own consumer missing: {r}"
    );
    assert!(
        !f.iter()
            .any(|p| p.contains("Dpb.DataProxy") || p.contains("dataproxy")),
        "leaked other service: {f:?}"
    );
    assert!(!r.to_string().to_lowercase().contains("othersecret"), "{r}");
}

#[test]
fn env_bridge_is_scoped_to_consuming_deployment() {
    let repo = repo();
    // DataProxy code reads env://LOGGING, which both deployments declare.
    let r = call(
        &repo,
        "trace_flow",
        r#"{"start_qualname":"Dpb.DataProxy.Startup.Configure","direction":"both","max_hops":6}"#,
    );
    let f = files(&r["trace"]);
    assert!(
        f.iter().any(|p| p.contains("apps/dataproxy/")),
        "own deployment missing: {r}"
    );
    assert!(
        !f.iter()
            .any(|p| p.contains("apps/datamgr/") || p.contains("Dpb.DataMgr")),
        "bridged into another service: {f:?}"
    );
}

#[test]
fn default_kinds_downstream_from_secret_uri_does_not_fan_out() {
    let repo = repo();
    let r = call(
        &repo,
        "analyze_impact",
        r#"{"qualname":"secret://datamgr-db-conn-str","direction":"downstream","max_depth":5}"#,
    );
    let f = files(&r["affected"]);
    assert!(f.iter().any(|p| p.contains("Dpb.DataMgr")), "{r}");
    assert!(
        !f.iter()
            .any(|p| p.contains("Dpb.DataProxy") || p.contains("dataproxy")),
        "leaked other service: {f:?}"
    );
    assert!(!r.to_string().to_lowercase().contains("othersecret"), "{r}");
}

#[test]
fn upstream_chain_from_reader_reaches_bicep_secret() {
    let repo = repo();
    for direction in ["upstream", "both"] {
        let r = call(
            &repo,
            "analyze_impact",
            &format!(
                r#"{{"qualname":"Dpb.DataMgr.Startup.Configure","direction":"{direction}","max_depth":5}}"#
            ),
        );
        let f = files(&r["affected"]);
        assert!(
            f.iter().any(|p| p == "infra/main.bicep"),
            "{direction}: chain reader -> env -> container -> secret -> bicep severed: {f:?}"
        );
        assert!(!r.to_string().to_lowercase().contains("othersecret"), "{r}");
    }
    let r = call(
        &repo,
        "trace_flow",
        r#"{"start_qualname":"Dpb.DataMgr.Startup.Configure","direction":"upstream","max_hops":6}"#,
    );
    assert!(
        files(&r["trace"]).iter().any(|p| p == "infra/main.bicep"),
        "{r}"
    );
}

#[test]
fn default_kinds_upstream_and_both_from_secret_uri_do_not_fan_out() {
    let repo = repo();
    for direction in ["upstream", "both"] {
        let r = call(
            &repo,
            "analyze_impact",
            &format!(
                r#"{{"qualname":"secret://datamgr-db-conn-str","direction":"{direction}","max_depth":5}}"#
            ),
        );
        let f = files(&r["affected"]);
        assert!(
            !f.iter()
                .any(|p| p.contains("Dpb.DataProxy") || p.contains("dataproxy")),
            "{direction}: leaked other service via external stub: {f:?}"
        );
    }
}

fn hashlib_repo() -> Repo {
    let root = fresh_root("lidx-ext-stub");
    for (name, func) in [("a", "run_a"), ("b", "run_b")] {
        let p = root.join(format!("{name}.py"));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(
            p,
            format!("import hashlib\n\n\ndef {func}():\n    return hashlib.sha256()\n"),
        )
        .unwrap();
    }
    let db = root.join(".lidx").join(".lidx.sqlite");
    Indexer::new(root.clone(), db.clone())
        .unwrap()
        .reindex()
        .unwrap();
    Repo { root, db }
}

#[test]
fn shared_external_api_does_not_connect_unrelated_callers() {
    let repo = hashlib_repo();
    let r = call(
        &repo,
        "analyze_impact",
        r#"{"qualname":"a.run_a","direction":"both","max_depth":5}"#,
    );
    // The stub itself is listed as a leaf, so the test cannot pass vacuously.
    assert!(r.to_string().contains("ext:hashlib.sha256"), "no stub: {r}");
    assert!(!r.to_string().contains("run_b"), "{r}");
    let r = call(
        &repo,
        "trace_flow",
        r#"{"start_qualname":"a.run_a","direction":"both","max_hops":5}"#,
    );
    assert!(r.to_string().contains("ext:hashlib.sha256"), "no stub: {r}");
    assert!(!r.to_string().contains("run_b"), "{r}");
}

#[test]
fn external_stub_as_seed_still_lists_its_callers() {
    let repo = hashlib_repo();
    let r = call(
        &repo,
        "analyze_impact",
        r#"{"qualname":"ext:hashlib.sha256","direction":"upstream","max_depth":5}"#,
    );
    let s = r.to_string();
    assert!(s.contains("run_a") && s.contains("run_b"), "{r}");
}

const TWO_SECRET_DEPLOYMENT: &str = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: svc
spec:
  template:
    spec:
      containers:
        - name: svc
          image: img
          env:
            - name: ALPHA
              valueFrom:
                secretKeyRef:
                  name: secret-alpha
                  key: value
            - name: BETA
              valueFrom:
                secretKeyRef:
                  name: secret-beta
                  key: value
"#;

const TWO_SECRET_BICEP: &str = r#"resource alpha 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'secret-alpha'
  properties: {
    value: 'x'
  }
}

resource beta 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'secret-beta'
  properties: {
    value: 'y'
  }
}
"#;

fn two_secret_repo() -> Repo {
    let root = fresh_root("lidx-config-reentry");
    let reader = r#"using System;

namespace Two;

public class Reader {
    public void Run() {
        var a = Environment.GetEnvironmentVariable("ALPHA");
        var b = Environment.GetEnvironmentVariable("BETA");
    }
}
"#;
    for (path, content) in [
        ("infra/main.bicep", TWO_SECRET_BICEP),
        ("infra/svc/deployment.yaml", TWO_SECRET_DEPLOYMENT),
        ("src/Reader.cs", reader),
    ] {
        let p = root.join(path);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }
    let db = root.join(".lidx").join(".lidx.sqlite");
    Indexer::new(root.clone(), db.clone())
        .unwrap()
        .reindex()
        .unwrap();
    Repo { root, db }
}

/// A container reached through two env URIs is expanded under each scope, so
/// both secrets' Bicep resources are reported (issue #188).
#[test]
fn container_reached_via_two_uris_reports_both_secrets() {
    let repo = two_secret_repo();
    let has = |qualnames: &[String], needle: &str| {
        qualnames.iter().any(|q| q.to_lowercase().contains(needle))
    };
    let r = call(
        &repo,
        "analyze_impact",
        r#"{"qualname":"Two.Reader.Run","direction":"upstream","max_depth":6}"#,
    );
    let affected: Vec<String> = r["affected"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|a| {
            a["symbol"]["qualname"]
                .as_str()
                .or_else(|| a["qualname"].as_str())
                .map(str::to_string)
        })
        .collect();
    assert!(
        has(&affected, "alpha") && has(&affected, "beta"),
        "impact: {affected:?}"
    );
    let r = call(
        &repo,
        "trace_flow",
        r#"{"start_qualname":"Two.Reader.Run","direction":"upstream","max_hops":8,"max_bytes":200000}"#,
    );
    let hops: Vec<String> = r["trace"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|h| h["symbol"]["qualname"].as_str().map(str::to_string))
        .collect();
    assert!(has(&hops, "alpha") && has(&hops, "beta"), "trace: {hops:?}");
}
