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

fn repo() -> Repo {
    let mut root = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    root.push(format!("lidx-config-scope-{nanos}"));
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
            .filter_map(|a| a["qualname"].as_str().map(str::to_string))
            .collect();
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
