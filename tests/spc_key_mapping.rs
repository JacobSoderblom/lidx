//! Issues #218 / #217: a SecretProviderClass is a keyed aggregator. Config
//! traversal follows one key through it in both directions: downstream from
//! `secret://X` to the synced secret, container, env URI and code binding;
//! upstream from code to only the Key Vault secret whose `objectName` maps to
//! the key the container reads.

use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;

const BICEP: &str = r#"resource secretDataMgrDbConnStr 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'datamgr-db-conn-str'
  properties: {
    value: 'x'
  }
}

resource secretAppInsightsConnStr 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'appinsights-conn-str'
  properties: {
    value: 'y'
  }
}

resource secretOrchestratorSqlConnStr 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'orchestrator-sql-conn-str'
  properties: {
    value: 'z'
  }
}

resource secretUnrelatedOtherSpc 'Microsoft.KeyVault/vaults/secrets@2021-06-01-preview' = {
  name: 'unrelated-other-spc'
  properties: {
    value: 'w'
  }
}
"#;

// `datamgr-db-conn-str` is synced into two secrets (`kv-secrets` under its own
// name, `kv-mirror` under `mirrored-key`); `orchestrator-sql-conn-str` omits
// `key`, which defaults to the objectName.
const SPC: &str = r#"apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: azure-keyvault-secrets
spec:
  provider: azure
  parameters:
    objects: |
      array:
        - |
          objectName: datamgr-db-conn-str
          objectType: secret
        - |
          objectName: appinsights-conn-str
          objectType: secret
        - |
          objectName: orchestrator-sql-conn-str
          objectType: secret
  secretObjects:
    - secretName: kv-secrets
      type: Opaque
      data:
        - objectName: datamgr-db-conn-str
          key: datamgr-db-conn-str
        - objectName: appinsights-conn-str
          key: appinsights-conn-str
        - objectName: orchestrator-sql-conn-str
    - secretName: kv-mirror
      type: Opaque
      data:
        - objectName: datamgr-db-conn-str
          key: mirrored-key
"#;

const OTHER_SPC: &str = r#"apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: other-spc
spec:
  provider: azure
  parameters:
    objects: |
      array:
        - |
          objectName: unrelated-other-spc
          objectType: secret
  secretObjects:
    - secretName: other-secrets
      type: Opaque
      data:
        - objectName: unrelated-other-spc
          key: unrelated-other-spc
"#;

fn deployment(name: &str, env: &str, secret: &str, key: &str) -> String {
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
            - name: {env}
              valueFrom:
                secretKeyRef:
                  name: {secret}
                  key: {key}
"#
    )
}

const DB_CS: &str = r#"using System;

namespace Dpb.Common.Database;

public class DatabaseOptions {
    public string ConnectionString { get; set; }
}

public static class DatabaseExtensions {
    public static void AddDatabase(IServiceCollection services, IConfiguration config) {
        services.Configure<DatabaseOptions>(config.GetSection("Database"));
    }
}

public class DatabaseConnectionFactory {
    public DatabaseConnectionFactory(IOptions<DatabaseOptions> options) {
    }
}
"#;

const MIRROR_CS: &str = r#"using System;

namespace Dpb.Mirror;

public class MirrorReader {
    public void Run() {
        var v = Environment.GetEnvironmentVariable("MIRROR__CONNECTIONSTRING");
    }
}
"#;

struct Repo {
    root: PathBuf,
    db: PathBuf,
}

impl Drop for Repo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

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
    let root = fresh_root("lidx-spc-keys");
    let files = [
        ("infra/main.bicep", BICEP.to_string()),
        ("infra/k8s/spc/secret-provider-class.yml", SPC.to_string()),
        ("infra/k8s/other/spc.yml", OTHER_SPC.to_string()),
        (
            "infra/k8s/datamgr/deployment.yml",
            deployment(
                "datamgr",
                "Database__ConnectionString",
                "kv-secrets",
                "datamgr-db-conn-str",
            ),
        ),
        (
            "infra/k8s/mirror/deployment.yml",
            deployment(
                "mirror",
                "MIRROR__CONNECTIONSTRING",
                "kv-mirror",
                "mirrored-key",
            ),
        ),
        (
            "infra/k8s/orchestrator/deployment.yml",
            deployment(
                "orchestrator",
                "Orchestrator__SqlConnStr",
                "kv-secrets",
                "orchestrator-sql-conn-str",
            ),
        ),
        ("src/Dpb.Common/Database.cs", DB_CS.to_string()),
        ("src/Dpb.Mirror/MirrorReader.cs", MIRROR_CS.to_string()),
    ];
    for (path, content) in files {
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

fn trace(repo: &Repo, start: &str, direction: &str) -> serde_json::Value {
    let r = call(
        repo,
        "trace_flow",
        &format!(
            r#"{{"start_qualname":"{start}","direction":"{direction}","max_hops":12,"max_response_bytes":2000000}}"#
        ),
    );
    // A truncated trace would mask a partial result (#221/#224).
    assert_eq!(r["truncated"], false, "truncated: {r}");
    r
}

fn impact(repo: &Repo, start: &str, direction: &str) -> Vec<String> {
    let r = call(
        repo,
        "analyze_impact",
        &format!(
            r#"{{"qualname":"{start}","direction":"{direction}","max_depth":12,"max_response_bytes":2000000}}"#
        ),
    );
    r["affected"]
        .as_array()
        .unwrap_or_else(|| panic!("no affected: {r}"))
        .iter()
        .map(|a| a["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

/// (distance, qualname) for every hop, in trace order.
fn hops(r: &serde_json::Value) -> Vec<(u64, String)> {
    r["trace"]
        .as_array()
        .unwrap_or_else(|| panic!("no trace: {r}"))
        .iter()
        .map(|h| {
            (
                h["distance"].as_u64().unwrap(),
                h["symbol"]["qualname"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn names(r: &serde_json::Value) -> Vec<String> {
    hops(r).into_iter().map(|(_, q)| q).collect()
}

fn dist(r: &serde_json::Value, needle: &str) -> u64 {
    hops(r)
        .into_iter()
        .find(|(_, q)| q.contains(needle))
        .unwrap_or_else(|| panic!("{needle} not in {:?}", hops(r)))
        .0
}

fn has(names: &[String], needle: &str) -> bool {
    names.iter().any(|q| q.contains(needle))
}

const SPC_Q: &str = "secretproviderclass/azure-keyvault-secrets";
const DB_CONTAINER: &str = "deployment/datamgr/container/datamgr";
const DB_OPTIONS: &str = "Dpb.Common.Database.DatabaseOptions";
const SECRET: &str = "secret://datamgr-db-conn-str";

/// Names that must never appear: sibling Bicep secrets, the unrelated SPC and
/// the unrelated Bicep secret it lists.
fn assert_no_siblings(n: &[String], what: &str) {
    for bad in [
        "secretAppInsightsConnStr",
        "secretOrchestratorSqlConnStr",
        "secretUnrelatedOtherSpc",
        "other-spc",
    ] {
        assert!(!has(n, bad), "{what}: leaked {bad}: {n:?}");
    }
}

#[test]
fn downstream_from_secret_follows_spc_key_to_containers_and_code() {
    let repo = repo();
    let r = trace(&repo, SECRET, "downstream");
    let n = names(&r);
    // SPC (seed) -> kv-secrets -> container -> env URI -> C# binding.
    let container = dist(&r, DB_CONTAINER);
    let add_database = dist(&r, "DatabaseExtensions.AddDatabase");
    assert_eq!((container, add_database), (1, 2), "{:?}", hops(&r));
    // The same objectName synced into a second secret reaches its consumer.
    assert_eq!(dist(&r, "deployment/mirror/container/mirror"), 1);
    assert!(has(&n, "Dpb.Mirror.MirrorReader.Run"), "{n:?}");
    // Other keys of the same synced secret are not followed.
    assert!(!has(&n, "orchestrator"), "{n:?}");
    assert_no_siblings(&n, "trace downstream");
    assert!(
        !n.iter().any(|q| q.contains(SPC_Q)),
        "revisited the SPC it started from: {n:?}"
    );

    let a = impact(&repo, SECRET, "downstream");
    assert!(has(&a, DB_CONTAINER) && has(&a, "AddDatabase"), "{a:?}");
    assert!(has(&a, "deployment/mirror/container/mirror"), "{a:?}");
    assert!(!has(&a, "orchestrator"), "{a:?}");
    assert_no_siblings(&a, "impact downstream");
}

#[test]
fn upstream_from_options_reaches_only_its_own_bicep_secret() {
    let repo = repo();
    let r = trace(&repo, DB_OPTIONS, "upstream");
    let n = names(&r);
    assert!(has(&n, "infra/main.secretDataMgrDbConnStr"), "{n:?}");
    assert_eq!(
        n.iter().filter(|q| q.starts_with("infra/main.")).count(),
        1,
        "{n:?}"
    );
    assert_no_siblings(&n, "trace upstream");
    // Other containers reading other keys of kv-secrets, and the mirror.
    assert!(!has(&n, "orchestrator") && !has(&n, "mirror"), "{n:?}");
    // The SPC is reached once and never revisited.
    assert_eq!(n.iter().filter(|q| q.contains(SPC_Q)).count(), 1, "{n:?}");
    assert_eq!(
        n.iter().filter(|q| q.contains(DB_CONTAINER)).count(),
        1,
        "container revisited: {:?}",
        hops(&r)
    );
    assert_eq!(r["paths_found"], 1, "{r}");

    let a = impact(&repo, DB_OPTIONS, "upstream");
    assert!(has(&a, "infra/main.secretDataMgrDbConnStr"), "{a:?}");
    assert_no_siblings(&a, "impact upstream");
    assert!(!has(&a, "orchestrator") && !has(&a, "mirror"), "{a:?}");
}

#[test]
fn upstream_narrows_per_synced_secret_and_defaults_omitted_key() {
    let repo = repo();
    // objectName mapped into two synced secrets: each narrows upstream to it.
    let r = trace(&repo, "Dpb.Mirror.MirrorReader.Run", "upstream");
    let n = names(&r);
    assert!(has(&n, "infra/main.secretDataMgrDbConnStr"), "{n:?}");
    assert!(!has(&n, DB_CONTAINER), "{n:?}");
    assert_no_siblings(&n, "mirror upstream");
    // `key` omitted in the SPC defaults to the objectName.
    let r = trace(
        &repo,
        "k8s://default/deployment/orchestrator/container/orchestrator",
        "upstream",
    );
    let n = names(&r);
    assert!(has(&n, "infra/main.secretOrchestratorSqlConnStr"), "{n:?}");
    assert!(!has(&n, "secretAppInsightsConnStr"), "{n:?}");
    assert!(!has(&n, "secretDataMgrDbConnStr"), "{n:?}");
    assert!(!has(&n, DB_CONTAINER), "{n:?}");
}

#[test]
fn spc_config_source_edge_records_object_name_mapping() {
    let repo = repo();
    let conn = rusqlite::Connection::open(&repo.db).unwrap();
    let detail = |target: &str| -> serde_json::Value {
        let d: String = conn
            .query_row(
                "SELECT e.detail FROM edges e JOIN symbols s ON s.id = e.source_symbol_id \
                 WHERE e.kind = 'CONFIG_SOURCE' AND e.target_qualname = ?1 \
                 AND s.qualname LIKE '%azure-keyvault-secrets'",
                [target],
                |r| r.get(0),
            )
            .unwrap();
        serde_json::from_str(&d).unwrap()
    };
    let entry = |object: &str, secret: &str, key: &str| serde_json::json!({"objectName": object, "secretName": secret, "key": key});
    assert_eq!(
        detail("secret://kv-secrets")["mapping"],
        serde_json::json!([
            entry("datamgr-db-conn-str", "kv-secrets", "datamgr-db-conn-str"),
            entry("appinsights-conn-str", "kv-secrets", "appinsights-conn-str"),
            // `key` omitted: defaults to the objectName.
            entry(
                "orchestrator-sql-conn-str",
                "kv-secrets",
                "orchestrator-sql-conn-str"
            ),
        ])
    );
    assert_eq!(
        detail("secret://kv-mirror")["mapping"],
        serde_json::json!([entry("datamgr-db-conn-str", "kv-mirror", "mirrored-key")])
    );
}
