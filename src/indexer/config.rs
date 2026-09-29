use serde_json::json;

pub const CONFIG_SOURCE_KIND: &str = "CONFIG_SOURCE";
pub const CONFIG_READ_KIND: &str = "CONFIG_READ";
pub const CONFIG_BIND_KIND: &str = "CONFIG_BIND";

/// Normalize an env var name to a canonical URI: `env://VARNAME`
/// Trims whitespace, rejects empty, uppercases.
pub fn normalize_env_var_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();
    Some(format!("env://{upper}"))
}

/// Normalize a K8s secret name to a canonical URI: `secret://name`
/// Trims whitespace, rejects empty, lowercases (preserves hyphens).
pub fn normalize_secret_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_lowercase();
    Some(format!("secret://{lower}"))
}

pub fn build_config_source_detail(
    source_type: &str,
    config_uri: &str,
    raw: &str,
    extra: Option<&serde_json::Value>,
) -> String {
    let mut obj = json!({
        "config_uri": config_uri,
        "raw": raw,
        "source_type": source_type,
        "role": "source",
    });
    if let Some(extra) = extra
        && let Some(map) = extra.as_object()
    {
        for (k, v) in map {
            obj[k] = v.clone();
        }
    }
    obj.to_string()
}

pub fn build_config_read_detail(
    source_type: &str,
    config_uri: &str,
    raw: &str,
    framework: &str,
) -> String {
    json!({
        "config_uri": config_uri,
        "raw": raw,
        "source_type": source_type,
        "framework": framework,
        "role": "reader",
    })
    .to_string()
}

/// Check if a string is a config URI (secret://, env://).
pub fn is_config_uri(s: &str) -> bool {
    s.starts_with("secret://") || s.starts_with("env://")
}

pub fn build_config_bind_detail(
    options_type: &str,
    wrapper_type: &str,
    binding_kind: &str,
    framework: &str,
) -> String {
    json!({
        "options_type": options_type,
        "wrapper_type": wrapper_type,
        "binding_kind": binding_kind,
        "framework": framework,
        "role": "consumer",
    })
    .to_string()
}

// --- Config-URI traversal scoping (issue #131) ---
//
// `trace_flow` / `analyze_impact` bridge CONFIG_SOURCE <-> CONFIG_READ on an
// exact `secret://` / `env://` URI. The nodes reached that way are
// *aggregators* (a SecretProviderClass listing many secrets, a K8s container
// declaring many env vars); expanding all their config edges fans out to
// unrelated secrets and other services. `ConfigScope` is the one tracker both
// traversals use.

use crate::model::Edge;
use std::collections::{BTreeSet, HashMap, HashSet};

fn is_config_kind(kind: &str) -> bool {
    matches!(
        kind,
        CONFIG_SOURCE_KIND | CONFIG_READ_KIND | CONFIG_BIND_KIND
    )
}

/// A config edge about to be bridged to its complement.
pub struct BridgeTarget {
    pub uri: String,
    pub edge_kind: String,
    pub origin_path: String,
    pub source_id: i64,
}

/// Max distinct entry URIs a single node is re-expanded under.
const MAX_ENTRIES_PER_NODE: usize = 8;

/// Which config URIs each traversed node was entered through. A node is
/// visited once per (node, entry URI): reached via two secrets it is expanded
/// under each scope, bounded by `MAX_ENTRIES_PER_NODE`.
#[derive(Default)]
pub struct ConfigScope {
    entered: HashMap<i64, BTreeSet<String>>,
    seed_uri: Option<String>,
}

impl ConfigScope {
    /// `seed_uri`: the config URI the seeds were resolved from, if any.
    pub fn new(seeds: &[i64], seed_uri: Option<&str>) -> Self {
        let mut scope = Self {
            seed_uri: seed_uri.map(str::to_string),
            ..Self::default()
        };
        if let Some(uri) = seed_uri {
            for &id in seeds {
                scope.entered.entry(id).or_default().insert(uri.to_string());
            }
        }
        scope
    }

    /// Entry URI the seed nodes were entered through.
    pub fn seed_entry(&self) -> Option<String> {
        self.seed_uri.clone()
    }

    /// Decide whether to enqueue `id`, reached by bridging a `bridge_kind`
    /// edge on `uri`. `first_visit`: `id` was never visited before. Returns
    /// the entry URI to expand it under (`Some(None)` = unscoped), or None to
    /// skip it. A config bridge re-enters an already-visited node when this
    /// (node, URI) pair is new.
    pub fn enter(
        &mut self,
        id: i64,
        bridge_kind: &str,
        uri: &str,
        first_visit: bool,
    ) -> Option<Option<String>> {
        if !is_config_kind(bridge_kind) {
            return first_visit.then_some(None);
        }
        let set = self.entered.entry(id).or_default();
        if set.contains(uri) || (!first_visit && set.len() >= MAX_ENTRIES_PER_NODE) {
            return None;
        }
        set.insert(uri.to_string());
        Some(Some(uri.to_string()))
    }

    /// URIs `id`'s config edges may follow, or None when it is unscoped.
    /// A node entered via URI `U` keeps `U` plus the secret/env-var mapping of
    /// its own `secretKeyRef` CONFIG_SOURCE edges (`detail.secret` <-> env var
    /// and its `__` section prefixes), in both directions, so the chain
    /// code -> `env://X` -> container -> `secret://S` -> Bicep stays intact.
    pub fn allowed(entry: Option<&str>, node_edges: &[Edge]) -> Option<HashSet<String>> {
        let uri = entry?;
        let mut allowed = HashSet::from([uri.to_string()]);
        let entry_var = uri.strip_prefix("env://");
        for e in node_edges.iter().filter(|e| e.kind == CONFIG_SOURCE_KIND) {
            let Some(tq) = e.target_qualname.as_deref() else {
                continue;
            };
            let Some(var) = tq.strip_prefix("env://") else {
                continue;
            };
            let Some(secret) = e
                .detail
                .as_deref()
                .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
                .and_then(|v| Some(v.get("secret")?.as_str()?.to_lowercase()))
            else {
                continue;
            };
            let secret_uri = format!("secret://{secret}");
            let related = uri == secret_uri
                || entry_var.is_some_and(|x| {
                    var == x || var.strip_prefix(x).is_some_and(|r| r.starts_with("__"))
                });
            if !related {
                continue;
            }
            allowed.insert(secret_uri);
            allowed.insert(tq.to_string());
            for (idx, _) in var.match_indices("__") {
                if idx > 0 {
                    allowed.insert(format!("env://{}", &var[..idx]));
                }
            }
        }
        Some(allowed)
    }
}

/// Whether `edge` may be followed from a node scoped to `allowed` (None =
/// unscoped). Non-config edges are never restricted.
pub fn config_edge_allowed(edge: &Edge, allowed: Option<&HashSet<String>>) -> bool {
    let Some(allowed) = allowed else { return true };
    !is_config_kind(&edge.kind)
        || edge
            .target_qualname
            .as_ref()
            .is_some_and(|tq| allowed.contains(tq))
}

/// Narrow the complement edges of an `env://` bridge to the same service as
/// the origin, when any of them is (prefer, never exclude: with no match all
/// are kept). Other URIs are returned unchanged.
pub fn prefer_same_service<'a>(uri: &str, origin_path: &str, bridged: &'a [Edge]) -> Vec<&'a Edge> {
    let all = || bridged.iter().collect::<Vec<_>>();
    if !uri.starts_with("env://") {
        return all();
    }
    let same: Vec<&Edge> = bridged
        .iter()
        .filter(|e| same_service(origin_path, &e.file_path))
        .collect();
    if same.is_empty() { all() } else { same }
}

const GENERIC_DIRS: &[&str] = &[
    "k8s",
    "kubernetes",
    "deploy",
    "deployment",
    "deployments",
    "manifests",
    "base",
    "overlays",
    "chart",
    "charts",
    "templates",
    "infra",
    "local",
    "apps",
    "components",
    "src",
    "config",
];

fn is_manifest(path: &str) -> bool {
    path.ends_with(".yaml") || path.ends_with(".yml")
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// ponytail: "same service" is a path heuristic between a YAML manifest and
/// code: code inside the manifest's directory, or the manifest's nearest
/// non-generic directory name (`apps/dataproxy/deployment.yaml` ->
/// `dataproxy`) appearing in the code path (`Dpb.DataProxy/...`), compared as
/// lowercase alphanumerics. It only *prefers*: a better-matching sibling
/// suppresses a bridge, but if nothing matches every bridge is kept. Ceiling:
/// a wrong token (`overlays/prod/...` -> `prod`) or a name variant
/// (`datamgr` vs `DataManager`) just makes nothing match, or lets a sibling
/// that matches by accident win; shared libraries are then dropped whenever a
/// sibling deployment's name does match.
fn same_service(a: &str, b: &str) -> bool {
    let (manifest, code) = match (is_manifest(a), is_manifest(b)) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => return false,
    };
    let dir = manifest.rsplit_once('/').map_or("", |(d, _)| d);
    if !dir.is_empty() && code.starts_with(&format!("{dir}/")) {
        return true;
    }
    dir.rsplit('/')
        .map(norm)
        .find(|t| t.len() >= 3 && !GENERIC_DIRS.contains(&t.as_str()))
        .is_some_and(|t| norm(code).contains(&t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_env_var_basic() {
        assert_eq!(
            normalize_env_var_name("DATABASE_URL"),
            Some("env://DATABASE_URL".to_string())
        );
    }

    #[test]
    fn normalize_env_var_lowercased() {
        assert_eq!(
            normalize_env_var_name("database_url"),
            Some("env://DATABASE_URL".to_string())
        );
    }

    #[test]
    fn normalize_env_var_whitespace() {
        assert_eq!(
            normalize_env_var_name("  FOO_BAR  "),
            Some("env://FOO_BAR".to_string())
        );
    }

    #[test]
    fn normalize_env_var_empty() {
        assert_eq!(normalize_env_var_name(""), None);
        assert_eq!(normalize_env_var_name("   "), None);
    }

    #[test]
    fn normalize_secret_basic() {
        assert_eq!(
            normalize_secret_name("datamgr-db-conn"),
            Some("secret://datamgr-db-conn".to_string())
        );
    }

    #[test]
    fn normalize_secret_uppercased() {
        assert_eq!(
            normalize_secret_name("MySecret"),
            Some("secret://mysecret".to_string())
        );
    }

    #[test]
    fn normalize_secret_empty() {
        assert_eq!(normalize_secret_name(""), None);
        assert_eq!(normalize_secret_name("   "), None);
    }

    #[test]
    fn is_config_uri_detects_schemes() {
        assert!(is_config_uri("secret://datamgr-db-conn-str"));
        assert!(is_config_uri("env://DATABASE_URL"));
        assert!(is_config_uri("env://DATABASE"));
        assert!(!is_config_uri("Foo.Bar.Baz"));
        assert!(!is_config_uri("DatabaseOptions"));
        assert!(!is_config_uri(""));
    }

    #[test]
    fn build_source_detail_json() {
        let detail = build_config_source_detail("env", "env://FOO", "FOO", None);
        let parsed: serde_json::Value = serde_json::from_str(&detail).unwrap();
        assert_eq!(parsed["config_uri"], "env://FOO");
        assert_eq!(parsed["role"], "source");
    }

    #[test]
    fn build_read_detail_json() {
        let detail = build_config_read_detail("env", "env://FOO", "FOO", "dotnet");
        let parsed: serde_json::Value = serde_json::from_str(&detail).unwrap();
        assert_eq!(parsed["config_uri"], "env://FOO");
        assert_eq!(parsed["framework"], "dotnet");
        assert_eq!(parsed["role"], "reader");
    }

    #[test]
    fn scope_reenters_per_uri_and_is_bounded() {
        let mut s = ConfigScope::default();
        assert_eq!(
            s.enter(1, CONFIG_SOURCE_KIND, "env://A", true),
            Some(Some("env://A".to_string()))
        );
        // Same node via another URI is re-entered; the same pair is not.
        assert_eq!(
            s.enter(1, CONFIG_SOURCE_KIND, "env://B", false),
            Some(Some("env://B".to_string()))
        );
        assert_eq!(s.enter(1, CONFIG_SOURCE_KIND, "env://B", false), None);
        // Non-config bridges stay visit-once.
        assert_eq!(s.enter(2, "RPC_IMPL", "x", false), None);
        assert_eq!(s.enter(2, "RPC_IMPL", "x", true), Some(None));
        // Bounded.
        let mut n = 0;
        for i in 0..100 {
            if s.enter(3, CONFIG_READ_KIND, &format!("env://U{i}"), i == 0)
                .is_some()
            {
                n += 1;
            }
        }
        assert_eq!(n, MAX_ENTRIES_PER_NODE);
    }

    #[test]
    fn service_preference_by_dir_name_or_ancestry() {
        let m = "infra/local/components/apps/dataproxy/deployment.yaml";
        assert!(same_service(m, "src/Dpb.DataProxy/Options.cs"));
        assert!(!same_service(m, "src/Dpb.DataMgr/Options.cs"));
        assert!(same_service("svc/a/deploy.yaml", "svc/a/src/x.cs"));
        assert!(!same_service("deploy.yaml", "anything.cs"));
        assert!(!same_service("a/x.bicep", "b/y.cs"));
    }
}
