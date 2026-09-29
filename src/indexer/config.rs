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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BridgeTarget {
    pub uri: String,
    pub edge_kind: String,
    pub origin_path: String,
    pub source_id: i64,
}

/// What a traversal step may expand a node under: everything (`Unscoped`), or
/// only config edges related to one config URI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Entry {
    Unscoped,
    Uri(String),
}

impl Entry {
    fn as_uri(&self) -> Option<&str> {
        match self {
            Entry::Unscoped => None,
            Entry::Uri(u) => Some(u),
        }
    }
}

/// A node the traversal should (re-)expand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub entry: Entry,
    /// The node was never visited before (a re-entry when false).
    pub first_visit: bool,
}

/// Max distinct entry URIs a single node is re-expanded under.
pub const MAX_ENTRIES_PER_NODE: usize = 8;

/// Why a traversal reports `truncated` when `ConfigScope::capped`.
pub const CAP_TRUNCATION_REASON: &str = "config re-entry cap reached: a node reached via more than 8 config URIs was expanded under only the first 8 (in sorted bridge order)";

/// Which entries each traversed node was expanded under. A node is visited
/// once per (node, entry): reached via two secrets it is expanded under each
/// scope. `Unscoped` is an entry like any other, and dominates: a node that
/// expanded `Unscoped` skips scoped re-entries (its expansion is a superset),
/// while a plain edge reaching a scoped-only node re-enters it `Unscoped`
/// (seeds resolved from a config URI stay scoped). Either arrival order
/// therefore reaches the same set of nodes.
#[derive(Default)]
pub struct ConfigScope {
    entered: HashMap<i64, BTreeSet<Entry>>,
    seed_uri: Option<String>,
    seeds_scoped: HashSet<i64>,
    capped: bool,
}

impl ConfigScope {
    /// `seed_uri`: the config URI the seeds were resolved from, if any.
    pub fn new(seeds: &[i64], seed_uri: Option<&str>) -> Self {
        let mut scope = Self {
            seed_uri: seed_uri.map(str::to_string),
            ..Self::default()
        };
        let entry = scope.seed_entry();
        for &id in seeds {
            scope.entered.entry(id).or_default().insert(entry.clone());
            if seed_uri.is_some() {
                scope.seeds_scoped.insert(id);
            }
        }
        scope
    }

    /// Entry the seed nodes are expanded under.
    pub fn seed_entry(&self) -> Entry {
        self.seed_uri.clone().map_or(Entry::Unscoped, Entry::Uri)
    }

    /// Whether a re-entry was refused because a node hit the per-node cap.
    pub fn capped(&self) -> bool {
        self.capped
    }

    /// A plain (non-bridge) edge reached `id`.
    pub fn admit_plain(&mut self, visited: &mut HashSet<i64>, id: i64) -> Option<Admission> {
        let first_visit = visited.insert(id);
        if self.seeds_scoped.contains(&id) {
            return None;
        }
        self.entered
            .entry(id)
            .or_default()
            .insert(Entry::Unscoped)
            .then_some(Admission {
                entry: Entry::Unscoped,
                first_visit,
            })
    }

    /// `id` was reached by bridging a `bridge_kind` edge on `uri`. A config
    /// bridge re-enters an already-visited node when this (node, URI) pair
    /// is new; other bridge kinds are visit-once.
    pub fn admit_bridge(
        &mut self,
        visited: &mut HashSet<i64>,
        id: i64,
        bridge_kind: &str,
        uri: &str,
    ) -> Option<Admission> {
        let first_visit = visited.insert(id);
        if !is_config_kind(bridge_kind) {
            return first_visit.then_some(Admission {
                entry: Entry::Unscoped,
                first_visit,
            });
        }
        let set = self.entered.entry(id).or_default();
        let entry = Entry::Uri(uri.to_string());
        if set.contains(&Entry::Unscoped) || set.contains(&entry) {
            return None;
        }
        if set.len() >= MAX_ENTRIES_PER_NODE {
            self.capped = true;
            return None;
        }
        set.insert(entry.clone());
        Some(Admission { entry, first_visit })
    }

    /// URIs a node's config edges may follow when expanded under `entry`, or
    /// None when unscoped. A node entered via URI `U` keeps `U` plus the secret/env-var mapping of
    /// its own `secretKeyRef` CONFIG_SOURCE edges (`detail.secret` <-> env var
    /// and its `__` section prefixes), in both directions, so the chain
    /// code -> `env://X` -> container -> `secret://S` -> Bicep stays intact.
    pub fn allowed(entry: &Entry, node_edges: &[Edge]) -> Option<HashSet<String>> {
        let uri = entry.as_uri()?;
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

    fn uri(u: &str) -> Entry {
        Entry::Uri(u.to_string())
    }

    #[test]
    fn scope_reenters_per_uri_and_is_bounded() {
        let mut s = ConfigScope::default();
        let mut v = HashSet::new();
        let a = s
            .admit_bridge(&mut v, 1, CONFIG_SOURCE_KIND, "env://A")
            .unwrap();
        assert_eq!((a.entry, a.first_visit), (uri("env://A"), true));
        // Same node via another URI is re-entered; the same pair is not.
        let b = s
            .admit_bridge(&mut v, 1, CONFIG_SOURCE_KIND, "env://B")
            .unwrap();
        assert_eq!((b.entry, b.first_visit), (uri("env://B"), false));
        assert!(
            s.admit_bridge(&mut v, 1, CONFIG_SOURCE_KIND, "env://B")
                .is_none()
        );
        // Non-config bridges stay visit-once.
        assert!(s.admit_bridge(&mut v, 2, "RPC_IMPL", "x").is_some());
        assert!(s.admit_bridge(&mut v, 2, "RPC_IMPL", "x").is_none());
        assert!(!s.capped());
        // Bounded, and the cap is reported.
        let admitted = (0..100)
            .filter(|i| {
                s.admit_bridge(&mut v, 3, CONFIG_READ_KIND, &format!("env://U{i}"))
                    .is_some()
            })
            .count();
        assert_eq!(admitted, MAX_ENTRIES_PER_NODE);
        assert!(s.capped());
    }

    #[test]
    fn unscoped_dominates_in_either_arrival_order() {
        // Bridge first, then plain: re-entered Unscoped.
        let mut s = ConfigScope::default();
        let mut v = HashSet::new();
        s.admit_bridge(&mut v, 1, CONFIG_READ_KIND, "env://A")
            .unwrap();
        let p = s.admit_plain(&mut v, 1).unwrap();
        assert_eq!((p.entry, p.first_visit), (Entry::Unscoped, false));
        assert!(
            s.admit_bridge(&mut v, 1, CONFIG_READ_KIND, "env://B")
                .is_none()
        );
        // Plain first: later scoped arrivals are subsumed.
        let mut s = ConfigScope::default();
        let mut v = HashSet::new();
        assert!(s.admit_plain(&mut v, 1).unwrap().first_visit);
        assert!(
            s.admit_bridge(&mut v, 1, CONFIG_READ_KIND, "env://A")
                .is_none()
        );
        // Seeds resolved from a URI stay scoped.
        let mut s = ConfigScope::new(&[9], Some("secret://x"));
        let mut v = HashSet::from([9]);
        assert!(s.admit_plain(&mut v, 9).is_none());
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
