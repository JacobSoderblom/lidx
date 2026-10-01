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
    // Never turn expression text (calls, quoted strings, subscripts, ...)
    // into a plausible-looking env URI (issue #225).
    if trimmed.chars().any(|c| {
        c.is_whitespace()
            || matches!(
                c,
                '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>' | ',' | '+'
            )
    }) {
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

/// A newly reached (node, entry) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub entry: Entry,
    /// Whether to expand the node under `entry`. False when the node has
    /// already expanded `Unscoped`, whose expansion is a superset: the pair
    /// is still reported (one hop per pair) but not re-walked.
    pub expand: bool,
}

/// Max distinct config URIs a node is entered under.
pub const MAX_ENTRIES_PER_NODE: usize = 8;

/// Why a traversal reports `truncated` when `ConfigScope::capped`.
pub const CAP_TRUNCATION_REASON: &str = "config re-entry cap reached: a node with more than 8 config URIs is entered under only its 8 lowest (sorted) URIs";

/// Which (node, entry) pairs a traversal has reached. A node is reached once
/// per pair: via two secrets it is expanded under each scope. Everything is
/// keyed by pair, never by arrival order, so the set of pairs (and so the
/// hops) is the same whichever edge is processed first:
/// - `Unscoped` is an entry like any other; a plain edge reaching a
///   scoped-only node reaches it `Unscoped`. Seeds keep the one entry they
///   start with (a URI seed stays scoped) and are never re-reached.
/// - A node that expanded `Unscoped` does not re-walk scoped entries (a
///   subset), but still reports them.
/// - A node can only be bridged into on a URI one of its own config edges
///   carries; when it has more than `MAX_ENTRIES_PER_NODE` such URIs only
///   the lowest (sorted) are admitted, independent of arrival order.
#[derive(Default)]
pub struct ConfigScope {
    seen: HashSet<(i64, Entry)>,
    eligible: HashMap<i64, BTreeSet<String>>,
    seed_uri: Option<String>,
    seeds: HashSet<i64>,
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
            scope.seen.insert((id, entry.clone()));
            scope.seeds.insert(id);
        }
        scope
    }

    /// Entry the seed nodes are expanded under.
    pub fn seed_entry(&self) -> Entry {
        self.seed_uri.clone().map_or(Entry::Unscoped, Entry::Uri)
    }

    /// Whether an entry was refused because a node hit the per-node cap.
    pub fn capped(&self) -> bool {
        self.capped
    }

    /// A plain (non-bridge) edge reached `id`.
    pub fn admit_plain(&mut self, id: i64) -> Option<Admission> {
        if self.seeds.contains(&id) || !self.seen.insert((id, Entry::Unscoped)) {
            return None;
        }
        Some(Admission {
            entry: Entry::Unscoped,
            expand: true,
        })
    }

    /// `id` was reached by bridging a `bridge_kind` edge on `uri`. Config
    /// bridges admit each new (node, URI) pair; other bridge kinds are
    /// visit-once. `node_uris` lists every config URI `id`'s own edges
    /// carry (called once per node, only for config bridges).
    pub fn admit_bridge(
        &mut self,
        id: i64,
        bridge_kind: &str,
        uri: &str,
        node_uris: impl FnOnce() -> BTreeSet<String>,
    ) -> Option<Admission> {
        if !is_config_kind(bridge_kind) {
            return self.admit_plain(id);
        }
        if self.seeds.contains(&id) {
            return None;
        }
        let eligible = self.eligible.entry(id).or_insert_with(|| {
            let mut uris = node_uris();
            uris.insert(uri.to_string());
            uris.into_iter().take(MAX_ENTRIES_PER_NODE).collect()
        });
        if !eligible.contains(uri) {
            self.capped = true;
            return None;
        }
        let entry = Entry::Uri(uri.to_string());
        if !self.seen.insert((id, entry.clone())) {
            return None;
        }
        let expand = !self.seen.contains(&(id, Entry::Unscoped));
        Some(Admission { entry, expand })
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

/// `resolution_kind` stamped on an HTTP bridge hop that crossed into another
/// service because it was the only one declaring the path (issue #233): a
/// guess, distinguishable from a same-service bridge (spec criterion 7).
pub const CROSS_SERVICE_KIND: &str = "cross_service_http";

/// Narrow the complement edges of a bridge to the same service as the origin.
/// `env://` bridges *prefer* it (with no match all are kept). HTTP bridges
/// are stricter, because a bare path like `/health/live` is declared by many
/// unrelated services: same-service routes win; with none, the routes are
/// kept only when they all belong to one service (a unique target), else
/// none (issue #233). Each edge is paired with whether it is such a
/// speculative cross-service fallback. Other bridges are returned unchanged.
pub fn prefer_same_service<'a>(
    uri: &str,
    origin_path: &str,
    bridged: &'a [Edge],
) -> Vec<(&'a Edge, bool)> {
    let all = |speculative: bool| bridged.iter().map(|e| (e, speculative)).collect();
    if !uri.starts_with("env://") {
        if bridged.is_empty() || !bridged.iter().all(|e| e.kind.starts_with("HTTP_")) {
            return all(false);
        }
        let origin = service_root(origin_path);
        let keys: Vec<String> = bridged.iter().map(|e| service_root(&e.file_path)).collect();
        let same: Vec<(&Edge, bool)> = bridged
            .iter()
            .zip(&keys)
            .filter(|(_, k)| **k == origin)
            .map(|(e, _)| (e, false))
            .collect();
        if !same.is_empty() {
            return same;
        }
        return if keys.iter().all(|k| *k == keys[0]) {
            all(true)
        } else {
            Vec::new()
        };
    }
    let same: Vec<(&Edge, bool)> = bridged
        .iter()
        .filter(|e| same_service(origin_path, &e.file_path))
        .map(|e| (e, false))
        .collect();
    if same.is_empty() { all(false) } else { same }
}

/// Service identity for a *code* path, used for HTTP code-vs-code narrowing.
/// Not shared with `same_service`: that relates a manifest (identified by its
/// deploy directory's name token) to code, and has no code-side root to
/// compare; the two never apply to the same pair of paths. A single key cannot
/// serve both relations. The root is the directories
/// leading to the first source/test layout directory (`py/orch/src/...` and
/// `py/orch/tests/...` -> `py/orch`; `services/team/foo/src/...` ->
/// `services/team/foo`). A path that starts with a layout directory takes the
/// next component too (`src/frontend/...` -> `src/frontend`), and a path with
/// no layout directory is capped at two components. Ceiling: a flat layout
/// splits a service across subpackages; every mis-key either merges or splits
/// services, so the HTTP caller can only get its old behaviour (a bridge) or
/// lose a speculative one.
fn service_root(path: &str) -> String {
    let dirs: Vec<&str> = path.split('/').collect();
    let dirs = &dirs[..dirs.len().saturating_sub(1)];
    let end = match dirs.iter().position(|d| is_layout_dir(d)) {
        Some(0) => 2.min(dirs.len()),
        Some(i) => i,
        None => 2.min(dirs.len()),
    };
    dirs[..end].join("/")
}

fn is_layout_dir(d: &str) -> bool {
    crate::indexer::test_detection::TEST_DIR_NAMES.contains(&d)
        || d.starts_with("test_")
        || matches!(d, "src" | "lib" | "app" | "cmd" | "internal" | "pkg")
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
    #[test]
    fn normalize_env_var_name_rejects_expression_text() {
        for bad in [
            "a b", "\"X\"", "'X'", "f(x)", "x[0]", "{x}", "<x>", "a,b", "a+b", "`x`", "",
        ] {
            assert_eq!(normalize_env_var_name(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn normalize_env_var_name_accepts_real_names() {
        for (raw, want) in [
            ("DATABASE_URL", "env://DATABASE_URL"),
            (
                "Database__ConnectionString",
                "env://DATABASE__CONNECTIONSTRING",
            ),
            ("my.var-1", "env://MY.VAR-1"),
            ("  PADDED ", "env://PADDED"),
        ] {
            assert_eq!(normalize_env_var_name(raw).as_deref(), Some(want));
        }
    }

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

    fn uris(n: usize) -> BTreeSet<String> {
        (0..n).map(|i| format!("env://U{i}")).collect()
    }

    #[test]
    fn scope_reenters_per_uri() {
        let mut s = ConfigScope::default();
        let a = s
            .admit_bridge(1, CONFIG_SOURCE_KIND, "env://U0", || uris(2))
            .unwrap();
        assert_eq!((a.entry, a.expand), (uri("env://U0"), true));
        let b = s
            .admit_bridge(1, CONFIG_SOURCE_KIND, "env://U1", || unreachable!())
            .unwrap();
        assert_eq!(b.entry, uri("env://U1"));
        assert!(
            s.admit_bridge(1, CONFIG_SOURCE_KIND, "env://U1", || unreachable!())
                .is_none()
        );
        // Non-config bridges stay visit-once.
        assert!(s.admit_bridge(2, "RPC_IMPL", "x", BTreeSet::new).is_some());
        assert!(s.admit_bridge(2, "RPC_IMPL", "x", BTreeSet::new).is_none());
        assert!(!s.capped());
    }

    #[test]
    fn cap_keeps_lowest_uris_whatever_the_arrival_order() {
        let expected: BTreeSet<String> = uris(MAX_ENTRIES_PER_NODE);
        for order in [(0..10).collect::<Vec<_>>(), (0..10).rev().collect()] {
            let mut s = ConfigScope::default();
            let admitted: BTreeSet<String> = order
                .into_iter()
                .map(|i| format!("env://U{i}"))
                .filter(|u| {
                    s.admit_bridge(3, CONFIG_READ_KIND, u, || uris(10))
                        .is_some()
                })
                .collect();
            assert_eq!(admitted, expected);
            assert!(s.capped());
        }
    }

    #[test]
    fn unscoped_dominates_expansion_in_either_arrival_order() {
        // Bridge first, then plain: both pairs are reached, both expand.
        let mut s = ConfigScope::default();
        assert!(
            s.admit_bridge(1, CONFIG_READ_KIND, "env://U0", || uris(2))
                .unwrap()
                .expand
        );
        let p = s.admit_plain(1).unwrap();
        assert_eq!((p.entry, p.expand), (Entry::Unscoped, true));
        // Plain first: the scoped pair is still reported, but not re-walked.
        let mut s = ConfigScope::default();
        s.admit_plain(1).unwrap();
        let b = s
            .admit_bridge(1, CONFIG_READ_KIND, "env://U0", || uris(2))
            .unwrap();
        assert_eq!((b.entry, b.expand), (uri("env://U0"), false));
        // Seeds resolved from a URI stay scoped.
        let mut s = ConfigScope::new(&[9], Some("secret://x"));
        assert!(s.admit_plain(9).is_none());
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

    #[test]
    fn service_root_keys() {
        assert_eq!(service_root("py/orch/src/a/b.py"), "py/orch");
        assert_eq!(service_root("py/orch/tests/t.py"), "py/orch");
        assert_eq!(
            service_root("services/team/foo/src/x.py"),
            "services/team/foo"
        );
        assert_eq!(
            service_root("services/team/bar/src/x.py"),
            "services/team/bar"
        );
        assert_ne!(
            service_root("src/frontend/a.ts"),
            service_root("src/backend/a.py")
        );
        assert_eq!(service_root("x.py"), "");
    }
}
