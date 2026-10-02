use serde_json::json;

pub const CONFIG_SOURCE_KIND: &str = "CONFIG_SOURCE";
pub const CONFIG_READ_KIND: &str = "CONFIG_READ";
pub const CONFIG_BIND_KIND: &str = "CONFIG_BIND";

/// Detail JSON keys of an SPC CONFIG_SOURCE edge's `secretObjects` mapping:
/// written by the YAML extractor, read by `ConfigScope`.
pub const SPC_MAPPING_FIELD: &str = "mapping";
pub const SPC_OBJECT_NAME_FIELD: &str = "objectName";
pub const SPC_SECRET_NAME_FIELD: &str = "secretName";
pub const SPC_KEY_FIELD: &str = "key";

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

/// The `__`-delimited section ancestors of an `env://` URI, longest first:
/// `env://A__B__C` -> `env://A__B`, `env://A`. Empty for any other URI.
pub fn env_section_prefixes(uri: &str) -> Vec<String> {
    let Some(name) = uri.strip_prefix("env://") else {
        return Vec::new();
    };
    let mut prefixes: Vec<String> = name
        .match_indices("__")
        .map(|(idx, _)| &name[..idx])
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| format!("env://{prefix}"))
        .collect();
    prefixes.reverse();
    prefixes
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
    /// Key within `uri` the bridge follows (see `Entry::Key`), when the
    /// edge's node pairs it with one.
    pub key: Option<String>,
    /// Keyed bridges (this one, or the node's own entry) never turn back to
    /// the node they came from: see `ConfigScope::note_bridge`.
    pub no_return: bool,
}

/// What a traversal step may expand a node under: everything (`Unscoped`), or
/// only config edges related to one config URI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Entry {
    Unscoped,
    Uri(String),
    /// One key of a keyed secret URI (`secret://kv-secrets` key `db-conn`):
    /// an aggregator entered this way follows only that key's mapping.
    Key(String, String),
}

impl Entry {
    /// `Uri`, or `Key` when the bridge follows one key of it.
    pub fn scoped(uri: &str, key: Option<&str>) -> Self {
        match key {
            Some(k) => Entry::Key(uri.to_string(), k.to_string()),
            None => Entry::Uri(uri.to_string()),
        }
    }

    fn as_uri(&self) -> Option<&str> {
        match self {
            Entry::Unscoped => None,
            Entry::Uri(u) | Entry::Key(u, _) => Some(u),
        }
    }

    fn key(&self) -> Option<&str> {
        match self {
            Entry::Key(_, k) => Some(k),
            _ => None,
        }
    }
}

fn detail_json(e: &Edge) -> Option<serde_json::Value> {
    serde_json::from_str(e.detail.as_deref()?).ok()
}

/// One `secretObjects[].data[]` entry on an SPC's CONFIG_SOURCE edge: the
/// Key Vault `objectName` synced into the edge's target secret as `key`.
fn spc_mapping(e: &Edge) -> Vec<(String, String)> {
    detail_json(e)
        .and_then(|d| {
            let m = d.get(SPC_MAPPING_FIELD)?.as_array()?;
            Some(
                m.iter()
                    .filter_map(|m| {
                        let obj = normalize_secret_name(m.get(SPC_OBJECT_NAME_FIELD)?.as_str()?)?;
                        Some((obj, m.get(SPC_KEY_FIELD)?.as_str()?.to_string()))
                    })
                    .collect(),
            )
        })
        .unwrap_or_default()
}

/// A container's `secretKeyRef` env edge: env var URI, the secret URI it
/// reads and the key within that secret (empty when unspecified).
struct EnvPairing<'a> {
    var: &'a str,
    secret_uri: String,
    key: String,
}

fn env_pairing(e: &Edge) -> Option<EnvPairing<'_>> {
    if e.kind != CONFIG_SOURCE_KIND {
        return None;
    }
    let var = e.target_qualname.as_deref()?;
    var.strip_prefix("env://")?;
    let d = detail_json(e)?;
    let secret = d.get("secret")?.as_str()?.to_lowercase();
    let key = d.get("key").and_then(|k| k.as_str()).unwrap_or("");
    Some(EnvPairing {
        var,
        secret_uri: format!("secret://{secret}"),
        key: key.to_string(),
    })
}

/// Whether `var` is the entry's env var or one of its `__` section children.
fn env_related(entry_var: Option<&str>, var: &str) -> bool {
    entry_var.is_some_and(|x| {
        let x = format!("env://{x}");
        var == x || var.strip_prefix(&x).is_some_and(|r| r.starts_with("__"))
    })
}

/// Every config URI `edges` (one node's) carry: the only URIs a config bridge
/// can enter that node on.
pub fn config_uris(edges: &[Edge]) -> BTreeSet<String> {
    edges
        .iter()
        .filter(|e| is_config_kind(&e.kind))
        .filter_map(|e| e.target_qualname.clone())
        .collect()
}

/// Result of `ConfigScope::admit_bridged`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeOutcome {
    /// A U-turn, or the node does not pair the bridge's key: ignore it.
    Skipped,
    /// The node was already reached under this entry (or hit the per-node
    /// cap): nothing new to expand, but a shorter path may still replace it.
    Refused,
    Admitted(Admission),
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
    /// (node, node it was bridged into from): a bridge never U-turns.
    came_from: HashSet<(i64, i64)>,
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
        key: Option<&str>,
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
        let entry = Entry::scoped(uri, key);
        if !self.seen.insert((id, entry.clone())) {
            return None;
        }
        let expand = !self.seen.contains(&(id, Entry::Unscoped));
        Some(Admission { entry, expand })
    }

    /// URIs a node's config edges may follow when expanded under `entry`, or
    /// None when unscoped. A node entered via URI `U` keeps `U` plus:
    /// - a container's `secretKeyRef` mapping (`detail.secret` <-> env var and
    ///   its `__` section prefixes), in both directions, so the chain
    ///   code -> `env://X` -> container -> `secret://S` -> Bicep stays intact.
    ///   Entered under key `K` of `S`, only the env vars reading `K`.
    /// - a SecretProviderClass's `secretObjects` mapping: entered via an
    ///   `objectName` it keeps the secrets that name is synced into; entered
    ///   via key `K` of a synced secret, only the `objectName`s mapped to `K`.
    pub fn allowed(entry: &Entry, node_edges: &[Edge]) -> Option<HashSet<String>> {
        let uri = entry.as_uri()?;
        let mut allowed = HashSet::from([uri.to_string()]);
        let entry_var = uri.strip_prefix("env://");
        for e in node_edges.iter().filter(|e| e.kind == CONFIG_SOURCE_KIND) {
            if let Some(EnvPairing {
                var,
                secret_uri,
                key,
            }) = env_pairing(e)
            {
                let key_ok = entry.key().is_none_or(|k| key.is_empty() || key == k);
                if !((uri == secret_uri && key_ok) || env_related(entry_var, var)) {
                    continue;
                }
                allowed.insert(secret_uri);
                allowed.insert(var.to_string());
                allowed.extend(env_section_prefixes(var));
                continue;
            }
            let Some(target) = e.target_qualname.as_deref() else {
                continue;
            };
            for (obj_uri, key) in spc_mapping(e) {
                if uri == obj_uri {
                    allowed.insert(target.to_string());
                }
                if uri == target && entry.key() == Some(key.as_str()) {
                    allowed.insert(obj_uri);
                }
            }
        }
        Some(allowed)
    }

    /// Keys of `node_edges`' node for the secret `uri`: the keys a container
    /// reads from it, or the keys an SPC syncs into it.
    fn declared_keys(node_edges: &[Edge], uri: &str) -> HashSet<String> {
        let mut keys = HashSet::new();
        for e in node_edges.iter().filter(|e| e.kind == CONFIG_SOURCE_KIND) {
            if let Some(p) = env_pairing(e) {
                if p.secret_uri == uri && !p.key.is_empty() {
                    keys.insert(p.key);
                }
            } else if e.target_qualname.as_deref() == Some(uri) {
                keys.extend(spc_mapping(e).into_iter().map(|(_, k)| k));
            }
        }
        keys
    }

    /// Whether a node reached by bridging key `key` of `uri` actually pairs
    /// with that key. A node declaring no keys for `uri` is not narrowed.
    pub fn accepts_key(node_edges: &[Edge], uri: &str, key: &str) -> bool {
        let keys = Self::declared_keys(node_edges, uri);
        keys.is_empty() || keys.contains(key)
    }

    /// The bridges a traversal step makes over `edge` while expanding a node
    /// under `entry`: one per key the node pairs with `edge`'s secret, or a
    /// single keyless one.
    pub fn bridges_for(
        entry: &Entry,
        node_edges: &[Edge],
        edge: &Edge,
        uri: &str,
        source_id: i64,
    ) -> Vec<BridgeTarget> {
        let mut keys: Vec<String> = match (edge.kind.as_str(), entry) {
            (_, Entry::Key(u, k)) if u == uri => vec![k.clone()],
            (CONFIG_READ_KIND, _) => node_edges
                .iter()
                .filter_map(env_pairing)
                .filter(|p| {
                    p.secret_uri == uri
                        && !p.key.is_empty()
                        && match entry {
                            Entry::Unscoped => true,
                            Entry::Uri(u) => {
                                u == uri || env_related(u.strip_prefix("env://"), p.var)
                            }
                            Entry::Key(..) => true,
                        }
                })
                .map(|p| p.key)
                .collect(),
            (CONFIG_SOURCE_KIND, Entry::Uri(u)) => spc_mapping(edge)
                .into_iter()
                .filter(|(obj, _)| obj == u)
                .map(|(_, k)| k)
                .collect(),
            _ => Vec::new(),
        };
        keys.sort();
        keys.dedup();
        let target = |key: Option<String>| BridgeTarget {
            uri: uri.to_string(),
            edge_kind: edge.kind.clone(),
            origin_path: edge.file_path.clone(),
            source_id,
            no_return: key.is_some() || entry.key().is_some(),
            key,
        };
        if keys.is_empty() {
            vec![target(None)]
        } else {
            keys.into_iter().map(|k| target(Some(k))).collect()
        }
    }

    /// Decide whether `bridge` may enter `to`, in one step: refuses a U-turn
    /// and a node that does not pair the bridge's key, then admits the
    /// (node, URI) pair and records the bridge. `load_edges` fetches `to`'s
    /// edges; it runs at most once, and only when needed.
    pub fn admit_bridged(
        &mut self,
        bridge: &BridgeTarget,
        to: i64,
        load_edges: impl FnOnce() -> Vec<Edge>,
    ) -> BridgeOutcome {
        if self.is_u_turn(bridge.source_id, to) {
            return BridgeOutcome::Skipped;
        }
        let cell = std::cell::OnceCell::new();
        let mut load = Some(load_edges);
        let mut edges =
            || -> &Vec<Edge> { cell.get_or_init(|| (load.take().expect("loaded once"))()) };
        if let Some(key) = bridge.key.as_deref()
            && !Self::accepts_key(edges(), &bridge.uri, key)
        {
            return BridgeOutcome::Skipped;
        }
        match self.admit_bridge(
            to,
            &bridge.edge_kind,
            &bridge.uri,
            bridge.key.as_deref(),
            || config_uris(edges()),
        ) {
            Some(a) => {
                self.note_bridge(bridge, to);
                BridgeOutcome::Admitted(a)
            }
            None => BridgeOutcome::Refused,
        }
    }

    /// Whether bridging from `from` into `to` would turn back to where `from`
    /// was bridged in from (only keyed bridges are recorded).
    pub fn is_u_turn(&self, from: i64, to: i64) -> bool {
        self.came_from.contains(&(from, to))
    }

    /// Record that `to` was bridged into from `from`, if `bridge` is keyed.
    pub fn note_bridge(&mut self, bridge: &BridgeTarget, to: i64) {
        if bridge.no_return {
            self.came_from.insert((to, bridge.source_id));
        }
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
    fn env_section_prefixes_are_proper_ancestors_longest_first() {
        assert_eq!(
            env_section_prefixes("env://A__B__C"),
            ["env://A__B", "env://A"]
        );
        assert!(env_section_prefixes("env://DATABASE").is_empty());
        assert!(env_section_prefixes("env://__X").is_empty());
        // `secret://` URIs have no sections.
        assert!(env_section_prefixes("secret://a__b").is_empty());
    }

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
            .admit_bridge(1, CONFIG_SOURCE_KIND, "env://U0", None, || uris(2))
            .unwrap();
        assert_eq!((a.entry, a.expand), (uri("env://U0"), true));
        let b = s
            .admit_bridge(1, CONFIG_SOURCE_KIND, "env://U1", None, || unreachable!())
            .unwrap();
        assert_eq!(b.entry, uri("env://U1"));
        assert!(
            s.admit_bridge(1, CONFIG_SOURCE_KIND, "env://U1", None, || unreachable!())
                .is_none()
        );
        // Non-config bridges stay visit-once.
        assert!(
            s.admit_bridge(2, "RPC_IMPL", "x", None, BTreeSet::new)
                .is_some()
        );
        assert!(
            s.admit_bridge(2, "RPC_IMPL", "x", None, BTreeSet::new)
                .is_none()
        );
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
                    s.admit_bridge(3, CONFIG_READ_KIND, u, None, || uris(10))
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
            s.admit_bridge(1, CONFIG_READ_KIND, "env://U0", None, || uris(2))
                .unwrap()
                .expand
        );
        let p = s.admit_plain(1).unwrap();
        assert_eq!((p.entry, p.expand), (Entry::Unscoped, true));
        // Plain first: the scoped pair is still reported, but not re-walked.
        let mut s = ConfigScope::default();
        s.admit_plain(1).unwrap();
        let b = s
            .admit_bridge(1, CONFIG_READ_KIND, "env://U0", None, || uris(2))
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
