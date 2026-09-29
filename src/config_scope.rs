//! Scoping rules for config-URI traversal (issue #131).
//!
//! `trace_flow` / `analyze_impact` bridge `CONFIG_SOURCE` <-> `CONFIG_READ` on
//! an exact `secret://` / `env://` URI. Without scoping, the nodes reached that
//! way are *aggregators* (a SecretProviderClass listing many secrets, a K8s
//! container declaring many env vars) and expanding all their config edges
//! fans out to unrelated secrets and to other services' code. Two rules:
//!
//! 1. A node entered via URI `U` only continues along config edges carrying
//!    `U` or the env var `U` was mapped to (`allowed_uris`, `edge_allowed`).
//! 2. An `env://` bridge only joins a K8s manifest to code of the same
//!    service (`bridge_edge_allowed`).

use crate::model::Edge;
use std::collections::HashSet;

fn is_config_kind(kind: &str) -> bool {
    matches!(kind, "CONFIG_SOURCE" | "CONFIG_READ" | "CONFIG_BIND")
}

/// URIs a node entered via `uri` may keep following: `uri` itself plus, for a
/// `secret://` entry, the env vars (and their `__` section prefixes) the
/// node's own `CONFIG_SOURCE` edges map that secret to
/// (`secretKeyRef` -> `detail.secret`).
pub fn allowed_uris(uri: &str, node_edges: &[Edge]) -> HashSet<String> {
    let mut allowed = HashSet::from([uri.to_string()]);
    let Some(name) = uri.strip_prefix("secret://") else {
        return allowed;
    };
    for e in node_edges.iter().filter(|e| e.kind == "CONFIG_SOURCE") {
        let Some(tq) = e.target_qualname.as_deref() else {
            continue;
        };
        let mapped = e
            .detail
            .as_deref()
            .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
            .and_then(|v| v.get("secret")?.as_str().map(str::to_lowercase));
        if mapped.as_deref() != Some(name) {
            continue;
        }
        allowed.insert(tq.to_string());
        if let Some(var) = tq.strip_prefix("env://") {
            for (idx, _) in var.match_indices("__") {
                if idx > 0 {
                    allowed.insert(format!("env://{}", &var[..idx]));
                }
            }
        }
    }
    allowed
}

/// Whether `edge` may be followed from a node scoped to `scope` (None =
/// unscoped). Non-config edges are never restricted.
pub fn edge_allowed(edge: &Edge, scope: Option<&HashSet<String>>) -> bool {
    let Some(scope) = scope else { return true };
    !is_config_kind(&edge.kind)
        || edge
            .target_qualname
            .as_ref()
            .is_some_and(|tq| scope.contains(tq))
}

/// Whether bridging `uri` from `origin_path` to `bridged` stays inside one
/// service. Only `env://` bridges are scoped.
pub fn bridge_edge_allowed(uri: &str, origin_path: &str, bridged: &Edge) -> bool {
    !uri.starts_with("env://") || same_service(origin_path, &bridged.file_path)
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
/// `dataproxy`) appearing in the code path (`Dpb.DataProxy/...`), compared
/// as lowercase alphanumerics. Ceiling: shared libraries and services whose
/// deployment directory name is not part of their code path never match
/// (missed bridge, not a false one); a manifest with no usable directory
/// name, and non-manifest pairs, are not scoped at all.
fn same_service(a: &str, b: &str) -> bool {
    let (manifest, code) = match (is_manifest(a), is_manifest(b)) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => return true,
    };
    let dir = manifest.rsplit_once('/').map_or("", |(d, _)| d);
    if !dir.is_empty() && code.starts_with(&format!("{dir}/")) {
        return true;
    }
    let token = dir
        .rsplit('/')
        .map(norm)
        .find(|t| t.len() >= 3 && !GENERIC_DIRS.contains(&t.as_str()));
    match token {
        Some(t) => norm(code).contains(&t),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_match_by_dir_name_or_ancestry() {
        let m = "infra/local/components/apps/dataproxy/deployment.yaml";
        assert!(same_service(m, "src/Dpb.DataProxy/Options.cs"));
        assert!(!same_service(m, "src/Dpb.DataMgr/Options.cs"));
        assert!(same_service("svc/a/deploy.yaml", "svc/a/src/x.cs"));
        assert!(same_service("deploy.yaml", "anything.cs"));
        assert!(same_service("a/x.bicep", "b/y.cs"));
    }
}
