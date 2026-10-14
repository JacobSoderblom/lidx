//! Methods that implement an interface or trait of the standard library or a
//! dependency (issue #472).
//!
//! The runtime or the dependency calls such a method through the interface,
//! so no call edge in the repository reaches it although it is used -- when
//! its receiver type is live. `dead_symbols` therefore treats it as reached
//! through its type, with the reason [`REASON`], and keeps the same method of
//! a dead type dead.
//!
//! - Go: `go_std_interface` (structural: name and signature against the
//!   standard-library interface table), and a type that is declared to
//!   implement an interface of another package (`var _ pkg.Iface = (*T)(nil)`,
//!   `T{}`, `&T{}`, `new(T)`, or a struct embedding `pkg.Iface`): the
//!   interface's methods are unknown (a dependency's source is not indexed),
//!   but only an exported method can satisfy an interface of another package,
//!   so the exported methods of T are live with T.
//! - Rust: the extractor links `impl ExternalTrait for T` methods to the
//!   trait's external stub with an `IMPLEMENTS` edge; `dead_symbols` reads it.
//! - TypeScript: the protocols the runtime calls on any object (`toString`,
//!   `Symbol.iterator`, ...) and the methods of the well-known external
//!   interfaces a class names in `implements` (`Disposable`, `Iterator`, ...).

/// The reason on the liveness path.
pub(crate) const REASON: &str = "implements_external_interface";

/// Methods the JavaScript runtime calls on any object, whatever the class
/// declares it implements.
const TS_RUNTIME_PROTOCOL: &[&str] = &[
    "toString",
    "toJSON",
    "valueOf",
    "[Symbol.iterator]",
    "[Symbol.asyncIterator]",
    "[Symbol.toPrimitive]",
    "[Symbol.dispose]",
    "[Symbol.asyncDispose]",
];

/// Well-known external interfaces (ECMAScript, DOM, Node, VS Code) and the
/// methods a class implementing them declares.
const TS_EXTERNAL_INTERFACES: &[(&str, &[&str])] = &[
    ("Disposable", &["dispose"]),
    ("AsyncDisposable", &["disposeAsync"]),
    ("IDisposable", &["dispose"]),
    ("Iterator", &["next", "return", "throw"]),
    ("AsyncIterator", &["next", "return", "throw"]),
    ("IterableIterator", &["next", "return", "throw"]),
    ("AsyncIterableIterator", &["next", "return", "throw"]),
    ("Generator", &["next", "return", "throw"]),
    ("PromiseLike", &["then"]),
    ("Promise", &["then", "catch", "finally"]),
    ("EventListenerObject", &["handleEvent"]),
    ("Comparable", &["compareTo"]),
    ("Transformer", &["start", "transform", "flush"]),
    ("UnderlyingSource", &["start", "pull", "cancel"]),
    ("UnderlyingSink", &["start", "write", "close", "abort"]),
    (
        "ProxyHandler",
        &["get", "set", "has", "deleteProperty", "apply", "construct"],
    ),
];

/// The external interface a TypeScript class method implements: a runtime
/// protocol method, or a method of an interface in `implemented` (the simple
/// names the class lists after `implements`). Generic arguments are ignored.
pub(crate) fn ts_external_interface(name: &str, implemented: &[String]) -> Option<String> {
    if TS_RUNTIME_PROTOCOL.contains(&name) {
        return Some(format!("runtime protocol {name}"));
    }
    implemented.iter().find_map(|iface| {
        let simple = iface.split('<').next().unwrap_or(iface);
        let simple = simple.rsplit('.').next().unwrap_or(simple).trim();
        TS_EXTERNAL_INTERFACES
            .iter()
            .find(|(n, methods)| *n == simple && methods.contains(&name))
            .map(|(n, _)| (*n).to_string())
    })
}

use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// Evidence that a Go type implements an interface of a package outside the
/// repository, read from the index: the `#[go_assert(..)]` signature of a
/// blank `var _ pkg.Iface = ...` assertion (the extractor parses its value),
/// or an embedded `pkg.Iface` field of the struct signature. `pkg` is
/// outside the repository when the file's import of it carries a place that
/// is not an in-repo directory (`IMPORTS` rows, resolved or not, keep the
/// extractor's `{"place": ..}` detail).
#[derive(Default)]
pub(crate) struct GoExternalEvidence {
    /// (package directory, type name) -> the interface asserted.
    asserted: Option<HashMap<(String, String), String>>,
    /// file id -> names its imports bind to packages outside the repository.
    external_names: HashMap<i64, HashSet<String>>,
}

impl GoExternalEvidence {
    /// The interface of another package that type `type_id`
    /// (`type_name`, declared in package directory `dir`) is asserted to or
    /// embeds, if any.
    pub(crate) fn of(
        &mut self,
        conn: &rusqlite::Connection,
        graph_version: i64,
        type_id: i64,
        type_name: &str,
        dir: &str,
    ) -> Result<Option<String>> {
        if self.asserted.is_none() {
            self.asserted = Some(self.collect_assertions(conn, graph_version)?);
        }
        if let Some(label) = self
            .asserted
            .as_ref()
            .and_then(|m| m.get(&(dir.to_string(), type_name.to_string())))
        {
            return Ok(Some(label.clone()));
        }
        let (signature, file_id): (Option<String>, i64) = conn.query_row(
            "SELECT signature, file_id FROM symbols WHERE id = ?1",
            [type_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let embedded = signature
            .as_deref()
            .and_then(crate::model::go_type_data)
            .map(embedded_qualified)
            .unwrap_or_default();
        if embedded.is_empty() {
            return Ok(None);
        }
        let external = self.external_names(conn, graph_version, file_id)?;
        Ok(embedded
            .into_iter()
            .find(|(q, _)| external.contains(q))
            .map(|(q, n)| format!("{q}.{n}")))
    }

    /// The names the imports of `file_id` bind to packages outside the
    /// repository (the standard library included).
    pub(crate) fn external_names(
        &mut self,
        conn: &rusqlite::Connection,
        graph_version: i64,
        file_id: i64,
    ) -> Result<&HashSet<String>> {
        if let std::collections::hash_map::Entry::Vacant(slot) = self.external_names.entry(file_id)
        {
            let mut stmt = conn.prepare_cached(
                "SELECT detail FROM edges
                 WHERE file_id = ?1 AND kind = 'IMPORTS' AND graph_version = ?2
                 UNION ALL
                 SELECT detail FROM unresolved_references
                 WHERE file_id = ?1 AND edge_kind = 'IMPORTS' AND graph_version = ?2",
            )?;
            let details = stmt
                .query_map(rusqlite::params![file_id, graph_version], |r| {
                    r.get::<_, Option<String>>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut names = HashSet::new();
            for detail in details.into_iter().flatten() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&detail) else {
                    continue;
                };
                let path = v["import_path"].as_str().unwrap_or_default();
                let place = v["place"].as_str().unwrap_or(path);
                if place.starts_with('>') {
                    continue;
                }
                let bound = v["bound_name"]
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| crate::indexer::go::default_package_name(path));
                if let Some(bound) = bound
                    && !bound.is_empty()
                    && bound != "_"
                    && bound != "."
                {
                    names.insert(bound);
                }
            }
            slot.insert(names);
        }
        Ok(&self.external_names[&file_id])
    }

    fn collect_assertions(
        &mut self,
        conn: &rusqlite::Connection,
        graph_version: i64,
    ) -> Result<HashMap<(String, String), String>> {
        let mut stmt = conn.prepare(
            "SELECT s.signature, f.path, f.id
             FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE s.kind = 'variable' AND s.name = '_' AND f.language = 'go'
               AND s.graph_version = ?1 AND s.signature LIKE '#[go_assert(%'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)",
        )?;
        let rows = stmt
            .query_map([graph_version], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        let mut out = HashMap::new();
        for (signature, path, file_id) in rows {
            let Some((iface, ty)) = crate::model::go_assert_data(&signature) else {
                continue;
            };
            let Some((qualifier, _)) = iface.split_once('.') else {
                continue;
            };
            if !self
                .external_names(conn, graph_version, file_id)?
                .contains(qualifier)
            {
                continue;
            }
            let dir = path.rsplit_once('/').map_or("", |(d, _)| d).to_string();
            out.entry((dir, ty.to_string()))
                .or_insert_with(|| iface.to_string());
        }
        Ok(out)
    }
}

/// `(qualifier, name)` of every embedded `pkg.Type` field in a Go struct
/// signature (`{~clio.UI; ~*io.Reader; ~Local}` marks embedded fields with `~`).
fn embedded_qualified(struct_text: &str) -> Vec<(String, String)> {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    let mut out = Vec::new();
    let mut rest = struct_text;
    while let Some(i) = rest.find('~') {
        rest = rest[i + 1..].trim_start_matches('*');
        let q: String = rest.chars().take_while(|c| ident(*c)).collect();
        let after = &rest[q.len()..];
        if let Some(tail) = after.strip_prefix('.') {
            let n: String = tail.chars().take_while(|c| ident(*c)).collect();
            if !q.is_empty() && !n.is_empty() {
                out.push((q, n));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_embedded_fields() {
        assert_eq!(
            embedded_qualified("{n int; ~clio.UI; ~*io.Reader; ~Local}"),
            vec![
                ("clio".to_string(), "UI".to_string()),
                ("io".to_string(), "Reader".to_string())
            ]
        );
    }

    #[test]
    fn ts_protocol_and_named_interface() {
        assert!(ts_external_interface("toString", &[]).is_some());
        assert!(ts_external_interface("dispose", &[]).is_none());
        assert_eq!(
            ts_external_interface("dispose", &["Disposable".into()]).as_deref(),
            Some("Disposable")
        );
        assert!(ts_external_interface("next", &["vscode.Disposable".into()]).is_none());
        assert!(ts_external_interface("next", &["Iterator<number>".into()]).is_some());
    }
}
