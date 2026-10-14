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

use super::resolver::{MethodSig, method_signature, std_interface};
use crate::indexer::go_deps::DepInterface;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

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
    /// Package directory -> the import paths of the packages outside the
    /// repository that its files import (issue #495), computed once.
    dir_imports: Option<HashMap<String, Vec<String>>>,
    /// Dependency package path -> its stored interfaces (`go_dep_interfaces`).
    dep_interfaces: HashMap<String, Rc<Vec<DepInterface>>>,
    /// (package path, interface) -> its full method set, `None` when an
    /// embedded interface could not be read.
    dep_method_sets: HashMap<(String, String), Option<DepMethodSet>>,
}

/// The methods of a dependency interface, embedded ones included, as
/// `(name, normalized signature)`.
type DepMethodSet = Rc<Vec<(String, MethodSig)>>;

/// How deep embedded interfaces of a dependency are followed.
const MAX_DEP_EMBED_DEPTH: usize = 4;

impl GoExternalEvidence {
    /// The interface of a dependency package that the type declared in
    /// package directory `dir` with methods `own` (`(name, signature)`)
    /// satisfies and that declares `method` (issue #495): every method of
    /// the interface, embedded ones included, is in `own` by name and
    /// signature (`resolver_go::method_signature`). Only the interfaces of
    /// packages the directory's files import are tried, read from
    /// `go_dep_interfaces` (sources on disk at index time); an interface
    /// with an embedded interface the index could not read proves nothing.
    pub(crate) fn dependency_interface(
        &mut self,
        conn: &rusqlite::Connection,
        graph_version: i64,
        dir: &str,
        own: &[(String, Option<String>)],
        method: &str,
    ) -> Result<Option<String>> {
        if self.dir_imports.is_none() {
            self.dir_imports = Some(self.collect_dir_imports(conn, graph_version)?);
        }
        let Some(imports) = self.dir_imports.as_ref().and_then(|m| m.get(dir)).cloned() else {
            return Ok(None);
        };
        let declared: HashMap<&str, MethodSig> = own
            .iter()
            .map(|(n, sig)| (n.as_str(), method_signature(sig.as_deref())))
            .collect();
        for pkg in imports {
            let interfaces = self.interfaces_of(conn, &pkg)?;
            for iface in interfaces.iter() {
                let Some(set) = self.method_set(conn, &pkg, &iface.name, 0)? else {
                    continue;
                };
                if set.is_empty() || !set.iter().any(|(m, _)| m == method) {
                    continue;
                }
                if set
                    .iter()
                    .all(|(m, sig)| declared.get(m.as_str()) == Some(sig))
                {
                    return Ok(Some(format!(
                        "implements {pkg}.{} (dependency)",
                        iface.name
                    )));
                }
            }
        }
        Ok(None)
    }

    fn interfaces_of(
        &mut self,
        conn: &rusqlite::Connection,
        pkg: &str,
    ) -> Result<Rc<Vec<DepInterface>>> {
        if let Some(known) = self.dep_interfaces.get(pkg) {
            return Ok(known.clone());
        }
        let rows = Rc::new(super::go_deps::go_dep_interfaces(conn, pkg)?);
        self.dep_interfaces.insert(pkg.to_string(), rows.clone());
        Ok(rows)
    }

    /// The method set of dependency interface `pkg.name`: its own methods
    /// and those of what it embeds (the same package's interfaces, another
    /// stored package's, or a standard-library interface of the table).
    fn method_set(
        &mut self,
        conn: &rusqlite::Connection,
        pkg: &str,
        name: &str,
        depth: usize,
    ) -> Result<Option<DepMethodSet>> {
        let key = (pkg.to_string(), name.to_string());
        if let Some(done) = self.dep_method_sets.get(&key) {
            return Ok(done.clone());
        }
        // a cycle or a chain too deep proves nothing
        self.dep_method_sets.insert(key.clone(), None);
        let found = self
            .interfaces_of(conn, pkg)?
            .iter()
            .find(|i| i.name == name)
            .cloned();
        let result = match found {
            Some(iface) if depth < MAX_DEP_EMBED_DEPTH => {
                let mut set: Vec<(String, MethodSig)> = iface
                    .methods
                    .iter()
                    .map(|(m, sig)| (m.clone(), method_signature(Some(sig))))
                    .collect();
                let mut complete = true;
                for embed in &iface.embeds {
                    let inner: Option<Vec<(String, MethodSig)>> =
                        match crate::indexer::go_deps::split_embed(embed) {
                            None => self
                                .method_set(conn, pkg, embed, depth + 1)?
                                .map(|s| s.as_ref().clone()),
                            Some((path, n)) => match std_interface(path, n) {
                                Some(methods) => Some(
                                    methods
                                        .iter()
                                        .map(|(m, sig)| {
                                            ((*m).to_string(), method_signature(Some(sig)))
                                        })
                                        .collect(),
                                ),
                                None => self
                                    .method_set(conn, path, n, depth + 1)?
                                    .map(|s| s.as_ref().clone()),
                            },
                        };
                    match inner {
                        Some(inner) => {
                            for (m, sig) in inner {
                                if !set.iter().any(|(have, _)| *have == m) {
                                    set.push((m, sig));
                                }
                            }
                        }
                        None => complete = false,
                    }
                }
                complete.then(|| Rc::new(set))
            }
            _ => None,
        };
        self.dep_method_sets.insert(key, result.clone());
        Ok(result)
    }

    /// Package directory -> import paths of the packages outside the
    /// repository that its Go files import, from the `IMPORTS` rows.
    fn collect_dir_imports(
        &self,
        conn: &rusqlite::Connection,
        graph_version: i64,
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut stmt = conn.prepare(
            "SELECT f.path, e.detail FROM edges e JOIN files f ON f.id = e.file_id
             WHERE e.kind = 'IMPORTS' AND e.graph_version = ?1 AND f.language = 'go'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
             UNION ALL
             SELECT f.path, ur.detail FROM unresolved_references ur JOIN files f ON f.id = ur.file_id
             WHERE ur.edge_kind = 'IMPORTS' AND ur.graph_version = ?1 AND f.language = 'go'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)",
        )?;
        let rows = stmt
            .query_map([graph_version], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        for (path, detail) in rows {
            let Some(detail) = detail else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&detail) else {
                continue;
            };
            let Some(import_path) = v["import_path"].as_str() else {
                continue;
            };
            if !crate::indexer::go_deps::is_dependency_place(
                v["place"].as_str().unwrap_or(import_path),
            ) {
                continue;
            }
            let dir = path.rsplit_once('/').map_or("", |(d, _)| d).to_string();
            let list = out.entry(dir).or_default();
            if !list.iter().any(|p| p == import_path) {
                list.push(import_path.to_string());
            }
        }
        for list in out.values_mut() {
            list.sort();
        }
        Ok(out)
    }
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
            return Ok(Some(format!("asserts {label}; method set unknown")));
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
        let named: Vec<String> = embedded
            .into_iter()
            .filter(|(q, _)| external.contains(q))
            .map(|(q, n)| format!("{q}.{n}"))
            .collect();
        Ok((!named.is_empty()).then(|| format!("embeds {}; method set unknown", named.join(", "))))
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
        let mut out: HashMap<(String, String), Vec<String>> = HashMap::new();
        for (signature, path, file_id) in rows {
            let Some((interfaces, ty)) = crate::model::go_assert_data(&signature) else {
                continue;
            };
            let dir = path.rsplit_once('/').map_or("", |(d, _)| d).to_string();
            for iface in interfaces.split(',') {
                let Some((qualifier, _)) = iface.split_once('.') else {
                    continue;
                };
                if !self
                    .external_names(conn, graph_version, file_id)?
                    .contains(qualifier)
                {
                    continue;
                }
                let known = out.entry((dir.clone(), ty.to_string())).or_default();
                if !known.iter().any(|k| k == iface) {
                    known.push(iface.to_string());
                }
            }
        }
        let out = out.into_iter().map(|(k, v)| (k, v.join(", "))).collect();
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
