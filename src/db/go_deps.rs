//! Storage of the interfaces a Go repository's dependencies declare (issue
//! #495; read by `indexer::go_deps`, used by `external_ifaces`). Two tables:
//! `go_dep_packages` keys the scan by `(module, version, package)`, and
//! `go_dep_interfaces` holds each package's exported interfaces with their
//! methods (`(name, signature)` pairs as JSON) and embedded interfaces.

use super::Db;
use crate::indexer::go_deps::{DepInterface, DepPackage};
use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashSet;

/// `(module, version, package path)` of one scanned dependency package.
pub type DepPackageKey = (String, String, String);

impl Db {
    /// The dependency packages already read, by `(module, version, package)`.
    pub fn go_dep_packages(&self) -> Result<HashSet<DepPackageKey>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare("SELECT module, version, pkg_path FROM go_dep_packages")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    /// Make the stored dependency packages exactly `keep` plus `added`:
    /// rows of a package outside `keep` are deleted, `added` packages (and
    /// their interfaces) inserted. Returns `(packages added, packages
    /// removed)`.
    pub fn store_go_dep_packages(
        &self,
        keep: &HashSet<DepPackageKey>,
        added: &[(DepPackage, Vec<DepInterface>)],
    ) -> Result<(usize, usize)> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut removed = 0;
        {
            let existing: Vec<DepPackageKey> = {
                let mut stmt =
                    tx.prepare("SELECT module, version, pkg_path FROM go_dep_packages")?;
                let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for key in existing.iter().filter(|k| !keep.contains(*k)) {
                tx.execute(
                    "DELETE FROM go_dep_interfaces WHERE module = ?1 AND version = ?2 AND pkg_path = ?3",
                    params![key.0, key.1, key.2],
                )?;
                tx.execute(
                    "DELETE FROM go_dep_packages WHERE module = ?1 AND version = ?2 AND pkg_path = ?3",
                    params![key.0, key.1, key.2],
                )?;
                removed += 1;
            }
            let mut insert_pkg = tx.prepare(
                "INSERT OR REPLACE INTO go_dep_packages (module, version, pkg_path, origin)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            let mut insert_iface = tx.prepare(
                "INSERT OR REPLACE INTO go_dep_interfaces
                 (module, version, pkg_path, name, methods, embeds)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for (package, interfaces) in added {
                insert_pkg.execute(params![
                    package.module,
                    package.version,
                    package.pkg_path,
                    package.origin
                ])?;
                for iface in interfaces {
                    insert_iface.execute(params![
                        package.module,
                        package.version,
                        package.pkg_path,
                        iface.name,
                        serde_json::to_string(&iface.methods)?,
                        serde_json::to_string(&iface.embeds)?,
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok((added.len(), removed))
    }

    /// `(importing file path, import path)` of every import a Go file of
    /// `graph_version` makes of a package outside the repository (the
    /// extractor's `IMPORTS` rows whose place is not an in-repo directory),
    /// the standard library included.
    pub fn go_foreign_imports(&self, graph_version: i64) -> Result<Vec<(String, String)>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path, e.detail FROM edges e JOIN files f ON f.id = e.file_id
             WHERE e.kind = 'IMPORTS' AND e.graph_version = ?1 AND f.language = 'go'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
             UNION ALL
             SELECT f.path, ur.detail FROM unresolved_references ur JOIN files f ON f.id = ur.file_id
             WHERE ur.edge_kind = 'IMPORTS' AND ur.graph_version = ?1 AND f.language = 'go'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)",
        )?;
        let rows = stmt.query_map([graph_version], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (path, detail) = row?;
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
            out.push((path, import_path.to_string()));
        }
        out.sort();
        out.dedup();
        Ok(out)
    }
}

/// The exported interfaces stored for dependency package `pkg_path`.
pub(crate) fn go_dep_interfaces(conn: &Connection, pkg_path: &str) -> Result<Vec<DepInterface>> {
    let mut stmt = conn.prepare_cached(
        "SELECT name, methods, embeds FROM go_dep_interfaces WHERE pkg_path = ?1 ORDER BY name",
    )?;
    let rows = stmt.query_map([pkg_path], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (name, methods, embeds) = row?;
        out.push(DepInterface {
            name,
            methods: serde_json::from_str(&methods)?,
            embeds: serde_json::from_str(&embeds)?,
        });
    }
    Ok(out)
}
