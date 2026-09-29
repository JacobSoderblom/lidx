//! Which hash-unchanged JS/TS files a sync or reindex must still
//! re-extract. Import candidates are chased through other files' exports and
//! through tsconfig aliases at extraction time (see `javascript`), so a file
//! can go stale without its own bytes changing.

use crate::db::Db;
use crate::indexer::javascript::{self, ConfigRef};
use crate::indexer::scan;
use anyhow::Result;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Meta key prefix for a file's stored export-surface hash.
const EXPORT_SURFACE_PREFIX: &str = "js_export_surface:";
/// Meta key for the tsconfig/jsconfig fingerprint.
pub(crate) const CONFIG_FINGERPRINT_KEY: &str = "js_config_fingerprint";

pub(crate) fn export_surface_key(rel_path: &str) -> String {
    format!("{EXPORT_SURFACE_PREFIX}{rel_path}")
}

pub(crate) struct JsStale<'a> {
    pub db: &'a Db,
    pub repo_root: &'a Path,
}

impl JsStale<'_> {
    /// Repo paths of every live JS/TS file in the index.
    pub fn js_ts_file_paths(&self) -> Result<Vec<String>> {
        let conn = self.db.read_conn()?;
        let mut stmt = conn.prepare("SELECT path FROM files WHERE deleted_version IS NULL")?;
        let paths = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(paths
            .into_iter()
            .filter(|p| javascript::is_js_ts_path(p))
            .collect())
    }

    /// JS/TS files that must be re-extracted although their own hash is
    /// unchanged, given the `changed` (edited, added or deleted) paths of a
    /// batch: files under a changed tsconfig, and importers of a JS/TS file
    /// whose export surface changed. Excludes `changed` itself. Call before
    /// any deletion, while importers' edges still exist.
    pub fn stale_js_files(
        &self,
        changed: &[String],
        graph_version: i64,
    ) -> Result<HashSet<String>> {
        let mut stale = self.config_stale(changed)?;
        stale.extend(self.import_stale(changed, graph_version)?);
        for path in changed {
            stale.remove(path);
        }
        Ok(stale)
    }

    /// Files whose alias resolution a changed tsconfig/jsconfig, or a base
    /// config some tsconfig `extends` (any depth), can affect.
    fn config_stale(&self, changed: &[String]) -> Result<HashSet<String>> {
        let mut stale = HashSet::new();
        let changed_json: HashSet<&str> = changed
            .iter()
            .filter(|p| p.ends_with(".json"))
            .map(String::as_str)
            .collect();
        if changed_json.is_empty() {
            return Ok(stale);
        }
        let files = self.js_ts_file_paths()?;
        // A changed config itself (also when deleted, so no longer an
        // owner) re-maps every import under its directory.
        for cfg in changed_json
            .iter()
            .filter(|p| javascript::is_js_config_path(p))
        {
            let dir = Path::new(cfg).parent().unwrap_or_else(|| Path::new(""));
            stale.extend(
                files
                    .iter()
                    .filter(|p| Path::new(p).starts_with(dir))
                    .cloned(),
            );
        }
        let mut chains: HashMap<PathBuf, Vec<ConfigRef>> = HashMap::new();
        for path in &files {
            let Some(dir) = javascript::find_owning_tsconfig_dir(self.repo_root, path) else {
                continue;
            };
            let chain = chains.entry(dir.clone()).or_insert_with(|| {
                javascript::config_chain(self.repo_root, &dir.join("tsconfig.json"))
            });
            let touched = chain
                .iter()
                .any(|c| matches!(c, ConfigRef::File(f) if changed_json.contains(f.as_str())));
            if touched {
                stale.insert(path.clone());
            }
        }
        Ok(stale)
    }

    /// Whether `path`'s export surface differs from the one stored when it
    /// was last indexed. A deleted, added or never-recorded file counts as
    /// changed; a deleted one's stored hash is zeroed so a later re-add is
    /// also seen as a change.
    fn export_surface_changed(&self, path: &str) -> Result<bool> {
        let key = export_surface_key(path);
        let current = javascript::export_surface_hash(self.repo_root, path);
        let stored = self.db.get_meta_i64(&key)?;
        if current.is_none() {
            self.db.set_meta_i64(&key, 0)?;
            return Ok(true);
        }
        Ok(stored != current)
    }

    /// Importers, per the stored `IMPORTS_FILE` edges (resolved, or still in
    /// the unresolved store), of each changed JS/TS file whose export
    /// surface changed. Recurses only through importers that re-export, since
    /// only those pass a change on. An added or deleted file is covered: an
    /// importer's edge targets the module name even while the file is missing.
    fn import_stale(&self, changed: &[String], graph_version: i64) -> Result<HashSet<String>> {
        let mut stale = HashSet::new();
        let mut frontier: Vec<String> = Vec::new();
        for path in changed.iter().filter(|p| javascript::is_js_ts_path(p)) {
            if self.export_surface_changed(path)? {
                frontier.push(javascript::module_name_from_rel_path(path));
            }
        }
        if frontier.is_empty() {
            return Ok(stale);
        }
        let conn = self.db.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT f.path FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN files f ON f.id = s.file_id
             WHERE e.graph_version = ?1 AND e.kind = 'IMPORTS_FILE' AND e.target_qualname = ?2
             UNION
             SELECT f.path FROM unresolved_references u
             JOIN files f ON f.id = u.file_id
             WHERE u.graph_version = ?1 AND u.edge_kind = 'IMPORTS_FILE' AND u.reference_name = ?2",
        )?;
        let mut seen: HashSet<String> = frontier.iter().cloned().collect();
        while let Some(module) = frontier.pop() {
            let importers = stmt
                .query_map(rusqlite::params![graph_version, module], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for path in importers {
                if !javascript::is_js_ts_path(&path) {
                    continue;
                }
                let importer_module = javascript::module_name_from_rel_path(&path);
                if javascript::is_re_exporting(self.repo_root, &path)
                    && seen.insert(importer_module.clone())
                {
                    frontier.push(importer_module);
                }
                stale.insert(path);
            }
        }
        Ok(stale)
    }

    /// Fingerprint of every `tsconfig.json`/`jsconfig.json` that could own a
    /// JS/TS file (in its directory or any ancestor) plus everything the
    /// owning tsconfigs extend (a missing package base contributes a marker).
    /// Config files aren't indexed, so a reindex can only notice an edit,
    /// addition or deletion by comparing this against the stored value.
    pub fn config_fingerprint(&self, js_paths: &[String]) -> i64 {
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for rel in js_paths {
            let mut dir = Path::new(rel).parent();
            while let Some(d) = dir {
                if !dirs.insert(d.to_path_buf()) {
                    break;
                }
                dir = d.parent();
            }
        }
        let mut data = Vec::new();
        let mut push_file = |rel: &str| {
            data.extend_from_slice(rel.as_bytes());
            data.push(0);
            if let Ok(bytes) = std::fs::read(self.repo_root.join(rel)) {
                data.extend_from_slice(&bytes);
            }
            data.push(0);
        };
        for dir in &dirs {
            for name in ["tsconfig.json", "jsconfig.json"] {
                push_file(&dir.join(name).to_string_lossy());
            }
        }
        let mut owners: BTreeSet<PathBuf> = BTreeSet::new();
        let mut chain_files: BTreeSet<String> = BTreeSet::new();
        let mut missing: BTreeSet<String> = BTreeSet::new();
        for rel in js_paths {
            let Some(dir) = javascript::find_owning_tsconfig_dir(self.repo_root, rel) else {
                continue;
            };
            if !owners.insert(dir.clone()) {
                continue;
            }
            for link in javascript::config_chain(self.repo_root, &dir.join("tsconfig.json")) {
                match link {
                    ConfigRef::File(f) => {
                        chain_files.insert(f);
                    }
                    ConfigRef::MissingPackage(spec) => {
                        missing.insert(spec);
                    }
                }
            }
        }
        for rel in &chain_files {
            push_file(rel);
        }
        for spec in &missing {
            data.extend_from_slice(b"missing:");
            data.extend_from_slice(spec.as_bytes());
            data.push(0);
        }
        scan::hash_i64(&data)
    }
}
