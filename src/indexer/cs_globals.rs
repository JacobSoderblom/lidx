//! C# `global using` directives apply to every file of the compilation, i.e.
//! the project (nearest `.csproj` ancestor directory; the whole repo when
//! there is none). A file's own bytes then no longer determine how it
//! resolves, so, like `js_stale`, this records each declaring file's
//! directives in `meta` and reports the hash-unchanged files a change to them
//! makes stale.

use crate::db::Db;
use crate::indexer::csharp;
use crate::indexer::extract::ExtensionMethodRow;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

const PREFIX: &str = "cs_global_usings:";

/// A declaring file's stored record: its project and directives.
type Record = (String, Vec<String>);

pub(crate) fn is_csharp_path(rel_path: &str) -> bool {
    rel_path.ends_with(".cs")
}

pub(crate) struct CsGlobals<'a> {
    pub db: &'a Db,
    pub repo_root: &'a Path,
}

/// Project directory (repo-relative, `""` for the repo root) of `rel_path`:
/// the nearest ancestor directory holding a `.csproj`, else the repo root.
/// `cache` memoises per directory.
pub(crate) fn project_dir(
    repo_root: &Path,
    rel_path: &str,
    cache: &mut HashMap<PathBuf, String>,
) -> String {
    let mut chain: Vec<PathBuf> = Vec::new();
    let mut dir = Path::new(rel_path).parent().map(Path::to_path_buf);
    let mut found = String::new();
    while let Some(d) = dir {
        if let Some(known) = cache.get(&d) {
            found = known.clone();
            break;
        }
        chain.push(d.clone());
        let has_csproj = std::fs::read_dir(repo_root.join(&d)).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|e| e.path().extension().is_some_and(|x| x == "csproj"))
        });
        if has_csproj {
            found = d.to_string_lossy().into_owned();
            break;
        }
        dir = d.parent().map(Path::to_path_buf);
    }
    for d in chain {
        cache.insert(d, found.clone());
    }
    found
}

fn encode(record: &Record) -> String {
    let mut out = record.0.clone();
    for entry in &record.1 {
        out.push('\n');
        out.push_str(entry);
    }
    out
}

fn decode(value: &str) -> Record {
    let mut lines = value.lines();
    let project = lines.next().unwrap_or("").to_string();
    (project, lines.map(str::to_string).collect())
}

impl CsGlobals<'_> {
    fn stored(&self) -> Result<BTreeMap<String, Record>> {
        Ok(self
            .db
            .meta_with_prefix(PREFIX)?
            .into_iter()
            .map(|(k, v)| (k[PREFIX.len()..].to_string(), decode(&v)))
            .collect())
    }

    /// `global using` entries of every project, keyed by project directory.
    pub fn by_project(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        for (_, (project, entries)) in self.stored()? {
            let list = out.entry(project).or_default();
            for entry in entries {
                if !list.contains(&entry) {
                    list.push(entry);
                }
            }
        }
        Ok(out)
    }

    /// Bring the stored records in line with `changed` (edited, added or
    /// deleted repo paths of a batch) and with the current project layout,
    /// and return the C# files, other than `changed` ones, of every project
    /// whose directives or membership thereby changed. `all_csharp` lists
    /// every live C# path; it is only called when something changed.
    pub fn prepare(
        &self,
        changed: &[String],
        all_csharp: impl FnOnce() -> Result<Vec<String>>,
    ) -> Result<HashSet<String>> {
        let stored = self.stored()?;
        let mut cache: HashMap<PathBuf, String> = HashMap::new();
        let mut projects: BTreeSet<String> = BTreeSet::new();
        let mut update = |path: &str, new: Option<Record>| -> Result<()> {
            let old = stored.get(path);
            if old == new.as_ref() {
                return Ok(());
            }
            projects.extend(old.map(|(p, _)| p.clone()));
            projects.extend(new.as_ref().map(|(p, _)| p.clone()));
            let key = format!("{PREFIX}{path}");
            match &new {
                Some(record) => self.db.set_meta_str(&key, &encode(record))?,
                None => self.db.delete_meta(&key)?,
            }
            Ok(())
        };
        let changed_set: HashSet<&str> = changed.iter().map(String::as_str).collect();
        for path in changed.iter().filter(|p| is_csharp_path(p)) {
            let source = std::fs::read_to_string(self.repo_root.join(path)).ok();
            let new = source
                .map(|s| csharp::scan_global_usings(&s))
                .filter(|g| !g.is_empty())
                .map(|g| (project_dir(self.repo_root, path, &mut cache), g));
            update(path, new)?;
        }
        // A `.csproj` added or removed moves files between projects.
        for (path, (project, entries)) in &stored {
            if changed_set.contains(path.as_str()) {
                continue;
            }
            let now = project_dir(self.repo_root, path, &mut cache);
            if &now != project {
                update(path, Some((now, entries.clone())))?;
            }
        }
        if projects.is_empty() {
            return Ok(HashSet::new());
        }
        let mut stale = HashSet::new();
        for path in all_csharp()? {
            if !changed_set.contains(path.as_str())
                && projects.contains(&project_dir(self.repo_root, &path, &mut cache))
            {
                stale.insert(path);
            }
        }
        Ok(stale)
    }

    /// Hash-unchanged C# files whose stored `CALLS` import candidates name an
    /// extension method declared in one of the `changed` paths. The
    /// extractor derives those candidates from the extension methods it sees
    /// in the same run (`csharp::extension_method_candidates`), so they go
    /// stale when the declaration is renamed, moved or removed (issue #256).
    /// Re-extracting the caller recomputes them like a fresh index would.
    /// `graph_version` is the version holding the files' current symbols and
    /// edges; call before any deletion.
    pub fn stale_extension_callers(
        &self,
        changed: &[String],
        graph_version: i64,
    ) -> Result<HashSet<String>> {
        let conn = self.db.read_conn()?;
        let mut declared: HashSet<String> = HashSet::new();
        {
            let mut stmt = conn.prepare(
                "SELECT s.qualname FROM symbols s JOIN files f ON f.id = s.file_id
                 WHERE s.graph_version = ?1 AND f.path = ?2 AND s.kind = 'method'
                   AND s.signature LIKE ?3",
            )?;
            for path in changed.iter().filter(|p| is_csharp_path(p)) {
                let rows = stmt.query_map(
                    rusqlite::params![graph_version, path, csharp::EXTENSION_SIGNATURE_LIKE],
                    |r| r.get::<_, String>(0),
                )?;
                for row in rows {
                    declared.insert(row?);
                }
            }
        }
        if declared.is_empty() {
            return Ok(HashSet::new());
        }
        let changed_set: HashSet<&str> = changed.iter().map(String::as_str).collect();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT f.path, e.import_candidates
             FROM edges e JOIN files f ON f.id = e.file_id
             WHERE e.graph_version = ?1 AND e.kind = 'CALLS' AND f.language = 'csharp'
               AND e.import_candidates IS NOT NULL",
        )?;
        let rows = stmt.query_map([graph_version], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut stale = HashSet::new();
        for row in rows {
            let (path, candidates) = row?;
            if changed_set.contains(path.as_str()) {
                continue;
            }
            let names: Vec<String> = serde_json::from_str(&candidates)
                .with_context(|| format!("decode import_candidates of an edge in {path}"))?;
            if names.iter().any(|n| declared.contains(n)) {
                stale.insert(path);
            }
        }
        Ok(stale)
    }

    /// Every C# extension method stored at `graph_version`, except those
    /// declared in the `changed` paths (edited, added or deleted files of
    /// this run: the extractor re-registers the survivors itself). Feeds
    /// `LanguageExtractor::seed_extension_methods` so the extension registry
    /// covers the whole repository, not just the files extracted this run
    /// (issue #256).
    pub fn extension_methods(
        &self,
        changed: &[String],
        graph_version: i64,
    ) -> Result<Vec<ExtensionMethodRow>> {
        let conn = self.db.read_conn()?;
        let changed_set: HashSet<&str> = changed.iter().map(String::as_str).collect();
        let mut stmt = conn.prepare(
            "SELECT f.id, f.path, s.qualname, s.signature
             FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE s.graph_version = ?1 AND f.language = 'csharp'
               AND s.kind = 'method' AND s.signature LIKE ?2",
        )?;
        let methods: Vec<(i64, String, String, String)> = stmt
            .query_map(
                rusqlite::params![graph_version, csharp::EXTENSION_SIGNATURE_LIKE],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?
            .collect::<rusqlite::Result<_>>()?;
        let mut namespaces: HashMap<i64, Vec<String>> = HashMap::new();
        let mut ns_stmt = conn.prepare(
            "SELECT qualname FROM symbols
             WHERE graph_version = ?1 AND file_id = ?2 AND kind = 'namespace'",
        )?;
        let mut out = Vec::new();
        for (file_id, path, qualname, signature) in methods {
            if changed_set.contains(path.as_str()) || !csharp::is_extension_signature(&signature) {
                continue;
            }
            if !namespaces.contains_key(&file_id) {
                let names = ns_stmt
                    .query_map(rusqlite::params![graph_version, file_id], |r| {
                        r.get::<_, String>(0)
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                namespaces.insert(file_id, names);
            }
            // The declaring namespace is the longest namespace symbol of
            // the file that prefixes the method's qualname.
            let namespace = namespaces[&file_id]
                .iter()
                .filter(|ns| {
                    qualname
                        .strip_prefix(ns.as_str())
                        .is_some_and(|r| r.starts_with('.'))
                })
                .max_by_key(|ns| ns.len())
                .cloned()
                .unwrap_or_default();
            out.push(ExtensionMethodRow {
                qualname,
                namespace,
                signature,
            });
        }
        Ok(out)
    }
}
