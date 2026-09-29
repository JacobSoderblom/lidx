//! C# `global using` directives apply to every file of the compilation, i.e.
//! the project (nearest `.csproj` ancestor directory; the whole repo when
//! there is none). A file's own bytes then no longer determine how it
//! resolves, so, like `js_stale`, this records each declaring file's
//! directives in `meta` and reports the hash-unchanged files a change to them
//! makes stale.

use crate::db::Db;
use crate::indexer::csharp;
use anyhow::Result;
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
}
