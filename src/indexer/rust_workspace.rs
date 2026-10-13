//! What the Rust extractor needs to know about the Cargo packages of a
//! repository (issue #434): which crate a path such as
//! `grep_regex::RegexMatcher::new` names when it is written in one of them.
//! Every package's items are qualified `crate::...`, so a path that starts
//! with another package's name can only be bound by knowing that package's
//! directory.
//!
//! Built from every `Cargo.toml` of the repository (workspace `members`
//! globs, each package's `[package] name`, a `[lib] name` override, and the
//! `path` / `workspace = true` / registry form of every dependency) and from
//! the crate-level re-exports of each library root (`pub use grep_cli as
//! cli;`). Built lazily once per index run (see `RustExtractor::begin_run`),
//! so an incremental sync sees what a fresh reindex would; its
//! [`fingerprint`](RustWorkspace::fingerprint) is what tells a sync that
//! hash-unchanged Rust files have to be extracted again.

use crate::db::resolver::{RUST_EXTERNAL_CRATE, rust_crate_pin};
use crate::indexer::scan::{self, ScanOptions};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;

/// How one package declares a dependency: where it comes from, and whether
/// the key is a rename (`package = ".."`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dep {
    source: DepSource,
    /// `key = { package = "real-name", .. }`: code names the crate by `key`.
    /// Without it code names the dependency's *library* (its `[lib] name`).
    renamed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DepSource {
    /// `path = ".."`: the repo-relative directory it names.
    Path(String),
    /// `workspace = true`: declared in the workspace root's
    /// `[workspace.dependencies]`.
    Inherited,
    /// A version, a git source or a registry rename: a crate outside the repo.
    Registry,
}

#[derive(Debug)]
struct Package {
    /// `[lib] name`, else the package name; `-` read as `_`. `None` when the
    /// package has no library target (nothing can name it in a path).
    lib_name: Option<String>,
    /// Dependency key (`-` read as `_`) -> declaration.
    deps: BTreeMap<String, Dep>,
    /// Directory of the workspace root this package is a member of.
    workspace_root: Option<String>,
    /// `pub use <crate> as <alias>;` in the library root.
    aliases: BTreeMap<String, String>,
    /// The edition is 2021 or later (`edition = "2021"`, or inherited from
    /// `[workspace.package]`), whose prelude adds `FromIterator`, `TryFrom`
    /// and `TryInto`.
    edition_2021: bool,
}

/// What a path's first segment names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CrateRef {
    /// A package of this repository: its directory (`""` for the root).
    Member(String),
    /// A dependency from outside the repository.
    External,
}

#[derive(Debug, Default)]
pub(crate) struct RustWorkspace {
    /// Repo-relative directory -> package.
    packages: BTreeMap<String, Package>,
    /// Workspace root directory -> its `[workspace.dependencies]`.
    workspace_deps: BTreeMap<String, BTreeMap<String, Dep>>,
}

/// A parsed manifest and the directory it sits in.
struct Manifest {
    dir: String,
    table: toml::Table,
}

impl RustWorkspace {
    pub(crate) fn scan(repo_root: &Path) -> Self {
        let mut manifests = Vec::new();
        // The indexer's own file walk, so ignore files and excluded
        // directories mean here what they mean for every other file.
        let walk = scan::walker_from(repo_root, ScanOptions::default(), None);
        for entry in walk.flatten() {
            if entry.file_name() != "Cargo.toml" || !entry.file_type().is_some_and(|t| t.is_file())
            {
                continue;
            }
            let Some(dir) = entry
                .path()
                .parent()
                .and_then(|p| p.strip_prefix(repo_root).ok())
                .map(|p| p.to_string_lossy().replace('\\', "/"))
            else {
                continue;
            };
            let Ok(text) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            let Ok(table) = text.parse::<toml::Table>() else {
                continue;
            };
            manifests.push(Manifest { dir, table });
        }
        manifests.sort_by(|a, b| a.dir.cmp(&b.dir));
        Self::from_manifests(repo_root, &manifests)
    }

    fn from_manifests(repo_root: &Path, manifests: &[Manifest]) -> Self {
        let mut workspace = Self::default();
        // Workspace roots: members globs, excludes and shared dependencies.
        let mut roots: Vec<(&str, Vec<String>, Vec<String>)> = Vec::new();
        for m in manifests {
            let Some(ws) = m.table.get("workspace").and_then(|v| v.as_table()) else {
                continue;
            };
            roots.push((
                &m.dir,
                string_list(ws.get("members")),
                string_list(ws.get("exclude")),
            ));
            let deps = ws
                .get("dependencies")
                .and_then(|v| v.as_table())
                .map(|t| read_deps(t, &m.dir))
                .unwrap_or_default();
            workspace.workspace_deps.insert(m.dir.clone(), deps);
        }
        for m in manifests {
            let Some(package) = m.table.get("package").and_then(|v| v.as_table()) else {
                continue;
            };
            let Some(name) = package.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let lib = m.table.get("lib").and_then(|v| v.as_table());
            let lib_root = lib
                .and_then(|l| l.get("path"))
                .and_then(|v| v.as_str())
                .map(|p| join_rel(&m.dir, p))
                .or_else(|| {
                    let default = join_rel(&m.dir, "src/lib.rs");
                    repo_root.join(&default).is_file().then_some(default)
                });
            let lib_name = lib_root.as_ref().map(|_| {
                normalize(
                    lib.and_then(|l| l.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(name),
                )
            });
            let mut deps = BTreeMap::new();
            collect_dependency_tables(&m.table, &m.dir, &mut deps);
            let workspace_root = roots
                .iter()
                .filter(|(root, members, exclude)| {
                    *root == m.dir
                        || (is_under(&m.dir, root)
                            && matches_any(members, root, &m.dir)
                            && !matches_any(exclude, root, &m.dir))
                })
                .max_by_key(|(root, ..)| root.len())
                .map(|(root, ..)| (*root).to_string());
            let aliases = lib_root
                .as_deref()
                .and_then(|path| std::fs::read_to_string(repo_root.join(path)).ok())
                .map(|text| crate::indexer::rust::reexported_crate_aliases(&text))
                .unwrap_or_default();
            let inherited = package
                .get("edition")
                .and_then(|v| v.as_table())
                .is_some_and(|t| t.get("workspace").and_then(|w| w.as_bool()) == Some(true));
            let edition = if inherited {
                workspace_root.as_ref().and_then(|root| {
                    manifests
                        .iter()
                        .find(|w| w.dir == *root)
                        .and_then(|w| w.table.get("workspace")?.get("package")?.get("edition"))
                        .and_then(|e| e.as_str())
                })
            } else {
                package.get("edition").and_then(|e| e.as_str())
            };
            let edition_2021 = edition.and_then(|e| e.parse::<u32>().ok()).unwrap_or(2015) >= 2021;
            workspace.packages.insert(
                m.dir.clone(),
                Package {
                    edition_2021,
                    lib_name,
                    deps,
                    workspace_root,
                    aliases,
                },
            );
        }
        workspace
    }

    /// The package a repo-relative file belongs to: the directory of the
    /// nearest `Cargo.toml` above it that declares one. (`rust::find_crate_root`
    /// answers a neighbouring question for module names -- the nearest
    /// `Cargo.toml` of any kind, a virtual workspace manifest included -- and
    /// reads the disk per file; this one runs on the manifests already
    /// scanned, and a virtual manifest is no package.)
    pub(crate) fn package_of(&self, rel_path: &str) -> Option<&str> {
        let mut dir = rel_path.rsplit_once('/').map_or("", |(dir, _)| dir);
        loop {
            if let Some((found, _)) = self.packages.get_key_value(dir) {
                return Some(found);
            }
            if dir.is_empty() {
                return None;
            }
            dir = dir.rsplit_once('/').map_or("", |(dir, _)| dir);
        }
    }

    /// What `name` (the first segment of a path written in the package at
    /// `package_dir`) refers to; `None` when it names no crate this index
    /// knows (a module, a type, `std`, an undeclared crate).
    pub(crate) fn lookup(&self, package_dir: &str, name: &str) -> Option<CrateRef> {
        let package = self.packages.get(package_dir)?;
        let name = normalize(name);
        let registry = Dep {
            source: DepSource::Registry,
            renamed: false,
        };
        for (key, declared) in &package.deps {
            let dep = match &declared.source {
                DepSource::Inherited => package
                    .workspace_root
                    .as_ref()
                    .and_then(|root| self.workspace_deps.get(root))
                    .and_then(|deps| deps.get(key))
                    .unwrap_or(&registry),
                _ => declared,
            };
            let member = match &dep.source {
                DepSource::Path(dir) => self.packages.get(dir).map(|p| (dir, p)),
                _ => None,
            };
            // Code names a dependency by its key when renamed, else by the
            // library it builds: `a = { path = "../a" }` with `[lib] name =
            // "a_lib"` is `a_lib::..`, and `a::..` names nothing.
            let code_name = match (&dep.source, dep.renamed, member) {
                (DepSource::Path(_), false, Some((_, p))) => p.lib_name.as_deref(),
                (DepSource::Path(_), false, None) => None,
                _ => Some(key.as_str()),
            };
            if code_name != Some(name.as_str()) {
                continue;
            }
            return match (&dep.source, member) {
                (DepSource::Registry, _) => Some(CrateRef::External),
                (DepSource::Path(dir), Some((_, p))) if p.lib_name.is_some() => {
                    Some(CrateRef::Member(dir.clone()))
                }
                _ => None,
            };
        }
        // A test, bench, example or binary of a package names its library
        // by the library's own name.
        if package.lib_name.as_deref() == Some(name.as_str()) {
            return Some(CrateRef::Member(package_dir.to_string()));
        }
        // Not declared: a sibling in the same workspace is still the only
        // crate the name can mean (manifests are not always complete in a
        // checkout).
        let root = package.workspace_root.as_ref()?;
        let mut siblings = self.packages.iter().filter(|(_, p)| {
            p.workspace_root.as_ref() == Some(root) && p.lib_name.as_deref() == Some(name.as_str())
        });
        let (dir, _) = siblings.next()?;
        siblings
            .next()
            .is_none()
            .then(|| CrateRef::Member(dir.clone()))
    }

    /// The crate `alias` names inside the library of the package at
    /// `member_dir` through `pub use <crate> as <alias>;`.
    pub(crate) fn follow_alias(&self, member_dir: &str, alias: &str) -> Option<CrateRef> {
        let target = self.packages.get(member_dir)?.aliases.get(alias)?;
        self.lookup(member_dir, target)
    }

    /// A stable summary of everything above: when it differs from the one a
    /// previous index recorded, files extracted against the old view are
    /// stale.
    pub(crate) fn fingerprint(&self) -> String {
        let text = format!("{:?}|{:?}", self.packages, self.workspace_deps);
        blake3::hash(text.as_bytes()).to_hex().to_string()
    }
}

/// The workspace as seen from one file: the package it belongs to decides
/// what its paths' first segments name.
pub(crate) struct PackageView {
    workspace: Rc<RustWorkspace>,
    package: String,
}

impl PackageView {
    /// The directory of the Cargo package the file belongs to (`""` for the
    /// repo root).
    pub(crate) fn package(&self) -> &str {
        &self.package
    }

    /// Whether the package is edition 2021 or later.
    pub(crate) fn edition_2021(&self) -> bool {
        self.workspace
            .packages
            .get(&self.package)
            .is_some_and(|p| p.edition_2021)
    }

    /// The view of the file at `rel_path`; `None` when no `Cargo.toml` above
    /// it declares a package.
    pub(crate) fn of_file(workspace: &Rc<RustWorkspace>, rel_path: &str) -> Option<Self> {
        Some(Self {
            workspace: Rc::clone(workspace),
            package: workspace.package_of(rel_path)?.to_string(),
        })
    }

    /// The `import_candidates` markers for paths written in this file whose
    /// first segment names another crate: a pin (`@<dir>|<rest>`) for a
    /// package of this repository, [`RUST_EXTERNAL_CRATE`] for a dependency
    /// from outside it. `paths` are `::`-joined, the first segment already
    /// read through the file's `use`s.
    pub(crate) fn markers(&self, paths: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for path in paths {
            let mut segments: Vec<&str> = path.split("::").collect();
            let Some(mut krate) = self.workspace.lookup(&self.package, segments[0]) else {
                continue;
            };
            segments.remove(0);
            // `grep::cli::f`: the facade crate re-exports `grep_cli` as `cli`.
            for _ in 0..4 {
                let (CrateRef::Member(dir), Some(alias)) = (&krate, segments.first()) else {
                    break;
                };
                let Some(next) = self.workspace.follow_alias(dir, alias) else {
                    break;
                };
                krate = next;
                segments.remove(0);
            }
            let marker = match krate {
                CrateRef::Member(dir) if !segments.is_empty() => {
                    rust_crate_pin(&dir, &segments.join("::"))
                }
                CrateRef::Member(_) => continue,
                CrateRef::External => RUST_EXTERNAL_CRATE.to_string(),
            };
            if !out.contains(&marker) {
                out.push(marker);
            }
        }
        out
    }
}

fn normalize(name: &str) -> String {
    name.replace('-', "_")
}

fn string_list(value: Option<&toml::Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Join `rel` (as written in a manifest) onto the repo-relative `dir`,
/// resolving `.` and `..`.
fn join_rel(dir: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = dir.split('/').filter(|p| !p.is_empty()).collect();
    for part in rel.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

fn is_under(dir: &str, root: &str) -> bool {
    root.is_empty() || dir == root || dir.starts_with(&format!("{root}/"))
}

/// Whether the directory `dir` matches one of the workspace `root`'s member
/// (or exclude) patterns, which are relative to `root`.
fn matches_any(patterns: &[String], root: &str, dir: &str) -> bool {
    let relative = if root.is_empty() {
        dir
    } else {
        dir.strip_prefix(root)
            .map(|rest| rest.trim_start_matches('/'))
            .unwrap_or(dir)
    };
    let segments: Vec<&str> = relative.split('/').filter(|s| !s.is_empty()).collect();
    patterns.iter().any(|pattern| {
        let pattern: Vec<&str> = pattern
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .collect();
        glob_segments(&pattern, &segments)
    })
}

fn glob_segments(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|skip| glob_segments(rest, &path[skip..])),
        Some((head, rest)) => match path.split_first() {
            Some((segment, tail)) => glob_segment(head, segment) && glob_segments(rest, tail),
            None => false,
        },
    }
}

/// `*` and `?` within one path segment.
fn glob_segment(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('*', rest)) => (0..=t.len()).any(|skip| go(rest, &t[skip..])),
            Some(('?', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some((c, rest)) => t.first() == Some(c) && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

/// The `[dependencies]`, `[dev-dependencies]` and `[build-dependencies]`
/// tables of a manifest, plain and under `[target.'cfg(..)']`.
fn collect_dependency_tables(table: &toml::Table, dir: &str, out: &mut BTreeMap<String, Dep>) {
    const SECTIONS: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];
    for section in SECTIONS {
        if let Some(deps) = table.get(section).and_then(|v| v.as_table()) {
            out.extend(read_deps(deps, dir));
        }
    }
    if let Some(targets) = table.get("target").and_then(|v| v.as_table()) {
        for target in targets.values().filter_map(|v| v.as_table()) {
            for section in SECTIONS {
                if let Some(deps) = target.get(section).and_then(|v| v.as_table()) {
                    out.extend(read_deps(deps, dir));
                }
            }
        }
    }
}

fn read_deps(deps: &toml::Table, dir: &str) -> BTreeMap<String, Dep> {
    deps.iter()
        .map(|(key, value)| {
            let spec = value.as_table();
            let source = match spec {
                Some(spec) => match (
                    spec.get("path").and_then(|v| v.as_str()),
                    spec.get("workspace"),
                ) {
                    (Some(path), _) => DepSource::Path(join_rel(dir, path)),
                    (None, Some(toml::Value::Boolean(true))) => DepSource::Inherited,
                    _ => DepSource::Registry,
                },
                None => DepSource::Registry,
            };
            let renamed = spec.is_some_and(|spec| spec.contains_key("package"));
            (normalize(key), Dep { source, renamed })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    impl RustWorkspace {
        fn dirs(&self) -> BTreeSet<&str> {
            self.packages.keys().map(String::as_str).collect()
        }
    }

    fn write(root: &Path, rel: &str, text: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn workspace() -> (tempfile::TempDir, RustWorkspace) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/*\", \"tools/cli\"]\nexclude = [\"crates/skip\"]\n\
             [workspace.dependencies]\nshared-core = { path = \"crates/shared-core\" }\nserde = \"1\"\n",
        );
        write(
            root,
            "crates/shared-core/Cargo.toml",
            "[package]\nname = \"shared-core\"\n",
        );
        write(root, "crates/shared-core/src/lib.rs", "pub fn f() {}\n");
        write(
            root,
            "crates/app/Cargo.toml",
            "[package]\nname = \"app\"\n[dependencies]\nshared-core.workspace = true\nserde.workspace = true\n\
             renamed = { path = \"../util\", package = \"util\" }\nreqwest = \"0.12\"\n\
             [dev-dependencies]\nutil = \"9\"\n",
        );
        write(
            root,
            "crates/app/src/lib.rs",
            "pub use shared_core as core;\n",
        );
        write(
            root,
            "crates/util/Cargo.toml",
            "[package]\nname = \"util\"\n[lib]\nname = \"util_lib\"\npath = \"src/lib.rs\"\n",
        );
        write(root, "crates/util/src/lib.rs", "");
        write(
            root,
            "crates/skip/Cargo.toml",
            "[package]\nname = \"skip\"\n",
        );
        write(root, "crates/skip/src/lib.rs", "");
        let ws = RustWorkspace::scan(root);
        (dir, ws)
    }

    #[test]
    fn finds_every_package_and_its_files() {
        let (_dir, ws) = workspace();
        assert_eq!(
            ws.dirs(),
            BTreeSet::from([
                "crates/app",
                "crates/shared-core",
                "crates/skip",
                "crates/util"
            ])
        );
        assert_eq!(ws.package_of("crates/app/src/x/y.rs"), Some("crates/app"));
        assert_eq!(ws.package_of("crates/app/tests/t.rs"), Some("crates/app"));
        assert_eq!(ws.package_of("elsewhere/x.rs"), None);
    }

    #[test]
    fn resolves_path_inherited_and_registry_dependencies() {
        let (_dir, ws) = workspace();
        let member = |d: &str| Some(CrateRef::Member(d.to_string()));
        assert_eq!(
            ws.lookup("crates/app", "shared_core"),
            member("crates/shared-core")
        );
        assert_eq!(ws.lookup("crates/app", "renamed"), member("crates/util"));
        assert_eq!(ws.lookup("crates/app", "serde"), Some(CrateRef::External));
        assert_eq!(ws.lookup("crates/app", "reqwest"), Some(CrateRef::External));
        // The registry `util` dev-dependency wins over the sibling member.
        assert_eq!(ws.lookup("crates/app", "util"), Some(CrateRef::External));
        // A package names its own library; an undeclared sibling is found.
        assert_eq!(ws.lookup("crates/app", "app"), member("crates/app"));
        assert_eq!(ws.lookup("crates/shared-core", "app"), member("crates/app"));
        assert_eq!(ws.lookup("crates/shared-core", "skip"), None);
        assert_eq!(ws.lookup("crates/util", "util_lib"), member("crates/util"));
        assert_eq!(ws.lookup("crates/app", "nothing"), None);
    }

    #[test]
    fn a_path_dependency_is_named_by_its_library() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "a/Cargo.toml",
            "[package]\nname = \"a-pkg\"\n[lib]\nname = \"a_lib\"\n",
        );
        write(root, "a/src/lib.rs", "");
        write(
            root,
            "b/Cargo.toml",
            "[package]\nname = \"b\"\n[dependencies]\na-pkg = { path = \"../a\" }\nrn = { path = \"../a\", package = \"a-pkg\" }\n",
        );
        write(root, "b/src/lib.rs", "");
        let ws = RustWorkspace::scan(root);
        let member = Some(CrateRef::Member("a".into()));
        assert_eq!(ws.lookup("b", "a_lib"), member);
        assert_eq!(ws.lookup("b", "rn"), member);
        assert_eq!(ws.lookup("b", "a_pkg"), None);
        assert_eq!(ws.lookup("b", "a"), None);
    }

    #[test]
    fn follows_crate_level_reexports() {
        let (_dir, ws) = workspace();
        assert_eq!(
            ws.follow_alias("crates/app", "core"),
            Some(CrateRef::Member("crates/shared-core".into()))
        );
        assert_eq!(ws.follow_alias("crates/app", "missing"), None);
    }

    #[test]
    fn fingerprint_changes_with_the_manifests() {
        let (dir, ws) = workspace();
        let before = ws.fingerprint();
        assert_eq!(RustWorkspace::scan(dir.path()).fingerprint(), before);
        write(
            dir.path(),
            "crates/util/Cargo.toml",
            "[package]\nname = \"util2\"\n",
        );
        assert_ne!(RustWorkspace::scan(dir.path()).fingerprint(), before);
    }

    #[test]
    fn globs_match_one_segment_or_many() {
        assert!(glob_segments(&["crates", "*"], &["crates", "a"]));
        assert!(!glob_segments(&["crates", "*"], &["crates", "a", "b"]));
        assert!(glob_segments(&["crates", "**"], &["crates", "a", "b"]));
        assert!(glob_segment("a?c*", "abcdef"));
    }
}
