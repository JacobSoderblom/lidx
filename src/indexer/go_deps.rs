//! The interfaces a Go repository's dependencies declare (issue #495), read
//! from sources that are already on disk: the module's `vendor/` directory
//! or the module cache. Nothing is downloaded, nothing is built and `go` is
//! never run; a module whose sources are not on disk contributes nothing.
//!
//! A method of a live repository type that, with the type's other exported
//! methods, satisfies one of these interfaces is called by the dependency
//! through it (staticcheck's own rule 8.2: a concrete type implements every
//! known interface), so `dead_symbols` keeps it alive -- see
//! `db::external_ifaces`. The scan is keyed by module version
//! (`go_dep_packages`): a package already read for the version `go.mod`
//! requires is not read again, and one no longer required is dropped, so an
//! incremental sync after a `go.mod` edit leaves the same rows as a fresh
//! reindex.
//!
//! The module cache is `$GOMODCACHE`, else `$GOPATH/pkg/mod` (the first
//! `GOPATH` element), else those keys from the `go env` file (`$GOENV`, or
//! the user config directory's `go/env`), else `$HOME/go/pkg/mod`. Module
//! `example.com/Dep` at `v1.2.3` lives in `<cache>/example.com/!dep@v1.2.3`
//! (the cache escapes an upper-case letter as `!` and its lower case). A
//! vendored module lives in `<module dir>/vendor/<module path>` at the
//! version `vendor/modules.txt` records.

use crate::indexer::tree_helpers::node_text;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tree_sitter::{Node, Parser};

/// One exported interface of a dependency package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepInterface {
    pub name: String,
    /// `(method name, signature)` in the extractor's form
    /// (`(p []byte) -> (n int, err error)`).
    pub methods: Vec<(String, String)>,
    /// Embedded interfaces (a `type X = Y` alias counts as embedding `Y`):
    /// `Name` for one of the same package, `import/path.Name` for another
    /// package's, the standard library included.
    pub embeds: Vec<String>,
}

/// A dependency package the repository imports, located on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepPackage {
    pub module: String,
    pub version: String,
    pub pkg_path: String,
    /// `vendor` or `modcache`.
    pub origin: &'static str,
    pub dir: PathBuf,
}

/// One `go.mod` of the repository with what the scan needs of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoModFile {
    /// Directory of the `go.mod`, relative to the repo root (`""` = root).
    pub dir: String,
    /// `(module path, version)` required, replacements applied.
    pub requires: Vec<(String, String)>,
    /// `(module path, version)` of `vendor/modules.txt`, when present.
    pub vendored: Vec<(String, String)>,
}

/// Every `go.mod` of the repository (`testdata` samples left out, as the Go
/// tool leaves them out), with its requirements and vendored modules.
pub fn go_mod_files(repo_root: &Path) -> Vec<GoModFile> {
    let mut out = Vec::new();
    let walk = ignore::WalkBuilder::new(repo_root)
        .filter_entry(|e| e.file_name() != "node_modules")
        .build();
    for entry in walk.flatten() {
        if entry.file_name() != "go.mod" || !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        let Some(dir) = entry
            .path()
            .parent()
            .and_then(|p| p.strip_prefix(repo_root).ok())
            .map(|p| p.to_string_lossy().replace('\\', "/"))
        else {
            continue;
        };
        if dir.split('/').any(|part| part == "testdata") {
            continue;
        }
        let vendored = std::fs::read_to_string(repo_root.join(&dir).join("vendor/modules.txt"))
            .map(|t| vendored_modules(&t))
            .unwrap_or_default();
        out.push(GoModFile {
            dir,
            requires: requirements(&text),
            vendored,
        });
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    out
}

/// The `require` directives of a `go.mod` (block form included) as
/// `(module path, version)`, with its `replace` directives applied: a module
/// replaced by another module takes the replacement's path and version; one
/// replaced by a directory is left out (a directory has no version to key a
/// cache by, and an in-repo one is indexed anyway).
pub fn requirements(go_mod: &str) -> Vec<(String, String)> {
    /// `(replaced path, replaced version if any, replacement module and
    /// version; `None` for a directory)`.
    type Replace = (String, Option<String>, Option<(String, String)>);
    let mut requires: Vec<(String, String)> = Vec::new();
    let mut replaces: Vec<Replace> = Vec::new();
    let mut block: Option<&str> = None;
    for raw in go_mod.lines() {
        let line = raw.split("//").next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (keyword, rest) = match block {
            Some(kw) => {
                if line == ")" {
                    block = None;
                    continue;
                }
                (kw, line)
            }
            None => {
                let Some((kw, rest)) = line.split_once(char::is_whitespace) else {
                    continue;
                };
                let rest = rest.trim();
                if rest == "(" {
                    if matches!(kw, "require" | "replace") {
                        block = Some(if kw == "require" {
                            "require"
                        } else {
                            "replace"
                        });
                    }
                    continue;
                }
                (kw, rest)
            }
        };
        match keyword {
            "require" => {
                let mut words = rest.split_whitespace();
                if let (Some(path), Some(version)) = (words.next(), words.next()) {
                    requires.push((path.trim_matches('"').to_string(), version.to_string()));
                }
            }
            "replace" => {
                let Some((left, right)) = rest.split_once("=>") else {
                    continue;
                };
                let mut left = left.split_whitespace();
                let Some(path) = left.next() else {
                    continue;
                };
                let version = left.next().map(str::to_string);
                let mut right = right.split_whitespace();
                let target = right.next().unwrap_or("").trim_matches('"');
                let target_version = right.next();
                let replacement = match target_version {
                    Some(v) if !is_dir_target(target) => Some((target.to_string(), v.to_string())),
                    _ => None,
                };
                replaces.push((path.trim_matches('"').to_string(), version, replacement));
            }
            _ => {}
        }
    }
    requires
        .into_iter()
        .filter_map(|(path, version)| {
            // the most specific replace wins: a versioned one over a bare one
            let hit = replaces
                .iter()
                .filter(|(p, v, _)| *p == path && v.as_deref().is_none_or(|v| v == version))
                .max_by_key(|(_, v, _)| v.is_some());
            match hit {
                None => Some((path, version)),
                Some((_, _, Some(replacement))) => Some(replacement.clone()),
                Some((_, _, None)) => None,
            }
        })
        .collect()
}

fn is_dir_target(target: &str) -> bool {
    target.starts_with("./")
        || target.starts_with("../")
        || target == "."
        || target == ".."
        || target.starts_with('/')
        || target.starts_with('\\')
        || target.chars().nth(1) == Some(':')
}

/// The `(module path, version)` pairs of a `vendor/modules.txt` (its
/// `# path version` lines; an `=> replacement` keeps the replaced path, as
/// the vendored directory is named after it).
pub fn vendored_modules(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("# ")?;
            let mut words = rest.split_whitespace();
            let path = words.next()?;
            let version = words.next()?;
            if path == "explicit" || !version.starts_with('v') {
                return None;
            }
            Some((path.to_string(), version.to_string()))
        })
        .collect()
}

/// The module cache directory: `GOMODCACHE`, else `GOPATH/pkg/mod`, else
/// the same keys from the `go env` file, else `<home>/go/pkg/mod`. `env`
/// reads one environment variable; the `go` tool is never run.
pub fn module_cache_dir(
    env: &dyn Fn(&str) -> Option<String>,
    home: Option<&Path>,
) -> Option<PathBuf> {
    let set = |value: Option<String>| value.filter(|v| !v.trim().is_empty());
    if let Some(cache) = set(env("GOMODCACHE")) {
        return Some(PathBuf::from(cache));
    }
    if let Some(gopath) = set(env("GOPATH")) {
        return Some(gopath_mod(&gopath));
    }
    let env_file = set(env("GOENV")).map(PathBuf::from).or_else(|| {
        let config = set(env("XDG_CONFIG_HOME")).map(PathBuf::from).or_else(|| {
            let home = home?;
            Some(if cfg!(target_os = "macos") {
                home.join("Library/Application Support")
            } else if cfg!(windows) {
                PathBuf::from(set(env("APPDATA"))?)
            } else {
                home.join(".config")
            })
        })?;
        Some(config.join("go").join("env"))
    });
    if let Some(text) = env_file.and_then(|p| std::fs::read_to_string(p).ok()) {
        let value = |key: &str| {
            text.lines().find_map(|line| {
                let (k, v) = line.split_once('=')?;
                (k.trim() == key && !v.trim().is_empty()).then(|| v.trim().to_string())
            })
        };
        if let Some(cache) = value("GOMODCACHE") {
            return Some(PathBuf::from(cache));
        }
        if let Some(gopath) = value("GOPATH") {
            return Some(gopath_mod(&gopath));
        }
    }
    Some(home?.join("go").join("pkg").join("mod"))
}

fn gopath_mod(gopath: &str) -> PathBuf {
    let sep = if cfg!(windows) { ';' } else { ':' };
    let first = gopath.split(sep).next().unwrap_or(gopath);
    PathBuf::from(first).join("pkg").join("mod")
}

/// A module path as the module cache spells it: an upper-case letter is
/// `!` and its lower case.
pub fn escape_module_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if c.is_ascii_uppercase() {
            out.push('!');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The dependency packages the repository imports whose sources are on disk:
/// `imports` is `(importing module dir, import path)` for every import of a
/// package outside the repository; `module_cache` the module cache, if any.
/// An import path is matched to the longest required module path of the
/// importing module's `go.mod`; the vendored copy is preferred, then the
/// module cache. The result is sorted and has no duplicates.
pub fn locate_packages(
    repo_root: &Path,
    go_mods: &[GoModFile],
    imports: &HashSet<(String, String)>,
    module_cache: Option<&Path>,
) -> Vec<DepPackage> {
    let mut out: Vec<DepPackage> = Vec::new();
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    for (module_dir, import_path) in imports {
        let Some(go_mod) = go_mods.iter().find(|m| m.dir == *module_dir) else {
            continue;
        };
        let Some((module, version)) = go_mod
            .requires
            .iter()
            .filter(|(path, _)| import_path == path || import_path.starts_with(&format!("{path}/")))
            .max_by_key(|(path, _)| path.len())
        else {
            continue;
        };
        let rest = import_path
            .strip_prefix(module.as_str())
            .map(|r| r.trim_start_matches('/'))
            .unwrap_or("");
        let vendored = go_mod
            .vendored
            .iter()
            .find(|(path, _)| path == module)
            .map(|(_, v)| v.clone());
        let (origin, version, dir) = match vendored {
            Some(v) => (
                "vendor",
                v,
                repo_root.join(module_dir).join("vendor").join(import_path),
            ),
            None => {
                let Some(cache) = module_cache else {
                    continue;
                };
                let mut dir = cache.join(format!("{}@{}", escape_module_path(module), version));
                if !rest.is_empty() {
                    dir = dir.join(rest);
                }
                ("modcache", version.clone(), dir)
            }
        };
        if !dir.is_dir() {
            continue;
        }
        let key = (module.clone(), version.clone(), import_path.clone());
        if !seen.insert(key) {
            continue;
        }
        out.push(DepPackage {
            module: module.clone(),
            version,
            pkg_path: import_path.clone(),
            origin,
            dir,
        });
    }
    out.sort_by(|a, b| {
        (&a.module, &a.version, &a.pkg_path).cmp(&(&b.module, &b.version, &b.pkg_path))
    });
    out
}

/// The exported interfaces declared by the non-test Go files of `dir`,
/// read-only; a file that cannot be read or parsed contributes nothing.
pub fn interfaces_in_dir(parser: &mut Parser, dir: &Path) -> Vec<DepInterface> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|e| e == "go")
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("_test.go"))
        })
        .collect();
    files.sort();
    let mut out = Vec::new();
    for file in files {
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        out.extend(interfaces_in(parser, &source));
    }
    out
}

/// The exported interfaces one Go source declares.
pub fn interfaces_in(parser: &mut Parser, source: &str) -> Vec<DepInterface> {
    let Some(tree) = parser.parse(source, None) else {
        return Vec::new();
    };
    let root = tree.root_node();
    let imports = super::go::collect_import_aliases(root, source);
    let mut out = Vec::new();
    let mut cursor = root.walk();
    for decl in root.named_children(&mut cursor) {
        if decl.kind() != "type_declaration" {
            continue;
        }
        let mut specs = decl.walk();
        for spec in decl.named_children(&mut specs) {
            if !matches!(spec.kind(), "type_spec" | "type_alias") {
                continue;
            }
            let (Some(name), Some(ty)) = (
                spec.child_by_field_name("name"),
                spec.child_by_field_name("type"),
            ) else {
                continue;
            };
            let name = node_text(name, source);
            if !name.chars().next().is_some_and(char::is_uppercase) {
                continue;
            }
            match ty.kind() {
                "interface_type" => out.push(DepInterface {
                    name,
                    methods: interface_methods(ty, source),
                    embeds: super::go::embedded_interfaces(ty, source)
                        .into_iter()
                        .map(|text| qualify(&text, &imports))
                        .collect(),
                }),
                // `type Alias = pkg.Iface`: whatever the target is; a
                // non-interface target resolves to nothing later on.
                "type_identifier" | "qualified_type" if spec.kind() == "type_alias" => {
                    out.push(DepInterface {
                        name,
                        methods: Vec::new(),
                        embeds: vec![qualify(&node_text(ty, source), &imports)],
                    })
                }
                _ => {}
            }
        }
    }
    out
}

/// `(name, signature)` of each method spec of an interface type.
fn interface_methods(type_node: Node<'_>, source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cursor = type_node.walk();
    for spec in type_node.named_children(&mut cursor) {
        if spec.kind() != "method_elem" {
            continue;
        }
        let Some(name) = spec.child_by_field_name("name") else {
            continue;
        };
        let name = node_text(name, source);
        if name.is_empty() {
            continue;
        }
        let signature = super::go::extract_function_signature(spec, source).unwrap_or_default();
        out.push((name, signature));
    }
    out
}

/// `pkg.Name` through the file's imports to `import/path.Name`; a bare
/// `Name` stays (the same package).
fn qualify(text: &str, imports: &HashMap<String, String>) -> String {
    match text.split_once('.') {
        Some((alias, name)) => match imports.get(alias) {
            Some(path) => format!("{path}.{name}"),
            None => text.to_string(),
        },
        None => text.to_string(),
    }
}

/// Whether the `place` an `IMPORTS` row records names a dependency: a
/// package outside the repository (an import path), or a vendored copy
/// inside it (`><module dir>/vendor/<path>`, indexed with the repository but
/// a dependency all the same).
pub fn is_dependency_place(place: &str) -> bool {
    match place.strip_prefix('>') {
        None => true,
        Some(dir) => dir.split('/').any(|seg| seg == "vendor"),
    }
}

/// `(package path, interface name)` of an embed written as `import/path.Name`;
/// `None` for a same-package `Name`.
pub fn split_embed(embed: &str) -> Option<(&str, &str)> {
    let (path, name) = embed.rsplit_once('.')?;
    (!path.is_empty()).then_some((path, name))
}

/// What one dependency sync did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GoDepSync {
    /// Packages read this time (not already stored for their module version).
    pub packages_read: usize,
    /// Packages dropped (no longer required or imported).
    pub packages_removed: usize,
    /// Interfaces read this time.
    pub interfaces_read: usize,
    /// Packages the repository imports whose sources are on disk.
    pub packages_available: usize,
}

impl super::Indexer {
    /// Bring the stored dependency interfaces in line with what `go.mod`
    /// requires and the Go files of `graph_version` import: read the packages
    /// not stored yet for their module version, drop the ones no longer
    /// wanted. Read-only on the sources; a repository without Go files or
    /// without `go.mod` does nothing. Cheap when nothing changed (one query
    /// over the `IMPORTS` rows and the `go.mod` files).
    pub fn sync_go_dependencies(&mut self, graph_version: i64) -> Result<GoDepSync> {
        let imports = self.db.go_foreign_imports(graph_version)?;
        let stored = self.db.go_dep_packages()?;
        if imports.is_empty() {
            if !stored.is_empty() {
                self.db.store_go_dep_packages(&HashSet::new(), &[])?;
            }
            return Ok(GoDepSync {
                packages_removed: stored.len(),
                ..GoDepSync::default()
            });
        }
        let started = std::time::Instant::now();
        let go_mods = go_mod_files(&self.repo_root);
        let module_dir_of = |file: &str| -> Option<String> {
            go_mods
                .iter()
                .filter(|m| {
                    m.dir.is_empty()
                        || file
                            .strip_prefix(m.dir.as_str())
                            .is_some_and(|rest| rest.starts_with('/'))
                })
                .max_by_key(|m| m.dir.len())
                .map(|m| m.dir.clone())
        };
        let wanted_imports: HashSet<(String, String)> = imports
            .into_iter()
            .filter_map(|(file, import_path)| Some((module_dir_of(&file)?, import_path)))
            .collect();
        let env = |key: &str| std::env::var(key).ok();
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let module_cache = match &self.go_module_cache {
            Some(dir) => Some(dir.clone()),
            None => module_cache_dir(&env, home.as_deref()),
        };
        let available = locate_packages(
            &self.repo_root,
            &go_mods,
            &wanted_imports,
            module_cache.as_deref(),
        );
        let keep: HashSet<DepPackageKey> = available
            .iter()
            .map(|p| (p.module.clone(), p.version.clone(), p.pkg_path.clone()))
            .collect();
        let mut parser = Parser::new();
        parser.set_language(&tree_sitter_go::LANGUAGE.into())?;
        let mut added = Vec::new();
        let mut interfaces_read = 0;
        for package in available.iter() {
            let key = (
                package.module.clone(),
                package.version.clone(),
                package.pkg_path.clone(),
            );
            if stored.contains(&key) {
                continue;
            }
            let interfaces = interfaces_in_dir(&mut parser, &package.dir);
            interfaces_read += interfaces.len();
            added.push((package.clone(), interfaces));
        }
        let removed = stored.iter().filter(|k| !keep.contains(*k)).count();
        if !added.is_empty() || removed > 0 {
            self.db.store_go_dep_packages(&keep, &added)?;
            eprintln!(
                "lidx: Go dependencies: read {} package(s) ({} interface(s)), dropped {} in {:.1}s",
                added.len(),
                interfaces_read,
                removed,
                started.elapsed().as_secs_f64()
            );
        }
        Ok(GoDepSync {
            packages_read: added.len(),
            packages_removed: removed,
            interfaces_read,
            packages_available: available.len(),
        })
    }
}

/// `(module, version, package path)` of one stored dependency package.
pub type DepPackageKey = (String, String, String);

#[cfg(test)]
mod tests {
    use super::*;

    fn go_parser() -> Parser {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_go::LANGUAGE.into())
            .unwrap();
        parser
    }

    #[test]
    fn requirements_apply_replace_directives() {
        let text = "module example.com/m\n\ngo 1.22\n\nrequire (\n\tgithub.com/a/b v1.2.3\n\tgithub.com/c/d v0.1.0 // indirect\n\tgithub.com/e/f v2.0.0\n\tgithub.com/g/h v1.0.0\n)\n\nrequire github.com/i/j v3.1.0\n\nreplace github.com/c/d => github.com/c/d2 v0.2.0\nreplace github.com/e/f v2.0.0 => ../f\nreplace (\n\tgithub.com/g/h => /abs/h\n)\n";
        assert_eq!(
            requirements(text),
            [
                ("github.com/a/b".to_string(), "v1.2.3".to_string()),
                ("github.com/c/d2".to_string(), "v0.2.0".to_string()),
                ("github.com/i/j".to_string(), "v3.1.0".to_string()),
            ]
        );
    }

    #[test]
    fn vendored_modules_read_modules_txt() {
        let text = "# github.com/a/b v1.2.3\n## explicit; go 1.22\ngithub.com/a/b\ngithub.com/a/b/sub\n# github.com/c/d v0.1.0 => github.com/c/d2 v0.2.0\n## explicit\ngithub.com/c/d\n";
        assert_eq!(
            vendored_modules(text),
            [
                ("github.com/a/b".to_string(), "v1.2.3".to_string()),
                ("github.com/c/d".to_string(), "v0.1.0".to_string()),
            ]
        );
    }

    #[test]
    fn module_cache_comes_from_the_environment_then_the_env_file_then_home() {
        let home = tempfile::tempdir().unwrap();
        let none = |_: &str| None;
        assert_eq!(
            module_cache_dir(&none, Some(home.path())),
            Some(home.path().join("go/pkg/mod"))
        );
        let cache = |key: &str| (key == "GOMODCACHE").then(|| "/c".to_string());
        assert_eq!(module_cache_dir(&cache, None), Some(PathBuf::from("/c")));
        let gopath = |key: &str| (key == "GOPATH").then(|| "/p:/q".to_string());
        assert_eq!(
            module_cache_dir(&gopath, None),
            Some(PathBuf::from("/p/pkg/mod"))
        );
        let env_file = home.path().join("env");
        std::fs::write(&env_file, "GOFLAGS=-mod=mod\nGOMODCACHE=/from/env/file\n").unwrap();
        let goenv_path = env_file.to_string_lossy().to_string();
        let goenv = move |key: &str| (key == "GOENV").then(|| goenv_path.clone());
        assert_eq!(
            module_cache_dir(&goenv, None),
            Some(PathBuf::from("/from/env/file"))
        );
        assert_eq!(
            escape_module_path("github.com/Azure/go-SDK"),
            "github.com/!azure/go-!s!d!k"
        );
    }

    #[test]
    fn interfaces_are_read_with_methods_embeds_and_aliases() {
        let source = "package dep\n\nimport (\n\t\"io\"\n\tother \"example.com/other/pkg\"\n)\n\n\
            type Runner interface {\n\t// Run runs.\n\tRun(ctx string, n int) error\n\tStop()\n}\n\n\
            type Closer interface {\n\tio.Reader\n\tother.Thing\n\tRunner\n\tClose() error\n}\n\n\
            type Alias = other.Loader\n\ntype hidden interface{ X() }\n\ntype NotIface struct{}\n\n\
            type Set[T any] interface{ Add(t T) }\n";
        let got = interfaces_in(&mut go_parser(), source);
        assert_eq!(
            got,
            [
                DepInterface {
                    name: "Runner".into(),
                    methods: vec![
                        ("Run".into(), "(ctx string, n int) -> error".into()),
                        ("Stop".into(), "()".into()),
                    ],
                    embeds: vec![],
                },
                DepInterface {
                    name: "Closer".into(),
                    methods: vec![("Close".into(), "() -> error".into())],
                    embeds: vec![
                        "io.Reader".into(),
                        "example.com/other/pkg.Thing".into(),
                        "Runner".into()
                    ],
                },
                DepInterface {
                    name: "Alias".into(),
                    methods: vec![],
                    embeds: vec!["example.com/other/pkg.Loader".into()],
                },
                DepInterface {
                    name: "Set".into(),
                    methods: vec![("Add".into(), "(t T)".into())],
                    embeds: vec![],
                },
            ]
        );
        assert_eq!(split_embed("io.Reader"), Some(("io", "Reader")));
        assert_eq!(
            split_embed("example.com/other/pkg.Thing"),
            Some(("example.com/other/pkg", "Thing"))
        );
        assert_eq!(split_embed("Runner"), None);
    }

    #[test]
    fn packages_are_located_in_vendor_then_the_module_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let write = |rel: &str, text: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write(
            "repo/go.mod",
            "module example.com/app\n\nrequire (\n\tgithub.com/a/Vend v1.0.0\n\tgithub.com/b/cached v2.1.0\n\tgithub.com/c/missing v0.0.1\n)\n",
        );
        write(
            "repo/vendor/modules.txt",
            "# github.com/a/Vend v1.0.0\n## explicit\ngithub.com/a/Vend/sub\n",
        );
        write("repo/vendor/github.com/a/Vend/sub/x.go", "package sub\n");
        write("cache/github.com/b/cached@v2.1.0/y.go", "package cached\n");
        write(
            "cache/github.com/b/cached@v2.1.0/inner/z.go",
            "package inner\n",
        );
        let go_mods = go_mod_files(&root.join("repo"));
        assert_eq!(go_mods.len(), 1);
        let imports: HashSet<(String, String)> = [
            ("".to_string(), "github.com/a/Vend/sub".to_string()),
            ("".to_string(), "github.com/b/cached".to_string()),
            ("".to_string(), "github.com/b/cached/inner".to_string()),
            ("".to_string(), "github.com/b/cached/absent".to_string()),
            ("".to_string(), "github.com/c/missing".to_string()),
            ("".to_string(), "io".to_string()),
        ]
        .into_iter()
        .collect();
        let found = locate_packages(
            &root.join("repo"),
            &go_mods,
            &imports,
            Some(&root.join("cache")),
        );
        let brief: Vec<(String, String, String, &str)> = found
            .iter()
            .map(|p| {
                (
                    p.module.clone(),
                    p.version.clone(),
                    p.pkg_path.clone(),
                    p.origin,
                )
            })
            .collect();
        assert_eq!(
            brief,
            [
                (
                    "github.com/a/Vend".to_string(),
                    "v1.0.0".to_string(),
                    "github.com/a/Vend/sub".to_string(),
                    "vendor"
                ),
                (
                    "github.com/b/cached".to_string(),
                    "v2.1.0".to_string(),
                    "github.com/b/cached".to_string(),
                    "modcache"
                ),
                (
                    "github.com/b/cached".to_string(),
                    "v2.1.0".to_string(),
                    "github.com/b/cached/inner".to_string(),
                    "modcache"
                ),
            ]
        );
        assert_eq!(found[0].dir, root.join("repo/vendor/github.com/a/Vend/sub"));
        assert_eq!(
            found[2].dir,
            root.join("cache/github.com/b/cached@v2.1.0/inner")
        );
        // no module cache at all: only the vendored package
        let found = locate_packages(&root.join("repo"), &go_mods, &imports, None);
        assert_eq!(found.len(), 1);
    }
}
